//! Binary serialization format for moving `Object`s between stores.

use std::convert::{TryFrom, TryInto};
use std::io;
use std::os::unix::io::{FromRawFd, IntoRawFd};

use anyhow::anyhow;
use futures::{stream, Stream};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use super::{Blob, ContentAddressable, Object, ObjectId, ObjectKind};
use crate::util;

const MAGIC_VALUE: &[u8] = b"store-pack";
const FORMAT_VERSION: u8 = 1;
const PACK_MAGIC_LEN: usize = MAGIC_VALUE.len() + 1;
const HEADER_LEN: usize = ObjectId::LENGTH + 9;

#[derive(Clone, Copy, PartialEq)]
#[repr(u8)]
enum EntryKind {
    Blob = 0,
    Exec = 1,
    Tree = 2,
    Package = 3,
    Spec = 4,
}

impl TryFrom<u8> for EntryKind {
    type Error = anyhow::Error;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0 => Ok(EntryKind::Blob),
            1 => Ok(EntryKind::Exec),
            2 => Ok(EntryKind::Tree),
            3 => Ok(EntryKind::Package),
            4 => Ok(EntryKind::Spec),
            b => Err(anyhow!("unrecognized object kind byte: {}", b)),
        }
    }
}

impl From<EntryKind> for ObjectKind {
    fn from(kind: EntryKind) -> Self {
        match kind {
            EntryKind::Blob | EntryKind::Exec => ObjectKind::Blob,
            EntryKind::Tree => ObjectKind::Tree,
            EntryKind::Package => ObjectKind::Package,
            EntryKind::Spec => ObjectKind::Spec,
        }
    }
}

/// Serializes store objects into a binary pack file.
#[derive(Debug)]
pub struct PackWriter<W> {
    inner: W,
}

impl<W: AsyncWrite + Unpin> PackWriter<W> {
    /// Creates a new `PackWriter<W>`.
    ///
    /// Returns `Err` if the magic value and pack format version could not be written.
    pub async fn new(mut inner: W) -> anyhow::Result<Self> {
        let mut magic = [0u8; PACK_MAGIC_LEN];
        magic[..MAGIC_VALUE.len()].copy_from_slice(MAGIC_VALUE);
        magic[MAGIC_VALUE.len()] = FORMAT_VERSION;
        inner.write_all(&magic).await?;
        inner.flush().await?;
        Ok(PackWriter { inner })
    }

    /// Appends the given object to the pack, writing it to the underlying buffer.
    ///
    /// Returns `Err` if a serialization or I/O error occurred.
    pub async fn append(&mut self, o: Object) -> anyhow::Result<()> {
        match o {
            Object::Blob(blob) => {
                let kind = if blob.is_executable() {
                    EntryKind::Exec
                } else {
                    EntryKind::Blob
                };
                let header = make_header(blob.object_id(), kind, blob.size());
                self.inner.write_all(&header).await?;

                let (reader, mut sync_writer) = os_pipe::pipe()?;
                let mut reader = unsafe { tokio::fs::File::from_raw_fd(reader.into_raw_fd()) };

                let handle = tokio::task::spawn_blocking(move || -> io::Result<_> {
                    let mut content = blob.into_content()?;
                    util::copy_wide(&mut content, &mut sync_writer)?;
                    Ok(())
                });

                tokio::io::copy(&mut reader, &mut self.inner).await?;
                handle.await.unwrap()?;
            }
            Object::Tree(tree) => self.write_meta_object(&tree, EntryKind::Tree).await?,
            Object::Package(pkg) => self.write_meta_object(&pkg, EntryKind::Package).await?,
            Object::Spec(spec) => self.write_meta_object(&spec, EntryKind::Spec).await?,
        }

        self.inner.flush().await?;

        Ok(())
    }

    async fn write_meta_object<O>(&mut self, obj: &O, kind: EntryKind) -> anyhow::Result<()>
    where
        O: ContentAddressable + Serialize,
    {
        let body = serde_json::to_vec(&obj)?;
        let header = make_header(obj.object_id(), kind, body.len() as u64);
        let combined: Vec<_> = header.iter().copied().chain(body).collect();
        self.inner.write_all(&combined).await?;
        Ok(())
    }

    /// Writes the pack footer and unwraps this `PackWriter<W>`, returning the underlying buffer.
    ///
    /// Returns `Err` if the footer could not be written or the buffer could not be flushed.
    pub async fn finish(self) -> anyhow::Result<W> {
        let mut inner = self.inner;
        inner.write_all(&[0u8; HEADER_LEN]).await?;
        inner.flush().await?;
        Ok(inner)
    }
}

fn make_header(id: ObjectId, kind: EntryKind, size: u64) -> [u8; HEADER_LEN] {
    let mut buf = [0u8; HEADER_LEN];
    buf[..ObjectId::LENGTH].copy_from_slice(id.as_bytes());
    buf[ObjectId::LENGTH] = kind as u8;
    buf[ObjectId::LENGTH + 1..].copy_from_slice(&size.to_be_bytes());
    buf
}

/// Deserializes a binary pack file into a stream of `Object`s.
///
/// The stream may yield `Err` if the stream is not a pack file, an object entry failed to parse,
/// the cryptographic hash for an object did not match, or an I/O error occurred.
pub fn pack_reader<'a, R>(reader: R) -> impl Stream<Item = anyhow::Result<Object>> + 'a
where
    R: AsyncRead + Unpin + 'a,
{
    // `PackReader<R>` was turned from a struct into a function because implementing the logic
    // entirely with polling was just too hard and too messy. Perhaps if Rust permits `async fn` in
    // traits one day, and `AsyncRead` is defined in terms of `async fn`, we could restore the old
    // implementation from the Git history, and sprinkle some `async`/`.await` keywords on it.

    async fn next_entry<R>(reader: &mut R, is_start: bool) -> anyhow::Result<Option<Object>>
    where
        R: AsyncRead + Unpin,
    {
        fn parse_header(header: [u8; HEADER_LEN]) -> anyhow::Result<(ObjectId, EntryKind, u64)> {
            let object_id = header[..ObjectId::LENGTH]
                .try_into()
                .map(ObjectId::from_bytes)?;
            let kind = EntryKind::try_from(header[ObjectId::LENGTH])?;
            let size = header[ObjectId::LENGTH + 1..]
                .try_into()
                .map(u64::from_be_bytes)?;

            Ok((object_id, kind, size))
        }

        if is_start {
            let mut magic = [0u8; PACK_MAGIC_LEN];
            reader.read_exact(&mut magic).await?;

            match &magic[..] {
                [m @ .., FORMAT_VERSION] if m == MAGIC_VALUE => {}
                _ => return Err(anyhow!("magic value not found, not a store packfile")),
            }
        }

        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header).await?;

        if header.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        let (object_id, kind, size) = parse_header(header)?;
        let object = match kind {
            EntryKind::Blob | EntryKind::Exec => {
                let (mut sync_reader, writer) = os_pipe::pipe()?;
                let mut writer = unsafe { tokio::fs::File::from_raw_fd(writer.into_raw_fd()) };

                let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                    let mut sync_writer = Blob::from_writer(kind == EntryKind::Exec);
                    util::copy_wide(&mut sync_reader, &mut sync_writer)?;
                    let (blob, _) = sync_writer.finish();
                    Ok(blob)
                });

                tokio::io::copy(&mut reader.take(size), &mut writer).await?;
                drop(writer);

                let blob = handle.await.unwrap()?;
                Object::Blob(blob)
            }
            EntryKind::Tree => {
                let mut buffer = vec![0u8; size as usize].into_boxed_slice();
                reader.read_exact(&mut buffer).await?;
                let tree = serde_json::from_slice(&buffer)?;
                Object::Tree(tree)
            }
            EntryKind::Package => {
                let mut buffer = vec![0u8; size as usize].into_boxed_slice();
                reader.read_exact(&mut buffer).await?;
                let pkg = serde_json::from_slice(&buffer)?;
                Object::Package(pkg)
            }
            EntryKind::Spec => {
                let mut buffer = vec![0u8; size as usize].into_boxed_slice();
                reader.read_exact(&mut buffer).await?;
                let spec = serde_json::from_slice(&buffer)?;
                Object::Spec(spec)
            }
        };

        if object.object_id() == object_id {
            Ok(Some(object))
        } else {
            Err(anyhow!(
                "hash mismatch: {:?} hashed to {}, but pack file lists {}",
                object.kind(),
                object.object_id(),
                object_id
            ))
        }
    }

    stream::unfold((reader, true), |(mut reader, is_start)| async move {
        next_entry(&mut reader, is_start)
            .await
            .transpose()
            .map(|res| (res, (reader, false)))
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::SeekFrom;

    use futures::{pin_mut, StreamExt};
    use tokio::io::AsyncSeekExt;

    use super::*;
    use crate::{platform, Entry, Package, Platform, References, Tree};

    const PACKAGE_NAME: &str = "example";
    #[rustfmt::skip::macros(platform)]
    const PACKAGE_SYSTEM: Platform = platform!(x86_64-linux-gnu);

    fn example_objects() -> Vec<Object> {
        let (first, _) = Blob::from_bytes(b"hello".to_vec(), false);
        let (second, _) = Blob::from_bytes(b"hola".to_vec(), true);
        let third = {
            let mut entries = BTreeMap::new();
            entries.insert(
                "regular.txt".into(),
                Entry::Blob {
                    id: first.object_id(),
                },
            );
            entries.insert(
                "executable".into(),
                Entry::Blob {
                    id: second.object_id(),
                },
            );
            Tree { entries }
        };
        let fourth = Package {
            name: PACKAGE_NAME.parse().unwrap(),
            system: PACKAGE_SYSTEM,
            references: References::new(),
            self_references: BTreeMap::new(),
            tree: third.object_id(),
        };

        vec![
            Object::Blob(first),
            Object::Blob(second),
            Object::Tree(third),
            Object::Package(fourth),
        ]
    }

    #[tokio::test]
    async fn round_trip() {
        let empty_buffer = std::io::Cursor::new(Vec::new());
        let mut writer = PackWriter::new(empty_buffer)
            .await
            .expect("failed to init writer");

        for obj in example_objects() {
            writer
                .append(obj)
                .await
                .expect("failed to serialize object");
        }

        let mut full_buffer = writer.finish().await.expect("failed to flush");
        full_buffer.seek(SeekFrom::Start(0)).await.unwrap();

        let mut blob_ids = Vec::new();
        let reader = pack_reader(&mut full_buffer).enumerate();
        pin_mut!(reader);

        while let Some((i, result)) = reader.next().await {
            eprintln!("received ({}): {:?}", i, result);
            match (i, result) {
                (0, Ok(Object::Blob(b))) if !b.is_executable() => blob_ids.push(b.object_id()),
                (1, Ok(Object::Blob(b))) if b.is_executable() => blob_ids.push(b.object_id()),
                (2, Ok(Object::Tree(t))) if t.entries.len() == 2 => {
                    let refs: Vec<_> = t.references().map(|(id, _)| id).collect();
                    assert!(refs.contains(&blob_ids[0]));
                    assert!(refs.contains(&blob_ids[1]));
                    blob_ids.push(t.object_id());
                }
                (3, Ok(Object::Package(p))) => {
                    assert_eq!(p.name.as_ref(), PACKAGE_NAME);
                    assert_eq!(p.system, PACKAGE_SYSTEM);
                    assert_eq!(p.references, References::new());
                    assert_eq!(p.self_references, BTreeMap::new());
                    assert_eq!(p.tree, blob_ids[2]);
                }
                (i, other) => panic!("received unexpected object ({}): {:?}", i, other),
            }
        }
    }
}
