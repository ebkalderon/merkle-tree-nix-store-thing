//! Binary serialization format for moving `Object`s between stores.

use std::convert::{TryFrom, TryInto};
use std::io::{Read, Write};

use anyhow::anyhow;

use super::{Blob, ContentAddressable, Object, ObjectId, PagedBuffer};

const MAGIC_VALUE: &[u8] = b"store-pack";
const FORMAT_VERSION: u8 = 1;
const HEADER_LEN: usize = ObjectId::LENGTH + 9;

#[derive(PartialEq)]
#[repr(u8)]
enum ObjectKind {
    Blob = 0,
    Exec = 1,
    Tree = 2,
    Package = 3,
}

impl TryFrom<u8> for ObjectKind {
    type Error = anyhow::Error;

    fn try_from(byte: u8) -> Result<Self, Self::Error> {
        match byte {
            0 => Ok(ObjectKind::Blob),
            1 => Ok(ObjectKind::Exec),
            2 => Ok(ObjectKind::Tree),
            3 => Ok(ObjectKind::Package),
            b => Err(anyhow!("unrecognized object kind byte: {}", b)),
        }
    }
}

/// Serializes store objects into a binary pack file.
#[derive(Debug)]
pub struct PackWriter<W> {
    inner: W,
}

impl<W: Write> PackWriter<W> {
    /// Creates a new `PackWriter<W>`.
    ///
    /// Returns `Err` if the magic value and pack format version could not be written.
    pub fn new(mut inner: W) -> anyhow::Result<Self> {
        let mut begin_buf = [0u8; MAGIC_VALUE.len() + 1];
        begin_buf[..MAGIC_VALUE.len()].copy_from_slice(MAGIC_VALUE);
        begin_buf[MAGIC_VALUE.len()] = FORMAT_VERSION;
        inner.write_all(&begin_buf)?;
        inner.flush()?;
        Ok(PackWriter { inner })
    }

    /// Appends the given object to the pack, writing it to the underlying buffer.
    ///
    /// Returns `Err` if a serialization or I/O error occurred.
    pub fn append(&mut self, o: Object) -> anyhow::Result<()> {
        fn make_header(id: ObjectId, kind: ObjectKind, len: u64) -> [u8; HEADER_LEN] {
            let mut buf = [0u8; HEADER_LEN];
            buf[..ObjectId::LENGTH].copy_from_slice(id.as_bytes());
            buf[ObjectId::LENGTH] = kind as u8;
            buf[ObjectId::LENGTH + 1..].copy_from_slice(&len.to_be_bytes());
            buf
        }

        match o {
            Object::Blob(mut blob) => {
                let kind = if blob.is_executable() {
                    ObjectKind::Exec
                } else {
                    ObjectKind::Blob
                };
                let header = make_header(blob.object_id(), kind, blob.len());
                self.inner.write_all(&header)?;
                crate::copy_wide(&mut blob, &mut self.inner)?;
            }
            Object::Tree(tree) => {
                let body = serde_json::to_vec(&tree)?;
                let header = make_header(tree.object_id(), ObjectKind::Tree, body.len() as u64);
                let combined: Vec<_> = header.iter().copied().chain(body).collect();
                self.inner.write_all(&combined)?;
            }
            Object::Package(pkg) => {
                let body = serde_json::to_vec(&pkg)?;
                let header = make_header(pkg.object_id(), ObjectKind::Package, body.len() as u64);
                let combined: Vec<_> = header.iter().copied().chain(body).collect();
                self.inner.write_all(&combined)?;
            }
        }

        self.inner.flush()?;
        Ok(())
    }

    /// Writes the pack footer and unwraps this `PackWriter<W>`, returning the underlying buffer.
    ///
    /// Returns `Err` if the footer could not be written or the buffer could not be flushed.
    pub fn finish(self) -> anyhow::Result<W> {
        let mut inner = self.inner;
        inner.write_all(&[0u8; HEADER_LEN])?;
        inner.flush()?;
        Ok(inner)
    }
}

/// Deserializes a binary packfile into an iterator of `Object`s.
#[derive(Debug)]
pub struct PackReader<R> {
    inner: R,
    done: bool,
}

impl<R: Read> PackReader<R> {
    /// Creates a new `PackReader<R>`.
    ///
    /// Returns `Err` if the given I/O stream is not in pack format.
    pub fn new(mut inner: R) -> anyhow::Result<Self> {
        let mut header = [0u8; MAGIC_VALUE.len() + 1];
        inner.read_exact(&mut header)?;
        match &header[..] {
            [m @ .., FORMAT_VERSION] if m == MAGIC_VALUE => Ok(PackReader { inner, done: false }),
            _ => Err(anyhow!("magic value not found, not a store packfile")),
        }
    }

    /// Unwraps this `PackReader<R>`, returning the underlying buffer.
    pub fn into_inner(self) -> R {
        self.inner
    }

    fn next_object(&mut self) -> anyhow::Result<Option<Object>> {
        fn parse_header(header: [u8; HEADER_LEN]) -> anyhow::Result<(ObjectId, ObjectKind, u64)> {
            let object_id = header[..ObjectId::LENGTH]
                .try_into()
                .map(ObjectId::from_bytes)?;
            let kind = ObjectKind::try_from(header[ObjectId::LENGTH])?;
            let len = header[ObjectId::LENGTH + 1..]
                .try_into()
                .map(u64::from_be_bytes)?;

            Ok((object_id, kind, len))
        }

        if self.done {
            return Ok(None);
        }

        let mut header = [0u8; HEADER_LEN];
        self.inner.read_exact(&mut header)?;

        if header.iter().all(|&b| b == 0) {
            self.done = true;
            return Ok(None);
        }

        let (object_id, kind, len) = parse_header(header)?;
        match kind {
            ObjectKind::Blob | ObjectKind::Exec => {
                let mut reader = (&mut self.inner).take(len);
                let mut buffer = PagedBuffer::with_threshold(32 * 1024 * 1024);
                crate::copy_wide(&mut reader, &mut buffer)?;
                Ok(Some(Object::Blob(Blob::from_reader_raw(
                    Box::new(buffer),
                    kind == ObjectKind::Exec,
                    len,
                    object_id,
                ))))
            }
            ObjectKind::Tree => {
                let mut buffer = vec![0u8; len as usize].into_boxed_slice();
                self.inner.read_exact(&mut buffer)?;
                let tree = serde_json::from_slice(&buffer)?;
                Ok(Some(Object::Tree(tree)))
            }
            ObjectKind::Package => {
                let mut buffer = vec![0u8; len as usize].into_boxed_slice();
                self.inner.read_exact(&mut buffer)?;
                let pkg = serde_json::from_slice(&buffer)?;
                Ok(Some(Object::Package(pkg)))
            }
        }
    }
}

impl<R: Read> Iterator for PackReader<R> {
    type Item = anyhow::Result<Object>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_object().transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{Seek, SeekFrom};

    use super::*;
    use crate::{Entry, Package, Tree};

    const PACKAGE_NAME: &str = "example";
    const PACKAGE_SYSTEM: &str = "x86_64-linux-gnu";

    fn example_objects() -> Vec<Object> {
        let first = Object::Blob(Blob::from_vec(b"hello".to_vec(), false));
        let second = Object::Blob(Blob::from_vec(b"hola".to_vec(), true));
        let third = Object::Tree({
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
        });
        let fourth = Object::Package(Package {
            name: PACKAGE_NAME.into(),
            system: PACKAGE_SYSTEM.into(),
            references: BTreeSet::new(),
            tree: third.object_id(),
        });

        vec![first, second, third, fourth]
    }

    #[test]
    fn round_trip() {
        let empty_buffer = std::io::Cursor::new(Vec::new());
        let mut writer = PackWriter::new(empty_buffer).expect("failed to init writer");
        for obj in example_objects() {
            writer.append(obj).expect("failed to serialize object");
        }

        let mut full_buffer = writer.finish().expect("failed to flush");
        full_buffer.seek(SeekFrom::Start(0)).unwrap();

        let reader = PackReader::new(full_buffer).expect("failed to init reader");
        let mut blob_ids = Vec::new();

        for (i, entry) in reader.enumerate() {
            eprintln!("received ({}): {:?}", i, entry);
            match (i, entry) {
                (0, Ok(Object::Blob(b))) if !b.is_executable() => blob_ids.push(b.object_id()),
                (1, Ok(Object::Blob(b))) if b.is_executable() => blob_ids.push(b.object_id()),
                (2, Ok(Object::Tree(t))) if t.entries.len() == 2 => {
                    let refs: Vec<_> = t.references().map(|(id, _)| id).collect();
                    assert!(refs.contains(&blob_ids[0]));
                    assert!(refs.contains(&blob_ids[1]));
                    blob_ids.push(t.object_id());
                }
                (3, Ok(Object::Package(p))) => {
                    assert_eq!(p.name, PACKAGE_NAME);
                    assert_eq!(p.system, PACKAGE_SYSTEM);
                    assert_eq!(p.references, BTreeSet::new());
                    assert_eq!(p.tree, blob_ids[2]);
                }
                (i, other) => panic!("received unexpected object ({}): {:?}", i, other),
            }
        }
    }
}
