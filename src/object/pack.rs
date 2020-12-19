//! Binary serialization format for moving `Object`s between stores.

use std::cell::{Cell, RefCell};
use std::convert::{TryFrom, TryInto};
use std::fmt::{self, Debug, Formatter};
use std::io::{self, Read, Write};

use anyhow::anyhow;
use serde::Serialize;

use super::{Blob, ContentAddressable, Object, ObjectId, ObjectKind};
use crate::util;

const MAGIC_VALUE: &[u8] = b"store-pack";
const FORMAT_VERSION: u8 = 1;
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
        match o {
            Object::Blob(blob) => {
                let kind = if blob.is_executable() {
                    EntryKind::Exec
                } else {
                    EntryKind::Blob
                };
                let header = make_header(blob.object_id(), kind, blob.size());
                self.inner.write_all(&header)?;
                let mut content = blob.into_content()?;
                util::copy_wide(&mut content, &mut self.inner)?;
            }
            Object::Tree(tree) => self.write_meta_object(&tree, EntryKind::Tree)?,
            Object::Package(pkg) => self.write_meta_object(&pkg, EntryKind::Package)?,
            Object::Spec(spec) => self.write_meta_object(&spec, EntryKind::Spec)?,
        }

        self.inner.flush()?;
        Ok(())
    }

    fn write_meta_object<O>(&mut self, obj: &O, kind: EntryKind) -> anyhow::Result<()>
    where
        O: ContentAddressable + Serialize,
    {
        let body = serde_json::to_vec(&obj)?;
        let header = make_header(obj.object_id(), kind, body.len() as u64);
        let combined: Vec<_> = header.iter().copied().chain(body).collect();
        self.inner.write_all(&combined)?;
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

fn make_header(id: ObjectId, kind: EntryKind, size: u64) -> [u8; HEADER_LEN] {
    let mut buf = [0u8; HEADER_LEN];
    buf[..ObjectId::LENGTH].copy_from_slice(id.as_bytes());
    buf[ObjectId::LENGTH] = kind as u8;
    buf[ObjectId::LENGTH + 1..].copy_from_slice(&size.to_be_bytes());
    buf
}

/// Deserializes a binary packfile into an iterator of `Object`s.
pub struct PackReader<R> {
    inner: PackReaderInner<R>,
}

impl<R: Read> PackReader<R> {
    /// Creates a new `PackReader<R>`.
    ///
    /// Returns `Err` if the given I/O stream is not in pack format.
    pub fn new(mut inner: R) -> anyhow::Result<Self> {
        let mut header = [0u8; MAGIC_VALUE.len() + 1];
        inner.read_exact(&mut header)?;

        let inner = PackReaderInner {
            reader: RefCell::new(inner),
            state: Cell::new(State::Ready),
        };

        match &header[..] {
            [m @ .., FORMAT_VERSION] if m == MAGIC_VALUE => Ok(PackReader { inner }),
            _ => Err(anyhow!("magic value not found, not a store packfile")),
        }
    }

    /// Unwraps this `PackReader<R>`, returning the underlying buffer.
    pub fn into_inner(self) -> R {
        self.inner.reader.into_inner()
    }

    /// Iterates over each entry in the pack stream.
    ///
    /// The [`Entry`] items yielded by this iterator are _lazy_: they do not buffer their entire
    /// contents into memory when created. Instead, the caller is expected to drain the `Entry`
    /// completely, either via [`Entry::deserialize()`] or the [`std::io::Read`] implementation,
    /// before advancing the iterator to the next item.
    ///
    /// If the iterator is advanced without completely draining the previous `Entry`, it will
    /// always return `Err` forever until the `Entry` is completely drained.
    pub fn entries(&mut self) -> Entries<R> {
        Entries { inner: &self.inner }
    }
}

/// Deserializes the entire pack file into a stream of objects.
impl<R: Read> IntoIterator for PackReader<R> {
    type Item = anyhow::Result<Object>;
    type IntoIter = Objects<R>;

    fn into_iter(self) -> Self::IntoIter {
        Objects { inner: self.inner }
    }
}

impl<R: Debug> Debug for PackReader<R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct(stringify!(PackReader))
            .field("inner", &self.inner.reader)
            .finish()
    }
}

/// Inner type for `PackReader` that is fully immutable.
///
/// The reason behind this is significant. If the `next_entry()` method below were instead mutable,
/// neither the `Entries` nor `Objects` iterators can be made to work, due to lifetime constraints.
/// See below for another real-world example of this pattern in a similar program:
///
/// https://github.com/alexcrichton/tar-rs/blob/462ebb1a591f2ebb57abe1bcd9db5e42fe1738d8/src/archive.rs
struct PackReaderInner<R> {
    reader: RefCell<R>,
    state: Cell<State>,
}

impl<R: Read> PackReaderInner<R> {
    fn next_entry(&self) -> anyhow::Result<Option<Entry<R>>> {
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

        match self.state.get() {
            State::Ready => {}
            State::Reading => return Err(anyhow!("previous `Entry` is not completely drained")),
            State::Done => return Ok(None),
        }

        let mut header = [0u8; HEADER_LEN];
        self.reader.borrow_mut().read_exact(&mut header)?;

        if header.iter().all(|&b| b == 0) {
            self.state.set(State::Done);
            return Ok(None);
        }

        let (object_id, kind, size) = parse_header(header)?;
        self.state.set(State::Reading);

        Ok(Some(Entry {
            id: object_id,
            kind,
            size,
            stream: (&self).take(size),
            state: &self.state,
        }))
    }
}

impl<'a, R: Read> Read for &'a PackReaderInner<R> {
    fn read(&mut self, into: &mut [u8]) -> io::Result<usize> {
        self.reader.borrow_mut().read(into)
    }
}

/// A read-only view into a single pack entry.
///
/// This struct is constructed by the [`Entries`] iterator returned by [`PackReader::entries()`].
/// See its documentation for more.
pub struct Entry<'a, R> {
    id: ObjectId,
    kind: EntryKind,
    size: u64,
    stream: io::Take<&'a PackReaderInner<R>>,
    state: &'a Cell<State>,
}

impl<'a, R: Read> Entry<'a, R> {
    /// Returns the declared cryptographic hash of the contained object.
    #[inline]
    pub fn id(&self) -> ObjectId {
        self.id
    }

    /// Returns the kind of the contained object.
    #[inline]
    pub fn kind(&self) -> ObjectKind {
        self.kind.into()
    }

    /// Returns the size, in bytes, of the contained object.
    #[inline]
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Deserializes the rest of this entry into an [`Object`](super::Object).
    ///
    /// Returns `Err` if the object failed to parse, the cryptographic hash did not match, or an
    /// I/O error occurred.
    pub fn deserialize(mut self) -> anyhow::Result<Object> {
        let object = match self.kind {
            EntryKind::Blob | EntryKind::Exec => {
                let mut writer = Blob::from_writer(self.kind == EntryKind::Exec);
                util::copy_wide(&mut self, &mut writer)?;
                let (blob, _) = writer.finish();
                Object::Blob(blob)
            }
            EntryKind::Tree => {
                let mut buffer = vec![0u8; self.size as usize].into_boxed_slice();
                self.read_exact(&mut buffer)?;
                let tree = serde_json::from_slice(&buffer)?;
                Object::Tree(tree)
            }
            EntryKind::Package => {
                let mut buffer = vec![0u8; self.size as usize].into_boxed_slice();
                self.read_exact(&mut buffer)?;
                let pkg = serde_json::from_slice(&buffer)?;
                Object::Package(pkg)
            }
            EntryKind::Spec => {
                let mut buffer = vec![0u8; self.size as usize].into_boxed_slice();
                self.read_exact(&mut buffer)?;
                let spec = serde_json::from_slice(&buffer)?;
                Object::Spec(spec)
            }
        };

        if object.object_id() == self.id {
            Ok(object)
        } else {
            Err(anyhow!(
                "hash mismatch: {:?} hashed to {}, but pack file lists {}",
                object.kind(),
                object.object_id(),
                self.id
            ))
        }
    }
}

/// Reads the raw [`Object`](super::Object) content from this `Entry` in a streaming fashion.
///
/// # Safety
///
/// This interface does not verify the object against the [`ObjectId`](super::ObjectId) checksum
/// returned by [`Entry::id()`]. It is the caller's responsibility to check this value after the
/// underlying I/O stream has been exhausted.
///
/// For a safe and convenient deserialization method, use [`Entry::deserialize()`].
impl<'a, R: Read> Read for Entry<'a, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.state.get() {
            State::Ready => return Ok(0),
            State::Reading => {}
            State::Done => unreachable!(),
        }

        let len = self.stream.read(buf)?;
        if self.stream.limit() == 0 {
            self.state.set(State::Ready);
        }

        Ok(len)
    }
}

impl<'a, R> Debug for Entry<'a, R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct(stringify!(Entry))
            .field("id", &self.id)
            .field("kind", &ObjectKind::from(self.kind))
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Clone, Copy)]
#[repr(u8)]
enum State {
    Ready,
    Reading,
    Done,
}

/// An iterator over the entries of a pack file.
///
/// This struct is created by [`PackReader::entries()`]. See its documentation for details.
pub struct Entries<'a, R: 'a> {
    inner: &'a PackReaderInner<R>,
}

impl<'a, R: Read> Iterator for Entries<'a, R> {
    type Item = anyhow::Result<Entry<'a, R>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next_entry().transpose()
    }
}

impl<'a, R: Debug> Debug for Entries<'a, R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct(stringify!(Entries))
            .field("inner", &self.inner.reader)
            .finish()
    }
}

/// An iterator that deserializes a pack file into [`Object`](super::Object)s.
///
/// This struct is created by the `into_iter` method on [`PackReader`] (provided by the
/// [`IntoIterator`] trait).
pub struct Objects<R> {
    inner: PackReaderInner<R>,
}

impl<R: Read> Iterator for Objects<R> {
    type Item = anyhow::Result<Object>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next_entry()
            .transpose()
            .map(|r| r.and_then(|entry| entry.deserialize()))
    }
}

impl<R: Debug> Debug for Objects<R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct(stringify!(Objects))
            .field("inner", &self.inner.reader)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::{Seek, SeekFrom};

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

        for (i, result) in reader.into_iter().enumerate() {
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
