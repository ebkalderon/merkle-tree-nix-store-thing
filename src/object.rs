//! Types of Merkle tree objects.

pub use self::id::ObjectId;
pub use self::name::{InstallName, PackageName};
pub use self::platform::{Arch, Env, Os, Platform};
pub use self::reference::{Offsets, References};

pub(crate) use self::reference::RewriteSink;

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::anyhow;
use memmap::Mmap;
use semver::Version;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use self::id::HashWriter;
use self::reference::ReferenceSink;
use self::spooled::SpooledTempFile;
use crate::util;

pub mod pack;

mod id;
mod name;
mod platform;
mod reference;
mod spooled;

const BLOB_FILE_EXT: &str = "blob";
const TREE_FILE_EXT: &str = "tree";
const PACKAGE_FILE_EXT: &str = "pkg";
const SPEC_FILE_EXT: &str = "spec";

/// A trait designating objects belonging to a `Store`.
///
/// These objects are nodes in a Merkle tree and can be stored and retrieved by their `ObjectId`.
pub trait ContentAddressable {
    /// Returns the unique cryptographic hash of the object.
    fn object_id(&self) -> ObjectId;

    /// Returns the size of the object, in bytes.
    fn len(&self) -> u64;
}

/// A list specifying all types of `Store` objects.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObjectKind {
    /// Plain file or executable.
    Blob,
    /// Filesystem directory possibly containing other `Blob` and `Tree` objects, one level deep.
    Tree,
    /// Installed package with a name, platform, package references, and an output directory tree.
    Package,
    /// Manifest which describes how to build a package from source.
    Spec,
}

impl ObjectKind {
    /// Enumerates all variants of `ObjectKind`.
    pub fn iter() -> impl Iterator<Item = Self> {
        use std::iter::once;
        once(ObjectKind::Blob)
            .chain(once(ObjectKind::Tree))
            .chain(once(ObjectKind::Package))
            .chain(once(ObjectKind::Spec))
    }

    /// Returns the string representation of the `ObjectKind`.
    ///
    /// This is commonly used as the file extension for objects in a filesystem-backed `Store`.
    pub const fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Blob => BLOB_FILE_EXT,
            ObjectKind::Tree => TREE_FILE_EXT,
            ObjectKind::Package => PACKAGE_FILE_EXT,
            ObjectKind::Spec => SPEC_FILE_EXT,
        }
    }
}

impl FromStr for ObjectKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            BLOB_FILE_EXT => Ok(ObjectKind::Blob),
            TREE_FILE_EXT => Ok(ObjectKind::Tree),
            PACKAGE_FILE_EXT => Ok(ObjectKind::Package),
            SPEC_FILE_EXT => Ok(ObjectKind::Spec),
            ext => Err(anyhow!("unrecognized object file extension: {}", ext)),
        }
    }
}

/// A Merkle tree object belonging to a `Store`.
#[derive(Debug)]
pub enum Object {
    /// Plain file or executable.
    Blob(Blob),
    /// Filesystem directory possibly containing other `Blob` and `Tree` objects, one level deep.
    Tree(Tree),
    /// Installed package with a name, platform, package references, and an output directory tree.
    Package(Package),
    /// Manifest which describes how to build a package from source.
    Spec(Spec),
}

impl Object {
    /// Returns the type of this object.
    #[inline]
    pub fn kind(&self) -> ObjectKind {
        match *self {
            Object::Blob(_) => ObjectKind::Blob,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Package(_) => ObjectKind::Package,
            Object::Spec(_) => ObjectKind::Spec,
        }
    }

    /// Attempts to consume this object and return a `Blob`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Blob`.
    #[inline]
    pub fn into_blob(self) -> Result<Blob, Self> {
        match self {
            Object::Blob(b) => Ok(b),
            other => Err(other),
        }
    }

    /// Attempts to consume this object and return a `Tree`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Tree`.
    #[inline]
    pub fn into_tree(self) -> Result<Tree, Self> {
        match self {
            Object::Tree(t) => Ok(t),
            other => Err(other),
        }
    }

    /// Attempts to consume this object and return a `Package`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Package`.
    #[inline]
    pub fn into_package(self) -> Result<Package, Self> {
        match self {
            Object::Package(o) => Ok(o),
            other => Err(other),
        }
    }

    /// Attempts to consume this object and return a `Spec`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Spec`.
    #[inline]
    pub fn into_spec(self) -> Result<Spec, Self> {
        match self {
            Object::Spec(o) => Ok(o),
            other => Err(other),
        }
    }
}

impl ContentAddressable for Object {
    fn object_id(&self) -> ObjectId {
        match *self {
            Object::Blob(ref o) => o.object_id(),
            Object::Tree(ref t) => t.object_id(),
            Object::Package(ref o) => o.object_id(),
            Object::Spec(ref o) => o.object_id(),
        }
    }

    fn len(&self) -> u64 {
        match *self {
            Object::Blob(ref o) => o.len(),
            Object::Tree(ref t) => t.len(),
            Object::Package(ref o) => o.len(),
            Object::Spec(ref o) => o.len(),
        }
    }
}

/// Underlying I/O streams that can back a blob object.
#[derive(Debug)]
enum Kind {
    Inline(Cursor<Vec<u8>>),
    Spooled(SpooledTempFile),
    Store(PathBuf),
}

/// Represents a blob object, i.e. a regular file or executable.
///
/// Unlike most files, though, blobs store no additional metadata apart from the executable bit and
/// the size on disk, in bytes. Timestamps are fixed to January 1st, 1970 and all extended
/// attributes are removed.
#[derive(Debug)]
pub struct Blob {
    stream: Kind,
    is_executable: bool,
    length: u64,
    object_id: ObjectId,
}

impl Blob {
    /// Hashes and returns a new `Blob` object from the given buffer.
    pub fn from_bytes(input: Vec<u8>, is_executable: bool) -> (Self, References) {
        let hasher = id::Hasher::new_blob(is_executable);
        let mut writer = ReferenceSink::new(HashWriter::with_hasher(hasher, std::io::sink()));
        writer.write_all(&input).unwrap();
        let (hasher, references) = writer.into_inner();

        let blob = Blob {
            length: input.len() as u64,
            stream: Kind::Inline(Cursor::new(input)),
            is_executable,
            object_id: hasher.object_id(),
        };

        (blob, references)
    }

    /// Hashes and returns a new `Blob` object from the file located at `path`.
    ///
    /// This constructor is generally more efficient than [`Blob::from_writer()`]. It uses
    /// memory-mapping and multi-threaded hashing whenever possible to rapidly process the file,
    /// otherwise falling back to either:
    ///
    /// 1. Regular buffered I/O only if the file in question is too large to be memory-mapped.
    /// 2. Reading the entire file into memory if it is smaller than 16 KiB, where the cost of
    ///    buffered file I/O or memory-mapping is usually not worth it.
    ///
    /// When processing files on the local filesystem, prefer using this constructor over
    /// `Blob::from_writer()` whenever possible.
    ///
    /// Returns `Err` if `path` does not exist or does not refer to a file, the user does not have
    /// permission to read the file, or another I/O error occurred.
    pub fn from_path<P: AsRef<Path>>(path: P) -> io::Result<(Self, References)> {
        util::open_large_read(
            path.as_ref(),
            |cursor, is_executable| Ok(Blob::from_bytes(cursor.into_inner(), is_executable)),
            |mmap, is_executable| {
                let mut writer = Blob::from_writer(is_executable);
                writer.write_all(mmap.get_ref())?;
                Ok(writer.finish())
            },
            |mut file, is_executable| {
                let mut writer = Blob::from_writer(is_executable);
                util::copy_wide(&mut file, &mut writer)?;
                Ok(writer.finish())
            },
        )
    }

    /// Returns a writer which creates a new `Blob` object from a stream of bytes.
    ///
    /// This writer will initially buffer the I/O stream in memory, spilling over into a temporary
    /// file on disk if the internal buffer grows beyond a 1 MiB threshold.
    pub fn from_writer(is_executable: bool) -> BlobWriter {
        let hasher = id::Hasher::new_blob(is_executable);
        let spooled = SpooledTempFile::new(1 * 1024 * 1024);
        BlobWriter {
            inner: ReferenceSink::new(HashWriter::with_hasher(hasher, spooled)),
            is_executable,
            length: 0,
        }
    }

    pub(crate) fn from_store_path(path: PathBuf, object_id: ObjectId) -> io::Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::metadata(&path)?;
        Ok(Blob {
            stream: Kind::Store(path),
            is_executable: metadata.mode() & 0o100 != 0,
            length: metadata.len(),
            object_id,
        })
    }

    /// Returns `true` if this blob has its executable bit set.
    #[inline]
    pub fn is_executable(&self) -> bool {
        self.is_executable
    }

    /// Consumes the blob and returns an I/O stream of its on-disk content.
    ///
    /// Returns `Err` if an I/O error occurred.
    pub fn into_content(self) -> io::Result<impl Read + Seek> {
        match self.stream {
            Kind::Inline(mut cursor) => {
                cursor.set_position(0);
                Ok(Contents::Inline(cursor))
            }
            Kind::Spooled(mut spooled) => {
                spooled.seek(SeekFrom::Start(0))?;
                Ok(Contents::Spooled(spooled))
            }
            Kind::Store(path) => util::open_large_read(
                &path,
                |cursor, _| Ok(Contents::Inline(cursor)),
                |mmap, _| Ok(Contents::Mmap(mmap)),
                |file, _| Ok(Contents::File(file)),
            ),
        }
    }

    /// Persists the blob to disk with as little redundant copying as possible.
    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        let mode = if self.is_executable { 0o544 } else { 0o444 };

        let result = match self.stream {
            Kind::Inline(inner) => {
                let mut temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
                temp.write_all(inner.get_ref())?;
                temp.flush()?;
                util::normalize_perms(temp.path(), mode)?;
                temp.persist(dest).map(|_| {}).map_err(|e| e.error)
            }
            Kind::Spooled(inner) => inner.persist(dest, mode),
            Kind::Store(src) if src == dest => panic!("cannot persist file to itself"),
            Kind::Store(src) => {
                let file_name = src.file_name().unwrap();
                let temp_path = PathBuf::from("/var/tmp").join(file_name);
                std::fs::copy(src, &temp_path)?;
                util::normalize_perms(&temp_path, mode)?;
                std::fs::rename(&temp_path, dest)
            }
        };

        match result {
            Ok(_) => Ok(()),
            Err(_) if dest.is_file() => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

impl ContentAddressable for Blob {
    fn object_id(&self) -> ObjectId {
        self.object_id
    }

    fn len(&self) -> u64 {
        self.length
    }
}

/// A writer which creates a new `Blob` from a byte stream.
///
/// This struct is created by [`Blob::from_writer()`]. See its documentation for more.
#[derive(Debug)]
pub struct BlobWriter {
    inner: ReferenceSink<HashWriter<SpooledTempFile>>,
    is_executable: bool,
    length: u64,
}

impl BlobWriter {
    /// Returns the finished `Blob` and its run-time references, if any were detected.
    pub fn finish(self) -> (Blob, References) {
        let (hasher, references) = self.inner.into_inner();
        let blob = Blob {
            object_id: hasher.object_id(),
            stream: Kind::Spooled(hasher.into_inner()),
            is_executable: self.is_executable,
            length: self.length,
        };

        (blob, references)
    }
}

impl Write for BlobWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.length += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

enum Contents {
    Inline(Cursor<Vec<u8>>),
    Mmap(Cursor<Mmap>),
    File(File),
    Spooled(SpooledTempFile),
}

impl Read for Contents {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            Contents::Inline(ref mut inner) => inner.read(buf),
            Contents::Mmap(ref mut inner) => inner.read(buf),
            Contents::File(ref mut inner) => inner.read(buf),
            Contents::Spooled(ref mut inner) => inner.read(buf),
        }
    }
}

impl Seek for Contents {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match *self {
            Contents::Inline(ref mut inner) => inner.seek(pos),
            Contents::Mmap(ref mut inner) => inner.seek(pos),
            Contents::File(ref mut inner) => inner.seek(pos),
            Contents::Spooled(ref mut inner) => inner.seek(pos),
        }
    }
}

/// A list of possible entries inside of a directory tree.
#[derive(Clone, Debug, Hash, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Entry {
    Tree { id: ObjectId },
    Blob { id: ObjectId },
    Symlink { target: PathBuf },
}

/// Represents a directory tree object.
///
/// Tree objects are only one level deep and may contain other trees, blobs, and symlinks.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Tree {
    /// File names mapped to directory entries in the tree.
    pub entries: BTreeMap<String, Entry>,
}

impl Tree {
    /// Iterates over all object IDs that this tree object references.
    pub fn references(&self) -> impl Iterator<Item = (ObjectId, ObjectKind)> + '_ {
        self.entries.values().filter_map(|entry| match entry {
            Entry::Tree { id } => Some((*id, ObjectKind::Tree)),
            Entry::Blob { id } => Some((*id, ObjectKind::Blob)),
            Entry::Symlink { .. } => None,
        })
    }

    /// Persists object to disk as a read-only JSON file located at `dest`.
    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        persist_json(&self, dest)
    }
}

impl ContentAddressable for Tree {
    fn object_id(&self) -> ObjectId {
        let json = serde_json::to_vec(self).unwrap();
        id::Hasher::new_tree().update(&json).finish()
    }

    fn len(&self) -> u64 {
        serde_json::to_vec(self).unwrap().len() as u64
    }
}

/// Represents a package object.
///
/// Package objects have an output directory tree and may reference other packages at run-time or
/// at build-time.
#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
pub struct Package {
    /// The human-readable name.
    pub name: PackageName,
    /// The target platform it supports.
    pub system: Platform,
    /// Any other packages it references at run-time.
    pub references: References,
    /// Any blob objects which contain self-references.
    pub self_references: BTreeMap<ObjectId, Offsets>,
    /// Output directory tree to be installed.
    pub tree: ObjectId,
}

impl Package {
    /// Computes the directory name where the package should be installed.
    pub fn install_name(&self) -> InstallName {
        InstallName::new(&self.name, self.object_id())
    }

    /// Persists object to disk as a read-only JSON file located at `dest`.
    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        persist_json(&self, dest)
    }
}

impl ContentAddressable for Package {
    fn object_id(&self) -> ObjectId {
        let json = serde_json::to_vec(self).unwrap();
        id::Hasher::new_package().update(&json).finish()
    }

    fn len(&self) -> u64 {
        serde_json::to_vec(self).unwrap().len() as u64
    }
}

/// Represents a package specification object.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Spec {
    /// The human-readable name.
    pub name: PackageName,
    /// The semantic version string.
    pub version: Version,
    /// Short description of the package.
    pub description: Option<String>,
    /// SPDX 2.1 expression.
    pub license: Option<SmolStr>,
    /// The target platform it supports.
    ///
    /// If left unspecified, it is assumed to match the build host.
    pub target: Option<Platform>,
    /// Packages required at run-time and build-time.
    pub dependencies: BTreeSet<ObjectId>,
    /// Packages only available at build-time.
    pub build_dependencies: BTreeSet<ObjectId>,
    /// Build script to execute in sandbox.
    pub builder: String,
}

impl Spec {
    /// Persists object to disk as a read-only JSON file located at `dest`.
    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        persist_json(&self, dest)
    }
}

impl ContentAddressable for Spec {
    fn object_id(&self) -> ObjectId {
        let json = serde_json::to_vec(self).unwrap();
        id::Hasher::new_spec().update(&json).finish()
    }

    fn len(&self) -> u64 {
        serde_json::to_vec(self).unwrap().len() as u64
    }
}

/// Persists `val` to disk as a read-only JSON file located at `dest`.
fn persist_json<T: Serialize>(val: &T, dest: &Path) -> anyhow::Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
    serde_json::to_writer(&mut temp, val)?;
    temp.flush()?;
    util::normalize_perms(temp.path(), 0o444)?;

    match temp.persist(dest) {
        Ok(_) => Ok(()),
        Err(_) if dest.is_file() => Ok(()),
        Err(e) => Err(e.into()),
    }
}
