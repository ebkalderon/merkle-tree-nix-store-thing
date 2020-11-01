//! Types of objects that comprise the Merkle tree.

pub use self::id::{HashWriter, Hasher, ObjectId};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::io::{Cursor, Read, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::anyhow;
use filetime::FileTime;
use memmap::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use self::buffer::PagedBuffer;

pub mod pack;

mod buffer;
mod id;

const BLOB_FILE_EXT: &str = "blob";
const TREE_FILE_EXT: &str = "tree";
const PACKAGE_FILE_EXT: &str = "pkg";

/// A trait designating objects belonging to a `Store`.
///
/// These objects are nodes in a Merkle tree and can be stored and retrieved by their `ObjectId`.
pub trait ContentAddressable {
    /// Returns the unique cryptographic hash of the object.
    fn object_id(&self) -> ObjectId;
}

/// A list specifying all types of `Store` objects.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum ObjectKind {
    /// Plain file or executable.
    Blob,
    /// Filesystem directory possibly containing other `Blob` and `Tree` objects, one level deep.
    Tree,
    /// Installed package with a name, platform, package references, and an output directory tree.
    Package,
}

impl ObjectKind {
    /// Enumerates all variants of `ObjectKind`.
    pub fn iter() -> impl Iterator<Item = Self> {
        use std::iter::once;
        once(ObjectKind::Blob)
            .chain(once(ObjectKind::Tree))
            .chain(once(ObjectKind::Package))
    }

    /// Returns the string representation of the `ObjectKind`.
    ///
    /// This is commonly used as the file extension for objects in a filesystem-backed `Store`.
    pub const fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Blob => BLOB_FILE_EXT,
            ObjectKind::Tree => TREE_FILE_EXT,
            ObjectKind::Package => PACKAGE_FILE_EXT,
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
}

impl Object {
    /// Returns the type of this object.
    pub fn kind(&self) -> ObjectKind {
        match *self {
            Object::Blob(_) => ObjectKind::Blob,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Package(_) => ObjectKind::Package,
        }
    }

    /// Attempts to consume this object and return a `Blob`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Blob`.
    pub fn into_blob(self) -> Result<Blob, Self> {
        match self {
            Object::Blob(b) => Ok(b),
            other => Err(other),
        }
    }

    /// Attempts to consume this object and return a `Tree`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Tree`.
    pub fn into_tree(self) -> Result<Tree, Self> {
        match self {
            Object::Tree(t) => Ok(t),
            other => Err(other),
        }
    }

    /// Attempts to consume this object and return a `Package`.
    ///
    /// Returns `Err(self)` if this object is not actually a `Package`.
    pub fn into_package(self) -> Result<Package, Self> {
        match self {
            Object::Package(o) => Ok(o),
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
        }
    }
}

/// Underlying I/O streams that can back a blob object.
enum Kind {
    Reader(Box<dyn Read>),
    Paged(PagedBuffer),
    File(tempfile::NamedTempFile),
    Mmap(Cursor<Mmap>),
}

/// Represents a blob object, i.e. a regular file or executable.
///
/// Unlike most files, though, blobs store no additional metadata apart from the executable bit and
/// the size on disk, in bytes. Timestamps are fixed to January 1st, 1970 and all extended
/// attributes are removed.
pub struct Blob {
    stream: Kind,
    is_executable: bool,
    length: u64,
    object_id: ObjectId,
}

impl Blob {
    /// Hashes and returns a new `Blob` object from the given buffer.
    pub fn from_vec(bytes: Vec<u8>, is_executable: bool) -> Self {
        let mut hasher = id::Hasher::new();
        hasher.update(blob_header(is_executable)).update(&bytes);
        Blob {
            length: bytes.len() as u64,
            stream: Kind::Reader(Box::new(Cursor::new(bytes))),
            is_executable,
            object_id: hasher.finish(),
        }
    }

    /// Hashes and returns a new `Blob` object from the file located at `path`.
    ///
    /// This constructor is more efficent than passing `std::fs::File` into `Blob::from_reader()`.
    /// It uses memory-mapping and multi-threaded hashing to rapidly process the file, falling back
    /// to regular file I/O only if the file in question is too large to be memory-mapped.
    ///
    /// When interacting with files on the local filesystem, prefer using this constructor over
    /// `Blob::from_reader()` whenever possible.
    ///
    /// Returns `Err` if `path` does not exist or does not refer to a file, the user does not have
    /// permission to read the file, or another I/O error occurred.
    pub fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let mut file = std::fs::File::open(&path)?;
        let metadata = file.metadata()?;
        let is_executable = metadata.mode() & 0o100 != 0;
        let header = blob_header(is_executable);

        if metadata.len() < 16 * 1024 {
            // Not worth it to mmap(2) small files. Load into memory instead.
            let buffer = Cursor::new(Vec::with_capacity(metadata.len() as usize));
            let mut writer = HashWriter::with_header(header, buffer);
            let length = crate::copy_wide(&mut file, &mut writer)?;
            Ok(Blob {
                object_id: writer.object_id(),
                stream: Kind::Reader(Box::new(writer.into_inner())),
                is_executable,
                length,
            })
        } else if metadata.len() <= isize::max_value() as u64 {
            // Prefer memory-mapping files wherever possible for performance.
            let mmap = unsafe { MmapOptions::new().len(metadata.len() as usize).map(&file)? };
            let mut hasher = id::Hasher::new();
            hasher.update(header).par_update(&mmap);
            Ok(Blob {
                stream: Kind::Mmap(Cursor::new(mmap)),
                is_executable,
                length: metadata.len(),
                object_id: hasher.finish(),
            })
        } else {
            // Only fall back to regular disk I/O if file is too large to mmap(2).
            let temp = tempfile::NamedTempFile::new()?;
            let mut writer = HashWriter::with_header(header, temp);
            let length = crate::copy_wide(&mut file, &mut writer)?;
            Ok(Blob {
                object_id: writer.object_id(),
                stream: Kind::File(writer.into_inner()),
                is_executable,
                length,
            })
        }
    }

    /// Hashes and returns a new `Blob` object from a reader.
    ///
    /// This will attempt to buffer the I/O stream into memory, spilling over into a temporary file
    /// on disk if the internal buffer grows beyond a 32 MB threshold.
    ///
    /// Returns `Err` if an I/O error occurred.
    pub fn from_reader<R: Read>(mut reader: R, is_executable: bool) -> anyhow::Result<Self> {
        let header = blob_header(is_executable);
        let paged_writer = PagedBuffer::with_threshold(32 * 1024 * 1024);
        let mut writer = HashWriter::with_header(header, paged_writer);
        let length = crate::copy_wide(&mut reader, &mut writer)?;

        Ok(Blob {
            object_id: writer.object_id(),
            stream: Kind::Paged(writer.into_inner()),
            is_executable,
            length,
        })
    }

    /// Constructs a new `Blob` from a reader and `ObjectId` without verifying the hash.
    pub(crate) fn from_reader_raw(
        reader: Box<dyn Read>,
        is_executable: bool,
        length: u64,
        object_id: ObjectId,
    ) -> Self {
        Blob {
            stream: Kind::Reader(reader),
            is_executable,
            length,
            object_id,
        }
    }

    /// Returns `true` if this blob has its executable bit set.
    pub fn is_executable(&self) -> bool {
        self.is_executable
    }

    /// Returns the size of the blob, in bytes.
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Persists the blob to disk with as little redundant copying as possible.
    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        if !dest.exists() {
            let mode = if self.is_executable { 0o544 } else { 0o444 };
            let perms = std::fs::Permissions::from_mode(mode);

            match self.stream {
                Kind::Reader(mut inner) => {
                    let mut temp = tempfile::NamedTempFile::new()?;
                    crate::copy_wide(&mut inner, &mut temp)?;

                    temp.as_file_mut().set_permissions(perms)?;
                    filetime::set_file_mtime(temp.path(), FileTime::zero())?;

                    temp.persist(dest)?;
                }
                Kind::Paged(inner) => inner.persist(dest, perms)?,
                Kind::File(mut inner) => {
                    inner.as_file_mut().set_permissions(perms)?;
                    filetime::set_file_mtime(inner.path(), FileTime::zero())?;
                    inner.persist(dest)?;
                }
                Kind::Mmap(inner) => {
                    // Use buffered I/O here because mmap-ed files may be larger in size.
                    let mut temp = tempfile::NamedTempFile::new()?;
                    let mut writer = std::io::BufWriter::with_capacity(64 * 1024, &mut temp);
                    writer.write_all(inner.get_ref())?;
                    writer.flush()?;
                    drop(writer);

                    temp.as_file_mut().set_permissions(perms)?;
                    filetime::set_file_mtime(temp.path(), FileTime::zero())?;

                    temp.persist(dest)?;
                }
            }
        }

        Ok(())
    }
}

impl ContentAddressable for Blob {
    fn object_id(&self) -> ObjectId {
        self.object_id
    }
}

impl Debug for Blob {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct(stringify!(Blob))
            .field("is_executable", &self.is_executable)
            .field("object_id", &self.object_id)
            .finish()
    }
}

impl Read for Blob {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.stream {
            Kind::Reader(ref mut inner) => inner.read(buf),
            Kind::Paged(ref mut inner) => inner.read(buf),
            Kind::File(ref mut inner) => inner.read(buf),
            Kind::Mmap(ref mut inner) => inner.read(buf),
        }
    }
}

const fn blob_header(is_executable: bool) -> &'static [u8] {
    if is_executable {
        b"exec:"
    } else {
        b"blob:"
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
}

impl ContentAddressable for Tree {
    fn object_id(&self) -> ObjectId {
        let tree_hash = serde_json::to_vec(self).unwrap();
        let mut hasher = id::Hasher::new();
        hasher.update(b"tree:").update(&tree_hash[..]);
        hasher.finish()
    }
}

/// The installed name for a package, which is the human-readable name for the package concatenated
/// with its tree object ID and separated by a hyphen.
///
/// Executables and scripts inside a package directory may reference other installed packages on
/// the system by absolute path, e.g. `<store>/packages/<name>-<id>/foo/bar.sh`, or by relative
/// path, as in `../<name>-<id>/foo/bar.sh`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstallName {
    /// The human-readable name of the package.
    pub name: SmolStr,
    /// Unique hash of the `Package` object it derives from.
    pub id: ObjectId,
}

impl Display for InstallName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.name, self.id)
    }
}

/// Represents a package object.
///
/// Package objects have an output directory tree and may reference other packages at run-time or
/// at build-time.
///
/// TODO: Need to handle builders, build-time dependencies, and hash rewriting.
#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
pub struct Package {
    /// The human-readable name.
    pub name: SmolStr,
    /// The target platform spec it supports.
    pub system: String,
    /// Any other packages it references at run-time.
    pub references: BTreeSet<ObjectId>,
    /// Output directory tree to be installed.
    pub tree: ObjectId,
}

impl Package {
    /// Computes the directory name where the package should be installed.
    pub fn install_name(&self) -> InstallName {
        InstallName {
            name: self.name.clone(),
            id: self.object_id(),
        }
    }
}

impl ContentAddressable for Package {
    fn object_id(&self) -> ObjectId {
        let pkg_hash = serde_json::to_vec(self).unwrap();
        let mut hasher = id::Hasher::new();
        hasher.update(b"pkg:").update(&pkg_hash[..]);
        hasher.finish()
    }
}
