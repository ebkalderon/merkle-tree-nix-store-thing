//! Types of Merkle tree objects.

pub use self::id::{HashWriter, Hasher, ObjectId};
pub use self::platform::{Arch, Env, Os, Platform};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Formatter};
use std::io::{self, Cursor, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::anyhow;
use memmap::{Mmap, MmapOptions};
use semver::Version;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use self::spooled::SpooledTempFile;

pub mod pack;

mod id;
mod platform;
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
}

/// Underlying I/O streams that can back a blob object.
#[derive(Debug)]
enum Kind {
    Inline(Cursor<Vec<u8>>),
    Mmap(Cursor<Mmap>),
    Spooled(SpooledTempFile),
    File(tempfile::NamedTempFile),
    StoreFile(std::fs::File, PathBuf),
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
    pub fn from_bytes(input: Vec<u8>, is_executable: bool) -> Self {
        let mut hasher = id::Hasher::new_blob(is_executable);
        hasher.update(&input);
        Blob {
            length: input.len() as u64,
            stream: Kind::Inline(Cursor::new(input)),
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
        match open_for_large_read(path.as_ref())? {
            Strategy::Inline(buffer, is_executable, length) => {
                let mut hasher = id::Hasher::new_blob(is_executable);
                hasher.update(buffer.get_ref());
                Ok(Blob {
                    stream: Kind::Inline(buffer),
                    is_executable,
                    length,
                    object_id: hasher.finish(),
                })
            }
            Strategy::Mmap(mmap, is_executable, length) => {
                let mut hasher = id::Hasher::new_blob(is_executable);
                hasher.par_update(mmap.get_ref());
                Ok(Blob {
                    stream: Kind::Mmap(mmap),
                    is_executable,
                    length,
                    object_id: hasher.finish(),
                })
            }
            Strategy::Io(mut file, is_executable, length) => {
                let hasher = id::Hasher::new_blob(is_executable);
                let temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
                let mut writer = HashWriter::with_hasher(hasher, temp);
                crate::copy_wide(&mut file, &mut writer)?;
                Ok(Blob {
                    object_id: writer.object_id(),
                    stream: Kind::File(writer.into_inner()),
                    is_executable,
                    length,
                })
            }
        }
    }

    /// Opens a `Blob` from a file path without hashing it, trusting the `object_id` to be correct.
    pub(crate) fn from_path_unchecked(path: &Path, object_id: ObjectId) -> anyhow::Result<Self> {
        match open_for_large_read(path)? {
            Strategy::Inline(buffer, is_executable, length) => Ok(Blob {
                stream: Kind::Inline(buffer),
                is_executable,
                length,
                object_id,
            }),
            Strategy::Mmap(mmap, is_executable, length) => Ok(Blob {
                stream: Kind::Mmap(mmap),
                is_executable,
                length,
                object_id,
            }),
            Strategy::Io(file, is_executable, length) => Ok(Blob {
                stream: Kind::StoreFile(file, path.to_owned()),
                is_executable,
                length,
                object_id,
            }),
        }
    }

    /// Hashes and returns a new `Blob` object from a reader.
    ///
    /// This will attempt to buffer the I/O stream into memory, spilling over into a temporary file
    /// on disk if the internal buffer grows beyond a 32 MB threshold.
    ///
    /// Returns `Err` if an I/O error occurred.
    pub fn from_reader<R: Read>(mut reader: R, is_executable: bool) -> anyhow::Result<Self> {
        let hasher = Hasher::new_blob(is_executable);
        let spooled_writer = SpooledTempFile::new(32 * 1024 * 1024);
        let mut writer = HashWriter::with_hasher(hasher, spooled_writer);
        let length = crate::copy_wide(&mut reader, &mut writer)?;

        Ok(Blob {
            object_id: writer.object_id(),
            stream: Kind::Spooled(writer.into_inner()),
            is_executable,
            length,
        })
    }

    /// Returns `true` if this blob has its executable bit set.
    #[inline]
    pub fn is_executable(&self) -> bool {
        self.is_executable
    }

    /// Returns the size of the blob, in bytes.
    #[inline]
    pub fn len(&self) -> u64 {
        self.length
    }

    /// Persists the blob to disk with as little redundant copying as possible.
    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        let mode = if self.is_executable { 0o544 } else { 0o444 };

        let result = match self.stream {
            Kind::Inline(inner) => {
                let mut temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
                temp.write_all(inner.get_ref())?;
                temp.flush()?;
                normalize_perms(temp.path(), mode)?;
                temp.persist(dest).map(|_| {}).map_err(|e| e.error)
            }
            Kind::Mmap(mut inner) => {
                let mut temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
                crate::copy_wide(&mut inner, &mut temp)?;
                normalize_perms(temp.path(), mode)?;
                temp.persist(dest).map(|_| {}).map_err(|e| e.error)
            }
            Kind::Spooled(inner) => inner.persist(dest, mode),
            Kind::File(inner) => {
                normalize_perms(inner.path(), mode)?;
                inner.persist(dest).map(|_| {}).map_err(|e| e.error)
            }
            Kind::StoreFile(_, src) if src == dest => panic!("cannot persist file to itself"),
            Kind::StoreFile(_, src) => {
                let file_name = src.file_name().unwrap();
                let temp_path = PathBuf::from("/var/tmp").join(file_name);
                std::fs::copy(src, &temp_path)?;
                normalize_perms(&temp_path, mode)?;
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
}

impl Read for Blob {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.stream {
            Kind::Inline(ref mut inner) => inner.read(buf),
            Kind::Mmap(ref mut inner) => inner.read(buf),
            Kind::Spooled(ref mut inner) => inner.read(buf),
            Kind::File(ref mut inner) => inner.read(buf),
            Kind::StoreFile(ref mut inner, _) => inner.read(buf),
        }
    }
}

/// A list of possible file I/O strategies with the data contents, executable bit, and length.
enum Strategy {
    Inline(Cursor<Vec<u8>>, bool, u64),
    Mmap(Cursor<Mmap>, bool, u64),
    Io(std::fs::File, bool, u64),
}

/// Selects the most efficient strategy to open a file, optimized for massive sequential reads.
fn open_for_large_read(file_path: &Path) -> anyhow::Result<Strategy> {
    let mut file = std::fs::File::open(file_path)?;
    let metadata = file.metadata()?;
    let is_executable = metadata.mode() & 0o100 != 0;

    if metadata.len() < 16 * 1024 {
        // Not worth it to mmap(2) small files. Load into memory instead.
        let mut buf = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut buf)?;
        Ok(Strategy::Inline(
            Cursor::new(buf),
            is_executable,
            metadata.len(),
        ))
    } else if metadata.len() <= isize::max_value() as u64 {
        // Prefer memory-mapping files wherever possible for performance.
        let mmap = unsafe { MmapOptions::new().len(metadata.len() as usize).map(&file)? };
        Ok(Strategy::Mmap(
            Cursor::new(mmap),
            is_executable,
            metadata.len(),
        ))
    } else {
        // Only fall back to regular file I/O if file is too large to mmap(2).
        Ok(Strategy::Io(file, is_executable, metadata.len()))
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
}

/// Directory name of an installed package.
///
/// This is the human-readable name of the package concatenated with its object ID, separated
/// by a hyphen. Installed packages are located in the `packages` directory, and their file
/// contents may reference paths in other packages' directories via absolute paths.
///
/// `InstallName` implements `AsRef<Path>` so it can be treated identically to `std::path::Path`.
///
/// # Example
///
/// Given an example package named `hello-1.0.0`, its install name string could be:
///
/// ```text
/// hello-1.0.0-fd53fe2392dc260e9cf414a39aeb43641c10ab48a726c58e76d06a7fe443d660
/// ```
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct InstallName(String);

impl InstallName {
    /// Returns the human-readable name component of the `InstallName`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use foo::{Arch, Env, Os, Package, Platform};
    /// #
    /// # let pkg = Package {
    /// #     name: "hello-1.0.0".into(),
    /// #     system: Platform { arch: Arch::X86_64, os: Os::Linux(Env::Gnu) },
    /// #     references: Default::default(),
    /// #     tree: "0000000000000000000000000000000000000000000000000000000000000000".parse().unwrap(),
    /// # };
    /// #
    /// let install_name = pkg.install_name();
    /// assert_eq!(install_name.name(), "hello-1.0.0");
    /// ```
    pub fn name(&self) -> &str {
        self.0.rsplitn(2, '-').nth(1).unwrap()
    }

    /// Returns the package ID component of the `InstallName`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use foo::{Arch, Env, ObjectId, Os, Package, Platform};
    /// #
    /// # let pkg = Package {
    /// #     name: "hello-1.0.0".into(),
    /// #     system: Platform { arch: Arch::X86_64, os: Os::Linux(Env::Gnu) },
    /// #     references: Default::default(),
    /// #     tree: "0000000000000000000000000000000000000000000000000000000000000000".parse().unwrap(),
    /// # };
    /// #
    /// let install_name = pkg.install_name();
    /// let id: ObjectId = "fd53fe2392dc260e9cf414a39aeb43641c10ab48a726c58e76d06a7fe443d660".parse().unwrap();
    /// assert_eq!(install_name.id(), id);
    /// ```
    pub fn id(&self) -> ObjectId {
        self.0.rsplitn(2, '-').nth(0).unwrap().parse().unwrap()
    }
}

impl AsRef<Path> for InstallName {
    fn as_ref(&self) -> &Path {
        self.0.as_ref()
    }
}

impl Display for InstallName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<InstallName> for String {
    fn from(name: InstallName) -> Self {
        name.0
    }
}

/// Represents a package object.
///
/// Package objects have an output directory tree and may reference other packages at run-time or
/// at build-time.
#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
pub struct Package {
    /// The human-readable name.
    pub name: SmolStr,
    /// The target platform it supports.
    pub system: Platform,
    /// Any other packages it references at run-time.
    pub references: BTreeSet<ObjectId>,
    /// Output directory tree to be installed.
    pub tree: ObjectId,
}

impl Package {
    /// Computes the directory name where the package should be installed.
    pub fn install_name(&self) -> InstallName {
        InstallName(format!("{}-{}", self.name, self.object_id()))
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
}

/// Represents a package specification object.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Spec {
    /// The human-readable name.
    pub name: SmolStr,
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
}

/// Persists `val` to disk as a read-only JSON file located at `dest`.
fn persist_json<T: Serialize>(val: &T, dest: &Path) -> anyhow::Result<()> {
    let mut temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
    serde_json::to_writer(&mut temp, val)?;
    temp.flush()?;
    normalize_perms(temp.path(), 0o444)?;

    match temp.persist(dest) {
        Ok(_) => Ok(()),
        Err(_) if dest.is_file() => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// Normalizes file permissons for `p` and sets all timestamps to January 1st, 1970.
fn normalize_perms(p: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(p, perms)?;
    filetime::set_file_atime(p, filetime::FileTime::zero())?;
    filetime::set_file_mtime(p, filetime::FileTime::zero())?;
    Ok(())
}
