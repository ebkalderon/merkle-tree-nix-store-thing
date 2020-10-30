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

use crate::util::{self, PagedBuffer};

mod id;

const BLOB_FILE_EXT: &str = "blob";
const TREE_FILE_EXT: &str = "tree";
const PACKAGE_FILE_EXT: &str = "pkg";

pub trait ContentAddressable {
    fn object_id(&self) -> ObjectId;
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum ObjectKind {
    Blob,
    Tree,
    Package,
}

impl ObjectKind {
    pub fn iter() -> impl Iterator<Item = Self> {
        use std::iter::once;
        once(ObjectKind::Blob)
            .chain(once(ObjectKind::Tree))
            .chain(once(ObjectKind::Package))
    }

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

#[derive(Debug)]
pub enum Object {
    Blob(Blob),
    Tree(Tree),
    Package(Package),
}

impl Object {
    pub fn kind(&self) -> ObjectKind {
        match *self {
            Object::Blob(_) => ObjectKind::Blob,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Package(_) => ObjectKind::Package,
        }
    }

    pub fn into_blob(self) -> Result<Blob, Self> {
        match self {
            Object::Blob(b) => Ok(b),
            other => Err(other),
        }
    }

    pub fn into_tree(self) -> Result<Tree, Self> {
        match self {
            Object::Tree(t) => Ok(t),
            other => Err(other),
        }
    }

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

enum Kind {
    Reader(Box<dyn Read>),
    Paged(PagedBuffer),
    File(tempfile::NamedTempFile),
    Mmap(Cursor<Mmap>),
}

pub struct Blob {
    stream: Kind,
    is_executable: bool,
    object_id: ObjectId,
}

impl Blob {
    pub fn from_vec(bytes: Vec<u8>, is_executable: bool) -> Self {
        let mut hasher = id::Hasher::new();
        hasher.update(blob_header(is_executable)).update(&bytes);
        Blob {
            object_id: hasher.finish(),
            stream: Kind::Reader(Box::new(Cursor::new(bytes))),
            is_executable,
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let mut file = std::fs::File::open(&path)?;
        let metadata = file.metadata()?;
        let is_executable = metadata.mode() & 0o100 != 0;
        let header = blob_header(is_executable);

        if metadata.len() < 16 * 1024 {
            let buffer = Cursor::new(Vec::with_capacity(metadata.len() as usize));
            let mut writer = HashWriter::with_header(header, buffer);
            util::copy_wide(&mut file, &mut writer)?;
            Ok(Blob {
                object_id: writer.object_id(),
                stream: Kind::Reader(Box::new(writer.into_inner())),
                is_executable,
            })
        } else if metadata.len() <= isize::max_value() as u64 {
            let mmap = unsafe { MmapOptions::new().len(metadata.len() as usize).map(&file)? };
            let mut hasher = id::Hasher::new();
            hasher.update(header).par_update(&mmap);
            Ok(Blob {
                object_id: hasher.finish(),
                stream: Kind::Mmap(Cursor::new(mmap)),
                is_executable,
            })
        } else {
            let temp = tempfile::NamedTempFile::new()?;
            let mut writer = HashWriter::with_header(header, temp);
            util::copy_wide(&mut file, &mut writer)?;
            Ok(Blob {
                object_id: writer.object_id(),
                stream: Kind::File(writer.into_inner()),
                is_executable,
            })
        }
    }

    pub fn from_reader<R: Read>(mut reader: R, is_executable: bool) -> anyhow::Result<Self> {
        let header = blob_header(is_executable);
        let paged_writer = PagedBuffer::with_threshold(32 * 1024 * 1024);
        let mut writer = HashWriter::with_header(header, paged_writer);
        util::copy_wide(&mut reader, &mut writer)?;

        Ok(Blob {
            object_id: writer.object_id(),
            stream: Kind::Paged(writer.into_inner()),
            is_executable,
        })
    }

    pub(crate) fn from_reader_raw(
        reader: Box<dyn Read>,
        is_executable: bool,
        object_id: ObjectId,
    ) -> Self {
        Blob {
            object_id,
            stream: Kind::Reader(reader),
            is_executable,
        }
    }

    pub fn is_executable(&self) -> bool {
        self.is_executable
    }

    pub(crate) fn persist(self, dest: &Path) -> anyhow::Result<()> {
        if !dest.exists() {
            let mode = if self.is_executable { 0o544 } else { 0o444 };
            let perms = std::fs::Permissions::from_mode(mode);

            match self.stream {
                Kind::Reader(mut inner) => {
                    let mut temp = tempfile::NamedTempFile::new()?;
                    util::copy_wide(&mut inner, &mut temp)?;

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

#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Entry {
    Tree { id: ObjectId },
    Blob { id: ObjectId },
    Symlink { target: PathBuf },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Tree {
    pub entries: BTreeMap<String, Entry>,
}

impl Tree {
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

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstallName {
    pub name: SmolStr,
    pub id: ObjectId,
}

impl Display for InstallName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.name, self.id)
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
pub struct Package {
    pub name: SmolStr,
    pub system: String,
    pub references: BTreeSet<ObjectId>,
    pub tree: ObjectId,
}

impl Package {
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
