pub use self::id::{HashWriter, Hasher, ObjectId};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::anyhow;
use serde::{Deserialize, Serialize};

use crate::util::{self, PagedBuffer};

mod id;

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

    pub fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Blob => "blob",
            ObjectKind::Tree => "tree",
            ObjectKind::Package => "pkg",
        }
    }
}

impl FromStr for ObjectKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "blob" => Ok(ObjectKind::Blob),
            "tree" => Ok(ObjectKind::Tree),
            "pkg" => Ok(ObjectKind::Package),
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

pub struct Blob {
    pub(crate) stream: Box<dyn Read>,
    pub(crate) is_executable: bool,
    pub(crate) object_id: ObjectId,
}

impl Blob {
    pub fn from_vec(bytes: Vec<u8>, is_executable: bool) -> Self {
        let mut hasher = id::Hasher::new();
        hasher.update(blob_header(is_executable)).update(&bytes);
        Blob {
            object_id: hasher.finish(),
            stream: Box::new(std::io::Cursor::new(bytes)),
            is_executable,
        }
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let mut file = std::fs::File::open(path)?;
        let is_executable = file.metadata()?.mode() & 0o100 != 0;

        let header = blob_header(is_executable);
        let mut hasher = HashWriter::with_header(header, std::io::sink());

        std::io::copy(&mut file, &mut hasher)?;
        file.seek(SeekFrom::Start(0))?;

        Ok(Blob {
            object_id: hasher.object_id(),
            stream: Box::new(file),
            is_executable,
        })
    }

    pub fn from_reader<R: Read>(mut reader: R, is_executable: bool) -> anyhow::Result<Self> {
        let header = blob_header(is_executable);
        let paged_writer = PagedBuffer::with_threshold(32 * 1024 * 1024);
        let mut writer = HashWriter::with_header(header, paged_writer);
        std::io::copy(&mut reader, &mut writer)?;
        Ok(Blob {
            object_id: writer.object_id(),
            stream: Box::new(writer.into_inner()),
            is_executable,
        })
    }

    pub fn is_executable(&self) -> bool {
        self.is_executable
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
        self.stream.read(buf)
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
        use std::hash::Hasher;

        let tree_hash = {
            let mut hasher = fnv::FnvHasher::default();
            self.entries.hash(&mut hasher);
            hasher.finish().to_be_bytes()
        };

        let mut hasher = id::Hasher::new();
        hasher.update(b"tree:").update(&tree_hash[..]);
        hasher.finish()
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InstallName {
    pub name: String,
    pub id: ObjectId,
}

impl Display for InstallName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.name, self.id)
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
pub struct Package {
    pub name: String,
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
        use std::hash::Hasher;

        let pkg_hash = {
            let mut hasher = fnv::FnvHasher::default();
            self.hash(&mut hasher);
            hasher.finish().to_be_bytes()
        };

        let mut hasher = id::Hasher::new();
        hasher.update(b"pkg:").update(&pkg_hash[..]);
        hasher.finish()
    }
}
