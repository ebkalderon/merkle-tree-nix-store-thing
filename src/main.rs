use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::hash::Hash;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context};
use filetime::FileTime;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};

// Utility

#[derive(Debug)]
enum Storage {
    Inline(Cursor<Box<[u8]>>, usize),
    File(tempfile::NamedTempFile),
}

#[derive(Debug)]
struct PagedBuffer {
    inner: Storage,
    threshold: usize,
}

impl PagedBuffer {
    pub fn with_threshold(t: usize) -> Self {
        let fixed_buf = Cursor::new(vec![0; t].into_boxed_slice());
        PagedBuffer {
            inner: Storage::Inline(fixed_buf, 0),
            threshold: t,
        }
    }
}

impl Read for PagedBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut b, _) => b.read(buf),
            Storage::File(ref mut b) => b.read(buf),
        }
    }
}

impl Seek for PagedBuffer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self.inner {
            Storage::Inline(ref mut b, _) => b.seek(pos),
            Storage::File(ref mut b) => b.seek(pos),
        }
    }
}

impl Write for PagedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut b, ref mut bytes_written) => {
                if *bytes_written + buf.len() > self.threshold {
                    // TODO: Should we create this in a directory like `<store>/tmp` for security?
                    let mut file = tempfile::NamedTempFile::new()?;
                    std::io::copy(b, &mut file)?;
                    file.as_file_mut().sync_all()?;

                    let len = file.write(buf)?;
                    self.inner = Storage::File(file);

                    Ok(len)
                } else {
                    let len = b.write(buf)?;
                    *bytes_written += len;
                    Ok(len)
                }
            }
            Storage::File(ref mut b) => b.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.inner {
            Storage::Inline(ref mut b, _) => b.flush(),
            Storage::File(ref mut b) => b.flush(),
        }
    }
}

// Filesystem objects

trait ContentAddressable {
    fn object_id(&self) -> ObjectId;
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ObjectId(blake3::Hash);

impl ObjectId {
    fn to_path_buf(&self) -> PathBuf {
        let text = self.0.to_hex();
        Path::new(&text[0..2]).join(&text[2..])
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", stringify!(ObjectId), self.0.to_hex())
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_hex())
    }
}

impl PartialOrd for ObjectId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.as_bytes().partial_cmp(&other.0.as_bytes())
    }
}

impl Ord for ObjectId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(&other.0.as_bytes())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: [u8; blake3::OUT_LEN] = hex::serde::deserialize(deserializer)?;
        Ok(ObjectId(bytes.into()))
    }
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.to_hex().serialize(serializer)
    }
}

#[derive(Debug)]
struct HashWriter<W> {
    inner: W,
    hasher: blake3::Hasher,
}

impl<W: Write> HashWriter<W> {
    fn with_header(header: &[u8], inner: W) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(header);
        HashWriter { inner, hasher }
    }

    fn object_id(&self) -> ObjectId {
        ObjectId(self.hasher.finalize())
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.hasher.update(&buf[0..len]);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
enum ObjectKind {
    Blob,
    Tree,
    Package,
}

impl ObjectKind {
    fn iter() -> impl Iterator<Item = Self> {
        use std::iter::once;
        once(ObjectKind::Blob)
            .chain(once(ObjectKind::Tree))
            .chain(once(ObjectKind::Package))
    }

    fn as_str(self) -> &'static str {
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
enum Object {
    Blob(Blob),
    Tree(Tree),
    Package(Package),
}

impl Object {
    fn kind(&self) -> ObjectKind {
        match *self {
            Object::Blob(_) => ObjectKind::Blob,
            Object::Tree(_) => ObjectKind::Tree,
            Object::Package(_) => ObjectKind::Package,
        }
    }

    fn into_blob(self) -> Result<Blob, Self> {
        match self {
            Object::Blob(b) => Ok(b),
            other => Err(other),
        }
    }

    fn into_tree(self) -> Result<Tree, Self> {
        match self {
            Object::Tree(t) => Ok(t),
            other => Err(other),
        }
    }

    fn into_package(self) -> Result<Package, Self> {
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

struct Blob {
    stream: Box<dyn Read>,
    is_executable: bool,
    object_id: ObjectId,
}

impl Blob {
    fn from_vec(bytes: Vec<u8>, is_executable: bool) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(if is_executable { b"exec:" } else { b"blob:" });
        hasher.update(&bytes);
        Blob {
            object_id: ObjectId(hasher.finalize()),
            stream: Box::new(std::io::Cursor::new(bytes)),
            is_executable,
        }
    }

    fn from_reader<R: Read>(mut reader: R, is_executable: bool) -> anyhow::Result<Self> {
        let header = if is_executable { b"exec:" } else { b"blob:" };
        let paged_writer = PagedBuffer::with_threshold(32 * 1024 * 1024);
        let mut writer = HashWriter::with_header(header, paged_writer);
        std::io::copy(&mut reader, &mut writer)?;
        Ok(Blob {
            object_id: writer.object_id(),
            stream: Box::new(writer.into_inner()),
            is_executable,
        })
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

#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
#[serde(tag = "type")]
enum Entry {
    Tree { id: ObjectId },
    Blob { id: ObjectId },
    Symlink { target: PathBuf },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Tree {
    entries: BTreeMap<String, Entry>,
}

impl Tree {
    fn references(&self) -> impl Iterator<Item = (ObjectId, ObjectKind)> + '_ {
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

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"tree:").update(&tree_hash[..]);
        ObjectId(hasher.finalize())
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct InstallName {
    name: String,
    id: ObjectId,
}

impl Display for InstallName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}-{}", self.name, self.id)
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
struct Package {
    name: String,
    system: String,
    references: BTreeSet<ObjectId>,
    tree: ObjectId,
}

impl Package {
    fn install_name(&self) -> InstallName {
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

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"pkg:").update(&pkg_hash[..]);
        ObjectId(hasher.finalize())
    }
}

// Store

type Objects<'a> = Box<dyn Iterator<Item = anyhow::Result<(ObjectId, ObjectKind)>> + 'a>;

trait Store {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId>;
    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object>;
    fn iter_objects(&self) -> anyhow::Result<Objects<'_>>;
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

    fn get_blob(&self, id: ObjectId) -> anyhow::Result<Blob> {
        self.get_object(id, Some(ObjectKind::Blob)).and_then(|o| {
            o.into_blob()
                .map_err(|_| anyhow!("{} is not a blob object", id))
        })
    }

    fn get_tree(&self, id: ObjectId) -> anyhow::Result<Tree> {
        self.get_object(id, Some(ObjectKind::Tree)).and_then(|o| {
            o.into_tree()
                .map_err(|_| anyhow!("{} is not a tree object", id))
        })
    }

    fn get_package(&self, id: ObjectId) -> anyhow::Result<Package> {
        self.get_object(id, Some(ObjectKind::Package))
            .and_then(|o| {
                o.into_package()
                    .map_err(|_| anyhow!("{} is not a package object", id))
            })
    }

    fn closure_for(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Vec<(ObjectId, ObjectKind)>> {
        #[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
        struct Ref(ObjectId, ObjectKind);

        impl Display for Ref {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                Display::fmt(&self.0, f)
            }
        }

        let refs = pkgs
            .into_iter()
            .map(|id| Ref(id, ObjectKind::Package))
            .collect();

        let closure = compute_closure(refs, |Ref(id, kind)| match kind {
            ObjectKind::Blob => Ok(BTreeSet::new()),
            ObjectKind::Tree => {
                let tree = self.get_tree(id)?;
                Ok(tree.references().map(|(id, kind)| Ref(id, kind)).collect())
            }
            ObjectKind::Package => {
                let p = self.get_package(id)?;
                let tree_ref = Ref(p.tree, ObjectKind::Tree);
                Ok(p.references
                    .into_iter()
                    .map(|id| Ref(id, ObjectKind::Package))
                    .chain(std::iter::once(tree_ref))
                    .collect())
            }
        })?;

        Ok(closure
            .into_iter()
            .map(|Ref(id, kind)| (id, kind))
            .collect())
    }
}

fn compute_closure<T, F>(items: BTreeSet<T>, get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
{
    struct ClosureBuilder<'a, T: Eq + Hash + Ord, F> {
        initial_items: &'a BTreeSet<T>,
        get_children: F,
        visited: HashSet<T>,
        parents: HashSet<T>,
        topo_sorted_items: Vec<T>,
    }

    impl<'a, T, F> ClosureBuilder<'a, T, F>
    where
        T: Copy + Display + Eq + Hash + Ord,
        F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
    {
        pub fn new(initial_items: &'a BTreeSet<T>, get_children: F) -> Self {
            ClosureBuilder {
                initial_items,
                get_children,
                visited: HashSet::new(),
                parents: HashSet::new(),
                topo_sorted_items: Vec::new(),
            }
        }

        pub fn compute(mut self) -> anyhow::Result<Vec<T>> {
            for item in self.initial_items {
                self.visit_dfs(*item, None)?;
            }

            self.topo_sorted_items.reverse();
            Ok(self.topo_sorted_items)
        }

        fn visit_dfs(&mut self, item: T, parent_item: Option<T>) -> anyhow::Result<()> {
            if self.parents.contains(&item) {
                return Err(anyhow!(
                    "detected cycle in closure reference graph: {} -> {}",
                    item,
                    parent_item.unwrap()
                ));
            }

            if !self.visited.insert(item) {
                return Ok(());
            }

            self.parents.insert(item);

            for child in (self.get_children)(item)? {
                if child != item {
                    self.visit_dfs(child, Some(item))?;
                }
            }

            self.topo_sorted_items.push(item);
            self.parents.remove(&item);

            Ok(())
        }
    }

    ClosureBuilder::new(&items, get_children).compute()
}

#[derive(Clone, Debug)]
enum InMemory {
    Blob {
        stream: Box<std::io::Cursor<Vec<u8>>>,
        is_executable: bool,
        object_id: ObjectId,
    },
    Tree(Tree),
    Package(Package),
}

impl InMemory {
    fn from_object(o: Object) -> anyhow::Result<Self> {
        match o {
            Object::Blob(mut b) => {
                let mut stream = Box::new(std::io::Cursor::new(Vec::new()));
                std::io::copy(&mut b.stream, &mut stream)?;
                Ok(InMemory::Blob {
                    stream,
                    is_executable: b.is_executable,
                    object_id: b.object_id(),
                })
            }
            Object::Tree(t) => Ok(InMemory::Tree(t)),
            Object::Package(p) => Ok(InMemory::Package(p)),
        }
    }

    fn kind(&self) -> ObjectKind {
        match *self {
            InMemory::Blob { .. } => ObjectKind::Blob,
            InMemory::Tree(_) => ObjectKind::Tree,
            InMemory::Package(_) => ObjectKind::Package,
        }
    }
}

impl From<InMemory> for Object {
    fn from(o: InMemory) -> Self {
        match o {
            InMemory::Blob {
                stream,
                is_executable,
                object_id,
            } => Object::Blob(Blob {
                stream,
                is_executable,
                object_id,
            }),
            InMemory::Tree(t) => Object::Tree(t),
            InMemory::Package(p) => Self::Package(p),
        }
    }
}

#[derive(Debug, Default)]
struct InMemoryStore {
    objects: BTreeMap<ObjectId, InMemory>,
}

impl Store for InMemoryStore {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        use std::collections::btree_map::Entry;
        let id = o.object_id();
        match self.objects.entry(id) {
            Entry::Occupied(_) => Ok(id),
            Entry::Vacant(e) => {
                e.insert(InMemory::from_object(o)?);
                Ok(id)
            }
        }
    }

    fn get_object(&self, id: ObjectId, _: Option<ObjectKind>) -> anyhow::Result<Object> {
        self.objects
            .get(&id)
            .cloned()
            .map(Object::from)
            .ok_or(anyhow!("object {} not found", id))
    }

    fn iter_objects(&self) -> anyhow::Result<Objects<'_>> {
        Ok(Box::new(
            self.objects.iter().map(|(&k, v)| (k, v.kind())).map(Ok),
        ))
    }

    fn contains_object(&self, id: &ObjectId, _: Option<ObjectKind>) -> anyhow::Result<bool> {
        Ok(self.objects.contains_key(id))
    }
}

#[derive(Debug)]
struct FsStore {
    objects_dir: PathBuf,
    packages_dir: PathBuf,
}

impl FsStore {
    pub fn open<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        let objects_dir = base.join("objects");
        let packages_dir = base.join("packages");

        if objects_dir.is_dir() && packages_dir.is_dir() {
            Ok(FsStore {
                objects_dir,
                packages_dir,
            })
        } else if base.exists() {
            Err(anyhow!("`{}` is not a store directory", base.display()))
        } else {
            Err(anyhow!("could not open, `{}` not found", base.display()))
        }
    }

    pub fn init<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        let objects_dir = base.join("objects");
        let packages_dir = base.join("packages");

        if !base.exists() {
            std::fs::create_dir(&base).context("could not create new store directory")?;
            std::fs::create_dir(&objects_dir).context("could not create `objects` dir")?;
            std::fs::create_dir(&packages_dir).context("could not create `packages` dir")?;
        }

        Self::open(base)
    }

    pub fn init_bare<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        let objects_dir = base.join("objects");
        let packages_dir = base.join("packages");

        let entries = std::fs::read_dir(&base).context("could not bare-init store directory")?;
        if entries.count() == 0 {
            std::fs::create_dir(&objects_dir).context("could not create `objects` dir")?;
            std::fs::create_dir(&packages_dir).context("could not create `packages` dir")?;
        } else if !objects_dir.exists() || !packages_dir.exists() {
            return Err(anyhow!("could not init store, expected empty directory"));
        }

        Ok(FsStore {
            objects_dir,
            packages_dir,
        })
    }

    fn checkout(&mut self, pkg: &Package) -> anyhow::Result<()> {
        let target_dir = self.packages_dir.join(pkg.install_name().to_string());

        if target_dir.exists() {
            Ok(())
        } else {
            let missing_refs: BTreeSet<_> = pkg
                .references
                .iter()
                .filter_map(|&id| self.get_package(id).ok())
                .map(|pkg| pkg.install_name())
                .filter(|n| !self.packages_dir.join(n.to_string()).exists())
                .collect();

            if missing_refs.is_empty() {
                let tree = self.get_tree(pkg.tree)?;
                let temp_dir = tempfile::tempdir()?;
                self.write_tree(temp_dir.path(), tree)?;

                let finished_dir = temp_dir.into_path();
                std::fs::rename(finished_dir, target_dir)?;

                Ok(())
            } else {
                Err(anyhow!(
                    "failed to checkout package, missing: {:?}",
                    missing_refs
                ))
            }
        }
    }

    fn write_tree(&mut self, tree_dir: &Path, tree: Tree) -> anyhow::Result<()> {
        if !tree_dir.exists() {
            std::fs::create_dir_all(&tree_dir)?;
            println!("=> created tree subdir: {}", tree_dir.display());
        }

        for (name, entry) in &tree.entries {
            let entry_path = tree_dir.join(name);
            match entry {
                Entry::Tree { id } => {
                    let subtree = self.get_tree(*id)?;
                    self.write_tree(&entry_path, subtree)?;
                }
                Entry::Blob { id } => {
                    let mut src = self.objects_dir.join(id.to_path_buf());
                    src.set_extension("blob");
                    std::fs::hard_link(&src, &entry_path).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::NotFound {
                            anyhow!("blob object {} not found", id)
                        } else {
                            e.into()
                        }
                    })?;
                    println!(
                        "=> hard-linked blob: {} -> {}",
                        src.display(),
                        entry_path.display()
                    );
                }
                Entry::Symlink { target } => {
                    std::os::unix::fs::symlink(&target, &entry_path)?;
                    let metadata = std::fs::symlink_metadata(&entry_path)?;
                    let atime = FileTime::from_last_access_time(&metadata);
                    filetime::set_symlink_file_times(&entry_path, atime, FileTime::zero())?;
                    println!(
                        "=> created symlink: {} -> {}",
                        entry_path.display(),
                        target.display()
                    );
                }
            }
        }

        filetime::set_file_mtime(&tree_dir, FileTime::zero())?;

        Ok(())
    }
}

impl Store for FsStore {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        fn write_object<F>(p: &Path, perms: u32, mut ser_fn: F) -> anyhow::Result<()>
        where
            F: FnMut(&std::fs::File) -> anyhow::Result<()>,
        {
            if !p.exists() {
                let mut file = tempfile::NamedTempFile::new()?;
                ser_fn(&file.as_file())?;
                file.flush()?;

                let perms = std::fs::Permissions::from_mode(perms);
                file.as_file_mut().set_permissions(perms)?;
                filetime::set_file_mtime(file.path(), FileTime::zero())?;

                file.as_file_mut().sync_all()?;
                file.persist(p)?;
            }

            Ok(())
        }

        let id = o.object_id();
        let mut path = self.objects_dir.join(id.to_path_buf());
        let parent_dir = path.parent().expect("path cannot be at filesystem root");

        if !parent_dir.exists() {
            std::fs::create_dir(parent_dir)?;
        }

        path.set_extension(o.kind().as_str());
        match o {
            Object::Blob(mut blob) => {
                let perms = if blob.is_executable { 0o544 } else { 0o444 };
                write_object(&path, perms, |mut file| {
                    std::io::copy(&mut blob, &mut file)?;
                    Ok(())
                })?;
            }
            Object::Tree(tree) => {
                write_object(&path, 0o444, |mut file| {
                    serde_json::to_writer(&mut file, &tree).map_err(From::from)
                })?;
            }
            Object::Package(pkg) => {
                self.checkout(&pkg)?;
                write_object(&path, 0o444, |mut file| {
                    serde_json::to_writer(&mut file, &pkg).map_err(From::from)
                })?;
            }
        }

        Ok(id)
    }

    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object> {
        let mut path = self.objects_dir.join(id.to_path_buf());

        let kind_exists = if kind.is_some() {
            kind.filter(|k| {
                path.set_extension(k.as_str());
                path.exists()
            })
        } else {
            ObjectKind::iter().find(|k| {
                path.set_extension(k.as_str());
                path.exists()
            })
        };

        match kind_exists {
            Some(ObjectKind::Blob) => {
                let reader = std::fs::File::open(path)?;
                let is_executable = reader.metadata()?.mode() & 0o100 != 0;
                let blob = Blob::from_reader(reader, is_executable)?;
                Ok(Object::Blob(blob))
            }
            Some(ObjectKind::Tree) => {
                let reader = std::fs::File::open(path)?;
                let tree = serde_json::from_reader(reader)?;
                Ok(Object::Tree(tree))
            }
            Some(ObjectKind::Package) => {
                let reader = std::fs::File::open(path)?;
                let package = serde_json::from_reader(reader)?;
                Ok(Object::Package(package))
            }
            None => Err(anyhow!("object {} not found", id)),
        }
    }

    fn iter_objects(&self) -> anyhow::Result<Objects<'_>> {
        let entries = std::fs::read_dir(&self.objects_dir)?
            .filter_map(|r| r.ok())
            .filter_map(|entry| entry.path().read_dir().ok())
            .flat_map(|iter| iter.filter_map(|r| r.ok()))
            .filter(|entry| entry.file_type().ok().filter(|ty| ty.is_file()).is_some());

        let objects = Box::new(entries.map(|entry| {
            let p = entry.path();
            let parts = p.parent().and_then(|s| s.file_stem()).zip(p.file_stem());
            let id_res = parts
                .map(|(prefix, rest)| prefix.to_str().into_iter().chain(rest.to_str()).collect())
                .ok_or(anyhow!("could not assemble object hash from path"))
                .and_then(|hash: String| {
                    let mut buf = [0u8; blake3::OUT_LEN];
                    hex::decode_to_slice(&hash, &mut buf).context("file path is not valid hash")?;
                    Ok(ObjectId(buf.into()))
                });

            let kind_res = p
                .extension()
                .and_then(|ext| ext.to_str())
                .ok_or(anyhow!("object file extension is not valid UTF-8"))
                .and_then(|ext| ext.parse());

            id_res.and_then(|id| kind_res.map(|kind| (id, kind)))
        }));

        Ok(objects)
    }

    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        let mut path = self.objects_dir.join(id.to_path_buf());

        if let Some(k) = kind {
            path.set_extension(k.as_str());
            Ok(path.exists())
        } else {
            for kind in ObjectKind::iter() {
                path.set_extension(kind.as_str());
                if path.exists() {
                    return Ok(true);
                }
            }

            Ok(false)
        }
    }
}

fn main() -> anyhow::Result<()> {
    // let mut store = InMemoryStore::default();
    let mut store = FsStore::init("./store")?;

    let txt_id = store.insert_object(Object::Blob(Blob::from_reader(
        std::io::Cursor::new(b"foobarbaz".to_vec()),
        false,
    )?))?;
    let rs_id = store.insert_object(Object::Blob(Blob::from_vec(
        b"fn main() {}".to_vec(),
        false,
    )))?;
    let sh_id = store.insert_object(Object::Blob(Blob::from_vec(b"echo \"hi\"".to_vec(), true)))?;

    let sub_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert("main.rs".into(), Entry::Blob { id: rs_id });
        Tree { entries }
    }))?;

    let main_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert("foo.txt".into(), Entry::Blob { id: txt_id });
        entries.insert("bar.sh".into(), Entry::Blob { id: sh_id });
        entries.insert(
            "baz.rs".into(),
            Entry::Symlink {
                target: "./src/main.rs".into(),
            },
        );
        entries.insert("src".into(), Entry::Tree { id: sub_tree_id });
        Tree { entries }
    }))?;

    let similar_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert("main.rs".into(), Entry::Blob { id: rs_id });
        Tree { entries }
    }))?;

    let pkg_id = store.insert_object(Object::Package(Package {
        name: "foo".into(),
        system: "x86_64-apple-darwin".into(),
        references: BTreeSet::new(),
        tree: main_tree_id,
    }))?;

    let pkg_id2 = store.insert_object(Object::Package({
        let mut references = BTreeSet::new();
        references.insert(pkg_id);
        Package {
            name: "bar".into(),
            system: "x86_64-apple-darwin".into(),
            references,
            tree: similar_tree_id,
        }
    }))?;

    println!("program 'foo': {:?}", store.get_package(pkg_id)?);
    println!("program 'bar': {:?}", store.get_package(pkg_id2)?);

    println!(
        "closure for 'foo' and 'bar': {:?}",
        store.closure_for({
            let mut pkgs = BTreeSet::new();
            pkgs.insert(pkg_id);
            pkgs.insert(pkg_id2);
            pkgs
        })?
    );

    Ok(())
}
