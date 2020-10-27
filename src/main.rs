use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{anyhow, Context};
use blake3::Hash;
use filetime::FileTime;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};

// Filesystem objects

trait ContentAddressable {
    fn object_id(&self) -> ObjectId;
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
struct ObjectId(Hash);

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

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
struct Blob {
    bytes: Vec<u8>,
    is_executable: bool,
}

impl ContentAddressable for Blob {
    fn object_id(&self) -> ObjectId {
        let mut hasher = blake3::Hasher::new();
        if self.is_executable {
            hasher.update(b"exec:");
        } else {
            hasher.update(b"blob:");
        }
        hasher.update(&self.bytes);
        ObjectId(hasher.finalize())
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

impl ContentAddressable for Tree {
    fn object_id(&self) -> ObjectId {
        use std::hash::{Hash, Hasher};

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
struct PackageId {
    name: String,
    id: ObjectId,
}

impl Display for PackageId {
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
    fn id(&self) -> PackageId {
        PackageId {
            name: self.name.clone(),
            id: self.object_id(),
        }
    }
}

impl ContentAddressable for Package {
    fn object_id(&self) -> ObjectId {
        use std::hash::{Hash, Hasher};

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
    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Option<Object>>;
    fn iter_objects(&self) -> anyhow::Result<Objects<'_>>;
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

    fn get_blob(&self, id: ObjectId) -> anyhow::Result<Option<Blob>> {
        self.get_object(id, Some(ObjectKind::Blob))
            .map(|opt| opt.and_then(|o| o.into_blob().ok()))
    }

    fn get_tree(&self, id: ObjectId) -> anyhow::Result<Option<Tree>> {
        self.get_object(id, Some(ObjectKind::Tree))
            .map(|opt| opt.and_then(|o| o.into_tree().ok()))
    }

    fn get_package(&self, id: ObjectId) -> anyhow::Result<Option<Package>> {
        self.get_object(id, Some(ObjectKind::Package))
            .map(|opt| opt.and_then(|o| o.into_package().ok()))
    }
}

#[derive(Debug, Default)]
struct InMemoryStore {
    objects: BTreeMap<ObjectId, Object>,
}

impl Store for InMemoryStore {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        let id = o.object_id();
        self.objects.entry(id).or_insert(o);
        Ok(id)
    }

    fn get_object(&self, id: ObjectId, _: Option<ObjectKind>) -> anyhow::Result<Option<Object>> {
        Ok(self.objects.get(&id).cloned())
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
    base: PathBuf,
}

impl FsStore {
    fn open<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        if base.is_dir() {
            Ok(FsStore { base })
        } else if base.exists() {
            Err(anyhow!("{} is not a directory", base.display()))
        } else {
            Err(anyhow!("path {} does not exist", base.display()))
        }
    }

    fn checkout(&mut self, pkg: &Package) -> anyhow::Result<()> {
        let packages_dir = self.base.join("packages");
        let target_dir = packages_dir.join(pkg.id().to_string());

        if target_dir.exists() {
            Ok(())
        } else {
            let missing_refs: BTreeSet<_> = pkg
                .references
                .iter()
                .filter_map(|&id| self.get_package(id).ok().flatten())
                .map(|pkg| pkg.id())
                .filter(|id| !packages_dir.join(id.to_string()).exists())
                .collect();

            if missing_refs.is_empty() {
                std::fs::create_dir_all(&packages_dir)?;

                let tree = self
                    .get_tree(pkg.tree)?
                    .ok_or(anyhow!("root tree object {} not found", pkg.tree))?;

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
                    let subtree = self
                        .get_tree(*id)?
                        .ok_or(anyhow!("tree object {} not found", id))?;

                    self.write_tree(&entry_path, subtree)?;
                }
                Entry::Blob { id } => {
                    let text = id.0.to_hex();
                    let mut src = self.base.join("objects").join(&text[0..2]).join(&text[2..]);
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
        fn write_object<F>(p: &Path, perms: u32, ser_fn: F) -> anyhow::Result<()>
        where
            F: Fn(&std::fs::File) -> anyhow::Result<()>,
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
        let text = id.0.to_hex();
        let mut path = self.base.join("objects").join(&text[0..2]);

        if !path.exists() {
            std::fs::create_dir_all(&path)?;
        }

        path.push(&text[2..]);
        path.set_extension(o.kind().as_str());

        match o {
            Object::Blob(blob) => {
                let perms = if blob.is_executable { 0o544 } else { 0o444 };
                write_object(&path, perms, |mut file| {
                    file.write_all(&blob.bytes[..]).map_err(From::from)
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

    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Option<Object>> {
        let text = id.0.to_hex();
        let mut path = self.base.join("objects").join(&text[0..2]).join(&text[2..]);

        let exists = if kind.is_some() {
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

        match exists {
            Some(ObjectKind::Blob) => {
                let bytes = std::fs::read(&path)?;
                let is_executable = std::fs::metadata(path)?.mode() & 0o100 != 0;
                Ok(Some(Object::Blob(Blob {
                    bytes,
                    is_executable,
                })))
            }
            Some(ObjectKind::Tree) => {
                let reader = std::fs::File::open(path)?;
                let tree = serde_json::from_reader(reader)?;
                Ok(Some(Object::Tree(tree)))
            }
            Some(ObjectKind::Package) => {
                let reader = std::fs::File::open(path)?;
                let package = serde_json::from_reader(reader)?;
                Ok(Some(Object::Package(package)))
            }
            None => Ok(None),
        }
    }

    fn iter_objects(&self) -> anyhow::Result<Objects<'_>> {
        let entries = std::fs::read_dir(self.base.join("objects"))?
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
        let text = id.0.to_hex();
        let mut path = self.base.join("objects").join(&text[0..2]).join(&text[2..]);

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
    let mut store = FsStore::open("store")?;

    let txt_id = store.insert_object(Object::Blob(Blob {
        bytes: b"foobarbaz".to_vec(),
        is_executable: false,
    }))?;
    let rs_id = store.insert_object(Object::Blob(Blob {
        bytes: b"fn main() {}".to_vec(),
        is_executable: false,
    }))?;
    let sh_id = store.insert_object(Object::Blob(Blob {
        bytes: b"echo \"hi\"".to_vec(),
        is_executable: true,
    }))?;

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

    println!("program 'foo': {:?}", store.get_package(pkg_id)?.unwrap());
    println!("program 'bar': {:?}", store.get_package(pkg_id2)?.unwrap());

    Ok(())
}
