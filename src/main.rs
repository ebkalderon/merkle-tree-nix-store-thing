use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

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

#[derive(Clone, Debug)]
enum Object {
    Blob(Blob),
    Tree(Tree),
    Package(Package),
}

impl Object {
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
struct Blob(Vec<u8>);

impl ContentAddressable for Blob {
    fn object_id(&self) -> ObjectId {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"blob:").update(&self.0);
        ObjectId(hasher.finalize())
    }
}

#[derive(Clone, Debug, Hash, Deserialize, Serialize)]
enum Entry {
    Tree { id: ObjectId },
    Blob { id: ObjectId, is_executable: bool },
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

type Objects<'a> = Box<dyn Iterator<Item = anyhow::Result<(ObjectId, Object)>> + 'a>;

trait Store {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId>;
    fn get_object(&self, id: &ObjectId) -> anyhow::Result<Option<Object>>;
    fn iter_objects(&self) -> anyhow::Result<Objects<'_>>;
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

    fn get_object(&self, id: &ObjectId) -> anyhow::Result<Option<Object>> {
        Ok(self.objects.get(id).cloned())
    }

    fn iter_objects(&self) -> anyhow::Result<Objects<'_>> {
        Ok(Box::new(self.objects.clone().into_iter().map(Ok)))
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
                .filter_map(|id| self.get_object(&id).ok().flatten())
                .filter_map(|obj| obj.into_package().ok().map(|pkg| pkg.id()))
                .filter(|pkg_id| !packages_dir.join(pkg_id.to_string()).exists())
                .collect();

            if missing_refs.is_empty() {
                std::fs::create_dir_all(&packages_dir)?;

                let tree = self
                    .get_object(&pkg.tree)?
                    .and_then(|o| o.into_tree().ok())
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
                        .get_object(&id)?
                        .and_then(|o| o.into_tree().ok())
                        .ok_or(anyhow!("tree object {} not found", id))?;

                    self.write_tree(&entry_path, subtree)?;
                }
                Entry::Blob { id, is_executable } => {
                    let blob = self
                        .get_object(&id)?
                        .and_then(|o| o.into_blob().ok())
                        .ok_or(anyhow!("blob object {} not found", id))?;

                    if *is_executable {
                        let mut file = std::fs::File::create(&entry_path)?;
                        file.write_all(&blob.0)?;
                        file.flush()?;
                        let perms = std::fs::Permissions::from_mode(0o544);
                        file.set_permissions(perms)?;
                        filetime::set_file_mtime(&entry_path, FileTime::zero())?;
                        file.sync_all()?;
                        println!("=> copied blob: {}", entry_path.display());
                    } else {
                        let text = id.0.to_hex();
                        let mut src = self.base.join("objects").join(&text[0..2]).join(&text[2..]);
                        src.set_extension("blob");
                        std::fs::hard_link(&src, &entry_path)?;
                        println!(
                            "=> hard-linked blob: {} -> {}",
                            src.display(),
                            entry_path.display()
                        );
                    }
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
        let id = o.object_id();
        let text = id.0.to_hex();

        let base_path = self.base.join("objects").join(&text[0..2]);
        if !base_path.exists() {
            std::fs::create_dir_all(&base_path)?;
        }

        let mut path = base_path.join(&text[2..]);
        match o {
            Object::Blob(blob) => {
                path.set_extension("blob");
                write_object(&path, |mut file| {
                    file.write_all(&blob.0[..]).map_err(From::from)
                })?;
            }
            Object::Tree(tree) => {
                path.set_extension("tree");
                write_object(&path, |mut file| {
                    serde_json::to_writer(&mut file, &tree).map_err(From::from)
                })?;
            }
            Object::Package(pkg) => {
                self.checkout(&pkg)?;
                path.set_extension("pkg");
                write_object(&path, |mut file| {
                    serde_json::to_writer(&mut file, &pkg).map_err(From::from)
                })?;
            }
        }

        Ok(id)
    }

    fn get_object(&self, id: &ObjectId) -> anyhow::Result<Option<Object>> {
        let text = id.0.to_hex();
        let path = std::fs::read_dir(self.base.join("objects").join(&text[0..2]))?
            .filter_map(|r| r.ok().map(|entry| entry.path()))
            .find(|entry| entry.file_stem().filter(|&p| p == &text[2..]).is_some());

        path.map(|p| open_object(&p)).transpose()
    }

    fn iter_objects(&self) -> anyhow::Result<Objects<'_>> {
        let entries = std::fs::read_dir(self.base.join("objects"))?
            .filter_map(|r| r.ok())
            .filter(|entry| entry.file_type().ok().filter(|ty| ty.is_dir()).is_some())
            .filter_map(|entry| entry.path().read_dir().ok())
            .flat_map(|iter| iter.filter_map(|r| r.ok()))
            .filter(|entry| entry.file_type().ok().filter(|ty| ty.is_file()).is_some());

        let objects = Box::new(entries.map(|entry| {
            let p = entry.path();
            let parts = p.parent().and_then(|s| s.file_stem()).zip(p.file_stem());
            let id = parts
                .map(|(x, y)| x.to_str().into_iter().chain(y.to_str()).collect())
                .ok_or(anyhow!("could not assemble object hash from path"))
                .and_then(|hash: String| {
                    let mut buf = [0u8; blake3::OUT_LEN];
                    hex::decode_to_slice(&hash, &mut buf).context("file path is not valid hash")?;
                    Ok(ObjectId(buf.into()))
                });

            id.and_then(|id| open_object(&entry.path()).map(|obj| (id, obj)))
        }));

        Ok(objects)
    }
}

fn open_object(path: &Path) -> anyhow::Result<Object> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some("blob") => {
            let bytes = std::fs::read(path)?;
            Ok(Object::Blob(Blob(bytes)))
        }
        Some("tree") => {
            let reader = std::fs::File::open(path)?;
            let tree = serde_json::from_reader(reader)?;
            Ok(Object::Tree(tree))
        }
        Some("pkg") => {
            let reader = std::fs::File::open(path)?;
            let package = serde_json::from_reader(reader)?;
            Ok(Object::Package(package))
        }
        Some(ext) => Err(anyhow!("unrecognized extension: {}", ext)),
        None => Err(anyhow!("object file is missing extension")),
    }
}

fn write_object<F>(obj_path: &Path, ser_fn: F) -> anyhow::Result<()>
where
    F: Fn(&std::fs::File) -> anyhow::Result<()>,
{
    if !obj_path.exists() {
        let mut file = tempfile::NamedTempFile::new()?;
        ser_fn(&file.as_file())?;
        file.flush()?;

        let perms = std::fs::Permissions::from_mode(0o444);
        file.as_file_mut().set_permissions(perms)?;
        filetime::set_file_mtime(file.path(), FileTime::zero())?;

        file.as_file_mut().sync_all()?;
        file.persist(obj_path)?;
    }

    Ok(())
}

fn main() -> anyhow::Result<()> {
    // let mut store = InMemoryStore::default();
    let mut store = FsStore::open("store")?;

    let txt_id = store.insert_object(Object::Blob(Blob(b"foobarbaz".to_vec())))?;
    let rs_id = store.insert_object(Object::Blob(Blob(b"fn main() {}".to_vec())))?;
    let sh_id = store.insert_object(Object::Blob(Blob(b"echo \"hi\"".to_vec())))?;

    let sub_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert(
            "main.rs".into(),
            Entry::Blob {
                id: rs_id,
                is_executable: false,
            },
        );
        Tree { entries }
    }))?;

    let main_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert(
            "foo.txt".into(),
            Entry::Blob {
                id: txt_id,
                is_executable: false,
            },
        );
        entries.insert(
            "bar.sh".into(),
            Entry::Blob {
                id: sh_id,
                is_executable: true,
            },
        );
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
        entries.insert(
            "main.rs".into(),
            Entry::Blob {
                id: rs_id,
                is_executable: false,
            },
        );
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

    println!(
        "program 'foo': {:?}",
        store
            .get_object(&pkg_id)?
            .and_then(|o| o.into_package().ok())
            .unwrap()
    );
    println!(
        "program 'bar': {:?}",
        store
            .get_object(&pkg_id2)?
            .and_then(|o| o.into_package().ok())
            .unwrap()
    );

    Ok(())
}
