//! Filesystem-backed store implementation.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use filetime::FileTime;

use super::{install, Backend, Objects, Packages};
use crate::{
    Blob, ContentAddressable, Entry, Object, ObjectExt, ObjectId, ObjectKind, Package, Tree,
};

const OBJECTS_SUBDIR: &str = "objects";
const PACKAGES_SUBDIR: &str = "packages";

/// A store implementation backed by the local filesystem.
///
/// It leverages hard linking to aggressively deduplicate files and save disk space. Unlike Nix,
/// this file-level deduplication does not require scheduled optimization passes that "stop the
/// world," but rather, it happens transparently each time new objects are inserted into the store.
#[derive(Debug)]
pub enum Filesystem {}

impl Backend for Filesystem {
    type Objects = FsObjects;
    type Packages = FsPackages;

    fn open(path: PathBuf) -> anyhow::Result<(Self::Objects, Self::Packages)> {
        let path = path.canonicalize()?;
        let objects_dir = path.join(OBJECTS_SUBDIR);
        let packages_dir = path.join(PACKAGES_SUBDIR);

        if objects_dir.is_dir() && packages_dir.is_dir() {
            Ok((FsObjects(objects_dir), FsPackages(packages_dir)))
        } else if path.exists() {
            Err(anyhow!("`{}` is not a store directory", path.display()))
        } else {
            Err(anyhow!("could not open, `{}` not found", path.display()))
        }
    }

    fn init(path: PathBuf) -> anyhow::Result<(Self::Objects, Self::Packages)> {
        let objects_dir = path.join(OBJECTS_SUBDIR);
        let packages_dir = path.join(PACKAGES_SUBDIR);

        if !path.exists() {
            std::fs::create_dir(&path).context("could not create new store directory")?;
            std::fs::create_dir(&objects_dir).context("could not create `objects` dir")?;
            std::fs::create_dir(&packages_dir).context("could not create `packages` dir")?;
        }

        Self::open(path)
    }

    fn init_bare(path: PathBuf) -> anyhow::Result<(Self::Objects, Self::Packages)> {
        let path = path.canonicalize()?;
        let objects_dir = path.join(OBJECTS_SUBDIR);
        let packages_dir = path.join(PACKAGES_SUBDIR);

        let entries = std::fs::read_dir(&path).context("could not bare-init store directory")?;
        if entries.count() == 0 {
            std::fs::create_dir(&objects_dir).context("could not create `objects` dir")?;
            std::fs::create_dir(&packages_dir).context("could not create `packages` dir")?;
        } else if !objects_dir.exists() || !packages_dir.exists() {
            return Err(anyhow!("could not init store, expected empty directory"));
        }

        Ok((FsObjects(objects_dir), FsPackages(packages_dir)))
    }
}

/// A filesystem-backed `objects` directory.
#[derive(Clone, Debug)]
pub struct FsObjects(PathBuf);

impl Objects for FsObjects {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        /// Ensures the parent directory of `p` exists, creating it atomically if it does not.
        fn ensure_parent_dir<F>(p: &Path, persist_obj: F) -> anyhow::Result<()>
        where
            F: FnOnce(&Path) -> anyhow::Result<()>,
        {
            let parent_dir = p.parent().expect("object path must have parent dir");

            if parent_dir.exists() {
                persist_obj(p)
            } else {
                let temp_dir = tempfile::tempdir_in("/var/tmp")?;
                let temp_file = temp_dir.path().join(p.file_name().unwrap());
                persist_obj(&temp_file)?;

                let temp_dir = temp_dir.into_path();
                match std::fs::rename(&temp_dir, &parent_dir) {
                    Ok(()) => Ok(()),
                    Err(_) if parent_dir.is_dir() => match std::fs::rename(&temp_file, p) {
                        Ok(()) => Ok(()),
                        Err(_) if p.is_file() => Ok(()),
                        Err(e) => Err(e.into()),
                    },
                    Err(e) => Err(e.into()),
                }
            }
        }

        // Prepare to serialize object into: `<store>/objects/ab/cdef01234567890.<kind>`
        let id = o.object_id();
        let mut path = self.0.join(id.to_path_buf());
        path.set_extension(o.kind().as_str());

        // Persist into the `objects` directory, ensuring the parent directory exists.
        if !path.exists() {
            match o {
                Object::Blob(blob) => ensure_parent_dir(&path, |p| blob.persist(p))?,
                Object::Tree(tree) => ensure_parent_dir(&path, |p| tree.persist(p))?,
                Object::Package(pkg) => ensure_parent_dir(&path, |p| pkg.persist(p))?,
                Object::Spec(spec) => ensure_parent_dir(&path, |p| spec.persist(p))?,
            }
        }

        Ok(id)
    }

    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object> {
        let mut path = self.0.join(id.to_path_buf());

        // Use `kind`, if specified, as a perf optimization to guess the file extension.
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
                let blob = Blob::from_store_path(path, id)?;
                Ok(Object::Blob(blob))
            }
            Some(ObjectKind::Tree) => {
                let file = std::fs::File::open(path)?;
                let tree = serde_json::from_reader(file)?;
                Ok(Object::Tree(tree))
            }
            Some(ObjectKind::Package) => {
                let file = std::fs::File::open(path)?;
                let package = serde_json::from_reader(file)?;
                Ok(Object::Package(package))
            }
            Some(ObjectKind::Spec) => {
                let file = std::fs::File::open(path)?;
                let spec = serde_json::from_reader(file)?;
                Ok(Object::Spec(spec))
            }
            None => Err(anyhow!("object {} not found", id)),
        }
    }

    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        let mut path = self.0.join(id.to_path_buf());

        // Use `kind`, if specified, as a perf optimization to guess the file extension.
        if let Some(k) = kind {
            path.set_extension(k.as_str());
            Ok(path.exists())
        } else {
            Ok(ObjectKind::iter().any(|k| {
                path.set_extension(k.as_str());
                path.exists()
            }))
        }
    }

    fn object_size(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<u64> {
        let mut path = self.0.join(id.to_path_buf());

        // Use `kind`, if specified, as a perf optimization to guess the file extension.
        if let Some(k) = kind {
            path.set_extension(k.as_str());
        } else {
            ObjectKind::iter().any(|k| {
                path.set_extension(k.as_str());
                path.exists()
            });
        };

        let metadata = std::fs::metadata(path)?;
        Ok(metadata.len())
    }
}

/// A filesystem-backed `packages` directory.
#[derive(Debug)]
pub struct FsPackages(PathBuf);

impl Packages for FsPackages {
    type Objects = FsObjects;

    fn path(&self) -> &Path {
        &self.0
    }

    fn instantiate(&mut self, pkg: &Package, objects: &Self::Objects) -> anyhow::Result<()> {
        let target_dir = self.0.join(pkg.install_name());

        if target_dir.exists() {
            Ok(())
        } else {
            let tree = objects.get_tree(pkg.tree)?;

            // Serialize the tree to a temporary directory first. This way, if an error occurs,
            // the `packages` directory will not be left in an inconsistent state.
            let temp_dir = tempfile::tempdir_in("/var/tmp")?;
            let mut builder = TreeBuilder::new(objects, pkg, &target_dir);
            builder.construct(tree, temp_dir.path())?;

            // Atomically move the package directory to its final location.
            let finished_dir = temp_dir.into_path();
            match std::fs::rename(finished_dir, &target_dir) {
                Ok(()) => Ok(()),
                Err(e) if e.raw_os_error() == Some(39) => Ok(()),
                Err(e) => Err(e).context(format!("failed to persist {}", target_dir.display())),
            }
        }
    }
}

// Use a struct with fields and methods because recursive closures are impossible in Rust.
struct TreeBuilder<'a> {
    objects: &'a FsObjects,
    pkg: &'a Package,
    root_dir: &'a Path,
}

impl<'a> TreeBuilder<'a> {
    fn new(objects: &'a FsObjects, pkg: &'a Package, root_dir: &'a Path) -> Self {
        TreeBuilder {
            objects,
            pkg,
            root_dir,
        }
    }

    fn construct(&mut self, tree: Tree, tree_dir: &Path) -> anyhow::Result<()> {
        if !tree_dir.exists() {
            std::fs::create_dir_all(&tree_dir)?;
            println!("=> created tree subdir: {}", tree_dir.display());
        }

        for (name, entry) in &tree.entries {
            let dst = tree_dir.join(name);
            match entry {
                Entry::Tree { id } => {
                    let subtree = self.objects.get_tree(*id)?;
                    self.construct(subtree, &dst)?;
                }
                Entry::Blob { id } => {
                    let mut src = self.objects.0.join(id.to_path_buf());
                    src.set_extension(ObjectKind::Blob.as_str());

                    if let Some(ref offsets) = self.pkg.self_references.get(&id) {
                        std::fs::copy(&src, &dst)?;

                        let mut file = std::fs::File::open(&dst)?;
                        install::rewrite_paths(&mut file, &self.root_dir, offsets)?;

                        let metadata = file.metadata()?;
                        let perms = metadata.permissions();
                        file.set_permissions(perms)?;

                        let zero = FileTime::zero();
                        filetime::set_symlink_file_times(&dst, zero, zero)?;
                    } else {
                        std::fs::hard_link(&src, &dst).map_err(|e| match e.kind() {
                            std::io::ErrorKind::NotFound => anyhow!("blob object {} not found", id),
                            _ => e.into(),
                        })?;
                        println!(
                            "=> hard-linked blob: {} -> {}",
                            src.display(),
                            dst.display()
                        );
                    }
                }
                Entry::Symlink { target } => {
                    std::os::unix::fs::symlink(&target, &dst)?;
                    let zero = FileTime::zero();
                    filetime::set_symlink_file_times(&dst, zero, zero)?;
                    println!(
                        "=> created symlink: {} -> {}",
                        dst.display(),
                        target.display()
                    );
                }
            }
        }

        filetime::set_file_mtime(&tree_dir, FileTime::zero())?;

        Ok(())
    }
}
