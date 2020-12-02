//! Filesystem-backed store implementation.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use filetime::FileTime;

use super::{Backend, Entries};
use crate::{Blob, ContentAddressable, Entry, Object, ObjectId, ObjectKind, Package, Tree};

const OBJECTS_SUBDIR: &str = "objects";
const PACKAGES_SUBDIR: &str = "packages";

/// A store implementation backed by the local filesystem.
///
/// It leverages hard linking to aggressively deduplicate files and save disk space. Unlike Nix,
/// this file-level deduplication does not require scheduled optimization passes that "stop the
/// world," but rather, it happens transparently each time new objects are inserted into the store.
#[derive(Debug)]
pub struct Filesystem {
    objects_dir: PathBuf,
    packages_dir: PathBuf,
}

impl Filesystem {
    /// Opens the store on the directory located in `path`.
    ///
    /// Returns `Err` if the path does not exist or is not a valid store directory.
    pub fn open(path: PathBuf) -> anyhow::Result<Self> {
        let path = path.canonicalize()?;
        let objects_dir = path.join(OBJECTS_SUBDIR);
        let packages_dir = path.join(PACKAGES_SUBDIR);

        if objects_dir.is_dir() && packages_dir.is_dir() {
            Ok(Filesystem {
                objects_dir,
                packages_dir,
            })
        } else if path.exists() {
            Err(anyhow!("`{}` is not a store directory", path.display()))
        } else {
            Err(anyhow!("could not open, `{}` not found", path.display()))
        }
    }

    /// Initializes a new store directory at `path` and opens it.
    ///
    /// If an empty target directory does not already exist at that location, it will be
    /// automatically created. If a store directory already exists at that location, it will be
    /// opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory, or if a new
    /// store directory could not be created at `path` due to permissions or other I/O errors.
    pub fn init(path: PathBuf) -> anyhow::Result<Self> {
        let objects_dir = path.join(OBJECTS_SUBDIR);
        let packages_dir = path.join(PACKAGES_SUBDIR);

        if !path.exists() {
            std::fs::create_dir(&path).context("could not create new store directory")?;
            std::fs::create_dir(&objects_dir).context("could not create `objects` dir")?;
            std::fs::create_dir(&packages_dir).context("could not create `packages` dir")?;
        }

        Self::open(path)
    }

    /// Initializes a store inside the empty directory referred to by `path` and opens it.
    ///
    /// If a store directory already exists at that location, it will be opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory or an empty
    /// directory, or the new store directory could not be initialized at `path` due to permissions
    /// or I/O errors.
    pub fn init_bare(path: PathBuf) -> anyhow::Result<Self> {
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

        Ok(Filesystem {
            objects_dir,
            packages_dir,
        })
    }

    fn instantiate(&mut self, pkg: &Package) -> anyhow::Result<()> {
        let target_dir = self.packages_dir.join(pkg.install_name());

        if target_dir.exists() {
            Ok(())
        } else {
            // Ensure all object references are present in the store before instantiation.
            let missing_refs: BTreeSet<_> = pkg
                .references
                .iter()
                .filter_map(|&id| self.get_package(id).ok())
                .map(|pkg| pkg.install_name())
                .filter(|name| !self.packages_dir.join(&name).exists())
                .collect();

            if missing_refs.is_empty() {
                let tree = self.get_tree(pkg.tree)?;

                // Serialize the tree to a temporary directory first. This way, if an error occurs,
                // the `packages` directory will not be left in an inconsistent state.
                let temp_dir = tempfile::tempdir_in("/var/tmp")?;
                self.write_tree(temp_dir.path(), tree)?;

                // Atomically move the package directory to its final location.
                let finished_dir = temp_dir.into_path();
                match std::fs::rename(finished_dir, &target_dir) {
                    Ok(()) => Ok(()),
                    Err(e) if e.raw_os_error() == Some(39) => Ok(()),
                    Err(e) => Err(e).context(format!("failed to persist {}", target_dir.display())),
                }
            } else {
                Err(anyhow!(
                    "failed to instantiate package, missing: {:?}",
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
                    src.set_extension(ObjectKind::Blob.as_str());
                    std::fs::hard_link(&src, &entry_path).map_err(|e| match e.kind() {
                        std::io::ErrorKind::NotFound => anyhow!("blob object {} not found", id),
                        _ => e.into(),
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

impl Backend for Filesystem {
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
        let mut path = self.objects_dir.join(id.to_path_buf());
        path.set_extension(o.kind().as_str());

        // Persist into the `objects` directory, ensuring the parent directory exists.
        if !path.exists() {
            match o {
                Object::Blob(blob) => ensure_parent_dir(&path, |p| blob.persist(p))?,
                Object::Tree(tree) => ensure_parent_dir(&path, |p| tree.persist(p))?,
                Object::Package(pkg) => ensure_parent_dir(&path, |p| {
                    self.instantiate(&pkg)?;
                    pkg.persist(p)
                })?,
                Object::Spec(spec) => ensure_parent_dir(&path, |p| spec.persist(p))?,
            }
        }

        Ok(id)
    }

    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object> {
        let mut path = self.objects_dir.join(id.to_path_buf());

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
            Some(ObjectKind::Blob) => Blob::from_path_unchecked(&path, id).map(Object::Blob),
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

    fn iter_objects(&self) -> anyhow::Result<Entries<'_>> {
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
                .and_then(|hash: String| hash.parse());

            let kind_res = p
                .extension()
                .and_then(|ext| ext.to_str())
                .ok_or(anyhow!("object file extension is not valid UTF-8"))
                .and_then(|ext| ext.parse());

            id_res.and_then(|id| kind_res.map(|kind| (id, kind)))
        }));

        Ok(objects)
    }

    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> bool {
        let mut path = self.objects_dir.join(id.to_path_buf());

        // Use `kind`, if specified, as a perf optimization to guess the file extension.
        if let Some(k) = kind {
            path.set_extension(k.as_str());
            path.exists()
        } else {
            ObjectKind::iter().any(|k| {
                path.set_extension(k.as_str());
                path.exists()
            })
        }
    }
}
