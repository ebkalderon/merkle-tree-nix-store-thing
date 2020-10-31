//! Filesystem-backed store implementation.

use std::collections::BTreeSet;
use std::io::Write;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context};
use filetime::FileTime;

use super::{Objects, Store};
use crate::object::{Blob, ContentAddressable, Entry, Object, ObjectId, ObjectKind, Package, Tree};

const OBJECTS_SUBDIR: &str = "objects";
const PACKAGES_SUBDIR: &str = "packages";

/// A store implementation backed by the local filesystem.
///
/// It leverages hard linking to aggressively deduplicate files and save disk space. Unlike Nix,
/// this file-level deduplication does not require scheduled optimization passes that "stop the
/// world," but rather, it happens transparently each time new objects are inserted into the store.
#[derive(Debug)]
pub struct FsStore {
    objects_dir: PathBuf,
    packages_dir: PathBuf,
}

impl FsStore {
    /// Opens the store on the directory located in `path`.
    ///
    /// Returns `Err` if the path does not exist or is not a valid store directory.
    pub fn open<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        let objects_dir = base.join(OBJECTS_SUBDIR);
        let packages_dir = base.join(PACKAGES_SUBDIR);

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

    /// Initializes a new store directory at `path` and opens it.
    ///
    /// If an empty target directory does not already exist at that location, it will be
    /// automatically created. If a store directory already exists at that location, it will be
    /// opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory, or if a new
    /// store directory could not be created at `path` due to permissions or other I/O errors.
    pub fn init<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        let objects_dir = base.join(OBJECTS_SUBDIR);
        let packages_dir = base.join(PACKAGES_SUBDIR);

        if !base.exists() {
            std::fs::create_dir(&base).context("could not create new store directory")?;
            std::fs::create_dir(&objects_dir).context("could not create `objects` dir")?;
            std::fs::create_dir(&packages_dir).context("could not create `packages` dir")?;
        }

        Self::open(base)
    }

    /// Initializes a store inside the empty directory referred to by `path` and opens it.
    ///
    /// If a store directory already exists at that location, it will be opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory or an empty
    /// directory, or the new store directory could not be initialized at `path` due to permissions
    /// or I/O errors.
    pub fn init_bare<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let base = path.into();
        let objects_dir = base.join(OBJECTS_SUBDIR);
        let packages_dir = base.join(PACKAGES_SUBDIR);

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
            // Ensure all object references are present in the store before checkout.
            let missing_refs: BTreeSet<_> = pkg
                .references
                .iter()
                .filter_map(|&id| self.get_package(id).ok())
                .map(|pkg| pkg.install_name())
                .filter(|n| !self.packages_dir.join(n.to_string()).exists())
                .collect();

            if missing_refs.is_empty() {
                let tree = self.get_tree(pkg.tree)?;

                // Serialize the tree to a temporary directory first. This way, if an error occurs,
                // the `packages` directory will not be left in an inconsistent state.
                let temp_dir = tempfile::tempdir()?;
                self.write_tree(temp_dir.path(), tree)?;

                // Atomically move the checked out package directory to its final location.
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

impl Store for FsStore {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        fn write_object<F>(p: &Path, perms: u32, mut write_fn: F) -> anyhow::Result<()>
        where
            F: FnMut(&std::fs::File) -> anyhow::Result<()>,
        {
            if !p.exists() {
                let mut file = tempfile::NamedTempFile::new()?;
                write_fn(&file.as_file())?;

                let perms = std::fs::Permissions::from_mode(perms);
                file.as_file_mut().set_permissions(perms)?;
                filetime::set_file_mtime(file.path(), FileTime::zero())?;

                file.as_file_mut().sync_all()?;
                file.persist(p)?;
            }

            Ok(())
        }

        // Prepare to serialize object into: `<store>/objects/ab/cdef01234567890.<kind>`
        let id = o.object_id();
        let mut path = self.objects_dir.join(id.to_path_buf());

        // Create the two-character parent directory, if it doesn't already exist.
        let parent_dir = path.parent().expect("path cannot be at filesystem root");
        if !parent_dir.exists() {
            std::fs::create_dir(parent_dir)?;
        }

        path.set_extension(o.kind().as_str());
        match o {
            Object::Blob(blob) => blob.persist(&path)?,
            Object::Tree(tree) => {
                write_object(&path, 0o444, |mut file| {
                    serde_json::to_writer(&mut file, &tree)?;
                    file.flush()?;
                    Ok(())
                })?;
            }
            Object::Package(pkg) => {
                self.checkout(&pkg)?;
                write_object(&path, 0o444, |mut file| {
                    serde_json::to_writer(&mut file, &pkg)?;
                    file.flush()?;
                    Ok(())
                })?;
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
            Some(ObjectKind::Blob) => {
                let file = std::fs::File::open(path)?;
                let is_executable = file.metadata()?.mode() & 0o100 != 0;
                Ok(Object::Blob(Blob::from_reader_raw(
                    Box::new(file),
                    is_executable,
                    id,
                )))
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

    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        let mut path = self.objects_dir.join(id.to_path_buf());

        // Use `kind`, if specified, as a perf optimization to guess the file extension.
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
