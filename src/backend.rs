//! Types of storage backends.

pub use self::fs::Filesystem;

use std::path::{Path, PathBuf};

use anyhow::anyhow;

use crate::{Blob, InstallName, Object, ObjectId, ObjectKind, Package, Spec, Tree};

mod fs;

/// A storage backend for the store.
pub trait Backend {
    /// Type of `objects` repository to use.
    type Objects: Objects;
    /// Type of `packages` repository to use.
    type Packages: Packages<Objects = Self::Objects>;

    /// Opens the store on the directory located in `path`.
    ///
    /// Returns `Err` if the path does not exist or is not a valid store directory.
    fn open(path: PathBuf) -> anyhow::Result<(Self::Objects, Self::Packages)>;

    /// Initializes a new store directory at `path` and opens it.
    ///
    /// If an empty target directory does not already exist at that location, it will be
    /// automatically created. If a store directory already exists at that location, it will be
    /// opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory, or if a new
    /// store directory could not be created at `path` due to permissions or other I/O errors.
    fn init(path: PathBuf) -> anyhow::Result<(Self::Objects, Self::Packages)>;

    /// Initializes a store inside the empty directory referred to by `path` and opens it.
    ///
    /// If a store directory already exists at that location, it will be opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory or an empty
    /// directory, or the new store directory could not be initialized at `path` due to permissions
    /// or I/O errors.
    fn init_bare(path: PathBuf) -> anyhow::Result<(Self::Objects, Self::Packages)>;
}

/// A content-addressable repository of Merkle tree objects.
pub trait Objects {
    /// Inserts a tree object into the store, returning its unique ID.
    ///
    /// Implementers _must_ ensure that this method behaves as a completely atomic transaction.
    /// Implementers _should_ take care to memoize this method such that if the object already
    /// exists in the store, this method does nothing.
    ///
    /// Returns `Err` if the object could not be inserted into the store or an I/O error occurred.
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId>;

    /// Looks up a specific tree object in the store and retrieves it, if it exists.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// store will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if the object does not exist or an I/O error occurred.
    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object>;

    /// Returns `true` if the store contains a tree object with the given unique ID, or `false`
    /// otherwise.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// store will attempt to guess the desired object type, if it is not immediately known.
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> bool;

    /// Looks up a `Blob` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Blob` object,
    /// or an I/O error occurred.
    fn get_blob(&self, id: ObjectId) -> anyhow::Result<Blob> {
        self.get_object(id, Some(ObjectKind::Blob)).and_then(|o| {
            o.into_blob()
                .map_err(|_| anyhow!("{} is not a blob object", id))
        })
    }

    /// Looks up a `Tree` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Tree` object,
    /// or an I/O error occurred.
    fn get_tree(&self, id: ObjectId) -> anyhow::Result<Tree> {
        self.get_object(id, Some(ObjectKind::Tree)).and_then(|o| {
            o.into_tree()
                .map_err(|_| anyhow!("{} is not a tree object", id))
        })
    }

    /// Looks up a `Package` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Package`
    /// object, or an I/O error occurred.
    fn get_package(&self, id: ObjectId) -> anyhow::Result<Package> {
        self.get_object(id, Some(ObjectKind::Package))
            .and_then(|o| {
                o.into_package()
                    .map_err(|_| anyhow!("{} is not a package object", id))
            })
    }

    /// Looks up a `Spec` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Spec` object,
    /// or an I/O error occurred.
    fn get_spec(&self, id: ObjectId) -> anyhow::Result<Spec> {
        self.get_object(id, Some(ObjectKind::Spec)).and_then(|o| {
            o.into_spec()
                .map_err(|_| anyhow!("{} is not a spec object", id))
        })
    }
}

/// A repository of installed packages.
pub trait Packages {
    /// The `objects` repository to use.
    type Objects: Objects;

    /// Returns the absolute path to the `packages` directory.
    ///
    /// This path is not required to exist on disk (i.e. an in-memory backend), but its value is
    /// nonetheless required when staging packages and patching out self-references.
    fn path(&self) -> &Path;

    /// Instantiates a `Package` object from the underlying Merkle tree.
    ///
    /// Implementers _must_ ensure that this method behaves as a completely atomic transaction.
    /// Implementers _should_ take care to memoize this method such that if the package is already
    /// installed, this method does nothing.
    ///
    /// This method does not verify that all references are present in the store before installing.
    ///
    /// Returns `Err` if the package could not be instantiated or an I/O error occurred.
    fn instantiate(&mut self, pkg: &Package, objects: &Self::Objects) -> anyhow::Result<()>;

    /// Returns `true` if the given package is installed.
    fn contains(&self, pkg_name: &InstallName) -> bool;

    /// Installs a `Package` object in the repository.
    ///
    /// This method verifies that all references are present in the store before installing.
    ///
    /// Returns `Err` if the package could not be instantiated or an I/O error occurred.
    fn install(&mut self, pkg: &Package, objects: &Self::Objects) -> anyhow::Result<()> {
        // Ensure all object references are present in the store before instantiation.
        let missing_refs: Vec<_> = pkg
            .references
            .iter()
            .copied()
            .filter(|&id| {
                objects
                    .get_package(id)
                    .ok()
                    .filter(|pkg| self.contains(&pkg.install_name()))
                    .is_none()
            })
            .collect();

        if !missing_refs.is_empty() {
            return Err(anyhow!(
                "failed to install package, missing references: {:?}",
                missing_refs
            ));
        }

        self.instantiate(pkg, &objects)
    }
}
