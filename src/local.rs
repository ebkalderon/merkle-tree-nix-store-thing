//! Local store interface and provided implementations.

pub use self::fs::Filesystem;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::anyhow;

use crate::copy::{Delta, Destination, Progress as CopyProgress, Source};
use crate::{closure, Closure, Object, ObjectId, ObjectKind, Objects, Package, Store};

mod fs;
mod install;

/// A content-addressable store of installed software packages.
#[derive(Debug)]
pub struct LocalStore<B: Backend = Filesystem> {
    objects: B::Objects,
    packages: B::Packages,
}

impl<B: Backend> LocalStore<B> {
    /// Opens the store on the directory located in `path`.
    ///
    /// Returns `Err` if the path does not exist or is not a valid store directory.
    pub fn open<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let (objects, packages) = B::open(path.into())?;
        Ok(LocalStore { objects, packages })
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
        let (objects, packages) = B::init(path.into())?;
        Ok(LocalStore { objects, packages })
    }

    /// Initializes a store inside the empty directory referred to by `path` and opens it.
    ///
    /// If a store directory already exists at that location, it will be opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory or an empty
    /// directory, or the new store directory could not be initialized at `path` due to permissions
    /// or I/O errors.
    pub fn init_bare<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let (objects, packages) = B::init_bare(path.into())?;
        Ok(LocalStore { objects, packages })
    }
}

impl<B: Backend> Objects for LocalStore<B> {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        if let Object::Package(ref pkg) = &o {
            self.packages.install(pkg, &self.objects)?;
        }

        self.objects.insert_object(o)
    }

    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object> {
        self.objects.get_object(id, kind)
    }

    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        self.objects.contains_object(id, kind)
    }

    fn object_size(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<u64> {
        self.objects.object_size(id, kind)
    }
}

impl<B: Backend> Store for LocalStore<B> {
    fn build_spec(&self, _spec: ObjectId) -> anyhow::Result<()> {
        unimplemented!()
    }
}

impl<'s, B: Backend> Source<'s> for LocalStore<B> {
    type Objects = Box<dyn Iterator<Item = anyhow::Result<Object>> + 's>;

    fn find_missing<D>(&self, dst: &D, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Delta>
    where
        D: Destination + ?Sized,
    {
        // This delta computation technique was shamelessly stolen from Git, as documented
        // meticulously in these two pages:
        //
        // https://matthew-brett.github.io/curious-git/git_push_algorithm.html
        // https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt

        let mut num_present = 0;
        let missing = closure::compute(&self.objects, pkgs, |id, kind| {
            let exists = dst.contains(&id, Some(kind))?;
            if exists {
                num_present += 1;
            }
            Ok(!exists)
        })?;

        Ok(Delta {
            num_present,
            missing,
        })
    }

    fn yield_objects(&'s self, closure: Closure) -> anyhow::Result<Self::Objects> {
        Ok(Box::new(closure.sort_yield().into_iter().map(
            move |(id, kind, _)| self.objects.get_object(id, Some(kind)),
        )))
    }
}

impl<B: Backend> Destination for LocalStore<B> {
    type Progress = std::iter::Empty<anyhow::Result<CopyProgress>>;

    fn contains(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        self.objects.contains_object(id, kind)
    }

    fn insert_objects<I>(&mut self, stream: I) -> anyhow::Result<Self::Progress>
    where
        I: Iterator<Item = anyhow::Result<Object>>,
    {
        for result in stream {
            let obj = result?;
            self.insert_object(obj)?;
        }

        Ok(std::iter::empty())
    }
}

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

    /// Installs a `Package` object in the repository.
    ///
    /// This method verifies that all references are present in the store before installing.
    ///
    /// Returns `Err` if the package could not be instantiated or an I/O error occurred.
    fn install(&mut self, pkg: &Package, objects: &Self::Objects) -> anyhow::Result<()> {
        let missing_refs: Vec<_> = pkg
            .references
            .iter()
            .copied()
            .filter(|&id| objects.get_package(id).ok().is_none())
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
