//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::closure::Closure;
pub use self::copy::*;
pub use self::object::*;

use std::collections::BTreeSet;
use std::path::PathBuf;

use crate::local::{Backend, Filesystem, Objects, Packages};

mod closure;
mod copy;
mod install;
mod local;
mod object;
mod util;

/// A content-addressable store of installed software packages.
#[derive(Debug)]
pub struct Store<B: Backend = Filesystem> {
    objects: B::Objects,
    packages: B::Packages,
}

impl<B: Backend> Store<B> {
    /// Opens the store on the directory located in `path`.
    ///
    /// Returns `Err` if the path does not exist or is not a valid store directory.
    pub fn open<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let (objects, packages) = B::open(path.into())?;
        Ok(Store { objects, packages })
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
        Ok(Store { objects, packages })
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
        Ok(Store { objects, packages })
    }

    /// Inserts a tree object into the store, returning its unique ID.
    ///
    /// Returns `Err` if the object could not be inserted into the store or an I/O error occurred.
    pub fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        if let Object::Package(ref pkg) = &o {
            self.packages.install(pkg, &self.objects)?;
        }

        self.objects.insert_object(o)
    }
}

impl<B: Backend> Store<B> {
    /// Computes the filesystem closure for the given packages.
    ///
    /// Returns `Err` if any of the given object IDs do not exist, any of the object IDs do not
    /// refer to a `Package` object, a cycle or structural inconsistency is detected in the
    /// reference graph, or an I/O error occurred.
    #[inline]
    pub fn compute_closure(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Closure> {
        closure::compute(&self.objects, pkgs, |_id, _kind| Ok(true))
    }
}

impl<'s, B: Backend> Source<'s> for Store<B> {
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
            let exists = dst.contains_object(&id, Some(kind))?;
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
        let rev_topo = closure.into_iter().rev();
        Ok(Box::new(rev_topo.map(move |(id, kind)| {
            self.objects.get_object(id, Some(kind))
        })))
    }
}

impl<B: Backend> Destination for Store<B> {
    type Progress = ();

    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        Ok(self.objects.contains_object(id, kind))
    }

    fn insert_objects<I>(&mut self, stream: I) -> anyhow::Result<Self::Progress>
    where
        I: Iterator<Item = anyhow::Result<Object>>,
    {
        for result in stream {
            let obj = result?;
            self.insert_object(obj)?;
        }

        Ok(())
    }
}
