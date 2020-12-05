//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::closure::{Closure, Delta};
pub use self::object::*;

use std::collections::BTreeSet;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use crate::local::{Backend, Filesystem, Objects, Packages};
use crate::remote::{ClosureStream, Remote};

pub mod remote;

mod closure;
mod install;
mod local;
mod object;
mod reference;

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
        closure::compute(self, pkgs)
    }

    /// Computes a delta closure which only contains objects that are missing on the remote store.
    ///
    /// Returns `Err` if any of the given object IDs do not exist in this store, any of the object
    /// IDs do not refer to a `Package` object, a cycle or structural inconsistency is detected in
    /// the reference graph, or an I/O error occurred.
    pub fn compute_delta<R>(&self, pkgs: BTreeSet<ObjectId>, dest: &R) -> anyhow::Result<Delta>
    where
        R: Remote + ?Sized,
    {
        closure::find_delta(self, dest, pkgs)
    }

    /// Iterates over the closure and lazily yields each element in reverse topological order.
    ///
    /// This ordering is important because it ensures objects and packages can be inserted into
    /// stores in a consistent order, where all references are inserted before their referrers.
    pub fn yield_closure(&self, mut closure: Closure) -> ClosureStream<'_> {
        Box::new(std::iter::from_fn(move || {
            if let Some((id, kind)) = closure.next() {
                Some(self.objects.get_object(id, Some(kind)))
            } else {
                None
            }
        }))
    }

    /// Copies `pkgs` and their dependencies to the remote source `dest`. This will resolve the
    /// delta closure between the source and the destination and only synchronize objects that are
    /// missing.
    pub fn copy_closure<R>(&self, pkgs: BTreeSet<ObjectId>, dest: &mut R) -> anyhow::Result<()>
    where
        R: Remote + ?Sized,
    {
        let delta = self.compute_delta(pkgs, dest)?;
        let objects = self.yield_closure(delta.missing);
        dest.upload_objects(objects)?;
        Ok(())
    }
}

impl<B: Backend> Remote for Store<B> {
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        Ok(self.objects.contains_object(id, kind))
    }

    fn download_objects(&self, closure: Closure) -> anyhow::Result<ClosureStream<'_>> {
        Ok(self.yield_closure(closure))
    }

    fn upload_objects(&mut self, stream: ClosureStream) -> anyhow::Result<()> {
        for result in stream {
            let obj = result?;
            self.insert_object(obj)?;
        }

        Ok(())
    }
}

/// An faster implementation of `std::io::copy()` which uses a larger 64K buffer instead of 8K.
///
/// This larger buffer size leverages SIMD on x86_64 and other modern platforms for faster speeds.
/// See this GitHub issue: https://github.com/rust-lang/rust/issues/49921
fn copy_wide<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> io::Result<u64> {
    let mut buffer = [0; 65536];
    let mut total = 0;
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(total),
            Ok(n) => {
                writer.write_all(&buffer[..n])?;
                total += n as u64;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}
