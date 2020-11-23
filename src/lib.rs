//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::closure::Closure;
pub use self::object::*;
pub use self::store::Entries;

use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use anyhow::anyhow;

use crate::closure::{compute_closure, compute_delta_closure, Include};
use crate::remote::Remote;
use crate::store::{Backend, Filesystem, Memory};

pub mod remote;

mod closure;
mod object;
mod store;

/// A streaming iterator of tree objects.
pub type Objects<'a> = Box<dyn Iterator<Item = anyhow::Result<Object>> + 'a>;

/// A content-addressable store of installed software packages.
#[derive(Debug)]
pub struct Store<B: Backend = Filesystem> {
    backend: B,
}

impl Store<Filesystem> {
    /// Opens the store on the directory located in `path`.
    ///
    /// Returns `Err` if the path does not exist or is not a valid store directory.
    #[inline]
    pub fn open<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let backend = Filesystem::open(path.into())?;
        Ok(Store::with_backend(backend))
    }

    /// Initializes a new store directory at `path` and opens it.
    ///
    /// If an empty target directory does not already exist at that location, it will be
    /// automatically created. If a store directory already exists at that location, it will be
    /// opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory, or if a new
    /// store directory could not be created at `path` due to permissions or other I/O errors.
    #[inline]
    pub fn init<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let backend = Filesystem::init(path.into())?;
        Ok(Store::with_backend(backend))
    }

    /// Initializes a store inside the empty directory referred to by `path` and opens it.
    ///
    /// If a store directory already exists at that location, it will be opened.
    ///
    /// Returns `Err` if `path` exists and does not point to a valid store directory or an empty
    /// directory, or the new store directory could not be initialized at `path` due to permissions
    /// or I/O errors.
    #[inline]
    pub fn init_bare<P: Into<PathBuf>>(path: P) -> anyhow::Result<Self> {
        let backend = Filesystem::init_bare(path.into())?;
        Ok(Store::with_backend(backend))
    }
}

impl Store<Memory> {
    /// Constructs a new in-memory store. This is useful for testing.
    #[inline]
    pub fn in_memory() -> Self {
        Store::with_backend(Memory::default())
    }
}

impl<B: Backend> Store<B> {
    fn with_backend(backend: B) -> Self {
        Store { backend }
    }

    /// Inserts a tree object into the store, returning its unique ID.
    ///
    /// Returns `Err` if the object could not be inserted into the store or an I/O error occurred.
    #[inline]
    pub fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        self.backend.insert_object(o)
    }

    /// Looks up a specific tree object in the store and retrieves it, if it exists.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// store will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if the object does not exist or an I/O error occurred.
    #[inline]
    pub fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object> {
        self.backend.get_object(id, kind)
    }

    /// Returns an iterator over the objects contained in this store.
    ///
    /// The order in which this iterator returns entries is platform and filesystem dependent.
    ///
    /// Returns `Err` if the store is corrupt or an I/O error occurred.
    #[inline]
    pub fn iter_objects(&self) -> anyhow::Result<Entries<'_>> {
        self.backend.iter_objects()
    }

    /// Returns `Ok(true)` if the store contains a tree object with the given unique ID, or
    /// `Ok(false)` otherwise.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// store will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if the store is corrupt or an I/O error occurred.
    #[inline]
    pub fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        self.backend.contains_object(id, kind)
    }

    /// Looks up a `Blob` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Blob` object,
    /// or an I/O error occurred.
    #[inline]
    pub fn get_blob(&self, id: ObjectId) -> anyhow::Result<Blob> {
        self.backend.get_blob(id)
    }

    /// Looks up a `Tree` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Tree` object,
    /// or an I/O error occurred.
    #[inline]
    pub fn get_tree(&self, id: ObjectId) -> anyhow::Result<Tree> {
        self.backend.get_tree(id)
    }

    /// Looks up a `Package` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Package`
    /// object, or an I/O error occurred.
    #[inline]
    pub fn get_package(&self, id: ObjectId) -> anyhow::Result<Package> {
        self.backend.get_package(id)
    }

    /// Looks up a `Spec` object with the given ID and retrieves it, if it exists.
    ///
    /// Returns `Err` if the object does not exist, the given ID does not refer to a `Spec` object,
    /// or an I/O error occurred.
    #[inline]
    pub fn get_spec(&self, id: ObjectId) -> anyhow::Result<Spec> {
        self.backend.get_spec(id)
    }

    /// Computes the filesystem closure for the given packages.
    ///
    /// Returns `Err` if any of the given object IDs do not exist, any of the object IDs do not
    /// refer to a `Package` object, a cycle or structural inconsistency is detected in the
    /// reference graph, or an I/O error occurred.
    pub fn compute_closure(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Closure> {
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
            ObjectKind::Spec => unimplemented!(),
        })?;

        Ok(closure
            .into_iter()
            .map(|Ref(id, kind)| (id, kind))
            .collect())
    }

    /// Computes a delta closure which only contains objects that are missing on the remote store.
    ///
    /// Returns `Err` if any of the given object IDs do not exist in this store, any of the object
    /// IDs do not refer to a `Package` object, a cycle or structural inconsistency is detected in
    /// the reference graph, or an I/O error occurred.
    pub fn compute_delta<R>(&self, pkgs: BTreeSet<ObjectId>, dest: &R) -> anyhow::Result<Closure>
    where
        R: Remote + ?Sized,
    {
        // This delta computation technique was shamelessly stolen from Git, as documented
        // meticulously in these two pages:
        //
        // https://matthew-brett.github.io/curious-git/git_push_algorithm.html
        // https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt

        let missing_pkgs = compute_delta_closure(pkgs, |id| {
            let p = self.get_package(id)?;
            if dest.contains_object(&id, Some(ObjectKind::Package))? {
                Ok(Include::No)
            } else {
                Ok(Include::Yes(p.references))
            }
        })?;

        let mut trees = BTreeSet::new();
        for id in &missing_pkgs {
            let p = self.get_package(*id)?;
            trees.insert(Ref(p.tree, ObjectKind::Tree));
        }

        let missing_content = compute_delta_closure(trees, |Ref(id, kind)| match kind {
            ObjectKind::Blob | ObjectKind::Tree if dest.contains_object(&id, Some(kind))? => {
                Ok(Include::No)
            }
            ObjectKind::Blob => Ok(Include::Yes(BTreeSet::new())),
            ObjectKind::Tree => {
                let tree = self.get_tree(id)?;
                let refs = tree.references();
                Ok(Include::Yes(refs.map(|(id, k)| Ref(id, k)).collect()))
            }
            ObjectKind::Package => Err(anyhow!("tree object cannot reference package object")),
            ObjectKind::Spec => unimplemented!(),
        })?;

        Ok(missing_pkgs
            .into_iter()
            .map(|id| Ref(id, ObjectKind::Package))
            .chain(missing_content)
            .map(|Ref(id, kind)| (id, kind))
            .collect())
    }

    /// Iterates over the closure and lazily yields each element in reverse topological order.
    ///
    /// This ordering is important because it ensures objects and packages can be inserted into
    /// stores in a consistent order, where all references are inserted before their referrers.
    pub fn yield_closure(&self, mut closure: Closure) -> Objects<'_> {
        Box::new(std::iter::from_fn(move || {
            if let Some((id, kind)) = closure.next() {
                Some(self.get_object(id, Some(kind)))
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
        let objects = self.yield_closure(delta);
        dest.upload_objects(objects)?;
        Ok(())
    }
}

impl<B: Backend> Remote for Store<B> {
    #[inline]
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool> {
        self.contains_object(id, kind)
    }

    #[inline]
    fn download_objects(&self, closure: Closure) -> anyhow::Result<Objects<'_>> {
        Ok(self.yield_closure(closure))
    }

    fn upload_objects(&mut self, objects: Objects) -> anyhow::Result<()> {
        for result in objects {
            let obj = result?;
            self.insert_object(obj)?;
        }

        Ok(())
    }
}

/// Newtype used only when computing closures.
///
/// This only exists because Rust disallows deriving/implementing traits for tuples.
#[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
struct Ref(ObjectId, ObjectKind);

impl Display for Ref {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        Display::fmt(&self.0, f)
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
