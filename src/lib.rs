//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::closure::Closure;
pub use self::object::*;
pub use self::store::Entries;

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::anyhow;

use crate::remote::{Objects, Remote};
use crate::store::{Backend, Filesystem, Memory};

pub mod remote;

mod closure;
mod object;
mod store;

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
        Ok(Store { backend })
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
        Ok(Store { backend })
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
        Ok(Store { backend })
    }
}

impl Store<Memory> {
    /// Constructs a new in-memory store. This is useful for testing.
    #[inline]
    pub fn in_memory() -> Self {
        Store {
            backend: Memory::default(),
        }
    }
}

impl<B: Backend> Store<B> {
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
}

impl<B: Backend> Store<B> {
    /// Installs an external directory in the store as a content-addressable package.
    ///
    /// Returns the ID of the installed package object.
    fn install_package(&mut self, pkg_name: &str, out_dir: &Path) -> anyhow::Result<ObjectId> {
        debug_assert!(!pkg_name.is_empty());
        debug_assert!(out_dir.is_dir());
        debug_assert!(out_dir.is_absolute());

        let tree_id = self.install_dir_tree(out_dir, out_dir)?;
        self.insert_object(Object::Package(Package {
            name: pkg_name.into(),
            system: Platform::host(),
            references: BTreeSet::new(), // TODO: Need to collect references.
            tree: tree_id,
        }))
    }

    /// Recursively inserts the contents of the given directory in the store as a tree object,
    /// patching out any self-references to `pkg_root` detected in blobs and symlinks by converting
    /// them to relative paths. This is to maintain the content addressable invariant of the store.
    ///
    /// Returns the ID of the installed tree object.
    fn install_dir_tree(&mut self, dir: &Path, pkg_root: &Path) -> anyhow::Result<ObjectId> {
        debug_assert!(dir.starts_with(pkg_root));

        let mut entries = BTreeMap::new();

        let entries_iter = std::fs::read_dir(dir)?;
        let mut children: Vec<_> = entries_iter.collect::<Result<_, _>>()?;
        children.sort_by_cached_key(|entry| entry.path());

        for child in children {
            let path = child.path();
            let file_name = path
                .file_name()
                .expect("path must have filename")
                .to_str()
                .ok_or_else(|| anyhow!("path {} contains invalid UTF-8", path.display()))?
                .to_owned();

            let file_type = child.file_type()?;
            if file_type.is_dir() {
                let id = self.install_dir_tree(&path, pkg_root)?;
                entries.insert(file_name, Entry::Tree { id });
            } else if file_type.is_file() {
                // TODO: Need to implement patching of blob self-references.
                // let id = patch_blob_self_refs(store, dir, root)?;
                // entries.insert(file_name, Entry::Blob { id });
            } else if file_type.is_symlink() {
                let target = path.read_link()?;
                let norm_target = target.canonicalize()?;

                let target = if norm_target.starts_with(pkg_root) {
                    pathdiff::diff_paths(norm_target, pkg_root).unwrap()
                } else {
                    target
                };

                entries.insert(file_name, Entry::Symlink { target });
            } else {
                unreachable!("entries can only be files, directories, or symlinks");
            }
        }

        let tree = Object::Tree(Tree { entries });
        self.insert_object(tree)
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
    #[inline]
    pub fn compute_delta<R>(&self, pkgs: BTreeSet<ObjectId>, dest: &R) -> anyhow::Result<Closure>
    where
        R: Remote + ?Sized,
    {
        closure::delta(self, dest, pkgs)
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
