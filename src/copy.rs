//! Functions for copying packages between stores.

use std::collections::{BTreeMap, BTreeSet};

use crate::{Closure, Object, ObjectId, ObjectKind};

/// Copies `pkgs` and all their dependencies from `src` to `dest`.
///
/// This will resolve the delta closure between the source and the destination and only synchronize
/// objects that are missing on the destination.
///
/// If both `src` and `dst` are both remote hosts, the objects yielded by `src` will be routed
/// through this host before being uploaded to `dst`. This is done for security reasons, where the
/// credentials for both reside on the local machine. However, it can impose a performance penalty.
pub fn copy_closure<'s, S, D>(
    src: &'s S,
    dst: &mut D,
    pkgs: BTreeSet<ObjectId>,
) -> anyhow::Result<CopyInfo<D>>
where
    S: Source<'s> + ?Sized,
    D: Destination + ?Sized,
{
    let delta = src.find_missing(dst, pkgs)?;
    let total_objects = delta.missing.len() + delta.num_present;
    let missing = delta.missing.iter().map(|&(id, _)| (id, 0)).collect();
    let objects = src.yield_objects(delta.missing)?;
    Ok(CopyInfo {
        total_objects,
        total_bytes: 0, // TODO: Need to implement.
        missing,
        progress: dst.insert_objects(objects)?,
    })
}

/// A source repository to copy from.
pub trait Source<'s> {
    /// Stream of tree objects.
    type Objects: Iterator<Item = anyhow::Result<Object>> + 's;

    /// Computes a delta closure which only contains objects that are missing at the destination.
    ///
    /// Returns `Err` if any of the given object IDs do not exist in this store, any of the object
    /// IDs do not refer to a `Package` object, a cycle or structural inconsistency is detected in
    /// the reference graph, or an I/O error occurred.
    fn find_missing<D>(&self, dst: &D, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Delta>
    where
        D: Destination + ?Sized;

    /// Iterates over the closure and lazily yields each element in reverse topological order.
    ///
    /// This ordering is important because it ensures objects and packages can be inserted into
    /// stores in a consistent order, where all references are inserted before their referrers.
    ///
    /// Returns `Err` if any of the object IDs do not actually exist in this store, or an I/O error
    /// occurred.
    fn yield_objects(&'s self, closure: Closure) -> anyhow::Result<Self::Objects>;
}

/// A destination repository to copy to.
pub trait Destination {
    /// Stream of progress updates.
    type Progress: Iterator<Item = anyhow::Result<Progress>>;

    /// Returns `Ok(true)` if the repository contains a tree object with the given unique ID, or
    /// `Ok(false)` otherwise.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// repository will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if an I/O error occurred.
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

    /// Copies the stream of objects to the repository, returning a stream of progress updates.
    ///
    /// Returns `Err` if any element of `objects` is `Err`, or an I/O error occurred.
    fn insert_objects<I>(&mut self, stream: I) -> anyhow::Result<Self::Progress>
    where
        I: Iterator<Item = anyhow::Result<Object>>;
}

/// A partial closure describing the delta between two package stores.
#[derive(Debug)]
pub struct Delta {
    /// Number of objects already present on the destination.
    pub num_present: usize,
    /// Closure of objects known to be missing on the destination.
    pub missing: Closure,
}

/// Represents an ongoing copy operation.
///
/// This struct is created by [`copy_closure()`]. See its documentation for more.
#[derive(Debug)]
pub struct CopyInfo<D: Destination + ?Sized> {
    /// Total number of objects in the closure.
    pub total_objects: usize,
    /// Total size of the closure, in bytes.
    pub total_bytes: u64,
    /// Objects that are missing at the destination.
    pub missing: BTreeMap<ObjectId, u64>,
    /// Stream of progress updates.
    pub progress: D::Progress,
}

impl<D: Destination + ?Sized> CopyInfo<D> {
    /// Returns the number of objects being copied to the destination.
    #[inline]
    pub fn objects_to_copy(&self) -> usize {
        self.missing.len()
    }

    /// Returns the number of bytes being copied to the destination.
    #[inline]
    pub fn bytes_to_copy(&self) -> u64 {
        self.missing.values().sum()
    }
}

/// A progress update for an ongoing copy operation.
#[derive(Debug)]
pub struct Progress {
    /// The object ID being copied.
    pub id: ObjectId,
    /// Number of bytes copied so far.
    pub bytes_copied: u64,
}