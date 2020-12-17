//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::closure::Closure;
pub use self::copy::copy_closure;
pub use self::local::{Backend, Store};
pub use self::object::*;

use std::collections::BTreeSet;

use anyhow::anyhow;

pub mod copy;

mod closure;
mod local;
mod object;
mod util;

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

    /// Returns the on-disk size of the object in bytes, if it exists in the store.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// store will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if the object does not exist or an I/O error occurred.
    fn object_size(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<u64>;

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

    /// Computes the filesystem closure for the given packages.
    ///
    /// Returns `Err` if any of the given object IDs do not exist, any of the object IDs do not
    /// refer to a `Package` object, a cycle or structural inconsistency is detected in the
    /// reference graph, or an I/O error occurred.
    fn compute_closure(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Closure> {
        closure::compute(self, pkgs, |_id, _kind| Ok(true))
    }
}
