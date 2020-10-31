//! Store interface and provided implementations.

pub use self::fs::FsStore;
pub use self::mem::MemoryStore;

use std::collections::{BTreeSet, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;

use anyhow::anyhow;

use crate::object::{Blob, Object, ObjectId, ObjectKind, Package, Tree};

mod fs;
mod mem;

/// An iterator of tree objects in a store.
///
/// The order in which this iterator returns entries is platform and filesystem dependent.
pub type Objects<'a> = Box<dyn Iterator<Item = anyhow::Result<(ObjectId, ObjectKind)>> + 'a>;

/// A content-addressable store of installed software packages.
pub trait Store {
    /// Inserts a tree object into the store, returning its unique ID.
    ///
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

    /// Returns an iterator over all tree objects in this store.
    ///
    /// The order in which this iterator returns entries is platform and filesystem dependent.
    ///
    /// Returns `Err` if the store is corrupt or an I/O error occurred.
    fn iter_objects(&self) -> anyhow::Result<Objects<'_>>;

    /// Returns `Ok(true)` if the store contains a tree object with the given unique ID, or
    /// `Ok(false)` otherwise.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// store will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if the store is corrupt or an I/O error occurred.
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

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

    /// Computes the filesystem closure for the given packages.
    ///
    /// Closures describe the complete reference graph for a package or set of packages. References
    /// might include individual files (blobs), directory trees, and other packages that the root
    /// requires at run-time or at build-time. Closures are represented as a flat
    /// topologically-sorted list of unique object IDs to enable efficient delta calculation
    /// between any two individual stores.
    ///
    /// Returns `Err` if any of the given object IDs do not exist, any of the object IDs do not
    /// refer to a `Package` object, a cycle or structural inconsistency is detected in the
    /// reference graph, or an I/O error occurred.
    fn closure_for(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Vec<(ObjectId, ObjectKind)>> {
        // Use newtype because Rust disallows deriving/implementing these traits for tuples.
        #[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
        struct Ref(ObjectId, ObjectKind);

        impl Display for Ref {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                Display::fmt(&self.0, f)
            }
        }

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
        })?;

        Ok(closure
            .into_iter()
            .map(|Ref(id, kind)| (id, kind))
            .collect())
    }
}

/// Performs a depth-first search of a directed acyclic graph (DAG), descending from the given root
/// nodes in `items` and retrieving their child nodes with the `get_children` lambda, and produces
/// a topologically sorted list of references, where if tree object `P` depends on `Q`, then `P` is
/// ordered before `Q` in the list.
///
/// This sorting is critical for quick and efficient delta computation between two different
/// stores, like when copying a closure over the network to a store on a remote machine, or when
/// querying remote machines for tree objects to download. This is shamelessly stolen from how Git
/// does delta computation, as documented meticulously in this file:
///
/// https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt
fn compute_closure<T, F>(items: BTreeSet<T>, get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
{
    // Use a struct with fields and methods because recursive closures are impossible in Rust.
    struct ClosureBuilder<'a, T: Eq + Hash + Ord, F> {
        initial_items: &'a BTreeSet<T>,
        get_children: F,
        visited: HashSet<T>,
        parents: HashSet<T>,
        topo_sorted_items: Vec<T>,
    }

    impl<'a, T, F> ClosureBuilder<'a, T, F>
    where
        T: Copy + Display + Eq + Hash + Ord,
        F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
    {
        pub fn new(initial_items: &'a BTreeSet<T>, get_children: F) -> Self {
            ClosureBuilder {
                initial_items,
                get_children,
                visited: HashSet::new(),
                parents: HashSet::new(),
                topo_sorted_items: Vec::new(),
            }
        }

        pub fn compute(mut self) -> anyhow::Result<Vec<T>> {
            for item in self.initial_items {
                self.visit_dfs(*item, None)?;
            }

            // The final list of object IDs is sorted Q -> P, instead of P -> Q; we must fix this.
            self.topo_sorted_items.reverse();
            Ok(self.topo_sorted_items)
        }

        fn visit_dfs(&mut self, item: T, parent_item: Option<T>) -> anyhow::Result<()> {
            // Reference cycles are forbidden, so exit early if one is found.
            if self.parents.contains(&item) {
                return Err(anyhow!(
                    "detected cycle in closure reference graph: {} -> {}",
                    item,
                    parent_item.unwrap()
                ));
            }

            // Return early if we have already visited this node before.
            if !self.visited.insert(item) {
                return Ok(());
            }

            // Mark this node as a parent, to detect cycles.
            self.parents.insert(item);

            // Continue descending into the child nodes in a DFS, if any exist.
            for child in (self.get_children)(item)? {
                if child != item {
                    self.visit_dfs(child, Some(item))?;
                }
            }

            // All children of this node have been handled, so it's safe to move on.
            self.topo_sorted_items.push(item);
            self.parents.remove(&item);

            Ok(())
        }
    }

    ClosureBuilder::new(&items, get_children).compute()
}
