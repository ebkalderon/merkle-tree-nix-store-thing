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

/// A filesystem closure for one or more packages.
///
/// Closures describe the complete reference graph for a package or set of packages. References
/// might include individual files (blobs), directory trees, and other packages that the root
/// requires at run-time or at build-time. Closures are represented as a flat topologically-sorted
/// list of unique object IDs to enable efficient delta calculation between any two individual
/// stores.
pub type Closure = Vec<(ObjectId, ObjectKind)>;

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
    /// Returns `Err` if any of the given object IDs do not exist, any of the object IDs do not
    /// refer to a `Package` object, a cycle or structural inconsistency is detected in the
    /// reference graph, or an I/O error occurred.
    fn compute_closure(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Closure> {
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

    /// Computes a delta closure which only contains objects that are missing on the remote store.
    ///
    /// Returns `Err` if any of the given object IDs do not exist in this store, any of the object
    /// IDs do not refer to a `Package` object, a cycle or structural inconsistency is detected in
    /// the reference graph, or an I/O error occurred.
    fn compute_delta(&self, pkgs: BTreeSet<ObjectId>, dest: &dyn Store) -> anyhow::Result<Closure> {
        // This delta computation technique is shamelessly stolen from Git, as documented
        // meticulously in these two pages:
        //
        // https://matthew-brett.github.io/curious-git/git_push_algorithm.html
        // https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt

        // Use newtype because Rust disallows deriving/implementing these traits for tuples.
        #[derive(Clone, Copy, Eq, Hash, PartialEq, PartialOrd, Ord)]
        struct Ref(ObjectId, ObjectKind);

        impl Display for Ref {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                Display::fmt(&self.0, f)
            }
        }

        let missing_pkgs = compute_delta_closure(pkgs, |id| {
            let p = self.get_package(id)?;
            if dest.contains_object(&id, Some(ObjectKind::Package))? {
                Ok(Path::Abandon)
            } else {
                Ok(Path::Descend(p.references))
            }
        })?;

        let mut trees = BTreeSet::new();
        for id in &missing_pkgs {
            let p = self.get_package(*id)?;
            trees.insert(Ref(p.tree, ObjectKind::Tree));
        }

        let missing_content = compute_delta_closure(trees, |Ref(id, kind)| match kind {
            ObjectKind::Blob | ObjectKind::Tree if dest.contains_object(&id, Some(kind))? => {
                Ok(Path::Abandon)
            }
            ObjectKind::Blob => Ok(Path::Descend(BTreeSet::new())),
            ObjectKind::Tree => {
                let tree = self.get_tree(id)?;
                let refs = tree.references();
                Ok(Path::Descend(refs.map(|(id, k)| Ref(id, k)).collect()))
            }
            ObjectKind::Package => Err(anyhow!("tree object cannot reference package object")),
        })?;

        Ok(missing_pkgs
            .into_iter()
            .map(|id| Ref(id, ObjectKind::Package))
            .chain(missing_content)
            .map(|Ref(id, kind)| (id, kind))
            .collect())
    }
}

/// Control flow for the `get_children` closure.
enum Path<T> {
    /// Indicates that traversal should continue with the given child nodes.
    Descend(BTreeSet<T>),
    /// Indicates that traversal should halt and return a partial result.
    Abandon,
}

/// Performs a depth-first search of a directed acyclic graph (DAG), descending from the given root
/// nodes in `items` and retrieving their child nodes with the `get_children` lambda, and produces
/// a topologically sorted list of references, where if tree object `P` depends on `Q`, then `P` is
/// ordered before `Q` in the list.
///
/// This sorting is important because it ensures objects and packages get inserted into the store
/// in a consistent order, where all references are inserted into the store before their referrers.
fn compute_closure<T, F>(items: BTreeSet<T>, mut get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
{
    compute_delta_closure(items, |item| get_children(item).map(Path::Descend))
}

/// Similar to `compute_closure()`, but with the option to halt early and return a partial result.
///
/// If `get_children` returns `Ok(Path::Descend(children))`, it indicates that we should descend
/// further in the child nodes in a regular depth-first search. However, if `get_children`  returns
/// `Ok(Path::Abandon)`, it means that we should stop descending any further and try again from a
/// different root node, if any, returning only a partial result.
fn compute_delta_closure<T, F>(items: BTreeSet<T>, get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<Path<T>>,
{
    #[derive(PartialEq)]
    enum Traversal {
        Continue,
        Halt,
    }

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
        F: FnMut(T) -> anyhow::Result<Path<T>>,
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

        fn visit_dfs(&mut self, item: T, parent_item: Option<T>) -> anyhow::Result<Traversal> {
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
                return Ok(Traversal::Continue);
            }

            // Decide whether to continue the DFS or abandon it in favor of the next initial item.
            let children = match (self.get_children)(item)? {
                Path::Descend(children) => children,
                Path::Abandon => return Ok(Traversal::Halt),
            };

            // Mark this node as a parent, to detect cycles.
            self.parents.insert(item);

            // Continue descending into the child nodes in a DFS, if any exist.
            for child in children {
                if self.visit_dfs(child, Some(item))? == Traversal::Halt {
                    self.parents.remove(&item);
                    return Ok(Traversal::Halt);
                }
            }

            // All children of this node have been handled, so it's safe to move on.
            self.topo_sorted_items.push(item);
            self.parents.remove(&item);

            Ok(Traversal::Continue)
        }
    }

    ClosureBuilder::new(&items, get_children).compute()
}
