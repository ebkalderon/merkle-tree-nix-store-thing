//! Types and helper functions for computing closures.

use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;

use anyhow::anyhow;

use crate::{ObjectId, ObjectKind, Objects};

/// A filesystem closure for one or more packages.
///
/// Closures describe the complete reference graph for a package or set of packages. References
/// might include individual files (blobs), directory trees, and other packages that the root
/// requires at run-time or at build-time.
///
/// Inside this closure is a topologically sorted list of Merkle tree objects, with packages sorted
/// first, followed by blobs and directory trees, and trailed by build specs.
///
/// This sorting is important because it ensures objects and packages get inserted into the store
/// in a consistent order, where all references are inserted into the store before their referrers.
#[derive(Debug)]
pub struct Closure(Vec<(ObjectId, ObjectKind)>);

impl Closure {
    /// Returns the number of elements in the closure.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl IntoIterator for Closure {
    type Item = (ObjectId, ObjectKind);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

/// Compute the filesystem closure for the given package set.
///
/// The `filter` closure is used to determine whether the given object should be included in the
/// closure. Given an object, the closure must return `Ok(true)` or `Ok(false)`, with `Err` being
/// reserved for I/O errors. The returned `Closure` will only contain objects for which the closure
/// returns `Ok(true)`.
pub fn compute<O, F>(obj: &O, pkgs: BTreeSet<ObjectId>, mut filter: F) -> anyhow::Result<Closure>
where
    O: Objects + ?Sized,
    F: FnMut(ObjectId, ObjectKind) -> anyhow::Result<bool>,
{
    let root_objects = pkgs
        .into_iter()
        .map(|id| (id, ObjectKind::Package))
        .collect();

    let closure = incremental_topo_sort(
        root_objects,
        |(id, kind)| match kind {
            ObjectKind::Blob => Ok(BTreeSet::new()),
            ObjectKind::Tree => {
                let tree = obj.get_tree(id)?;
                Ok(tree.references().collect())
            }
            ObjectKind::Package => {
                let pkg = obj.get_package(id)?;
                Ok(pkg
                    .references
                    .into_iter()
                    .map(|id| (id, kind))
                    .chain(std::iter::once((pkg.tree, ObjectKind::Tree)))
                    .collect())
            }
            ObjectKind::Spec => {
                let spec = obj.get_spec(id)?;
                Ok(spec
                    .dependencies
                    .into_iter()
                    .chain(spec.build_dependencies)
                    .map(|id| (id, kind))
                    .collect())
            }
        },
        |(id, kind)| filter(id, kind),
        |item, parent_item| {
            anyhow!(
                "detected cycle in closure reference graph: {} -> {}",
                item.0,
                parent_item.0
            )
        },
    )?;

    let ordered = {
        let mut pkgs = Vec::new();
        let mut content = Vec::new();
        let mut specs = Vec::new();

        for (id, kind) in closure {
            match kind {
                ObjectKind::Package => pkgs.push((id, kind)),
                ObjectKind::Spec => specs.push((id, kind)),
                _ => content.push((id, kind)),
            }
        }

        pkgs.into_iter().chain(content).chain(specs).collect()
    };

    Ok(Closure(ordered))
}

/// Returns a topologically sorted list of all nodes reachable from `initial_items`, where if tree
/// object `P` depends on `Q`, then `P` is ordered before `Q` in the list.
///
/// This function performs a depth-first search on a directed acyclic graph, descending from the
/// root nodes in `initial_items` and retrieving their child nodes with the `get_children` lambda.
///
/// The `filter` closure allows us to determine whether to abandon the ongoing depth-first search
/// in favor of the next item in the `initial_items` set.
fn incremental_topo_sort<T, F1, F2, F3>(
    initial_items: BTreeSet<T>,
    get_children: F1,
    filter: F2,
    cycle_error: F3,
) -> anyhow::Result<Vec<T>>
where
    T: Copy + Eq + Hash + Ord,
    F1: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
    F2: FnMut(T) -> anyhow::Result<bool>,
    F3: FnMut(T, T) -> anyhow::Error,
{
    // Use a struct with fields and methods because recursive closures are impossible in Rust.
    struct DfsSorter<'a, T, F1, F2, F3> {
        initial_items: &'a BTreeSet<T>,
        get_children: F1,
        filter: F2,
        cycle_error: F3,
        visited: HashSet<T>,
        parents: HashSet<T>,
        topo_sorted_items: Vec<T>,
    }

    impl<'a, T, F1, F2, F3> DfsSorter<'a, T, F1, F2, F3>
    where
        T: Copy + Eq + Hash + Ord,
        F1: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
        F2: FnMut(T) -> anyhow::Result<bool>,
        F3: FnMut(T, T) -> anyhow::Error,
    {
        pub fn new(items: &'a BTreeSet<T>, get_children: F1, filter: F2, cycle_error: F3) -> Self {
            DfsSorter {
                initial_items: items,
                get_children,
                filter,
                cycle_error,
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
                return Err((self.cycle_error)(item, parent_item.unwrap()));
            }

            // Return early if we have already visited this node before.
            if !self.visited.insert(item) {
                return Ok(());
            }

            // Decide whether to continue the DFS or abandon it in favor of the next item.
            let children = if (self.filter)(item)? {
                (self.get_children)(item)?
            } else {
                return Ok(());
            };

            // Mark this node as a parent, to detect cycles.
            self.parents.insert(item);

            // Continue descending into the child nodes in a DFS, if any exist.
            for child in children {
                self.visit_dfs(child, Some(item))?;
            }

            // All children of this node have been handled, so it's safe to move on.
            self.topo_sorted_items.push(item);
            self.parents.remove(&item);

            Ok(())
        }
    }

    DfsSorter::new(&initial_items, get_children, filter, cycle_error).compute()
}
