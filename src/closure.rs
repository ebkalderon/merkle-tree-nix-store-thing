//! Types and helper functions for computing closures.

use std::collections::{BTreeSet, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::iter::FromIterator;

use anyhow::anyhow;

use crate::{ObjectId, ObjectKind};

/// A filesystem closure for one or more packages.
///
/// Closures describe the complete reference graph for a package or set of packages. References
/// might include individual files (blobs), directory trees, and other packages that the root
/// requires at run-time or at build-time.
#[derive(Clone, Debug, Hash, PartialEq)]
pub struct Closure(Vec<(ObjectId, ObjectKind)>);

impl FromIterator<(ObjectId, ObjectKind)> for Closure {
    fn from_iter<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (ObjectId, ObjectKind)>,
    {
        Closure(iter.into_iter().collect())
    }
}

impl Iterator for Closure {
    type Item = (ObjectId, ObjectKind);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop()
    }
}

/// Control flow for the `get_children` closure.
pub enum Include<T> {
    /// Indicates that the node should be included in the closure and descend into its child nodes.
    Yes(BTreeSet<T>),
    /// Indicates that the node should not be included in the closure.
    No,
}

/// Performs a depth-first search of a directed acyclic graph (DAG), descending from the given root
/// nodes in `items` and retrieving their child nodes with the `get_children` lambda, and produces
/// a topologically sorted list of references, where if tree object `P` depends on `Q`, then `P` is
/// ordered before `Q` in the list.
///
/// This sorting is important because it ensures objects and packages get inserted into the store
/// in a consistent order, where all references are inserted into the store before their referrers.
pub fn compute_closure<T, F>(items: BTreeSet<T>, mut get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
{
    compute_delta_closure(items, |item| get_children(item).map(Include::Yes))
}

/// Similar to `compute_closure()`, but with the option to skip nodes and return a partial result.
///
/// If `get_children` returns `Ok(Include::Yes(children))`, it indicates that we should
/// include the current node in the closure and descend further in the child nodes. However, if
/// `get_children` returns `Ok(Include::No)`, it means that we should not include the
/// current node in the closure and not attempt to descend any further.
pub fn compute_delta_closure<T, F>(items: BTreeSet<T>, get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<Include<T>>,
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
        F: FnMut(T) -> anyhow::Result<Include<T>>,
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

            // Decide whether to continue the DFS or abandon it in favor of the next item.
            let children = match (self.get_children)(item)? {
                Include::Yes(children) => children,
                Include::No => return Ok(()),
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

    ClosureBuilder::new(&items, get_children).compute()
}
