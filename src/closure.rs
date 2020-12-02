//! Types and helper functions for computing closures.

use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;
use std::iter::FromIterator;

use anyhow::anyhow;

use crate::remote::Remote;
use crate::{Backend, ObjectId, ObjectKind, Objects, Store};

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

/// Compute the filesystem closure for the given package set.
pub fn compute<B: Backend>(store: &Store<B>, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Closure> {
    let refs = pkgs
        .into_iter()
        .map(|id| (id, ObjectKind::Package))
        .collect();

    let closure = topo_sort(refs, |(id, kind)| match kind {
        ObjectKind::Blob => Ok(BTreeSet::new()),
        ObjectKind::Tree => {
            let tree = store.objects.get_tree(id)?;
            Ok(tree.references().collect())
        }
        ObjectKind::Package => {
            let p = store.objects.get_package(id)?;
            let tree_ref = (p.tree, ObjectKind::Tree);
            Ok(p.references
                .into_iter()
                .map(|id| (id, ObjectKind::Package))
                .chain(std::iter::once(tree_ref))
                .collect())
        }
        ObjectKind::Spec => unimplemented!(),
    })?;

    Ok(Closure(closure))
}

/// Resolve the delta closure for the given package set between `src` and `dst`.
pub fn delta<B, R>(src: &Store<B>, dst: &R, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Closure>
where
    B: Backend,
    R: Remote + ?Sized,
{
    // This delta computation technique was shamelessly stolen from Git, as documented
    // meticulously in these two pages:
    //
    // https://matthew-brett.github.io/curious-git/git_push_algorithm.html
    // https://github.com/git/git/blob/master/Documentation/technical/pack-protocol.txt

    let refs = pkgs
        .into_iter()
        .map(|id| (id, ObjectKind::Package))
        .collect();

    let missing_pkgs = topo_sort_partial(refs, |(id, kind)| {
        let p = src.objects.get_package(id)?;
        if dst.contains_object(&id, Some(kind))? {
            Ok(Include::No)
        } else {
            let refs = p.references.into_iter().map(|id| (id, kind)).collect();
            Ok(Include::Yes(refs))
        }
    })?;

    let mut trees = BTreeSet::new();
    for (id, _) in &missing_pkgs {
        let p = src.objects.get_package(*id)?;
        trees.insert((p.tree, ObjectKind::Tree));
    }

    let missing_content = topo_sort_partial(trees, |(id, kind)| match kind {
        ObjectKind::Blob | ObjectKind::Tree if dst.contains_object(&id, Some(kind))? => {
            Ok(Include::No)
        }
        ObjectKind::Blob => Ok(Include::Yes(BTreeSet::new())),
        ObjectKind::Tree => {
            let tree = src.objects.get_tree(id)?;
            Ok(Include::Yes(tree.references().collect()))
        }
        ObjectKind::Package => Err(anyhow!("tree object cannot reference package object")),
        ObjectKind::Spec => unimplemented!(),
    })?;

    Ok(missing_pkgs.into_iter().chain(missing_content).collect())
}

/// A node in the reference graph.
type Node = (ObjectId, ObjectKind);

/// Control flow for the `get_children` closure.
enum Include {
    /// Indicates that the node should be included in the closure and descend into its child nodes.
    Yes(BTreeSet<Node>),
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
fn topo_sort<F>(items: BTreeSet<Node>, mut get_children: F) -> anyhow::Result<Vec<Node>>
where
    F: FnMut(Node) -> anyhow::Result<BTreeSet<Node>>,
{
    topo_sort_partial(items, |item| get_children(item).map(Include::Yes))
}

/// Similar to `topo_sort()`, but with the option to skip nodes and return a partial result.
///
/// If `get_children` returns `Ok(Include::Yes(children))`, it indicates that we should
/// include the current node in the closure and descend further in the child nodes. However, if
/// `get_children` returns `Ok(Include::No)`, it means that we should not include the
/// current node in the closure and not attempt to descend any further.
fn topo_sort_partial<F>(items: BTreeSet<Node>, get_children: F) -> anyhow::Result<Vec<Node>>
where
    F: FnMut(Node) -> anyhow::Result<Include>,
{
    // Use a struct with fields and methods because recursive closures are impossible in Rust.
    struct ClosureBuilder<'a, F> {
        initial_items: &'a BTreeSet<Node>,
        get_children: F,
        visited: HashSet<Node>,
        parents: HashSet<Node>,
        topo_sorted_items: Vec<Node>,
    }

    impl<'a, F> ClosureBuilder<'a, F>
    where
        F: FnMut(Node) -> anyhow::Result<Include>,
    {
        pub fn new(initial_items: &'a BTreeSet<Node>, get_children: F) -> Self {
            ClosureBuilder {
                initial_items,
                get_children,
                visited: HashSet::new(),
                parents: HashSet::new(),
                topo_sorted_items: Vec::new(),
            }
        }

        pub fn compute(mut self) -> anyhow::Result<Vec<Node>> {
            for item in self.initial_items {
                self.visit_dfs(*item, None)?;
            }

            // The final list of object IDs is sorted Q -> P, instead of P -> Q; we must fix this.
            self.topo_sorted_items.reverse();
            Ok(self.topo_sorted_items)
        }

        fn visit_dfs(&mut self, item: Node, parent_item: Option<Node>) -> anyhow::Result<()> {
            // Reference cycles are forbidden, so exit early if one is found.
            if self.parents.contains(&item) {
                return Err(anyhow!(
                    "detected cycle in closure reference graph: {} -> {}",
                    item.0,
                    parent_item.unwrap().0
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
