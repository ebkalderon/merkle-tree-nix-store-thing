//! Types and helper functions for computing closures.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{self, Display, Formatter};

use anyhow::anyhow;

use crate::{ObjectId, ObjectKind, Objects};

type Node = (ObjectId, ObjectKind);

/// A filesystem closure for one or more packages.
///
/// Closures describe the complete reference graph for a package or set of packages. References
/// might include individual files (blobs), directory trees, and other packages that the root
/// requires at run-time or at build-time.
#[derive(Clone, Debug)]
pub struct Closure {
    nodes: BTreeMap<Node, BTreeSet<Node>>,
}

impl Closure {
    /// Returns the number of elements in the closure.
    #[inline]
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Iterates over each element in the closure.
    pub fn iter(&self) -> impl Iterator<Item = &Node> + '_ {
        self.nodes.keys()
    }

    /// Returns a list of graph nodes sorted in topological order.
    pub fn sort_topological(&self) -> Vec<Node> {
        let mut sorted = Vec::new();
        let mut visited = BTreeSet::new();

        for node in self.nodes.keys() {
            self.sort_inner_visit(node, &mut sorted, &mut visited);
        }

        sorted
    }

    fn sort_inner_visit(&self, node: &Node, dst: &mut Vec<Node>, visited: &mut BTreeSet<Node>) {
        if !visited.insert(*node) {
            return;
        }

        for child in &self.nodes[node] {
            self.sort_inner_visit(child, dst, visited);
        }

        dst.push(*node);
    }

    /// Returns a list of graph nodes sorted in closure yield order.
    ///
    /// The elements are sorted in topological order and partitioned into three groups:
    ///
    /// 1. [`Spec`](crate::Spec)
    /// 2. [`Blob`](crate::Blob) and [`Tree`](crate::Tree) objects
    /// 3. [`Package`](crate::Package) objects
    ///
    /// This ordering is crucial because it ensures that a closure can be inserted into the store
    /// in a consistent order, where all references are inserted into the store before their
    /// referrers.
    pub fn sort_yield(&self) -> Vec<Node> {
        let mut pkgs = Vec::new();
        let mut content = Vec::new();
        let mut specs = Vec::new();

        for (id, kind) in self.sort_topological() {
            match kind {
                ObjectKind::Package => pkgs.push((id, kind)),
                ObjectKind::Spec => specs.push((id, kind)),
                _ => content.push((id, kind)),
            }
        }

        specs.into_iter().chain(content).chain(pkgs).collect()
    }

    /// Returns an object that implements [`Display`](std::fmt::Display) which renders the closure
    /// as a Graphviz DOT diagram.
    ///
    /// If `show_content` is `true`, then content objects like [`Blob`](crate::Blob) and
    /// [`Tree`](crate::Tree) will be included in the diagram as well. Otherwise, they are omitted
    /// from the diagram for the sake of reducing noise.
    #[inline]
    pub fn render_dot(&self, show_content: bool) -> DotDiagram<'_> {
        DotDiagram {
            inner: self,
            show_content,
        }
    }
}

#[derive(Debug)]
pub struct DotDiagram<'a> {
    inner: &'a Closure,
    show_content: bool,
}

impl<'a> Display for DotDiagram<'a> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        writeln!(f, "digraph G {{")?;

        let mut done = HashSet::new();
        for (node, children) in &self.inner.nodes {
            if !done.insert(node) {
                continue;
            }

            let (id, kind) = node;
            match kind {
                ObjectKind::Blob | ObjectKind::Tree if !self.show_content => continue,
                _ => writeln!(f, r#"  "{}" [shape = box]"#, id)?,
            }

            for (child_id, kind) in children {
                match kind {
                    ObjectKind::Blob | ObjectKind::Tree if !self.show_content => continue,
                    _ if child_id != id => writeln!(f, r#"  "{}" -> "{}""#, child_id, id)?,
                    _ => {}
                }
            }
        }

        write!(f, "}}")
    }
}

/// Compute the filesystem closure for the given package set.
///
/// The `filter` closure is used to determine whether the given object should be included in the
/// closure. Given an object, the closure must return `Ok(true)` or `Ok(false)`, with `Err` being
/// reserved for I/O errors. The returned `Closure` will only contain objects for which the closure
/// returns `Ok(true)`.
pub fn compute<O, F>(obj: &O, roots: BTreeSet<ObjectId>, mut filter: F) -> anyhow::Result<Closure>
where
    O: Objects,
    F: FnMut(ObjectId, ObjectKind) -> anyhow::Result<bool>,
{
    struct State<'a> {
        obj: &'a dyn Objects,
        filter: &'a mut dyn FnMut(ObjectId, ObjectKind) -> anyhow::Result<bool>,
        visited: HashSet<Node>,
        parents: HashSet<Node>,
    }

    fn visit(
        state: &mut State,
        nodes: &mut BTreeMap<Node, BTreeSet<Node>>,
        item: Node,
        parent_item: Option<Node>,
    ) -> anyhow::Result<()> {
        // Reference cycles are forbidden, so exit early if one is found.
        if state.parents.contains(&item) {
            return Err(anyhow!(
                "detected cycle in closure reference graph: {} -> {}",
                item.0,
                parent_item.unwrap().0
            ));
        }

        // Return early if we have already visited this node before.
        if !state.visited.insert(item) {
            return Ok(());
        }

        // Decide whether to continue the DFS or abandon it in favor of the next item.
        let (id, kind) = item;
        let children: Vec<_> = if (state.filter)(id, kind)? {
            nodes.entry(item).or_default();
            match kind {
                ObjectKind::Blob => Vec::new(),
                ObjectKind::Tree => {
                    let tree = state.obj.get_tree(id)?;
                    tree.references().collect()
                }
                ObjectKind::Package => {
                    let pkg = state.obj.get_package(id)?;
                    pkg.references
                        .into_iter()
                        .map(|id| (id, kind))
                        .chain(std::iter::once((pkg.tree, ObjectKind::Tree)))
                        .collect()
                }
                ObjectKind::Spec => {
                    let spec = state.obj.get_spec(id)?;
                    spec.dependencies
                        .into_iter()
                        .chain(spec.build_dependencies)
                        .map(|id| (id, kind))
                        .collect()
                }
            }
        } else {
            return Ok(());
        };

        // Mark this node as a parent, to detect cycles.
        state.parents.insert(item);

        // Continue descending into the child nodes in a DFS, if any exist.
        for child in children {
            visit(state, nodes, child, Some(item))?;
        }

        // All children of this node have been handled, so it's safe to move on.
        state.parents.remove(&item);

        // Insert edge connecting `parent_item` to `item`.
        if let Some(parent) = parent_item {
            nodes.entry(parent).or_default().insert(item);
        }

        Ok(())
    }

    let mut nodes = BTreeMap::new();
    let mut state = State {
        obj,
        filter: &mut filter,
        visited: HashSet::new(),
        parents: HashSet::new(),
    };

    for root in roots.iter().map(|&id| (id, ObjectKind::Package)) {
        visit(&mut state, &mut nodes, root, None)?;
    }

    Ok(Closure { nodes })
}
