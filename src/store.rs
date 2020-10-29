pub use self::fs::FsStore;
pub use self::mem::MemoryStore;

use std::collections::{BTreeSet, HashSet};
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;

use anyhow::anyhow;

use crate::object::{Blob, Object, ObjectId, ObjectKind, Package, Tree};

mod fs;
mod mem;

pub type Objects<'a> = Box<dyn Iterator<Item = anyhow::Result<(ObjectId, ObjectKind)>> + 'a>;

pub trait Store {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId>;
    fn get_object(&self, id: ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<Object>;
    fn iter_objects(&self) -> anyhow::Result<Objects<'_>>;
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

    fn get_blob(&self, id: ObjectId) -> anyhow::Result<Blob> {
        self.get_object(id, Some(ObjectKind::Blob)).and_then(|o| {
            o.into_blob()
                .map_err(|_| anyhow!("{} is not a blob object", id))
        })
    }

    fn get_tree(&self, id: ObjectId) -> anyhow::Result<Tree> {
        self.get_object(id, Some(ObjectKind::Tree)).and_then(|o| {
            o.into_tree()
                .map_err(|_| anyhow!("{} is not a tree object", id))
        })
    }

    fn get_package(&self, id: ObjectId) -> anyhow::Result<Package> {
        self.get_object(id, Some(ObjectKind::Package))
            .and_then(|o| {
                o.into_package()
                    .map_err(|_| anyhow!("{} is not a package object", id))
            })
    }

    fn closure_for(&self, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Vec<(ObjectId, ObjectKind)>> {
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

fn compute_closure<T, F>(items: BTreeSet<T>, get_children: F) -> anyhow::Result<Vec<T>>
where
    T: Copy + Display + Eq + Hash + Ord,
    F: FnMut(T) -> anyhow::Result<BTreeSet<T>>,
{
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

            self.topo_sorted_items.reverse();
            Ok(self.topo_sorted_items)
        }

        fn visit_dfs(&mut self, item: T, parent_item: Option<T>) -> anyhow::Result<()> {
            if self.parents.contains(&item) {
                return Err(anyhow!(
                    "detected cycle in closure reference graph: {} -> {}",
                    item,
                    parent_item.unwrap()
                ));
            }

            if !self.visited.insert(item) {
                return Ok(());
            }

            self.parents.insert(item);
            for child in (self.get_children)(item)? {
                if child != item {
                    self.visit_dfs(child, Some(item))?;
                }
            }

            self.topo_sorted_items.push(item);
            self.parents.remove(&item);

            Ok(())
        }
    }

    ClosureBuilder::new(&items, get_children).compute()
}
