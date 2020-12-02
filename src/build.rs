//! Temporary place to put the WIP install_package() logic.
//!
//! TODO: Refactor or remove.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use anyhow::anyhow;

use super::{Backend, Entry, Object, ObjectId, Package, Platform, Store, Tree};

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
