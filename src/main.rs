use std::collections::{BTreeMap, BTreeSet};

use self::object::{Blob, Entry, Object, Package, Tree};
use self::store::Store;

mod object;
mod store;
mod util;

fn main() -> anyhow::Result<()> {
    // let mut store = store::MemoryStore::default();
    let mut store = store::FsStore::init("./store")?;

    let txt_id = store.insert_object(Object::Blob(Blob::from_reader(
        std::io::Cursor::new(b"foobarbaz".to_vec()),
        false,
    )?))?;
    let rs_id = store.insert_object(Object::Blob(Blob::from_vec(
        b"fn main() {}".to_vec(),
        false,
    )))?;
    let sh_id = store.insert_object(Object::Blob(Blob::from_vec(b"echo \"hi\"".to_vec(), true)))?;

    let sub_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert("main.rs".into(), Entry::Blob { id: rs_id });
        Tree { entries }
    }))?;

    let main_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert("foo.txt".into(), Entry::Blob { id: txt_id });
        entries.insert("bar.sh".into(), Entry::Blob { id: sh_id });
        entries.insert(
            "baz.rs".into(),
            Entry::Symlink {
                target: "./src/main.rs".into(),
            },
        );
        entries.insert("src".into(), Entry::Tree { id: sub_tree_id });
        Tree { entries }
    }))?;

    let similar_tree_id = store.insert_object(Object::Tree({
        let mut entries = BTreeMap::new();
        entries.insert("main.rs".into(), Entry::Blob { id: rs_id });
        Tree { entries }
    }))?;

    let pkg_id = store.insert_object(Object::Package(Package {
        name: "foo".into(),
        system: "x86_64-apple-darwin".into(),
        references: BTreeSet::new(),
        tree: main_tree_id,
    }))?;

    let pkg_id2 = store.insert_object(Object::Package({
        let mut references = BTreeSet::new();
        references.insert(pkg_id);
        Package {
            name: "bar".into(),
            system: "x86_64-apple-darwin".into(),
            references,
            tree: similar_tree_id,
        }
    }))?;

    println!("program 'foo': {:?}", store.get_package(pkg_id)?);
    println!("program 'bar': {:?}", store.get_package(pkg_id2)?);

    println!(
        "closure for 'foo' and 'bar': {:?}",
        store.closure_for({
            let mut pkgs = BTreeSet::new();
            pkgs.insert(pkg_id);
            pkgs.insert(pkg_id2);
            pkgs
        })?
    );

    Ok(())
}
