use std::collections::{BTreeMap, BTreeSet};

use foo::{Blob, Entry, LocalStore, Object, Package, Platform, Tree};

fn main() -> anyhow::Result<()> {
    // let mut store = Store::in_memory();
    let mut store: LocalStore = LocalStore::init("./store")?;

    let txt_id = store.insert_object(Object::Blob(
        Blob::from_bytes(b"foobarbaz".to_vec(), false).0,
    ))?;
    let rs_id = store.insert_object(Object::Blob(
        Blob::from_bytes(b"fn main() {}".to_vec(), false).0,
    ))?;
    let sh_id = store.insert_object(Object::Blob(
        Blob::from_bytes(b"echo \"hi\"".to_vec(), true).0,
    ))?;

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
        name: "foo".parse().unwrap(),
        system: Platform::host(),
        references: BTreeSet::new(),
        self_references: BTreeMap::new(),
        tree: main_tree_id,
    }))?;

    let pkg_id2 = store.insert_object(Object::Package({
        let mut references = BTreeSet::new();
        references.insert(pkg_id);
        Package {
            name: "bar".parse().unwrap(),
            system: Platform::host(),
            references,
            self_references: BTreeMap::new(),
            tree: similar_tree_id,
        }
    }))?;

    let mut pkgs = BTreeSet::new();
    pkgs.insert(pkg_id);
    pkgs.insert(pkg_id2);

    println!(
        "closure for 'foo' and 'bar': {:?}",
        store.compute_closure(pkgs.clone())?
    );

    let mut store2: LocalStore = LocalStore::init("./store2")?;

    println!("copying delta from store -> store2");
    let info = foo::copy_closure(&store, &mut store2, pkgs)?;
    println!("{:?}", info);

    Ok(())
}
