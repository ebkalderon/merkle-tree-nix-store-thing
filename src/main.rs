use std::collections::{BTreeMap, BTreeSet};

use foo::{Blob, Entry, Object, Package, Platform, Store, Tree};

fn main() -> anyhow::Result<()> {
    // let mut store = Store::in_memory();
    let mut store = Store::init("./store")?;

    let txt_id = store.insert_object(Object::Blob(Blob::from_reader(
        std::io::Cursor::new(b"foobarbaz".to_vec()),
        false,
    )?))?;
    let rs_id = store.insert_object(Object::Blob(Blob::from_bytes(
        b"fn main() {}".to_vec(),
        false,
    )))?;
    let sh_id = store.insert_object(Object::Blob(Blob::from_bytes(
        b"echo \"hi\"".to_vec(),
        true,
    )))?;

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
        system: Platform::host(),
        references: BTreeSet::new(),
        tree: main_tree_id,
    }))?;

    let pkg_id2 = store.insert_object(Object::Package({
        let mut references = BTreeSet::new();
        references.insert(pkg_id);
        Package {
            name: "bar".into(),
            system: Platform::host(),
            references,
            tree: similar_tree_id,
        }
    }))?;

    println!("program 'foo': {:?}", store.get_package(pkg_id)?);
    println!("program 'bar': {:?}", store.get_package(pkg_id2)?);

    let mut pkgs = BTreeSet::new();
    pkgs.insert(pkg_id);
    pkgs.insert(pkg_id2);

    println!(
        "closure for 'foo' and 'bar': {:?}",
        store.compute_closure(pkgs.clone())?
    );

    let mut store2 = Store::init("./store2")?;
    println!(
        "delta closure between store and store2: {:?}",
        store.compute_delta(pkgs.clone(), &store2)?
    );

    println!("copying delta from store -> store2");
    store.copy_closure(pkgs, &mut store2)?;

    Ok(())
}
