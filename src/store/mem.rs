//! In-memory store implementation intended for testing.

use std::collections::BTreeMap;
use std::io::Cursor;

use anyhow::anyhow;

use super::{Objects, Store};
use crate::object::{Blob, ContentAddressable, Object, ObjectId, ObjectKind, Package, Tree};

/// Private replacement for `Object` which implements `Clone`.
///
/// `Object` does not implement `Clone` because `Blob` contains non-cloneable fields. The method
/// signature of `Store::get_object()` returns an owned `Object` value, so we define this cloneable
/// type convertible to and from `Object` to compensate.
#[derive(Clone, Debug)]
enum Inline {
    Blob {
        stream: Box<Cursor<Vec<u8>>>,
        is_executable: bool,
        length: u64,
        object_id: ObjectId,
    },
    Tree(Tree),
    Package(Package),
}

impl Inline {
    fn from_object(o: Object) -> anyhow::Result<Self> {
        match o {
            Object::Blob(mut b) => {
                let mut stream = Box::new(std::io::Cursor::new(Vec::new()));
                let length = std::io::copy(&mut b, &mut stream)?;
                Ok(Inline::Blob {
                    stream,
                    is_executable: b.is_executable(),
                    length,
                    object_id: b.object_id(),
                })
            }
            Object::Tree(t) => Ok(Inline::Tree(t)),
            Object::Package(p) => Ok(Inline::Package(p)),
        }
    }

    fn kind(&self) -> ObjectKind {
        match *self {
            Inline::Blob { .. } => ObjectKind::Blob,
            Inline::Tree(_) => ObjectKind::Tree,
            Inline::Package(_) => ObjectKind::Package,
        }
    }
}

impl From<Inline> for Object {
    fn from(o: Inline) -> Self {
        match o {
            Inline::Blob {
                stream,
                is_executable,
                length,
                object_id,
            } => Object::Blob(Blob::from_reader_raw(
                stream,
                is_executable,
                length,
                object_id,
            )),
            Inline::Tree(t) => Object::Tree(t),
            Inline::Package(p) => Self::Package(p),
        }
    }
}

/// A store implementation kept in memory, useful for testing.
#[derive(Debug, Default)]
pub struct MemoryStore {
    objects: BTreeMap<ObjectId, Inline>,
}

impl Store for MemoryStore {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        use std::collections::btree_map::Entry;
        let id = o.object_id();
        match self.objects.entry(id) {
            Entry::Occupied(_) => Ok(id),
            Entry::Vacant(e) => {
                e.insert(Inline::from_object(o)?);
                Ok(id)
            }
        }
    }

    fn get_object(&self, id: ObjectId, _: Option<ObjectKind>) -> anyhow::Result<Object> {
        self.objects
            .get(&id)
            .cloned()
            .map(Object::from)
            .ok_or(anyhow!("object {} not found", id))
    }

    fn iter_objects(&self) -> anyhow::Result<Objects<'_>> {
        Ok(Box::new(
            self.objects.iter().map(|(&k, v)| (k, v.kind())).map(Ok),
        ))
    }

    fn contains_object(&self, id: &ObjectId, _: Option<ObjectKind>) -> anyhow::Result<bool> {
        Ok(self.objects.contains_key(id))
    }
}
