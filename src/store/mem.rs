//! In-memory store implementation intended for testing.

use std::collections::BTreeMap;
use std::io::Read;

use anyhow::anyhow;

use super::{Backend, Iter};
use crate::object::{Blob, ContentAddressable, Object, ObjectId, ObjectKind, Package, Spec, Tree};

/// Private replacement for `Object` which implements `Clone`.
///
/// `Object` does not implement `Clone` because `Blob` contains non-cloneable fields. The method
/// signature of `Store::get_object()` returns an owned `Object` value, so we define this cloneable
/// type convertible to and from `Object` to compensate.
#[derive(Clone, Debug)]
enum Inline {
    Blob {
        stream: Vec<u8>,
        is_executable: bool,
        object_id: ObjectId,
    },
    Tree(Tree),
    Package(Package),
    Spec(Spec),
}

impl Inline {
    fn from_object(o: Object) -> anyhow::Result<Self> {
        match o {
            Object::Blob(mut b) => {
                let mut stream = Vec::with_capacity(b.len() as usize);
                b.read_to_end(&mut stream)?;
                Ok(Inline::Blob {
                    stream,
                    is_executable: b.is_executable(),
                    object_id: b.object_id(),
                })
            }
            Object::Tree(t) => Ok(Inline::Tree(t)),
            Object::Package(p) => Ok(Inline::Package(p)),
            Object::Spec(s) => Ok(Inline::Spec(s)),
        }
    }

    fn kind(&self) -> ObjectKind {
        match *self {
            Inline::Blob { .. } => ObjectKind::Blob,
            Inline::Tree(_) => ObjectKind::Tree,
            Inline::Package(_) => ObjectKind::Package,
            Inline::Spec(_) => ObjectKind::Spec,
        }
    }
}

impl From<Inline> for Object {
    fn from(o: Inline) -> Self {
        match o {
            Inline::Blob {
                stream,
                is_executable,
                object_id,
            } => Object::Blob(Blob::from_bytes_unchecked(stream, is_executable, object_id)),
            Inline::Tree(t) => Object::Tree(t),
            Inline::Package(p) => Object::Package(p),
            Inline::Spec(s) => Object::Spec(s),
        }
    }
}

/// A store implementation kept in memory, useful for testing.
#[derive(Debug, Default)]
pub struct Memory {
    objects: BTreeMap<ObjectId, Inline>,
}

impl Backend for Memory {
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

    fn iter_objects(&self) -> anyhow::Result<Iter<'_>> {
        Ok(Box::new(
            self.objects.iter().map(|(&k, v)| (k, v.kind())).map(Ok),
        ))
    }

    fn contains_object(&self, id: &ObjectId, _: Option<ObjectKind>) -> anyhow::Result<bool> {
        Ok(self.objects.contains_key(id))
    }
}
