use std::collections::BTreeMap;
use std::io::Cursor;

use anyhow::anyhow;

use super::{Objects, Store};
use crate::object::{Blob, ContentAddressable, Object, ObjectId, ObjectKind, Package, Tree};

#[derive(Clone, Debug)]
enum InMemory {
    Blob {
        stream: Box<Cursor<Vec<u8>>>,
        is_executable: bool,
        object_id: ObjectId,
    },
    Tree(Tree),
    Package(Package),
}

impl InMemory {
    fn from_object(o: Object) -> anyhow::Result<Self> {
        match o {
            Object::Blob(mut b) => {
                let mut stream = Box::new(std::io::Cursor::new(Vec::new()));
                std::io::copy(&mut b.stream, &mut stream)?;
                Ok(InMemory::Blob {
                    stream,
                    is_executable: b.is_executable(),
                    object_id: b.object_id(),
                })
            }
            Object::Tree(t) => Ok(InMemory::Tree(t)),
            Object::Package(p) => Ok(InMemory::Package(p)),
        }
    }

    fn kind(&self) -> ObjectKind {
        match *self {
            InMemory::Blob { .. } => ObjectKind::Blob,
            InMemory::Tree(_) => ObjectKind::Tree,
            InMemory::Package(_) => ObjectKind::Package,
        }
    }
}

impl From<InMemory> for Object {
    fn from(o: InMemory) -> Self {
        match o {
            InMemory::Blob {
                stream,
                is_executable,
                object_id,
            } => Object::Blob(Blob {
                stream,
                is_executable,
                object_id,
            }),
            InMemory::Tree(t) => Object::Tree(t),
            InMemory::Package(p) => Self::Package(p),
        }
    }
}

#[derive(Debug, Default)]
pub struct InMemoryStore {
    objects: BTreeMap<ObjectId, InMemory>,
}

impl Store for InMemoryStore {
    fn insert_object(&mut self, o: Object) -> anyhow::Result<ObjectId> {
        use std::collections::btree_map::Entry;
        let id = o.object_id();
        match self.objects.entry(id) {
            Entry::Occupied(_) => Ok(id),
            Entry::Vacant(e) => {
                e.insert(InMemory::from_object(o)?);
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
