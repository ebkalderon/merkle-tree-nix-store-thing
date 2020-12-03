//! Remote interface and provided implementations.

use std::time::Duration;

use crate::{Closure, Object, ObjectId, ObjectKind};

mod chunker;

/// A streaming iterator of tree objects.
pub type ClosureStream<'a> = Box<dyn Iterator<Item = anyhow::Result<Object>> + 'a>;

/// A remote source from which objects can be fetched or uploaded.
pub trait Remote {
    /// Returns `Ok(true)` if the remote source contains a tree object with the given unique ID, or
    /// `Ok(false)` otherwise.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// remote will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if the connection to the remote was lost or an I/O error occurred.
    fn contains_object(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

    /// Requests to download a pack of objects built from the given closure.
    ///
    /// Returns `Err` if a requested object does not exist on the remote, the connection to the
    /// remote was lost, or an I/O error occurred.
    fn download_objects(&self, closure: Closure) -> anyhow::Result<ClosureStream<'_>>;

    /// Copies the stream of objects to the remote source.
    ///
    /// Returns `Err` if any element of `objects` is `Err`, the connection to the remote was lost,
    /// or an I/O error occurred.
    fn upload_objects(&mut self, stream: ClosureStream) -> anyhow::Result<()>;

    /// Sends a ping request to the remote source and measures the latency.
    fn ping(&self) -> anyhow::Result<Duration> {
        Ok(Duration::from_secs(0))
    }
}
