//! Functions for copying packages between stores.

use std::collections::BTreeSet;

use async_trait::async_trait;
use futures::{try_join, StreamExt};
use tokio::fs::File;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::pack::{PackStream, Progress};
use crate::{Closure, ObjectId, ObjectKind};

/// Copies `pkgs` and all their dependencies from `src` to `dest`.
///
/// This will resolve the delta closure between the source and the destination and only synchronize
/// objects that are missing on the destination.
///
/// If both `src` and `dst` are both remote hosts, the objects yielded by `src` will be routed
/// through this host before being uploaded to `dst`. This is done for security reasons, where the
/// credentials for both reside on the local machine. However, it can impose a performance penalty.
pub async fn copy_closure<S, D, F>(
    src: &S,
    dst: &mut D,
    pkgs: BTreeSet<ObjectId>,
    mut progress: F,
) -> anyhow::Result<Delta>
where
    S: Source + ?Sized,
    D: Destination + ?Sized,
    F: FnMut(&Progress),
{
    let delta = src.find_missing(dst, pkgs)?;

    let (reader, mut writer) = async_pipe()?;
    let (mut reader, mut progress_rx) = PackStream::new(reader);

    let send = src.send_pack(delta.missing.clone(), &mut writer);
    let recv = dst.recv_pack(&mut reader);
    let progress = async move {
        while let Some(p) = progress_rx.next().await {
            progress(&p);
        }
        Ok(())
    };

    try_join!(send, recv, progress)?;

    Ok(delta)
}

/// A source repository to copy from.
#[async_trait(?Send)]
pub trait Source {
    /// Computes a delta closure which only contains objects that are missing at the destination.
    ///
    /// Returns `Err` if any of the given object IDs do not exist in this store, any of the object
    /// IDs do not refer to a `Package` object, a cycle or structural inconsistency is detected in
    /// the reference graph, or an I/O error occurred.
    fn find_missing<D>(&self, dst: &D, pkgs: BTreeSet<ObjectId>) -> anyhow::Result<Delta>
    where
        D: Destination + ?Sized;

    /// Writes the objects in the closure as a pack file and sends it over the `writer`.
    ///
    /// Elements _must_ be yielded in topological order for the pack to be considered valid. This
    /// ordering is important because it ensures objects and packages can be inserted into stores
    /// in a consistent order, where all references are inserted before their referrers.
    ///
    /// Returns `Err` if any of the object IDs do not actually exist in this store, or an I/O error
    /// occurred.
    async fn send_pack<W>(&self, closure: Closure, writer: &mut W) -> anyhow::Result<()>
    where
        W: AsyncWrite + Unpin;
}

/// A destination repository to copy to.
#[async_trait(?Send)]
pub trait Destination {
    /// Returns `Ok(true)` if the repository contains a tree object with the given unique ID, or
    /// `Ok(false)` otherwise.
    ///
    /// If the type of the requested object is known up-front, implementers _can_ use this detail
    /// to locate and retrieve the object faster. Otherwise, callers can specify `None` and the
    /// repository will attempt to guess the desired object type, if it is not immediately known.
    ///
    /// Returns `Err` if an I/O error occurred.
    fn contains(&self, id: &ObjectId, kind: Option<ObjectKind>) -> anyhow::Result<bool>;

    /// Copies the packfile stream from `reader` to the destination.
    ///
    /// Elements _must_ be yielded in topological order for the pack to be considered valid. This
    /// ordering is important because it ensures objects and packages can be inserted into stores
    /// in a consistent order, where all references are inserted before their referrers.
    ///
    /// Returns `Err` if the pack stream could not be decoded, the yielded objects were not sorted
    /// in topological order, or an I/O error occurred.
    async fn recv_pack<R>(&mut self, reader: &mut R) -> anyhow::Result<()>
    where
        R: AsyncRead + Unpin;
}

/// A partial closure describing the delta between two package stores.
///
/// This struct is created by [`copy_closure()`]. See its documentation for more.
#[derive(Debug)]
pub struct Delta {
    /// Number of objects already present on the destination.
    pub num_present: usize,
    /// Closure of objects known to be missing on the destination.
    pub missing: Closure,
}

fn async_pipe() -> std::io::Result<(File, File)> {
    use std::os::unix::io::{FromRawFd, IntoRawFd};

    let (reader, writer) = os_pipe::pipe()?;
    let reader = unsafe { File::from_raw_fd(reader.into_raw_fd()) };
    let writer = unsafe { File::from_raw_fd(writer.into_raw_fd()) };

    Ok((reader, writer))
}
