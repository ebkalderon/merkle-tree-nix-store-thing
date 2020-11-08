//! Byte stream chunker for efficient network transmission.

use std::io::{self, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::mpsc;
use futures::Stream;
use tokio::io::AsyncWrite;

const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Writer which breaks incoming bytes into an async stream of fixed-sized chunks.
#[derive(Debug)]
pub struct Chunker<T: From<Vec<u8>> + Send + Sync + 'static> {
    buf: Vec<u8>,
    sender: mpsc::UnboundedSender<T>,
}

impl<T> Chunker<T>
where
    T: From<Vec<u8>> + Send + Sync + 'static,
{
    /// Creates a new `Chunker` with a default maximum chunk size of 64 KB and returns the outgoing
    /// byte stream along with it.
    #[inline]
    pub fn new() -> (Self, Pin<Box<dyn Stream<Item = T> + Send + Sync>>) {
        Chunker::with_max_size(DEFAULT_CHUNK_SIZE)
    }

    /// Creates a new `Chunker` with maximum chunk size `cap` and returns the outgoing byte stream
    /// along with it.
    pub fn with_max_size(cap: usize) -> (Self, Pin<Box<dyn Stream<Item = T> + Send + Sync>>) {
        debug_assert!(cap > 0);

        let (tx, rx) = mpsc::unbounded();
        let service = Chunker {
            buf: Vec::with_capacity(cap),
            sender: tx,
        };

        (service, Box::pin(rx))
    }
}

impl<T> Write for Chunker<T>
where
    T: From<Vec<u8>> + Send + Sync + 'static,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let remaining = self.buf.capacity() - self.buf.len();
        let is_full = remaining <= buf.len();
        let bytes = if is_full { remaining } else { buf.len() };
        self.buf.extend_from_slice(&buf[..bytes]);

        if is_full {
            self.flush()?;
        }

        Ok(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            let cap = self.buf.capacity();
            let full_buf = std::mem::replace(&mut self.buf, Vec::with_capacity(cap));

            if let Err(_) = self.sender.unbounded_send(full_buf.into()) {
                // If this error is returned, no further writes will succeed either. Therefore,
                // it's acceptable to just drop the full_buf (now e.into_inner()) rather than put
                // it back as self.buf; it won't cause us to write a stream with a gap.
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "receiver was dropped",
                ));
            }
        }
        Ok(())
    }
}

impl<T> AsyncWrite for Chunker<T>
where
    T: From<Vec<u8>> + Send + Sync + 'static,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(self.write(buf))
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(self.flush())
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _: &mut Context) -> Poll<io::Result<()>> {
        Poll::Ready(self.flush())
    }
}

impl<T> Drop for Chunker<T>
where
    T: From<Vec<u8>> + Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.flush().ok();
    }
}
