//! Hybrid memory and disk backed buffer type.

use std::fmt::{self, Debug, Formatter};
use std::fs::Permissions;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

use filetime::FileTime;

enum Storage {
    Inline(Cursor<Vec<u8>>),
    File(tempfile::NamedTempFile),
}

impl Debug for Storage {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Storage::Inline(ref cur) => f
                .debug_struct("Inline")
                .field("inner", cur.get_ref())
                .field("pos", &cur.position())
                .finish(),
            Storage::File(ref file) => f.debug_tuple("File").field(file).finish(),
        }
    }
}

/// A buffer which spills over to disk once its length grows beyond a set threshold.
#[derive(Debug)]
pub struct PagedBuffer {
    inner: Storage,
    threshold: usize,
}

impl PagedBuffer {
    /// Creates a new `PagedBuffer` with the given spillover threshold.
    pub fn with_threshold(t: usize) -> Self {
        PagedBuffer {
            inner: Storage::Inline(Cursor::new(Vec::new())),
            threshold: t,
        }
    }

    /// Persists the buffer to disk with as little redundant copying as possible.
    ///
    /// If the buffer is held in main memory, it is copied to a temporary file and atomically moved
    /// to the final destination. If the buffer has already spilled over to disk, the already
    /// existing temporary file is simply moved to the final destination, no extra copying needed.
    pub fn persist(self, dest: &Path, perms: Permissions) -> anyhow::Result<()> {
        match self.inner {
            Storage::Inline(mut inner) => {
                let mut temp = tempfile::NamedTempFile::new()?;
                crate::copy_wide(&mut inner, &mut temp)?;
                temp.as_file_mut().set_permissions(perms)?;
                filetime::set_file_mtime(temp.path(), FileTime::zero())?;
                temp.persist(dest)?;
            }
            Storage::File(mut inner) => {
                inner.as_file_mut().set_permissions(perms)?;
                filetime::set_file_mtime(inner.path(), FileTime::zero())?;
                inner.persist(dest)?;
            }
        }

        Ok(())
    }
}

impl Read for PagedBuffer {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut inner) => inner.read(buf),
            Storage::File(ref mut inner) => inner.read(buf),
        }
    }
}

impl Seek for PagedBuffer {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self.inner {
            Storage::Inline(ref mut inner) => inner.seek(pos),
            Storage::File(ref mut inner) => inner.seek(pos),
        }
    }
}

impl Write for PagedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut inner) => {
                if inner.get_ref().len() + buf.len() > self.threshold {
                    // TODO: Should we create this in a directory like `<store>/tmp` for security?
                    let mut file = tempfile::NamedTempFile::new()?;
                    file.write_all(inner.get_ref())?;
                    file.flush()?;

                    let len = file.write(buf)?;
                    self.inner = Storage::File(file);
                    Ok(len)
                } else {
                    inner.write(buf)
                }
            }
            Storage::File(ref mut inner) => inner.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner {
            Storage::Inline(ref mut inner) => inner.flush(),
            Storage::File(ref mut inner) => inner.flush(),
        }
    }
}
