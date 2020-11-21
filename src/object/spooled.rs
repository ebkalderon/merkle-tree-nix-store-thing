//! Similar to `tempfile::SpooledTempFile` except it can be persisted to disk.

use std::fs::Permissions;
use std::io::{self, Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

use filetime::FileTime;

#[derive(Debug)]
enum Storage {
    InMemory(Cursor<Vec<u8>>),
    OnDisk(tempfile::NamedTempFile),
}

/// A buffer which spills over to disk once its length grows beyond a set max_size.
#[derive(Debug)]
pub struct SpooledTempFile {
    inner: Storage,
    max_size: usize,
}

impl SpooledTempFile {
    /// Creates a new `SpooledTempFile` with the given spillover max_size.
    pub fn new(max_size: usize) -> Self {
        SpooledTempFile {
            inner: Storage::InMemory(Cursor::new(Vec::new())),
            max_size,
        }
    }

    /// Persists the buffer to disk with as little redundant copying as possible.
    ///
    /// If the buffer is held in main memory, it is copied to a temporary file and atomically moved
    /// to the final destination. If the buffer has already spilled over to disk, the already
    /// existing temporary file is simply moved to the final destination, no extra copying needed.
    pub fn persist(self, dest: &Path, perms: Permissions) -> anyhow::Result<()> {
        match self.inner {
            Storage::InMemory(cursor) => {
                let mut temp = tempfile::NamedTempFile::new_in("/var/tmp")?;
                temp.write_all(cursor.get_ref())?;
                temp.flush()?;
                temp.as_file_mut().set_permissions(perms)?;
                filetime::set_file_mtime(temp.path(), FileTime::zero())?;
                temp.persist(dest)?;
            }
            Storage::OnDisk(mut file) => {
                file.as_file_mut().set_permissions(perms)?;
                filetime::set_file_mtime(file.path(), FileTime::zero())?;
                file.persist(dest)?;
            }
        }

        Ok(())
    }
}

impl Read for SpooledTempFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.inner {
            Storage::InMemory(ref mut cursor) => cursor.read(buf),
            Storage::OnDisk(ref mut file) => file.read(buf),
        }
    }
}

impl Seek for SpooledTempFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        match self.inner {
            Storage::InMemory(ref mut cursor) => cursor.seek(pos),
            Storage::OnDisk(ref mut file) => file.seek(pos),
        }
    }
}

impl Write for SpooledTempFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.inner {
            Storage::InMemory(ref mut cursor) => {
                if cursor.get_ref().len() + buf.len() > self.max_size {
                    // TODO: Should we create this in a directory like `<store>/tmp` for security?
                    let mut file = tempfile::NamedTempFile::new_in("/var/tmp")?;
                    file.write_all(cursor.get_ref())?;
                    file.seek(SeekFrom::Start(cursor.position()))?;

                    let len = file.write(buf)?;
                    self.inner = Storage::OnDisk(file);
                    Ok(len)
                } else {
                    cursor.write(buf)
                }
            }
            Storage::OnDisk(ref mut file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner {
            Storage::InMemory(ref mut cursor) => cursor.flush(),
            Storage::OnDisk(ref mut file) => file.flush(),
        }
    }
}