use std::io::{Cursor, Read, Seek, SeekFrom, Write};

#[derive(Debug)]
enum Storage {
    Inline(Cursor<Vec<u8>>),
    File(tempfile::NamedTempFile),
}

#[derive(Debug)]
pub struct PagedBuffer {
    inner: Storage,
    threshold: usize,
}

impl PagedBuffer {
    pub fn with_threshold(t: usize) -> Self {
        PagedBuffer {
            inner: Storage::Inline(Cursor::new(Vec::new())),
            threshold: t,
        }
    }
}

impl Read for PagedBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut inner) => inner.read(buf),
            Storage::File(ref mut inner) => inner.read(buf),
        }
    }
}

impl Seek for PagedBuffer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self.inner {
            Storage::Inline(ref mut inner) => inner.seek(pos),
            Storage::File(ref mut inner) => inner.seek(pos),
        }
    }
}

impl Write for PagedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut inner) => {
                if inner.get_ref().len() + buf.len() > self.threshold {
                    // TODO: Should we create this in a directory like `<store>/tmp` for security?
                    let mut file = tempfile::NamedTempFile::new()?;
                    std::io::copy(inner, &mut file)?;
                    file.as_file_mut().sync_data()?;

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

    fn flush(&mut self) -> std::io::Result<()> {
        match self.inner {
            Storage::Inline(ref mut inner) => inner.flush(),
            Storage::File(ref mut inner) => inner.flush(),
        }
    }
}
