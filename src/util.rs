use std::io::{Cursor, Read, Seek, SeekFrom, Write};

#[derive(Debug)]
enum Storage {
    Inline(Cursor<Box<[u8]>>, usize),
    File(tempfile::NamedTempFile),
}

#[derive(Debug)]
pub struct PagedBuffer {
    inner: Storage,
    threshold: usize,
}

impl PagedBuffer {
    pub fn with_threshold(t: usize) -> Self {
        let fixed_buf = Cursor::new(vec![0; t].into_boxed_slice());
        PagedBuffer {
            inner: Storage::Inline(fixed_buf, 0),
            threshold: t,
        }
    }
}

impl Read for PagedBuffer {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut b, _) => b.read(buf),
            Storage::File(ref mut b) => b.read(buf),
        }
    }
}

impl Seek for PagedBuffer {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self.inner {
            Storage::Inline(ref mut b, _) => b.seek(pos),
            Storage::File(ref mut b) => b.seek(pos),
        }
    }
}

impl Write for PagedBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.inner {
            Storage::Inline(ref mut b, ref mut bytes_written) => {
                if *bytes_written + buf.len() > self.threshold {
                    // TODO: Should we create this in a directory like `<store>/tmp` for security?
                    let mut file = tempfile::NamedTempFile::new()?;
                    std::io::copy(b, &mut file)?;
                    file.as_file_mut().sync_data()?;

                    let len = file.write(buf)?;
                    self.inner = Storage::File(file);

                    Ok(len)
                } else {
                    let len = b.write(buf)?;
                    *bytes_written += len;
                    Ok(len)
                }
            }
            Storage::File(ref mut b) => b.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.inner {
            Storage::Inline(ref mut b, _) => b.flush(),
            Storage::File(ref mut b) => b.flush(),
        }
    }
}
