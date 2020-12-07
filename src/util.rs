//! Common utilities for working with I/O.

use std::fs::File;
use std::io::{self, Cursor, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use memmap::{Mmap, MmapOptions};

/// A faster implementation of `std::io::copy()` which uses a larger 64K buffer instead of 8K.
///
/// This larger buffer size leverages SIMD on x86_64 and other modern platforms for faster speeds.
/// See this GitHub issue: https://github.com/rust-lang/rust/issues/49921
pub fn copy_wide<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> io::Result<u64> {
    let mut buffer = [0; 65536];
    let mut total = 0;
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(total),
            Ok(n) => {
                writer.write_all(&buffer[..n])?;
                total += n as u64;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Selects the most efficient strategy to open a file, optimized for large sequential reads.
pub fn open_large_read<T, F1, F2, F3>(path: &Path, inline: F1, mmap: F2, io: F3) -> io::Result<T>
where
    F1: Fn(Cursor<Vec<u8>>, bool) -> io::Result<T>,
    F2: Fn(Cursor<Mmap>, bool) -> io::Result<T>,
    F3: Fn(File, bool) -> io::Result<T>,
{
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    let is_executable = metadata.mode() & 0o100 != 0;

    if metadata.len() < 16 * 1024 {
        // Not worth it to mmap(2) small files. Load into memory instead.
        let mut buf = Vec::with_capacity(metadata.len() as usize);
        file.read_to_end(&mut buf)?;
        inline(Cursor::new(buf), is_executable)
    } else if metadata.len() <= isize::max_value() as u64 {
        // Prefer memory-mapping files wherever possible for performance.
        let handle = unsafe { MmapOptions::new().len(metadata.len() as usize).map(&file)? };
        mmap(Cursor::new(handle), is_executable)
    } else {
        // Only fall back to regular file I/O if file is too large to mmap(2).
        io(file, is_executable)
    }
}

/// Normalizes file permissons for `p` and sets all timestamps to January 1st, 1970.
pub fn normalize_perms(p: &Path, mode: u32) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let perms = std::fs::Permissions::from_mode(mode);
    std::fs::set_permissions(p, perms)?;
    let zero = filetime::FileTime::zero();
    filetime::set_symlink_file_times(p, zero, zero)?;
    Ok(())
}
