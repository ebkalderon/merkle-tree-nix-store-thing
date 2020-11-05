//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::closure::Closure;
pub use self::object::*;

use std::io::{self, Read, Write};

pub mod remote;
pub mod store;

mod closure;
mod object;

/// An faster implementation of `std::io::copy()` which uses a larger 64K buffer instead of 8K.
///
/// This larger buffer size leverages SIMD on x86_64 and other modern platforms for faster speeds.
/// See this GitHub issue: https://github.com/rust-lang/rust/issues/49921
fn copy_wide<R: Read, W: Write>(reader: &mut R, writer: &mut W) -> io::Result<u64> {
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
