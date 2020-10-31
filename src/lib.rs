//! Prototype content-addressable Nix-like store backed by a Merkle tree.

pub use self::object::*;

pub mod store;

mod object;
mod util;
