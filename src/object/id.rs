//! Object ID and associated helper types.

use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Context;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};

/// A unique cryptographic hash representing an object (blob, tree, package).
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ObjectId(blake3::Hash);

impl ObjectId {
    /// The number of bytes in an `ObjectId` hash.
    pub const LENGTH: usize = blake3::OUT_LEN;

    /// Creates a new `ObjectId` from a raw byte array.
    pub fn from_bytes(bytes: [u8; Self::LENGTH]) -> Self {
        ObjectId(bytes.into())
    }

    /// Returns the raw byte representation of the object ID.
    pub fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        self.0.as_bytes()
    }

    /// Computes the canonical filesystem path representation of the object ID.
    ///
    /// The parent directory is the first two characters of the hash, joined with the remainder of
    /// the hash. An extension is commonly added to the end of the file name to distinguish the
    /// object's type, but this is not required and depends on the backing representation of the
    /// `Store` being used.
    pub fn to_path_buf(&self) -> PathBuf {
        let text = self.0.to_hex();
        Path::new(&text[0..2]).join(&text[2..])
    }
}

impl Debug for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", stringify!(ObjectId), self.0.to_hex())
    }
}

impl Display for ObjectId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0.to_hex())
    }
}

impl From<[u8; ObjectId::LENGTH]> for ObjectId {
    fn from(arr: [u8; ObjectId::LENGTH]) -> Self {
        ObjectId(arr.into())
    }
}

impl FromStr for ObjectId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut buf = [0u8; blake3::OUT_LEN];
        hex::decode_to_slice(s, &mut buf).context("string is not valid object hash")?;
        Ok(ObjectId(buf.into()))
    }
}

impl PartialOrd for ObjectId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.as_bytes().partial_cmp(&other.0.as_bytes())
    }
}

impl Ord for ObjectId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.as_bytes().cmp(&other.0.as_bytes())
    }
}

impl<'de> Deserialize<'de> for ObjectId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: [u8; blake3::OUT_LEN] = hex::serde::deserialize(deserializer)?;
        Ok(ObjectId(bytes.into()))
    }
}

impl Serialize for ObjectId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.to_hex().serialize(serializer)
    }
}

/// An incremental hasher that computes object IDs.
#[derive(Debug)]
pub struct Hasher(blake3::Hasher);

impl Hasher {
    /// Constructs a new `Hasher` for a blob object.
    #[inline]
    pub fn new_blob(is_executable: bool) -> Self {
        Hasher::with_header(if is_executable { b"exec:" } else { b"blob:" })
    }

    /// Constructs a new `Hasher` for a tree object.
    #[inline]
    pub fn new_tree() -> Self {
        Hasher::with_header(b"tree:")
    }

    /// Constructs a new `Hasher` for a package object.
    #[inline]
    pub fn new_package() -> Self {
        Hasher::with_header(b"pkg:")
    }

    /// Constructs a new `Hasher` for a spec object.
    #[inline]
    pub fn new_spec() -> Self {
        Hasher::with_header(b"spec:")
    }

    fn with_header(header: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(header);
        Hasher(hasher)
    }

    /// Adds input bytes to the hash state. You can call this any number of times.
    ///
    /// This method is single threaded, and it is recommended to call it with a buffer of at least
    /// 8 KiB (AVX2) to 16 KiB (AVX2 + AVX-512) in size for best performance.
    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update(bytes);
        self
    }

    /// Adds input bytes to the hash state, but potentially using multi-threading. You can call this
    /// any number of times.
    ///
    /// To get any performance benefit from multi-threading, the input buffer size needs to be very
    /// large. As a rule of thumb on x86_64, there is no benefit to multi-threading inputs less
    /// than 128 KiB.
    pub fn par_update(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update_with_join::<blake3::join::RayonJoin>(bytes);
        self
    }

    /// Finalizes the hash state and returns the computed `ObjectId`.
    pub fn finish(&self) -> ObjectId {
        ObjectId(self.0.finalize())
    }
}

/// Wraps an I/O writer and hashes its contents, producing an `ObjectId`.
///
/// While writing, it is recommended to pass buffers of at least 8 KiB (AVX2) to 16 KiB (AVX2 +
/// AVX-512) in size for best performance.
#[derive(Debug)]
pub struct HashWriter<W> {
    inner: W,
    hasher: Hasher,
}

impl<W: Write> HashWriter<W> {
    /// Creates a new `HashWriter<W>` with the given `Hasher` instance.
    pub fn with_hasher(hasher: Hasher, inner: W) -> Self {
        HashWriter { inner, hasher }
    }

    /// Finalizes the hash state and returns the computed `ObjectId`.
    pub fn object_id(&self) -> ObjectId {
        self.hasher.finish()
    }

    /// Unwraps this `HashWriter<W>`, returning the underlying buffer.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.hasher.update(&buf[0..len]);
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
