use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::Context;
use serde::{de::Deserializer, ser::Serializer, Deserialize, Serialize};

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct ObjectId(blake3::Hash);

impl ObjectId {
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

#[derive(Debug, Default)]
pub struct Hasher(blake3::Hasher);

impl Hasher {
    pub fn new() -> Self {
        Hasher(blake3::Hasher::new())
    }

    pub fn update(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update(bytes);
        self
    }

    pub fn par_update(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.update_with_join::<blake3::join::RayonJoin>(bytes);
        self
    }

    pub fn finish(&self) -> ObjectId {
        ObjectId(self.0.finalize())
    }
}

#[derive(Debug)]
pub struct HashWriter<W> {
    inner: W,
    hasher: Hasher,
}

impl<W: Write> HashWriter<W> {
    pub fn with_header(header: &[u8], inner: W) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(header);
        HashWriter { inner, hasher }
    }

    pub fn object_id(&self) -> ObjectId {
        self.hasher.finish()
    }

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
