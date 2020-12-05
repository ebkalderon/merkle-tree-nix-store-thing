//! Types for handling package references.

use std::collections::BTreeSet;
use std::io::{self, Write};

use nom::bytes::complete::{self, tag};
use nom::bytes::streaming::take;
use nom::character::complete::{anychar, hex_digit1};
use nom::combinator::{complete, map, map_parser, map_res, verify};
use nom::multi::{many0, many_till};
use nom::sequence::{pair, preceded};
use nom::IResult;

use crate::object::is_package_name;
use crate::ObjectId;

/// Wraps a writer and scans the bytes being written for references.
///
/// In this context, a "reference" is a byte string containing a relative or absolute path pointing
/// to an installed package entry in the store. Scanning for path references in script files and
/// executable binaries is a critical step in detecting run-time dependencies for
/// [`Package`](crate::Package) objects.
#[derive(Debug)]
pub struct ReferenceSink<W> {
    inner: W,
    refs: BTreeSet<ObjectId>,
    buf: Vec<u8>,
}

impl<W: Write> ReferenceSink<W> {
    /// Creates a new `ReferenceSink<W>` which will scan `inner` for references.
    pub fn new(inner: W) -> Self {
        ReferenceSink {
            inner,
            refs: BTreeSet::new(),
            buf: Vec::new(),
        }
    }

    ///  Returns the set of unique references detected in the stream so far.
    pub fn references(&self) -> &BTreeSet<ObjectId> {
        &self.refs
    }

    /// Unwraps this `ReferenceSink<W>`, returning the underlying writer and any detected references.
    pub fn into_inner(self) -> (W, BTreeSet<ObjectId>) {
        (self.inner, self.refs)
    }
}

impl<W: Write> Write for ReferenceSink<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.inner.write(buf)?;
        self.buf.extend_from_slice(&buf[..len]);

        match many1_streaming(reference)(&self.buf) {
            Err(nom::Err::Incomplete(_)) => {}
            Err(_) => self.buf.clear(),
            Ok((remaining, pkg_id)) => {
                self.refs.extend(pkg_id);
                self.buf = remaining.to_vec();
            }
        }

        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Parses a package reference in a streaming fashion.
///
/// For efficiency (and a higher hit rate), just search for the hash part of the file name. This
/// assumes that all package references have the form `name-HASH`.
fn reference(input: &[u8]) -> IResult<&[u8], ObjectId> {
    let hash = map_parser(hex_digit1, complete::take(ObjectId::STR_LENGTH));
    let prefix = pair(verify(anychar, |c| is_package_name(*c)), tag("-"));
    let reference = map(many_till(take(1usize), preceded(prefix, hash)), |(_, b)| b);
    map_res(map_res(reference, std::str::from_utf8), |s: &str| s.parse())(input)
}

/// This function is similar to [`nom::multi::many1()`], except it runs the first parser in a
/// streaming fashion and the rest afterwards in a complete fashion.
fn many1_streaming<T, F>(mut parser: F) -> impl FnMut(&[u8]) -> IResult<&[u8], Vec<T>>
where
    F: FnMut(&[u8]) -> IResult<&[u8], T> + Clone,
{
    move |input| {
        let (input, first) = map(&mut parser, |v| vec![v])(input)?;
        let (input, rest) = many0(complete(&mut parser))(input)?;
        Ok((input, first.into_iter().chain(rest).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_references_short_chunks() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut sink = ReferenceSink::new(cursor);

        sink.write_all(b"heotnuhox/store").unwrap();
        sink.write_all(b"/packages/hell").unwrap();
        sink.write_all(b"o-1.0.0-fd53fe2392dc2").unwrap();
        sink.write_all(b"60e9cf414a39aeb43").unwrap();
        sink.write_all(b"641c10ab48a726c58e76").unwrap();
        sink.write_all(b"d06a7fe443d660/bin/he").unwrap();
        sink.write_all(b"llo8hzeyxhu").unwrap();

        let id: ObjectId = "fd53fe2392dc260e9cf414a39aeb43641c10ab48a726c58e76d06a7fe443d660"
            .parse()
            .unwrap();

        let (_cursor, mut references) = sink.into_inner();
        assert!(references.remove(&id));
        assert!(references.is_empty());
    }

    #[test]
    fn detects_references_long_chunks() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut sink = ReferenceSink::new(cursor);

        let mut long = b"oetnkjbm\0motnhqj/store/packages/hello-1.0.0-fd53fe2392dc260e9cf".to_vec();
        long.extend(b"414a39aeb43641c10ab48a726c58e76d06a7fe443d660oetetihoxonitbon/store/p");
        long.extend(b"ackages/hola-1.0.0-066d344ef7a60d67e85c627a84ba01c14634bea93a414fc9e0");
        long.extend(b"62cd2932ef35df84fuhjteetidbk/store/packages/nihao-1.0.0-4605fc3d0d20b");
        long.extend(b"641146b7932ef6e86e963af8c41");

        sink.write_all(&long).unwrap();
        sink.write_all(b"da4cf470d73639aac4a22e5e748k\n0").unwrap();

        let id1: ObjectId = "fd53fe2392dc260e9cf414a39aeb43641c10ab48a726c58e76d06a7fe443d660"
            .parse()
            .unwrap();

        let id2: ObjectId = "066d344ef7a60d67e85c627a84ba01c14634bea93a414fc9e062cd2932ef35df"
            .parse()
            .unwrap();

        let id3: ObjectId = "4605fc3d0d20b641146b7932ef6e86e963af8c41da4cf470d73639aac4a22e5e"
            .parse()
            .unwrap();

        let (_cursor, mut references) = sink.into_inner();
        assert!(references.remove(&id1));
        assert!(references.remove(&id2));
        assert!(references.remove(&id3));
        assert!(references.is_empty());
    }
}
