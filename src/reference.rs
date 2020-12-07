//! Types for handling package references.

use std::collections::BTreeSet;
use std::io::{self, Write};
use std::path::Path;

use anyhow::anyhow;
use nom::bytes::complete::{self, tag};
use nom::bytes::streaming::take;
use nom::character::complete::{anychar, hex_digit1};
use nom::combinator::{complete, map, map_parser, map_res, verify};
use nom::multi::{many0, many_till};
use nom::sequence::{pair, preceded};
use nom::IResult;

use crate::object::is_package_name;
use crate::ObjectId;

/// Wraps a writer and replaces path references in its output that match a pattern.
///
/// In this context, a "reference" is a byte string containing a relative or absolute path pointing
/// to an installed package entry in the store. Patching references in script files and executable
/// binaries is a critical step in building [`Package`](crate::Package) objects from source, or
/// relocating built packages to new store prefixes.
#[derive(Debug)]
pub struct RewriteSink<W> {
    inner: W,
    offsets: BTreeSet<u64>,
    pattern: Box<[u8]>,
    replace: Box<[u8]>,
    cursor: u64,
    buf: Vec<u8>,
}

impl<W: Write> RewriteSink<W> {
    /// Creates a new `RewriteSink<W>` which replaces occurrences of `pattern` with `replace`.
    ///
    /// The length of `replace` must be less than or equal to the length of `pattern`. If `replace`
    /// is shorter than `pattern`, it is padded with `/` characters until the lengths match.
    /// However, if `replace` is longer than `pattern`, this function returns an error.
    pub fn new(inner: W, pattern: &Path, replace: &Path) -> anyhow::Result<Self> {
        let pat = pattern.to_string_lossy().into_owned().into_bytes();
        let mut rep = replace.to_string_lossy().into_owned().into_bytes();

        if rep.len() < pat.len() {
            rep.extend(vec![b'/'; pat.len() - rep.len()]);
        } else if rep.len() > rep.len() {
            return Err(anyhow!(
                "new path {} is longer than old path, binary text replacement is impossible",
                replace.display()
            ));
        }

        Ok(RewriteSink {
            inner,
            offsets: BTreeSet::new(),
            pattern: pat.into_boxed_slice(),
            replace: rep.into_boxed_slice(),
            cursor: 0,
            buf: Vec::new(),
        })
    }

    /// Unwraps this `RewriteSink<W>`, returning the underlying writer and replacement offsets.
    ///
    /// The buffer is written out before returning the writer.
    pub fn into_inner(mut self) -> io::Result<(W, BTreeSet<u64>)> {
        self.flush()?;
        Ok((self.inner, self.offsets))
    }
}

impl<W: Write> Write for RewriteSink<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut temp = self.buf.clone();
        temp.extend_from_slice(buf);

        let pat_len = self.pattern.len();
        let mut i = 0;

        // Scan through `temp` and replace any matching patterns.
        while let Some(pos) = temp[i..].windows(pat_len).position(|w| *w == *self.pattern) {
            self.offsets.insert(self.cursor + pos as u64);
            temp[pos..pos + pat_len].copy_from_slice(&self.replace);
            i = pos;
        }

        // Handle matching text possibly staggered across multiple `write()` calls.
        if temp.len() < self.pattern.len() {
            self.buf.clear();
            self.buf.extend_from_slice(&temp);
        } else {
            let start = temp.len() - self.pattern.len() + 1;
            let end = start + self.pattern.len() - 1;
            self.buf.clear();
            self.buf.extend_from_slice(&temp[start..end]);
        }

        let consumed = temp.len() - self.buf.len();
        self.inner.write_all(&temp[..consumed])?;
        self.cursor += consumed as u64;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if !self.buf.is_empty() {
            self.inner.write_all(&self.buf)?;
            self.cursor += self.buf.len() as u64;
            self.buf.clear();
        }
        self.inner.flush()
    }
}

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
    F: FnMut(&[u8]) -> IResult<&[u8], T>,
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
    fn rewrites_paths() {
        let cursor = std::io::Cursor::new(Vec::new());
        let pat = "/store/packages/.staging/hello-1.0.0-0000000000000000000000000000000000000000000000000000000000000000";
        let rep = "/store/packages/hello-1.0.0-fd53fe2392dc260e9cf414a39aeb43641c10ab48a726c58e76d06a7fe443d660";
        let mut sink = RewriteSink::new(cursor, Path::new(pat), Path::new(rep)).unwrap();

        sink.write_all(b"teteoetjnjwougyr.jwjn./store").unwrap();
        sink.write_all(b"/packages/.staging/hello-1.0").unwrap();
        sink.write_all(b".0-0000000000000000000000000").unwrap();
        sink.write_all(b"0000000000000000000000000000").unwrap();
        sink.write_all(b"00000000000ett833\0etjj,3#/s").unwrap();
        sink.write_all(b"tore/packages/.staging/hello").unwrap();
        sink.write_all(b"-1.0.0-000000000000000000000").unwrap();
        sink.write_all(b"0000000000000000000000000000").unwrap();
        sink.write_all(b"000000000000000etkte72tjto'q").unwrap();

        let expected_str = "teteoetjnjwougyr.jwjn./store/packages/hello-1.0.0-fd53fe2392dc260e9c\
            f414a39aeb43641c10ab48a726c58e76d06a7fe443d660/////////ett833\0etjj,3#/store/package\
            s/hello-1.0.0-fd53fe2392dc260e9cf414a39aeb43641c10ab48a726c58e76d06a7fe443d660//////\
            ///etkte72tjto'q";

        let mut expected_offs = BTreeSet::new();
        expected_offs.insert(22);
        expected_offs.insert(137);

        let (cursor, offsets) = sink.into_inner().unwrap();
        let patched = String::from_utf8(cursor.into_inner()).unwrap();
        assert_eq!(patched, expected_str);
        assert_eq!(offsets, expected_offs);
    }

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

        let mut expected = BTreeSet::new();
        expected.insert(id);

        let (_cursor, references) = sink.into_inner();
        assert_eq!(references, expected);
    }

    #[test]
    fn detects_references_long_chunks() {
        let cursor = std::io::Cursor::new(Vec::new());
        let mut sink = ReferenceSink::new(cursor);

        let long = b"oetnkjbm\0motnhqj/store/packages/hello-1.0.0-fd53fe2392dc260e9cf414a39aeb43\
            641c10ab48a726c58e76d06a7fe443d660oetetihoxonitbon/store/packages/hola-1.0.0-066d344\
            ef7a60d67e85c627a84ba01c14634bea93a414fc9e062cd2932ef35df84fuhjteetidbk/store/packag\
            es/nihao-1.0.0-4605fc3d0d20b641146b7932ef6e86e963af8c41";

        sink.write_all(long).unwrap();
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

        let mut expected = BTreeSet::new();
        expected.insert(id1);
        expected.insert(id2);
        expected.insert(id3);

        let (_cursor, references) = sink.into_inner();
        assert_eq!(references, expected);
    }
}
