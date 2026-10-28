// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fmt::{self, Display};
use std::io::{self, Write};
use std::path::PathBuf;
use std::str;

use crate::errors::{Error, Result};

/// A path or filename within Mercurial (typically within manifests or changegroups).
///
/// Mercurial treats pathnames as sequences of bytes, but the manifest format
/// assumes they cannot contain zero bytes. The bytes are not necessarily utf-8
/// and so cannot be converted into a string (or - strictly speaking - be displayed).
#[derive(Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct Path(Vec<u8>);

impl Path {
    pub fn new<P: AsRef<[u8]>>(p: P) -> Result<Path> {
        let p = p.as_ref();
        Self::verify(p)?;
        Ok(Path(p.into())) // copy slice into vec
    }

    fn verify(p: &[u8]) -> Result<()> {
        if p.contains(&0) {
            return Err(Error::InvalidPath("paths cannot contain '\\0'".into()));
        }
        Ok(())
    }

    pub fn generate<W: Write>(&self, out: &mut W) -> io::Result<()> {
        out.write_all(&self.0[..])
    }

    /// Perform the mapping to a filesystem path used in a .hg directory
    pub fn fsencode(&self, dotencode: bool) -> PathBuf {
        fn filter<P: AsRef<[u8]>>(p: P, dotencode: bool) -> String {
            let p = p.as_ref();
            let p = fnencode(p);
            let p = auxencode(p, dotencode);
            String::from_utf8(p).expect("bad utf8")
        }

        // TODO assume fncache
        // TODO doesn't do long path hashing
        let mut path = self.0.split(|c| *c == b'/').rev();
        let file = path.next();

        let mut ret: PathBuf = path
            .rev()
            .map(|p| filter(direncode(p), dotencode))
            .collect();

        if let Some(file) = file {
            ret.push(filter(file, dotencode));
        }

        ret
    }

    /// Store-relative filelog file with extension `ext` (`i` or `d`), `data/…` or hashed `dh/…`.
    ///
    /// A hashed name covers the extension, so the `.d` is not the `.i` with a new extension.
    pub fn store_filelog(&self, dotencode: bool, ext: &[u8]) -> PathBuf {
        let mut raw = Vec::with_capacity(self.0.len() + 6 + ext.len());
        raw.extend_from_slice(b"data/");
        raw.extend_from_slice(&self.0);
        raw.push(b'.');
        raw.extend_from_slice(ext);

        let de = encodedir(&raw);
        let encoded = fncache_encode(&de, dotencode);
        let chosen = if raw.len() > MAX_STORE_PATH_LEN || encoded.len() > MAX_STORE_PATH_LEN {
            hashencode(&de, dotencode)
        } else {
            encoded
        };
        pathbuf_from_slashes(&chosen)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        &self.0[..]
    }
}

impl AsRef<[u8]> for Path {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[cfg(test)]
static COMPONENT_CHARS: std::sync::LazyLock<Vec<u8>> = std::sync::LazyLock::new(|| {
    (1..b'/').chain((b'/' + 1)..255).collect()
});

#[cfg(test)]
fn gen_range(g: &mut quickcheck::Gen, low: usize, high: usize) -> usize {
    use quickcheck::Arbitrary;
    low + usize::arbitrary(g) % (high - low)
}

#[cfg(test)]
impl quickcheck::Arbitrary for Path {
    fn arbitrary(g: &mut quickcheck::Gen) -> Self {
        let size = g.size();
        // Up to sqrt(size) components, each with length from 1 to 2 *
        // sqrt(size) -- don't generate zero-length components. (This isn't
        // verified by Path::verify() but is good to have as a real distribution
        // of paths.)
        //
        // TODO: deal with or filter out '..' and friends.
        //
        // TODO: do we really want a uniform distribution over component chars
        // here?
        let size_sqrt = std::cmp::max((size as f64).sqrt() as usize, 2);

        let mut path = Vec::new();

        for i in 0..gen_range(g, 1, size_sqrt) {
            if i > 0 {
                path.push(b'/');
            }
            let n = gen_range(g, 1, 2 * size_sqrt);
            for _ in 0..n {
                path.push(*g.choose(&COMPONENT_CHARS[..]).unwrap());
            }
        }

        Path(path)
    }

    // Skip over shrink for now because it's non-trivial to do.
}

static HEX: &[u8] = b"0123456789abcdef";

fn hexenc(byte: u8, out: &mut Vec<u8>) {
    out.push(b'~');
    out.push(HEX[((byte >> 4) & 0xf) as usize]);
    out.push(HEX[((byte >> 0) & 0xf) as usize]);
}

// Encode directory names
fn direncode(elem: &[u8]) -> Vec<u8> {
    let mut ret = Vec::new();

    ret.extend_from_slice(elem);
    if elem.ends_with(b".hg") || elem.ends_with(b".i") || elem.ends_with(b".d") {
        ret.extend_from_slice(b".hg")
    }

    ret
}

fn fnencode<E: AsRef<[u8]>>(elem: E) -> Vec<u8> {
    let elem = elem.as_ref();
    let mut ret = Vec::new();

    for e in elem {
        let e = *e;
        match e {
            0..=31 | 126..=255 => hexenc(e, &mut ret),
            b'\\' | b':' | b'*' | b'?' | b'"' | b'<' | b'>' | b'|' => hexenc(e, &mut ret),
            b'A'..=b'Z' => {
                ret.push(b'_');
                ret.push(e - b'A' + b'a');
            }
            b'_' => ret.extend_from_slice(b"__"),
            _ => ret.push(e),
        }
    }

    ret
}

fn lowerencode(elem: &[u8]) -> Vec<u8> {
    let mut ret = Vec::new();

    for e in elem {
        let e = *e;
        match e {
            0..=31 | 126..=255 => hexenc(e, &mut ret),
            b'\\' | b':' | b'*' | b'?' | b'"' | b'<' | b'>' | b'|' => hexenc(e, &mut ret),
            b'A'..=b'Z' => {
                ret.push(e - b'A' + b'a');
            }
            _ => ret.push(e),
        }
    }

    ret
}

// if path element is a reserved windows name, remap last character to ~XX
fn auxencode<E: AsRef<[u8]>>(elem: E, dotencode: bool) -> Vec<u8> {
    let elem = elem.as_ref();
    let mut ret = Vec::new();

    if elem.len() > 0 {
        // Remove trailing '.' or ' ' for later handling
        let (name, tail) = match elem[elem.len() - 1] {
            b'.' | b' ' => (&elem[..elem.len() - 1], Some(elem[elem.len() - 1])),
            _ => (elem, None),
        };
        if dotencode && (name[0] == b'.' || name[0] == b' ') {
            // if dotencode and first char is '.' or ' ', then hex encode it
            hexenc(name[0], &mut ret);
            ret.extend_from_slice(&name[1..]);
        } else {
            // if base portion of name is a windows reserved name,
            // then hex encode 3rd char
            let pos = name.iter().position(|c| *c == b'.').unwrap_or(name.len());
            match &name[..pos] {
                b"aux" | b"con" | b"prn" | b"nul" => {
                    ret.extend_from_slice(&name[..2]);
                    hexenc(name[2], &mut ret);
                    ret.extend_from_slice(&name[3..]);
                }
                b"com" | b"lpt" if pos >= 4 && name[3] >= b'1' && name[3] <= b'9' => {
                    ret.extend_from_slice(&name[..2]);
                    hexenc(name[2], &mut ret);
                    ret.extend_from_slice(&name[3..]);
                }
                _ => ret.extend_from_slice(name),
            }
        }
        // hex encode trailing '.' or ' '
        if let Some(tail) = tail {
            hexenc(tail, &mut ret)
        }
    }

    ret
}

const MAX_STORE_PATH_LEN: usize = 120;
const DIR_PREFIX_LEN: usize = 8;
const MAX_SHORT_DIRS_LEN: usize = 8 * (DIR_PREFIX_LEN + 1) - 4;

/// `.hg/`, `.i/`, and `.d/` become `.hg.hg/`, `.i.hg/`, and `.d.hg/`.
/// Replacements are not scanned again.
fn encodedir(path: &[u8]) -> Vec<u8> {
    let path = replace_bytes(path, b".hg/", b".hg.hg/");
    let path = replace_bytes(&path, b".i/", b".i.hg/");
    replace_bytes(&path, b".d/", b".d.hg/")
}

/// Undo `encodedir`, as Mercurial's `store.decodedir` does when it reads fncache.
pub fn decodedir(path: &[u8]) -> Vec<u8> {
    let path = replace_bytes(path, b".d.hg/", b".d/");
    let path = replace_bytes(&path, b".i.hg/", b".i/");
    replace_bytes(&path, b".hg.hg/", b".hg/")
}

fn replace_bytes(hay: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(hay.len());
    let mut i = 0;
    while i < hay.len() {
        if hay[i..].starts_with(from) {
            out.extend_from_slice(to);
            i += from.len();
        } else {
            out.push(hay[i]);
            i += 1;
        }
    }
    out
}

fn fncache_encode(de: &[u8], dotencode: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(de.len());
    for (i, elem) in de.split(|c| *c == b'/').enumerate() {
        if i > 0 {
            out.push(b'/');
        }
        out.extend_from_slice(&auxencode(fnencode(elem), dotencode));
    }
    out
}

fn hashencode(de: &[u8], dotencode: bool) -> Vec<u8> {
    let digest = sha1_hex(de);
    let rest = de.get(5..).unwrap_or(&[]);
    let lowered = lowerencode(rest);
    let mut parts: Vec<Vec<u8>> = lowered
        .split(|c| *c == b'/')
        .map(|elem| auxencode(elem, dotencode))
        .collect();
    let basename = parts.pop().unwrap_or_default();
    let ext = splitext(&basename);

    let mut sdirs = Vec::new();
    let mut sdirslen = 0usize;
    for part in &parts {
        let mut prefix = part[..DIR_PREFIX_LEN.min(part.len())].to_vec();
        if let Some(last) = prefix.last_mut() {
            if *last == b'.' || *last == b' ' {
                *last = b'_';
            }
        }
        let next = if sdirslen == 0 {
            prefix.len()
        } else {
            let next = sdirslen + 1 + prefix.len();
            if next > MAX_SHORT_DIRS_LEN {
                break;
            }
            next
        };
        sdirs.push(prefix);
        sdirslen = next;
    }

    let mut dirs = Vec::new();
    for (i, dir) in sdirs.iter().enumerate() {
        if i > 0 {
            dirs.push(b'/');
        }
        dirs.extend_from_slice(dir);
    }
    if !dirs.is_empty() {
        dirs.push(b'/');
    }

    let mut res = Vec::new();
    res.extend_from_slice(b"dh/");
    res.extend_from_slice(&dirs);
    res.extend_from_slice(&digest);
    res.extend_from_slice(ext);
    if res.len() < MAX_STORE_PATH_LEN {
        let space = MAX_STORE_PATH_LEN - res.len();
        let filler = &basename[..space.min(basename.len())];
        res.clear();
        res.extend_from_slice(b"dh/");
        res.extend_from_slice(&dirs);
        res.extend_from_slice(filler);
        res.extend_from_slice(&digest);
        res.extend_from_slice(ext);
    }
    res
}

fn sha1_hex(data: &[u8]) -> Vec<u8> {
    crate::hash::Sha1::from(data).to_hex().as_bytes().to_vec()
}

/// Extension including the dot. A name of only dots has none.
fn splitext(name: &[u8]) -> &[u8] {
    match name.iter().rposition(|byte| *byte == b'.') {
        Some(dot) if name[..dot].iter().any(|byte| *byte != b'.') => &name[dot..],
        _ => b"",
    }
}

fn pathbuf_from_slashes(bytes: &[u8]) -> PathBuf {
    let mut path = PathBuf::new();
    for comp in bytes.split(|byte| *byte == b'/') {
        path.push(str::from_utf8(comp).expect("store path is utf-8"));
    }
    path
}

impl Display for Path {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}", String::from_utf8_lossy(self.0.as_ref()))
    }
}

// Implement our own Debug so that strings are displayed properly
impl fmt::Debug for Path {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Path({:?})", str::from_utf8(self.0.as_ref()))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    quickcheck::quickcheck! {
        /// Verify that instances generated by quickcheck are valid.
        fn path_gen(p: Path) -> bool {
            Path::verify(&p.0).is_ok()
        }
    }

    #[test]
    fn path_make() {
        assert!(Path::new(b"1234abc").is_ok());
    }

    #[test]
    fn bad_path() {
        assert!(Path::new(b"\0").is_err());
    }
    #[test]
    fn bad_path2() {
        assert!(Path::new(b"abc\0").is_err());
    }
    #[test]
    fn bad_path3() {
        assert!(Path::new(b"ab\0cde").is_err());
    }

    #[test]
    fn path_cmp() {
        let a = Path::new(b"a").unwrap();
        let b = Path::new(b"b").unwrap();

        assert!(a < b);
        assert!(a == a);
        assert!(b == b);
        assert!(a <= a);
        assert!(a <= b);
    }

    #[test]
    fn fsencode_simple() {
        let a = Path::new(b"foo/bar").unwrap();
        let p = a.fsencode(false);

        assert_eq!(p, PathBuf::from("foo/bar"));
    }

    #[test]
    fn fsencode_simple_single() {
        let a = Path::new(b"bar").unwrap();
        let p = a.fsencode(false);

        assert_eq!(p, PathBuf::from("bar"));
    }

    #[test]
    fn fsencode_hexquote() {
        let a = Path::new(b"oh?/wow~:<>").unwrap();
        let p = a.fsencode(false);

        assert_eq!(p, PathBuf::from("oh~3f/wow~7e~3a~3c~3e"));
    }

    #[test]
    fn fsencode_direncode() {
        assert_eq!(
            Path::new(b"foo.d/bar.d").unwrap().fsencode(false),
            PathBuf::from("foo.d.hg/bar.d")
        );
        assert_eq!(
            Path::new(b"foo.hg/bar.d").unwrap().fsencode(false),
            PathBuf::from("foo.hg.hg/bar.d")
        );
        assert_eq!(
            Path::new(b"tests/legacy-encoding.hg")
                .unwrap()
                .fsencode(false),
            PathBuf::from("tests/legacy-encoding.hg")
        );
    }

    #[test]
    fn fsencode_direncode_single() {
        let a = Path::new(b"bar.d").unwrap();
        let p = a.fsencode(false);

        assert_eq!(p, PathBuf::from("bar.d"));
    }

    #[test]
    fn fsencode_upper() {
        let a = Path::new(b"HELLO/WORLD").unwrap();
        let p = a.fsencode(false);

        assert_eq!(p, PathBuf::from("_h_e_l_l_o/_w_o_r_l_d"));
    }

    #[test]
    fn fsencode_upper_direncode() {
        let a = Path::new(b"HELLO.d/WORLD.d").unwrap();
        let p = a.fsencode(false);

        assert_eq!(p, PathBuf::from("_h_e_l_l_o.d.hg/_w_o_r_l_d.d"));
    }

    #[test]
    fn decodedir_matches_mercurial() {
        // Cases from the `store.decodedir` docstring.
        assert_eq!(decodedir(b"data/foo.i"), b"data/foo.i");
        assert_eq!(decodedir(b"data/foo.i.hg/bla.i"), b"data/foo.i/bla.i");
        assert_eq!(decodedir(b"data/foo.i.hg.hg/bla.i"), b"data/foo.i.hg/bla.i");
        assert_eq!(decodedir(&encodedir(b"data/a.d/b.hg/c.i")), b"data/a.d/b.hg/c.i");
    }

    #[test]
    fn store_filelog_short_mixed_case_stays_under_data() {
        let path = Path::new(b"Foo.txt").unwrap();
        assert_eq!(
            path.store_filelog(true, b"i"),
            std::path::Path::new("data").join("_foo.txt.i")
        );
    }

    #[test]
    fn hashed_filelog_data_file_has_its_own_hash() {
        // Names Mercurial 7 wrote in mozilla-central's store.
        let path = Path::new(
            b"devtools/docs/user/remote_debugging/debugging_firefox_for_android_with_webide/remote-debugging-android-open-tabs.png",
        )
        .unwrap();
        let dir = std::path::Path::new("dh/devtools/docs/user/remote_d/debuggin");
        assert_eq!(
            path.store_filelog(true, b"i"),
            dir.join("remote-debugging-android-open-tabs.pnga21fc45b37284615573320a40498186c8e570826.i")
        );
        assert_eq!(
            path.store_filelog(true, b"d"),
            dir.join("remote-debugging-android-open-tabs.png91f07312cd3f5d4bb1b8b8306656c1e2030489d9.d")
        );
    }
}
