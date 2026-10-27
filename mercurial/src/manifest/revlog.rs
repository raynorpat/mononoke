// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::collections::BTreeMap;
use std::io::{self, Write};
use std::str;

use futures::future::{self, BoxFuture, FutureExt};
use futures::stream::{self, BoxStream, StreamExt};

use mercurial_types::manifest::{Content, Entry, Manifest, Type};
use mercurial_types::{BlobNode, NodeHash, Parents, Path};

use std::result::Result as StdResult;

use crate::errors::{ChainErr, Error, Result};
use crate::RevlogRepo;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct Details {
    nodeid: NodeHash,
    flag: Type,
}

/// Revlog Manifest v1
#[derive(Debug, PartialEq)]
pub struct RevlogManifest {
    repo: Option<RevlogRepo>,
    files: BTreeMap<Path, Details>,
}

// Each manifest revision contains a list of the file revisions in each changeset, in the form:
//
// <filename>\0<hex file revision id>[<flags>]\n
//
// Source: mercurial/parsers.c:parse_manifest()
//
// NB: filenames are sequences of non-zero bytes, not strings
pub fn parse(data: &[u8]) -> Result<BTreeMap<Path, Details>> {
    let mut files = BTreeMap::new();

    for line in data.split(|b| *b == b'\n') {
        if line.len() == 0 {
            break;
        }

        let (name, rest) = match find(line, &0) {
            None => return Err("Malformed entry: no \\0".into()),
            Some(nil) => {
                let (name, rest) = line.split_at(nil);
                if let Some((_, hash)) = rest.split_first() {
                    (name, hash)
                } else {
                    return Err("Malformed entry: no hash".into());
                }
            }
        };

        let path = Path::new(name).chain_err(|| "invalid path in manifest")?;
        let details = Details::parse(rest)?;

        // XXX check path > last entry in files
        files.insert(path, details);
    }

    Ok(files)
}

impl RevlogManifest {
    pub fn empty() -> RevlogManifest {
        RevlogManifest {
            repo: None,
            files: BTreeMap::new(),
        }
    }

    pub fn new(repo: RevlogRepo, node: BlobNode) -> Result<RevlogManifest> {
        node.as_blob()
            .as_slice()
            .ok_or_else(|| "node missing data".into())
            .and_then(|blob| Self::parse(Some(repo), blob))
    }

    pub fn parse(repo: Option<RevlogRepo>, data: &[u8]) -> Result<RevlogManifest> {
        parse(data).map(|files| RevlogManifest { repo, files })
    }

    pub fn generate<W: Write>(&self, out: &mut W) -> io::Result<()> {
        for (k, v) in &self.files {
            k.generate(out)?;
            out.write_all(b"\0")?;
            v.generate(out)?;
            out.write_all(b"\n")?;
        }
        Ok(())
    }

    pub fn lookup(&self, path: &Path) -> Option<&Details> {
        self.files.get(path)
    }

    pub fn manifest(&self) -> Vec<(&Path, &Details)> {
        self.files.iter().collect()
    }
}

impl Details {
    fn parse(data: &[u8]) -> Result<Details> {
        if data.len() < 40 {
            return Err("hash too small".into());
        }

        let (hash, flags) = data.split_at(40);
        let hash = match str::from_utf8(hash) {
            Err(_) => return Err("malformed hash".into()),
            Ok(hs) => hs,
        };
        let nodeid = hash.parse().chain_err(|| "malformed hash")?;

        if flags.len() > 1 {
            return Err("More than 1 flag".into());
        }

        let flag = if flags.len() == 0 {
            Type::File
        } else {
            match flags[0] {
                b'l' => Type::Symlink,
                b'x' => Type::Executable,
                b't' => Type::Tree,
                unk => return Err(format!("Unknown flag {}", unk).into()),
            }
        };

        Ok(Details {
            nodeid: nodeid,
            flag: flag,
        })
    }

    fn generate<W: Write>(&self, out: &mut W) -> io::Result<()> {
        write!(out, "{}{}", self.nodeid, self.flag)
    }

    pub fn nodeid(&self) -> &NodeHash {
        &self.nodeid
    }

    pub fn flag(&self) -> Type {
        self.flag
    }

    pub fn is_symlink(&self) -> bool {
        self.flag == Type::Symlink
    }

    pub fn is_tree(&self) -> bool {
        self.flag == Type::Tree
    }

    pub fn is_executable(&self) -> bool {
        self.flag == Type::Executable
    }

    pub fn is_file(&self) -> bool {
        self.flag == Type::File
    }
}

fn find<T>(haystack: &[T], needle: &T) -> Option<usize>
where
    T: PartialEq,
{
    haystack.iter().position(|e| e == needle)
}

pub struct RevlogEntry {
    repo: RevlogRepo,
    path: Path,
    details: Details,
}

impl Manifest for RevlogManifest {
    type Error = Error;

    fn lookup(
        &self,
        path: &Path,
    ) -> BoxFuture<'static, StdResult<Option<Box<dyn Entry<Error = Self::Error>>>, Self::Error>>
    {
        let repo = self.repo.as_ref().expect("missing repo").clone();
        let found = self.files.get(path).map(|details| {
            RevlogEntry {
                repo,
                path: path.clone(),
                details: *details,
            }
            .boxed()
        });

        future::ready(Ok(found)).boxed()
    }

    fn list(
        &self,
    ) -> BoxStream<'static, StdResult<Box<dyn Entry<Error = Self::Error>>, Self::Error>> {
        let repo = self.repo.as_ref().expect("missing repo").clone();
        let entries: Vec<_> = self
            .manifest()
            .into_iter()
            .map(|(path, details)| (path.clone(), *details))
            .collect();
        stream::iter(entries.into_iter().map(move |(path, details)| {
            let entry = RevlogEntry {
                repo: repo.clone(),
                path,
                details,
            };
            Ok(entry.boxed())
        }))
        .boxed()
    }
}

impl Entry for RevlogEntry {
    type Error = Error;

    fn get_type(&self) -> Type {
        self.details.flag()
    }

    fn get_parents(&self) -> BoxFuture<'static, StdResult<Parents, Self::Error>> {
        let path = self.path.clone();
        let hash = *self.details.nodeid();
        let result = self
            .repo
            .get_file_revlog(&path)
            .and_then(|revlog| revlog.get_rev_by_nodeid(&hash))
            .map(|node| *node.parents());
        future::ready(result).boxed()
    }

    fn get_content(&self) -> BoxFuture<'static, StdResult<Content<Self::Error>, Self::Error>> {
        let path = self.path.clone();
        let hash = *self.details.nodeid();
        let kind = self.get_type();
        let result = (|| -> Result<Content<Error>> {
            let data = self
                .repo
                .get_file_revlog(&path)
                .and_then(|revlog| revlog.get_rev_by_nodeid(&hash))
                .map(|node| node.as_blob().clone())?;
            match kind {
                Type::File => Ok(Content::File(data)),
                Type::Executable => Ok(Content::Executable(data)),
                Type::Symlink => {
                    let bytes = data
                        .as_slice()
                        .ok_or_else(|| Error::from("missing blob data"))?;
                    Ok(Content::Symlink(Path::new(bytes)?))
                }
                Type::Tree => unimplemented!(),
            }
        })()
        .map_err(|err| {
            Error::with_chain(
                err,
                format!("Can't get content for {} node {}", path, hash),
            )
        });
        future::ready(result).boxed()
    }

    fn get_hash(&self) -> &NodeHash {
        self.details.nodeid()
    }

    fn get_path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;
    use std::str;

    use mercurial_types::manifest::Type;
    use mercurial_types::Path;

    use super::*;

    #[test]
    fn test_find() {
        assert_eq!(find(b"abc123", &b'b'), Some(1));
        assert_eq!(find(b"abc123", &b'x'), None);
        assert_eq!(find(b"abc123abc", &b'b'), Some(1));
        assert_eq!(find(b"", &b'b'), None);
    }

    #[test]
    fn empty() {
        assert_eq!(
            RevlogManifest::parse(None, b"").unwrap(),
            RevlogManifest {
                repo: None,
                files: BTreeMap::new(),
            }
        );
    }

    #[test]
    fn bad_nonil() {
        match RevlogManifest::parse(None, b"hello123") {
            Ok(m) => panic!("unexpected manifest {:?}", m),
            Err(e) => println!("got expected error: {}", e),
        }
    }

    #[test]
    fn bad_nohash() {
        match RevlogManifest::parse(None, b"hello123\0") {
            Ok(m) => panic!("unexpected manifest {:?}", m),
            Err(e) => println!("got expected error: {}", e),
        }
    }

    #[test]
    fn bad_badhash1() {
        match RevlogManifest::parse(None, b"hello123\0abc123") {
            Ok(m) => panic!("unexpected manifest {:?}", m),
            Err(e) => println!("got expected error: {}", e),
        }
    }

    #[test]
    fn good_one() {
        match RevlogManifest::parse(
            None,
            b"hello123\0da39a3ee5e6b4b0d3255bfef95601890afd80709xltZZZ\n",
        ) {
            Ok(m) => {
                let expect = vec![
                    (
                        Path::new(b"hello123").unwrap(),
                        Details {
                            nodeid: "da39a3ee5e6b4b0d3255bfef95601890afd80709".parse().unwrap(),
                            flag: Type::Symlink,
                        },
                    ),
                ];
                assert_eq!(m.files.into_iter().collect::<Vec<_>>(), expect);
            }
            Err(e) => println!("got expected error: {}", e),
        }
    }

    #[test]
    fn one_roundtrip() {
        // Only one flag because its unclear how multiple flags should be ordered
        const RAW: &[u8] = b"hello123\0da39a3ee5e6b4b0d3255bfef95601890afd80709x\n";
        let m = RevlogManifest::parse(None, RAW).expect("failed to parse");

        let mut out = Vec::new();
        m.generate(&mut out).expect("generate failed");

        if RAW != &out[..] {
            println!("\nRAW: {:?}", str::from_utf8(RAW));
            println!("out: {:?}", str::from_utf8(out.as_ref()));
            panic!(
                "out ({} bytes) mismatch RAW ({} bytes)",
                RAW.len(),
                out.len()
            );
        }
    }

    const MANIFEST: &[u8] = include_bytes!("flatmanifest.bin");

    #[test]
    fn fullmanifest() {
        match RevlogManifest::parse(None, MANIFEST) {
            Ok(m) => {
                println!("Got manifest:");
                for (k, v) in &m.files {
                    println!("{:?} {:?}", k, v);
                }
            }
            Err(e) => panic!("Failed to load manifest: {}", e),
        }
    }

    #[test]
    fn roundtrip() {
        let m = RevlogManifest::parse(None, MANIFEST).expect("parse failed");

        let mut out = Vec::new();
        m.generate(&mut out).expect("generate failed");

        if MANIFEST != &out[..] {
            panic!(
                "out ({} bytes) mismatch MANIFEST ({} bytes)",
                MANIFEST.len(),
                out.len()
            )
        }
    }
}
