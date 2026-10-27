// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

#![deny(warnings)]

use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ascii::AsciiStr;
use futures::future::{self, BoxFuture, FutureExt};
use futures::stream::{self, BoxStream, StreamExt};
use thiserror::Error;

use bookmarks::{Bookmarks, BookmarksMut, ListBookmarks, ListBookmarksMut, Version};
use mercurial_types::NodeHash;

/// Failure to read or write a stock bookmark file.
#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid bookmark line: {}", String::from_utf8_lossy(.0))]
    InvalidBookmarkLine(Vec<u8>),
    #[error("invalid hash: {}", String::from_utf8_lossy(.0))]
    InvalidHash(Vec<u8>),
    #[error("stock bookmarks have no directory to write")]
    MissingBase,
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl Error {
    pub fn kind(&self) -> &Self {
        self
    }
}

pub type ErrorKind = Error;

pub type Result<T> = std::result::Result<T, Error>;

/// Implementation of bookmarks as they exist in stock Mercurial inside `.hg/bookmarks`.
/// The file has a list of entries:
///
/// ```text
/// <hash1> <bookmark1-name>
/// <hash2> <bookmark2-name>
/// ...
/// ```
///
/// Bookmark names are arbitrary bytestrings, and hashes are always NodeHashes.
///
/// `read` opens `<base>/bookmarks` and treats a missing file as empty. `write` emits one
/// `<40-hex><space><name>\n` line per bookmark, sorted by name. `set` and `delete` update the
/// map and write that file. This does not take Mercurial's repository lock.
#[derive(Debug)]
pub struct StockBookmarks {
    bookmarks: Mutex<HashMap<Vec<u8>, NodeHash>>,
    // Directory passed to `read`, joined with `bookmarks` on write. `None` for `from_reader`.
    base: Option<PathBuf>,
}

impl StockBookmarks {
    pub fn read<P: Into<PathBuf>>(base: P) -> Result<Self> {
        let base = base.into();

        let file = fs::File::open(base.join("bookmarks"));
        match file {
            Ok(file) => {
                let mut parsed = Self::from_reader(file)?;
                parsed.base = Some(base);
                Ok(parsed)
            }
            Err(ref err) if err.kind() == io::ErrorKind::NotFound => {
                // The .hg/bookmarks file is not guaranteed to exist. Treat it as empty if it
                // doesn't.
                Ok(StockBookmarks {
                    bookmarks: Mutex::new(HashMap::new()),
                    base: Some(base),
                })
            }
            Err(err) => Err(err.into()),
        }
    }

    /// Write `<base>/bookmarks`. Lines are `<40-hex><space><name>\n`, sorted by name.
    pub fn write(&self, base: &Path) -> Result<()> {
        let map = self.bookmarks.lock().expect("bookmarks lock poisoned");
        Self::write_map(base, &map)
    }

    fn from_reader<R: Read>(reader: R) -> Result<Self> {
        let mut bookmarks = HashMap::new();

        // Bookmark names might not be valid UTF-8, so use split() instead of lines().
        for line in BufReader::new(reader).split(b'\n') {
            let line = line?;
            // <hash><space><bookmark name>, where hash is 40 bytes, the space is 1 byte
            // and the bookmark name is at least 1 byte.
            if line.len() < 42 || line[40] != b' ' {
                return Err(Error::InvalidBookmarkLine(line));
            }
            let bmname = &line[41..];
            let hash_slice = &line[..40];
            let hash = AsciiStr::from_ascii(hash_slice)
                .map_err(|_| Error::InvalidHash(hash_slice.to_vec()))?;
            let node = NodeHash::from_ascii_str(hash)
                .map_err(|_| Error::InvalidHash(hash_slice.to_vec()))?;
            bookmarks.insert(bmname.to_vec(), node);
        }

        Ok(StockBookmarks {
            bookmarks: Mutex::new(bookmarks),
            base: None,
        })
    }

    fn current_version(map: &HashMap<Vec<u8>, NodeHash>, key: &[u8]) -> Version {
        if map.contains_key(key) {
            Version::from(1)
        } else {
            Version::absent()
        }
    }

    fn render(map: &HashMap<Vec<u8>, NodeHash>) -> Vec<u8> {
        let mut names: Vec<&Vec<u8>> = map.keys().collect();
        names.sort();
        let mut out = Vec::new();
        for name in names {
            let hash = map[name];
            out.extend_from_slice(hash.to_hex().as_str().as_bytes());
            out.push(b' ');
            out.extend_from_slice(name);
            out.push(b'\n');
        }
        out
    }

    fn write_map(base: &Path, map: &HashMap<Vec<u8>, NodeHash>) -> Result<()> {
        fs::write(base.join("bookmarks"), Self::render(map))?;
        Ok(())
    }

    fn stored_base(&self) -> Result<PathBuf> {
        self.base.clone().ok_or(Error::MissingBase)
    }

    fn set_inner(&self, key: &[u8], value: NodeHash, version: &Version) -> Result<Option<Version>> {
        let mut map = self.bookmarks.lock().expect("bookmarks lock poisoned");
        if &Self::current_version(&map, key) != version {
            return Ok(None);
        }
        let base = self.stored_base()?;
        let previous = map.insert(key.to_vec(), value);
        if let Err(err) = Self::write_map(&base, &map) {
            match previous {
                Some(old) => {
                    map.insert(key.to_vec(), old);
                }
                None => {
                    map.remove(key);
                }
            }
            return Err(err);
        }
        Ok(Some(Version::from(1)))
    }

    fn delete_inner(&self, key: &[u8], version: &Version) -> Result<Option<Version>> {
        let mut map = self.bookmarks.lock().expect("bookmarks lock poisoned");
        if &Self::current_version(&map, key) != version {
            return Ok(None);
        }
        if !map.contains_key(key) {
            return Ok(Some(Version::absent()));
        }
        let base = self.stored_base()?;
        let previous = map.remove(key).expect("bookmark present");
        if let Err(err) = Self::write_map(&base, &map) {
            map.insert(key.to_vec(), previous);
            return Err(err);
        }
        Ok(Some(Version::absent()))
    }
}

impl Bookmarks for StockBookmarks {
    type Value = NodeHash;
    type Error = Error;

    fn get(
        &self,
        name: &dyn AsRef<[u8]>,
    ) -> BoxFuture<'static, Result<Option<(NodeHash, Version)>>> {
        let value = match self
            .bookmarks
            .lock()
            .expect("bookmarks lock poisoned")
            .get(name.as_ref())
        {
            Some(hash) => Some((*hash, Version::from(1))),
            None => None,
        };
        future::ready(Ok(value)).boxed()
    }
}

impl BookmarksMut for StockBookmarks {
    fn set(
        &self,
        key: &dyn AsRef<[u8]>,
        value: &NodeHash,
        version: &Version,
    ) -> BoxFuture<'static, Result<Option<Version>>> {
        future::ready(self.set_inner(key.as_ref(), *value, version)).boxed()
    }

    fn delete(
        &self,
        key: &dyn AsRef<[u8]>,
        version: &Version,
    ) -> BoxFuture<'static, Result<Option<Version>>> {
        future::ready(self.delete_inner(key.as_ref(), version)).boxed()
    }
}

impl ListBookmarks for StockBookmarks {
    fn keys(&self) -> BoxStream<'static, Result<Vec<u8>>> {
        // collect forces evaluation early, so that the stream can safely outlive self
        let keys = self
            .bookmarks
            .lock()
            .expect("bookmarks lock poisoned")
            .keys()
            .map(|key| Ok(key.clone()))
            .collect::<Vec<_>>();
        stream::iter(keys).boxed()
    }
}

impl ListBookmarksMut for StockBookmarks {}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Cursor;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use assert_matches::assert_matches;
    use futures::executor::block_on;
    use futures::TryStreamExt;

    use super::*;

    fn hash_ones() -> NodeHash {
        "1111111111111111111111111111111111111111".parse().unwrap()
    }

    fn hash_twos() -> NodeHash {
        "2222222222222222222222222222222222222222".parse().unwrap()
    }

    fn assert_bookmark_get(
        bookmarks: &StockBookmarks,
        key: &dyn AsRef<[u8]>,
        expected: Option<NodeHash>,
    ) {
        let expected = match expected {
            Some(hash) => Some((hash, Version::from(1))),
            None => None,
        };
        assert_eq!(block_on(bookmarks.get(key)).unwrap(), expected);
    }

    #[test]
    fn test_parse() {
        let disk_bookmarks = b"\
            1111111111111111111111111111111111111111 abc\n\
            2222222222222222222222222222222222222222 def\n\
            1111111111111111111111111111111111111111 test123\n";
        let reader = Cursor::new(&disk_bookmarks[..]);

        let bookmarks = StockBookmarks::from_reader(reader).unwrap();
        assert_bookmark_get(&bookmarks, &"abc", Some(hash_ones()));
        assert_bookmark_get(&bookmarks, &"def", Some(hash_twos()));
        assert_bookmark_get(&bookmarks, &"test123", Some(hash_ones()));

        // Bookmarks that aren't present
        assert_bookmark_get(&bookmarks, &"abcdef", None);

        // keys should return all the keys here
        let mut list = block_on(bookmarks.keys().try_collect::<Vec<_>>()).unwrap();
        list.sort();
        assert_eq!(list, vec![&b"abc"[..], &b"def"[..], &b"test123"[..]]);
    }

    /// Test a bunch of invalid bookmark lines
    #[test]
    fn test_invalid() {
        let reader = Cursor::new(&b"111\n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(
            bookmarks.unwrap_err().kind(),
            &ErrorKind::InvalidBookmarkLine(_)
        );

        // no space or bookmark name
        let reader = Cursor::new(&b"1111111111111111111111111111111111111111\n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(
            bookmarks.unwrap_err().kind(),
            &ErrorKind::InvalidBookmarkLine(_)
        );

        // no bookmark name
        let reader = Cursor::new(&b"1111111111111111111111111111111111111111 \n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(
            bookmarks.unwrap_err().kind(),
            &ErrorKind::InvalidBookmarkLine(_)
        );

        // no space after hash
        let reader = Cursor::new(&b"1111111111111111111111111111111111111111ab\n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(
            bookmarks.unwrap_err().kind(),
            &ErrorKind::InvalidBookmarkLine(_)
        );

        // short hash
        let reader = Cursor::new(&b"111111111111111111111111111111111111111  1ab\n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(bookmarks.unwrap_err().kind(), &ErrorKind::InvalidHash(_));

        // non-ASCII
        let reader = Cursor::new(&b"111111111111111111111111111111111111111\xff test\n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(bookmarks.unwrap_err().kind(), &ErrorKind::InvalidHash(_));

        // not a valid hex string
        let reader = Cursor::new(&b"abcdefgabcdefgabcdefgabcdefgabcdefgabcde test\n"[..]);
        let bookmarks = StockBookmarks::from_reader(reader);
        assert_matches!(bookmarks.unwrap_err().kind(), &ErrorKind::InvalidHash(_));
    }

    #[test]
    fn write_round_trip_sorts_names() {
        let dir = PathBuf::from(std::env::temp_dir()).join(format!(
            "stockbookmarks-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let _cleanup = CleanupDir(dir.clone());

        let bookmarks = StockBookmarks::read(&dir).unwrap();
        block_on(bookmarks.create(b"zeta", &hash_twos()))
            .unwrap()
            .expect("create zeta");
        block_on(bookmarks.create(b"alpha", &hash_ones()))
            .unwrap()
            .expect("create alpha");

        let bytes = fs::read(dir.join("bookmarks")).unwrap();
        let expected = format!("{} alpha\n{} zeta\n", hash_ones(), hash_twos());
        assert_eq!(bytes, expected.as_bytes());

        let again = StockBookmarks::read(&dir).unwrap();
        assert_bookmark_get(&again, b"alpha", Some(hash_ones()));
        assert_bookmark_get(&again, b"zeta", Some(hash_twos()));
        assert_bookmark_get(&again, b"missing", None);
    }

    struct CleanupDir(PathBuf);

    impl Drop for CleanupDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
}
