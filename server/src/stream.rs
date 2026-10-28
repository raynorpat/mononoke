// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Stream clone: the store files as they are on disk, in a bundle2 `stream2` part.
//!
//! The client writes each file straight into its store, so a clone costs one
//! file write per store file instead of a revlog append per revision.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::PathBuf;

use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use mercurial::revlogrepo::Required;
use mercurial::RevlogRepo;
use mercurial_bundles::part_encode::PartEncodeBuilder;
use mercurial_bundles::{Chunk, Error as BundleError};
use mercurial_types::path::decodedir;
use mercurial_types::{percent_encode, Path};

use crate::bundle::read_in_order;
use crate::errors::{Error, Result};

/// Files up to this size are read whole on the read threads. Larger ones (the
/// changelog, the manifest and a few hundred filelogs) are sent in pieces of
/// this size. With the read batch and the queue this bounds memory to about 512 MiB.
const PIECE: usize = 1 << 20;
/// Pieces queued between the file reader and the bundle writer.
const PIECE_QUEUE: usize = 256;

/// A store file as listed: its store-relative name before encoding (what the
/// client writes through its own store encoding), its path, and its size.
pub struct StoreFile {
    name: Vec<u8>,
    path: PathBuf,
    size: u64,
}

/// The `streamreqs` capability value, or `None` when this repo cannot be listed
/// for a stream clone. Store encoding and working-copy requirements are left
/// out, as Mercurial does, because the client applies its own.
pub fn stream_requirements(repo: &RevlogRepo) -> Option<String> {
    let reqs = repo.get_requirements();
    // The store is listed through fncache.
    if !reqs.contains(&Required::Store) || !reqs.contains(&Required::Fncache) {
        return None;
    }
    let mut names: Vec<String> = reqs
        .iter()
        .filter(|req| {
            !matches!(
                req,
                Required::Store | Required::Fncache | Required::Dotencode | Required::ShareSafe
            )
        })
        .map(ToString::to_string)
        .collect();
    names.sort();
    Some(names.join(","))
}

/// Every store file with its size: filelogs, then the manifest, then the
/// changelog, in the order Mercurial sends them.
///
/// Sizes come from one walk of the store directories, because the first open
/// of each of a million files costs milliseconds. The caller holds off pushes
/// while this runs so the sizes are one snapshot.
pub fn list_store(repo: &RevlogRepo) -> Result<Vec<StoreFile>> {
    let store = repo.store_path();
    let mut sizes = HashMap::new();
    for top in ["data", "dh"] {
        walk(&store, &store.join(top), &mut sizes)?;
    }

    let fncache = fs::read(store.join("fncache"))?;
    let mut paths = BTreeSet::new();
    for line in fncache
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        // fncache keeps `encodedir` names, such as `changelog.d.hg/` for a `changelog.d/` directory.
        let line = decodedir(line);
        let path = line
            .strip_prefix(b"data/")
            .and_then(|rest| {
                rest.strip_suffix(b".i")
                    .or_else(|| rest.strip_suffix(b".d"))
            })
            .ok_or_else(|| {
                Error::from(format!(
                    "stream clone: unexpected fncache entry {}",
                    String::from_utf8_lossy(&line)
                ))
            })?;
        paths.insert(path.to_vec());
    }

    let dotencode = repo.get_requirements().contains(&Required::Dotencode);
    let mut files = Vec::with_capacity(paths.len() + 4);
    for path in paths {
        let filelog = Path::new(&path).map_err(|err| Error::from(err.to_string()))?;
        for ext in [&b"i"[..], b"d"] {
            let rel = filelog.store_filelog(dotencode, ext);
            let Some(&size) = sizes.get(&rel) else {
                if ext == b"i" {
                    return Err(format!(
                        "stream clone: fncache lists {} but it is missing",
                        rel.display()
                    )
                    .into());
                }
                continue;
            };
            let mut name = b"data/".to_vec();
            name.extend_from_slice(&path);
            name.push(b'.');
            name.extend_from_slice(ext);
            files.push(StoreFile {
                name,
                path: store.join(rel),
                size,
            });
        }
    }

    for name in [
        "00manifest.i",
        "00manifest.d",
        "00changelog.i",
        "00changelog.d",
    ] {
        let path = store.join(name);
        match fs::metadata(&path) {
            Ok(meta) => files.push(StoreFile {
                name: name.as_bytes().to_vec(),
                path,
                size: meta.len(),
            }),
            // An inline revlog has no `.d`.
            Err(err) if err.kind() == io::ErrorKind::NotFound && name.ends_with(".d") => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(files)
}

/// Record the size of every file under `dir`, keyed by its path relative to `store`.
fn walk(
    store: &std::path::Path,
    dir: &std::path::Path,
    sizes: &mut HashMap<PathBuf, u64>,
) -> Result<()> {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        // A store with no long paths has no `dh`.
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.into()),
    };
    for entry in entries {
        let entry = entry?;
        // On Windows this comes from the directory listing, without opening the file.
        let meta = entry.metadata()?;
        let path = entry.path();
        if meta.is_dir() {
            walk(store, &path, sizes)?;
        } else {
            let rel = path
                .strip_prefix(store)
                .expect("walked under the store")
                .to_path_buf();
            sizes.insert(rel, meta.len());
        }
    }
    Ok(())
}

/// The `stream2` part for `files`, read on the blocking pool as it is written.
///
/// Each file is sent at its listed size. A file that has become shorter fails
/// the clone, as in Mercurial: a push split or rewrote that revlog after the
/// listing. Longer files are fine because revlogs only grow by appending.
pub fn stream_part(files: Vec<StoreFile>, requirements: &str) -> Result<PartEncodeBuilder> {
    let bytecount: u64 = files.iter().map(|file| file.size).sum();
    let mut builder = PartEncodeBuilder::mandatory("stream2").map_err(bundle_err)?;
    builder
        .add_mparam("requirements", percent_encode(requirements))
        .map_err(bundle_err)?;
    builder
        .add_mparam("filecount", files.len().to_string())
        .map_err(bundle_err)?;
    builder
        .add_mparam("bytecount", bytecount.to_string())
        .map_err(bundle_err)?;

    let (tx, mut rx) = tokio::sync::mpsc::channel(PIECE_QUEUE);
    tokio::task::spawn_blocking(move || {
        let sent = send_files(&files, &mut |bytes| {
            tx.blocking_send(Ok(bytes))
                .map_err(|_| Error::from("getbundle writer went away"))
        });
        if let Err(err) = sent {
            let _ = tx.blocking_send(Err(BundleError::from(err.to_string())));
        }
    });
    let pieces = futures::stream::poll_fn(move |cx| rx.poll_recv(cx));
    builder.set_data_generated(pieces.map(|piece| piece.and_then(Chunk::new)));
    Ok(builder)
}

fn bundle_err(err: BundleError) -> Error {
    Error::from(err.to_string())
}

/// Send each file as Mercurial's stream v2 entry: `s` (the store), the name
/// length and data length as uvarints, the name, then the data.
fn send_files(files: &[StoreFile], send: &mut dyn FnMut(Bytes) -> Result<()>) -> Result<()> {
    let mut listed = files.iter();
    read_in_order(
        files,
        |file| {
            if file.size as usize <= PIECE {
                read_listed(file).map(Some)
            } else {
                Ok(None)
            }
        },
        &mut |data: Option<Vec<u8>>| {
            let file = listed.next().expect("one result per file");
            let mut entry = entry_header(file);
            match data {
                Some(data) => {
                    entry.extend_from_slice(&data);
                    send(entry.freeze())
                }
                None => {
                    send(entry.freeze())?;
                    send_in_pieces(file, send)
                }
            }
        },
    )
}

fn entry_header(file: &StoreFile) -> BytesMut {
    let mut out = BytesMut::with_capacity(file.name.len() + 21);
    out.put_u8(b's');
    put_uvarint(&mut out, file.name.len() as u64);
    put_uvarint(&mut out, file.size);
    out.extend_from_slice(&file.name);
    out
}

fn read_listed(file: &StoreFile) -> Result<Vec<u8>> {
    let mut data = Vec::with_capacity(file.size as usize);
    File::open(&file.path)?
        .take(file.size)
        .read_to_end(&mut data)?;
    check_size(file, data.len() as u64)?;
    Ok(data)
}

fn send_in_pieces(file: &StoreFile, send: &mut dyn FnMut(Bytes) -> Result<()>) -> Result<()> {
    let mut reader = File::open(&file.path)?.take(file.size);
    let mut sent = 0;
    loop {
        let mut piece = Vec::with_capacity(PIECE);
        let read = (&mut reader).take(PIECE as u64).read_to_end(&mut piece)?;
        if read == 0 {
            return check_size(file, sent);
        }
        sent += read as u64;
        send(Bytes::from(piece))?;
    }
}

fn check_size(file: &StoreFile, read: u64) -> Result<()> {
    if read == file.size {
        return Ok(());
    }
    Err(format!(
        "clone could only read {read} bytes from {}, but expected {} bytes",
        String::from_utf8_lossy(&file.name),
        file.size
    )
    .into())
}

/// Mercurial's `util.uvarintencode`: seven bits a byte, low bits first, with
/// the high bit set on every byte but the last.
fn put_uvarint(out: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        out.put_u8((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    out.put_u8(value as u8);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uvarint(value: u64) -> Vec<u8> {
        let mut out = BytesMut::new();
        put_uvarint(&mut out, value);
        out.to_vec()
    }

    #[test]
    fn uvarint_matches_mercurial() {
        // Values from the `util.uvarintencode` docstring.
        assert_eq!(uvarint(0), b"\x00");
        assert_eq!(uvarint(1), b"\x01");
        assert_eq!(uvarint(127), b"\x7f");
        assert_eq!(uvarint(1337), b"\xb9\n");
        assert_eq!(uvarint(65536), b"\x80\x80\x04");
    }

    #[test]
    fn shorter_file_than_listed_fails() {
        let dir = std::env::temp_dir().join(format!("mononoke-stream-{}", std::process::id()));
        fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("short.i");
        fs::write(&path, b"abc").expect("write");
        let file = StoreFile {
            name: b"data/short.i".to_vec(),
            path,
            size: 5,
        };
        let err = read_listed(&file).expect_err("short read");
        assert_eq!(
            err.to_string(),
            "clone could only read 3 bytes from data/short.i, but expected 5 bytes"
        );
        let mut sent = Vec::new();
        let err = send_in_pieces(&file, &mut |bytes| {
            sent.push(bytes);
            Ok(())
        })
        .expect_err("short piece");
        assert!(err.to_string().starts_with("clone could only read 3 bytes"));
        fs::remove_dir_all(&dir).expect("cleanup");
    }
}
