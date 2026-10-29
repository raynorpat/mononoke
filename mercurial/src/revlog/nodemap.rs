// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Reader for Mercurial's persistent nodemap: a `.n` docket naming a `.nd`
//! file that holds a trie from node prefixes to revisions.

use std::path::Path;

use mercurial_types::NodeHash;

use super::Datafile;
use crate::errors::Result;

const BLOCK_SIZE: usize = 64;

pub(super) struct Docket {
    pub uid: Vec<u8>,
    pub tip_rev: u64,
    pub data_length: u64,
    pub tip_node: Vec<u8>,
}

/// Parse `00changelog.n`: a version byte `1`, then `>BQQQQ` (uid length, tip
/// rev, data length, unused, tip node length), the uid and the tip node.
pub(super) fn parse_docket(bytes: &[u8]) -> Option<Docket> {
    let (&version, rest) = bytes.split_first()?;
    if version != 1 || rest.len() < 33 {
        return None;
    }
    let uid_len = rest[0] as usize;
    let field = |n: usize| u64::from_be_bytes(rest[1 + 8 * n..9 + 8 * n].try_into().unwrap());
    let tip_node_len = field(3) as usize;
    let body = &rest[33..];
    if body.len() < uid_len.checked_add(tip_node_len)? {
        return None;
    }
    Some(Docket {
        uid: body[..uid_len].to_vec(),
        tip_rev: field(0),
        data_length: field(1),
        tip_node: body[uid_len..uid_len + tip_node_len].to_vec(),
    })
}

/// The uid a `.n` docket names its data file by.
pub fn docket_uid(bytes: &[u8]) -> Option<Vec<u8>> {
    parse_docket(bytes).map(|docket| docket.uid)
}

#[derive(Debug)]
pub(super) struct PersistentNodeMap {
    pub trie: Datafile,
    pub data_length: usize,
    pub tip_rev: u32,
}

impl PersistentNodeMap {
    /// The revision the trie holds for `node`'s prefix. It may be another
    /// node sharing that prefix, so the caller checks the entry.
    pub fn candidate(&self, node: &NodeHash) -> Option<u32> {
        let data = self.trie.as_slice().get(..self.data_length)?;
        let mut block = (data.len() / BLOCK_SIZE).checked_sub(1)?;
        for &byte in node.sha1().as_ref() {
            for nibble in [byte >> 4, byte & 0xf] {
                let at = block * BLOCK_SIZE + nibble as usize * 4;
                let v = i32::from_be_bytes(data.get(at..at + 4)?.try_into().unwrap());
                match v {
                    -1 => return None,
                    0.. => block = v as usize,
                    _ => return Some((-(v as i64 + 2)) as u32),
                }
            }
        }
        None
    }
}

/// Read the docket beside `idx_path` and the data file it names, if both exist.
pub(super) fn load(idx_path: &Path) -> Result<Option<(Docket, Datafile)>> {
    let Ok(bytes) = std::fs::read(idx_path.with_extension("n")) else {
        return Ok(None);
    };
    let Some(docket) = parse_docket(&bytes) else {
        return Ok(None);
    };
    let (Some(dir), Some(stem)) = (idx_path.parent(), idx_path.file_stem()) else {
        return Ok(None);
    };
    let mut name = stem.to_os_string();
    name.push("-");
    name.push(String::from_utf8_lossy(&docket.uid).as_ref());
    name.push(".nd");
    match Datafile::map(dir.join(name)) {
        Ok(data) => Ok(Some((docket, data))),
        Err(_) => Ok(None),
    }
}
