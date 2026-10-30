// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Git-LFS pointer and oid parsing, following hgext/lfs/pointer.py.

use std::fmt;

use crate::errors::Result;

const VERSION: &str = "https://git-lfs.github.com/spec/v1";

/// A sha256 object id: 64 lowercase hex characters, so it is safe in paths.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Oid(String);

impl Oid {
    pub fn parse(s: &str) -> Result<Oid> {
        if s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
            Ok(Oid(s.to_owned()))
        } else {
            Err(format!("invalid lfs oid: {:?}", s).into())
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Relative store location, `<2 hex>/<62 hex>`.
    pub fn store_path(&self) -> String {
        format!("{}/{}", &self.0[..2], &self.0[2..])
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn sha256_oid(digest: &[u8; 32]) -> Oid {
    Oid(digest.iter().map(|b| format!("{:02x}", b)).collect())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pointer {
    pub oid: Oid,
    pub size: u64,
}

impl Pointer {
    pub fn parse(text: &[u8]) -> Result<Pointer> {
        let bad = |why: &str| format!("invalid lfs pointer: {}", why);
        let text = std::str::from_utf8(text).map_err(|_| bad("not utf-8"))?;
        let mut lines = text.split_terminator('\n');

        let version = lines.next().and_then(|l| l.strip_prefix("version "));
        if version != Some(VERSION) {
            return Err(bad("missing or unknown version").into());
        }

        let (mut oid, mut size, mut prev) = (None, None, "");
        for line in lines {
            let (key, value) = line.split_once(' ').ok_or_else(|| bad("line without value"))?;
            if key.is_empty()
                || !key.bytes().all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-'))
            {
                return Err(bad("bad key").into());
            }
            if key <= prev {
                return Err(bad("keys not sorted").into());
            }
            prev = key;
            match key {
                "version" => return Err(bad("repeated version").into()),
                "oid" => {
                    let hex = value.strip_prefix("sha256:").ok_or_else(|| bad("bad oid"))?;
                    oid = Some(Oid::parse(hex)?);
                }
                "size" => {
                    if !value.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(bad("bad size").into());
                    }
                    size = Some(value.parse().map_err(|_| bad("bad size"))?);
                }
                _ => {}
            }
        }
        match (oid, size) {
            (Some(oid), Some(size)) => Ok(Pointer { oid, size }),
            _ => Err(bad("missing oid or size").into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const OID: &str = "ab0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcd";

    #[test]
    fn pointer_parses_hg_output() {
        let text = format!(
            "version https://git-lfs.github.com/spec/v1\noid sha256:{}\nsize 12\nx-is-binary 0\n",
            OID
        );
        let p = Pointer::parse(text.as_bytes()).unwrap();
        assert_eq!(p.oid, Oid::parse(OID).unwrap());
        assert_eq!(p.size, 12);
    }

    #[test]
    fn pointer_rejects_malformed() {
        let v = "version https://git-lfs.github.com/spec/v1\n";
        let upper = OID.to_uppercase();
        let cases = [
            format!("oid sha256:{}\nsize 12\n", OID),
            format!("oid sha256:{}\n{}size 12\n", OID, v),
            format!("{}size 12\noid sha256:{}\n", v, OID),
            format!("{}oid sha256:{}\nsize 12\n", v, upper),
            format!("{}oid sha256:{}\nsize 12\n", v, &OID[1..]),
            format!("{}oid sha256:{}\nsize 12a\n", v, OID),
            format!("{}oid sha256:{}\n", v, OID),
            format!("{}oid sha256:{}\nsize 12\nbad_key 1\n", v, OID),
            format!("version https://hawser.github.com/spec/v1\noid sha256:{}\nsize 12\n", OID),
            format!("{}oid sha256:{}\nsize 12\n{}", v, OID, v),
            String::new(),
        ];
        for c in &cases {
            assert!(Pointer::parse(c.as_bytes()).is_err(), "accepted {:?}", c);
        }
    }

    #[test]
    fn oid_parse_rejects_path_tricks() {
        assert!(Oid::parse(&format!("../{}", &OID[..61])).is_err());
        assert!(Oid::parse(&format!("ab/{}", &OID[3..])).is_err());
        assert!(Oid::parse(&OID.to_uppercase()).is_err());
        let oid = Oid::parse(OID).unwrap();
        assert_eq!(oid.store_path(), format!("ab/{}", &OID[2..]));
        assert_eq!(sha256_oid(&[0xab; 32]).as_str(), "ab".repeat(32));
    }
}
