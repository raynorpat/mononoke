// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::fmt::Display;
use std::io::{self, Write};

use bytes::{BufMut, Bytes, BytesMut};

use crate::batch;
use crate::Response;

fn separated<I, W>(write: &mut W, iter: I, sep: &str) -> io::Result<()>
where
    I: IntoIterator,
    I::Item: Display,
    W: Write,
{
    let iter = iter.into_iter();

    let mut first = true;
    for it in iter {
        if first {
            first = false;
        } else {
            write!(write, "{}", sep)?;
        }
        write!(write, "{}", it)?;
    }
    write!(write, "\n")?;
    Ok(())
}

pub fn encode(response: &Response, out: &mut BytesMut) {
    let res = encode_cmd(response);
    out.reserve(10 + res.len());
    if !response.is_stream() {
        out.extend_from_slice(format!("{}\n", res.len()).as_bytes());
    }
    out.put(res);
}

/// Encode the result of an individual command completion. This is used by both
/// encode and batch encoding.
pub fn encode_cmd(response: &Response) -> Bytes {
    match response {
        Response::Batch(results) => {
            let escaped_results: Vec<_> = results.iter().map(batch::escape).collect();
            Bytes::from(escaped_results.join(&b";"[..]))
        }

        Response::Hello(map) => {
            let mut out = Vec::new();

            for (k, caps) in map.iter() {
                write!(out, "{}: {}\n", k, caps.join(" ")).expect("write to vec failed");
            }

            Bytes::from(out)
        }

        Response::Between(vecs) => {
            let mut out = Vec::new();

            for v in vecs {
                separated(&mut out, v, " ").expect("write to vec failed");
            }

            Bytes::from(out)
        }

        Response::Debugwireargs(res) | Response::Unbundle(res) => res.clone(),

        // Callers write the stream themselves, and batch rejects getbundle.
        Response::Getbundle(_) => unreachable!("getbundle is streamed, not encoded"),

        Response::Heads(set) => {
            let mut out = Vec::new();

            separated(&mut out, set, " ").expect("write to vec failed");

            Bytes::from(out)
        }

        Response::Known(knowns) => {
            let out: Vec<_> = knowns
                .iter()
                .map(|known| if *known { b'1' } else { b'0' })
                .collect();

            Bytes::from(out)
        }

        Response::Pushkey(ok) => Bytes::from_static(if *ok { b"1\n" } else { b"0\n" }),

        Response::Listkeys(pairs) => {
            let mut names: Vec<_> = pairs.iter().collect();
            names.sort_by(|left, right| left.0.cmp(right.0));
            let mut out = Vec::new();
            for (key, value) in names {
                out.extend_from_slice(key);
                out.push(b'\t');
                out.extend_from_slice(value);
                out.push(b'\n');
            }
            Bytes::from(out)
        }

        // Raw branch lines. `encode` adds the SSH length prefix.
        Response::Branchmap(map) => {
            let mut names: Vec<_> = map.keys().collect();
            names.sort();
            let mut out = Vec::new();
            for name in names {
                out.extend_from_slice(name.as_bytes());
                let mut nodes: Vec<_> = map[name].iter().collect();
                nodes.sort();
                for node in nodes {
                    out.push(b' ');
                    let hex = node.to_hex();
                    out.extend_from_slice(hex.as_str().as_bytes());
                }
                out.push(b'\n');
            }
            Bytes::from(out)
        }

        other => panic!("Response for {:?} unimplemented", other),
    }
}
