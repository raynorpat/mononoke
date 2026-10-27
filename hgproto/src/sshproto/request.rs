// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Parsers for the Mercurial SSH wire protocol.
//!
//! Incomplete input is `WireErr::Incomplete` so the codec can wait for more
//! bytes. A hard mismatch is an error. Parameter values are length-delimited,
//! so parsers of those values treat their input as complete.

use std::collections::HashMap;
use std::str;

use bytes::{Bytes, BytesMut};
use mercurial_types::NodeHash;

use crate::batch;
use crate::errors::{Error, Result};
use crate::{GetbundleArgs, Request};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
enum WireErr {
    #[error("incomplete")]
    Incomplete,
    #[error("digit")]
    Digit,
    #[error("alphanumeric")]
    AlphaNumeric,
    #[error("alt")]
    Alt,
    #[error("mapres")]
    MapRes,
    #[error("tag")]
    Tag,
    #[error("eof")]
    Eof,
    #[error("custom")]
    Custom,
}

impl From<WireErr> for Error {
    fn from(err: WireErr) -> Self {
        Error::Msg(err.to_string())
    }
}

type ParseRes<'a, T> = std::result::Result<(&'a [u8], T), WireErr>;

type ParamParser = for<'a> fn(&'a [u8], usize) -> ParseRes<'a, HashMap<Vec<u8>, Vec<u8>>>;

fn tag<'a>(input: &'a [u8], expected: &[u8]) -> ParseRes<'a, ()> {
    if input.len() < expected.len() {
        if expected.starts_with(input) {
            return Err(WireErr::Incomplete);
        }
        return Err(WireErr::Tag);
    }
    if input.starts_with(expected) {
        Ok((&input[expected.len()..], ()))
    } else {
        Err(WireErr::Tag)
    }
}

fn take(input: &[u8], len: usize) -> ParseRes<'_, &[u8]> {
    if input.len() < len {
        Err(WireErr::Incomplete)
    } else {
        Ok((&input[len..], &input[..len]))
    }
}

fn eof(input: &[u8]) -> ParseRes<'_, ()> {
    if input.is_empty() {
        Ok((input, ()))
    } else {
        Err(WireErr::Eof)
    }
}

/// Parse an unsigned decimal integer. Reaching the end of input is incomplete:
/// more digits may follow.
fn integer(input: &[u8]) -> ParseRes<'_, usize> {
    if input.is_empty() {
        return Err(WireErr::Incomplete);
    }
    match input.iter().position(|b| !b.is_ascii_digit()) {
        None => Err(WireErr::Incomplete),
        Some(0) => Err(WireErr::Digit),
        Some(idx) => {
            let text = str::from_utf8(&input[..idx]).map_err(|_| WireErr::MapRes)?;
            let value = text.parse().map_err(|_| WireErr::MapRes)?;
            Ok((&input[idx..], value))
        }
    }
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_cont(b: u8) -> bool {
    is_ident_start(b) || b.is_ascii_digit()
}

/// Return an identifier of the form [a-zA-Z_][a-zA-Z0-9_]*. Reaching the end of
/// input is incomplete, as more identifier may follow.
fn ident(input: &[u8]) -> ParseRes<'_, &[u8]> {
    if input.is_empty() {
        return Err(WireErr::Incomplete);
    }
    if !is_ident_start(input[0]) {
        return Err(WireErr::AlphaNumeric);
    }
    let mut idx = 1;
    while idx < input.len() {
        if !is_ident_cont(input[idx]) {
            return Ok((&input[idx..], &input[..idx]));
        }
        idx += 1;
    }
    Err(WireErr::Incomplete)
}

/// As `ident`, but input is complete, so the end of input ends the identifier.
fn ident_complete(input: &[u8]) -> ParseRes<'_, &[u8]> {
    match ident(input) {
        Err(WireErr::Incomplete) => Ok((b"", input)),
        other => other,
    }
}

fn ident_string(input: &[u8]) -> ParseRes<'_, String> {
    let (rest, name) = ident_complete(input)?;
    Ok((rest, String::from_utf8_lossy(name).into_owned()))
}

/// A "*" parameter's argument is a count of following parameters.
fn param_star(input: &[u8]) -> ParseRes<'_, HashMap<Vec<u8>, Vec<u8>>> {
    let (input, _) = tag(input, b"* ")?;
    let (input, count) = integer(input)?;
    let (input, _) = tag(input, b"\n")?;
    params(input, count)
}

/// A named parameter: `ident <bytelen>\n` followed by that many value bytes.
fn param_kv(input: &[u8]) -> ParseRes<'_, HashMap<Vec<u8>, Vec<u8>>> {
    let (input, key) = ident(input)?;
    let (input, _) = tag(input, b" ")?;
    let (input, len) = integer(input)?;
    let (input, _) = tag(input, b"\n")?;
    let (input, val) = take(input, len)?;
    let mut map = HashMap::with_capacity(1);
    map.insert(key.to_vec(), val.to_vec());
    Ok((input, map))
}

/// SSH protocol params. `count` is the number of required parameters, including
/// a "*" parameter but not the parameters that "*" expands to.
fn params(input: &[u8], count: usize) -> ParseRes<'_, HashMap<Vec<u8>, Vec<u8>>> {
    let mut input = input;
    let mut have = 0usize;
    let mut ret = HashMap::with_capacity(count);
    while have < count {
        let (rest, kv, n) = match param_star(input) {
            Ok((rest, kv)) => (rest, kv, 1),
            Err(WireErr::Incomplete) => return Err(WireErr::Incomplete),
            Err(_) => match param_kv(input) {
                Ok((rest, kv)) => {
                    let n = kv.len();
                    (rest, kv, n)
                }
                Err(WireErr::Incomplete) => return Err(WireErr::Incomplete),
                Err(_) => return Err(WireErr::Alt),
            },
        };
        for (k, v) in kv {
            ret.insert(k, v);
        }
        have += n;
        input = rest;
    }
    Ok((input, ret))
}

fn batch_param_escaped(input: &[u8]) -> ParseRes<'_, (Vec<u8>, Vec<u8>)> {
    if input.is_empty() {
        return Err(WireErr::Incomplete);
    }
    let eq = match input.iter().position(|b| *b == b'=') {
        Some(0) => return Err(WireErr::Tag),
        Some(idx) => idx,
        None => return Err(WireErr::Incomplete),
    };
    let key = &input[..eq];
    let after = &input[eq + 1..];
    let val_len = after.iter().position(|b| *b == b',').unwrap_or(after.len());
    let val = &after[..val_len];
    let rest = &after[val_len..];
    let key = batch::unescape(key).map_err(|_| WireErr::MapRes)?;
    let val = batch::unescape(val).map_err(|_| WireErr::MapRes)?;
    Ok((rest, (key, val)))
}

/// Batch parameters are a comma-delimited list. `count` is unused: there are
/// no star params in batch syntax, and the bytes are already length-delimited.
fn batch_params(input: &[u8], _count: usize) -> ParseRes<'_, HashMap<Vec<u8>, Vec<u8>>> {
    if input.is_empty() {
        return Ok((input, HashMap::new()));
    }
    let mut rest = input;
    let mut out = HashMap::new();
    loop {
        let (next, (key, val)) = match batch_param_escaped(rest) {
            Ok(parsed) => parsed,
            Err(WireErr::Incomplete) => return Err(WireErr::Tag),
            Err(err) => return Err(err),
        };
        out.insert(key, val);
        if next.is_empty() {
            return Ok((next, out));
        }
        let (after, _) = match tag(next, b",") {
            Ok(parsed) => parsed,
            Err(WireErr::Incomplete) => return Err(WireErr::Tag),
            Err(err) => return Err(err),
        };
        rest = after;
    }
}

/// A nodehash is 40 hex digits.
fn nodehash(input: &[u8]) -> ParseRes<'_, NodeHash> {
    let (rest, hex) = take(input, 40)?;
    let text = str::from_utf8(hex).map_err(|_| WireErr::MapRes)?;
    let hash = text.parse().map_err(|_| WireErr::MapRes)?;
    Ok((rest, hash))
}

fn pair(input: &[u8]) -> ParseRes<'_, (NodeHash, NodeHash)> {
    let (input, left) = nodehash(input)?;
    let (input, _) = tag(input, b"-")?;
    let (input, right) = nodehash(input)?;
    Ok((input, (left, right)))
}

fn pairlist(input: &[u8]) -> ParseRes<'_, Vec<(NodeHash, NodeHash)>> {
    separated(input, b" ", pair)
}

fn hashlist(input: &[u8]) -> ParseRes<'_, Vec<NodeHash>> {
    separated(input, b" ", nodehash)
}

fn separated<'a, T, F>(input: &'a [u8], sep: &[u8], mut one: F) -> ParseRes<'a, Vec<T>>
where
    F: FnMut(&'a [u8]) -> ParseRes<'a, T>,
{
    if input.is_empty() {
        return Ok((input, Vec::new()));
    }
    let mut rest = input;
    let mut out = Vec::new();
    loop {
        match one(rest) {
            Ok((next, val)) => {
                out.push(val);
                rest = next;
            }
            Err(WireErr::Incomplete) => return Err(WireErr::Incomplete),
            Err(_) if out.is_empty() => return Ok((input, Vec::new())),
            Err(err) => return Err(err),
        }
        if rest.is_empty() {
            return Ok((rest, out));
        }
        match tag(rest, sep) {
            Ok((next, _)) => rest = next,
            Err(WireErr::Incomplete) => return Err(WireErr::Tag),
            Err(_) => return Ok((rest, out)),
        }
    }
}

/// Comma-separated arbitrary values. The input is complete and exact.
fn commavalues(input: &[u8]) -> ParseRes<'_, Vec<Vec<u8>>> {
    if input.is_empty() {
        // `split` would yield a single empty element.
        Ok((b"", Vec::new()))
    } else {
        Ok((
            b"",
            input
                .split(|c| *c == b',')
                .map(|val| val.to_vec())
                .collect(),
        ))
    }
}

fn cmd(input: &[u8]) -> ParseRes<'_, (Vec<u8>, Vec<u8>)> {
    let sp = match input.iter().position(|b| *b == b' ') {
        Some(0) => return Err(WireErr::Tag),
        Some(idx) => idx,
        None => return Err(WireErr::Incomplete),
    };
    let name = input[..sp].to_vec();
    let after = &input[sp + 1..];
    let end = after.iter().position(|b| *b == b';').unwrap_or(after.len());
    Ok((&after[end..], (name, after[..end].to_vec())))
}

fn cmdlist(input: &[u8]) -> ParseRes<'_, Vec<(Vec<u8>, Vec<u8>)>> {
    separated(input, b";", cmd)
}

fn parseval<'a, F, T>(params: &'a HashMap<Vec<u8>, Vec<u8>>, key: &str, parser: F) -> Result<T>
where
    F: Fn(&'a [u8]) -> ParseRes<'a, T>,
{
    match params.get(key.as_bytes()) {
        None => crate::bail!("missing param {}", key),
        Some(val) => match parser(val.as_ref()) {
            Ok((rest, parsed)) => match eof(rest) {
                Ok(_) => Ok(parsed),
                Err(_) => crate::bail!("Unconsumed characters remain after parsing param"),
            },
            Err(WireErr::Incomplete) => {
                crate::bail!("param parse incomplete: {:?}", WireErr::Incomplete)
            }
            Err(err) => crate::bail!("param parse failed: {:?}", err),
        },
    }
}

fn parseval_default<'a, F, T>(
    params: &'a HashMap<Vec<u8>, Vec<u8>>,
    key: &str,
    parser: F,
) -> Result<T>
where
    F: Fn(&'a [u8]) -> ParseRes<'a, T>,
    T: Default,
{
    match params.get(key.as_bytes()) {
        None => Ok(T::default()),
        Some(val) => match parser(val.as_ref()) {
            Ok((rest, parsed)) => match eof(rest) {
                Ok(_) => Ok(parsed),
                Err(_) => crate::bail!(
                    "Unconsumed characters remain after parsing param: {:?}",
                    rest
                ),
            },
            Err(WireErr::Incomplete) => {
                crate::bail!("param parse incomplete: {:?}", WireErr::Incomplete)
            }
            Err(err) => crate::bail!("param parse failed: {:?}", err),
        },
    }
}

fn parse_named<'a, F>(
    input: &'a [u8],
    name: &[u8],
    parse_params: ParamParser,
    nargs: usize,
    build: F,
) -> ParseRes<'a, Request>
where
    F: FnOnce(HashMap<Vec<u8>, Vec<u8>>) -> Result<Request>,
{
    let (input, _) = tag(input, name)?;
    let (input, _) = tag(input, b"\n")?;
    let (rest, kv) = parse_params(input, nargs)?;
    match build(kv) {
        Ok(req) => Ok((rest, req)),
        Err(_) => Err(WireErr::Custom),
    }
}

fn attempt<'a>(
    parsed: ParseRes<'a, Request>,
) -> std::result::Result<Option<(&'a [u8], Request)>, WireErr> {
    match parsed {
        Ok(val) => Ok(Some(val)),
        Err(WireErr::Incomplete) => Err(WireErr::Incomplete),
        Err(_) => Ok(None),
    }
}

fn parse_request<'a>(input: &'a [u8], parse_params: ParamParser) -> ParseRes<'a, Request> {
    if let Some(val) = attempt(parse_named(input, b"batch", parse_params, 2, |kv| {
        Ok(Request::Batch {
            cmds: parseval(&kv, "cmds", cmdlist)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"between", parse_params, 1, |kv| {
        Ok(Request::Between {
            pairs: parseval(&kv, "pairs", pairlist)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"branchmap", parse_params, 0, |_| {
        Ok(Request::Branchmap)
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"branches", parse_params, 1, |kv| {
        Ok(Request::Branches {
            nodes: parseval(&kv, "nodes", hashlist)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"clonebundles", parse_params, 0, |_| {
        Ok(Request::Clonebundles)
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"capabilities", parse_params, 0, |_| {
        Ok(Request::Capabilities)
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"changegroup", parse_params, 1, |kv| {
        Ok(Request::Changegroup {
            roots: parseval(&kv, "roots", hashlist)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(
        input,
        b"changegroupsubset",
        parse_params,
        2,
        |kv| {
            Ok(Request::Changegroupsubset {
                heads: parseval(&kv, "heads", hashlist)?,
                bases: parseval(&kv, "bases", hashlist)?,
            })
        },
    ))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(
        input,
        b"debugwireargs",
        parse_params,
        3,
        |kv| {
            let one = parseval(&kv, "one", ident_complete)?.to_vec();
            let two = parseval(&kv, "two", ident_complete)?.to_vec();
            Ok(Request::Debugwireargs {
                one,
                two,
                all_args: kv,
            })
        },
    ))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"getbundle", parse_params, 1, |kv| {
        Ok(Request::Getbundle(GetbundleArgs {
            // Some params are currently ignored, like:
            // - obsmarkers
            // - cg
            // - cbattempted
            // If those params are needed, they should be parsed here.
            heads: parseval_default(&kv, "heads", hashlist)?,
            common: parseval_default(&kv, "common", hashlist)?,
            bundlecaps: parseval_default(&kv, "bundlecaps", commavalues)?,
            listkeys: parseval_default(&kv, "listkeys", commavalues)?,
        }))
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"heads", parse_params, 0, |_| {
        Ok(Request::Heads)
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"hello", parse_params, 0, |_| {
        Ok(Request::Hello)
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"listkeys", parse_params, 1, |kv| {
        Ok(Request::Listkeys {
            namespace: parseval(&kv, "namespace", ident_string)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"lookup", parse_params, 1, |kv| {
        Ok(Request::Lookup {
            key: parseval(&kv, "key", ident_string)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"known", parse_params, 2, |kv| {
        Ok(Request::Known {
            nodes: parseval(&kv, "nodes", hashlist)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"pushkey", parse_params, 4, |kv| {
        Ok(Request::Pushkey {
            namespace: parseval(&kv, "namespace", ident_string)?,
            key: parseval(&kv, "key", ident_string)?,
            old: parseval(&kv, "old", nodehash)?,
            new: parseval(&kv, "new", nodehash)?,
        })
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"streamout", parse_params, 0, |_| {
        Ok(Request::Streamout)
    }))? {
        return Ok(val);
    }
    if let Some(val) = attempt(parse_named(input, b"unbundle", parse_params, 1, |kv| {
        Ok(Request::Unbundle {
            heads: parseval(&kv, "heads", hashlist)?,
            bundle: Bytes::new(),
        })
    }))? {
        return Ok(val);
    }
    Err(WireErr::Alt)
}

/// Parse a non-batched command.
pub fn parse(buf: &mut BytesMut) -> Result<Option<Request>> {
    parse_common(buf, params)
}

/// Parse a single batched command (with its parameters in batched form).
pub fn parse_batch(buf: &mut BytesMut) -> Result<Option<Request>> {
    parse_common(buf, batch_params)
}

fn parse_common(buf: &mut BytesMut, parse_params: ParamParser) -> Result<Option<Request>> {
    let outcome = match parse_request(&buf[..], parse_params) {
        Ok((rest, val)) => Ok((buf.len() - rest.len(), val)),
        Err(WireErr::Incomplete) => Err(None),
        Err(err) => Err(Some(err)),
    };
    match outcome {
        Ok((consume, val)) => {
            let _ = buf.split_to(consume);
            Ok(Some(val))
        }
        Err(None) => Ok(None),
        Err(Some(err)) => Err(Error::with_chain(
            err,
            Error::CommandParse(String::from_utf8_lossy(buf).into_owned()),
        )),
    }
}

/// Test individual combinators
#[cfg(test)]
mod test {
    use maplit::hashmap;
    use mercurial_types::nodehash;

    use super::*;

    #[test]
    fn test_integer() {
        assert_eq!(integer(b"1234 "), Ok((&b" "[..], 1234)));
        assert_eq!(integer(b"1234"), Err(WireErr::Incomplete));
    }

    #[test]
    fn test_ident() {
        assert_eq!(ident(b"1234 "), Err(WireErr::AlphaNumeric));
        assert_eq!(ident(b" 1234 "), Err(WireErr::AlphaNumeric));
        assert_eq!(ident(b"foo"), Err(WireErr::Incomplete));
        assert_eq!(ident(b"foo "), Ok((&b" "[..], &b"foo"[..])));
    }

    #[test]
    fn test_param_star() {
        let p = b"* 0\ntrailer";
        assert_eq!(param_star(p), Ok((&b"trailer"[..], HashMap::new())));

        let p = b"* 1\n\
                  foo 12\n\
                  hello world!trailer";
        assert_eq!(
            param_star(p),
            Ok((
                &b"trailer"[..],
                hashmap! {
                    b"foo".to_vec() => b"hello world!".to_vec(),
                }
            ))
        );

        let p = b"* 2\n\
                  foo 12\n\
                  hello world!\
                  bar 4\n\
                  bloptrailer";
        assert_eq!(
            param_star(p),
            Ok((
                &b"trailer"[..],
                hashmap! {
                    b"foo".to_vec() => b"hello world!".to_vec(),
                    b"bar".to_vec() => b"blop".to_vec(),
                }
            ))
        );

        let p = b"* 0\n";
        assert_eq!(param_star(p), Ok((&b""[..], HashMap::new())));

        let p = b"* 1\n\
                  foo 12\n\
                  hello world!";
        assert_eq!(
            param_star(p),
            Ok((
                &b""[..],
                hashmap! {
                    b"foo".to_vec() => b"hello world!".to_vec(),
                }
            ))
        );
    }

    #[test]
    fn test_param_kv() {
        let p = b"foo 12\n\
                  hello world!trailer";
        assert_eq!(
            param_kv(p),
            Ok((
                &b"trailer"[..],
                hashmap! {
                    b"foo".to_vec() => b"hello world!".to_vec(),
                }
            ))
        );

        let p = b"foo 12\n\
                  hello world!";
        assert_eq!(
            param_kv(p),
            Ok((
                &b""[..],
                hashmap! {
                    b"foo".to_vec() => b"hello world!".to_vec(),
                }
            ))
        );
    }

    #[test]
    fn test_params() {
        let p = b"bar 12\n\
                  hello world!\
                  foo 7\n\
                  blibble\
                  very_long_key_no_data 0\n\
                  is_ok 1\n\
                  y\n\
                  badly formatted thing ";

        match params(p, 1) {
            Ok((_, v)) => {
                assert_eq!(
                    v,
                    hashmap! {
                        b"bar".to_vec() => b"hello world!".to_vec(),
                    }
                )
            }
            bad => panic!("bad result {:?}", bad),
        }

        match params(p, 2) {
            Ok((_, v)) => {
                assert_eq!(
                    v,
                    hashmap! {
                        b"bar".to_vec() => b"hello world!".to_vec(),
                        b"foo".to_vec() => b"blibble".to_vec(),
                    }
                )
            }
            bad => panic!("bad result {:?}", bad),
        }

        match params(p, 4) {
            Ok((b"\nbadly formatted thing ", v)) => {
                assert_eq!(
                    v,
                    hashmap! {
                        b"bar".to_vec() => b"hello world!".to_vec(),
                        b"foo".to_vec() => b"blibble".to_vec(),
                        b"very_long_key_no_data".to_vec() => b"".to_vec(),
                        b"is_ok".to_vec() => b"y".to_vec(),
                    }
                )
            }
            bad => panic!("bad result {:?}", bad),
        }

        match params(p, 5) {
            Err(WireErr::Alt) => (),
            bad => panic!("bad result {:?}", bad),
        }

        match params(&p[..3], 1) {
            Err(WireErr::Incomplete) => (),
            bad => panic!("bad result {:?}", bad),
        }

        for l in 0..p.len() {
            match params(&p[..l], 4) {
                Err(WireErr::Incomplete) => (),
                Ok((remain, ref kv)) => {
                    assert_eq!(kv.len(), 4);
                    assert!(
                        b"\nbadly formatted thing ".starts_with(remain),
                        "remain \"{:?}\"",
                        remain
                    );
                }
                bad => panic!("bad result l {} bad {:?}", l, bad),
            }
        }
    }

    #[test]
    fn test_params_star() {
        let star = b"* 1\n\
                     foo 0\n\
                     bar 0\n";
        match params(star, 2) {
            Err(WireErr::Incomplete) => panic!("unexpectedly incomplete"),
            Ok((remain, kv)) => {
                assert_eq!(remain, b"");
                assert_eq!(
                    kv,
                    hashmap! {
                        b"foo".to_vec() => vec![],
                        b"bar".to_vec() => vec![],
                    }
                );
            }
            Err(err) => panic!("unexpected error {:?}", err),
        }

        let star = b"* 2\n\
                     foo 0\n\
                     plugh 0\n\
                     bar 0\n";
        match params(star, 2) {
            Err(WireErr::Incomplete) => panic!("unexpectedly incomplete"),
            Ok((remain, kv)) => {
                assert_eq!(remain, b"");
                assert_eq!(
                    kv,
                    hashmap! {
                        b"foo".to_vec() => vec![],
                        b"bar".to_vec() => vec![],
                        b"plugh".to_vec() => vec![],
                    }
                );
            }
            Err(err) => panic!("unexpected error {:?}", err),
        }

        let star = b"* 0\n\
                     bar 0\n";
        match params(star, 2) {
            Err(WireErr::Incomplete) => panic!("unexpectedly incomplete"),
            Ok((remain, kv)) => {
                assert_eq!(remain, b"");
                assert_eq!(
                    kv,
                    hashmap! {
                        b"bar".to_vec() => vec![],
                    }
                );
            }
            Err(err) => panic!("unexpected error {:?}", err),
        }

        match params(&star[..4], 2) {
            Err(WireErr::Incomplete) => (),
            Ok((remain, kv)) => panic!("unexpected Done remain {:?} kv {:?}", remain, kv),
            Err(err) => panic!("unexpected error {:?}", err),
        }
    }

    #[test]
    fn test_batch_param_escaped() {
        let p = b"foo=b:ear";

        assert_eq!(
            batch_param_escaped(p),
            Ok((&b""[..], (b"foo".to_vec(), b"b=ar".to_vec())))
        );
    }

    #[test]
    fn test_batch_params() {
        let p = b"foo=bar";

        assert_eq!(
            batch_params(p, 0),
            Ok((
                &b""[..],
                hashmap! {
                    b"foo".to_vec() => b"bar".to_vec(),
                }
            ))
        );

        let p = b"foo=bar,biff=bop,esc:c:o:s:e=esc:c:o:s:e";

        assert_eq!(
            batch_params(p, 0),
            Ok((
                &b""[..],
                hashmap! {
                    b"foo".to_vec() => b"bar".to_vec(),
                    b"biff".to_vec() => b"bop".to_vec(),
                    b"esc:,;=".to_vec() => b"esc:,;=".to_vec(),
                }
            ))
        );

        let p = b"";

        assert_eq!(batch_params(p, 0), Ok((&b""[..], HashMap::new())));
    }

    #[test]
    fn test_nodehash() {
        assert_eq!(
            nodehash(b"0000000000000000000000000000000000000000"),
            Ok((&b""[..], nodehash::NULL_HASH))
        );

        assert_eq!(
            nodehash(b"000000000000000000000000000000x000000000"),
            Err(WireErr::MapRes)
        );

        assert_eq!(
            nodehash(b"000000000000000000000000000000000000000"),
            Err(WireErr::Incomplete)
        );
    }

    #[test]
    fn test_parseval_extra_characters() {
        let kv = hashmap! {
            b"foo".to_vec() => b"0000000000000000000000000000000000000000extra".to_vec(),
        };
        match parseval(&kv, "foo", hashlist) {
            Err(_) => (),
            _ => {
                panic!(
                    "Paramval parse failed: Did not raise an error for param\
                         with trailing characters."
                )
            }
        }
    }

    #[test]
    fn test_parseval_default_extra_characters() {
        let kv = hashmap! {
            b"foo".to_vec() => b"0000000000000000000000000000000000000000extra".to_vec(),
        };
        match parseval_default(&kv, "foo", hashlist) {
            Err(_) => (),
            _ => {
                panic!(
                    "paramval_default parse failed: Did not raise an error for param\
                         with trailing characters."
                )
            }
        }
    }

    #[test]
    fn test_pair() {
        let p =
            b"0000000000000000000000000000000000000000-0000000000000000000000000000000000000000";
        assert_eq!(
            pair(p),
            Ok((&b""[..], (nodehash::NULL_HASH, nodehash::NULL_HASH)))
        );

        assert_eq!(pair(&p[..80]), Err(WireErr::Incomplete));
        assert_eq!(pair(&p[..41]), Err(WireErr::Incomplete));
        assert_eq!(pair(&p[..40]), Err(WireErr::Incomplete));
    }

    #[test]
    fn test_pairlist() {
        let p =
            b"0000000000000000000000000000000000000000-0000000000000000000000000000000000000000 \
              0000000000000000000000000000000000000000-0000000000000000000000000000000000000000";
        assert_eq!(
            pairlist(p),
            Ok((
                &b""[..],
                vec![
                    (nodehash::NULL_HASH, nodehash::NULL_HASH),
                    (nodehash::NULL_HASH, nodehash::NULL_HASH),
                ]
            ))
        );

        let p =
            b"0000000000000000000000000000000000000000-0000000000000000000000000000000000000000";
        assert_eq!(
            pairlist(p),
            Ok((&b""[..], vec![(nodehash::NULL_HASH, nodehash::NULL_HASH),]))
        );
    }

    #[test]
    fn test_hashlist() {
        let p =
            b"0000000000000000000000000000000000000000 0000000000000000000000000000000000000000 \
              0000000000000000000000000000000000000000 0000000000000000000000000000000000000000";
        assert_eq!(
            hashlist(p),
            Ok((
                &b""[..],
                vec![
                    nodehash::NULL_HASH,
                    nodehash::NULL_HASH,
                    nodehash::NULL_HASH,
                    nodehash::NULL_HASH,
                ]
            ))
        );

        let p = b"0000000000000000000000000000000000000000";
        assert_eq!(hashlist(p), Ok((&b""[..], vec![nodehash::NULL_HASH,])));
    }

    #[test]
    fn test_commavalues() {
        let p = b"";
        assert_eq!(commavalues(p), Ok((&b""[..], vec![])));

        let p = b"abc";
        assert_eq!(commavalues(p), Ok((&b""[..], vec![b"abc".to_vec()])));

        let p = b"123,abc,test,456";
        assert_eq!(
            commavalues(p),
            Ok((
                &b""[..],
                vec![
                    b"123".to_vec(),
                    b"abc".to_vec(),
                    b"test".to_vec(),
                    b"456".to_vec(),
                ]
            ))
        );
    }

    #[test]
    fn test_cmd() {
        let p = b"foo bar";
        assert_eq!(cmd(p), Ok((&b""[..], (b"foo".to_vec(), b"bar".to_vec()))));

        let p = b"noparam ";
        assert_eq!(cmd(p), Ok((&b""[..], (b"noparam".to_vec(), b"".to_vec()))));
    }

    #[test]
    fn test_cmdlist() {
        let p = b"foo bar";
        assert_eq!(
            cmdlist(p),
            Ok((&b""[..], vec![(b"foo".to_vec(), b"bar".to_vec()),]))
        );

        let p = b"foo bar;biff blop";
        assert_eq!(
            cmdlist(p),
            Ok((
                &b""[..],
                vec![
                    (b"foo".to_vec(), b"bar".to_vec()),
                    (b"biff".to_vec(), b"blop".to_vec()),
                ]
            ))
        );
    }
}

/// Test parsing each command
#[cfg(test)]
mod test_parse {
    use std::fmt::Display;

    use maplit::hashmap;

    use super::*;

    fn hash_ones() -> NodeHash {
        "1111111111111111111111111111111111111111".parse().unwrap()
    }

    fn hash_twos() -> NodeHash {
        "2222222222222222222222222222222222222222".parse().unwrap()
    }

    fn hash_threes() -> NodeHash {
        "3333333333333333333333333333333333333333".parse().unwrap()
    }

    fn hash_fours() -> NodeHash {
        "4444444444444444444444444444444444444444".parse().unwrap()
    }

    /// Common code for testing parsing:
    /// - check all truncated inputs return "Ok(None)"
    /// - complete inputs return the expected result, and leave any remainder in
    ///    the input buffer.
    fn test_parse<I: AsRef<[u8]> + Display>(inp: I, exp: Request) {
        let inbytes = inp.as_ref();

        for l in 0..inbytes.len() - 1 {
            let mut buf = BytesMut::from(&inbytes[0..l]);
            match parse(&mut buf) {
                Ok(None) => (),
                Ok(Some(val)) => {
                    panic!(
                        "BAD PASS: inp >>{}<< len {} passed unexpectedly val {:?}",
                        inp, l, val
                    )
                }
                Err(err) => {
                    panic!(
                        "BAD FAIL: inp >>{}<< len {} failed {:?} (not incomplete)",
                        inp, l, err
                    )
                }
            };
        }

        let extra = b"extra";
        for l in 0..extra.len() {
            let mut buf = BytesMut::from(inbytes);
            buf.extend_from_slice(&extra[0..l]);
            match parse(&mut buf) {
                Ok(Some(val)) => assert_eq!(val, exp),
                Ok(None) => panic!("BAD INCOMPLETE: inp >>{}<< extra {} incomplete", inp, l),
                Err(err) => {
                    panic!(
                        "BAD FAIL: inp >>{}<< extra {} failed {:?} (not incomplete)",
                        inp, l, err
                    )
                }
            };
            assert_eq!(&buf[..], &extra[0..l]);
        }
    }

    #[test]
    fn test_parse_batch() {
        let inp = "batch\n\
                   * 0\n\
                   cmds 6\n\
                   hello ";

        test_parse(
            inp,
            Request::Batch {
                cmds: vec![(b"hello".to_vec(), vec![])],
            },
        )
    }

    #[test]
    fn test_parse_between() {
        let inp = "between\n\
                   pairs 163\n\
                   1111111111111111111111111111111111111111-2222222222222222222222222222222222222222 \
                   3333333333333333333333333333333333333333-4444444444444444444444444444444444444444";
        test_parse(
            inp,
            Request::Between {
                pairs: vec![(hash_ones(), hash_twos()), (hash_threes(), hash_fours())],
            },
        );
    }

    #[test]
    fn test_parse_branchmap() {
        let inp = "branchmap\n";

        test_parse(inp, Request::Branchmap {});
    }

    #[test]
    fn test_parse_branches() {
        let inp = "branches\n\
                   nodes 163\n\
                   1111111111111111111111111111111111111111 2222222222222222222222222222222222222222 \
                   3333333333333333333333333333333333333333 4444444444444444444444444444444444444444";
        test_parse(
            inp,
            Request::Branches {
                nodes: vec![hash_ones(), hash_twos(), hash_threes(), hash_fours()],
            },
        );
    }

    #[test]
    fn test_parse_clonebundles() {
        let inp = "clonebundles\n";

        test_parse(inp, Request::Clonebundles {});
    }

    #[test]
    fn test_parse_capabilities() {
        let inp = "capabilities\n";

        test_parse(inp, Request::Capabilities {});
    }

    #[test]
    fn test_parse_changegroup() {
        let inp = "changegroup\n\
                   roots 81\n\
                   1111111111111111111111111111111111111111 2222222222222222222222222222222222222222";

        test_parse(
            inp,
            Request::Changegroup {
                roots: vec![hash_ones(), hash_twos()],
            },
        );
    }

    #[test]
    fn test_parse_changegroupsubset() {
        let inp = "changegroupsubset\n\
                   heads 40\n\
                   1111111111111111111111111111111111111111\
                   bases 81\n\
                   2222222222222222222222222222222222222222 3333333333333333333333333333333333333333";

        test_parse(
            inp,
            Request::Changegroupsubset {
                heads: vec![hash_ones()],
                bases: vec![hash_twos(), hash_threes()],
            },
        );
    }

    #[test]
    fn test_parse_debugwireargs() {
        let inp = "debugwireargs\n\
                   * 2\n\
                   three 5\nTHREE\
                   empty 0\n\
                   one 3\nONE\
                   two 3\nTWO";
        test_parse(
            inp,
            Request::Debugwireargs {
                one: b"ONE".to_vec(),
                two: b"TWO".to_vec(),
                all_args: hashmap! {
                    b"one".to_vec() => b"ONE".to_vec(),
                    b"two".to_vec() => b"TWO".to_vec(),
                    b"three".to_vec() => b"THREE".to_vec(),
                    b"empty".to_vec() => vec![],
                },
            },
        );
    }

    #[test]
    fn test_parse_getbundle() {
        let inp = "getbundle\n\
                   * 0\n";

        test_parse(
            inp,
            Request::Getbundle(GetbundleArgs {
                heads: vec![],
                common: vec![],
                bundlecaps: vec![],
                listkeys: vec![],
            }),
        );

        let inp = "getbundle\n\
                   * 5\n\
                   heads 40\n\
                   1111111111111111111111111111111111111111\
                   common 81\n\
                   2222222222222222222222222222222222222222 3333333333333333333333333333333333333333\
                   bundlecaps 14\n\
                   cap1,CAP2,cap3\
                   listkeys 9\n\
                   key1,key2\
                   extra 5\n\
                   extra";
        test_parse(
            inp,
            Request::Getbundle(GetbundleArgs {
                heads: vec![hash_ones()],
                common: vec![hash_twos(), hash_threes()],
                bundlecaps: vec![b"cap1".to_vec(), b"CAP2".to_vec(), b"cap3".to_vec()],
                listkeys: vec![b"key1".to_vec(), b"key2".to_vec()],
            }),
        );
    }

    #[test]
    fn test_parse_heads() {
        let inp = "heads\n";

        test_parse(inp, Request::Heads {});
    }

    #[test]
    fn test_parse_hello() {
        let inp = "hello\n";

        test_parse(inp, Request::Hello {});
    }

    #[test]
    fn test_parse_listkeys() {
        let inp = "listkeys\n\
                   namespace 9\n\
                   bookmarks";

        test_parse(
            inp,
            Request::Listkeys {
                namespace: "bookmarks".to_string(),
            },
        );
    }

    #[test]
    fn test_parse_lookup() {
        let inp = "lookup\n\
                   key 9\n\
                   bookmarks";

        test_parse(
            inp,
            Request::Lookup {
                key: "bookmarks".to_string(),
            },
        );
    }

    #[test]
    fn test_parse_known() {
        let inp = "known\n\
                   * 0\n\
                   nodes 40\n\
                   1111111111111111111111111111111111111111";

        test_parse(
            inp,
            Request::Known {
                nodes: vec![hash_ones()],
            },
        );
    }

    #[test]
    fn test_parse_pushkey() {
        let inp = "pushkey\n\
                   namespace 9\n\
                   bookmarks\
                   key 6\n\
                   foobar\
                   old 40\n\
                   1111111111111111111111111111111111111111\
                   new 40\n\
                   2222222222222222222222222222222222222222";

        test_parse(
            inp,
            Request::Pushkey {
                namespace: "bookmarks".to_string(),
                key: "foobar".to_string(),
                old: hash_ones(),
                new: hash_twos(),
            },
        );
    }

    #[test]
    fn test_parse_streamout() {
        let inp = "streamout\n";

        test_parse(inp, Request::Streamout {});
    }

    #[test]
    fn test_parse_unbundle() {
        let inp = "unbundle\n\
                   heads 40\n\
                   1111111111111111111111111111111111111111";

        test_parse(
            inp,
            Request::Unbundle {
                heads: vec![hash_ones()],
                bundle: Bytes::new(),
            },
        );
    }

    #[test]
    fn test_batch_parse_heads() {
        let mut inp = BytesMut::from(&b"heads\n"[..]);

        match parse_batch(&mut inp) {
            Ok(Some(res)) => assert_eq!(res, Request::Heads {}),
            Ok(None) => panic!("unexpected incomplete input"),
            Err(err) => panic!("failed with {:?}", err),
        }
    }

    #[test]
    fn test_parse_batch_heads() {
        let inp = "batch\n\
                   * 0\n\
                   cmds 100\n\
                   heads ;\
                   known nodes=ee07e8c0780b5059e874c5b0dbcab2278fde2a14 \
                   3243aa153e20a170cd2c7441c595c44a9b087f5b";

        test_parse(
            inp,
            Request::Batch {
                cmds: vec![
                    (b"heads".to_vec(), vec![]),
                    (
                        b"known".to_vec(),
                        b"nodes=ee07e8c0780b5059e874c5b0dbcab2278fde2a14 \
                      3243aa153e20a170cd2c7441c595c44a9b087f5b"
                            .to_vec(),
                    ),
                ],
            },
        );
    }
}
