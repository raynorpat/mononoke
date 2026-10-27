// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Stream, TryFutureExt, TryStreamExt};

use crate::bail;
use crate::changegroup::{self, packer::Cg2Packer};
use crate::errors::*;
use crate::part_encode::PartEncodeBuilder;

pub fn listkey_part<N, S, K, V, E>(namespace: N, items: S) -> Result<PartEncodeBuilder>
where
    N: Into<Bytes>,
    S: Stream<Item = std::result::Result<(K, V), E>> + Send + Unpin + 'static,
    E: std::error::Error + Send + 'static,
    K: AsRef<[u8]> + Send + 'static,
    V: AsRef<[u8]> + Send + 'static,
{
    let mut builder = PartEncodeBuilder::mandatory("listkeys")?;
    builder.add_mparam("namespace", namespace)?;
    // Ideally we'd use a size_hint here, but streams don't appear to have one.
    let payload = Vec::with_capacity(256);
    let fut = items
        .try_fold(payload, |mut payload, (key, value)| async move {
            payload.extend_from_slice(key.as_ref());
            payload.push(b'\t');
            payload.extend_from_slice(value.as_ref());
            payload.push(b'\n');
            Ok(payload)
        })
        .map_err(|err| Error::with_chain(err.to_string(), ErrorKind::ListkeyGeneration));

    builder.set_data_future(fut);

    Ok(builder)
}

/// Mandatory `changegroup` part, version `02`.
///
/// `chunks` are packed with `Cg2Packer` and the chunk bytes become the part
/// payload. Callers choose bundle2 compression on `Bundle2EncodeBuilder`.
pub fn changegroup_part(chunks: Vec<changegroup::Part>) -> Result<PartEncodeBuilder> {
    let mut builder = PartEncodeBuilder::mandatory("changegroup")?;
    builder.add_mparam("version", "02")?;
    builder.set_data_bytes(pack_changegroup(chunks)?)?;
    Ok(builder)
}

fn pack_changegroup(chunks: Vec<changegroup::Part>) -> Result<Bytes> {
    let mut packer = Cg2Packer::new(futures::stream::iter(
        chunks.into_iter().map(Ok::<_, Error>),
    ));
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut payload = Vec::new();
    loop {
        match Pin::new(&mut packer).poll_next(&mut cx) {
            Poll::Ready(Some(Ok(chunk))) => {
                payload.extend_from_slice(&chunk.into_bytes()?);
            }
            Poll::Ready(Some(Err(err))) => return Err(err),
            Poll::Ready(None) => break,
            Poll::Pending => {
                bail!(ErrorKind::Bundle2Encode(
                    "changegroup packer would block".into()
                ));
            }
        }
    }
    Ok(Bytes::from(payload))
}
