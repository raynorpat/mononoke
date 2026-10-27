// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Type definitions for inner streams.
#![deny(warnings)]

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll};

use ascii::AsciiStr;
use bytes::Bytes;
use futures::future::{self, Ready};
use futures::Stream;
use futures_ext::{StreamExt, StreamLayeredExt, StreamWrapper, TakeWhile};
use slog::{self, o};
use tokio::io::AsyncRead;

use crate::bail;
use crate::changegroup;
use crate::errors::*;
use crate::part_header::PartHeader;
use crate::part_outer::{OuterFrame, OuterStream};

// --- Part parameters

macro_rules! add_part {
    ( $m:expr, $part_type:expr, [$( $params:expr ),*] ) => {{
        let mut h = HashSet::new();
        $(h.insert($params);)*
        $m.insert(AsciiStr::from_ascii($part_type).unwrap(), h);
    }}
}

static KNOWN_PARAMS: LazyLock<HashMap<&'static AsciiStr, HashSet<&'static str>>> =
    LazyLock::new(|| {
        let mut m: HashMap<&'static AsciiStr, HashSet<&'static str>> = HashMap::new();
        add_part!(m, "changegroup", ["version", "nbchanges", "treemanifest"]);
        m
    });

type PayloadPred = fn(&OuterFrame) -> Ready<Result<bool>>;

type PayloadTake<R> = TakeWhile<OuterStream<R>, PayloadPred, Ready<Result<bool>>, OuterFrame>;

struct PayloadBytes<R> {
    inner: PayloadTake<R>,
}

impl<R> Stream for PayloadBytes<R>
where
    R: AsyncRead + Unpin,
{
    type Item = Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(frame))) => Poll::Ready(Some(Ok(frame.get_payload()))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<R> StreamWrapper<PayloadTake<R>> for PayloadBytes<R> {
    fn into_inner(self) -> PayloadTake<R> {
        self.inner
    }
}

pub struct Cg2InnerStream<R> {
    inner: futures_ext::decode::LayeredDecode<PayloadBytes<R>, changegroup::unpacker::Cg2Unpacker>,
}

impl<R> Cg2InnerStream<R> {
    pub fn into_outer(self) -> OuterStream<R> {
        let bytes = StreamWrapper::into_inner(self.inner);
        let take = StreamWrapper::into_inner(bytes);
        StreamWrapper::into_inner(take)
    }
}

impl<R> Stream for Cg2InnerStream<R>
where
    R: AsyncRead + Unpin,
{
    type Item = Result<InnerPart>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.get_mut().inner).poll_next(cx)
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum InnerPart {
    Cg2(changegroup::Part),
}

impl InnerPart {
    pub fn is_cg2(&self) -> bool {
        match self {
            InnerPart::Cg2(_) => true,
        }
    }

    pub fn cg2_part(self) -> changegroup::Part {
        match self {
            InnerPart::Cg2(part) => part,
        }
    }
}

pub fn validate_header(header: PartHeader) -> Result<Option<PartHeader>> {
    match KNOWN_PARAMS.get(header.part_type_lower()) {
        Some(known_params) => {
            // Make sure all the mandatory params are recognized.
            let unknown_params: Vec<_> = header
                .mparams()
                .keys()
                .filter(|param| !known_params.contains(param.as_str()))
                .cloned()
                .collect();
            if !unknown_params.is_empty() {
                bail!(ErrorKind::BundleUnknownPartParams(
                    header.part_type().to_ascii_string(),
                    unknown_params
                ));
            }
            Ok(Some(header))
        }
        None => {
            if header.is_mandatory() {
                bail!(ErrorKind::BundleUnknownPart(header));
            }
            Ok(None)
        }
    }
}

/// Convert an OuterStream into an InnerStream using the part header.
pub fn inner_stream<R>(
    header: &PartHeader,
    stream: OuterStream<R>,
    logger: &slog::Logger,
) -> Cg2InnerStream<R>
where
    R: AsyncRead + Unpin,
{
    let bytes = PayloadBytes {
        inner: stream.take_while_wrapper(is_payload as PayloadPred),
    };
    match header.part_type_lower().as_str() {
        "changegroup" => Cg2InnerStream {
            inner: bytes.decode(changegroup::unpacker::Cg2Unpacker::new(
                logger.new(o!("stream" => "cg2")),
            )),
        },
        _ => panic!("TODO: make this an error"),
    }
}

fn is_payload(item: &OuterFrame) -> Ready<Result<bool>> {
    future::ready(Ok(item.is_payload()))
}
