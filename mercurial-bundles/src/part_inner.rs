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
use bytes::{Bytes, BytesMut};
use futures::future::{self, Ready};
use futures::Stream;
use futures_ext::{StreamExt, StreamLayeredExt, StreamWrapper, TakeWhile};
use slog::{self, o};
use tokio::io::AsyncRead;
use tokio_util::codec::Decoder;

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
        add_part!(m, "pushkey", ["namespace", "key", "old", "new"]);
        // No parameters. The heads are the part payload, not a param.
        m.insert(AsciiStr::from_ascii("check:heads").unwrap(), HashSet::new());
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

enum PayloadDecode<R> {
    Cg2(
        futures_ext::decode::LayeredDecode<PayloadBytes<R>, changegroup::unpacker::Cg2Unpacker>,
    ),
    CheckHeads(futures_ext::decode::LayeredDecode<PayloadBytes<R>, CheckHeadsDecoder>),
    Skip(futures_ext::decode::LayeredDecode<PayloadBytes<R>, SkipDecoder>),
}

pub struct Cg2InnerStream<R> {
    inner: PayloadDecode<R>,
}

struct SkipDecoder;

impl Decoder for SkipDecoder {
    type Item = InnerPart;
    type Error = Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<InnerPart>> {
        buf.clear();
        Ok(None)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<InnerPart>> {
        buf.clear();
        Ok(None)
    }
}

struct CheckHeadsDecoder;

impl Decoder for CheckHeadsDecoder {
    type Item = InnerPart;
    type Error = Error;

    // Leave the bytes buffered. `LayeredDecode` calls `decode_eof` only when
    // the buffer is non-empty, and the whole part is one payload.
    fn decode(&mut self, _buf: &mut BytesMut) -> Result<Option<InnerPart>> {
        Ok(None)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<InnerPart>> {
        Ok(Some(InnerPart::CheckHeads(buf.split().freeze())))
    }
}

impl<R> Cg2InnerStream<R> {
    pub fn into_outer(self) -> OuterStream<R> {
        let bytes = match self.inner {
            PayloadDecode::Cg2(inner) => StreamWrapper::into_inner(inner),
            PayloadDecode::CheckHeads(inner) => StreamWrapper::into_inner(inner),
            PayloadDecode::Skip(inner) => StreamWrapper::into_inner(inner),
        };
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
        match &mut self.get_mut().inner {
            PayloadDecode::Cg2(inner) => Pin::new(inner).poll_next(cx),
            PayloadDecode::CheckHeads(inner) => Pin::new(inner).poll_next(cx),
            PayloadDecode::Skip(inner) => Pin::new(inner).poll_next(cx),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum InnerPart {
    Cg2(changegroup::Part),
    /// Payload of a `check:heads` part: concatenated 20-byte node ids.
    CheckHeads(Bytes),
}

impl InnerPart {
    pub fn is_cg2(&self) -> bool {
        match self {
            InnerPart::Cg2(_) => true,
            InnerPart::CheckHeads(_) => false,
        }
    }

    pub fn cg2_part(self) -> changegroup::Part {
        match self {
            InnerPart::Cg2(part) => part,
            InnerPart::CheckHeads(_) => panic!("cg2_part called on check:heads"),
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
    let inner = match header.part_type_lower().as_str() {
        "changegroup" => PayloadDecode::Cg2(bytes.decode(changegroup::unpacker::Cg2Unpacker::new(
            logger.new(o!("stream" => "cg2")),
        ))),
        "check:heads" => PayloadDecode::CheckHeads(bytes.decode(CheckHeadsDecoder)),
        // Pushkey data is in the part header. Other recognized parts are not stored.
        _ => PayloadDecode::Skip(bytes.decode(SkipDecoder)),
    };
    Cg2InnerStream { inner }
}

fn is_payload(item: &OuterFrame) -> Ready<Result<bool>> {
    future::ready(Ok(item.is_payload()))
}
