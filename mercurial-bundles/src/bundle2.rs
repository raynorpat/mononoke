// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Overall coordinator for parsing bundle2 streams.

use std::fmt::{self, Debug, Display, Formatter};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use futures_ext::{AsyncReadExt, FramedStream, ReadLeadingBuffer};
use slog;
use tokio::io::AsyncRead;

use crate::errors::*;
use crate::part_inner::{inner_stream, Cg2InnerStream};
use crate::part_outer::{outer_stream, OuterFrame, OuterStream};
use crate::stream_start::StartDecoder;
use crate::Bundle2Item;

#[derive(Debug)]
pub struct Bundle2Stream<R> {
    inner: Bundle2StreamInner,
    current_stream: CurrentStream<R>,
}

#[derive(Debug)]
struct Bundle2StreamInner {
    logger: slog::Logger,
    app_errors: Vec<Error>,
}

enum CurrentStream<R> {
    Start(FramedStream<R, StartDecoder>),
    Outer(OuterStream<ReadLeadingBuffer<R>>),
    Inner(Cg2InnerStream<ReadLeadingBuffer<R>>),
    Invalid,
    End,
}

impl<R> CurrentStream<R> {
    pub fn take(&mut self) -> Self {
        mem::replace(self, CurrentStream::Invalid)
    }
}

impl<R> Display for CurrentStream<R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            CurrentStream::Start(_) => "start",
            CurrentStream::Outer(_) => "outer",
            CurrentStream::Inner(_) => "inner",
            CurrentStream::Invalid => "invalid",
            CurrentStream::End => "end",
        };
        write!(fmt, "{}", s)
    }
}

impl<R> Debug for CurrentStream<R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            CurrentStream::Start(_) => write!(f, "Start(..)"),
            CurrentStream::Outer(_) => write!(f, "Outer(..)"),
            CurrentStream::Inner(_) => write!(f, "Inner(inner_stream)"),
            CurrentStream::Invalid => write!(f, "Invalid"),
            CurrentStream::End => write!(f, "End"),
        }
    }
}

impl<R> Bundle2Stream<R>
where
    R: AsyncRead,
{
    pub fn new(read: R, logger: slog::Logger) -> Bundle2Stream<R> {
        Bundle2Stream {
            inner: Bundle2StreamInner {
                logger,
                app_errors: Vec::new(),
            },
            current_stream: CurrentStream::Start(read.framed_stream(StartDecoder)),
        }
    }

    pub fn app_errors(&self) -> &[Error] {
        &self.inner.app_errors
    }
}

impl<R> Stream for Bundle2Stream<R>
where
    R: AsyncRead + Unpin,
{
    type Item = Result<Bundle2Item>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let current_stream = this.current_stream.take();
        let (ret, stream) = this.inner.poll_next(current_stream, cx);
        this.current_stream = stream;
        ret
    }
}

impl Bundle2StreamInner {
    fn poll_next<R>(
        &mut self,
        current_stream: CurrentStream<R>,
        cx: &mut Context<'_>,
    ) -> (Poll<Option<Result<Bundle2Item>>>, CurrentStream<R>)
    where
        R: AsyncRead + Unpin,
    {
        match current_stream {
            CurrentStream::Start(mut stream) => match Pin::new(&mut stream).poll_next(cx) {
                Poll::Ready(Some(Err(e))) => {
                    (Poll::Ready(Some(Err(e))), CurrentStream::Start(stream))
                }
                Poll::Pending => (Poll::Pending, CurrentStream::Start(stream)),
                Poll::Ready(None) => (Poll::Ready(None), CurrentStream::Start(stream)),
                Poll::Ready(Some(Ok(start))) => {
                    match outer_stream(&start, stream.into_inner_leading(), &self.logger) {
                        Err(e) => {
                            // Can't do much if reading stream level params
                            // failed -- go to the invalid state.
                            (Poll::Ready(Some(Err(e))), CurrentStream::Invalid)
                        }
                        Ok(v) => (
                            Poll::Ready(Some(Ok(Bundle2Item::Start(start)))),
                            CurrentStream::Outer(v),
                        ),
                    }
                }
            },
            CurrentStream::Outer(mut stream) => match Pin::new(&mut stream).poll_next(cx) {
                Poll::Ready(Some(Err(e))) => {
                    if e.is_app_error() {
                        // Don't return these, just continue processing the stream.
                        self.app_errors.push(e);
                        self.poll_next(CurrentStream::Outer(stream), cx)
                    } else {
                        (Poll::Ready(Some(Err(e))), CurrentStream::Outer(stream))
                    }
                }
                Poll::Pending => (Poll::Pending, CurrentStream::Outer(stream)),
                Poll::Ready(None) => (Poll::Ready(None), CurrentStream::Outer(stream)),
                Poll::Ready(Some(Ok(OuterFrame::Header(header)))) => {
                    let inner = CurrentStream::Inner(inner_stream(&header, stream, &self.logger));
                    (Poll::Ready(Some(Ok(Bundle2Item::Header(header)))), inner)
                }
                Poll::Ready(Some(Ok(OuterFrame::Discard))) => {
                    self.poll_next(CurrentStream::Outer(stream), cx)
                }
                Poll::Ready(Some(Ok(OuterFrame::StreamEnd))) => {
                    // No more parts to go.
                    (Poll::Ready(None), CurrentStream::End)
                }
                Poll::Ready(Some(Ok(_))) => panic!("Expected a header or StreamEnd!"),
            },
            CurrentStream::Inner(mut stream) => match Pin::new(&mut stream).poll_next(cx) {
                Poll::Ready(Some(Err(e))) => {
                    (Poll::Ready(Some(Err(e))), CurrentStream::Inner(stream))
                }
                Poll::Pending => (Poll::Pending, CurrentStream::Inner(stream)),
                Poll::Ready(Some(Ok(v))) => (
                    Poll::Ready(Some(Ok(Bundle2Item::Inner(v)))),
                    CurrentStream::Inner(stream),
                ),
                Poll::Ready(None) => {
                    // This part is now parsed -- go back to the outer stream.
                    self.poll_next(CurrentStream::Outer(stream.into_outer()), cx)
                }
            },
            CurrentStream::Invalid => (
                Poll::Ready(Some(Err(ErrorKind::Bundle2Decode(
                    "corrupt byte stream".into(),
                )))),
                CurrentStream::Invalid,
            ),
            CurrentStream::End => (Poll::Ready(None), CurrentStream::End),
        }
    }
}
