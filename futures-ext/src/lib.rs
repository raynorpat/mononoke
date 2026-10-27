#![deny(warnings)]
// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::{Future, Stream};
use tokio::io::AsyncRead;
use tokio_util::codec::{Decoder, Encoder};

mod frame;
mod futures_ordered;
mod stream_wrappers;
mod streamfork;

#[cfg(test)]
mod testutil;

pub mod decode;
pub mod encode;

pub use frame::{FramedStream, ReadLeadingBuffer};
pub use futures_ordered::{futures_ordered, FuturesOrdered};
pub use stream_wrappers::{BoxStreamWrapper, StreamWrapper, TakeWhile};
pub use streamfork::Forker;

/// Map any completion to `()`.
///
/// Adapt an existing `Future` to return `()`, while still waiting for the
/// underlying `Future` to complete.
pub struct Discard<F>(F);

impl<F> Discard<F> {
    pub fn new(f: F) -> Self {
        Discard(f)
    }
}

impl<F: Future> Future for Discard<F> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // `F` is structurally pinned.
        let inner = unsafe { self.map_unchecked_mut(|d| &mut d.0) };
        match inner.poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub trait FutureExt: Future + Sized {
    /// Map a `Future` to have output `()`. Useful when a future only drives
    /// a computation and its result will be ignored.
    fn discard(self) -> Discard<Self> {
        Discard(self)
    }
}

impl<T: Future> FutureExt for T {}

pub trait StreamExt: Stream {
    /// Fork elements to two sinks, depending on a predicate.
    ///
    /// The input is a `Stream<Item = Result<T, _>>`. `pred` returns `Ok(false)`
    /// to send the value to `out1` and `Ok(true)` to send it to `out2`. The
    /// returned future finishes with the input stream and both sinks when the
    /// input ends.
    fn streamfork<T, InErr, Out1, Out2, F, E>(
        self,
        out1: Out1,
        out2: Out2,
        pred: F,
    ) -> Forker<Self, Out1, Out2, F, T>
    where
        Self: Sized + Stream<Item = Result<T, InErr>> + Unpin,
        Out1: futures::Sink<T> + Unpin,
        Out2: futures::Sink<T, Error = Out1::Error> + Unpin,
        F: FnMut(&T) -> Result<bool, E>,
        E: From<InErr> + From<Out1::Error>,
    {
        streamfork::streamfork(self, out1, out2, pred)
    }

    fn take_while_wrapper<T, E, P, Fut>(self, pred: P) -> TakeWhile<Self, P, Fut, T>
    where
        Self: Sized + Stream<Item = Result<T, E>> + Unpin,
        P: FnMut(&T) -> Fut,
        Fut: Future<Output = Result<bool, E>> + Unpin,
    {
        stream_wrappers::take_while::new(self, pred)
    }

    fn encode<Enc, T, E>(self, encoder: Enc) -> encode::LayeredEncoder<Self, Enc>
    where
        Self: Sized + Stream<Item = Result<T, E>> + Unpin,
        Enc: Encoder<T>,
        Enc::Error: From<E>,
    {
        encode::encode(self, encoder)
    }

    fn enumerate(self) -> Enumerate<Self>
    where
        Self: Sized,
    {
        Enumerate::new(self)
    }
}

impl<T: Stream> StreamExt for T {}

pub trait AsyncReadExt: AsyncRead + Sized {
    fn framed_stream<C>(self, codec: C) -> FramedStream<Self, C>
    where
        C: Decoder,
    {
        frame::framed_stream(self, codec)
    }
}

impl<T: AsyncRead> AsyncReadExt for T {}

/// Decode a stream of byte chunks with a tokio-util `Decoder`.
pub trait StreamLayeredExt<E>: Stream<Item = Result<Bytes, E>> {
    fn decode<Dec>(self, decoder: Dec) -> decode::LayeredDecode<Self, Dec>
    where
        Self: Sized,
        Dec: Decoder,
        Dec::Error: From<E>;
}

impl<T, E> StreamLayeredExt<E> for T
where
    T: Stream<Item = Result<Bytes, E>>,
{
    fn decode<Dec>(self, decoder: Dec) -> decode::LayeredDecode<Self, Dec>
    where
        Self: Sized,
        Dec: Decoder,
        Dec::Error: From<E>,
    {
        decode::decode(self, decoder)
    }
}

pub struct Enumerate<In> {
    inner: In,
    count: usize,
}

impl<In> Enumerate<In> {
    fn new(inner: In) -> Self {
        Enumerate { inner, count: 0 }
    }
}

impl<In: Stream + Unpin> Stream for Enumerate<In> {
    type Item = (usize, In::Item);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(v)) => {
                let c = this.count;
                this.count += 1;
                Poll::Ready(Some((c, v)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Turn `io::Result<T>` into `Poll<io::Result<T>>`, mapping `WouldBlock` to
/// `Pending`.
#[macro_export]
macro_rules! handle_nb {
    ($e:expr) => {
        match $e {
            Ok(t) => ::std::task::Poll::Ready(Ok(t)),
            Err(ref e) if e.kind() == ::std::io::ErrorKind::WouldBlock => {
                ::std::task::Poll::Pending
            }
            Err(e) => ::std::task::Poll::Ready(Err(e)),
        }
    };
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::channel::mpsc;
    use futures::executor::block_on;
    use futures::stream::{self, StreamExt as FuturesStreamExt};

    #[test]
    fn discard() {
        let (tx, rx) = mpsc::channel(1);
        let fwd = stream::iter(vec![Ok::<_, mpsc::SendError>(123)])
            .forward(tx)
            .discard();
        block_on(fwd);
        let v: Vec<_> = block_on(FuturesStreamExt::collect(rx));
        assert_eq!(v, vec![123]);
    }

    #[test]
    fn enumerate() {
        let s = stream::iter(vec![Ok::<_, ()>("hello"), Ok("there"), Ok("world")]);
        let es = Enumerate::new(s);
        let v: Vec<_> = block_on(FuturesStreamExt::collect(es));

        assert_eq!(
            v,
            vec![(0, Ok("hello")), (1, Ok("there")), (2, Ok("world"))]
        );
    }
}
