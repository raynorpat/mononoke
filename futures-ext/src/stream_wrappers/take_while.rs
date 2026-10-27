// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::{Future, Sink, Stream};

use super::StreamWrapper;

/// Take elements from a `Stream<Item = Result<T, E>>` while a predicate holds.
///
/// The predicate sees the success value. `Ok(false)` ends the stream.
/// A predicate error is forwarded and the stream stops yielding that item.
#[must_use = "streams do nothing unless polled"]
pub struct TakeWhile<S, P, Fut, T> {
    stream: S,
    pred: P,
    pending: Option<(Fut, T)>,
    done_taking: bool,
}

pub fn new<S, P, Fut, T, E>(s: S, p: P) -> TakeWhile<S, P, Fut, T>
where
    S: Stream<Item = Result<T, E>>,
    P: FnMut(&T) -> Fut,
    Fut: Future<Output = Result<bool, E>>,
{
    TakeWhile {
        stream: s,
        pred: p,
        pending: None,
        done_taking: false,
    }
}

impl<S, P, Fut, T> TakeWhile<S, P, Fut, T> {
    pub fn get_ref(&self) -> &S {
        &self.stream
    }

    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<S, P, Fut, T, Item> Sink<Item> for TakeWhile<S, P, Fut, T>
where
    S: Sink<Item> + Unpin,
    P: Unpin,
    Fut: Unpin,
    T: Unpin,
{
    type Error = S::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Item) -> Result<(), Self::Error> {
        Pin::new(&mut self.get_mut().stream).start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.get_mut().stream).poll_close(cx)
    }
}

impl<S, P, Fut, T, E> Stream for TakeWhile<S, P, Fut, T>
where
    S: Stream<Item = Result<T, E>> + Unpin,
    P: FnMut(&T) -> Fut + Unpin,
    Fut: Future<Output = Result<bool, E>> + Unpin,
    T: Unpin,
{
    type Item = Result<T, E>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done_taking {
            return Poll::Ready(None);
        }

        if this.pending.is_none() {
            let item = match Pin::new(&mut this.stream).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(item))) => item,
            };
            let fut = (this.pred)(&item);
            this.pending = Some((fut, item));
        }

        let (fut, _) = this.pending.as_mut().expect("pending predicate");
        match Pin::new(fut).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(true)) => {
                let (_, item) = this.pending.take().expect("pending item");
                Poll::Ready(Some(Ok(item)))
            }
            Poll::Ready(Ok(false)) => {
                this.done_taking = true;
                Poll::Ready(None)
            }
            Poll::Ready(Err(e)) => {
                this.pending = None;
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

impl<S, P, Fut, T> StreamWrapper<S> for TakeWhile<S, P, Fut, T> {
    fn into_inner(self) -> S {
        self.stream
    }
}
