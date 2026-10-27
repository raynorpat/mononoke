// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{Fuse, StreamExt};
use futures::{Future, Sink, Stream};

/// Fork a stream of `Result<T, _>` into two sinks.
///
/// `pred` returns `Ok(false)` to send the value to `out1`, and `Ok(true)` to
/// send it to `out2`. The future resolves to the input stream and both sinks
/// once the input ends and both sinks have been flushed.
pub fn streamfork<In, T, InErr, Out1, Out2, F, E>(
    inp: In,
    out1: Out1,
    out2: Out2,
    pred: F,
) -> Forker<In, Out1, Out2, F, T>
where
    In: Stream<Item = Result<T, InErr>> + Unpin,
    Out1: Sink<T> + Unpin,
    Out2: Sink<T, Error = Out1::Error> + Unpin,
    F: FnMut(&T) -> Result<bool, E>,
    E: From<InErr> + From<Out1::Error>,
{
    Forker {
        inp: Some(inp.fuse()),
        out1: Out::new(out1),
        out2: Out::new(out2),
        pred,
    }
}

pub struct Forker<In, Out1, Out2, F, T> {
    inp: Option<Fuse<In>>,
    out1: Out<Out1, T>,
    out2: Out<Out2, T>,
    pred: F,
}

struct Out<S, T> {
    out: Option<S>,
    buf: Option<T>,
}

impl<S, T> Out<S, T> {
    fn new(s: S) -> Self {
        Out {
            out: Some(s),
            buf: None,
        }
    }

    fn sink(&mut self) -> &mut S {
        self.out.as_mut().expect("sink missing")
    }

    fn take_result(&mut self) -> S {
        self.out.take().expect("sink missing")
    }
}

impl<S, T, SE> Out<S, T>
where
    S: Sink<T, Error = SE> + Unpin,
{
    fn push<E: From<SE>>(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        let item = match self.buf.take() {
            Some(item) => item,
            None => return Poll::Ready(Ok(())),
        };
        match Pin::new(self.sink()).poll_ready(cx) {
            Poll::Pending => {
                self.buf = Some(item);
                Poll::Pending
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(())) => match Pin::new(self.sink()).start_send(item) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(e.into())),
            },
        }
    }

    fn try_start_send<E: From<SE>>(
        &mut self,
        cx: &mut Context<'_>,
        item: T,
    ) -> Poll<Result<(), E>> {
        debug_assert!(self.buf.is_none());
        match Pin::new(self.sink()).poll_ready(cx) {
            Poll::Pending => {
                self.buf = Some(item);
                Poll::Pending
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
            Poll::Ready(Ok(())) => match Pin::new(self.sink()).start_send(item) {
                Ok(()) => Poll::Ready(Ok(())),
                Err(e) => Poll::Ready(Err(e.into())),
            },
        }
    }

    fn poll_flush<E: From<SE>>(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), E>> {
        match Pin::new(self.sink()).poll_flush(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e.into())),
        }
    }
}

impl<In, Out1, Out2, F, T> Forker<In, Out1, Out2, F, T> {
    fn take_result(&mut self) -> (In, Out1, Out2) {
        let inp = self.inp.take().expect("input missing").into_inner();
        (inp, self.out1.take_result(), self.out2.take_result())
    }
}

impl<In, T, InErr, Out1, Out2, F, E> Future for Forker<In, Out1, Out2, F, T>
where
    In: Stream<Item = Result<T, InErr>> + Unpin,
    Out1: Sink<T> + Unpin,
    Out2: Sink<T, Error = Out1::Error> + Unpin,
    F: FnMut(&T) -> Result<bool, E> + Unpin,
    E: From<InErr> + From<Out1::Error>,
    T: Unpin,
{
    type Output = Result<(In, Out1, Out2), E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        if let Some(err) = ready_ok(this.out1.push(cx)) {
            return err;
        }
        if let Some(err) = ready_ok(this.out2.push(cx)) {
            return err;
        }

        loop {
            match Pin::new(this.inp.as_mut().expect("input missing")).poll_next(cx) {
                Poll::Ready(Some(Ok(item))) => match (this.pred)(&item) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(to_out2) => {
                        let send = if to_out2 {
                            this.out2.try_start_send(cx, item)
                        } else {
                            this.out1.try_start_send(cx, item)
                        };
                        if let Some(err) = ready_ok(send) {
                            return err;
                        }
                    }
                },
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Err(e.into())),
                Poll::Ready(None) => {
                    if let Some(err) = ready_ok(this.out1.poll_flush(cx)) {
                        return err;
                    }
                    if let Some(err) = ready_ok(this.out2.poll_flush(cx)) {
                        return err;
                    }
                    return Poll::Ready(Ok(this.take_result()));
                }
                Poll::Pending => {
                    if let Poll::Ready(Err(e)) = this.out1.poll_flush(cx) {
                        return Poll::Ready(Err(e));
                    }
                    if let Poll::Ready(Err(e)) = this.out2.poll_flush(cx) {
                        return Poll::Ready(Err(e));
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

/// `None` when `poll` is ready and successful. Otherwise the poll to return.
fn ready_ok<T, E>(poll: Poll<Result<(), E>>) -> Option<Poll<Result<T, E>>> {
    match poll {
        Poll::Ready(Ok(())) => None,
        Poll::Ready(Err(e)) => Some(Poll::Ready(Err(e))),
        Poll::Pending => Some(Poll::Pending),
    }
}

#[cfg(test)]
mod test {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::executor::block_on;
    use futures::stream;
    use futures::Sink;

    use super::*;

    struct VecSink<T>(Vec<T>);

    impl<T: Unpin> Sink<T> for VecSink<T> {
        type Error = ();

        fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), ()> {
            self.get_mut().0.push(item);
            Ok(())
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[test]
    fn simple() {
        let nums = stream::iter((0i32..10).map(Ok::<_, ()>));
        let (_, even, odd) = block_on(streamfork(
            nums,
            VecSink(Vec::new()),
            VecSink(Vec::new()),
            |n| Ok::<_, ()>(*n % 2 == 1),
        ))
        .unwrap();

        assert_eq!(even.0, vec![0, 2, 4, 6, 8]);
        assert_eq!(odd.0, vec![1, 3, 5, 7, 9]);
    }
}
