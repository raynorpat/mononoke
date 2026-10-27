// Copyright (c) 2017-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Serially drive a sequence of futures and stream their outputs in order.

use std::fmt;
use std::future::{Future, IntoFuture};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;

/// Serially executes futures from an iterator.
///
/// Each future is polled to completion before the next one starts. The stream
/// item is that future's `Output`. When the output is a `Result`, an error does
/// not drop the rest of the iterator: the next future starts on the following poll.
#[must_use = "streams do nothing unless polled"]
pub struct FuturesOrdered<I>
where
    I: IntoIterator,
    I::Item: IntoFuture,
{
    elems: I::IntoIter,
    current: Option<<I::Item as IntoFuture>::IntoFuture>,
}

impl<I> fmt::Debug for FuturesOrdered<I>
where
    I: IntoIterator,
    I::Item: IntoFuture,
    <<I as IntoIterator>::Item as IntoFuture>::IntoFuture: fmt::Debug,
{
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("FuturesOrdered")
            .field("current", &self.current)
            .finish()
    }
}

/// Stream the outputs of `iter`'s futures, running one at a time.
pub fn futures_ordered<I>(iter: I) -> FuturesOrdered<I>
where
    I: IntoIterator,
    I::Item: IntoFuture,
{
    let mut elems = iter.into_iter();
    let current = next_future(&mut elems);
    FuturesOrdered { elems, current }
}

impl<I> Stream for FuturesOrdered<I>
where
    I: IntoIterator,
    I::Item: IntoFuture,
    I::IntoIter: Unpin,
    <I::Item as IntoFuture>::IntoFuture: Unpin,
{
    type Item = <I::Item as IntoFuture>::Output;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.current.take() {
            Some(mut fut) => match Pin::new(&mut fut).poll(cx) {
                Poll::Ready(v) => {
                    this.current = next_future(&mut this.elems);
                    Poll::Ready(Some(v))
                }
                Poll::Pending => {
                    this.current = Some(fut);
                    Poll::Pending
                }
            },
            None => Poll::Ready(None),
        }
    }
}

fn next_future<I>(elems: &mut I) -> Option<<I::Item as IntoFuture>::IntoFuture>
where
    I: Iterator,
    I::Item: IntoFuture,
{
    elems.next().map(IntoFuture::into_future)
}

#[cfg(test)]
mod test {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures::channel::mpsc;
    use futures::executor::block_on;
    use futures::future::ready;
    use futures::{Future, StreamExt, TryStreamExt};

    use super::*;

    #[test]
    fn test_basic() {
        assert_eq!(
            block_on(
                futures_ordered(vec![ready(Ok::<_, i32>(1)), ready(Ok(2))]).try_collect::<Vec<_>>()
            ),
            Ok(vec![1, 2])
        );

        assert_eq!(
            block_on(
                futures_ordered(vec![ready(Ok(1)), ready(Err(2)), ready(Ok(3))])
                    .try_collect::<Vec<_>>()
            ),
            Err(2)
        );
    }

    #[test]
    fn test_serial() {
        let (tx, rx) = mpsc::channel(2);
        // If both futures ran at once, rx would observe [20, 10].
        let futs = vec![delayed_future(10, tx.clone(), 4), delayed_future(20, tx, 2)];

        block_on(futures_ordered(futs).collect::<Vec<_>>());
        let results: Vec<_> = block_on(rx.collect());
        assert_eq!(results, vec![10, 20]);
    }

    fn delayed_future<T>(v: T, tx: mpsc::Sender<T>, count: usize) -> DelayedFuture<T> {
        DelayedFuture {
            send: Some((v, tx)),
            count,
        }
    }

    struct DelayedFuture<T> {
        send: Option<(T, mpsc::Sender<T>)>,
        count: usize,
    }

    impl<T: Unpin> Future for DelayedFuture<T> {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            this.count -= 1;
            if this.count == 0 {
                let (v, mut tx) = this.send.take().unwrap();
                tx.try_send(v).expect("send");
                Poll::Ready(())
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}
