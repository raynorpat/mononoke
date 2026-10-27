// Copyright (c) 2017-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::executor::block_on;
use futures::future::{ready, Ready, TryFuture};

use super::*;

fn poll_now<F>(fut: &mut F) -> Poll<F::Output>
where
    F: Future + Unpin,
{
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(&waker);
    Pin::new(fut).poll(&mut cx)
}

struct Upperer<'a>(&'a AtomicUsize);
impl<'a> Filler for Upperer<'a> {
    type Key = String;
    type Value = Ready<Result<String, ()>>;

    fn fill(&self, key: &Self::Key) -> Self::Value {
        self.0.fetch_add(1, Ordering::Relaxed);
        ready(Ok(key.to_uppercase()))
    }
}

#[test]
fn simple() {
    let count = AtomicUsize::new(0);
    let c = Asyncmemo::new_unbounded(Upperer(&count));

    assert!(c.is_empty());
    assert_eq!(c.len(), 0);

    assert_eq!(count.load(Ordering::Relaxed), 0);

    let v = block_on(c.get("foo")).unwrap();
    assert_eq!(v, "FOO");
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert!(!c.is_empty());
    assert_eq!(c.len(), 1);

    let v = block_on(c.get("foo")).unwrap();
    assert_eq!(v, "FOO");
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert_eq!(c.len(), 1);

    let v = block_on(c.get("bar")).unwrap();
    assert_eq!(v, "BAR");
    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert_eq!(c.len(), 2);
}

#[test]
fn clear() {
    let count = AtomicUsize::new(0);
    let c = Asyncmemo::new_unbounded(Upperer(&count));

    assert!(c.is_empty());
    assert_eq!(c.len(), 0);

    assert_eq!(count.load(Ordering::Relaxed), 0);

    let v = block_on(c.get("foo")).unwrap();
    assert_eq!(v, "FOO");
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert!(!c.is_empty());
    assert_eq!(c.len(), 1);

    c.clear();

    assert!(c.is_empty());
    assert_eq!(c.len(), 0);

    let v = block_on(c.get("foo")).unwrap();
    assert_eq!(v, "FOO");
    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert!(!c.is_empty());
    assert_eq!(c.len(), 1);
}

struct Delayed<F>
where
    F: Filler,
{
    inner: F,
    count: usize,
}

struct Delay<F> {
    fut: F,
    remains: usize,
}

impl<F> Future for Delay<F>
where
    F: TryFuture + Unpin,
{
    type Output = Result<F::Ok, F::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.remains == 0 {
            Pin::new(&mut this.fut).try_poll(cx)
        } else {
            this.remains -= 1;
            Poll::Pending
        }
    }
}

impl<F> Delayed<F>
where
    F: Filler,
{
    fn new(count: usize, inner: F) -> Self {
        Delayed { inner, count }
    }
}

impl<F> Filler for Delayed<F>
where
    F: Filler,
    F::Value: TryFuture + Unpin,
{
    type Key = F::Key;
    type Value = Delay<F::Value>;

    fn fill(&self, key: &Self::Key) -> Self::Value {
        Delay {
            fut: self.inner.fill(key),
            remains: self.count,
        }
    }
}

#[test]
fn more() {
    let count = AtomicUsize::new(0);
    let c = Asyncmemo::new_unbounded(Delayed::new(2, Upperer(&count)));

    let mut v = c.get("hello");
    assert_eq!(poll_now(&mut v), Poll::Pending);
    assert_eq!(poll_now(&mut v), Poll::Pending);

    match poll_now(&mut v) {
        Poll::Pending => panic!("unexpected not ready"),
        Poll::Ready(value) => assert_eq!(value.unwrap(), "HELLO"),
    }
}

#[test]
fn limit() {
    let count = AtomicUsize::new(0);
    let c = Asyncmemo::with_limits(Upperer(&count), 2, usize::MAX);

    assert_eq!(c.len(), 0);

    let v1 = block_on(c.get("hello")).unwrap();
    assert_eq!(v1, "HELLO");
    assert_eq!(c.len(), 1);

    let v2 = block_on(c.get("goodbye")).unwrap();
    assert_eq!(v2, "GOODBYE");
    assert_eq!(c.len(), 2);

    let v3 = block_on(c.get("world")).unwrap();
    assert_eq!(v3, "WORLD");
    assert_eq!(c.len(), 2);

    let v4 = block_on(c.get("ungulate")).unwrap();
    assert_eq!(v4, "UNGULATE");
    assert_eq!(c.len(), 2);
}

struct FillArced<F>(F);
struct Arcer<F>(F);

impl<F> Filler for FillArced<F>
where
    F: Filler,
    F::Value: TryFuture + Unpin,
{
    type Key = F::Key;
    type Value = Arcer<F::Value>;

    fn fill(&self, key: &F::Key) -> Self::Value {
        Arcer(self.0.fill(key))
    }
}

impl<F> Future for Arcer<F>
where
    F: TryFuture + Unpin,
{
    type Output = Result<Arc<F::Ok>, F::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.get_mut().0).try_poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(value)) => Poll::Ready(Ok(Arc::new(value))),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }
}

#[test]
fn expensive() {
    // Use Arc for expensive-to-copy values
    let count = AtomicUsize::new(0);
    let c = Asyncmemo::new_unbounded(FillArced(Upperer(&count)));

    let v = block_on(c.get("Hello")).unwrap();
    assert_eq!(*v, "HELLO");
}

#[test]
fn invalidate() {
    // Use Arc for expensive-to-copy values
    let count = AtomicUsize::new(0);
    let c = Asyncmemo::new_unbounded(FillArced(Upperer(&count)));

    let v = block_on(c.get("Hello")).unwrap();
    assert_eq!(*v, "HELLO");
    assert_eq!(count.load(Ordering::Relaxed), 1);

    let v = block_on(c.get("Hello")).unwrap();
    assert_eq!(*v, "HELLO");
    assert_eq!(count.load(Ordering::Relaxed), 1);

    c.invalidate("Hello");

    let v = block_on(c.get("Hello")).unwrap();
    assert_eq!(*v, "HELLO");
    assert_eq!(count.load(Ordering::Relaxed), 2);

    let v = block_on(c.get("Hello")).unwrap();
    assert_eq!(*v, "HELLO");
    assert_eq!(count.load(Ordering::Relaxed), 2);
}
