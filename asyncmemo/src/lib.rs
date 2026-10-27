// Copyright (c) 2017-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Asynchronous Memoizing Cache
//!
//! This crate implements a cache to memoize results calculated by some async process.
//!
//! The primary access method is `Asyncmemo::get()`. If the result has been previously calculated
//! and cached, then the result is directly returned from the cache.
//!
//! Otherwise an implementation of `Filler` which produces new results. Since the process
//! constructing the result is async, each query to fetch the result will poll the process
//! and will update the cache when it finishes.
//!
//! TODO: add interface to choose eviction policy/predicate
#![deny(warnings)]

use std::hash::Hash;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::future::TryFuture;

#[cfg(test)]
mod test;

mod boundedhash;
mod weight;

use boundedhash::BoundedHash;
pub use weight::Weight;

/// Asynchronous memoizing cache for async processes
///
/// The cache requires an instance of an implementation of the `Filler` trait
/// to generate new results.
pub struct Asyncmemo<F>
where
    F: Filler,
    F::Key: Eq + Hash,
{
    inner: Arc<AsyncmemoInner<F>>,
}

/// Generate a result for the cache.
///
/// The function implemented by `fill()` should be referentially transparent - the output
/// should only depend on the value of the `key` parameter.
/// It may fail - the failure will be propagated to one of the callers, and the result won't
/// be cached.
///
/// `Value` is a future whose `Output` is `Result<success, error>`. Only the success value is cached.
pub trait Filler {
    type Key;
    type Value: TryFuture;

    fn fill(&self, key: &Self::Key) -> Self::Value;
}

type CacheHash<F> = BoundedHash<<F as Filler>::Key, Slot<<F as Filler>::Value>>;

struct AsyncmemoInner<F>
where
    F: Filler,
    F::Key: Eq + Hash,
{
    hash: Mutex<CacheHash<F>>,
    filler: F,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum Slot<F>
where
    F: TryFuture,
{
    Waiting(F),
    Full(F::Ok),
}

impl<F> Weight for Slot<F>
where
    F: TryFuture,
    F::Ok: Weight,
{
    fn get_weight(&self) -> usize {
        match self {
            Slot::Waiting(_) => 0,
            Slot::Full(value) => value.get_weight(),
        }
    }
}

/// Pending result from a cache lookup
pub struct MemoFuture<F>
where
    F: Filler,
    F::Key: Eq + Hash,
{
    cache: Arc<AsyncmemoInner<F>>,
    key: F::Key,
}

impl<F> MemoFuture<F>
where
    F: Filler,
    F::Key: Eq + Hash + Weight + Clone + Unpin,
    F::Value: TryFuture + Unpin,
    <F::Value as TryFuture>::Ok: Weight + Clone,
{
    // Check for existing entry.
    fn handle_present(
        &self,
        hash: &mut CacheHash<F>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<<F::Value as TryFuture>::Ok>, <F::Value as TryFuture>::Error>> {
        // Do the lookup, and as much processing as we can while `ent` is in scope.
        // If we found it and have a result to return, then we can trim the cache once
        // `ent` goes out of scope.
        let found = if let Some(mut ent) = hash.entry(self.key.clone()) {
            let polled = match ent.get_mut() {
                // Still in progress - bump it along, update entry if complete.
                // We don't weigh the Future the Filler returns, so updating the Future
                // via `poll` won't change the weight.
                Slot::Waiting(fut) => match Pin::new(fut).try_poll(cx) {
                    Poll::Pending => None,
                    Poll::Ready(ready) => Some(ready),
                },
                // Got a complete value, return it directly
                Slot::Full(value) => return Poll::Ready(Ok(Some(value.clone()))),
            };

            match polled {
                None => return Poll::Pending,
                Some(Err(err)) => {
                    // A finished future must not be polled again, and failures are not cached.
                    ent.remove();
                    return Poll::Ready(Err(err));
                }
                Some(Ok(ret)) => {
                    let upd = Slot::Full(ret.clone());

                    // Now that `get_mut()` is out of scope, we can operate on `ent` again.
                    // Check to see if upd will fit into cache or not.
                    let trim = if ent.may_fit(&upd) {
                        // Store original in cache
                        ent.update(upd);

                        // trim in case update puts us over
                        true
                    } else {
                        // Delete entry we're not going to use
                        ent.remove();

                        // no need to trim
                        false
                    };

                    Some((ret, trim))
                }
            }
        } else {
            None
        };

        // If we did find something, then return it, and trim the cache if needed
        // now that `upd` is out of scope.
        let ret = found.map(|(ret, trim)| {
            if trim {
                hash.trim();
            }
            ret
        });
        Poll::Ready(Ok(ret))
    }

    fn handle_missing(
        &self,
        hash: &mut CacheHash<F>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<<F::Value as TryFuture>::Ok, <F::Value as TryFuture>::Error>> {
        // Does not exist - make space for it
        let ok = hash.trim_entries(1);
        assert!(ok, "Can't make room for one more entry?");

        let key = self.key.clone();

        // Get the future. If it is complete immediately, then insert the
        // value directly, otherwise store the future for further processing.
        let mut fut = self.cache.filler.fill(&key);

        match Pin::new(&mut fut).try_poll(cx) {
            Poll::Pending => {
                // We require the weight limit to be large enough to hold a key - otherwise
                // we can't hold the future to wait for it to complete. (We also discount the
                // possible weight of the future itself, since it might be dynamic.)
                let inserted = hash.insert(key, Slot::Waiting(fut)).is_ok();
                if !inserted {
                    panic!(
                        "key weight {} too large for limit {}",
                        self.key.get_weight(),
                        hash.weightlimit()
                    );
                }

                Poll::Pending
            }

            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),

            Poll::Ready(Ok(value)) => {
                // Try to insert the entry. If it's too large, then we just give on caching
                // the result and return it directly.
                // XXX If "too-large" entries are common, then optimize out the redundant clone
                // in that case.
                let _ = hash.insert(key, Slot::Full(value.clone()));

                Poll::Ready(Ok(value))
            }
        }
    }
}

impl<F> std::future::Future for MemoFuture<F>
where
    F: Filler,
    F::Key: Eq + Hash + Weight + Clone + Unpin,
    F::Value: TryFuture + Unpin,
    <F::Value as TryFuture>::Ok: Weight + Clone,
{
    type Output = Result<<F::Value as TryFuture>::Ok, <F::Value as TryFuture>::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut hash = this.cache.hash.lock().expect("locked poisoned");

        match this.handle_present(&mut hash, cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Some(ret))) => Poll::Ready(Ok(ret)),
            Poll::Ready(Ok(None)) => this.handle_missing(&mut hash, cx),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }
}

impl<F> Asyncmemo<F>
where
    F: Filler,
    F::Key: Hash + Eq + Weight,
    <F::Value as TryFuture>::Ok: Weight,
{
    /// Construct a new bounded cache. It enforces two distinct limits:
    /// - entrylimit - the max number of entries
    /// - weightlimit - the max abstract "weight" of the entries (both keys and values)
    ///
    /// Weight is typically memory use.
    pub fn with_limits(fill: F, entrylimit: usize, weightlimit: usize) -> Self {
        assert!(entrylimit > 0);
        assert!(weightlimit > 0);

        let inner = AsyncmemoInner {
            hash: Mutex::new(BoundedHash::new(entrylimit, weightlimit)),
            filler: fill,
        };

        Asyncmemo {
            inner: Arc::new(inner),
        }
    }

    /// Construct an unbounded cache.
    ///
    /// This is pretty dangerous for any non-toy use.
    pub fn new_unbounded(fill: F) -> Self {
        Self::with_limits(fill, usize::MAX, usize::MAX)
    }

    /// Look up a result for a particular key/arg
    ///
    /// The future will either complete immediately if the result is already
    /// known, or will wait for an in-progress result (perhaps failing), or
    /// initiate new process to generate a result.
    ///
    /// This only caches successful results - it does not cache errors as a
    /// negative cache.
    pub fn get<K: Into<F::Key>>(&self, key: K) -> MemoFuture<F> {
        MemoFuture {
            cache: self.inner.clone(),
            key: key.into(),
        }
    }

    /// Invalidate a specific key
    pub fn invalidate<K: Into<F::Key>>(&self, key: K) {
        let mut locked = self.inner.hash.lock().expect("lock poison");
        let key = key.into();
        let _ = locked.remove(&key);
    }

    /// Reset the cache. This removes all results (complete and in-progress) from the cache.
    /// This drops the futures of in-progress entries, which should propagate cancellation
    /// if necessary.
    pub fn clear(&self) {
        let mut locked = self.inner.hash.lock().expect("lock poison");

        locked.clear()
    }

    /// Trim cache size to limits.
    pub fn trim(&self) {
        let mut locked = self.inner.hash.lock().expect("lock poison");

        locked.trim_entries(0);
        locked.trim_weight(0);
    }

    /// Return number of entries in cache.
    pub fn len(&self) -> usize {
        let hash = self.inner.hash.lock().expect("lock poison");
        hash.len()
    }

    /// Return true if cache is empty.
    pub fn is_empty(&self) -> bool {
        let hash = self.inner.hash.lock().expect("lock poison");
        hash.is_empty()
    }
}

impl<F> Clone for Asyncmemo<F>
where
    F: Filler,
    F::Key: Eq + Hash,
{
    fn clone(&self) -> Self {
        Asyncmemo {
            inner: self.inner.clone(),
        }
    }
}
