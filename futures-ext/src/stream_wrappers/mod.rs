// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

pub mod take_while;

pub use self::take_while::TakeWhile;

/// A stream that wraps another stream. `into_inner` consumes this stream and
/// returns the original stream.
pub trait StreamWrapper<S> {
    fn into_inner(self) -> S;
}

/// Boxed form of `StreamWrapper`, for trait objects.
pub trait BoxStreamWrapper<S> {
    fn get_ref(&self) -> &S;
    fn get_mut(&mut self) -> &mut S;
    fn into_inner(self: Box<Self>) -> S;
}
