// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Non-blocking, buffered decompression.

use std::fmt::{self, Debug, Formatter};
use std::io::{self, BufReader, Read};
use std::pin::Pin;
use std::task::{Context, Poll};

use bzip2::read::BzDecoder;
use flate2::read::GzDecoder;
use tokio::io::{AsyncRead, ReadBuf};
use zstd::stream::read::Decoder as ZstdDecoder;

use crate::membuf::with_poll_waker;

pub struct Decompressor<R> {
    d_type: DecompressorType,
    inner: Dec<R>,
}

enum Dec<R> {
    Bzip2(BzDecoder<R>),
    Gzip(GzDecoder<R>),
    Zstd(ZstdDecoder<'static, BufReader<R>>),
    Plain(R),
}

#[derive(Clone, Copy, Debug)]
pub enum DecompressorType {
    Bzip2,
    Gzip,
    Zstd,
    Uncompressed,
}

impl<R: Read> Decompressor<R> {
    pub fn new(r: R, dt: DecompressorType) -> Self {
        let inner = match dt {
            DecompressorType::Bzip2 => Dec::Bzip2(BzDecoder::new(r)),
            DecompressorType::Gzip => Dec::Gzip(GzDecoder::new(r)),
            // `Decoder::new` only fails if zstd cannot allocate a context.
            DecompressorType::Zstd => Dec::Zstd(ZstdDecoder::new(r).expect("zstd decoder")),
            DecompressorType::Uncompressed => Dec::Plain(r),
        };
        Decompressor { d_type: dt, inner }
    }

    pub fn get_ref(&self) -> &R {
        match &self.inner {
            Dec::Bzip2(dec) => dec.get_ref(),
            Dec::Gzip(dec) => dec.get_ref(),
            Dec::Zstd(dec) => dec.get_ref().get_ref(),
            Dec::Plain(r) => r,
        }
    }

    pub fn get_mut(&mut self) -> &mut R {
        match &mut self.inner {
            Dec::Bzip2(dec) => dec.get_mut(),
            Dec::Gzip(dec) => dec.get_mut(),
            Dec::Zstd(dec) => dec.get_mut().get_mut(),
            Dec::Plain(r) => r,
        }
    }

    pub fn into_inner(self) -> R {
        match self.inner {
            Dec::Bzip2(dec) => dec.into_inner(),
            Dec::Gzip(dec) => dec.into_inner(),
            Dec::Zstd(dec) => dec.finish().into_inner(),
            Dec::Plain(r) => r,
        }
    }
}

impl<R: Read> Read for Decompressor<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match &mut self.inner {
            Dec::Bzip2(dec) => dec.read(buf),
            Dec::Gzip(dec) => dec.read(buf),
            Dec::Zstd(dec) => dec.read(buf),
            Dec::Plain(r) => r.read(buf),
        }
    }
}

impl<R: Read + Unpin> AsyncRead for Decompressor<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let dest = buf.initialize_unfilled();
        let result = with_poll_waker(cx.waker(), || Read::read(this, dest));
        match result {
            Ok(n) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl<R> Debug for Decompressor<R> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("Decompressor")
            .field("decoder_type", &self.d_type)
            .finish()
    }
}
