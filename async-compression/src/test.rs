// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::cmp;
use std::io::{self, Cursor, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use bzip2::Compression;
use futures::executor::block_on;
use quickcheck::{quickcheck, TestResult};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

use crate::compressor::{Compressor, CompressorType};
use crate::decompressor::Decompressor;
use crate::membuf::MemBuf;
use crate::metered::{MeteredRead, MeteredWrite};
use crate::retry::retry_write;
use crate::ZSTD_DEFAULT_LEVEL;

quickcheck! {
    fn test_bzip2_roundtrip(input: Vec<u8>) -> TestResult {
        roundtrip(CompressorType::Bzip2(Compression::default()), &input)
    }

    fn test_gzip_roundtrip(input: Vec<u8>) -> TestResult {
        roundtrip(CompressorType::Gzip, &input)
    }

    fn test_noop_roundtrip(input: Vec<u8>) -> TestResult {
        roundtrip(CompressorType::Uncompressed, &input)
    }

    fn test_zstd_roundtrip(input: Vec<u8>) -> TestResult {
        roundtrip(CompressorType::Zstd { level: ZSTD_DEFAULT_LEVEL }, &input)
    }
}

fn roundtrip(ct: CompressorType, input: &[u8]) -> TestResult {
    let compressed_buf = MeteredWrite::new(Cursor::new(Vec::new()));
    let mut compressor = MeteredWrite::new(Compressor::new(compressed_buf, ct));
    let res = compressor.write_all(input);
    assert!(matches!(res, Ok(())));
    assert_eq!(compressor.total_thru(), input.len() as u64);

    let compressed_buf = compressor.into_inner().try_finish().unwrap();
    assert_eq!(
        compressed_buf.total_thru(),
        compressed_buf.get_ref().position()
    );
    let compressed_buf = compressed_buf.into_inner();
    let compressed = compressed_buf.get_ref();

    let read_buf = MeteredRead::new(MemBuf::new(compressed.len().max(1)));
    let mut decoder = MeteredRead::new(Decompressor::new(read_buf, ct.decompressor_type()));

    let written = decoder
        .get_mut()
        .get_mut()
        .get_mut()
        .write_buf(compressed)
        .expect("write_buf");
    assert_eq!(written as u64, compressed_buf.position());
    decoder.get_mut().get_mut().get_mut().mark_eof();

    let mut result = Vec::new();
    block_on(decoder.read_to_end(&mut result)).expect("read_to_end");
    assert_eq!(decoder.total_thru(), input.len() as u64);
    assert_eq!(
        decoder.get_ref().get_ref().total_thru(),
        compressed_buf.position()
    );
    assert_eq!(input, result.as_slice());
    TestResult::passed()
}

/// `AsyncRead` that yields a few bytes, then `Pending`, and does not implement
/// `std::io::Read`. `Decompressor::new` must accept it.
struct PausingRead {
    data: Vec<u8>,
    pos: usize,
    pause: bool,
}

impl PausingRead {
    fn new(data: Vec<u8>) -> Self {
        PausingRead {
            data,
            pos: 0,
            pause: true,
        }
    }
}

impl AsyncRead for PausingRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pause {
            this.pause = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.pause = true;
        if this.pos >= this.data.len() {
            return Poll::Ready(Ok(()));
        }
        let n = cmp::min(3, cmp::min(buf.remaining(), this.data.len() - this.pos));
        buf.put_slice(&this.data[this.pos..this.pos + n]);
        this.pos += n;
        Poll::Ready(Ok(()))
    }
}

#[test]
fn decompresses_async_read_that_is_not_std_read() {
    let raw = b"hello from an AsyncRead socket";
    let kinds = [
        CompressorType::Uncompressed,
        CompressorType::Gzip,
        CompressorType::Bzip2(Compression::default()),
        CompressorType::Zstd {
            level: ZSTD_DEFAULT_LEVEL,
        },
    ];
    for ct in kinds {
        let mut compressor = Compressor::new(Cursor::new(Vec::new()), ct);
        compressor.write_all(raw).unwrap();
        let compressed = compressor.try_finish().unwrap().into_inner();

        let mut dec = Decompressor::new(PausingRead::new(compressed), ct.decompressor_type());
        let mut out = Vec::new();
        block_on(dec.read_to_end(&mut out)).expect("decompress");
        assert_eq!(out, raw);
    }
}

struct FinishAfterCountTestWriter {
    counter: u8,
    fail_with: Option<io::ErrorKind>,
}

impl FinishAfterCountTestWriter {
    fn new_ok() -> Self {
        FinishAfterCountTestWriter {
            counter: 0,
            fail_with: None,
        }
    }

    fn new_failed(error_kind: io::ErrorKind) -> Self {
        FinishAfterCountTestWriter {
            counter: 0,
            fail_with: Some(error_kind),
        }
    }
}

impl Write for FinishAfterCountTestWriter {
    fn write(&mut self, _: &[u8]) -> io::Result<usize> {
        if self.counter == 3 {
            self.fail_with.map_or(Ok(42), |k| Err(io::Error::from(k)))
        } else {
            self.counter += 1;
            Err(io::Error::from(io::ErrorKind::Interrupted))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl AsyncWrite for FinishAfterCountTestWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Write::write(self.get_mut(), buf))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Pending
    }
}

#[test]
fn test_ok_after_retry() {
    let mut writer = FinishAfterCountTestWriter::new_ok();
    let buffer = [0; 10];
    let status = retry_write(&mut writer, &buffer).expect("should succeed");
    assert_eq!(status, 42);
}

#[test]
fn test_fail_after_retry() {
    let mut writer = FinishAfterCountTestWriter::new_failed(io::ErrorKind::NotFound);
    let buffer = [0; 10];
    let status = retry_write(&mut writer, &buffer);
    assert_eq!(status.unwrap_err().kind(), io::ErrorKind::NotFound);
}
