// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Multiplex stdin/stdout/stderr over one byte stream.
//!
//! Each message is a netstring (`<len>:<payload>,`). The payload's first byte
//! selects the stream: 0 stdin, 1 stdout, 2 stderr.

use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

#[derive(Debug)]
pub struct SshDecoder;

#[derive(Debug)]
pub struct SshEncoder;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum SshStream {
    Stdin,
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SshMsg(SshStream, Bytes);

impl SshMsg {
    pub fn new(stream: SshStream, data: Bytes) -> Self {
        SshMsg(stream, data)
    }

    pub fn from_slice<T>(stream: SshStream, t: T) -> Self
    where
        T: AsRef<[u8]>,
    {
        Self::new(stream, Bytes::copy_from_slice(t.as_ref()))
    }

    pub fn stream(&self) -> SshStream {
        self.0
    }

    pub fn data(self) -> Bytes {
        self.1
    }
}

impl AsRef<[u8]> for SshMsg {
    fn as_ref(&self) -> &[u8] {
        self.1.as_ref()
    }
}

impl SshDecoder {
    pub fn new() -> Self {
        SshDecoder
    }
}

impl Default for SshDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for SshDecoder {
    type Item = SshMsg;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<SshMsg>> {
        let Some(mut data) = decode_netstring(buf)? else {
            return Ok(None);
        };
        if data.is_empty() {
            return Ok(None);
        }
        let stream = match data.split_to(1)[0] {
            0 => SshStream::Stdin,
            1 => SshStream::Stdout,
            2 => SshStream::Stderr,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "bad ssh stream",
                ));
            }
        };
        Ok(Some(SshMsg(stream, data)))
    }
}

impl SshEncoder {
    pub fn new() -> Self {
        SshEncoder
    }
}

impl Default for SshEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder<SshMsg> for SshEncoder {
    type Error = io::Error;

    fn encode(&mut self, msg: SshMsg, buf: &mut BytesMut) -> io::Result<()> {
        let mut payload = BytesMut::with_capacity(1 + msg.1.len());
        let stream_byte = match msg.0 {
            SshStream::Stdin => 0,
            SshStream::Stdout => 1,
            SshStream::Stderr => 2,
        };
        payload.put_u8(stream_byte);
        payload.extend_from_slice(&msg.1);
        encode_netstring(&payload, buf);
        Ok(())
    }
}

fn encode_netstring(payload: &[u8], dst: &mut BytesMut) {
    let header = format!("{}:", payload.len());
    dst.reserve(header.len() + payload.len() + 1);
    dst.extend_from_slice(header.as_bytes());
    dst.extend_from_slice(payload);
    dst.extend_from_slice(b",");
}

fn decode_netstring(buf: &mut BytesMut) -> io::Result<Option<Bytes>> {
    let colon = match buf.iter().position(|b| *b == b':') {
        Some(i) => i,
        None => {
            if buf.len() > 10 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "netstring length too long",
                ));
            }
            return Ok(None);
        }
    };
    if colon == 0 || !buf[..colon].iter().all(|b| b.is_ascii_digit()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid netstring length",
        ));
    }
    let len_str = std::str::from_utf8(&buf[..colon])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let len: usize = len_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let header = colon + 1;
    let total = header
        .checked_add(len)
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "netstring length overflow"))?;
    if buf.len() < total {
        return Ok(None);
    }
    if buf[header + len] != b',' {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "netstring missing comma",
        ));
    }
    let frame = buf.split_to(total);
    Ok(Some(frame.freeze().slice(header..header + len)))
}

#[cfg(test)]
mod test {
    use bytes::{BufMut, Bytes, BytesMut};
    use tokio_util::codec::{Decoder, Encoder};

    use super::SshStream::*;
    use super::*;

    trait ToBytes: AsRef<[u8]> {
        fn bytes(&self) -> Bytes {
            Bytes::copy_from_slice(self.as_ref())
        }
    }

    impl<T: AsRef<[u8]>> ToBytes for T {}

    #[test]
    fn encode_simple() {
        let mut buf = BytesMut::with_capacity(1024);
        let mut encoder = SshEncoder::new();

        encoder
            .encode(SshMsg::new(Stdin, b"ls -l".bytes()), &mut buf)
            .expect("encode failed");

        assert_eq!(buf.as_ref(), b"6:\x00ls -l,");
    }

    #[test]
    fn encode_zero() {
        let mut buf = BytesMut::with_capacity(1024);
        let mut encoder = SshEncoder::new();

        encoder
            .encode(SshMsg::new(Stdin, b"".bytes()), &mut buf)
            .expect("encode failed");

        assert_eq!(buf.as_ref(), b"1:\x00,");
    }

    #[test]
    fn encode_one() {
        let mut buf = BytesMut::with_capacity(1024);
        let mut encoder = SshEncoder::new();

        encoder
            .encode(SshMsg::new(Stdin, b"X".bytes()), &mut buf)
            .expect("encode failed");

        assert_eq!(buf.as_ref(), b"2:\x00X,");
    }

    #[test]
    fn encode_multi() {
        let mut buf = BytesMut::with_capacity(1024);
        let mut encoder = SshEncoder::new();

        encoder
            .encode(SshMsg::new(Stdin, b"X".bytes()), &mut buf)
            .expect("encode failed");
        encoder
            .encode(SshMsg::new(Stdout, b"Y".bytes()), &mut buf)
            .expect("encode failed");
        encoder
            .encode(SshMsg::new(Stderr, b"Z".bytes()), &mut buf)
            .expect("encode failed");

        assert_eq!(buf.as_ref(), b"2:\x00X,2:\x01Y,2:\x02Z,");
    }

    #[test]
    fn decode_simple() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"6:\x00ls -l,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(Some(ref res)) if res == &SshMsg::new(Stdin, b"ls -l".bytes()) => (),
            bad => panic!("decode failed: {:?}", bad.as_ref()),
        }
    }

    #[test]
    fn decode_zero() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"1:\x00,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(Some(ref res)) if res == &SshMsg::new(Stdin, b"".bytes()) => (),
            bad => panic!("decode failed: {:?}", bad.as_ref()),
        }
    }

    #[test]
    fn decode_one() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"2:\x00X,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(Some(ref res)) if res == &SshMsg::new(Stdin, b"X".bytes()) => (),
            bad => panic!("decode failed: {:?}", bad.as_ref()),
        }
    }

    #[test]
    fn decode_multi() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"2:\x00X,2:\x01Y,2:\x02Z,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(Some(ref res)) if res == &SshMsg::new(Stdin, b"X".bytes()) => (),
            bad => panic!("decode failed: {:?}", bad.as_ref()),
        }
        match decoder.decode(&mut buf) {
            Ok(Some(ref res)) if res == &SshMsg::new(Stdout, b"Y".bytes()) => (),
            bad => panic!("decode failed: {:?}", bad.as_ref()),
        }
        match decoder.decode(&mut buf) {
            Ok(Some(ref res)) if res == &SshMsg::new(Stderr, b"Z".bytes()) => (),
            bad => panic!("decode failed: {:?}", bad.as_ref()),
        }
    }

    #[test]
    fn decode_bad() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"2:\x03X,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(bad) => panic!("unexpected success: {:?}", bad),
            Err(_err) => (),
        }
    }

    #[test]
    fn decode_short_framing() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"3:\x02X,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(None) => (),
            bad => panic!("bad framing: {:?}", bad),
        }
    }

    #[test]
    fn decode_broken_framing() {
        let mut buf = BytesMut::with_capacity(1024);
        buf.put_slice(b"1:\x02X,");

        let mut decoder = SshDecoder::new();

        match decoder.decode(&mut buf) {
            Ok(bad) => panic!("unexpected success: {:?}", bad),
            Err(_err) => (),
        }
    }
}
