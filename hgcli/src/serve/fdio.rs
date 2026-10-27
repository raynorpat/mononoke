// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

//! Provide access blocking IO sources/sinks asynchronously
//!
//! Adapter for blocking IO source (implements std::io::Read) or
//! sink (implements std::io::Write) to an async Stream or Sink.
//! The async portion is implemented with a `futures::channel::mpsc`
//! bounded channel pair.
use std::io::{self, Read, Write};
use std::thread;

use bytes::Bytes;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::executor::block_on;
use futures::SinkExt;
use futures::StreamExt;

const BUFSZ: usize = 8192;
const NUMBUFS: usize = 2;

/// Async adapter for `Read`
#[derive(Debug)]
pub struct Reader<R> {
    reader: R,
}

impl<R> Reader<R>
where
    R: Read + Send + 'static,
{
    /// Construct an adapter for a synchronous source.
    pub fn new(reader: R) -> Self {
        Reader { reader }
    }

    /// Return an async Stream containing chunks of data read from the
    /// `reader`. Each chunk is an instance of `std::io::Result`, but it will shut
    /// down after sending the first error encountered.
    ///
    /// This method starts a thread to manage the synchronous source. The thread will
    /// shut down once a read completes, either because it returned an error or because
    /// the channel shuts down.
    ///
    /// The two parameters are the number of queued buffers and the max buffer size.
    /// The buffer size should match the expected IO size; if its much larger then it
    /// could result in wasted memory - especially if there are a lot of queued buffers.
    /// The number of queued buffers governs the amount of coupling between the IO source
    /// and its consumer.
    ///
    /// The bufsz must be > 0; it will panic if it is zero.
    pub fn source(self, nbuffers: usize, bufsz: usize) -> Receiver<io::Result<Bytes>> {
        let (mut tx, rx) = channel(nbuffers);
        let mut reader = self.reader;

        if bufsz == 0 {
            panic!("Must have non-zero buffer size")
        }

        thread::spawn(move || loop {
            let mut buf = vec![0; bufsz];

            let read = match reader.read(&mut buf[..]) {
                Ok(0) => break,
                Ok(sz) => {
                    buf.truncate(sz);
                    Ok(Bytes::from(buf))
                }
                Err(err) => Err(err),
            };

            let iserr = read.is_err();
            if block_on(tx.send(read)).is_err() {
                break;
            }
            if iserr {
                break;
            }
        });

        rx
    }
}

/// Async adapter for `Write`
#[derive(Debug)]
pub struct Writer<W> {
    writer: W,
}

impl<W> Writer<W>
where
    W: Write + Send + 'static,
{
    /// Construct an adapter for a synchronous sink.
    pub fn new(writer: W) -> Self {
        Writer { writer }
    }

    /// Construct an async Sink
    ///
    /// The `nbuffers` parameter specifies how many buffers are queued before
    /// blocking; this defines the amount of coupling there is between the data
    /// producer and its sink.
    ///
    /// This method creates a thread to manage the synchronous sink. The thread
    /// exits if either the `Sender` is closed or if the write fails.
    pub fn sink(self, nbuffers: usize) -> Sender<Bytes> {
        let (tx, mut rx) = channel::<Bytes>(nbuffers);
        let mut writer = self.writer;

        thread::spawn(move || {
            while let Some(buf) = block_on(rx.next()) {
                if writer.write_all(&buf[..]).is_err() {
                    break;
                }
                if writer.flush().is_err() {
                    break;
                }
            }
        });

        tx
    }
}

/// Helper to produce an async stream for stdin
pub fn stdin() -> Receiver<io::Result<Bytes>> {
    Reader::new(io::stdin()).source(NUMBUFS, BUFSZ)
}

/// Helper to produce an async sink for stdout
pub fn stdout() -> Sender<Bytes> {
    Writer::new(io::stdout()).sink(NUMBUFS)
}

/// Helper to produce an async sink for stderr
pub fn stderr() -> Sender<Bytes> {
    Writer::new(io::stderr()).sink(NUMBUFS)
}
