// Copyright (c) 2004-present, Facebook, Inc.
// All Rights Reserved.
//
// This software may be used and distributed according to the terms of the
// GNU General Public License version 2 or any later version.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::io::{self, Cursor};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use assert_matches::assert_matches;
use async_compression::{CompressorType, ZSTD_DEFAULT_LEVEL};
use futures::executor::block_on;
use futures::task::{waker, ArcWake};
use futures::{Future, StreamExt};
use mercurial_types::delta::Fragment;
use mercurial_types::{Delta, NodeHash, Path, NULL_HASH};
use partial_io::quickcheck_types::{GenWouldBlock, PartialWithErrors};
use partial_io::PartialAsyncRead;
use quickcheck::{Gen, QuickCheck};
use slog::{o, Drain, Logger};
use slog_term;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::bundle2::Bundle2Stream;
use crate::bundle2_encode::Bundle2EncodeBuilder;
use crate::changegroup;
use crate::errors::*;
use crate::part_encode::PartEncodeBuilder;
use crate::part_header::PartHeaderBuilder;
use crate::parts::changegroup_part;
use crate::types::StreamHeader;
use crate::utils::get_compression_param;
use crate::Bundle2Item;

const BZIP2_BUNDLE2: &[u8] = include_bytes!("fixtures/bzip2.bin");
const UNCOMP_BUNDLE2: &[u8] = include_bytes!("fixtures/uncompressed.bin");
const UNKNOWN_COMPRESSION_BUNDLE2: &[u8] = include_bytes!(
    "fixtures/unknown-compression.\
     bin"
);

const CHANGESET1_HASH_STR: &str = "b2040b24fd5cdfaf36e3164ddc357e834167b14a";
const CHANGESET2_HASH_STR: &str = "415ab71954c98ea93dab4b8f61f04ca57bc5c33c";
const MANIFEST1_HASH_STR: &str = "afcff2144f55cfa5d9b04ac4ed6598f26035aa77";
const MANIFEST2_HASH_STR: &str = "aa93dc3435cbfecd0c4c245b80b2a0b9ed35a015";
const ABC_HASH_STR: &str = "b80de5d138758541c5f05265ad144ab9fa86d1db";
const DEF_HASH_STR: &str = "bb969a19e8853962b4347bea4c24796324f10d8b";

#[test]
fn test_parse_bzip2() {
    let mut quickcheck = QuickCheck::new().rng(Gen::new(20));
    quickcheck.quickcheck(parse_bzip2 as fn(PartialWithErrors<GenWouldBlock>) -> ());
}

fn parse_bzip2(read_ops: PartialWithErrors<GenWouldBlock>) {
    parse_bundle(BZIP2_BUNDLE2, Some("BZ"), read_ops);
}

#[test]
fn test_parse_uncompressed() {
    let mut quickcheck = QuickCheck::new().rng(Gen::new(20));
    quickcheck.quickcheck(parse_uncompressed as fn(PartialWithErrors<GenWouldBlock>) -> ());
}

fn parse_uncompressed(read_ops: PartialWithErrors<GenWouldBlock>) {
    parse_bundle(UNCOMP_BUNDLE2, None, read_ops);
}

#[test]
fn test_parse_unknown_compression() {
    let bundle2_buf = Cursor::new(UNKNOWN_COMPRESSION_BUNDLE2.to_vec());
    let outer_stream_err = parse_stream_start(bundle2_buf, Some("IL")).unwrap_err();
    assert_matches!(outer_stream_err.kind(),
                    &ErrorKind::Bundle2Decode(ref msg) if msg == "unknown compression 'IL'");
}

#[test]
fn test_empty_bundle_roundtrip_zstd() {
    empty_bundle_roundtrip(CompressorType::Zstd {
        level: ZSTD_DEFAULT_LEVEL,
    });
}

#[test]
fn test_empty_bundle_roundtrip_uncompressed() {
    empty_bundle_roundtrip(CompressorType::Uncompressed);
}

fn empty_bundle_roundtrip(ct: CompressorType) {
    // Encode an empty bundle.
    let cursor = Cursor::new(Vec::with_capacity(32 * 1024));
    let mut builder = Bundle2EncodeBuilder::new(cursor);
    builder.set_compressor_type(ct);
    builder
        .add_stream_param("Foo".into(), "123".into())
        .unwrap();
    builder
        .add_stream_param("bar".into(), "456".into())
        .unwrap();
    let mut buf = block_on(builder.build()).unwrap();
    buf.set_position(0);

    // Now decode it.
    let logger = make_root_logger();
    let mut stream = Bundle2Stream::new(buf, logger);
    let item = next_bundle_item(&mut stream);

    let mut mparams = HashMap::new();
    let mut aparams = HashMap::new();
    mparams.insert("foo".into(), "123".into());
    mparams.insert("compression".into(), get_compression_param(&ct).into());
    aparams.insert("bar".into(), "456".into());
    let expected_header = StreamHeader {
        m_stream_params: mparams,
        a_stream_params: aparams,
    };

    assert_eq!(item, Some(Bundle2Item::Start(expected_header)));

    let item = next_bundle_item(&mut stream);
    assert!(item.is_none());
}

#[test]
fn test_unknown_part_zstd() {
    unknown_part(CompressorType::Zstd {
        level: ZSTD_DEFAULT_LEVEL,
    });
}

#[test]
fn test_unknown_part_uncompressed() {
    unknown_part(CompressorType::Uncompressed);
}

fn unknown_part(ct: CompressorType) {
    let cursor = Cursor::new(Vec::with_capacity(32 * 1024));
    let mut builder = Bundle2EncodeBuilder::new(cursor);

    builder.set_compressor_type(ct);

    let unknown_part = PartEncodeBuilder::mandatory("unknown:unknown").unwrap();

    builder.add_part(unknown_part);
    let mut buf = block_on(builder.build()).unwrap();
    buf.set_position(0);

    let logger = make_root_logger();
    let mut stream = Bundle2Stream::new(buf, logger);
    let mut parts = Vec::new();
    while let Some(part) = next_bundle_item(&mut stream) {
        parts.push(part);
    }

    // Only the stream header should have been returned.
    let mut m_stream_params = HashMap::new();
    m_stream_params.insert("compression".into(), get_compression_param(&ct).into());
    let expected = StreamHeader {
        m_stream_params,
        a_stream_params: HashMap::new(),
    };
    assert_eq!(parts, vec![Bundle2Item::Start(expected)]);

    // Make sure the error was accumulated.
    let app_errors = stream.app_errors();
    assert_eq!(app_errors.len(), 1);
    assert_matches!(app_errors[0].kind(),
                    &ErrorKind::BundleUnknownPart(ref header)
                    if header.part_type() == "UNKNOWN:UNKNOWN");
}

fn parse_bundle(
    input: &[u8],
    compression: Option<&str>,
    read_ops: PartialWithErrors<GenWouldBlock>,
) {
    let bundle2_buf = Cursor::new(input.to_vec());
    let partial_read = PartialAsyncRead::new(bundle2_buf, read_ops);
    let mut stream = parse_stream_start(partial_read, compression).unwrap();

    let res = next_bundle_item(&mut stream).unwrap();

    let mut header = PartHeaderBuilder::new("CHANGEGROUP").unwrap();
    header.add_mparam("version", "02").unwrap();
    header.add_aparam("nbchanges", "2").unwrap();
    let header = header.build(0);
    assert_eq!(res, Bundle2Item::Header(header));

    verify_cg2(&mut stream);

    assert!(next_bundle_item(&mut stream).is_none());

    // Make sure the stream is fused.
    assert!(next_bundle_item(&mut stream).is_none());
}

fn verify_cg2<R>(stream: &mut Bundle2Stream<R>)
where
    R: AsyncRead + Unpin,
{
    let res = next_cg2_part(stream);
    assert_eq!(*res.section(), changegroup::Section::Changeset);
    let chunk = res.chunk();

    // Verify that changesets parsed correctly.
    let changeset1_hash = NodeHash::from_str(CHANGESET1_HASH_STR).unwrap();
    assert_eq!(chunk.node, changeset1_hash);
    assert_eq!(chunk.p1, NULL_HASH);
    assert_eq!(chunk.p2, NULL_HASH);
    assert_eq!(chunk.base, NULL_HASH);
    assert_eq!(chunk.linknode, changeset1_hash);
    let frags = chunk.delta.fragments();
    assert_eq!(frags.len(), 1);
    assert_eq!(frags[0].start, 0);
    assert_eq!(frags[0].end, 0);
    assert_eq!(frags[0].content.len(), 98);

    let res = next_cg2_part(stream);
    assert_eq!(*res.section(), changegroup::Section::Changeset);
    let chunk = res.chunk();

    let changeset2_hash = NodeHash::from_str(CHANGESET2_HASH_STR).unwrap();
    assert_eq!(chunk.node, changeset2_hash);
    assert_eq!(chunk.p1, changeset1_hash);
    assert_eq!(chunk.p2, NULL_HASH);
    assert_eq!(chunk.base, NULL_HASH);
    assert_eq!(chunk.linknode, changeset2_hash);
    let frags = chunk.delta.fragments();
    assert_eq!(frags.len(), 1);
    assert_eq!(frags[0].start, 0);
    assert_eq!(frags[0].end, 0);
    assert_eq!(frags[0].content.len(), 102);

    let res = next_cg2_part(stream);
    assert_matches!(
        res,
        changegroup::Part::SectionEnd(changegroup::Section::Changeset)
    );

    // Verify basic properties of manifests.
    let res = next_cg2_part(stream);
    assert_eq!(*res.section(), changegroup::Section::Manifest);
    let chunk = res.chunk();

    let manifest1_hash = NodeHash::from_str(MANIFEST1_HASH_STR).unwrap();
    assert_eq!(chunk.node, manifest1_hash);
    assert_eq!(chunk.p1, NULL_HASH);
    assert_eq!(chunk.p2, NULL_HASH);
    assert_eq!(chunk.base, NULL_HASH);
    assert_eq!(chunk.linknode, changeset1_hash);

    let res = next_cg2_part(stream);
    assert_eq!(*res.section(), changegroup::Section::Manifest);
    let chunk = res.chunk();

    let manifest2_hash = NodeHash::from_str(MANIFEST2_HASH_STR).unwrap();
    assert_eq!(chunk.node, manifest2_hash);
    assert_eq!(chunk.p1, manifest1_hash);
    assert_eq!(chunk.p2, NULL_HASH);
    assert_eq!(chunk.base, manifest1_hash); // In this case there's a delta.
    assert_eq!(chunk.linknode, changeset2_hash);

    let res = next_cg2_part(stream);
    assert_matches!(
        res,
        changegroup::Part::SectionEnd(changegroup::Section::Manifest)
    );

    // Filelog section
    let res = next_cg2_part(stream);
    assert_eq!(*res.section(), changegroup::Section::Filelog(path(b"abc")));
    let chunk = res.chunk();

    let abch = NodeHash::from_str(ABC_HASH_STR).unwrap();
    assert_eq!(chunk.node, abch);
    assert_eq!(chunk.p1, NULL_HASH);
    assert_eq!(chunk.p2, NULL_HASH);
    assert_eq!(chunk.base, NULL_HASH);
    assert_eq!(chunk.linknode, changeset1_hash);
    assert_eq!(chunk.delta.fragments().len(), 0); // empty file

    let res = next_cg2_part(stream);
    assert_matches!(
        res,
        changegroup::Part::SectionEnd(ref section)
            if *section == changegroup::Section::Filelog(path(b"abc"))
    );

    let res = next_cg2_part(stream);
    assert_eq!(*res.section(), changegroup::Section::Filelog(path(b"def")));
    let chunk = res.chunk();

    let defh = NodeHash::from_str(DEF_HASH_STR).unwrap();
    assert_eq!(chunk.node, defh);
    assert_eq!(chunk.p1, NULL_HASH);
    assert_eq!(chunk.p2, NULL_HASH);
    assert_eq!(chunk.base, NULL_HASH);
    assert_eq!(chunk.linknode, changeset2_hash);
    assert_eq!(chunk.delta.fragments().len(), 1);

    // That's it, wrap this up.
    let res = next_cg2_part(stream);
    assert_matches!(
        res,
        changegroup::Part::SectionEnd(ref section)
            if *section == changegroup::Section::Filelog(path(b"def"))
    );

    let res = next_cg2_part(stream);
    assert_matches!(res, changegroup::Part::End);
}

fn path(bytes: &[u8]) -> Path {
    Path::new(bytes).unwrap()
}

fn parse_stream_start<R>(reader: R, compression: Option<&str>) -> Result<Bundle2Stream<R>>
where
    R: AsyncRead + Unpin,
{
    let mut m_stream_params = HashMap::new();
    let a_stream_params = HashMap::new();
    if let Some(compression) = compression {
        m_stream_params.insert("compression".into(), compression.into());
    }
    let expected = StreamHeader {
        m_stream_params,
        a_stream_params,
    };

    let logger = make_root_logger();

    let mut stream = Bundle2Stream::new(reader, logger);
    match block_on(stream.next()) {
        Some(Ok(item)) => {
            assert_eq!(item.stream_header(), expected);
            Ok(stream)
        }
        Some(Err(err)) => Err(err),
        None => panic!("bundle2 stream ended before the stream header"),
    }
}

fn make_root_logger() -> Logger {
    let plain = slog_term::PlainSyncDecorator::new(io::stdout());
    Logger::root(slog_term::FullFormat::new(plain).build().fuse(), o!())
}

fn next_cg2_part<R>(stream: &mut Bundle2Stream<R>) -> changegroup::Part
where
    R: AsyncRead + Unpin,
{
    next_bundle_item(stream).unwrap().inner_part().cg2_part()
}

fn next_bundle_item<R>(stream: &mut Bundle2Stream<R>) -> Option<Bundle2Item>
where
    R: AsyncRead + Unpin,
{
    match block_on(stream.next()) {
        Some(Ok(item)) => Some(item),
        Some(Err(err)) => panic!("bundle2 stream error: {}", err),
        None => None,
    }
}

#[test]
fn changegroup_part_emits_cg02_fulltext() {
    let node = NodeHash::from_str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let delta = Delta::new(vec![Fragment {
        start: 0,
        end: 0,
        content: b"alpha\n".to_vec(),
    }])
    .unwrap();
    let chunks = vec![
        changegroup::Part::CgChunk(
            changegroup::Section::Changeset,
            changegroup::CgDeltaChunk {
                node,
                p1: NULL_HASH,
                p2: NULL_HASH,
                base: NULL_HASH,
                linknode: node,
                flags: 0,
                delta,
            },
        ),
        changegroup::Part::SectionEnd(changegroup::Section::Changeset),
        changegroup::Part::End,
    ];

    let part = changegroup_part(futures::stream::iter(
        chunks.into_iter().map(Ok::<_, crate::errors::Error>),
    ))
    .unwrap();
    let cursor = Cursor::new(Vec::with_capacity(32 * 1024));
    let mut builder = Bundle2EncodeBuilder::new(cursor);
    builder.set_compressor_type(CompressorType::Uncompressed);
    builder.add_part(part);
    let mut buf = block_on(builder.build()).unwrap();
    buf.set_position(0);

    let logger = make_root_logger();
    let mut stream = Bundle2Stream::new(buf, logger);
    let _start = next_bundle_item(&mut stream).unwrap();
    let header = match next_bundle_item(&mut stream).unwrap() {
        Bundle2Item::Header(header) => header,
        other => panic!("expected changegroup header, got {:?}", other),
    };
    assert_eq!(header.part_type().as_str(), "CHANGEGROUP");
    assert_eq!(
        header.mparams().get("version").map(bytes::Bytes::as_ref),
        Some(&b"02"[..])
    );

    let part = next_bundle_item(&mut stream)
        .unwrap()
        .inner_part()
        .cg2_part();
    let chunk = part.chunk();
    assert_eq!(chunk.node, node);
    assert_eq!(chunk.base, NULL_HASH);
    assert_eq!(chunk.delta.fragments()[0].content, b"alpha\n");
}

struct TaskWakes(AtomicUsize);

impl ArcWake for TaskWakes {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Accepts every write. After the first flush, the next write returns `Pending`
/// once and wakes `cx`. That write is the compression trailer from `try_finish`.
struct FinishGate {
    buf: Vec<u8>,
    armed: bool,
    blocked_once: bool,
}

impl AsyncWrite for FinishGate {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.armed && !this.blocked_once {
            this.blocked_once = true;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().armed = true;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[test]
fn finish_pending_wakes_task_and_completes() {
    let flag = Arc::new(TaskWakes(AtomicUsize::new(0)));
    let task_waker = waker(Arc::clone(&flag));
    let mut cx = Context::from_waker(&task_waker);

    let mut builder = Bundle2EncodeBuilder::new(FinishGate {
        buf: Vec::new(),
        armed: false,
        blocked_once: false,
    });
    builder.set_compressor_type(CompressorType::Gzip);
    let mut encode = builder.build();

    match Pin::new(&mut encode).poll(&mut cx) {
        Poll::Pending => {}
        Poll::Ready(Ok(_)) => panic!("encode finished without a Pending finish write"),
        Poll::Ready(Err(err)) => panic!("encode failed: {err}"),
    }
    assert!(
        flag.0.load(Ordering::SeqCst) >= 1,
        "Pending during finish did not wake the task"
    );

    let writer = match Pin::new(&mut encode).poll(&mut cx) {
        Poll::Ready(Ok(writer)) => writer,
        Poll::Pending => panic!("encode stayed Pending after the writer was woken"),
        Poll::Ready(Err(err)) => panic!("encode failed: {err}"),
    };
    assert!(writer.blocked_once);
    assert!(writer.buf.starts_with(b"HG20"));
}

#[test]
fn chain_err_preserves_bundle2_decode_kind() {
    let err = Err::<(), Error>(Error::from("root"))
        .chain_err(|| ErrorKind::Bundle2Decode("bad stream".into()))
        .unwrap_err();
    assert_matches!(
        err.kind(),
        &ErrorKind::Bundle2Decode(ref msg) if msg == "bad stream"
    );
    assert!(!err.is_app_error());

    let header = PartHeaderBuilder::new("UNKNOWN").unwrap().build(0);
    let err = Err::<(), Error>(Error::from("root"))
        .chain_err(|| ErrorKind::BundleUnknownPart(header))
        .unwrap_err();
    assert!(err.is_app_error());
    assert_matches!(err.kind(), &ErrorKind::BundleUnknownPart(_));

    let io_err = io::Error::from(err);
    assert_eq!(StdError::source(&io_err).unwrap().to_string(), "root");
}
