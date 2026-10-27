# Mononoke Mercurial Serve Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build this Mononoke tree with current stable Rust and serve a local Mercurial revlog repo so stock `hg` can clone and push it over ssh, HTTP, and HTTPS, including a mozilla-central clone that uses `generaldelta`, `revlogv1`, and `revlog-compression-zstd`.

**Architecture:** One `mononoke` process opens `<repo>/.hg` as a `RevlogRepo` and answers `HgCommands`. SSH stays a stdio relay: `hgcli serve --stdio` connects to `<repo>/.hg/mononoke.sock`. HTTP and HTTPS are a second listener in the same process, speaking Mercurial HTTP protocol v1, and they call the same `HgCommands`. History in `getbundle` is an uncompressed bundle2 whose changegroup 02 carries full-text deltas. Push appends snapshot revisions to the existing revlogs and rewrites `.hg/bookmarks`.

**Tech Stack:** Rust stable, edition 2021, Cargo workspace, tokio 1, futures 0.3, thiserror 2, clap 4, tokio `net::UnixListener` (Winsock `AF_UNIX` on Windows), `zstd`, `flate2`, `sha1`, `memmap2`, `bitflags` 2. No `nom`, no `error-chain`, no `tokio-core`, no `futures` 0.1.

**Spec:** `docs/superpowers/specs/2026-10-26-mononoke-serve-design.md`

## Global Constraints

- `cargo build --workspace` succeeds on this Windows machine with current stable Rust.
- One `mononoke` process serves a local revlog repository (the parent of `.hg`).
- A stock `hg` on this machine can clone that repo over `ssh://`, `http://`, and `https://`, and the clone has history and a working copy.
- A stock `hg push` over those same three transports writes new changesets into the served revlog store. Ordinary `hg` pointed at the served repo can see them afterwards.
- On Windows, the ssh handoff socket is opened with Winsock. On Unix, the existing Unix-domain socket stays.
- The revlog reader and writer handle repositories whose `.hg/requires` includes `generaldelta`, `revlogv1`, and `revlog-compression-zstd`.
- `eden-server` and `blobimport` are removed, along with crates that only they use.
- Mononoke does not clone mozilla-central itself.
- No login, accounts, or authorization.
- `hgqlserve` and `mercurial-graphql` stay.
- Requirement names other than `generaldelta`, `revlogv1`, and `revlog-compression-zstd`, plus names `RevlogRepo` already accepts, are not implemented. Opening a repo that lists anything else fails, and the error names the unknown requirement.
- `treemanifest` stays a recognized name and still fails open. Display text contains `treemanifest` and `not served`.
- Unknown-requirement Display text stays `Unknown requirement "<line>"`.
- Push appends to the revlog store the repo already uses. Snapshot base index equals the new revision's own index. Zstd is used when `.hg/requires` contains `revlog-compression-zstd`.
- `hgcli` implements only `serve --stdio`. It does not listen for HTTP.
- SSH socket path is `<repo>/.hg/mononoke.sock` on both platforms. Bind replaces a stale socket file when connect fails, and refuses the bind when connect succeeds.
- Bundle2 compression is `CompressorType::Uncompressed`.
- HTTP is protocol v1 only. `Content-Type` is `application/mercurial-0.1`. Do not advertise `http-v2` or any capability containing `v2`.
- Default HTTP bind is `127.0.0.1:8000`. Default HTTPS port is `8443`, and the HTTPS listener starts only when both `--tls-cert` and `--tls-key` are present.
- Automated tests run on Windows and shell out to `hg` on `PATH`. mozilla-central is a manual run, not part of `cargo test`.
- There is no git repo yet. Task 1 creates one. Each task commits only its own files.

## File structure

- `Cargo.toml`, `rust-toolchain.toml` — workspace, stable toolchain.
- `mercurial-types/` — hashes, paths, deltas, changesets. Leaf crate.
- `bookmarks/`, `bookmarks/stockbookmarks/` — bookmark trait and `.hg/bookmarks` read/write.
- `asyncmemo/`, `futures-ext/`, `async-compression/`, `sshrelay/` — support crates the serve path already uses.
- `mercurial/` — revlog read, append, `RevlogRepo::open`.
- `mercurial-bundles/` — bundle2 and changegroup 02. New `changegroup_part` lives in `src/parts/mod.rs`.
- `hgproto/` — `HgCommands` and the ssh codec.
- `server/` — bin `mononoke`: socket listener, HTTP(S), `RepoClient`, bundle build, push.
- `hgcli/` — bin `hgcli`: stdio relay only.
- `mercurial-graphql/`, `cmds/` — kept bins `dumprev`, `idxdump`, `hgqlserve`.
- `server/tests/serve.rs` — the automated clone/push test.
- Deleted: `eden-server/`, `blobrepo/`, `blobstore/`, `heads/`, `bookmarks/filebookmarks/`, `bookmarks/membookmarks/`, `cmds/blobimport.rs`, `tests/integration/`.

## Review Focus

These are the inputs most likely to be mishandled. The test that pins each one is named here and written in the task that owns the code.

- A generaldelta index entry whose base revision equals its own index is a snapshot. Reading it returns the full text, not an error. Test: `revlog_snapshot_base_equals_own_index_returns_full_text` (Task 5).
- A revlog chunk whose first bytes are `28 b5 2f fd` is a zstd frame. Reading it returns the decompressed payload. Test: `zstd_magic_chunk_decompresses` (Task 5).
- `unbundle` whose `heads` are not the server's current heads writes nothing and replies with `0`. Test: `unbundle_rejects_mismatched_heads_without_writing` (Task 11).
- `.hg/requires` containing a line the parser does not know fails open, and the error includes that line verbatim. Test: `unknown_requirement_names_the_line` (Task 5).
- An HTTPS clone that does not trust the server certificate fails. Test: `https_clone_fails_without_the_server_cert` (Task 13).

---

### Task 1: Toolchain, workspace, and delete the blob path

**Files:**
- Create: `rust-toolchain.toml`, `Cargo.toml`, `mercurial-types/Cargo.toml`
- Delete: `eden-server/`, `blobrepo/`, `blobstore/`, `heads/`, `bookmarks/filebookmarks/`, `bookmarks/membookmarks/`, `cmds/blobimport.rs`, `tests/integration/test-eden-server.t`
- Modify: `tests/integration/library.sh` (remove `blobimport` and `edenserver` functions only)
- Test: existing tests in `mercurial-types/`

**Interfaces:**
- Consumes: nothing
- Produces: package `mercurial-types` on edition 2021. Public modules stay `delta`, `nodehash`, `path`, `utils`, `repo`, `manifest`, `changeset`, `blobnode`, `blob`, `node`, `errors`. `NULL_HASH` and `NodeHash` stay. SHA-1 comes from the `sha1` crate, not `rust-crypto`. `Delta::new(frags: Vec<Fragment>) -> Result<Delta>` and `Fragment { start: usize, end: usize, content: Vec<u8> }` stay.

- [ ] **Step 1: Install stable Rust and create the workspace**

If `cargo` is not on `PATH`, install rustup from `https://win.rustup.rs/x86_64` with the default stable toolchain, then invoke `cargo` as `%USERPROFILE%\.cargo\bin\cargo.exe` until the shell picks it up.

`rust-toolchain.toml`:

```toml
[toolchain]
channel = "stable"
```

Root `Cargo.toml` members for this task: `mercurial-types` only. `resolver = "2"`. Package edition `"2021"`. Dependencies the crate actually needs after the port: `thiserror` 2, `sha1` 0.10, `ascii`, `itertools`, `serde` 1 with `derive`, `bytes` 1, `futures` 0.3. Dev-dependencies: `quickcheck`. Drop `rust-crypto`, `error-chain`, `heapsize`, and `heapsize_derive`.

- [ ] **Step 2: Run the existing tests and confirm they do not compile**

Run: `cargo test -p mercurial-types`

Expected: FAIL to compile.

- [ ] **Step 3: Delete the blob path and port `mercurial-types`**

Delete the paths listed above. Remove the `blobimport` and `edenserver` functions from `tests/integration/library.sh`. Leave `mononoke` and `hgmn`.

Port `mercurial-types` onto the dependencies in Step 1. Replace `error-chain` with `thiserror`. Keep the Display string `Unknown requirement` out of this crate; it lives in `mercurial`. Existing public behavior covered by this crate's tests stays.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mercurial-types`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git init
git add Cargo.toml rust-toolchain.toml mercurial-types tests/integration/library.sh
git commit -m "feat: start the cargo workspace and remove the blob path"
```

### Task 2: Bookmarks and the memo cache

**Files:**
- Create: `bookmarks/Cargo.toml`, `bookmarks/stockbookmarks/Cargo.toml`, `asyncmemo/Cargo.toml`
- Modify: root `Cargo.toml` members
- Test: existing tests in those crates

**Interfaces:**
- Consumes: `mercurial_types::NodeHash`
- Produces: `Bookmarks`, `BookmarksMut`, `ListBookmarks`, `Version` on futures 0.3 (`Future<Output = Result<...>>`, `Stream<Item = Result<...>>`). `StockBookmarks::read<P: Into<PathBuf>>(base: P) -> Result<Self>` still reads `.hg/bookmarks` and treats a missing file as empty. `StockBookmarks::write(&self, base: &Path) -> Result<()>` writes the same `<40-hex><space><name>\n` lines, sorted by name. `BookmarksMut::set` and `delete` update that map and write the file. `asyncmemo::Filler` and `Asyncmemo` stay available to `RevlogRepo`.

- [ ] **Step 1: Add the three packages to the workspace**

Edition 2021. `futures` 0.3, `thiserror` 2, `serde` 1. `stockbookmarks` depends on `bookmarks` and `mercurial-types`.

- [ ] **Step 2: Run the tests and confirm they do not compile**

Run: `cargo test -p bookmarks -p stockbookmarks -p asyncmemo`

Expected: FAIL to compile.

- [ ] **Step 3: Port the three crates and add `StockBookmarks::write`**

Preserve the read format documented on `StockBookmarks`. `write` emits one line per bookmark, hash then space then name, newline terminated, names sorted.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p bookmarks -p stockbookmarks -p asyncmemo`

Expected: PASS. Add a unit test `write_round_trip_sorts_names` if none exists: write two bookmarks, read them back, names match, file bytes are name-sorted.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml bookmarks asyncmemo
git commit -m "feat: port bookmarks and write .hg/bookmarks"
```

### Task 3: Compression, ssh framing, and stream helpers

**Files:**
- Create: `futures-ext/Cargo.toml`, `async-compression/Cargo.toml`, `sshrelay/Cargo.toml`
- Modify: root `Cargo.toml`
- Test: existing tests in those crates

**Interfaces:**
- Consumes: nothing from Task 2
- Produces, on futures 0.3 and tokio 1 IO traits:
  - `futures_ext::encode::encode` and `StreamExt::encode`
  - `futures_ext::decode::decode` and `StreamLayeredExt::decode` for `Stream<Item = Result<Bytes, E>>`
  - `StreamExt::streamfork`
  - `async_compression::Compressor`, `CompressorType`, `Decompressor`, `DecompressorType` including zstd
  - `sshrelay::{SshDecoder, SshEncoder, SshMsg, SshStream}` as tokio-util codecs

- [ ] **Step 1: Add the three packages**

`bytes` 1, `tokio` 1 (`io-util`, `macros`, `rt`), `tokio-util` with `codec`, `futures` 0.3, `zstd`, `bzip2`, `flate2`, `thiserror` 2, `slog`. Dev: `quickcheck`, `tokio` with `rt`.

- [ ] **Step 2: Run the tests and confirm they do not compile**

Run: `cargo test -p futures-ext -p async-compression -p sshrelay`

Expected: FAIL to compile.

- [ ] **Step 3: Port the three crates**

Keep the names in the Interfaces block. Implement them on futures 0.3. Do not take a dependency on `futures` 0.1 or `tokio-core`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p futures-ext -p async-compression -p sshrelay`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml futures-ext async-compression sshrelay
git commit -m "feat: port compression and ssh framing to tokio 1"
```

### Task 4: Port the mercurial crate far enough for its current tests

**Files:**
- Create: `mercurial/Cargo.toml`
- Modify: `mercurial/src/revlog/parser.rs`, `mercurial/src/**` as required to compile
- Test: existing `mercurial` tests, including `parser::test::test_header_0` and `test_header_1`

**Interfaces:**
- Consumes: `mercurial-types`, `stockbookmarks`, `asyncmemo`
- Produces: `Revlog::new(idx: Vec<u8>, data: Option<Vec<u8>>) -> Result<Revlog>`, `Revlog::from_idx_data`, `RevlogRepo::open<P: Into<PathBuf>>(base: P) -> Result<RevlogRepo>`, `Required`, `get_heads`, `changeset_exists`, `get_changeset_by_nodeid`, `get_manifest_by_nodeid`, `get_file_revlog`, `bookmarks`, `changesets`. `parser::header`, `parser::indexng`, `parser::deltachunk`, `parser::literal` keep those names and the `Entry` fields already in `parser.rs`.

- [ ] **Step 1: Add package `mercurial`**

Edition 2021. Deps: `flate2`, `lz4_flex`, `memmap2`, `bitflags` 2, `thiserror` 2, `futures` 0.3, `time` 0.3, `itertools`, path deps on `mercurial-types`, `stockbookmarks`, `asyncmemo`. No `nom`.

- [ ] **Step 2: Run the tests and confirm they do not compile**

Run: `cargo test -p mercurial`

Expected: FAIL to compile.

- [ ] **Step 3: Port the crate and replace the nom parsers**

Rewrite `parser.rs` as ordinary functions. Keep the header layout: 2-byte big-endian features, then 2-byte big-endian version (`0` revlog0, `1` revlog NG). NG index entry is 64 bytes: 6-byte offset, 2-byte flags, four `u32`s (compressed length, uncompressed length, base, link), two parent `u32`s, 32-byte node slot of which 20 bytes are the node id. `baserev == u32::MAX` stays `None`. Do not add zstd in this task. Existing tests must pass unchanged.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mercurial`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml mercurial
git commit -m "feat: port the mercurial revlog crate off nom"
```

### Task 5: Zstd chunks, generaldelta snapshots, and requirement errors

**Files:**
- Modify: `mercurial/src/revlog/parser.rs`, `mercurial/src/revlog/mod.rs`, `mercurial/src/revlogrepo.rs`
- Test: `mercurial` unit tests named below

**Interfaces:**
- Consumes: `Revlog::new`, `Required`, `RevlogRepo::open` from Task 4
- Produces: `Required::RevlogCompressionZstd`, displayed and parsed as `revlog-compression-zstd`. `RevlogRepo::open` returns `Err` whose Display is `Unknown requirement "<line>"` for an unknown line, and `treemanifest is not served` when `treemanifest` is present. `Revlog::get_rev` returns full text for a zstd snapshot whose base index equals its own index.

- [ ] **Step 1: Write the failing tests**

`zstd_magic_chunk_decompresses`: build a one-revision in-memory revlog with `Revlog::new`. Header features include `GENERAL_DELTA`. The only entry's base index is `0` (its own index). The chunk bytes are a zstd frame (`28 b5 2f fd`) of the bytes `hello revlog`. `get_rev(0)` returns a blob whose bytes are `hello revlog`.

`revlog_snapshot_base_equals_own_index_returns_full_text`: same shape, chunk is the existing uncompressed literal form of `snapshot-text`, base index `0`. `get_rev(0)` returns `snapshot-text`, not an error.

`unknown_requirement_names_the_line`: a temp `.hg/requires` whose lines are `store`, `revlogv1`, and `share-safe`, plus the minimum empty revlog files `open` needs, yields an error whose Display contains `share-safe`.

`treemanifest_is_not_served`: requirements `store`, `revlogv1`, `treemanifest` yield an error whose Display contains `treemanifest is not served`.

`zstd_requirement_is_accepted`: requirements `store`, `fncache`, `dotencode`, `generaldelta`, `revlogv1`, `revlog-compression-zstd` make `open` return `Ok`.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cargo test -p mercurial -- zstd_magic_chunk_decompresses`

Expected: FAIL to compile or FAIL the assertion. The other four new tests fail the same way.

- [ ] **Step 3: Implement the reader rules**

In `deltachunk` and `literal`, if the chunk starts with `28 b5 2f fd`, zstd-decompress the frame, then parse the decompressed bytes with the existing literal or delta parser.

In `construct_general`: if `baserev == idx`, the chunk is a snapshot; decompress and return it as the full text. If `baserev > idx`, return an error. If `baserev < idx`, keep the existing walk.

Add `RevlogCompressionZstd` to `Required` / `FromStr` / `Display` as `revlog-compression-zstd`. After a successful parse, if `Treemanifest` is in the set, return the treemanifest error before `Ok`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mercurial`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mercurial
git commit -m "feat: read zstd generaldelta snapshots and reject unknown requirements"
```

### Task 6: Append a snapshot revision

**Files:**
- Modify: `mercurial/src/revlog/mod.rs`
- Test: unit tests in that file

**Interfaces:**
- Consumes: `Revlog::new`, `Revlog::get_rev`, zstd and snapshot reads from Task 5
- Produces:

```rust
pub const INLINE_LIMIT: usize = 131072;

pub enum RevlogCompression {
    Zlib,
    Zstd,
}

pub struct RevlogAppend {
    pub node: NodeHash,
    pub p1: Option<NodeHash>,
    pub p2: Option<NodeHash>,
    pub linkrev: Option<u32>,
    pub text: Vec<u8>,
}

impl Revlog {
    pub fn append_snapshot(&mut self, rev: RevlogAppend, compression: RevlogCompression) -> Result<u32>;
}
```

The returned `u32` is the new revision index. The stored node id is `rev.node`, not a recomputed hash.

- [ ] **Step 1: Write the failing tests**

`append_snapshot_round_trips_zstd`: start from `Revlog::new(Vec::new(), None)`. `append_snapshot` with text `alpha\n` and `RevlogCompression::Zstd` writes the first entry, including a header with version 1 and features inline plus generaldelta. `get_rev` of the new index returns `alpha\n`. The stored chunk starts with `28 b5 2f fd`. The entry's base index equals its own index.

`append_snapshot_converts_past_inline_limit`: a generaldelta revlog that is inline, append a text larger than `INLINE_LIMIT` minus the index overhead, then `get_rev` returns that text and the header no longer has the inline flag. A second `append_snapshot` onto that non-inline revlog also round-trips.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cargo test -p mercurial append_snapshot_round_trips_zstd append_snapshot_converts_past_inline_limit`

Expected: FAIL (`append_snapshot` missing)

- [ ] **Step 3: Implement `append_snapshot`**

Write a snapshot: base index equals the new revision's own index, parents and linkrev and node taken from `RevlogAppend`. `Zstd` stores a zstd frame of the full text with no extra prefix byte. `Zlib` stores a zlib stream. If the revlog is inline and the written size would exceed `INLINE_LIMIT`, move existing data into the `.d` side file when a path is known, clear the inline flag, then append. For an in-memory revlog, the same split is the `data: Option<Vec<u8>>` already on `Revlog::new`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mercurial append_snapshot`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add mercurial
git commit -m "feat: append zstd generaldelta snapshot revisions"
```

### Task 7: Changegroup 02 part

**Files:**
- Create: `mercurial-bundles/Cargo.toml`
- Modify: `mercurial-bundles/src/parts/mod.rs`
- Test: a new unit test in `mercurial-bundles`

**Interfaces:**
- Consumes: `futures-ext`, `async-compression`, `mercurial_types::Delta`, `mercurial_types::NodeHash`, `NULL_HASH`
- Produces: `parts::changegroup_part(chunks: Vec<changegroup::Part>) -> Result<PartEncodeBuilder>`. The part is mandatory, type `changegroup`, mparam `version` = `02`. Payload is what `Cg2Packer` emits for those chunks. `Bundle2EncodeBuilder` stays, and callers still set `CompressorType::Uncompressed`.

- [ ] **Step 1: Add the package and the failing test**

`changegroup_part_emits_cg02_fulltext`: one `Part::CgChunk` for `Section::Changeset` whose `CgDeltaChunk` has `base` `NULL_HASH` and `Delta::new(vec![Fragment { start: 0, end: 0, content: b"alpha\n".to_vec() }])`, then `SectionEnd` and `End`. Encode it through `changegroup_part` and an uncompressed `Bundle2EncodeBuilder`. Decode with the existing bundle2 reader. The changegroup part version param is `02`, and the decoded chunk's node and delta bytes match.

- [ ] **Step 2: Run the test and confirm it fails**

Run: `cargo test -p mercurial-bundles changegroup_part_emits_cg02_fulltext`

Expected: FAIL (`changegroup_part` missing, or the package does not compile)

- [ ] **Step 3: Port `mercurial-bundles` and add `changegroup_part`**

Port the crate to futures 0.3, tokio 1, and thiserror 2 so its existing tests pass. `changegroup_part` feeds `chunks` through `Cg2Packer`, concatenates the `Chunk` bytes, and `set_data_bytes` on a mandatory `changegroup` part with mparam `version` = `02`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mercurial-bundles`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml mercurial-bundles
git commit -m "feat: encode a changegroup 02 bundle part"
```

### Task 8: Port hgproto

**Files:**
- Create: `hgproto/Cargo.toml`
- Modify: `hgproto/src/service.rs`, `hgproto/src/lib.rs` as required by the signature below
- Test: existing `hgproto` tests, including `service::test::hello`

**Interfaces:**
- Consumes: `mercurial-bundles`, `futures-ext`, `sshrelay`
- Produces:

```rust
pub type HgCommandRes<T> = futures::future::BoxFuture<'static, Result<T, Error>>;
```

`HgCommands` methods keep their names and argument lists from `hgproto/src/service.rs`, with two changes: `unbundle(&self, heads: Vec<NodeHash>, bundle: Bytes) -> HgCommandRes<Bytes>`, and `stream_out` may stay unimplemented. `Request::Unbundle { heads, bundle: Bytes }` carries the bundle. `HgService::call` still handles `Debugwireargs` itself.

- [ ] **Step 1: Add package `hgproto`**

Edition 2021. `futures` 0.3, `bytes` 1, `tokio-util`, `thiserror` 2, `slog`, `nom` is not used; port `sshproto` parsers the same way Task 4 ported revlog parsers, keeping the existing request tests green.

- [ ] **Step 2: Run the tests and confirm they do not compile**

Run: `cargo test -p hgproto`

Expected: FAIL to compile.

- [ ] **Step 3: Port `hgproto` onto `HgCommandRes`**

Default trait methods still return an error whose Display contains the command name (`unimplemented` equivalent). `hello` test still sees the capabilities map the dummy returns.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p hgproto`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml hgproto
git commit -m "feat: port hgproto to futures 0.3"
```

### Task 9: mononoke socket and hgcli relay

**Files:**
- Create: `server/Cargo.toml`, `hgcli/Cargo.toml`
- Modify: `server/src/listener.rs`, `server/src/main.rs`, `hgcli/src/serve/mod.rs`, `hgcli/src/main.rs`
- Test: `server/src/listener.rs` unit test `unix_socket_round_trip`

**Interfaces:**
- Consumes: `HgService`, `HgCommands`, `RepoClient` once it compiles against Task 8. `RevlogRepo::open`.
- Produces: bin `mononoke` from package `mononoke`. CLI: `mononoke [--debug] <REPODIR>`. It opens `<REPODIR>/.hg` and listens on `<REPODIR>/.hg/mononoke.sock`. Bin `hgcli`: `hgcli -R <REPO> serve --stdio` connects to that socket and copies stdin, stdout, and stderr. Any other `hgcli` mode returns an error whose Display contains `Only stdio server is supported`. Both bins use `tokio::net::UnixListener` and `tokio::net::UnixStream`.

- [ ] **Step 1: Write the failing socket test**

`unix_socket_round_trip` in `server`: bind a temp directory's `mononoke.sock` with the listener function, connect with `tokio::net::UnixStream`, write the bytes `ping`, and read them from the accepted socket. On Windows this is Winsock `AF_UNIX`.

`stale_socket_file_is_replaced`: create an empty regular file at `mononoke.sock`, call the listener, and expect `Ok`. A second listener on that same path, while the first is still accepting, returns `Err`.

- [ ] **Step 2: Run the test and confirm it fails**

Run: `cargo test -p mononoke unix_socket_round_trip`

Expected: FAIL (package missing or listener still on `tokio_uds`)

- [ ] **Step 3: Port `server` and `hgcli`**

Replace `tokio_uds` and `nix` with tokio 1 `UnixListener` / `UnixStream`. Keep the stale-socket behavior: if bind fails because the file exists, connect; on connect success return the error; on connect failure delete the file and bind again. Clap 4. `RepoClient` compiles against `HgCommandRes` but still serves the bookmark-only bundle it serves today. `hgcli` drops the port and address flags. `--stdio` is the only serve mode.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mononoke unix_socket_round_trip` and `cargo build -p hgcli`

Expected: PASS and the hgcli binary builds

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml server hgcli
git commit -m "feat: listen on mononoke.sock with tokio unix sockets"
```

### Task 10: getbundle sends history

**Files:**
- Create: `server/src/bundle.rs`, `server/tests/serve.rs`
- Modify: `server/src/repo.rs`, `mercurial-bundles/src/parts/mod.rs` only if Task 7's helper needs a caller-facing tweak
- Delete: `tests/integration/`
- Test: `server/tests/serve.rs` functions `ssh_clone_has_file_contents` and `ssh_pull_learns_bookmark_without_new_changesets`

**Interfaces:**
- Consumes: `RevlogRepo`, `append_snapshot` is not required yet, `changegroup_part`, `HgCommandRes`, `StockBookmarks::read`
- Produces:

```rust
pub fn missing_nodes(repo: &RevlogRepo, common: &[NodeHash]) -> Result<Vec<NodeHash>>;
pub fn history_part(repo: &RevlogRepo, common: &[NodeHash]) -> Result<PartEncodeBuilder>;
```

`missing_nodes` lists changelog nodes in revision order that are not equal to a `common` node and not an ancestor of one. `NULL_HASH` does not exclude real nodes. `history_part` emits one changegroup 02: changeset section, then manifest section, then one filelog section per path sorted by path bytes. Each `CgDeltaChunk.base` is `NULL_HASH`. The delta is `Delta::new(vec![Fragment { start: 0, end: 0, content: <raw full text> }])`. Changelog `linknode` is the changeset node. Manifest and filelog `linknode` is the changeset that introduced that node. `RepoClient::getbundle` adds that part, and when `listkeys` contains `bookmarks` it still adds the bookmark part. `listkeys` of `phases` is an empty map. `branchmap` returns `default` mapped to changelog heads. `wireprotocaps` stays `lookup`, `known`, `getbundle`, and the existing `bundle2=` value.

`server/tests/serve.rs` helpers, reused by later tasks:

```rust
fn sibling_bin(name: &str) -> PathBuf; // directory of CARGO_BIN_EXE_mononoke, plus name, .exe on Windows
fn init_zstd_repo(root: &Path) -> PathBuf; // hg init + one commit
fn write_ssh_shim(dir: &Path, hgcli: &Path, repo: &Path) -> PathBuf;
fn hg(repo: &Path, args: &[&str]) -> std::process::Output;
```

`init_zstd_repo` runs `hg` with `format.generaldelta=true`, `format.revlog-compression=zstd`, `format.sparse-revlog=false`, `format.use-persistent-nodemap=false`, `format.use-share-safe=false`, `format.use-dirstate-v2=false`, `format.exp-use-copies-side-data-changeset=false`. It writes `a.txt` containing `alpha\n`, commits with message `first`, and asserts `.hg/requires` is a subset of `store`, `fncache`, `dotencode`, `generaldelta`, `revlogv1`, `revlog-compression-zstd`, `usefncache`. Any other line fails the test with the file contents. `write_ssh_shim` writes a script the test passes as `ui.ssh`. The script ignores its arguments and execs `hgcli -R <repo> serve --stdio` with inherited stdio (`.cmd` on Windows, `.sh` otherwise).

- [ ] **Step 1: Write the failing tests**

`ssh_clone_has_file_contents`: `init_zstd_repo`, start `mononoke` on it, `hg clone ssh://user@dummy/repo dest` with the ssh shim and `HGPLAIN=1`. Dest `a.txt` is `alpha\n`. Dest `hg log -T {desc}` contains `first`.

`ssh_pull_learns_bookmark_without_new_changesets`: filesystem-clone the served repo with ordinary `hg clone`, add bookmark `test-bookmark` on the served repo, `hg pull` the dest through the ssh shim. Dest `hg bookmarks` contains `test-bookmark`. Dest changelog still has one changeset.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cargo build -p hgcli` and then `cargo test -p mononoke --test serve`

Expected: FAIL (clone has no file, or the test does not compile)

- [ ] **Step 3: Fill `getbundle` with history**

Implement `missing_nodes` and `history_part` as specified. `create_bundle` adds `history_part` and keeps the bookmark listkeys part. Implement `listkeys` for `bookmarks` from `StockBookmarks` and for `phases` as an empty map. Implement `branchmap` as `default` to heads. Delete `tests/integration/` in this task; `serve.rs` is the replacement.

- [ ] **Step 4: Run the tests**

Run: `cargo build -p hgcli` and then `cargo test -p mononoke --test serve`

Expected: PASS. `hg` must be on `PATH`. If it is not, install Mercurial so `hg version` works and rerun.

- [ ] **Step 5: Commit**

```bash
git add server tests/integration
git commit -m "feat: send changelog, manifest, and files in getbundle"
```

### Task 11: Push

**Files:**
- Create: `server/src/push.rs`
- Modify: `server/src/repo.rs`, `server/tests/serve.rs`
- Test: `ssh_push_appears_in_served_repo`, `unbundle_rejects_mismatched_heads_without_writing`

**Interfaces:**
- Consumes: `Revlog::append_snapshot`, `RevlogCompression`, `StockBookmarks::write`, `changegroup` unpacker, `HgCommands::unbundle(&self, heads, bundle) -> HgCommandRes<Bytes>`, `pushkey`
- Produces:

```rust
pub fn apply_unbundle(repo_path: &Path, heads: &[NodeHash], bundle: &[u8]) -> Result<Vec<u8>>;
```

If `heads` is not the current changelog head set, return `b"0\n"` and do not write. Otherwise apply the bundle2 changegroup, write snapshots with `RevlogCompression::Zstd` when `revlog-compression-zstd` is required and `Zlib` otherwise, update bookmarks from `pushkey` parts, and return `b"1\n"`. `RepoClient::unbundle` wraps that reply in an uncompressed bundle2 whose mandatory part is `reply:changegroup`. Bundle2 parts other than changegroup and bookmark pushkey are accepted and not stored. `pushkey` of namespace `bookmarks` updates `.hg/bookmarks` when `old` matches the current hash or the bookmark is absent and `old` is `NULL_HASH`. `wireprotocaps` adds `unbundle` and `pushkey` here, so `hello` advertises them before the HTTP task.

- [ ] **Step 1: Write the failing tests**

`ssh_push_appears_in_served_repo`: from a clone created like Task 10, write `a.txt` as `beta\n`, commit message `second`, `hg push` through the ssh shim. `hg log -T {desc}` in the served repo contains `second`. `a.txt` there is `beta\n`. A new ssh clone's `a.txt` is `beta\n`.

`unbundle_rejects_mismatched_heads_without_writing`: call `apply_unbundle` with a head list of one fresh random `NodeHash` and an empty bundle. The served repo's changelog length is unchanged. The returned bytes are `0\n`.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cargo test -p mononoke --test serve ssh_push`

Expected: FAIL

- [ ] **Step 3: Implement `apply_unbundle` and `pushkey`**

Head check first. Then unpack changegroup 02, reconstruct full text by applying each delta to the `base` node (the `NULL_HASH` base means the delta applies to empty), and `append_snapshot` onto the changelog, the manifest, and each filelog. Ignore other bundle parts. Bookmark `pushkey` uses `StockBookmarks`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mononoke --test serve`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server
git commit -m "feat: apply unbundle onto the revlog store"
```

### Task 12: HTTP clone and push

**Files:**
- Create: `server/src/http.rs`
- Modify: `server/src/main.rs`, `server/src/repo.rs` (`wireprotocaps`), `server/tests/serve.rs`
- Test: `http_clone_and_push`

**Interfaces:**
- Consumes: `HgCommands` from Tasks 10 and 11, including `branchmap`, `listkeys`, `getbundle`, `unbundle`, `pushkey`, `heads`, `known`, `hello`
- Produces: `mononoke [--debug] [--http-addr ADDR] [--http-port PORT] <REPODIR>`. Defaults `127.0.0.1` and `8000`. The HTTP listener runs in the same process as the socket. `serve_http` dispatches `cmd` to the same `RepoClient`.

```rust
pub async fn serve_http(listener: tokio::net::TcpListener, repo: Arc<Repo>) -> Result<()>;
```

HTTP v1 only. GET or POST `?cmd=<name>`. POST arguments are repeated `<name> <decimal-length>\n<value bytes>`. Response `Content-Type` is `application/mercurial-0.1`. Capabilities add `branchmap` and `listkeys` to the list Task 11 already extended with `unbundle` and `pushkey`. No capability string contains `v2`.

- [ ] **Step 1: Write the failing test**

`http_clone_and_push`: start `mononoke` with `--http-port` on an ephemeral port. `hg clone http://127.0.0.1:<port>/ dest`. Dest `a.txt` is `alpha\n`. Commit `second` / `beta\n` in dest, `hg push` to that URL. Served repo `hg log` contains `second` and `a.txt` is `beta\n`.

- [ ] **Step 2: Run the test and confirm it fails**

Run: `cargo test -p mononoke --test serve http_clone_and_push`

Expected: FAIL (connection refused, or the flag does not exist)

- [ ] **Step 3: Implement the HTTP v1 listener**

Parse `cmd` and the length-prefixed arguments. Call `RepoClient`. Write the response bytes with `Content-Type: application/mercurial-0.1`. No authentication.

- [ ] **Step 4: Run the test**

Run: `cargo test -p mononoke --test serve http_clone_and_push`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server
git commit -m "feat: serve Mercurial HTTP protocol v1"
```

### Task 13: HTTPS

**Files:**
- Modify: `server/src/main.rs`, `server/src/http.rs`, `server/Cargo.toml`, `server/tests/serve.rs`
- Test: `https_clone_and_push`, `https_clone_fails_without_the_server_cert`

**Interfaces:**
- Consumes: `serve_http` from Task 12
- Produces: flags `--tls-cert <PATH>` and `--tls-key <PATH>`, both required together, and `--https-port` default `8443`. When both files are present, the same process also serves HTTPS on that port with the same HTTP v1 protocol. `serve_http` stays the plaintext handler; TLS accepts on a `tokio-rustls` listener and then uses the same connection handler.

- [ ] **Step 1: Write the failing tests**

`https_clone_and_push`: generate a self-signed cert for `127.0.0.1` in the test (crate `rcgen` as a dev-dependency), pass `--tls-cert` and `--tls-key`, clone `https://127.0.0.1:<port>/` with `--config web.cacerts=<cert.pem>`. Dest `a.txt` is `alpha\n`. Push `second` the same way. Served `a.txt` is `beta\n`.

`https_clone_fails_without_the_server_cert`: the same URL with no `web.cacerts` override exits non-zero and does not create a working copy.

- [ ] **Step 2: Run the tests and confirm they fail**

Run: `cargo test -p mononoke --test serve https_clone`

Expected: FAIL

- [ ] **Step 3: Add the TLS listener**

`tokio-rustls` with the cert and key from the flags. Do not add a flag that disables client verification. If only one of `--tls-cert` and `--tls-key` is set, exit with an error that names the missing flag.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mononoke --test serve https_clone`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add server
git commit -m "feat: serve Mercurial over HTTPS"
```

### Task 14: Kept commands, workspace build, manual mozilla-central run

**Files:**
- Create: `mercurial-graphql/Cargo.toml`, `cmds/Cargo.toml`
- Modify: root `Cargo.toml`, `README.md` (replace the "does not build" caveat with the serve commands below)
- Test: `cargo build --workspace` and `cargo test --workspace`

**Interfaces:**
- Consumes: `mercurial::RevlogRepo`, bins `mononoke` and `hgcli`
- Produces: workspace members `mercurial-graphql` and `cmds`. Bins `dumprev`, `idxdump`, `hgqlserve`. `hgqlserve_build_info` stays behind `cfg(fbcode_build)` and is not a crates.io dependency. `cargo build --workspace` does not require that crate.

- [ ] **Step 1: Add the two packages with the failing build**

`cmds` bins point at `dumprev.rs`, `idxdump.rs`, and `hgqlserve.rs`. `hgqlserve` depends on `mercurial-graphql` and `mercurial`.

- [ ] **Step 2: Run the workspace build and confirm it fails**

Run: `cargo build --workspace`

Expected: FAIL until these crates are ported.

- [ ] **Step 3: Port `mercurial-graphql` and the three bins**

Same edition and dependency rules as the earlier tasks. No new behavior. Facebook-only build info stays cfg'd out.

- [ ] **Step 4: Run the workspace and the manual acceptance commands**

Run: `cargo test --workspace` then `cargo build --workspace`

Expected: PASS

Manual acceptance, after that pass. This downloads mozilla-central and is not part of `cargo test`. If `RevlogRepo::open` fails, stop. The error names the requirement. Do not implement that requirement in this step.

```bat
hg clone https://hg-edge.mozilla.org/mozilla-central C:\src\mozilla-central
cargo run -p mononoke -- C:\src\mozilla-central --http-port 8000
hg clone ssh://user@dummy/mozilla-central C:\src\mozilla-central-ssh
hg clone http://127.0.0.1:8000/ C:\src\mozilla-central-http
```

Use the Task 10 ssh shim with `ui.ssh` and `ui.remotecmd` unset (the shim execs `hgcli` itself). `hg log -l 1` in both clones prints a changeset. `hg status` in both is empty. Do not push to hg-edge.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml mercurial-graphql cmds README.md
git commit -m "feat: build the remaining crates in the workspace"
```
