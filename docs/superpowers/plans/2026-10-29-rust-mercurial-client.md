# Rust Mercurial Client Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a native Rust Mercurial client for Windows and Linux that shares its repository engine with Mononoke and interoperates with stock Mercurial in the same working copy.

**Architecture:** Extend `mercurial` with transactions, repository operations, working-copy handling and transport-neutral exchange. Keep types and bundle codecs in their existing crates; add client wire framing in `hgproto` and a thin `hg-client` application, binary `hgr`. Migrate server repository algorithms to the engine without reducing server protocol support.

**Tech Stack:** Existing Rust stable/edition 2021 workspace, Tokio/futures, memmap2, zstd/flate2, clap, stock Mercurial 7.0.3 as test oracle; reqwest with rustls for client HTTP(S), system OpenSSH for SSH. Select a compatible reqwest release against the execution-time toolchain and official documentation; record its resolved version in validation evidence. No Python or stock-hg runtime fallback.

**Spec:** [Approved design](../specs/2026-10-29-rust-mercurial-client-design.md). Read it together with this plan.

## Global Constraints

- Windows and Linux, with Windows as the primary performance target.
- Stock Mercurial 7.0.3 is the initial Windows/Linux compatibility oracle.
- Implement clone, pull, push, update, status, diff, add, remove, commit, log and merge; include the approved minimal resolve/recover operations.
- Stock Mercurial and the Rust client must be able to alternate operations in the same repository and working copy, including continuing an unfinished merge.
- Initial engineering budget: at most 256 MiB of queued transfer payload, excluding separately measured indexes, current revision reconstruction, parser state and metadata.
- Aim for at least 2x stock Mercurial's warm clean-status speed on the Windows Mozilla checkout; this is aspirational until a baseline exists.
- Require no repeatable regression above 10% on the other controlled Windows scenarios, after accounting for measured variance; investigate misses and obtain an explicit decision before relaxing a gate.
- Use five measured warm samples after a warm-up for short commands; at least three for expensive local transfer/update cases.
- `C:\src\mozilla-central` is the read-only source fixture. Public clone/pull from `https://hg-edge.mozilla.org/mozilla-central/` is mandatory acceptance; never push there.
- Keep `hgcli` as the SSH relay. Do not modify unrelated server/LFS work, automatically convert repositories, introduce a mandatory daemon, or load Python extensions/hooks.
- Preserve supported v1/v2 dirstates; new repositories use share-safe, zstd and dirstate v2 once its interoperability tests pass. Implement exactly the design's store/protocol support table.
- Existing legacy server capabilities remain available; the new client deliberately does not negotiate bundle1, changegroup 01, stream v1, changegroup 04 or experimental repository formats.

## Review Focus

1. Same-size file edits within one timestamp tick must not appear clean (Tasks 13, 15).
2. Stock `hg` replacing or truncating mapped store/dirstate files must invalidate Rust views safely, especially on Windows (Tasks 4, 13).
3. Killing a writer between file data, docket and journal publication must allow opposite-client recovery (Tasks 5, 6, 7, 19, 26).
4. Case/Unicode aliases, Windows reserved paths and symlink traversal must not overwrite another tracked path or `.hg` (Tasks 3, 19).
5. Lost push responses, cross-origin redirects and a changing remote head set must not cause duplicate replay, credential leakage or false success (Tasks 10, 11, 21, 30).

---

## Execution context and gates

This is one integrated plan: transactions, exchange and working-copy operations
share interfaces and a release contract. The gates provide independently
testable checkpoints, not reduced-scope releases.

| Gate | Tasks | Evidence before proceeding |
|---|---|---|
| A: safe shared store | 1-7 | Reference fixtures, requirement checks, locking, journaling, revlog/nodemap writes and opposite-client recovery. |
| B: shared exchange | 8-12 | Streaming engine, stock wire adapters, server migration and unchanged server compatibility. |
| C: read and checkout | 13-20 | Dirstate, names/DAG, status/log/diff, CLI, checkout and clone/pull. |
| D: write and merge | 21-26 | Push, add/remove, normal commits, merge/resolve, dirty update and merge commits. |
| E: release evidence | 27-30 | Windows/Linux matrix, fuzz/failure tests, benchmarks, Mozilla acceptance and packaging. |

At execution time inspect `git status`, `git log` and applicable instructions;
use an isolated worktree through the using-git-worktrees skill. At planning
time HEAD is `bb45fb8` and server/protocol edits are in progress. Preserve
them and reconcile committed interfaces before extraction. Do not copy
unrelated unfinished changes into the client work or reset the shared checkout.

Baseline commands, using the actual package name `mononoke`:

```text
cargo test --workspace --exclude mononoke
cargo test -p mononoke --lib
cargo test -p mononoke --test serve -- --test-threads=1
```

Record pre-existing failures separately. Every task uses a red/green cycle:
the initial failure must exercise the intended missing behavior, then the
named command must pass. A compile failure is acceptable only when the task
first introduces the referenced API. Test snippets below specify assertions
inside named tests; fixtures named in prose are constructed in that test,
not undeclared production APIs. Use failpoints only in test builds.

Each commit step stages exactly that task's files and uses its short message;
inspect the staged diff first. Format only changed files. Do not run unrelated
workspace-wide formatting or repeatedly run the entire suite per small task.

## File and interface map

Create modules only in the task that makes them useful. `mod.rs` exports its
task's module; `lib.rs` and relevant Cargo manifests are implicit wiring files
for every added module, explicitly owned by the task that first adds it.

| Files | Responsibility |
|---|---|
| `tools/hg-test-support/{Cargo.toml,src/lib.rs}`, `tools/hg-test-support/src/{fixture,process,snapshot}.rs` | Isolated reference-hg fixtures, child processes and snapshots; dev dependency only. |
| `mercurial/src/{requirements,repository}.rs` | Client/server access profiles and an engine wrapper around existing RevlogRepo. |
| `mercurial/src/store/{mod,guard,journal,transaction,recovery}.rs` | Filesystem identity, locks, journal-backed store writes and recovery. |
| `mercurial/src/metadata.rs` | Branch, bookmarks, phases and tags interpretation/publication. |
| `mercurial/src/exchange/{mod,types,ingest,stream_receive,history,stream,discovery}.rs` | Transfer policy, streaming records, ingestion/export, stream-clone reception and DAG discovery. |
| `mercurial/src/workcopy/{mod,paths,dirstate,v1,v2,tracked_hint,matcher,status,update,tracking}.rs` | Byte-safe working paths, dirstate, ignores, status, checkout and scheduled changes. |
| `mercurial/src/{history,diff,commit}.rs`, `mercurial/src/merge/{mod,ancestor,plan,state,text,tools,resolve}.rs` | Read operations, commit construction, merge planning/state/resolution. |
| `hgproto/src/client/{mod,capabilities,request,response}.rs` | Client direction of the existing wire protocol, with requests separated from streaming bodies. |
| `hg-client/{Cargo.toml,src/lib.rs,src/main.rs}`, `hg-client/src/{cli,config,output,transfer}.rs`, `hg-client/src/peer/{mod,http,ssh,local,clonebundles}.rs` | Application, transport peers and command dispatch. Package `hg-client`, binary `hgr`. |
| `hg-client/src/commands/{mod,read,exchange,write,merge}.rs` | Small command adapters calling the engine, not duplicate repository logic. |
| `mercurial/tests/*.rs`, `hgproto/tests/client.rs`, `hg-client/tests/*.rs` | New focused integration tests named in tasks. Existing `server/tests/serve.rs` retains server regressions. |
| `hg-client/examples/bench.rs`, `hg-client/benches/scenarios.toml`, `docs/client-{compatibility,benchmarks,usage}.md` | Reproducible benchmark runner, support contract and usage. |
| `.github/workflows/client.yml`, `fuzz/{Cargo.toml,fuzz_targets/*.rs}` | Platform matrix and isolated fuzz workspace, added only at Task 27/28. |

Common production contracts are introduced in Task 2. `Result<T>` means
`mercurial::Result<T>` inside the engine. `HgPath` is an import alias for
existing `mercurial_types::Path`, while `std::path::Path` is always qualified
in API declarations below. Existing `NodeHash`, `RevlogRepo`, `RevlogAppend`
and `RevlogCompression` are reused. Requests are typed structures, not bags
of CLI strings. Errors carry structured kinds including unsupported feature,
corrupt input, lock contention, conflict, interrupted operation, cancellation
and indeterminate remote result; display adapters supply human text.

## Tasks

### Task 1: Build reference fixtures and record protocol/format evidence

**Files:** Create test-support files above and `docs/client-compatibility.md`; modify root `Cargo.toml`, `mercurial/Cargo.toml` (dev dependency). Test: `tools/hg-test-support/tests/reference.rs`.
**Dependencies:** None.
**Interfaces:** Produce `Fixture::new() -> Fixture`, `root() -> &std::path::Path`, `write(path: &str, bytes: &[u8])`, `hg(args: &[&str]) -> CommandOutput`, `client(binary: &std::path::Path, args: &[&str]) -> CommandOutput`, `snapshot() -> RepoSnapshot`. `CommandOutput { code: i32, stdout: Vec<u8>, stderr: Vec<u8> }`; `RepoSnapshot` is comparable and records file bytes/flags, requirements, parents, tip, copies, phases, bookmarks, merge state and journal presence, excluding disposable caches. The helper asserts on spawn errors; tests inspect exit codes.

- [ ] Add `reference_is_isolated` and `snapshot_detects_repository_change`: two deterministic stock commits must change the snapshot; injected host config/extensions cannot affect the fixture. Assert `assert_eq!(f.hg(&["verify"]).code, 0);` and `assert_ne!(before, after);`.
- [ ] Run `cargo test -p hg-test-support --test reference`; expect the new fixture contract to fail before implementation.
- [ ] Implement fixtures with fresh per-test home/config, `HGPLAIN=1`, empty controlled `HGRCPATH`, fixed author/date and child-process timeouts. Read pinned upstream `transaction.py`, `localrepo.py`, `merge.py`, `mergestate.py`, `dirstateutils/{docket,v2}.py`, `requirements.py`, `streamclone.py`, `wireprotov1peer.py`; record exact version and recovery/format expectations in the compatibility document.
- [ ] Add a read-only public capability probe to the reference test as an explicitly ignored `public_peer_contract` test. Run `cargo test -p hg-test-support --test reference public_peer_contract -- --ignored --nocapture` when network is available. Record raw capabilities and supported path; unavailable network is recorded as blocked, never success. Proceed with independent work, but resolve any mandatory-format mismatch before public-transfer implementation is finalized.
- [ ] Rerun the local tests, require PASS, and commit `test: add isolated Mercurial reference fixtures`.

### Task 2: Establish repository access profiles and shared operation types

**Files:** Create `mercurial/src/{requirements,repository}.rs`; modify `mercurial/src/{lib,errors,revlogrepo}.rs`; test `mercurial/tests/requirements.rs`.
**Dependencies:** 1.
**Interfaces:** Produce `Repository::open(root: &std::path::Path, access: Access) -> Result<Repository>`, `Repository::store(&self) -> &RevlogRepo`, `Repository::root(&self) -> &std::path::Path`. `Access::{ServerStore,ClientStore,ClientWorkingCopy}`; `Operation::{Read,Pull,Push,Update,Track,Commit,Merge,Recover}`. Produce `OperationContext { cancel: Arc<AtomicBool>, limits: Limits }`, `Limits { queued_payload_bytes: usize, cache_bytes: usize }` with defaults 268435456 and 67108864 respectively; cache default is a tuning choice, not a total-RSS promise. Produce `Parents { p1: NodeHash, p2: NodeHash }` and `FileFlags { executable: bool, symlink: bool }`. Existing low-level open remains available to server callers during migration.

- [ ] Add `client_rejects_server_only_semantics`, `split_requirements_are_unioned`, `v1_without_generaldelta_is_readable`, `working_copy_requirements_are_not_store_requirements`. For LZ4/manifestv2/sqldirstate/LFS or unknown requirements assert `assert!(client_open.is_err()); assert_eq!(before, after);`; server profile preserves existing accepted behavior.
- [ ] Run `cargo test -p mercurial --test requirements`; expect missing profile behavior.
- [ ] Implement exact format policy from the design, including experimental copies-sidedata/internal-phase/bookmarks-in-store rejection unless covered by the approved set. Validate extension-owned markers such as LFS flags, obsolescence state and `.hgsubstate` where relevant; never reinterpret unsupported content as ordinary files. Profiles may reject a working copy while allowing supported no-update history access.
- [ ] Rerun the focused tests and existing `cargo test -p mercurial --lib`; require PASS.
- [ ] Commit `mercurial: separate client and server requirement profiles`.

### Task 3: Enforce repository-path and platform filesystem safety

**Files:** Create `mercurial/src/workcopy/{mod,paths}.rs`; modify `mercurial-types/src/path.rs` only for reusable byte validation; test `mercurial/tests/paths.rs`.
**Dependencies:** 2.
**Interfaces:** Produce `audit_checkout_paths(root: &std::path::Path, paths: &[HgPath]) -> Result<()>`, `working_path(root: &std::path::Path, path: &HgPath) -> Result<PathBuf>` and `safe_replace(root: &std::path::Path, path: &HgPath, source: &mut dyn Read, flags: FileFlags) -> Result<()>`. Validate again while opening parents; preflight alone is not protection against a swapped symlink.

- [ ] Add `rejects_hg_alias_and_parent_traversal`, `windows_collisions_preflight_without_writes`, `symlink_parent_cannot_escape`, `linux_non_utf8_roundtrip`. Feed `.hg`, case aliases, trailing dots/spaces, drive/UNC paths, reserved devices and decomposed/composed names according to actual filesystem aliasing. Assert `assert_eq!(outside_before, outside_after); assert_eq!(working_before, working_after);` on rejection.
- [ ] Run `cargo test -p mercurial --test paths`; expect unsupported audit cases.
- [ ] Implement component-aware path auditing and platform conversion without lossy string round trips. Preserve Linux byte paths and stock Windows symlink/execute-bit behavior. New content uses same-directory temporary files and safe replacement; no recursive deletion derived from unverified paths.
- [ ] Rerun on Windows and Linux; platform-specific tests report explicit capability skips only when the filesystem truly lacks that capability.
- [ ] Commit `mercurial: audit working-copy filesystem paths`.

### Task 4: Add lock guards and safe repository-view invalidation

**Files:** Create `mercurial/src/store/{mod,guard}.rs`; modify `mercurial/src/{lock,repository}.rs`, `mercurial/src/revlog/mod.rs`; test `mercurial/tests/locking.rs`.
**Dependencies:** 2.
**Interfaces:** Produce `Repository::lock(&self, scope: LockScope, ctx: &OperationContext) -> Result<RepoGuard<'_>>`, `LockScope::{Store,WorkingCopyAndStore}`, `RepoGuard::repo(&self) -> &Repository`, and `Repository::refresh(&self) -> Result<()>` using internally synchronized views. Guard owns locks for its lifetime; update acquires wlock before store lock. No raw mmap slices may survive a view refresh or lock boundary.

- [ ] Add `hg_and_rust_writers_exclude_each_other`, `cancel_lock_wait`, `outside_replacement_invalidates_view`, `truncated_map_is_not_dereferenced`. Assert `assert_eq!(observed_tip, stock_tip);` after external replacement and that a cancelled wait has acquired no locks.
- [ ] Run `cargo test -p mercurial --test locking -- --test-threads=1`; expect missing guard/invalidation behavior.
- [ ] Build on existing `RepoLock`, use a 600-second default wait with cancellation, compare file identity/length/stamps and known tip/docket data, revalidate after lock acquisition. Mutable or truncatable file access must be protected or buffered; do not rely on mmap catching concurrent truncation. Follow stock stale-lock rules; never remove a live lock blindly.
- [ ] Rerun focused tests and existing lock tests; require PASS on both platforms.
- [ ] Commit `mercurial: guard repository views with interoperable locks`.

### Task 5: Implement Mercurial journals and cross-client recovery

**Files:** Create `mercurial/src/store/{journal,transaction,recovery}.rs`; test `mercurial/tests/recovery.rs`.
**Dependencies:** 1, 3, 4.
**Interfaces:** Define `StoreTransaction<'guard,'repo>` with `begin(guard: &'guard RepoGuard<'repo>, description: &[u8]) -> Result<Self>`; the transaction cannot outlive the guard. Methods `append(&mut self, store_path: &HgPath, bytes: &[u8]) -> Result<()>`, `replace(&mut self, location: Location, path: &HgPath, bytes: &[u8]) -> Result<()>`, `commit(self) -> Result<()>`, `abort(self) -> Result<()>`; `Location::{Store,Hg}`. Later signatures elide these transaction lifetimes. Produce `recover(repo: &Repository, ctx: &OperationContext) -> Result<bool>`. Define test failpoints at before/after journal flush, append, backup, replace and transaction close; default builds contain no environment-triggered failpoints.

- [ ] Add `stock_recovers_rust_journal`, `rust_recovers_stock_journal`, `new_file_removed_on_abort`, `metadata_backup_replayed`. Generate real interrupted stock transactions in a child using a controlled test-only fail mechanism. At each Rust failpoint assert `assert_eq!(recovered, before); assert_eq!(f.hg(&["verify"]).code, 0);` after opposite-client recovery.
- [ ] Run `cargo test -p mercurial --test recovery -- --test-threads=1`; expect journal/recovery gaps.
- [ ] Implement the reference journal and backupfiles formats, VFS locations and truncation/removal ordering from Task 1. Match `localrepo._writejournal` description records and `transaction.rollback`; make journals durable before writes. Register metadata backups, not just revlog lengths. Distinguish recover from history rollback.
- [ ] Rerun with injected write failures, process termination, stale locks handled explicitly by the harness, and truncated journal records; PASS requires no unrelated path changes and no false successful recovery.
- [ ] Commit `mercurial: add standard journals and recovery`.

### Task 6: Route revlog and metadata writes through transactions

**Files:** Modify `mercurial/src/revlog/mod.rs`, `mercurial/src/revlogrepo.rs`; create `mercurial/src/metadata.rs`; test `mercurial/tests/store_writes.rs`.
**Dependencies:** 5.
**Interfaces:** Produce `StoreTransaction::append_revision(&mut self, log: LogId, revision: RevlogAppend, delta: Option<(NodeHash, mercurial_types::delta::Delta)>, compression: RevlogCompression) -> Result<NodeHash>`, `LogId::{Changelog,Manifest,Directory(HgPath),File(HgPath)}`. Produce `Metadata { branch: Vec<u8>, bookmarks: BTreeMap<Vec<u8>,NodeHash>, active_bookmark: Option<Vec<u8>>, phases: BTreeMap<NodeHash,Phase> }`, `Phase::{Public,Draft,Secret}`, `read_metadata(repo: &Repository) -> Result<Metadata>`, `StoreTransaction::write_metadata(&mut self, value: &Metadata) -> Result<()>`. The phases map stores boundary roots, not an expanded entry for every revision; serialize the stock phase-root format.

Also produce `RevisionMetadata { node: NodeHash, parents: Parents, linkrev: Option<u32>, flags: u16 }` and `StoreTransaction::append_stream_revision(&mut self, log: LogId, metadata: RevisionMetadata, text: &mut dyn Read, compression: RevlogCompression) -> Result<NodeHash>`. This validates/hashes and compresses a snapshot incrementally through owned temporary storage. Large revisions use this path; do not force them back into existing `RevlogAppend.text: Vec<u8>`. Small revisions retain the existing convenience API. Unsupported revision flags are refused by the access profile.

- [ ] Add `new_filelog_and_fncache_abort_together`, `inline_split_recovers`, `tree_manifest_transaction`, `phase_and_bookmark_backup_replayed`, `node_hash_is_validated`. Assert `assert_eq!(f.hg(&["verify"]).code, 0);` after successful appends, and `assert_eq!(before, after_recovery);` after each publication failure.
- [ ] Run `cargo test -p mercurial --test store_writes -- --test-threads=1`; expect missing transactional entry points.
- [ ] Adapt existing append/delta code, including inline-to-split changes and fncache. Validate hashes/parents/linkrevs and maintain approved raw/zlib/zstd reads. Keep legacy server codecs available. During migration retain a clearly internal legacy adapter; remove externally usable unguarded mutation entry points in Task 12 when server callers migrate.
- [ ] Rerun focused tests and `cargo test -p mercurial --lib`; require PASS.
- [ ] Commit `mercurial: transact revlog and repository metadata writes`.

### Task 7: Maintain persistent nodemaps safely

**Files:** Modify `mercurial/src/revlog/{mod,nodemap}.rs`; test `mercurial/tests/nodemap.rs`.
**Dependencies:** 6.
**Interfaces:** Produce `StoreTransaction::refresh_nodemap(&mut self, log: LogId) -> Result<()>`; existing node lookup remains the read interface. Reuse the reference docket/trie layout; no separate authoritative index.

- [ ] Add `stock_reads_rust_nodemap`, `stale_tip_falls_back_correctly`, `docket_publication_recovers`, `nodemap_mismatch_never_returns_wrong_node`. For every existing and absent node assert `assert_eq!(cached_lookup, uncached_lookup);` before/after append and external replacement.
- [ ] Run `cargo test -p mercurial --test nodemap`; expect writer/publication gaps.
- [ ] Implement append or bounded rebuild and publish data before docket inside the transaction. Validate tip/node/length against the revlog; discard stale derived state and rebuild/fall back. Keep current reader behavior where correct.
- [ ] Rerun with killed publication and stock `hg verify`, require PASS; record rebuild cost without claiming a performance result yet.
- [ ] Commit `mercurial: maintain interoperable persistent nodemaps`.

### Task 8: Define transport-neutral exchange records and bounded ingestion

**Files:** Create `mercurial/src/exchange/{mod,types,ingest,stream_receive}.rs`; modify `mercurial/Cargo.toml`, `mercurial-bundles/src/{bundle2,part_inner,chunk}.rs` for bounded decoding and stream2/metadata parts; test `mercurial/tests/ingest.rs`.
**Dependencies:** 6, 7.
**Interfaces:** Produce `ByteStream = Pin<Box<dyn Stream<Item=Result<bytes::Bytes>> + Send>>`, `BundleOffer { changegroups: BTreeSet<u8>, stream_versions: BTreeSet<u8>, store_requirements: BTreeSet<Vec<u8>>, compression: BTreeSet<Vec<u8>> }`, `ApplyReport { added: u64, heads: Vec<NodeHash> }`. Produce `async fn ingest(repo: &Repository, input: ByteStream, ctx: &OperationContext) -> Result<ApplyReport>`. Ingest owns a store transaction and its guard. Produce `async fn receive_stream2(empty_repo: &Repository, requirements: &BTreeSet<Vec<u8>>, payload: ByteStream, ctx: &OperationContext) -> Result<ApplyReport>` for an owned empty clone staging repository only, and `verify_store(repo: &Repository, ctx: &OperationContext) -> Result<()>` for streamed-store integrity before publication. Streams validate mandatory parts and stage changes before publication; typed internal records may spool oversized revisions to files.

- [ ] Add `cg02_cg03_apply_without_full_bundle_buffer`, `mandatory_part_after_revisions_aborts_all`, `oversized_revision_spills`, `malformed_delta_rolls_back`, `stream2_counts_and_names_validated`, `stream2_refuses_nonempty_store`, `corrupt_streamed_revision_refuses_publication`. Instrument queued bytes and assert `assert!(peak_queued <= 268_435_456); assert_eq!(f.hg(&["verify"]).code, 0);`; failing bundles must restore the pre-ingest snapshot. A stream2 with duplicate/escaping names, wrong file/byte totals, bad revision hashes or truncated payload must never publish a valid clone.
- [ ] Run `cargo test -p mercurial --test ingest`; expect whole-buffer/new API failures.
- [ ] Implement streaming decoding with byte-accounted backpressure, integer/length/delta validation, mandatory part/parameter checks and transactional append. Add stream2 decoding (requirements, counts, logical store names and data boundaries), listkeys/phases/bookmarks and supported check/reply/error parts from the stock bundle contract. Store paths are encoded once, not treated as arbitrary filesystem paths. The staged-store verifier checks revlog structure, hashes and dependencies through shared readers with bounded reconstruction; account for this cost in clone benchmarks rather than skipping design-mandated validation. Separate compressed framing limits from reconstructed text limits. No conversion back to `Vec<all revisions>`; spool large delta application/reconstruction and use Task 6's streaming append for giant revisions.
- [ ] Rerun with cancellation at each section and malformed compression; require PASS and cleanup confined to owned staging files.
- [ ] Commit `mercurial: ingest exchange streams transactionally`.

### Task 9: Extract history selection, discovery and streamed export

**Files:** Create `mercurial/src/exchange/{history,stream,discovery}.rs`; adapt `server/src/{bundle,stream}.rs` to delegate without yet changing push; test `mercurial/tests/export.rs`.
**Dependencies:** 8.
**Interfaces:** Produce `Selection { heads: Vec<NodeHash>, common: Vec<NodeHash> }`, `async fn export(repo: &Repository, selection: Selection, offer: &BundleOffer, ctx: &OperationContext) -> Result<ByteStream>`, `async fn export_store(repo: &Repository, ctx: &OperationContext) -> Result<ByteStream>`. Produce `Discovery::new(local_heads: Vec<NodeHash>, remote_heads: Vec<NodeHash>)`, `next_query(&mut self, repo: &Repository) -> Result<Vec<NodeHash>>`, `accept_known(&mut self, nodes: &[NodeHash], known: &[bool]) -> Result<()>`, `finish(self) -> Result<Selection>`. Use common DAG code, not an O(history) remote request per revision.

- [ ] Add `selected_heads_include_only_missing_ancestors`, `merge_discovery_is_complete`, `stream_names_use_store_encoding_once`, `stock_applies_export`. Assert selected node sets equal the stock oracle, and `assert_eq!(source_tip, destination_tip);` after stock application. Test excluded/secret revisions are never exported.
- [ ] Run `cargo test -p mercurial --test export`; expect missing shared exports.
- [ ] Move applicable selection/parallel file reading/store enumeration from the server. Return engine `ByteStream`, not `hgproto::BundleStream`; server adapters supply legacy response framing and errors. Preserve deterministic dependency order and supported legacy server selection semantics.
- [ ] Rerun focused tests and `cargo test -p mononoke --test serve -- --test-threads=1`; require PASS.
- [ ] Commit `mercurial: share history discovery and streamed export`.

### Task 10: Implement client wire negotiation and framing

**Files:** Create `hgproto/src/client/{mod,capabilities,request,response}.rs`; test `hgproto/tests/client.rs`.
**Dependencies:** 8, 9.
**Interfaces:** Produce `Capabilities::parse(bytes: &[u8]) -> hgproto::Result<Capabilities>`, `Capabilities::offer(&self) -> BundleOffer`, `encode_request(request: &ClientRequest) -> hgproto::Result<Bytes>`, `ResponseDecoder::new(command: CommandKind)`, `ResponseDecoder::decode(&mut self, input: &mut BytesMut) -> hgproto::Result<Option<Reply>>`. Define `CommandKind` for capabilities/hello/batch/heads/known/lookup/branchmap/listkeys/getbundle/unbundle/pushkey/clonebundles/clonebundles_manifest. `ClientRequest` mirrors corresponding existing `Request` parameters but uses byte-valued lookup/name/key fields, and its `Unbundle { heads: Vec<NodeHash> }` has no buffered body. `Reply::{Scalar(Bytes),StreamChunk(Bytes),StreamEnd}`. Reuse existing types where semantically compatible; the server's current `Request::Unbundle { bundle: Bytes, ... }` must not force buffering on the new client. No engine dependency on `hgproto`.

- [ ] Add `percent_encoded_bundlecaps_roundtrip`, `partial_response_frames`, `unknown_optional_capability_ignored`, `no_common_modern_format_errors`, `changing_heads_restarts_discovery`. Assert `assert_eq!(offer.changegroups, BTreeSet::from([2,3]));` for the supported fixture; never select 01/04 from a mixed offer.
- [ ] Run `cargo test -p hgproto --test client`; expect missing client codec behavior.
- [ ] Implement request encoding and response decoding for HTTP-v1 bodies and SSH length/chunk framing; batch known queries; process compression negotiation independently of bundle compression. Reject truncated/malformed responses and preserve remote diagnostics.
- [ ] Rerun focused tests and existing `cargo test -p hgproto`; require PASS.
- [ ] Commit `hgproto: add client negotiation and response codecs`.

### Task 11: Add configuration and HTTP(S)/SSH/local peers

**Files:** Create `hg-client/{Cargo.toml,src/lib.rs}`, `hg-client/src/{config,transfer}.rs`, `hg-client/src/peer/{mod,http,ssh,local}.rs`; modify root `Cargo.toml`; test `hg-client/tests/peers.rs`.
**Dependencies:** 3, 10.
**Interfaces:** Produce client `Error` and `Result<T>` aliases wrapping engine/protocol/I/O failures; `ClientConfig::load(repo: Option<&std::path::Path>, overrides: &[(String,String)]) -> Result<ClientConfig>`, `ClientConfig::check_operation(op: Operation) -> Result<()>`. Peer methods return boxed Send futures borrowing the peer/context: `capabilities() -> Result<Capabilities>`, `call(request: ClientRequest) -> Result<Bytes>`, `receive(request: ClientRequest) -> Result<ByteStream>`, `send(request: ClientRequest, body: ByteStream) -> Result<ByteStream>`; each accepts `&mut self` and `&OperationContext`. Scalar calls default to a 16 MiB response limit with an explicit configurable override; bundle upload replies remain streamed to handle error/output parts without unbounded buffering. Produce `PeerLocation::{Local(PathBuf),Remote(String)}`, `PeerLocation::parse(value: &OsStr) -> Result<Self>` and `connect(location: &PeerLocation, config: &ClientConfig) -> Result<Box<dyn Peer>>`; preserve non-UTF-8 local paths and percent-encoded remote paths. A returned stream owns its body/session handle so it can satisfy ByteStream's lifetime. Client errors never expose credentials.

- [ ] Add `config_precedence_and_includes`, `configured_hook_refuses_affected_write`, `redirect_strips_cross_origin_auth`, `tls_validation_is_enabled`, `ssh_arguments_do_not_invoke_local_shell`. Assert `assert!(redirected_authorization.is_none()); assert!(bad_cert_result.is_err());`; test defaults, proxy bypass rules and non-ASCII/space-containing repository locations.
- [ ] Run `cargo test -p hg-client --test peers -- --test-threads=1`; expect missing adapters.
- [ ] Implement relevant Mercurial system/user/repo precedence, HGRCPATH, includes and cycle diagnostics; typed identity/path/TLS/proxy/merge settings. Block configured hooks and extensions affecting an operation; unsupported benign display settings may be ignored. Use reqwest/rustls with at most 10 redirects, basic auth and proxy support; use process argument arrays locally and correct remote-shell quoting for SSH, system agent and configurable remote command. Local peers use the same exchange API, not writable hardlinks.
- [ ] Rerun against ephemeral stock HTTP/HTTPS and SSH stdio fixtures plus invalid credentials/certificates; PASS requires bounded cancellation and separate stderr handling. Real SSH authentication gets a platform integration job in Task 27.
- [ ] Commit `client: add configured HTTP and SSH peers`.

### Task 12: Migrate server mutations to the shared engine

**Files:** Modify `server/src/{repo,push,bundle,stream,http,main}.rs`, `hgproto/src/{lib,service}.rs` only as necessary for stream ownership; modify `mercurial/src/{revlogrepo,metadata}.rs`, `mercurial/src/revlog/mod.rs`; test existing `server/tests/serve.rs` plus `server/tests/transactions.rs`.
**Dependencies:** 6-11.
**Interfaces:** Server `RepoClient` adapts incoming transport bodies to `ByteStream`, calls `ingest`, `export`, `export_store` and transaction-backed metadata APIs, then encodes existing replies. Add `compare_and_set_bookmark(repo: &Repository, name: &[u8], old: Option<NodeHash>, new: Option<NodeHash>, ctx: &OperationContext) -> Result<bool>` in metadata; use it for both server pushkey and client updates. Legacy server input uses a codec adapter into the same transaction, not a second append implementation.

- [ ] Add `server_push_kill_is_hg_recoverable`, `bookmark_cas_rechecks_under_lock`, `server_receive_memory_is_bounded`; preserve legacy 01/02/03 and tree-manifest tests. Assert `assert_eq!(recovered, before);` and unchanged legacy response fixtures.
- [ ] Run `cargo test -p mononoke --test transactions -- --test-threads=1`; expect old mutation path failures.
- [ ] Migrate callers, remove whole-bundle push buffering where it resides in transport as well as apply logic, and restrict raw append/fncache/requirement mutation APIs to engine internals. Remove duplicate outer server lock acquisition for delegated engine mutations: there must be one transaction/lock owner, not a nested attempt to acquire the same Mercurial lock. Keep server serialization policy, existing legacy capabilities and source-specific server policy; no unrelated lookup/LFS refactor.
- [ ] Run `cargo test -p mononoke --lib`, `cargo test -p mononoke --test serve -- --test-threads=1`, and the transaction tests; require PASS.
- [ ] Commit `server: use shared transactional exchange engine`.

### Task 13: Read and publish dirstate v1/v2 and tracked hints

**Files:** Create `mercurial/src/workcopy/{dirstate,v1,v2,tracked_hint}.rs`; test `mercurial/tests/dirstate.rs`.
**Dependencies:** 5, 6.
**Interfaces:** Produce `Dirstate::read(repo: &Repository) -> Result<Dirstate>`, `Dirstate::parents(&self) -> Parents`, `Dirstate::entries(&self) -> impl Iterator<Item=(&HgPath,&DirstateEntry)>`, `Dirstate::set_parents(&mut self, parents: Parents)`, `Dirstate::set_entry(&mut self, path: HgPath, value: DirstateEntry)`, `Dirstate::publish(&self, guard: &RepoGuard<'_>, transaction: Option<&mut StoreTransaction>) -> Result<()>`. `DirstateEntry` carries tracked/removed/merged flags, optional size/mtime/mode and copy source; uncertain stat fields remain unknown, not zero-as-clean. Preserve input format and expose `Dirstate::empty(format: DirstateFormat)`, `DirstateFormat::{V1,V2}`.

- [ ] Add `stock_reads_both_rust_dirstates`, `rust_reads_stock_copies_and_parents`, `docket_replacement_invalidates_view`, `truncated_v2_data_rejected`, `same_tick_stat_is_unknown`. Assert `assert_eq!(rust_parents, stock_parents); assert_eq!(rust_copy_map, stock_copy_map);` after alternating writes.
- [ ] Run `cargo test -p mercurial --test dirstate`; expect missing codecs.
- [ ] Implement v1 and v2 using Task 1 sources: v2 marker, padded parents, metadata, used-data length and UUID file. Write new data before docket, register backups when in a transaction, atomically publish under wlock otherwise; update/invalidate tracked-key hints correctly. Never clean up old data while a valid docket still references it.
- [ ] Rerun under stock status/debugdirstate and recovery failpoints; require PASS for missing/empty/invalid dockets and external writes.
- [ ] Commit `mercurial: support interoperable dirstate formats`.

### Task 14: Centralize revision resolution, metadata and DAG queries

**Files:** Create `mercurial/src/history.rs`; extend `mercurial/src/metadata.rs`; test `mercurial/tests/history.rs`.
**Dependencies:** 7, 13.
**Interfaces:** Produce `resolve(repo: &Repository, expression: &[u8]) -> Result<NodeHash>`, `parents(repo: &Repository, node: NodeHash) -> Result<Parents>`, `ancestors(repo: &Repository, heads: &[NodeHash]) -> Result<Vec<NodeHash>>`, `best_common_ancestors(repo: &Repository, a: NodeHash, b: NodeHash) -> Result<Vec<NodeHash>>`. Read-only full ancestry returns index/node data, never reconstructed file contents; request-local algorithms may use bounded/frontier traversal instead of materializing it.

- [ ] Add `symbols_match_stock`, `ambiguous_prefix_refused`, `tags_resolve_across_heads`, `crisscross_has_multiple_best_ancestors`, `secret_revisions_are_local_only`. Assert `assert_eq!(resolved, oracle_node); assert_eq!(rust_bases, stock_bases);` for null/tip/negative revision/branch/tag/bookmark/dot and merges.
- [ ] Run `cargo test -p mercurial --test history`; expect resolver gaps.
- [ ] Implement byte-oriented lookup and branch/tag semantics from pinned sources, including `.hgtags` merges and `.hg/localtags`; phase visibility feeds discovery/export. Reuse changelog indexes/nodemaps; avoid loading every changeset body for hash lookup.
- [ ] Rerun focused tests and export tests; require PASS.
- [ ] Commit `mercurial: share revision and ancestry queries`.

### Task 15: Implement matchers, ignore rules and accurate status

**Files:** Create `mercurial/src/workcopy/{matcher,status}.rs`; test `mercurial/tests/status.rs`.
**Dependencies:** 3, 13, 14.
**Interfaces:** Produce `Matcher::compile(root: &std::path::Path, patterns: &[Vec<u8>], includes: &[Vec<u8>], excludes: &[Vec<u8>]) -> Result<Matcher>`, `StatusOptions { matcher: Matcher, clean: bool, ignored: bool, unknown: bool, copies: bool }`, `StatusEntry { path: HgPath, kind: StatusKind, copy_source: Option<HgPath> }`, `StatusKind::{Modified,Added,Removed,Missing,Unknown,Ignored,Clean}` and `status(repo: &Repository, options: &StatusOptions, ctx: &OperationContext) -> Result<Vec<StatusEntry>>`.

- [ ] Add `all_status_classes_match_stock`, `nested_subinclude_relative_paths`, `same_size_same_tick_edit_detected`, `ignore_change_invalidates_results`, `byte_paths_preserved`. Assert `assert_eq!(rust_status, normalized_stock_status);` with one file per class and a same-length rewrite in the current ambiguous dirstate timestamp tick. Do not demand detection of arbitrary deliberate restoration of an old unambiguous mtime beyond stock Mercurial's stat guarantees.
- [ ] Run `cargo test -p mercurial --test status`; expect missing matching/stat logic.
- [ ] Implement stock literal/glob/regexp matching, include/subinclude ignore semantics and directory pruning. Enumerate efficiently; trust only unambiguous dirstate stat data and rehash ambiguous files. Include merge/copy state in results. Bound workers and cache memory; no watcher dependency.
- [ ] Rerun fixture matrix on both systems; require PASS and assert the clean unambiguous fixture performs no tracked-file content reads through test I/O counters.
- [ ] Commit `mercurial: implement fast working-copy status`.

### Task 16: Add the executable CLI and read commands

**Files:** Create `hg-client/src/{main,cli,output}.rs`, `hg-client/src/commands/{mod,read}.rs`; modify `hg-client/Cargo.toml`; test `hg-client/tests/read.rs`.
**Dependencies:** 11, 15.
**Interfaces:** Produce `pub fn run(args: impl IntoIterator<Item=OsString>) -> i32`, binary entry calls it. `hgr -R ROOT status` accepts path filters and `--all/--clean/--ignored/--unknown/--copies`, `--include/--exclude`, and `--json`. Global `--config SECTION.KEY=VALUE` passes to ClientConfig. Exit 0 success, 1 differences/unresolved/no-op where stock uses 1, 255 user-facing fatal failure. JSON v1 includes a schema version and byte-preserving path fields (UTF-8 display plus base64 raw bytes when necessary).

- [ ] Add `status_cli_matches_engine`, `non_utf8_json_roundtrip`, `repository_discovery_and_config`, `no_python_runtime_needed`. Assert `assert_eq!(result.code, 0); assert_eq!(json_paths, expected_bytes);` with Python/hg absent from the client child PATH, while the harness still knows the oracle executable.
- [ ] Run `cargo test -p hg-client --test read`; expect no binary/command yet.
- [ ] Wire clap and typed adapters, repository discovery, Ctrl-C cancellation and stderr progress. Implement only status in this task; later tasks add their command adapters. Keep machine output separate from diagnostics and secrets.
- [ ] Rerun focused tests and `cargo build -p hg-client --release`; require PASS.
- [ ] Commit `client: add hgr status and structured output`.

### Task 17: Implement bounded log pages

**Files:** Extend `mercurial/src/history.rs`, `hg-client/src/commands/read.rs`, `hg-client/src/cli.rs`; test `mercurial/tests/log.rs`, extend `hg-client/tests/read.rs`.
**Dependencies:** 14, 16.
**Interfaces:** Produce `LogQuery { start: Option<NodeHash>, limit: usize, before_revision: Option<u32>, paths: Vec<HgPath> }`, `LogEntry { node: NodeHash, parents: Parents, author: Vec<u8>, date: (i64,i32), message: Vec<u8>, branch: Vec<u8> }`, `LogPage { entries: Vec<LogEntry>, next_before_revision: Option<u32> }`, `log(repo: &Repository, query: &LogQuery) -> Result<LogPage>`. CLI `log -r REV -l LIMIT [PATH...] --before REVNUM --json`; default limit 20 is a documented client choice, explicit `--limit 0` requests all via incremental pages.

- [ ] Add `pages_have_no_duplicates`, `path_log_includes_merge_changes`, `small_log_reads_only_selected_bodies`. Assert `assert_eq!(paged_nodes, oracle_nodes); assert!(body_reads <= page_len);` for a query without path filtering; separately bound path-filter work without pretending every path query is O(page size).
- [ ] Run `cargo test -p mercurial --test log`; expect missing page API.
- [ ] Implement lazy changeset decoding and stable revision cursors; path history uses filelogs/manifests and copy metadata. CLI emits pages without materializing an unlimited log in memory.
- [ ] Rerun log and client read tests; require PASS.
- [ ] Commit `client: add paginated repository log`.

### Task 18: Implement text/binary diff and copy/flag reporting

**Files:** Create `mercurial/src/diff.rs`; extend client read commands/CLI; test `mercurial/tests/diff.rs`, client read tests.
**Dependencies:** 15-17.
**Interfaces:** Produce `DiffSource::{WorkingCopy,Revision(NodeHash)}`, `DiffOptions { old: DiffSource, new: DiffSource, matcher: Matcher, context: usize }`, `diff(repo: &Repository, options: &DiffOptions, output: &mut dyn Write, ctx: &OperationContext) -> Result<()>`. CLI `diff [-r OLD [-r NEW]] [PATH...] --git --context N`; default old is p1, new is working copy, context 3. Machine-readable summaries use the same JSON v1 envelope.

- [ ] Add `text_patch_applies_to_old_tree`, `binary_and_flag_changes_reported`, `copy_metadata_is_preserved`, `missing_vs_removed_diff`. Assert `assert_eq!(patched_tree, expected_new_tree);` using stock patch/import in a scratch fixture for text patches; binary cases compare reported paths/metadata without promising embedded binary patch support.
- [ ] Run `cargo test -p mercurial --test diff`; expect missing diff API.
- [ ] Implement bounded per-file comparison and unified/git-style text headers, no-newline markers and binary diagnostics. Never decode arbitrary file bytes as UTF-8. Document exactly which patch forms are emitted; preserve source copy metadata in headers.
- [ ] Rerun engine/client tests; require PASS for revision pairs and working changes.
- [ ] Commit `client: add interoperable diff output`.

### Task 19: Implement clean checkout and interrupted-update semantics

**Files:** Create `mercurial/src/workcopy/update.rs`; create `hg-client/src/commands/write.rs`, extend CLI; test `mercurial/tests/update.rs`, `hg-client/tests/update.rs`.
**Dependencies:** 3-7, 13-16.
**Interfaces:** Produce `UpdateMode::{Normal,Clean}`, `UpdateReport { updated: u64, removed: u64, unresolved: u64 }`, `update(repo: &Repository, target: NodeHash, mode: UpdateMode, ctx: &OperationContext) -> Result<UpdateReport>`. Normal mode initially refuses modifications requiring a merge; Task 26 completes that behavior. CLI `update [-r REV] [--clean]`.

- [ ] Add `clean_checkout_flat_and_tree`, `preflight_unknown_file_collision`, `stock_detects_interrupted_rust_update`, `rust_detects_stock_updatestate`, `clean_update_clears_completed_state`. Assert the interrupted marker contains target hex bytes, `assert_ne!(f.hg(&["commit", "-m", "unsafe"]).code, 0);`, and after explicit recovery-by-clean-update `assert_eq!(f.hg(&["status"]).stdout, b"");`.
- [ ] Run `cargo test -p mercurial --test update -- --test-threads=1`; expect missing update/state behavior.
- [ ] Match pinned `merge.py` ordering: preflight, hold wlock, write standard `.hg/updatestate` before apply, safely materialize files, record dirstate/parents/branch, publish state, and clear the marker only once consistent. Task 1 reference fixture pins what stock commands refuse and how explicit clean update recovers. Rust reports an interrupted update and requires explicit user action; `recover` is not advertised as restoring working-file edits. Do not invent private recovery state.
- [ ] Rerun after killing before/after file and dirstate publication, Windows case-only moves, symlinks and executable bits. Explicit clean update may discard edits as documented; normal mode must preserve them.
- [ ] Commit `client: add safe checkout and update state handling`.

### Task 20: Implement clone, clonebundle acceleration and pull

**Files:** Create `hg-client/src/commands/exchange.rs`, `hg-client/src/peer/clonebundles.rs`; extend CLI/transfer and `mercurial/src/repository.rs`; test `hg-client/tests/clone_pull.rs`.
**Dependencies:** 8-11, 13, 14, 19.
**Interfaces:** Produce `async fn clone_repo(source: &PeerLocation, destination: &std::path::Path, revision: Option<Vec<u8>>, no_update: bool, config: &ClientConfig, ctx: &OperationContext) -> Result<ApplyReport>`, `async fn pull(repo: &Repository, peer: &mut dyn Peer, heads: &[NodeHash], update_after: bool, ctx: &OperationContext) -> Result<ApplyReport>`. CLI `clone [-U] [-r REV] SOURCE DEST`, `pull [-r REV] [-u] [SOURCE]`; normal clone invokes Task 19 checkout. A valid store plus failed checkout is a nonzero error with the destination reported.

Produce engine `Repository::init(root: &std::path::Path, options: InitOptions) -> Result<Repository>`, `InitOptions { tree_manifests: bool, persistent_nodemap: bool, dirstate: DirstateFormat }`. It creates an owned empty repository with supported store/fncache/dotencode/generaldelta/sparserevlog/zstd/share-safe requirements, preserving negotiated manifest shape and optional nodemap policy. The application selects options, but never implements `.hg` store initialization itself. Task 8's stream2 receiver imports into this staging repository; publication does not preserve writable mappings to paths that are being renamed.

- [ ] Add `stream_and_changegroup_clones_match_stock`, `unsupported_stream_falls_back_to_cg`, `clonebundle_then_incremental_pull`, `pull_preserves_working_copy`, `failed_clone_does_not_publish_invalid_store`, `local_clone_has_no_writable_hardlinks`. Assert `assert_eq!(cloned_tip, selected_tip); assert_eq!(working_before, working_after_pull);` and stock verify success.
- [ ] Run `cargo test -p hg-client --test clone_pull -- --test-threads=1`; expect missing commands.
- [ ] Stage on the destination volume, validate requirements/paths and refuse a nonempty destination. Negotiate modern offers and import optional supported HTTPS clonebundles; skip incompatible optional bundles, fall back to peer exchange, then discover/pull selected heads. Initialize share-safe/zstd/dirstate v2 and write default path. Adopt pre-existing repository formats without conversion. Clean up only owned paths and keep unknown user files intact.
- [ ] Rerun stock HTTP(S), SSH stdio, local and Mononoke fixture variants, selected heads, compression combinations, cancellation and checkout errors; require PASS.
- [ ] Commit `client: add streaming clone and pull`.

### Task 21: Implement push, phase exchange and race handling

**Files:** Extend client exchange/transfer, `mercurial/src/exchange/discovery.rs`, `mercurial/src/metadata.rs`; test `hg-client/tests/push.rs`.
**Dependencies:** 9-12, 14, 20.
**Interfaces:** Produce `async fn push(repo: &Repository, peer: &mut dyn Peer, heads: &[NodeHash], allow_new_heads: bool, ctx: &OperationContext) -> Result<PushReport>`, `PushReport { sent: u64, remote_heads: Vec<NodeHash> }`; errors distinguish refused heads from indeterminate acknowledgement. CLI `push [-r REV] [--force] [DEST]`; force explicitly bypasses the normal new-head policy but never bypasses mandatory format validation.

- [ ] Add `push_to_stock_and_mononoke`, `publishing_and_nonpublishing_phases`, `secret_history_not_sent`, `bookmark_compare_and_swap`, `remote_race_does_not_replay`, `response_loss_is_indeterminate`. Assert `assert_eq!(send_attempts, 1);` when a server applied the bundle but lost the response; inspection may determine outcome, but no blind retry is permitted.
- [ ] Run `cargo test -p hg-client --test push -- --test-threads=1`; expect missing push semantics.
- [ ] Discover common/outgoing history, transmit modern bundle parts and check:heads, interpret replies, and synchronize phases/bookmarks under local locks. Re-read remote state after races; show an actionable indeterminate result when the outcome cannot be proved. Standard read-only retry does not authorize automatic replay of writes.
- [ ] Rerun small push and cancellation fixtures with peak queued-byte checks; require PASS.
- [ ] Commit `client: add push and phase synchronization`.

### Task 22: Implement add/remove state transitions

**Files:** Create `mercurial/src/workcopy/tracking.rs`; extend client write/CLI; test `mercurial/tests/tracking.rs`, `hg-client/tests/write.rs`.
**Dependencies:** 13, 15, 16.
**Interfaces:** Produce `TrackAction::{Add,Remove}`, `TrackOptions { action: TrackAction, matcher: Matcher, force: bool, after: bool }`, `track(repo: &Repository, options: &TrackOptions, ctx: &OperationContext) -> Result<Vec<HgPath>>`. CLI `add [PATH...]`, `remove [--after] [--force] [PATH...]` with include/exclude matchers.

- [ ] Add `stock_sees_rust_add_remove`, `modified_remove_refused`, `missing_remove_after`, `ignored_directory_add_matches_stock`. Assert `assert_eq!(rust_scheduled, stock_scheduled); assert_eq!(modified_bytes_before, modified_bytes_after);` for refused ordinary remove.
- [ ] Run `cargo test -p mercurial --test tracking`; expect missing transitions.
- [ ] Apply tracked-state changes under wlock, preserve copy records and existing dirstate format, and use Task 3 operations for explicit deletion. Ignore handling must distinguish explicit paths and recursive discovery as the stock oracle does.
- [ ] Rerun engine/client write tests on v1/v2; require PASS.
- [ ] Commit `client: add file tracking and removal`.

### Task 23: Implement deterministic normal and partial commits

**Files:** Create `mercurial/src/commit.rs`; extend client write/CLI; test `mercurial/tests/commit.rs`, client write tests.
**Dependencies:** 6, 7, 13-15, 22.
**Interfaces:** Produce `CommitOptions { message: Vec<u8>, author: Vec<u8>, date: (i64,i32), matcher: Matcher, close_branch: bool }`, `commit(repo: &Repository, options: &CommitOptions, ctx: &OperationContext) -> Result<NodeHash>`. CLI `commit [-m TEXT|-l FILE] [-u USER] [-d DATE] [PATH...]`; when no message option is supplied, invoke configured editor safely and reject an empty/aborted message. Branch/bookmark state follows the existing working copy; no separate branch/tag command is added.

- [ ] Add `normal_commit_node_matches_stock`, `partial_commit_preserves_other_changes`, `copy_rename_metadata_matches_stock`, `new_commit_is_draft`, `close_branch_and_bookmark_move`, `commit_failure_is_recoverable`. With equal metadata and parent fixtures assert `assert_eq!(rust_node, stock_node); assert_eq!(f.hg(&["verify"]).code, 0);`.
- [ ] Run `cargo test -p mercurial --test commit -- --test-threads=1`; expect missing commit logic.
- [ ] Construct file revisions, flat/tree manifests and changeset bytes with exact parent/hash conventions, copy metadata and author/message bytes. Use bounded delta-base search with documented budgets and existing chain limits. Write through StoreTransaction, publish phases/bookmark/dirstate coherently; reject unresolved/unsupported states. Empty and partial-merge commits must follow stock refusal rules, not create invented revisions.
- [ ] Rerun fixture matrix and kill at each commit publication boundary; opposite-client recovery must pass verify and preserve uncommitted file bytes.
- [ ] Commit `client: create interoperable normal and partial commits`.

### Task 24: Implement merge ancestry, file plans and standard state

**Files:** Create `mercurial/src/merge/{mod,ancestor,plan,state}.rs`; test `mercurial/tests/merge_plan.rs`.
**Dependencies:** 13-15, 19, 23.
**Interfaces:** Produce `MergePlan::build(repo: &Repository, other: NodeHash) -> Result<MergePlan>`, `MergeState::read(repo: &Repository) -> Result<MergeState>`, `MergeState::publish(&self, guard: &RepoGuard<'_>) -> Result<()>`. Plan records local/other/base nodes and typed actions for each path (keep/get/remove/merge/conflict); state preserves standard optional records and rejects unknown mandatory records. Multiple merge bases use Mercurial's pinned ancestor policy; no arbitrary first-base selection.

- [ ] Add `crisscross_plan_matches_stock`, `rename_modify_and_directory_conflicts`, `standard_merge_state_roundtrip`, `unknown_mandatory_state_blocks_writes`. Assert `assert_eq!(rust_bases, stock_bases); assert_eq!(roundtrip_records, input_records);` and exact unresolved path/reason sets for ambiguous copies.
- [ ] Run `cargo test -p mercurial --test merge_plan`; expect missing planners/state codecs.
- [ ] Implement copy tracing from stored metadata and DAG-aware ancestor selection, standard `.hg/merge` records/backups and local-content preservation. Preflight dirty/untracked conflicts and refuse unsupported states before writes. This task plans actions; it does not yet mark file conflicts resolved.
- [ ] Rerun targeted flat/tree/rename/flag cases against stock merge-state inspection; require PASS.
- [ ] Commit `mercurial: plan merges and preserve standard merge state`.

### Task 25: Apply merges and implement resolution tools

**Files:** Create `mercurial/src/merge/{text,tools,resolve}.rs`, `hg-client/src/commands/merge.rs`; extend merge module/CLI; test `mercurial/tests/merge.rs`, `hg-client/tests/merge.rs`.
**Dependencies:** 24.
**Interfaces:** Produce `merge(repo: &Repository, other: NodeHash, tool: &MergeTool, ctx: &OperationContext) -> Result<UpdateReport>`, `MergeTool::{Internal,External { program: PathBuf, args: Vec<OsString> }}`, `ResolveAction::{List,Rerun,Mark,Unmark}`, `ResolveEntry { path: HgPath, resolved: bool }`, `resolve(repo: &Repository, action: ResolveAction, matcher: &Matcher, tool: &MergeTool, ctx: &OperationContext) -> Result<Vec<ResolveEntry>>`. CLI `merge [-r REV] [--tool TOOL]`; `resolve [--list|--mark|--unmark] [--tool TOOL] [PATH...]`, default reruns unresolved matches.

- [ ] Add `nonoverlap_text_merge_is_clean`, `overlap_binary_delete_conflicts_remain_unresolved`, `external_tool_cancel_preserves_backup`, `stock_resolves_rust_merge`, `rust_resolves_stock_merge`. Assert `assert_eq!(unresolved_paths, expected); assert_eq!(parents.p2, other);` and compare standard state after marking/unmarking.
- [ ] Run `cargo test -p mercurial --test merge -- --test-threads=1`; expect missing application/tools.
- [ ] Apply planned actions using safe replacements and standard state. Internal three-way merging preserves byte content/newlines; binary/ambiguous cases require resolution. Run configured tools with controlled argument substitution, retain originals/backups on failure and publish dirstate/merge state conservatively. Unresolved merge exits 1, not 0.
- [ ] Rerun both directions of conflict resolution with stock `hg`; require PASS, including interrupted application and Linux byte paths.
- [ ] Commit `client: add merge and conflict resolution`.

### Task 26: Complete dirty updates, merge commits and recover CLI

**Files:** Extend `mercurial/src/{commit,workcopy/update,merge/resolve}.rs`, client write/merge/CLI; test `mercurial/tests/merge_commit.rs`, extend update/recovery/client merge tests.
**Dependencies:** 19, 23-25.
**Interfaces:** Complete existing `update` and `commit` contracts; no duplicate mutation APIs. Add CLI `recover` calling `store::recovery::recover`; exit 0 if recovered, 1 if no journal. Normal dirty update uses merge machinery and preserves local modifications according to stock behavior; explicit clean update is the documented merge-abandon path.

- [ ] Add `dirty_update_preserves_or_conflicts`, `merge_commit_node_matches_stock`, `unresolved_commit_refused`, `partial_merge_commit_refused`, `opposite_client_finishes_merge`, `recover_cli_restores_transaction_not_working_edits`. Assert `assert_eq!(rust_merge_node, stock_merge_node); assert_eq!(committed_parents, expected_parents);` and no journal/merge-state leftovers after success.
- [ ] Run `cargo test -p mercurial --test merge_commit -- --test-threads=1`; expect remaining normal-only commit/update gaps.
- [ ] Integrate two-parent commit construction, stock changed-file-list semantics, branch/bookmark behavior and transactional state cleanup. Make dirty-update conflicts resumable by either client. Preserve explicit destructive clean semantics and differentiate transaction recovery from interrupted checkout handling.
- [ ] Run focused merge_commit/update/recovery tests plus `cargo test -p hg-client -- --test-threads=1`; require PASS.
- [ ] Commit `client: complete merge commits and cross-client recovery`.

### Task 27: Add the Windows/Linux release interoperability matrix

**Files:** Create `.github/workflows/client.yml`, `hg-client/tests/interoperability.rs`, `hg-client/tests/support/ssh_server.rs`; extend `tools/hg-test-support/src/process.rs`, `docs/client-compatibility.md`.
**Dependencies:** 12, 20-26.
**Interfaces:** No new engine API. Matrix covers native Windows/MSVC and Linux x86_64, exact Mercurial 7.0.3, supported format/compression profiles, client/server pairings and platform SSH integration. Existing project Windows floor is Windows 10 1803; document tested OS builds and run an explicit floor check before claiming it beyond available CI images.

- [ ] Add `alternating_client_workflow`, `cross_client_interruptions`, `native_ssh_exchange`, `runtime_without_hg_python`, `unsupported_semantics_do_not_mutate`. Alternate stock/Rust commands in the same working copy, including unfinished merge; assert `assert_eq!(f.hg(&["verify"]).code, 0); assert_eq!(final_snapshot, expected);`.
- [ ] Run `cargo test -p hg-client --test interoperability -- --test-threads=1`; first add a fixture that exposes a missing boundary, not a deliberately incorrect assertion solely to make the suite red.
- [ ] Implement runner provisioning with isolated config, deterministic versions, real sshd/key authentication on an ephemeral loopback fixture and HTTPS certificates. Fix failures in the owning modules; no platform-specific semantic shortcuts. Unsupported runner capability is an explicit missing release gate, not an ignored passing test.
- [ ] Run baseline workspace/server commands plus the full client suite on both platforms; require PASS and retain artifacts/version manifests.
- [ ] Commit `test: cover Windows and Linux client interoperability`.

### Task 28: Fuzz input parsers and exercise failure boundaries

**Files:** Create isolated `fuzz/Cargo.toml`, `fuzz/fuzz_targets/{bundle,delta,dirstate,merge_state,paths}.rs`, `mercurial/tests/failure_matrix.rs`; extend engine test failpoints and helper process support.
**Dependencies:** 26, 27.
**Interfaces:** Fuzz existing public decoders with finite byte/allocation budgets; test-only I/O injection supports short reads/writes, ENOSPC-equivalent, cancellation, renamed files and deterministic child termination. No production flag allows arbitrary fault injection.

- [ ] Add `all_publish_boundaries_recover`, `short_write_never_marks_clean`, `malformed_input_has_bounded_allocation`, `outside_writer_invalidates_caches`. Assert `assert_eq!(recovered_snapshot, pre_operation_snapshot);` for failed store transactions and conservative dirty/interrupted state for working-tree failures.
- [ ] Run `cargo test -p mercurial --test failure_matrix -- --test-threads=1`; expect any untested failure-path gaps to be revealed and recorded.
- [ ] Build fuzz targets and seed from real v1/v2 dirstates, cg02/cg03 bundles, merge state and Windows/Linux path cases. Fix crashes/overflows/leaks at their owning layer; classify expected validation errors separately from panics. Use deterministic tests for known regressions.
- [ ] Run `cargo fuzz run bundle -- -max_total_time=300 -rss_limit_mb=1024` from `fuzz/`, repeat for the other four targets, and rerun failure_matrix. Archive seeds/crashes and require zero unresolved crashes; bounded fuzzing is not proof of complete safety.
- [ ] Commit `test: exercise parser and transaction failure boundaries`.

### Task 29: Establish reproducible benchmarks and tune Windows

**Files:** Create `hg-client/examples/bench.rs`, `hg-client/benches/scenarios.toml`, `hg-client/tests/bench_runner.rs`, `docs/client-benchmarks.md`; optimize only measured owning modules.
**Dependencies:** 27, 28.
**Interfaces:** Benchmark executable accepts `--source PATH --scratch PATH --scenario NAME --reference-hg PATH --client PATH --output FILE`, with scenario names `clone-local`, `checkout`, `status-clean`, `status-dirty`, `status-unknown`, `diff`, `log`, `update-near`, `update-far`, `commit`, `pull`, `push`, `merge-clean`, `merge-conflict`. Produce JSON samples with platform/config/source-tip, warm/cold label, wall/CPU/peak-memory/I/O/network/queue metrics and storage growth. Missing counters are null with an explanation, never zero.

- [ ] Add `bench_refuses_source_as_scratch`, `bench_counts_samples_and_labels_cache`, `bench_compares_equivalent_revisions`, `bench_preserves_source`. Assert `assert_eq!(short_samples, 5); assert_eq!(expensive_samples, 3); assert_eq!(source_before, source_after);`. Assert queue metric `<= 268_435_456` and calculate `speedup = hg_median / rust_median`.
- [ ] Run `cargo test -p hg-client --test bench_runner`; expect missing harness; implement against tiny fixtures first.
- [ ] Build release binaries and run the approved scenarios using one reusable scratch derivative and recorded source revision. Preflight free space, keep source immutable, capture antivirus/cache conditions and all specification metrics. Profile Windows status enumeration, dirstate stats, content reads, delta chains and queue behavior; tune only measured bottlenecks with preserved correctness tests.
- [ ] Run `cargo run --release -p hg-client --example bench -- --source C:\src\mozilla-central --scratch <dedicated-scratch> --scenario status-clean --reference-hg <hg-path> --client <hgr-path> --output <report-path>` and each named scenario with real verified absolute paths. Require no unexplained >10% controlled regression; report the 2x clean-status aspiration honestly. Run representative Linux cases and record chain-depth/store-growth comparisons.
- [ ] Commit `perf: measure and tune large-repository client operations` only after relevant regressions pass; keep machine-specific large outputs out of Git and summarize results in the benchmark document.

### Task 30: Prove Mozilla acceptance and publish release documentation

**Files:** Create `hg-client/tests/mozilla.rs`, `docs/client-usage.md`; update `README.md`, `docs/{building,source-layout,hg-serve-differences,client-compatibility,client-benchmarks}.md`; add `hg-client/tests/package.rs`.
**Dependencies:** 1-29; public capability evidence must be resolved.
**Interfaces:** Ignored tests `local_mozilla_acceptance` and `public_mozilla_acceptance` read explicit `HGR_MOZILLA_SOURCE`, `HGR_MOZILLA_SCRATCH` and executable paths supplied by the harness. They never infer a writable path from the source. Package checks locate `hgr` without changing `hgcli`.

- [ ] Add `package_runs_without_python`, `mozilla_selected_heads_verify`, `public_pull_after_clone`, `source_remains_unchanged`. Assert `assert_eq!(verify_exit, 0); assert_eq!(selected_nodes, cloned_nodes); assert_eq!(checkout_status, b"");` for pinned checkout; compare selected head sets if the public tip moves.
- [ ] Run package tests locally; run `cargo test -p hg-client --test mozilla local_mozilla_acceptance -- --ignored --nocapture --test-threads=1` against a locally served read-only source and owned scratch. Verify full clone/check-out and controlled scratch exchanges with stock hg. Release evidence must cover both stream and changegroup on tiny fixtures; avoid retaining several Mozilla clones unnecessarily.
- [ ] Run `cargo test -p hg-client --test mozilla public_mozilla_acceptance -- --ignored --nocapture --test-threads=1` with sufficient scratch space and network. Perform fresh HTTPS clone, stock verify/checkout inspection and a subsequent pull; retain capabilities and source-head evidence. A DNS/auth/service outage leaves this gate incomplete. Never push to the public source. Do not weaken format support silently to force this test through.
- [ ] Build/package Windows and Linux release binaries; document prerequisites (system SSH/editor/merge tools), exact supported formats/options, JSON schema/exit codes, unsupported hooks/extensions, recovery versus clean update, observed performance and any blockers. Run final workspace/server/client suites and compare the completed matrix to every design requirement.
- [ ] Commit `docs: document and validate Rust Mercurial client release` after verified results. No release-complete claim while an acceptance gate is blocked or a correctness/performance exception lacks an explicit decision.

## Coverage and self-review record

| Approved requirement | Owning tasks |
|---|---|
| Shared engine and preserved legacy server support | 2, 4-12 |
| Modern requirements, compression, tree manifests, nodemaps | 2, 6-10, 13, 20 |
| Same working copy, standard state and byte-safe paths | 3-7, 13-15, 19, 22-28 |
| Clone/pull/push; local, HTTP(S), SSH and public Mozilla | 8-12, 20, 21, 27, 30 |
| Update/status/diff/log | 14-19, 26 |
| Add/remove/normal and partial commit | 22, 23 |
| Merge/resolve/two-parent commit and opposite-client continuation | 24-27 |
| Stock journaling, recover and interrupted updates | 4-7, 12, 13, 19, 23, 26, 28 |
| Config, hooks/extensions refusal, identities, TLS/proxy/auth | 2, 11, 16, 23, 25, 27 |
| Phases, tags, branches, bookmarks, copies and flags | 6, 9, 13-15, 18, 19, 21-26 |
| Bounded memory, oversized revisions and cancellation | 2, 4, 8-12, 20, 21, 28, 29 |
| Windows/Linux tests and parser fuzzing | 27, 28 |
| Windows benchmark methodology and targets | 29 |
| Packaging/docs and honest release evidence | 27, 29, 30 |

Self-review performed against the approved design: every requirement has an
owning task; the five Review Focus conditions have explicit assertions;
engine interfaces never depend on client/server transport types; newly named
production types have owning Interfaces blocks; task order follows their
dependencies. Some early deliverables intentionally expose only a safe subset
(for example clean update before dirty-update merging); Tasks 26 and 30
prevent that checkpoint from being mistaken for the complete first release.

No product code or dependency changes, benchmark workloads or Mercurial-repository
mutations were executed to create this plan. Runtime test commands above are required
future execution steps, not reports of tests already passing. The next step
is user review and selection of native or subagent-driven execution.
