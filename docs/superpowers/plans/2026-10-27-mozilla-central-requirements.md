# Mozilla-central Requirements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve the Mercurial 7 clone at `C:\src\mozilla-central` so a second stock Mercurial 7 client can clone it and get the same tip and an empty `hg status`.

**Architecture:** `RevlogRepo::open` unions `.hg/requires` with `.hg/store/requires` when `share-safe` is present, and accepts `sparserevlog` as the existing per-chunk generaldelta read. A self-based revision is a full text even when the generaldelta flag is off. `history_part` keeps changelog chunks as null-base full texts and forwards the stored manifest and filelog delta when that base is already available to the client. File-node lookup reuses one cached manifest text when the next revision's stored base is that revision. Long filelog paths use Mercurial's existing 120-byte hybrid encode so indexes under `store/dh` open. Push, wire capabilities, and bundle streaming stay as they are.

**Tech Stack:** Rust stable, edition 2021, Cargo resolver 2, tokio 1, futures 0.3, thiserror 2, zstd 0.13, flate2, sha1 0.10. Stock `hg` is Mercurial 7.0.3 on `PATH`. No new crates.

**Spec:** `docs/superpowers/specs/2026-10-27-mozilla-central-requirements-design.md`

## Global Constraints

- Unknown-requirement text stays `Unknown requirement "<line>"`.
- `treemanifest` still fails open, in either requires file, and the message contains `treemanifest is not served`.
- A missing `.hg/store/requires` while `share-safe` is present is an error that names `share-safe`. It is not `Unknown requirement "share-safe"`.
- `sparserevlog` adds no header flag, no density heuristic, and no writer. `SPARSE_REVLOG_MAX_CHAIN_LENGTH` is not enforced.
- A revision whose index base is itself is a full text whether or not generaldelta is set. Decompress it as a literal (zstd, zlib, lz4, or raw).
- The sequential non-generaldelta walker is not otherwise rewritten.
- Manifest and filelog revisions are changegroup 02 deltas. Changelog entries stay full texts.
- Revisions of one revlog are emitted in revision order. Stored fragments are forwarded, not recomputed, when the base node is already in this changegroup or already on the client.
- Otherwise send `base` `NULL_HASH` and one fragment `{start: 0, end: 0}` holding the full text.
- Bundle2 compression stays uncompressed. Changegroup version stays `02`.
- No push changes, no new bundle parts, no new wire capabilities, no revlog v2, no store rewrite.
- One process, socket `<repo>/.hg/mononoke.sock`, HTTPS only when both a cert and a key are passed.
- The client is stock Mercurial 7. The source repo is `C:\src\mozilla-central`. Do not clone it from the network again. Do not push to hg-edge.
- Manual proof is one HTTP clone to a new directory. ssh and HTTPS are proved on the tiny repo. Tip nodes equal and `hg status` is empty. Do not start a second or third mozilla-central working copy.
- The manifest cache is one slot for that `getbundle`. It is not a map of every manifest. Streaming the changegroup is not required.
- Implement on a new branch from master `58b35ab`. Do not commit these tasks onto master.
- If a `cargo` link fails because `D:` is full, set `CARGO_TARGET_DIR=C:\src\mononoke-target` and `CARGO_INCREMENTAL=0`.
- Serve tests that bind ports run as `cargo test -p server --test serve -- --test-threads=1`.

## File structure

- `mercurial/src/revlogrepo.rs` — `Required::ShareSafe`, `Required::SparseRevlog`, union of the two requires files.
- `mercurial/src/revlog/mod.rs` — self-base literal read (already in `get_entry`) and the one-slot reconstruction cache.
- `mercurial/src/revlog/test.rs` — changelog-shaped and sparse-chain fixtures.
- `mercurial-types/src/path.rs` — `Path::store_filelog`, Mercurial hybrid encode into `data/` or `dh/`.
- `server/src/bundle.rs` — `history_part` emits stored manifest and filelog deltas.
- `server/tests/serve.rs` — tiny share-safe sparse repo cloned over ssh, HTTP, and HTTPS.
- No changes to `server/src/push.rs`, HTTP capability lists, or revlog append.

## Review Focus

These are the inputs most likely to be mishandled. The test that pins each one is written in the task that owns the code.

- `share-safe` is present and `.hg/store/requires` is missing. Open fails and the message names `share-safe` and does not say `Unknown requirement`. Test: `share_safe_missing_store_requires_names_share_safe` (Task 1).
- A non-generaldelta self-base chunk is zlib (`x…`) or raw (`u` plus text), not zstd. Reading it returns that text. Test: `non_generaldelta_self_base_zlib_and_raw_are_literals` (Task 2).
- The cached manifest is not this revision's stored base. Reconstruction follows the snapshot chain and does not apply the cached text. Test: `reconstruction_ignores_cache_when_base_is_not_cached_rev` (Task 3).
- The client already has the stored file base via `common`, and that base is not in the new changegroup. The emitted chunk's `base` is that filenode. Test: `second_file_revision_keeps_its_base_when_client_has_it` (Task 5).
- A mixed-case path whose `data/…i` form is longer than 120 bytes lives under `store/dh`. `get_file_revlog` returns the file bytes. Test: `long_mixed_case_path_opens_from_store_dh` (Task 4).

---

### Task 1: Recognize share-safe and sparserevlog

**Files:**
- Modify: `mercurial/src/revlogrepo.rs` (`Required`, `FromStr`, `Display`, `RevlogRepo::open`)
- Test: `mercurial/src/revlogrepo.rs` (`mod test`)

**Interfaces:**
- Consumes: `ErrorKind::UnknownReq` displays `Unknown requirement "{0}"`. `open_requires` writes `.hg/requires` plus empty changelog and manifest indexes.
- Produces: `Required::ShareSafe` displays `share-safe`. `Required::SparseRevlog` displays `sparserevlog`. `RevlogRepo::open` unions both files. `get_requirements` returns that set. Later tasks open a Mercurial 7 repo that has `share-safe` only in the outer file.

- [ ] **Step 1: Write the failing tests**

Change `unknown_requirement_names_the_line` so the unknown outer line is `not-a-real-requirement`, not `share-safe`. Add `open_split(label, outer, store: Option<&[&str]>)` that does what `open_requires` does, and writes `store/requires` only when `store` is `Some`.

```rust
#[test]
fn share_safe_reads_store_requires() {
    let repo = open_split(
        "share",
        &["share-safe"],
        Some(&[
            "dotencode",
            "fncache",
            "generaldelta",
            "revlog-compression-zstd",
            "revlogv1",
            "sparserevlog",
            "store",
        ]),
    )
    .expect("open");
    let names: Vec<_> = repo.get_requirements().iter().map(|r| r.to_string()).collect();
    for line in ["share-safe", "sparserevlog", "generaldelta", "revlogv1", "revlog-compression-zstd", "store", "fncache", "dotencode"] {
        assert!(names.iter().any(|n| n == line), "{line} missing from {names:?}");
    }
}

#[test]
fn share_safe_missing_store_requires_names_share_safe() {
    let err = open_split("missing", &["share-safe"], None).expect_err("missing store/requires");
    let msg = err.to_string();
    assert!(msg.contains("share-safe"), "{msg}");
    assert!(!msg.contains("Unknown requirement"), "{msg}");
}

#[test]
fn same_line_in_both_requires_files_opens() {
    open_split("both", &["share-safe", "store"], Some(&["store", "sparserevlog"])).expect("union");
}

#[test]
fn treemanifest_in_store_requires_is_not_served() {
    let err = open_split("tree-store", &["share-safe"], Some(&["store", "treemanifest"]))
        .expect_err("treemanifest");
    assert!(err.to_string().contains("treemanifest is not served"), "{}", err);
}

#[test]
fn unknown_store_requirement_names_the_line() {
    let err = open_split("unk-store", &["share-safe"], Some(&["not-a-real-requirement"]))
        .expect_err("unknown store line");
    assert!(
        err.to_string().contains("Unknown requirement \"not-a-real-requirement\""),
        "{}",
        err
    );
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p mercurial --lib share_safe_reads_store_requires`

Expected: FAIL. `share-safe` is still an unknown requirement, so `share_safe_reads_store_requires` returns that error.

- [ ] **Step 3: Implement the union in `RevlogRepo::open`**

Add the two enum variants and their `Display` / `FromStr` arms. Parse `.hg/requires` as today. If the set contains `ShareSafe`, parse `.hg/store/requires` into the same `HashSet`. `NotFound` on that file returns an error whose text contains `share-safe` and is not `UnknownReq`. After the union, `Treemanifest` still returns `treemanifest is not served`. `sparserevlog` is only a recognized name.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p mercurial --lib revlogrepo::test`

Expected: PASS, including `unknown_requirement_names_the_line`, `treemanifest_is_not_served`, and `zstd_requirement_is_accepted`.

- [ ] **Step 5: Commit**

```powershell
git add mercurial/src/revlogrepo.rs
git commit -m "feat: read share-safe store requirements"
```

### Task 2: Pin the changelog and sparse reads

**Files:**
- Modify: `mercurial/src/revlog/mod.rs` only if a test in this task fails
- Test: `mercurial/src/revlog/test.rs`

**Interfaces:**
- Consumes: `Revlog::new(idx, Some(data))`, `ng_entry`, `get_rev`, `parser::literal`. `get_entry` already sets `baserev = None` when generaldelta is off and `baserev == Some(idx)`, so `get_chunk` takes the literal path. `construct_general` already follows `baserev` and reads each chunk at its own offset and `compressed_len`.
- Produces: no new signature. `get_rev` on a changelog-shaped revlog returns the zstd text. `get_rev` on a generaldelta chain whose base skips an intervening revision returns the reconstructed text. Task 5 relies on `Chunk::Literal` for snapshots and `Chunk::Deltas(RevIdx, Vec<bdiff::Delta>)` for stored deltas.

- [ ] **Step 1: Write the tests**

Use `ng_entry` for 64-byte revlog-NG entries. On entry 0 only, set bytes `0..2` to the feature word and bytes `2..4` to version `1`. Put a distinct byte at `entry[51]` so the node ids differ. Parents and linkrev stay `u32::MAX`.

`changelog_self_base_zstd_reads_full_text`: features `0x0000`, not inline, data file separate, one revision, `baserev` 0, chunk is `zstd::encode_all(b"hello changelog", 0)`. `get_rev(RevIdx::from(0u32))` returns that text.

`non_generaldelta_self_base_zlib_and_raw_are_literals`: same header, two separate revlogs. One chunk is `u` plus `raw-text`. The other is a zlib stream of `zlib-text` (first byte `x`). Each `get_rev(0)` returns that text.

`sparse_chain_skips_unrelated_revision`: features `0x0002`, not inline. Rev 0 is `u` plus `aaaa`, `baserev` 0. Rev 1 is `u` plus `zzzz`, `baserev` 1, data offset `len(rev0 chunk)`. Rev 2's chunk is `u` plus one fragment `start=0, end=1, len=1, content=b` (each integer is 4 big-endian bytes), `baserev` 0, not 1. `get_rev(2)` equals `baaa`.

- [ ] **Step 2: Run the tests**

Run: `cargo test -p mercurial --lib changelog_self_base_zstd_reads_full_text`

Run: `cargo test -p mercurial --lib non_generaldelta_self_base_zlib_and_raw_are_literals`

Run: `cargo test -p mercurial --lib sparse_chain_skips_unrelated_revision`

Expected: PASS. The reader already treats a non-generaldelta self-base as a literal and already slices each generaldelta chunk by offset.

- [ ] **Step 3: Change production code only when a test failed**

If the changelog or literal test failed, treat `baserev == Some(idx)` as a literal in `get_chunk` even when generaldelta is off. If the sparse test failed, read each chain member with that entry's offset and `compressed_len`. Do not rewrite `construct_simple`. Do not add a sparse writer.

- [ ] **Step 4: Re-run the three tests**

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add mercurial/src/revlog/test.rs mercurial/src/revlog/mod.rs
git commit -m "test: read a self-based changelog and a sparse chain"
```

### Task 3: Reuse the last manifest text

**Files:**
- Modify: `mercurial/src/revlog/mod.rs` (`RevlogInner`, `construct_general`)
- Test: `mercurial/src/revlog/test.rs`

**Interfaces:**
- Consumes: `construct_general`, `get_chunk`, `delta::compat::apply_deltas`.
- Produces: `RevlogInner.text_cache: Option<(RevIdx, Vec<u8>)>`. `get_rev` / `get_manifest_by_nodeid` reuse it. No new public method. Task 5's file-node lookup calls `get_manifest_by_nodeid` and depends on this slot.

- [ ] **Step 1: Write the test**

Generaldelta, not inline, three revisions. Rev 0 literal `hello`, `baserev` 0. Rev 1 delta against 0 that inserts `!` at offset 5 (`start=5, end=5, len=1`). Rev 2 delta against 0, not against 1, that inserts `?` at offset 5.

```rust
#[test]
fn reconstruction_ignores_cache_when_base_is_not_cached_rev() {
    let revlog = sparse_fixture(); // the three revisions above
    let first = revlog.get_rev(RevIdx::from(1u32)).expect("rev 1");
    assert_eq!(first.as_blob().as_slice(), Some(&b"hello!"[..]));
    let skipped = revlog.get_rev(RevIdx::from(2u32)).expect("rev 2");
    assert_eq!(skipped.as_blob().as_slice(), Some(&b"hello?"[..]));
}
```

- [ ] **Step 2: Run the test**

Run: `cargo test -p mercurial --lib reconstruction_ignores_cache_when_base_is_not_cached_rev`

Expected: PASS before the cache exists. The current walker already rebuilds rev 2 from rev 0. The test stays as the guard against applying the cached text to the wrong base.

- [ ] **Step 3: Add the one-slot cache in `construct_general`**

Add `text_cache: Option<(RevIdx, Vec<u8>)>` on `RevlogInner`. The only `RevlogInner { ... }` literal is in `Revlog::init`; set `text_cache: None` there. In `construct_general`, after loading the target entry: if `entry.baserev == Some(cached_idx)` for the stored slot, `get_chunk` that revision, apply those deltas to the cached bytes, store `(tgtidx, text)`, and return. Otherwise keep the existing snapshot walk, then store the result in the same slot. Do not cache any other revision. Do not change `construct_simple`. A snapshot (`baserev == idx`) does not hit the slot.

- [ ] **Step 4: Re-run the test and `sparse_chain_skips_unrelated_revision`**

Expected: PASS. The diff adds `text_cache` and the early return in `construct_general`. A green test with no change there is not done.

- [ ] **Step 5: Commit**

```powershell
git add mercurial/src/revlog/mod.rs mercurial/src/revlog/test.rs
git commit -m "feat: reuse the last reconstructed revlog text"
```

### Task 4: Open filelogs under store/dh

**Files:**
- Modify: `mercurial-types/src/path.rs`
- Modify: `mercurial/src/revlogrepo.rs` (`file_index_path`)
- Test: `mercurial-types/src/path.rs`, `mercurial/src/revlogrepo.rs`

**Interfaces:**
- Consumes: `fnencode`, `auxencode`, `direncode`, and the existing `lowerencode`. `sha1` 0.10 is already a `mercurial-types` dependency. `file_index_path` is private and is what `get_file_revlog` calls.
- Produces: `Path::store_filelog(&self, dotencode: bool) -> std::path::PathBuf`, store-relative, `/` components, either `data/…i` or `dh/…i`. `get_file_revlog` uses it. Task 6's short paths stay `data/<name>.i`.

`C:\src\mozilla-central\.hg\store\dh` is populated (hashed names such as `browser`, `devtools`, 8-character prefixes). `Path::fsencode` never returns a `dh/` path. This task is that encode, from Mercurial 7 `store.py` `_pathencode` / `_hashencode`. It is not a new requirement and not a new on-disk format.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn store_filelog_short_mixed_case_stays_under_data() {
    let path = Path::new(b"Foo.txt").unwrap();
    assert_eq!(
        path.store_filelog(true),
        std::path::Path::new("data").join("_foo.txt.i")
    );
}
```

`long_mixed_case_path_opens_from_store_dh` in `revlogrepo.rs`: `hg init` with the same format flags `init_zstd_repo` uses (`generaldelta`, `revlog-compression=zstd`, `sparse-revlog=false`, `use-persistent-nodemap=false`, `use-share-safe=false`, `use-dirstate-v2=false`, `exp-use-copies-side-data-changeset=false`). Add one file whose name is `A` plus 113 `b`s (114 bytes), contents `long\n`, and commit it. Assert `.hg/requires` contains `dotencode`, no index exists at `store/data/<name>.i`, and an index exists under `store/dh`. `RevlogRepo::open` on `.hg` then `get_file_revlog` of that path: `get_rev(0)` equals `long\n`.

- [ ] **Step 2: Run the tests**

Run: `cargo test -p mercurial-types --lib store_filelog_short_mixed_case_stays_under_data`

Run: `cargo test -p mercurial --lib long_mixed_case_path_opens_from_store_dh`

Expected: FAIL to compile, then FAIL to open the hashed file.

- [ ] **Step 3: Implement `Path::store_filelog`**

Build `raw` as the bytes `data/` + the logical path with `/` separators + `.i`. `de` is `encodedir(raw)` only: replace `.hg/` with `.hg.hg/`, then `.i/` with `.i.hg/`, then `.d/` with `.d.hg/`. Do not fnencode `de`.

Let `encoded` be fnencode + auxencode of `de`, joined with `/`. If `raw.len() > 120` or `encoded.len() > 120`, return the hash form. Otherwise return `encoded`.

Hash form, matching `_hashencode(de, dotencode)`:

- Digest is lowercase hex SHA-1 of `de` (40 chars).
- `lowerencode(de[5..])` (skip `data/`). This lowercases `A-Z` and encodes reserved bytes as `~xx`. It does not insert `_` before capitals.
- `auxencode` those components with the `dotencode` flag.
- Directory prefixes are the first 8 bytes of each directory component, at most 68 bytes of prefixes plus slashes (`8 * (8 + 1) - 4`). A prefix that ends in `.` or space replaces that byte with `_`.
- Extension is `splitext` of the final component, including the dot (`.i`).
- `res` is `dh/` + prefixes + `/` (if any prefix) + digest + extension. If `120 - res.len()` is positive, insert that many leading bytes of the final component, extension included, between the prefixes and the digest. The extension is still appended after the digest.

Count lengths on the `/` byte string. Build the `PathBuf` by splitting on `/`. `file_index_path` becomes `store.join(path.store_filelog(dotencode))` and no longer adds a second `.i`.

- [ ] **Step 4: Run both tests**

Expected: PASS. Also run `cargo test -p mercurial-types --lib fsencode`.

Expected: PASS.

- [ ] **Step 5: Commit**

```powershell
git add mercurial-types/src/path.rs mercurial/src/revlogrepo.rs
git commit -m "feat: open filelogs stored under store/dh"
```

### Task 5: Send stored manifest and filelog deltas

**Files:**
- Modify: `server/src/bundle.rs` (`history_part` and the helpers under it)
- Test: `server/src/bundle.rs` (`mod tests`)

**Interfaces:**
- Consumes: `Revlog::get_chunk(RevIdx) -> Result<Chunk>`, `Revlog::get_entry`, `Revlog::get_rev_by_nodeid`, `Revlog::index_of(&NodeHash) -> Result<u32>`, `RevlogRepo::manifest_revlog`, `RevlogRepo::get_file_revlog`, `RevlogRepo::get_manifest_by_nodeid` (uses Task 3's cache), `fulltext_chunk`, `parent_nodes`, `NULL_HASH`. `Chunk::Deltas` carries `Vec<mercurial_types::bdiff::Delta>` (`start`, `end`, `content`). `CgDeltaChunk.delta` is `mercurial_types::delta::Delta`, built with `Delta::new(Vec<Fragment>)`. `BlobNode::new(bytes, p1, p2)` sets `maybe_renamed` for a p2-only parent.
- Produces: `fn history_parts(repo: &RevlogRepo, common: &[NodeHash]) -> Result<Vec<Part>>` in `bundle.rs` (private; the tests call it). `history_part` calls it and then `changegroup_part`. `history_part`'s signature stays `pub fn history_part(repo: &RevlogRepo, common: &[NodeHash]) -> Result<PartEncodeBuilder>`. Changelog parts stay null-base full texts. Manifest and filelog parts use the stored base node when that node is available. `renamed_filelog_parent_stays_in_p2` stays green.

- [ ] **Step 1: Write the failing tests**

Add a test helper that runs `hg init` with `format.use-share-safe=true`, `format.generaldelta=true`, `format.revlog-compression=zstd`, `format.sparse-revlog=true`, plus the extra flags `init_zstd_repo` turns off (`use-persistent-nodemap`, `use-dirstate-v2`, `exp-use-copies-side-data-changeset`). Two commits must edit `a.txt` (`alpha\n`, then `beta\n`). Assert `.hg/requires` contains `share-safe`, `.hg/store/requires` contains `sparserevlog`, `generaldelta`, `revlogv1`, and `revlog-compression-zstd`, the filelog index starts with feature bytes `00 03`, and rev 1's `baserev` is `Some(RevIdx::from(0u32))`. If this `hg` will not write that split, build those require files and the same revlog bytes by hand and say so in the test. The assertions below stay.

`second_file_revision_uses_stored_base`: call `history_parts(repo, &[])`. The filelog `CgChunk`s are two. The first `base` is `NULL_HASH`. The second `base` equals the first chunk's `node` and is not `NULL_HASH`. `mercurial_types::delta::apply(b"alpha\n", second.delta.clone())` equals `beta\n`. Every `Section::Changeset` chunk has `base == NULL_HASH`.

`second_file_revision_keeps_its_base_when_client_has_it`: `common` is the changeset node from `hg log -r 0 -T {node}` (`NodeHash` implements `FromStr`). The filelog section has one chunk. Its `base` equals `filelog.get_entry(RevIdx::from(0)).nodeid`. Its `node` equals rev 1. That base node is not itself a chunk in this group.

- [ ] **Step 2: Run the tests**

Run: `cargo test -p server --lib second_file_revision_uses_stored_base`

Expected: FAIL because the second file chunk's `base` is `NULL_HASH`.

- [ ] **Step 3: Forward stored deltas from `history_parts`**

Keep the changelog loop on `fulltext_chunk`. Build `available: HashSet<NodeHash>` from `common`, excluding `NULL_HASH`.

Walk changelog index order. For every changeset, remember its manifest id. If the changeset is not missing, insert that manifest id into `available`, and when `files()` is non-empty insert each filenode from `get_manifest_by_nodeid` into `available`. If it is missing, queue the manifest and the new filenodes as today.

Emit queued manifest revisions sorted by `manifest_revlog.index_of`. Emit each file's revisions sorted by that filelog's `index_of`. After each emitted node, insert it into `available`.

For one manifest or file revision, `get_chunk` of its index:

- `Chunk::Literal`: `fulltext_chunk` with that text. Do not call `get_rev`.
- `Chunk::Deltas(base_idx, deltas)` and `get_entry(base_idx).nodeid` is in `available`: `CgDeltaChunk.base` is that node. Copy each `bdiff::Delta` into a `Fragment` and `Delta::new`. Do not call `get_rev`.
- Otherwise: `get_rev_by_nodeid` and `fulltext_chunk` (`base` `NULL_HASH`).

Parents come from the index. Map `entry.p1` / `entry.p2` through `get_entry` to node ids, then `BlobNode::new` and the existing `parent_nodes`. Section ends stay where they are. Do not change `push.rs`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p server --lib bundle::tests`

Expected: PASS, including `renamed_filelog_parent_stays_in_p2`.

- [ ] **Step 5: Commit**

```powershell
git add server/src/bundle.rs
git commit -m "feat: send stored manifest and filelog deltas"
```

### Task 6: Clone the tiny share-safe repo

**Files:**
- Modify: `server/tests/serve.rs`
- Test: `server/tests/serve.rs`

**Interfaces:**
- Consumes: `init_zstd_repo`'s format flags, `Running::start`, `Running::start_https`, `write_test_cert`, `write_ssh_shim`, `hg`, `hg_ssh`, `sibling_bin`. `history_part` from Task 5. `RevlogRepo::open` from Task 1.
- Produces: no library API. The existing serve tests still pass.

- [ ] **Step 1: Write the failing test**

`share_safe_sparse_repo_clones_over_ssh_http_and_https`. Init with the Task 5 flags, including `format.use-share-safe=true` and `format.sparse-revlog=true`, and the same two revisions of `a.txt`. Assert the require lines from the spec: outer file contains `share-safe`; `store/requires` contains `sparserevlog`, `generaldelta`, `revlogv1`, and `revlog-compression-zstd`. Record `hg log -r tip -T {node}`.

Serve that repo. Clone over ssh (`ssh://user@dummy/repo` plus `write_ssh_shim`), `http://127.0.0.1:{port}/`, and `https://127.0.0.1:{https_port}/` with the test cert's `web.cacerts`. In each dest, `hg log -r tip -T {node}` equals the source tip and `hg status` has empty stdout. `a.txt` is `beta\n`.

- [ ] **Step 2: Run the test**

Run: `cargo test -p server --test serve share_safe_sparse_repo_clones_over_ssh_http_and_https -- --test-threads=1`

Expected: FAIL until Tasks 1 and 5 are in this branch. After them, this step is the integration check.

- [ ] **Step 3: Fix only integration glue**

No new protocol behavior. If `hg` did not write the share-safe split, use the hand-built bytes from Task 5 and keep the stock `hg` client. Do not weaken the tip or status assertions. If the clone fails because a chunk is wrong, fix `history_parts`, not this test.

- [ ] **Step 4: Run the serve tests and the workspace**

Run: `cargo test -p server --test serve -- --test-threads=1`

Expected: PASS, including the existing clone and push tests.

Run: `cargo test --workspace -- --test-threads=1`

Expected: PASS. The ignored revlog test may stay ignored.

- [ ] **Step 5: Commit**

```powershell
git add server/tests/serve.rs
git commit -m "test: clone a share-safe sparse repo over ssh, http, and https"
```

### Task 7: Clone mozilla-central once over HTTP

**Files:**
- None, unless open fails on a requirement this plan already names. Do not add a requirement that is not in the spec.

**Interfaces:**
- Consumes: the `mononoke` binary from this branch. `RevlogRepo::open` is called on `C:\src\mozilla-central\.hg` because the CLI takes the parent of `.hg`. A failed open is logged and the process can still exit 0. Success is the socket file, not the exit code.
- Produces: one clone at `C:\src\mozilla-central-http`.

- [ ] **Step 1: Confirm the source and the destination**

`C:\src\mozilla-central\.hg\requires` is the single line `share-safe`. Do not clone from hg-edge. If `C:\src\mozilla-central-http` already exists, stop. Do not delete it.

- [ ] **Step 2: Serve HTTP and wait for the socket**

Build `mononoke` from this branch. Start it with `--http-port` on a free port and repo argument `C:\src\mozilla-central`. Write stdout and stderr to `C:\src\mozilla-central-mononoke.txt`. Wait until `C:\src\mozilla-central\.hg\mononoke.sock` exists. If the log contains `Unknown requirement` or `treemanifest is not served`, stop and report that line. Do not implement it.

- [ ] **Step 3: Clone once**

`hg clone http://127.0.0.1:<port>/ C:\src\mozilla-central-http`. Let it run. This is hours, not the 120-second test timeout. Do not start a second clone. Do not push.

- [ ] **Step 4: Compare tip and status**

`hg -R C:\src\mozilla-central log -r tip -T {node}` and the same command on `C:\src\mozilla-central-http` print the same node. `hg -R C:\src\mozilla-central-http status` prints nothing. A different tip is a failure even if status is empty. Leave the server log in place.

- [ ] **Step 5: Do not commit a clone**

No source change, no commit. Stop the `mononoke` process when the comparison is done.
