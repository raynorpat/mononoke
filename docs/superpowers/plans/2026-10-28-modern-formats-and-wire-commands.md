# Modern Formats and Remaining Wire Commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Serve Mercurial 7.0 default repositories (persistent-nodemap) and treemanifest repositories, and answer every remaining v1 wire command the way `hg serve` does.

**Architecture:** The revlog layer reads Mercurial's on-disk nodemap trie instead of building a node map. `mercurial-bundles` speaks changegroup `01`, `02` and `03`, including directory-manifest groups. A Rust port of Mercurial's C `bdiff` produces byte-identical `01` deltas. The server gains the legacy commands (`changegroup`, `changegroupsubset`, bundle1 `getbundle`/`unbundle`, `stream_out`, `branches`), real tags/branches in `lookup`/`branchmap`, clone bundles, and reads one setting from `.hg/hgrc`.

**Tech Stack:** Rust 2021, tokio 1, futures 0.3, flate2, bzip2 (already in `async-compression`). Stock `hg` 7.0.3 on `PATH`. Reference source: `C:\Users\raynorpat\scoop\apps\mercurial\current\lib\mercurial\` (Python). The C `bdiff` source is not installed; fetch `mercurial/bdiff.c`, `mercurial/bdiff.h`, `mercurial/cext/bdiff.c` at tag `7.0.3` from `https://repo.mercurial-scm.org/hg/raw-file/7.0.3/<path>`.

**Spec:** `docs/superpowers/specs/2026-10-28-modern-formats-and-wire-commands-design.md`

## Global Constraints

- Work on a new branch `formats` from `master`. Never commit to `master`.
- Commit messages: `<subsystem>: <what it does>`, one or two lines, no trailers (CLAUDE.md).
- Behaviour to match is Mercurial 7.0.3's Python source. Read the file named in each task before writing code.
- Out of scope: `revlogv2` (`exp-revlogv2.2`), `changelogv2` (`exp-changelog-v2`), changegroup `04`, narrow, largefiles, LFS. These stay "Unknown requirement".
- The server never writes `.n` / `.nd` nodemap files.
- Requirement names exactly: `persistent-nodemap`, `treemanifest`.
- Bundle2 `changegroup` capability: `01,02,03` for every repository. `getbundle` sends the highest version both sides list, except a treemanifest repository only sends `03`.
- `unbundle` capability value: `unbundle=HG10GZ,HG10BZ,HG10UN` (`bundle2.bundlepriority`).
- Legacy command output (`changegroup`, `changegroupsubset`, bundle1 `getbundle`, `branches`, `stream_out`, `clonebundles`, `clonebundles_manifest`, `lookup`) is byte-identical to `hg serve` on the same repository. `branchmap` is compared as parsed sets (Mercurial's line order is dict order).
- Only `.hg/hgrc` is read, only `[server] peer-bundle-cache-root`. No `%include`, no user or system config.
- Existing tests keep `format.use-persistent-nodemap=false`. New tests that need the nodemap pass `--config format.use-persistent-nodemap=true`. If `hg` reports "accessing `persistent-nodemap` repository without associated fast implementation", add `--config storage.revlog.persistent-nodemap.slow-path=allow` to every `hg` call on that repo.
- Mercurial writes a nodemap only for a non-inline revlog. Fixtures grow the changelog with `hg debugbuilddag +3000` so it splits.
- Serve tests that bind ports: `cargo test -p mononoke --test serve -- --test-threads=1`.
- If a link fails because `D:` is full, set `CARGO_TARGET_DIR=C:\src\mononoke-target` and `CARGO_INCREMENTAL=0`.
- **Shared with the LFS plan** (`docs/superpowers/plans/2026-10-28-lfs.md`): `CgVersion`, `CgDeltaChunk.flags`, `with_version`, `changegroup_part_version`, `client_cg_versions`, `RevlogRepo::add_requirement` and the live requirement set. If the LFS branch has already landed them, use them as they are and only add what this plan adds (`CgVersion::V1`, tree sections). If not, build them here with exactly the LFS plan's names and signatures. One deliberate difference: a changegroup part with no `version` parameter means `01` (as `bundle2.handlechangegroup` defaults), not `02`.

## File Structure

- `mercurial/src/revlog/nodemap.rs` (new): docket and trie reading, prefix lookup.
- `mercurial/src/revlog/mod.rs`: nodemap hookup in node lookup, `load_node_map`, `reload`.
- `mercurial/src/revlogrepo.rs`: `Required::{PersistentNodemap}`, accept `Treemanifest`, directory manifest revlogs, live requirements, `add_requirement`, `hgrc` value, dirstate parent, `.hgtags` reading helpers.
- `mercurial-types/src/path.rs`: `store_dirlog`.
- `mercurial-types/src/textdiff.rs` (new): bdiff port.
- `mercurial-bundles/src/changegroup/{mod,packer,unpacker}.rs`, `part_inner.rs`, `parts/mod.rs`: `CgVersion { V1, V2, V3 }`, flags, `Section::Treemanifest`.
- `server/src/bundle.rs`: version-aware history parts, tree groups, `01` deltas, roots-based selection.
- `server/src/push.rs`: tree groups, `treemanifest=1`, `01` parts, bundle1 unbundle.
- `server/src/stream.rs`: nodemap and `meta/` files, stream v1.
- `server/src/names.rs` (new): branch map cache, tags, `lookup` symbol resolution.
- `server/src/repo.rs`: capabilities, version negotiation, new commands.
- `server/src/main.rs`, `server/src/http.rs`: streamed responses for the new commands, bundle1 push replies.
- `hgproto/src/{lib.rs,service.rs,sshproto/request.rs,sshproto/response.rs}`: new requests/responses, `stream_out` name fix.
- `server/tests/serve.rs`: end-to-end tests, `hg serve` comparison helper.
- `docs/hg-serve-differences.md`, `docs/running.md`.

## Review Focus

- `hg commit` on a persistent-nodemap repo while the server runs replaces `00changelog.n` and deletes the old `.nd`. The server's next request must use the new nodemap, and a stream clone listed before the commit must still complete. Test: `nodemap_repo_commit_while_serving_is_served` (Task 2).
- A nodemap docket whose tip no longer matches the index (after `hg strip`). The server must ignore it and never answer a node with a wrong revision. Test: `stale_docket_after_strip_falls_back` (Task 1).
- A node that is not in the revlog but shares a trie prefix with one that is. The lookup must fail, not return the neighbour. Test: `unknown_node_sharing_prefix_is_not_found` (Task 1).
- Treemanifest directories whose store names need encoding (uppercase, or long enough for `dh/`). Clone, push and stream clone must all find them. Test: `tree_repo_with_encoded_dir_names_round_trips` (Task 6).
- Branch names with spaces or non-ASCII bytes in `branchmap`. They must be URL-quoted like Mercurial does, and `hg pull -r "<name>"` must work. Test: `branch_names_with_spaces_are_quoted` (Task 9).

---

### Task 1: Read the persistent nodemap

**Files:**
- Create: `mercurial/src/revlog/nodemap.rs`
- Modify: `mercurial/src/revlog/mod.rs:86-95,256-262,344-365,463-495`
- Test: `mercurial/src/revlog/test.rs`

**Interfaces:**
- Produces, in `nodemap.rs` (crate-private):
  - `struct Docket { uid: Vec<u8>, tip_rev: u64, data_length: u64, tip_node: Vec<u8> }`, `fn parse_docket(bytes: &[u8]) -> Option<Docket>`: version byte `1`, then `>BQQQQ` (uid length, tip rev, data length, unused, tip node length), uid, tip node. Any other version or short input is `None`.
  - `struct PersistentNodeMap { trie: Datafile, tip_rev: u32 }`, `fn candidate(&self, node: &NodeHash) -> Option<u32>`: walk from the **last** 64-byte block; each block is 16 big-endian `i32`; for each hex nibble of `node`, `-1` → `None`, `>= 0` → descend into that block, `<= -2` → `Some(-(v + 2))`. Only the first `data_length` bytes count.
  - `fn load(idx_path: &Path) -> Result<Option<(Docket, Datafile)>>`: docket at `idx_path.with_extension("n")`; data at `<idx stem>-<uid>.nd` in the same directory. Missing files are `Ok(None)`.
- Produces, on `Revlog`:
  - `pub fn use_persistent_nodemap(&self) -> Result<bool>`: if the revlog is non-inline and the docket validates (`tip_rev < len`, node at `tip_rev` == `tip_node`, data file at least `data_length` long), keep it and return `true`. Also remembers that the nodemap is wanted, so `reload` re-reads it.
  - Node lookup (`get_idx_by_nodeid`): cache hit as today. On a miss with a nodemap: `candidate` → read that entry → return it only if its node is equal; otherwise search only revisions after `tip_rev`. A miss there is "not found" without a full scan.
  - `load_node_map`: with a nodemap, parses only revisions after `tip_rev`.

- [ ] **Step 1: Write the failing tests** (in `revlog/test.rs`; build fixtures with stock `hg` in a temp dir, `hg init` with `format.use-persistent-nodemap=true`, then `hg debugbuilddag +3000`, then check `hg debugnodemap --metadata` reports a tip rev)

`nodemap_resolves_every_node`: open `00changelog.i`, `use_persistent_nodemap()` is `true`, every node from `hg log -T "{node}\n"` resolves to its revision.

`unknown_node_sharing_prefix_is_not_found`: take an existing node, flip its last nibble, assert `index_of` is an error.

`nodemap_load_parses_only_the_tail`: after `use_persistent_nodemap()` and `load_node_map()`, the crate-internal `nodeidx` holds fewer than 10 entries (the trie covers the rest).

`revisions_after_docket_tip_resolve`: append two revisions with `append_snapshot` after loading; both resolve, and `hg debugnodemap --check` plus `hg verify` still pass.

`stale_docket_after_strip_falls_back`: simulate a strip `hg` did not record by overwriting the `tip_node` bytes in `00changelog.n` with 20 zero bytes; `use_persistent_nodemap()` is `false` and every node still resolves.

`docket_parses_mercurial_bytes`: `parse_docket` on the bytes of a real `00changelog.n` gives the uid that names the existing `.nd` file.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p mercurial nodemap`
Expected: compile failure on `use_persistent_nodemap`.

- [ ] **Step 3: Implement** as in Interfaces. Read `revlogutils/nodemap.py` (`persisted_data`, `parse_data`, `_find_node`) and `revlog.py` `_register_nodemap_info` first.

- [ ] **Step 4: Run tests**

Run: `cargo test -p mercurial`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add mercurial/src/revlog
git commit -m "revlog: look nodes up in Mercurial's persistent nodemap"
```

### Task 2: Serve persistent-nodemap repositories

**Files:**
- Modify: `mercurial/src/revlogrepo.rs:34-103,142-178,255-261`
- Modify: `server/src/stream.rs:39-156,225-297`
- Test: `mercurial/src/revlogrepo.rs` tests, `server/tests/serve.rs`

**Interfaces:**
- Consumes: `Revlog::use_persistent_nodemap` (Task 1).
- Produces: `Required::PersistentNodemap` (displays `persistent-nodemap`). `RevlogRepo::open` calls `use_persistent_nodemap()` on the changelog and the manifest when the requirement is present. `stream_requirements` leaves `PersistentNodemap` out (stream-ignorable). `list_store` adds each of `00manifest`/`00changelog`'s `.n` and current `-<uid>.nd`, in the order Mercurial's `store.py` lists a revlog's files. `StoreFile` gains `data: Option<Bytes>`: nodemap files are read into memory while listing under the store lock, and `send_files` sends that data, because `hg` replaces them by rename and deletes the old `.nd`.

- [ ] **Step 1: Write the failing tests**

`persistent_nodemap_requirement_opens` (revlogrepo tests): requires `["store","revlogv1","persistent-nodemap"]` opens.

`stream_requirements_skip_persistent_nodemap` (stream.rs tests): a repo with the requirement → the `streamreqs` value does not contain `persistent-nodemap`.

`nodemap_repo_clones_pulls_and_pushes` (serve.rs): `hg init` with `format.use-persistent-nodemap=true` and every other format at Mercurial 7.0's default, `hg debugbuilddag +3000`, one real commit with a file. Over ssh and HTTP: stream clone, `--pull` clone, commit in the clone, push. Afterwards on the server: `hg verify` and `hg debugnodemap --check` pass. The stream clone's `.hg/store` has `00changelog.n`.

`nodemap_repo_commit_while_serving_is_served` (serve.rs): start the server, stream clone once (so a listing is cached), `hg commit` in the served repo, then `hg pull` in the clone gets the new commit, and a second fresh stream clone succeeds and verifies.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p mercurial revlogrepo && cargo test -p mononoke --test serve nodemap -- --test-threads=1`
Expected: `Unknown requirement "persistent-nodemap"`.

- [ ] **Step 3: Implement** as in Interfaces. Read `store.py` (`REVLOG_FILES_VOLATILE_EXT`, the revlog entry file order) and `requirements.py` `STREAM_IGNORABLE_REQUIREMENTS` first.

- [ ] **Step 4: Run tests**

Run: `cargo test -p mercurial && cargo test -p mononoke --lib && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add mercurial/src server/src server/tests
git commit -m "server: serve persistent-nodemap repositories and stream their nodemaps"
```

### Task 3: Changegroup 01, 02 and 03 with tree sections

**Files:**
- Modify: `mercurial-bundles/src/changegroup/mod.rs`, `packer.rs`, `unpacker.rs`
- Modify: `mercurial-bundles/src/part_inner.rs:43,210-220`, `mercurial-bundles/src/parts/mod.rs:45-54`, `mercurial-bundles/src/quickcheck_types.rs`
- Modify: every `CgDeltaChunk { … }` literal (`server/src/bundle.rs`, `server/src/push.rs`, bundle tests)

**Interfaces:**
- Produces (LFS-plan names, plus `V1` and the tree section):
  - `pub enum CgVersion { V1, V2, V3 }`, `param()` → `"01"`/`"02"`/`"03"`, `from_param(&[u8]) -> Option<CgVersion>`.
  - `CgDeltaChunk.flags: u16` (written only in V3).
  - `Section::Treemanifest(Path)`: the directory without its trailing `/` (`"dir"`, `"a/b"`). On the wire the header chunk is `dir/`.
  - `Cg2Packer::with_version(stream, CgVersion)`, `Cg2Unpacker::with_version(logger, CgVersion)`; `new` stays V2. `changegroup_part_version(parts, CgVersion)` sets mparam `version`.
  - `inner_stream` picks the unpacker from the `version` mparam; a missing one is `V1`.
- Wire rules (from `changegroup.py`):
  - V1 header `>20s20s20s20s` (node, p1, p2, linknode). The packer drops `base`; the producer must have made the delta against the previous chunk of the group, or p1 for the group's first. The unpacker fills `base` the same way (`cg1unpacker._deltaheader`).
  - V3 header `>20s20s20s20s20sH`. After the root manifest group's close chunk come zero or more directory groups (header chunk `dir/`, deltas, close chunk), then one empty chunk, then files. The V3 packer writes that empty chunk before the first `Filelog` section or before `End`, whichever comes first after the manifests.

- [ ] **Step 1: Write the failing tests** (in `changegroup/mod.rs`)

`round_trip_every_version`: the existing quickcheck round-trip, run for V1 (bases rewritten to the previous node / p1, flags 0), V2 (flags 0) and V3 (random flags, plus random `Treemanifest` groups between the manifest and the files).

`cg3_bytes_match_mercurial`: a treemanifest repo made with stock `hg` (`--config experimental.treemanifest=1`, files in two directories, two commits), `hg bundle --all -t 'none-v2;cg.version=03' out.hg`, decode with `Bundle2Stream`, re-encode the changegroup part with `with_version(V3)`: payload bytes identical.

`cg1_bytes_match_mercurial`: same with a flat repo and `-t 'none-v1'` (bundle1, `HG10UN` header then the raw `01` changegroup): re-encoding the parsed parts with `V1` gives identical bytes after the 6-byte header.

`missing_version_param_means_01`: a bundle2 changegroup part without `version` decodes with the V1 unpacker.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p mercurial-bundles changegroup`
Expected: compile failure on `CgVersion`.

- [ ] **Step 3: Implement** as in Interfaces. Read `cg1unpacker`, `cg3unpacker`, `_makecg1packer`, `_makecg3packer` and `_packermap` in `changegroup.py` first.

- [ ] **Step 4: Run tests**

Run: `cargo test -p mercurial-bundles && cargo test -p mononoke --lib && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass (existing clones still use 02; the server does not offer 03 until Task 5).

- [ ] **Step 5: Commit**

```bash
git add mercurial-bundles server/src
git commit -m "bundles: pack and unpack changegroup 01 and 03 with directory manifests"
```

### Task 4: Port Mercurial's bdiff

**Files:**
- Create: `mercurial-types/src/textdiff.rs`
- Modify: `mercurial-types/src/lib.rs`

**Interfaces:**
- Produces: `pub fn textdiff(a: &[u8], b: &[u8]) -> Delta` in `mercurial_types::textdiff`, giving exactly the hunks `bdiff.bdiff(a, b)` from Mercurial's C extension gives (same starts, ends and replacement bytes, same order), so packing it yields the same bytes as `mdiff.textdiff`. Port `bdiff_splitlines`, `bdiff_diff` (with its `recurse`/`longest_match` and the hash table sizing) and the hunk emission in `cext/bdiff.c` `bdiff()`, including its handling of a common prefix and suffix. File header: GPL-2.0 notice and "Ported from Mercurial 7.0.3 mercurial/bdiff.c and mercurial/cext/bdiff.c".

- [ ] **Step 1: Write the failing tests** (in `textdiff.rs`)

`matches_mercurial_on_samples`: for each pair, write both texts to temp files and get Mercurial's bytes with `hg debugshell -c "import sys; from mercurial import mdiff; sys.stdout.buffer.write(mdiff.textdiff(open(r'A','rb').read(), open(r'B','rb').read()))"` (if `-c` is not supported, use `hg debugshell` with the script on stdin). Assert our packed delta equals them. Pairs: empty→text, text→empty, identical, one line changed, lines inserted at start/middle/end, no trailing newline on either side, CRLF lines, a 2 MB text with scattered edits, repeated identical lines (`"a\n" * 1000` vs one removed), binary bytes with no newlines.

`applies_back`: quickcheck: `delta::apply(a, textdiff(a, b)) == b`.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p mercurial-types textdiff`
Expected: compile failure.

- [ ] **Step 3: Implement** as in Interfaces, from the 7.0.3 C source (see Tech Stack).

- [ ] **Step 4: Run tests**

Run: `cargo test -p mercurial-types`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add mercurial-types
git commit -m "types: port Mercurial's bdiff for byte-identical deltas"
```

### Task 5: Serve treemanifest repositories

**Files:**
- Modify: `mercurial-types/src/path.rs:66-84`
- Modify: `mercurial/src/revlogrepo.rs:142-178,255-330`
- Modify: `server/src/bundle.rs:86-151`, `server/src/repo.rs:62-106,245-293`, `server/src/stream.rs:79-156`
- Test: `mercurial-types/src/path.rs`, `server/src/bundle.rs`, `server/tests/serve.rs`

**Interfaces:**
- Consumes: `CgVersion`, `Section::Treemanifest`, `changegroup_part_version` (Task 3).
- Produces:
  - `Path::store_dirlog(&self, dotencode: bool, ext: &[u8]) -> PathBuf`: the store path of `meta/<dir>/00manifest.<ext>`, through the same encode/hash pipeline as `store_filelog` (Mercurial's hashed form also drops the 5-byte `meta/` prefix). Share one private helper with `store_filelog`.
  - `RevlogRepo::open` accepts `treemanifest`. `RevlogRepo::is_treemanifest(&self) -> bool`. `RevlogRepo::read_dir_revlog(&self, dir: &Path) -> Result<Revlog>`. `RevlogRepo::manifest_dirs(&self) -> Result<Vec<Path>>`: every directory with a `meta/<dir>/00manifest.i` line in fncache, sorted.
  - `client_cg_versions(bundlecaps: &[Vec<u8>]) -> Vec<CgVersion>` (LFS-plan helper; here it also keeps `01`). `create_bundle` picks the highest common version; a treemanifest repository with no common `03` fails with Mercurial's `no common changegroup version`.
  - `history_part(repo, common, heads, version: CgVersion)`. For V3 on a treemanifest repository, after the root manifest group, one group per directory from `manifest_dirs()`, each with the revisions whose linkrev is a missing changeset (the same rule files use), read with `read_in_order`. The part gets mparam `treemanifest=1` when the repository is treemanifest.
  - Bundle2 caps: `changegroup=01,02,03`.
  - `list_store` takes `meta/…` fncache lines (store paths via `store_dirlog`) and walks `meta` for sizes as it does `data` and `dh`.

- [ ] **Step 1: Write the failing tests**

`store_dirlog_matches_mercurial` (path.rs): for `dir`, `Upper/Case`, and a 200-character directory name, the path equals the one `hg` created on disk under `.hg/store` (fixture made with `hg init --config experimental.treemanifest=1`).

`tree_history_sends_directory_groups` (bundle.rs): in a treemanifest fixture with `a/x`, `b/y`, two commits, collect V3 parts with empty `common`: there are `Treemanifest` groups for `a` and `b`, each revision once, and decoding the encoded part with stock `hg debugbundle --all` lists `a/` and `b/`.

`tree_repo_clones_over_every_transport` (serve.rs): treemanifest fixture; stream clone and `--pull` clone over ssh and HTTP; each clone has `treemanifest` in its requires, `hg verify` passes, and `hg files -r tip` matches the server.

`flat_repo_still_offers_02` (serve.rs): `hg clone --config experimental.changegroup3=false` from a flat repo succeeds.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p mercurial-types store_dirlog && cargo test -p mononoke --lib bundle:: && cargo test -p mononoke --test serve tree -- --test-threads=1`
Expected: failures (`treemanifest is not served`, missing functions).

- [ ] **Step 3: Implement** as in Interfaces. Read `store.py` (`_hybridencode`, `_hashencode`, fncache `meta/` handling), `changegroup.py` (`_makecg3packer`, `generatemanifests`, `supportedoutgoingversions`), `exchange.py` `_getbundlechangegrouppart` first.

- [ ] **Step 4: Run tests**

Run: `cargo test --workspace && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass. Every existing serve test now pulls with 03.

- [ ] **Step 5: Commit**

```bash
git add mercurial-types mercurial server
git commit -m "server: serve treemanifest repositories over changegroup 03"
```

### Task 6: Push into treemanifest repositories

**Files:**
- Modify: `mercurial/src/revlogrepo.rs` (append-side dir revlogs, `add_requirement`)
- Modify: `server/src/push.rs:48-99,220-292`
- Test: `mercurial/src/revlogrepo.rs`, `server/tests/serve.rs`

**Interfaces:**
- Consumes: Task 3 sections, Task 5 `store_dirlog`.
- Produces:
  - `RevlogRepo::dir_revlog_for_append(&self, dir: &Path) -> Result<Revlog>`: creates an empty `meta/<dir>/00manifest.i` and its fncache line (`meta/<dir>/00manifest.i`, `encodedir`'d) when new. `RevlogRepo::record_dir_data(&self, dir: &Path)` adds the `.d` line once split, like `record_file_data`.
  - `RevlogRepo::add_requirement` and the live requirement set, per the LFS plan's Task 2 interface (skip if already landed).
  - `push.rs`: `Section::Treemanifest` groups are appended like the others, to the directory revlog. A changegroup part with `treemanifest=1` into a flat repository that has changesets fails with `bundle contains tree manifests, but local repo is non-empty and does not use tree manifests`; into an empty flat repository it first calls `add_requirement(Required::Treemanifest)`. A flat changegroup into a treemanifest repository is stored as Mercurial stores it (flat texts in the root manifest).

- [ ] **Step 1: Write the failing tests**

`dir_revlog_for_append_records_fncache` (revlogrepo tests): after `dir_revlog_for_append("Sub/Dir")`, fncache has the `meta/Sub/Dir/00manifest.i` line and the index exists at the `store_dirlog` path.

`tree_repo_push_round_trips` (serve.rs): clone a treemanifest repo over ssh, commit changes in a new directory, a changed directory and a removed directory, push; on the server `hg verify` passes and `hg files -r tip` / `hg cat` match the client.

`tree_repo_with_encoded_dir_names_round_trips` (serve.rs): the same with directories `Upper/Case` and one 200 characters long, plus a fresh stream clone afterwards that verifies.

`tree_push_into_empty_flat_repo_adds_requirement` (serve.rs): push from a treemanifest clone into an empty flat repo served by Mononoke; its requires (store requires when share-safe) gain `treemanifest`, and `hg verify` passes.

`tree_push_into_flat_repo_is_refused` (serve.rs): pushing from a treemanifest clone into an unrelated flat repo that has history fails with the message above and leaves the server's `00changelog.i` unchanged.

`flat_client_push_into_tree_repo_matches_hg` (serve.rs): from a flat 7.0 clone of a flat repo, push into an empty treemanifest repo served by Mononoke, and do the same against `hg serve` on a second identical repo. Both servers' `hg verify` outputs and `hg debugindex -m` outputs are equal.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p mercurial revlogrepo && cargo test -p mononoke --test serve tree -- --test-threads=1`
Expected: failures.

- [ ] **Step 3: Implement** as in Interfaces. Read `bundle2.py` `handlechangegroup` and `changegroup.py` `cg3unpacker._unpackmanifests` first.

- [ ] **Step 4: Run tests**

Run: `cargo test --workspace && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add mercurial server
git commit -m "push: accept directory manifests and treemanifest pushes"
```

### Task 7: Serve changegroup 01 (changegroup, changegroupsubset, bundle1 getbundle)

**Files:**
- Modify: `hgproto/src/lib.rs:142-172`, `hgproto/src/service.rs:247-256`
- Modify: `server/src/bundle.rs`, `server/src/repo.rs:62-72,245-293,511-516`
- Modify: `server/src/main.rs:272-288`, `server/src/http.rs:126-193,566-605,657-680`
- Test: `server/src/bundle.rs`, `server/tests/serve.rs`

**Interfaces:**
- Consumes: `CgVersion::V1` (Task 3), `textdiff` (Task 4).
- Produces:
  - `HgCommands::changegroup(&self, roots) -> HgCommandRes<BundleStream>`, `changegroupsubset(&self, bases, heads) -> HgCommandRes<BundleStream>`; `Response::Changegroup(BundleStream)`, `Response::Changegroupsubset(BundleStream)`; `is_stream()` covers them. ssh streams them like `Getbundle`; HTTP sends them through the same compression negotiation as `Getbundle` (they are `streamres` in Mercurial). HTTP framing for `changegroup roots=` and `changegroupsubset bases= heads=`.
  - `fn missing_from_roots(repo, roots: &[NodeHash], heads: &[NodeHash]) -> Result<(Vec<NodeHash>, Vec<bool>)>` in `bundle.rs`: `roots::heads` (descendants of the roots, roots included, that are ancestors of the heads), as `discovery.outgoing(missingroots=…)`. `NULL_HASH` as a root means all ancestors of the heads.
  - `pub enum Selection { Common { common: Vec<NodeHash>, heads: Vec<NodeHash> }, Roots { roots: Vec<NodeHash>, heads: Vec<NodeHash> } }`; `history_part(repo, selection: Selection, version: CgVersion)` replaces Task 5's `(repo, common, heads, version)` (update its callers). `Common` uses `missing_revs`, `Roots` uses `missing_from_roots`.
  - `history_part` with `V1` produces, per group, each revision as a delta against the previous revision sent in that group (p1 of the first, as `storageutil.emitrevisions` with `CG_DELTAMODE_PREV`): the stored delta when its stored base is exactly that previous revision (`revlog.revdiff`), otherwise `textdiff(prev_text, text)`; an empty text for a null base.
  - `changegroup` returns the raw `01` stream (no bundle header) for `roots` against `heads()`; `changegroupsubset` for `bases`/`heads`. A treemanifest repository answers both with an error.
  - `getbundle` without `HG20` in `bundlecaps` returns the raw `01` changegroup, as `exchange.getbundlechunks` / `changegroup.makestream` do. It has no `HG10UN` header (verified against Mercurial 7.0.3 during Task 7).
  - `wireprotocaps` adds `changegroupsubset`.

- [x] **Step 1: Write the failing tests**

Add to `serve.rs` a helper `HgServe::start(repo) -> HgServe { port }` (runs `hg -R <repo> serve -p <port> --config web.push_ssl=false --config "web.allow-push=*"`, waits for the port, kills on drop) and `fn raw_command(port, query: &str) -> Vec<u8>` (HTTP/1.1 `GET /?<query>` with `Connection: close`, handles `Content-Length` and chunked bodies, zlib-inflates an `application/mercurial-0.1` body when the command is a stream response).

`changegroup_matches_hg_serve`: fixture with a merge, a rename and three files. For `cmd=changegroup&roots=<40 zeros>`, `cmd=changegroup&roots=<rev 2>`, `cmd=changegroupsubset&bases=<rev 1>&heads=<rev 4>`: Mononoke's bytes equal `hg serve`'s.

`bundle1_getbundle_matches_hg_serve`: `cmd=getbundle&heads=<tip>&common=<rev 1>` with no `bundlecaps`: bytes equal.

`v1_deltas_reuse_stored_prev_delta` (bundle.rs): in a fixture where revision 2's stored base is revision 1, the V1 chunk for revision 2 carries the stored delta unchanged.

`ssh_legacy_pull_works` (serve.rs): `hg pull --config devel.legacy.exchange=bundle1` over ssh into an empty repo succeeds and verifies.

- [x] **Step 2: Run them to verify they fail**

Run: `cargo test -p mononoke --lib bundle:: && cargo test -p mononoke --test serve legacy -- --test-threads=1`
Expected: failures.

- [x] **Step 3: Implement** as in Interfaces. Read `wireprotov1server.py` (`changegroup`, `changegroupsubset`, `getbundle`), `discovery.py` `outgoing`, `utils/storageutil.py` `emitrevisions`, `revlog.py` `revdiff`, `exchange.py` `getbundlechunks` first.

- [x] **Step 4: Run tests**

Run: `cargo test --workspace && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass.

- [x] **Step 5: Commit**

```bash
git add hgproto server
git commit -m "server: serve changegroup 01 to legacy clients"
```

### Task 8: Accept changegroup 01 and bundle1 pushes

**Files:**
- Modify: `hgproto/src/lib.rs`, `hgproto/src/sshproto/response.rs`
- Modify: `server/src/push.rs:48-140`, `server/src/repo.rs:62-94,626-644`, `server/src/main.rs:200-300`, `server/src/http.rs:657-680`
- Test: `server/src/push.rs`, `server/tests/serve.rs`

**Interfaces:**
- Consumes: V1 unpacker (Task 3).
- Produces:
  - `Response::Pushres { ret: i64, output: Bytes }`. ssh: two frames, the output and then `ret` as decimal (`_sshv1respondbytes(b'')`, `_sshv1respondbytes(b'%d' % res)`). HTTP: body `"{ret}\n{output}"`.
  - `push.rs`: a bundle starting `HG10UN`, `HG10GZ` (zlib) or `HG10BZ` (bzip2 with the `BZ` magic stripped, as `exchange.readbundle` re-adds it) is decoded and its `01` changegroup applied with the same checks and locking as bundle2 pushes, replying `Pushres { ret: 1, output }` on success and Mercurial's bundle1 error reply for a heads mismatch. A bundle2 changegroup part with `version=01` (or none) is applied with the V1 unpacker.
  - Caps: `unbundle=HG10GZ,HG10BZ,HG10UN` instead of plain `unbundle`.

- [x] **Step 1: Write the failing tests**

`bundle1_push_over_ssh_and_http` (serve.rs): from a clone, commit, `hg push --config devel.legacy.exchange=bundle1` over ssh and over HTTP; server `hg verify` passes and has the commit.

`bundle2_cg01_part_is_applied` (push.rs): build a bundle2 with stock `hg bundle -t 'none-v2;cg.version=01' -r tip --base <parent>` from a clone, `apply_repo` it, the revision is in the changelog and `hg verify` passes.

`hg10_compressions_decode` (push.rs): `hg bundle -t gzip-v1`, `bzip2-v1`, `none-v1` of the same revision each apply.

`unbundle_capability_lists_bundle1_types` (repo.rs tests): caps contain `unbundle=HG10GZ,HG10BZ,HG10UN`.

- [x] **Step 2: Run them to verify they fail**

Run: `cargo test -p mononoke --lib push:: repo:: && cargo test -p mononoke --test serve bundle1 -- --test-threads=1`
Expected: failures.

- [x] **Step 3: Implement** as in Interfaces. Read `wireprotov1server.py` `unbundle`, `wireprotoserver.py` (`pushres` encoding), `exchange.py` `readbundle`, `bundle2.py` `handlechangegroup` first.

- [x] **Step 4: Run tests**

Run: `cargo test --workspace && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass.

- [x] **Step 5: Commit**

```bash
git add hgproto server
git commit -m "push: accept changegroup 01 and bundle1 pushes"
```

### Task 9: Tags, named branches and `.` (lookup, branchmap, branches)

**Files:**
- Create: `server/src/names.rs`
- Modify: `server/src/repo.rs:109-160,353-414,468-485`, `server/src/main.rs` (warm-up), `hgproto/src/sshproto/response.rs:115-130`, `hgproto/src/sshproto/request.rs` (if `branches` framing needs it), `server/src/http.rs:566-605,657-680`
- Test: `server/src/names.rs`, `server/tests/serve.rs`

**Interfaces:**
- Produces, in `names.rs`:
  - `pub struct BranchCache { names: Vec<Vec<u8>>, branch_of: Vec<u32>, closed: Vec<bool> }` built from changeset extras (`branch`, missing means `default`; `close` marks closed), with `fn extend(&mut self, repo: &RevlogRepo) -> Result<()>` reading only revisions it has not seen, and `fn heads(&self, parents: &[[Option<u32>; 2]]) -> BTreeMap<Vec<u8>, Vec<u32>>` (parents per revision from the changelog index; a revision is a branch head unless a child on the same branch exists; closed heads included; ordered by revision). `fn tip(&self, name: &[u8], parents: &[[Option<u32>; 2]]) -> Option<u32>`: the highest open head, or the highest head when all are closed (`branchtip`).
  - `pub fn global_tags(repo: &RevlogRepo, heads: &[NodeHash]) -> Result<Vec<(Vec<u8>, NodeHash)>>`: `.hgtags` from every head merged as `tags.py` `_readtags`/`_updatetags` do, null nodes removing a tag, plus `.hg/localtags`.
  - `pub fn dirstate_parent(hg_dir: &Path) -> Result<NodeHash>`: p1 from `.hg/dirstate` (dirstate-v1: first 20 bytes; dirstate-v2: the docket's parents, see `dirstateutils/docket.py`), `NULL_HASH` when there is no dirstate.
  - `resolve_key` follows `scmutil.revsymbol`: `.`/`tip`/`null`, revision number, full hash, then bookmarks, tags, branches (`namespaces.py` order, branch → `tip`), then hash prefix.
  - `Repo` keeps a `BranchCache` behind a mutex: built in `warm_up` after the node map, extended before each `branchmap`/`lookup`, rebuilt when `hgrepo()` reloads.
  - `branchmap`: real branches; the response line is `quote(name) node node…`, joined by `\n` with no trailing newline (fix the current encoder). `branches(nodes)`: `localrepo.branches` (empty → tip) encoded as `wireprototypes.encodelist` lines; HTTP framing for `branches nodes=`.

- [x] **Step 1: Write the failing tests**

`branch_heads_follow_mercurial` (names.rs): fixture with branches `default`, `stable` (two heads, one closed), `feature x` (with a space): `heads()` equals `hg heads --closed -T "{branch}:{rev}\n"`.

`global_tags_follow_mercurial` (names.rs): fixture with tags added on two heads, one tag moved, one removed, and a local tag: result equals `hg tags -T "{tag}:{node}\n"` minus `tip`.

`lookup_matches_hg_serve` (serve.rs, uses Task 7's `HgServe`/`raw_command`): for keys `.`, `tip`, `null`, `0`, `-1`, a tag, a moved tag, a branch name, a closed branch, a bookmark, a 6-char prefix, an ambiguous prefix and `nosuch`: bytes equal.

`branch_names_with_spaces_are_quoted` (serve.rs): `branchmap` parsed into a map equals `hg serve`'s; `hg pull -r "feature x"` from Mononoke succeeds.

`branches_matches_hg_serve` (serve.rs): `cmd=branches&nodes=<tip> <a merge>`: bytes equal.

`clone_r_tag_and_branch` (serve.rs): `hg clone -r v1.0` and `hg clone -r stable` get only the right ancestors.

- [x] **Step 2: Run them to verify they fail**

Run: `cargo test -p mononoke --lib names:: && cargo test -p mononoke --test serve lookup branch -- --test-threads=1`
Expected: failures.

- [x] **Step 3: Implement** as in Interfaces. Read `scmutil.py` `revsymbol`, `namespaces.py`, `tags.py`, `branchmap.py`, `localrepo.py` `branches`/`branchtip`, `dirstateutils/docket.py` first.

- [x] **Step 4: Run tests**

Run: `cargo test --workspace && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass.

- [x] **Step 5: Commit**

```bash
git add hgproto server
git commit -m "server: resolve tags, named branches and . and report real branches"
```

### Task 10: stream_out, clone bundles and protocaps

**Files:**
- Modify: `hgproto/src/lib.rs`, `hgproto/src/service.rs`, `hgproto/src/sshproto/request.rs` (fix `streamout` → `stream_out`; add `protocaps caps`, `clonebundles_manifest *`, `get_cached_bundle_inline path`), `hgproto/src/sshproto/response.rs`
- Modify: `mercurial/src/revlogrepo.rs` (hgrc value)
- Modify: `server/src/stream.rs`, `server/src/repo.rs:62-106`, `server/src/main.rs:272-288`, `server/src/http.rs`
- Test: `hgproto` request tests, `server/src/stream.rs`, `server/tests/serve.rs`

**Interfaces:**
- Produces:
  - `RevlogRepo::hgrc_value(&self, section: &str, key: &str) -> Result<Option<String>>`: reads `.hg/hgrc` on every call (so edits apply without restart); `[section]` headers, `key = value`, `#`/`;` comments, continuation lines starting with whitespace. Missing file → `None`.
  - `Request::Protocaps { caps }` → `Response::Protocaps` encoded `OK`. `Request::ClonebundlesManifest` → `Response::Clonebundles(String)` with `.hg/clonebundles.manifest` as is (empty if missing). `Request::Clonebundles` drops lines starting with `peer-bundle-cache://`. Caps `clonebundles` and `clonebundles_manifest` are advertised only when the manifest file exists (`hgext/clonebundles.py`).
  - `Request::GetCachedBundleInline { path }` → `Response::GetCachedBundleInline(BundleStream)` with `prefer_uncompressed: true`: uvarint length then the file, from `<.hg>/<peer-bundle-cache-root>/<path>`; Mercurial's errors when the setting or file is missing (`clonebundle %s does not exist`) or the canonical path leaves the root (`clonebundle %s is using an illegal path`).
  - `Request::Streamout` → `Response::Streamout(BundleStream)`: stream v1 (`streamclone.generatev1wireproto`): `0\n`, `"{filecount} {bytecount}\n"`, then per file `"{name}\0{size}\n"` and its bytes, from `list_store` under the store lock. HTTP sends it uncompressed (`streamreslegacy`). Caps: `stream` when the streamed requirements are only `revlogv1`, otherwise `streamreqs=…` (as `_capabilities`).

- [ ] **Step 1: Write the failing tests**

`parses_new_commands` (hgproto request tests): `stream_out\n`, `protocaps\ncaps 5\nfoo b`, `clonebundles_manifest\n* 0\n`, `get_cached_bundle_inline\npath 3\na.b` parse to the new requests.

`stream_out_matches_hg_serve` (serve.rs): `cmd=stream_out` bytes equal on a flat repo without persistent-nodemap, and `hg clone --stream --config experimental.stream-v2=false` (check `streamclone.py` for the exact switch) clones and verifies.

`clonebundles_match_hg_serve` (serve.rs): with a manifest holding one `https://` line and one `peer-bundle-cache://b.hg` line: both commands' bytes equal `hg serve --config extensions.clonebundles=`.

`inline_clone_bundle_clones` (serve.rs): `hg bundle --all -t zstd-v2 .hg/bundles/full.hg` in the served repo, `.hg/hgrc` with `[server]\npeer-bundle-cache-root = bundles`, manifest line `peer-bundle-cache://full.hg BUNDLESPEC=zstd-v2`; `hg clone` over ssh uses it (`applying clone bundle` in output) and verifies. A manifest pointing at `../requires` is refused with the illegal-path message.

`hgrc_value_reads_sections` (revlogrepo tests): comments, continuation, missing key, missing file.

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p hgproto && cargo test -p mononoke --test serve stream_out clonebundle -- --test-threads=1`
Expected: failures.

- [ ] **Step 3: Implement** as in Interfaces. Read `wireprotov1server.py` (`stream`, `clonebundles`, `clonebundles_2`, `get_cached_bundle_inline`, `protocaps`, `_capabilities`), `streamclone.py` (`generatev1`, `generatev1wireproto`), `bundlecaches.py` `get_manifest` first.

- [ ] **Step 4: Run tests**

Run: `cargo test --workspace && cargo test -p mononoke --test serve -- --test-threads=1`
Expected: pass.

- [ ] **Step 5: Commit**

```bash
git add hgproto mercurial server
git commit -m "server: serve stream_out, clone bundles and protocaps"
```

### Task 11: Documentation

**Files:**
- Modify: `docs/hg-serve-differences.md`, `docs/running.md`

- [ ] **Step 1: Update** "Repository formats": `persistent-nodemap` and `treemanifest` accepted; the server reads but never writes nodemap files, and `hg` brings them up to date. "Refused" keeps revlogv2/changelogv2/narrow. "Wire protocol commands": every v1 command is implemented except largefiles/LFS; remove the "Not implemented or partial" items that are done. "Serving": the server now reads `[server] peer-bundle-cache-root` from `.hg/hgrc` and nothing else. `running.md`: how to set up `clonebundles.manifest` and inline clone bundles.

- [ ] **Step 2: Verify**: every command name and requirement in the docs matches the code (`grep` the capability strings in `server/src/repo.rs`).

- [ ] **Step 3: Commit**

```bash
git add docs
git commit -m "docs: describe nodemap, treemanifest and legacy command support"
```
