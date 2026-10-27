# Serve the local mozilla-central clone

## Intent

Mononoke already serves a small Mercurial repo over ssh, HTTP, and HTTPS. The repository that matters is the Mercurial 7 clone already on disk at `C:\src\mozilla-central`. Opening it stops on `Unknown requirement "share-safe"`. After that line is understood, the server has to read the changelog, manifest, and filelogs the way that clone actually stored them, and a second Mercurial 7 client on this machine has to clone the server and get the same history and a working copy.

Push into the served repo is already implemented for the small-repo format. It is not part of this work.

## Goals

- `RevlogRepo::open` on `C:\src\mozilla-central\.hg` succeeds.
- One `mononoke` process serves that repo over ssh, HTTP, and HTTPS, the same way it serves a small repo today.
- A stock Mercurial 7 `hg clone` of that server produces a repo whose tip changeset node equals the source tip, and whose `hg status` is empty.
- The changelog, manifest, and file contents behind that tip are the bytes Mercurial stored. A matching tip node is the check: the changeset hash covers its parents and its manifest node, and the manifest hash covers the file nodes.

## Non-goals

- Mononoke does not clone mozilla-central from the network. The clone at `C:\src\mozilla-central` is the source.
- No push, no new bundle parts, and no new wire capabilities.
- No revlog v2, persistent nodemap, tree manifest, sidedata, or bookmarks. This clone has none of those.
- No rewriting of the served store into another format.
- No sparse-revlog writer. Snapshot spacing stays whatever is already on disk.
- No login, accounts, or authorization.

## Anti-goals

- Accepting `share-safe` or `sparserevlog` as names and then returning the wrong full text is a failed clone.
- A clone whose `hg status` is empty while its tip node differs from the source is a failed clone.
- Three full working copies of mozilla-central are not the acceptance. One is.

## What is on disk

`C:\src\mozilla-central\.hg\requires` contains only:

```
share-safe
```

`C:\src\mozilla-central\.hg\store\requires` contains:

```
dotencode
fncache
generaldelta
revlog-compression-zstd
revlogv1
sparserevlog
store
```

There is no `.hg/bookmarks` file. `store/phaseroots` is empty.

Mercurial 7 treats `share-safe` as a working-copy marker. Its meaning is that `.hg/store/requires` exists and must be read with the outer file. It does not change revlog bytes. The code that does this is `localrepo.py` around the `SHARESAFE_REQUIREMENT` branch, and the comment in `requirements.py` on `SHARESAFE_REQUIREMENT`.

`sparserevlog` is not a new index format. `requirements.py` says a delta chain may cover a long span of the data file, with unrelated revisions in between, so a reader must take each chunk by its own offset and length instead of slurping the span. The index stays the 64-byte revlog v1 entry. When this requirement is on, Mercurial also caps a new chain at 1000 revisions (`SPARSE_REVLOG_MAX_CHAIN_LENGTH`). That cap is a writer rule. This work does not write revlogs.

Measured on this clone:

- `store/00changelog.i` is revlog v1. The feature word is `0x0000`: not inline, generaldelta off. The data file is separate (`00changelog.d`) and its chunks are zstd (`28 b5 2f fd`). Across 500 sampled revisions, and at the tip, the index base equals the revision itself. Mercurial does this on purpose. `changelog.py` clears `FLAG_GENERALDELTA` and sets `_storedeltachains = False`, so each changelog entry is a compressed full text.
- `store/00manifest.i` is revlog v1, feature word `0x0002`: generaldelta on, not inline. Sampled revisions are deltas against an earlier base, not against themselves. The tip walks 83 steps back to a snapshot (a revision whose base is itself). The tip's uncompressed manifest is about 58 MB.
- Filelogs sampled under `store/data` are revlog v1, feature word `0x0003`: generaldelta and inline. `store/dh` is the existing hashed-path directory used with `fncache` and `dotencode`.

`RevlogRepo::open` today reads only `.hg/requires`. `Required` does not include `share-safe` or `sparserevlog`, so open fails on the first line.

`Revlog::get_chunk` treats "base equals this revision" as a full text only when the generaldelta flag is set. On this changelog that flag is off, so those zstd frames are parsed as deltas and the read fails. `construct_general` already walks base pointers and reads each chunk at its own offset. That is the sparse read this manifest needs.

`history_part` sends every revision as a changegroup 02 chunk whose `base` is the null node and whose delta is one fragment `{start: 0, end: 0, content: full text}`. It also rebuilds a manifest from scratch for every changeset, once to look up file nodes and again to put that full text in the bundle. This clone has 886,060 manifest revisions and a tip manifest of about 58 MB, stored in about 970 MB of deltas. Null-base full texts of those manifests, held in one buffer, are tens of terabytes and will not finish.

## How it works

### Opening the repo

`RevlogRepo::open` reads `.hg/requires`. If a line is `share-safe`, it also reads `.hg/store/requires` and unions the two sets. A missing `store/requires` while `share-safe` is present is an error, and the message names `share-safe`.

`share-safe` and `sparserevlog` are recognized requirements. `sparserevlog` adds no header flag and does not change delta application. `dotencode`, `fncache`, `generaldelta`, `revlogv1`, `revlog-compression-zstd`, and `store` stay recognized.

Any other line still fails open. The error text stays `Unknown requirement "<line>"`. `treemanifest` still fails open, whether it appears in `.hg/requires` or in `.hg/store/requires`, and the message still contains `treemanifest is not served`.

Path lookup for filelogs stays the encoder already used for `dotencode` and `fncache`, including paths that land under `store/dh`.

### Reading revisions

A revision whose index base is itself is a full text, whether or not the generaldelta flag is set. The chunk is decompressed as a literal (zstd, zlib, lz4, or raw), not as a delta list. That is what makes this changelog readable.

A generaldelta revision whose base is an earlier revision stays a delta against that revision's full text. The reader follows base pointers to a full text and applies deltas from oldest to newest. It reads each chunk at that chunk's offset and compressed length. It does not read the bytes between two chain members. That is the whole of `sparserevlog` for this server.

The sequential non-generaldelta walker is left as it is, except for the self-base rule above. This changelog never uses a sequential chain. A repo that does is outside this clone.

### Deltas in the changegroup

Manifest and filelog revisions are changegroup 02 deltas, not a full text of every revision. Revisions of one revlog are emitted in revision order.

For each revision, the stored index base decides the chunk:

- The base is this revision, or there is no base. Send `base` as the null node and one fragment `{start: 0, end: 0}` holding the full text. Every changelog revision in this clone is this case. Changelog entries stay full texts. They are small.
- The base is an earlier revision whose node is already in this changegroup, or is in the client's `common` set. Send `base` as that node's hash. The delta is the fragment list already stored in the revlog, after decompression. Do not invent a new delta for this case.
- The stored base is not available to the client. Send a full text against the null node.

The first revision of a file has a null base. A later revision uses the stored delta. The client rebuilds the text. The tip hash still proves the result.

File-node lookup still has to read manifest text. While one `getbundle` runs, a reconstructed manifest is kept and reused when the next revision's delta base is that revision. A revision that does not delta against the kept text is rebuilt from its own snapshot. The cache lasts for that bundle build. It does not change bytes on disk. The shape of the cache is the builder's. Without the reuse, lookup walks back to a snapshot for every changeset and the clone will not finish.

The deltified changegroup may stay one buffer, as `getbundle` does today. Its size is on the order of the deltified store, which already fits on this machine. A streaming rewrite is not required.

### What does not change

SSH, HTTP, and HTTPS stay as they are: one process, Winsock socket at `<repo>/.hg/mononoke.sock`, HTTP on the configured address, HTTPS only when both a cert and a key are passed. Bundle2 compression stays uncompressed. Changegroup version stays `02`. Unknown requirements other than the two names in this document still fail open.

## Acceptance

Automated tests use a tiny repository created by the stock `hg` on this machine, with `format.use-share-safe`, `format.generaldelta`, `format.revlog-compression=zstd`, and `format.sparse-revlog` enabled, and with the same extra format flags the existing serve tests turn off so the repo stays revlog v1. The test asserts that `.hg/requires` contains `share-safe` and that `.hg/store/requires` contains `sparserevlog`, `generaldelta`, `revlogv1`, and `revlog-compression-zstd`. If this `hg` will not write that split, the builder constructs the same bytes by hand and says so in the test. The client is still stock `hg`.

That tiny repo has at least two revisions of one file, so the second file revision is stored as a delta. It is served by `mononoke`. A stock `hg clone` over ssh, HTTP, and HTTPS each gets a working copy. In each clone the tip changeset node equals the source tip and `hg status` is empty. The served changegroup uses a non-null `base` for that second file revision. Existing serve tests keep passing.

A unit test covers a changelog-shaped revlog: version 1, generaldelta off, not inline, one revision whose base is itself and whose chunk is zstd full text. Reading that revision returns the text. A unit test covers a manifest-shaped chain: generaldelta on, a later revision whose base skips an unrelated revision stored between them, and the reconstructed text matches. A unit test covers `share-safe` plus a `store/requires` that lists `sparserevlog`, and `treemanifest` inside `store/requires` still fails with `treemanifest is not served`. An unknown line in `store/requires` still fails with `Unknown requirement "<line>"`.

The manual acceptance, after those tests pass, is one clone. Serve `C:\src\mozilla-central` with `mononoke` on HTTP. Clone it with stock Mercurial 7 to a new directory. The clone's tip changeset node equals the source tip. `hg status` in the clone is empty. Do not push anywhere. Do not start a second or third working copy of mozilla-central. ssh and HTTPS are covered by the tiny repo.

## Calls

- One HTTP clone of mozilla-central is the manual proof. ssh and HTTPS are proved on the tiny repo. Three full working copies would take days and would not test a different reader.
- The tip node plus an empty `hg status` is the proof the clone is right. A matching tip hash covers the ancestor hashes and the tip manifest.
- `sparserevlog` is recognized and read with the existing per-chunk generaldelta walk. No density heuristic and no snapshot writer are added.
- A self-base is a full text even when generaldelta is off. The sequential non-generaldelta walker is not otherwise rewritten.
- Reusing the last manifest full text during one `getbundle` is required for file-node lookup. How the cache is stored is the builder's choice.
- Manifest and filelog chunks forward the stored delta when its base is already available to the client. Changelog chunks stay full texts. The deltified bundle stays one buffer. Streaming is not required.
- A missing `.hg/store/requires` next to `share-safe` is an error that names `share-safe`.
