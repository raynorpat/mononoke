# Modern repository formats and the remaining wire commands

## Intent

Mononoke should serve repositories made by a stock, modern Mercurial without
anyone changing their format first, and answer every wire protocol command
`hg serve` answers. Today a fresh `hg init` from Mercurial 7.0 is refused
("Unknown requirement \"persistent-nodemap\""), treemanifest repositories
are refused, and several commands return nothing or "unimplemented" (see
"Not implemented or partial" in `docs/hg-serve-differences.md`).

There is no single failing user behind this; the goal is to support modern
Mercurial by default, and to cut startup time on large repositories by using
the persistent nodemap instead of building one.

The behaviour to match is Mercurial's own Python source. Version 7.0.3 is
installed with scoop at `~/scoop/apps/mercurial/current/lib/mercurial/`;
read the files named below before building each part. Tested client:
Mercurial 7.0.3.

The work is two parts, done in this order:

1. Formats: `persistent-nodemap`, then `treemanifest`.
2. Wire commands: `lookup`, `branchmap`, `branches`, `changegroup`,
   `changegroupsubset`, `stream_out`, `clonebundles`,
   `clonebundles_manifest`, `get_cached_bundle_inline`, `protocaps`.

## Goals

1. A repository created by `hg init` with Mercurial 7.0 defaults (which
   include `persistent-nodemap`) and one created with
   `--config experimental.treemanifest=1` can each be cloned (stream and
   `--pull`), pulled from and pushed to through Mononoke over ssh and HTTP,
   and `hg verify` passes on both the server repository and the clone
   afterwards.
2. With a persistent nodemap present, server startup loads it instead of
   building the changelog node map from the index.
3. `hg clone -r <tag>`, `hg pull -r <named branch>` and `hg clone -r .`
   work. `hg branches` on a clone lists the same branch heads as the
   server repository.
4. Each legacy command's response matches `hg serve`'s response for the same
   repository, byte for byte where the format is deterministic.
5. `docs/hg-serve-differences.md` is updated: the formats move to the
   accepted list and the "Not implemented or partial" commands section
   shrinks to what is still out of scope.

## Non-goals

- `revlogv2` (`exp-revlogv2.2`), `changelogv2` (`exp-changelog-v2`) and
  changegroup `04`. Mercurial still marks these experimental and their
  on-disk layout can change; they stay refused as "Unknown requirement".
- Narrow repositories, largefiles, LFS (has its own design,
  `2026-10-28-lfs-design.md`).
- `debugwireargs` (a test-only command).
- Searching for better delta bases, or rewriting the persistent nodemap on
  push (see below).

## Anti-goals

- Writing anything that leaves a repository `hg` cannot read or that fails
  `hg verify`. The server shares the repository with `hg` (see "Writing to a
  served repository" in `docs/hg-serve-differences.md`).
- A treemanifest repository that clones but cannot be pushed to.
- Making large-repository startup or clone slower than today.

## Part 1a: persistent-nodemap

Source to read: `mercurial/revlogutils/nodemap.py`,
`revlog.py` `_register_nodemap_info`, `store.py` (volatile files),
`requirements.py` (`STREAM_IGNORABLE_REQUIREMENTS`).

On disk, per revlog (`00changelog`, `00manifest`; never for an inline
revlog):

- `<radix>.n`, the docket: `>B` version (must be 1), then `>BQQQQ`
  (uid length, tip rev, data length, unused bytes, tip node length), then
  the uid and the tip node. A different version means "no nodemap".
- `<radix>-<uid>.nd`, the trie: blocks of 16 big-endian **32-bit** signed
  entries (64 bytes; `S_BLOCK = ">" + "l"*16`, despite the comment saying
  64-bit). Entry −1 is empty, ≥0 is a child block index, ≤−2 is revision
  `-(v+2)`. The root is the **last** block. Read only the first
  `data length` bytes. Each level consumes one hex digit of the node.
- The trie holds the shortest unique prefix, so a hit must be checked
  against the full node in the index.

Behaviour:

- Accept `persistent-nodemap` in `requires` / `store/requires`.
- When loading a revlog's node map: if the docket exists, the data file is
  long enough, `tip_rev` is within the index and the node at `tip_rev`
  equals `tip_node`, use the trie for revisions `0..=tip_rev` and index the
  revisions after `tip_rev` the way the server does today. Otherwise fall
  back to today's full build. This is the same validity rule Mercurial uses.
- **On push the server does not touch `.n` / `.nd`.** Mercurial accepts a
  docket whose tip is behind the index (it indexes the newer revisions
  itself) and brings it up to date on its next write. Rewriting the trie is
  risk for no read-side gain.
- When the changelog is reloaded because `hg` changed it, the nodemap is
  re-read too.
- Stream clones include the `.n` and `.nd` files, as Mercurial's store walk
  does. The requirement is stream-ignorable: each client keeps or drops it
  by its own config, so it is not added to the stream requirements sent to
  the client.

## Part 1b: treemanifest

Source to read: `changegroup.py` (`cg3unpacker`, `_makecg3packer`,
`allsupportedversions`, `supportedoutgoingversions`, the tree walk in the
packer), `manifest.py` (`treemanifest.parse`, `dirtext`, `_addtree`,
`dirlog`), `bundle2.py` `handlechangegroup` (the `treemanifest` part
parameter), `store.py` / fncache for `meta/` paths.

Format facts:

- Directory manifests live at `meta/<dir>/00manifest.i` (`.d`), the root
  at `00manifest.i`. In a manifest's text, a subdirectory is a line
  `name\0<hex node>t` (no trailing slash in the text), pointing to that
  directory's manifest node in `meta/<path>/`.
- File paths are found by walking the tree from the root manifest.

Changegroup `03`:

- Delta header `>20s20s20s20s20sH` (node, p1, p2, base, linknode, flags).
  After the root manifest group come directory groups: a header chunk
  naming the directory (e.g. `dir/`), its deltas, a close chunk. One extra
  empty chunk ends the manifest section before the files.
- Mercurial 7 has `experimental.changegroup3` on by default, so a stock
  `hg serve` offers `03` for every repository. Mononoke does the same.
  The bundle2 `changegroup` capability lists the versions the repository
  can *receive* (`bundle2.getrepocaps` uses `supportedincomingversions`):
  Mercurial lists `01,02,03` for both kinds, and Mononoke does the same,
  so it accepts `01` pushes (see Part 2).
  `getbundle` picks from the versions it can *send*
  (`supportedoutgoingversions`), which for a treemanifest repository is
  only `03`.
- The LFS plan (`docs/superpowers/plans/2026-10-28-lfs.md`, Tasks 3 and 6)
  already specifies changegroup `03` in `mercurial-bundles`
  (`CgVersion { V2, V3 }`, `CgDeltaChunk.flags`, `with_version`). Use that
  same interface. Whichever project lands first builds it; the other reuses
  it. This design adds the directory-manifest groups on top.

Serving (`getbundle`):

- For every manifest revision sent, also send the directory manifest
  revisions the client lacks, found by walking `t` entries, in the order
  Mercurial's packer uses. Files to send are found by walking the trees.
- Changegroup parts from a treemanifest repository carry the part
  parameter `treemanifest=1`, so a cloning client adds the requirement.

Pushes (`unbundle`):

- Accept `03` with directory groups and write each into
  `meta/<dir>/00manifest.i` with the existing delta-or-snapshot rule,
  adding fncache entries for new directory revlogs.
- A part with `treemanifest=1` sent to a flat repository with history is
  refused with Mercurial's message ("bundle contains tree manifests, but
  local repo is non-empty and does not use tree manifests"). Into an empty
  flat repository, match what Mercurial does (it adds the requirement).
- A flat changegroup (no directory groups, no `treemanifest=1`) pushed into
  a treemanifest repository is stored as Mercurial stores it: flat texts in
  the root `00manifest`. Mercurial allows a repository to hold both
  (`allsupportedversions` comment), so the getbundle walk and file
  discovery must handle a root manifest with `dir/file` lines and no
  `meta/` revlog. A test pushes from a flat 7.0 client into a tree
  repository and compares `hg verify` on the result with what `hg serve`
  produces for the same push. (Matching Mercurial here was the author's
  call, made while writing this up.)
- Both flat and tree pushes are checked and applied as one unit, as today.

Stream clones already copy the whole store, so `meta/` comes along. A test
confirms it.

## Part 2: wire commands

Source to read: `wireprotov1server.py`, `scmutil.py` `revsymbol`,
`localrepo.py` `lookup`, `tags.py`, `branchmap.py`, `bundlecaches.py`,
`streamclone.py` `generatev1`.

- **`lookup`**: add tags, named branches and `.`, in `revsymbol`'s order.
  Tags: `.hgtags` on every head, merged as `tags.py` does, plus
  `.hg/localtags`. `.` is the first parent recorded in `.hg/dirstate`
  (dirstate-v1 and dirstate-v2 docket). A failed lookup returns
  Mercurial's error text with success `0`, as today.
- **`branchmap`**: real named branches with all heads, closed ones
  included, branch names URL-quoted. Branch names come from the
  changeset's `branch` extra (absent means `default`).
- **`branches`**: the legacy linear-segment walk (`localrepo.branches`).
- **`changegroup roots`** and **`changegroupsubset bases heads`**:
  uncompressed changegroup `01`. `01` has no generaldelta, so each delta is
  against the previous revision sent. The output must be byte-identical to
  `hg serve`'s, so port Mercurial's diff routine to Rust: the C `bdiff.c`
  (`bdiff_splitlines`, `bdiff_diff`, and the `bdiff` patch encoding in
  `mercurial/cext/bdiff.c`), which is what `hg serve` runs. The
  `mercurial/pure/bdiff.py` fallback uses `difflib` and gives different
  output, so it is not the reference. The C source is not in the scoop
  install; take it from the 7.0.3 release source. Both projects are
  GPL-2.0. Reuse a stored delta only where Mercurial does (stored base is
  the previous revision sent). Tests compare the port's output with
  `mdiff.textdiff` from the installed `hg` on generated and real texts. Not offered for treemanifest
  repositories (Mercurial cannot make `01` from them); return an error
  there. Advertise `changegroupsubset` as Mercurial does.
- **Receiving `01` pushes**: `unbundle` accepts a bundle2 changegroup part
  with `version=01` (and a part with no `version`, which means `01`, as
  `handlechangegroup` defaults). The `01` delta header is
  `>20s20s20s20s` (node, p1, p2, linknode). The delta base is the previous
  node in the same group, or p1 for the first one (`cg1unpacker._deltaheader`).
  Stored with the existing delta-or-snapshot rule, checked and applied as
  one unit like `02`/`03` pushes.
- **Pre-bundle2 pushes**: old clients send `unbundle` with a bundle1
  stream (`HG10UN`, `HG10GZ`, `HG10BZ`: a 6-byte header, then a `01`
  changegroup, compressed as named). Accept these too, and advertise
  `unbundle=HG10GZ,HG10BZ,HG10UN` as `wireprotov1server._capabilities`
  does (`bundle2.bundlepriority`). The reply is bundle1's plain integer
  result, not a bundle2 reply (`wireprotov1server.unbundle`). For a
  treemanifest repository, do what `hg serve` does with a bundle1 push
  (accept or refuse; check `server.bundle1*` defaults in
  `configitems.toml`). Tests push with stock hg forced to the old path
  (`--config devel.legacy.exchange=bundle1`) and with a bundle2 `01` part,
  then check `hg verify`.
- **`stream_out`**: legacy stream v1 from the existing stream-clone file
  listing and store lock. Advertise `stream` when the only streamed format
  is `revlogv1`, otherwise `streamreqs=<sorted list>`, as
  `_capabilities` does.
- **`clonebundles`** and **`clonebundles_manifest`**: return
  `.hg/clonebundles.manifest` (empty if missing). `clonebundles` drops
  lines starting with `peer-bundle-cache://`.
- **`get_cached_bundle_inline path`**: stream the file under the
  configured bundle root, prefixed with its length as a uvarint, after
  checking that the resolved path stays below the root. Without the
  setting, fail as Mercurial does.
- **`protocaps caps`**: answer `OK`.

### Reading `hgrc`

The server starts reading the repository's `.hg/hgrc`, for the settings
these commands need: `server.peer-bundle-cache-root`. The file is re-read
when it changes, like the changelog check before each request.

## Constraints from the existing code

- Requirements are parsed in `mercurial/src/revlogrepo.rs`
  (`Required`, `RevlogRepo::open`, which currently refuses
  `treemanifest`).
- The node map is built by `Revlog::load_node_map`
  (`mercurial/src/revlog/mod.rs`), called in the background from
  `server/src/repo.rs`.
- Commands are the `HgCommands` trait in `hgproto/src/service.rs`, served by
  `server/src/repo.rs`. The ssh parser already knows every command name in
  `hgproto/src/sshproto/request.rs`.
- Pushes go through `server/src/push.rs`, changegroups through
  `mercurial-bundles` and `server/src/bundle.rs`, stream clones through
  `server/src/stream.rs`.
- Existing tests build repositories with
  `--config format.use-persistent-nodemap=false`. Leave them; add tests
  that create repos with the 7.0 defaults.

## Left to the builder

- Module layout, type names, and how the trie is read (mmap or read).
- How branch names and tags are cached in memory between requests, as long
  as large-repository startup is no slower than today and changes made by
  `hg` are picked up by the existing reload check.
- Exact test fixtures, as long as each goal above has an end-to-end test
  with stock `hg`.
