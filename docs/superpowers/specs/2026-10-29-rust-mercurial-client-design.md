# Rust Mercurial client and shared repository engine

Date: 2026-10-29
Status: approved by the user on 2026-10-29; implementation has not started.

## Intent and agreed scope

Build a performance-focused Rust Mercurial client alongside this Mononoke
server. The first release supports Windows and Linux, with Windows as the
primary performance target. It implements clone, pull, push, update, status,
diff, add, remove, commit, log, and merge. The user described add/remove as one
workflow; both commands are required.

Stock Mercurial and the Rust client must be able to alternate operations in
the same repository and working copy, including continuing an unfinished
merge. The user selected a shared repository engine: consolidate Mercurial
repository behavior used by the client and server instead of developing two
implementations.

The principal large-repository fixture is `C:\src\mozilla-central`. A fresh
clone from <https://hg-edge.mozilla.org/mozilla-central/> is also required.
Compatibility is not limited to repositories served by Mononoke.

This document defines the first release, not an implementation task list.
The delivery stages below are internal checkpoints; none reduces the agreed
first-release command scope.

## Current evidence

Inspected the working tree on 2026-10-29, initially based on commit `644fea9`,
with pre-existing edits in server and protocol files. During drafting, HEAD
advanced to `d1f7399` and further server edits remained in progress. Those
changes and the existing LFS design/plan are separate work and must be
preserved. Recheck their status before extracting shared code; do not assume
this snapshot is a frozen base.

- `mercurial/src/revlog/` already reads memory-mapped revlogs, reconstructs
  deltas, appends revisions, and reads persistent nodemaps.
- `mercurial/src/revlogrepo.rs` handles repository requirements, flat and tree
  manifests, filelog paths, and fncache. Recognizing a requirement is not
  proof of complete semantic support: its dirstate requirements are explicitly
  accepted without reading working-copy state.
- `mercurial-types` provides node hashes, paths, file metadata, and delta
  primitives. `mercurial-bundles` provides bundle2 and changegroup codecs.
- `server/src/repo.rs` negotiates bundle2, changegroups 01/02/03, and stream
  clone v2. `server/src/bundle.rs` and `server/src/stream.rs` contain reusable
  exchange algorithms currently coupled to the server.
- `server/src/push.rs` currently buffers changegroup revisions before
  applying them. Reusing it unchanged would not provide the bounded ingestion
  memory or crash recovery required by this client.
- `mercurial/src/lock.rs` has interoperable lock primitives, but this is not
  a complete repository transaction or working-copy recovery engine.
- `hgcli` is the SSH server relay, not a user-facing Mercurial client. Keep
  its name and purpose intact.
- Installed reference implementation: Mercurial 7.0.3. Its source is at
  `C:\Users\raynorpat\scoop\apps\mercurial\current\lib\mercurial`.
  Planning must consult its transaction, dirstate, merge, store, and exchange
  implementations as well as black-box behavior.
- The local Mozilla repository has `share-safe` in `.hg/requires`, and
  `dotencode`, `fncache`, `generaldelta`, `revlog-compression-zstd`, `revlogv1`,
  `sparserevlog`, and `store` in `.hg/store/requires`.
- Direct endpoint compatibility is unverified. A read-only `hg identify`
  probe failed with `getaddrinfo failed` in this execution environment; a
  browser fetch returned a client challenge. Neither establishes the wire
  capabilities of the Mozilla endpoint. The installed user's configuration
  also emits an unrelated missing `hggit` extension warning; automated
  comparisons must isolate configuration.

Existing user documentation predates some format changes. Code, pinned
Mercurial source, and executable interoperability tests take precedence over
the outdated support tables.

## Goals, non-goals, and failure conditions

Goals:

1. All requested commands operate natively in Rust, using ordinary Mercurial
   repository and working-copy formats.
2. Stock Mercurial 7.0.3 is the initial Windows/Linux compatibility oracle.
   Record exact versions in results; wider version claims require their own
   test matrix.
3. Both applications share storage validation, locking, transactions,
   revision construction, history selection, and exchange application.
4. Large transfers use bounded queues and streaming. Small operations avoid
   repeated whole-history reconstruction and unnecessary file-content reads.
5. Conflicts, cancellation, disk errors, and process termination never cause
   silent loss of existing user changes or publication of a partial commit.

Non-goals for release one:

- Complete `hg` CLI/extension compatibility, arbitrary revsets/templates,
  history editing, rebase, evolve, strip, shelves, subrepositories, narrow or
  sparse working copies, shared-store working copies, or remote serving from
  the new client. `sparserevlog` storage remains in scope.
- Experimental revlog/changelog formats and changegroup 04.
- Largefiles/LFS support in the client; independent server work does not
  automatically add client support.
- A mandatory daemon, filesystem watcher, virtual filesystem, or private
  replacement store. Optional later acceleration must preserve correctness
  without those facilities.
- Automatic repository conversion or a runtime Python/stock-`hg` fallback.
  Stock `hg` is allowed as a test oracle, not as the implementation of a
  requested command.

Failure conditions even if command demos work: divergent hashes or parents;
an `hg verify` failure; dirty files overwritten without an explicit destructive
choice; ignoring an unsupported semantic requirement; trusting a stale cache;
or an unbounded transfer payload retained in RAM.

## Approaches and selected architecture

Considered two approaches:

1. **Shared repository engine, selected by the user.** Extend `mercurial`
   into the authoritative repository engine and move applicable algorithms
   out of the server. Client and server benefit from the same fixes and
   performance improvements. Cost: staged refactoring with server regression
   coverage, rather than merely linking the existing storage code.
2. **Shared storage/codecs only.** Reuse revlogs and bundles but keep mutation
   and exchange algorithms in each application. This disturbs the server
   less initially but duplicates correctness-sensitive behavior and makes
   future optimization and compatibility work more expensive.

Keep a layered workspace rather than merging every crate into one file or
package:

| Layer | Responsibility |
|---|---|
| `mercurial-types` | Hashes, byte-oriented repository paths, immutable revision and delta primitives; no application policy. |
| `mercurial-bundles` | Streaming bundle2/changegroup codecs and validated transport-neutral records; depends on types, never on server/client. |
| `mercurial` | Shared repository engine: requirements, store, DAG queries, revision construction, transactions, working-copy state, and transport-neutral exchange. May depend on bundle codecs without creating a cycle. |
| `hgproto` | Wire commands, capability negotiation and request/response encoding; add the missing client-side direction. Depends on the engine where needed, never vice versa. |
| New client crate | CLI, output, configuration loading, HTTP(S)/SSH peer adapters, cancellation and progress; calls engine operations. |
| `server` and `hgcli` | Serving policy, listeners, concurrency and SSH relay. Server calls the same engine for repository operations. |

The engine must not depend on HTTP, SSH processes, CLI parsing, or server
types. Exchange takes capability descriptions and streams through interfaces,
not a server `RepoClient`. Runtime and queue choices remain implementation
details. Server-only caches and request scheduling need not move.

Working-copy code belongs in separate engine modules and is loaded only for
operations that need it. Shared mutation APIs require a transaction/lock
context so applications cannot casually append revisions or update fncache
outside the correctness boundary. Narrow low-level APIs can remain internal
for readers and tests. The new binary's public name is left to the builder;
do not rename or repurpose `hgcli`.

## Format and protocol contract

The following is the proposed release support policy, not a claim that all
of it already works. Support means validated reads, writes where applicable,
and alternating-client tests. Split store and working-copy requirements must
be evaluated separately.

| Area | First-release contract |
|---|---|
| Store | Revlog v1, inline and split files; `store`, fncache/dotencode paths, generaldelta and sparserevlog. Read supported v1 data without generaldelta too; it is necessary for existing changelogs and ordinary histories. |
| Compression | Read raw, zlib, and zstd revlog chunks. New store writes prefer zstd with appropriate requirements. Do not require every existing chunk to be zstd. |
| Layout | Existing unsplit requirements and `share-safe` split requirements; new repositories use share-safe. This does not promise `hg share` (`shared`/`relshared`). |
| Manifests | Flat manifests and the tree-manifest representation implemented here, with real bidirectional stock-`hg` fixtures. Clone preserves the negotiated representation. |
| Nodemap | Read, validate and maintain Mercurial persistent nodemaps when required; absent/stale cache data must have a correct rebuild/fallback path. Publish dockets and data safely under transaction control. |
| Working copy | Dirstate v1 and v2, tracked-key hints when present, parents, copies, branch and bookmarks, and standard Mercurial merge state. Preserve an existing format; use dirstate v2 for new working copies after interoperability coverage passes. |
| Wire | Mercurial wire protocol v1 carrying bundle2; changegroups 02 and 03, stream clone v2 when advertised and requirements are supported. HTTP, HTTPS and SSH. |
| Bundle compression | Uncompressed, zlib and zstd where the negotiated wire/bundle container allows them; implement the framing of each layer separately. |
| Unsupported | Revlog/changelog v2 experiments, changegroup 04, legacy bundle1/changegroup 01 and stream v1, LZ4-extension repositories, manifestv2, sqldirstate, largefiles/LFS, narrow/shared/sparse-working-copy requirements, unsupported mandatory parts and extension-owned semantics. |

The narrower client contract must not remove any server support. The server
may continue serving legacy protocols through shared codecs with endpoint
policy selecting what to advertise. Engine requirement profiles distinguish
server store access from client working-copy access; permissive server flags
must not silently authorize a client mutation.

Extension markers and configurations that affect required semantics must
be checked even if they are not in `requires`. Reject unsupported stores or
operations before writes, with the exact feature named. Do not silently
downgrade or delete requirements.

Remote capabilities determine the transfer strategy. Prefer stream v2 for
a full clone when the local engine supports the advertised store; otherwise
use bundle2 changegroups. A stream offer containing unsupported requirements
can be declined in favor of a supported changegroup representation. An
unsupported mandatory bundle feature aborts without publishing a clone or
transaction. Never assume a stock Mercurial peer advertises Mononoke's exact
capabilities.

Clonebundle manifests are supported as an optional acceleration path using
supported bundle2 formats and HTTPS artifact URLs. Unsupported optional
entries fall back to normal peer exchange; no legacy bundle decoder is added
solely for an optional artifact. Follow with normal discovery/pull to reach
the peer's selected heads. Streaming is not a prerequisite for public clone
correctness. Do not assume resumability; restart failed transfers unless a
validated resume mechanism is deliberately added later.

## Working-copy and command behavior

Use Mercurial semantics for hashes, parent order, file metadata, executable
and symlink flags, copies/renames, branch names, bookmarks, tags and phases.
Unknown optional data is preserved where the format permits; unknown
mandatory state prevents mutation. Normal commits are draft and peer phase
exchange honors publishing behavior. Existing server phase behavior is not
the client specification.

The minimum CLI supports repository selection, paths, revisions, message and
author selection, machine-readable output, and the normal options necessary
for the workflows below. Human output need not be byte-identical to `hg`.
Revision selection supports full hashes, unambiguous prefixes, revision
numbers, `.`, `null`, `tip`, bookmarks, tags, and branch names. Full revsets
and templating are out of scope. Centralize resolution so commands agree.

| Operation | Required behavior |
|---|---|
| Clone | Local and remote sources; empty destination or a destination created by the operation; selected revision and no-update modes; normal clone produces a usable checkout and default remote path. Stage and validate store data before publishing it. Avoid writable hardlink sharing with the source. |
| Pull | Discover missing history, ingest incrementally, exchange phases/bookmarks appropriately, and leave working files and parents unchanged unless update is explicitly requested. |
| Push | Discover outgoing history; send bounded streams, phase/bookmark updates and head checks; honor normal new-head refusal. Handle concurrent remote change without replaying a possibly committed push blindly. |
| Update | Clean and dirty working copies, backward/forward revision movement, copy/rename and flag changes. Use merge machinery for supported local modifications; preserve changes or report a conflict. Destructive clean mode must be explicit. |
| Status | Modified, added, removed, missing, unknown, ignored and clean selections; nested ignore rules, file matchers, copies and branch/parent state. Do not read file contents when validated stat information is sufficient. |
| Diff | Working copy against parent or chosen revision, revision pairs and path filters, text and binary reporting, executable/symlink changes, copies/renames and a documented patch-compatible format. |
| Add/remove | Explicit files/directories, appropriate ignore and missing-file handling, and Mercurial-compatible scheduled state changes. Ordinary remove does not discard modified files without an explicit force choice. |
| Commit | Partial path commits and full commits; deterministic revision construction for fixed metadata; normal and two-parent merge commits; reject unresolved conflicts. Persist filelogs, manifests, changelog, fncache, phases, bookmarks and dirstate coherently. |
| Log | Bounded output and pagination, revision/path filters, parents, merges, author/date/message and copies; avoid loading every full changeset to show a small page. |
| Merge | Ancestor selection including multiple merge bases, file/directory and rename conflicts, add/add, modify/delete, binary and flag conflicts, standard merge-state records and two-parent state. Preserve local edits and unresolved results. |

Merge cannot be a usable workflow without conflict management. Proposed
supporting surface: a minimal `resolve` command to list, rerun, mark and
unmark file resolutions, plus `recover` for interrupted repository
transactions. These are enabling operations for the requested workflow,
not a promise of all Mercurial auxiliary commands. Use explicit clean update
to abandon a merge, matching the documented destructive semantics; do not
invent an automatic rollback that discards user edits. Both clients must be
able to resolve and commit a merge started by the other.

Provide an internal three-way text merge and configurable external merge
tools. Ambiguous binary or copy conflicts stay unresolved instead of silently
choosing a side. Preserve copy metadata written by stock `hg`, even though
dedicated copy/rename commands are not required for release one.

Support Mercurial config precedence for relevant system, user and repository
settings, includes, default paths, identities, ignore rules, supported
format settings, TLS and merge tools, with documented Windows/Linux locations.
Parse repository names/contents as Mercurial bytes rather than assuming all
paths or commit messages are UTF-8. Do not implement extension loading or
arbitrary hooks. Refuse affected operations when a configured hook or known
extension would supply semantics the client cannot honor; unrelated UI
settings may be ignored with a documented policy. No silent Python fallback.

HTTP(S) uses certificate validation, proxies, bounded redirects and basic
authentication when configured; never forward credentials to a different
origin on redirect. SSH uses the platform SSH executable/agent with robust
argument handling and supports a configured remote command for Mononoke's
relay or stock `hg serve --stdio`. Do not require an authentication plugin
framework in the initial release.

## Consistency, failure, and recovery

1. Use Mercurial-compatible locks and lock ordering. Detect outside changes
   before trusting cached repository or working-copy state, then revalidate
   after taking write locks. A memory map is not a synchronization guarantee;
   tolerate truncation/replacement and Windows sharing rules deliberately.
2. Implement a shared append transaction with Mercurial-compatible journals
   and backup records. Record recovery information before modifying a file;
   cover appended files, newly created revlogs, fncache, requirements and
   replaced metadata. A complete operation publishes consistent roots and
   working-copy metadata in the correct order. Reference `transaction.py`,
   `localrepo.py` and `vfs.py`; merely serializing writes is insufficient.
3. Standard journals must permit stock `hg recover` after a Rust transaction
   fails, and Rust recovery must handle the supported stock journal format.
   Restore or invalidate derived caches after abort. Do not promise `rollback`
   history undo, which is distinct from recovering an incomplete transaction.
4. Working-tree updates cannot be made atomically across all files. Preflight
   conflicts and paths, write files through safe temporary replacements where
   possible, and use standard dirstate/merge state and interrupted-update
   markers understood by the reference Mercurial (`.hg/updatestate` is used
   by the inspected `merge.py`). Failure must remain
   detectable by both clients; never mark partially written content clean.
   The implementation plan must pin the exact reference recovery behavior
   and fixtures before selecting the write order. A private marker that stock
   `hg` ignores is not an acceptable correctness mechanism.
5. Clone staging lives on the destination filesystem. A failed clone cannot
   masquerade as a valid repository; cleanup only touches paths the operation
   created. Checkout failure after a valid store is published is reported as
   a checkout failure with a recoverable repository, not successful clone.
6. Validate revision hashes, lengths, delta bounds, dependencies and paths
   before publication. Reject traversal, `.hg` aliasing, case collisions,
   reserved Windows names and unrepresentable working paths. A no-update
   clone may keep a valid history that cannot be checked out on that platform.
   Use stock Windows symlink/execute-bit behavior under the same filesystem
   capabilities, rather than changing stored revision identities.
7. Cancellation and errors release resources and abort transactions. Process
   termination and disk-full tests prove recovery; power-loss durability
   additionally requires flush/rename ordering consistent with the platform.
   Do not claim stronger filesystem guarantees than the OS provides.

## Performance strategy and measurements

Optimize verified bottlenecks. Retain the streaming store path, memory-mapped
indexes and nodemap lookup where safe; add bounded parallelism for directory
enumeration, independent decompression, hashing and file writes. Keep revision
dependency ordering and transaction publication serialized where necessary.

Status uses dirstate, directory enumeration and conservative stat checks.
Coarse or ambiguous timestamps, rapid same-size edits and clock changes must
fall back to content checks. A fresh process must have a fast path without
relying on a watcher or resident process. Windows path APIs, antivirus effects,
sharing modes and NTFS behavior are first-class benchmark variables.

Use byte budgets as well as item counts on pipelines. Initial engineering
budget: at most 256 MiB of queued transfer payload, excluding separately
measured indexes, current revision reconstruction, parser state and metadata.
Oversized revisions must spill or follow a bounded special path rather than
defeat the budget. Bound caches and document metadata that scales with file
or revision counts; do not claim total RSS is constant. Avoid collecting a
whole bundle, all file contents, or all reconstructed changesets in memory.

Use deltas already suitable for a recipient and bounded delta-base search for
new commits. Record storage growth and chain depth as well as speed, so a
faster commit does not produce pathological future pulls or updates.

Baseline against stock Mercurial 7.0.3 with equivalent options, extensions
disabled unless explicitly under test, identical source tip, destination
volume and transport. Record CPU, RAM, OS, filesystem, storage, antivirus,
tool versions and repository revision/file counts. Separate truly cold runs
from warm filesystem-cache runs and fresh-process startup. If cold caches
cannot be controlled, label the run rather than presenting it as cold.

Required Windows scenarios:

- Clone from locally served Mozilla: transfer without checkout, checkout
  alone, and end-to-end; stream v2 versus changegroup when available.
- Public Mozilla clone and subsequent pull: interoperability and observed
  transfer rates, not a deterministic speed gate against a changing WAN.
- Clean status, one modified tracked file, many modifications and untracked
  directory trees; working diff and a bounded recent log.
- Nearby update, distant update, a small local commit, incremental pull and
  push, clean merge and conflicted merge on a scratch derivative.

Measure wall time, CPU time, peak working set/RSS, available process I/O
counters, network bytes, cache/index costs and repository size growth. Use
five measured warm samples after a warm-up for short commands; use at least
three for expensive local transfer/update cases, reusing scratch storage
safely rather than accumulating full clones. Report medians and spread.

Proposed targets, to be approved with this design: aim for at least 2x stock
Mercurial's warm clean-status speed on the Windows Mozilla checkout. Require
no repeatable regression above 10% on the other controlled Windows scenarios,
after accounting for measured variance; a miss requires investigation and
an explicit scope/performance decision, not removal of the benchmark. The
2x target is aspirational until the baseline exists, not a claimed result or
permission to weaken correctness. Linux runs the same functional matrix and
a representative performance subset; Windows is the tuning priority.

## Verification and release acceptance

Use tiny deterministic fixtures for every command and edge case on both
platforms before expensive Mozilla runs. Compare state, hashes, contents,
parents, copies, flags, phases, bookmarks and conflict records, not merely
exit status or similar terminal output. Run stock `hg verify` after mutations
and exchanges. Tests use isolated configuration, deterministic identity/date
where needed, and a pinned reference version on each platform.

Mandatory interoperability sequences include:

- Stock add/commit -> Rust status/log/diff/update; then Rust add/remove/commit
  -> stock status/log/diff/update/verify, in the same checkout.
- Rust clone and pull from Mononoke and stock HTTP(S)/SSH servers; stock
  clone/pull from repositories written by Rust; pushes in both directions.
- Begin a conflict in Rust, resolve and commit with stock `hg`, and reverse
  the sequence. Include interrupted updates, partial commits, branch and
  bookmark movement, copies, symlinks, executable flags and non-UTF-8 paths
  where the platform permits them.
- Crash each journal/publication boundary; recover with the opposite client.
  Test disk-full, cancellation, malformed bundles, interrupted streams,
  stale nodemaps, stock writes between Rust invocations and concurrent writers.
- Test every supported requirement combination with generated targeted
  fixtures, and rejection without mutation for unsupported requirements,
  compression, mandatory parts and extension semantics. Fuzz parsers and
  delta application with bounded allocation.

Large acceptance uses the existing Mozilla source read-only. Serve that
source or an isolated derivative; all commits, merge experiments and pushes
target scratch repositories. Never push to Mozilla's public service or mutate
the user's source just to obtain benchmark samples. Avoid keeping multiple
full-size scratch clones unnecessarily; measure required space before runs.

For the public endpoint, capture capabilities, choose a supported exchange
path, clone a recorded revision, verify history and checkout with stock `hg`,
then pull again. If the endpoint changes during the test, compare the selected
head set rather than an assumed moving tip. If its mandatory requirements
exceed this contract, report the exact gap and revise the design before
implementation; do not silently drop public-clone acceptance or implement
legacy formats without a scope decision. DNS/authentication/service outages
remain environmental blockers, not successful compatibility tests.

## Delivery order and decisions left to the builder

Implement in coherent stages after an approved implementation plan:

1. Pin compatibility fixtures and baselines; inventory the public endpoint
   and inspect the exact stock working-copy/recovery formats.
2. Establish shared engine boundaries, safe transactions and invalidation;
   migrate server repository operations with regression tests.
3. Add client transport negotiation and streaming clone/pull/push, including
   stock-server and public-endpoint integration.
4. Add working-copy readers, status/diff/log and safe checkout/update.
5. Add add/remove/commit, state publication and cross-client recovery.
6. Complete merge/resolve and cross-client unfinished-operation tests.
7. Profile and tune Windows, run Linux coverage, and execute full Mozilla
   acceptance. Update user-facing support documentation to actual results.

The builder chooses module/API names, crate feature gating, worker counts,
buffer tuning, parser and HTTP libraries, output schemas, and packaging.
These choices must respect bounded memory, standard files, both operating
systems and the dependency direction above. Packaging must state that system
SSH and user-selected external editors/merge tools are external programs;
core commands require neither Python nor stock Mercurial at runtime.

## Author proposals requiring design approval

The user explicitly selected the command set, both operating systems,
Windows performance priority, local/public Mozilla acceptance, shared working
copy, and shared-engine approach. The following are design-author proposals:

- Native Rust core without a Python fallback; Mercurial 7.0.3 as the initial
  pinned compatibility oracle, with a deliberately bounded CLI/format set.
- Keep the existing type and bundle crates under a shared `mercurial` engine,
  rather than consolidating every Mercurial-related crate into one package.
- Dirstate v1/v2 support, v2 for new checkouts, tree manifests, and safe
  persistent-nodemap maintenance; exclude extensions and experimental formats.
- Minimal resolve/recover operations to complete merge and recovery workflows.
- No daemon/watch service in release one; 256 MiB queued-payload budget,
  proposed Windows performance targets and benchmark methodology above.

The user approved this design and requested implementation planning on
2026-10-29. The next artifact is a task-level implementation plan using the
writing-plans workflow; the current request is to write that plan, not to
start product code or dependency changes.

## Design review

A fresh builder reviewed this document on 2026-10-29 for questions that would
prevent implementation planning and reported "nothing important left".
This is a design-completeness review, not verification of implemented behavior
or approval of the author's proposed scope and performance choices.
