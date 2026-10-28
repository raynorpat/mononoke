# GraphQL browser (`hgqlserve`)

`hgqlserve` is a small, read-only GraphQL server for looking at repositories.
It is separate from the `mononoke` server, and only reads the repository files
on disk. It has no mutations and no subscriptions.

## Running it

```sh
hgqlserve [-l HOST:PORT] REPODIR [REPODIR...]
```

`-l`/`--listen` defaults to `localhost:8080`. Each `REPODIR` is mounted under
the last part of its path. A repository at `/srv/hg/myrepo` is served at:

| URL | Method | What it is |
|---|---|---|
| `/myrepo/query` | GET | GraphiQL, an in-browser query editor. |
| `/myrepo/gql` | POST | GraphQL request, JSON body `{"query": ..., "variables": ..., "operationName": ...}`. |
| `/myrepo/gql?query=...` | GET | GraphQL request in the URL (`query`, `variables`, `operationName`). |

A repository that fails to open is skipped with a message. `hgqlserve` exits
if no repository opens. The same requirements apply as for the server (see
[hg-serve-differences.md](hg-serve-differences.md#repository-formats)).

## Schema

### `Repo` (query root)

| Field | Type | Meaning |
|---|---|---|
| `required` | `[String!]!` | The repository's requirements. |
| `heads` | `[Changeset!]!` | Head changesets. |
| `changeset(id: NodeId!)` | `Changeset!` | One changeset, or an error if it does not exist. |
| `changesets` | `[Changeset!]!` | Every changeset, in revision order. There is no paging, so avoid it on large repositories. |

### `Changeset`

| Field | Type | Meaning |
|---|---|---|
| `id` | `NodeId!` | Changeset hash. |
| `manifest` | `Manifest!` | The changeset's manifest. |
| `user` | `String!` | Author. |
| `comments` | `String!` | Commit message. |
| `time` | `Time!` | Commit time. |
| `paths` | `[GQLPath!]!` | Files this changeset touched. |
| `parents` | `[Changeset!]!` | Parent changesets (none, one or two). |

### `Time`

| Field | Type | Meaning |
|---|---|---|
| `time` | `Int!` | UTC seconds since the Unix epoch. |
| `tz` | `Int!` | Timezone offset from UTC, in seconds. |

### `Manifest`

| Field | Type | Meaning |
|---|---|---|
| `id` | `NodeId!` | Manifest hash. |
| `paths` | `[GQLPath!]!` | Every path in the manifest. |
| `entries` | `[ManifestObj!]!` | Every entry, as `File` or `Symlink`. |
| `lookup(path: GQLPath!)` | `ManifestObj!` | One entry, or an error if the path is not present. |

### `ManifestObj` (interface: `File` or `Symlink`)

Both types have `path: GQLPath!` and `id: NodeId!` (the file revision hash).
Use inline fragments for the rest.

`File`:

| Field | Type | Meaning |
|---|---|---|
| `contents` | `String!` | File contents, decoded as UTF-8. Bytes that are not valid UTF-8 become U+FFFD, so binary files do not round-trip. |
| `size` | `Int!` | Size in bytes. |
| `parents` | `[File!]!` | Parent file revisions. |

`Symlink`:

| Field | Type | Meaning |
|---|---|---|
| `target` | `GQLPath!` | Where the link points. |
| `size` | `Int!` | Length of the link's target path, in bytes. |
| `parents` | `[Symlink!]!` | Parent revisions. |

Only plain files and symlinks are handled. An executable file, or any other
entry type, makes `entries` or `lookup` panic, and that stops `hgqlserve`. See
Limitations.

### Scalars

- `NodeId`: a 40-character hex SHA-1 hash.
- `GQLPath`: a repository-relative path, as a string.

## Examples

Heads, with their authors and messages:

```graphql
{
  heads {
    id
    user
    comments
    time { time tz }
  }
}
```

One file at a given changeset:

```graphql
query ($cs: NodeId!) {
  changeset(id: $cs) {
    manifest {
      lookup(path: "README.md") {
        id
        ... on File { size contents }
      }
    }
  }
}
```

With curl:

```sh
curl -s http://localhost:8080/myrepo/gql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ heads { id comments } }"}'
```

## Limitations

- Read-only. There are no mutations.
- Bookmarks, branches, tags and phases are not exposed.
- `changesets` and `Manifest.entries` load everything at once. There is no
  paging.
- Executable files are not supported yet. `GQLManifestObj::new` reaches
  `unimplemented!()` for any entry that is not a plain file or a symlink. The
  panic stops `hgqlserve`, because it handles requests on its main thread. On a
  repository with executable files, query single paths you know are not
  executable, or avoid `Manifest.entries`.
- Tree manifests are not supported.
- Integer fields are 32-bit GraphQL `Int`. A value that does not fit is an
  error.
- No authentication. Bind it to `localhost` or a trusted network.
