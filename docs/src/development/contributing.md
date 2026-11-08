# Contributing

The binding rules for working on DTFS are in `CLAUDE.md` at the repository root (Part A: system knowledge and conventions that must not break; Part B: how a change lands). This page is a short orientation and does not repeat them.

## Start here

1. Build and test with the Docker image: [Building from Source](./building.md), [Testing](./testing.md).
2. Read the [Architecture Overview](../architecture/overview.md), in particular the safety invariants.
3. For history behind an invariant, or before touching a byte-touching path, read [Invariants and Bug Archaeology](../internals/invariants.md) and `.claude/rules/integrity.md`.

## How a change lands

- Reproduce or locate, write a failing test at the purest layer, make the smallest fix, run the crate tests in Docker, run the whole workspace for cross-crate changes.
- Commit to `master`. Use a branch only for wire or on-disk format changes (`proto/*.proto`, `ClientRequest`, `apply_request`, `#[serde]`-persisted structs) or multi-session work.
- Open a PR only when a release-note line is wanted.
- Do not force-push or amend a pushed commit, skip hooks, or bypass signing.

## Commit messages

```text
<type>: <summary, at most 72 chars>

What changed and why, in a few lines. Name the covering test only
when it is not obvious from the diff.
```

`<type>` is `bug`, `feat`, `int` or `docs`; it becomes the release-note line via git-cliff. `Breaking:` and `Update-required:` trailers route to their own release-note sections. Group related edits in one commit.

## Rules of thumb

- A behavior change visible to an operator, agent or the data plane updates `CLAUDE.md` in the same commit.
- Changes to `dtfs-common/src/config.rs` update the examples and the configuration docs together.
- Memory: never accumulate a full chunk in a `Vec` on a hot path; stream and hash incrementally.
- Integrity: BLAKE3 end to end, tmp plus `sync_all` plus atomic rename, WAL before the operation, never delete a chunk until its replacement is verified. Metadata mutations go through Raft; `apply` stays deterministic.

## Where things live

| What | Where |
|------|-------|
| Raft state machine, log store, transport | `crates/dtfs-raft/src/` |
| Metadata gRPC, placement, scheduler, tiering, REST, WebUI | `crates/dtfs-manager/src/` |
| Chunk store, scrub, replication | `crates/dtfs-data/src/` |
| FUSE filesystem, read-ahead, write buffer | `crates/dtfs-fuse/src/` |
| Shared config, hash ring, crypto, memory budget | `crates/dtfs-common/src/` |
| Protobuf | `proto/` |
| Examples | `deploy/examples/*.toml` |
| Docs | `docs/src/` (detailed history in `docs/src/internals/invariants.md`) |

## Adding an RPC, endpoint or command

- **gRPC:** define it in `proto/*.proto`, implement the server side in manager or data node, add client support in `dtfs-client`, update the [gRPC API reference](../api-reference/grpc-api.md). A mutating RPC must be HMAC-stamped when `shared_secret` is set, and a wire change goes on a branch.
- **REST:** add the route and handler under `crates/dtfs-manager/src/api/` (by resource), keep it behind the shared-secret mutation gate, audit it, mirror it in `dtfs-cli` and the WebUI (every WebUI mutation goes through `mutateFetch`), and update the [REST API reference](../api-reference/rest-api.md).
- **CLI:** add a variant in `crates/dtfs-cli/src/main.rs`; a mutation prints the server's error body and exits non-zero. Update the [CLI reference](../api-reference/cli-reference.md).
