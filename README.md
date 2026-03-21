# DTFS2

Distributed tiered file storage: **Raft** managers (metadata), **chunk** data nodes, **gRPC** data plane, optional **FUSE** (Linux), **REST/WebUI** on managers.

## Build (WSL recommended)

On Windows, use **WSL** so the full workspace (including `rocksdb`, `fuse3`) compiles:

```bash
cd /mnt/c/Users/you/git_github.com/dtfs2
cargo build
```

Rust **1.94.x** may ICE on some crates without lint suppression; `dtfs-manager` / `dtfs-data` binaries use crate-level `allow` as a workaround — see `ISSUES_TO_FIX.md`.

## Layout

- `crates/` — Rust workspace (`dtfs-manager`, `dtfs-data`, `dtfs-client`, `dtfs-fuse`, `dtfs-raft`, …)
- `proto/` — Protobuf / gRPC definitions
- `deploy/` — Docker Compose, sample configs, systemd units
- `tests/integration/` — Shell-based cluster tests (with Docker)
- `ISSUES_TO_FIX.md` — Known gaps and fix status

## Docs

- `docs/` — mdBook sources (including testing notes)
- `tests/integration/README.md` — how to run shell integration flow

## License

MIT (see `Cargo.toml` workspace.package).
