# Issues to Fix — DTFS2

This document tracks gaps and bugs. **Status** reflects the codebase after the latest fixes (see git history). Build validation: **`cargo build` in WSL** (Linux target).

---

## Status summary

| # | Issue | Status |
|---|--------|--------|
| 1 | Register node does not update Raft | **Fixed** — `register_node` validates `shared_secret` (when set), proposes `RegisterNode`, returns stable `node_id` (`node_id_from_hostname`). |
| 2 | No metadata commit after chunk writes | **Fixed** — `CommitChunkLocations` RPC + FUSE/client flush calls `commit_chunk_locations`. |
| 3 | FUSE chunk id vs content-defined id | **Fixed** — flush uses `PutChunkResponse.chunk_id`; placeholder sent to `write_chunk`. |
| 4 | JoinHandler not wired | **Fixed** — `POST /api/v1/join/request`, `POST /api/v1/join/complete`; handler in `AppState`. |
| 5 | UpdateChunkLocations replace-all + scheduler wipe | **Fixed** — `merge_chunk_location_entries` in FSM; scheduler uses real `chunk_index` + target `grpc_addr`. |
| 6 | Placement unused; empty replicas; heartbeat/report no-ops | **Partially fixed** — `PlacementEngine` used in `allocate_chunks` with replication from `defaults.replication.hot` + fallbacks; heartbeats update usage + health; `report_chunk_status` updates node health. Further work: richer replica policies, chunk-level reconciliation. |
| 7 | get_disk_status empty | **Fixed** — populated from `ChunkStore::disk_manager()`. |
| 8 | report_stats wrong total_chunks | **Fixed** — uses `ChunkStore::stats()`. |
| 9 | get_file_locations addresses vs grpc | **Fixed** — `resolve_node_contact`; grouping by `chunk_hash` with merged `node_addresses`; commits store `http://host:port` as `node_id`. |
| 10 | TLS config unused; plain HTTP everywhere | **Open** — startup **warn** if `tls.mode` is not `disabled`/`off`; full tonic TLS not implemented. |
| 11 | ARC cache unused on data node | **Fixed** — cache wired in `get_chunk` / `put_chunk` with metrics hit/miss. |
| 12 | Rustc ICE on build (1.94.x) | **Mitigated** — `#![allow(dead_code, unused_imports)]` on `dtfs-manager` and `dtfs-data` binaries (ICE in `check_mod_deathness` / diagnostics). Remove when upstream fixes. |
| 13 | retry max_retries unbounded | **Fixed** — cap `4096` in `RetryConfig::new`. |
| 14 | Docs vs missing Cargo integration tests | **Fixed** — `docs/.../testing.md` and `tests/integration/README.md` updated (see below). |

---

## P0 — Correctness / data loss risk (original notes)

### 1. Data node registration …

**Resolved as above.** Shared secret field: `RegisterNodeRequest.shared_secret` (proto). Data node sends `config.cluster.shared_secret`.

### 2. FUSE … metadata commit

**Resolved.** Proto: `CommitChunkLocations`, `ChunkLocationCommit`.

### 3. FUSE chunk IDs …

**Resolved.**

### 4. JoinHandler …

**Resolved.** REST routes documented in this file § “Join API”.

### 5. UpdateChunkLocations merge

**Resolved.** Public `merge_chunk_location_entries` in `dtfs-raft` + unit test `merge_tests`.

---

## P1 — Cluster operations

### 6. Placement / replication / heartbeats

**Remaining:** volume-specific replication in allocate, `report_chunk_status` chunk-level Raft updates, `UpdateNodeUsage` could use `chunk_count` field on `NodeInfo` if added.

### 9. get_file_locations …

**Resolved** (see table).

---

## P2 — Data node / security

### 7–8, 11

**Resolved** (see table).

### 10. TLS

**Plan:** Centralize rustls config for tonic server + client channels; align scheduler/data-node URLs with `https://` when enabled.

---

## P3 — Tooling / docs

### 12. Rustc ICE

**Mitigation** in `crates/dtfs-manager/src/main.rs` and `crates/dtfs-data/src/main.rs`. Track rust-lang issues for `check_mod_deathness` + annotate_snippets ICE.

### 13. retry …

**Resolved.**

### 14. Documentation

**Resolved** in-repo: integration tests are **shell + Docker** under `tests/integration/`; unit tests via `cargo test -p <crate>`.

---

## Join API (REST)

- `POST /api/v1/join/request` — JSON `{ "nonce", "hmac_hex", "hostname" }` → challenge + optional CA PEM.  
- `POST /api/v1/join/complete` — JSON body with `node_id`, `grpc_addr`, `hostname`, location fields, `disks[]` → Raft `RegisterNode` via `JoinHandler::complete_join`.

---

## Build (WSL)

```bash
cd /mnt/c/path/to/dtfs2
cargo build
# optional
cargo test -p dtfs-raft merge_tests
```

---

## Suggested follow-up order

1. **TLS (#10)** — real tonic TLS.  
2. **Issue 6 remainder** — volume-scoped replication, chunk report → Raft.  
3. **Remove ICE allows (#12)** when rustc fixed.  
4. **Single raft write per heartbeat** (batch `UpdateNodeUsage` + `UpdateNodeStatus` or new combined command).

---

*Keep this file updated whenever issues are fixed or new ones are discovered.*
