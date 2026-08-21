# PLAN-LIS-19 — Authenticate data-plane PutChunk / DeleteChunk

## Goals
- Fail-closed HMAC auth on data-plane `PutChunk` and `DeleteChunk` when `cluster.shared_secret` is set.
- Legitimate callers (FUSE put, data→data replication put, manager delete/flush) stamp auth.
- Pure-layer tests + release case for unauthorized DeleteChunk rejection.

## Non-goals
- mTLS (CF-6) — orthogonal; HMAC is the interim R2 close.
- Auth on GetChunk / VerifyChunk / ListChunks (read path; not R2).
- Manager-confirmed unreferenced check on DeleteChunk (optional R7; residual TOCTOU).

## Locked decisions
- Headers: `x-dtfs-rpc-ts` (unix secs) + `x-dtfs-rpc-mac` (hex HMAC-SHA256).
- MAC message: `dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n` where method is `PutChunk`|`DeleteChunk`, binding is chunk_id for delete and empty for put.
- Max skew 300s. Empty shared_secret → skip (join/forward back-compat).
- PermissionDenied on missing/invalid auth when secret configured.

## Tasks
| ID | Status | Done when |
|----|--------|-----------|
| LIS-19-1 | ✅ | crypto helpers + unit tests in `dtfs-common` |
| LIS-19-2 | ✅ | `dtfs-data` server rejects unauthenticated Put/Delete; unit tests |
| LIS-19-3 | ✅ | Callers stamp: client put, replication put, manager delete/scheduler |
| LIS-19-4 | ✅ | Release case `60-data-rpc-auth-delete.sh` + AGENTS.md note |

## Verification log
- 2026-08-21 WSL: `cargo test -p dtfs-common --lib crypto::tests --release` → 10 passed (incl. data_rpc_mac_*)
- 2026-08-21 WSL: `cargo test -p dtfs-data --release -- delete_chunk_rejects_missing_auth` → ok
- 2026-08-21 WSL: `cargo test -p dtfs-data --release -- delete_chunk_accepts_valid_auth` → ok
- 2026-08-21 Sec review ([LIS-33](/LIS/issues/LIS-33)): APPROVE
- Builder SSH (10.20.0.150) unreachable this heartbeat; live release case not run.
- 2026-08-21: rebased onto origin/master (CF-6/CO-1-1 DeleteChunk shape) for PR.

## Deviations
- Dropped PutChunk empty-stream unit test (rustc 1.94 ICE on that shape); PutChunk gate shares `require_data_rpc_auth` with DeleteChunk (covered) + crypto tests.
- Optional manager-confirmed unreferenced DeleteChunk (R7) deferred.
- Release case stays out of QUICK_CASES (needs live cluster + grpcurl).

## Cross-cutting
- Do not weaken RF / delete-safety (Bug CI fanout still runs on manager).
- Coordinated deploy: data + manager + fuse together (update-required).
