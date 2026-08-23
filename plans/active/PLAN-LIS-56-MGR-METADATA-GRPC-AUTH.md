# PLAN-LIS-56 — Auth-gate mutating manager metadata gRPC

## Goals
- Fail-closed HMAC auth on mutating ManagerService RPCs when `cluster.shared_secret` is set.
- Constant-time compares in `rest_auth::check_mutation_auth` and `ClusterConfig::accepts_shared_secret`.
- Legitimate callers (dtfs-client / FUSE) stamp auth; unit + release case + docs.

## Non-goals
- mTLS require-mode default (option B) — deferred; HMAC is the R1/R2-style close.
- Auth on read-only ManagerService RPCs / Heartbeat / RegisterNode body path.
- Changing ForwardClientWrite (already CO-2-2).

## Locked decisions
- Prefer fix A: `dtfs-mgr-rpc` MAC plane, same `x-dtfs-rpc-ts`/`x-dtfs-rpc-mac` headers + 300s skew as LIS-19.
- Binding = primary path (or `from\nto` / `source\ndest` / hostname for ReleaseLeasesByHost).
- Empty secret → legacy skip.

## Tasks
| ID | Status | Done when |
|----|--------|-----------|
| LIS-56-1 | ✅ | crypto mgr helpers + constant_time_eq fixes |
| LIS-56-2 | ✅ | metadata `require_mgr_rpc_auth` on mutating RPCs + unit tests |
| LIS-56-3 | ✅ | dtfs-client stamps mutating manager calls |
| LIS-56-4 | ✅ | release case `81-mgr-metadata-grpc-auth.sh` + docs/AGENTS |

## Verification log
- 2026-08-21 WSL: `cargo test -p dtfs-common --lib crypto::tests --release` → 15 passed (incl. mgr_rpc_mac_*)
- 2026-08-21 WSL: `cargo test -p dtfs-manager --release lis56_` → 4 passed
- 2026-08-21 WSL: `cargo test -p dtfs-manager --release rest_auth::` → 7 passed
- 2026-08-21 WSL: `cargo test -p dtfs-common --release lis20_` → 3 passed
- 2026-08-21 WSL: `cargo check -p dtfs-client --release` → ok
- Builder SSH (10.20.0.150) unreachable this heartbeat; live release case not run.

## Deviations
- Verified on WSL home copy (builder down); release case ships for lab when grpcurl available.

## Cross-cutting
- Coordinated deploy: manager + fuse/client together (update-required).
- Do not weaken ForwardClientWrite or RegisterNode contracts.
