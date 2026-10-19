# CLAUDE.md — Operating Guide for Claude on DTFS

DTFS = Raft-managed tiered distributed filesystem in Rust. This file is canonical contract: keep accurate, update in same commit as any behaviour/convention change.

Environment (2026-09): no build VM, no lab or prod cluster reachable. All verification via unit/crate tests in local Docker build image (§ A.4). Cluster-only behaviour reasoned about, not executed.

Part A = DTFS knowledge. Part B = how to work on it.

---

## Quick rules (every response)

- Read file before editing; don't re-read unchanged files; skip >100KB files unless required.
- Thorough reasoning, concise output. Code first, explain only if non-obvious. No sycophancy, no emojis, no em-dashes, straight quotes, copy-paste-safe code.
- Don't guess APIs/versions/flags/SHAs/package names. Verify first.
- Token-efficient: targeted Grep/Read with `offset`/`limit`, narrow edits, batch independent tool calls in one message.
- Don't over-engineer: simplest solution, fewest moving parts, solve what's in front. No speculative features, no premature abstraction, no error handling for impossible cases.
- Reviews/debugging: state finding, show fix, stop. Never speculate without reading code; say so if cause unclear.

---

# Part A — Codebase Knowledge

## A.1 Mission & target workload

DTFS serves **one household-scale Plex media library across multiple datacenters with asymmetric bandwidth**. Every design decision anchors to this.

Reference deployment ("Plexmedia"), three DCs:

| DC | Link | Role | Key nodes |
|----|------|------|-----------|
| LELI | ≤40 Mbps | Primary archive, every chunk anchored here | mgr01-03 (Raft quorum), data-primary01 (~50 TB HDD), client01 |
| TORO | ~400 Mbps | Working set + read-ahead cache | mgr04, data-warm01 (~1 TB SSD), client02 |
| NRB | ~1 Gbps | Fresh-download landing, replicates to LELI | mgr05, data-warm02 (~1 TB HDD), client03 |

Node may host several roles (e.g. `toro-plex` = mgr04+data-warm01+client02+Plex on one box), so memory budgets **per-role with host-wide ceiling**. DTFS never starves co-resident app.

Workload: Plex playback ≤60 Mbps/stream, ~5 streams/instance. Reads dominate (most chunks read once or twice ever); writes bursty (finished torrent lands 30 GB season in minutes; absorb without OOM-killing anything). **Anchor invariant: every chunk MUST have ≥1 replica on LELI's data-primary01.** Tiering/migration may move *other* replicas freely; LELI copy sacred.

Non-goals: multi-tenant isolation, POSIX completeness (target = Plex FUSE read + occasional write; xattrs/mmap/open-unlink races best-effort), online hot-rebalance across reconfigs (drain-then-replace instead).

## A.2 Architecture

**dtfs-manager** Raft peer + REST/gRPC frontend, owns metadata (files, chunk locations, node registry, volumes, leases, scrub, migration). **dtfs-data** data plane, stores chunks per-tier, streams put/get, reports usage+SMART. **dtfs-fuse** FUSE client at `/mnt/dtfs`, gRPC to managers+data nodes, local read-ahead cache. **dtfs-client** shared client crate (dtfs-fuse + dtfs-cli). **dtfs-raft** openraft state machine; log = source of truth. **dtfs-common** shared types (BLAKE3, hash ring, version). **dtfs-proto** protobuf/tonic codegen.

**Write:** acquire write_lease → allocate_chunks (target node addrs) → per chunk, stream PutChunk pieces to tmp file on target disk (no full-chunk `Vec`, Bug I) → commit_chunk_from_tmp (rename + meta + WAL clear) → commit_chunk_locations (durable file entry), all through Raft. CF-3b: commit_chunk_locations rejects write when any chunk lands fewer distinct `(node_id, disk_id)` replicas than `write_safety.min_replicas_before_ack`.

**Read:** lookup metadata (cached ENTRY_TTL) → pick read replica (`location.rs::select_read_replica`: configured `read_location_priority` beats physical distance beats tier tie-break on equal distance only) → stream GetChunk → optionally cache locally → readahead prefetches. `GetFileLocationsResponse` carries `volume_id`; client never downgrades known volume to `default`, `allocate_chunks` always places by file's own registered volume. Foreground chunk miss single-flighted per chunk_id. `GetFileLocations` omits replicas on Down/Failed nodes or Failed/Removed disks unless chunk has no live replica at all (then all rows returned). FUSE client listing only learner managers can read but never write: client mutations not leader-forwarded, so `manager_addr`/`manager_addrs` must include ≥1 voter. Detail: `docs/src/internals/invariants.md` § A.2.

## A.3 Conventions — Do Not Break

**A.3.1 Snapshot compat.** `StateMachineData` serialises to disk on snapshot. Every new field MUST carry `#[serde(default)]` so old snapshots load. Add regression test (`state_machine::posix_path_regression` or `snapshot_persistence_tests`) per new field. CR-2-5: `ClientRequest` field landing in replicated state (e.g. timestamp) MUST also carry `#[serde(default)] now_ms: u64`, stamped by proposer, apply falls back to `log_id.index` when `0` (legacy entry). See `docs/src/internals/invariants.md` § A.8.

**A.3.2 Memory discipline.** No `Vec<u8>` accumulation of full chunks on hot path, read or write. Stream to/off disk, hash incrementally. Memory caps best-effort, never reject by default (§ A.5). `[memory]` runs on all three roles (shared core `dtfs-common/src/memory.rs`, per-role ladder rung); `MemoryConfig::validate()` and `PlacementConfig::validate()` run at startup. FUSE caches bounded (`METADATA_CACHE_MAX_ENTRIES = 4096`, swept on `metadata_ttl`; disk cache evicts to 90% with hysteresis). FUSE detail: `.claude/rules/fuse.md`.

**A.3.3 Integrity is the prime directive.** Every byte-touching path must: (1) carry BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2) write `.dat` AND `.meta` via tmp + `sync_all()` + atomic rename; (3) WAL op before performing, clear only after fsync; (4) never delete chunk until replacement verified or no live file references id. Scrub faults quarantine by RENAMING to `.dtfs/quarantine/<id>.dat.bad`, never `remove_file`, and only after re-hashing `.dat` fails to repair `.meta`; (5) never silently downgrade replication factor; (6) verify on every read and on scrub. Mismatch = `Status::data_loss`, FUSE fails over, disabling `verify_on_read` needs explicit ack; (7) when `cluster.shared_secret` set, every mutating data-plane RPC (incl. `RecordCacheHit`) and every mutating ManagerService RPC (incl. `Heartbeat`, `ReportChunkStatus`) requires HMAC-SHA256 `x-dtfs-rpc-ts`+`x-dtfs-rpc-mac` pair; REST mutations use LIS-18 header. Checklist: `.claude/rules/integrity.md`.

**A.3.4 Mandatory vs cache replicas.** Two independent classes, never conflated: `mandatory_replicas.<tier>` (protected, only under-replication scheduler maintains) and `cache_replicas.<tier>.{min,max}` (demand-driven, evictable, only tiering engine creates). Every promote/demote checks anchor against destination first. Replica presence/removal keyed by `(node_id, disk_id)`, never `node_id` alone. "Surviving" replica justifying drop of another must be LIVE (`live_replica_tier`); `apply_remove_chunk_location` refuses to remove last live replica regardless of policy. Anchor datacenter strings compare case-insensitively. Detail: `.claude/rules/integrity.md`, `docs/src/internals/invariants.md` § A.3.4.

**A.3.5 Build-date / version stamp.** Every dev build MUST stamp `BUILD_DATE` fresh (only distinguishing field between two dev builds). Deploy builds MUST go through wrapper (`scripts/build-linux-amd64.sh`, `scripts/dtfs-build.sh`, `scripts/dtfs-release.sh <tag>`); confirm with `dtfs-<bin> --version`. Release builds also stamp real `DTFS_VERSION`. Cluster deploys via GitLab pipeline (`.gitlab-ci.yml`): commit subject starting `release vX.Y.Z` ships that version; starting `deploy` ships alpha `v<base>_<YYYYMMDDHHmm>`. Both create GitLab release and run `deploy/ansible/update-cluster.yml` (managers learners → followers → leader, then data nodes, then clients, health wait + delay per node) then `scripts/dtfs-cluster-healthcheck.sh`. Detail: `docs/src/operations/ci-pipeline.md`. Guards: `scripts/tests/test-{build-date-fresh,release-version-stamp}.sh`. Mechanism (Bug BS, AZ-5): `docs/src/internals/invariants.md` § A.3.5.

## A.4 Build / test

Linux toolchain only (Windows host can't build rocksdb + fuse3). Build/test in Docker image `dtfs-buildenv:latest` (`docker/buildenv.Dockerfile`: rust 1.94.1 bookworm + protoc, clang, libfuse3-dev; rebuild with `docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .`). Checkout bind-mounted at `/build`; cache in named volumes `dtfs-target` (`/target`) and `dtfs-registry`. From Git Bash `MSYS_NO_PATHCONV=1` required; don't use `bash -lc` inside image (drops cargo from `PATH`):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "${DTFS_REPO_HOST_PATH:-C:/Users/<user>/git_github.com/dtfs2}:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release
```

Always `--release`. Run `cargo test -p dtfs-common --release` after any config/example change (`example_*_toml_parses`). Parallel agents share `dtfs-target` volume, so cargo may block on build lock; rerun flaked `example_*_toml_parses` once. Crate-level `#![allow(dead_code)]` on dtfs-manager/dtfs-data works around rustc 1.94 dead-code ICE. Don't remove without re-verifying.

Deploy/release builds MUST go through wrapper so `BUILD_DATE` fresh (§ A.3.5); bare `cargo build --release` stamps stale. `scripts/dtfs-release.sh <tag>` stamps `DTFS_VERSION`, builds, writes `target/release-artifacts/<tag>/`; pipeline `release` job uploads that dir to GitLab package registry and creates release (`scripts/ci/gitlab-release.sh`).

`tests/release/cases/*.sh` and `tests/proxmox/` = cluster-only harnesses kept for when lab exists again. Not part of current verification loop; no change required to ship one.

## A.5 Memory model

Deterministic, bounded, best-effort: slow down under pathological load, never OOM-kill. All three roles accept optional `[memory]` block (CF-5, core in `dtfs-common/src/memory.rs`, per-role ladder, § A.3.2; defaults warn 80 / shed 92 / sample 5):

```toml
[memory]
hard_cap_mb          = 1500   # 0 = unbounded (back-compat)
warn_threshold_pct   = 75
shed_threshold_pct   = 88     # last-resort write reject (Bug G), opt-in
sample_interval_secs = 3
```

Ladder (cheapest first, goal: stay under without rejecting): stream to disk not heap → shrink data/FUSE caches → drop unconsumed readahead → pause background tasks → TCP backpressure → (opt-in) RESOURCE_EXHAUSTED.

Composite host (e.g. `toro-plex`): set `hard_cap_mb` per role (else systemd `MemoryMax`); total ≤ host RAM − Plex headroom − OS overhead (`tests/proxmox/abuse/14-composite-host-budget.sh`). Every daemon needs own unique `[node].hostname`; FUSE registers under `[location].host` (`<data-hostname>_cln`). See `docs/src/configuration/`.

`[grpc].max_message_size_mb` (manager/data) and `[client].max_message_size_mb` (fuse) govern tonic message-size caps via `dtfs_common::config::max_message_bytes`; each role validates (reject `0`) alongside `memory.validate()` at startup.

## A.6 Data-integrity audit checklist

Run mentally when touching write/migrate/delete/scrub path. Ten items in `.claude/rules/integrity.md` (auto-loaded for `dtfs-data/**`, `dtfs-manager/src/scheduler.rs`, `dtfs-raft/**`); bug history per item in `docs/src/internals/invariants.md` § A.6.

## A.8 File map

One line per file. Bug/task history per row: `docs/src/internals/invariants.md` § A.8.

| File | Responsibility |
|---|---|
| `dtfs-raft/src/state_machine.rs` | `apply_request`, authoritative state-mutation point; sticky node states survive `RegisterNode`; directory rename re-keys children; blank-SM startup with purged log refuses to start as voter |
| `dtfs-raft/src/lib.rs::ClientRequest` | Command enum; new mutation = new variant + arm (+ `#[serde(default)] now_ms`) |
| `dtfs-raft/src/network.rs::full_snapshot` | Snapshot-stream RPC; unreachable-vs-storage error mapping, bounded in-flight sends |
| `dtfs-raft/src/log_store.rs` | RocksDB Raft log/vote/last-purged; synced writes, range-delete truncate, purge + `last_purged` in one batch |
| `dtfs-manager/src/scheduler.rs` + `scheduler/{repair,migration,flush,orphan}.rs` | TaskScheduler: struct, tick loop, lease GC in scheduler.rs; inherent-impl continuations per concern in submodules. SOLE owner of replica repair; leader-gated, bounded, Raft-remove-before-byte-delete; disk-keyed orphan reap; leader re-checked before every delete batch |
| `dtfs-manager/src/tiering.rs` | AV cache-tick planner + legacy temperature migration; never holds SM read lock across await |
| `dtfs-manager/src/monitor.rs` | Heartbeat health detection only; MUST NOT schedule repair; leader-gated; Suspect/Down thresholds from `[monitoring]` (read at startup); warns on receiver-clock skew |
| `dtfs-manager/src/metadata.rs` | ManagerService gRPC over `raft_write`; follower writes forward with HMAC + `forwardable()` allow-list; ack floor clamped to tier's RF; placement from file's own volume; heartbeat whose liveness write can't commit fails RPC (NOT_FOUND = re-register); `GetFileLocations` drops non-live replicas unless none live |
| `dtfs-data/src/main.rs::manager_lifecycle` | Register + 5 s heartbeat loop; walks manager list after 3 failures, re-registers on NOT_FOUND; startup chunk rescan runs in background after WAL replay so server binds immediately |
| `dtfs-manager/src/api.rs` + `api/{raft,cluster,nodes,files,volumes,audit,config}.rs` | REST router + AppState in api.rs; handlers by resource in submodules (each `use super::*`, re-exported `pub(crate)`); LIS-18 auth; decommission refuses while replicas remain |
| `dtfs-manager/src/api/chunks.rs` | Read-only `GET /api/v1/chunks[?state=]`, `/chunks/{id}`, `/memory` over `placement_state::for_each_chunk_placement`; pure `*_json` cores are unit-tested |
| `dtfs-manager/src/rest_auth.rs` | LIS-18 mutation-auth check + axum middleware |
| `dtfs-manager/src/membership.rs` | Online raft membership validation |
| `dtfs-manager/src/join.rs` | Two-phase data-node join (HMAC handshake); node_id derived server-side |
| `dtfs-manager/src/webui.rs` | Embedded SPA; every mutation through `mutateFetch` (test-enforced); full CLI parity |
| `dtfs-data/src/server.rs::put_chunk` | Streaming put path; HMAC auth gates all data-plane RPCs |
| `dtfs-data/src/replication.rs::replicate_to` | Push-replication engine; timeout sized from chunk bytes |
| `dtfs-common/src/replication.rs` | Shared replication-deadline helpers |
| `dtfs-common/src/crypto.rs` | Join HMAC + data-rpc/mgr-rpc stamp/verify |
| `dtfs-data/src/store.rs::commit_chunk_from_tmp` | Atomic commit (§ A.6) |
| `dtfs-common/src/memory.rs` | Role-agnostic memory budget core |
| `dtfs-data/src/memory.rs` | Data-node rung; `[cache]` parsed but inert |
| `dtfs-fuse/src/filesystem.rs` | FUSE↔cluster bridge; rules in `.claude/rules/fuse.md` |
| `dtfs-fuse/src/readahead.rs` | `ReadAheadEngine`; prefetch bound + Shed rung |
| `dtfs-fuse/src/fuse_adapter.rs` | fuse3 trait impl; every op error routes through `cluster_err_to_errno` |
| `dtfs-common/build.rs` + `src/version.rs` | `VERSION`/`BUILD_DATE` stamps (§ A.3.5) |
| `dtfs-cli/src/main.rs` | REST client (clap), full webui parity; mutations print server error body, exit non-zero. List: `docs/src/api-reference/cli-reference.md` |
| `dtfs-cli/src/updater.rs` | `dtfs-cli update`: GitLab package registry (default, deploy/access token) or GitHub releases; `latest` = newest full `vX.Y.Z`; verify, atomic install, restart |
| `scripts/dtfs-release.sh` | Release builder; stamps version, assembles tarball + SHA256SUMS + git-cliff notes |
| `.gitlab-ci.yml` + `scripts/ci/{version,gitlab-release}.sh` | Pipeline: keyword gate on commit subject, version resolution, package upload + GitLab release |
| `deploy/ansible/update-cluster.yml` + `tasks/update-node.yml` | Rolling cluster update used by pipeline deploy stage; order from live Raft state, per-node health wait + delay |
| `scripts/dtfs-cluster-healthcheck.sh` | Read-only post-deploy gate: leader agreement, status ok, all data nodes healthy, version stamps |
| `dtfs-common/src/tls.rs` | `TlsContext`; all gRPC routes through it; enable only cluster-wide |
| `tests/release/`, `tests/proxmox/` | Cluster-only harnesses, dormant without lab |

---

# Part B — Operating Discipline

## B.1 How a change lands

Reproduce/locate → failing test in purest layer (pure helper > state-machine apply > manager aggregator > FUSE helper) → smallest fix → `cargo test -p <crate> --release` in Docker (§ A.4), full workspace suite for cross-crate changes → commit to `master`. Branch only for wire/on-disk format change (`proto/*.proto`, `ClientRequest`, `apply_request`, `#[serde]`-persisted structs) or multi-session work. PR only when release-note line wanted (`bug:`/`feat:` subjects).

Model split (hard requirement): analysis, planning, verification stay with session's selected model; all implementation/coding goes to subagent at `model: sonnet`, `effort: medium` (`dtfs-implementer`, or `cavecrew-builder` for 1-2 file edit). Main session writes task spec, then verifies diff and Docker test results; never edits source itself.

## B.2 Plans

Session todo list = only planning tool; no plans directory, no plan file format. Work outliving session carried by what already persists: commit body (what changed, why, what still open), `docs/src/internals/invariants.md` § A.8 for behaviour and bug history, final chat report for deferred findings (file:line, numbers, fix direction). User wants written plan → they ask.

## B.3 Commits

```
<type>: <summary ≤72 chars>

What changed and why, in a few lines. Name the covering test only when
it isn't obvious from the diff.

Co-Authored-By: <model> <noreply@<vendor>.com>
```

`<type>` is `bug`/`feat`/`int`/`docs`, becomes release-note line via `git-cliff`/`.github/cliff.toml` (`Breaking:`/`Update-required:` trailers route sections). Group related edits into one commit; don't split fix into per-file commits. Never `--no-verify`, never `--amend` pushed commit, never bypass signing (`.claude/hooks/guard-git.sh` blocks these).

## B.4 Tests

Observe fail → implement → observe pass → crate suite. New durability/anchor invariants get property-style test over random ops. Cluster-only behaviour can't execute here: say so in commit body instead of authoring unrunnable release case.

## B.5 Where things live

`docs/src/` (archaeology: `docs/src/internals/invariants.md`) · `crates/<crate>/src/` · `proto/` · `tests/release/` + `tests/proxmox/` (dormant) · `scripts/` · `deploy/examples/*.toml` · `.claude/{agents,rules,hooks}/`. No root status files, no loose `.md` under `crates/`/`tests/`, no in-repo changelog. Config-surface sync (`.claude/rules/config-surface.md`): `dtfs-common/src/config.rs` changes update examples + docs together.

## B.6 Updating this file

Behaviour change visible to operator, agent, or data plane updates CLAUDE.md in same commit; fold in any session instruction that overrode rule here.