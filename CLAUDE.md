# CLAUDE.md — Operating Guide for Claude on DTFS

DTFS is a Raft-managed tiered distributed filesystem in Rust. This file is the canonical contract: keep it accurate and update it in the same commit as any behaviour or convention change.

Environment (2026-09): no build VM, no lab or production cluster reachable. Everything is verified with unit/crate tests in the local Docker build image (§ A.4). Cluster-only behaviour is reasoned about, not executed.

Part A = what to know about DTFS. Part B = how to work on it.

---

## Quick rules (every response)

- Read a file before editing; don't re-read unchanged files; skip >100KB files unless required.
- Thorough reasoning, concise output. Code first, explain only if non-obvious. No sycophancy, no emojis, no em-dashes, straight quotes, copy-paste-safe code.
- Don't guess APIs/versions/flags/SHAs/package names — verify first.
- Token-efficient: targeted Grep/Read with `offset`/`limit`, narrow edits, batch independent tool calls into one message.
- Don't over-engineer: simplest solution, fewest moving parts, solve what's in front of you. No speculative features, no premature abstraction, no error handling for impossible cases.
- Reviews/debugging: state the finding, show the fix, stop. Never speculate without reading the code; say so if the cause is unclear.

---

# Part A — Codebase Knowledge

## A.1 Mission & target workload

DTFS serves **one household-scale Plex media library across multiple datacenters with asymmetric bandwidth**. Every design decision anchors to this.

Reference deployment ("Plexmedia"), three DCs:

| DC | Link | Role | Key nodes |
|----|------|------|-----------|
| LELI | ≤40 Mbps | Primary archive — every chunk anchored here | mgr01-03 (Raft quorum), data-primary01 (~50 TB HDD), client01 |
| TORO | ~400 Mbps | Working set + read-ahead cache | mgr04, data-warm01 (~1 TB SSD), client02 |
| NRB | ~1 Gbps | Fresh-download landing, replicates to LELI | mgr05, data-warm02 (~1 TB HDD), client03 |

A node may host several roles (e.g. `toro-plex` = mgr04+data-warm01+client02+Plex on one box), so memory budgets are **per-role with a host-wide ceiling** — DTFS never starves a co-resident app.

Workload: Plex playback ≤60 Mbps/stream, ~5 streams/instance. Reads dominate (most chunks read once or twice ever); writes are bursty (a finished torrent lands a 30 GB season in minutes — absorb without OOM-killing anything). **Anchor invariant: every chunk MUST have ≥1 replica on LELI's data-primary01.** Tiering/migration may move *other* replicas freely; the LELI copy is sacred.

Non-goals: multi-tenant isolation, POSIX completeness (Plex's FUSE read + occasional write is the target; xattrs/mmap/open-unlink races are best-effort), online hot-rebalance across reconfigs (drain-then-replace instead).

## A.2 Architecture

**dtfs-manager** Raft peer + REST/gRPC frontend, owns metadata (files, chunk locations, node registry, volumes, leases, scrub, migration). **dtfs-data** data plane, stores chunks per-tier, streams put/get, reports usage+SMART. **dtfs-fuse** FUSE client at `/mnt/dtfs`, gRPC to managers+data nodes, local read-ahead cache. **dtfs-client** shared client crate (dtfs-fuse + dtfs-cli). **dtfs-raft** openraft state machine — the log is the source of truth. **dtfs-common** shared types (BLAKE3, hash ring, version). **dtfs-proto** protobuf/tonic codegen.

**Write:** acquire write_lease → allocate_chunks (target node addrs) → per chunk, stream PutChunk pieces to a tmp file on the target disk (no full-chunk `Vec`, Bug I) → commit_chunk_from_tmp (rename + meta + WAL clear) → commit_chunk_locations (durable file entry), all through Raft. CF-3b: commit_chunk_locations rejects the write when any chunk lands fewer distinct `(node_id, disk_id)` replicas than `write_safety.min_replicas_before_ack`.

**Read:** lookup metadata (cached ENTRY_TTL) → pick the read replica (`location.rs::select_read_replica`: configured `read_location_priority` beats physical distance beats a tier tie-break on equal distance only) → stream GetChunk → optionally cache locally → readahead prefetches. `GetFileLocationsResponse` carries `volume_id`; the client never downgrades a known volume to `default`, and `allocate_chunks` always places by the file's own registered volume. A foreground chunk miss is single-flighted per chunk_id. `GetFileLocations` omits replicas on Down/Failed nodes or Failed/Removed disks unless a chunk has no live replica at all (then every row is returned). A FUSE client that only lists learner managers can read but never write: client mutations are not leader-forwarded, so `manager_addr`/`manager_addrs` must include at least one voter. Detail: `docs/src/internals/invariants.md` § A.2.

## A.3 Conventions — Do Not Break

**A.3.1 Snapshot compat.** `StateMachineData` serialises to disk on snapshot. Every new field MUST carry `#[serde(default)]` so old snapshots load cleanly. Add a regression test (`state_machine::posix_path_regression` or `snapshot_persistence_tests`) per new field. CR-2-5 — a `ClientRequest` field that lands in replicated state (e.g. a timestamp) MUST also carry `#[serde(default)] now_ms: u64`, stamped by the proposer, with the apply falling back to `log_id.index` when it's `0` (legacy entry) — see `docs/src/internals/invariants.md` § A.8.

**A.3.2 Memory discipline.** No `Vec<u8>` accumulation of full chunks on the hot path, read or write — stream to/off disk, hash incrementally. Memory caps are best-effort, never reject by default (§ A.5). `[memory]` runs on all three roles (shared core `dtfs-common/src/memory.rs`, per-role ladder rung); `MemoryConfig::validate()` and `PlacementConfig::validate()` run at startup. FUSE caches are bounded (`METADATA_CACHE_MAX_ENTRIES = 4096`, swept on `metadata_ttl`; disk cache evicts to 90% with hysteresis). FUSE detail: `.claude/rules/fuse.md`.

**A.3.3 Integrity is the prime directive.** Every path that touches bytes must: (1) carry a BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2) write `.dat` AND `.meta` via tmp + `sync_all()` + atomic rename; (3) WAL the op before performing it, clear only after fsync; (4) never delete a chunk until a replacement is verified or no live file references the id — scrub faults quarantine by RENAMING to `.dtfs/quarantine/<id>.dat.bad`, never `remove_file`, and only after re-hashing the `.dat` fails to repair the `.meta`; (5) never silently downgrade replication factor; (6) verify on every read and on scrub — a mismatch is `Status::data_loss`, FUSE fails over, disabling `verify_on_read` needs an explicit ack; (7) when `cluster.shared_secret` is set, every mutating data-plane RPC (incl. `RecordCacheHit`) and every mutating ManagerService RPC (incl. `Heartbeat`, `ReportChunkStatus`) requires the HMAC-SHA256 `x-dtfs-rpc-ts`+`x-dtfs-rpc-mac` pair; REST mutations use the LIS-18 header. Checklist: `.claude/rules/integrity.md`.

**A.3.4 Mandatory vs cache replicas.** Two independent classes, never conflated: `mandatory_replicas.<tier>` (protected, only the under-replication scheduler maintains them) and `cache_replicas.<tier>.{min,max}` (demand-driven, evictable, only the tiering engine creates them). Every promote/demote checks the anchor against the destination first. Replica presence/removal is keyed by `(node_id, disk_id)`, never `node_id` alone. A "surviving" replica that justifies dropping another must be LIVE (`live_replica_tier`), and `apply_remove_chunk_location` refuses to remove the last live replica regardless of policy. Anchor datacenter strings compare case-insensitively. Detail: `.claude/rules/integrity.md`, `docs/src/internals/invariants.md` § A.3.4.

**A.3.5 Build-date / version stamp.** Every dev build MUST stamp `BUILD_DATE` fresh (the only distinguishing field between two dev builds) — deploy builds MUST go through a wrapper (`scripts/build-linux-amd64.sh`, `scripts/dtfs-build.sh`, `scripts/dtfs-release.sh <tag>`); confirm with `dtfs-<bin> --version`. Release builds additionally stamp real `DTFS_VERSION`. Cluster deploys go through the GitLab pipeline (`.gitlab-ci.yml`): a commit whose subject starts with `release vX.Y.Z` ships that version, one starting with `deploy` ships an alpha `v<base>_<YYYYMMDDHHmm>`; both create a GitLab release and run `deploy/ansible/update-cluster.yml` (managers learners → followers → leader, then data nodes, then clients, health wait + delay per node) followed by `scripts/dtfs-cluster-healthcheck.sh`. Detail: `docs/src/operations/ci-pipeline.md`. Guards: `scripts/tests/test-{build-date-fresh,release-version-stamp}.sh`. Mechanism (Bug BS, AZ-5): `docs/src/internals/invariants.md` § A.3.5.

## A.4 Build / test

Linux toolchain only (Windows host can't build rocksdb + fuse3). Build and test in the Docker image `dtfs-buildenv:latest` (`docker/buildenv.Dockerfile`: rust 1.94.1 bookworm + protoc, clang, libfuse3-dev; rebuild with `docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .`). The checkout is bind-mounted at `/build`; the cache lives in the named volumes `dtfs-target` (`/target`) and `dtfs-registry`. From Git Bash `MSYS_NO_PATHCONV=1` is required and `bash -lc` must not be used inside the image (it drops cargo from `PATH`):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "${DTFS_REPO_HOST_PATH:-C:/Users/<user>/git_github.com/dtfs2}:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release
```

Always `--release`. Run `cargo test -p dtfs-common --release` after any config/example change (`example_*_toml_parses`). Parallel agents share the `dtfs-target` volume, so cargo may block on the build lock; rerun a flaked `example_*_toml_parses` once. A crate-level `#![allow(dead_code)]` on dtfs-manager/dtfs-data works around a rustc 1.94 dead-code ICE — don't remove without re-verifying.

Deploy/release builds MUST go through a wrapper so `BUILD_DATE` is fresh (§ A.3.5); a bare `cargo build --release` stamps stale. `scripts/dtfs-release.sh <tag>` stamps `DTFS_VERSION`, builds, writes `target/release-artifacts/<tag>/`; the pipeline's `release` job uploads that directory to the GitLab package registry and creates the release (`scripts/ci/gitlab-release.sh`).

`tests/release/cases/*.sh` and `tests/proxmox/` are cluster-only harnesses kept for when a lab exists again; they are not part of the current verification loop and no change is required to ship one.

## A.5 Memory model

Deterministic, bounded, best-effort: slow down under pathological load, never OOM-kill. All three roles accept an optional `[memory]` block (CF-5, core in `dtfs-common/src/memory.rs`, per-role ladder — § A.3.2; defaults warn 80 / shed 92 / sample 5):

```toml
[memory]
hard_cap_mb          = 1500   # 0 = unbounded (back-compat)
warn_threshold_pct   = 75
shed_threshold_pct   = 88     # last-resort write reject (Bug G), opt-in
sample_interval_secs = 3
```

Ladder (cheapest first, goal: stay under without rejecting): stream to disk not heap → shrink data/FUSE caches → drop unconsumed readahead → pause background tasks → TCP backpressure → (opt-in) RESOURCE_EXHAUSTED.

Composite host (e.g. `toro-plex`): set `hard_cap_mb` per role (else systemd `MemoryMax`); total ≤ host RAM − Plex headroom − OS overhead (`tests/proxmox/abuse/14-composite-host-budget.sh`). Every daemon needs its own unique `[node].hostname`; FUSE registers under `[location].host` (`<data-hostname>_cln`) — see `docs/src/configuration/`.

`[grpc].max_message_size_mb` (manager/data) and `[client].max_message_size_mb` (fuse) govern the tonic message-size caps via `dtfs_common::config::max_message_bytes`; each role validates them (reject `0`) alongside `memory.validate()` at startup.

## A.6 Data-integrity audit checklist

Run mentally when you touch a write/migrate/delete/scrub path. The ten items live in `.claude/rules/integrity.md` (auto-loaded for `dtfs-data/**`, `dtfs-manager/src/scheduler.rs`, `dtfs-raft/**`); the bug history behind each is in `docs/src/internals/invariants.md` § A.6.

## A.8 File map

One line per file. Bug/task history per row: `docs/src/internals/invariants.md` § A.8.

| File | Responsibility |
|---|---|
| `dtfs-raft/src/state_machine.rs` | `apply_request`, the authoritative state-mutation point; sticky node states survive `RegisterNode`; directory rename re-keys children; blank-SM startup with a purged log refuses to start as a voter |
| `dtfs-raft/src/lib.rs::ClientRequest` | Command enum; new mutation = new variant + arm (+ `#[serde(default)] now_ms`) |
| `dtfs-raft/src/network.rs::full_snapshot` | Snapshot-stream RPC; unreachable-vs-storage error mapping, bounded in-flight sends |
| `dtfs-raft/src/log_store.rs` | RocksDB Raft log/vote/last-purged; synced writes, range-delete truncate, purge + `last_purged` in one batch |
| `dtfs-manager/src/scheduler.rs` + `scheduler/{repair,migration,flush,orphan}.rs` | TaskScheduler: struct, tick loop and lease GC in scheduler.rs; inherent-impl continuations per concern in the submodules. SOLE owner of replica repair; leader-gated, bounded, Raft-remove-before-byte-delete; disk-keyed orphan reap; leader re-checked before every delete batch |
| `dtfs-manager/src/tiering.rs` | AV cache-tick planner + legacy temperature migration; never holds the SM read lock across an await |
| `dtfs-manager/src/monitor.rs` | Heartbeat health detection only; MUST NOT schedule repair; leader-gated; Suspect/Down thresholds from `[monitoring]` (read at startup); warns on receiver-clock skew |
| `dtfs-manager/src/metadata.rs` | ManagerService gRPC over `raft_write`; follower writes forward with HMAC + `forwardable()` allow-list; ack floor clamped to the tier's RF; placement resolved from the file's own volume; a heartbeat whose liveness write cannot commit fails the RPC (NOT_FOUND = re-register); `GetFileLocations` drops non-live replicas unless none is live |
| `dtfs-data/src/main.rs::manager_lifecycle` | Register + 5 s heartbeat loop; walks the manager list after 3 failures, re-registers on NOT_FOUND; the startup chunk rescan runs in the background after the WAL replay so the server binds immediately |
| `dtfs-manager/src/api.rs` + `api/{raft,cluster,nodes,files,volumes,audit,config}.rs` | REST router + AppState in api.rs; handlers by resource in the submodules (each `use super::*`, re-exported `pub(crate)`); LIS-18 auth; decommission refuses while replicas remain |
| `dtfs-manager/src/rest_auth.rs` | LIS-18 mutation-auth check + axum middleware |
| `dtfs-manager/src/membership.rs` | Online raft membership validation |
| `dtfs-manager/src/join.rs` | Two-phase data-node join (HMAC handshake); node_id derived server-side |
| `dtfs-manager/src/webui.rs` | Embedded SPA; every mutation through `mutateFetch` (test-enforced); full parity with the CLI |
| `dtfs-data/src/server.rs::put_chunk` | Streaming put path; HMAC auth gates all data-plane RPCs |
| `dtfs-data/src/replication.rs::replicate_to` | Push-replication engine; timeout sized from chunk bytes |
| `dtfs-common/src/replication.rs` | Shared replication-deadline helpers |
| `dtfs-common/src/crypto.rs` | Join HMAC + data-rpc/mgr-rpc stamp/verify |
| `dtfs-data/src/store.rs::commit_chunk_from_tmp` | Atomic commit (§ A.6) |
| `dtfs-common/src/memory.rs` | Role-agnostic memory budget core |
| `dtfs-data/src/memory.rs` | Data-node rung; `[cache]` is parsed but inert |
| `dtfs-fuse/src/filesystem.rs` | FUSE↔cluster bridge; rules in `.claude/rules/fuse.md` |
| `dtfs-fuse/src/readahead.rs` | `ReadAheadEngine`; prefetch bound + Shed rung |
| `dtfs-fuse/src/fuse_adapter.rs` | fuse3 trait impl; every op error routes through `cluster_err_to_errno` |
| `dtfs-common/build.rs` + `src/version.rs` | `VERSION`/`BUILD_DATE` stamps (§ A.3.5) |
| `dtfs-cli/src/main.rs` | REST client (clap), full parity with the webui; mutations print the server error body and exit non-zero. List: `docs/src/api-reference/cli-reference.md` |
| `dtfs-cli/src/updater.rs` | `dtfs-cli update` — GitLab package registry (default, deploy/access token) or GitHub releases; `latest` = newest full `vX.Y.Z`; verify, atomic install, restart |
| `scripts/dtfs-release.sh` | Release builder; stamps version, assembles tarball + SHA256SUMS + git-cliff notes |
| `.gitlab-ci.yml` + `scripts/ci/{version,gitlab-release}.sh` | Pipeline: keyword gate on the commit subject, version resolution, package upload + GitLab release |
| `deploy/ansible/update-cluster.yml` + `tasks/update-node.yml` | Rolling cluster update used by the pipeline's deploy stage; order from live Raft state, per-node health wait + delay |
| `scripts/dtfs-cluster-healthcheck.sh` | Read-only post-deploy gate: leader agreement, status ok, all data nodes healthy, version stamps |
| `dtfs-common/src/tls.rs` | `TlsContext`; all gRPC routes through it; enable only cluster-wide |
| `tests/release/`, `tests/proxmox/` | Cluster-only harnesses, dormant without a lab |

---

# Part B — Operating Discipline

## B.1 How a change lands

Reproduce/locate → failing test in the purest layer (pure helper > state-machine apply > manager aggregator > FUSE helper) → smallest fix → `cargo test -p <crate> --release` in Docker (§ A.4), full workspace suite for cross-crate changes → commit to `master`. Branch only for a wire/on-disk format change (`proto/*.proto`, `ClientRequest`, `apply_request`, `#[serde]`-persisted structs) or work that spans sessions. PR only when a release-note line is wanted (`bug:`/`feat:` subjects).

## B.2 Plans

The session todo list is the only planning tool; there is no plans directory and no plan file format. Work that must outlive a session is carried by what already persists: the commit body (what changed, why, what is still open), `docs/src/internals/invariants.md` § A.8 for behaviour and bug history, and the final chat report for deferred findings (file:line, numbers, fix direction). If the user wants a written plan, they ask for one.

## B.3 Commits

```
<type>: <summary ≤72 chars>

What changed and why, in a few lines. Name the covering test only when
it isn't obvious from the diff.

Co-Authored-By: <model> <noreply@<vendor>.com>
```

`<type>` is `bug`/`feat`/`int`/`docs` and becomes the release-note line via `git-cliff`/`.github/cliff.toml` (`Breaking:`/`Update-required:` trailers route sections). Group related edits into one commit; don't split a fix into per-file commits. Never `--no-verify`, never `--amend` a pushed commit, never bypass signing (`.claude/hooks/guard-git.sh` blocks these).

## B.4 Tests

Observe fail → implement → observe pass → crate suite. New durability/anchor invariants get a property-style test over random ops. Cluster-only behaviour cannot be executed here: say so in the commit body instead of authoring an unrunnable release case.

## B.5 Where things live

`docs/src/` (archaeology: `docs/src/internals/invariants.md`) · `crates/<crate>/src/` · `proto/` · `tests/release/` + `tests/proxmox/` (dormant) · `scripts/` · `deploy/examples/*.toml` · `.claude/{agents,rules,hooks}/`. No root status files, no loose `.md` under `crates/`/`tests/`, no in-repo changelog. Config-surface sync (`.claude/rules/config-surface.md`): `dtfs-common/src/config.rs` changes update the examples + docs together.

## B.6 Updating this file

A behaviour change visible to an operator, agent, or the data plane updates CLAUDE.md in the same commit; fold in any session instruction that overrode a rule here.
