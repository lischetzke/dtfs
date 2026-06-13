# AGENTS.md — Operating Guide for AI Agents on DTFS

DTFS is a Raft-managed tiered distributed filesystem in Rust. This file
is the canonical contract: keep it accurate and update it in the **same
commit** as any behaviour, convention, or workload change. `CLAUDE.md`
is just `@AGENTS.md` — edit this file, not that one. Other tools (Codex,
Cursor, …) read AGENTS.md directly.

Part A = what to know about DTFS. Part B = how to work on it.

---

## Quick rules (every response)

- Read a file before editing; don't re-read unchanged files. Skip files
  >100KB unless required.
- Thorough reasoning, concise output. Code first; short explanation only
  if non-obvious. No sycophancy, no emojis, no em-dashes, straight
  quotes only, copy-paste-safe code.
- Don't guess APIs, versions, flags, commit SHAs, or package names —
  verify in code/docs first.
- Token-efficient: targeted Grep/Read with `offset`/`limit` over
  full-file reads; narrow edits over re-emitting whole files; reuse
  already-read context; batch independent tool calls into one message.
- Don't over-engineer. Simplest solution that works, for code and for
  actions alike — fewest moving parts, smallest diff, least new
  machinery. Solve the problem in front of you, not an imagined future
  one. No speculative features, no abstractions for single-use code, no
  error handling for impossible cases, no docstrings/types on code you
  aren't changing. Three similar lines beat a premature abstraction. When
  in doubt, do the smaller thing.
- Reviews and debugging: state the finding, show the fix, stop. Never
  speculate about a bug without reading the code first; if the cause is
  unclear, say so.

---

# Part A — Codebase Knowledge

## A.1 Mission & target workload

DTFS serves **one household-scale Plex media library across multiple
datacenters with asymmetric bandwidth**. Every design decision anchors
to this.

Reference deployment ("Plexmedia"), three DCs:

| DC | Link | Role |
|----|------|------|
| LELI | ≤40 Mbps | Primary archive — holds every chunk at least once |
| TORO | ~400 Mbps | Working set + read-ahead cache for playback |
| NRB | ~1 Gbps | Fresh-download landing, replicates to LELI |

Nodes: **LELI** = mgr01-03 (Raft quorum, write durability),
data-primary01 (~50 TB HDD, anchor copy of every chunk — may be N
nodes), client01 (Plex read). **TORO** = mgr04, data-warm01 (~1 TB SSD,
active set, empty at bootstrap), client02 (Plex read). **NRB** = mgr05,
data-warm02 (~1 TB HDD, fresh downloads), client03 (download write). A
node may host several roles (toro-plex = mgr04+data-warm01+client02+Plex
on one box), so memory budgets are **per-role with a host-wide ceiling**
so DTFS never starves a co-resident app.

Workload: Plex playback ≤60 Mbps/stream, ~5 streams/instance (~300 Mbps
egress peak at TORO/LELI). Reads dominate — most chunks read once or
twice ever; a small hot set rolls every few weeks as new media arrives.
Writes are bursty — a finished torrent lands a 30 GB season in minutes;
absorb it without OOM-killing anything. **Anchor invariant: every chunk
MUST have ≥1 replica on LELI's data-primary01.** Tiering/migration may
move *other* replicas freely; the LELI copy is sacred.

Non-goals: multi-tenant isolation; POSIX completeness (target Plex's
FUSE read + occasional write; xattrs/mmap/open-unlink races are
best-effort); online hot-rebalance across reconfigs (use
drain-then-replace).

## A.2 Architecture

- **dtfs-manager** — Raft peer + REST/gRPC frontend. Owns metadata: file
  metadata, chunk locations, node registry, volume configs, write
  leases, scrub schedules, alerts, migration tasks.
- **dtfs-data** — data plane. Stores chunks per-tier (hot/warm/cold/
  tape), streams put/get over gRPC, reports per-disk usage + SMART.
- **dtfs-fuse** — FUSE client mounting the namespace at /mnt/dtfs. gRPC
  to managers (metadata) and data nodes (chunk I/O). Local disk cache
  for read-ahead.
- **dtfs-client** — shared client crate used by dtfs-fuse and dtfs-cli.
- **dtfs-raft** — openraft state machine. The log is the source of
  truth; all metadata mutations go through it.
- **dtfs-common** — shared types + helpers (BLAKE3 checksums, hash ring,
  build.rs version string).
- **dtfs-proto** — protobuf/tonic codegen.

**Write:** acquire write_lease → allocate_chunks (returns target node
addrs) → per chunk, stream PutChunk pieces to a tmp file on the target
disk (no full-chunk `Vec`, see Bug I) → commit_chunk_from_tmp (rename +
meta + WAL clear) → commit_chunk_locations (durable file entry). All
manager steps go through Raft. CF-3b: commit_chunk_locations rejects the
write (success=false, no Raft write) when any chunk lands fewer distinct
`(node_id, disk_id)` replicas than the volume's
`write_safety.min_replicas_before_ack` (`metadata::ack_floor_satisfied`).

**Read:** lookup metadata (cached for ENTRY_TTL) → pick the read replica
(`location.rs::select_read_replica`, U4 order: (1) configured
`read_location_priority` — first replica whose datacenter/host matches the
earliest-listed entry, operator intent, beats distance; (2) physical distance
(`Location::distance`); (3) tier tie-break on *equal* distance — prefer warm
over the cold anchor HDD, never overriding a strictly-nearer replica) → stream
GetChunk → optionally cache locally (P0-11) → readahead engine prefetches
(folder- or file-sequential). `GetFileLocations` carries each replica's tier
(`ChunkLocationInfo.node_tiers`, positional with `node_addresses`); an older
manager leaves it empty (→ unknown tier → pure distance). It also carries each
replica's topology `Location` (`ChunkLocationInfo.node_locations`, positional
with `node_addresses`, CO-3-10) — the manager fills it from each replica node's
registered Location and the FUSE client learns it into `known_locations`
(keyed by node address, `filesystem.rs::learn_locations` on metadata refresh)
so `select_read_replica` can actually rank by distance + `read_location_priority`
(before CO-3-10 the map was never populated, so read routing was dead). An older
manager (or an unknown replica node) leaves an entry empty/all-empty → not
learned → that replica falls back to arbitrary/pure-distance order, exactly like
`node_tiers`.

## A.3 Conventions — Do Not Break

**A.3.1 Snapshot compat.** `StateMachineData` serialises to disk on
snapshot. Every new field MUST carry `#[serde(default)]` so old
snapshots load cleanly. Add a regression test
(state_machine::posix_path_regression or snapshot_persistence_tests) per
new field.

**A.3.2 Memory discipline.** No `Vec<u8>` accumulation of full chunks on
the hot path — stream to disk, hash incrementally (Bug I). The read-serve
path has the same parity (U2): the data-node `GetChunk` handler streams
the chunk off disk in 8 MB pieces (`read_exact`) and runs `verify_on_read`
via an incremental BLAKE3 hash, so a cache MISS never holds the whole
chunk in heap. Memory caps
are best-effort; never reject requests by default (the RESOURCE_EXHAUSTED
shed is opt-in, last resort — see § A.5). The `[memory]` block
(`hard_cap_mb` etc.) is implemented on **all three roles** (CF-5): the
role-agnostic core (`MemoryBudget`, `PressureLevel`, RSS sampler) lives in
`dtfs-common/src/memory.rs`; each role wires its own cheap ladder rung —
dtfs-data sheds put_chunk + shrinks ARC, dtfs-manager pauses scheduler
ticks at Warn, dtfs-fuse halves the readahead `cache_mb` at Warn / drops
unconsumed prefetch at Shed. systemd `MemoryMax` remains the hard backstop
for all roles.

**A.3.3 Integrity is the prime directive.** Every path that touches
bytes must:
1. Compute/carry a BLAKE3 checksum end-to-end (IncrementalHasher → stored
   in ChunkMeta.checksum).
2. Write new chunks to tmp + atomic rename after `sync_all()`
   (store::commit_chunk_from_tmp).
3. WAL the op before performing it; clear the WAL only after fsync.
   Recovery scans the WAL on startup.
4. Never delete a chunk until either a replacement replica is verified by
   the receiving data node, or the chunk's file has been unlinked at the
   manager and policy allows it. Chunks are content-addressed and may be
   referenced by several files (or re-created by an identical re-upload),
   so every delete-by-id path MUST re-check at dispatch time that no live
   file still references the id (Bug CI: `fanout_delete_allowed` in the
   unlink fan-out; skipped ids audit as `delete.fanout_skipped_live`).
5. Never silently downgrade replication factor — if RF=N can't be placed,
   queue migration tasks; don't accept the write at RF<N.
6. Verify on every read and on scrub (Bug BC-8). `verify_on_read=true`
   (default) makes the data node BLAKE3-hash each chunk before serving;
   mismatch → `Status::data_loss` → FUSE falls over to a healthy replica
   (P0-2), and the fault is pushed to the WebUI /ui/integrity feed
   (BC-12e). Disabling verify_on_read needs an explicit ack at the API
   boundary (BC-8 ack guard) and is the only mode where reads can deliver
   corrupt bytes. Scrub (quick+deep+orphan) still runs.

Full audit checklist: § A.6.

**A.3.4 Mandatory vs cache replicas (Bug AV invariant).** Two
independent replica classes:
- `mandatory_replicas.<tier>` — protected. **Only the under-replication
  scheduler maintains them** (`scheduler::check_under_replicated`, reads
  `effective_mandatory_replicas()`); it refuses to drop below the
  configured count. Coverage counts only serviceable replicas (node not
  Down/Failed, disk not Failed/Removed — `live_replica_tier`, Bug CH),
  so node/disk loss surfaces as a tier deficit and backfills here; the
  health monitor never schedules repair.
- `cache_replicas.<tier>.{min,max}` — demand-driven, evictable. **Only
  the tiering engine creates them, and only when the promotion threshold
  fires** (`promote_reads` / `promote_window_secs`). The under-
  replication scheduler MUST NOT backfill cache replicas.
- The cache-flush planner (`scheduler::plan_flush_cache`) refuses any
  flush that would strand the mandatory copy → `cache.flush_refused`
  audit + `AnchorViolated` alert. The dispatcher records
  `cache.flush_replica` per drop and `cache.flush_complete` per call.
- Every promote/demote must check `would_violate_anchor` against the
  destination state. The AV-2-4 planner filters unsafe demotions; the
  apply path (`RemoveChunkLocation` with `enforce_anchor`) re-checks as
  defence-in-depth.
- Engine-driven demote ≠ operator flush: idle-decay demote clears the
  cooldown (next read re-promotes); operator flush sets a future cooldown
  (no immediate re-promote). Route demote-like paths through
  `should_clear_cooldown_on_demote(cooldown_until_ms, now_ms)` (past/
  absent → clear; future → preserve), never an unconditional
  `SetChunkCooldown(0)`.
- A mandatory replica MAY co-locate with a cache replica of the same
  chunk on one node (different tier). Replica presence is keyed by
  `(node_id, disk_id)` (≡ `(node, tier)`), never `node_id` alone (Bug BH,
  `scheduler::replica_already_present`) — a node-only dedup drops the
  second-tier mandatory entry and loops the anchor-lag repair forever.
- **Removal is disk-targeted too (CO-1-1).** Every replica-drop path now
  carries the specific `disk_id` so it removes exactly the `(node_id,
  disk_id)` copy, never a co-located anchor on another disk of the same
  node. `ClientRequest::RemoveChunkLocation` and
  `RemoveCorruptChunkReplicas` carry `#[serde(default = "legacy_disk_id")]
  disk_id: u32`; the `u32::MAX` sentinel (old snapshots/log entries) means
  legacy node-wide removal (match on `(chunk_index, node_id)` only). The
  data-plane `DeleteChunkRequest.disk_id` is `optional uint32` (proto3
  explicit presence): present → unlink only that disk's copy
  (`store::delete_chunk_idempotent_on_disk`); absent (old manager / whole-
  chunk reclaim) → legacy first-found across `all_disks()`. The three
  cache/tiering removers thread the targeted disk:
  `scheduler::plan_excess_spill_removal` (returns `(node, disk)`),
  `scheduler::plan_flush_cache` (`FlushAction.disk_id`), and
  `tiering::plan_chunk_demotions` (`TickDemotion.disk_id`). Whole-chunk
  reclaimers (unlink fan-out, orphan reaper) intentionally stay node-wide
  (`disk_id = None`).

When a new code path creates OR removes replicas, make the class explicit
in its name/doc and consult one of these checks.

**A.3.5 Dev-build date freshness (hard requirement).** Every dev build
MUST stamp its own build instant into `BUILD_DATE` — `VERSION` defaults
to `v0.0.0-dev` (release builds override it via `DTFS_VERSION`, see
below), so for two *dev* builds `BUILD_DATE` is the only distinguishing
field (WebUI node table, logs, node registry). Mechanism (Bug BS):
`dtfs-common/build.rs` emits `DTFS_BUILD_DATE` from `SOURCE_DATE_EPOCH`
(falls back to `SystemTime::now()` when unset) and declares
`cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH`. The build wrappers
(`scripts/build-linux-amd64.sh`, `scripts/dtfs-build.sh`) export
`SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(date -u +%s)}"` before cargo
build, so each wrapper run re-stamps. A bare `cargo build` with the var
unset keeps the cached stamp (fast local incremental, but stale — never
deploy it). **Deploy builds MUST go through a wrapper** (or export the
var). Confirm a binary's stamp with `dtfs-<bin> --version`. Guard:
`scripts/tests/test-build-date-fresh.sh`. Reproducible-build envs that
pin `SOURCE_DATE_EPOCH` are the sanctioned exception (wrappers honour a
caller-set value).

**Release version stamp (AZ-5).** `build.rs` also emits `DTFS_VERSION`
(default `v0.0.0-dev`, with `rerun-if-env-changed`); `version.rs` reads
it for `VERSION`/`VERSION_STRING`. `scripts/dtfs-release.sh <tag>`
exports `DTFS_VERSION=<tag>` before the build so a release binary's
`--version` (and the WebUI/node table) reports the real tag; plain/dev
builds stay `v0.0.0-dev`. `dtfs-cli update --check` compares that
installed version against the latest GitHub release tag. Guard:
`scripts/tests/test-release-version-stamp.sh`.

## A.4 Build / test / deploy

The workspace builds on **Linux/WSL** only (the Windows host can't build
rocksdb + fuse3). The `dtfs-builder` VM hosts the build chain at
**10.150.0.150** (the hostname `dtfs-builder` resolves only inside the
lab DNS 10.150.0.2/3 — from the operator host, use the IP). Access:
`ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150`. The builder has
rustup (`source ~/.cargo/env` first — login shells don't auto-add
`~/.cargo/bin`), protoc, fuse3, `mdbook`, and `clang`/`libclang-dev`
(needed by the rocksdb bindgen in dtfs-manager/dtfs-data), so the full
workspace builds. `debian` has passwordless sudo.

The builder is **not** a git checkout — sync the working tree to
`~/dtfs2` (the operator host has no `rsync`; use `tar` over SSH), then
build over SSH:

```bash
# from the operator host, in the worktree root; exclude build/VCS/agent
# dirs AND ./dummy_files (a ~255 GB gitignored local fixture dir — the
# builder disk is 32 GB; syncing it fills the disk mid-tar)
tar czf - --exclude=./target --exclude=./.git --exclude=./.claude \
    --exclude=./dummy_files . \
  | ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150 \
      'mkdir -p ~/dtfs2 && tar xzf - -C ~/dtfs2'
ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150 \
  'source ~/.cargo/env && cd ~/dtfs2 && cargo test -p dtfs-cli --release'
```

```bash
cargo build --release --workspace
cargo test  --release --workspace
cargo test -p dtfs-data --release   # per-crate
```

**Stale-artifact trap (Bug CF deploy hiccup).** The tar sync preserves
source mtimes, so a just-synced edit can land OLDER than cached build
artifacts and cargo will silently reuse a stale rlib (a lib's
`#[cfg(test)]` tests can still pass — the lib-test binary rebuilds
independently — while downstream binaries link the pre-edit lib).
Before deploying a binary that must contain a fresh change: `touch` the
edited file(s) on the builder (or rebuild after a marker change) and
verify the binary actually carries the change (`grep -ac "<new log
string>" target/release/<bin>` or the `--version` stamp).

Deploy builds MUST go through a wrapper (`scripts/build-linux-amd64.sh`,
`scripts/dtfs-build.sh`, or `scripts/dtfs-release.sh <tag>` for a tagged
release) so `BUILD_DATE` is fresh (§ A.3.5). Bare `cargo build --release`
is fine for iteration but stamps stale; re-stamp with
`SOURCE_DATE_EPOCH=$(date -u +%s) cargo build --release …`. Cutting a
release: `scripts/dtfs-release.sh <tag>` stamps `DTFS_VERSION`, builds,
and writes `target/release-artifacts/<tag>/` (the `dtfs-linux-amd64.tar.gz`
the updater pulls, `SHA256SUMS`) + the `gh release create` line (release
notes auto-generated from merged PRs via `--generate-notes`).

A crate-level `#![allow(dead_code)]` on the dtfs-manager and dtfs-data
binaries works around a rustc 1.94 dead-code ICE — don't remove it
without first verifying the ICE no longer reproduces on the current
toolchain.

Rolling deploy: scp the new binary, `install -m 755`,
`systemctl restart`. Roll managers leader-last, then data nodes, then
FUSE clients.

Lab: Proxmox on 10.150.0.0/24. PVE host root `ssh root@10.150.0.121`
(password-only; store via sshpass only on the operator's WSL home, never
committed). VM key `~/.ssh/dtfs-lab/id_ed25519`. Bandwidth shaping in
`tests/proxmox/ansible/roles/dtfs-netshape/`.

## A.5 Memory model

Deterministic, bounded, best-effort: under pathological load we slow
down, never OOM-kill. All three role configs accept an optional `[memory]`
block (CF-5; shared core in `dtfs-common/src/memory.rs`, per-role ladder
rung — § A.3.2; defaults warn 80 / shed 92 / sample 5):

```toml
[memory]
hard_cap_mb          = 1500   # 0 = unbounded (back-compat; new deploys non-zero)
warn_threshold_pct   = 75
shed_threshold_pct   = 88     # last-resort write reject (Bug G), opt-in
sample_interval_secs = 3
```

Strategy ladder when RSS nears the cap (cheapest first; goal: stay under
*without* rejecting): 1) stream to disk, never buffer whole chunks
(Bug I); 2) shrink ARC cache (data) / FUSE local cache (client); 3) drop
unconsumed readahead chunks; 4) pause background tasks (scrub, tiering,
metrics); 5) throttle write acceptance via TCP backpressure; 6) (opt-in,
last resort) return RESOURCE_EXHAUSTED to new put_chunk, client retries
with backoff.

Composite host (toro-plex = mgr04+data-warm01+client02+Plex): set
`hard_cap_mb` per role where supported (else systemd `MemoryMax`); total
≤ host RAM − Plex headroom − OS overhead.
`tests/proxmox/abuse/14-composite-host-budget.sh` validates the sum.
Composite-host identity rule: every daemon needs its own unique
`[node].hostname` (node_id = hash(hostname)); the FUSE client registers
under `[location].host` (`<data-hostname>_cln` convention) — see
`docs/src/configuration/` and `deploy/examples/`.

## A.6 Data-integrity audit checklist

Run when you touch a write/migrate/delete/scrub path; mark each "Verified
by <test>" in the commit.
1. BLAKE3 via IncrementalHasher; final hash in ChunkMeta.checksum.
2. On-disk `.dat` written to `tmp/<uuid>.dat.tmp`, atomic-renamed to
   `chunks/<aa>/<bb>/<id>.dat` only after `sync_all()`.
3. `.meta` JSON written with `sync_all()` before the put's WAL entry is
   cleared.
4. Counter updates (bytes_stored, chunk_count) happen after meta is
   durable, so a crash mid-put leaves a reconcilable tmp+wal residue.
5. commit_chunk_from_tmp removes the tmp on any error path.
6. Migration marks a task Done only after VerifyChunk on the target
   succeeds; the scheduler never removes the source (only tier-demote /
   unlink delete a chunk).
7. Scrub BLAKE3s the disk file vs chunk.meta.checksum; mismatch →
   chunks_corrupt + ScrubCorruption alert; auto-repair via
   replicate_chunk.
8. unlink removes manager-side FileMetadata first, then GC deletes chunks
   async; a crash between leaves orphans (reclaimed by scrub), never
   dangling pointers. Bug CB — the orphan reaper measures its grace
   against each chunk's durable on-disk `.dat` mtime
   (`scheduler::orphan_reap_due`, fed by `ListChunks.chunk_mtimes_ms`),
   NOT a manager-local first-observed clock, so reclamation survives
   manager restarts / leadership changes (pre-CB the in-memory clock
   reset every restart and deleted-file disk was never freed under
   rolling-restart churn). An unknown mtime (pre-CB data node) defers
   the reap rather than dropping a possibly in-flight chunk.
9. rename is a single Raft command; open handles keep reading the same
   chunks (path updated, chunk_id binding unchanged).
10. Reads use replica-fallback (P0-2): try the next replica on error,
    fail only after all candidates fail.

## A.7 Bandwidth + network shaping

`tests/proxmox/ansible/roles/dtfs-netshape/`. Per-class `tc qdisc` + IFB:

| Class | Up/Down | Jitter |
|-------|---------|--------|
| LELI | 40 Mbps | 25±5 ms |
| TORO | 400 Mbps | 5±1 ms |
| NRB | 1 Gbps | 1±0.5 ms |

Apply via apply-shaping.yml, revert via revert-shaping.yml. **Exempt:
SSH/22, manager gRPC/9401, manager API/9402, data gRPC/9500, NTP/123** —
otherwise Raft heartbeats fail under jitter and the cluster can't elect.

## A.8 File map

- `dtfs-raft/src/state_machine.rs` — `apply_request`, the authoritative
  state-mutation point.
- `dtfs-raft/src/lib.rs::ClientRequest` — command enum; a new mutation =
  new variant + new arm in apply_request.
- `dtfs-raft/src/network.rs::full_snapshot` — snapshot-stream RPC. Bug CL:
  network failures here (peer unreachable / timeout / empty response) MUST map
  to the transient `StreamingError::Unreachable` (openraft retries), NEVER to
  `StreamingError::StorageError` — openraft treats StorageError as a fatal
  local-storage failure and shuts down the leader's RaftCore (caused
  cluster-wide quorum loss on dtfs-lm). Only genuine local snapshot reads use
  StorageError. CNF-4: cross-peer snapshot sends are capped at 2 in flight
  (`SNAPSHOT_SEND_PERMITS` semaphore — each send holds ~2.7x snapshot size in
  heap as base64+JSON); the shared base64 encoder/decoder pair lives here
  (`base64_encode`/`base64_decode`, used by the dtfs-manager snapshot
  receiver). The transport is a JSON-string unary RPC — serde_bytes would
  serialize as an int array (bigger than base64), so don't drop the base64
  without a wire change. CO-3-6: the peer channel sets NO channel-level
  `.timeout()` (tonic's GrpcTimeout layer takes the MIN of channel + per-request
  timeout, so a fixed channel timeout would clamp the snapshot RPC back down);
  every `send_rpc` sets its own per-request timeout via
  `tonic::Request::set_timeout`. AppendEntries/Vote pass the 5s `RPC_TIMEOUT`;
  the snapshot send passes `snapshot_rpc_timeout(bytes, option.hard_ttl())` =
  max(openraft's hard_ttl, snapshot_bytes / 1 MBps floored at 5s), so a >25 MB
  snapshot to a learner behind the 40 Mbps LELI shape gets enough wall-clock to
  land instead of dying at the old 5s default. A fired timeout is still a tonic
  Status -> `unreachable_err` -> `RPCError::Unreachable` -> `StreamingError::
  Unreachable` (transient), preserving the Bug CL mapping above.
- `dtfs-manager/src/scheduler.rs` — migration/re-replication loop; the
  SOLE owner of replica repair (Bug AC leader gate + Bug CH).
  `schedule_path_migration` (operator moves), `flush_cache_for_path` +
  `plan_flush_cache` (drop cache replicas; emit cache.flush_* audit +
  AnchorViolated on refusal), `check_under_replicated` (counts coverage
  via `live_replica_tier` — replicas on Down/Failed nodes or
  Failed/Removed disks don't count, so node/disk loss backfills here),
  `validate_task_target` (dispatch fast-fails tasks targeting
  non-Data/dead/tier-less nodes → `migration.invalid_target` audit),
  `check_misplaced_rebalance` / `plan_excess_spill_removal` (Bug BX-5
  rebalance). `advance_tasks` drives the task state machine each 30s tick;
  U3 made it multi-transition-per-tick (round loop until no progress, so a
  chunk climbs Pending->Done in ONE tick) + bounded-parallel copy
  (`MIGRATION_COPY_CONCURRENCY`=6 in-flight `ReplicateChunk` RPCs;
  `MAX_ADVANCE_ROUNDS`=8 guard). The transition logic lives in the generic
  `advance_migration_tasks` over the `MigrationOps` trait
  (`TaskScheduler` is the live impl; unit-tested via a mock in
  `advance_round_tests`); a task still attempts at most one copy + one
  verify RPC per tick (preserves the pre-U3 retry cadence + verify-mismatch
  re-copy-next-tick). **Bug CN — flood-aftermath robustness:** (a) `tick`
  prunes terminal (Done/Failed past-retention) tasks FIRST via
  `prune_terminal_tasks` and persists that immediately, BEFORE the
  under-replication scan + advance — pre-CN the prune ran only at the end of
  `advance_tasks`, so a large terminal backlog (prod 2026-06-10: 2439 Failed
  tasks after the Bug CM wedge) starved its own GC and every leader reloaded
  the poison; (b) each `replicate_chunk`/`verify_chunk` RPC is wall-clock
  bounded by `MIGRATION_RPC_TIMEOUT` (60s) — without it a stalled data node
  hung the `buffer_unordered` copy round and the whole scheduler loop after
  one leader-onset pass. So the pre-tick prune may add a SECOND
  `save_migration_state` Raft write when it drops tasks; steady-state is still
  one. The orphan_scrub (`orphan_scrub_tick`) reaps per-node in two cases,
  both gated by the Bug CB durable-mtime grace (`orphan_reap_due`): a HASH
  orphan (no file references the hash; `chunk_is_orphan`) AND **CO-3-2** a
  DE-LOCATED chunk (on a node's disk but the SM records no location entry
  naming that node — the rebalance / excess-spill path drops the SM entry via
  `RemoveChunkLocation` but never deletes the bytes; `chunk_delocated_on_node`);
  the reap audits `cause=orphan|delocated`. The reap stays node-wide (legacy
  first-found delete; ListChunks carries no disk_id).
- `dtfs-manager/src/monitor.rs` — heartbeat health detection only
  (Bug CH): commits node-status transitions, fires NodeDown/SMART
  alerts. MUST NOT schedule repair — the 2026-06-06 storm came from a
  monitor-owned evacuation path (PLAN-BUG-CH). **Leader-gated (Bug CM):**
  only the current leader evaluates liveness + commits `UpdateNodeStatus`
  (and runs `check_disk_health`), via the same `metrics.current_leader ==
  id` gate as the scheduler (Bug AC); `monitor_status_changes` returns
  empty off-leader. Bug CJ moved liveness onto the replicated
  `last_seen_ms`, but a non-leader reading its own bursty-stale replicated
  view still committed `Down` and flapped the node (prod dtfs-lm v0.1.5,
  2026-06-09, two cross-DC learner managers) — the leader's log is
  authoritative + single-writer. Recovery stays the heartbeat handler's
  `UpdateNodeStatus(healthy)` (any manager, forwarded).
- `dtfs-manager/src/metadata.rs` — gRPC service; thin wrappers over
  `raft_write(ClientRequest::*)`. Follower-originated writes proxy to the
  leader via `ForwardClientWrite`; CO-2-2 authenticates that RPC — the
  forwarder stamps `ForwardWriteRequest.shared_secret` (field 2) from
  `config.cluster.shared_secret` and the leader rejects a mismatch with
  `permission_denied` via a CONSTANT-TIME compare (`forward_secret_ok`,
  which reuses `dtfs_common::crypto::constant_time_eq` — never `==` on the
  secret); an empty configured secret skips the check (back-compat for
  plaintext clusters). Any new forward site MUST populate that field.
  CO-3-7: `report_chunk_status` commits its
  corrupt/missing handling (UpdateNodeStatus + RemoveCorruptChunkReplicas)
  through `raft_write_or_forward` and returns `accepted: false` on failure —
  never `let _ = raft_write(...)`, which on a follower silently dropped the
  de-location so the bad replica stayed advertised healthy and
  re-replication never queued. The dtfs-data heartbeat loop re-enqueues the
  drained corrupt ids when the ack is `accepted == false` (same as a
  transport error), so the retry actually happens.
- `dtfs-manager/src/api.rs` — REST endpoints (WebUI + CLI); `get_stats`
  is the WebUI home aggregator. The config read endpoints (`get_config`,
  `get_config_effective`) return the **effective** config = the node's
  startup `state.config` with the Raft cluster override
  (`cluster_config.config_json`) projected on top via `json_deep_merge`
  (Bug CM) — a saved cluster-wide override MUST read back as `current`, or
  the WebUI editor looks broken. Adding a config read path? Route it
  through `effective_config_value`, never raw `state.config` (CO-5-9 — the
  `get_config` `defaults` block follows the same merge). The write path
  (`update_config`) deep-merges the override onto the startup config (which,
  since `UpdateConfig` replaces the override wholesale, IS the post-save
  effective config) and REQUIRES the merge to deserialize as `ManagerConfig`
  (`merge_and_validate_override`, 400 on type error, CO-4-3), then persists
  `toml::to_string_pretty` of the validated merge — never raw override JSON
  into the `.toml`.
- `dtfs-manager/src/membership.rs` — CF-2 online raft membership: pure
  validation for the leader-only `/api/v1/raft/{add-learner,promote,
  remove}` endpoints (openraft `add_learner`/`change_membership`;
  `initial_voters` is bootstrap-only).
- `dtfs-manager/src/join.rs` — two-phase data-node join (HMAC handshake;
  `/api/v1/join/{request,complete}` in api.rs). Phase-1 `handle_join_request`
  verifies HMAC(secret, nonce), issues a challenge, and persists it with a
  60s single-use TTL (`challenges` map). Phase-2 `complete_join` (CO-2-3
  security) requires `HMAC(secret, challenge)` (constant-time
  `verify_completion_inner`, consumes the challenge), derives
  `node_id = node_id_from_hostname(hostname)` SERVER-SIDE (never trust a
  client id), then does the `RegisterNode` Raft write. Empty
  `cluster.shared_secret` = documented unauthenticated downgrade (skips the
  challenge/HMAC checks). docs/src/security/cluster-join.md.
- `dtfs-manager/src/webui.rs` — embedded SPA, format-string HTML
  (`{{`/`}}` for literal braces).
- `dtfs-data/src/server.rs::put_chunk` — streaming put path.
- `dtfs-data/src/store.rs::commit_chunk_from_tmp` — atomic commit
  (§ A.6).
- `dtfs-common/src/memory.rs` — CF-5 role-agnostic memory budget core
  (`MemoryBudget`, `PressureLevel`, RSS sampler, `run_monitor`); shared by
  all three roles.
- `dtfs-data/src/memory.rs` — Bug G data-node rung: re-exports the common
  core + the ARC-cache shrinker monitor (`run_monitor_with_cache`).
- `dtfs-fuse/src/filesystem.rs` — FUSE↔cluster bridge: write leases,
  readahead engine, local disk cache. CO-3-12: `read()` clones the
  per-call data (path/chunk_size/volume_id/locations) out of the
  `open_files` entry and DROPS the DashMap guard BEFORE the per-chunk
  network loop (same idiom as `write()`/`flush()`) — never hold an
  `open_files` guard across an `.await`, or a stalled chunk fetch
  serializes concurrent opens of the same file.
- `dtfs-fuse/src/fuse_adapter.rs` — fuse3 trait impl; cookie scheme at
  `dir_entry_cookie()` — don't diverge readdir/readdirplus (Bug F).
  CO-4-16: metadata ops map cluster errors via `cluster_err_to_errno` —
  ENOENT ONLY for a genuine gRPC `NotFound`, EIO for transient/unknown
  (manager outage, election, timeout). Mapping a blip to ENOENT makes Plex
  drop the library entry. New error-returning op? Route it through the
  helper, never a bare `Err(_) => ENOENT`.
- `dtfs-common/build.rs` + `src/version.rs` — VERSION (default
  `v0.0.0-dev`, overridable via `DTFS_VERSION`) + BUILD_DATE (§ A.3.5);
  every binary reports these via RegisterNode.
- `dtfs-cli/src/updater.rs` — `dtfs-cli update <target>` (download →
  sha256-verify vs MANIFEST → atomic install → restart) + the
  `[updater]` config; local-node only. Pairs with the release builder.
- `scripts/dtfs-release.sh` — release builder: stamps `DTFS_VERSION`,
  builds via `dtfs-build.sh`, assembles the fixed-name
  `dtfs-linux-amd64.tar.gz` + `SHA256SUMS`, prints/`--publish`es
  `gh release create --generate-notes`.
- `dtfs-common/src/tls.rs` — CF-6 `TlsContext`: resolves each role's
  `[tls]` block once at startup (auto/require/disabled; default stays
  plaintext for back-compat) and builds every tonic server/client TLS
  config + scheme-aware endpoint. ALL gRPC listeners and connects (manager,
  data, raft peers, FUSE via `DtfsClient::set_tls`) route through it; mixed
  TLS state partitions the cluster, so enable only cluster-wide
  (docs/src/security/tls.md rollout).
- `plans/active/` — in-flight plans (see plans/README.md).
- `tests/proxmox/abuse/` — numbered shell regression tests.

---

# Part B — Operating Discipline

Applies to every non-trivial change (new feature, multi-bug wave,
refactor touching >3 files, anything >1 day). For trivial work
(one-liners, single doc edits) use judgement and just do it. The fixing
procedure (§ B.1), plan workflow (§ B.2), commit discipline incl. the
`Co-Authored-By:` trailer (§ B.4), and test discipline (§ B.5) are
mandatory for non-trivial work; commits that bypass them get reverted. If
a session instruction conflicts with a rule here, the user wins — flag
it so this file gets updated (§ B.7).

## B.1 Fixing procedure

Every fix, no matter how small:
1. **Write a failing test** in the purest layer that shows the bug
   (§ B.5). Run it, observe the failure.
2. **Implement** the smallest fix that turns it green. No bundled
   cleanup.
3. **Verify**: the new test passes, the crate's suite has no
   regressions, and live-verify on the lab if the bug is operational.
   Quote the failing-then-passing run in the commit.

On a new bug: reproduce → check `plans/active/` for an existing plan that
covers it (add a task there if so) → else pick the next bug letter
(alphabetical from the highest in `plans/active/`) → create
`plans/active/PLAN-BUG-<letter>-<name>.md` (§ B.2) → follow the three
steps.

## B.2 Plan-driven workflow

Before code for a non-trivial change, write
`plans/active/PLAN-<id>-<short-name>.md`. Keep it lean. It must contain:
- Goals + non-goals.
- Locked-in decisions from the user.
- Data-model changes with concrete shapes (struct fields, proto
  messages, Raft variants, config keys).
- Task list with stable IDs (`<plan-id>-<phase>-<n>`), each listing:
  files, implementation sketch, required tests, "done when" acceptance
  criteria, and a status marker (⬜ todo / 🟦 wip / ✅ done / 🟥 blocked /
  🟨 review).
- Cross-cutting invariants.
- Verification log + Deviations sections (filled in as work proceeds).

The plan is the source of truth. When reality diverges, update the plan
(Deviations) first, then continue. Material design changes (new
field/task, dropped task) need user review before being committed.
**Retention: delete the plan file once every task is ✅ and merged — git
history keeps it.** Do not archive plans or leave stale plan files
around.

## B.3 Per-task loop

Mark 🟦 → implement the smallest change for the listed tests + done-when
→ `cargo test -p <crate> --release` green → full crate suite green on the
build host → deploy the affected binary → live-verify the "done when" →
add a verification-log entry to the plan (task ID, date, commands run,
output snippet) → commit (§ B.4) → mark ✅. The verification-log entry and
status flip land in the **same commit** as the code.

## B.4 Commit discipline

Work on a branch and merge via PR when the change is user-visible — the
PR title becomes the release-note line (§ B.6); label it for its
release-notes section. Committing small internal work directly to
master is fine. Commit format:

```
<plan-id>-<phase>-<n>: <one-line summary, <72 chars>

<2-3 sentence body: what changed, what tests cover it, any deviation
from the plan sketch.>

Plan: <relative-path-to-plan> (<task-id> ✅)
Co-Authored-By: <model-name> <noreply@<vendor>.com>
```

The `Plan:` trailer (so `git log --grep "Plan:"` filters agent-driven
work) and the `Co-Authored-By:` trailer are mandatory. One commit per
task; co-locate doc updates with the code they describe. Never
`--no-verify` (fix the failing hook, don't bypass), never `--amend` a
pushed commit, never bypass GPG signing if the repo requires it.

## B.5 Test discipline

- Write the failing test in the purest layer: pure helper / state-machine
  apply (unit, ms) > manager aggregator (unit vs StateMachineData) > FUSE
  helper (unit) > cross-process (shell under
  `tests/proxmox/abuse/NN-name.sh`). A compile error counts as the
  failing test.
- Observe fail → implement → observe pass → run the full crate suite →
  live-verify if operational.
- New durability/anchor invariants warrant a property-style test over
  many random op sequences.

**Release-suite registration (mandatory).** Every test exercising
cluster-visible behaviour (new API, operator action, WebUI flow hitting
the manager, failure/recovery path, or a placement/integrity/scrub/
quarantine/routing invariant) MUST also ship a case under
`tests/release/cases/NN-name.sh` in the **same commit**.
`tests/release/run.sh` is the single artefact the operator runs before a
release tag — anything not in it isn't release-gated. Rules: one case per
scenario, numbered + named for the behaviour; last stdout line
`STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug-id>]`; `trap case_cleanup
EXIT`; no operator interaction (use `wait_for` from
`tests/release/lib/common.sh`); KNOWN_ISSUE pins to a bug id and
auto-promotes when it closes; cheap critical-path cases list their stem
in `QUICK_CASES`. A pure internal refactor with no behaviour change may
skip release coverage — note that in the commit.

## B.6 Structure discipline

Where a file lives is part of its meaning. Canonical locations:

| Artefact | Location |
|---|---|
| In-flight work plan | `plans/active/PLAN-<id>-<name>.md` (delete when done, § B.2) |
| Cluster scan / lab report | `plans/reports/<NAME>-<date>.md` (e.g. `ISSUE-SCAN-…`) |
| Release note (user-visible change) | PR title + description — GitHub auto-generates release notes from merged PRs (`gh release create --generate-notes`, grouped via `.github/release.yml`); no in-repo changelog |
| GitHub repo config (release-notes grouping, workflows) | `.github/` |
| Architecture / config / ops reference | `docs/src/...` (mdBook), same commit as the behaviour change |
| Code | `crates/<crate>/src/...` (new crate → workspace `Cargo.toml` entry) |
| Protobuf | `proto/<file>.proto` (rebuild dtfs-proto after edits) |
| Unit tests | inline `#[cfg(test)]` modules |
| Integration / shell tests | `tests/integration/`, `tests/proxmox/abuse/NN-name.sh` |
| Release-gate e2e | `tests/release/cases/NN-name.sh` + `lib/` + `run.sh` (§ B.5) |
| Operator scripts | `scripts/` |
| Deployment surface | `deploy/` |
| Canonical example configs | `deploy/examples/{manager,data,fuse}.toml` — every option, optional ones commented out with their compiled-in default |
| Agent guide | `AGENTS.md` (CLAUDE.md is just the `@AGENTS.md` shim — don't add content there) |

**Config-surface sync (mandatory).** Any change to a config struct in
`crates/dtfs-common/src/config.rs` (new/renamed/removed option, changed
default) MUST, in the **same commit**, update (1) the matching
`deploy/examples/*.toml` and (2) the matching reference page under
`docs/src/configuration/`. The `example_*_toml_parses` tests in
dtfs-common gate (1); keep the docs streamlined — one table row per
option, no duplicated complete-example blocks (the example files are the
single source for those).

Don't: invent a top-level directory without adding a row here first;
check in a root PROGRESS/status file (git log + plans are the status
sources); scatter loose `.md` notes through `crates/` or `tests/` (work
→ `plans/`, user-visible → the PR description, subsystem → `docs/`).
Release notes are auto-generated from merged PR titles at release time;
there is no in-repo changelog. PR title: `<type>: <short description>`
with type one of `bug` / `feat` / `int` (internal); the description is
an imperative, user-visible summary ≤72 chars, written for the operator
reading the release page. Label the PR with its type (`bug` / `feat` /
`int` — `int` is excluded from the notes: refactors, tests, CI,
docs-only); add `breaking` / `update-required` / `known-issue` when one
of those sections applies instead. Unlabeled PRs land in "Other
Changes". Grouping lives in `.github/release.yml`.

## B.7 Updating this file

This file is canonical; stale guidance is worse than none. When you
change operator-, agent-, or data-plane-visible behaviour, update
AGENTS.md in the **same commit**: § A.1 (topology), § A.3 (conventions/
invariants), § A.5 (memory strategy), § A.6 (integrity contract), § A.8
(file responsibilities / new crate), § B.4 (commit conventions), § B.5
(test conventions), § B.6 (new doc surface or moved artefact), § B.1-B.3
(workflow shape). If a session instruction overrode a rule, fold the
resolution in here. Note "AGENTS.md updated" or "no change needed
because …" in the commit description.
