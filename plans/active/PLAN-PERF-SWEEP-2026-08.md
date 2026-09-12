# PLAN-PERF-SWEEP-2026-08 — Plex-workload performance sweep (WI-01/04/05/09) + review remediation

Four work items applied in one proactive sweep against the Plexmedia reference
deployment (AGENTS.md A.1), then remediated against an Opus-tier review
(`scratchpad/sweep/REVIEW.md`). This plan supersedes `PLAN-WI-05-prefetch-fanout.md`
(deleted in the same change that adds this file).

## Goals

**WI-01 — batch the write flush.** `dtfs-fuse` amortizes `AllocateChunks` +
`CommitChunkLocations` round trips over `write.flush_batch_chunks` (default 4)
complete chunks instead of firing one round trip per chunk, cutting manager
round trips on a torrent-landing write burst (A.1 "absorb it without
OOM-killing anything").

**WI-04 — size-scaled replication deadlines + per-target copy cap.** The
manager's outer `ReplicateChunk` deadline and the data node's own inner
transfer budget both scale with `chunk_bytes` instead of being fixed
constants that inverted (outer < inner) for chunks above ~50 MB with 6-way
fan-out; `MIGRATION_COPIES_PER_TARGET` caps concurrent copies to any single
target so all fan-out doesn't slam the anchor DC's one data node at once.

**WI-05 — bounded prefetch fan-out + join.**
- Cap concurrent prefetch `GetChunk` streams (`readahead.prefetch_max_inflight`,
  default 2) so an uncapped fan-out cannot starve the near chunk on a slow
  cross-DC link (e.g. the 40 Mbps LELI path).
- Keep the prefetch window (`prefetch_chunks`) as deep as configured; only
  bound how many of those fetches are on the wire at once, nearest first.
- Fix the dormancy bug: `spawn_prefetch`'s old `cache_bytes() >=
  max_cache_bytes` gate went permanently true under the default power-of-two
  sizing (64 MB cache / 4 MB chunks) after one `cache_mb` of reads, and
  silently killed all further prefetch.
- Foreground read joins an in-flight prefetch instead of duplicating it, but
  only a prefetch already on the wire, bounded by a small constant (see
  Locked-in decisions).

**WI-09 — fire-and-forget migration hint.** `MigrationTrigger::on_remote_read`'s
`AllocateChunks` RPC (the migration hint sent when a read lands on a
non-preferred replica) fires in the background instead of being awaited
inline after the chunk bytes are already in hand, so a manager outage/election
no longer stalls an in-hand read for up to `MANAGER_WALK_DEADLINE_MS` (15s)
per chunk.

**Remediation (this pass).** Fix the blockers/highs/mediums the review found
in the above four work items before they land: an unbounded production
`Vec<String>` leak in the readahead engine (B1), a `join_pending` join that
could double the P0-2 replica-fallover budget (B2), a byte-unbounded write
batch on large-chunk volumes (H1), a deleted CF-5 Shed gate with no
replacement (H2), release cases 96/99 that cannot pass on any cluster (B3,
H3), an unbounded `pending_migrations` map (H4), a silent walk-back of Bug
CN's 60s scheduler-stall bound (B4), a dead boundary test superseded by a
stronger one (M1), a near-vacuous synchronous-function timeout test (M2), no
plan file covering three of the four work items (M4), and a case 97 that
could strand the cluster at 128 MB chunks on a hard kill plus over-claims
being a pre-fix regression gate (M6).

## Non-goals

- No manager, Raft, or proto change for the FUSE-side items (WI-01, WI-05,
  WI-09) — `dtfs-fuse` only.
- No change to `store_in_cache` eviction-victim selection.
- No wiring of the dead `evict()` on read consumption.
- No cross-file prefetch (`prefetch_next_files`) behaviour change.
- No `StateMachineData` change anywhere in this sweep — A.3.1 (snapshot
  `#[serde(default)]`) does not apply to any task below.
- WI-04 does not touch replica placement, anchor policy, or the mandatory
  vs cache replica classes (A.3.4) — only RPC deadlines and copy
  concurrency.

## Locked-in decisions

- `prefetch_max_inflight: u32`, default 2, `.max(1)` at every consumer.
- Near-first ordering via a oneshot chain (`prev_acquired`), not a bare FIFO
  semaphore alone — spawn order does not guarantee acquire order.
- `pending` value is `PendingPrefetch { handle, done: watch::Receiver<bool>,
  started: Arc<AtomicBool> }`.
- **B2 (user-locked, replaces the original WI-05 plan's "join bounded by
  `client.chunk_read_timeout_secs`" decision):** `read()` joins an in-flight
  prefetch ONLY IF that prefetch has already ACQUIRED its `prefetch_slots`
  permit (i.e. it is genuinely on the wire, not queued behind the
  `prev_acquired` chain). The join is bounded by a small constant,
  `readahead::PREFETCH_JOIN_TIMEOUT` = 2s, never by `chunk_read_timeout_secs`.
  Only the first candidate (`candidates.first()`) is tried — prefetch targets
  `read_candidates`' head (REV-2-3), so no other key can hold a pending entry
  for this chunk. On join timeout, or on a queued-but-not-started prefetch,
  `join_pending` returns `None` immediately and `read()` falls through to its
  ordinary candidate loop exactly as before, preserving the P0-2 fallover
  budget `tests/release/cases/69-read-hung-node-fallover.sh` asserts
  (`ELAPSED <= TMO*3`, `TMO=12`).
- **B4 (user-locked):** do not raise the manager's outer scheduler stall
  ceiling to fix the WI-04 deadline inversion. Lower the data node's inner
  `REPLICATE_TIMEOUT_FLOOR` to 30s instead, keep size-scaling for large
  chunks, and cap the outer deadline at 300s (`MIGRATION_RPC_CEILING`) so a
  128 MB-chunk volume can never park the scheduler for minutes. The inner
  deadline stays strictly below the outer at every chunk size (the original
  inversion must not come back at the cap boundary either). Resulting table
  (`crates/dtfs-common/src/replication.rs`):

  | chunk | inner (data node, `replicate_transfer_timeout`) | outer (manager, `migration_rpc_timeout`) |
  |---|---|---|
  | 4 MB (default) | 30 s | **60 s** |
  | 32 MB | 32 s | 62 s |
  | 64 MB | 64 s | 94 s |
  | 128 MB (largest supported) | 128 s | 158 s |
  | 256 MB | 256 s | 286 s |
  | >= 270 MB (`REPLICATE_TIMEOUT_CEILING`, cap) | 270 s | **300 s** (`MIGRATION_RPC_CEILING`) |

  `MIGRATION_RPC_MARGIN` = 30s unchanged; `REPLICATE_TIMEOUT_CEILING` (270s) +
  `MIGRATION_RPC_MARGIN` (30s) = `MIGRATION_RPC_CEILING` (300s) exactly, so the
  outer never exceeds its ceiling and the inner never touches it.
- **H1:** the write-flush batch is clamped both by chunk count
  (`1..=MAX_FLUSH_BATCH_CHUNKS` = 32) and by bytes
  (`FLUSH_BATCH_MAX_BYTES` = 64 MB, `writebuf::effective_batch_chunks`), so a
  large-chunk volume degrades to per-chunk flushing instead of buffering
  hundreds of MB to GB per open file.
- **H2:** the CF-5 Shed rung is served by an explicit
  `prefetch_enabled: Arc<AtomicBool>` gate
  (`ReadAheadCacheController::set_prefetch_enabled`), pinned false at Shed by
  `run_readahead_pressure_ladder` and true at Normal/Warn, checked in
  `on_chunk_read` before any prefetch spawns. This replaces the deleted
  `cache_bytes() >= max_cache_bytes` dormancy gate, which the WI-05 sweep
  removed because it observed only stored bytes and went permanently true
  under the default sizing; a byte gate is not re-added (A.3.2 — in-flight
  bytes are already bounded by `prefetch_slots`).

## Data-model / config-surface changes

- `crates/dtfs-common/src/config.rs`: `[readahead] prefetch_max_inflight: u32`
  (default 2, `default_prefetch_max_inflight`); `[write] flush_batch_chunks: u32`
  (default 4, `default_flush_batch_chunks`) — the byte clamp
  (`FLUSH_BATCH_MAX_BYTES`) is applied internally in `writebuf.rs`, not a
  second config key. Both mirrored in `deploy/examples/fuse.toml` and
  `docs/src/configuration/fuse-client.md` (config-surface sync, B.6).
- `crates/dtfs-common/src/replication.rs` (new file, WI-04): constants
  `REPLICATION_FLOOR_BYTES_PER_SEC`, `REPLICATE_TIMEOUT_FLOOR` (30s),
  `MIGRATION_RPC_MARGIN` (30s), `MIGRATION_RPC_CEILING` (300s),
  `REPLICATE_TIMEOUT_CEILING` (270s); functions `replicate_transfer_timeout`,
  `migration_rpc_timeout`. Consumed by `dtfs-data/src/replication.rs` and
  `dtfs-manager/src/scheduler.rs` — no code change needed at either call
  site for the B4 remediation, only the shared constants moved.
- `crates/dtfs-fuse/src/readahead.rs`: `PendingPrefetch { handle, done,
  started: Arc<AtomicBool> }` (the `started` flag replaces the deleted,
  unbounded `started_fetches: Vec<String>` field — see Deviations);
  `PREFETCH_JOIN_TIMEOUT: Duration` = 2s; `prefetch_enabled: Arc<AtomicBool>`
  on both `ReadAheadEngine` and `ReadAheadCacheController`.
- `crates/dtfs-fuse/src/migration.rs`: `MAX_PENDING_MIGRATIONS: usize` = 4096
  bound on `pending_migrations: DashMap<String, Instant>`, with
  `prune_expired()` invoked from `on_remote_read` when the map reaches the
  cap (opportunistic pruning, no new background task).
- No `StateMachineData` field anywhere in this sweep — A.3.1 does not apply.

## Task list

Phase 1 = WI-01, 2 = WI-04, 3 = WI-05, 4 = WI-09, 5 = release cases +
verification. Already-implemented work is ✅; every remediation item below
is 🟦 pending the builder + Minicluster run recorded in the Verification log.

| ID | Status | Description | Files |
|----|--------|--------------|-------|
| PERF-SWEEP-1-1 | ✅ | write buffer batches `flush_batch_chunks` complete chunks per `flush_segments_to_cluster` round trip | `crates/dtfs-fuse/src/writebuf.rs`, `filesystem.rs` |
| PERF-SWEEP-1-2 | ✅ | H1: `effective_batch_chunks` clamps by both count (`1..=32`) and bytes (`FLUSH_BATCH_MAX_BYTES` = 64 MB); `wi01_h1_effective_batch_is_byte_bounded` + `wi01_h1_large_chunk_volume_flushes_per_chunk` pass | `crates/dtfs-fuse/src/writebuf.rs` |
| PERF-SWEEP-2-1 | ✅ | `dtfs_common::replication` module: `replicate_transfer_timeout`/`migration_rpc_timeout` helpers, both call sites migrated | `crates/dtfs-common/src/replication.rs`, `dtfs-data/src/replication.rs`, `dtfs-manager/src/scheduler.rs` |
| PERF-SWEEP-2-2 | ✅ | `MIGRATION_COPIES_PER_TARGET` per-target copy gate alongside the existing global `MIGRATION_COPY_CONCURRENCY` cap | `crates/dtfs-manager/src/scheduler.rs` |
| PERF-SWEEP-2-3 | ✅ | B4: `REPLICATE_TIMEOUT_FLOOR` lowered 120s->30s, `MIGRATION_RPC_CEILING` (300s) + `REPLICATE_TIMEOUT_CEILING` (270s) added so `migration_rpc_timeout(4 MB) == 60s` (Bug CN bound intact) and inner stays strictly below outer at every size; `b4_default_chunk_keeps_the_bug_cn_sixty_second_bound` + `b4_inner_deadline_is_strictly_below_outer_at_the_cap_boundary` + updated `manager_deadline_exceeds_data_node_deadline` / `transfer_budget_scales_with_chunk_size` pass | `crates/dtfs-common/src/replication.rs` |
| PERF-SWEEP-2-4 | ✅ | M1: dead boundary test `copy_concurrency_is_bounded` (vacuous once every task shares one target) deleted, superseded by `copies_across_targets_use_the_global_cap` | `crates/dtfs-manager/src/scheduler.rs` |
| PERF-SWEEP-3-1 | ✅ | `readahead.prefetch_max_inflight` config key + example/doc sync | `crates/dtfs-common/src/config.rs`, `deploy/examples/fuse.toml`, `docs/src/configuration/fuse-client.md` |
| PERF-SWEEP-3-2 | ✅ | `prefetch_slots` semaphore + near-first oneshot chain (`prev_acquired`); `wi_05_prefetch_fan_out_is_capped_and_near_first` passes | `crates/dtfs-fuse/src/readahead.rs` |
| PERF-SWEEP-3-3 | ✅ | dormancy gate (`cache_bytes() >= max_cache_bytes`) removed from `spawn_prefetch`; `wi_05_prefetch_runs_when_cache_is_at_budget` passes | `crates/dtfs-fuse/src/readahead.rs` |
| PERF-SWEEP-3-4 | ✅ | `pending` -> `PendingPrefetch`, `join_pending()`, `DtfsFilesystem::read()` joins before duplicating; `wi_05_foreground_join_returns_prefetched_bytes` passes | `crates/dtfs-fuse/src/readahead.rs`, `filesystem.rs` |
| PERF-SWEEP-3-5 | ✅ | B1: `started_fetches: Vec<String>` (unbounded, process-lifetime) deleted; replaced by `PendingPrefetch.started: Arc<AtomicBool>`, set true only after `prefetch_slots.acquire_owned()` succeeds and dropped with its entry on task completion — bounded by the prefetch window, not by uptime | `crates/dtfs-fuse/src/readahead.rs` |
| PERF-SWEEP-3-6 | ✅ | B2: `join_pending` refuses (`None`, no await) a `started == false` entry; `filesystem.rs` join site rewritten to try only `candidates.first()`, bounded by `PREFETCH_JOIN_TIMEOUT` (2s), never `chunk_read_timeout_secs`; `b2_join_pending_refuses_a_queued_prefetch` + `b2_join_pending_is_bounded_by_the_two_second_constant` + updated `wi_05_foreground_join_returns_prefetched_bytes` pass | `crates/dtfs-fuse/src/readahead.rs`, `filesystem.rs` |
| PERF-SWEEP-3-7 | ✅ | H2: `prefetch_enabled: Arc<AtomicBool>` gate on `ReadAheadEngine`/`ReadAheadCacheController`, checked in `on_chunk_read`; `run_readahead_pressure_ladder` pins it false at Shed, true at Normal/Warn; `h2_shed_gate_stops_new_prefetch` passes | `crates/dtfs-fuse/src/readahead.rs`, `main.rs` |
| PERF-SWEEP-3-8 | ✅ | AGENTS.md A.3.2 + A.8 corrected: dormancy-gate claim scoped to default power-of-two sizing only (not unconditional), `prefetch_enabled` documented as the CF-5 Shed rung, join site documented with `PREFETCH_JOIN_TIMEOUT` | `AGENTS.md` |
| PERF-SWEEP-4-1 | ✅ | `MigrationTrigger::spawn_on_remote_read` fires `AllocateChunks` in the background (`tokio::spawn`), no longer awaited inline after chunk bytes are already fetched, no longer clears the cooldown entry on failure | `crates/dtfs-fuse/src/migration.rs`, `filesystem.rs` |
| PERF-SWEEP-4-2 | ✅ | H4: `MAX_PENDING_MIGRATIONS` (4096) bound on `pending_migrations`, `prune_expired()` wired into `on_remote_read` (opportunistic, at-cap only) instead of being dead code; `h4_pending_migrations_is_bounded` passes | `crates/dtfs-fuse/src/migration.rs` |
| PERF-SWEEP-4-3 | ✅ | M2: near-vacuous `spawn_on_remote_read_returns_before_rpc_completes` (wrapped a synchronous function in a timeout) replaced with `wi09_spawn_on_remote_read_runs_nothing_inline`, an ordering test that asserts nothing ran inline before the call returned; the `known_locations` guard is NOT covered there (it lives in `filesystem.rs`, outside `MigrationTrigger`'s reach) and remains covered only by inspection; `drop(listener)` contradiction removed with the old test | `crates/dtfs-fuse/src/migration.rs` |
| PERF-SWEEP-5-1 | ✅ | B3a: case 96 queries `/api/v1/files/inspect` and the `AllocateChunks` journal grep with the mount-relative `API1`/`API2` paths (`${F1#$MOUNT}`), not the `$MOUNT`-prefixed FUSE path | `tests/release/cases/96-write-batch-flush.sh` |
| PERF-SWEEP-5-2 | ✅ | B3b: case 96's node-DC lookup parses `NodeResponse.location` (`"region/datacenter/rack/host"` string) instead of treating it as an object with a `datacenter` key | `tests/release/cases/96-write-batch-flush.sh` |
| PERF-SWEEP-5-3 | ✅ | B3c: `client_flush_batch_chunks` also reads `[client].chunk_size_mb` and computes the effective batch (`min(configured, 64 MB / chunk_size)`, clamped `1..=32`), mirroring `writebuf::effective_batch_chunks`, so `EXPECT_MIN` is correct on a large-chunk client | `tests/release/cases/96-write-batch-flush.sh` |
| PERF-SWEEP-5-4 | ✅ | H3: case 99's warm-up performs a real 1 MB `dd` partial read (not just `stat`) before the manager ports are DROPped, so `GetFileLocations`/`known_locations` are actually populated pre-outage | `tests/release/cases/99-read-survives-manager-outage.sh` |
| PERF-SWEEP-5-5 | ✅ | M6: case 97 restores `defaults.chunk_size_mb` immediately after the step-4 write (before the 600s step-5 wait, not only in `case_cleanup`), traps `EXIT INT TERM`, and states the SCOPE caveat (steady-state coverage, not a pre-fix regression gate) in its header; same B3b node-location-string bug fixed in its anchor-DC fallback lookup | `tests/release/cases/97-large-chunk-anchor-backfill.sh` |
| PERF-SWEEP-5-6 | ✅ | M4: this plan file created covering all four work items + the remediation, `PLAN-WI-05-prefetch-fanout.md` deleted | `plans/active/PLAN-PERF-SWEEP-2026-08.md` |
| PERF-SWEEP-5-7 | 🟦 | full builder suite (`cargo check --workspace --all-targets`, `cargo test --workspace`) run on 10.150.0.150, plus cases 96/97/99 executed on the Minicluster with output recorded in the Verification log below | `plans/active/PLAN-PERF-SWEEP-2026-08.md` |

## Cross-cutting invariants

- Anchor invariant (A.3.4): untouched by any task above — no replica
  create/remove logic changed; WI-04 only changes RPC deadlines and copy
  concurrency, not placement.
- No whole-chunk heap accumulation growth (A.3.2): prefetch in-flight bytes
  are `prefetch_max_inflight * chunk_size`, down from `prefetch_chunks *
  chunk_size`; the write buffer's un-committed ceiling is `FLUSH_BATCH_MAX_BYTES`
  = 64 MB per open file (H1), not the unbounded `flush_batch_chunks *
  chunk_size` the pre-remediation diff allowed at large chunk sizes.
- CO-5-17 ordering preserved: `started_rx` gate stays first in
  `spawn_prefetch`; the `prev_acquired` await and the `prefetch_slots`
  permit acquire go strictly after it.
- CF-5 ladder: Warn halves `readahead.cache_mb`; Shed both calls
  `drop_unconsumed()` (aborts + clears in-flight prefetch) AND pins
  `prefetch_enabled` false (H2) so the next `on_chunk_read` cannot
  immediately re-spawn what Shed just dropped.
- P0-2 replica fallback: `join_pending` returning `None` (no pending entry,
  not started, failed, or timed out) always falls through to the ordinary
  candidate loop; the join can add at most one `PREFETCH_JOIN_TIMEOUT` (2s)
  in front of that loop, per chunk (B2) — case 69's `ELAPSED <= TMO*3`
  budget (`TMO=12`) is unaffected.
- Bug CN's 60s scheduler-stall bound holds at the default 4 MB chunk size
  after WI-04 (B4): a stalled data node cannot hang a `buffer_unordered`
  copy round or the 30s scheduler tick beyond the same margin it always had;
  large-chunk volumes get a longer but hard-capped (300s) budget instead of
  an unbounded one.
- The FUSE `pending_migrations` cooldown map is bounded at
  `MAX_PENDING_MIGRATIONS` (4096) via opportunistic `prune_expired()` (H4),
  not unbounded process-lifetime growth.

## Verification log

- 2026-08-24: `cargo check --workspace --all-targets` and `cargo test
  --workspace` in a rust 1.94 Linux container: 753 passed, 0 failed,
  including all 15 new tests (covers PERF-SWEEP-1-* through 4-* as
  implemented at that point; does not yet cover the G5 release-case edits
  in this file, which are shell-only).
- Builder run (10.150.0.150) and Minicluster run for
  PERF-SWEEP-5-7 (cases 96/97/99): pending — record command + output here
  when run.

## Deviations

- The WI-05 plan's original `started_fetches: Vec<String>` deviation (kept
  unconditional "to avoid a cfg-gated struct-literal field") is VOID: B1
  removed the field entirely, replaced by `PendingPrefetch.started:
  Arc<AtomicBool>` (PERF-SWEEP-3-5), which needed no `#[cfg(test)]` dodge.
- Two extra debug log lines survive from the original WI-05 work, needed so
  release case 98 can observe "no duplicate fetch" and "prefetch still runs
  past cache_mb" without a new metrics endpoint: `"chunk fetch complete
  (foreground miss)"` in `filesystem.rs`, and `"readahead prefetch join
  hit"` in the join path (also `filesystem.rs`).
- Release case for WI-05 is numbered `98-readahead-prefetch-bounded.sh`, not
  `96` — `96` was already taken by `96-write-batch-flush.sh` (WI-01) when
  WI-05 was implemented.

**Retention: delete this file once every task above is ✅ and merged (B.2).**
