# PLAN-CN-FOLLOWUPS — Open/deferred tasks after Bug CM + Bug CN

> Handoff plan. The 2026-06-10 `dtfs-lm` swap-wedge incident is resolved and
> shipped (see below); this file is the durable to-do for the leftover items so
> any agent/tool/model can pick them up. Keep it updated; delete it (§ B.2)
> once every task is ✅ and merged.

## Background (what already shipped)

Incident: a tiering `cache.promote` flood (root: monitor not leader-gated)
drove the 2 GB LELI managers past their `MemoryMax` and swap-wedged them, then
a rolling restart churned the cluster headless. Recovered by bumping
lm-mgr01/02/03 to **8 GB** (Proxmox) + deploying the fix; root-caused and
hardened in two bugs, both deployed to prod as a **dev build** and PR'd:

- **Bug CM** — leader-gate the health monitor (`monitor.rs`): only the raft
  leader evaluates + commits `UpdateNodeStatus`. Kills the cross-DC-learner
  flap that drove the flood.
- **Bug CN** — flood-aftermath robustness: scheduler **prune-first** in `tick`
  (persist terminal-task GC before advance), per-RPC **`MIGRATION_RPC_TIMEOUT`**
  (60 s) on `replicate_chunk`/`verify_chunk` (a stalled node can't hang the
  loop), and tiering **`sweep_dead_chunk_state`** (evict `access_stats`/
  `read_windows` for chunks gone from `sm.files`).

Both are on branch `bug/cm-cn-prod-stability` → **PR #36**
(https://github.com/lischetzke/dtfs/pull/36, label `bug`). **Operator merges +
releases — never cut a release from this plan.**

Prod state (2026-06-11): 3/3 Limburg voters on 8 GB, leader mgr03, scheduler
unstuck (migrations 2439→0), `status=ok`, 3/3 data Healthy, namespace intact
(13 files). The two learners (fsn/hun-mgr01) run the same dev build.

Access (per `environments.yml`, AGENTS § A.4, memory `dtfs-lm-cluster`):
build on the dtfs-builder VM (`debian@10.150.0.150`, key `~/.ssh/dtfs-lab/
id_ed25519`, `source ~/.cargo/env`); Minicluster `mini-mgr0{1,2,3}` via the
builder; prod over Tailscale (`root@<tailscale-ip>`, key `~/.ssh/id_ecdsa`).
Manager REST on :9402, unit `dtfs-manager`. Stale-artifact trap: `touch` edited
files on the builder before deploying (§ A.4).

## Goals

Finish the deferred items cleanly, each as an independent dev-build PR.

## Non-goals / locked-in decisions

- **No release.** Operator cuts releases; no `v0.1.6`, no `dtfs-release.sh`.
- **No volume promote-policy change.** Cache promotes are expected by design
  (folder-readahead warms the hot cache). The fix was bounding the manager's
  memory under that volume, not dampening promotes.
- **No full snapshot server-streaming** (over-engineering for an 8 GB box; the
  RPC is a native non-proto unary call → would need cross-version coordination).
  Do the internal heap win instead (CNF-4).

## Recommended order: CNF-1 → CNF-2 → CNF-3 → CNF-4 → CNF-5

---

### CNF-1 — Resolve `anchor_pending=1` ✅  (correctness/observability)

Prod persistently reports `anchor_pending=1` with an empty migration queue
(count=0): the live scheduler is NOT scheduling an anchor repair for one chunk.

- Investigate (read-only): `GET /api/v1/files/inspect?path=<file>` across the
  13 files to find the chunk whose replicas don't satisfy the anchor
  (`datacenter=limburg`, `required_tier=cold`). Cross-read the projection
  `count_anchor_pending_files` / `anchor_satisfied_by` (`api.rs:705-749`) vs
  the planner `plan_anchor_lag_repair` (`scheduler.rs:~2994-3056`) + caller
  `check_anchor_lag` (`scheduler.rs:~625-730`).
- Two likely causes + fix:
  1. **Projection false-positive (most likely)**: `api.rs:~738`
     `info.disks.get(e.disk_id as usize)` drops a placement via `filter_map`
     when `disk_id` is out of `Vec` bounds (disk list reindexed) → anchor check
     sees fewer replicas than exist. Fix: resolve the disk by stable
     `disk_id`/uuid, not `Vec` index, in BOTH the projection and the planner's
     satisfaction check so they agree.
  2. **Real planner gap**: chunk genuinely lacks the cold/Limburg copy and the
     planner finds no eligible target (the only Limburg data node already holds
     it on a *different* tier → Gate-3 excludes the node outright). Fix: exclude
     only `(node, target-tier)`, not the whole node (Bug BH `(node_id, disk_id)`
     keying), so a multi-tier node can still receive the cold anchor copy.
- Files: `crates/dtfs-manager/src/api.rs`, `crates/dtfs-manager/src/scheduler.rs`
  (reuse `live_replica_tier` / `(node_id, disk_id)` patterns).
- Tests: unit test the disk_id-robust resolution (pure). Live: `anchor_pending`
  → 0 after deploy. Done when: prod `anchor_pending=0`, cause known, PR'd.

### CNF-2 — Bug CN release case + configurable task retention ✅  (test discipline)

Shipped (branch `batch/cnf-2-task-retention-config`):

- `raft.failed_task_retention_secs` / `raft.done_task_retention_secs` in
  `RaftConfig` (serde defaults 1800/300 = the old hardcoded consts, now
  removed; prod behaviour unchanged), threaded via `main.rs` into
  `TaskScheduler::new` (secs→ms with `saturating_mul`); `should_retain_task`
  takes the windows as params (pure, unit-tested incl. a configurable-window
  test) and the Bug CN pre-tick `prune_terminal_tasks` consumes them
  (rebase over #36 moved the prune from `advance_tasks` to tick start).
- Config-surface sync: `deploy/examples/manager.toml` `[raft]` +
  `docs/src/configuration/manager.md` table rows; `example_manager_toml_parses`
  + `default_manager_toml` assert the defaults.
- Release case `tests/release/cases/61-scheduler-backlog-drains.sh`: short
  retention (Done 30s / Failed 45s) on all managers (leader-last restart),
  seed cold-born files, `set-mandatory warm=1` → real warm-backfill tasks →
  Done batch; assert the Done backlog prunes within 150s of first
  observation (discriminates the configured 30s window from the 300s
  compiled-in default), then stop a data node and assert the second
  backfill wave / any Failed stragglers still drain (Bug CN tick survival).
  NOT in `QUICK_CASES` (2x rolling manager restarts, ~5-10 min; § B.5 caps
  quick cases at cheap critical-path) — deviations from the original
  sketch, see Deviations.

### CNF-3 — RocksDB per-CF memory bound ✅  (hardening)

Raft log store CFs use `Options::default()` (`crates/dtfs-raft/src/log_store.rs
:110-111`) — no write-buffer/block-cache cap. Extend `RocksDbLogOpts`
(`log_store.rs:~60`, already carries `keep_log_file_num`/`max_manifest_bytes`)
with `write_buffer_mb` + `block_cache_mb`; apply `set_write_buffer_size` + a
single SHARED `rocksdb::Cache` across both CFs in `new_with_opts`; thread from
`RaftConfig` (`config.rs:~394`) like the existing `rocksdb_*` fields
(`main.rs:~98`), conservative defaults (~64 MB write buffer / shared 64 MB
cache). Config-surface sync (§ B.6). Tests: `log_store` open-with-bounded-opts +
small write workload; `cargo test -p dtfs-raft`/`-p dtfs-common`. Low urgency
(8 GB), but correct defense-in-depth. Done when: bounded from config + docs +
parse-tested, PR'd.

### CNF-4 — Bound the snapshot-send heap ✅  (hardening, simpler win)

`full_snapshot` (`crates/dtfs-raft/src/network.rs:199-274`, `SnapshotPayload`)
reads the whole snapshot into `Vec<u8>`, base64-encodes, JSON-serializes per
learner (+33% bloat, serial). Internal-only fix (NO wire/streaming change):
(1) drop base64 — serialize bytes via `serde_bytes` in `SnapshotPayload`
(sender `network.rs:~214-225`, receiver `crates/dtfs-manager/src/server.rs:
~108-137`); confirm wire-compat across versions, else gate behind the dev-build
rollout. (2) cap concurrent snapshot sends (semaphore around the send path).
Tests: `SnapshotPayload` round-trip unit test; Minicluster rolling restart →
learner InstallSnapshots to lag 0. If step 1 is wire-incompatible, ship only
step 2 + note it. Done when: no base64+JSON double-encode, concurrency capped,
PR'd.

### CNF-5 — Ops housekeeping ⬜  (no code, no release)

1. **Standardize manager `MemoryMax`.** Recovery left ad-hoc transient
   `systemctl set-property` caps (mgr01/03 = 6 G, mgr02 = 5 G, hun-mgr01 =
   1.8 G; the transient values were lost on mgr02's reboot). All three LELI
   voters are now 8 GB: set consistent **persisted** drop-ins under
   `/etc/systemd/system/dtfs-manager.service.d/` (e.g. voters `MemoryMax=6G`/
   `MemoryHigh=5G`; learners sized to RAM) so they survive reboots. `MemoryMax`
   stays the systemd backstop alongside the app `[memory]` ladder.
2. **Delete `plans/active/PLAN-BUG-CM-monitor-leader-gate.md`** once PR #36 is
   merged (§ B.2; git history retains it). After merge, small commit.

Verify: `systemctl show dtfs-manager -p MemoryMax -p MemoryHigh` per node shows
the standardized values + survives `daemon-reload`. Done when: caps consistent +
persisted; CM plan deleted post-merge.

## Cross-cutting invariants

- Build via a wrapper / `SOURCE_DATE_EPOCH` for a fresh `BUILD_DATE` (§ A.3.5);
  deploy as dev builds (`v0.0.0-dev`). Operator owns merge + release.
- Update `AGENTS.md` (§ A.8 / § A.5 / config-surface § B.6) in the SAME commit
  as each behaviour/config change.
- Don't touch the anchor invariant, the promote policy, or the monitor/scheduler
  gates already shipped in CM/CN.

## Verification log

- **CNF-3** 2026-06-11 — `RocksDbLogOpts` gained `write_buffer_mb` /
  `block_cache_mb` (default 64/64 MiB); `new_with_opts` builds per-CF
  `Options` with `set_write_buffer_size` + ONE shared
  `Cache::new_lru_cache` via `BlockBasedOptions::set_block_cache` on both
  CFs (rocksdb 0.22 API verified in registry source; the crate clones the
  cache into the DB outlive set). Threaded from `RaftConfig.
  rocksdb_{write_buffer,block_cache}_mb` (main.rs); config surface synced
  (deploy/examples/manager.toml + docs/src/configuration/manager.md).
  Failing-first: new test `log_store_bounded_memory_write_workload`
  (open with custom bounds, 100-entry append + read-back, reopen) failed
  to compile (E0560/E0609 on the new fields), then passed after the
  implementation. Builder runs: `cargo test -p dtfs-raft --release` (62
  passed), `cargo test -p dtfs-common --release` (67 passed, incl.
  `example_manager_toml_parses`), full `cargo test --release --workspace`
  green. Internal hardening, no cluster-visible behaviour → no release
  case (§ B.5).
- **CNF-4** 2026-06-11 — failing-first: added `network.rs`
  `cnf4_snapshot_send_tests` (base64 round-trip vs the receiver decoder +
  permit-bound test) before the impl; `cargo test -p dtfs-raft --release` on
  dtfs-builder failed with E0425 (`base64_decode`,
  `acquire_snapshot_send_permit`, `SNAPSHOT_SEND_CONCURRENCY` missing). After
  impl: `cargo test -p dtfs-raft --release` → 63 passed, 0 failed (incl.
  `snapshot_send_permits_bound_concurrency`,
  `base64_round_trip_with_receiver_decoder`); `cargo test -p dtfs-manager
  --release` → 238 passed; full `cargo test --release --workspace` → all
  suites ok, 0 failed. Wire format unchanged; live InstallSnapshot
  verification happens at the operator's next rolling deploy.
- **CNF-1** (2026-06-11; suites re-verified green 2026-06-12): replica
  disk/tier resolution indexed `info.disks`
  by `e.disk_id as usize` (Vec position) instead of matching the
  `DiskInfo.disk_id` field, so a non-dense/reordered disk list makes the
  projection (and several planners) resolve the wrong disk or drop the
  placement. Fixed via new `dtfs_raft::NodeInfo::disk(disk_id)`
  (find-by-field) at ALL read sites keyed by `ChunkLocationEntry.disk_id`:
  `api.rs::count_anchor_pending_files`, `scheduler.rs::check_anchor_lag`,
  `check_misplaced_rebalance`, `live_replica_tier`,
  `placement_state.rs::resolve_dc_tier`, tiering
  `build_av_cache_snapshot` (id_to_tiers table dropped, uses the helper),
  `state_machine.rs::apply_remove_chunk_location` (enforce_anchor), plus the
  derived `FlushNodeSummary.disk_tiers` table (now `(disk_id, tier)` pairs).
  The WRITE side was converted too: `scheduler.rs::register_replica_location`
  (AV-5-9) recorded `resolved_disk_id` as a Vec POSITION
  (`.position(|d| d.tier == tier)`); now `resolve_disk_id_for_tier` records
  the matching disk's `disk_id` field, so reader and writer agree.
  Failing-first on the builder: 4 sparse-disk_id read-side tests observed
  red pre-fix (e.g. `anchor_projection_resolves_disk_by_stable_id`:
  `left: 1, right: 0`; `cnf1_remove_chunk_location_anchor_resolves_disk_by_stable_id`:
  `RemoveChunkLocation refused: would strand anchor`) and
  `new_replica_records_disk_id_field_not_position` red pre-write-fix
  (`left: 0, right: 1`), all green post-fix; full dtfs-manager + dtfs-raft
  suites and the full workspace suite green.
- **CNF-2** (2026-06-12): failing-first by construction — parameterising
  `should_retain_task` breaks every pre-existing call site/test (E0061)
  until updated (§ B.5: compile error counts). Builder:
  `cargo test -p dtfs-common --release` 67/0 (incl.
  `example_manager_toml_parses` + `default_manager_toml` asserting the
  1800/300 defaults), `cargo test -p dtfs-manager --release` 239/0 (incl.
  `cnf2_retention_windows_are_configurable`), full workspace 0 failed.
  Live on the Minicluster (dev build 20260612-071946, rolling deploy
  mgr02→mgr03→leader mgr01, quorum stable, term 230→231): case 61 PASS —
  set-mandatory warm=1 produced 52 Done backfill tasks, Done backlog
  drained 28s after first observation (configured 30s window; the 300s
  default would still retain them at the 150s deadline), second backfill
  wave with mini-data02 down also pruned (tick survival). Case 55 re-run
  as regression (see PR).

## Deviations

- **CNF-4 step 1 (drop base64) rejected** — the transport is a JSON-*string*
  RPC; serde_json serializes `serde_bytes` as an int array (larger than
  base64), so dropping base64 is a net loss without a wire change. Shipped
  instead: process-wide `SNAPSHOT_SEND_PERMITS` semaphore (2 concurrent
  snapshot sends, permit held across the cancel select), copy reduction on
  the send path (`Cursor::into_inner` instead of a `bytes_from_cursor` copy,
  drop raw bytes after base64-encode, drop the payload struct after JSON
  serialize, drop the RPC envelope after `to_vec`), shared encoder/decoder
  (`dtfs_raft::network::base64_{encode,decode}`, receiver's local copy
  removed) so the pair is round-trip tested. Wire format byte-identical
  (mixed-version safe).
- **CNF-1**: the live read-only inspection of the 13 prod files and the
  post-deploy `anchor_pending=0` check are deferred to the operator (this
  worker has no prod/Tailscale access). Cause 1 (projection/planner
  false-negative on non-dense disk ids) is confirmed by unit test; cause 2
  (Gate-3 node-level exclusion) was inspected and is NOT present — the
  planner's target selection already excludes only tier-holders, so no
  change there.
- **CNF-1 caveat**: registration currently assigns `disk_id` positionally
  (`dtfs-data/src/disk.rs` / `main.rs`: `disk_id: i as u32`, and
  RegisterNode replaces `NodeInfo.disks` wholesale), so in states produced
  by a single registration `disk_id == Vec index` and the fix is
  behaviour-neutral. It removes the latent projection-vs-planner divergence
  and makes reader+writer agree on the field-keyed contract, but the live
  `anchor_pending=1` may have a different trigger (e.g. a stale
  `ChunkLocationEntry.disk_id` from a since-shrunk disk config — invisible
  to BOTH positional and field lookups). If `anchor_pending` is still 1
  after deploy, the next suspect is disk-id reassignment on re-register
  (Bug BJ; the persisted BN-1 uuid is the available stable identity) — do
  NOT re-investigate the lookup sites.
- **CNF-2**: the sketch's Failed-batch generator ("stop a data node →
  Failed tasks") is not provokable on the Minicluster: every data node
  carries every tier, and `plan_path_migration`'s `already_satisfied`
  check is node-tier-based, so `/api/v1/migrations` schedules nothing
  (first run SKIPped). Case 61 instead drives REAL warm backfills via
  `set-mandatory warm=1` and asserts the configured **Done** window
  (discriminating 150s deadline); the Failed-window assert runs
  opportunistically when the node-stop catches in-flight copies. Also
  NOT added to `QUICK_CASES` (sketch said add it): two rolling manager
  restarts put it at ~5-10 min, over the quick-case budget.
- **CNF-2 incidental fix**: `tests/release/lib/common.sh::mgr_post_leader`
  only recognised the bare `"error":"has to forward request"` follower
  rejection; set-mandatory wraps it as `"raft write failed: has to
  forward request …"`, so the helper returned the follower error instead
  of walking on (observed live: first case-61 run FAILed on it). Match
  loosened to the invariant substring.
