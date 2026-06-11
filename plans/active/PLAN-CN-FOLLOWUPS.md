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

### CNF-1 — Resolve `anchor_pending=1` ⬜  (correctness/observability)

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

### CNF-2 — Bug CN release case + configurable task retention ⬜  (test discipline)

Ship the § B.5 release-gate e2e for Bug CN. Blocked by the 30-min hardcoded
`FAILED_TASK_RETENTION_MS` (`scheduler.rs:3035`), so make it configurable then
test fast.

- Thread `failed_task_retention_secs` (+ opt. `done_task_retention_secs`) into
  `RaftConfig` (`crates/dtfs-common/src/config.rs:~394`) with serde defaults =
  the current consts (prod behaviour unchanged); consume in the scheduler;
  populate in `main.rs:~98` like the existing `rocksdb_*` fields.
- Config-surface sync (§ B.6, gated by `example_*_toml_parses`):
  `deploy/examples/manager.toml` `[raft]` + `docs/src/configuration/`.
- Release case `tests/release/cases/NN-scheduler-backlog-drains.sh`: short
  failed-retention; stop a data node to force a Failed-task batch, restart it,
  assert (a) the scheduler keeps ticking (RPC timeout → no hang) and (b) the
  Failed backlog prunes to ~0 within a couple ticks. `trap case_cleanup EXIT`,
  final `STATUS:` line, add the stem to `run.sh` `QUICK_CASES`.
- Reuse `should_retain_task`/`prune_terminal_tasks` (pure, unit-tested) + case
  55's node-stop pattern. Done when: configurable, case PASSes on Minicluster,
  surface synced, PR'd.

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
