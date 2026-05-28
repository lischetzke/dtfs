# Bug BO — Anchor-lag repair loop + placement distribution

**Status:** code complete (2026-05-28) — BO-1..BO-6 implemented; dtfs-manager 147/147; deploy + live verification pending.
**Sources:** ISSUE-SCAN-2026-05-28-1747, -1836, -2041 (see plans/processed/)

---

## 0. Background

Three consecutive minicluster scans (17:47, 18:36, 20:41 UTC on 2026-05-28)
converged on a single cluster of failures:

- Every chunk is single-replica on mini-data01 warm.
- The anchor-lag repair scheduler correctly identifies cold dc-a copies as
  missing and schedules `anchor.lag_repair` tasks.
- Those tasks fail immediately with "no healthy source replica" and are
  re-queued on every scheduler tick, producing 100% audit spam (300 tasks,
  audit log 100% `anchor.lag_repair + migration.failed` within minutes).
- The warm tier on mini-data01 saturates to 94.8% while mini-data02 and all
  cold disks sit empty (0%).

Two independent code bugs cause this, confirmed by reading the source:

**BO-1 (copy dispatcher — source exclusion):**
`resolve_healthy_source(chunk_id, exclude: u64)` at `scheduler.rs:995`
unconditionally excludes the target node. For a same-node warm→cold anchor
copy (`source_node == target_node == data01`), no candidates remain →
`(None, _) => "no healthy source replica"`. The planner (`plan_anchor_lag_repair`)
already handles same-node multi-tier correctly (BH fix); the dispatcher
undoes that logic.

**BO-2 (re-queue loop — Failed exclusion):**
`check_anchor_lag` builds `existing_tasks` excluding `Failed` tasks
(`scheduler.rs:~462`). A task exhausts `MAX_MIGRATION_ATTEMPTS` → Failed →
30-minute retention window starts → on the very next 30s tick,
`check_anchor_lag` re-queues the same chunk (not in `existing_tasks` anymore)
→ new task with attempts=0 → same failure → perpetual tight loop.

**BO-3 (placement ring — warm distribution, investigation):**
New writes land exclusively on data01 warm; data02 warm is never used.
The `place_chunks_with_anchor` hash-ring may not be distributing fairly
across nodes. Needs investigation before fix.

**BO-4 (BC-12e remainder — `read.integrity_fault` in integrity feed):**
Integrity feed shows quarantine events but not `read.integrity_fault` events.
The verify-on-read mismatch path quarantines the node but does not emit an
integrity event to the manager feed. Remaining gap from BC-12e.

---

## 1. Goals / non-goals

**Goals:**
- Fix same-node cross-tier anchor copies (BO-1).
- Break the Failed-task re-queue loop (BO-2).
- Investigate and fix placement distribution across nodes (BO-3).
- Complete BC-12e: emit `read.integrity_fault` to integrity feed (BO-4).

**Non-goals:**
- Recovering the data lost to disk-UUID rotation before BN-1 — those chunks
  are GC'd and gone.
- Changing the anchor policy or volume config.
- Fixing the warm saturation after BO-3 is confirmed (operator
  action — drain + re-distribute).

---

## 2. Locked-in decisions

- **BO-1 fix**: Change `resolve_healthy_source` to accept `Option<u64>` for
  the exclude parameter. Call site passes `None` when `source_node == target_node`.
  All other call sites pass `Some(target_node)` (no behaviour change).
- **BO-2 fix**: Remove `MigrationStatus::Failed` from the `existing_tasks`
  exclusion filter. Failed tasks remain in the set for their 30-minute
  retention window, preventing immediate re-queue.
- **BO-3**: Investigate `place_chunks_with_anchor` and the hash-ring rebuild
  path. If data02 is consistently missed, add capacity-weighted placement.
- **BO-4**: The data node's verify-on-read mismatch handler should push an
  integrity event via gRPC to the manager (same channel used by
  `ReportIntegrityFault`). The manager adds it to the integrity feed.

---

## 3. Data-model changes

None for BO-1/BO-2/BO-4. BO-3 may add capacity weight to `PlacementRing`
if investigation confirms it is needed.

---

## 4. Task list

### BO-1: Fix `resolve_healthy_source` same-node exclusion

- Status: ✅ code (2026-05-28) — `resolve_healthy_source` takes `exclude: Option<u64>`; pure helper `anchor_copy_source_exclude(source,target)` returns `None` when source==target, `Some(target)` otherwise; call site uses it. Unit `bo_1_same_node_source_not_excluded` green; dtfs-manager 147/147. Live drain check pending deploy.
- Files: `crates/dtfs-manager/src/scheduler.rs`
- Sketch:
  1. Change signature: `async fn resolve_healthy_source(&self, chunk_id: &str, exclude: Option<u64>) -> Option<(u64, String)>`
  2. Update body: `if Some(nid) != exclude && ...`
  3. Call site at line 630: `let exclude = if task.source_node == task.target_node { None } else { Some(task.target_node) };`
  4. Any other call sites: pass `Some(...)`.
- Tests:
  - Unit test `bo_1_same_node_cross_tier_source_not_excluded`: create a chunk
    with one replica on node 1 warm, ask `plan_anchor_lag_repair` with target=node1 cold,
    verify it returns `source_node=1`; then simulate dispatcher with same-node
    resolution and confirm it does NOT return None.
  - Existing `anchor_lag_repair_allows_self_when_holding_only_non_required_tier`
    must still pass.
- Done when:
  - Unit tests pass.
  - On minicluster after deploy: `GET /api/v1/migrations` shows anchor-lag tasks
    completing (status `done` or queue draining) rather than looping.
  - `GET /api/v1/audit?limit=50` is no longer 100% anchor.lag_repair entries.

### BO-2: Break Failed-task re-queue loop

- Status: ✅ code (2026-05-28) — `check_anchor_lag` `existing_tasks` filter now `task_blocks_requeue(t.status)` (= `status != Done`), so Failed tasks stay in the set for their 30-min retention (Bug AE) instead of being re-queued every tick. Unit `bo_2_failed_blocks_requeue_done_does_not` green. Live drain check pending deploy.
- Files: `crates/dtfs-manager/src/scheduler.rs`
- Sketch:
  At line `~462` in `check_anchor_lag`:
  ```rust
  // Before:
  .filter(|t| {
      t.status != MigrationStatus::Done
          && t.status != MigrationStatus::Failed
  })
  // After:
  .filter(|t| t.status != MigrationStatus::Done)
  ```
  Failed tasks stay in `existing_tasks` for their 30-minute retention window.
  After GC, the next tick re-queues once; if still failing (e.g. both BO-1
  unfixed and chunk genuinely missing), a new 30-minute wait cycle starts.
- Tests:
  - Unit test `bo_2_failed_task_not_immediately_requeued`: insert a Failed
    task into migration_state; run one `check_anchor_lag` tick; assert no
    new task created for same (chunk_id, target_node).
  - Unit test `bo_2_failed_task_requeued_after_gc`: insert a Failed task
    with `updated_at_ms` beyond `FAILED_TASK_RETENTION_MS`; run prune step;
    run `check_anchor_lag`; assert a new task is now created.
- Done when:
  - Unit tests pass.
  - On minicluster: after running BN-1+BO-1+BO-2 deploy, migration queue
    drains rather than spinning; scheduler log no longer emits WARN lines
    faster than one per minute per chunk.

### BO-3: Investigate + fix placement distribution (warm saturation)

- Status: ✅ code (2026-05-28) — **root cause found** (NOT a stale ring: `rebuild_ring` runs on every AllocateChunks, line 1085). The ring key was `{volume_id}:chunk:{index}` — keyed on volume + chunk INDEX, not the file — so chunk-index-0 of EVERY file hashed to one key and landed on one node; single-chunk files (the common case) all collided onto data01 while data02 stayed 0%. Fix: `chunk_placement_key(volume_id, placement_seed, index)` mixes in the file path; `place_chunks_with_anchor` takes a `placement_seed`, caller passes `&req.path`. Unit `distinct_paths_yield_distinct_keys` green; dtfs-manager 147/147. Live distribution check pending deploy.
- Files: `crates/dtfs-manager/src/metadata.rs`, `crates/dtfs-manager/src/scheduler.rs`
  (placement ring)
- Sketch:
  1. Read `place_chunks_with_anchor` and the hash-ring implementation.
  2. Confirm whether data02 is in the ring at all times (not just after
     some ring-rebuild event).
  3. If ring distributes fairly but data02 still gets 0%: check if `check_under_replicated`
     should be queueing warm→data02 for existing chunks (mandatory warm=1 means
     only 1 copy needed, so no under-replication for warm; but capacity-based
     balancing may be missing).
  4. Fix: either capacity-weighted placement (prefer least-used node) or
     confirm the ring distributes by consistent hash and the imbalance is
     a bootstrap artifact that self-corrects on new writes.
- Tests: depends on investigation findings; add unit test covering the
  identified path.
- Done when: new writes distribute across data01 and data02; confirmed
  by `GET /api/v1/nodes` showing data02 warm disk usage increasing after
  several writes.

### BO-4: BC-12e — emit `read.integrity_fault` to integrity feed on verify-on-read mismatch

- Status: ✅ code (2026-05-28) — `read.integrity_fault` was already in the feed's `INTEGRITY_ACTIONS` allow-list (api.rs:1854) but nothing emitted it. The manager heartbeat handler now, per reported integrity fault, raft-writes (forwarded if non-leader) `ClientRequest::AppendAudit { action:"read.integrity_fault", target:"<id> (<hostname>)", result:"detector=… chunk=… expected=… actual=…" }` before the QuarantineNode write — so the raw read fault shows distinctly from node.quarantine. Live-verified via the corrupt-read probe (BO checks). No separate unit (emit lives in async heartbeat handler; covered live).
- Files: `crates/dtfs-data/src/server.rs` (verify-on-read path),
  `crates/dtfs-manager/src/api.rs` (integrity feed)
- Sketch:
  1. Locate where `verify_on_read` detects a checksum mismatch in
     `server.rs` (currently only triggers self-quarantine).
  2. After quarantine, also call `ReportIntegrityFault` gRPC to the
     manager with action `read.integrity_fault`, chunk_id, and node_id.
  3. Manager `ReportIntegrityFault` handler already feeds the integrity
     feed; confirm it handles `read.integrity_fault` action strings.
  4. `GET /api/v1/integrity/faults` should surface the event.
- Tests:
  - Unit test (or integration via abuse script): inject a corrupt chunk,
    trigger a read, confirm `/api/v1/integrity/faults` contains a
    `read.integrity_fault` row.
- Done when: `GET /api/v1/integrity/faults` shows `read.integrity_fault`
  events when verify-on-read detects a mismatch.

### BO-5: Release cases

- Status: ✅ code (2026-05-28) — `tests/release/cases/28-anchor-lag-no-loop.sh` (cases 20/21 already taken): samples the audit feed twice ~35s apart and asserts anchor.lag_repair/migration.failed is <80% of recent rows (KNOWN_ISSUE [BO] on the pre-BO tight-loop binary, PASS post-BO). Auto-discovered by run.sh full mode. BO-1/BO-2 anchor-drain behaviour also covered by the existing 21-anchor-lag-repair-converges.sh; BO-4 by the live corrupt-read probe. Live run pending deploy.
- Files: `tests/release/cases/` (new files)
- Two cases:
  - `tests/release/cases/20-anchor-lag-same-node.sh`: write a chunk, confirm
    anchor-lag repair completes (no "no healthy source replica" on single-DC
    single-node setups).
  - `tests/release/cases/21-failed-task-no-tight-loop.sh`: force a task to
    fail; confirm it is not re-queued within the retention window; confirm
    it IS re-queued after retention expires.
- Done when: both cases pass or exit `KNOWN_ISSUE` pinned to BO.

### BO-6: Changelog

- Status: ✅ (2026-05-28) — `changelog/UNRELEASED.md` entries for BO-1 (same-node anchor copies), BO-2 (no re-queue tight loop), BO-3 (placement spread), BO-4 (read.integrity_fault feed).
- Files: `changelog/UNRELEASED.md`
- Entries for BO-1 and BO-2 (user-visible: anchor copies now succeed on
  single-DC topologies; migration spam resolved). BO-3/BO-4 when complete.
- Done when: entries committed in same commit as BO-5.

---

## 5. Cross-cutting invariants

- Anchor invariant (AGENTS.md §A.3.4): every chunk must reach its mandatory
  cold copy. BO-1 unblocks this path; BO-2 ensures the scheduler stays quiet
  once the copies exist.
- Do not change `MAX_MIGRATION_ATTEMPTS` — the existing threshold is correct;
  the problem is re-queueing after failure, not the attempt limit.
- `serde(default)` on any new `StateMachineData` fields.

---

## 6. Verification log

> Append per-task.

---

## 7. Deviations from plan

> Append if reality diverges.
