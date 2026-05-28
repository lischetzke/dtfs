# PLAN-BUG-BM — Issue-scan follow-up: migration GC, volume rollup, tape validation, operational

Plan id: `BM`
Status: 🟦 in progress
Author: Claude Opus 4.7 (1M context)
Created: 2026-05-28
Sources: ISSUE-SCAN-2026-05-28-1240 F1/F2/F3; ISSUE-SCAN-2026-05-28-1416 F5/F6/F7/F8

---

## 0. Background

Remaining code bugs and operational items from the 2026-05-28 issue
scans not covered by BH (anchor-lag dedup), BJ (RegisterNode audit spam),
BK (scrub level listing), or BL (heartbeat non-leader forwarding).

| ID | Source | Severity | Short description |
|----|--------|----------|-------------------|
| BM-1 | 1240-F1 | high | Failed anchor migration task lingers; `source==target` for replication task |
| BM-2 | 1240-F3 | low | `default` volume `total_size=0` despite 169 files |
| BM-3 | 1240-F2 / 1416-F6/F7 | medium | `tape:1` mandatory/replication on volumes with no tape disks |
| BM-4 | 1416-F5 | medium | ~499 orphan chunks on disk vs Raft state (reconciliation needed) |
| BM-5 | 1416-F8 | low | mini-mgr02 missing `version`/`build_date` (stale binary) |
| BM-6 | 1416-F1 | high | Two corrupt chunks on disk; deep scrub needed to assess full scope |

---

## 1. Root causes (code-confirmed where applicable)

### BM-1: Failed anchor migration not reaped

`crates/dtfs-manager/src/scheduler.rs` — task `anchor-70585` reached
status `failed` after 4 retry attempts (`last_error="chunk no longer
exists"`). No GC path removes `failed` migration tasks whose chunk has
been unlinked; the task lives in the queue forever. Separately,
`source_node == target_node` in an anchor replication task means the
scheduler tried to replicate a chunk from a node to itself — a planning
error that should be guarded at task creation (plan_anchor_lag_repair).

### BM-2: `total_size=0` on default volume

`crates/dtfs-manager/src/api.rs` volume-list aggregator. `total_size`
is either not computed (no rollup of `FileMetadata.size` for files
belonging to the volume) or the field is zero-initialized and never
summed. Root-cause requires reading the volume-list handler; noted as
"unknown aggregation gap" by the scan — confirmed as code gap, not data
gap (disk usage is 2.2 GB non-zero).

### BM-3: Tape tier constraint never validated

No validator prevents `mandatory_replicas.tape > 0` from being saved
when no `tape`-tier disk exists in the cluster. The under-replication
scheduler will silently queue failing tasks for every file in those
volumes once files are written.

### BM-4: Orphan chunks (~499, ~1.95 GB)

Likely GC lag following heavy replication + TCP failures at ~10:14.
Hypothesis: chunks evicted from Raft location state (cache demote/flush)
but on-disk files not yet removed by the GC scheduler. Alternatively,
Raft location records lost. Cannot distinguish without a deep scrub
cross-check. No code fix until root cause is confirmed.

### BM-5: mini-mgr02 missing version/build_date

Operational: mini-mgr02 registered before Bug AY (version/build_date in
RegisterNode) was deployed, or is running a pre-AY binary. Raft state
entry simply has empty strings for those fields.

### BM-6: Two corrupt chunks on disk

mini-data02 chunk `06e7…` at 09:28; mini-data01 chunk `abee…` at 10:29.
Both detected via BLAKE3 mismatch, reads refused. With BL fixed and BJ
mitigated, QuarantineNode + RemoveCorruptChunkReplicas should have
committed — verify on live cluster. A deep scrub on both nodes is needed
to assess whether additional corrupt chunks exist beyond what
`verify_on_read` caught.

---

## 2. Goals / Non-goals

### Goals
- Code fix: GC path for `failed` migration tasks after max retries.
- Code fix: guard against `source == target` in anchor-repair task planner.
- Code fix: `total_size` correctly reflects file bytes for each volume.
- Code fix: warn (at minimum) when `mandatory_replicas.tape > 0` is saved
  with no tape disks present.
- Operational: deep scrub on Minicluster to confirm corrupt-chunk scope
  and orphan chunk status.
- Operational: redeploy mini-mgr02 with current binary.
- Operational: fix `av-demo` and `default` volume tape configs on
  Minicluster.

### Non-goals
- No automatic removal of tape constraint (operator decision).
- No re-architecture of the migration queue.
- No change to how Raft location entries are evicted (that is BH
  territory for anchor placements).

---

## 3. Task list

### BM-1 — Reap stale failed migration tasks + guard source == target
- Status: ⬜
- **Files:** `crates/dtfs-manager/src/scheduler.rs`
- **Implementation sketch:**
  - In the migration scheduler's retry/completion path, after a task
    exceeds `max_attempts` (or errors with "chunk no longer exists"),
    mark the task `abandoned` (or remove it) rather than leaving it as
    `failed`. Add a scheduler sweep that runs after the main migration
    tick: for each `failed` task older than a configurable grace window
    (default: 1 h), remove it from the queue via a Raft write.
  - In `plan_anchor_lag_repair` (scheduler.rs ~line 2313), add a guard:
    if the selected `target_node == source_node`, skip this chunk and
    log a warn. (This should be impossible after BH-1 lands, but a
    defence-in-depth guard catches any new path.)
- **Tests (BM-1):**
  - Unit: apply a ReplicationTask with status `failed` and age > grace,
    assert the sweep removes it.
  - Unit: plan_anchor_lag_repair with a single-node topology produces no
    task (source == target avoided).
- **Done when:** `cargo test -p dtfs-manager --release` green; failed
  anchor-70585 is removed from the live queue after deploy.

### BM-2 — Fix volume total_size rollup in volume-list API
- Status: ⬜
- **Files:** `crates/dtfs-manager/src/api.rs` (volume-list handler)
- **Implementation sketch:** Read the volume-list aggregator. Locate
  where `total_size` is constructed for each volume. Either it is
  zero-initialized with no sum, or it skips `FileMetadata.size`. Fix to
  iterate `sm.files.values().filter(|f| f.volume == volume_name).map(|f|
  f.size).sum()`. Confirm `FileMetadata` carries a `size` field (read
  dtfs-raft/lib.rs) and that it is populated at write time.
- **Tests (BM-2):**
  - Unit: state machine with 3 files on volume "default" each 100 bytes;
    assert volume list returns `total_size = 300`.
  - Fails on current code, passes after fix.
- **Done when:** `cargo test -p dtfs-manager --release` green; live
  `/api/v1/volumes` shows `total_size ≈ 2.2 GB` for default volume.

### BM-3 — Warn on unsatisfiable tape constraint at volume save
- Status: ⬜
- **Files:** `crates/dtfs-manager/src/api.rs` (update_volume or similar
  handler), `crates/dtfs-manager/src/webui.rs` (if volume config UI
  saves inline)
- **Implementation sketch:** In the volume-update/create API handler,
  before writing `ClientRequest::UpdateVolumeConfig`, check whether any
  `mandatory_replicas.tape > 0`. If so, scan `sm.nodes.values()` for
  any data node whose `disks` contains a `tier == Tape` entry. If none
  found, return a 400 with a diagnostic message:
  `"mandatory_replicas.tape=N but no tape-tier disks are registered"`.
  Apply the same check for `replication.tape`. This is a warning-class
  gate — apply `mandatory_replicas` check as an error, `replication`
  check as a warning (non-fatal but logged).
- **Tests (BM-3):**
  - Unit: attempt to set `mandatory_replicas.tape=1` when no tape disk
    node in sm.nodes; assert the API returns an error.
  - Unit: `replication.tape=1` with no tape disk logs a warn but
    succeeds.
- **Done when:** `cargo test -p dtfs-manager --release` green.

### BM-4 — Minicluster: run deep scrub to assess orphan + corrupt scope
- Status: ⬜
- **Operational (no code change).**
- **Steps:**
  1. Run `dtfs-cli scrub --level deep` on mini-data01 and mini-data02
     (or trigger via manager API once BL is deployed so results commit).
  2. Capture scrub output: `chunks_checked`, `chunks_corrupt`,
     `chunks_missing`. Compare to disk usage from `/api/v1/nodes`.
  3. If `chunks_missing` > 0: these are Raft-tracked chunks with no
     on-disk file — under-replication; scheduler should queue repair.
  4. If on-disk count >> scrub checked count after BL: orphans confirmed
     as GC lag; verify they resolve on the next GC pass.
  5. For the two known corrupt chunks (`06e7…`, `abee…`): confirm they
     are now tracked as quarantined in `/api/v1/nodes` (BL must be
     deployed first). If not quarantined, re-trigger fault reporting
     manually.
- **Done when:** deep scrub results logged in §5; corrupt chunk status
  resolved; orphan count drops or explained.

### BM-5 — Minicluster: redeploy mini-mgr02 with current binary
- Status: ⬜
- **Operational (no code change).**
- **Steps:**
  1. Build current master on dtfs-builder:
     `cargo build --release -p dtfs-manager`.
  2. `scp + systemctl restart dtfs-manager` on mini-mgr02
     (`10.150.0.172`).
  3. Confirm mini-mgr02 re-registers with `version` + `build_date` in
     `/api/v1/nodes`.
- **Done when:** mini-mgr02 shows current `version`/`build_date` in
  node API.

### BM-6 — Minicluster: fix tape volume configs
- Status: ⬜
- **Operational (no code change until BM-3 lands).**
- **Steps:**
  1. `av-demo`: set `mandatory_replicas.tape = 0` (and decide whether
     it needs `anchor={dc-a, cold}` to match `default`).
  2. `default`: set `replication.tape = 0` (it is non-fatal today but
     generates silent scheduler noise).
  3. Use `dtfs-cli volume update` or the WebUI Config page.
- **Done when:** both volumes show `tape: 0` in `/api/v1/volumes`.

### BM-7 — Release-suite coverage for BM-1 + BM-2
- Status: ⬜
- **Files:** `tests/release/cases/NN-failed-migration-reap.sh`,
  `tests/release/cases/NN-volume-total-size.sh`, `tests/release/run.sh`
- **Done when:** cases present, registered, PASS in latest run.
  Required per § B.5.1 (cluster-visible: migration queue + volume stats).

### BM-8 — Docs + changelog
- Status: ⬜
- **Files:** `changelog/UNRELEASED.md`
- Entries: (1) failed migration tasks with no-longer-existent chunk are
  now reaped after grace period; (2) `total_size` in volume list now
  correct; (3) unsatisfiable tape constraint rejected at volume save.
- **Done when:** entries present, same commit as BM-1/BM-2/BM-3.

---

## 4. Sequencing note

**BL must deploy before BM-4** (deep scrub results only commit reliably
once BL is live). **BJ-1 should be deployed before BM-4** (audit log
must not be saturated by RegisterNode spam while assessing integrity
fault results). BM-1 through BM-3 and BM-5/BM-6 are independent of
each other.

---

## 5. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|

---

## 6. Cross-cutting invariants

- Migration queue changes (BM-1): must not accidentally reap `in_progress`
  or `pending` tasks. Only `failed` tasks past the grace window.
- Volume rollup (BM-2): must not O(files × volumes) on every stats call.
  Precompute per-volume sizes into a cache or iterate once and bucket.
- Tape constraint check (BM-3): check `sm.nodes` at the time of the API
  call, not at scheduler time — topology may change after volume save.

---

## 7. Deviations from plan

_(empty)_

---

## 8. Notes for the implementer

- For BM-2, read the volume-list handler in api.rs first — determine
  whether `total_size` is missing entirely or present but zeroed. Also
  confirm `FileMetadata` has a `size` field (check dtfs-raft/lib.rs
  around `FileMetadata` struct).
- For BM-1, confirm `max_attempts` constant or config key for migration
  retries before writing the grace-window sweep. The abandoned task
  should emit one `warn!` with the task_id + last_error so operators
  can correlate.
- The two corrupt chunk IDs for BM-4/BM-6 verification:
  - mini-data02: `06e70191246e53d8cc53033606b4fd8c34767dc76a82c9fcfa99ac7507a27d71`
  - mini-data01: `abee47b6c34c7c980de1a772c9ea5e1e4cea28640e1750eab123915928e13ebe`
