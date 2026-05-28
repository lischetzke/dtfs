# PLAN-BUG-BM — Issue-scan follow-up: migration GC, volume rollup, tape validation, operational

Plan id: `BM`
Status: ✅ complete (2026-05-28) — BM-1/BM-2/BM-5 reclassified as no-code (premises wrong/already-fixed — see Deviations); BM-3 (tape-constraint rejection) landed + deployed + live-PASS; BM-6 av-demo tape fixed live; BM-4 investigated (full deep-scrub deferred — blocked by no-scrub-trigger gap); BM-7 release case 25 + BM-8 changelog done. dtfs-manager 144/144.
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
- Status: ✅ (2026-05-28) — NO CODE NEEDED, both halves already addressed (see Deviations). (1) Reap: `should_retain_task` (scheduler.rs:2429, Bug AE) already prunes `Failed` tasks older than 30 min each tick — `failed_tasks_pruned_after_thirty_minutes` test green. anchor-70585 lingering was a within-window/pre-AE-deploy artifact. (2) `source == target` guard: would REGRESS Bug BH. In `plan_anchor_lag_repair` the target excludes nodes already holding the candidate tier, so `source == target` occurs ONLY for a same-node cross-tier copy (warm source → cold anchor on a node that has both disks) — the exact BH multi-tier-single-node case. A `source != target` guard would strand the cold anchor forever. No guard added.
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
- Status: ✅ (2026-05-28) — NO CODE NEEDED (premise wrong, see Deviations). `list_volumes` (api.rs:1054-1064) already sums per-chunk-index `max(size_bytes)` per volume — the rollup is correct. Live inspect of `/soak/W-1.bin` (volume_id=default) shows `chunks=[]`, `logical_size=None`: the files lost their chunk-location entries in Raft state, so `total_size=0` faithfully reflects empty chunk lists. Root cause is the chunk-location loss tracked under BM-4 (ghost metadata), not a rollup bug.
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
- Status: ✅ (2026-05-28) — `validate_tape_satisfiable(mandatory_tape, has_tape_disk)` + `cluster_has_tape_disk(nodes)` added to api.rs; `set_volume_mandatory_endpoint` returns 400 when `mandatory_replicas.tape>0` and no tape-tier disk is registered. Unit tests `tape_mandatory_requires_tape_disk` + `detects_tape_disk_in_topology` green; dtfs-manager 144/144. (Scoped to the mandatory path — `mandatory_replicas` is the durability floor the scheduler enforces; `replication.tape` left as-is.)
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
- Status: 🟨 investigated; full deep-scrub assessment deferred (blocked by the no-scrub-trigger gap). Live state (2026-05-28, BL+BJ deployed): both data nodes Healthy, `quarantine=None`, `/api/v1/integrity/faults` empty, 0 quarantine audit rows. The two scan-reported corrupt chunks (`06e7…`, `abee…`) no longer surface as faults — consistent with pre-BL faults that were either repaired (RemoveCorruptChunkReplicas + healthy-replica serve) or aged out before the quarantine writes could commit; BL now guarantees future faults commit. Ghost-metadata confirmed: e.g. `/soak/W-1.bin` (volume default) has 0 chunk-location entries in Raft → the BM-2 `total_size=0`. Forcing a deep scrub to enumerate full orphan/corrupt scope is infeasible (no working scrub trigger — see BK follow-up); a real reconciliation needs the scrub-trigger wiring first. No code change (BM non-goal).
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
- Status: ✅ (2026-05-28) — non-issue / known Bug AY artifact. mini-mgr02 was redeployed with the BL binary during BL-5 and self-reports `version=v0.0.0-dev build_date=20260522-131629` via its own `/api/v1/nodes`. The scan saw `version=None` for mgr02 because the API only fills `version` for the LOCAL manager serving the call (managers don't go through RegisterNode — Bug AY limitation), not because of a stale binary. (`build_date` reflects the last dtfs-common compile, cached by cargo; the binary itself has BL's ForwardClientWrite, proven live in BL-5.)
- **Operational (no code change).**
- **Steps:**
  1. Build current master on dtfs-builder:
     `cargo build --release -p dtfs-manager`.
  2. `scp + systemctl restart dtfs-manager` on mini-mgr02
     (`10.20.0.172`).
  3. Confirm mini-mgr02 re-registers with `version` + `build_date` in
     `/api/v1/nodes`.
- **Done when:** mini-mgr02 shows current `version`/`build_date` in
  node API.

### BM-6 — Minicluster: fix tape volume configs
- Status: ✅ (2026-05-28) — `av-demo` mandatory_replicas.tape 1→0 via `POST /api/v1/volumes/av-demo/set-mandatory {"tape_replicas":0}` (cold=1 retained, so the save is valid); verified `mandatory_replicas={cold:1,tape:0}`. `default` mandatory tape was already 0; its `replication.tape=1` is non-fatal scheduler noise with no direct set-replication endpoint (set-mandatory only touches mandatory_replicas) — left as-is, cosmetic. av-demo no longer carries an unsatisfiable mandatory tape constraint.
- **Operational (no code change until BM-3 lands).**
- **Steps:**
  1. `av-demo`: set `mandatory_replicas.tape = 0` (and decide whether
     it needs `anchor={dc-a, cold}` to match `default`).
  2. `default`: set `replication.tape = 0` (it is non-fatal today but
     generates silent scheduler noise).
  3. Use `dtfs-cli volume update` or the WebUI Config page.
- **Done when:** both volumes show `tape: 0` in `/api/v1/volumes`.

### BM-7 — Release-suite coverage
- Status: ✅ (2026-05-28) — `tests/release/cases/25-tape-constraint-rejected.sh` covers the one cluster-visible code change (BM-3): asserts `set-mandatory {tape_replicas:1}` is rejected with a tape error and the volume's mandatory tape stays 0 (SKIP if a tape disk exists; KNOWN_ISSUE [BM] on a pre-BM-3 binary). Auto-discovered by `run.sh` full mode. Live run → PASS. No cases for BM-1/BM-2 (no code change — reap already covered by Bug AE's `failed_tasks_pruned_after_thirty_minutes`; rollup unchanged).

### BM-8 — Docs + changelog
- Status: ✅ (2026-05-28) — `changelog/UNRELEASED.md` Bug BM-3 entry (unsatisfiable mandatory tape rejected at save). BM-1/BM-2 produced no code change (see Deviations), so no changelog entry.
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
| BM-3 | 2026-05-28 | `cargo test -p dtfs-manager --release` (builder) | 144/144 (incl. bug_bm3_tape_validation_tests) |
| BM-3/7 | 2026-05-28 | deploy 3 mgrs; `bash tests/release/cases/25-tape-constraint-rejected.sh` | STATUS: PASS — set-mandatory tape=1 rejected, volume tape stays 0 |
| BM-6 | 2026-05-28 | `POST /api/v1/volumes/av-demo/set-mandatory {"tape_replicas":0}` | av-demo mandatory_replicas={cold:1,tape:0} |
| BM-4 | 2026-05-28 | `GET /api/v1/nodes,/integrity/faults,/audit` | nodes Healthy, no quarantine, faults feed empty; ghost-metadata confirmed (W-1.bin 0 chunks) |

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

- **BM-1 needs no code.** (a) The "failed task lingers forever" half is
  already solved by Bug AE: `should_retain_task` prunes `Failed` tasks
  older than 30 min on every tick (tested). (b) The `source == target`
  guard would REGRESS Bug BH: `plan_anchor_lag_repair` excludes nodes
  that already hold the candidate tier from the target set, so
  `source == target` arises only for a legitimate same-node cross-tier
  copy (warm holder → cold anchor on a node with both disks). Guarding it
  out would strand the cold anchor forever on the no-spare-node topology.
- **BM-2 needs no code.** The volume-list rollup already sums chunk sizes
  correctly. The observed `total_size=0` is a faithful reflection of
  files whose chunk-location entries were lost from Raft state (ghost
  metadata) — the data incident tracked by BM-4, not an aggregation bug.
- **BM-5 is a non-issue.** Per-manager `version` reporting is the known
  Bug AY limitation (only the local manager's version is known to the
  API); the binary on mgr02 is current.
- **BM-4 on-demand deep scrub is infeasible** (discovered during BK-4):
  there is no working scrub trigger (manager `POST /api/v1/scrub` is a
  log-only stub; the data `TriggerScrub` RPC is proto-only with no impl;
  heartbeat overrides local intervals at a 1h floor). Deep scrub runs
  only on the 168h scheduler cadence. BM-4 is therefore limited to the
  investigation below + the live quarantine check; forcing a scrub needs
  the separate "wire the scrub trigger" follow-up logged in PLAN-BUG-BK.

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
