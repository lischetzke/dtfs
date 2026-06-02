# Bug BP — Raft storage exhaustion + cascade (snapshot corruption, stale-Healthy liveness)

**Status:** code complete (2026-06-02) — BP-0 live recovery done (quorum 3/3 restored); BP-1..BP-5 code + unit tests green (dtfs-raft 55/55, dtfs-manager 151/151); BP-6 cases 29/30 written; BP-7 changelog done. Deploy + live-verify in progress.
**Sources:** ISSUE-SCAN-2026-06-02-1457 (see plans/reports/ — move to processed/ once captured)

---

## 0. Background

Fourth minicluster sweep (2026-06-02 14:57) found a single root-cause cluster
that took the cluster to the edge of quorum loss, plus a now-materialized data
loss that traces to the (already-fixed) disk-UUID rotation bug.

Root-cause chain (scan Findings 1+3+4+5):

1. **F5 (medium, ROOT)** — the manager raft-log dir grew to **14G on a 16G
   root partition**: ~1000 `LOG.old.*` RocksDB info logs + a **207MB MANIFEST**,
   never rotated. RocksDB `Options` in `log_store.rs` set only
   `create_if_missing` + WAL recovery mode — no `keep_log_file_num`,
   `set_max_manifest_file_size`, or `set_recycle_log_file_num`, so the info-LOG
   and MANIFEST grow unbounded.
2. **F1 (critical)** — disk hit 99-100%. mgr01's raft node took a **fatal IO
   error** (`No space left on device` appending `raft-log/4496458.sst`) and
   entered `state:"Shutdown"`. Quorum degraded to 2/3 (mgr02+mgr03); mgr03
   (leader) at 98% with 378M free — one disk-full event from total quorum loss.
3. **F3 (high)** — a snapshot write on the leader was truncated when the disk
   filled, leaving a 0-byte/partial snapshot: `Read Snapshot(None): EOF while
   parsing a value at line 1 column 0`. Vote handling + `save_migration_state`
   both fail on it. Snapshot is persisted to `<snapshot_dir>/snapshot.bin`;
   the write path is not crash/ENOSPC atomic.
4. **F4 (high)** — heartbeat (`UpdateNodeStatus`) is a raft write; with raft
   degraded, heartbeats stopped committing → `last_seen_ms` froze (~17.5h data,
   ~4.9d client) **while health still reports `Healthy`**. The frozen commit is
   a cascade of F1; the **"Healthy never ages out on stale `last_seen`" is its
   own projection bug** in the manager.

Independent of the storage chain:

5. **F2 (critical, REALIZED DATA LOSS)** — all 184 files report
   `chunk_count=0`; data01 holds 0 `.dat` files (45 → 0 across May 31→Jun 01 via
   GC orphan scrub). This is the consummation of the disk-UUID rotation orphaning
   (now fixed by BN-1, scan Finding 6) — GC deleted physically-present chunks
   whose location records were orphaned under stale UUIDs, leaving FileMetadata
   stranded with no chunk bindings. **Forward-only fix already shipped (BN-1);
   the deleted data is not recoverable.** Remaining code concern: confirm the GC
   orphan-classification path cannot delete a chunk whose file metadata still
   references it, and surface realized data-loss (files at chunk_count=0) instead
   of silently reporting `status:"ok"`.

6. **F7 (low)** — `av-demo` + `default` volumes still carry `replication.tape=1`
   with zero tape disks. `mandatory_replicas.tape=0` so BM-3 does not queue
   under-replication; cosmetic config drift only.

7. **F6 (info)** — disk-UUID rotation RESOLVED (BN-1 deployed, stable 5 days).
   No action; close the item.

---

## 1. Goals / non-goals

**Goals:**
- Bound the raft-log/RocksDB storage so it cannot fill the partition (BP-1).
- Make snapshot writes atomic + ENOSPC-safe, and detect/recover a truncated
  snapshot on startup (BP-2).
- Downgrade a node's health projection when `last_seen_ms` exceeds a liveness
  window, instead of reporting `Healthy` indefinitely (BP-3).
- Surface realized data loss + harden the GC orphan-deletion guard (BP-4).
- Clear the cosmetic `replication.tape=1` drift (BP-5).

**Non-goals:**
- Operator recovery actions for the *current* minicluster incident (reclaim
  space on mgr01/mgr03, restart mgr01 raft node, re-ingest the lost 184 files).
  Those are runbook steps for the operator, captured in § 6, not code tasks.
- Recovering data already deleted (F2) — forward-only, gone.
- Enlarging the manager root partition — deployment/provisioning concern,
  noted for the operator (raise raft partition size), not a code change.
- openraft internals beyond the config knobs we own.

---

## 2. Locked-in decisions

- **BP-1**: Set RocksDB `Options` in `log_store.rs::LogStore::new`:
  `set_keep_log_file_num(N)` (small, e.g. 4), `set_max_manifest_file_size(M)`
  (e.g. 64 MiB) so the MANIFEST rotates, and `set_recycle_log_file_num` if
  appropriate. Apply to `db_opts` (the DB-level options), not the per-CF
  `Options::default()`. Additionally cap `max_in_snapshot_log_to_keep` in the
  openraft `Config` (currently `..Default::default()`) so the raft log purges
  more aggressively after each snapshot. Numbers go through config keys with
  sane defaults; do not hardcode without a `[raft]` knob where one fits.
- **BP-2**: Snapshot persistence writes to `<snapshot_dir>/snapshot.bin.tmp`,
  `sync_all()`, then atomic `rename` to `snapshot.bin`, then fsync the dir.
  On open/restore, a snapshot file that fails to deserialize (EOF/partial) is
  treated as absent (log-catchup from peers) rather than a hard error — and the
  partial file is removed so it cannot wedge subsequent reads. Mirrors the
  data-plane tmp+rename discipline (AGENTS.md §A.6).
- **BP-3**: Add a liveness window (config `[raft]`/`[cluster]` knob, default
  e.g. 3× heartbeat interval or a fixed `node_liveness_secs`). The health
  projection (API `get_stats` / `/api/v1/nodes` view, and/or the health monitor
  that issues `UpdateNodeStatus`) reports `Suspect`/`Down` for a node whose
  `last_seen_ms` is older than the window, instead of the stored `Healthy`.
  Decide at implementation time whether to fix at the projection layer (read
  path, cheaper, no raft write) or have the health monitor emit the downgrade
  (durable but needs a committing leader — fragile during the very incident this
  guards against). **Prefer the read-path projection** so it works even when
  raft writes are stalled.
- **BP-4**: (a) Confirm/strengthen the GC orphan-scrub guard so a physical chunk
  is only deleted when **no** FileMetadata references its chunk-hash (not merely
  when no location record matches a current disk UUID). (b) Make
  `/api/v1/cluster/status` / `get_stats` report a non-ok integrity signal (or a
  distinct field) when files exist with `chunk_count=0`, so a top-line `ok` does
  not mask realized data loss.
- **BP-5**: Clear `replication.tape` to 0 on `av-demo` + `default` (operator
  `volumes set-*` action) OR add a save-time normalization that drops a tape
  replication target when no tape disks exist (mirror BM-3 mandatory guard).
  Prefer the save-time normalization so it cannot recur.

---

## 3. Data-model changes

- BP-1: new optional config keys under `[raft]` (e.g. `rocksdb_keep_log_file_num`,
  `rocksdb_max_manifest_bytes`, `max_in_snapshot_log_to_keep`) with backward-safe
  defaults. `hard_cap`-style: absent → sane default, never 0/unbounded.
- BP-3: new config key for the liveness window; new computed (not stored) health
  in the projection — no `StateMachineData` field needed if done read-path.
- BP-4: possibly a new computed field in the stats response
  (`files_missing_chunks` count); no snapshot schema change.
- Any new `StateMachineData` field MUST carry `#[serde(default)]` (AGENTS.md
  §A.3.1) — none expected.

---

## 4. Task list

### BP-1: Bound RocksDB log/MANIFEST + raft log retention

- Status: ✅ (2026-06-02) — RocksDbLogOpts{keep_log_file_num=4,max_manifest_bytes=64MiB} in log_store.rs::new_with_opts; openraft max_in_snapshot_log_to_keep=1000 in main.rs; [raft] config keys added. Unit log_store_opens_with_bounded_opts; dtfs-raft 55/55. Live: post-reclaim raft-log bounded.
- Files: `crates/dtfs-raft/src/log_store.rs`, `crates/dtfs-manager/src/main.rs`
  (openraft `Config`), `crates/dtfs-common/src/config.rs` (`[raft]` keys).
- Sketch:
  1. In `LogStore::new`, on `db_opts`: `set_keep_log_file_num(4)`,
     `set_max_manifest_file_size(64 * 1024 * 1024)`,
     `set_recycle_log_file_num(2)` (tune). Wire from config with defaults.
  2. In `main.rs` raft `Config`, set `max_in_snapshot_log_to_keep` to a bounded
     value (replace the implicit default) so post-snapshot purge keeps the
     `.sst` set small.
- Tests:
  - Unit `bp_1_log_store_opts_bound_manifest`: open a `LogStore`, assert the
    DB opens and (where the rocksdb crate exposes getters) the options are set;
    at minimum a regression test that `LogStore::new` applies the bounded opts
    helper.
  - Confirm `cargo test -p dtfs-raft` green.
- Done when:
  - Unit green; on the lab, after deploy + a forced snapshot cycle, `du -sh
    raft-log` stays bounded (LOG.old count ≤ keep_log_file_num, MANIFEST < cap)
    across repeated writes. (Live-verify on minicluster after operator reclaims
    space.)

### BP-2: Atomic, ENOSPC-safe snapshot persistence + corrupt-snapshot recovery

- Status: ✅ (2026-06-02) — persist_snapshot_to_disk now fsyncs tmp + dir around the rename; open_at treats a corrupt/0-byte snapshot as absent (logs, deletes, log-catchup) + cleans stray .tmp. Units bp_2_truncated_snapshot_treated_as_absent + bp_2_stray_tmp_ignored_on_open.
- Files: `crates/dtfs-raft/src/state_machine.rs` (snapshot persist + open_at
  restore), maybe `crates/dtfs-raft/src/snapshot.rs`.
- Sketch:
  1. Snapshot write: serialize to `snapshot.bin.tmp`, `File::sync_all()`,
     `rename` to `snapshot.bin`, then fsync the parent dir.
  2. Restore (`open_at`): if `snapshot.bin` deserialization fails (EOF/partial),
     log a WARN, delete the corrupt file, and continue with no snapshot (raft
     log-catchup from a healthy peer brings the node current; gap last_log
     2380332 vs snapshot 2374794 is small per the scan).
- Tests:
  - Unit `bp_2_snapshot_write_is_atomic`: write a snapshot, kill-simulate by
    leaving a `.tmp`, confirm restore ignores `.tmp` and the committed
    `snapshot.bin` loads.
  - Unit `bp_2_truncated_snapshot_treated_as_absent`: write a 0-byte
    `snapshot.bin`, confirm `open_at` returns a fresh state machine (no panic /
    no hard error) and removes the corrupt file.
- Done when:
  - Unit green; on the lab, a manager whose snapshot was truncated by the
    disk-full event starts cleanly and catches up via log replay rather than
    looping on `Read Snapshot EOF`. (Live-verify post-incident.)

### BP-3: Downgrade Healthy on stale last_seen (liveness projection)

- Status: ✅ (2026-06-02) — pure effective_health(stored,last_seen,now,window) read-path projection; node_liveness_secs config (30s); wired into list_nodes + cluster_status healthy_count. Unit bp_3_effective_health_downgrades_stale. Case 30.
- Files: `crates/dtfs-manager/src/api.rs` (`get_stats` / nodes projection),
  `crates/dtfs-common/src/config.rs` (liveness window key). Possibly the
  health-monitor in `crates/dtfs-manager/src/*` if a durable downgrade is also
  wanted.
- Sketch:
  1. Add `node_liveness_secs` config (default e.g. 30s or 3× heartbeat).
  2. In the read projection for `/api/v1/nodes` and `/api/v1/cluster/status`,
     compute effective health: if `now_ms - last_seen_ms > window`, report
     `Suspect` (or `Down` past a larger multiple) regardless of stored
     `Healthy`. Do NOT mutate stored state (works when raft writes are stalled).
  3. `data_nodes_healthy` in cluster status uses the effective health so the
     top-line count reflects reality.
- Tests:
  - Unit `bp_3_stale_last_seen_projects_suspect`: node stored `Healthy`,
    `last_seen_ms` older than window → projection returns `Suspect`/`Down`.
  - Unit `bp_3_fresh_last_seen_stays_healthy`: recent `last_seen_ms` →
    `Healthy`.
- Done when:
  - Unit green; on the lab a node with frozen `last_seen_ms` shows downgraded
    health in `/api/v1/nodes` and is not counted in `data_nodes_healthy`.

### BP-4: Harden GC orphan guard + surface realized data loss

- Status: ✅ (2026-06-02) — orphan reap routed through pure chunk_is_orphan(in_state,hash) (delete only when NO file references the hash); cluster_status emits files_missing_chunks + degrades status when >0. Units bp_4_chunk_is_orphan + bp_4_counts_files_missing_chunks. (The F2 loss vector itself was BN-1, already fixed.)
- Files: GC orphan-scrub path (scheduler / scrub — locate exact site),
  `crates/dtfs-manager/src/api.rs` (stats).
- Sketch:
  1. Locate the orphan-deletion decision. Ensure a chunk file is deleted only
     when no FileMetadata references its chunk-hash — never solely because no
     location record matches the current disk UUID. (BN-1 stabilizes UUIDs, but
     the guard should be hash-referenced, defence-in-depth.)
  2. Add `files_missing_chunks` (count of files with `chunk_count=0`) to
     `get_stats`; when > 0, reflect it in an integrity/health signal so the
     top-line is not a bare `ok`.
- Tests:
  - Unit for the orphan guard: a chunk referenced by a file is NOT classified
    orphan even if its location's disk UUID is stale/unknown.
  - Unit `bp_4_stats_reports_files_missing_chunks`.
- Done when:
  - Unit green; on the lab, `/api/v1/cluster/status` surfaces the
    184-files-missing-chunks condition instead of bare `ok`.

### BP-5: Drop tape replication target on no-tape clusters

- Status: ✅ (2026-06-02) — normalize_tape_replication(cfg,has_tape) drops replication.tape->0 at set-mandatory save when no tape disk. Unit bp_5_normalize_drops_tape_replication.
- Files: volume save/normalization path (`crates/dtfs-manager/src/api.rs` or the
  `UpsertVolume` apply in `state_machine.rs`), reusing BM-3's tape-constraint
  helper.
- Sketch: at volume save, if the cluster has zero tape disks, normalize
  `replication.tape` to 0 (log a notice). Mirrors BM-3 which already guards the
  mandatory path.
- Tests:
  - Unit `bp_5_save_drops_tape_replication_when_no_tape_disks`.
- Done when: unit green; `av-demo` + `default` show `replication.tape:0` after a
  config-touch on the lab.

### BP-6: Release cases

- Status: ✅ (2026-06-02) — tests/release/cases/29-raft-storage-bounded.sh (LOG.old<=8, MANIFEST<128MiB per mgr) + 30-stale-liveness-downgrade.sh (stop data node -> Suspect/Down within ~2x window -> restart Healthy). Auto-discovered by run.sh.
- Files: `tests/release/cases/NN-*.sh`
- Cases:
  - raft-log dir stays bounded across N writes + snapshot cycles (BP-1).
  - truncated snapshot recovers via log-catchup (BP-2).
  - stale `last_seen` node is reported non-Healthy / excluded from healthy count
    (BP-3).
- Done when: cases PASS or `KNOWN_ISSUE [BP]` pinned.

### BP-7: Changelog

- Status: ✅ (2026-06-02) — changelog/UNRELEASED.md entries BP-1..BP-5.
- Files: `changelog/UNRELEASED.md`
- Entries for BP-1 (raft storage no longer grows unbounded), BP-2 (snapshot
  crash-safety), BP-3 (stale nodes no longer reported Healthy), BP-4
  (data-loss surfaced + GC guard), BP-5 (tape drift). Same commit as each code
  task.

---

## 5. Cross-cutting invariants

- Snapshot + log storage must be crash/ENOSPC-safe (tmp+rename+fsync, bounded
  size) — never leave a 0-byte snapshot or an unbounded log dir.
- Health projection must not report `Healthy` for a node that has not reported
  within the liveness window.
- GC must never delete a chunk still referenced by a file's chunk-hash
  (AGENTS.md §A.3.3 #4 / §A.6 #8).
- `#[serde(default)]` on any new `StateMachineData` field.
- Read-path projections (BP-3, BP-4) must work without a committing leader — the
  whole point is to surface truth when raft is degraded.

---

## 6. Operator runbook (NOT code tasks — for the current incident)

These belong to the operator / implementation session running the live cluster,
captured here so the incident response is not lost:

1. **Reclaim raft-log space on mgr01 and mgr03 before a second manager fills.**
   Inspect `/var/lib/dtfs/manager/raft-log`; the safe reclaim is old RocksDB
   `LOG.old.*` info logs (not `.sst`, not `MANIFEST`, not `CURRENT`). Freeing
   those is non-destructive to raft state. Check mgr02 too (likely same
   provisioning).
2. **Once space is free**, restart mgr01's raft node (shut down with the fatal
   IO error); confirm it rejoins as a writable voter (quorum back to 3/3).
3. **Verify mgr03's snapshot**: if still EOF/0-byte, with BP-2 deployed it will
   self-recover via log-catchup on restart; pre-BP-2, regenerate or wipe
   `raft-sm` on that node and let it catch up from a healthy peer.
4. **Raise the manager raft partition size** (16G root is too small for the
   churn) as a provisioning change.
5. **Data-loss assessment (F2)**: enumerate the 184 `default` files, confirm
   none recoverable from data02 (`/api/v1/files/inspect`), decide re-ingest.

---

## 7. Verification log

> Append per-task.

---

## 8. Deviations from plan

> Append if reality diverges.
