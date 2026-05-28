# PLAN-BUG-BN — Persistent disk UUIDs + heartbeat audit spam

Plan id: `BN`
Status: 🟦 code complete (2026-05-28) — BN-1..BN-4 code + unit tests green (dtfs-data 68/68, dtfs-raft 52/52); release cases 26/27 written; deploy + live verification pending.
Author: Claude Sonnet 4.6
Created: 2026-05-28
Sources: BJ Deviations (disk UUID follow-up candidate); live audit feed observation post-BJ

---

## 0. Background

Two unrelated bugs discovered after BJ shipped:

**BN-1: Disk UUIDs regenerated on every boot.**
`crates/dtfs-data/src/main.rs:359`:
```rust
uuid: uuid::Uuid::new_v4().to_string(),
```
Fresh random UUID minted for every disk on every data-node boot. No file, no serial
read, no stable seed anywhere. Defeats any UUID-based disk identity: placement
dedup, scrub tracking, and BJ's `disks_identity_eq` (which worked around this by
keying on `mount_path` instead). First surfaced during BJ live testing — three
restart cycles produced three distinct UUID sets for the same physical disks.

**BN-2: UpdateNodeUsage and UpdateNodeStatus spam the audit log unconditionally.**
`crates/dtfs-raft/src/state_machine.rs`:
- `UpdateNodeUsage` arm (line 1172): calls `push_audit_capped` on every apply.
  Data nodes send usage heartbeats every 5 s. With 2 data nodes: ~24 audit rows/min.
- `UpdateNodeStatus` arm (line 1082): calls `push_audit_capped` on every apply,
  including repeated `Healthy -> Healthy` transitions from the health monitor.

Both were explicitly out of scope for BJ (BJ non-goal: "no change to heartbeat /
usage-update audit behaviour"). Post-BJ, with RegisterNode spam removed, these two
variants now dominate the audit feed (197 + 196 rows observed on live cluster),
evicting genuine integrity events against the `MAX_AUDIT_LOG_ENTRIES` cap — the same
eviction problem BJ fixed for RegisterNode.

---

## 1. Goals / Non-goals

### Goals
- Disk UUIDs stable across data-node restarts; lost only when the disk is
  wiped/reformatted.
- `UpdateNodeUsage` never writes to the audit log (telemetry, not an auditable
  event).
- `UpdateNodeStatus` only writes an audit row when the node's health status
  actually transitions (e.g., `Healthy -> Suspect`, `Down -> Healthy`). Repeated
  same-state applications produce no audit row.

### Non-goals
- No change to `UpdateNodeDiskUsage` (already has no audit call).
- No change to the heartbeat RPC wire format or cadence.
- No change to how BJ's `disks_identity_eq` keys on `mount_path` — that stays as
  the stable identity for material-equality dedup regardless of UUID stability.
- No history/retention policy for disk UUID files.
- No UUID migration for existing deployments (old random UUIDs are harmless; new
  stable UUIDs take effect on next restart).

---

## 2. Data-model / storage changes

### BN-1
New file on each managed disk: `<mount_path>/.dtfs-disk-uuid` (plain text, one UUID
per file, no JSON). Written once on first discovery; read on all subsequent boots.

No `StateMachineData` field changes. `DiskInfo.uuid` continues to be a `String`;
its value becomes stable after this fix.

### BN-2
No data-model changes. Two call sites in `state_machine.rs` are modified in place.

---

## 3. Task list

### BN-1 — Persist disk UUID to mount-path file
- Status: ✅ code (2026-05-28) — `load_or_create_disk_uuid(&Path)` in store.rs; main.rs disk-build calls it with graceful fresh-UUID fallback on I/O error. 3 unit tests (stable, corrupt-regenerated, valid-not-rewritten) green; dtfs-data 68/68. Live restart check via case 26 pending deploy.
- **Files:** `crates/dtfs-data/src/store.rs` (new helper), `crates/dtfs-data/src/main.rs` (call site)
- **Implementation sketch:**
  Add a helper in `store.rs` (or a new `disk_uuid.rs` module if cleaner):
  ```rust
  pub fn load_or_create_disk_uuid(mount_path: &Path) -> std::io::Result<String> {
      let uuid_file = mount_path.join(".dtfs-disk-uuid");
      if uuid_file.exists() {
          let s = std::fs::read_to_string(&uuid_file)?;
          let s = s.trim().to_string();
          if uuid::Uuid::parse_str(&s).is_ok() {
              return Ok(s);
          }
          warn!("disk UUID file corrupt at {:?}, regenerating", uuid_file);
      }
      let u = uuid::Uuid::new_v4().to_string();
      std::fs::write(&uuid_file, &u)?;
      // fsync file + parent dir for crash durability
      {
          let f = std::fs::File::open(&uuid_file)?;
          f.sync_all()?;
      }
      {
          let d = std::fs::File::open(mount_path)?;
          d.sync_all()?;
      }
      Ok(u)
  }
  ```
  In `main.rs:359`, replace `uuid::Uuid::new_v4().to_string()` with
  `load_or_create_disk_uuid(&d.path).unwrap_or_else(|e| { warn!("disk UUID file
  unavailable for {:?}: {e}", d.path); uuid::Uuid::new_v4().to_string() })`.
  The fallback (fresh UUID on file error) degrades gracefully for read-only mounts
  or permission issues — better than crashing the boot loop.
- **Tests (BN-1):**
  - Unit: create a temp dir, call `load_or_create_disk_uuid` twice; assert both
    calls return the same UUID and the file exists.
  - Unit: write a corrupt UUID file (`"not-a-uuid"`), call helper, assert a new
    valid UUID is returned and file is overwritten.
  - Unit: call helper on a dir with a valid UUID file; assert no file write occurs
    (mtime unchanged or read-path returns early without open-for-write).
- **Done when:** `cargo test -p dtfs-data --release` green; data node restarted 3x
  on Minicluster shows identical UUIDs for each disk in `/api/v1/nodes` across
  restarts.

### BN-2 — Suppress heartbeat audit spam (UpdateNodeUsage + UpdateNodeStatus)
- Status: ✅ code (2026-05-28) — `UpdateNodeUsage` arm: `push_audit_capped` removed (pure telemetry). `UpdateNodeStatus` arm: captures `old_health` after the BF-6 quarantine early-return, audits only when `node.health != old_health`. 3 unit tests (usage no-audit, status no-transition, status real-transition) green; dtfs-raft 52/52. `grep push_audit_capped` confirms no other unconditional heartbeat-path audit remains.
- **Files:** `crates/dtfs-raft/src/state_machine.rs`
- **Implementation sketch:**

  **`UpdateNodeUsage` (line 1172):** Remove `push_audit_capped` entirely. Usage
  counters and `last_seen_ms` are telemetry; the values are exposed live via
  `/api/v1/nodes`. No integrity or security value justifies an audit row per
  heartbeat.

  **`UpdateNodeStatus` (line 1082):** Gate the audit call on a genuine transition.
  `HealthStatus` derives `PartialEq` (`types.rs:148`), so:
  ```rust
  // capture old status before the match that mutates node.health
  let old_health = node.health;
  match status.as_str() { /* existing arms unchanged */ }
  if node.health != old_health {
      push_audit_capped(
          &mut sm.audit_log,
          audit(format!("UpdateNodeStatus: {node_id} -> {status}")),
      );
  }
  ```
  Place the capture AFTER the BF-6 quarantine guard (which returns early), so the
  audit only fires on paths that actually mutate state.

- **Tests (BN-2):**
  - Unit (`UpdateNodeUsage`): apply an `UpdateNodeUsage` to a state machine with
    a known node; assert `sm.audit_log.len()` is unchanged. Fails on current code,
    passes after fix.
  - Unit (`UpdateNodeStatus` — no transition): apply `UpdateNodeStatus { status:
    "healthy" }` twice; assert exactly one audit row total (the first application
    transitions from whatever the initial state is; the second is a no-op).
  - Unit (`UpdateNodeStatus` — real transition): apply `Healthy` then `Suspect`;
    assert two audit rows.
  - Existing BJ unit tests must remain green (they do not touch these variants).
- **Done when:** `cargo test -p dtfs-raft --release` green; live cluster audit feed
  no longer accumulates `UpdateNodeUsage` rows; `UpdateNodeStatus` rows appear only
  on genuine health transitions.

### BN-3 — Release-suite coverage
- Status: ✅ code (2026-05-28) — `tests/release/cases/26-disk-uuid-stable.sh` (restart data node, assert disk UUID set unchanged) + `27-heartbeat-audit-suppressed.sh` (wait 6 cycles, assert UpdateNodeUsage audit rows stay 0). Auto-discovered by run.sh full mode. Live PASS pending deploy.
- **Files:** `tests/release/cases/NN-disk-uuid-stable.sh`,
  `tests/release/cases/NN-heartbeat-audit-suppressed.sh`,
  `tests/release/run.sh`
- **Implementation sketch:**
  - `NN-disk-uuid-stable.sh`: capture disk UUID for a data node via
    `/api/v1/nodes`, restart `dtfs-data` on that node, re-capture UUID, assert
    values match. Status protocol; `trap case_cleanup EXIT`; `wait_for`.
  - `NN-heartbeat-audit-suppressed.sh`: capture `audit_log` length, wait 30 s
    (6 heartbeat cycles), re-capture length, assert delta == 0 (no new
    UpdateNodeUsage rows). Then trigger a node health transition
    (stop + restart `dtfs-data`) and assert exactly one `UpdateNodeStatus` row
    appears for the transition. Status protocol; `trap case_cleanup EXIT`.
- **Done when:** both cases present, registered in `run.sh`, PASS in latest run.
  Required per AGENTS.md § B.5.1 (cluster-visible: node registry + audit feed).

### BN-4 — Docs + changelog
- Status: ✅ (2026-05-28) — `changelog/UNRELEASED.md` Bug BN-1 (stable disk UUIDs + operator wipe note) and Bug BN-2 (heartbeat telemetry no longer audited; status audits only on transition) entries added.
- **Files:** `changelog/UNRELEASED.md`
- **Entries:**
  1. Disk UUIDs now persisted to `<mount_path>/.dtfs-disk-uuid` and stable across
     data-node restarts.
  2. Node usage heartbeats no longer generate audit log entries.
  3. Node status audit entries only written on genuine health-state transitions.
- **Done when:** entries present, same commit as BN-1 / BN-2.

---

## 4. Sequencing note

BN-1 and BN-2 are independent of each other and of all other in-flight plans.
BN-2 should deploy before BM-4 (deep scrub / audit verification) so the audit feed
is clean enough to observe genuine integrity events.

---

## 5. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|

---

## 6. Cross-cutting invariants

- BN-1: fallback to fresh UUID (not crash) when the UUID file is unwritable —
  read-only mounts, permission errors, full filesystem. Log a `warn!`.
- BN-1: `load_or_create_disk_uuid` must NOT be called on non-existent mount paths.
  Callers must only invoke it after the disk has been verified mountable (already
  the case at the point in main.rs where disk_infos is built).
- BN-2: removing the `UpdateNodeUsage` audit must not remove any _other_ audit
  calls in that arm. Verify the arm has no other `push_audit_capped` after the
  fix.
- BN-2: the `UpdateNodeStatus` guard must sit AFTER the BF-6 early-return
  (line 1062-1068) — the BF-6 path returns `ClientResponse::Success` without
  mutating health, so `old_health == node.health` anyway, but being explicit
  avoids confusion.

---

## 7. Deviations from plan

_(empty)_

---

## 8. Notes for the implementer

- For BN-1: confirm `ManagedDisk` in `store.rs` is the right init site vs.
  main.rs. If `ManagedDisk` has a `uuid` field that main.rs reads, put
  `load_or_create_disk_uuid` in `ManagedDisk::new`. If `DiskInfo` is built
  entirely in main.rs with no store.rs struct carrying uuid, put the helper in
  `store.rs` but call it from main.rs. Either way, the helper belongs in
  `dtfs-data`, not `dtfs-common`.
- For BN-2: after removing the `UpdateNodeUsage` audit call, do a `grep
  push_audit_capped crates/dtfs-raft/src/state_machine.rs` to confirm the only
  remaining heartbeat-path audit is `UpdateNodeStatus` (on transitions). Any
  other unconditional audit in a high-frequency path is a candidate for a follow-up
  BN task.
- The `.dtfs-disk-uuid` file is operator-visible. If an operator wipes and
  re-provisions a disk under the same mount path, they should delete this file so
  the new disk gets a new UUID. Document in the operational note in the changelog.
