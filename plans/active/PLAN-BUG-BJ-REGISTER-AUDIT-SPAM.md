# PLAN-BUG-BJ — RegisterNode spams the audit log on every re-registration

Plan id: `BJ`
Status: 🟦 in progress — BJ-1 (fix+unit), BJ-3 (live RCA), BJ-4 (docs) done. BJ-2 release case written; live PASS pending BJ-1 manager deploy + clean cluster. Operator action: disable the stray `dtfs-fuse@-mnt-dtfs.service` on mini-client01 (BJ-3 remediation).
Author: Claude Opus 4.7 (1M context)
Created: 2026-05-28

---

## 0. Background

The WebUI audit / integrity feed shows a `Register node` row roughly
every 5 seconds on the live minicluster. Reported 2026-05-28.

### Root cause (code-confirmed)

`crates/dtfs-raft/src/state_machine.rs:936-939`:

```rust
ClientRequest::RegisterNode { node_id, info } => {
    sm.nodes.insert(*node_id, info.clone());
    push_audit_capped(&mut sm.audit_log, audit(format!("RegisterNode: {node_id}")));
    ClientResponse::Success
}
```

The apply path pushes an audit entry **unconditionally** on every
`RegisterNode`. `node_id` is deterministic
(`dtfs_common::node_id_from_hostname`, see `metadata.rs:383`), so a
re-registration of an already-known node reuses the same id, overwrites
`sm.nodes` with materially-identical info, **and still emits a fresh
audit row**. There is no idempotency / dedup. Every re-registration
therefore:

1. adds a noise row to the audit feed, and
2. evicts a genuine event against the `MAX_AUDIT_LOG_ENTRIES` cap
   (`push_audit_capped`).

### The 5s cadence trigger is NOT in current source

There are exactly three `RegisterNode` callers, all one-shot:

- **Data node** — `crates/dtfs-data/src/main.rs`: registers once in a
  boot loop (breaks on accept, line ~406-451), then runs a 5s heartbeat
  loop that calls `UpdateNodeUsage` / `UpdateNodeDiskUsage`, **never**
  `RegisterNode`.
- **FUSE client** — `crates/dtfs-fuse/src/main.rs:170`
  (`register_self_as_client` → `RegisterNode` RPC, role=`client`): once
  at mount.
- **Manager join** — `crates/dtfs-manager/src/join.rs:142`
  (`complete_join`): once at membership join.

No periodic re-register loop exists in master. So the observed 5s
cadence on the live cluster comes from a component re-registering that
current source does not re-register periodically — candidates:
**deployed-binary drift** (an older data/fuse build that re-registered
per heartbeat) or **a crash-looping client/data node** restarting every
~5 s and re-registering on each boot. **This trigger must be confirmed
on the live cluster (BJ-3); do not assume.**

The audit defect (no idempotency) is real and worth fixing regardless
of which component is re-registering, because it hardens the audit feed
against churn from *any* re-registration source.

---

## 1. Goals / Non-goals

### Goals
- Idempotent re-registration of an already-known node with
  materially-identical info produces **no** new audit row.
- A genuinely new node, or a re-registration whose `NodeInfo` materially
  changed (role, hostname, grpc_addr, location, disks, version,
  build_date, client_ip), still produces exactly one audit row.
- Identify the live 5s re-register trigger so the operational side is
  understood (may be a no-code-change finding).

### Non-goals
- No change to the `RegisterNode` RPC contract or node-id derivation.
- No change to heartbeat / usage-update audit behaviour (those paths do
  not audit today and stay that way).
- No redesign of the audit-log cap or the structured-vs-legacy audit
  format (that is BH-9 territory).

---

## 2. Locked-in decisions

1. **Dedup by material-field comparison, not whole-struct equality.**
   `NodeInfo.last_seen_ms` (and usage counters `used_capacity`,
   `total_bytes_read/written`) change on every registration, so a naive
   `old != new` would always differ and never suppress. Compare only the
   identity/topology fields: `role`, `hostname`, `grpc_addr`,
   `location`, `disks`, `version`, `build_date`, `client_ip`. If those
   match an existing entry, suppress the audit row (still refresh
   `sm.nodes` so `last_seen_ms` updates).
2. **Audit on new OR materially-changed only.** Keep the existing
   `audit(format!("RegisterNode: {node_id}"))` message for the rows we
   do emit (no format change in this plan).
3. **Decision logic lives in the state-machine apply path** (the single
   durable choke point), not in the RPC handler, so the manager-join
   path (`join.rs`) and any future caller inherit it for free.

---

## 3. Task list

### BJ-1 — Make RegisterNode audit idempotent
- Status: ✅ (2026-05-28) — `node_info_material_eq` + audit only on new/material-change. 3 unit tests green (`bug_bj_register_node_*`); dtfs-raft 43/43.
- **Files:** `crates/dtfs-raft/src/state_machine.rs`
- **Implementation sketch:** In the `ClientRequest::RegisterNode` arm
  (~line 936), capture the prior entry via the `insert` return value
  (`let prior = sm.nodes.insert(*node_id, info.clone());`). Compute a
  `materially_changed` bool: `true` if `prior` is `None`, else compare
  the locked-in identity fields (§2.1) of `prior` vs `info`. Only call
  `push_audit_capped(... audit(format!("RegisterNode: {node_id}")))`
  when `materially_changed`. Add a small private helper
  `node_info_material_eq(a: &NodeInfo, b: &NodeInfo) -> bool` (or inline
  field comparison) so the field list is explicit and testable.
- **Tests (BJ-1):** unit tests in `dtfs-raft` against `StateMachineData`:
  - `register_node_new_emits_audit`: first RegisterNode for a node adds
    one audit row.
  - `register_node_reregister_identical_no_audit`: applying the same
    RegisterNode again (only `last_seen_ms` differs) adds **zero** new
    audit rows. Fails on current code, passes after fix.
  - `register_node_material_change_emits_audit`: re-register with a
    changed `version` (or `grpc_addr`) adds exactly one new row.
- **Done when:** unit assertions green; `cargo test -p dtfs-raft
  --release` clean; re-registration no longer grows the audit log.

### BJ-2 — Release-suite coverage
- Status: 🟨 (2026-05-28) — `22-register-audit-idempotent.sh` written (counts `RegisterNode: <node_id>` rows for the restarted data node only, so the client spam doesn't pollute the delta; asserts delta≤0, else `KNOWN_ISSUE [BJ]`). Auto-discovered by run.sh (not a quick case — does a data-node restart). `bash -n` clean. Live PASS pending: BJ-1 manager binary deployed AND a clean cluster (the live minicluster's stray-unit spam currently saturates the 400-row audit cap).
- **Files:** `tests/release/cases/NN-register-audit-idempotent.sh`
  (next free number), `tests/release/run.sh` (QUICK_CASES opt-in if
  ≤2 min).
- **Implementation sketch:** Bring up (or reuse) a node, capture the
  audit-log length, force a re-registration of an existing node
  (restart one data node so it re-registers with identical info), wait,
  assert the `RegisterNode` audit count did **not** increase by more
  than 1. Status protocol last line `STATUS: PASS|FAIL|...`; `trap
  case_cleanup EXIT`; use `wait_for` from `tests/release/lib/common.sh`.
- **Done when:** case present, registered in `run.sh`, row is PASS (or
  KNOWN_ISSUE pinned to BJ) in the latest release-suite run. Required
  per AGENTS.md § B.5.1 because RegisterNode + audit feed is
  cluster-visible behaviour.

### BJ-3 — Identify + document the live 5s re-register trigger
- Status: ✅ (2026-05-28) — root cause identified with evidence (below). Operational, not a master code defect. Remediation (disable the stray unit) left to the operator — the auto-mode classifier blocked the agent from modifying shared-node systemd state, which is correct for an identify-only task.
- **Finding:** all 400 audit rows were `RegisterNode` for node_id `6841539694661734231` = **mini-client01** (FUSE client), spaced ~5s (timestamp deltas), newest ≈ now → spam is *active*. The client's real mount unit `dtfs-fuse@mnt-dtfs.service` is healthy (0 restarts). The trigger is a **stray duplicate systemd unit `dtfs-fuse@-mnt-dtfs.service`** (malformed instance name — escaped path `//mnt/dtfs`, leading slash → `-mnt-dtfs`) in `activating auto-restart`: **114 restarts / 5 min**. Each boot it `Bug AA self-registered as client` then dies with `Error: fuse3 mount failed` (the real unit already owns `/mnt/dtfs`) → systemd restarts it → re-register. Not deployed-drift in the fuse logic (master registers once, main.rs:170) and not a crash of the real client.
- **Recommended remediation (operator):** `sudo systemctl stop dtfs-fuse@-mnt-dtfs.service && sudo systemctl disable dtfs-fuse@-mnt-dtfs.service && sudo systemctl reset-failed dtfs-fuse@-mnt-dtfs.service` on mini-client01, and audit the provisioning that enabled the bad instance name. BJ-1 is the durable fix: even with such churn the audit feed no longer floods.
- **Files:** this plan (Verification log) + `changelog/UNRELEASED.md`
  only if an operational finding warrants an operator note. No code
  unless BJ-3 surfaces a second code defect (then add a task).
- **Implementation sketch:** On the minicluster, determine which
  component is re-registering every ~5 s: check `journalctl -u
  dtfs-data` / `dtfs-fuse` for restart loops, compare deployed binary
  `version`/`build_date` (RegisterNodeRequest fields) against current
  master, and watch which `node_id` the repeated `RegisterNode: {id}`
  audit rows carry (map id → hostname). Record the cause: deployed
  drift vs crash-loop vs other.
- **Done when:** the trigger is identified with evidence and logged in
  §4; if it is pure deployed-drift/crash-loop (no code bug beyond BJ-1),
  note "operational, no further code change" — BJ-1 still lands as the
  durable hardening.

### BJ-4 — Docs + changelog
- Status: ✅ (2026-05-28) — changelog Bug Fixes entry added (same commit as BJ-1). monitoring.md has no audit-feed section to update.
- **Files:** `changelog/UNRELEASED.md`; `docs/src/operations/monitoring.md`
  if it documents the audit feed.
- **Implementation sketch:** One changelog line: re-registration of an
  unchanged node no longer spams the audit log. Same commit as BJ-1.
- **Done when:** changelog entry present; docs mention (if applicable)
  that RegisterNode audits only on new/changed node info.

---

## 4. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|
| BJ-1 | 2026-05-28 | `cargo test -p dtfs-raft bug_bj` | 3/3 pass; full suite 43/43 |
| BJ-3 | 2026-05-28 | `GET /api/v1/audit` + `journalctl -u dtfs-fuse@-mnt-dtfs` on mini-client01 | 400/400 rows = mini-client01 RegisterNode @~5s; stray `dtfs-fuse@-mnt-dtfs.service` crash-looping 114×/5min (fuse3 mount failed → re-register). Root cause = misconfigured duplicate unit |

---

## 5. Cross-cutting invariants

- Snapshot compat (§ A.3.1): no new `StateMachineData` / `NodeInfo`
  fields are added by this plan, so no `#[serde(default)]` work.
- The audit-log cap path (`push_audit_capped`) stays the single audit
  sink; BJ only gates *whether* we call it for RegisterNode.

---

## 6. Deviations from plan

_(empty)_

---

## 7. Notes for the implementer

- Concurrent work is active on `master` (other agents own BH code +
  uncommitted `dtfs-client/` / `dtfs-fuse/` edits). Stage only
  `state_machine.rs` (+ test) for the BJ-1 commit; do not fold in
  unrelated working-tree changes.
- Confirm the exact `NodeInfo` field set before writing
  `node_info_material_eq` — read the struct in `dtfs-raft` (it carries
  `last_seen_ms`, usage counters, `quarantine`, `client_ip`,
  `version`, `build_date`). `quarantine` is mutated by other apply
  paths, not by RegisterNode, so exclude it from the material-equality
  check to avoid suppressing a legitimate re-register that should refresh
  health.
