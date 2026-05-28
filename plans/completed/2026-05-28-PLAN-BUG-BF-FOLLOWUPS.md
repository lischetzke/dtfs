# Bug BF — Close Bug BC deferred items + cleanups

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

Bug BC scrub/quarantine landed in the previous session but several
pieces were explicitly deferred:

- Data-side detects `verify_on_read` mismatches and returns
  `Status::data_loss`, but mgr never auto-quarantines: operators
  must read the integrity feed + click the modal manually.
- `put_chunk` doesn't self-reject when locally faulted.
- No auto-unquarantine cooldown sweep despite the config knob.
- `orphan_scrub` only handles on-disk-not-in-state.
- Per-volume `scrub_interval_secs` still wired in heartbeat
  aggregation (warn-logged only).
- Dead `fsck_interval_secs` + `custom` fields not deprecated.
- Release cases 03/04 SKIP incorrectly — `/api/v1/scrub` exists.
- openraft 0.10.0-alpha.17 deprecation warning lingers.
- 24+ GB existing orphans on lab disks not yet reaped (default
  cadence too slow for verification).
- BA 4h soak cancelled; on-demand re-run procedure undocumented.

Goal: drain the deferred list so the BC quarantine pipeline is
autonomous (data-side detects → mgr auto-quarantines → cooldown
auto-clears) and suite + docs reflect reality.

## Tasks (15 total; merge order in plan-doc § Order)

### Phase 1 — Proto + collector (managers first) — ✅ 2026-05-27
- ✅ **BF-1** Extend `HeartbeatRequest` proto with `repeated IntegrityFault integrity_faults = 10`.
- ✅ **BF-2** `IntegrityFaultCollector` in `dtfs-data` (Mutex<Vec> + AtomicBool, 4 unit tests).

### Phase 2 — Data-side — ✅ 2026-05-27
- ✅ **BF-3** `verify_on_read` mismatch + meta-absent → `collector.record(...)`.
- ✅ **BF-4** `put_chunk` self-reject when `is_self_quarantined()`.
- ✅ **BF-5** Drain collector into heartbeat; re-enqueue on both rejection AND RPC failure.

### Phase 3 — Manager auto-quarantine — ✅ 2026-05-27
- ✅ **BF-6** Mgr `heartbeat` handler iterates `integrity_faults` → `QuarantineNode` raft writes + `RemoveCorruptChunkReplicas`. Defence-in-depth: heartbeat-side `UpdateNodeStatus` to "healthy" now skipped when node is Quarantined; state_machine `apply_request` refuses status overwrite when `quarantine.is_some()`.

### Phase 3.5 — Self-quarantine clear (added 2026-05-27 — found during live test)
- ✅ **BF-x** `HeartbeatResponse.self_quarantine_cleared` (proto field 4). Mgr sets true when its view of the node is NOT Quarantined. Data side clears the sticky `IntegrityFaultCollector::self_quarantined` flag on this signal so operator unquarantine re-enables `put_chunk` WITHOUT a `systemctl restart dtfs-data`. (Not in original BF plan; discovered when live bitrot test left data01 unable to accept new writes after unquarantine.)

### Phase 4 — Scheduler ticks — ✅ 2026-05-27
- ✅ **BF-7** `run_auto_unquarantine_sweep` template `run_orphan_scrub`; cooldown=0 (default) disables.
- ✅ **BF-8** `orphan_scrub_tick` in-state-missing inverse → `QuarantineNode { OrphanScrubInStateMissing }`.

### Phase 5 — Cleanups + docs — ✅ 2026-05-27
- ✅ **BF-9** Per-volume `scrub_interval_secs` removed from heartbeat aggregation; field `#[deprecated]` + `#[serde(skip_serializing_if)]`; warn-once helper.
- ✅ **BF-10** `#[deprecated]` on `fsck_interval_secs` + `custom`; warn-once.
- ✅ **BF-11** Release cases 03 + 04 SKIP guard fixed to recognize `"status":"accepted"`.
- ✅ **BF-12** openraft `last_quorum_acked.elapsed().as_millis()` via `use openraft::Instant`.
- ✅ **BF-13** `docs/src/operations/orphan-cleanup-runbook.md` written.
- ✅ **BF-14** 4h soak on-demand procedure appended to BA plan.

### Phase 6 — soak-found bugs — ✅ 2026-05-27
- ✅ **BF-15** `plan_anchor_lag_repair` excluded any node already holding the chunk on ANY tier. Single-node-multi-tier topology (data01 with both warm + cold disks in the anchor DC) → cold replicas never scheduled, cold tier stayed empty forever. Fix: filter only by candidate_tier holders, not by ANY tier. 2 new unit tests pin both sides (self-as-target-allowed + self-as-target-skipped-when-satisfied). Live verified: 60s after deploy, data01:cold went 0 → 167 chunks via anchor.lag_repair audit rows.

## Order
1. BF-11, BF-12, BF-13, BF-14 (trivial / docs / unblocks CI signal).
2. BF-9, BF-10 (cleanups; reduce conflict surface in metadata.rs).
3. BF-7, BF-8 (scheduler-only; no proto wire change).
4. BF-1 → BF-2 → {BF-3, BF-4} → BF-5 → BF-6 (auto-quarantine pipeline; **roll managers cluster-wide before data nodes** per BF-1).

## Dependencies
```
BF-1 → BF-2 → BF-3
            → BF-4
            → BF-5 → BF-6 (also needs BF-1)

BF-7, BF-8: scheduler-only (independent)
BF-9: land BEFORE BF-6 (same metadata.rs fn)
BF-10-14: independent
```

## Verification
End-to-end on lab minicluster after Phase 4:
1. `cargo test --release -p {common,raft,manager,data}` — all green incl. new `bug_bf_*`.
2. `cargo build --release` — no openraft deprecation warning.
3. Roll managers (mgr02, mgr03, mgr01) THEN data nodes.
4. **Auto-quarantine**: `tests/proxmox/abuse/18-bug-bc-bitrot.sh` — faulty node `Quarantined` without operator action; `/ui/integrity` shows row; FUSE read succeeds via fallback.
5. **Auto-unquarantine**: set `auto_unquarantine_secs=120`, restore byte, wait 130s, node back to `Healthy`.
6. **orphan in-state-missing**: SSH data node, `rm` one `.dat`, wait `orphan_interval_secs`, assert `Quarantined OrphanScrubInStateMissing`.
7. **Per-volume scrub removed**: set + tail data01 journal — no `AZ-1 scrub override` beyond cluster default.
8. **Release suite**: `bash tests/release/run.sh` (full) — PASS=21 on 3-data-node cluster; lab's 2-data may persist case 13 SKIP.
9. **Existing orphans**: per BF-13 runbook; `du -sh /mnt/*-*/chunks` shrinks to within ~5 % of `logical_bytes × RF`.

## Commit-message convention
```
BF-<n>: <one-line summary, <72 chars>>

<body>

Plan: plans/active/PLAN-BUG-BF-FOLLOWUPS.md (BF-<n> done)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```
