# PLAN-BUG-CM — Leader-gate the health monitor (residual CJ flap)

## Problem

On prod (`dtfs-lm`, v0.1.5 build 20260609-174832) data-node health flaps
Down/Healthy on a ~9 s period for the two network-distant data nodes
(`dtfs-fsn-data01`, `dtfs-hun-data01`), while the LELI-local `dtfs-lm-data01`
never flaps. The flap drives a `cache.promote` raft-write flood (tiering
re-queues promotes whose target keeps going Down) on the 2 GB leader and
leaves 344 migration tasks stuck `pending/attempts:0`. Evidence:
`plans/reports/ISSUE-SCAN-2026-06-09-2124.md`.

## Root cause

`crates/dtfs-manager/src/monitor.rs::run()` evaluates liveness and commits
`UpdateNodeStatus(down)` on **every manager**, with no leader gate. Bug CJ
(#29) changed the liveness source from a per-manager in-memory heartbeat map
to the replicated `last_seen_ms`, on the assumption that "every manager
observes the same freshness." That assumption breaks for a **learner whose
applied log is bursty-stale**: the two co-located learner managers
(`dtfs-fsn-mgr01`, `dtfs-hun-mgr01`) sit on flaky cross-DC Tailscale links;
the leader's `AppendEntries`/heartbeat to them intermittently times out
(`HeartbeatWorker ... failed to send a heartbeat: Err(Elapsed(()))` in the
leader log). Between catch-up bursts the learner's `sm.nodes.last_seen_ms`
is stale, so its 10 s monitor tick computes `elapsed > DOWN_MS (30 s)` and
commits `UpdateNodeStatus(down)` via `write_via_leader`. That fights the
heartbeat handler's `UpdateNodeStatus(healthy)` (committed every 5 s) → the
committed `NodeStatus` flaps. `applied_log_lag` reads 0 when sampled via the
API because replication catches up between the monitor ticks that fired off
the stale window.

Confirmed live 2026-06-09 ~21:36 UTC: both learners committing
`UpdateNodeStatus(<data-node>, down)` every ~10 s for all three data nodes;
the three voters' monitors committed no Down since their 21:08 restart;
clocks synced within ~1 s (not skew); `liveness_changes`/`last_seen` wiring
correct.

The scheduler already avoids this exact class with the **Bug AC leader
gate** (`scheduler.rs::tick`, `metrics.current_leader == metrics.id`). The
monitor's own module doc says repair is "owned exclusively by the
leader-gated scheduler tick" — but the monitor's *detection + status
commit* was never itself leader-gated.

## Goals

- Only the current raft leader evaluates data-node liveness and commits
  `UpdateNodeStatus` transitions. Followers/learners do not.
- Stop the flap (and therefore the downstream `cache.promote` flood + stuck
  promotes) without changing thresholds, heartbeat routing, or tiering.

## Non-goals (separate follow-ups, noted for triage)

- Data-node heartbeat fail-over off a flaky-forward learner to a voter.
- `cache_replicas.cooldown_secs:0` + `promote_reads:1` promote-policy damper
  on volume `default` (Finding 4 of the scan; operator config change).
- Raising `DOWN_MS`/`node_liveness_secs` for cross-DC links.
- Fixing the flaky leader→learner link itself.

## Decisions (locked)

- Mirror the scheduler's Bug AC gate exactly: read
  `self.raft.metrics().borrow_watched()`, leader iff
  `current_leader == id`. No new node_id plumbing (metrics carry `id`).
- Encode the gate in a pure, unit-testable helper
  `monitor_status_changes(nodes, now, suspect, down, is_leader)` that wraps
  `liveness_changes` and returns empty when `!is_leader`. `run()` also gates
  `check_disk_health` (alerts/logging) behind the same `is_leader`.
- Recovery path is unchanged (heartbeat handler's `UpdateNodeStatus(healthy)`
  from any manager, forwarded). On leader loss, the new leader resumes
  detection within one tick; during a no-leader window nobody marks Down
  (correct — there is no one to commit to anyway).

## Tasks

### CM-1 — failing test + leader gate ✅
- Files: `crates/dtfs-manager/src/monitor.rs`.
- Test (purest layer, `bug_cm_leader_gate_tests`): a 60 s-stale Data node
  yields `[(id, Down)]` when `is_leader=true` and `[]` when `is_leader=false`.
  Fails to compile today (`monitor_status_changes` absent) = the failing test.
- Impl: add `monitor_status_changes` (gate + `liveness_changes`); `run()`
  computes `is_leader` from raft metrics, calls it, and gates
  `check_disk_health` behind `is_leader`.
- Done when: `cargo test -p dtfs-manager --release` green; the new test
  fails before the gate, passes after.

### CM-2 — release-gate case ✅ (written; Minicluster live-verify pending)
- Files: `tests/release/cases/60-monitor-leader-gate-no-flap.sh`.
- Asserts: across a window, at most one manager (the leader) ever logs
  `dtfs_manager::monitor: ... is DOWN` / `Committed UpdateNodeStatus(... down)`
  for a healthy data node; a follower/learner logs none. `QUICK_CASES` stem.
- Done when: case PASSes on Minicluster; KNOWN_ISSUE→PASS auto-promote on
  deploy.

### CM-3 — docs/AGENTS in same commit ✅
- `AGENTS.md` § A.8 monitor.rs line: add "leader-gated (Bug CM) — detection
  + status commits run only on the leader, like the scheduler's Bug AC gate."
- Update the monitor module doc to state the leader gate.

## Cross-cutting invariants

- Monitor stays detection-only (Bug CH) AND now leader-only (Bug CM).
- No change to the heartbeat-handler recovery contract.

## Verification log

- CM-1 (2026-06-09, builder 10.150.0.150): added `monitor_status_changes`
  gate + 2 tests. "Failing" state = compile error (the fn did not exist
  pre-change); the gate is what makes `is_leader=false` return empty while
  the ungated `liveness_changes` returns `Down` for the same stale node
  (`stale_data_node_is_down`). After fix:
  `cargo test -p dtfs-manager --release monitor::` →
  `follower_emits_no_status_changes_even_when_stale ... ok`,
  `follower_silent_for_suspect_range ... ok` (7 passed). Full crate suite:
  `cargo test -p dtfs-manager --release` → `240 passed; 0 failed`. (Needed
  `cargo clean -p dtfs-proto` first — synced tree hit the stale-proto
  artifact trap on `ChunkLocationInfo.node_tiers`.)
- (pending) Minicluster `tests/release/run.sh --quick` incl. case 60.
- (pending) prod deploy (5 managers, leader-last) + live re-sample: flap
  gone, cache.promote flood drained, 344 promotes + anchor_pending clear.

## Deviations

- (none yet)
