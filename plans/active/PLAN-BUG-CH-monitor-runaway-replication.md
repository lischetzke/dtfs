# PLAN-BUG-CH — monitor node-down runaway replication storm

Incident: 2026-06-06 ~21:03Z, production (dtfs-lm) headless >2h. Full
evidence: `plans/reports/ISSUE-SCAN-2026-06-06-2335.md`. Root cause:
`monitor.rs::handle_node_down` (and its twin `handle_disk_failed`) is a
legacy, parallel repair path with four defects — runs on every manager
(no leader gate), targets ANY healthy node (no Data-role filter, so
managers/clients; round-robin over HashMap order defeats the
(chunk,target) dedup), hardcodes `DiskTier::Hot`, and re-fires every
monitor tick because `check_nodes` re-emits Down continuously. Result:
~1920 garbage tasks/40s flooded the leader (36,162 persisted tasks),
leader OOM-wedged, election deadlock.

## Goals

- Exactly ONE owner of replica repair: the leader-gated scheduler tick.
- Node/disk loss repaired correctly: replicas on dead nodes/disks stop
  counting toward mandatory tier coverage, so `check_under_replicated`
  backfills via its existing (deduped, tier-aware, role-safe, pruned)
  machinery.
- Poisoned tasks already persisted in a cluster's migration state
  fast-fail at dispatch instead of burning 6 ReplicateChunk attempts
  against managers/clients.

## Non-goals (separate findings, triage owns)

- RegisterNode retry backoff under leaderless cluster (Finding 6).
- openraft 400ms vote-timeout tuning (Finding 1 contributor).
- advance_tasks dispatch-rate bound / data-node replication intake
  memory pressure (Finding 4).
- check_anchor_lag counting (anchor node down has no in-topology repair
  on the reference deployments; revisit if a multi-cold topology lands).
- Quarantined-node replica counting (Bug BC semantics unchanged: their
  replicas still count as existing; scrub auto-repair handles faults).

## Locked-in decisions

- User: "find and fix the root cause" (2026-06-07). Scope = the monitor
  runaway, not the contributing factors above.
- Repair consolidation over repairing the monitor's own planner: delete
  `handle_node_down` / `handle_disk_failed` instead of adding a leader
  gate + role filter + tier derivation to them (fewest moving parts;
  scheduler already has correct, tested planning).
- Monitor keeps: heartbeat-driven status detection (any manager commits
  UpdateNodeStatus via raft forward — heartbeats land on ONE arbitrary
  manager, so detection must NOT be leader-gated), NodeDown alert,
  SMART disk-status logging.

## Data-model changes

None. No new Raft variants, no proto changes, no config keys. New audit
action string: `migration.invalid_target` (same family as Bug AE's
`migration.orphan_dropped`).

## Tasks

### CH-1-1: health-aware replica counting in check_under_replicated ✅

- Files: `crates/dtfs-manager/src/scheduler.rs`
- Sketch: extract pure fn `live_replica_tier(node: Option<&NodeInfo>,
  disk_id: u32) -> Option<DiskTier>` — returns the disk tier only when
  node health ∉ {Down, Failed} and disk status ∉ {Failed, Removed};
  None otherwise. Replace the `tier_of` closure in
  `check_under_replicated` so `count_on` ignores dead replicas (chunk
  goes tier-short → existing BX-4 backfill repairs node loss). Keep
  `tier_holders` (target exclusion) on the same live view. Add explicit
  `role == NodeRole::Data` to the `healthy_nodes` target filter
  (today only implicit via empty-disk tier match).
- Tests: new `mod bug_ch_live_replica_tests` — (a) Down node → None,
  (b) Failed disk on Healthy node → None, (c) Healthy node+disk →
  Some(tier), (d) Suspect node still counts (no premature storm),
  (e) unknown node/disk index → None.
- Done when: tests pass; a chunk whose only cold replica sits on a Down
  node is scheduled for cold backfill by the tick (covered via the pure
  fn + existing under_replicated_tiers tests composing).

### CH-1-2: fast-fail invalid-target tasks at dispatch ✅

- Files: `crates/dtfs-manager/src/scheduler.rs`
- Sketch: pure fn `validate_task_target(node: Option<&NodeInfo>, tier:
  DiskTier) -> Result<(), &'static str>` — Err when target missing, role
  != Data, health != Healthy, or no live disk of `tier`. In
  `advance_tasks` Copying arm (before source resolution), look up
  `task.target_node` in sm; on Err mark task Failed immediately
  (last_error = reason, audit `migration.invalid_target`), mirroring the
  Bug AE orphan-drop. Drains poisoned queues (36k tasks in prod) in ~1
  tick each instead of 6 RPC attempts × 30s.
- Tests: `mod bug_ch_target_validation_tests` — manager target → Err,
  client target → Err, healthy Data node without the tier → Err, with
  Failed-status disk of that tier → Err, healthy Data + live tier →
  Ok, missing node → Err.
- Done when: tests pass; Copying arm transitions invalid-target tasks
  to Failed without calling replicate_chunk.

### CH-1-3: monitor stops scheduling repairs ✅

- Files: `crates/dtfs-manager/src/monitor.rs`,
  `crates/dtfs-manager/src/main.rs`
- Sketch: delete `handle_node_down` + `handle_disk_failed`; `run()`
  keeps status transitions + NodeDown alert; `check_disk_health` keeps
  SMART Degraded/Failed WARN logs (repair now falls out of CH-1-1).
  Drop the now-unused `scheduler` field from `HealthMonitor::new` and
  the main.rs wiring.
- Tests: compile (field removal breaks any stale caller); behaviour
  covered by CH-1-1/1-2 + release case.
- Done when: `dtfs-manager` builds; no `schedule_replication` caller
  left outside scheduler.rs + tiering.rs.

### CH-1-4: release-suite case ✅

- Files: `tests/release/cases/55-node-down-no-task-storm.sh`
- Sketch: on the release cluster — stop one data node's dtfs-data;
  wait past down_threshold; poll `/api/v1/migrations` on TWO ticks:
  assert (a) no task targets a Manager/Client node id, (b) task count
  stays bounded (< chunk_count × live data nodes, no growth between
  polls beyond backfill need), (c) managers still answer
  `/api/v1/health` and raft keeps a leader. Restart the data node in
  cleanup.
- Done when: case emits STATUS: PASS against a fixed cluster, FAIL
  against the pre-fix behaviour (storm).

### CH-1-5: docs + AGENTS.md sync ✅

- Files: `AGENTS.md` (§ A.8 monitor/scheduler responsibilities)
- Sketch: note monitor = detection/alerts only, scheduler tick = sole
  repair owner (extends Bug AC), and the live-replica counting rule.
- Done when: same commit as code.

### CH-1-6: status commits forward to the leader ✅

- (Added 2026-06-07 after case 55 exposed it — see Deviations.)
- Files: `crates/dtfs-manager/src/metadata.rs`,
  `crates/dtfs-manager/src/monitor.rs`, `crates/dtfs-manager/src/main.rs`
- Sketch: data nodes heartbeat ONE arbitrary manager, so down-detection
  usually happens on a follower; the monitor's UpdateNodeStatus was a
  bare client_write that silently errored with ForwardToLeader and the
  transition never committed (pre-CH this was masked only when the
  receiver happened to be the leader). New shared
  `metadata::write_via_leader` (local write or ForwardClientWrite proxy
  — same Bug BL mechanism the heartbeat writes use) +
  `is_forward_to_leader_err` classifier; monitor carries
  `cluster.initial_voters` for leader resolution.
- Tests: `bug_ch_forward_classify_tests` (both wild error shapes
  classify, real errors do not); release case 55 wait_for-Down leg.
- Done when: case 55 passes with the victim reported Down via the API.

## Cross-cutting invariants

- Mandatory replicas only via `check_under_replicated` (§ A.3.4) — this
  plan REMOVES a violator, adds none.
- Suspect (≤30s overdue) replicas still count — no repair storms on
  transient stalls; only Down/Failed lose coverage credit.
- Replica presence stays keyed (node, disk/tier) (Bug BH) — untouched.
- No mutation of anchor semantics; the LELI copy logic untouched.

## Verification log

- 2026-06-07 CH-1-1 + CH-1-2 failing test: synced to dtfs-builder, ran
  `cargo test -p dtfs-manager --release` with only the new test mods in
  the tree -> `error[E0432]: unresolved import super::live_replica_tier`
  + `...super::validate_task_target`, exit 101 (compile error = the B.5
  failing-test form for not-yet-extracted pure helpers).
- 2026-06-07 CH-1-1..1-3 implemented, same commands on builder:
  `cargo test -p dtfs-manager --release bug_ch` -> 13 passed
  (6 bug_ch_live_replica_tests + 7 bug_ch_target_validation_tests);
  full crate suite 220 passed 0 failed; full workspace
  `cargo test --release --workspace` -> all suites 0 failed.
- 2026-06-07 CH-1-3: `grep -rn schedule_replication crates/dtfs-manager/src/`
  -> callers remain only in scheduler.rs + tiering.rs (monitor gone).
- 2026-06-07 CH-1-4: `bash -n` clean. Ran the case from the builder
  against the Minicluster on its PRE-fix binary (v0.0.0-dev
  20260606-185513): STATUS: PASS but vacuous — `chunks=38 baseline=0
  poll1=0 poll2=0 victim=mini-data02`; the old monitor only storms for
  a down-node chunk with a surviving second replica as source, and the
  victim held single-replica chunks only. Victim selection strengthened
  (max used_capacity data node).
- 2026-06-07 CH-1-4 + CH-1-6 (user approved the Minicluster deploy):
  deployed build 20260607-095429 to mini-mgr01..03 leader-last →
  re-election ok (leader mini-mgr03 term 94, lag 0). Case 55 run #2:
  STATUS: FAIL — `wait_for: victim node reported Down did not become
  true in 90s`. Journals: mini-mgr02 (follower, heartbeat receiver)
  logged `Node 5195573662920911622 is DOWN` every 10s but never
  `Committed UpdateNodeStatus` — the monitor's bare client_write errors
  ForwardToLeader on a follower and the commit was dropped (latent
  pre-CH flaw, masked while receiver == leader). Implemented CH-1-6;
  crate suite 221 passed 0 failed on builder; redeployed build
  20260607-100809. Case 55 run #3: STATUS: PASS (`chunks=38 baseline=0
  poll1=0 poll2=0 victim=mini-data02`), victim reported Down within the
  window and recovered Healthy after cleanup; migrations count 0.
- 2026-06-07 CH-1-6 residual: in the PASS run the heartbeat receiver
  was again the leader (mini-mgr03), so write_via_leader's LOCAL branch
  is live-proven; the FORWARD branch is proven by unit test +
  composition (identical ForwardClientWrite path Bug BL uses for every
  follower-received heartbeat write in production). A targeted
  receiver≠leader live test required stopping a manager (quorum
  reduction on the shared lab) — denied by auto-mode; left for the
  next release-gate run.
- 2026-06-07 storm-leg caveat: on this Minicluster data shape the
  pre-fix monitor did not storm (victim chunks lacked a surviving
  second replica as copy source), so case 55's storm assertions
  gate regressions but the FAIL leg was demonstrated via the
  detection assertion instead.

## Deviations

- 2026-06-07: user approved the Minicluster deploy; CH-1-4 live-verified
  there (see log). Production deploy explicitly deferred by the user
  ("create PR only, more things to do for release v0.1.3") — prod
  recovery + the 36k-task drain happen with the v0.1.3 rollout.
- 2026-06-07: case 55 exposed a latent detection flaw (status commits
  dropped on follower receivers) → task CH-1-6 added mid-plan. Flagged
  to the user in the session summary; lands in the same PR (#27).
