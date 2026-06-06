# PLAN-BUG-CF — Production hardening: fan-out leak, raft membership, honesty gaps

Source: ISSUE-SCAN-2026-06-06-1550 (plans/reports/) + operator decisions
2026-06-06. Production cluster "dtfs" (environments.yml `production`).

## Goals

1. Stop the delete fan-out orphan leak (Bug N path) and make it observable.
2. Online raft membership change, then join `dtfs-dc1-mgr01` into the live
   cluster without re-initialization.
3. Enforce `write_safety.min_replicas_before_ack` (currently stored but
   consumed by nothing on the write path).
4. Close observability gaps that masked all of the above.
5. `[memory]` ladder for manager + fuse roles.
6. Truth-pass over recovery/join docs (procedures describe code that does
   not exist).

## Non-goals

- TLS implementation (tracked as CF-6 stub; cluster runs on tailscale —
  acceptable interim per operator risk acceptance).
- Second durable replica / external chunk backup: operator explicitly
  accepted single-cold-copy risk (decision 2, 2026-06-06).
- Plexmedia reference-topology parity.

## Locked-in decisions (operator, 2026-06-06)

- Keep the dc1 manager role and fix membership properly (no re-init
  improvisation; backup-recovery.md's restore-then-bootstrap flow
  contradicts `raft.initialize()` semantics and has never been tested).
- Volume `default`: `promote_reads=1`, `promote_window_secs=43200`
  (applied live via set-cache, verified `changed:true`).
- `min_replicas_before_ack = 1` is the intended ack floor
  (`[defaults.write_safety]` updated on all 4 manager tomls; stored volume
  value `2` is inert until CF-3 lands, then must be reconciled).
- Memory caps: data.toml `[memory]` (dc1 1024 MB, prod-data01 2048 MB,
  verified engaged) + systemd MemoryHigh/MemoryMax drop-ins on all
  manager/data/fuse units (live via set-property + persisted drop-ins).
- CF-2 membership class (user, 2026-06-06): dc1 joins as **LEARNER** with
  full state replication — no voter promotion for now. The voter set
  stays the 3 prod managers; the promote/remove endpoints still ship so
  the API is complete, but production rollout stops at add-learner.

## Data-model / API changes

- New manager API (leader-only, shared-secret-gated, mutating):
  - `POST /api/v1/raft/add-learner` `{hostname, addr}` →
    `Raft::add_learner`
  - `POST /api/v1/raft/promote` `{hostname}` → `Raft::change_membership`
    (voters ∪ node)
  - `POST /api/v1/raft/remove` `{hostname}` (voter demote/remove)
  - GET `/api/v1/raft/state` extended: `learners[]`, `membership_config`.
- dtfs-cli: `raft add-learner|promote|remove|members`.
- Audit actions: `raft.add_learner`, `raft.promote`, `raft.remove`,
  `delete.fanout_failed` (new), result semantics per existing vocabulary.
- No proto changes expected (openraft API is in-process; HTTP only).
- No `StateMachineData` fields added (membership lives in openraft log) —
  if any field IS added, § A.3.1 serde(default) + regression test applies.

## Tasks

### CF-1: delete fan-out failure — root-cause, re-dial, observability 🟦 (CF-1a ✅)

**Root cause FOUND (CF-1a, 2026-06-06):** `metadata.rs` DeleteFile fan-out
built its endpoint as `format!("http://{}", addr)` on the registry
`grpc_addr` that already carries the scheme (Bug W) →
`http://http://ip:port` → tonic transport error on every call. The Bug W
part-2 helper `scheduler::normalize_endpoint` existed precisely for this
and was simply not used at this call site. Fix: extracted pure
`plan_delete_fanout()` (groups chunks per node, normalizes endpoints),
3 regression tests (`bug_n_fanout_*`), release case
`tests/release/cases/49-delete-fanout-reclaims.sh` (+ QUICK_CASES).
Remaining in CF-1: `delete.fanout_failed` audit event (with CF-4b).

- Files: `crates/dtfs-manager/src/metadata.rs` (Bug N fan-out site),
  client-channel construction/caching used by the fan-out path.
- Evidence: gRPC `transport error` against `http://100.64.0.20:9500` while
  raw TCP connects and data↔data gRPC works; 61 failures 12:49-13:48 UTC,
  burst of 119/123 deletes ~14:30-14:41 UTC; 0 since the target data node
  restarted 14:41 UTC (but also 0 deletes since — unverified, probe via
  FUSE create+delete was permission-blocked this session).
- Sketch: reproduce with verbose tonic logging; inspect whether the fan-out
  reuses a cached channel created while the target was down (13:28-13:33
  restart window) and never re-dials; fix = re-dial on transport error
  (or per-call channel with connect timeout). Emit
  `delete.fanout_failed` audit row on every failure (today journald-only).
- Tests: unit on channel-retry decision if extractable; release case
  `tests/release/cases/NN-delete-fanout-recovers.sh`: stop data node,
  unlink file, restart data node, assert fan-out retries/reconciler drains
  + audit row exists.
- Done when: live probe (create+delete via FUSE) produces either clean
  fan-out or audited failure + eventual orphan reap; no silent leak.

### CF-2: online raft membership change + join dc1 🟦

- Files: `crates/dtfs-manager/src/api.rs` (routes), new
  `crates/dtfs-manager/src/membership.rs` (pure validation helpers),
  `dtfs-cli`. (`DtfsRaft` already exposes openraft's
  `add_learner`/`change_membership` directly — verified
  openraft 0.10.0-alpha.17: `add_learner(id, node, blocking)`,
  `change_membership(impl Into<ChangeMembers>, retain)`,
  `ChangeMembers::{AddVoterIds, RemoveVoters, RemoveNodes}` — no
  dtfs-raft changes needed.)
- Sketch: leader-gated endpoints calling openraft `add_learner` /
  `change_membership`; map hostname→node_id via `node_id_from_hostname`;
  reject if addr unreachable or hostname collides with an existing
  non-manager node_id. Update `initial_voters` docs: list is bootstrap-only;
  runtime membership via the new API.
- **Decision (user, 2026-06-06): LEARNER with full state replication.**
  dc1 replicates the full log/snapshot (fixes the empty-API symptom),
  serves local reads; no quorum-math change. Promote/remove endpoints
  implemented for API completeness but not applied to dc1.
- Tests: state-machine/unit where possible; Minicluster live-verify
  (add-learner a fresh manager, watch InstallSnapshot, promote, remove);
  release case `NN-raft-add-learner.sh`.
- Production rollout: build via wrapper (§ A.3.5), rolling deploy managers
  leader-last, then `dtfs-cli raft add-learner dtfs-dc1-mgr01
  100.64.0.10:9401`; verify dc1 `/api/v1/raft/state` shows leader +
  catching log, `/cluster/status` converges to the prod view.
- Done when: dc1 manager serves the same cluster view as the leader;
  original user symptom closed.

### CF-3: enforce min_replicas_before_ack ⬜

- Files: write/commit path (`dtfs-manager/src/metadata.rs`
  commit_chunk_locations / allocate path; possibly dtfs-fuse ack handling).
- Evidence: grep shows zero consumers of `write_safety.min_replicas_before_ack`
  outside display/tests — the setting is decorative; writes ack on
  whatever placement succeeded (BV-1 comment in api.rs:392 already admits
  "counts ANY replicas" for the projection, but nothing gates the ack).
- Sketch: thread volume write_safety into the commit decision; ack only
  after ≥N distinct `(node,disk)` replicas durably committed; timeout →
  error per `write_timeout_secs`. Reconcile stored volume `default` value
  (2) with operator intent (1) BEFORE enabling enforcement — needs either
  a set-write-safety endpoint (add alongside set-cache) or volume
  migration.
- Tests: state-machine/unit for the gate; release case write-with-ack=2 on
  single-replica topology must block/timeout, ack=1 must succeed.
- Done when: configured ack floor provably gates client ack; volume
  `default` carries the intended value 1.

### CF-4: observability honesty ⬜

- Files: `dtfs-manager/src/api.rs` (audit ordering), `audit.rs`,
  `webui.rs` + `/api/v1/nodes` projection.
- Items:
  a. Local-ring `get_audit_log` returns OLDEST-N (`query()` iterates
     insertion order + truncate) while the replicated branch returns
     NEWEST-N (`.rev().take`) — make local ring newest-first to match;
     update docs note (audit-log.md gotcha then shrinks).
  b. `delete.fanout_failed` audit/integrity event (CF-1 emits; this task
     surfaces it in `/api/v1/integrity/faults` filter + WebUI feed).
  c. Node registry: a Manager-role node that is NOT in raft membership
     (voter or learner) must not render plain "Healthy" — add
     `raft_member: bool` to the nodes projection; WebUI badge
     "unjoined". Production evidence: leader roster showed
     `dtfs-dc1-mgr01 Manager Healthy`, `manager_count:4` while dc1 was an
     empty term-0 learner.
- Tests: unit per item (ordering, projection); release case extends the
  existing nodes/audit checks.
- Done when: an unjoined manager is visibly flagged; audit `limit` is
  newest-first on both branches; fan-out failures appear in the feed.

### CF-5: [memory] for dtfs-manager and dtfs-fuse ⬜

- Files: `dtfs-common/src/config.rs` (add `memory: MemoryConfig` to
  ManagerConfig + FuseConfig), `dtfs-manager/src/main.rs` (sampler +
  ladder hooks: shrink caches, pause background tasks), `dtfs-fuse`
  (sampler + readahead/local-cache shrink), examples + docs per the
  config-surface sync rule (§ B.6).
- Note: systemd MemoryHigh/MemoryMax drop-ins deployed 2026-06-06 are the
  interim backstop; in-process ladder degrades gracefully instead of
  cgroup-kill.
- Tests: config parse defaults (extend example parse tests), ladder unit
  tests mirroring Bug G's data-node ones.
- Done when: both roles log "memory budget configured" on startup with a
  non-zero cap from their toml; AGENTS.md § A.3.2/A.5 updated to "all
  roles".

### CF-6: TLS implementation (stub) ⬜

- Wire the existing cert machinery (join-issued certs, TlsConfig) into
  tonic server/client builders for manager + data gRPC; remove the
  "plaintext (TLS not yet implemented)" warnings; update
  docs/src/security/tls.md status banner.
- Large; schedule after CF-1..4. Interim risk accepted (tailscale).

### CF-8: manual scrub trigger actually dispatches ✅

- Evidence (2026-06-06 prod): `dtfs-cli scrub --level quick` → API
  `accepted` + audit row, but ZERO scrub starts on any data node
  (`trigger_scrub` in api.rs is audit+log only; heartbeat
  `ScrubConfigOverride` carries interval policy, not on-demand runs;
  `scrub.rs` always logs `Starting {} scrub` — absence conclusive).
  Release case 03 only asserted the `accepted` contract, so the gap was
  never caught. Same genus as CF-3 (decorative setting).
- Files: `proto/manager.proto` (`ScrubRequestNow` + HeartbeatResponse
  field 5), `dtfs-raft/src/lib.rs` (`ClientRequest::TriggerScrub`,
  `PendingScrub`), `state_machine.rs` (`pending_scrub` field +
  apply arm + § A.3.1 snapshot test), `dtfs-manager/src/api.rs`
  (trigger_scrub raft-writes; rejects non-all targets instead of
  pretending), `metadata.rs` (heartbeat relays pending request),
  `dtfs-data/src/scrub.rs` (`manual_scrub_due` dedupe/staleness helper,
  `run()` selects on a manual-trigger channel), `dtfs-data/src/main.rs`
  (heartbeat → channel), release case 03.
- Sketch: trigger raft-replicates `PendingScrub{id,level,requested_at_ms}`
  (any manager's heartbeat handler relays it — Bug CC lesson: data
  nodes heartbeat to arbitrary managers); response carries it every
  heartbeat; data node dedupes by id (in-memory) and ignores requests
  older than 1 h (restart-reconnect guard); manual run resets that
  level's interval timer. Request is never cleared, only replaced —
  dedupe + staleness make that safe.
- Tests: state-machine apply + old-snapshot-default (unit),
  `manual_scrub_due` (unit), case 03 extended to assert
  `stats.last_scrub` advances after trigger.
- Done when: trigger on a live cluster produces `Starting quick scrub`
  on every data node + a fresh `scrub_results_by_node` entry; case 03
  fails against a pre-CF-8 manager.
- Mixed-version note: roll ALL managers before first trigger (new
  ClientRequest variant must not hit an old manager via raft); old data
  nodes ignore the new heartbeat field harmlessly.

### CF-7: docs truth pass — recovery/join procedures ⬜

- Files: `docs/src/operations/backup-recovery.md` ("Single Manager
  Recovery" auto-join fiction; "Full Manager Group Recovery"
  restore-then-bootstrap contradicts `raft.initialize()` non-empty-state
  refusal; `raft-snapshots/` path does not exist — actual dirs are
  `raft-log/` + `raft-sm/`), `docs/src/security/cluster-join.md` (scope:
  data/client registration, not raft membership — verify + state it).
- Rewrite around the CF-2 reality once it lands (recovery = restore member
  state, rejoin via snapshot; growth = add-learner API).
- Done when: every documented recovery/join step maps to code that exists,
  verified against Minicluster.

## Cross-cutting invariants

- § A.3.3 integrity rules on any touched delete/replication path; orphan
  reaping stays mtime-grace-based (Bug CB).
- § A.3.4 replica classes: CF-3 must count mandatory+cache replicas
  correctly by `(node_id, disk_id)` (Bug BH).
- § A.3.1 snapshot compat for any new persisted field.
- Release-suite registration (§ B.5) for every cluster-visible change.

## Verification log

- 2026-06-06 (pre-plan ops, this session): set-cache
  `{promote_reads:1, promote_window_secs:43200}` on volume `default` →
  `changed:true`, GET verified. `[defaults.write_safety]` ack=1 on all 4
  manager tomls (applies at restart; backups `manager.toml.bak-*`).
  data.toml `[memory]` dc1=1024/prod-data01=2048 → both daemons restarted,
  log `Bug G memory budget configured` (dc1 slots=32, prod-data01 slots=96).
  systemd memory drop-ins + live `set-property` on dtfs-manager ×4,
  dtfs-data ×2, dtfs-fuse@mnt-dtfs ×1, values confirmed via
  `systemctl show`. environments.yml fixed (tabs, dc1 manager key →
  dtfs-dc1-mgr01). Leader at execution time: prod-cluster-mgr03, term 20.
  Bug N: 0 occurrences since prod-data01 dtfs-data restart 14:41 UTC, but
  0 deletes since either — CF-1 verification still open (FUSE probe
  denied by session permissions).
- dc1 configs: operator had already fixed `[node].hostname` +
  4th `initial_voters` entry (= `dtfs-dc1-mgr01`) on all 4 managers before
  this session's survey; config side of the join is DONE, only CF-2
  membership remains.

- 2026-06-06 CF-1a deployed + live-verified: built v0.0.0-dev
  (20260606-151610) via wrapper from master. Tests: dtfs-common 52/52,
  dtfs-manager 186/186 on the build host (~/dtfs2-cf, separate from the
  operator's ~/dtfs2). Rolling deploy dc1-mgr → mgr01 → mgr02 → leader
  mgr03; quorum stable (term 20, lag 0 on all, leader unchanged). Restart
  also activated the [defaults.write_safety] ack=1 tomls. Live verify:
  FUSE create+delete of /.cf1a-verify-probe → leader logged DeleteFile
  with ZERO "Bug N" warnings (pre-fix: warn on every delete). NOTE:
  operator mass-deleted the library pre-fix (files 112 → 3); ~238 GB of
  leaked orphan chunks remain on the cold disk until the orphan reaper's
  24 h mtime grace elapses — expect a large reclaim within ~2 days; do
  not mistake it for data loss.

- 2026-06-06 CF-2 implemented + lab-verified. Unit: `membership::tests`
  5 new tests observed failing (todo! stub) then green;
  `cargo test --release --workspace` 428 passed / 0 failed on the
  builder. Built `20260606-170540` via wrapper; rolling-deployed
  dtfs-manager to mini-mgr02 → mini-mgr03 → leader mini-mgr01 (quorum
  stable throughout, term 78 unchanged, lag 0). Release case
  `50-raft-add-learner.sh` PASS on the Minicluster — full lifecycle:
  throwaway learner boots → bad shared_secret 403 → follower answers
  409 + leader hint → add-learner ok → learner replicates FULL state
  (`applied_log_lag` 0, its API serves the real cluster view — the
  production dc1 symptom) → promote → voter → remove → membership
  restored (3 voters, no learners, no residue) → audit rows
  `raft.add_learner`/`raft.promote`/`raft.remove` all `ok`.
  **Production join of dtfs-dc1-mgr01 not yet executed** (deploy +
  add-learner on prod-cluster needs operator go; decision = learner only,
  no promote). Incidental fix this session: `.gitattributes`
  `*.sh text eol=lf` (e8fbab6) — the Windows-checkout CRLF worktree
  broke the builder's shell wrappers via the tar sync.

- 2026-06-06 CF-8 implemented + lab-verified. Observed failure first:
  live production trigger (`dtfs-cli scrub --level quick`, audit row
  `scrub.trigger.quick` ts 1780770916) produced ZERO scrub starts on
  either data node in 90 s (scrub.rs always logs `Starting {} scrub` —
  absence conclusive); plus `E0063 missing field pending_scrub`
  compile-fail for the new state-machine field. Green: dtfs-raft 73 /
  dtfs-data 60 / dtfs-manager 191, workspace 438/0 on the builder.
  Built `20260606-185513` via wrapper; deployed Minicluster managers
  (mgr02→mgr03→leader mgr01) + data nodes (data01, data02). Live:
  one trigger id `0808f054-…` → BOTH data nodes logged
  `CF-8 manual quick scrub request … accepted` →
  `Starting quick scrub on all disks` within one heartbeat (~5 s),
  ran it exactly once (id dedupe held across the 5 s re-delivery),
  results shipped (case 03 asserts fresh `scrub_results_by_node` from
  every data node). Release cases: 03 PASS (now asserts EXECUTION +
  honest 400 on per-target), 04 PASS, 50 regression PASS. The
  "case 03 fails pre-CF-8" leg holds by construction (pre-CF-8
  dispatches nothing → wait_for times out); not separately re-run
  against the old binary. Production gets CF-8 with its next deploy.

## Deviations

- Session work rebased from stale branch bug-cb-orphan-reap onto master
  (cherry-picks 9e051a2..0a255ce); two doc conflicts resolved by keeping
  both sides (master's AZ-5 [updater] sections + this plan's new
  sections). Release case renumbered 42 → 49 (master's chaos suite took
  42-48).
