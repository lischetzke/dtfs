# Internals: bug archaeology and detailed invariants

This page holds the detailed "why" behind AGENTS.md Part A's condensed
rules: the bug/task IDs (Bug-letter, CO-\*, REV-\*, WI-\*, U\*, R\*, CR-\*)
that motivated each invariant, kept here so AGENTS.md itself stays under
its 250-line budget. AGENTS.md keeps a one-line pointer at each section;
this is the full narrative. Read it when you need to understand *why* a
rule exists, are touching the exact code path it describes, or are
reviewing a diff against it — not by default.

## A.2 — Read path replica selection (U4, CO-3-10)

`location.rs::select_read_replica` orders candidates: (1) configured
`read_location_priority` — first replica whose datacenter/host matches
the earliest-listed entry, operator intent beats distance; (2) physical
distance (`Location::distance`); (3) tier tie-break on *equal* distance
only — prefer warm over the cold anchor HDD, never overriding a
strictly-nearer replica.

`GetFileLocations` carries each replica's tier (`ChunkLocationInfo.
node_tiers`, positional with `node_addresses`); an older manager leaves
it empty (unknown tier → pure distance). It also carries each replica's
topology `Location` (`ChunkLocationInfo.node_locations`, positional,
CO-3-10) — the manager fills it from each replica node's registered
Location and the FUSE client learns it into `known_locations` (keyed by
node address, `filesystem.rs::learn_locations` on metadata refresh) so
`select_read_replica` can actually rank by distance + priority. Before
CO-3-10 the map was never populated, so read routing was dead. An older
manager, or an unknown replica node, leaves an entry empty/all-empty →
not learned → that replica falls back to arbitrary/pure-distance order,
exactly like `node_tiers`.

## A.3.4 — Mandatory vs cache replicas, full detail (Bug AV)

See `.claude/rules/integrity.md` for the complete invariant — it is
duplicated there (path-scoped, loads for the crates that implement it)
because it is as much an integrity rule as an architecture one. Extra
detail not in that copy:

- `anchor_pending` (`api::count_anchor_pending_files`), `chunk_placement`
  (`placement_state::resolve_dc_tier`), and `check_anchor_lag` placement
  builds (LIS-41) all use the same live-replica gate as
  `check_under_replicated` — Down-node cold must not keep those counters
  false-green.
- Engine-driven demote != operator flush: idle-decay demote clears the
  cooldown (next read re-promotes); operator flush sets a future
  cooldown (no immediate re-promote). Route demote-like paths through
  `should_clear_cooldown_on_demote(cooldown_until_ms, now_ms)` (past/
  absent → clear; future → preserve), never an unconditional
  `SetChunkCooldown(0)`.
- A volume with an active AV cache policy (`cache_replicas`,
  `tiering::volume_has_active_cache_policy`) is driven SOLELY by the AV
  cache tick; the legacy temperature-profile demote/promote SKIPS its
  chunks (CO-4-6) — otherwise, after an operator flush sets a future
  cooldown, the legacy promote would re-create the warm replica during
  the cooldown window, defeating the flush.
- **CR-3-1 (MGR-2)** — pre-fix, `build_av_cache_snapshot`
  (`TickPlacement`) and `FlushSnapshot::from_sm` resolved a placement's
  tier from the node table alone (disk existence, not liveness), so
  `plan_chunk_demotions`'s `mandatory_still_present` and
  `plan_flush_cache`'s `mandatory_survives` could count a cold anchor
  replica sitting on a Down node (or a Failed disk) as the surviving
  mandatory copy and let the LAST live replica (a warm cache copy) be
  dropped. Both snapshot builders now resolve each placement through
  `scheduler::live_replica_tier`, and `TickPlacement`/
  `FlushNodeSummary.disk_tiers` carry a `live: bool` alongside the tier;
  the "still present" / "survives" checks additionally require
  `live == true`. The dead placement is still LISTED (so it can itself
  be dropped/repaired), just never counted as a survivor.
- **CR-3-2 (MGR-2 defence)** — `apply_remove_chunk_location` gained a
  second, anchor-independent guard: before mutating, it scans whether
  the row being removed is itself live (node not Down/Failed, disk not
  Failed/Removed) and, if so, whether any OTHER live row of the same
  `(path, chunk_index)` would remain. If not, the removal is refused
  with `"...would remove the last live replica..."`. This runs AFTER
  the existing anchor survivor scan, so an anchor-policy volume keeps
  the more specific "would strand anchor" message when both checks
  would refuse the same removal; a volume with no anchor policy (or an
  anchor-irrelevant removal) is still protected by this second guard.
  A row that is itself dead can always be removed — it wasn't serving
  anything.

## A.6 — Data-node durability hardening (CR-4)

Four related fixes to `dtfs-data/src/store.rs`'s commit/delete/scrub
paths, landed together because each one closes a window the others'
tests would otherwise exercise against half-fixed code.

- **CR-4-1 (DATA-1) quarantine by rename, scrub-repairs-forward.**
  `quarantine_chunk_on_disk` used to `remove_file` the `.dat` outright;
  it now renames it to `.dtfs/quarantine/<id>.dat.bad` (overwriting a
  stale one from an earlier quarantine of the same id), so a genuinely
  corrupt chunk's bytes survive for forensics instead of being
  destroyed. Separately, `scrub_disk`'s `meta_read` / `meta_parse` /
  missing-`.meta` branches no longer jump straight to
  `corrupt_chunk_ids`: they first re-hash the already-stat'd `.dat`
  (`try_repair_meta_from_dat`) and, on a match against the id encoded in
  the filename, rewrite `.meta` via `write_meta_atomic` and count the
  chunk healthy (`ScrubResult.chunks_meta_repaired`) instead of
  quarantining a chunk whose bytes were never actually wrong — only a
  digest mismatch or an unreadable `.dat` still quarantines.
- **CR-4-2 (DATA-2) atomic `.meta` write.** `commit_chunk_from_tmp_inner`
  used to `File::create` the `.meta` directly at its final path; a crash
  mid-write left a torn (partial/0-byte) sidecar right where readers
  look for it. `write_meta_atomic` writes to this disk's `tmp/` dir,
  `sync_all`s, renames over the final path, and fsyncs the destination
  directory, so the final path only ever holds a complete `.meta` or
  none. `recover_from_wal`'s `PutChunk` arm treats a present-but-
  unparseable `.meta` exactly like a missing one (CO-1-4b) and rolls the
  half-committed chunk back.
- **CR-4-3 (DATA-4) post-rename cleanup.** Once `commit_chunk_from_tmp_
  inner` has renamed the tmp `.dat` into place, any LATER failure (most
  likely the `.meta` write) is cleaned up by `cleanup_after_dat_
  committed`: it removes the `.dat`, the `.meta` (if any), and the WAL
  entry before returning the error, so a `.dat` never survives without
  its `.meta` companion.
- **CR-4-4 (DATA-3) per-chunk lock + op-keyed WAL.** `ChunkStore` now
  holds 256 striped `tokio::sync::Mutex<()>` (indexed by the chunk id's
  first byte, in-memory only — a fresh process starts every stripe
  unlocked, which is fine because the WAL is what makes recovery correct
  across a restart, not the lock). `commit_chunk_from_tmp_inner` and
  `delete_chunk_idempotent_on_disk` each hold their chunk's stripe for
  the whole operation, so a same-id put and delete can never interleave
  into a torn state. The WAL filename also moved from the single
  `<id>.wal.json` to op-keyed `<id>.put.wal.json` / `<id>.delete.wal.
  json`, so a put and a delete never contend over (and clobber) one WAL
  file; `recover_from_wal` matches on the `.json` extension and reads the
  operation out of the JSON body, so it keeps replaying the legacy
  `<id>.wal.json` name unchanged.

## A.3.5 — Build-date / version stamp mechanism (Bug BS, AZ-5)

`dtfs-common/build.rs` emits `DTFS_BUILD_DATE` from `SOURCE_DATE_EPOCH`
(falls back to `SystemTime::now()` when unset) and declares
`cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH`. The build wrappers
(`scripts/build-linux-amd64.sh`, `scripts/dtfs-build.sh`) export
`SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(date -u +%s)}"` before cargo
build, so each wrapper run re-stamps. A bare `cargo build` with the var
unset keeps the cached stamp (fast local incremental, but stale — never
deploy it). Reproducible-build envs that pin `SOURCE_DATE_EPOCH` are the
sanctioned exception (wrappers honour a caller-set value). `build.rs`
also emits `DTFS_VERSION` (default `v0.0.0-dev`, same
`rerun-if-env-changed`); `version.rs` reads it for `VERSION`/
`VERSION_STRING`. `scripts/dtfs-release.sh <tag>` exports
`DTFS_VERSION=<tag>` before the build so a release binary's `--version`
(and the WebUI/node table) reports the real tag; plain/dev builds stay
`v0.0.0-dev`. `dtfs-cli update --check` compares the installed version
against the latest GitHub release tag.

## A.4 — Stale-artifact trap (Bug CF)

The builder tar sync preserves source mtimes, so a just-synced edit can
land OLDER than cached build artifacts and cargo will silently reuse a
stale rlib (a lib's `#[cfg(test)]` tests can still pass — the lib-test
binary rebuilds independently — while downstream binaries link the
pre-edit lib). Before deploying a binary that must contain a fresh
change: `touch` the edited file(s) on the builder (or rebuild after a
marker change) and verify the binary actually carries the change
(`grep -ac "<new log string>" target/release/<bin>` or the `--version`
stamp). A crate-level `#![allow(dead_code)]` on the dtfs-manager and
dtfs-data binaries works around a rustc 1.94 dead-code ICE — don't
remove it without first verifying the ICE no longer reproduces on the
current toolchain.

## A.8 — File map, full narrative

### `dtfs-raft/src/network.rs::full_snapshot`

Bug CL: network failures here (peer unreachable / timeout / empty
response) MUST map to the transient `StreamingError::Unreachable`
(openraft retries), NEVER to `StreamingError::StorageError` — openraft
treats StorageError as a fatal local-storage failure and shuts down the
leader's RaftCore (caused cluster-wide quorum loss on prod-cluster). Only
genuine local snapshot reads use StorageError.

CNF-4: cross-peer snapshot sends are capped at 2 in flight
(`SNAPSHOT_SEND_PERMITS` semaphore — each send holds ~2.7x snapshot size
in heap as base64+JSON); the shared base64 encoder/decoder pair lives
here (`base64_encode`/`base64_decode`, used by the dtfs-manager snapshot
receiver). The transport is a JSON-string unary RPC — serde_bytes would
serialize as an int array (bigger than base64), so don't drop the base64
without a wire change.

CO-3-6: the peer channel sets NO channel-level `.timeout()` (tonic's
GrpcTimeout layer takes the MIN of channel + per-request timeout, so a
fixed channel timeout would clamp the snapshot RPC back down); every
`send_rpc` sets its own per-request timeout via `tonic::Request::
set_timeout`. AppendEntries/Vote pass the 5s `RPC_TIMEOUT`; the snapshot
send passes `snapshot_rpc_timeout(bytes, option.hard_ttl())` =
max(openraft's hard_ttl, snapshot_bytes / 1 MBps floored at 5s), so a
>25 MB snapshot to a learner behind the 40 Mbps LELI shape gets enough
wall-clock to land instead of dying at the old 5s default. A fired
timeout is still a tonic Status -> `unreachable_err` ->
`RPCError::Unreachable` -> `StreamingError::Unreachable` (transient),
preserving the Bug CL mapping above.

### `dtfs-raft/src/state_machine.rs`

CR-2-1 (RAFT-1): `RegisterNode`'s apply reads the prior `NodeInfo` before
overwriting `sm.nodes` and carries `Quarantined` (+ its `QuarantineInfo`)
or `Maintenance`/`Draining` forward onto the incoming `info`. The sole
producer (`metadata.rs`) always builds `health: Healthy, quarantine:
None`, so every process restart (a plain re-register) used to silently
clear an integrity quarantine or an operator's maintenance/drain state
and re-admit the node into read/allocate/replication pools. Fires an
extra `RegisterNode: <id> (sticky state preserved: <state>)` audit row
when carry-forward actually happens; `node_info_material_eq` already
excludes health/quarantine from its comparison, so this doesn't disturb
the Bug BJ audit-dedup.

CR-2-2 (RAFT-2): `RenameFile`'s apply collects every `from/...` child key
(a `BTreeMap` range, collected into a `Vec` BEFORE any mutation, since a
`BTreeMap` can't be iterated and mutated at once) and re-keys each to
`to/...`, moving its `posix_locks`/`write_leases` entry along with it. A
synthetic-dir source (no exact `from` key, but children exist under
`from/`) now succeeds instead of erroring. The rename is refused
(`ClientResponse::Error`) when the destination already has children
(real or synthetic) or when `to` lies inside `from`'s own subtree
(`to.starts_with("{from}/")`). Pre-fix, only the exact `from` key moved;
a directory's children stayed stranded under the old prefix, so `mv
/Movies/Old /Movies/New` silently "lost" every file in the moved
directory (still visible as a synthetic dir at the old path).

CR-2-3 (RAFT-3): `UpdateChunkLocations`'s apply computes `extent =
max(chunk_index * chunk_size + size_bytes)` over the merged chunks and
grows (never shrinks) an already-set `logical_size` to at least that
extent (`None` stays `None` — a chunk-only file still falls back to the
summed-chunks size in `metadata.rs`). Pre-fix, `SetFileSize` set
`logical_size` once and nothing ever revisited it, so `open(O_TRUNC)`
then rewrite (cp, rsync `--inplace`, shell `>`, SQLite journal recycle)
committed chunks past the truncated size while every reader kept seeing
the stale (usually 0) size forever.

CR-2-4 (RAFT-7): `DeleteFile`'s apply also removes the path's
`write_leases`/`posix_locks` entry and refuses (`Error`) when a child
key exists under `path/`, instead of leaking the lease (a re-create
within `lease_ms` hit a stale `WriteLeaseConflict`) and letting `rm -r`
report success on a still-populated tree. `cluster_err_to_errno` in
`dtfs-fuse/src/fuse_adapter.rs` maps gRPC `FailedPrecondition` to
`ENOTEMPTY`, and `metadata.rs::delete_file` checks the Raft response
BEFORE computing/spawning the chunk-delete fan-out (previously the
fan-out ran unconditionally, which would have deleted live chunks on a
refused delete) and maps the refusal to `Status::failed_precondition`.

CR-2-5 (RAFT-4): `UpdateChunkLocations`, `RemoveCorruptChunkReplicas`,
`RenameFile`, `SetFileSize`, `CreateSymlink`, `CreateLink`, and
`RemoveChunkLocation` each carry `#[serde(default)] now_ms: u64`. Their
apply arms compute `eff_now_ms = if now_ms == 0 { log_id.index } else {
now_ms }` (the CO-3-1 fallback pattern) and route both `updated_at_ms`
and their own audit row through a dedicated `audit_at(summary, ts)`
closure instead of the shared wall-clock `audit` closure, so two
replicas applying the same log entry produce byte-identical state.
`apply_remove_chunk_location` took an explicit `now_ms: u64` parameter
in place of an inline `SystemTime::now()` call that used to diverge per
replica. The top-of-apply `SystemTime::now()` stays for arms outside
this seven-variant scope (`DeleteFile`, `RegisterNode`, `QuarantineNode`,
...), which still route through the wall-clock `audit` closure — closing
their determinism gap is future work, not part of RAFT-4's scope.

CR-2-6 (RAFT-8, RAFT-9): `UpdateConfig`'s apply always derives `version
= sm.cluster_config.version + 1` and ignores the wire-supplied `version`
(the CAS `expected_version` check is unchanged), closing a
version-regression hole a buggy/malicious proposer could hit even when
the CAS base matched. `ReportScrubResult` clamps a non-finite
`duration_secs` to `0.0` before it reaches `ScrubRecord`, since
`serde_json` turns NaN/inf into `null` on serialize and every later
snapshot load then fails to deserialize (discarding the whole snapshot —
see RAFT-6).

### `dtfs-manager/src/scheduler.rs`

**REV-1-1 — every manager background loop is leader-gated, not just
`tick()`.** `TaskScheduler::is_leader()` is the single gate;
`run_orphan_scrub`, `run_auto_unquarantine_sweep`, `run_lease_gc`,
`TieringEngine::run` and `POST /api/v1/files/flush_cache` all take it.
These loops dispatch *direct data-node delete RPCs*, which no Raft
rejection can undo, so a follower running them physically deletes live
bytes (a follower's tiering read-window is always empty, so it reads
every cache replica as idle). Any new background loop MUST take this
gate. The tiering state sweep runs BEFORE the gate: it must keep pruning
on followers, which are exactly the nodes that ingest `report_access`
without ever evaluating.

REV-1-2: the orphan reaper additionally requires a caught-up state
machine (`orphan_sweep_allowed`) — reaping against a still-replaying view
classifies every file created during the outage as an orphan.

REV-3-4: task creation is bounded (`MAX_NEW_TASKS_PER_TICK`,
`MAX_LIVE_TASKS`) with an O(1) per-tick dedup index; the scan is
idempotent so the remainder resumes next tick, and the cap is logged,
never silent.

REV-3-10: a location row whose bytes are missing is RECONCILED (dropped,
never the last live copy, anchor-checked) so the backfill sees the real
deficit; the node is quarantined only when a row cannot be reconciled or
the miss count is node-scale.

Key entry points: `schedule_path_migration` (operator moves),
`flush_cache_for_path` + `plan_flush_cache` (drop cache replicas; emit
`cache.flush_*` audit + `AnchorViolated` on refusal),
`check_under_replicated` (counts coverage via `live_replica_tier` —
replicas on Down/Failed nodes or Failed/Removed disks don't count, so
node/disk loss backfills here), `validate_task_target` (dispatch
fast-fails tasks targeting non-Data/dead/tier-less nodes →
`migration.invalid_target` audit), `check_misplaced_rebalance` /
`plan_excess_spill_removal` (Bug BX-5 rebalance).

**CR-3-1..3-6 (MGR-1/2/4/5/6/7/8/9), 2026-09-12 core-review fixes:**

- CR-3-1/MGR-2: see § A.3.4 above (`plan_flush_cache`/`plan_chunk_demotions`
  liveness-gated survivors).
- CR-3-4/MGR-4/8/9: `MigrationOps::register_replica` returns
  `Result<(), String>` — the Completing dispatch (Phase 4 of
  `advance_round`) routes `Err` through the existing `record_copy_failure`
  helper (bumps `attempts`, stays Completing, Failed+alert only at
  `MAX_MIGRATION_ATTEMPTS`) instead of unconditionally advancing to Done.
  `register_replica_location` collapsed two separately-acquired SM read
  locks into one and delegates to a pure `plan_replica_registration(files,
  chunk_hash, target_node, resolved_disk_id)`, which registers the new
  replica at EVERY chunk_index in EVERY referencing file that carries the
  hash (not just the first file/first index under the old two-lock split,
  which could observe two different snapshots of the file tree between
  the lock acquisitions and default a vanished hash to chunk_index 0 —
  MGR-9) and returns no writes at all when the hash is unresolvable.
  `resolve_disk_id_for_tier` now returns `Option<u32>`, filtering
  Failed/Removed disks (MGR-8): a dead disk of the right tier is never a
  valid registration target.
- CR-3-5/MGR-5/6: `check_under_replicated`'s dedup index and
  `schedule_replication`'s `already_scheduled` check both use
  `task_blocks_requeue` (not a bespoke `!= Done && != Failed`), so a
  lingering Failed task's retention window also blocks an immediate
  re-queue — pre-fix, a deterministically-failing chunk (its only source
  replica on a Failed disk) got a fresh task every 30s tick forever.
  Source selection now goes through a pure `pick_live_source` (first
  entry whose `live_replica_tier` resolves, not "any row on a healthy
  node" — a copy on a Failed disk of an otherwise-healthy node is not
  readable); target selection through a pure `pick_backfill_target`,
  which additionally requires `placement_cfg.has_room` on the candidate
  disk (`TaskScheduler` gained a `placement_cfg` field, wired from
  `config.placement.clone()` in `main.rs`). `check_anchor_lag` now
  threads a `created`/`live_tasks` pair through the same
  `task_budget_available` bound `check_under_replicated` uses (pre-fix
  it had NO bound at all) and inserts the just-scheduled `(chunk_id,
  target_node)` into `existing_tasks` immediately after each schedule
  instead of only once at the top of the pass, closing the window where
  two files sharing a hash (content-addressed re-download/copy) could
  both schedule a task against the same `(chunk, target)`.
- CR-3-6/MGR-7: the heartbeat handler's integrity-fault-batch block
  (`metadata.rs`) now captures `Err` from both the `QuarantineNode` and
  `RemoveCorruptChunkReplicas` forwards via a pure
  `integrity_fault_batch_accepted(quarantine_ok, remove_corrupt_ok)` and
  surfaces either failure as `HeartbeatResponse.accepted = false`
  (previously hardcoded `true`); the data node already re-enqueues
  drained faults on `!accepted` (Bug BF-5), closing the same
  election-window drop class CO-3-7 fixed for `report_chunk_status`.
  `AppendAudit` in the same block stays best-effort.

`advance_tasks` drives the task state machine each 30s tick. U3 made it
multi-transition-per-tick (round loop until no progress, so a chunk
climbs Pending->Done in ONE tick) + bounded-parallel copy
(`MIGRATION_COPY_CONCURRENCY`=6 in-flight `ReplicateChunk` RPCs
globally; WI-04 added `MIGRATION_COPIES_PER_TARGET`=2 in-flight against
any ONE target address, gated by a per-round `HashMap<tgt_addr,
Semaphore>` built fresh in `advance_round` — every anchor-repair copy
targets the single LELI anchor node, so without the per-target gate the
global cap fair-shared one 40 Mbps link 6 ways and no single copy
finished before its deadline; the permit is acquired BEFORE the RPC so
the deadline starts only once the copy actually begins, and the gate
queues surplus jobs, it never drops them; `MAX_ADVANCE_ROUNDS`=8 guard).
The transition logic lives in the generic `advance_migration_tasks` over
the `MigrationOps` trait (`TaskScheduler` is the live impl; unit-tested
via a mock in `advance_round_tests`); a task still attempts at most one
copy + one verify RPC per tick (preserves the pre-U3 retry cadence +
verify-mismatch re-copy-next-tick).

**Bug CN — flood-aftermath robustness:** (a) `tick` prunes terminal
(Done/Failed past-retention) tasks FIRST via `prune_terminal_tasks` and
persists that immediately, BEFORE the under-replication scan + advance —
pre-CN the prune ran only at the end of `advance_tasks`, so a large
terminal backlog (prod 2026-06-10: 2439 Failed tasks after the Bug CM
wedge) starved its own GC and every leader reloaded the poison; (b) each
`replicate_chunk`/`verify_chunk` RPC is wall-clock bounded — without it a
stalled data node hung the `buffer_unordered` copy round and the whole
scheduler loop after one leader-onset pass.

WI-04: `verify_chunk` stays on the fixed `MIGRATION_RPC_TIMEOUT` (60s —
it re-hashes a file already local to the target, no bytes cross the
wire, so it doesn't scale with the link); `replicate_chunk` is bounded by
`dtfs_common::replication::migration_rpc_timeout(chunk_bytes)`, sized
from the chunk's own byte count (`chunk_size_in_any_file`, looked up
alongside the Bug AE orphan guard) = **60s at the 4 MB default chunk**
(Bug CN's bound preserved exactly), scaling at 1 MiB/s to a hard **300s**
ceiling, and always strictly greater than the data node's own inner
`replicate_transfer_timeout(chunk_bytes)` budget for the same copy
(`dtfs-data/src/replication.rs::replicate_to`; **30s** floor, **270s**
ceiling).

**B4:** PRE-WI-04 the two were unrelated fixed constants and INVERTED — a
fixed 60s outer deadline under a fixed 120s inner one made the callee's
own timeout dead code at every chunk size, since the manager always
aborted first (dropping a partial transfer the source still considered
healthy). WI-04's first cut removed the inversion the wrong way round:
it kept the 120s inner floor and derived the outer from it (>= 150s for
every chunk, including the 4 MB default), which restored outer > inner
but walked back Bug CN's bound. The landed fix LOWERED the inner floor
(120s -> 30s) rather than raising the outer, so Bug CN's 60s bound holds
again at the default chunk size; both budgets are additionally capped
(inner 270s, outer 300s) so a 128 MB-chunk volume can't park an advance
round for minutes either. The inner deadline MUST stay strictly below
the outer at every chunk size, including at the cap boundary — that
inversion must never come back.

The pre-tick prune may add a SECOND `save_migration_state` Raft write
when it drops tasks; steady-state is still one. **CO-3-5:** `tick` and
`schedule_replication` both load-modify-save the migration state; both
hold the `migration_state_lock` (`tokio::Mutex`) across the load..save
so a concurrent schedule during a tick can't clobber a task or regress
`next_task_id`.

The orphan scrub (`orphan_scrub_tick`) reaps per-node in two cases, both
gated by the Bug CB durable-mtime grace (`orphan_reap_due`): a HASH
orphan (no file references the hash; `chunk_is_orphan`) AND **CO-3-2** a
DE-LOCATED chunk (on a node's disk but the SM records no location entry
naming that node — the rebalance / excess-spill path drops the SM entry
via `RemoveChunkLocation` but never deletes the bytes;
`chunk_delocated_on_node`); the reap audits `cause=orphan|delocated`. The
reap stays node-wide (legacy first-found delete; ListChunks carries no
disk_id).

**CO-3-3:** before the BF-8 in-state-missing quarantine fires, it
re-reads the CURRENT SM (`current_expected_chunks_for_node` +
`still_missing_after_recheck`) and drops any chunk a concurrent
demote/unlink de-located between the tick's opening snapshot and the
ListChunks reply — so a healthy node is never quarantined for a demote
race.

**CO-5-7:** `FlushSnapshot`/`FlushAction` carry the file's OWN volume
anchor (not the default volume's) so a non-default-volume flush enforces
its own anchor. **CO-5-8:** the flush `truncated` counter counts the
remaining would-drop replica entries directly, not
`chunks_examined - actions.len()`.

**CO-1-3:** the cache-flush dispatch (and the tiering demote dispatch)
de-locate each replica Raft-remove-FIRST via the shared
`delocate_replica` over the `DelocateOps` seam (live impl
`SchedulerDelocate`): `RemoveChunkLocation` (with `enforce_anchor`)
commits BEFORE the `DeleteChunkReplica` byte delete. A Raft-remove
failure issues no byte delete (`raft_failures`++, retry); a post-commit
byte-delete failure is NOT rolled back (`rpc_failures`++, audit
`rpc_failed_after_remove`) — the leftover bytes are reclaimed by the
CO-3-2 reaper. `check_misplaced_rebalance` is already Raft-remove-only
(it never deletes bytes; the reaper mops the excess-spill leftover).
Ordering covered by `co_1_3_delocate_order_tests`.

### `dtfs-manager/src/monitor.rs`

Heartbeat health detection only (Bug CH): commits node-status
transitions, fires NodeDown/SMART alerts. MUST NOT schedule repair — the
2026-06-06 storm came from a monitor-owned evacuation path
(PLAN-BUG-CH). **Leader-gated (Bug CM):** only the current leader
evaluates liveness + commits `UpdateNodeStatus` (and runs
`check_disk_health`), via the same `metrics.current_leader == id` gate
as the scheduler (Bug AC); `monitor_status_changes` returns empty
off-leader. Bug CJ moved liveness onto the replicated `last_seen_ms`, but
a non-leader reading its own bursty-stale replicated view still
committed `Down` and flapped the node (prod prod-cluster v0.1.5, 2026-06-09,
two cross-DC learner managers) — the leader's log is authoritative +
single-writer. Recovery stays the heartbeat handler's
`UpdateNodeStatus(healthy)` (any manager, forwarded).

### `dtfs-manager/src/metadata.rs`

gRPC service; thin wrappers over `raft_write(ClientRequest::*)`.
Follower-originated writes proxy to the leader via `ForwardClientWrite`;
CO-2-2 authenticates that RPC — the forwarder stamps
`ForwardWriteRequest.shared_secret` (field 2) from
`config.cluster.shared_secret` and the leader rejects a mismatch with
`permission_denied` via a CONSTANT-TIME compare (`forward_secret_ok`,
reusing `dtfs_common::crypto::constant_time_eq` — never `==` on the
secret); an empty configured secret skips the check (back-compat for
plaintext clusters). Any new forward site MUST populate that field.

CO-3-7: `report_chunk_status` commits its corrupt/missing handling
(UpdateNodeStatus + RemoveCorruptChunkReplicas) through
`raft_write_or_forward` and returns `accepted: false` on failure — never
`let _ = raft_write(...)`, which on a follower silently dropped the
de-location so the bad replica stayed advertised healthy and
re-replication never queued. The dtfs-data heartbeat loop re-enqueues the
drained corrupt ids when the ack is `accepted == false` (same as a
transport error), so the retry actually happens.

**CR-3-3 (MGR-1):** the `ReportChunkReads` intercept in
`forward_client_write` (runs before Raft, applying directly to the
LEADER's in-memory tiering engine) used to call `tiering.report_access`
with a hardcoded `file_path=""` and `DiskTier::Hot` for every reported
chunk. `path_to_volume.get("")` in `tiering::evaluate` is always `None`,
so the CO-4-6 AV-cache-volume skip could never see it, and the legacy
temperature path ran on every reported chunk regardless of its volume's
cache policy — bypassing `promote_reads`/the operator-flush cooldown and,
for chunks with zero total reads, misclassifying as Frozen and
scheduling a doomed Tape migration every tick. The intercept now
resolves the real `file_path` and the replica's real tier on `node_id`
via `scheduler::live_replica_tier`. `tiering::evaluate` also gained its
own defence: a new pure `legacy_path_should_skip(file_path,
path_to_volume, av_cache_volumes)` skips a profile whose `file_path`
doesn't resolve to any known file at all (as `""` never does), in
addition to the existing CO-4-6 AV-volume skip.

**R1/MGR-3 (PR #107 review):** the first cut resolved each entry
one-by-one via a single-chunk `resolve_chunk_read_context(sm, chunk_id,
node_id)` — an O(files x chunks) `files_referencing_chunk` scan per
entry — while holding `self.sm_data.read()` across every
`tiering.report_access(...).await` call in the loop (each of which
takes tiering's own `access_stats` write lock). An N-entry
`ReportChunkReads` batch therefore cost O(N x total_chunks) work under
the SM read lock, and — because `tokio::sync::RwLock` is fair — blocked
the Raft apply writer (and every other SM reader) for the duration: the
same MGR-3 anti-pattern already flagged for `build_av_cache_snapshot`.
Fixed by a new pure `build_chunk_read_context_index(files, nodes,
wanted, node_id)` that resolves every wanted chunk hash in ONE pass
over `sm.files` (first-referencing-file semantics preserved: the path
comes from the first file whose BTreeMap/lexicographic position holds
any row of that hash; the tier comes from a row in that SAME file whose
`node_id` matches the reporter). The intercept now builds this index
under one short `sm_data.read()`, drops the lock, then loops
`report_access` with no SM lock held at all.

**R2/MGR-1 (PR #107 review):** `resolve_chunk_read_context` (kept as
the single-chunk helper the unit tests exercise directly) and the new
batch index both used to fall back to `DiskTier::Hot` when the
reporting node's replica couldn't be resolved as live (unknown disk, or
the node/disk Down/Failed/Removed) — reintroducing exactly the
fabricated-tier bug this task fixed: `evaluate` would still plan a
bogus Hot->Warm "demote" off a tier that was never actually being
served. Both now resolve to `None` (no entry / no tier) for an
unresolvable replica, and the intercept skips `report_access` entirely
for such an entry (logged at `debug`) instead of feeding the engine a
synthetic tier.

**CR-3-6 (MGR-7):** the heartbeat's integrity-fault-batch block
(sibling to the `ReportChunkReads` handling above) had the same
silent-forward-failure shape CO-3-7 fixed for `report_chunk_status`,
just not yet fixed here: `let _ = raft_write_or_forward(QuarantineNode
{..})` and the batched `RemoveCorruptChunkReplicas` forward were both
fire-and-forget, and `HeartbeatResponse.accepted` was hardcoded `true`
regardless. During an election (or any transient forward failure) the
node ended up neither quarantined nor de-located, and the corrupt
replica stayed advertised healthy until the next scrub cycle. Both
writes now route through a pure `integrity_fault_batch_accepted(
quarantine_ok, remove_corrupt_ok)`; either `Err` sets `accepted = false`,
and the data node's heartbeat loop already re-enqueues its drained
faults on that (Bug BF-5) — same retry contract as CO-3-7.

### `dtfs-manager/src/api.rs`

REST endpoints (WebUI + CLI); `get_stats` is the WebUI home aggregator.
The config read endpoints (`get_config`, `get_config_effective`) return
the **effective** config = the node's startup `state.config` with the
Raft cluster override (`cluster_config.config_json`) projected on top via
`json_deep_merge` (Bug CM) — a saved cluster-wide override MUST read back
as `current`, or the WebUI editor looks broken. Adding a config read
path? Route it through `effective_config_value`, never raw
`state.config` (CO-5-9 — the `get_config` `defaults` block follows the
same merge). The write path (`update_config`) deep-merges the override
onto the startup config (which, since `UpdateConfig` replaces the
override wholesale, IS the post-save effective config) and REQUIRES the
merge to deserialize as `ManagerConfig` (`merge_and_validate_override`,
400 on type error, CO-4-3), then persists `toml::to_string_pretty` of the
validated merge — never raw override JSON into the `.toml`.

Operator node decommission (`POST /api/v1/nodes/{id}/decommission` ->
`decommission_node_endpoint` -> Raft `ClientRequest::DeregisterNode`)
removes a drained/dead node from the registry; the apply REFUSES
(`ClientResponse::Error`) if the node still holds any chunk replica
(`sm.files[*].chunks[*].node_id`) — integrity guard A.6 #5, drain/
re-replicate first. The endpoint pre-checks the same and the apply
re-checks authoritatively (TOCTOU-safe).

### `dtfs-manager/src/join.rs`

Two-phase data-node join (HMAC handshake; `/api/v1/join/{request,
complete}` in api.rs). Phase-1 `handle_join_request` verifies HMAC(secret,
nonce), issues a challenge, and persists it with a 60s single-use TTL
(`challenges` map). Phase-2 `complete_join` (CO-2-3 security) requires
HMAC(secret, challenge) (constant-time `verify_completion_inner`,
consumes the challenge), derives `node_id =
node_id_from_hostname(hostname)` SERVER-SIDE (never trust a client id),
then does the `RegisterNode` Raft write. Empty `cluster.shared_secret` =
documented unauthenticated downgrade (skips the challenge/HMAC checks).
See `docs/src/security/cluster-join.md`.

### `dtfs-data/src/replication.rs::replicate_to`

Push-replication engine (source node streams a chunk to a replica target
in ~1 MiB pieces, Bug CI). WI-04: its own transfer timeout (connect +
stream + remote commit + CO-5-4 hash validation) is
`dtfs_common::replication::replicate_transfer_timeout(total)`, sized
from the chunk's on-disk byte count (30s floor, 270s ceiling, B4), not a
fixed constant.

### `dtfs-common/src/replication.rs`

WI-04 shared replication-deadline helpers (`replicate_transfer_timeout`,
`migration_rpc_timeout`); the one home for the outer(manager)-vs-
inner(data node) deadline pair since `dtfs-manager` does not depend on
`dtfs-data`. Mirrors `dtfs-raft/src/network.rs::snapshot_rpc_timeout`
(CO-3-6). B4: inner budget `REPLICATE_TIMEOUT_FLOOR` 30s /
`REPLICATE_TIMEOUT_CEILING` 270s; outer adds the unchanged 30s
`MIGRATION_RPC_MARGIN`, capped at `MIGRATION_RPC_CEILING` 300s — 60s
outer at the 4 MB default chunk, 300s outer at >=270 MB, strictly above
the inner at every size.

### `dtfs-data/src/memory.rs`

Bug G data-node rung: re-exports the common core. REV-4-5 deleted
`run_monitor_with_cache` and unwired `cache.rs`: the in-memory ARC chunk
cache had no `put` caller anywhere, so the "shrink ARC" rung evicted
nothing while its budget still throttled `put_chunk` slot sizing.
`[cache]` is parsed but inert for this role.

### `dtfs-fuse/src/filesystem.rs`

See `.claude/rules/fuse.md` for the await-guard and write-batching
detail (path-scoped, loads for this crate). Additional write-path
narrative not in that copy:

CR-1-1: `writebuf.rs`'s `FileWriteBuffer` is `BTreeMap<u32, ChunkBuf>`
(per chunk index), NOT one contiguous `Vec<u8>` spanning the file —
`write()` splits at chunk boundaries before merging, so a write below an
already-flushed chunk or past the resident high-water mark can never
zero-fill (and later re-emit as real data) a chunk it doesn't touch; a
chunk whose buffered range doesn't cover `0..chunk_size` is tagged
`needs_seed` on the emitted `ChunkSegment` instead of being gap-filled.

CR-1-2: `write()`'s RMW seed (`chunks_needing_seed()`) runs over EVERY
chunk index the write touches, including index 0 (no more
`effective_offset > 0` gate), and a seed-read failure propagates as an
error (`?` -> EIO) instead of a `warn!` that let a truncated chunk get
committed; the O_APPEND pre-seed in `open_with_append` runs AFTER the
write lease is granted and resolves the tail chunk via
`chunk_at(max_idx)`, never a positional index.

CR-1-3: `flush()` returns `Ok(())` immediately for a non-writable fh (the
kernel issues FUSE flush on every close(2), including read-only opens)
before touching the path-keyed write buffer; `release()` only drops that
buffer when the fh being released is writable AND no other writable fh
remains open on the same path (`other_writer_open()`).

CR-1-4: `OpenFile.committed_any` is set on every open handle sharing a
path the moment `flush_segments_to_cluster` first commits a chunk for
it; `flush()` now ALWAYS `write_buffer.restore_segments()` on a
cluster-write failure (never unlinks), and the F-013 ghost-unlink moved
to `release()` (`should_ghost_unlink(flush_failed, committed_any)`),
firing only when the path has NEVER committed anything — a file with any
committed chunk is never unlinked on a later flush failure.

CR-1-5 (DATA-5 FUSE half): `flush_segments_to_cluster` hashes each
segment once (`blake3_hex`) and sends it as the declared `chunk_id` on
every `write_chunk_bytes` call (previously always `""`); a reply whose
`chunk_id` disagrees is treated as a failed replica (excluded from
`successes`), never committed as a location.

R2: `flush_segments_to_cluster` also refuses (`segments_committable()`)
to commit ANY segment with `offset_in_chunk > 0` — reachable whenever the
RMW seed in `write()` was skipped because there was nothing cheap to seed
FROM (no cached `locations_snapshot`, no `chunk_at(idx)` entry, or an
empty `node_addresses` list; each now `warn!`s at the skip site) — since
the data node stores a `PutChunkRequest`'s bytes as the chunk's whole
content from byte 0, and a partial segment would land shifted.

### `dtfs-fuse/src/readahead.rs`

See `.claude/rules/fuse.md` for the current prefetch-fan-out and
Shed-rung mechanism. Historical note: the old `cache_bytes() >=
max_cache_bytes` dormancy gate in `spawn_prefetch` is gone. It only ever
saw STORED bytes, never in-flight ones, so it could not bound
concurrency; and because `store_in_cache` always evicts down to at most
the budget, the stored total lands exactly ON the budget under the
shipped power-of-two defaults (64 MB `readahead.cache_mb` / 4 MB
`client.chunk_size_mb` = sixteen 4 MB entries), which made the old `>=`
gate go true and stay true from the first `cache_mb` of reads onward,
silently killing all further prefetch — **for the default sizing only**.
With ragged sizes (a file's short final chunk, or a `cache_mb` not a
multiple of the chunk size) the stored total settles just below the
budget and the old gate never fired at all, so "permanently true" was
never an unconditional property of `store_in_cache`. Memory stays
bounded by byte eviction (Bug CE) plus the `prefetch_slots` in-flight
cap; do NOT re-add a byte gate. B1: the old `started_fetches:
Arc<Mutex<Vec<String>>>` test-observability field (pushed on every
prefetch, never drained — an unbounded production heap leak) is gone.

### `dtfs-fuse/src/fuse_adapter.rs`

Cookie scheme at `dir_entry_cookie()` — don't diverge readdir/
readdirplus (Bug F). CR-1-6 extended the CO-4-16 error-mapping rule (see
`.claude/rules/fuse.md`) to `readdir`/`readdirplus` (were a bare
`|_| ENOENT`) and to `mkdir`/`unlink`/`rmdir`/`create`/`rename`/
`symlink`/`link` (were a bare `EIO`) — every metadata op's error arm now
goes through `cluster_err_to_errno`.
