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

CR2-7-6 (RAFT-6) closes the hazard that discard opens: `main.rs` startup
now calls `log_store::blank_sm_startup_allowed(sm_is_blank, last_purged)`
right after opening the log store and state machine — a blank state
machine (BP-2 discarded a corrupt snapshot) combined with a log already
purged past some point refuses to start as a voter, since the discarded
snapshot was the only surviving copy of everything before the purge
point. A genuinely fresh, never-bootstrapped node (`last_purged ==
None`) is unaffected.

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

**CR-5-1 (PROTO-1):** `heartbeat` and `report_chunk_status` were the
only two mutating `ManagerService` RPCs not gated by
`require_mgr_rpc_auth` — any caller who knew (or guessed) a node id
could poison that node's health status or drop replica entries via
`report_chunk_status` without the cluster secret. Both handlers now
call `require_mgr_rpc_auth(secret, "Heartbeat"|"ReportChunkStatus",
&req.node_id, request.metadata())` before `into_inner()`, matching the
`delete_file` template. `dtfs-data/src/main.rs` stamps both outbound
calls via a new `stamp_mgr_rpc_request` (mirrors
`dtfs-client::stamp_mgr_mutation`) from `[cluster].shared_secret`;
`RegisterNode` is untouched — it carries its own `shared_secret` field.
Empty secret still skips on both sides (legacy back-compat). Deploy
manager and data together.

**CR-5-2 (MGR-11):** CO-2-2's `forward_secret_ok` authenticates WHO
forwards a write via `ForwardClientWrite` but never checked WHAT was
forwarded — an authenticated caller (a compromised or buggy follower,
or anyone holding the shared secret) could smuggle an arbitrary
`ClientRequest` (e.g. `UpdateConfig`) through the leader's
`raft.client_write` as if it were a legitimate heartbeat or
health-monitor forward. A new pure `fn forwardable(&ClientRequest) ->
bool` allow-lists exactly the variants a follower legitimately
proposes today (grepped from `raft_write_or_forward` /
`proxy_to_leader` / `write_via_leader`): `UpdateNodeUsage`,
`UpdateNodeDiskUsage`, `UpdateNodeStatus`, `AppendAudit`,
`QuarantineNode`, `RemoveCorruptChunkReplicas`, `ReportScrubResult`,
`ReportChunkReads`. `forward_client_write` checks it immediately after
deserializing the forwarded request (before the `ReportChunkReads`
fast path and before any Raft write) and denies + audits
(`category=raft, event=auth.denied`) anything else.

**CR-5-3 (MGR-12):** `write_via_leader` mapped only the transport
`Result` of `forward_client_write`, so a forward that transported fine
but was REJECTED by the leader (bad shared secret, an MGR-11
disallowed variant, or a failed Raft apply) came back `Ok(Response)`
at the tonic layer with `ForwardWriteResponse.success == false` and was
silently treated as a successful write — the caller (health monitor,
heartbeat forward) believed the write landed when it never did. A new
pure `fn forward_result(&ForwardWriteResponse) -> Result<(), String>`
inspects `.success`/`.error`; `write_via_leader` now returns its
verdict instead of discarding the response body.

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

### `dtfs-data/src/main.rs` — control-plane hardening (CR-5-4/5-6)

**CR-5-4 (DATA-6):** the data node's `Heartbeat`/`ReportChunkStatus`/
`RegisterNode` calls carried no per-request deadline, and no manager
peer channel set an HTTP/2 keepalive, so a manager that accepted the
TCP connection but stopped answering (wedged leader, partition) could
hang the heartbeat loop indefinitely; a half-open connection was only
detected by the OS-level TCP timeout (minutes). `dtfs-common/src/
tls.rs::apply_keepalive` (`http2_keep_alive_interval(10s)`,
`keep_alive_timeout(5s)`, `keep_alive_while_idle(true)`) is now wired
into every `TlsContext::endpoint()`, and a new `control_request<T>`
(`set_timeout(10s)`) wraps `register_node` directly and, via
`stamp_mgr_rpc_request`, `heartbeat`/`report_chunk_status`.

**CR-5-6 (DATA-8):** `ChunkStatusReport` sent every drained corrupt id
in one gRPC message — after a large scrub or a rolling-restart-sized
backlog this could be an oversized report. A pure `fn batches(ids,
n) -> Vec<Vec<String>>` (`CHUNK_STATUS_REPORT_BATCH_SIZE = 1000`)
splits the send into fixed-size batches; on the first failed batch
(transport error or `accepted == false`) the loop re-enqueues that
batch plus every unsent batch after it — not the whole original set —
so a batch the manager already accepted is never re-sent on retry.

### `dtfs-data/src/server.rs::record_cache_hit` (CR-5-5, DATA-11)

The last mutating data-plane RPC not gated by LIS-19/55/83: any caller
who could reach a data node's gRPC port could inflate a chunk's read
counter (and so its tiering promotion score) without the cluster
secret. Now gated by `require_data_rpc_auth(secret, "RecordCacheHit",
&req.chunk_id, md)`, bound to `chunk_id` exactly like `GetChunk`.
`dtfs-client/src/client.rs::record_cache_hit` stamps `x-dtfs-rpc-*`
from `[client].shared_secret`, mirroring `get_chunk`'s stamp block.

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

## Archive — text removed from AGENTS.md and .claude/rules on 2026-09-13

Moved here verbatim during the token-efficiency pass so the load-bearing files stay short. Nothing below is a live rule on its own; the condensed rules in AGENTS.md and `.claude/rules/` are authoritative.

### AGENTS.md A.2 Read paragraph (pre-2026-09-13)

**Read:** lookup metadata (cached ENTRY_TTL) → pick the read replica (`location.rs::select_read_replica`: configured `read_location_priority` beats physical distance beats a tier tie-break on equal distance only) → stream GetChunk → optionally cache locally (P0-11) → readahead prefetches. Full U4/CO-3-10 detail (how tier + topology reach the FUSE client's `known_locations`): `docs/src/internals/invariants.md` § A.2. CR-6-4 (FUSE-6): `GetFileLocationsResponse` carries the file's `volume_id` on the wire; the FUSE client's `fetch_file_locations` keeps whatever volume_id it already believes when an older manager sends `""`, never silently downgrading to `"default"` — and `allocate_chunks` always resolves placement from the file's OWN registered volume when it already exists, ignoring a stale/wrong request value. CR-6-6 (FUSE-9): a foreground chunk-miss is single-flighted by chunk_id (content-addressed) — the first miss becomes the fetch Owner, a concurrent miss on the same chunk_id joins the Owner's result instead of issuing its own `GetChunk`.

### AGENTS.md A.3.2 (pre-2026-09-13)

**A.3.2 Memory discipline.** No `Vec<u8>` accumulation of full chunks on the hot path, read or write — stream to/off disk, hash incrementally (Bug I, U2). Memory caps are best-effort, never reject by default (§ A.5). `[memory]` (CF-5) runs on all three roles, shared core `dtfs-common/src/memory.rs`, each with its own ladder rung (data sheds put_chunk; manager pauses scheduler ticks at Shed; fuse halves readahead at Warn, stops prefetch at Shed). `MemoryConfig::validate()` (CR-6-1/CMN-1) is called by all three `main.rs` right after config load, rejecting `warn_threshold_pct >= shed_threshold_pct` or either `> 100` at startup instead of flapping the ladder at runtime; the manager also calls the new `PlacementConfig::validate()` (CMN-8) rejecting a `min_free_pct` outside `0.0..=100.0`. systemd `MemoryMax` backstops all roles. FUSE detail: `.claude/rules/fuse.md`. CR-6-5 (FUSE-5): the FUSE client's `metadata_cache` is bounded (`METADATA_CACHE_MAX_ENTRIES = 4096`, FIFO eviction via a DashMap + insertion-order queue) and periodically swept for expired entries on the `metadata_ttl` cadence — before this, one full chunk-location list per path ever stat'ed was retained forever, pinning hundreds of MB during a Plex library scan on a 1 GB client. CR2-8-3/CR2-8-4 (FUSE-11/12): `local_cache.rs`'s disk cache now classifies a viewer read from cumulative per-fh consumption (`OpenFile.bytes_read_on_fh`) or active readahead rather than one 128 KB kernel call's length, and `evict_to_size` targets 90% of `max_bytes` (hysteresis) instead of a full O(N) stat sweep on almost every `put()`.

### AGENTS.md A.3.3 (pre-2026-09-13)

**A.3.3 Integrity is the prime directive.** Every path that touches bytes must: (1) carry a BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2) write new chunks to tmp + atomic rename after `sync_all()`; (3) WAL the op before performing it, clear only after fsync; (4) never delete a chunk until a replacement is verified or the referencing file is unlinked and no other live file references the id (Bug CI `fanout_delete_allowed`) — a scrub-detected fault quarantines by RENAMING the `.dat` to `.dtfs/quarantine/<id>.dat.bad` (CR-4-1), never `remove_file`, and only after a re-hash of the `.dat` against its own id fails to repair the `.meta` in place; (5) never silently downgrade replication factor; (6) verify on every read and on scrub (Bug BC-8) — a mismatch is `Status::data_loss`, FUSE falls over (P0-2), disabling `verify_on_read` needs an explicit API-boundary ack; (7) LIS-19/55/83 — when `cluster.shared_secret` is set, every mutating data-plane RPC requires an HMAC-SHA256 `x-dtfs-rpc-ts`+`x-dtfs-rpc-mac` pair (empty secret = legacy skip), including `RecordCacheHit` (DATA-11 — previously ungated, letting any caller inflate a chunk's tiering promotion score); (8) LIS-56 — mutating ManagerService RPCs require the same HMAC shape over `dtfs-mgr-rpc`, including CR-5-1's `Heartbeat` and `ReportChunkStatus` (previously ungated: any node id could poison node status or drop replica entries without the cluster secret) — data nodes stamp both from `[cluster].shared_secret`; deploy manager+data together; REST mutations stay on LIS-18. Full checklist + HMAC shape: § A.6 / `.claude/rules/integrity.md`.

### AGENTS.md A.3.4 (pre-2026-09-13)

**A.3.4 Mandatory vs cache replicas (Bug AV invariant).** Two independent classes, never conflated: `mandatory_replicas.<tier>` (protected, only the under-replication scheduler maintains them) and `cache_replicas.<tier>.{min,max}` (demand-driven, evictable, only the tiering engine creates them). Every promote/demote checks the anchor against the destination first (CO-1-2). Replica presence/removal is keyed by `(node_id, disk_id)`, never `node_id` alone (Bug BH, CO-1-1). CR-3-1 (MGR-2) — every planner that decides whether a "surviving" mandatory/anchor replica justifies dropping another one (`plan_chunk_demotions`'s `mandatory_still_present`, `plan_flush_cache`'s `mandatory_survives`) must resolve that survivor's liveness through `live_replica_tier` first; a replica on a Down/Failed node or Failed/Removed disk can never be the thing that authorises the drop, and the state-machine's `apply_remove_chunk_location` (CR-3-2) independently refuses to remove the last live replica of a chunk regardless of anchor policy. CR-3-7 (CMN-5) — an anchor `datacenter` string and a node's registered `datacenter` are compared with `eq_ignore_ascii_case` (`AnchorPolicy::is_satisfied_by`, the two `hash_ring.rs` anchor-DC compares), matching `SpillPolicy`/`read_location_priority`: a casing mismatch must never make the anchor permanently unsatisfiable. New code that creates/removes replicas: consult `.claude/rules/integrity.md` / `docs/src/internals/invariants.md` § A.3.4.

### AGENTS.md A.5 tail (pre-2026-09-13)

CR2-8-1 (CMN-3): `[grpc].max_message_size_mb` (manager/data) and the mirrored `[client].max_message_size_mb` (fuse) now actually govern the tonic `max_decoding/encoding_message_size` caps via the shared `dtfs_common::config::max_message_bytes` helper — previously a hardcoded 32 MB literal ignored the parsed config value — and each role's `main.rs` calls the matching `validate()` (reject `0`) alongside `memory.validate()`.

### AGENTS.md A.6 (pre-2026-09-13)

## A.6 Data-integrity audit checklist

Run mentally when you touch a write/migrate/delete/scrub path; name the covering test in the commit body only where it isn't obvious. Full checklist with narrative context lives in `.claude/rules/integrity.md`, loaded automatically for `dtfs-data/**`, `dtfs-manager/src/scheduler.rs`, `dtfs-raft/**`. Summary: (1) BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2-3) tmp+atomic-rename+`sync_all()` for both `.dat` and `.meta` — `.meta` is written via a `write_meta_atomic` tmp+rename helper (CR-4-2), never a direct create at the final path, so a crash never leaves a torn sidecar; (4) counters update only after meta is durable, and only for a genuinely new chunk (a re-upload of an already-complete chunk is not double-counted, CR-4-6); (5) tmp removed on any put error path, and any failure AFTER the `.dat` rename also removes the `.dat`/`.meta`/WAL entry it just committed (CR-4-3) — a 256-way striped per-chunk lock plus op-keyed WAL names (`.put.wal.json`/`.delete.wal.json`, legacy `.wal.json` still replayed) keep a same-id put and delete from interleaving into a torn state (CR-4-4); (6) migration verifies before marking Done, de-location is Raft-remove-then-byte-delete (the CO-3-2 reaper mops a failed byte-delete); (7) scrub BLAKE3-checks vs `checksum`, auto-repairs via `replicate_chunk` — but first tries to repair a missing/unparseable `.meta` by re-hashing the `.dat` against its own id (CR-4-1); only a genuine mismatch or unreadable `.dat` quarantines; (8) unlink deletes metadata before bytes, orphan reap keys off durable on-disk mtime (Bug CB), not a manager-local clock — CR2-7-2 (MGR-10): when the data node's `ListChunks` reply carries `chunk_disk_ids`, the reap is keyed by `(node_id, disk_id)` instead of node-wide, so a de-located leftover on one disk of a node is reaped correctly even when another disk of the SAME node still holds a live replica of the same chunk; an older data node reporting no disk ids falls back to the prior node-wide behaviour; (9) rename is one Raft command, chunk_id binding unchanged; (10) reads use replica-fallback (P0-2).

### AGENTS.md A.8 table (pre-2026-09-13)

| File | Responsibility |
|---|---|
| `dtfs-raft/src/state_machine.rs` | `apply_request`, the authoritative state-mutation point. CR-2-1..2-6 (RAFT-1/2/3/4/8/9): `RegisterNode` carries forward sticky Quarantined/Maintenance/Draining state instead of resetting to Healthy, `RenameFile` re-keys every child under a renamed directory (not just the exact key), `UpdateChunkLocations` grows `logical_size` to cover chunks committed past a truncate, and the seven `now_ms`-carrying variants apply deterministically — see `docs/src/internals/invariants.md` § A.8. CR2-7-6 (RAFT-6): `open_at`'s BP-2 corrupt-snapshot discard (blank SM) is checked at manager startup against the log store's `last_purged` — a blank SM with an already-purged log refuses to start as a voter (`log_store::blank_sm_startup_allowed`, called from `main.rs`) since the discarded snapshot was the only surviving copy of the pre-purge history |
| `dtfs-raft/src/lib.rs::ClientRequest` | Command enum; new mutation = new variant + arm |
| `dtfs-raft/src/network.rs::full_snapshot` | Snapshot-stream RPC; unreachable-vs-storage error mapping (Bug CL), bounded in-flight sends (CNF-4) |
| `dtfs-raft/src/log_store.rs` | RocksDB-backed Raft log/vote/last-purged store. CR2-7-5 (RAFT-5): `append`/`save_vote`/`persist_last_purged` use a synced `WriteOptions` (`set_sync(true)`) instead of `put_cf` + `flush_cf` (which only flushed the memtable, no fsync, and created one L0 SST per commit); `truncate_after` is one `delete_range_cf` instead of a delete-key-by-key loop (atomic, no torn-log hole on a crash mid-loop); `purge` commits its range delete and the `last_purged` meta write as one synced `WriteBatch` so both are durable together or neither is |
| `dtfs-manager/src/scheduler.rs` | Migration/re-replication loop, SOLE owner of replica repair; leader-gated, bounded, Raft-remove-before-byte-delete. CR-3-1..3-6 (MGR-1/2/4/5/6/7/8/9): `plan_flush_cache`/`plan_chunk_demotions` (tiering.rs) resolve survivor liveness via `live_replica_tier`; `register_replica` (`MigrationOps`) returns `Result` so a failed Completing registration retries instead of advancing to Done; `register_replica_location` collapsed to one SM lock via the pure `plan_replica_registration`, registering every carrying chunk_index in every referencing file; `resolve_disk_id_for_tier` returns `Option<u32>`, filtering Failed/Removed disks; `check_under_replicated`/`schedule_replication` dedup via `task_blocks_requeue`, source/target picks via the pure `pick_live_source`/`pick_backfill_target` (the latter requiring `placement_cfg.has_room`); `check_anchor_lag` now honours the REV-3-4 task budget and updates its dedup set inline; the heartbeat integrity-fault forwards (metadata.rs) surface `accepted: false` on either write's failure — see `docs/src/internals/invariants.md` § A.8. CR2-7-3 (MGR-13): `orphan_scrub_tick` re-checks `is_leader()` + `orphan_sweep_allowed` at the top of every per-node iteration AND immediately before every delete batch (not just once before the loop), auditing `orphan.sweep_aborted_not_leader` and stopping the sweep on either failing. CR2-7-2 (MGR-10): see § A.6 item 8. |
| `dtfs-manager/src/tiering.rs` | AV cache-tick planner + legacy temperature migration. CR2-7-1 (MGR-3): `build_av_cache_snapshot` collects the SM-derived per-chunk inputs under `state_data.read()`, drops that lock, then takes `read_windows.write()` ONCE for the whole batch via the pure `window_counts_for_chunk` — pre-fix it held the SM read lock across a per-chunk × per-DC `read_windows.write()` await, and a fair `tokio::sync::RwLock` queued every gRPC/REST reader behind one snapshot. |
| `dtfs-manager/src/monitor.rs` | Heartbeat health detection only (Bug CH); MUST NOT schedule repair; leader-gated (Bug CM) |
| `dtfs-manager/src/metadata.rs` | ManagerService gRPC over `raft_write`; follower writes forward with HMAC + a `forwardable()` variant allow-list (MGR-11); `write_via_leader` checks `ForwardWriteResponse.success` (MGR-12); LIS-56 auth incl. Heartbeat/ReportChunkStatus. CR-6-2 (CMN-2): `clamp_ack_floor_to_rf` clamps `write_safety.min_replicas_before_ack` down to the effective mandatory RF of the tier chunks actually landed on, warning once per volume + auditing `write_safety.floor_clamped`, so a floor above a tier's own RF (e.g. default 2 above cold's default RF 1) no longer rejects every commit to that tier forever. CR-6-4 (FUSE-6): `get_file_locations` fills `GetFileLocationsResponse.volume_id`; `allocate_chunks` resolves placement via `resolve_placement_volume_id` — the file's OWN registered volume when it exists, never a stale/wrong request value. |
| `dtfs-manager/src/api.rs` | REST endpoints; LIS-18 auth; effective-config merge; decommission refuses while replicas remain |
| `dtfs-manager/src/rest_auth.rs` | LIS-18 mutation-auth check + axum middleware |
| `dtfs-manager/src/membership.rs` | CF-2 online raft membership validation |
| `dtfs-manager/src/join.rs` | Two-phase data-node join (HMAC handshake); node_id derived server-side |
| `dtfs-manager/src/webui.rs` | Embedded SPA; LIS-35 mutating POSTs carry the shared secret. UIP (2026-09): covers every `api.rs` mutation the CLI has — volume create/edit (mandatory, anchor, spill, cache, write-safety, migrate-policy), scrub-all, raft membership (`/ui/raft`), maintenance, unquarantine, decommission — all through `mutateFetch`; a webui test forbids any inline `method: 'POST'` |
| `dtfs-data/src/server.rs::put_chunk` | Streaming put path; LIS-19/55/83 auth gates all data-plane RPCs |
| `dtfs-data/src/replication.rs::replicate_to` | Push-replication engine; timeout sized from chunk bytes |
| `dtfs-common/src/replication.rs` | Shared manager/data-node replication-deadline helpers |
| `dtfs-common/src/crypto.rs` | Join HMAC + data-rpc/mgr-rpc stamp/verify |
| `dtfs-data/src/store.rs::commit_chunk_from_tmp` | Atomic commit (§ A.6) |
| `dtfs-common/src/memory.rs` | CF-5 role-agnostic memory budget core |
| `dtfs-data/src/memory.rs` | Data-node rung; `[cache]` is parsed but inert (REV-4-5) |
| `dtfs-fuse/src/filesystem.rs` | FUSE↔cluster bridge; await-guard + write-batching rules in `.claude/rules/fuse.md`. CR-6-5 (FUSE-5): bounded, periodically-swept `metadata_cache`. CR-6-6 (FUSE-8/9/10): per-(fh,chunk)-transition `record_cache_hit` notify (semaphore-bounded), single-flighted foreground chunk-miss fetches (`foreground_inflight`), `on_chunk_read` called on every transition (hit or miss) — full detail in `.claude/rules/fuse.md`. |
| `dtfs-fuse/src/readahead.rs` | `ReadAheadEngine`; prefetch bound + Shed-rung, `.claude/rules/fuse.md` |
| `dtfs-fuse/src/fuse_adapter.rs` | fuse3 trait impl; every op error routes through `cluster_err_to_errno` |
| `dtfs-common/build.rs` + `src/version.rs` | `VERSION`/`BUILD_DATE` stamps (§ A.3.5) |
| `dtfs-cli/src/main.rs` | REST client over `api.rs` (clap). UIP (2026-09): feature-complete against the webui — `volumes {create,show,migrate-policy,set-cache,set-mandatory,set-write-safety,set-anchor,set-spill,set-schedules}`, `files {list,inspect,flush-cache}`, `migrations create`, `unquarantine`, `decommission`, `config {set,effective}`, `integrity faults`, `stats`, `scrub`, `raft {members,add-learner,promote,remove}`. Mutations via `post_with_status` print the server error body and exit non-zero. Full list: `docs/src/api-reference/cli-reference.md` |
| `dtfs-cli/src/updater.rs` | `dtfs-cli update` — download, verify, atomic install, restart |
| `scripts/dtfs-release.sh` | Release builder; stamps version, assembles tarball, prints `gh release create` |
| `dtfs-common/src/tls.rs` | CF-6 `TlsContext`; all gRPC routes through it; enable only cluster-wide |
| `plans/active/` | Optional short plans for multi-session work (`plans/README.md`) |
| `tests/release/`, `tests/proxmox/` | Cluster-only harnesses, dormant without a lab |

### .claude/rules/integrity.md (pre-2026-09-13, full narrative)

---
paths:
  - "crates/dtfs-data/**"
  - "crates/dtfs-manager/src/scheduler.rs"
  - "crates/dtfs-raft/**"
---

# Data-integrity audit checklist (AGENTS.md § A.6)

Run this whenever you touch a write/migrate/delete/scrub path; mark each
"Verified by `<test>`" in the commit. Full narrative context (why each
item exists, the bugs that motivated it) is inline below — this is the
long form of AGENTS.md A.3.3/A.6, loaded only for the crates it governs.

1. BLAKE3 via `IncrementalHasher`; final hash in `ChunkMeta.checksum`.
2. On-disk `.dat` written to `tmp/<uuid>.dat.tmp`, atomic-renamed to
   `chunks/<aa>/<bb>/<id>.dat` only after `sync_all()`.
3. `.meta` JSON written with `sync_all()` before the put's WAL entry is
   cleared. **CR-4-2** — the write itself is tmp+rename (`write_meta_
   atomic`: tmp file in the disk's `tmp/` dir, `sync_all`, rename over
   the final `.meta` path, fsync the directory), never a direct
   `File::create` at the final path, so a crash mid-write can never leave
   a torn (partial/0-byte) `.meta` where readers look for it.
   `recover_from_wal`'s `PutChunk` arm treats a present-but-unparseable
   `.meta` the same as a missing one (CO-1-4b) and rolls the half-
   committed chunk back.
4. Counter updates (`bytes_stored`, `chunk_count`) happen after meta is
   durable, so a crash mid-put leaves a reconcilable tmp+wal residue.
5. `commit_chunk_from_tmp` removes the tmp on any error path. **CR-4-3**
   — a failure AFTER the `.dat` rename (typically the `.meta` write) is
   cleaned up by `cleanup_after_dat_committed`, which removes the `.dat`,
   any `.meta`, and the WAL entry before returning the error, so a
   committed `.dat` never survives without its `.meta` companion.
   **CR-4-4** — `commit_chunk_from_tmp_inner` and
   `delete_chunk_idempotent_on_disk` each hold a 256-way striped
   per-chunk `tokio::sync::Mutex<()>` (indexed by the id's first byte)
   for their whole operation, so a concurrent put and delete of the SAME
   chunk id can never interleave into a torn state; the WAL filename is
   op-keyed (`<id>.put.wal.json` / `<id>.delete.wal.json`, plus the
   legacy `<id>.wal.json` still replayed) so they never contend over one
   WAL file either.
6. Migration marks a task Done only after `VerifyChunk` on the target
   succeeds; the scheduler never removes the source (only tier-demote /
   unlink delete a chunk). **CO-1-3** — every de-location-with-byte-delete
   path (tiering demote dispatch, scheduler cache-flush dispatch) issues
   the Raft `RemoveChunkLocation` (with `enforce_anchor`) FIRST and the
   data-plane `DeleteChunkReplica` byte delete AFTER, via the shared
   `scheduler::delocate_replica` (over the `DelocateOps` seam). On a
   Raft-remove failure NO byte delete is issued (the replica stays live in
   the SM + on disk; retry next tick). A byte-delete RPC that fails after
   a committed remove is NEVER rolled back — the leftover on-disk bytes
   are now a chunk de-located on that node, which the **CO-3-2** reaper
   reclaims (item 8). This makes de-location atomic at the metadata layer:
   no phantom replica (bytes gone but still advertised live) and no
   stranded anchor can result from a crash/RPC failure in the window.
7. Scrub BLAKE3s the disk file vs `chunk.meta.checksum`; mismatch →
   `chunks_corrupt` + `ScrubCorruption` alert; auto-repair via
   `replicate_chunk`. **CR-4-1** — before recording a `meta_read` /
   `meta_parse` / missing-`.meta` fault as corrupt, the scrub re-hashes
   the already-stat'd `.dat` and, on a match against the id encoded in
   the filename, rewrites `.meta` from the verified `.dat`
   (`try_repair_meta_from_dat` + `write_meta_atomic`) and counts the
   chunk healthy (`ScrubResult.chunks_meta_repaired`) instead of
   quarantining it — only a digest mismatch or an unreadable `.dat`
   still quarantines. `quarantine_chunk_on_disk` itself now RENAMES the
   `.dat` to `.dtfs/quarantine/<id>.dat.bad` (overwriting an earlier
   quarantine of the same id) instead of `remove_file`ing it, so a
   genuinely corrupt chunk's bytes are preserved for forensics rather
   than destroyed.
8. Unlink removes manager-side `FileMetadata` first, then GC deletes
   chunks async; a crash between leaves orphans (reclaimed by scrub),
   never dangling pointers. **Bug CB** — the orphan reaper measures its
   grace against each chunk's durable on-disk `.dat` mtime
   (`scheduler::orphan_reap_due`, fed by `ListChunks.chunk_mtimes_ms`),
   NOT a manager-local first-observed clock, so reclamation survives
   manager restarts / leadership changes (pre-CB the in-memory clock
   reset every restart and deleted-file disk was never freed under
   rolling-restart churn). An unknown mtime (pre-CB data node) defers
   the reap rather than dropping a possibly in-flight chunk. **CO-3-2**
   generalised the reaper to also reclaim a chunk present on a node's disk
   for which the SM records NO location entry naming that node
   ("de-located"; `scheduler::chunk_delocated_on_node`), after the same
   grace — this is what mops the byte-delete-failed leftovers produced by
   the CO-1-3 Raft-remove-first ordering (item 6). **CR2-7-2 (MGR-10)** —
   when the data node's `ListChunks` reply carries `chunk_disk_ids`, the
   de-located check and the reap delete are keyed by `(node_id, disk_id)`
   instead of node-wide, so a leftover on ONE disk of a node is reaped
   correctly even when another disk of the SAME node still holds a live
   replica of the same chunk (the cold-anchor + warm-cache co-location);
   an older data node reporting no disk ids falls back to the prior
   node-wide behaviour exactly.
9. Rename is a single Raft command; open handles keep reading the same
   chunks (path updated, chunk_id binding unchanged).
10. Reads use replica-fallback (P0-2): try the next replica on error, fail
    only after all candidates fail.

## Data-plane RPC auth (LIS-19/55/83, AGENTS.md § A.3.3 #7)

When `cluster.shared_secret` is non-empty, `PutChunk` / `DeleteChunk` /
`ReplicateChunk` / `GetChunk` / `ListChunks` / `VerifyChunk` / **`Record-
CacheHit`** (CR-5-5/DATA-11) require gRPC metadata `x-dtfs-rpc-ts` +
`x-dtfs-rpc-mac` (HMAC-SHA256 over
`dtfs-data-rpc\nv1\n{method}\n{ts}\n{binding}\n`; DeleteChunk/GetChunk/
VerifyChunk/RecordCacheHit bind `chunk_id`; ReplicateChunk binds
`chunk_id|target_address`; PutChunk/ListChunks empty binding). Missing or
invalid → `PermissionDenied`. Empty secret = legacy skip; data-node
startup refuses empty secret unless `allow_insecure_empty_secret`.
Callers that stamp: FUSE/client put+get+record_cache_hit, data→data
replication put, manager scheduler ReplicateChunk/VerifyChunk/
ListChunks, manager unlink fan-out + scheduler flush. `RecordCacheHit`
was the last mutating data-plane RPC left ungated pre-CR-5-5 — an
unauthenticated caller could inflate a chunk's read counter and its
tiering promotion score.

## Manager-plane RPC auth (LIS-56, AGENTS.md § A.3.3 #8)

When `cluster.shared_secret` is non-empty, mutating `ManagerService`
gRPC RPCs (`CreateFile`/`DeleteFile`/leases/…, and since **CR-5-1**
also `Heartbeat` and `ReportChunkStatus`) require the same header shape
over the `dtfs-mgr-rpc` plane (`metadata::require_mgr_rpc_auth`); reads
stay open. Pre-CR-5-1, Heartbeat/ReportChunkStatus were the only two
mutating RPCs not gated — any caller who knew a node id could poison
node health or drop replica entries without the secret. Data nodes
stamp both from `[cluster].shared_secret` (`dtfs-data/src/main.rs::
stamp_mgr_rpc_request`); deploy manager + data together.

A follower's `ForwardClientWrite` (used by the heartbeat/health-monitor
forward path when a write lands on a non-leader) is authenticated by
`forward_secret_ok` (CO-2-2, constant-time) but that only checks WHO is
forwarding. **CR-5-2 (MGR-11)** added a second gate on WHAT is
forwarded: `forwardable(&ClientRequest)` allow-lists exactly the
variants a follower legitimately proposes today (`UpdateNodeUsage`,
`UpdateNodeDiskUsage`, `UpdateNodeStatus`, `AppendAudit`,
`QuarantineNode`, `RemoveCorruptChunkReplicas`, `ReportScrubResult`,
`ReportChunkReads`); anything else is denied + audited
(`auth.denied`) before it ever reaches `raft.client_write`. **CR-5-3
(MGR-12)** — the caller side (`write_via_leader`) now inspects
`ForwardWriteResponse.success`/`.error` via `forward_result` instead of
discarding the response body, so a forward rejected by the leader (bad
secret, a disallowed MGR-11 variant, or a failed Raft apply) is no
longer silently treated as a successful write.

## Mandatory vs cache replicas (Bug AV invariant, AGENTS.md § A.3.4)

Two independent, never-mixed replica classes:
- `mandatory_replicas.<tier>` — protected; only
  `scheduler::check_under_replicated` maintains them, reading
  `effective_mandatory_replicas()`, counting only serviceable replicas
  (`live_replica_tier`, Bug CH) so node/disk loss surfaces as a deficit.
- `cache_replicas.<tier>.{min,max}` — demand-driven, evictable; only the
  tiering engine creates them, only on the promotion threshold. The
  under-replication scheduler MUST NOT backfill cache replicas.
- **CR-3-1 (MGR-2)** — a "surviving" mandatory/anchor replica must be
  LIVE to count: `plan_chunk_demotions`'s `mandatory_still_present` and
  `plan_flush_cache`'s `mandatory_survives` resolve every placement's
  liveness through `live_replica_tier` before treating it as the
  survivor that authorises dropping another replica. A replica on a
  Down/Failed node or Failed/Removed disk is not servable and must
  never be the reason a demote/flush is allowed to proceed.
  **CR-3-2** — independent of anchor policy, `apply_remove_chunk_location`
  also refuses to remove the last LIVE replica of a `(path,
  chunk_index)`; a dead row can always be removed, but the removal of a
  live row is refused when no other live row of that chunk survives.
- `scheduler::plan_flush_cache` refuses any flush that would strand the
  mandatory copy → `cache.flush_refused` audit + `AnchorViolated` alert.
- Every promote/demote checks the anchor against the destination state:
  legacy path via `would_violate_anchor`; AV idle-demote via
  `tiering::demote_preserves_anchor` (checks another placement still
  satisfies `effective_anchor`, records `TickPlan.anchor_protected`); the
  apply path (`RemoveChunkLocation` with `enforce_anchor`) re-checks as
  defence-in-depth. The mandatory-tier survival check ALONE is not
  enough: a chunk's only anchor-DC replica can be a cache copy while the
  mandatory copy lives elsewhere.
- Replica presence is keyed by `(node_id, disk_id)`, never `node_id`
  alone (Bug BH) — a mandatory and a cache replica of the same chunk may
  co-locate on one node on different disks/tiers.
- Removal is disk-targeted (CO-1-1): every replica-drop path carries the
  specific `disk_id` so it removes exactly the `(node_id, disk_id)` copy,
  never a co-located anchor on another disk of the same node. Whole-chunk
  reclaimers (unlink fan-out, orphan reaper) intentionally stay node-wide.

When a new code path creates OR removes replicas, name the class
explicitly and consult one of these checks.

### .claude/rules/fuse.md (pre-2026-09-13, full narrative)

---
paths:
  - "crates/dtfs-fuse/**"
---

# dtfs-fuse memory ladder + await-guard rules (AGENTS.md § A.3.2, § A.8)

## Never hold a DashMap guard across an `.await`

`filesystem.rs::read()` clones the per-call data (path/chunk_size/
volume_id/locations) out of the `open_files` entry and drops the DashMap
guard BEFORE the per-chunk network loop (same idiom as `write()`/
`flush()`). Holding an `open_files` guard across an `.await` serializes
concurrent opens of the same file. The same rule covers
`known_locations`: the post-fetch migration-hint lookup
(`known_locations.get(&node_addr)`) uses `.map(|r| r.clone())` so the
`Ref` guard drops at the end of that statement, before the migration
trigger runs, never across an `.await`/spawn.

`migration` (`MigrationTrigger`) is fire-and-forget: `read()` calls
`spawn_on_remote_read` (`tokio::spawn`, never `.await`s `on_remote_read`
inline) because the RPC it issues (`DtfsClient::allocate_chunks`) sets no
per-call deadline and retries `Unavailable` for up to the 15s manager
walk deadline — awaiting it inline after the chunk bytes are already in
hand could stall a completed read for that long.

## Write batching (WI-01)

The write buffer (`writebuf.rs`) holds back complete chunks until
`write.flush_batch_chunks` (default 4, clamp `1..=32`) are available
before `flush()` fires, so `flush_segments_to_cluster` amortizes one
`AllocateChunks` + `CommitChunkLocations` round trip over a batch instead
of paying it per chunk. `flush_all` (fsync/close) always drains held-back
chunks regardless of batch state. The effective batch is additionally
byte-clamped (`writebuf::effective_batch_chunks`,
`FLUSH_BATCH_MAX_BYTES` = 64 MB) so batching never holds more than 64 MB
of un-committed data per open file — the memory ladder below sheds
readahead, not write buffers.

R1 (FUSE-1 memory half) — `writebuf::DIRTY_BYTES_CAP` (2x
`FLUSH_BATCH_MAX_BYTES`) governs total dirty bytes per open file: the
buffer is force-drained via `WriteBuffer::flush_complete()` whenever a
sparse-hole zero-fill or an incoming write would meet or exceed the cap,
so a write far past EOF (e.g. a torrent client's first piece landing at
offset 100 MB) can no longer materialise the whole hole into heap before
committing anything.

## Foreground-read prefetch join (WI-05 / B2)

`read()`'s foreground miss tries `readahead.join_pending()` against ONLY
the FIRST candidate (prefetch targets `read_candidates`' head), bounded
by the fixed `readahead::PREFETCH_JOIN_TIMEOUT` (2s, deliberately NOT
`client.chunk_read_timeout_secs`), and refuses to wait on a prefetch that
has not yet acquired its `prefetch_slots` permit (still queued), instead
returning `None` immediately. A chunk genuinely on the wire is JOINED
rather than re-fetched with a second `GetChunk` over the same link;
`None` falls through to the unchanged candidate loop, preserving the
P0-2 fallover budget (`tests/release/cases/69-read-hung-node-fallover.sh`,
`ELAPSED <= TMO*3`).

## Prefetch fan-out bound (WI-05)

`readahead.rs::ReadAheadEngine` bounds prefetch fan-out with a
`prefetch_slots` semaphore sized from `readahead.prefetch_max_inflight`
(default 2) — the prefetch WINDOW stays `prefetch_chunks` deep (default
8), but fetches acquire their slot chained in ascending order
(`spawn_prefetch`'s `prev_acquired` oneshot) so at most
`prefetch_max_inflight` `GetChunk` streams are ever on the wire at once,
nearest chunk first. Before this an uncapped fan-out shared one pooled
HTTP/2 channel per node N ways and starved the nearest chunk instead of
adding bounded heap. Do NOT re-add the old `cache_bytes() >=
max_cache_bytes` dormancy gate — it only ever saw stored bytes, never
in-flight ones, and could go permanently true under the default sizing,
silently killing all further prefetch.

## Memory ladder (CF-5, shared core in `dtfs-common/src/memory.rs`)

FUSE halves the readahead `cache_mb` at Warn, drops unconsumed prefetch
AND stops spawning new prefetch at Shed, via
`ReadAheadEngine.prefetch_enabled: Arc<AtomicBool>` — `on_chunk_read`
early-returns while it is false, so `drop_unconsumed()` is not undone by
the very next read; `ReadAheadCacheController::set_prefetch_enabled()` is
pinned false at Shed and true at Normal/Warn by
`main.rs::run_readahead_pressure_ladder`. `pending` holds
`PendingPrefetch { handle, done, started }`, where `started` is set true
the instant a prefetch task acquires its `prefetch_slots` permit,
letting `join_pending()` above distinguish a fetch genuinely in flight
from one still queued. systemd `MemoryMax` remains the hard backstop.

## Per-chunk cache-hit notify, single-flight misses, continuous prefetch (CR-6-6)

`read()` tracks `OpenFile.last_read_chunk: Option<u32>` per fh; the pure
`should_notify(last, current)` decides whether the CURRENT chunk index is
a transition from the last one this fh touched. Three things gate on the
same transition signal (computed once, right after building read
candidates, before any cache lookup):

- **FUSE-8** — `record_cache_hit` fires at most once per (fh, chunk)
  transition, not once per 128 KB kernel read (a 4 MB chunk read in
  128 KB pieces used to fire ~32 of these RPCs). In-flight notify tasks
  are additionally bounded by a `Semaphore(CACHE_HIT_NOTIFY_MAX_INFLIGHT
  = 8)`; a permit-less notify is simply skipped (a missed metrics ping
  is acceptable, unlike a stalled read).
- **FUSE-10** — `readahead.on_chunk_read` fires on every transition, hit
  or miss (previously only on a miss), so the prefetch window slides
  continuously instead of stalling for one synchronous foreground fetch
  every `prefetch_chunks+1` chunks.
- **FUSE-9** — a foreground cache MISS is single-flighted by chunk_id
  (content-addressed, so any candidate node is interchangeable) via
  `claim_foreground_fetch`/`complete_foreground_fetch` over a
  `foreground_inflight: DashMap<String, watch::Receiver<Option<Bytes>>>`.
  The first miss becomes the `Owner` (does the real `GetChunk` via the
  extracted `fetch_chunk_from_candidates`, then publishes the result and
  clears the entry); a concurrent miss on the SAME chunk_id becomes a
  `Waiter` that awaits the Owner's `watch::Receiver` instead of issuing
  its own fetch. A `Waiter` whose Owner's fetch failed (published `None`)
  falls back to fetching it itself rather than failing the read. This
  keeps N concurrent kernel reads into one cold chunk down to exactly one
  `GetChunk` over the link instead of N.

New WI-05/B2/H2 test coverage is unaffected: the join-pending path (a
chunk already ON THE WIRE as a *prefetch*) is untouched and still tried
before the miss branch; `foreground_inflight` only covers the foreground
fetch loop itself.

## Disk-cache viewer-read hint + eviction hysteresis (CR2-8-3/CR2-8-4)

`local_cache.rs`'s viewer/scanner atime hint can no longer be driven by
one kernel read's length (FUSE-11): FUSE reads arrive in ~128 KB pieces,
always under `ATIME_UPDATE_MIN_BYTES` (1 MiB), so `read()` now tracks
`OpenFile.bytes_read_on_fh` and combines it with
`ReadAheadEngine::is_active()` via the pure `filesystem::is_viewer_read`
helper, passed into the new `LocalChunkCache::get_with_hint(id,
is_viewer)` (the old `get(id, bytes_hint)` delegates to it). FUSE-12:
`evict_to_size` targets `EVICT_TARGET_PCT` (90%) of `max_bytes` instead
of exactly `max_bytes`, so a cache at steady state pays a full O(N) stat
sweep roughly once per 10% of the cap's worth of new data instead of on
almost every single `put()`; the dead `lru: BTreeMap` index (inserted,
never read) is gone.

## FUSE error mapping (CO-4-16)

`fuse_adapter.rs` metadata ops map cluster errors via
`cluster_err_to_errno` — ENOENT ONLY for a genuine gRPC `NotFound`, EIO
for transient/unknown (manager outage, election, timeout). A blip mapped
to ENOENT makes Plex drop the library entry; route every new
error-returning op through the helper, never a bare `Err(_) => ENOENT`.
