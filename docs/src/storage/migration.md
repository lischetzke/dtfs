# Migration

The scheduler on the manager leader moves and copies chunks: repair after a failure, cache promotion, operator flush and migrate requests, and drain. All of them use one task state machine so a chunk is never lost mid-transfer. The scheduler is the sole owner of replica repair.

## Task state machine

```text
Pending -> Copying -> Verifying -> Completing -> Done
                                          \-> Failed (after MAX_MIGRATION_ATTEMPTS)
```

The task table is persisted through Raft, so a new leader resumes from the committed state.

- **Pending**: a source replica (live) and a target disk were chosen.
- **Copying**: the manager asks the target to pull the chunk with `ReplicateChunk`. The target streams it to a temp file while hashing and commits it with the normal atomic put. A failed or interrupted copy restarts from the beginning of the chunk on the next attempt.
- **Verifying**: `VerifyChunk` on the target re-hashes the stored bytes against the chunk id. A mismatch sends the task back to Copying.
- **Completing**: the new location is registered through Raft. Only the new row is sent, on the disk id the target reported; lease-less location updates are a union and cannot erase other replicas. For a move, the old replica is then removed.
- **Done**: terminal. For a move, the source is removed only now, with the Raft removal first and the byte delete second.

### Source Protection

The source replica is never removed until the target replica has been verified and the metadata update committed. This invariant holds even if the manager crashes and a new leader is elected. The new leader reads the migration state from the Raft log and either continues or rolls back as appropriate.

If the migration fails after all retries, the source replica remains in place and the manager raises an alert. No data is lost.

## Crash recovery

After a leader change the new leader loads the task table and continues each non-terminal task from its recorded state; every stage is idempotent (a repeated copy of an existing verified chunk succeeds, a repeated registration is a union, a repeated removal of an absent row is a no-op). A task whose source or target is gone is retried with another candidate. If a Raft removal is refused, for example because it would leave no live replica or strand the anchor, the byte delete is not performed.

## Scheduler Throughput

The scheduler runs one pass every `SCHEDULER_TICK` (30 s) on the
Raft leader. A pass first checks that it is still leader and stops early if not. Within a pass it drives every tracked task through the state
machine and then persists the whole migration table back to Raft once.

**Multi-stage per tick.** A single pass loops over the tasks in rounds until
a round makes no further progress, so a freshly scheduled chunk can climb
`Pending -> Copying -> Verifying -> Completing -> Done` within ONE 30s tick
rather than advancing a single state per tick. With a large backlog this
makes warm coverage rise smoothly across ticks instead of staying flat for a
whole pass and then batch-jumping. A task still attempts at most one copy and
one verify RPC per tick: a verify checksum mismatch flaps the task back to
`Copying` and re-copies on the NEXT tick, and a transient
copy/source failure burns one retry per tick (up to `MAX_MIGRATION_ATTEMPTS`
= 6 attempts, ~3 minutes, before the task is marked `Failed` and alerted).

**Bounded-parallel copies.** The `Copying`-stage `ReplicateChunk` RPCs in a
round are dispatched concurrently, capped at `MIGRATION_COPY_CONCURRENCY` (6)
in flight. The cap is deliberately small: each copy is a streaming gRPC that
competes with foreground reads/writes on the source data node, and the
primary archive's uplink (LELI, the asymmetric-bandwidth bottleneck) is
easily saturated by a handful of concurrent streams. The `Verifying` and
`Completing` stages stay serial (verify is cheap; `Completing` writes Raft).

The constants live in `crates/dtfs-manager/src/scheduler.rs`. Each target receives at most 2 concurrent copies, a pass is bounded by a time budget and a round limit, and the number of new tasks per tick and of live tasks is capped so repair of mandatory replicas and the anchor is never starved by cache promotion. A failed Raft write or a state-machine refusal is treated as a failure of that step (one shared response checker), never as success.

## Throughput limits

Throughput is bounded by concurrency, not by a bandwidth shaper: at most 6 copies in flight cluster-wide, 2 per target, and one migration RPC timeout of 60 s. There is no `[migration]` bandwidth setting and no time-of-day schedule. This matters most for the LELI uplink (40 Mbps); repair and anchor-lag copies toward LELI are the main consumers.

## Operator control

- Migrate: `dtfs-cli migrations create <path_prefix> --target-tier hot|warm|cold|tape [--target-dc DC]` moves every chunk under a path prefix toward a tier (and optionally a datacenter). It goes through the same verified path and the same anchor check.
- Flush cache: drop cache replicas of a path (sets a cooldown). Refused flushes (anchor) are audited as `cache.flush_refused`.
- Evacuate a node: put it in maintenance mode, let the scheduler move its replicas, then decommission; decommission refuses while replicas remain. See [Maintenance Mode](../operations/maintenance-mode.md), [Cluster Management](../operations/cluster-management.md) and [Disk Management](../operations/disk-management.md).
- There is no global pause/resume command for migrations.

## Monitoring

The manager `/metrics` endpoint exposes `dtfs_migration_failed_total` and `dtfs_tier_migrations_completed_total`. Task detail is in the REST API (`GET /api/v1/migrations`) and the WebUI; see [Monitoring](../operations/monitoring.md).
