# Migration

DTFS migrates chunks between storage tiers, between nodes during rebalancing, and to new replicas during re-replication. All migrations use a crash-safe state machine that ensures data is never lost, even if a node fails mid-transfer.

## Migration State Machine

Every migration task progresses through five states in strict order:

```text
Pending -> Copying -> Verifying -> Completing -> Done
```

Each state transition is persisted to the manager's Raft log before work begins, so that after a crash the migration resumes from the last committed state rather than restarting from scratch.

### States

**Pending**

The migration has been scheduled but not yet started. The manager has selected a source replica and a target node. The task sits in the migration table until the next scheduler pass picks it up (see throughput below).

**Copying**

The source data node streams the chunk data to the target data node. The transfer uses a dedicated gRPC streaming RPC with flow control. If the copy is interrupted (network failure, node restart), it resumes from the last acknowledged byte offset. Partial data on the target is retained across restarts.

**Verifying**

The target data node computes a BLAKE3 checksum over the received data and reports it to the manager. The manager compares this checksum against the authoritative checksum stored in the metadata. If the checksums match, the migration advances. If they do not match, the target data is discarded and the migration returns to Copying for a retry (up to `max_retries`).

**Completing**

The manager updates the chunk's placement metadata to include the new replica location. For tier migrations (not replication increases), the manager also marks the old replica for removal. The metadata update is committed through Raft.

**Done**

The migration is complete. If the migration was a tier change, the old replica is removed from the source node only after the metadata update has been committed. The source node confirms deletion back to the manager.

### Source Protection

The source replica is never removed until the target replica has been verified and the metadata update committed. This invariant holds even if the manager crashes and a new leader is elected. The new leader reads the migration state from the Raft log and either continues or rolls back as appropriate.

If the migration fails after all retries, the source replica remains in place and the manager raises an alert. No data is lost.

## Crash Recovery

On manager startup (or leader election), the new leader scans the migration table for tasks in non-terminal states:

| State at crash | Recovery action |
|---------------|-----------------|
| Pending | Re-enqueue the task |
| Copying | Resume the copy from the last acknowledged offset |
| Verifying | Re-request verification from the target |
| Completing | Re-apply the metadata update (idempotent) |
| Done | No action needed |

Data nodes also persist their local migration state. If a data node restarts while serving as a copy target, it reports its partial progress to the manager, which decides whether to resume or restart the copy.

## Scheduler Throughput

The migration scheduler runs one pass every `SCHEDULER_TICK` (30s) on the
Raft leader. Within a pass it drives every tracked task through the state
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

Both constants live in `crates/dtfs-manager/src/scheduler.rs`. Priority of
work scheduled into the table is:

1. **Re-replication** (under-replicated chunks) -- highest priority.
2. **Rebalancing** (capacity skew correction) -- medium priority.
3. **Tier migration** (temperature-driven) -- lowest priority.

## Bandwidth Limits

Migration traffic can be throttled to preserve bandwidth for production workloads:

```toml
[migration]
# Maximum bandwidth per migration stream
bandwidth_limit_mbps = 200

# Maximum aggregate migration bandwidth per data node
node_bandwidth_limit_mbps = 500
```

The per-stream limit prevents a single migration from monopolizing a node's network capacity. The per-node aggregate limit provides a ceiling regardless of how many concurrent migrations involve that node.

### Dynamic Throttling

DTFS supports dynamic bandwidth adjustment based on time of day:

```toml
[[migration.bandwidth_schedule]]
start = "08:00"
end   = "18:00"
bandwidth_limit_mbps = 100

[[migration.bandwidth_schedule]]
start = "18:00"
end   = "08:00"
bandwidth_limit_mbps = 500
```

During business hours, migration bandwidth is reduced to minimize impact on user-facing traffic. Outside business hours, the full bandwidth allowance is used to catch up on pending migrations.

## Manual Migration Control

### Pausing and Resuming

Operators can pause and resume migrations globally or per node:

```bash
# Pause all migrations cluster-wide
dtfs-cli migration pause

# Resume all migrations
dtfs-cli migration resume

# Pause migrations on a specific node
dtfs-cli migration pause --node data-01

# List active migrations
dtfs-cli migration list --state copying,verifying
```

Pausing a migration in the Copying state stops the data transfer at the next checkpoint. The partial data is retained on the target. Resuming continues from the last checkpoint.

### Draining a Node

When decommissioning a node, all chunks must be migrated off before the node can be removed:

```bash
dtfs-cli node drain data-01
```

This creates migration tasks for every chunk on the node. Drain migrations use the re-replication priority level to complete as quickly as possible. The node enters a `draining` state and rejects new chunk placements.

## Monitoring

Migration metrics are exported by the manager:

- `dtfs_migration_tasks_total{state="pending|copying|verifying|completing|done|failed"}` -- current task count by state.
- `dtfs_migration_bytes_transferred_total` -- total bytes transferred across all migrations.
- `dtfs_migration_duration_seconds` -- histogram of end-to-end migration duration.
- `dtfs_migration_retries_total` -- total verification failures that triggered retries.
- `dtfs_migration_bandwidth_bytes_per_second` -- current aggregate migration throughput.
