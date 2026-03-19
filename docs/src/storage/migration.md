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

The migration has been scheduled but not yet started. The manager has selected a source replica and a target node. The task sits in a queue until a migration slot becomes available (see concurrency limits below).

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

## Concurrency Limits

The number of concurrent migrations is limited to avoid saturating network and disk I/O:

```toml
[migration]
# Maximum concurrent migrations cluster-wide
max_concurrent_global = 20

# Maximum concurrent migrations per data node (as source or target)
max_concurrent_per_node = 4
```

The manager's migration scheduler respects both limits. When a slot becomes available, the highest-priority pending task is started. Priority is determined by:

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
