# Maintenance Mode

Maintenance mode allows operators to safely take nodes offline for planned work such as hardware replacement, OS upgrades, or disk maintenance without risking data loss.

## Overview

When a node enters maintenance mode:

1. **No new chunk placements** are directed to the node.
2. **Chunk draining** begins: the manager schedules replication of all chunks on the node to other healthy nodes.
3. **Existing reads** continue to be served until the node is actually stopped.
4. The node health status transitions to `Maintenance`, then to `Draining` while evacuation is in progress.

## Enabling Maintenance Mode

### Via CLI

```bash
dtfs-cli maintenance <node-id> --enable
```

### Via REST API

```bash
curl -X POST http://<manager>:9402/api/v1/nodes/<node-id>/maintenance \
  -H 'Content-Type: application/json' \
  -d '{"enable": true}'
```

### Via gRPC

The `AdminService.SetMaintenanceMode` RPC can be used programmatically:

```
SetMaintenanceMode(node_id: "<id>", enable: true)
```

## Draining Process

Once maintenance mode is enabled:

1. The manager marks the node as `Maintenance` in the Raft state machine.
2. The scheduler identifies all chunks stored exclusively on the maintenance node (chunks with no other replicas).
3. For each under-replicated chunk, the scheduler creates a replication task targeting a healthy node with matching tier capacity.
4. Replication tasks respect the configured `migration_bandwidth_limit_mbps` (default: 200 Mbps) to avoid saturating the network.
5. As chunks are replicated, the node status transitions to `Draining`.

### Monitoring Drain Progress

```bash
# Check node status and disk utilization
dtfs-cli nodes --id <node-id>

# Watch the cluster status for under-replicated chunks
dtfs-cli status
```

The node remains in `Draining` state until all its chunks have at least the minimum replica count satisfied on other nodes.

### Drain Duration

Drain time depends on:

- Total data stored on the node.
- Available network bandwidth (capped by `migration_bandwidth_limit_mbps`).
- Number of healthy target nodes with available capacity.
- Current cluster I/O load.

**Estimate:** At 200 Mbps bandwidth limit, draining 1 TB takes approximately 11 hours. Increase the bandwidth limit for faster drains during off-peak hours.

## Performing Maintenance

Once draining is complete:

1. Verify the node shows `Draining` with zero remaining exclusive chunks, or has transitioned to a fully drained state.
2. Stop the node process:

```bash
systemctl stop dtfs-data   # or dtfs-manager
```

3. Perform the maintenance work (hardware swap, OS update, etc.).
4. Restart the node:

```bash
systemctl start dtfs-data
```

5. Disable maintenance mode:

```bash
dtfs-cli maintenance <node-id> --disable
```

The node returns to `Healthy` status and resumes accepting new chunk placements.

## Manager Node Maintenance

For manager nodes, the procedure is similar but requires extra care around Raft quorum:

1. Ensure the cluster has enough managers to maintain quorum without the target node (e.g., 2 of 3 remaining).
2. If the target is the Raft leader, the leader will automatically transfer leadership when it shuts down gracefully.
3. Place the node in maintenance mode, stop it, perform work, restart, and disable maintenance mode.
4. Verify the node has rejoined the Raft group:

```bash
dtfs-cli status
```

**Warning:** Never put more than one manager in maintenance mode at a time in a 3-node manager group. This would break quorum and halt all metadata operations.

## Canceling Maintenance Mode

If you need to cancel a maintenance operation before draining completes:

```bash
dtfs-cli maintenance <node-id> --disable
```

The node immediately returns to `Healthy` status:

- New chunk placements resume.
- In-progress replication tasks complete, but no new drain tasks are scheduled.
- The extra replicas created during draining remain in place and may be removed later by the replication manager if the replication factor is exceeded.

## Planned Downtime Checklist

For scheduled cluster-wide maintenance windows:

1. **Verify cluster health.** Ensure all nodes are `Healthy` with no degraded disks or ongoing scrubs.

```bash
dtfs-cli status
dtfs-cli nodes
```

2. **Check replication status.** Confirm no chunks are under-replicated.

3. **Drain nodes one at a time.** Never drain multiple nodes simultaneously unless the replication factor can absorb it.

4. **Monitor FUSE clients.** During maintenance, FUSE clients may experience higher latency as they are redirected to more distant replicas. Partition behavior settings control what happens if managers become unreachable (see [Partition Behavior](../access/partition-behavior.md)).

5. **Stagger maintenance across failure domains.** Do not maintain all nodes in the same rack or datacenter simultaneously.

6. **Document the window.** Audit log entries are created for all maintenance mode transitions. Review them afterward:

```bash
dtfs-cli audit --action maintenance --limit 50
```

## Audit Trail

All maintenance mode operations are recorded in the audit log:

| Action | Description |
|--------|-------------|
| `maintenance.toggle` | Maintenance mode enabled or disabled on a node |
| `tier.promote` / `tier.demote` | Chunk migrations triggered by draining |

See [Audit Log](../security/audit-log.md) for details on querying and exporting audit records.
