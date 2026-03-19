# Cluster Management

This chapter covers adding, removing, and managing Manager and Data nodes in a DTFS cluster.

## Architecture Overview

A DTFS cluster consists of two node types:

- **Manager nodes** -- Maintain cluster metadata, run Raft consensus, handle file namespace operations, and coordinate placement decisions.
- **Data nodes** -- Store chunks on XFS-formatted disks, serve read/write requests, and report status via heartbeats.

Manager nodes form a Raft group. An odd number (3, 5, or 7) is recommended for fault tolerance. Data nodes are stateless from a consensus perspective; they register with the manager and maintain their state locally on disk.

## Adding a Manager Node

### Prerequisites

1. The new host has `dtfs-manager` installed and a valid configuration at `/etc/dtfs/manager.toml`.
2. The `shared_secret` in the configuration matches the existing cluster.
3. Network connectivity exists between the new node and all existing managers on the Raft port (default `9400`) and gRPC port (default `9401`).

### Procedure

1. Configure the new manager. Set `cluster.bootstrap = false` and point `cluster.manager_addr` to any existing manager:

```toml
[cluster]
name = "my-cluster"
shared_secret = "your-shared-secret"
bootstrap = false
```

2. Start the manager process:

```bash
systemctl start dtfs-manager
```

3. The new manager executes the cluster join protocol (see [Cluster Join Protocol](../security/cluster-join.md)):
   - Sends an HMAC-authenticated `RequestJoin` RPC to an existing manager.
   - Receives the cluster CA certificate and current manager list.
   - Completes the join with a challenge-response exchange.

4. The Raft leader adds the new node as a learner, then promotes it to a voter once it has caught up with the log.

5. Verify the node joined:

```bash
dtfs-cli --manager <any-manager>:9402 nodes
```

### Scaling Considerations

- Always maintain an **odd number** of managers (3, 5, or 7). Even numbers do not improve fault tolerance.
- Adding a 4th manager to a 3-node group is acceptable temporarily during a rolling replacement, but the cluster should settle on an odd count.
- Manager nodes are lightweight. They store only metadata (in RocksDB) and Raft logs.

## Removing a Manager Node

### Graceful Removal

1. Ensure the node to be removed is **not** the current Raft leader. If it is, trigger a leader transfer first by restarting the leader or waiting for an election.

2. Remove the node from the Raft group via the REST API:

```bash
curl -X POST http://<leader>:9402/api/v1/nodes/<node-id>/maintenance \
  -H 'Content-Type: application/json' \
  -d '{"enable": true}'
```

3. Once the node is in maintenance mode and the remaining managers confirm quorum without it, stop the process:

```bash
systemctl stop dtfs-manager
```

4. The Raft leader will eventually remove the non-responsive voter from the configuration after the `down_after_missed` heartbeat threshold is exceeded.

### Emergency Removal

If a manager node has failed and cannot be recovered:

1. Verify that the remaining managers still form a quorum (e.g., 2 of 3 are healthy).
2. The Raft leader will detect the failed node after `suspect_after_missed` (default: 3) missed heartbeats and mark it suspect, then `down_after_missed` (default: 6) to mark it down.
3. Once marked down, the leader removes the node from the voter set.
4. Add a replacement manager promptly to restore the desired fault tolerance level.

## Adding a Data Node

### Prerequisites

1. `dtfs-data` is installed with a valid configuration at `/etc/dtfs/data.toml`.
2. One or more XFS-formatted disks are mounted and listed in the `[[disks]]` configuration sections.
3. The `shared_secret` matches the cluster.

### Procedure

1. Configure the data node:

```toml
[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "your-shared-secret"

[[disks]]
path = "/mnt/ssd-a"
tier = "hot"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"
```

2. Start the data node:

```bash
systemctl start dtfs-data
```

3. The data node:
   - Performs the cluster join protocol against the manager.
   - Sends a `RegisterNode` RPC with its hostname, location, and disk inventory.
   - Begins periodic heartbeats (default every 5 seconds).

4. The manager records the new node in the Raft state machine, making it available for chunk placement.

5. Verify:

```bash
dtfs-cli nodes
```

The new node should appear with `Healthy` status within a few heartbeat intervals.

### Location Configuration

Each data node declares its position in the topology hierarchy:

```toml
[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "data-01"
```

The placement engine uses this information for location-aware replica distribution. See [Data Distribution](../architecture/data-distribution.md) for details.

## Removing a Data Node

### Graceful Decommission

1. Place the node in maintenance mode to stop new chunk placements and begin draining:

```bash
dtfs-cli maintenance <node-id> --enable
```

2. The manager initiates replication of all chunks on the node to other data nodes with matching tier capacity. Monitor progress:

```bash
dtfs-cli nodes --id <node-id>
```

3. Once draining is complete (all chunks have been replicated elsewhere), stop the process:

```bash
systemctl stop dtfs-data
```

4. The manager marks the node as removed after it stops heartbeating.

See [Maintenance Mode](./maintenance-mode.md) for more details on the draining process.

### Forced Removal

If a data node has failed unrecoverably:

1. The manager detects the failure after `down_after_missed` missed heartbeats.
2. The scheduler automatically re-replicates under-replicated chunks from remaining replicas to healthy nodes.
3. Data availability depends on the replication factor. With the default hot replication factor of 3, the cluster can tolerate one data node failure with no data loss.

## Monitoring Cluster Membership

### REST API

```bash
# Full cluster status
curl http://<manager>:9402/api/v1/cluster/status

# List all nodes with health
curl http://<manager>:9402/api/v1/nodes

# Single node details including disks
curl http://<manager>:9402/api/v1/nodes/<node-id>
```

### CLI

```bash
dtfs-cli status          # Cluster summary
dtfs-cli nodes           # All nodes in table format
dtfs-cli nodes --id X    # Detailed view of one node
dtfs-cli health          # Quick manager health check
```

### Node Health States

| State | Meaning |
|-------|---------|
| `Healthy` | Node is heartbeating normally. |
| `Suspect` | Node has missed `suspect_after_missed` heartbeats (default: 3). |
| `Down` | Node has missed `down_after_missed` heartbeats (default: 6). Re-replication begins. |
| `Maintenance` | Operator placed the node in maintenance mode. No new placements; draining in progress. |
| `Draining` | Active chunk evacuation underway. |
| `Failed` | Permanent failure recorded. Node will not recover. |

## Rolling Upgrades

To upgrade the cluster with zero downtime:

1. Upgrade manager nodes one at a time, starting with non-leaders. After each upgrade, wait for the node to rejoin the Raft group and catch up.
2. Upgrade data nodes one at a time. Place each in maintenance mode, upgrade, restart, then disable maintenance mode.
3. Upgrade FUSE clients last. They reconnect to managers automatically.

## Best Practices

- Run 3 or 5 manager nodes. Three is sufficient for most deployments; five is recommended for large clusters or multi-datacenter setups.
- Spread manager nodes across failure domains (racks or datacenters) to survive localized outages.
- Do not co-locate manager and data node roles on the same host in production. Managers should have low, predictable latency for Raft consensus.
- Always verify quorum health before removing any manager node.
- When adding data nodes, allow time for the placement engine to begin distributing new writes before decommissioning old nodes.
