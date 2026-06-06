# Cluster Management

This chapter covers adding, removing, and managing Manager and Data nodes in a DTFS cluster.

## Architecture Overview

A DTFS cluster consists of two node types:

- **Manager nodes** -- Maintain cluster metadata, run Raft consensus, handle file namespace operations, and coordinate placement decisions.
- **Data nodes** -- Store chunks on XFS-formatted disks, serve read/write requests, and report status via heartbeats.

Manager nodes form a Raft group. An odd number (3, 5, or 7) is recommended for fault tolerance. Data nodes are stateless from a consensus perspective; they register with the manager and maintain their state locally on disk.

## Adding a Manager Node

> **Current limitation: the Raft voter set is fixed at bootstrap.** The voter
> membership is established once, when the bootstrap manager calls
> `raft.initialize()` with the `[[cluster.initial_voters]]` list. There is no
> online add-learner / change-membership operation yet — neither in the API
> nor the CLI. The HMAC join protocol
> ([Cluster Join Protocol](../security/cluster-join.md)) registers **data
> nodes and clients** in the node registry; it does not change Raft
> membership.

What this means in practice:

- A manager listed in `initial_voters` at bootstrap time is a voter. If its
  local Raft state is later lost or wiped, it re-syncs from the leader via
  snapshot on restart — wiping a *member's* state is recoverable.
- A manager **not** in the bootstrap voter set (or listed with a hostname
  that does not exactly match its `[node].hostname` — see the
  identity contract on the [manager configuration page](../configuration/manager.md))
  can never join the running cluster. It starts as an inert, empty learner:
  term 0, no leader, empty namespace. Its API serves an empty cluster view —
  the typical symptom is a manager whose WebUI shows no data nodes, no
  clients, and zero files while the rest of the cluster is healthy.
- **Never** set `bootstrap = true` on such a node to "fix" it: it will
  initialize a second, divergent single-voter cluster (metadata split-brain)
  that keeps its own log and never federates.

### Procedure (requires planned re-initialization)

To grow the voter set today:

1. Add the new manager to `[[cluster.initial_voters]]` in **every** manager's
   `manager.toml`, with `hostname` exactly equal to the new node's
   `[node].hostname` and a peer-routable `addr`.
2. Take a metadata backup (see [Backup & Recovery](./backup-recovery.md)).
3. Re-initialize the Raft group with the new voter set following the
   full manager group recovery procedure in
   [Backup & Recovery](./backup-recovery.md) (stop managers, clear Raft
   state, `bootstrap = true` on exactly one node, restore snapshot).
4. Verify all voters appear: `curl http://<mgr>:9402/api/v1/raft/state` —
   check `voters` and `current_leader` on every manager.

### Recovering a broken member (wipe and re-sync)

For a manager that IS in the voter set but has bad local Raft state (stuck
candidate, poisoned vote, corrupt log):

1. Stop the service: `systemctl stop dtfs-manager`.
2. Move aside `<data_dir>/raft-log/` and `<data_dir>/raft-sm/`
   (default `data_dir` is `/var/lib/dtfs`).
3. Start the service. The manager rejoins as a follower and installs a
   snapshot from the leader.

The Bug AC poisoned-state detector performs this wipe automatically on
startup when it detects a stuck-candidate state; the manual procedure covers
cases the detector does not catch. Note again: this only works for a node
already in the membership — wiping a never-joined manager just leaves it
empty.

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

## Reclaiming warm cache space (Bug AV flush)

When the operator needs to free space on warm-tier disks ahead of
the natural idle-decay window — for example, before a large
import campaign, or to evict stale media that's no longer being
watched — the cache-flush primitive drops opportunistic cache
replicas while preserving the mandatory anchor.

### When to use it

| Situation | Action |
|-----------|--------|
| "I want to free 200 GB on toro's warm disk before tonight's import" | `flush_cache` on the relevant prefix |
| "I deleted a season from Plex and want the cache disks cleaned" | `flush_cache` on the prefix the season lived under |
| "I want to permanently archive — never warm again" | Not this. Change the volume's `cache_replicas.warm = { min: 0, max: 0 }` instead. The flush is a *temporary* eviction; the tiering engine will re-promote on the next 2 reads within 4 h. |

### Procedure

```bash
# Drop every warm cache replica under /movies/Season-1/, with the
# default 5-minute cooldown to prevent immediate re-promotion.
curl -X POST http://mgr-01:9402/api/v1/files/flush_cache \
     -H 'content-type: application/json' \
     -d '{"path_prefix":"/movies/Season-1/","cache_tiers":["warm"]}'
```

### WebUI alternative (Bug AV — AV-4-3 / AV-4-4)

The Volumes page in the WebUI now embeds the per-volume policy
inline and exposes the same flush operation without curl:

* **Details** button on each row expands a card showing the
  volume's mandatory / cache / anchor policy (AV-4-3).
* **Edit policy…** opens an inline modal that POSTs to
  `/api/v1/volumes/{id}/set-cache` (AV-4-4) — useful for raising
  `warm.max`, flipping `per_dc`, or tightening
  `cooldown_secs` without dropping to the CLI.
* **Flush cache…** opens the same `flush_cache` modal the Data
  Explorer's right-click menu uses; pre-fills the volume's root
  prefix.

These three actions cover the same operator surface as the curl
+ `dtfs-cli` paths and use the AV-4-2 list-endpoint projection
to display the current policy without an extra round-trip.

Response shape:

```json
{
  "status": "accepted",
  "files_matched": 12,
  "chunks_examined": 482,
  "replicas_dropped": 482,
  "mandatory_refusals": 0,
  "truncated": 0,
  ...
}
```

* `replicas_dropped` is the count of per-replica delete RPCs that
  succeeded.
* `mandatory_refusals` is the count of chunks where the planner
  *refused* to flush because doing so would have stranded the
  mandatory copy — **this should be 0** on a healthy volume. Any
  non-zero value means the planner protected against data loss
  AND fired an `AnchorViolated` alert; investigate the audit log
  (`cache.flush_refused` entries) to understand which chunks
  were affected.

### What to expect in the audit log

After a flush you should see, in order:

1. One `cache.flush` row (the API-level call entry).
2. N × `cache.flush_replica` rows (one per successful per-replica
   delete; N = `replicas_dropped`).
3. Optional `cache.flush_refused` rows (one per refusal; if you
   see these, also expect an `AnchorViolated` alert via the
   configured webhook sinks).
4. One `cache.flush_complete` row (the per-call summary).

```bash
# Pull the recent flush audit entries:
curl -s "http://mgr-01:9402/api/v1/audit?action=cache.&limit=20" \
  | jq .
```

### Cooldown — why immediate re-reads don't re-promote

After a flush, the engine sets each affected chunk's
`cache_cooldown_until_ms[chunk_index]` to `now + cooldown_secs`
(default 5 min). Within that window, even reads that would
normally meet the `promote_reads` threshold are refused, so a
playback that started immediately after a flush continues
serving from the cold (mandatory) replica.

The cooldown is **automatically cleared** when the engine itself
demotes an idle cache replica (the natural decay path). This
preserves the asymmetry:

| Eviction cause | Cooldown after eviction | First read re-promotes? |
|----------------|--------------------------|-------------------------|
| Operator flush | yes (5 min by default) | no, until cooldown expires |
| Engine idle-demote | no | yes, immediately on next read |

### Anchor-aware refusals

A flush refusal indicates the planner detected a chunk where
dropping the cache replicas would leave the mandatory tier with
0 copies. This **should never happen** on a healthy volume —
it means either (a) the chunk's mandatory placement is
genuinely missing (a data-node failure not yet recovered), or
(b) the volume's `mandatory_replicas` is misconfigured.

Investigate by:

```bash
# Find the audited refusals:
curl -s "http://mgr-01:9402/api/v1/audit?action=cache.flush_refused&limit=20" | jq .

# Cross-check the chunk's placements:
dtfs-cli files | grep <path>
# Or via REST:
curl -s http://mgr-01:9402/api/v1/files | jq '.files[] | select(.path == "<path>")'
```

For details on the planner's refusal logic, see
[Tiering → Anchor-aware demotion](../storage/tiering.md#anchor-aware-demotion).

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
