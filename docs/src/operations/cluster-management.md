# Cluster Management

This chapter covers adding, removing, and managing Manager and Data nodes in a DTFS cluster.

## Architecture Overview

A DTFS cluster consists of two node types:

- **Manager nodes** -- Maintain cluster metadata, run Raft consensus, handle file namespace operations, and coordinate placement decisions.
- **Data nodes** -- Store chunks on local disks (XFS recommended), serve read/write requests, and report status via heartbeats.
- **FUSE clients** -- Mount the filesystem; they register under their `[location].host` and appear in the node table with role `Client`.

Manager nodes form a Raft group. An odd number (3, 5, or 7) is recommended for fault tolerance. Data nodes are not part of consensus; they register with a manager (shared secret in the request) and keep their chunks and identity files locally on disk. An automation-oriented version of the procedures below, with the checks after each step, is in [Deployment automation](../deployment/overview.md).

## Adding a Manager Node

Raft membership lives in the raft log. `[[cluster.initial_voters]]` is
consulted **once**, at bootstrap (`bootstrap = true` →
`raft.initialize()`); after that, growing or shrinking the live cluster
goes through the membership API / CLI below. The `RegisterNode` join
([Cluster Join Protocol](../security/cluster-join.md)) registers **data
nodes and clients** in the node registry; it does not change Raft
membership.

Two ground rules:

- A manager that was never joined starts as an inert, empty learner:
  term 0, no leader, empty namespace. Its API serves an empty cluster
  view — the typical symptom is a manager whose WebUI shows no data
  nodes, no clients, and zero files while the rest of the cluster is
  healthy. Join it; do not trust its reads until it has caught up.
- **Never** set `bootstrap = true` on such a node to "fix" it: it will
  initialize a second, divergent single-voter cluster (metadata
  split-brain) that keeps its own log and never federates.

### Procedure (online, no re-initialization)

1. Install + configure the new manager (`bootstrap = false`; its
   `[node].hostname` unique cluster-wide — on a composite host it must
   differ from the data daemon's hostname, since `node_id =
   hash(hostname)`). Add it to `[[cluster.initial_voters]]` in every
   manager's `manager.toml` anyway: the list no longer joins anyone,
   but the BR-1 self-register loop and follower→leader request
   forwarding read manager addresses from it.
2. Start the service and confirm its API answers:
   `curl http://<new-mgr>:9402/api/v1/health`.
3. Join it as a **learner** (full state replication, no vote). Send to
   the leader; a follower answers `409` with a `leader` hint:

   ```bash
   dtfs-cli --manager <leader>:9402 raft add-learner <hostname> <ip:9401> \
       --secret <cluster shared_secret>     # or DTFS_SHARED_SECRET env
   # REST equivalent:
   # POST /api/v1/raft/add-learner {"hostname":..., "addr":..., "shared_secret":...}
   ```

   `addr` is the peer-routable manager gRPC `host:port` (same form as
   an `initial_voters` entry — never a wildcard).
4. Watch it catch up: `dtfs-cli --manager <new-mgr>:9402 raft members`
   (or `GET /api/v1/raft/state`) until `current_leader` is set and
   `applied_log_lag` is `0`, and `/api/v1/cluster/status` matches the
   leader's view. Large state arrives via snapshot install; on a slow
   WAN link this can take a while.
5. Optionally promote it to **voter** once caught up:

   ```bash
   dtfs-cli --manager <leader>:9402 raft promote <hostname> --secret ...
   ```

   A learner serves reads and replicates everything but does not vote
   — often the right end state for a remote-DC manager (no extra WAN
   hop in elections, quorum math unchanged).
6. Verify: `raft members` shows the node with the expected role on
   every manager; the audit log carries a `raft.add_learner` (and
   `raft.promote`) row.

All three membership endpoints (`add-learner`, `promote`, `remove`) are
leader-only and require the cluster `shared_secret` in the body.

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

Membership never changes by itself: a dead voter stays in the voter set
(and keeps counting against quorum math) until an operator removes it.

1. Ensure the node to remove is **not** the current Raft leader (if it
   is, restart it so another voter wins the election first).
2. Remove it from the membership (learner or voter; the endpoint
   refuses to remove the last voter):

```bash
dtfs-cli --manager <leader>:9402 raft remove <hostname> --secret <shared_secret>
# REST: POST /api/v1/raft/remove {"hostname":..., "shared_secret":...}
```

3. Verify with `raft members` on the remaining managers, then stop the
   process: `systemctl stop dtfs-manager`.
4. Remove its `[[cluster.initial_voters]]` entry from the remaining
   managers' tomls (takes effect at their next restart) so the
   self-register/forwarding pools stop dialing it.

For a manager that failed permanently and cannot be recovered, the same
`raft remove` works as long as the remaining voters hold quorum (e.g.
2 of 3) — run it promptly so a second failure cannot cost quorum, and
add a replacement via add-learner/promote.

## Adding a Data Node

### Prerequisites

1. `dtfs-data` is installed with a valid configuration at `/etc/dtfs/data.toml`.
2. One or more disks are mounted (by UUID), owned by `dtfs`, and listed in the `[[disks]]` configuration sections. The list is append-only, see [Disk Management](./disk-management.md#disk-ids-and-disks-order).
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
   - Replays its write-ahead log, binds its gRPC port, and sends a `RegisterNode` RPC (hostname, location, disk inventory, authenticated with the shared secret) to the first reachable manager in `manager_addr` / `manager_addrs`.
   - Begins periodic heartbeats (every 5 seconds). After 3 failed heartbeats it walks the manager list, and it re-registers when a manager answers NOT_FOUND.
   - Rescans its chunks in the background after startup, so the port is open before the inventory walk finishes.

4. The manager records the new node in the Raft state machine, making it available for chunk placement.

5. Verify:

```bash
dtfs-cli nodes
```

The new node should appear with `Healthy` status within a few heartbeat intervals and its `disk_count` should equal the number of `[[disks]]` entries.

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

There is no one-shot drain. Removal is: make sure every chunk on the node has
other live replicas, stop the node, then deregister it.

1. Put the node in maintenance mode so it is not marked `Down` and receives no new
   placements (this does not move data):

   ```bash
   dtfs-cli --secret "$DTFS_SHARED_SECRET" maintenance <node-id> --enable
   ```

2. Ensure its data is replicated elsewhere (volume replication policy plus
   `dtfs-cli migrations create`, see [Migration](../storage/migration.md)), and never
   remove the only node holding a chunk's anchor replica.
3. Stop and disable the service: `systemctl disable --now dtfs-data`.
4. Deregister: `dtfs-cli decommission <node-id> --operator-id <who> --reason "<why>"`.
   The server answers 409 while any file still lists a replica on the node.

For a node that is permanently dead and cannot be drained, use
`dtfs-cli nodes remove-dead <node-id> [--dry-run] [--accept-data-loss]`
instead (node must be Down/Failed past the Down threshold). It drops rows that
have another live replica, leaves anchor-pending rows for repair, and removes
rows with no live replica only with `--accept-data-loss` (damaged files move to
`/.dtfs-lost/`, which Plex should not scan; restore from backup or delete); see
[Decommission and Replace](../deployment/decommission-replace.md#a-node-that-is-permanently-dead).

Details and the disk and manager variants: [Decommission and Replace](../deployment/decommission-replace.md).
See [Maintenance Mode](./maintenance-mode.md) for exactly what the flag does.

### Forced Removal

If a data node has failed unrecoverably:

1. The manager marks the node `Suspect` and then `Down` after `suspect_after_missed` and `down_after_missed` missed heartbeats (15 s and 30 s by default).
2. The scheduler re-creates the missing mandatory replicas from remaining live replicas on healthy nodes. Replicas on a `Down` node do not count as live.
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
| `Maintenance` | Operator flag (sticky). No new placements, never auto-marked `Down`; no data is moved. |
| `Quarantined` | An integrity fault quarantined the node (sticky; `dtfs-cli unquarantine` after the cause is understood). |
| `Degraded` / `Failed` | Derived from disk health reported by the node. |
| `Draining` | Defined but not set by any operator API or scheduler path in this revision. |

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

The WebUI exposes the same operations without curl:

* Data > Volumes: **Details** expands the volume's mandatory, cache
  and anchor policy; **Edit policy** opens a modal that posts only the
  changed fields to the matching `/api/v1/volumes/{id}/set-*` endpoint
  and asks for confirmation before anchor or write-safety changes.
* Data > Explorer: the row menu (right-click, the "..." button, or
  Enter on a focused row) offers **Flush cache** and **Migrate** for a
  folder or file prefix.

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

The release pipeline does this for every `release`/`deploy` commit via
`deploy/ansible/update-cluster.yml` ([CI Pipeline](./ci-pipeline.md)). The order
(managers: learners, follower voters, leader last; then data nodes; then clients),
the per-node steps, health gates, rollback and the config flips that must wait
until every manager is updated are specified in
[Rolling Update and Rollback](../deployment/rolling-update.md).

## Best Practices

- Run 3 or 5 manager nodes. Three is sufficient for most deployments; five is recommended for large clusters or multi-datacenter setups.
- Spread manager nodes across failure domains (racks or datacenters) to survive localized outages.
- Co-locating roles on one host is supported (the reference deployment does it) but each daemon needs its own hostname and its own memory budget; see [Filesystem Layout and Units](../deployment/filesystem-layout.md#memory-limits-on-co-hosted-boxes). Keep the Raft voters on hosts with low, predictable latency to each other.
- Always verify quorum health before removing any manager node.
- When adding data nodes, allow time for the placement engine to begin distributing new writes before decommissioning old nodes.
