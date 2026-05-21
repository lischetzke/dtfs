# Replication

DTFS replicates chunks across multiple data nodes to tolerate hardware failures without data loss. Replication factors are configured independently per tier, allowing operators to balance durability against storage cost.

## Two replication classes: mandatory vs cache

Bug AV (2026-05) introduced two independent replication classes per
volume. Conceptually:

| Class | Source of truth | Who creates them | Who removes them |
|-------|-----------------|------------------|-------------------|
| **Mandatory** | `mandatory_replicas.{hot,warm,cold,tape}` | The under-replication scheduler — re-creates lost mandatory replicas continuously | Nobody, except operator-driven volume reconfiguration or `unlink` |
| **Cache** | `cache_replicas.<tier>.{min,max}` | The tiering engine, on demand-driven read promotion | Operator (`POST /api/v1/files/flush_cache`), or tiering engine when the chunk goes cold again |

The two classes coexist on the same chunk record — a `/movies/x.mkv`
chunk on `data-primary01` (mandatory cold) and `data-warm01` (cache
warm) sees both replicas listed in `chunk.node_addresses`; the
distinction is virtual, derived from the volume's policy plus each
replica's storage tier.

See [Volume Configuration → Replication classes](../configuration/volume-config.md#replication-classes)
for the configuration surface, [`Tiering`](./tiering.md#cache-tier-semantics-bug-av)
for the promotion + cooldown rules that drive cache replica
creation, and `cache.flush_replica` / `cache.flush_refused` audit
events for the operator-visible lifecycle.

### Anchor invariant

Volumes additionally carry an `anchor.datacenter` setting (optionally
pinned to a specific `anchor.required_tier`). The scheduler refuses
any migration that would leave the volume with no mandatory replica
in the anchor DC, and the cache-flush planner refuses any flush that
would strand the anchor (an `AnchorViolated` alert fires when a
flush attempt produces refusals — see the AV-1-11 hooks for details).

## Per-Tier Replication Factors

Each temperature tier has its own default replication factor:

| Tier | Default Replicas | Rationale |
|------|-----------------|-----------|
| Hot | 3 | High availability and read throughput for frequently accessed data |
| Warm | 2 | Moderate durability with reduced storage overhead |
| Cold | 1 | Cost-efficient storage for infrequently accessed data |

These defaults are configured at the cluster level and can be overridden per volume:

```toml
# Cluster-wide defaults
[defaults.replication]
hot  = 3
warm = 2
cold = 1

# Per-volume override
[volume.critical-data.replication]
hot  = 5
warm = 3
cold = 2
```

When a chunk's temperature changes and its tier shifts (see [Tiering](./tiering.md)), the target replication factor changes as well. For example, a chunk moving from hot (3 replicas) to warm (2 replicas) will have one replica decommissioned after the migration completes. A chunk moving from cold (1 replica) to warm (2 replicas) will have a new replica created.

### Frozen Tier Behavior

The frozen tier inherits the cold tier's replication factor by default. Operators who want to reduce replicas further for frozen data can set it explicitly:

```toml
[defaults.replication]
frozen = 1
```

## Write Safety

DTFS enforces configurable safety guarantees on every write to prevent data loss during node failures.

### min_replicas_before_ack

The `min_replicas_before_ack` parameter controls how many replicas must confirm a write before the client receives a success response. The default is 2:

```toml
[defaults.write_safety]
min_replicas_before_ack = 2
```

The write flow proceeds as follows:

1. The FUSE client sends a write request to the primary data node for the chunk.
2. The primary writes the data to its local disk and forwards the write to the secondary replicas in parallel.
3. Once `min_replicas_before_ack` nodes (including the primary) have confirmed the write to durable storage, the primary responds to the client with success.
4. Remaining replicas continue to receive the write asynchronously.

Setting `min_replicas_before_ack` equal to the replication factor provides the strongest durability guarantee but increases write latency. Setting it to 1 gives the lowest latency but risks data loss if the single acknowledging node fails before replication completes.

### fsync Behavior

By default, data nodes call `fsync` on every acknowledged write to guarantee that the data has reached stable storage:

```toml
[defaults.write_safety]
fsync = true
```

Disabling `fsync` improves write throughput but introduces a risk window: data confirmed to the client may be lost if the node loses power before the kernel flushes its page cache. This is generally only acceptable for scratch or temporary data.

### write_timeout

The `write_timeout_secs` parameter sets the maximum time the primary will wait for replica acknowledgments before returning an error to the client:

```toml
[defaults.write_safety]
write_timeout_secs = 30
```

If the timeout expires before `min_replicas_before_ack` replicas confirm, the write fails and the client receives an I/O error. The client may retry the write. Chunks that end up under-replicated due to timeouts are flagged for re-replication by the manager.

## Failure Domain Spreading

DTFS uses the topology hierarchy to spread replicas across independent failure domains, minimizing the chance that a single hardware failure (a disk, a host, a rack, or a datacenter) destroys all copies of a chunk.

### Topology Hierarchy

Every data node registers its location in a four-level hierarchy:

```text
Region > Datacenter > Rack > Host
```

This is configured in the data node's configuration file:

```toml
[location]
region     = "us-east"
datacenter = "dc1"
rack       = "rack-12"
```

### Placement Rules

The placement engine applies the following rules when selecting nodes for a chunk's replicas, evaluated in order of priority:

1. **No two replicas on the same host.** This is always enforced and cannot be relaxed.
2. **Spread across racks.** If the replication factor is <= the number of available racks, each replica is placed on a different rack.
3. **Spread across datacenters.** For geo-distributed clusters with `spread_across_dcs = true`, replicas are distributed across datacenters when possible.

```toml
[placement]
failure_domain = "rack"         # "host", "rack", or "datacenter"
spread_across_dcs = false
```

The `failure_domain` setting controls the minimum separation. With `failure_domain = "rack"` (the default), the placement engine guarantees that no two replicas of the same chunk share a rack. If there are not enough distinct racks to satisfy the replication factor, placement falls back to host-level separation and the manager raises a warning.

### Geo-Distributed Clusters

In clusters that span multiple datacenters, operators can require that at least one replica exists in each datacenter:

```toml
[placement]
spread_across_dcs = true
min_dcs = 2
```

With `min_dcs = 2`, the placement engine ensures that no single datacenter failure can cause data loss. This increases write latency due to cross-datacenter replication but provides the strongest durability guarantee for critical data.

## Re-Replication

When a data node fails or a disk is removed, the manager detects under-replicated chunks through heartbeat timeouts and schedules re-replication:

1. The manager identifies all chunks that had a replica on the failed node.
2. For each under-replicated chunk, the placement engine selects a new target node that satisfies the failure-domain constraints.
3. A healthy replica streams the chunk data to the new target.
4. Once the new replica is verified (BLAKE3 checksum match), the chunk is marked as fully replicated.

Re-replication respects bandwidth limits to avoid saturating the network. See [Migration](./migration.md) for bandwidth configuration.

## Monitoring

The manager exposes replication metrics:

- `dtfs_replication_factor{tier="hot|warm|cold|frozen"}` -- configured replication factor per tier.
- `dtfs_chunks_under_replicated` -- number of chunks below their target replication factor.
- `dtfs_chunks_over_replicated` -- number of chunks above their target (pending decommission).
- `dtfs_replication_write_ack_duration_seconds` -- histogram of time from write initiation to `min_replicas_before_ack` confirmations.
- `dtfs_replication_rereplicate_total` -- total re-replication operations triggered.
