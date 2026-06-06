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

> **Every tier needs a maintainer, or it holds zero copies.** A replica only
> exists if one of the two classes maintains it on that tier:
>
> - `mandatory_replicas.<tier> > 0` — the under-replication scheduler creates
>   and repairs these continuously, and
>   `min_replicas_before_ack` durability rests on them; or
> - `cache_replicas.<tier>.min/max > 0` **with a firing promotion trigger** —
>   cache replicas are created only by the tiering engine when a chunk's
>   reads reach `promote_reads` within `promote_window_secs`. With
>   `promote_reads = 0` the trigger never fires and a `min > 0` floor is
>   inert: the scheduler is *forbidden* from backfilling cache replicas
>   (that is the Bug AV class separation).
>
> The legacy `replication.<tier>` count on its own creates nothing. Example
> footgun: `replication = {cold = 1, warm = 1}` +
> `mandatory_replicas = {cold = 1}` + `cache_replicas.warm = {min = 1,
> promote_reads = 0}` yields a cluster where every file settles at **one**
> cold copy — the warm "replica" never materializes, and placement reports
> `undergoal: 0` because only mandatory replicas are counted. If you want a
> durable second copy, put it in `mandatory_replicas`.

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
| Tape | 1 | The tape library itself provides durability; 2 only with two independent libraries |

These defaults are configured at the cluster level (manager.toml) and can be
overridden per volume through the API/CLI (`dtfs-cli volumes set-mandatory`,
`set-cache` — see [Volume Configuration](../configuration/volume-config.md)):

```toml
# Cluster-wide defaults (manager.toml)
[defaults.replication]
hot  = 3
warm = 2
cold = 1
tape = 1
```

When a chunk's temperature changes and its tier shifts (see [Tiering](./tiering.md)), the target replication factor changes as well. For example, a chunk moving from hot (3 replicas) to warm (2 replicas) will have one replica decommissioned after the migration completes. A chunk moving from cold (1 replica) to warm (2 replicas) will have a new replica created.

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

> **The ack floor is only as durable as its maintainers.** `N` acknowledged
> replicas at write time stay `N` replicas only if the volume's policy keeps
> them (see "Every tier needs a maintainer" above). If the extra ack comes
> from a tier with no `mandatory_replicas` entry and no firing cache
> promotion, that copy is later dropped and the file settles below `N` —
> the configured guarantee silently degrades. Before raising
> `min_replicas_before_ack`, confirm the topology has enough disks on
> maintained tiers to durably hold `N` copies.

### fsync Behavior

Data nodes always `sync_all()` a chunk before the atomic rename that commits
it, and sync the metadata before clearing the write-ahead log entry — an
acknowledged write has reached stable storage. There is no configuration
option to disable this.

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
