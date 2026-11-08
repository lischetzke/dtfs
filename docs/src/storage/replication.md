# Replication

DTFS replicates chunks across multiple data nodes to tolerate hardware failures without data loss. Replication factors are configured independently per tier, allowing operators to balance durability against storage cost.

## Two replication classes: mandatory vs cache

Each volume has two independent replication classes:

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
for the configuration surface, [`Tiering`](./tiering.md#cache-tier-semantics)
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
>   (that is the class separation).
>
> The legacy `replication.<tier>` count on its own creates nothing. Example
> footgun: `replication = {cold = 1, warm = 1}` +
> `mandatory_replicas = {cold = 1}` + `cache_replicas.warm = {min = 1,
> promote_reads = 0}` yields a cluster where every file settles at **one**
> cold copy — the warm "replica" never materializes, and placement reports
> `undergoal: 0` because only mandatory replicas are counted. If you want a
> durable second copy, put it in `mandatory_replicas`.

### Anchor invariant

Volumes carry an `anchor.datacenter` setting (optionally pinned to a specific `anchor.required_tier`). At least one **live** replica of every chunk must sit in the anchor datacenter. It is enforced at every place a replica can disappear:

- Placement forces one replica into the anchor datacenter at allocation time (the tier constraint is relaxed for that slot).
- The anchor-lag pass creates the anchor copy for chunks that landed without one.
- Every promote, demote, flush and migration checks the anchor against its destination (`would_violate_anchor`, `demote_preserves_anchor`). An additive copy never subtracts its source; a move does.
- The state machine re-checks on `RemoveChunkLocation` (`enforce_anchor`) and refuses to remove the last live replica regardless of policy.
- A surviving replica that justifies dropping another must be live. A copy on a Down node does not count.
- Datacenter names compare case-insensitively everywhere.

A refused flush raises an `AnchorViolated` alert; see `dtfs_anchor_protected_total` in [Monitoring](../operations/monitoring.md).

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

The scheduler is the only component that creates or repairs mandatory replicas. It counts only live replicas, and removing a surplus replica goes through the same Raft-first path as every other removal (see [Re-Replication](#re-replication)).

## Write Safety

DTFS enforces configurable safety guarantees on every write to prevent data loss during node failures.

### min_replicas_before_ack

The `min_replicas_before_ack` parameter controls how many replicas must confirm a write before the client receives a success response. The default is 2:

```toml
[defaults.write_safety]
min_replicas_before_ack = 2
```

The write flow (see [Data Flow](../architecture/data-flow.md#write)):

1. The FUSE client asks the manager for placement and streams the chunk to **every** target in parallel; there is no primary-to-replica forwarding on the write path. The manager fills the floor **across maintained tiers**: when the volume's write tier cannot supply `min_replicas_before_ack` distinct nodes (for example one cold node plus two warm nodes), it adds targets on other tiers whose mandatory replica count is above 0, on distinct healthy nodes, honouring the spill-forbid lists. Cache-only tiers (mandatory 0) never fill the floor.
2. The client waits for every target to answer and continues when at least `ceil(N/2)` acknowledged the same chunk id.
3. `CommitChunkLocations` is rejected unless every chunk has at least `min_replicas_before_ack` distinct `(node_id, disk_id)` replicas on maintained tiers among the acknowledged targets. The floor is clamped only to what the cluster can physically hold: the number of healthy data nodes with a usable disk on a maintained tier (a single-node cluster commits at 1, and a cluster whose other nodes are quarantined or in maintenance clamps to what remains; the clamp is logged once and audited as `write_safety.floor_clamped`).
4. Targets that did not acknowledge are filled in later by the repair scheduler.

Setting `min_replicas_before_ack` equal to the replication factor gives the strongest guarantee but needs every target up; 1 gives the lowest latency but a single node failure right after the ack can lose the chunk.

> **The ack floor is only as durable as its maintainers.** `N` acknowledged
> replicas at write time stay `N` replicas only if the volume's policy keeps
> them (see "Every tier needs a maintainer" above). If the extra ack comes
> from a tier with no `mandatory_replicas` entry and no firing cache
> promotion, that copy is later dropped and the file settles below `N`.
> Before raising `min_replicas_before_ack`, confirm the topology has enough
> disks on maintained tiers to durably hold `N` copies.

### fsync Behavior

Data nodes always `sync_all()` a chunk before the atomic rename that commits
it, and sync the metadata before clearing the write-ahead log entry — an
acknowledged write has reached stable storage. There is no configuration
option to disable this.

### write_timeout_secs

`write_timeout_secs` is part of the volume's `write_safety` record and is stored and editable, but the current write path does not read it. Per-chunk timeouts come from the client (`client.chunk_read_timeout_secs` for reads) and from gRPC deadlines.

## Failure Domain Spreading

Placement spreads replicas with a `Rack`-level failure domain: one replica per host, preferring distinct racks, with a relaxed second pass when the topology is too small. The anchor datacenter is then forced in. The details, including the capacity filters, are in [Data Distribution](../architecture/data-distribution.md#node-selection). The `[placement]` block of `manager.toml` only holds the free-space headroom (`min_free_bytes`, `min_free_pct`) and `allow_capacity_spill`.

## Re-Replication

When a node goes Down or a disk fails, the leader's scheduler repairs chunks whose **live** mandatory replica count is below policy:

1. Find chunks with a replica on the failed node or disk. Replicas on Down nodes and Failed disks do not count as live.
2. Pick a live source and a healthy target on a disk that has room, honouring the failure-domain and anchor rules.
3. Copy with `ReplicateChunk`. The target hashes as it writes and the scheduler marks the task done only after `VerifyChunk` on the target.
4. Register only the **new** replica row, using the disk id the target reported. A lease-less location update is a union, so it cannot erase other replicas, add a new chunk index or grow the file size; if any index was skipped it returns `Error` (the valid rows are still merged) and the scheduler retries. `VerifyChunk` in step 3 names the disk the target reported, so it checks that disk's copy only.
5. Removing a surplus or misplaced replica later goes through `delocate_replica`: Raft `RemoveChunkLocation` first, byte delete after. If the Raft write fails or the state machine refuses it, no byte is deleted.

Repair is bounded per tick (new tasks and live tasks are capped) and never competes with itself: the health monitor only reports state, and the scheduler is the sole owner of repair. Bandwidth and concurrency limits are in [Migration](./migration.md).

## Monitoring

Under-replication, anchor and migration counters are exposed on the manager's `/metrics` endpoint and in `GET /api/v1/stats`; see [Monitoring](../operations/monitoring.md) for the current metric names.
