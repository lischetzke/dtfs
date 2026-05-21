# Tiering

DTFS automatically classifies data into temperature tiers based on access patterns and migrates chunks to the appropriate storage media. The tiering engine runs on the manager leader node and periodically evaluates every chunk in the cluster.

## Cache tier semantics (Bug AV)

> **Status (2026-05):** Phase 1 (schema, flush, audit, alert,
> migration helpers) and Phase 2 (demand-driven promotion +
> idle-window demotion + cooldown clearance) are complete. The
> legacy temperature path below still runs alongside the new
> engine for backward compatibility with volumes that don't have
> `cache_replicas` configured. New AV-style volumes use the
> rules described here.

Cache replicas are **opportunistic** copies created by the tiering
engine in response to read demand. Unlike mandatory replicas they
are:

* Freely evictable (operator flush; tiering-engine demotion).
* Not counted toward write-safety thresholds (mandatory replicas
  only).
* Allowed to differ in count per DC (`cache_replicas.per_dc = true`
  is the default; set `false` to scope the cache pool globally).

See [Volume Configuration → Replication classes](../configuration/volume-config.md#replication-classes)
for the full schema reference.

### Promotion math

For each `(chunk, DC, cache-tier)` triple, the engine evaluates on
every tick (default 5 min, configurable via
`tiering.evaluation_interval_secs`):

1. **Window count** — sum of reads from this DC in the last
   `cache_replicas.promote_window_secs` (default 4 h).
2. **Cooldown check** — if the chunk's
   `cache_cooldown_until_ms[idx]` is in the future, refuse this
   tier this tick.
3. **Threshold check** — refuse if window count <
   `cache_replicas.promote_reads` (default 2).
4. **Target count** — basic promotion path schedules
   `min(min, max) - current_in_dc` new replicas. Always 0 once
   `min` is reached (the redundancy-escalation pass that ramps
   toward `max` is `ENABLE_CACHE_REDUNDANCY_ESCALATION = false`
   in Phase 2; flipping it is a deferred follow-up gated on a
   real per-DC HA workload to validate against).
5. **Target selection** — pick a healthy non-draining node in the
   requesting DC that has a disk of the target tier and doesn't
   already hold this chunk. Tie-break by highest free capacity,
   then lowest `node_id` (deterministic audit output).

When `per_dc = false`, all DC reads contribute to one global
pool and the engine places the cache copy in the DC that
generated the most reads (best locality).

### Demotion math

For each existing cache-tier placement:

1. **Tier check** — must be a *cache* tier (i.e., the volume's
   `cache_replicas.<tier>.max > 0` AND
   `mandatory_replicas.<tier> == 0`).
2. **Window check** — `reads_in_window_by_dc[placement.dc]` must
   be 0 (no reads from this DC inside `promote_window_secs`).
3. **Anchor safety** — at least one mandatory-tier replica must
   survive the drop (the planner refuses any demotion that would
   strand the mandatory copy; defence-in-depth: the apply path
   also re-checks via `enforce_anchor` on `RemoveChunkLocation`).

A surviving demotion triggers the full sequence:

* `DeleteChunkReplica` RPC on the data node (idempotent
  `was_present`/`freed_bytes` shape).
* `RemoveChunkLocation` Raft write removes the SM entry.
* Conditional `SetChunkCooldown(expires_at_ms = 0)` — clears
  the chunk's cooldown entry **only if** the snapshot cooldown
  was 0 or in the past. A future operator-flush cooldown is
  left intact so the operator's intent ("no immediate
  re-promotion") wins.

Demotion does NOT consult the cooldown when *deciding*. The
cooldown only protects against immediate re-promotion; demotion
itself is allowed because the cache replica is already eligible
for eviction.

### Cooldown semantics

`cache_replicas.cooldown_secs` (default 300 s = 5 min) is the
window after an operator flush during which the engine refuses
to re-promote. Stored per `(file, chunk_index)` in
`FileMetadata.cache_cooldown_until_ms` as the absolute
millisecond-since-epoch expiry. Lifecycle:

* **Operator flush** sets `cooldown_until_ms = now +
  cooldown_secs`.
* **Promotion path** in the planner checks `now <
  cooldown_until_ms` and refuses.
* **Engine-driven demote** clears the entry via
  `SetChunkCooldown(0)` IFF the snapshot cooldown was past or
  absent. Future operator cooldowns are preserved.
* **State-machine apply**: `expires_at_ms = 0` removes the
  entry; any other value is `max()`-merged with the existing
  one (monotonic — concurrent flushes never regress).

This produces the user-visible behaviour `flush → no
re-promote for 5 min → operator-decay clears it → next read
re-promotes`, versus the engine-driven decay path `idle →
demote → immediate read re-promotes` (no cooldown set, no
wait).

### Anchor-aware demotion

The planner never emits a demotion that would leave 0
mandatory-tier replicas. This invariant lives in two places:

* `plan_chunk_demotions` filters before emitting the action.
* `RemoveChunkLocation`'s state-machine apply path re-checks
  via `enforce_anchor` (defence-in-depth against any future
  planner regression).

If a tier-aware anchor is configured
(`anchor.required_tier = "cold"`), the engine treats it as the
mandatory floor for demotion decisions.

### Worked example — `/movies/Wicked.mkv` lifecycle

Volume: cold mandatory, warm cache per-DC with `min=1, max=1`,
`promote_reads=2`, `promote_window_secs=14400` (4 h),
`cooldown_secs=300` (5 min). Anchor: `leli` DC, cold tier.

```
t=0:        Write of /movies/Wicked.mkv.
            Placement engine schedules 1 cold replica on
            LELI/data-primary01 (anchor-satisfying).
            State: {LELI cold}.

t=10:       Plex on TORO opens the file. 1 read from TORO.
            Window count for (chunk, toro) = 1.
            Below threshold (2); planner emits no promotion.

t=120:      Same TORO Plex session continues. 2nd read.
            Window count for (chunk, toro) = 2 ≥ threshold.
            Planner emits promotion → schedule warm replica on
            TORO/data-warm01 (no cooldown, no existing warm).
            State: {LELI cold, TORO warm}.

t=300:      Operator clicks "Flush cache" on /movies/Wicked.mkv.
            scheduler.flush_cache_for_path drops the warm
            replica and sets cooldown_until_ms = t=600s.
            State: {LELI cold} + cooldown active.

t=305:      TORO Plex re-opens. 1 read. Window count = 1.
            Below threshold AND below cooldown — no promotion.

t=400:      2nd read. Window count = 2. ≥ threshold BUT cooldown
            still active (t=400 < 600). No promotion.

t=610:      3rd read after cooldown expired. Window count
            depends on what the window saw (the t=305/t=400
            reads remain inside the 4h window). ≥ threshold.
            Planner promotes again → warm replica appears.

t=14_500:   No reads for 4 hours. Engine tick: window for TORO
            now empty. Planner emits demotion of the TORO warm
            replica. Dispatcher: DeleteChunkReplica →
            RemoveChunkLocation → SetChunkCooldown(0) (the
            snapshot cooldown is 0/past, so clear succeeds).
            State: {LELI cold}, no cooldown.

t=14_600:   Plex opens the file again. 1 read.
            (subsequent reads continue the new window — no
            cooldown blocks anything.)
```

The legacy temperature path runs in parallel for volumes
without a `cache_replicas` policy (back-compat); volumes WITH
one drive the engine described here.

## Temperature Classification

Each chunk maintains an access counter that the data nodes report to the manager via periodic heartbeats. The manager uses these counters to assign one of four temperature levels:

| Temperature | Threshold | Default Storage Target |
|-------------|-----------|------------------------|
| Hot | >= 5 reads per hour | NVMe / fast SSD |
| Warm | >= 2 reads per day (but below hot threshold) | SATA SSD |
| Cold | No access for 14+ days | HDD |
| Frozen | No access for 60+ days | HDD (reduced replicas) |

These thresholds are configurable per volume:

```toml
[volume.mydata.tiering]
hot_reads_per_hour   = 5
warm_reads_per_day   = 2
cold_after_days      = 14
frozen_after_days    = 60
```

The thresholds are evaluated from hottest to coldest. A chunk that meets the hot threshold is classified as hot regardless of how recently it was last accessed. A chunk that does not meet the warm threshold and has not been accessed for 14 days is cold.

### Access Counting

Data nodes track per-chunk read counters using a sliding-window approach:

- **Hourly window**: the number of reads in the current clock hour, used for the hot threshold.
- **Daily window**: the number of reads in the current calendar day (UTC), used for the warm threshold.
- **Last-access timestamp**: the wall-clock time of the most recent read, used for the cold and frozen thresholds.

Write operations also count as access events and reset the last-access timestamp, but they do not increment the read counters.

## Disk Assignment

When a data node starts, it registers each of its disks with the manager, including the disk type (NVMe, SSD, HDD) and the physical location in the topology hierarchy. The manager uses this information to build a mapping from temperature tiers to candidate disks.

```toml
# data-node.toml
[[disk]]
path = "/mnt/nvme0"
tier = "hot"
capacity_reserved_percent = 5

[[disk]]
path = "/mnt/ssd0"
tier = "warm"
capacity_reserved_percent = 5

[[disk]]
path = "/mnt/hdd0"
tier = "cold"
capacity_reserved_percent = 10
```

A single disk can serve only one tier. If no disk is assigned to a particular tier on a node, that node is not eligible for placement of chunks at that temperature.

### Capacity Reservation

The `capacity_reserved_percent` field reserves a percentage of each disk for temporary overhead during migrations and rebalancing. Once a disk crosses the `100 - capacity_reserved_percent` usage threshold, the manager stops placing new chunks on it and begins draining existing chunks to other disks in the same tier.

## Tiering Evaluation Cycle

The manager evaluates chunk temperatures on a configurable interval (default: 5 minutes):

1. **Collect**: the manager aggregates the latest access statistics from data-node heartbeats.
2. **Classify**: each chunk is assigned a temperature based on the thresholds.
3. **Compare**: the chunk's current tier (determined by the disk it resides on) is compared to the target tier for its temperature.
4. **Schedule**: if the tiers do not match, a migration task is created. See [Migration](./migration.md) for details on the task lifecycle.

```toml
[tiering]
evaluation_interval_secs = 300    # 5 minutes
max_concurrent_migrations = 10
```

### Hysteresis

To avoid thrashing -- where a chunk oscillates between tiers because its access rate sits near a threshold boundary -- the tiering engine applies a hysteresis margin. A chunk must exceed the promotion threshold by 20% to be promoted, and must fall below the demotion threshold by 20% to be demoted. For example, a warm chunk is only promoted to hot if it reaches 6 reads per hour (5 * 1.2), and a hot chunk is only demoted to warm if it drops below 4 reads per hour (5 * 0.8).

```toml
[tiering]
hysteresis_percent = 20
```

## Location-Aware Migration

When the tiering engine decides to migrate a chunk to a different tier, the placement engine selects a target disk that satisfies two constraints:

1. **Tier match**: the target disk must be assigned to the correct tier.
2. **Failure domain spread**: the new placement must maintain the required separation across failure domains (see [Replication](./replication.md)).

If no suitable disk exists in the same datacenter, the migration is deferred and an alert is raised. DTFS never migrates a chunk to a location that would violate the failure-domain policy.

### Cross-Datacenter Migration

For geo-distributed clusters, cross-datacenter migration is disabled by default because of bandwidth costs. It can be enabled per volume:

```toml
[volume.mydata.tiering]
allow_cross_dc_migration = false
```

When enabled, the placement engine prefers intra-datacenter targets but will fall back to cross-datacenter targets if no local disk has capacity in the required tier.

## Manual Tier Override

Administrators can pin a file or directory to a specific tier, bypassing the automatic temperature classification:

```bash
dtfs-cli tier pin /data/important-dataset --tier hot
dtfs-cli tier unpin /data/important-dataset
```

Pinned chunks are never demoted by the tiering engine. They can still be promoted if their access rate exceeds a higher tier's threshold, unless the `--strict` flag is used:

```bash
dtfs-cli tier pin /data/archive --tier cold --strict
```

## Monitoring

The manager exposes tiering metrics via the REST API and the WebUI dashboard:

- `dtfs_tiering_chunks_by_temperature{temperature="hot|warm|cold|frozen"}` -- current chunk count per temperature.
- `dtfs_tiering_migrations_scheduled_total` -- total migrations scheduled since startup.
- `dtfs_tiering_migrations_completed_total` -- total migrations completed.
- `dtfs_tiering_migrations_failed_total` -- total migrations that failed and were retried.
- `dtfs_tiering_evaluation_duration_seconds` -- histogram of evaluation cycle duration.
