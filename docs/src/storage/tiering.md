# Tiering

DTFS automatically classifies data into temperature tiers based on access patterns and migrates chunks to the appropriate storage media. The tiering engine runs on the manager leader node and periodically evaluates every chunk in the cluster.

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
