# Disk Management

This chapter covers adding, removing, and replacing disks on DTFS Data nodes, as well as tier assignment.

## Disk Model

Each Data node manages one or more locally-mounted disks. Disks are XFS-formatted filesystems where chunks are stored in a two-level directory structure based on the BLAKE3 content hash prefix (e.g., `/mnt/ssd-a/ab/cd/<chunk_hex>`).

Every disk has the following properties tracked by the cluster:

| Property | Description |
|----------|-------------|
| `disk_id` | Numeric identifier, unique per node. |
| `uuid` | Persistent UUID for the disk (survives re-ordering). |
| `mount_path` | Filesystem mount point (e.g., `/mnt/ssd-a`). |
| `tier` | Storage tier: `hot`, `warm`, or `cold`. |
| `total_bytes` | Total capacity. |
| `used_bytes` | Current usage. |
| `status` | One of: `healthy`, `degraded`, `draining`, `failed`, `removed`. |

## Tier Assignment

Each disk is assigned to exactly one storage tier in the data node configuration:

```toml
[[disks]]
path = "/mnt/nvme-0"
tier = "hot"

[[disks]]
path = "/mnt/ssd-a"
tier = "warm"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"
```

### Tier Guidelines

| Tier | Typical Media | Replication Default | Use Case |
|------|---------------|--------------------:|----------|
| `hot` | NVMe SSD | 3 | Frequently accessed data, recent writes |
| `warm` | SATA SSD | 2 | Moderate access, recent data aging out of hot |
| `cold` | HDD / high-capacity SSD | 1 | Archival data, infrequently accessed |

Tier assignment drives two behaviors:

1. **Replication factor** -- The volume's per-tier replication settings determine how many copies are maintained on each tier. See [Replication](../storage/replication.md).
2. **Tiering migration** -- The tiering engine moves chunks between tiers based on access temperature. See [Tiering](../storage/tiering.md).

### Changing a Disk's Tier

To change the tier of an existing disk:

1. Update the `tier` field in `/etc/dtfs/data.toml`.
2. The data node detects the configuration change via live reload (see [Live Reload](../configuration/live-reload.md)) and reports the updated disk info to the manager on the next heartbeat.
3. Existing chunks on the disk are not moved immediately. The tiering engine will gradually migrate them if the chunk temperature does not match the new tier assignment.
4. To force immediate migration, trigger a manual scrub and tier evaluation:

```bash
dtfs-cli scrub --disk /mnt/ssd-a --level quick
```

## Adding a Disk

### Procedure

1. Physically install the disk and format it with XFS:

```bash
mkfs.xfs /dev/sdX
mkdir -p /mnt/new-disk
mount /dev/sdX /mnt/new-disk
```

2. Add the disk to `/etc/dtfs/data.toml`:

```toml
[[disks]]
path = "/mnt/new-disk"
tier = "warm"
```

3. If the data node is running and live reload is enabled, the new disk is picked up automatically. Otherwise, restart the data node:

```bash
systemctl restart dtfs-data
```

4. The data node reports the new disk to the manager via the next heartbeat. The manager begins including the disk in placement decisions.

5. Verify:

```bash
dtfs-cli nodes --id <node-id>
```

The new disk should appear in the disk list with `healthy` status.

### Filesystem Requirements

- **XFS is required.** DTFS relies on XFS features for efficient file allocation and `O_DIRECT` support.
- Ensure the mount includes `noatime` for performance:

```
/dev/sdX /mnt/new-disk xfs noatime,inode64 0 0
```

## Removing a Disk

### Graceful Removal

1. Place the disk in draining mode. This is done by removing the disk entry from the configuration file, which the data node interprets as a drain request.

2. Alternatively, use the API to set the node to maintenance mode if you need to remove the entire node:

```bash
dtfs-cli maintenance <node-id> --enable
```

3. The data node stops accepting new writes to the disk and begins migrating existing chunks to other disks (on the same node if available, or to other nodes).

4. Monitor draining progress by checking the disk status:

```bash
dtfs-cli nodes --id <node-id>
```

The disk transitions through states: `healthy` -> `draining` -> `removed`.

5. Once the disk shows `removed` status and zero chunks remain, physically remove it.

### Emergency Disk Failure

If a disk fails unexpectedly:

1. The data node detects I/O errors and marks the disk as `failed`.
2. The node reports the failure to the manager via the next heartbeat.
3. The manager begins re-replicating chunks that were stored on the failed disk, using replicas on other nodes.
4. The failed disk is excluded from all future placement decisions.
5. Replace the disk and add it as a new disk following the procedure above.

## Replacing a Disk

Disk replacement is a combination of removing the old disk and adding a new one:

1. Place the old disk in draining mode (or let failure detection handle it).
2. Wait for chunk evacuation to complete.
3. Physically swap the disk.
4. Format, mount, and configure the new disk with the same or different tier.
5. The data node picks up the new disk and reports it to the manager.

If the replacement disk has the same mount path as the old one, update the configuration to reflect any tier changes.

## Disk Status Monitoring

### Per-Node Disk View

```bash
dtfs-cli nodes --id <node-id>
```

This shows all disks on a node with their tier, capacity, usage, and status.

### REST API

```bash
curl http://<manager>:9402/api/v1/nodes/<node-id>
```

The response includes a `disks` array with full disk details.

### gRPC

The `DataService.GetDiskStatus` RPC returns disk information directly from a data node, bypassing the manager. This is useful for troubleshooting when the manager is unreachable.

## Disk Health Checks

The scrub engine performs periodic integrity checks on all disks. See [Scrubbing](../storage/scrubbing.md) for details.

Quick scrubs verify chunk metadata (sizes, counts), while deep scrubs recompute BLAKE3 checksums to detect bit rot.

## Capacity Planning

- The placement engine avoids disks that are above 90% utilization.
- When a disk approaches capacity, chunks are preferentially placed on other disks in the same tier.
- If all disks in a tier are full, writes to that tier will fail. Ensure adequate capacity headroom, especially on the hot tier where new writes land.

## Best Practices

- Use consistent mount paths across nodes for operational simplicity (e.g., `/mnt/ssd-{a,b,c}` for hot, `/mnt/hdd-{a,b}` for cold).
- Always use XFS with `noatime,inode64` mount options.
- Label disks by tier to avoid accidental mis-assignment after hardware maintenance.
- Monitor disk utilization and plan capacity expansions before reaching 80% usage on any tier.
- After adding new disks, the cluster does not automatically rebalance existing data. New writes will preferentially fill the emptier disks. To force rebalancing, use a manual scrub + tier evaluation cycle.
