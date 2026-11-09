# Disk Management

This chapter covers disks on DTFS data nodes: the disk model, tiers, adding,
replacing and retiring disks, and monitoring. The step-by-step procedures with
their checks are in [Decommission and Replace](../deployment/decommission-replace.md).

## Disk Model

Each data node manages one or more locally mounted filesystems. Chunks are stored
under `<mount>/chunks/<2 hex>/<2 hex>/<chunk_hex>.dat` (with a `.meta` file next
to it), in-flight writes under `<mount>/tmp/`, write-ahead markers under
`<mount>/.dtfs/wal/`, and quarantined chunks under `<mount>/.dtfs/quarantine/`.

| Property | Description |
|----------|-------------|
| `disk_id` | The disk's 0-based position in `[[disks]]`, persisted in `<mount>/.dtfs-disk-id`. Replica identity is `(node_id, disk_id)`. |
| `uuid` | Stable UUID in `<mount>/.dtfs-disk-uuid`, created on first start. |
| `mount_path` | The configured `path`. |
| `tier` | `hot`, `warm`, `cold` or `tape`. |
| `total_bytes` / `used_bytes` | Capacity as reported by the node. |
| `status` | `healthy`, `degraded`, `draining`, `failed` or `removed`. Operators do not set it: `degraded` and `failed` come from the node's health checks; nothing sets `draining` today. |

## Tier Assignment

Each disk belongs to exactly one tier:

```toml
[[disks]]
path = "/mnt/nvme-0"
tier = "hot"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"
```

Typical use: `hot` for NVMe, `warm` for SATA SSD, `cold` for the large HDD archive
(the anchor copy lives on the cold tier of the anchor datacenter in the reference
deployment). Tier drives the per-tier replication policy ([Replication](../storage/replication.md))
and the tiering engine's moves ([Tiering](../storage/tiering.md)).

Changing the `tier` of an existing entry keeps its position, so it is safe. The
`[[disks]]` section is **not** live-reloadable ([Live Reload](../configuration/live-reload.md)):
edit `/etc/dtfs/data.toml` and restart `dtfs-data`. Existing chunks stay where
they are; the tiering engine adjusts over time, and `dtfs-cli migrations create
<prefix> --target-tier T` adds replicas on a tier explicitly.

## Disk ids and `[[disks]]` order

A disk's `disk_id` is its position (0-based) in the `[[disks]]` list. On first
start the data node writes it to `<mount>/.dtfs-disk-id`. On later starts it
**refuses to start** if the disk now sits at a different position (the error
reads `disk <path> was disk_id N but is now at [[disks]] position M`), because
renumbering would make the manager attribute chunks to the wrong disk and the
orphan reaper delete the wrong copies.

Rules: only append new `[[disks]]` entries at the end; never remove or reorder an
entry in the middle; mount by `UUID=` or `LABEL=` so a device rename cannot swap
two disks. To retire a disk, make sure its content has other live replicas, then
replace it in place (same position) with the new disk. A replacement disk starts
with no `.dtfs-disk-id` and takes the position it is configured at.

## Adding a Disk

1. Format and mount it (XFS with `noatime,inode64` is the documented choice; any
   POSIX filesystem with atomic rename and fsync works), and `chown dtfs:dtfs` the
   mount point:

   ```bash
   mkfs.xfs /dev/sdX
   mkdir -p /mnt/new-disk
   echo 'UUID=<uuid> /mnt/new-disk xfs noatime,inode64 0 0' >> /etc/fstab
   mount /mnt/new-disk && chown dtfs:dtfs /mnt/new-disk
   ```

2. Append it to `/etc/dtfs/data.toml` (at the end):

   ```toml
   [[disks]]
   path = "/mnt/new-disk"
   tier = "warm"
   ```

3. Add the path to `ReadWritePaths=` (and `RequiresMountsFor=`) in
   `/etc/systemd/system/dtfs-data.service.d/disks.conf`, `systemctl daemon-reload`,
   then `systemctl restart dtfs-data`. Without the drop-in the unit's
   `ProtectSystem=strict` makes the disk read-only to the daemon
   ([Filesystem Layout](../deployment/filesystem-layout.md#drop-ins)).
4. Verify with `dtfs-cli nodes --id <node-id>`: the disk appears with `healthy`
   status and `.dtfs-disk-id` on the new mount equals its position.

Existing data is not rebalanced. New writes prefer disks with more free space.

## Removing or retiring a Disk

There is no drain operation for a disk. Do not delete its `[[disks]]` entry if it
is in the middle of the list: the data node would refuse to start. Replace it in
place instead (below), or, for the last entry only, remove the entry after its
chunks are covered by other replicas. Evacuation relies on replication policy,
`dtfs-cli migrations create` and the scheduler; see
[Decommission and Replace](../deployment/decommission-replace.md).

### Emergency Disk Failure

1. The data node detects I/O errors and reports the disk as `failed` in its next
   heartbeat; the manager logs a "SMART alert" for it.
2. A failed disk's replicas stop counting as live; the repair scheduler re-creates
   the mandatory replicas from copies on other disks or nodes. The disk is excluded
   from placement.
3. Wait for `files_missing_chunks == 0` and no undergoal chunks
   (`GET /api/v1/chunks?state=undergoal`), then replace the disk in place.

If a chunk had its only copy on the failed disk, that is data loss; do not wipe
anything and escalate.

## Replacing a Disk

Replace at the **same `[[disks]]` position and mount path**.

- Healthy disk, planned swap: stop `dtfs-data`, copy the whole filesystem to the new
  disk with attributes and hidden files (`rsync -aHAXx`), keeping
  `.dtfs-disk-id` and `.dtfs-disk-uuid`, mount it at the old path, start.
- Failed disk: let repair finish first (above), then mount the empty replacement
  at the same path and start; it takes the configured position, and the scheduler
  drops the now-stale location rows (when another live copy exists) and refills
  the disk. A node that reports chunks missing whose last copy was on that disk is
  quarantined.

## Reclaiming Space After Deletes

Deleting a file (`unlink`) only removes its metadata; there is no immediate chunk
delete. The orphan reaper deletes chunks that no file references once their
on-disk `.dat` is older than `orphan_grace_secs` (default 86 400 s, minimum 21 600 s), checked every
`orphan_interval_secs` (default 86 400 s), so free space returns about one to two
days after the delete. See [Orphan Chunk Cleanup](./orphan-cleanup-runbook.md) to
speed that up.

## Disk Status Monitoring

```bash
dtfs-cli nodes --id <node-id>                       # disks of one node
curl http://<manager>:9402/api/v1/nodes/<node-id>    # JSON, includes a disks array
```

The `DataService.GetDiskStatus` RPC returns disk information directly from a data
node, bypassing the manager, which helps when the manager is unreachable. With
`cluster.shared_secret` set it requires the `x-dtfs-rpc-*` stamp (method
`GetDiskStatus`, empty binding, see [Authentication](../security/authentication.md)).

## Integrity and Scrubbing

The scrub engine runs on every data node; quick scrubs check chunk presence and
sizes, deep scrubs recompute BLAKE3 checksums. A fault renames the chunk into
`.dtfs/quarantine/<id>.dat.bad` (never deletes it) and the replica is repaired from
another copy. With `[scrub] quarantine_on_fault = true` (default) a single fault
also quarantines the whole node until an operator runs `dtfs-cli unquarantine`;
with `false` faults are still recorded and chunks quarantined individually but the
node keeps serving. `dtfs-cli scrub --level quick|deep` triggers a cluster-wide
scrub (targeted scrubs are not supported). See
[Scrubbing](../storage/scrubbing.md).

## Capacity Planning

- Placement keeps `[placement] min_free_bytes` (default 1 GiB) and `min_free_pct`
  (default 5 %) free on every disk; writes that would dip below that are refused
  on that disk (typed `RESOURCE_EXHAUSTED`) or spilled to another tier when
  `allow_capacity_spill = true`.
- If every disk of a tier is full, writes needing that tier fail. Keep headroom,
  especially on the tier where new writes land.
- Plan expansion before 80 % usage on a tier.

## Best Practices

- Consistent mount paths across nodes (`/mnt/ssd-a`, `/mnt/hdd-a`).
- Mount by UUID with `noatime`.
- Keep the intended `[[disks]]` order in your config management data, not derived.
- After adding disks the cluster does not rebalance existing data on its own.
