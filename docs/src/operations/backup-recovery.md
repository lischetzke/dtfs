# Backup & Recovery

DTFS is designed for durability through replication, but operator errors, software bugs, or catastrophic multi-failure events can still cause data loss. This chapter covers backup strategies and disaster recovery procedures.

## What to Back Up

### Raft State (Critical)

The Raft state machine contains all cluster metadata:

- File namespace (paths, chunk mappings)
- Node registry and health records
- Volume configurations
- Cluster-wide configuration

This state is stored in RocksDB under the manager's `data_dir` (default: `/var/lib/dtfs/manager/`).

**Loss of all Raft state means loss of the file namespace.** Chunk data would still exist on data nodes but would be unaddressable without metadata.

### Raft Snapshots

The Raft implementation takes periodic snapshots based on the `snapshot_threshold` configuration (default: every 10,000 log entries). Snapshots capture the full state machine state and are stored alongside the Raft log.

### Configuration Files

Back up all configuration files:

- `/etc/dtfs/manager.toml` (each manager)
- `/etc/dtfs/data.toml` (each data node)
- `/etc/dtfs/fuse.toml` (each FUSE client)

These are not stored in the cluster and must be backed up independently.

### Chunk Data

Chunk data is inherently replicated across data nodes. For most deployments, the replication factor provides sufficient durability. However, for critical data with a cold replication factor of 1, consider external backups.

## Backup Procedures

### Raft Snapshot Backup

The recommended backup method for cluster metadata:

1. Identify the current Raft leader:

```bash
dtfs-cli status
```

2. Trigger a snapshot on the leader (or wait for the next automatic snapshot):

```bash
# Snapshots are automatic. To verify the latest:
ls -la /var/lib/dtfs/manager/raft-snapshots/
```

3. Copy the snapshot directory to a backup location:

```bash
rsync -av /var/lib/dtfs/manager/raft-snapshots/ \
  /backup/dtfs/manager-snapshots/$(date +%Y%m%d)/
```

4. Back up the full RocksDB directory for a complete state capture:

```bash
# Stop writes briefly or use a filesystem snapshot
rsync -av /var/lib/dtfs/manager/ \
  /backup/dtfs/manager-full/$(date +%Y%m%d)/
```

### Automated Backup Schedule

Set up a cron job on each manager node:

```cron
# Daily Raft snapshot backup at 02:00
0 2 * * * rsync -av /var/lib/dtfs/manager/raft-snapshots/ /backup/dtfs/snapshots/$(hostname)/$(date +\%Y\%m\%d)/

# Weekly full RocksDB backup at 03:00 Sunday
0 3 * * 0 rsync -av /var/lib/dtfs/manager/ /backup/dtfs/full/$(hostname)/$(date +\%Y\%m\%d)/
```

### Configuration Backup

```bash
# Back up all config files
rsync -av /etc/dtfs/ /backup/dtfs/config/$(hostname)/
```

### Off-Site Backup

For disaster recovery across datacenters, copy backups to remote storage:

```bash
# Example: sync to S3-compatible storage
aws s3 sync /backup/dtfs/ s3://dtfs-backups/$(hostname)/
```

## Recovery Procedures

### Single Manager Recovery

If one manager in a multi-node group fails:

1. The remaining managers continue operating with quorum.
2. Provision a new host and install `dtfs-manager`.
3. Configure it with the same `shared_secret` and point it to an existing manager.
4. Start the manager. It joins the Raft group and automatically syncs state from the leader.
5. No backup restoration is needed; Raft replication handles state transfer.

### Full Manager Group Recovery

If all manager nodes are lost (total cluster metadata loss):

1. Stop any surviving manager processes.

2. Choose one host as the bootstrap node.

3. Restore the most recent Raft snapshot backup to the data directory:

```bash
rsync -av /backup/dtfs/manager-full/latest/ /var/lib/dtfs/manager/
```

4. Configure the bootstrap node with `cluster.bootstrap = true`:

```toml
[cluster]
name = "my-cluster"
shared_secret = "your-shared-secret"
bootstrap = true
```

5. Start the bootstrap manager:

```bash
systemctl start dtfs-manager
```

6. The manager initializes a new single-node Raft group from the restored state.

7. Add additional manager nodes following the normal join procedure (see [Cluster Management](./cluster-management.md)).

8. Data nodes will reconnect automatically once they detect the manager is available.

### Data Node Recovery

If a data node is lost:

1. The manager detects the failure and begins re-replicating under-replicated chunks.
2. No manual intervention is needed for data recovery if replication factors are sufficient.
3. Provision a replacement node, configure it, and add it to the cluster.
4. New writes will be placed on the replacement node, and the scheduler will rebalance over time.

### Chunk Data Recovery

If chunks are lost (e.g., disk failure on a node with replication factor 1):

1. Check if any replica exists on other nodes:

```bash
dtfs-cli scrub --level deep
```

2. If replicas exist, the scrub and replication manager will restore the target replica count.
3. If no replicas exist, the data is lost. This is why cold-tier replication factor 1 should be used only for data that is backed up externally or is non-critical.

## Disaster Recovery Plan

### Recovery Time Objectives

| Scenario | RTO | Data Loss |
|----------|-----|-----------|
| Single data node failure | Minutes | None (if replication factor >= 2) |
| Single manager failure | Minutes | None (Raft replication) |
| Minority manager failure | Minutes | None |
| Majority manager failure | Hours | Possible metadata loss since last snapshot |
| Total cluster loss | Hours | Data loss limited to changes since last backup |

### Disaster Recovery Runbook

1. **Assess the failure.** Determine which nodes are lost and whether quorum is maintained.

2. **If quorum is maintained:** Replace failed nodes normally. No backup restoration needed.

3. **If quorum is lost but data nodes survive:**
   - Restore manager group from backup.
   - Data nodes reconnect and report their chunk inventories.
   - The manager reconciles metadata with actual chunk locations.

4. **If everything is lost:**
   - Restore managers from the most recent backup.
   - Provision new data nodes.
   - Data stored on the original data nodes is lost unless external backups exist.

## Backup Verification

Periodically verify that backups are usable:

1. Restore a backup to a test environment.
2. Bootstrap a single manager from the restored data.
3. Verify the file namespace is intact:

```bash
dtfs-cli files --limit 100
```

4. Confirm node metadata is consistent.

Run backup verification at least quarterly, or monthly for critical deployments.

## Best Practices

- Back up Raft snapshots daily and retain at least 7 days of history.
- Store backups off-site in a different failure domain than the cluster.
- Use filesystem-level snapshots (LVM, ZFS) for consistent RocksDB backups without stopping the manager.
- Monitor backup job success/failure with your alerting system.
- Document the recovery procedure and test it regularly.
- For cold-tier data with replication factor 1, implement external backups or increase the replication factor.
- Keep configuration files in version control (e.g., Git) for easy restoration and audit history.
