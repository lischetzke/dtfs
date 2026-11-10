# Backup & Recovery

DTFS is designed for durability through replication, but operator errors, software bugs, or catastrophic multi-failure events can still cause data loss. This chapter covers backup strategies and disaster recovery procedures.

## What to Back Up

### Raft State (Critical)

The Raft state machine contains all cluster metadata:

- File namespace (paths, chunk mappings)
- Node registry and health records
- Volume configurations
- Cluster-wide configuration

This state lives under the manager's `data_dir` (`/var/lib/dtfs/manager` with the installed layout, which the unit's `ReadWritePaths=` requires; the compiled-in default is `/var/lib/dtfs`) in two directories:

- `raft-log/` -- the RocksDB Raft log (the source of truth for all metadata mutations).
- `raft-sm/` -- the state-machine snapshot. The latest snapshot is the single file `raft-sm/snapshot.bin`; it carries the full FSM state plus the Raft membership, so a restart restores membership before any log replay.

**Loss of all Raft state means loss of the file namespace.** Chunk data would still exist on data nodes but would be unaddressable without metadata.

### Raft Snapshots

The Raft implementation takes periodic snapshots based on the `snapshot_threshold` configuration (default: every 10,000 log entries). Each snapshot atomically replaces `raft-sm/snapshot.bin`; there is no separate `raft-snapshots/` directory. The snapshot captures the full state machine state and the membership config.

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

2. Snapshots are automatic. To verify the latest one on disk:

```bash
ls -la /var/lib/dtfs/manager/raft-sm/
```

3. Copy the state-machine snapshot to a backup location:

```bash
rsync -av /var/lib/dtfs/manager/raft-sm/ \
  /backup/dtfs/manager-sm/$(date +%Y%m%d)/
```

4. Back up the full `data_dir` (both `raft-log/` and `raft-sm/`) for a complete state capture:

```bash
# Stop writes briefly or use a filesystem snapshot
rsync -av /var/lib/dtfs/manager/ \
  /backup/dtfs/manager-full/$(date +%Y%m%d)/
```

### Automated Backup Schedule

Set up a cron job on each manager node:

```cron
# Daily Raft snapshot backup at 02:00
0 2 * * * rsync -av /var/lib/dtfs/manager/raft-sm/ /backup/dtfs/sm/$(hostname)/$(date +\%Y\%m\%d)/

# Weekly full data_dir backup at 03:00 Sunday (raft-log/ + raft-sm/)
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

If one manager in a multi-node group fails, the remaining managers keep
quorum and the cluster stays up. How you recover the failed member
depends on whether it kept its local Raft state.

**Case A -- the member kept its `data_dir` (`raft-log/` + `raft-sm/`).**
It is still in the voter set and already knows its identity. Just restart
the service:

```bash
systemctl start dtfs-manager
```

It rejoins as a follower on its own and catches up from the leader (log
replay, or an `InstallSnapshot` if it fell far behind). No membership
change and no backup restoration are needed.

If startup instead logs a refusal naming a purged log index (RAFT-6 /
CR2-7-6: the on-disk snapshot was corrupt/unreadable and this node's log
has already been purged past it, so no local copy of the pre-purge
history survives), treat it like Case B below — do not force a restart.
The manager never deletes an undecodable snapshot: it renames it to
`raft-sm/snapshot.bin.undecodable-<unix ts>`. If the refusal follows a
rollback to an older binary, the file is likely a valid snapshot from the
newer one: move it back to `raft-sm/snapshot.bin` and start the version
that wrote it, instead of wiping.



**Case B -- the member lost its state** (disk failure, fresh host, wiped
`data_dir`). A manager with empty Raft state does **not** auto-join: it
comes up as an inert, empty learner (term 0, no leader, empty namespace)
and never re-enters the voter set by itself. You must re-add it through
the membership API:

1. Provision/repair the host, install `dtfs-manager`, and configure it
   with the same `shared_secret` and `cluster.name`, `bootstrap = false`.
   Give it the failed member's original `[node].hostname` to recover that
   member's identity (the raft node id is `hash(hostname)`), or a new
   hostname to bring it back as a distinct member.
2. Start the service and confirm its API answers
   (`curl http://<mgr>:9402/api/v1/health`).
3. From the **leader**, add it back as a learner, then promote it once it
   has caught up:

   ```bash
   dtfs-cli --manager <leader>:9402 raft add-learner <hostname> <ip:9401> --secret <shared_secret>
   # watch GET /api/v1/raft/state on the new node until applied_log_lag == 0
   dtfs-cli --manager <leader>:9402 raft promote <hostname> --secret <shared_secret>
   ```

These commands are leader-only and shared-secret gated; a follower
answers `409` with a leader hint. See
[Cluster Management → Adding a Manager Node](./cluster-management.md#adding-a-manager-node)
for the full procedure and the learner-vs-voter distinction.

### Full Manager Group Recovery

If every manager has lost its Raft state but you hold a backup, recover
from **one** surviving member identity and grow the group back. Do **not**
restore a backup and set `bootstrap = true`: `raft.initialize()` refuses
a non-empty state, and bootstrapping over restored state would instead try
to form a second, divergent single-voter cluster (split-brain). The
restored snapshot already carries the old membership, so the recovery is a
restore-and-restart, not a re-bootstrap.

1. Stop any surviving manager processes.

2. Choose one host as the recovery node. Use the `data_dir` backup taken
   from one of the original managers (its identity = `hash([node].hostname)`
   must match the hostname you give the recovery node).

3. Restore that member's `data_dir` (both `raft-log/` and `raft-sm/`):

```bash
systemctl stop dtfs-manager
rsync -av --delete /backup/dtfs/manager-full/latest/ /var/lib/dtfs/manager/
```

4. Configure the recovery node with `bootstrap = false` and the matching
   hostname / `shared_secret` / `cluster.name`:

```toml
[node]
hostname = "mgr01"            # must match the backed-up member's hostname

[cluster]
name = "my-cluster"
shared_secret = "your-shared-secret"
bootstrap = false
```

5. Start the recovery manager:

```bash
systemctl start dtfs-manager
```

6. The manager loads the restored snapshot and log; its membership config
   still lists the original voter set. **Important constraint:** with the
   other voters gone, a multi-voter membership cannot reach quorum, so this
   single survivor cannot elect a leader on its own and will not accept
   writes yet. Confirm it has loaded the restored state but report
   `current_leader = null`:

```bash
curl http://<recovery-mgr>:9402/api/v1/raft/state   # voters lists the old set, current_leader null
```

7. Restore quorum by re-adding the other manager identities. There is no
   single-command "force new single-node membership" path today; the
   supported route is to bring the other original members back (restore
   each one's `data_dir` from backup, or repair the hosts) so the existing
   voter set regains a majority and elects a leader. Once a leader exists,
   use the membership API to drop permanently dead voters and add
   replacements:

```bash
dtfs-cli --manager <leader>:9402 raft remove <dead-hostname> --secret <shared_secret>
dtfs-cli --manager <leader>:9402 raft add-learner <new-hostname> <ip:9401> --secret <shared_secret>
dtfs-cli --manager <leader>:9402 raft promote <new-hostname> --secret <shared_secret>
```

   If you cannot bring back enough original voters to reach a majority, the
   cluster is stuck without quorum: forcing a smaller voter set from a
   single survivor requires manual openraft surgery that has no operator
   command today. Treat losing a majority of managers permanently as a
   genuine disaster and prioritise restoring those identities from backup.

8. Data nodes reconnect automatically once a leader is elected and the
   manager API is serving.

See [Cluster Management](./cluster-management.md) for the membership
commands referenced above.

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

1. Restore a `data_dir` backup (`raft-log/` + `raft-sm/`) to a test host.
2. Start a single manager (`bootstrap = false`, hostname matching the
   backed-up member). It loads the restored state; note that a single
   survivor of a multi-voter membership will report `current_leader = null`
   and cannot serve writes until quorum is restored (see Full Manager Group
   Recovery), but its `/api/v1/raft/state` and snapshot load confirm the
   backup is intact.
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
