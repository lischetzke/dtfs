# Failure Scenarios

Quick-reference answers to the most common "what happens if…" questions an
operator asks before their first 2 a.m. page. Each entry links to the
deep-dive doc that covers tuning and recovery in full.

The detection windows below are the defaults shipped with the binary and
are configurable per cluster.

---

## What happens if a data node dies unexpectedly?

**Detection (15-30 seconds).** Every data node sends a heartbeat to the
manager Raft cluster. The cluster manager marks a node `Suspect` after
15 seconds of silence and `Down` after 30 seconds
(`crates/dtfs-manager/src/cluster.rs` thresholds, written to the Raft
state machine via `UpdateNodeStatus`). Only the Raft leader judges, and a
newly elected leader counts the silence from its own election, so a manager
failover or rolling restart does not mark healthy data nodes `Down`; a
node that dies around a leader change is detected up to 30 s later.

**Reads continue immediately.** Clients pick the next healthy replica
via the `location.rs` distance heuristic. If a read RPC fails mid-flight
the FUSE client falls back to another replica without surfacing an
error (P0-2 replica fallback).

**Writes continue.** The chunk allocator (`allocate_chunks`) excludes
nodes with status `Down` or `Suspect`. Writes that were in flight to
the dead node fail their gRPC stream, the FUSE client retries the put
on a different replica candidate, and the write lease is renewed
unchanged.

**Re-replication starts.** `HealthMonitor::handle_node_down`
enumerates every chunk that lived on the dead node and enqueues
`ReplicationTask`s. The scheduler runs them subject to the global
concurrency cap, respecting the
[mandatory-vs-cache replica boundary](../storage/replication.md) — only
mandatory replicas are restored automatically; cache replicas are
left for the tiering engine to re-create on demand.

**Anchor invariant.** If the dead node held the only LELI copy of any
chunk, the under-replication scheduler flags it loud (alert
`AnchorViolated`) and prioritises restoring the anchor before any
warm/cold backfill.

**Recovery (node comes back).** The data node re-registers via the
join protocol on startup. The manager flips its status back to
`Healthy` on the next heartbeat. Chunks that were already
re-replicated elsewhere are not deleted from the returning node at once: its
replicas count as live again, replicas above the goal are removed by the
scheduler's excess-removal pass, and chunks no file references any more by the
orphan reaper after its grace period.

See also: [Cluster Management](./cluster-management.md),
[Replication](../storage/replication.md).

---

## What happens if a disk dies or becomes unreadable?

**SMART monitoring.** Each data node polls disk health and includes
`DiskStatus` (`Healthy`, `Degraded`, `Failed`) in its heartbeats. A
disk transitioning to `Degraded` or `Failed` is logged by the manager as a
"SMART warning/alert" (`crates/dtfs-manager/src/monitor.rs::check_disk_health`);
there is no webhook for it yet ([Alerting](./alerting.md)).

**Read returns I/O error from the kernel.** The data server reports
the failed read back to the client, which falls back to the next
replica. The chunk is also flagged as a `pending_corruption` candidate
on that disk.

**Scrub catches silent corruption.** The scrub scheduler computes
BLAKE3 over the on-disk `.dat` file and compares to
`ChunkMeta.checksum`. A mismatch quarantines the bad chunk on the
local disk (the file is renamed into `.dtfs/quarantine/`, never deleted),
reports the fault to the manager, and the repair scheduler re-creates the
replica from a healthy copy. With `[scrub] quarantine_on_fault = true` (the
default) the node also self-quarantines until an operator runs
`dtfs-cli unquarantine`; with `false` the fault is recorded and reported but the
node keeps serving.

**Auto-repair.** The scheduler uses `replicate_chunk` to copy a clean
replica onto a different disk (or different node, if the failed disk
is the only one on the host). The quarantined `.dat.bad` file stays in
`.dtfs/quarantine/` for the operator; DTFS does not delete it.

**Disk marked failed.** A disk the node reports as `Failed` stops
counting as a live replica location and is excluded from placement; the
manager then re-creates the affected mandatory replicas from other copies.

See also: [Disk Management](./disk-management.md),
[Scrubbing](../storage/scrubbing.md).

---

## What happens when one of three managers fails?

**Raft tolerates `(N-1)/2` failures.** A three-manager quorum survives
one failure. The leader (if it was the leader that died) is replaced
in `election_timeout_min..election_timeout_max` (default ~1 second).
Metadata reads continue throughout; writes pause for the duration of
the election (sub-second under normal conditions).

**Clients failover automatically.** The shared `DtfsClient`
maintains a `manager_addrs` list and rotates to the next reachable
address on any `Unavailable` RPC (`advance_active_manager`). The
caller sees one retried RPC, not an error.

**Re-join is automatic.** A manager that comes back online replays
its Raft log against the current leader's log. If its log is too far
behind, it pulls a snapshot and applies the tail. The poisoned-state
detector (Bug AC) covers the corner case where a manager comes up as
a stuck candidate — it wipes its own vote and log and rejoins as a
follower.

See also: [Consensus (Raft)](../architecture/consensus.md),
[Cluster Management](./cluster-management.md).

---

## What happens when all managers fail simultaneously (power outage)?

**The cluster has no metadata plane.** Reads and writes return errors
from the FUSE client until at least 2 of 3 managers are back (Raft
quorum). Data nodes continue running and continue answering chunk
fetches if you talk to them directly via the gRPC API, but new files
cannot be created and metadata cannot be read.

**Data on disk is safe.** The integrity contract guarantees:

- Every chunk is committed via `tmp + atomic rename` (no torn writes).
- Every metadata mutation was Raft-replicated and fsynced before
  acknowledgement, and is stored in each manager's local Raft log and
  the latest snapshot under `/var/lib/dtfs/manager/`.
- The anchor invariant means every chunk has at least one copy on the
  LELI `data-primary01` archive, so even if every other DC is wiped
  the data exists.

**Recovery is just "start the managers".** When power returns:

1. Boot the three manager hosts. systemd auto-starts
   `dtfs-manager.service` (`Restart=on-failure`, 5s delay).
2. Each manager loads its local snapshot and replays its Raft log.
3. As soon as 2 of 3 are up, they elect a leader and the cluster is
   writable again.

No manual `bootstrap` step is required for a cluster that was
previously running — bootstrap is only for a brand-new cluster.

See also: [Cold Start](#how-to-start-the-cluster-after-a-power-outage),
[Backup & Recovery](./backup-recovery.md).

---

## How to start the cluster after a power outage

### Case A — managers went dark, data nodes stayed up

1. Power on the three manager hosts (LELI: mgr01, mgr02, mgr03).
2. systemd starts `dtfs-manager.service` on each.
3. Wait for `curl -s http://mgr01:9402/readyz` to return `200 OK` on
   at least 2 of 3 managers (typically <30 seconds).
4. Data nodes that were already running re-establish heartbeats on
   their next interval (default 5 seconds). The cluster manager
   flips them back to `Healthy`.
5. FUSE clients reconnect automatically via `connect_many`.

No manual intervention. Verify with `curl http://mgr01:9402/api/v1/health`.

### Case B — managers AND data nodes went dark

1. Power on the manager hosts first. Wait for quorum (Case A step 3).
2. Power on the data nodes. Each one re-registers via gRPC
   `RegisterNode` (→ `RegisterNode` Raft write) and starts
   sending heartbeats. The manager marks them `Healthy` after the
   first successful heartbeat.
3. Power on FUSE clients last. The mount unit runs
   `release_leases_by_host` on startup to clear any orphan write
   leases left behind by the crash, then opens the mount normally.
4. Verify with the WebUI at `http://mgr01:9402/ui` — all nodes should
   show green within ~30 seconds.

**Cold-start ordering is important** because:

- Data nodes need a live manager to register with — if a data node
  comes up before quorum it backs off and retries (no crash, no data
  loss, but the WebUI will show it as `Down` until the manager catches
  up).
- FUSE clients need a live manager to acquire write leases — they
  reconnect cleanly via `connect_many`, but won't service syscalls
  until at least one manager answers.

### Case C — only some managers come back

If only 1 of 3 managers boots, the cluster will not elect a leader
(quorum requires 2). The single manager will retry voting indefinitely
and serve no traffic. Boot a second manager — quorum is restored and
the cluster recovers without further intervention.

**Do not** try to reduce membership to a single-node quorum unless you
have lost the other managers permanently. That is a separate, manual
disaster-recovery procedure documented in
[Backup & Recovery](./backup-recovery.md).

---

## What happens when a client loses its connection?

**FUSE syscalls block (briefly), then retry.** The shared client
treats `Unavailable` RPC errors as transient and rotates to the next
manager in its `manager_addrs` list via `advance_active_manager`. Under a
brief network blip the FUSE caller sees a slow syscall, not an error.

**Write leases survive short partitions.** A write lease has a 120 s
TTL and is renewed at the 60 s mark
(`WRITE_LEASE_MS`/`WRITE_LEASE_RENEW_AFTER_MS` in
`crates/dtfs-fuse/src/filesystem.rs`). A client that reconnects inside
that window keeps writing without re-acquiring; one that reconnects
later transparently re-acquires on the next write.

**Memory backpressure.** If a data node returns `RESOURCE_EXHAUSTED`
(Bug G shed path), the client retries with backoff
(`put_chunk_retry_backoff`). The caller sees a slow write, not an
error.

**Long partitions.** If every manager is unreachable past the retry
budget, the syscall returns `EIO` to the application. Plex shows
"playback error" and the user can retry. No data is lost — the next
syscall replays once the client reconnects.

See also: [Partition Behavior](../access/partition-behavior.md),
[FUSE Mount](../access/fuse-mount.md).

---

## How to reconnect clients

**Most of the time, you don't.** The FUSE client reconnects
automatically. The cases where manual intervention helps:

### The mount looks frozen and won't recover

The FUSE process is alive but every syscall hangs (typically because
the manager pool was unreachable for longer than the retry budget and
the client is stuck waiting on a never-completing RPC).

1. `sudo systemctl restart dtfs-fuse@<volume>.service` — the unit's
   `ExecStart` runs the orphan-lease cleanup
   (`release_leases_by_host`) before opening the mount, so writes that
   were in flight at restart get their leases released cleanly.
2. The mount comes back. Applications retry their syscalls.

### The mount is gone (kernel detached)

A FUSE crash can leave the kernel in a state where `/mnt/dtfs` reports
"Transport endpoint is not connected". Recover with:

1. `sudo fusermount3 -u /mnt/dtfs` (or `sudo umount -l /mnt/dtfs` if
   the lazy unmount is required).
2. `sudo systemctl restart dtfs-fuse@<volume>.service`.

### The client lost its manager list

If the client was deployed with stale `manager_addrs` (e.g., you moved
managers to new IPs without updating the FUSE config), update
`/etc/dtfs/fuse.toml` and restart. The client only re-reads the
manager list on process startup.

### Bulk reconnect after a network event

Run the restart in parallel across hosts:

```bash
ansible -i /path/to/inventory.yml clients \
    -m systemd -a "name=dtfs-fuse@plex.service state=restarted"
```

The orphan-lease cleanup is safe to run concurrently across clients —
each client only releases leases tagged with its own hostname.

See also: [FUSE Mount](../access/fuse-mount.md),
[Troubleshooting](./troubleshooting.md).

---

## Detection-time cheat sheet

| Failure | Detected within | Reads | Writes |
|---|---|---|---|
| Data node dies | 30 s | Failover immediate | Failover immediate |
| Disk dies (I/O error) | Immediate (per-read) | Failover per chunk | Excluded next allocation |
| Disk dies (silent corruption) | Next scrub pass | Wrong bytes possible | OK |
| Manager dies (1 of 3) | ~1 s (Raft election) | Sub-second pause | Sub-second pause |
| All managers die | Immediate | EIO until quorum returns | EIO until quorum returns |
| Client loses connection | Per-syscall retry budget | Blocked, then retry | Blocked, then retry |

---

## Related deep dives

- [Cluster Management](./cluster-management.md) — node lifecycle,
  drain, replace.
- [Disk Management](./disk-management.md) — add/remove disks, replace
  failed drives.
- [Backup & Recovery](./backup-recovery.md) — snapshot/restore,
  disaster recovery from a single surviving manager.
- [Replication](../storage/replication.md) — mandatory vs cache
  replicas, under-replication scheduler.
- [Scrubbing](../storage/scrubbing.md) — quick/deep scrub cadence and
  corruption handling.
- [Consensus (Raft)](../architecture/consensus.md) — election timing,
  log compaction, snapshots.
- [Partition Behavior](../access/partition-behavior.md) — client
  behavior under network splits.
- [Troubleshooting](./troubleshooting.md) — diagnosing common
  symptoms.
