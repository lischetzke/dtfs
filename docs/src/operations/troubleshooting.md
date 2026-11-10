# Troubleshooting

This chapter covers common issues, diagnostic commands, and resolution procedures for DTFS clusters.

## Diagnostic Commands

### Quick Health Check

```bash
# Manager health
dtfs-cli health

# Cluster overview
dtfs-cli status

# All nodes with health states
dtfs-cli nodes

# Detailed node view including disks
dtfs-cli nodes --id <node-id>
```

### REST API Diagnostics

```bash
# Cluster status (JSON)
curl -s http://<manager>:9402/api/v1/cluster/status | jq .

# Node list
curl -s http://<manager>:9402/api/v1/nodes | jq .

# Specific node details
curl -s http://<manager>:9402/api/v1/nodes/<node-id> | jq .

# Audit log (recent events)
curl -s http://<manager>:9402/api/v1/audit?limit=20 | jq .

# Current configuration
curl -s http://<manager>:9402/api/v1/config | jq .
```

### Log Inspection

DTFS uses structured logging via the `tracing` framework. Control log verbosity with the `RUST_LOG` environment variable:

```bash
# Default (info level)
RUST_LOG=info

# Debug level for a specific crate
RUST_LOG=dtfs_manager=debug,dtfs_raft=debug

# Trace level for maximum detail
RUST_LOG=dtfs_data=trace

# Filter to specific modules
RUST_LOG=dtfs_data::scrub=debug,dtfs_data::cache=info
```

When running via systemd, check logs with:

```bash
journalctl -u dtfs-manager -f
journalctl -u dtfs-data -f --since "10 minutes ago"
```

## Common Issues

### Node Shows as "Suspect" or "Down"

**Symptoms:** `dtfs-cli nodes` shows a node with `Suspect` or `Down` health status.

**Causes:**
- Network connectivity issues between the node and the manager.
- The node process has crashed or is not running.
- The node is overloaded and unable to send heartbeats in time.

**Diagnosis:**

```bash
# Check if the node process is running
ssh <node-host> systemctl status dtfs-data

# Test network connectivity to the manager
ssh <node-host> curl -s http://<manager>:9402/api/v1/health

# Check node logs for errors
ssh <node-host> journalctl -u dtfs-data --since "5 minutes ago"
```

**Resolution:**
- If the process is stopped, restart it: `systemctl start dtfs-data`
- If there are network issues, resolve connectivity. The node will return to `Healthy` after the next successful heartbeat.
- If the node is overloaded, check for runaway scrub operations or excessive I/O. Reduce `scrub.max_io_percent` if needed.

### Raft Leader Election Loops

**Symptoms:** Frequent leader changes visible in logs or metrics (`dtfs_raft_term` incrementing rapidly).

**Causes:**
- Network instability between manager nodes.
- Clock skew between managers.
- One or more managers under heavy load.
- Disk I/O latency on the Raft log storage.

**Diagnosis:**

```bash
# Check Raft metrics
curl -s http://<manager>:9402/metrics | grep dtfs_raft

# Check manager logs for election events
journalctl -u dtfs-manager | grep -i "election\|vote\|leader"

# Check network latency between managers
ping <other-manager>
```

**Resolution:**
- Ensure stable, low-latency network between all manager nodes. Raft is sensitive to network jitter.
- Move the Raft data directory to fast storage (SSD/NVMe).
- Increase `raft.election_timeout_ms` (default: 400ms) if the network has variable latency.
- Ensure NTP is configured and clock skew is minimal.

### Service Does Not Start After a Reboot (bind address not available)

**Symptoms:** After a reboot, `dtfs-manager` (or `dtfs-data` / `dtfs-fuse`) is
not running even though it is `enabled`. The boot-time log shows the process
started, failed to bind, and exited:

```text
INFO  dtfs_manager::server: Manager gRPC server listening on 100.x.y.z:9401
ERROR dtfs_manager: gRPC server: bind interface is not available / bind
      address cannot be found — the network interface may not be up yet
      (e.g. Tailscale not connected at boot) (addr=100.x.y.z:9401): ...
```

Affected nodes are inconsistent across a rolling reboot — some come up, some
do not — because the failure is a timing race.

**Cause:** the daemon binds its listener to a specific overlay-network address
(e.g. its Tailscale `100.x` IP). `network-online.target`, which the unit waits
on, only covers the primary interface — the Tailscale interface and its
address are brought up later, asynchronously by `tailscaled`. If the daemon
starts before the address is assigned, the bind fails with `EADDRNOTAVAIL`.

> Note: `After=tailscaled.service` alone does **not** fix this — it waits for
> the tailscaled *process* to start, not for the tailnet to be *connected* and
> the address assigned. There is no stock systemd target for "Tailscale
> connected".

On current builds the daemon exits **non-zero** on a bind failure, so
`Restart=on-failure` retries automatically until the interface is up — the
node self-heals within `RestartSec`. On older builds the daemon exited `0`
(treated by systemd as success), so it never retried and stayed down until a
manual `systemctl start`.

**Diagnosis:**

```bash
# Is the unit enabled but inactive after boot?
systemctl is-enabled dtfs-manager; systemctl is-active dtfs-manager

# Find the failed first-start in this boot's log
journalctl -u dtfs-manager -b | grep -iE "bind|transport error|listening|exited"

# Confirm the bound address belongs to the overlay interface, and whether
# it was assigned yet at the service start time
tailscale ip -4
ip -4 addr show
```

**Resolution:**

1. Immediate: `systemctl start dtfs-manager` (the address is up by now).
2. Permanent — gate startup on the interface being connected, via a drop-in so
   it survives package upgrades:

   ```ini
   # /etc/systemd/system/dtfs-manager.service.d/10-wait-tailscale.conf
   [Unit]
   After=tailscaled.service
   Wants=tailscaled.service

   [Service]
   # Block until this node has a Tailscale IPv4 (bounded so a never-connected
   # tailnet fails the start instead of hanging; Restart=on-failure then retries)
   ExecStartPre=/usr/bin/timeout 60 sh -c 'until tailscale ip -4 >/dev/null 2>&1; do sleep 1; done'
   ```

   Then `systemctl daemon-reload`. Apply the same drop-in (adjusting the unit
   name) to `dtfs-data` and `dtfs-fuse@*`, which bind `:9500` / the mount on the
   same overlay address. Verify the `tailscale` binary path with
   `command -v tailscale` and adjust if it is not `/usr/bin/tailscale`.
3. Alternative: bind to a wildcard address (`0.0.0.0:9401`) in the daemon
   config so startup does not depend on a specific overlay address. Only do
   this where the listening ports are otherwise firewalled to the overlay
   (the reference deployment exempts `9401`/`9402`/`9500` on the tailnet).

### FUSE Client Cannot Connect

**Symptoms:** FUSE mount fails or shows errors when accessing files.

**Causes:**
- Manager is unreachable from the client host.
- TLS certificate issues.
- Shared secret mismatch.

**Diagnosis:**

```bash
# Test manager connectivity
curl -s http://<manager>:9402/api/v1/health

# Check FUSE client logs
journalctl -u dtfs-fuse -f

# Verify TLS configuration
openssl s_client -connect <manager>:9401
```

**Resolution:**
- Verify the `manager_addr` in `/etc/dtfs/fuse.toml` is correct.
- If using auto-TLS, ensure the client successfully completed the join protocol and received the cluster CA.
- Check that the `shared_secret` matches between the client and the cluster.

### Scrub Reports Corrupt Chunks

**Symptoms:** Scrub results show `chunks_corrupt > 0`.

**Causes:**
- Bit rot on the underlying storage media.
- Disk hardware failure (early stages).
- Interrupted write that left a partial chunk.

**Diagnosis:**

```bash
# Recent integrity faults and which node/chunk they hit
curl -s "http://<manager>:9402/api/v1/integrity/faults?limit=50" | jq .

# Cluster-wide deep scrub (targeted scrubs are not supported)
dtfs-cli scrub --level deep

# Check disk health with smartctl
ssh <node-host> smartctl -a /dev/sdX
```

**Resolution:**
1. The bad chunk is renamed into `<disk>/.dtfs/quarantine/<id>.dat.bad` (never deleted). If replicas exist on other nodes, the repair scheduler re-creates the replica from a healthy copy. Repeated fault rows after a data-node restart are the same quarantined chunks being re-reported, not new corruption.
   With `[scrub] quarantine_on_fault = true` (default) the node also self-quarantines (health `Quarantined`, excluded from placement) until you run `dtfs-cli unquarantine <node-id> --operator-id <who> --reason-acknowledged "<what you verified, 16+ chars>"`. Unquarantine only after the cause (disk, memory) is understood.
2. If the disk shows hardware errors (`smartctl` reports), plan a disk replacement. See [Disk Management](./disk-management.md).
3. If corruption is isolated, the scrub marks the chunk for re-replication. No manual intervention is needed if the replication factor is greater than 1.

### Write Failures

**Symptoms:** Applications report errors writing files through the FUSE mount.

**Causes:**
- Insufficient data nodes with capacity in the target tier.
- All disks in the target tier are full.
- Write safety quorum cannot be met (not enough replicas acknowledge).
- Manager is unavailable (partition scenario).

**Diagnosis:**

```bash
# Check cluster capacity
dtfs-cli status

# Check per-node disk utilization
dtfs-cli nodes

# Check write safety configuration
dtfs-cli config
```

**Resolution:**
- Add more disk capacity to the appropriate tier.
- If write safety requires `min_replicas_before_ack = 2` but only one data node is available, either add more nodes or temporarily lower the write safety (not recommended for production).
- If the manager is unreachable, check the FUSE client's partition behavior mode. See [Partition Behavior](../access/partition-behavior.md).

### High Latency on Reads

**Symptoms:** File reads through FUSE are slow.

**Causes:**
- Cache miss -- the requested chunk is not in the FUSE read-ahead or local disk cache. (Data nodes have no in-memory chunk cache; every read streams from disk.)
- Remote reads -- the chunk is being served from a distant datacenter.
- Network congestion.
- Data node under heavy I/O from scrubbing or replication.

**Diagnosis:**

```bash
# Check if reads are crossing datacenter boundaries (in FUSE client logs)
journalctl -u dtfs-fuse | grep "remote"
```

**Resolution:**
- Raise the FUSE client's `[readahead] cache_mb` or enable the local disk cache (`[cache] local_cache_dir`, `local_cache_max_gb`).
- Enable or tune read-ahead mode on the FUSE client. See [Read-Ahead Modes](../access/read-ahead.md).
- If reads are consistently remote, the tiering engine should eventually migrate replicas closer. Check that tiering is enabled.
- Reduce scrub I/O intensity by lowering `scrub.max_io_percent`.

### Configuration Not Propagating

**Symptoms:** Configuration changes made via the API do not take effect on data nodes.

**Causes:**
- Configuration version mismatch between the manager and data nodes.
- Data node is not heartbeating (and therefore not receiving config updates).
- Live reload is disabled or the config file was not saved correctly.

**Diagnosis:**

```bash
# Check config version on the manager
dtfs-cli config

# Compare with data node's reported config version (from detailed node view)
dtfs-cli nodes --id <node-id>
```

**Resolution:**
- Restart the data node to force a config refresh.
- Verify the data node is heartbeating successfully.
- Check that the manager's heartbeat response includes `config_update` when the data node's version is outdated.

### A Daemon Refuses To Start

The process exits with a message naming the cause; under systemd it restarts every
5 s, so read `journalctl -u <unit> -n 30`. Do not retry-loop; fix the cause.

| Message contains | Cause | Fix |
|---|---|---|
| `was disk_id N but is now at [[disks]] position M` (data) | A disk moved to a different `[[disks]]` position | Restore the original order; never remove or reorder entries. See [Disk Management](./disk-management.md#disk-ids-and-disks-order). |
| `is not a valid disk id` (data) | `.dtfs-disk-id` damaged | Restore it from the disk's expected position; do not guess. |
| `Raft state machine snapshot was corrupt/unreadable ... Refusing to start as a voter` (manager) | Undecodable snapshot moved aside (`raft-sm/snapshot.bin.undecodable-<ts>`) while the log is already purged | After a rollback: move the file back to `raft-sm/snapshot.bin` and start the version that wrote it. Otherwise stop, move `raft-log/` and `raft-sm/` aside, restart as a learner from a healthy peer ([Backup and Recovery](./backup-recovery.md)). |
| `Raft state machine is stale: it has applied up to index N but this node's log was already purged up to index M` (manager) | An old `snapshot.bin` next to a newer log | Restore `raft-sm` from a recent backup, or wipe both directories and re-join as a learner; never restore a lone old snapshot over a newer log. |
| `cluster.shared_secret is empty` (manager, data) | Empty secret | Set it (same on all nodes). |
| `[memory] ...`, `[placement] min_free_pct ...`, `max_message_size_mb must be > 0` | Invalid ladder, percentage or size | Fix the named key. |
| `... must be > 0`, `heartbeat_interval_ms ... must be below election_timeout_ms` | Zero or inverted intervals (`[tiering]`, `[monitoring]`, `[raft]`, FUSE `[cache] prune_interval_secs`, `[partition_safety] quorum_loss_timeout_secs`) | Fix the named key. |
| unit status 226/NAMESPACE | A `ReadWritePaths=` directory does not exist | Create `/var/lib/dtfs/<role>` and mount the disks ([Filesystem Layout](../deployment/filesystem-layout.md)). |
| read-only file system on the Raft log (manager) | `data_dir` is not the unit's writable path | Set `data_dir = "/var/lib/dtfs/manager"`. |

### Disk Shows "Failed" Status

**Symptoms:** A disk appears as `failed` in the node details.

**Causes:**
- Filesystem I/O errors.
- Disk unmounted or mount point inaccessible.
- Hardware failure.

**Diagnosis:**

```bash
# Check if the filesystem is mounted
ssh <node-host> mount | grep <disk-path>

# Check system logs for I/O errors
ssh <node-host> dmesg | tail -50

# Check SMART data
ssh <node-host> smartctl -a /dev/sdX
```

**Resolution:**
- If the disk is unmountable, replace it. See [Disk Management](./disk-management.md).
- If the mount was lost due to a transient issue, remount and restart the data node. The disk will return to `healthy` status after the next successful heartbeat. Use `RequiresMountsFor=` in the unit drop-in so a missing mount fails the start instead of initialising an empty directory as a new disk.
- For hardware failures, replace the disk and add the new one to the configuration.

## Performance Diagnostics

### Throughput Testing

```bash
# Write a test file through FUSE
dd if=/dev/urandom of=/mnt/dtfs/test-file bs=4M count=250

# Read a test file
dd if=/mnt/dtfs/test-file of=/dev/null bs=4M
```

### Cache Effectiveness

Caching is on the client: the read-ahead cache (`[readahead] cache_mb`) and the optional local disk cache (`[cache] local_cache_dir`). The data node's `[cache]` block is parsed but inert. There is no cache hit-ratio metric in this revision; judge by `journalctl -u dtfs-fuse` and by read latency while replaying the same file.

### Network Diagnostics

```bash
# Test gRPC connectivity to a data node (with shared_secret set, an
# unstamped call answers PermissionDenied, which still proves reachability)
grpcurl -plaintext <data-node>:9500 dtfs.data.DataService/GetDiskStatus

# Measure latency between FUSE client and data nodes
ping <data-node>
```

## Collecting a Support Bundle

When reporting issues, collect the following information:

1. Cluster status: `dtfs-cli status`
2. Node list: `dtfs-cli nodes`
3. Recent audit entries: `dtfs-cli audit --limit 50`
4. Configuration: `dtfs-cli config`
5. Manager logs: `journalctl -u dtfs-manager --since "1 hour ago"`
6. Data node logs: `journalctl -u dtfs-data --since "1 hour ago"`
7. FUSE client logs: `journalctl -u dtfs-fuse --since "1 hour ago"`
8. System information: `uname -a`, `df -h`, `free -h`
