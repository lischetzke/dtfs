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
# Run a targeted deep scrub
dtfs-cli scrub --disk /mnt/affected-disk --level deep

# Check disk health with smartctl
ssh <node-host> smartctl -a /dev/sdX
```

**Resolution:**
1. If replicas exist on other nodes, the corrupt chunk will be automatically re-replicated from a healthy copy.
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
- Cache miss -- the requested chunk is not in the ARC cache or read-ahead cache.
- Remote reads -- the chunk is being served from a distant datacenter.
- Network congestion.
- Data node under heavy I/O from scrubbing or replication.

**Diagnosis:**

```bash
# Check cache metrics
curl -s http://<data-node>:9500/metrics | grep cache

# Check if reads are crossing datacenter boundaries (in FUSE client logs)
journalctl -u dtfs-fuse | grep "remote"
```

**Resolution:**
- Increase the data node ARC cache size (`cache.max_memory_mb` in `data.toml`).
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
- If the mount was lost due to a transient issue, remount and restart the data node. The disk will return to `healthy` status after the next successful heartbeat.
- For hardware failures, replace the disk and add the new one to the configuration.

## Performance Diagnostics

### Throughput Testing

```bash
# Write a test file through FUSE
dd if=/dev/urandom of=/mnt/dtfs/test-file bs=128M count=10

# Read a test file
dd if=/mnt/dtfs/test-file of=/dev/null bs=128M
```

### Cache Effectiveness

Monitor the cache hit ratio metric:

```bash
curl -s http://<data-node>:9500/metrics | grep dtfs_data_cache
```

A hit ratio below 50% indicates the working set exceeds the cache size. Consider increasing `cache.max_memory_mb`.

### Network Diagnostics

```bash
# Test gRPC connectivity to a data node
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
