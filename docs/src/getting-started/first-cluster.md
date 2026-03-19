# First Cluster

This guide walks through deploying a production-style DTFS cluster with 3 manager nodes, 4 data nodes across 2 racks, and FUSE clients. It assumes you have already completed the [Installation](./installation.md) steps on all machines.

## Cluster Topology

We will deploy the following topology:

```text
Region: us-east
Datacenter: dc-1
+-----------------------------------------------------------+
|                                                           |
|  Rack A                       Rack B                      |
|  +-------------------------+  +-------------------------+ |
|  | mgr-01  (manager)       |  | mgr-02  (manager)       | |
|  | data-01 (SSD hot+warm)  |  | data-03 (SSD hot+warm)  | |
|  | data-02 (HDD cold)      |  | data-04 (HDD cold)      | |
|  +-------------------------+  +-------------------------+ |
|                                                           |
|  mgr-03 (manager, Rack A or B -- odd count for quorum)   |
|                                                           |
+-----------------------------------------------------------+

Clients:
  client-01 (Rack A, mounts /mnt/dtfs)
  client-02 (Rack B, mounts /mnt/dtfs)
```

### Node Roles Summary

| Hostname | Role | Rack | Disks |
|----------|------|------|-------|
| mgr-01 | Manager | rack-a | -- |
| mgr-02 | Manager | rack-b | -- |
| mgr-03 | Manager | rack-a | -- |
| data-01 | Data | rack-a | /mnt/ssd-a (hot), /mnt/ssd-b (warm) |
| data-02 | Data | rack-a | /mnt/hdd-a (cold) |
| data-03 | Data | rack-b | /mnt/ssd-c (hot), /mnt/ssd-d (warm) |
| data-04 | Data | rack-b | /mnt/hdd-b (cold) |
| client-01 | FUSE | rack-a | -- |
| client-02 | FUSE | rack-b | -- |

## Step 1: Prepare Storage Disks

On each data node, format and mount the storage disks. Use XFS for production:

```bash
# On data-01:
sudo mkfs.xfs /dev/sdb
sudo mkfs.xfs /dev/sdc
sudo mkdir -p /mnt/ssd-a /mnt/ssd-b
sudo mount /dev/sdb /mnt/ssd-a
sudo mount /dev/sdc /mnt/ssd-b

# On data-02:
sudo mkfs.xfs /dev/sdb
sudo mkdir -p /mnt/hdd-a
sudo mount /dev/sdb /mnt/hdd-a
```

Repeat for `data-03` and `data-04` with their respective disks.

Create the DTFS metadata directories on each disk:

```bash
sudo mkdir -p /mnt/ssd-a/.dtfs /mnt/ssd-a/chunks /mnt/ssd-a/tmp
```

## Step 2: Generate a Shared Secret

All nodes in the cluster authenticate using a shared secret. Generate one and distribute it to all machines:

```bash
openssl rand -hex 32
# Example output: a3f8c1d9e2b7...
```

Use this value in the `shared_secret` field of every configuration file.

## Step 3: Configure Manager Nodes

### mgr-01 (Bootstrap Node)

```bash
cat <<'EOF' | sudo tee /etc/dtfs/manager.toml
[node]
id = "auto"
hostname = "mgr-01"
data_dir = "/var/lib/dtfs/manager"

[location]
region = "us-east"
datacenter = "dc-1"
rack = "rack-a"
host = "mgr-01"

[cluster]
name = "production"
shared_secret = "YOUR_SHARED_SECRET_HERE"
bootstrap = true

[raft]
listen_addr = "0.0.0.0:9400"
election_timeout_ms = 400
heartbeat_interval_ms = 150
snapshot_threshold = 10000

[grpc]
listen_addr = "0.0.0.0:9401"
max_message_size_mb = 256

[api]
listen_addr = "0.0.0.0:9402"
cors_origins = ["*"]

[tls]
mode = "auto"

[defaults]
chunk_size_mb = 128

[defaults.write_safety]
min_replicas_before_ack = 2
write_timeout_secs = 30

[defaults.replication]
hot = 3
warm = 2
cold = 1

[tiering]
enabled = true
hot_threshold_reads_per_hour = 5
cold_threshold_days = 14
frozen_threshold_days = 60
migration_bandwidth_limit_mbps = 200
evaluation_interval_secs = 3600

[monitoring]
heartbeat_interval_secs = 5
suspect_after_missed = 3
down_after_missed = 6
EOF
```

> **Important:** Only the first manager node should have `bootstrap = true`. This initializes the Raft cluster.

### mgr-02 and mgr-03

Use the same configuration as `mgr-01` with the following changes:

```toml
[node]
hostname = "mgr-02"  # or "mgr-03"

[location]
rack = "rack-b"      # mgr-02 is in rack-b; mgr-03 in rack-a
host = "mgr-02"      # or "mgr-03"

[cluster]
bootstrap = false     # These nodes join an existing cluster
```

## Step 4: Configure Data Nodes

### data-01 (Hot + Warm SSD)

```bash
cat <<'EOF' | sudo tee /etc/dtfs/data.toml
[node]
id = "auto"
hostname = "data-01"
data_dir = "/var/lib/dtfs/data"

[location]
region = "us-east"
datacenter = "dc-1"
rack = "rack-a"
host = "data-01"

[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "YOUR_SHARED_SECRET_HERE"

[grpc]
listen_addr = "0.0.0.0:9500"
max_message_size_mb = 256

[tls]
mode = "auto"

[[disks]]
path = "/mnt/ssd-a"
tier = "hot"

[[disks]]
path = "/mnt/ssd-b"
tier = "warm"

[cache]
max_memory_mb = 8192
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
EOF
```

### data-02 (Cold HDD)

```toml
[node]
hostname = "data-02"

[location]
rack = "rack-a"
host = "data-02"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"

[cache]
max_memory_mb = 2048
```

Repeat the pattern for `data-03` (rack-b, hot+warm) and `data-04` (rack-b, cold).

## Step 5: Configure FUSE Clients

### client-01

```bash
cat <<'EOF' | sudo tee /etc/dtfs/fuse.toml
[client]
manager_addr = "mgr-01:9401"

[location]
region = "us-east"
datacenter = "dc-1"
rack = "rack-a"
host = "client-01"

[mount]
mountpoint = "/mnt/dtfs"

[readahead]
mode = "folder-sequential"
prefetch_chunks = 8
prefetch_next_files = 3

[cache]
local_cache_dir = "/var/cache/dtfs"
local_cache_max_gb = 50

[partition_safety]
mode = "strict"
quorum_loss_timeout_secs = 10
show_warning = true
EOF
```

The `location` field is critical for the FUSE client. It determines which data nodes are considered "nearest" for read operations. A client in `rack-a` will prefer reading from `data-01` and `data-02` over nodes in `rack-b`. See [Data Distribution](../architecture/data-distribution.md) for details on location-aware placement.

## Step 6: Start the Cluster

Start components in this order:

### 1. Bootstrap Manager

```bash
# On mgr-01:
sudo RUST_LOG=dtfs=info dtfs-manager --config /etc/dtfs/manager.toml
```

Wait for the bootstrap message:

```text
INFO  Raft cluster bootstrapped with node <id>
```

### 2. Join Additional Managers

```bash
# On mgr-02:
sudo RUST_LOG=dtfs=info dtfs-manager --config /etc/dtfs/manager.toml

# On mgr-03:
sudo RUST_LOG=dtfs=info dtfs-manager --config /etc/dtfs/manager.toml
```

The Raft cluster will elect a leader and form a 3-node quorum. You should see leader election messages in the logs:

```text
INFO  Raft: node <id> elected as leader for term 1
```

### 3. Start Data Nodes

```bash
# On each data node:
sudo RUST_LOG=dtfs=info dtfs-data --config /etc/dtfs/data.toml
```

Each data node registers with the manager and begins sending heartbeats. The manager logs will show:

```text
INFO  RegisterNode: <node_id>
```

### 4. Mount FUSE Clients

```bash
# On each client:
sudo RUST_LOG=dtfs=info dtfs-fuse --config /etc/dtfs/fuse.toml
```

## Step 7: Verify the Cluster

### Check Cluster Health via WebUI

Navigate to `http://mgr-01:9402` in a browser. The dashboard should show:

- 3 manager nodes (one leader, two followers)
- 4 data nodes, all healthy
- Disk utilization across all tiers

### Verify Replication

Write a file and confirm it is replicated:

```bash
# On client-01:
dd if=/dev/urandom of=/mnt/dtfs/test-replication.bin bs=1M count=256
```

This creates a 256 MB file, which is split into two 128 MB chunks. With `hot = 3` replication, each chunk is stored on 3 different data nodes.

### Check Failure Domain Spread

The [placement engine](../architecture/data-distribution.md) ensures that the 3 replicas of each hot chunk are placed on nodes in different racks. For our topology:

```text
Chunk 0: data-01 (rack-a) + data-03 (rack-b) + data-01 or data-03 (best effort)
Chunk 1: data-03 (rack-b) + data-01 (rack-a) + ...
```

## Running as systemd Services

For production, run each component as a systemd service:

```bash
cat <<'EOF' | sudo tee /etc/systemd/system/dtfs-manager.service
[Unit]
Description=DTFS Manager Node
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/dtfs-manager --config /etc/dtfs/manager.toml
Restart=on-failure
RestartSec=5
Environment=RUST_LOG=dtfs=info
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now dtfs-manager
```

Create similar unit files for `dtfs-data` and `dtfs-fuse`.

## Scaling the Cluster

### Adding a Data Node

1. Install DTFS on the new machine
2. Create a `data.toml` pointing to any manager address
3. Format and mount disks
4. Start `dtfs-data`

The data node registers automatically. The placement engine incorporates the new node into the hash ring, and subsequent writes will begin using it. Existing data is not automatically rebalanced -- see [Cluster Management](../operations/cluster-management.md) for rebalancing operations.

### Adding a Manager Node

Manager nodes participate in Raft consensus. Adding a manager requires a Raft membership change:

1. Start the new manager with `bootstrap = false`
2. The existing leader adds the new node to the Raft membership
3. The new node receives a snapshot and begins replicating the log

> **Warning:** Always maintain an odd number of manager nodes (3 or 5) to ensure a clear quorum. See [Consensus](../architecture/consensus.md) for quorum requirements.

## Next Steps

- [Architecture Overview](../architecture/overview.md) -- understand the system design
- [Tiering](../storage/tiering.md) -- configure automatic hot/cold migration
- [Monitoring](../operations/monitoring.md) -- set up health monitoring and alerting
- [Security](../security/tls.md) -- configure TLS encryption
