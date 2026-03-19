# Quick Start

This guide gets a single-node DTFS instance running in under 5 minutes. It is intended for development and experimentation -- see [First Cluster](./first-cluster.md) for production deployments.

## Prerequisites

- DTFS binaries built (see [Installation](./installation.md))
- Linux with FUSE3 support (or WSL 2)

## Step 1: Create Configuration Files

Create the configuration directory and write minimal config files for all three components.

```bash
sudo mkdir -p /etc/dtfs
sudo mkdir -p /var/lib/dtfs/manager
sudo mkdir -p /var/lib/dtfs/data
sudo mkdir -p /mnt/dtfs-data       # Chunk storage directory
sudo mkdir -p /mnt/dtfs             # FUSE mount point
```

### Manager Configuration

```bash
cat <<'EOF' | sudo tee /etc/dtfs/manager.toml
[node]
id = "auto"
hostname = "localhost"
data_dir = "/var/lib/dtfs/manager"

[location]
region = "local"
datacenter = "local"
rack = "rack-1"
host = "localhost"

[cluster]
name = "dtfs-quickstart"
shared_secret = "change-me-in-production"
bootstrap = true

[raft]
listen_addr = "0.0.0.0:9400"
election_timeout_ms = 400
heartbeat_interval_ms = 150

[grpc]
listen_addr = "0.0.0.0:9401"

[api]
listen_addr = "0.0.0.0:9402"

[tls]
mode = "disabled"

[defaults]
chunk_size_mb = 128

[defaults.write_safety]
min_replicas_before_ack = 1
write_timeout_secs = 30

[defaults.replication]
hot = 1
warm = 1
cold = 1

[tiering]
enabled = false

[monitoring]
heartbeat_interval_secs = 5
suspect_after_missed = 3
down_after_missed = 6
EOF
```

> **Note:** For a single-node setup, replication is set to 1 for all tiers and `min_replicas_before_ack` is 1. The `bootstrap = true` flag tells this manager to initialize a new Raft cluster.

### Data Node Configuration

```bash
cat <<'EOF' | sudo tee /etc/dtfs/data.toml
[node]
id = "auto"
hostname = "localhost"
data_dir = "/var/lib/dtfs/data"

[location]
region = "local"
datacenter = "local"
rack = "rack-1"
host = "localhost"

[cluster]
manager_addr = "localhost:9401"
shared_secret = "change-me-in-production"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "disabled"

[[disks]]
path = "/mnt/dtfs-data"
tier = "hot"

[cache]
max_memory_mb = 1024
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
EOF
```

### FUSE Client Configuration

```bash
cat <<'EOF' | sudo tee /etc/dtfs/fuse.toml
[client]
manager_addr = "localhost:9401"

[location]
region = "local"
datacenter = "local"
rack = "rack-1"
host = "localhost"

[mount]
mountpoint = "/mnt/dtfs"

[readahead]
mode = "folder-sequential"
prefetch_chunks = 8
prefetch_next_files = 3

[cache]
local_cache_max_gb = 10

[partition_safety]
mode = "strict"
quorum_loss_timeout_secs = 10
EOF
```

## Step 2: Start the Manager

Open a terminal and start the manager node:

```bash
RUST_LOG=dtfs=info dtfs-manager --config /etc/dtfs/manager.toml
```

You should see output similar to:

```text
INFO  dtfs_manager > Starting DTFS Manager with config: /etc/dtfs/manager.toml
INFO  dtfs_manager > Manager localhost at 0.0.0.0:9401 (local/local/rack-1/localhost)
INFO  dtfs_manager > Raft log store opened at "/var/lib/dtfs/manager/raft-log"
INFO  dtfs_manager > Raft cluster bootstrapped with node <id>
INFO  dtfs_manager > Tiering engine started (evaluation interval: 3600s)
```

The manager is now running with:
- gRPC service on port **9401**
- REST API and WebUI on port **9402**
- Raft internal communication on port **9400**

## Step 3: Start the Data Node

Open a second terminal:

```bash
RUST_LOG=dtfs=info dtfs-data --config /etc/dtfs/data.toml
```

Expected output:

```text
INFO  dtfs_data > Starting DTFS Data Node with config: /etc/dtfs/data.toml
INFO  dtfs_data > Data node localhost at 0.0.0.0:9500 (local/local/rack-1/localhost)
INFO  dtfs_data > Initialized 1 disks
INFO  dtfs_data > Initialized ARC cache: max_memory=1024MB, max_entries=8192
INFO  dtfs_data > Started scrub scheduler background task
```

The data node registers itself with the manager and begins sending heartbeats.

## Step 4: Mount the Filesystem

Open a third terminal and start the FUSE client:

```bash
RUST_LOG=dtfs=info dtfs-fuse --config /etc/dtfs/fuse.toml
```

Expected output:

```text
INFO  dtfs_fuse > starting DTFS FUSE client
INFO  dtfs_fuse > connecting to manager
INFO  dtfs_fuse > connected to manager successfully
INFO  dtfs_fuse > filesystem initialised
INFO  dtfs_fuse > waiting for shutdown signal (Ctrl+C)
```

The DTFS filesystem is now mounted at `/mnt/dtfs`. You can use standard file commands:

```bash
# Write a file
echo "Hello, DTFS!" > /mnt/dtfs/hello.txt

# Read it back
cat /mnt/dtfs/hello.txt

# List files
ls -la /mnt/dtfs/

# Copy a larger file
cp /path/to/largefile.bin /mnt/dtfs/
```

## Step 5: Open the WebUI

Open a browser and navigate to:

```
http://localhost:9402
```

The WebUI shows:
- Cluster health status
- Registered nodes and their disk utilization
- Volume configuration
- Recent audit log entries

## Verifying the Setup

You can verify all components are communicating by checking the manager logs. After starting the data node, the manager should log:

```text
INFO  dtfs_manager > RegisterNode: <node_id>
```

And periodic heartbeats:

```text
DEBUG dtfs_manager > Heartbeat from node <node_id>
```

## Architecture of This Setup

```text
+------------------+
|   Your App       |
|   (cp, cat, ls)  |
+--------+---------+
         |
   +-----v------+     FUSE mount at /mnt/dtfs
   | dtfs-fuse  |     Connects to manager on :9401
   | :fuse.toml |     Read-ahead: folder-sequential
   +-----+------+
         |  gRPC
   +-----v--------+
   | dtfs-manager  |   Raft (single-node), metadata
   | :9400 (raft)  |   REST API + WebUI on :9402
   | :9401 (grpc)  |
   | :9402 (api)   |
   +-----+---------+
         |  gRPC
   +-----v------+
   | dtfs-data   |   Chunk storage on /mnt/dtfs-data
   | :9500       |   ARC cache: 1 GB
   +-------------+
```

## Stopping

Press `Ctrl+C` in each terminal to stop the components. Stop in reverse order:

1. FUSE client (`dtfs-fuse`)
2. Data node (`dtfs-data`)
3. Manager (`dtfs-manager`)

## Troubleshooting

**"failed to connect to DTFS manager"** -- Make sure the manager is running and the `manager_addr` in your config matches the manager's gRPC listen address.

**"No disk for tier Hot"** -- The data node cannot find a disk configured for the requested tier. Verify the `[[disks]]` section in `data.toml` and ensure the path exists.

**Permission denied on /mnt/dtfs** -- FUSE mounts may require the user to be in the `fuse` group:

```bash
sudo usermod -aG fuse $USER
# Log out and back in
```

**RocksDB errors on startup** -- Ensure the `data_dir` directories exist and are writable.

## Next Steps

- [First Cluster](./first-cluster.md) -- scale out to a multi-node deployment
- [Manager Configuration](../configuration/manager.md) -- tune all manager settings
- [Architecture Overview](../architecture/overview.md) -- understand how the system works
