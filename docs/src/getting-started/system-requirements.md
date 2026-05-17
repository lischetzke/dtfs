# System Requirements

Hardware sizing for a DTFS cluster depends on the **roles** each host runs and the **scale** of the namespace + access pattern. This page gives per-role baselines, scaling formulas you can plug your own numbers into, and concrete recommendations for the common deployment shapes.

DTFS runs as up to three independent processes on a host:

| Process | Role | systemd unit |
|---|---|---|
| `dtfs-manager` | Raft consensus, metadata, REST/gRPC API, **WebUI** | `dtfs-manager.service` |
| `dtfs-data` | Chunk storage on local disks, ARC cache, scrub | `dtfs-data.service` |
| `dtfs-fuse` | POSIX mount via FUSE | `dtfs-fuse@<escaped-mountpoint>.service` |

Any combination of these three is supported on the same host. There is no separate WebUI process — it is served by `dtfs-manager` on the API port (default 9402).

---

## TL;DR — recommended shapes

| Shape | Hosts | Roles per host | Use case |
|---|---|---|---|
| **Single-node** | 1 | mgr + data + client | Home lab / desktop / dev workstation |
| **Small HA (3-node)** | 3 | mgr + data + client | 3-node home or small office, every host can mount |
| **Service tier + clients** | 3 service + N clients | service: mgr + data · client: client only | Production media servers |
| **Control + storage tier** | 3 mgr + N data + M clients | mgr only · data only · client only | Large clusters (>10 data nodes) |

Numbers below are per host. Pick the role combination row(s) that match your deployment and add them up.

---

## Per-role baselines (idle + minimum)

| Role | CPU (cores) | RAM | Disk (OS) | Disk (state/data) | Network |
|---|---|---|---|---|---|
| Manager | 1 | 512 MB | 10 GB | 5 GB (Raft log + snapshots) | 100 Mbit/s |
| Data node | 1 | 2 GB (ARC cache) | 10 GB | per-tier disks, see formula | 1 Gbit/s |
| FUSE client | 1 | 512 MB | 10 GB | — (network FS) | 1 Gbit/s |
| WebUI | included in manager | — | — | — | — |

These are the *minimum* numbers to start a node and pass the smoke tests. Real workloads need the headroom calculated below.

---

## Scaling formulas

### Manager

```
Manager RAM ≈ 512 MB
            + 200 bytes × file_count                       # FSM file index
            + 120 bytes × chunk_count × replication_factor # chunk locations
            + 2 MB                                          # audit log (capped 10 000)
            + data-node-count × ~4 KB                       # node table
            + RocksDB block cache (~64 MB default)
```

Manager disk (state) ≈ `snapshot_threshold × ~200 B` (Raft log, RocksDB-backed; bounded because `dtfs-raft` purges past the last snapshot) plus a few MB for the persisted snapshot file under `<data_dir>/raft-sm/`.

Manager CPU is mostly idle. Peaks are: snapshot serialise + write to disk (≈ every `snapshot_threshold` log entries, default 200), and the periodic scheduler/tiering tick (default every 30 s / 3600 s). One core is sufficient for clusters with < 5 M files; bump to 2 cores if you also enable the WebUI for many concurrent operators.

**Worked example** — 1 M media files at average 250 MB each, replication factor 3, 128 MB chunks:

```
chunk_count = 1_000_000 × ceil(250 MB / 128 MB) = 2_000_000
chunk_locations = 2_000_000 × 3 = 6_000_000
FSM RAM ≈ 0.2 KB × 1M (files)        = ~ 200 MB
       + 0.12 KB × 6M (chunk locs)   = ~ 720 MB
       + overhead                    = ~ 100 MB
       ≈ 1.0 GB working set
```

So a 2 GB RAM allowance gives you 2× headroom.

### Data node

```
Data RAM ≈ 512 MB                            # process + connections
         + ARC cache (cache.max_memory_mb, default 2 GB)
         + 64 bytes × chunks_on_this_node     # in-memory chunk index

Data disk (per tier) ≈ tier_capacity_target / data_node_count × replication_factor
```

ARC cache should be sized to hold the *working set* of frequently-read chunks. For media-server workloads (each video read mostly once per access), 1–2 GB is plenty; bump to 4–8 GB if many users re-read the same files concurrently.

Data CPU: 1 core suffices for HDD-bound workloads. On NVMe/10 GbE, scale to **1 core per 500 MB/s of expected sustained throughput** — BLAKE3 hashing + tonic gRPC compression are the dominant CPU costs.

**Worked example** — 4 data nodes, 100 TB usable storage, RF=3:

```
raw_capacity = 100 TB × 3 = 300 TB
per-node disk = 300 TB / 4 = 75 TB
```

Split that across whatever tier mix you want (e.g. 5 TB hot NVMe + 70 TB cold HDD per node).

### FUSE client

```
FUSE RAM ≈ 200 MB                                  # process + tokio runtime
        + chunk_size × max_concurrent_open_writes   # write buffer
        + readahead.max_size_mb (configured, default 32 MB) × N inflight prefetches
        + cache.max_size_mb (client cache, default 512 MB if enabled)
```

For a media-player client doing one stream at a time, 1 GB RAM is plenty. For a backup-server client writing many large files in parallel, plan on `chunk_size × parallelism` for the write buffer — at chunk_size=128 MB and 8 parallel writers that's 1 GB just for buffering.

FUSE CPU: 4 cores baseline; this is the **minimum** for stable behaviour on Tokio (we force `worker_threads=4` to avoid F-026 — the 1-vCPU stall). Sequential single-stream reads/writes on a fast NIC will easily saturate a core on BLAKE3 + tonic; plan for 2–4 cores per concurrent saturating stream.

FUSE disk: none. It's a network filesystem; the kernel page cache is in RAM.

### WebUI

The WebUI is a single-page application embedded into the `dtfs-manager` binary and served on the same HTTP port as the REST API (default 9402). It adds **no extra process and no extra resource budget** — the per-manager numbers above already cover it. The only consideration is exposing port 9402 to the operator network (or behind a reverse proxy) and serving the static assets on each API request, which is sub-millisecond overhead.

---

## Network bandwidth

| Direction | Typical traffic | Notes |
|---|---|---|
| Client → Manager | ~10 KB per file open / create | Metadata RPCs only; no bulk data |
| Client → Data node | Full chunk bandwidth | Reads/writes go *direct* to a chunk replica |
| Data node ↔ Data node | Replication + re-replication | At RF=3 every write touches 3 nodes |
| Manager ↔ Manager | Raft replication of FSM mutations | Small; bounded by mutation rate |
| Data node → Manager | Heartbeats every 5 s | Tiny (< 1 KB each) |

**Rule of thumb**: for an X MB/s sustained user workload at RF=3, expect ≈ **3 × X MB/s** on the data-node-side internal network (each write fans out to 3 replicas). Plan a separate internal VLAN at 10 GbE for data nodes if you have > 4 nodes or > 500 MB/s aggregate throughput.

---

## Recommendations by role mix

### One-host all-in-one (mgr + data + client)

```
CPU:  4 cores  (1 mgr + 1 data + 4 fuse minimum, but the 4 fuse threads
                are mostly idle and share with the others; 4 is the floor)
RAM:  8 GB     (1 GB mgr + 4 GB data ARC + 1.5 GB fuse + 1.5 GB headroom)
Disk: 50 GB OS + your storage capacity for chunks
Net:  1 Gbit/s
```

This is the right shape for a home-lab / single-machine demo. Run **three of these** for an HA cluster — every host has every role.

### Service tier (3 hosts), each mgr + data

```
CPU:  4 cores
RAM:  6 GB     (1 GB mgr + 4 GB data ARC + 1 GB headroom)
Disk: 50 GB OS + capacity disks for tiers
Net:  10 Gbit/s preferred (replication + many client streams)
```

Run these as the cluster's storage tier. Mount FUSE on separate application/client hosts.

### Application tier (FUSE-only client hosts)

```
CPU:  4 cores  (Tokio worker_threads=4; the minimum that avoids F-026 stall)
RAM:  2 GB     (1 GB FUSE process + 1 GB headroom for kernel page cache)
Disk: 20 GB OS
Net:  1–10 Gbit/s depending on workload
```

Scale horizontally — one host per application.

### Control-plane-only tier (managers only, no chunks)

```
CPU:  2 cores
RAM:  4 GB     (room for FSM growth + RocksDB cache)
Disk: 50 GB SSD  (Raft log + snapshots benefit from low latency)
Net:  1 Gbit/s
```

Use this shape when you have > 10 data nodes and want the control plane isolated from chunk I/O.

### Pure data nodes (storage tier, no manager, no client)

```
CPU:  2–4 cores
RAM:  4 GB     (mostly ARC cache)
Disk: as much as you want, per tier
Net:  10 Gbit/s (chunk replication is bandwidth-hungry)
```

### Manager + client (uncommon — admin workstation)

```
CPU:  4 cores  (fuse minimum dominates)
RAM:  4 GB     (1 GB mgr + 1.5 GB fuse + 1.5 GB headroom)
Disk: 50 GB
Net:  1 Gbit/s
```

This is useful when an operator workstation wants both the WebUI/API service and a local POSIX mount but isn't pulling its weight as a storage node.

---

## Quick sizing checklist

Before deploying ask yourself:

1. **How many files do I expect?** Multiplies into FSM RAM on every manager.
2. **What's the average file size? × replication factor = raw bytes** → split across data nodes.
3. **What's the peak concurrent throughput across all clients?** Multiplies into data-node CPU + NIC.
4. **How many simultaneous open files per FUSE client?** Multiplies into client RAM for write buffers.
5. **Do I want the control plane isolated?** Yes if > 10 data nodes; otherwise mgr+data co-located is fine.
6. **Do I need the WebUI?** It's free — already served by each manager on port 9402.

The Ansible inventory at `tests/ansible/inventory/group_vars/managers.yml` documents every tuneable (`dtfs_cache_size_mb`, `dtfs_raft_snapshot_threshold`, `dtfs_chunk_size_mb`, etc.); change those to match your sizing target, not the binary itself.
