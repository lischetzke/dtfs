# System Requirements

Hardware sizing for a DTFS cluster depends on the **roles** each host runs and the **scale** of the namespace + access pattern. This page gives per-role baselines, scaling formulas you can plug your own numbers into, and concrete recommendations for the common deployment shapes.

DTFS runs as up to three independent processes on a host:

| Process | Role | systemd unit |
|---|---|---|
| `dtfs-manager` | Raft consensus, metadata, REST/gRPC API, **WebUI** | `dtfs-manager.service` |
| `dtfs-data` | Chunk storage on local disks, scrub | `dtfs-data.service` |
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
| Data node | 1 | 1 GB | 10 GB | per-tier disks, see formula | 1 Gbit/s |
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

Manager CPU is mostly idle. Peaks are: snapshot serialise + write to disk (≈ every `snapshot_threshold` log entries, default 10 000), and the periodic scheduler/tiering tick (default every 30 s / 3600 s). One core is sufficient for clusters with < 5 M files; bump to 2 cores if you also enable the WebUI for many concurrent operators.

The chunk size is a volume setting (`defaults.chunk_size_mb`, default **4 MB**) and
multiplies `chunk_count`; the FUSE clients must use the same value. Pick it before
loading data.

**Worked example** — the Plexmedia library, about 50 TB of mostly 1 to 20 GB media
files (taking 40 000 files), 4 MB chunks, mostly one replica on the anchor tier plus
one cache replica:

```
chunk_count     ≈ 50 TB / 4 MB = 12_500_000
chunk_locations ≈ 12_500_000 × 2 = 25_000_000
FSM RAM ≈ 0.2 KB × 40k (files)        = ~ 8 MB
       + 0.12 KB × 25M (chunk locs)   = ~ 3.0 GB
       + overhead                     = ~ 0.2 GB
       ≈ 3.2 GB working set
```

With 128 MB chunks the same library has 32x fewer chunk locations (about 0.1 GB).
The per-entry byte figures are estimates; confirm with `GET /api/v1/memory` on a
loaded manager and size `[memory] hard_cap_mb` from the measurement.

### Data node

```
Data RAM ≈ 512 MB                            # process + connections
         + max_concurrent_chunk_writes × chunk_size   # in-flight puts (default 8 × 4 MB)
         + 64 bytes × chunks_on_this_node     # in-memory chunk index

Data disk (per tier) ≈ tier_capacity_target / data_node_count × replication_factor
```

The data node has no in-memory chunk cache: reads stream from disk and the `[cache]` block is parsed but inert. Read caching is done by the FUSE client (read-ahead buffer and an optional local disk cache). Bound the data node with `[memory] hard_cap_mb` and the systemd `MemoryMax` ([Filesystem Layout](../deployment/filesystem-layout.md#memory-limits-on-co-hosted-boxes)).

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
        + chunk_size × max_concurrent_open_writes   # write buffer (flush batch capped at 64 MB per open write)
        + readahead.cache_mb (default 64 MB)        # prefetch cache, halved at the warn rung
        # the optional local cache (cache.local_cache_dir) is on disk, not in RAM
```

For a media-player client doing one stream at a time, 1 GB RAM is plenty. For a backup-server client writing many large files in parallel, plan on `chunk_size × parallelism` for the write buffer — at a 128 MB chunk size and 8 parallel writers that's 1 GB just for buffering (with the default 4 MB chunks it is far less).

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
RAM:  8 GB     (1 GB mgr + 2 GB data + 1.5 GB fuse + 3.5 GB headroom for the OS and applications)
Disk: 50 GB OS + your storage capacity for chunks
Net:  1 Gbit/s
```

This is the right shape for a home-lab / single-machine demo. Run **three of these** for an HA cluster — every host has every role.

### Service tier (3 hosts), each mgr + data

```
CPU:  4 cores
RAM:  4 GB     (1 GB mgr + 2 GB data + 1 GB headroom)
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
RAM:  2 GB     (process, write concurrency, page cache)
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

Automation variables and the per-role memory budget for co-hosted boxes are in [Role Variables and Idempotency](../deployment/role-variables.md) and [Filesystem Layout and Units](../deployment/filesystem-layout.md); host prerequisites (OS, ports, disks) are in [Host Prerequisites](../deployment/host-prerequisites.md).
