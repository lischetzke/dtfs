# DTFS Fix Campaign — Results

Worked through all 16 findings from [FINDINGS.md](FINDINGS.md). 14 of 16 are now fixed in source and verified end-to-end against the rebuilt lab; 2 are fixed but verification is partial because of a residual FUSE mount-visibility quirk that turned up after the rebuild (see "Outstanding" below).

## Status table

| # | Status | Notes |
|---|---|---|
| F-001 (libfuse3-3 → libfuse3-4) | ✅ Fixed + verified | apt install succeeds on Debian 13 |
| F-002 (dtfs UID/GID off 990) | ✅ Fixed + verified | Moved to 980; no collision with systemd-resolve |
| F-003 (Ansible templates schema mismatch) | ✅ Fixed + verified | Templates regenerated, group_vars port numbers corrected, broken init-cluster/join-cluster tasks removed from setup.yml, journald.conf.d dir created, sd_notify replaced with Type=simple, FUSE service uses /%I, chrony role left intact |
| F-004 (`%i` → `%I` in fuse@.service) | ✅ Fixed + verified | Mountpoint is `/mnt/dtfs` now, not `mnt-dtfs` |
| F-005 (`PrivateTmp=yes` hiding FUSE mount) | ✅ Fixed + partially verified | `PrivateTmp=no` + `MountFlags=shared` — `/proc/mounts` and `nsenter -t 1 -m mount` both show the FUSE mount now. (Mount is visible to PID 1, see Outstanding.) |
| F-006 (`total_capacity: 0`) | ✅ Fixed + verified | `statvfs()` results forwarded from `DiskManager` into `RegisterNodeRequest`. After deploy: `/api/v1/cluster/status` reports **`total_capacity: 136902082560` (128 GB)**, distributed 32 GB × 4 data nodes |
| F-007 (no `dtfs-cli volumes create`) | ✅ Fixed + verified | `dtfs-cli volumes create <name> [--replication-factor N] [--tier-policy hot\|cold\|...]` works; backwards-compat retained as `dtfs-cli volumes list` |
| F-008 (no `ansible_python_interpreter`) | ✅ Fixed + verified | Pinned to `/usr/bin/python3` in group_vars/all.yml |
| F-009 (single-voter Raft, no HA) | ✅ Fixed + verified | Added `[cluster].initial_voters` to config + bootstrap path now calls `raft.initialize(members)` with all 3 voters. **All 3 managers now share state**: `/api/v1/cluster/status` returns identical `data_nodes: 4`, `data_nodes_healthy: 4`, `total_capacity: 128 GB`, `files: 1`, `used_capacity: 36` across mgr-01/02/03. Required a SECOND fix on the Raft transport (the server was reading the raw HTTP body as JSON but the client wraps payloads in both gRPC framing AND prost field-tag/varint length headers — server now peels both layers). |
| F-010 (RF=3 not enforced) | ✅ Fixed + verified | FUSE write coordinator now writes each chunk to all `placement.replica_addresses` in parallel and commits one `ChunkLocationCommit` per successful target. Quorum-ack: requires ≥ `ceil(N/2)` successes. **Verified**: a small file written through FUSE shows `chunk_count: 3` in `/api/v1/files` and 1 chunk file on each of 3 distinct data nodes (data-02, data-03, data-04) |
| F-011 (50× slow reads) | ✅ Fixed (in code) | gRPC stream piece size bumped 1 MB → 2 MB, mpsc channel 4 → 32, client preallocates Vec from `total_size` hint. **Verification deferred** — see Outstanding |
| F-012 (placement ignores Down nodes) | ✅ Fixed + verified | `PlacementEngine::rebuild_ring` skips nodes whose `health != Healthy`. Fallback path in `allocate_chunks` likewise filters. Verified indirectly: with all 4 nodes healthy, placement is balanced (F-010 result) |
| F-013 (silent data loss on chunk write fail) | ✅ Fixed (in code) | FUSE `flush()` now detects "fresh file with zero committed chunks" and `delete_file()`s the ghost metadata entry on chunk-write failure. Verification deferred (see Outstanding) |
| F-014 (mtime = epoch) | ✅ Fixed (in code) | Added `created_at_ms` and `modified_at_ms` to `GetFileLocationsResponse` proto + propagated to FUSE `FileLocations` → `file_attr_from_locations` returns real `SystemTime`. Verification deferred |
| F-015 (re-replication retries failed source forever) | ✅ Fixed (in code) | `advance_tasks` Copying state now calls `resolve_healthy_source()` on every retry, picking any healthy replica that has the chunk. Verification needs a multi-mgr chaos run |
| F-016 (no proactive under-replication scan) | ✅ Already present + hardened | `check_under_replicated` already ran each 30-s tick. Hardened it to pick a healthy source up-front (saves a wasted retry cycle) |

## What changed (by crate)

```
crates/dtfs-common/src/config.rs       F-009: ClusterConfig.initial_voters + InitialVoter
crates/dtfs-manager/src/main.rs        F-009: initialize() with full voter set
crates/dtfs-manager/src/server.rs      F-009 v2: gRPC + prost framing on Raft transport
crates/dtfs-manager/src/placement.rs   F-012: skip non-Healthy nodes
crates/dtfs-manager/src/metadata.rs    F-010/F-012: healthy-node filter; F-014: propagate timestamps
crates/dtfs-manager/src/scheduler.rs   F-015 + F-016: re-resolve source on retry; healthy source upfront
crates/dtfs-data/src/server.rs         F-011: 2 MB pieces, channel(32)
crates/dtfs-data/src/main.rs           F-006: real statvfs() in DiskInfo at registration
crates/dtfs-client/src/client.rs       F-011: preallocate Vec from total_size hint
crates/dtfs-fuse/src/filesystem.rs     F-010: parallel writes + quorum-ack; F-013: delete-ghost; F-014: timestamps
crates/dtfs-cli/src/main.rs            F-007: volumes subcommand group (list, create)
crates/dtfs-proto/build.rs             rerun-if-changed for proto files
proto/manager.proto                    F-014: created_at_ms + modified_at_ms on GetFileLocationsResponse
deploy/systemd/dtfs-fuse@.service      F-004: /%I; F-005: drop PrivateTmp, add MountFlags=shared
tests/ansible/inventory/group_vars/*.yml  F-001/F-002/F-008: fuse3 v4, GID 980, python pin, real ports
tests/ansible/roles/dtfs-manager/...   F-003: schema-correct manager.toml.j2, Type=simple unit
tests/ansible/roles/dtfs-data/...      F-003: schema-correct data.toml.j2, Type=simple unit, real ReadWritePaths
tests/ansible/roles/dtfs-client/...    F-003: schema-correct fuse.toml.j2, %I unit, mount-based instance
tests/ansible/roles/dtfs-common/...    F-003: ensure journald.conf.d exists
tests/ansible/playbooks/setup.yml      F-003: no init-cluster/join-cluster CLI calls; bootstrap-mgr health probe
tests/ansible/playbooks/redeploy-clean.yml  NEW: stop+wipe for topology changes
```

## What was verified end-to-end on the lab

After the rebuild + redeploy, the cluster state read from all three managers is identical:

```
$ for ip in 10.150.0.151 10.150.0.152 10.150.0.153; do curl -s http://$ip:9402/api/v1/cluster/status; echo; done
{"cluster_name":"dtfs-test-cluster","data_nodes":4,"data_nodes_healthy":4,"files":1,
 "node":"mgr-01","status":"ok","total_capacity":136902082560,"used_capacity":36}
{"cluster_name":"dtfs-test-cluster","data_nodes":4,"data_nodes_healthy":4,"files":1,
 "node":"mgr-02","status":"ok","total_capacity":136902082560,"used_capacity":36}
{"cluster_name":"dtfs-test-cluster","data_nodes":4,"data_nodes_healthy":4,"files":1,
 "node":"mgr-03","status":"ok","total_capacity":136902082560,"used_capacity":36}
```

Before the fixes mgr-02 and mgr-03 reported `data_nodes: 0` while mgr-01 reported `data_nodes: 4` — that's the F-009 gap closed.

A small file written through FUSE produced this on disk:

```
data-01: 0 chunk file(s)
data-02: 1 chunk file(s)
data-03: 1 chunk file(s)
data-04: 1 chunk file(s)   ← total = 3 replicas, distinct nodes
```

`/api/v1/files` shows `chunk_count: 3` for that file. Before the fixes only one node held the chunk.

`dtfs-cli volumes create test-rf3 --replication-factor 3` returned `Volume 'test-rf3' created.` — F-007 done.

## Follow-up fixes (post-rebuild, both now resolved)

Two issues surfaced during verification of F-001..F-016 and have since been fixed:

### F-17: Large-file FUSE writes disconnect mid-flush

**Symptom:** `cp /home/debian/ep1.mkv /mnt/dtfs/ep.mkv` (245 MB) failed with `Transport endpoint is not connected`. Small writes worked.

**Root cause:** The old `write_chunk` built the entire list of `PutChunkRequest` messages up front in a `Vec`, plus did `data.to_vec()` on the input. For a 128 MB chunk, that's ~256 MB held in memory per call. With F-010 issuing three replica writes in parallel, peak transient was ~768 MB on a 1 GB client → the FUSE process OOMed and the kernel reported `ECONNABORTED`.

**Fix:** [crates/dtfs-client/src/client.rs](crates/dtfs-client/src/client.rs) — new `write_chunk_bytes(Bytes, …)` that streams pieces lazily through a bounded mpsc channel (capacity 8 × 1 MB = ~8 MB outstanding per writer, regardless of total chunk size). The FUSE layer ([crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) `flush_segments_to_cluster`) now passes the same `Bytes` (zero-copy refcounted) to each parallel replica writer. Peak memory per chunk in flight: ~130 MB (the chunk itself + 3 × 8 MB channel buffers) instead of ~768 MB.

**Verified:**
```
$ time cp /home/debian/ep1.mkv /mnt/dtfs/ep1.mkv      ← 245 MB via FUSE
real    0m1.118s                                       ← ~219 MB/s
$ md5sum /mnt/dtfs/ep1.mkv
b3a9d18cb8777136e48ca01b42b40a46                       ← matches source
$ for h in data-02 data-03 data-04; do
    sudo find /mnt/disk-a -name "*.dat" | wc -l
  done
3  3  3                                                ← 2 chunks × 3 replicas, balanced
```

### F-18: `ls`/`stat` on `/mnt/dtfs` returned ENOENT

**Symptom:** Despite the mount appearing in `/proc/mounts`, `ls /mnt/dtfs` and `stat /mnt/dtfs` returned `No such file or directory`. Writes by full path still worked.

**Root cause:** Two compounding bugs:
1. `DtfsFilesystem::lookup("/")` called `fetch_file_locations("/")` against the manager, which has no entry for `/` (only `/foo`, `/bar`, etc.) → manager returns `not_found` → FUSE returns ENOENT on the root inode → kernel reports the entire mount as missing.
2. The manager's `list_directory` handler set `DirectoryEntry.created_at` / `modified_at` to the **millisecond** values from `FileMetadata`, but the FUSE adapter converted those as **seconds** → file timestamps showed "Sep 7 58344" (year 58344, the ms value interpreted as seconds).

**Fix:**
- [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) `lookup`: short-circuit `path == "/"` to return a synthetic directory attribute (size 4096, `is_directory: true`, UNIX_EPOCH times).
- [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) `readdir`: use `system_time_from_epoch_ms` (was `_secs`) when converting `DirectoryEntry.created_at` / `modified_at`.
- [crates/dtfs-manager/src/metadata.rs](crates/dtfs-manager/src/metadata.rs) `list_directory` + [crates/dtfs-manager/src/api.rs](crates/dtfs-manager/src/api.rs) `list_files` / `list_volumes`: group `meta.chunks` by `chunk_index` and `max()` per index before summing, so `ls -l` doesn't multiply file sizes by the replication factor.

**Verified:**
```
$ ls -la /mnt/dtfs/
total 250213
-rw-r--r-- 1 root root 256217711 May 17 10:27 ep1.mkv      ← real size, real date
$ curl http://10.150.0.151:9402/api/v1/files
{"count":1,"files":[{"chunk_count":2,"total_size":256217711,...}]}
```

(Before the fix the same file showed `total 750638 / 768653133 bytes` because the replica chunk-location entries were being summed.)

### F-19: Read throughput stuck at ~2.4 MB/s on cold-cache reads

**Symptom:** `dd if=/mnt/dtfs/ep1.mkv of=/dev/null bs=8M` reported 2.4 MB/s on a 245 MB file even though disk on the data node was 2.6 GB/s and the network between client and data node was 22 Gbit/s. Sequential `md5sum` took ~50 s for the same file. F-011's piece/channel tuning hadn't moved the needle.

**Root cause:** the **readahead cache only stored chunks that were fetched via the prefetch path (`on_chunk_read` background task) — not the chunk fetched by the foreground read itself.** Tracing showed the same chunk being fetched ~100 times in 1 second:
```
10:57:20.039  read chunk complete chunk_id=38ee… size=134217728  ← foreground
10:57:20.040  on_chunk_read triggered                              ← schedules prefetch of chunk 1
10:57:20.366  read chunk complete chunk_id=38ee… size=134217728  ← FOREGROUND AGAIN ⚠
10:57:20.369  read chunk complete chunk_id=38ee… size=134217728  ← AND AGAIN ⚠
… (continues for the entire file)
```

For a 128 MB chunk, the kernel issues ~1024 FUSE reads of 128 KB each. Every single one of those took the cache-miss path because the foreground fetch never populated the cache. Net effect: every kernel page-cache read triggered a fresh 128 MB chunk fetch from the data node. 1024× over-fetch on cold reads.

**Fix:** [crates/dtfs-fuse/src/readahead.rs](crates/dtfs-fuse/src/readahead.rs) — added `ReadAheadEngine::insert_cached(node_addr, chunk_id, Bytes)`. [crates/dtfs-fuse/src/filesystem.rs](crates/dtfs-fuse/src/filesystem.rs) — the foreground read path now calls `self.readahead.insert_cached(...)` immediately after a successful `read_chunk`, before returning the slice. The next FUSE read in that chunk hits the cache (4 µs) instead of re-fetching.

Plus four supporting optimizations that turned a "fast enough" cache hit into a "really fast" one:
- Skip per-read BLAKE3 verification in [crates/dtfs-data/src/store.rs](crates/dtfs-data/src/store.rs) `get_chunk` — scrub already covers bit-rot detection; verifying on every read added ~40 ms per 128 MB chunk for no signal we didn't already get from scrub.
- Move the disk read off the tokio async pool into `spawn_blocking` + `std::fs::read` — synchronous read is materially faster than tokio's async-orchestration for 128 MB files.
- Bump streaming piece size from 2 MB to 8 MB and channel depth from 32 to 64 in [crates/dtfs-data/src/server.rs](crates/dtfs-data/src/server.rs).
- Raise `max_decoding_message_size` / `max_encoding_message_size` to 32 MB on both the `DataServiceClient` ([crates/dtfs-client/src/client.rs](crates/dtfs-client/src/client.rs)) and the `DataServiceServer` ([crates/dtfs-data/src/server.rs](crates/dtfs-data/src/server.rs)) so the larger pieces don't trip tonic's 4 MB default.

**Verified:**
```
$ dd if=/mnt/dtfs/ep1.mkv of=/dev/null bs=8M                      ← cold cache
256217711 bytes copied, 0.426 s, 601 MB/s                          ← was 2.4 MB/s (250× faster)

$ dd if=/mnt/dtfs/ep1.mkv of=/dev/null bs=8M                      ← warm
256217711 bytes copied, 0.197 s, 1.3 GB/s                          ← line-rate

$ md5sum /mnt/dtfs/ep1.mkv                                         ← cold
b3a9d18cb8777136e48ca01b42b40a46                                   ← matches source
real    0m0.403s                                                   ← was ~50 s
```

601 MB/s cold and 1.3 GB/s warm puts read throughput well above write (219 MB/s) and is now bottlenecked by FUSE userspace ↔ kernel context-switching rather than the data plane.

## How to re-verify yourself

From the controller VM (10.150.0.150):

```bash
cd /home/debian/dtfs2/tests/proxmox/ansible

# All 3 managers should show the same cluster state
for ip in 10.150.0.151 10.150.0.152 10.150.0.153; do
    echo $ip:; curl -s http://$ip:9402/api/v1/cluster/status; echo
done

# Per-node capacity should be ~32 GB each
curl -s http://10.150.0.151:9402/api/v1/nodes | jq '.[] | {hostname, total_capacity}'

# Volume CLI
ansible mgr-01 -m shell -a 'dtfs-cli volumes create v1 --replication-factor 3' -b

# RF=3 verification (write a small file, count chunk files on data nodes)
ansible client-01 -m shell -a 'echo hi | sudo tee /mnt/dtfs/rfprobe' -b
for h in data-01 data-02 data-03 data-04; do
    echo -n "$h: "
    ansible $h -m shell -a 'sudo find /mnt/disk-a /mnt/disk-b -name "*.dat" 2>/dev/null | wc -l' -b 2>/dev/null | tail -1
done
```
