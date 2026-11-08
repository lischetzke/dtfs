# FUSE Mount

`dtfs-fuse` mounts the filesystem as a local directory. Applications use ordinary POSIX calls; the client talks to managers for metadata and directly to data nodes for chunk bytes.

```text
Application --POSIX--> dtfs-fuse --metadata--> dtfs-manager
                          |
                          +--chunk bytes---> dtfs-data
```

## Mounting

```bash
dtfs-fuse --config /etc/dtfs/fuse.toml --mountpoint /mnt/dtfs
```

`--config` defaults to `/etc/dtfs/fuse.toml`; `--mountpoint` overrides `[mount].mountpoint`. There are no subcommands. Unmount with `fusermount3 -u /mnt/dtfs`, or stop the service. A bad config makes the process exit at startup (a restart loop under systemd), so check `journalctl -u 'dtfs-fuse@*'` after editing.

Minimal `fuse.toml`:

```toml
[client]
manager_addr  = "mgr01:9401"
manager_addrs = ["mgr02:9401", "mgr03:9401"]   # list every manager
shared_secret = "..."

[location]
region = "eu"
datacenter = "toro"
rack = "r1"
host = "toro-plex_cln"

[mount]
mountpoint = "/mnt/dtfs"
allow_other = true
```

`manager_addr` plus `manager_addrs` must include at least one Raft **voter**. Reads work against any manager, including learners, but client mutations are not forwarded by learners, so every write would fail. Full reference: [FUSE Client Configuration](../configuration/fuse-client.md).

The systemd template is `deploy/systemd/dtfs-fuse@.service` (instance name = mount path, for example `dtfs-fuse@mnt-dtfs`).

## Reads

See [Data Flow](../architecture/data-flow.md#read) for the full path. In short: location lookup (5 s cache), replica ranking, single-flighted chunk fetch with failover, server-side verification, client-side BLAKE3 check of the fetched bytes, then caching and read-ahead.

### Replica selection

Ranked in this order:

1. `read_location_priority` (top-level `fuse.toml` key, ordered list of datacenter or host names, case-insensitive): the first replica matching the earliest entry wins, even over a nearer one.
2. Physical distance from `[location]`: same host, same rack, same datacenter, same region, other region.
3. On equal distance only, prefer a faster tier (warm over the cold anchor HDD).

A fetch that fails (timeout `client.chunk_read_timeout_secs`, `data_loss`, checksum mismatch, transport error) moves to the next candidate. The read fails only when every candidate failed. Errors map to errno through one function: only a genuine gRPC `NotFound` becomes `ENOENT`; a transient failure is `EIO`.

## Writes

Writes are buffered per chunk, flushed in batches (`write.flush_batch_chunks`, default 4) and committed under a write lease. Details and failure handling in [Data Flow](../architecture/data-flow.md#write). Points that affect behaviour:

- **Chunk size.** `client.chunk_size_mb` (default 4) must equal the volume's chunk size. The client reads the file's real chunk size from the manager and refuses a write whose configured size does not match.
- **Read-modify-write.** Writing into the middle of an existing chunk reads that chunk once to seed the buffer; a seed failure is an error, not a silent partial overwrite.
- **First-write tier.** `[write] first_write_tier = "cold"` (default) sends no tier hint, so the manager uses the volume's own tier. `"nearest-warm"` asks for the warm tier when a healthy warm disk exists; it selects a tier, not a datacenter. The volume's anchor still applies to the same allocation. Whatever the tier, the manager adds targets on other maintained tiers until `min_replicas_before_ack` distinct nodes hold the chunk (e.g. one cold anchor copy plus one warm copy), so a write is never acknowledged on fewer durable copies than the floor unless the cluster cannot hold that many.
- **`O_TRUNC`.** `open` and `create` with `O_TRUNC` on a writable descriptor truncate the file (the kernel hands `O_TRUNC` to `open`), so `> file` and `cp` over an existing file start from size 0.
- **`fallocate`.** Mode 0 only ever grows the file (a request inside the current size changes nothing); `FALLOC_FL_KEEP_SIZE` succeeds without growing; every other mode (punch hole, zero range, ...) returns `EOPNOTSUPP`. The size it compares against is the largest of a fresh (uncached) manager size, the write buffer end and every open handle's known size; if the fresh fetch fails the call returns `EIO` instead of guessing, so a stale view cannot shrink the file.
- **Holes and writes past EOF.** A write into a hole chunk below the end of file is seeded with zeros first, so the chunk is complete and commits; a write past EOF seeds the partial tail chunk the same way.
- **Stale locations.** If the locations refresh after a commit fails, the open handles' location snapshots are invalidated and the next read or write re-fetches them from the manager instead of using the stale list.
- **Dirty-data cap.** Dirty bytes per open file are capped (about twice the flush batch); a write that would exceed it drains first.
- **Rename** moves buffers, inode mappings and mode overrides with the path. Unaligned **truncate** rewrites the tail chunk; a bare path truncate uses a temporary internal writer.
- **Ghost unlink.** A file whose create never committed any chunk is removed on release; a file that has ever committed is never ghost-unlinked.

## Directories, inodes and metadata

- `opendir` takes one snapshot of the listing and serves `readdir` and `readdirplus` pages from it until `releasedir`.
- The client counts kernel lookups per inode and drops an inode only when the count reaches zero. fuse3 batch-forget carries no counts, so an inode that was not looked up for 5 s is dropped instead.
- File attributes: symlinks are reported as symlinks (`is_symlink` from the manager); file mode overrides are kept client-side and re-keyed on rename. The metadata cache is bounded (4096 entries) and swept on its TTL.

## Locks

`fcntl` byte-range locks are tracked inside the client process only. Ranges from the kernel are inclusive; unlock carves the owner's ranges instead of dropping a whole overlapping lock, and `flush` drops the closing owner's locks. The manager lock RPCs exist but are not used, so locks do **not** coordinate between two mounts.

## Memory

An optional `[memory]` block caps the client. At Warn the read-ahead cache budget halves; at Shed unconsumed prefetch is dropped and prefetching stops. See [Architecture Overview](../architecture/overview.md#memory-model).

## Monitoring

The client has no Prometheus endpoint. It registers itself with the managers (appears in the node table when `client.shared_secret` is set) and logs operations at `RUST_LOG` level. Cluster-side views are in [Monitoring](../operations/monitoring.md).

## NFS re-export

A DTFS mount can be re-exported with the Linux kernel NFS server (`exportfs`, use an explicit `fsid=`). Create, read, write, rename and delete work. Known limits:

- After a `dtfs-fuse` restart, NFS clients get `ESTALE` and must remount (inode numbers are not stable across restarts).
- An NFSv3 EXCLUSIVE create that is retransmitted returns `EEXIST` (create verifiers are not stored).
- Ownership is the daemon's uid/gid; `chown` is ignored.
- The kernel NFS server may open, flush and release a file repeatedly during a long write; this has not been tested end to end.
