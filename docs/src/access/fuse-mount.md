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

Writes are buffered per chunk, flushed in batches (`write.flush_batch_chunks`, default 4; up to `write.upload_concurrency`, default 2, chunks of a batch upload at once) and committed under a write lease, one commit per batch after all its chunks are stored. Details and failure handling in [Data Flow](../architecture/data-flow.md#write). Points that affect behaviour:

- **Chunk size.** `client.chunk_size_mb` (default 4) must equal the volume's chunk size. The client reads the file's real chunk size from the manager and refuses a write whose configured size does not match.
- **Read-modify-write.** Writing into the middle of an existing chunk reads that chunk once to seed the buffer; a seed failure is an error, not a silent partial overwrite.
- **First-write tier.** `[write] first_write_tier = "cold"` (default) sends no tier hint, so the manager uses the volume's own tier. `"nearest-warm"` asks for the warm tier when a healthy warm disk exists; it selects a tier, not a datacenter. The volume's anchor still applies to the same allocation. Whatever the tier, the manager adds targets on other maintained tiers until `min_replicas_before_ack` distinct nodes hold the chunk (e.g. one cold anchor copy plus one warm copy), so a write is never acknowledged on fewer durable copies than the floor unless the cluster cannot hold that many.
- **Anchor sync.** `[write] anchor_sync = true` (default) uploads every chunk to the anchor datacenter before the write is acknowledged, which caps a writer far from the anchor at that datacenter's link speed (LELI: about 40 Mbps, so 30 GB takes over an hour and a half). `false` makes the manager place each chunk on `min_replicas_before_ack` maintained nodes outside the anchor datacenter, whatever the write tier; anchor-datacenter nodes are used only when the others cannot reach the floor. The anchor copy, and any other missing mandatory copies, follow through the scheduler's repair. Until it lands the file is [anchor pending](../glossary.md) ("Catching up"): at least `min_replicas_before_ack` copies, none in the anchor datacenter. A client inside the anchor datacenter is unaffected.
- **`O_TRUNC`.** `open` and `create` with `O_TRUNC` on a writable descriptor truncate the file (the kernel hands `O_TRUNC` to `open`), so `> file` and `cp` over an existing file start from size 0.
- **`fallocate`.** Mode 0 only ever grows the file (a request inside the current size changes nothing); `FALLOC_FL_KEEP_SIZE` succeeds without growing; every other mode (punch hole, zero range, ...) returns `EOPNOTSUPP`. The size it compares against is the largest of a fresh (uncached) manager size, the write buffer end and every open handle's known size; if the fresh fetch fails the call returns `EIO` instead of guessing, so a stale view cannot shrink the file.
- **Holes and writes past EOF.** A write past EOF buffers zeros only in the two edge chunks of the gap: the old partial tail chunk (from the old end to the chunk end) and the write's own chunk (from the chunk start to the write offset). Chunks lying entirely inside the gap are neither buffered nor uploaded; a missing chunk below the end of file reads as zeros. With `write.async_commit` on or off the content is the same, there are just fewer uploads (earlier releases uploaded an all-zero chunk per gap chunk). When such a write skips whole chunks, the client first sets the file size on the manager (one `SetFileSize` to the current end), so the size stays right once the chunks above the gap commit; a write past EOF always ends with data, and a file ending in a gap after truncate-extend keeps the truncate size. A write into a hole chunk below the end of file (no chunk stored at that index) is seeded with zeros first, so the chunk can commit; a write past EOF seeds the partial tail chunk with its stored bytes. The zeros of a hole seed and the zeros between the old end and a write past it are unwritten: a chunk holding them is not uploaded by the normal batch flush until writes cover them (reads still return zeros there), while close, fsync (with `write.async_commit` off), truncate, the `async_commit_secs` timer and the dirty cap commit it with the zeros. A chunk committed that way remembers its unwritten ranges while the file stays open (dropped on close of the last writer, unlink, rename-over and once written; moved by rename, cut by truncate), so a later write into it waits for the rest instead of re-uploading the chunk per write.
- **Locations after a commit.** Each committed batch is merged into the open handles' chunk lists locally, so writing a large file does not re-fetch its whole location list from the manager per batch. When the local merge does not apply (no location list on a handle, a commit without a write lease, a list from an older manager without chunk indices, overlapping commits on the same file, or a handle that was closed or unlinked meanwhile), the handles' lists are marked stale instead.
- **Stale locations.** A stale handle re-fetches its location list from the manager on its next read or write. A fetch that overlapped a commit (or a truncate) on the same file is never used: the call waits for that commit to finish and fetches again, and fails with `EIO` if commits keep the file busy for longer than the write-lease length (120 s).
- **Dirty-data cap.** Dirty bytes per open file are capped (about twice the flush batch); a write that would exceed it drains first.
- **Rename** moves buffers, inode mappings and mode overrides with the path. Unaligned **truncate** rewrites the tail chunk; a bare path truncate uses a temporary internal writer.
- **Ghost unlink.** A file whose create never committed any chunk is removed on release; a file that has ever committed is never ghost-unlinked.

## Directories, inodes and metadata

- `opendir` takes one snapshot of the listing and serves `readdir` and `readdirplus` pages from it until `releasedir`.
- The client counts kernel lookups per inode and drops an inode only when the count reaches zero. fuse3 batch-forget carries no counts, so an inode that was not looked up for 5 s is dropped instead.
- Times: a new file reports its creation time; mtime then follows writes, truncates and renames. Explicit times from `utimensat` (`touch -d`, `touch -r`, `cp -p`, `rsync -t`) are ignored: there is no replicated command for them.
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
- The kernel NFS server may open, flush and release a file repeatedly during a long write. A second writable open of a file already open for write in the same dtfs-fuse process shares the first handle's write lease and its chunk-location list instead of failing with EBUSY: it sends no request to the managers (before, every reopen fetched the whole location list twice and took the lease through Raft, so knfsd's per-COMMIT open slowed a 65.7 GiB rsync from 29 to 20 MiB/s as the file grew). Only when that handle's list is stale does the open fetch it. The lease is released when the last of those handles closes. This has not been tested end to end.
- With a `sync` export, knfsd sends an NFS COMMIT (a FUSE `fsync`) about every second during a write. By default each one uploads the partial tail chunk, which the next write re-reads and re-uploads: about 4x upload traffic and a few MB/s. `write.async_commit = true` ([FUSE client config](../configuration/fuse-client.md#write-optional)) stops that: fsync no longer commits partial chunks, which go out when they fill, after `async_commit_secs`, or when knfsd closes the last writable file. Full chunks upload in the background, so neither a write nor an fsync waits for the data nodes (knfsd's single thread otherwise received nothing while each batch uploaded: 9-20 MiB/s on a NAS-backed data disk); the last writer's close still waits for them. knfsd opens, fsyncs and closes a separate file for every NFS COMMIT while the client's NFSv4 open stays open; with `async_commit` such a close commits nothing, only the last writer's close commits (otherwise each COMMIT re-uploaded every partially filled chunk, about 7x upload traffic). The cost is that NFS-acknowledged data can sit only in `dtfs-fuse` memory for that window and is lost if `dtfs-fuse` or the host crashes. Use it for media libraries only.
- NFS client and server settings for a fast single writer (measured on a real re-export, rsync of large media files):
  - Do not mount with the client `sync` option: every NFS WRITE then becomes a FUSE `fsync`, which uploads the partial tail chunk again (about 1 MB/s). Keep the export itself `sync`.
  - Avoid `nconnect` and run knfsd with one thread (`[nfsd]` `threads=1` in `/etc/nfs.conf` or a file in `/etc/nfs.conf.d/`). Parallel connections and threads deliver writes out of order; a write past the buffered end zero-fills the gap, and a chunk holding such unwritten zeros stays in memory until the late write fills it (or close, the async timer or the dirty cap commits it with the zeros). Heavy reordering therefore holds more chunks in client memory, up to the per-file dirty cap; a chunk the cap drain or timer had to commit early is re-read and uploaded once more when the late writes complete it.
- **Stop with files still open.** On `systemctl stop` (SIGTERM) `dtfs-fuse` unmounts (up to 15 s). If that fails or times out because files are still open, it then commits the buffered writes of every still-open writable file (up to 22 s in total) before releasing its write leases (up to 5 s) and exiting; anything it could not commit is logged at error with path, chunk indices and bytes, and is lost. With `write.async_commit = true` those are bytes fsync already acknowledged, so unexport first (below). The whole stop fits in 42 s, below the `TimeoutStopSec=45s` of units installed before 110 s (hosts updated only with `dtfs-cli update` keep their old unit); the current unit's `TimeoutStopSec=110s` is headroom. Do not override it below 45 s.
- **Unexport before stopping the client.** While a path under the mount is exported, knfsd holds the mount: `fusermount3 -u` fails with `EBUSY`, `dtfs-fuse` exits anyway (after committing what the open files buffered, above), and the dead mount (`Transport endpoint is not connected`) makes every later start fail. Unexport every export under the mount first (`exportfs -u <client>:<path>` per export, then `exportfs -f`), and re-export after the mount is up.
- **NFSv4 opens survive the unexport.** An NFSv4 client that holds a file open (an rsync in progress, with its open and write delegation) keeps knfsd's reference on the mount after `exportfs -u`, so the unmount still fails with `EBUSY`. Revoke that state with `echo /mnt/dtfs > /proc/fs/nfsd/unlock_filesystem` (kernel 6.9 or newer; older kernels release only NLM locks there) and retry the unmount for a few seconds while knfsd drops its cached file references. NFSv3 has no open state, so a v3-only export does not need this.

A drop-in for the `dtfs-fuse@` instance does this on every start and stop. The empty `ExecStartPre=` and `ExecStop=` lines clear the template's own `fusermount3 -u` so the unexport runs first. Adjust the client spec (as in `/etc/exports`) and the path, one `-u` line per export:

```ini
# /etc/systemd/system/dtfs-fuse@mnt-dtfs.service.d/nfs-export.conf
[Service]
ExecStartPre=
ExecStartPre=-/usr/sbin/exportfs -u *:/mnt/dtfs/media
ExecStartPre=-/usr/sbin/exportfs -f
ExecStartPre=-/bin/sh -c 'grep -q " /mnt/dtfs fuse" /proc/mounts && echo /mnt/dtfs > /proc/fs/nfsd/unlock_filesystem'
ExecStartPre=-/bin/sh -c 'for i in $$(seq 15); do grep -q " /mnt/dtfs fuse" /proc/mounts || exit 0; /usr/bin/fusermount3 -u /mnt/dtfs && exit 0; sleep 1; done; exit 1'
# Type=simple: wait for the mount before re-exporting.
ExecStartPost=/bin/sh -c 'for i in $$(seq 30); do mountpoint -q /mnt/dtfs && exec /usr/sbin/exportfs -r; sleep 1; done; exit 1'
ExecStop=
ExecStop=-/usr/sbin/exportfs -u *:/mnt/dtfs/media
ExecStop=-/usr/sbin/exportfs -f
ExecStop=-/bin/sh -c 'grep -q " /mnt/dtfs fuse" /proc/mounts && echo /mnt/dtfs > /proc/fs/nfsd/unlock_filesystem'
ExecStop=/bin/sh -c 'for i in $$(seq 15); do grep -q " /mnt/dtfs fuse" /proc/mounts || exit 0; /usr/bin/fusermount3 -u /mnt/dtfs && exit 0; sleep 1; done; exit 1'
```

`$$` is systemd's escape for a literal `$`. Run `systemctl daemon-reload` after adding it. This drop-in is not shipped or tested by the installer; the dev-cluster NFS check mounts with NFSv3 and does not exercise the NFSv4 revoke.
