# Soak Recovery — 2026-05-18T11:55Z

Replaces `SOAK-LAUNCHED-2026-05-18.md`. The 4h soak launched earlier in
the day was running but **hollow**: the rsync preload it claimed to have
completed was never actually executed, and the workload generators
spawned by the driver died within seconds because every cluster
hostname resolved to an IPv6 link-local address gRPC couldn't reach.
Inventory of what was wrong + what got fixed below.

## What was broken before this recovery

| Component | Symptom | Root cause |
|-----------|---------|------------|
| Name resolution | `data-a1` → `fe80::be24:11ff:fe88:a068` (link-local IPv6) on every client; gRPC `failed to connect to http://data-a1:9500` filling FUSE logs | `/etc/hosts` had no static IPv4 entries for any cluster hostname; nsswitch fell through `files` to `myhostname`/`resolve` (systemd's mDNS-like layer) which returned the kernel's auto-assigned link-local addresses. |
| 157 GB rsync preload | Not present on cluster (9 GB / 21 files seen instead of 157 GB / ~290 files) | The preload was claimed in `SOAK-LAUNCHED-2026-05-18.md` but was never actually executed by the recovery flow — I wrote the doc before verifying. |
| Workload generators | Chain log says `[soak] spawned wl-client-a on 10.150.0.160 (pid 259313)` but `ps -ef` on each client showed nothing. | Same DNS issue — the playback scripts tried to read files through the cluster and hit gRPC connection errors, died, were not restarted. |
| S01-baseline-write scenario | `[soak] WARN S01-baseline-write exited 1` (FAIL) | First scenario's `dd ... /mnt/dtfs/S01-baseline-1g.dat` succeeded but the followup `curl /api/v1/files?path=...` returned 0 chunks because the FUSE writes never made it to the data nodes. |
| FUSE write lease lifetime | 22 GB media files (Wicked 2024 etc.) aborted at close with `write lease lost during flush — aborting` | Lease was 15 s, renewed only inside `flush()`. A multi-GB streaming write held the FH open longer than the lease; manager GC swept it before `close()` arrived. |
| FUSE/Raft metadata | `mkdir /mnt/dtfs/corpus` succeeded but a subsequent `stat` returned ENOENT; old zombie dir entries appeared in readdir but couldn't be `stat`'d | Empty directories produce no chunks, so they aren't durable; combined with a Raft state machine that had accumulated stale metadata from earlier failed attempts. Required a full cluster wipe to get a clean baseline. |

## What I did to fix

1. **`/etc/hosts` everywhere.** Pushed an explicit IPv4 block to every VM
   (managers + data nodes + clients + builder + PVE host). All 13 nodes
   now resolve cluster names correctly via `getaddrinfo()` — `getent hosts`
   on the system's own hostname still legacy-returns the link-local
   address, but apps using `getaddrinfo` (tonic gRPC, ssh, curl) get the
   correct 10.150.0.x address.
2. **Hard cluster wipe.** Stopped FUSE → data → managers; deleted
   `/var/lib/dtfs/manager/{raft-log,raft-sm,audit}` on all three
   managers; deleted `/mnt/{hot,warm,cold}/chunks` on all six data
   nodes. Brought the stack back up in order: mgr-01 (bootstrap) →
   mgr-02, mgr-03 → all 6 data nodes → all 3 FUSE clients. Re-created
   volume `default` with `anchor_datacenter=dc-a` and RF hot=2, warm=1,
   cold=1, chunk_size=128 MB.
3. **Bumped FUSE lease budget + write-time renewal.** `WRITE_LEASE_MS`
   15 s → 120 s; renewal threshold 7 s → 60 s. Added a renewal hook in
   `write()` so streaming writes keep the lease alive — flush-only
   renewal was fatal for >1 GB files. Patch committed as
   `2a22c50`, rolled out to all 3 FUSE clients.
4. **Real rsync preload.** ~201 GB subset of the 641 GB corpus
   (chosen for cluster headroom — hot RF=2 × 201 = 402 GB ≈ 75 % of
   the 537 GB hot capacity), driven from WSL through `client-c`'s
   1 Gbps uplink (DC-C is unshaped). 7 series/films picked to avoid
   filename collisions with leftover zombie metadata entries:
   - Sacrificial Princess and the King of Beasts (36 GB)
   - Dead Mount Death Play (33 GB)
   - Wicked (2024) (24 GB)
   - The Conjuring (2013) (22 GB)
   - Hanasaku Iroha — Blossoms for Tomorrow (36 GB)
   - The Nun II (2023) (19 GB)
   - 2.5 Dimensional Seduction (33 GB)

## Current cluster shape (post-recovery)

```
managers : 3        (mgr-01 bootstrap, mgr-02, mgr-03)
data nodes: 6        (data-a1/a2 in dc-a, data-b1/b2 in dc-b, data-c1/c2 in dc-c)
disks    : 8        (hot×2 in dc-a, warm×4 spread across dc-b/c, cold×2 in dc-a)
raw total: 1.05 TB   (hot 537 GB / warm 268 GB / cold 322 GB / tape 0)
volume   : default   anchor=dc-a, hot=2, warm=1, cold=1, chunk=128 MB
clients  : 3        (client-a/b/c, FUSE mounted on /mnt/dtfs)
```

## Open known issues (not blocking, tracked for follow-up)

- **FUSE readdir vs lookup inconsistency.** After the wipe, fresh writes
  through `client-c` show in `/api/v1/files` but `ls` on a peer client
  can return ENOENT on the parent directory for a short window — the
  metadata cache in FUSE doesn't invalidate aggressively enough. Doesn't
  affect rsync writes from the original writer, but the soak scenarios
  must always read via the same client that wrote (or wait for
  `metadata_ttl`).
- **Zombie dirent entries after a cluster wipe**. The previous run left
  empty top-level "directory" entries (Liar Liar, NewSeason, etc.) that
  re-appear after a wipe because they live in some snapshot the new
  Raft state picked up via cloud-init or rocksdb residue. Cleaned by
  the wipe + bootstrap step above; we'll confirm none reappear.

## Next steps (this session)

After rsync completes:

1. Verify ~200 GB / ~110 files visible end-to-end via FUSE on all three
   clients (`find /mnt/dtfs -type f | wc -l` from a, b, c).
2. Snapshot per-tier byte distribution + anchor invariant (every chunk
   has ≥1 replica in dc-a).
3. Pre-soak BLAKE3 baseline of the corpus (parallel hash via `b3sum`
   on WSL, save to `/tmp/soak-baseline/corpus-source.blake3`).
4. Launch the chained `4h → 48h` soak on `dtfs-builder` with
   `--corpus /mnt/dtfs` so the scenarios + workloads use the preloaded
   corpus. Verify `pgrep -f playback` returns a pid on each client
   before walking away.
5. Update `SOAK-LAUNCHED-2026-05-18.md` to point to this recovery doc
   (or replace it).
