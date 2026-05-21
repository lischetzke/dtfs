# Soak Re-Launched After Bug Fixes — 2026-05-18T11:02Z

Previous run found a counter-rebuild bug + a per-disk-tier WebUI gap; both
fixed and verified live before this run starts. Cluster is clean: no
chunks from prior runs counted, volume `default` recreated with the
3-DC anchor + new RF, all 3 managers + 6 data nodes + 3 FUSE clients
running the latest binaries.

## What's actually running now

```
PID 259305  chain wrapper (4h → 48h)
PID 259308  bash run-soak.sh --duration 4h --results-dir /home/debian/soak-results/4h-20260518T1102
PID 259322  python3 collect-metrics.py
+ workload generators on each client
+ rsync preload (WSL PID 21846) — 157 GB corpus subset into client-c
```

## Results land at

- **4h**: `/home/debian/soak-results/4h-20260518T1102/REPORT.md` (~15:02 UTC)
- **48h**: `/home/debian/soak-results/48h-20260518T1102/REPORT.md` (~next-day 15:02 UTC)
- **Chain log**: `/tmp/soak-chain.log` on `dtfs-builder`

## Verified live before launch

```
manager_count: 3        ← was 0 before (managers don't self-register)
data_node_count: 6
client_count: 0         ← will jump to 3 once workloads acquire write leases
file_count: 0
tier_breakdown:
  hot   disks=2 0.00/536.61 GB     ← was missing entirely
  warm  disks=4 0.00/268.17 GB
  cold  disks=2 0.00/321.97 GB
  tape  disks=0 0.00/0.00   GB
```

WebUI Topology page now sorts `Region → DC → Rack → Host` ASC with
locale-aware + numeric comparison (so `data-a2` sorts before `data-a10`).

## Caching design as deployed for this soak

`client-b` (DC-B) is the only client with a disk-backed cache:

```
[cache]
local_cache_dir = "/var/cache/dtfs"
local_cache_max_gb = 100      # primary eviction trigger
max_age_days = 90              # safety upper bound; LRU rules first
prune_interval_secs = 3600
```

LRU rules:

- Scanner reads (< 1 MiB transferred per FUSE read call) do NOT refresh
  atime. Plex library walks no longer reset the LRU position. Verified
  by `scanner_read_does_not_update_atime`,
  `peek_never_updates_atime`, and
  `size_eviction_uses_viewer_atime_not_scanner` tests.
- Viewer reads (≥ 1 MiB) bump atime to now.
- Background age-pruner removes entries whose atime is older than 90
  days (cap can be set to None in config to disable entirely).
- Folder-sequential readahead in `client-b` mirrors prefetched
  next-episode chunks into the disk cache with fresh atime, so they
  sit at the top of the LRU and outlast scanner storms.

## Regression tests committed in this branch

```
dtfs-data store::tests::
  rescan_rebuilds_counters_after_restart      ← the lab regression
  rescan_handles_empty_disk
  rescan_handles_missing_chunks_dir
  rescan_idempotent
  rescan_ignores_tmp_and_wal_files
  rescan_replaces_stale_counter_not_accumulates

dtfs-fuse local_cache::tests::
  scanner_read_does_not_update_atime
  viewer_read_updates_atime
  peek_never_updates_atime
  size_eviction_uses_viewer_atime_not_scanner
  (+ put_get_roundtrip, size_eviction, age_eviction, reindex_from_disk)
```

`cargo test --workspace` green.

## Manual post-soak BLAKE3 verify

Source baseline lives on the Windows host (cluster doesn't have the
corpus). Run from WSL after both soaks complete:

```bash
ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.20.0.162 \
    'cd /mnt/dtfs && find . -type f \( -name "*.mkv" -o -name "*.mp4" \) -print0 \
     | xargs -0 -P 4 -I {} bash -c "h=\$(b3sum --no-names \"{}\") && printf \"%s\t%s\n\" \"\$h\" \"{}\""' \
    > /tmp/soak-baseline/corpus-fuse.blake3
diff <(sort /tmp/soak-baseline/corpus-source.blake3) \
     <(sort /tmp/soak-baseline/corpus-fuse.blake3)
# No output = PASS.
```
