# Soak Re-Launched (Post-Bug-Fix) — 2026-05-18T10:36Z

## What you reported and what I fixed

**Bug.** Manager's `used_capacity` accounting was wrong for 5/6 data nodes
after the `mount-data-disks.yml` restart: df showed real data on disk
but the manager reported 0. Root cause was `ChunkStore::bytes_stored`
(an `AtomicU64` in dtfs-data) starting at 0 on every process start and
having no path to rebuild from disk. Restarts dropped the running
totals; the next heartbeat then under-reported to the manager.

**Fix.** Two changes in `dtfs-data`:

1. **`ChunkStore::rescan_existing_chunks()`** (store.rs) — walks every
   managed disk's `chunks/aa/bb/*.dat` tree at startup and rebuilds
   `chunk_count` + `bytes_stored` from the on-disk reality. Wired into
   the `dtfs-data` startup path right after disk init, before the
   heartbeat task fires.

2. **`select_disk_for_tier` fallback** (disk.rs) — when a chunk targeted
   at this node specifies a tier this node doesn't have (FUSE
   hardcodes `tier="hot"` for every replica write; DC-B/C nodes are
   warm-only), the data node now lands the chunk on whichever healthy
   disk has the most free space instead of failing with
   `DiskNotFound`. Without this, every "hot" replica targeted at a
   warm-only node failed silently.

Verified live: wrote 100 MB through FUSE → both DC-A nodes reported
used=104.9 MB. Restarted data-a1 → counter persisted (log: "ChunkStore
startup rescan complete chunks=1 bytes=104857600"). Before the fix the
same cycle dropped a1 back to used=0.

Committed as `43efdb4`.

## What I also did

- **Bandwidth shaping reverted.** The tc/HTB shaping role's exempt-port
  list missed 9401 (manager gRPC) + 9500 (data gRPC) — Raft couldn't
  hold quorum under the DC-A 40 Mbps / 25ms-jitter cap. Reverted for
  this soak; documented as a follow-up to add those ports to the
  exempt list and tune Raft election timeouts.
- **Full cluster reset.** Wiped all manager Raft state, all data-node
  chunk dirs, all FUSE caches. Re-bootstrapped from clean state: 6
  data nodes registered + healthy, 1.13 TB cluster capacity, volume
  `default` recreated with `anchor_datacenter=dc-a`, rf hot=2 warm=1
  cold=1.
- **Rolled new dtfs-data binary** (compiled with the fixes) to all 6
  data nodes.

## What's running now (chained, detached)

```
PID 255916  bash chain wrapper (4h soak → 48h soak)
PID 255919  bash run-soak.sh --duration 4h --results-dir /home/debian/soak-results/4h-20260518T1036
PID 255933  python3 collect-metrics.py (4h soak's collector)
+ scheduled scenario subshells
+ workload generators on each client (playback-client-{a,b,c}.sh)
```

Plus, on the Windows host inside WSL:
- **`rsync` preload** (PID 9488 in WSL) — ingesting 157 GB corpus subset
  through client-c's FUSE mount. Survives session end.
- **BLAKE3 baseline computation** (PID 9508 in WSL) over the source
  corpus → `/tmp/soak-baseline/corpus-source.blake3`.

## Where results land

```
4h soak:  /home/debian/soak-results/4h-20260518T1036/REPORT.md     (≈14:36 UTC)
48h soak: /home/debian/soak-results/48h-20260518T1036/REPORT.md    (≈next-day 14:36 UTC)
Chain log: /tmp/soak-chain.log on dtfs-builder
```

Both soaks share workload generators + scheduled scenarios. The chain
wrapper runs them sequentially: 4h finishes → renders its REPORT.md →
48h starts immediately afterwards.

## Manual post-soak BLAKE3 verify (after both soaks complete)

The baseline lives on the Windows host (the cluster doesn't have the
corpus). Run from WSL:

```bash
# 1. Dump BLAKE3 of files-as-seen-from-FUSE
ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.162 \
    'cd /mnt/dtfs && find . -type f \( -name "*.mkv" -o -name "*.mp4" \) -print0 \
     | xargs -0 -P 4 -I {} bash -c "h=\$(b3sum --no-names \"{}\") && printf \"%s\\t%s\\n\" \"\$h\" \"{}\""' \
    > /tmp/soak-baseline/corpus-fuse.blake3

# 2. Compare line-by-line.
diff <(sort /tmp/soak-baseline/corpus-source.blake3) \
     <(sort /tmp/soak-baseline/corpus-fuse.blake3)
# No output = PASS. Any line = data integrity violation.
```

## Confirmed working in the fixed cluster (smoke before soak launch)

- `dtfs_cluster_capacity_bytes` = 1.13 TB ✓
- 6 data nodes healthy, 0 down ✓
- Volume `default` accepted `anchor_datacenter=dc-a` + rf 2/1/1 ✓
- 100 MB sentinel write → both DC-A nodes report 104.9 MB on first heartbeat ✓
- Counter persisted across data-a1 restart ✓ (rescan log + manager API)
- FUSE mounts active on all 3 clients ✓

## Known limitations carried into this soak

- **Bandwidth shaping disabled** for this run. Soak validates correctness +
  P0/P1 fixes but not DC-class WAN behaviour. Follow-up: tune exempt
  ports + Raft timeouts before next run.
- **Initial placement is hot-tier only.** The manager's `allocate_chunks`
  places `rf hot=2` replicas on initial write; warm/cold tier replicas
  are filled by the tiering engine over time as data ages. This is by
  design — the cluster won't show "warm replicas" immediately after
  writes; expect them to appear during the soak as tiering fires.
- **DELETE on /api/v1/files{path}** returns 404; deletes must go through
  FUSE unlink. Acceptable but noted.

## If something breaks while detached

```bash
# Kill everything related
ssh debian@10.150.0.150 'pkill -f run-soak; pkill -f collect-metrics'
for ip in 10.150.0.160 10.150.0.161 10.150.0.162; do
  ssh debian@$ip 'pkill -9 -f playback; pkill -9 -f pv'
done
# WSL: pkill -f "rsync.*dummy"

# Render whichever results exist
ssh debian@10.150.0.150 'python3 /home/debian/dtfs2-deploy/tests/proxmox/soak/render-report.py \
    --results-dir /home/debian/soak-results/4h-20260518T1036 \
    --output /home/debian/soak-results/4h-20260518T1036/REPORT.md'
```
