# Unreleased

## Breaking Changes
- License changed from **MIT** to **AGPL-3.0**. Downstream
  redistribution and network use are now subject to AGPL-3.0 terms —
  notably, any modified version reachable over a network must offer
  its source to users.

## Special Requirements for Updates
- Repository moved to <https://github.com/lischetzke/dtfs>. Existing
  clones must update their `origin` remote:
  `git remote set-url origin https://github.com/lischetzke/dtfs.git`.

## Known Issues
- _(none)_

## New Features
- **Bug AV — two-class tier lifecycle (Phase 4 close-out).** Volumes
  now declare separate `mandatory_replicas` (durability floor enforced
  by the under-replication scheduler) and `cache_replicas`
  (opportunistic, demand-driven, owned by the tiering engine). The
  `GET /api/v1/volumes` list endpoint embeds the full per-volume
  projection (AV-4-2); the WebUI Volumes page exposes inline policy
  cards + an edit modal posting to `/set-cache` (AV-4-3 / AV-4-4);
  the cache-tier redundancy-escalation pass that ramps toward `max`
  is now an opt-in runtime flag `tiering.enable_cache_redundancy_
  escalation` (AV-4-5, was compile-time const); and
  `DeleteChunkReplica` is WAL-recoverable across crashes (AV-4-7).
- The FUSE writer now propagates the manager-resolved tier through
  every `put_chunk` RPC (AV-4-1d); previously it hardcoded `"hot"`
  and silently landed chunks on the wrong tier whenever the
  data-node's `select_disk_for_tier` fell back to the
  "any-healthy-disk" path.

## Bug Fixes
- **AV-4-8 (1) — Bug Z bootstrap stops overwriting operator config.**
  Every manager start used to re-`UpsertVolume` the default volume,
  destroying any subsequent `set-cache` / `set-mandatory` the
  operator had applied. The bootstrap upsert now checks the state
  machine for the `default` volume and skips when present; fresh
  clusters still get the initial volume on the first boot.
- **AV-4-8 (2) — FUSE `ChunkLocationCommit.disk_id` propagation.**
  The FUSE writer hardcoded `disk_id: 0` regardless of which disk
  the data-node actually chose. The manager's tiering engine then
  resolved every placement against `disks[0]` and silently
  misclassified cold-tier chunks as warm-tier, killing the cache
  promotion lifecycle (no `cache.promote` audit, no warm replica
  even with 100+ reads in window). `PutChunkResponse` now carries
  `disk_id`; the data-node populates from `commit_chunk_from_tmp`
  (which now returns `(ChunkMeta, u32)`); FUSE propagates into
  `ChunkLocationCommit`.

## Bug Fixes
- _(none yet)_
