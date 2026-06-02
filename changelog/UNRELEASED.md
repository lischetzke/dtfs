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
- **Bug BB — per-tier disk usage is no longer fabricated by apportionment.**
  `/api/v1/stats` `tier_breakdown.used_bytes` now sums each disk's real
  reported `used_bytes` per tier. The previous logic decided per-disk
  whether to use the real value or apportion the node's `used_capacity` by
  capacity share, so a genuinely-empty tier disk on an otherwise-used node
  was assigned a fabricated share (originally surfacing as equal-size warm +
  cold disks reporting identical-to-the-byte usage). Apportionment now
  applies only to a legacy node that ships no per-disk usage at all.
- **Bug BP-1 — manager raft storage no longer grows unbounded.** RocksDB
  info-LOG (`LOG.old.*`) and MANIFEST were uncapped and grew to ~6.8 GB +
  ~207 MB, filling a 16 GB manager partition and taking the cluster to the
  edge of quorum loss. The log store now bounds `keep_log_file_num` (4) and
  `max_manifest_file_size` (64 MiB), and openraft retains far fewer
  post-snapshot log entries (`max_in_snapshot_log_to_keep`), so the on-disk
  `.sst` set stays small. New `[raft]` config keys
  (`rocksdb_keep_log_file_num`, `rocksdb_max_manifest_bytes`,
  `max_in_snapshot_log_to_keep`) with bounded defaults.
- **Bug BP-2 — snapshot writes are crash/ENOSPC-safe.** The state-machine
  snapshot is written to `snapshot.bin.tmp`, fsynced, atomically renamed,
  and the directory fsynced, so a disk-full / crash mid-write can never
  leave a truncated `snapshot.bin`. On startup a corrupt/0-byte snapshot is
  discarded (and removed) instead of wedging the manager on `Read Snapshot
  EOF`; the node catches up via raft log replay.
- **Bug BP-3 — stale nodes are no longer reported Healthy.** The node
  health shown by `/api/v1/nodes` and counted in
  `/api/v1/cluster/status` is now projected against a liveness window
  (`node_liveness_secs`, default 30 s): a node whose `last_seen_ms` is
  stale is reported `Suspect` (then `Down`), even while raft writes are
  stalled — a read-path projection that needs no committing leader.
- **Bug BP-4 — realized data loss is surfaced.** `/api/v1/cluster/status`
  now reports `files_missing_chunks` and degrades its top-line `status`
  when files exist with zero chunk bindings, instead of a bare `ok`. The
  GC orphan-reap guard is made explicit: a chunk is deleted only when no
  FileMetadata references its hash (defence-in-depth against the
  disk-UUID-rotation data-loss vector closed by BN-1).
- **Bug BP-5 — phantom tape replication is normalized away.** Saving a
  volume on a cluster with no tape-tier disks drops a `replication.tape`
  target to 0 (the non-fatal sibling of BM-3's mandatory-tape reject), so
  the scheduler is not handed an unsatisfiable target.
- **Bug BJ — re-registering an unchanged node no longer spams the audit
  log.** `RegisterNode` previously wrote an audit row on every apply, so
  a node re-registering (deterministic node id reused) added a noise row
  each time and evicted genuine events against the audit-log cap. The
  state machine now audits a registration only when the node is new or
  its identity/topology fields (role, hostname, grpc_addr, location,
  disks, version, build_date, client_ip) materially changed; volatile
  fields (last_seen / usage counters) are still refreshed silently.
- **Bug BH — anchor-lag repair now converges on no-spare-node
  multi-tier clusters.** When a volume keeps a warm cache replica on
  every data node plus a mandatory cold anchor, the cold-repair target
  already held the chunk on its warm disk. The repair completion path
  deduped recorded replicas by node id alone, so the new cold entry was
  never persisted: anchor-lag repair re-scheduled the same chunks
  forever and `flush_cache` refused every chunk with
  `reason=mandatory_strand`. The guard now keys on `(node_id, disk_id)`,
  so a second-tier (cold) replica is recorded on a node that already
  holds the warm copy; the repair queue drains and flush stops being
  refused once the anchor exists. Production-shaped clusters (cold
  anchor on a node that holds no warm cache) were unaffected.
- **Bug BO-1 — anchor copies succeed on single-DC / single-node-per-tier
  topologies.** A same-node warm→cold anchor copy no longer fails with "no
  healthy source replica": the copy dispatcher only excludes the target
  node when it differs from the source, so a node that holds the chunk on
  its warm disk can serve its own cold anchor copy.
- **Bug BO-2 — failed anchor-lag repairs no longer spin a tight re-queue
  loop.** A task that exhausts its retry attempts now stays in the
  scheduler's de-dup set for its retention window, so the same chunk is
  not re-queued on every 30 s tick. This eliminates the audit-log flooding
  (previously ~100% anchor.lag_repair + migration.failed).
- **Bug BO-3 — new chunk placement spreads across data nodes.** The
  placement ring key now mixes in the file path, so chunk index 0 of every
  file no longer collides onto a single node. Single-chunk files (the
  common case) now distribute across data nodes instead of saturating one.
- **Bug BO-4 — verify-on-read mismatches surface in the integrity feed.**
  A `read.integrity_fault` event (node + chunk + expected/actual hashes) is
  now emitted to `/api/v1/integrity/faults` when a data node refuses a
  corrupt chunk on read, distinct from the subsequent node-quarantine
  event (completes BC-12e).
- **Bug BN-1 — disk UUIDs are now stable across data-node restarts.**
  Each disk's UUID is persisted to `<mount_path>/.dtfs-disk-uuid` on first
  discovery and read on every later boot; previously a fresh random UUID
  was minted each boot, defeating UUID-based disk identity. Operational
  note: when wiping/re-provisioning a disk under the same mount path,
  delete `.dtfs-disk-uuid` so the new disk gets a fresh UUID.
- **Bug BN-2 — heartbeat telemetry no longer spams the audit log.** Node
  usage heartbeats (`UpdateNodeUsage`, every 5 s) no longer write an audit
  row, and node-status updates (`UpdateNodeStatus`) audit only a genuine
  health transition (e.g. Healthy→Suspect), not repeated same-state
  applications. This stops real integrity events being evicted against the
  audit-log cap.
- **Bug BM-3 — unsatisfiable mandatory tape constraint rejected at volume
  save.** `POST /api/v1/volumes/{id}/set-mandatory` now returns 400 when
  `mandatory_replicas.tape > 0` but no tape-tier disk is registered in the
  cluster. Previously the API accepted it and the under-replication
  scheduler queued a replication task that could never succeed for every
  chunk on the volume.
- **Bug BL — non-leader managers now forward heartbeat Raft writes to the
  leader.** A data node whose heartbeat landed on a follower had its Raft
  writes (node usage / liveness, per-disk usage, auto-quarantine, and
  corrupt-replica removal) silently dropped — openraft returned
  `ForwardToLeader` and the handler discarded it. Symptoms: `last_seen_ms`
  frozen for hours, integrity faults never recorded in the audit/integrity
  feed, and a corrupt self-quarantined node re-enabled for writes within
  one heartbeat. Followers now proxy these writes to the current leader via
  a new internal `ForwardClientWrite` RPC, so liveness and integrity state
  stay current regardless of which manager a node heartbeats.
- **Bug BL — self-quarantine no longer falsely cleared.** A data node that
  reports an integrity fault in a heartbeat is never told it is clear in
  the same cycle, so it cannot re-enable `put_chunk` on a chunk it just
  found corrupt before the quarantine commits.
- **Bug BK — WebUI now lists quick and deep scrub results separately per
  node.** Scrub results were keyed by node id alone in Raft state, so a
  deep scrub overwrote the quick result (and vice versa) and the
  dashboard "Last Scrub" card showed only the single most-recent level.
  Results are now retained per `(node, level)` (the legacy per-node map
  is kept one release for snapshot back-compat and migrated on load),
  `GET /api/v1/stats` returns one entry per `(node, level)`, and the
  WebUI renders one card per node and level.
- **Bug BQ-1 — idle FUSE clients stay reported online.** A mounted but
  idle client registered once (Bug AA) and never refreshed its
  `last_seen_ms`, so once BP-3's liveness projection shipped, an alive
  read-only client was shown `Down` after `node_liveness_secs`. The FUSE
  client now runs a periodic keepalive (re-`RegisterNode`, role=client) on
  a cadence shorter than the liveness window; the re-register is
  audit-idempotent (Bug BJ) so it adds no audit noise. New
  `[client] heartbeat_interval_secs` config key (default 10 s; `0`
  disables for legacy register-once behaviour).
- **Bug BQ-2 — FUSE systemd unit uses the correct `fusermount3` path.**
  The `dtfs-fuse@.service` `ExecStartPre`/`ExecStop` called
  `/bin/fusermount3`, which is absent on client images without the
  `/bin -> /usr/bin` usr-merge symlink (`status=203/EXEC`), leaving the
  mount half-dead on stop/restart. The unit (and the ansible template)
  now reference `/usr/bin/fusermount3`.
