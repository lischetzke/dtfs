# Tiering

The tiering engine runs on the manager leader. It watches per-datacenter read counts reported by data nodes and decides when to add or drop **cache replicas** (opportunistic, evictable copies on faster disks near the readers). It never touches mandatory replicas and never violates the anchor; those belong to the repair scheduler (see [Replication](./replication.md)).

## Cache tier semantics

Cache replicas are created by the tiering engine in response to read demand. Unlike mandatory replicas they are:

* Freely evictable (operator flush, tiering demotion).
* Not counted toward write-safety thresholds or the replication goal.
* Counted per datacenter (`cache_replicas.per_dc = true`, the default). Set `false` to pool all reads into one global cache.

See [Volume Configuration](../configuration/volume-config.md#replication-classes) for the schema.

The engine ticks every `tiering.evaluation_interval_secs` (default **3600**, one hour; set a shorter value if you want faster promotion). Only the leader acts; a follower skips the tick.

### Scan exclusion

A FUSE mount can list library-scanner process names in
`untracked_read_processes` ([FUSE client](../configuration/fuse-client.md)).
Their reads are still verified but carry no requester DC and send no
cache-hit notification, so the engine never counts them as demand and a
scan promotes nothing.

### Promotion math

For each `(chunk, DC, cache-tier)` triple, the engine evaluates on
every tick:

1. **Window count** — sum of reads from this DC in the last
   `cache_replicas.promote_window_secs` (default 4 h).
2. **Cooldown check** — if the chunk's
   `cache_cooldown_until_ms[idx]` is in the future, refuse this
   tier this tick.
3. **Threshold check** — refuse if window count <
   `cache_replicas.promote_reads` (default 2).
4. **Target count** — basic promotion path schedules
   `min(min, max) - current_in_dc` new replicas. Always 0 once
   `min` is reached. The redundancy-escalation pass that ramps
   toward `max` is opt-in via the
   `tiering.enable_cache_redundancy_escalation` runtime config
   flag (default `false`, changeable at runtime). When
   enabled, the planner schedules up to `max - current_in_dc`
   replicas once `current_in_dc >= min` AND read count is at
   least `2 * promote_reads` — the audit reason code switches
   from `promote_threshold_met` to `promote_redundancy` so
   operators can distinguish the two paths.
5. **Target selection** — pick a healthy non-draining node in the
   requesting DC that has a disk of the target tier and doesn't
   already hold this chunk. Tie-break by highest free capacity,
   then lowest `node_id` (deterministic audit output).

When `per_dc = false`, all DC reads contribute to one global
pool and the engine places the cache copy in the DC that
generated the most reads (best locality).

### Demotion math

For each existing cache-tier placement:

1. **Tier check** — must be a *cache* tier (i.e., the volume's
   `cache_replicas.<tier>.max > 0` AND
   `mandatory_replicas.<tier> == 0`).
2. **Window check** — no reads from the placement's DC inside the
   volume's idle window: `cache_replicas.retention_secs`, or
   `promote_window_secs` when it is 0. Volumes with `retention_secs` set
   use the node-local retention pass below instead, on every disk whose
   last-read index answers; this ring-based check is their fallback.
3. **Anchor safety** — at least one other **live** replica must
   satisfy the anchor (datacenter, and tier if `required_tier` is
   set). A replica on a Down, Failed, Maintenance or Quarantined node
   (or one with a quarantine record), on a Failed or Removed disk, or
   one already slated for demotion in the same plan, does not count
   (`survivor_replica_tier`, #114). The apply
   path re-checks via `enforce_anchor` on `RemoveChunkLocation`.
4. **Read history** — read windows live only in the leader's memory.
   After an election or leader restart no idle demotion runs until this
   leader has held the role for the longest idle window among the planned
   demotions, and
   none while the read-window store hit its key cap within that window
   (evicted counts would read as 0). Promotions are not delayed.

A surviving demotion triggers the full sequence:

* `RemoveChunkLocation` Raft write removes the SM entry FIRST. If the
  write fails or the state machine refuses it, nothing is deleted.
* Only then the `DeleteChunkReplica` RPC deletes the bytes
  (idempotent `was_present`/`freed_bytes` shape). A failed byte delete is
  left for the orphan reaper.
* Conditional `SetChunkCooldown(expires_at_ms = 0)` — clears
  the chunk's cooldown entry **only if** the snapshot cooldown
  was 0 or in the past. A future operator-flush cooldown is
  left intact so the operator's intent ("no immediate
  re-promotion") wins.

Demotion does NOT consult the cooldown when *deciding*. The
cooldown only protects against immediate re-promotion; demotion
itself is allowed because the cache replica is already eligible
for eviction.

### Cooldown semantics

`cache_replicas.cooldown_secs` (default 300 s = 5 min) is the
window after an operator flush during which the engine refuses
to re-promote. Stored per `(file, chunk_index)` in
`FileMetadata.cache_cooldown_until_ms` as the absolute
millisecond-since-epoch expiry. Lifecycle:

* **Operator flush** sets `cooldown_until_ms = now +
  cooldown_secs`.
* **Promotion path** in the planner checks `now <
  cooldown_until_ms` and refuses.
* **Engine-driven demote** clears the entry via
  `SetChunkCooldown(0)` IFF the snapshot cooldown was past or
  absent. Future operator cooldowns are preserved.
* **State-machine apply**: `expires_at_ms = 0` removes the
  entry; any other value is `max()`-merged with the existing
  one (monotonic — concurrent flushes never regress).

This produces the user-visible behaviour `flush → no
re-promote for 5 min → operator-decay clears it → next read
re-promotes`, versus the engine-driven decay path `idle →
demote → immediate read re-promotes` (no cooldown set, no
wait).

### Anchor-aware demotion

The planner never emits a demotion that would leave the anchor
unsatisfied by a live replica. This invariant lives in two places:

* `plan_chunk_demotions` filters before emitting the action.
* `RemoveChunkLocation`'s state-machine apply path re-checks
  via `enforce_anchor` (defence-in-depth against any future
  planner regression).

If a tier-aware anchor is configured
(`anchor.required_tier = "cold"`), the engine treats it as the
mandatory floor for demotion decisions.

### Dispatch limits

Promotions are planned for every chunk and then dispatched as one batch. At most `[tiering] max_promotions_per_tick` (default 50) NEW tasks are scheduled per tick (tasks already in flight do not count) so anchor and mandatory repair, which share the scheduler's task budget, are never starved. The rest are re-planned on the next tick. Target selection is a pure function: a healthy, non-draining node in the reading datacenter with a serviceable disk of the cache tier that does not already hold the chunk, highest free capacity first, lowest node id as the tie-break. A promotion is an additive copy, so the source replica is not subtracted in the anchor check.

### Link budget

`[[tiering.links]]` entries (`to_dc`, `max_mbps`, see [manager configuration](../configuration/manager.md#tiering)) give a datacenter behind a slow link one shared copy budget. Every replication task whose target node is in `to_dc` and whose source node is in another datacenter is charged the chunk's size when the leader creates it: under-replication repair, anchor-lag repair, path migration, reactive promotion, legacy temperature migration and pre-caching alike. The budget is a token bucket of `max_mbps` with a 120 s burst (a chunk larger than the burst still fits once the bucket has filled to its size). A copy that does not fit is not scheduled this round: nothing is recorded, and the next scheduler or tiering tick plans it again. Copies inside one datacenter are never charged. Datacenter names compare case-insensitively. No entries (the default) means unlimited, the behaviour before this setting existed.

The bucket lives in the leader's memory: a new leader starts with full buckets. Pre-cache copies pass both this budget and `precache_max_mbps`. Path migration reports skipped chunks as `link_throttled`; re-run it to pick them up. The leader logs one info line per datacenter per minute with the skipped count and bytes, and `GET /api/v1/tiering/precache` returns `link_budget_backlog` (per link: `to_dc`, `max_mbps`, `tokens_bytes`, and `skipped_requests` / `skipped_bytes` since this manager became leader).

> **Warning:** an entry for the anchor datacenter also throttles anchor-lag and mandatory repair into it. After a node loss or a burst of `anchor_sync=false` writes, chunks stay without their anchor copy for longer. Size `max_mbps` for the anchor DC to keep up with your write rate.

Not covered: copies do not yield to foreground reads, and budgets are per target datacenter only, not per (source, target) pair.

### Capacity eviction

Idle demotion is time-based; a cache disk can still fill, after which promotions and pre-cache stop (`capacity_limited`). With `[tiering] cache_high_watermark_pct` and `cache_low_watermark_pct` set (both 0 = off, otherwise `0 < low < high <= 99`), the leader checks every 30 s (in the pre-cache loop) each hot/warm disk of a node that is not Down, Failed, Maintenance or Quarantined. Usage is the heartbeat's filesystem usage, so non-DTFS data on the disk counts too.

A disk at or above `high` lists its least recently read chunks (`ListOldestChunks`, data nodes track a per-chunk last-read time; library-scan reads do not count) and evicts cache copies oldest first until `used - low% of total` bytes are freed, at most 512 per disk and round:

* A chunk is evicted only if every location row of that hash on that exact disk is a cache copy that the idle demote could drop too: the tier is a cache tier of the file's volume and not mandatory, a live mandatory replica survives elsewhere, and the anchor stays satisfied by a live replica (anchor-pending chunks are never touched). Removing only some rows would free nothing.
* Pass 1 skips copies the pre-cache protects; only if that is short does pass 2 also take copies protected by an active pre-cache cursor. Copies under `precache.pins` are never evicted (there is no "evict pins as last resort" option).
* The manager pages the list (keyset cursor `(last_read_ms, chunk_id)`) up to 8 pages, 32k entries, per disk and round while the plan is short of its target, so a disk full of mandatory copies at the old end still reaches its cache copies. A data node that ignores the cursor returns the first page again; paging then stops.
* Each copy goes through the idle demote's path: `RemoveChunkLocation` with `enforce_anchor` through Raft first, then the byte delete. Audit `cache.evict_capacity` per copy, one warn line per disk and round.

While the watermarks are set, promotion target selection and pre-cache room never plan a hot/warm disk past `low`, so they do not immediately trigger eviction. A data node too old for `ListOldestChunks`, a cold/tape disk, or a last-read index not seeded yet skips the disk for that round (logged once at info).

### Retention demotion

The idle demote above trusts read counts kept in the leader's memory, so it waits a full window after every leader change and after a read-window key-cap eviction. With a long window (days) that means it rarely runs. A volume can instead set `cache_replicas.retention_secs` (0 = unset, the ring-based idle demote with `promote_window_secs` as before; non-zero needs every manager on v0.1.16, otherwise the REST call is refused with 409).

For such volumes the 30 s pre-cache loop visits each hot/warm disk of a node that is not Down, Failed, Maintenance or Quarantined at most every 10 min, asks it for chunks unread for the shortest `retention_secs` of any volume (`ListOldestChunks` with `older_than_ms`, up to 4096, oldest first; library-scan reads do not count) and demotes up to 512 copies per disk and visit:

* every location row of the hash on that disk belongs to a volume with `retention_secs` set that has elapsed since the chunk's last read (`now - last_read >= retention`), and the file was not updated or given a replica within `promote_window_secs` + 180 s;
* the same per-row safety as capacity eviction: cache tier, live mandatory survivor, anchor kept by a live replica (anchor-pending chunks are never touched);
* copies protected by the pre-cache (cursors, scheduled copies) and pins are never taken.

Like capacity eviction, the manager pages the list up to 32k entries per disk and visit while fewer than 512 copies are planned.

No leader-tenure or read-window gate applies: the last-read time is persisted on the data node. Each copy goes through `RemoveChunkLocation` with `enforce_anchor` first, then the byte delete; audit `cache.demote` with result `retention_elapsed` and the last-read age.

A disk whose index answered within the last 30 min is "capable": the ring-based idle demote skips copies of retention volumes there. A disk that answers Unimplemented (old data node), FailedPrecondition (cold/tape) or Unavailable (index not seeded, node unreachable) is not, and keeps the ring-based idle demote with `retention_secs` as the idle window and its leader-tenure gate.

### Fill on read

With promotion, a first playback in a remote DC pulls every chunk over the WAN and the later promotion copies the same chunks over the WAN again. A volume with `cache_replicas.fill = "on_read"` (default `"on_promote"`; `on_read` needs every manager on v0.1.16, otherwise the REST call is refused with 409) lets the FUSE reader keep the copy it already fetched:

* The FUSE client sends its `[location].datacenter` with `GetFileLocations`. The manager answers a fill target when the volume is `on_read`, has a cache tier (cache `max > 0`, not mandatory; hottest first) and some node in that DC (case-insensitive) is eligible like a promotion target: survivor, not draining, a tier disk with room and below `cache_low_watermark_pct` when set; most free, then lowest node id. An older manager ignores the DC: no target, no fill.
* On a viewer chunk transition (a tracked process past the first 1 MiB of the handle, not `untracked_read_processes`) of a chunk that has no replica in the client's DC and was not filled recently (4096 ids), the client puts the verified bytes on the target (`PutChunk` with the fill tier, at most 2 in flight, dropped when full, never awaited by the read) and calls `CommitCacheFill`.
* `CommitCacheFill` (HMAC-stamped like every mutating manager RPC) refuses: unknown file `NotFound`; volume not `on_read`, no row with that hash at that index, unknown or non-survivor node or a node outside `client_dc`, or the DC already holding `cache_replicas.<tier>.max` live copies on the tier (per DC when `per_dc`, else global) `FailedPrecondition`; a disk not on the node or not on a cache tier of the volume `InvalidArgument`. A row already on that (node, disk) is success. It registers the row through the lease-less union-only `UpdateChunkLocations` (forwarded to the leader from a follower) and audits `cache.fill`.
* A refused or failed commit leaves an unregistered copy that the orphan reaper removes after its grace. The copy is an ordinary cache copy afterwards: retention, idle demote and capacity eviction apply.

### Worked example — `/movies/Wicked.mkv` lifecycle

Volume: cold mandatory, warm cache per-DC with `min=1, max=1`,
`promote_reads=2`, `promote_window_secs=14400` (4 h),
`cooldown_secs=300` (5 min). Anchor: `leli` DC, cold tier.

```
t=0:        Write of /movies/Wicked.mkv.
            Placement engine schedules 1 cold replica on
            LELI/data-primary01 (anchor-satisfying).
            State: {LELI cold}.

t=10:       Plex on TORO opens the file. 1 read from TORO.
            Window count for (chunk, toro) = 1.
            Below threshold (2); planner emits no promotion.

t=120:      Same TORO Plex session continues. 2nd read.
            Window count for (chunk, toro) = 2 ≥ threshold.
            Planner emits promotion → schedule warm replica on
            TORO/data-warm01 (no cooldown, no existing warm).
            State: {LELI cold, TORO warm}.

t=300:      Operator clicks "Flush cache" on /movies/Wicked.mkv.
            scheduler.flush_cache_for_path drops the warm
            replica and sets cooldown_until_ms = t=600s.
            State: {LELI cold} + cooldown active.

t=305:      TORO Plex re-opens. 1 read. Window count = 1.
            Below threshold AND below cooldown — no promotion.

t=400:      2nd read. Window count = 2. ≥ threshold BUT cooldown
            still active (t=400 < 600). No promotion.

t=610:      3rd read after cooldown expired. Window count
            depends on what the window saw (the t=305/t=400
            reads remain inside the 4h window). ≥ threshold.
            Planner promotes again → warm replica appears.

t=14_500:   No reads for 4 hours. Engine tick: window for TORO
            now empty. Planner emits demotion of the TORO warm
            replica. Dispatcher: DeleteChunkReplica →
            RemoveChunkLocation → SetChunkCooldown(0) (the
            snapshot cooldown is 0/past, so clear succeeds).
            State: {LELI cold}, no cooldown.

t=14_600:   Plex opens the file again. 1 read.
            (subsequent reads continue the new window — no
            cooldown blocks anything.)
```

The temperature path below runs only for volumes WITHOUT an active `cache_replicas` policy; a volume with one is driven solely by the engine above, so a legacy promote can never undo an operator flush.

## Temperature classification (volumes without a cache policy)

Data nodes report per-chunk read counts in heartbeats. The engine classifies each chunk:

| Temperature | Rule (first match) | Target tier |
|-------------|--------------------|-------------|
| Hot | at least `hot_threshold_reads_per_hour` (5) reads in the last hour | hot |
| Warm | at least 2 reads in the last 24 hours | warm |
| Frozen | no access for more than `frozen_threshold_days` (60) | tape |
| Cold | no access for more than `cold_threshold_days` (14) | cold |
| Warm | otherwise | warm |

```toml
[tiering]                       # manager.toml
enabled = true
hot_threshold_reads_per_hour = 5
cold_threshold_days = 14
frozen_threshold_days = 60
evaluation_interval_secs = 3600
enable_cache_redundancy_escalation = false
```

When the target tier differs from the chunk's current tier, a replication to a node with a disk of the target tier is scheduled, subject to the anchor check. This path is deliberately slow: at most 8 migrations are planned per tick and the rest are re-derived on the next tick. There is no hysteresis margin and no per-path pinning.

Disks declare their tier in `data.toml` (`[[disks]]`, `tier = "hot|warm|cold|tape"`); see [Data Node Configuration](../configuration/data-node.md) and [Tape Storage](./tape.md).

## Pre-caching

Reactive promotion waits for reads to reach a datacenter. Pre-caching copies the files a reader will open next into that datacenter's cache tier before they are opened. It is opt-in per volume (`precache.mode`, see [Volume Configuration](../configuration/volume-config.md#pre-caching-policy)) and runs on the leader every 30 s.

**Signal.** Reads reported by the data nodes (the same 30 s batches that feed reactive promotion) are remembered per (file, datacenter). Reads with no datacenter, or from the `unknown` bucket, are ignored; this is also how FUSE `untracked_read_processes` keeps library scans from counting. A file counts as being played when a chunk from index 12 on (and not within the last 2 chunks) was read, or more than 64 distinct chunks were read. Header and tail probes by scanners therefore never start pre-caching. The head skip assumes the default `readahead.prefetch_chunks = 8`: one read near chunk 0 prefetches up to index 9, and a 16 MiB probe with 4 MiB chunks reaches index 11. A client with `prefetch_chunks >= 11` makes every header probe count as playing. Because of the head and tail skips a file needs at least 15 chunks (`i >= 12 && i + 2 < chunk_count`) to count as played: with 64 MiB chunks files under about 960 MiB never start pre-caching, with 16 MiB chunks under about 240 MiB, with 4 MiB chunks under 60 MiB. The most recently played file per directory (`files`) or per series (`plex`) is the cursor, active while its last read is within the volume's `precache.cursor_ttl_secs` (0 = `promote_window_secs`; non-zero needs every manager on v0.1.16).

**Modes and ordering.** In `files` mode the next files in the cursor's directory are taken in natural order (`E2` before `E10`, case-insensitive text). In `plex` mode the series root is the nearest of the parent or grandparent directory holding a regular file named `.plexmatch`; all files under it are ordered component by component (`Season 02/E10` before `Season 10/E01`). Specials, Extras, Trailers and similar folders are not part of the playback order. Directories, symlinks, dot-files, files under 16 MiB and files with an active write lease are skipped.

**Selection.** Whole files, contiguous after the cursor, until the budget (`series_max_gb`, or `next_files` / `next_max_gb`) runs out; the leftover budget caches the head of the next file (whole chunks only), then selection stops. The file being watched is excluded: read-ahead and reactive promotion own it. New episodes are picked up on the next pass.

**Capacity and eviction order.** One copy per chunk and datacenter, on the hottest tier with `cache_replicas.<t>.max > 0` and no mandatory replicas, placed like a reactive promotion. A node's room is its serviceable cache-tier disks minus the placement headroom and a 10% reserve; when it runs out planning stops and the datacenter shows `capacity_limited`. Pre-caching never deletes anything. Protection from the idle demote ends in this order: series beyond `max_series` (oldest first), then file cursors beyond 8 per datacenter, then cursors older than the cursor TTL. Unprotected copies are removed only by the idle demote, retention demotion and capacity eviction; the first two skip protected copies. An operator `flush_cache` removes protected copies too and its cooldown stops re-copying.

**Bandwidth and yield.** Copies start at an average of `[tiering] precache_max_mbps` (default 10, 120 s burst); a copy that does not fit the balance waits, later ones do not jump ahead. The normal scheduler limits (6 copies at once, 2 per target node) still apply. With 24 or more live scheduler tasks of any class (repair included) pre-caching schedules nothing, and below that it adds at most the room left under 24. A chunk larger than the burst (for example 64 MiB at 4 Mbps) still schedules: the bucket holds at least the largest planned chunk. One planning pass walks at most 200000 chunk entries under the state read lock (cursors first, then pins); a pin that does not fit is marked `truncated`.

### Pinned prefixes

`precache.pins` (`{prefix, datacenter}`, at most 32 per volume) keeps a subtree cached in one datacenter whatever the mode, even with mode `off`; reads are not needed. Each pass lists the regular files of the pin's own volume under the prefix (whole path components: `/tv/Blue` does not match `/tv/Bluey`; sidecars under 16 MiB, dot-files and files being written are skipped) and applies every pre-caching placement rule: cache tiers only, never onto a node already holding the chunk, never for anchor-pending chunks, capacity and reserve, the rate limit and the repair yield. Pin copies rank after every cursor copy. Cached pinned chunks are protected from the idle demote for as long as the pin exists; an operator `flush_cache` still removes them. At most 5000 files are listed per pin per pass; `truncated` in the status marks a larger subtree, a pass out of its chunk-walk budget, or a datacenter whose cache is full (the pin stops there).

**Anchor.** Pre-caching only adds cache replicas through the verified replication path. Chunks without a live anchor copy (anchor-pending, judged like the dashboard: anchor DC and the anchor's `required_tier`) are not copied.

**Leader change.** Copy tasks are Raft state and finish on their own. The new leader starts with an empty protection set and rebuilds cursors within 30 to 90 s; the ring-based idle demote is refused for its idle window after a new leader's tenure starts (retention demotion is not gated; it runs after the pre-cache pass of the same 30 s tick, so it sees the protection that pass rebuilt from the read activity this manager holds), so nothing is lost in between. A volume whose policy stops validating (for example after `set-cache` removed the cache tier) is skipped and logged once.

**Recommended profile.** Plex library volume: `plex` mode, `series_max_gb` 20, `max_series` 3, a warm cache tier with `max = 1` and `per_dc = true`, `promote_window_secs` 14400 or more, and `precache_max_mbps` at most half the DC's link.

## Operator controls

- `dtfs-cli` volume commands set `mandatory_replicas`, `cache_replicas` and the anchor ([Volume Configuration](../configuration/volume-config.md)).
- A cache flush (`POST /api/v1/files/flush_cache`) drops cache replicas for a path and sets a cooldown.
- An explicit migrate request moves a chunk to a target tier (and optionally datacenter) through the same verified-copy path ([Migration](./migration.md)).

## Monitoring

The manager `/metrics` endpoint exposes `dtfs_tier_migrations_completed_total`, `dtfs_migration_failed_total` and `dtfs_anchor_protected_total`; the audit log records `tier.promote`, `tier.demote`, `cache.flush_replica` and `cache.flush_refused`. See [Monitoring](../operations/monitoring.md).

### Pre-cache monitoring

`dtfs-cli precache status` (REST `GET /api/v1/tiering/precache`, leader only, 409 with a leader hint elsewhere) shows per datacenter the room, desired and cached bytes, whether the cache is capacity-limited, the active series and cursors with their files, the pinned prefixes (prefix, datacenter, bytes, cached bytes, `truncated`), the last scheduled batch, `live_tasks` (all scheduler tasks) with `yielded_live_tasks` (the loop scheduled nothing because of them), `inflight_tasks` (pre-cache's own copies only), and volumes skipped because their policy no longer validates. The REST response also carries `link_budget_backlog` ([link budget](#link-budget)). `/metrics` carries `dtfs_precache_is_leader`, `dtfs_precache_inflight_tasks`, `dtfs_precache_live_tasks` and `dtfs_precache_{desired_bytes,cached_bytes,series_active,capacity_limited}{dc="..."}`. A `desired_bytes` that stays above `cached_bytes` with `capacity_limited` 1 means the cache tier is too small for the plan.
