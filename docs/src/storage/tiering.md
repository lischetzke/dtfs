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
2. **Window check** — `reads_in_window_by_dc[placement.dc]` must
   be 0 (no reads from this DC inside `promote_window_secs`).
3. **Anchor safety** — at least one other **live** replica must
   satisfy the anchor (datacenter, and tier if `required_tier` is
   set). A replica on a Down, Failed, Maintenance or Quarantined node
   (or one with a quarantine record), on a Failed or Removed disk, or
   one already slated for demotion in the same plan, does not count
   (`survivor_replica_tier`, #114). The apply
   path re-checks via `enforce_anchor` on `RemoveChunkLocation`.
4. **Read history** — read windows live only in the leader's memory.
   After an election or leader restart no idle demotion runs until this
   leader has held the role for the largest `promote_window_secs`, and
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

Promotions are planned for every chunk and then dispatched as one batch. At most 50 NEW tasks are scheduled per tick (tasks already in flight do not count) so anchor and mandatory repair, which share the scheduler's task budget, are never starved. The rest are re-planned on the next tick. Target selection is a pure function: a healthy, non-draining node in the reading datacenter with a serviceable disk of the cache tier that does not already hold the chunk, highest free capacity first, lowest node id as the tie-break. A promotion is an additive copy, so the source replica is not subtracted in the anchor check.

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

## Operator controls

- `dtfs-cli` volume commands set `mandatory_replicas`, `cache_replicas` and the anchor ([Volume Configuration](../configuration/volume-config.md)).
- A cache flush (`POST /api/v1/files/flush_cache`) drops cache replicas for a path and sets a cooldown.
- An explicit migrate request moves a chunk to a target tier (and optionally datacenter) through the same verified-copy path ([Migration](./migration.md)).

## Monitoring

The manager `/metrics` endpoint exposes `dtfs_tier_migrations_completed_total`, `dtfs_migration_failed_total` and `dtfs_anchor_protected_total`; the audit log records `tier.promote`, `tier.demote`, `cache.flush_replica` and `cache.flush_refused`. See [Monitoring](../operations/monitoring.md).
