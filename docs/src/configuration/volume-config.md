# Volume Configuration

Each volume in DTFS has its own configuration that controls chunk sizing, replication, and write safety. Volume settings are managed through the Web UI or REST API and stored in the Raft-replicated state machine.

All volume settings support [live reload](./live-reload.md).

## Volume Settings Reference

| Setting | Type | Default | Valid Range | Description |
|---------|------|---------|-------------|-------------|
| `id` | `VolumeId` | *(auto-generated)* | Any non-empty string | Unique identifier for the volume. Set at creation time and cannot be changed. |
| `name` | `String` | `"default"` | Any non-empty string | Human-readable name for the volume. Can be changed after creation. |
| `chunk_size_bytes` | `u64` | `134217728` (128 MiB) | 1048576--1073741824 (1 MiB--1 GiB) | Size in bytes of each chunk. Files are split into chunks of this size for distribution across data nodes. Larger chunks reduce metadata overhead; smaller chunks reduce write amplification and improve parallel read performance. This value is set at volume creation and cannot be changed afterward. |
| `replication.hot` | `u32` | `3` | 1--9 | Legacy per-tier replication factor for hot. Treated as a *fallback* when `mandatory_replicas` is unset (see [Replication classes](#replication-classes)). |
| `replication.warm` | `u32` | `2` | 1--9 | Legacy per-tier replication factor for warm. Fallback when `mandatory_replicas` is unset. |
| `replication.cold` | `u32` | `1` | 1--9 | Legacy per-tier replication factor for cold. Fallback when `mandatory_replicas` is unset. |
| `mandatory_replicas.hot` | `u32` | `0` | 0--9 | Bug AV — number of replicas the under-replication scheduler enforces on the hot tier. `0` opts the tier out of mandatory replication entirely (cache-only via tiering, if `cache_replicas.hot` is set). |
| `mandatory_replicas.warm` | `u32` | `0` | 0--9 | Bug AV — same as above for warm. |
| `mandatory_replicas.cold` | `u32` | `0` | 0--9 | Bug AV — same as above for cold. Typically `1` on Plex-style archives (cold is the durable anchor). |
| `mandatory_replicas.tape` | `u32` | `0` | 0--9 | Bug AV — same as above for tape. |
| `cache_replicas.warm.min` | `u32` | `0`/`1`*  | 0--9 | Bug AV — minimum warm cache replicas created per promotion. `0` (and an absent `[cache_replicas.warm]` block) disables warm caching. `*`Default jumps to `1` if the `[cache_replicas.warm]` block is *present* but empty. |
| `cache_replicas.warm.max` | `u32` | `0`/`1`*  | min--9 | Bug AV — upper bound for the redundancy-escalation pass (Phase-2-deferred; honoured today as the create cap). |
| `cache_replicas.hot.{min,max}` | `u32` | `0`/`1`* | 0--9 | Bug AV — same for the hot tier (rare; cache-on-hot is mostly used for top-of-LRU prediction). |
| `cache_replicas.per_dc` | `bool` | `true` | true/false | Bug AV — when `true`, every promoting DC gets its own cache replica(s). When `false`, cache replicas are global (any one DC counts). |
| `cache_replicas.promote_reads` | `u32` | `2` | 1--1000 | Bug AV — number of reads required within `promote_window_secs` (from the same DC if `per_dc=true`) to trigger a cache promotion. |
| `cache_replicas.promote_window_secs` | `u64` | `14400` (4 h) | 60--604800 (1 min--1 wk) | Bug AV — sliding window for the `promote_reads` counter. |
| `cache_replicas.cooldown_secs` | `u64` | `300` (5 min) | 0--86400 (0--1 d) | Bug AV — how long after an operator-initiated cache flush the tiering engine refuses to re-promote the same chunk. Prevents flap loops. |
| `anchor.datacenter` | `String` | *(none)* | DC ID in cluster topology | Bug AV — the under-replication scheduler must always keep at least one mandatory replica in this DC. Refuses any migration that would strand the anchor (`AlertKind::AnchorViolated` fires if a planner does try). |
| `anchor.required_tier` | `Option<DiskTier>` | *(none)* | `hot`/`warm`/`cold`/`tape` | Bug AV — when set, the anchor replica must additionally live on this tier. `None` means any tier in the anchor DC satisfies the invariant. |
| `write_safety.min_replicas_before_ack` | `u32` | `2` | 1--replication factor | Minimum replicas that must acknowledge a write before the client receives success. Cannot exceed the *mandatory* replication factor for the relevant tier (or legacy `replication` if `mandatory_replicas` is unset). |
| `write_safety.write_timeout_secs` | `u32` | `30` | 1--3600 | Maximum seconds to wait for write acknowledgements before returning an error. |

## Replication classes

Bug AV (2026-05) splits per-tier replicas into two independent classes:

* **Mandatory** — `mandatory_replicas.*` is what the under-replication
  scheduler enforces. The tiering engine never touches these. They
  are the durability floor: lose one, the scheduler re-creates it.
  Plex-style deployments usually set `cold = 1` here (the anchor) and
  leave everything else at `0`.
* **Cache** — `cache_replicas.<tier>.{min,max}` is what the tiering
  engine *opportunistically* creates in response to read demand.
  These replicas are freely evictable: the operator can drop them via
  `POST /api/v1/files/flush_cache` (see
  [REST API](../api-reference/rest-api.md#post-apiv1filesflush_cache)),
  and the tiering engine will re-create them when the next
  `promote_reads`-in-`promote_window_secs` threshold is met.

Reads are served from whichever copy is closest — the tiering engine
doesn't care whether a candidate replica is mandatory or cache.

### Back-compat with the legacy `replication` block

Existing volumes ship the pre-AV `replication` block. The new schema
treats it as a **fallback**:

* If `mandatory_replicas` is all-zero (the unset default), the
  scheduler reads from `replication` instead.
* `mandatory_replicas` populated → `replication` is ignored by the
  scheduler (still kept on disk for rollback safety).
* `cache_replicas` absent → no cache promotions happen, regardless of
  what `replication` says.

To migrate a volume to the new schema explicitly:

```
$ dtfs-cli volumes migrate-policy default
Volume 'default' migrate-policy: applied (mandatory hot=0 warm=2 cold=1 tape=1)
```

Re-running it is a no-op. The corresponding REST endpoint is
`POST /api/v1/volumes/{id}/migrate-policy`. See
[CLI reference](../api-reference/cli-reference.md#volumes-migrate-policy)
and
[REST API](../api-reference/rest-api.md#post-apiv1volumesidmigrate-policy).

`set-mandatory` on a legacy volume (one whose `mandatory_replicas` is
still all-zero) seeds the new field from the **effective** floor first —
i.e. the legacy `replication` values — and *then* applies the tiers you
supplied. So bumping one tier never silently drops the others: setting
`cold=2` on a `warm=2 cold=1` legacy volume yields `warm=2 cold=2`, not
`warm=0 cold=2`. A volume already on the new schema is edited in place
(only the supplied tiers change). The anchor's required tier still cannot
be set to `0`, and an all-zero result is rejected.

### Example A — Plexmedia (the canonical use case)

3 DCs, asymmetric bandwidth. Cold tier in LELI is the archive; warm
tier in TORO + NRB is the SSD cache for active viewing. No hot tier.
Anchor pinned to **cold in LELI** so every chunk is durably parked on
the 50 TB HDD pool, regardless of what the cache does.

```toml
[defaults.mandatory_replicas]
# Every chunk has exactly 1 cold replica (anchor copy).
hot  = 0
warm = 0
cold = 1
tape = 0

[defaults.cache_replicas]
per_dc              = true
promote_reads       = 2       # 2 reads to count as "hot in this DC"
promote_window_secs = 14400   # over 4 hours
cooldown_secs       = 300     # 5-min cooldown after operator flush

[defaults.cache_replicas.warm]
min = 1  # First read-from-DC threshold breach creates 1 warm replica
max = 1  # No fault-tolerance ramp; 1 warm copy per DC is enough

[defaults.anchor]
datacenter   = "leli"
required_tier = "cold"        # anchor MUST be cold (not just any tier)

[defaults]
chunk_size_mb = 16

[defaults.write_safety]
min_replicas_before_ack = 1   # anchor (cold) replica suffices to ACK
write_timeout_secs      = 60
```

Behaviour:

```
File "/movies/Wicked.mkv" — never read:
  LELI/data-primary01 cold      ✓ mandatory
  TORO/data-warm01    (none)
  NRB/data-warm02     (none)

After 2 reads from TORO viewer within 4h:
  LELI/data-primary01 cold      ✓ mandatory
  TORO/data-warm01    warm      ✓ cache (promoted)
  NRB/data-warm02     (none)

After operator clicks "Flush cache" on /movies/Wicked.mkv:
  LELI/data-primary01 cold      ✓ mandatory (anchor preserved)
  TORO/data-warm01    (gone)    + 5-min cooldown
  NRB/data-warm02     (none)
```

### Example B — Single-DC home lab

One DC, three racks, no DR concerns. Cold = anchor, warm = local
cache. Same shape as Plexmedia at smaller scale; this is the policy
applied to the minicluster after `volumes migrate-policy`.

```toml
[defaults.mandatory_replicas]
cold = 1

[defaults.cache_replicas]
per_dc              = true
promote_reads       = 2
promote_window_secs = 14400
cooldown_secs       = 300

[defaults.cache_replicas.warm]
min = 1
max = 1

[defaults.anchor]
datacenter   = "dc-a"
required_tier = "cold"

[defaults]
chunk_size_mb = 4
```

See [`storage/replication.md`](../storage/replication.md) for the
broader replication semantics (RF math, write safety, frozen tier),
and [`storage/tiering.md`](../storage/tiering.md) for the tiering
engine that drives cache promotion.

## Creating a Volume

Volumes are created through the REST API or Web UI. At creation time, you can override any of the cluster-wide defaults:

```
POST /api/v1/volumes
```

```json
{
  "name": "media-archive",
  "chunk_size_bytes": 268435456,
  "replication": {
    "hot": 2,
    "warm": 2,
    "cold": 1
  },
  "write_safety": {
    "min_replicas_before_ack": 1,
    "write_timeout_secs": 60
  }
}
```

Fields omitted from the request body inherit their values from the [cluster-wide defaults](./cluster-config.md).

## Modifying a Volume

Replication factors and write safety settings can be changed on a live volume:

```
PUT /api/v1/volumes/{volume_id}
```

```json
{
  "replication": {
    "hot": 3
  },
  "write_safety": {
    "min_replicas_before_ack": 2
  }
}
```

Only the fields included in the request body are changed. Omitted fields retain their current values.

### Immutable Settings

The following settings cannot be changed after volume creation:

- `id` -- The volume identifier.

### True config pane (AZ-5)

The WebUI `/ui/config` page renders every `[section.key]` from the
manager's loaded `ManagerConfig` with `name / scope / current / default`.

* `GET /api/v1/config/effective` — returns an array of
  `{name, scope, current, default, is_default}` entries (one per
  flat key). Use this to drive any external dashboard.
* `POST /api/v1/config` — body
  `{"config_json": "{...}", "write_to_disk": true|false}`. When
  `write_to_disk=true` the manager re-serialises the merged config
  back to `/etc/dtfs/manager.toml` (atomic tmp+rename) so the
  change survives a process restart. When `false`, only the
  in-memory live config + Raft cluster_config are updated (legacy
  behaviour).
* **Boundary check (AZ-5-A3)**: invalid JSON returns 400 with
  `{"error": "...", "preserved_current": true}` and the live
  config + on-disk file stay untouched. The WebUI re-fetches the
  effective config on failure to revert the input field to the
  pre-attempt value.

The page shows ALL keys (operator-set and at-default both). A
chip-style "(default)" marker flags rows where current == default.
Edit a row's value, click **Apply (live only)** for an in-memory
update or **Apply + write to disk** to persist; pending count is
shown at the top of the page.

### Per-file chunk_size (AZ-3)

`chunk_size_bytes` is editable on the volume, but the change does
**not** retroactively affect files already written. Each
`FileMetadata` snapshots the volume's `chunk_size_bytes` at
`CreateFile` time and stores it as `FileMetadata.chunk_size`; the
FUSE read path resolves byte-offset → chunk-index against that
per-file value (`dtfs-fuse/src/filesystem.rs::resolve_chunk_size`).
Consequences:

- Existing files remain readable byte-for-byte after a resize.
- New files created after the resize use the new value.
- A volume can carry a mix of file chunk sizes indefinitely.
- The per-file value is the load-bearing invariant; do not
  attempt to "fix up" `FileMetadata.chunk_size` to match the
  volume after a resize — that would break reads of files written
  at the prior size.

Regression contract: `dtfs-raft` unit tests
`bug_az_3_volume_chunk_size_change_does_not_touch_existing_file`
and `bug_az_3_new_file_after_resize_uses_new_chunk_size`. Live
verification: `tests/proxmox/abuse/16-bug-az-chunk-size-shrink.sh`.

### Effects of Changing Replication

When the replication factor is increased, the cluster begins creating additional replicas in the background. The volume remains fully available during this process. When the replication factor is decreased, excess replicas are garbage-collected during the next tiering evaluation cycle.

### Effects of Changing Write Safety

Changes to `min_replicas_before_ack` and `write_timeout_secs` take effect immediately for new writes. In-flight writes that were started before the change complete under the old settings.

## Default Volume

Every cluster has a volume named `"default"` that is created automatically during cluster bootstrap. It uses the cluster-wide default settings. The default volume cannot be deleted, but its replication and write safety settings can be modified like any other volume.
