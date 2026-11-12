# Volume Configuration

A volume is a policy container: chunk size, replication, cache promotion, anchor, write safety and spill rules. Volumes live in the Raft state machine, not in a TOML file. They are created and edited through the REST API, the CLI or the WebUI, and every change takes effect immediately for new writes and for the next scheduler pass. There is no volume delete or rename endpoint.

`manager.toml` `[defaults]` only seeds the auto-created `default` volume (at first bootstrap) and the defaults applied to volumes created through the API; see [Manager `[defaults]`](./manager.md#defaults).

## Settings

Shape returned by `GET /api/v1/volumes/{id}` ([REST API](../api-reference/rest-api.md#volumes)).

| Setting | Type | Default for a new volume | Edited by |
|---|---|---|---|
| `volume_id` | string | lowercase `name` with spaces replaced by `-` | create only |
| `name` | string | required at create | create only |
| `chunk_size_bytes` | u64 | `[defaults].chunk_size_mb` (4 MiB) x 1048576, or the request's `chunk_size_mb` (1 to 64; `0` means the default) | create only |
| `replication.{hot,warm,cold}` | u32 | request `replication_hot/warm/cold`, else `[defaults.replication]` (built-in 0, 2, 1 when the table is absent) | create only |
| `replication.tape` | u32 | `1` | not editable |
| `mandatory_replicas.{hot,warm,cold,tape}` | u32 | `0` each (legacy fallback, see below) | `set-mandatory`, `migrate-policy` |
| `cache_replicas.{hot,warm,cold,tape}.{min,max}` | u32 | `0`/`0` (disabled) | `set-cache` |
| `cache_replicas.per_dc` | bool | `true` | `set-cache` |
| `cache_replicas.promote_reads` | u32 | `2` | `set-cache` |
| `cache_replicas.promote_window_secs` | u64 | `14400` (4 h) | `set-cache` |
| `cache_replicas.cooldown_secs` | u64 | `300` | `set-cache` |
| `cache_replicas.retention_secs` | u64 | `0` (= `promote_window_secs`) | `set-cache`; non-zero needs every manager on v0.1.16 (409). Idle time before a cache copy is demoted, read from the data node's last-read index ([Tiering](../storage/tiering.md#retention-demotion)) |
| `cache_replicas.fill` | `"on_promote"` \| `"on_read"` | `"on_promote"` | `set-cache`; `on_read` needs every manager on v0.1.16 (409). `on_read`: a FUSE reader copies a chunk it fetched from another DC to a cache node in its own DC ([Tiering](../storage/tiering.md#fill-on-read)) |
| `anchor.datacenter` | string | request `anchor_datacenter`; none if omitted (`[defaults].anchor_datacenter` applies only to the auto-created `default` volume) | create, `set-anchor` |
| `anchor.required_tier` | `hot`/`warm`/`cold`/`tape` or null | null (any tier in the anchor DC) | `set-anchor` |
| `write_safety.min_replicas_before_ack` | u32 | `[defaults.write_safety]` (2) | `set-write-safety` |
| `write_safety.write_timeout_secs` | u32 | `[defaults.write_safety]` (30) | `set-write-safety` |
| `spill.allow` | bool or null | null (inherit `[placement] allow_capacity_spill`) | `set-spill` |
| `spill.forbidden_tiers` | list of tier | `[]` | `set-spill` |
| `spill.forbidden_datacenters` | list of string | `[]` | `set-spill` |

Constraints enforced by the API (HTTP 400, the volume is left unchanged):

- `set-cache`: per tier `max >= min`. `min = max = 0` disables the tier. Only the fields you pass change.
- `set-mandatory`: the result must not be all zero; if the anchor has a `required_tier`, that tier's count must be above zero; a non-zero `tape` count needs a tape-tier disk registered in the cluster.
- `set-anchor`: a non-empty `required_tier` needs `mandatory_replicas.<tier> > 0`. An empty `datacenter` clears the anchor. An unknown datacenter (no registered node reports it, case-insensitive) is refused unless `force: true`.
- `set-write-safety`: both values at least 1.
- `set-spill`: tier names must be `hot`, `warm`, `cold` or `tape`.

## Chunk size

`chunk_size_mb` (create request) or `[defaults].chunk_size_mb` fixes `chunk_size_bytes` for the volume's lifetime. The value is honoured per volume. Every file records the chunk size in force when it was created and reads use that per-file value, so files written earlier stay readable if the volume default changes later. The FUSE client's `[client].chunk_size_mb` must equal the volume's chunk size: a write to a file with a different chunk size is refused.

## Replication classes

Two independent classes, never conflated:

- **Mandatory** (`mandatory_replicas.<tier>`): the durability floor. Only the under-replication scheduler maintains it. With an anchor set, every chunk also keeps at least one replica in the anchor datacenter.
- **Cache** (`cache_replicas.<tier>.{min,max}`): demand-driven and evictable. Only the tiering engine creates them, after `promote_reads` reads within `promote_window_secs` (per datacenter when `per_dc`). `POST /api/v1/files/flush_cache` drops them and sets `cooldown_secs` before re-promotion.

Legacy fallback: while all `mandatory_replicas` are zero, the scheduler uses `replication` as the mandatory floor. `volumes migrate-policy` copies `replication` into `mandatory_replicas` once (idempotent, `replication` is kept). `set-mandatory` on a legacy volume first seeds from that effective floor, then applies your values, so changing one tier never zeroes the others.

See [Replication](../storage/replication.md) and [Tiering](../storage/tiering.md).

## Example: Plexmedia

One cold copy on the LELI archive, warm cache copies in the other datacenters on demand:

```bash
curl -X POST http://mgr-01:9402/api/v1/volumes -H 'X-Dtfs-Shared-Secret: ...' \
  -H 'content-type: application/json' \
  -d '{"name":"media","chunk_size_mb":16,"anchor_datacenter":"leli"}'
dtfs-cli volumes set-mandatory media --hot-replicas 0 --warm-replicas 0 --cold-replicas 1
dtfs-cli volumes set-anchor media --datacenter leli --required-tier cold
dtfs-cli volumes set-cache media --warm-min 1 --warm-max 1 --per-dc true \
    --promote-reads 2 --promote-window-secs 14400 --cooldown-secs 300
dtfs-cli volumes set-write-safety media --min-replicas-before-ack 1 --write-timeout-secs 60
```

Result: every chunk has one cold replica in `leli`; two reads from a datacenter within four hours add one warm replica there; an operator flush removes it and blocks re-promotion for five minutes while the cold anchor stays.

## Pre-caching policy

`precache` (set with `dtfs-cli volumes set-precache` or `POST /api/v1/volumes/{id}/set-precache`) tells the manager to copy the files a reader is likely to open next into the reader's datacenter cache tier. It only adds cache replicas; idle / retention demotion and capacity eviction remove them.

| Field | Default | Meaning |
|---|---|---|
| `mode` | `off` | `off`, `files` (next files in the same directory) or `plex` (next episodes of a series whose root holds a `.plexmatch` file) |
| `next_files` | 2 | whole files to pre-cache after the file being watched |
| `next_max_gb` | 0 | GiB cap on that selection (0 = no byte cap) |
| `series_max_gb` | 20 | GiB budget per series (plex) |
| `max_series` | 3 | series kept pre-cached per volume per datacenter (plex) |
| `cursor_ttl_secs` | 0 (= `cache_replicas.promote_window_secs`) | how long a played file stays a cursor after its last read; non-zero needs every manager on v0.1.16 |
| `pins` | `[]` | up to 32 `{prefix, datacenter}` entries: subtrees kept pre-cached in a datacenter whatever the mode (see [Tiering](../storage/tiering.md#pinned-prefixes)) |

Constraints for any mode other than `off`: a cache tier with `cache_replicas.<tier>.max > 0` whose `mandatory_replicas.<tier>` is 0, and `cache_replicas.per_dc = true`. A pin prefix must start with `/` and must not be `/`; the pin datacenter must match a registered node's datacenter (case-insensitive, REST answers 400 otherwise). Pins alone (mode `off`) are valid and need the same cache tier and `per_dc`. Plex needs `series_max_gb` and `max_series` above 0; files needs `next_files` or `next_max_gb` above 0. The setting needs every manager on v0.1.16 or later (409 otherwise).

```bash
dtfs-cli volumes set-precache media --mode plex --series-max-gb 20 --max-series 3
dtfs-cli volumes set-precache media --pin /tv/Bluey@toro --pin /movies/Kids@toro   # replaces the pin list
dtfs-cli volumes set-precache media --clear-pins
```

## Deleting a volume

`dtfs-cli volumes delete <id>` (REST `DELETE /api/v1/volumes/{id}`). An empty volume needs one confirmation. A volume that still holds files (or directory markers) needs two, the second being the volume id typed exactly; non-interactively pass `--yes --confirm <id>`. Its files are then deleted the same way as an unlink: metadata first, and the orphan reaper reclaims the chunk bytes after `orphan_grace_secs` (at least 6 h). The `default` volume cannot be deleted, and a volume with a file open for writing is refused until the writer closes it. Clients still configured with the deleted volume id get "volume not found" on create.

## Default volume

The leader creates the `default` volume from its own `[defaults]` (cluster-scope keys: keep them identical on every manager) on the first scheduler tick that finds no volume at all, retried every tick until it lands. It is a bootstrap seed: once any volume exists a missing `default` is recreated only while some file still has `volume_id == "default"` (a deleted, unused `default` stays gone), and an existing `default` is never overwritten. Files created without a volume id land in it. A file's volume is fixed at creation; the FUSE client never downgrades a known volume to `default`, and chunk placement always follows the file's own volume.
