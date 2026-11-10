# REST API

The manager serves a JSON REST API and the WebUI on `[api].listen_addr` (default `0.0.0.0:9402`). It is used by `dtfs-cli`, the WebUI and external tooling. Source of truth: the router in `crates/dtfs-manager/src/api.rs` and the handlers in `crates/dtfs-manager/src/api/`.

Base URL: `http://<manager-host>:9402`. All versioned routes are under `/api/v1`.

## Conventions

### Authentication

| Request | Requirement |
|---|---|
| `GET`, `HEAD`, `OPTIONS`, `/healthz`, `/readyz`, `/metrics` | None. Do not expose the API port to untrusted networks. |
| Every other `POST` (all mutations) | When `[cluster].shared_secret` is non-empty: header `X-Dtfs-Shared-Secret: <secret>` or `Authorization: Bearer <secret>`. With an empty secret (lab only) no header is needed. |

Failures: `401` `{"error": "shared_secret required (X-Dtfs-Shared-Secret or Authorization: Bearer)"}` when missing, `403` `{"error": "shared_secret mismatch"}` when wrong. Denials are audited as `auth.denied`.

The three Raft membership endpoints additionally require the secret in the request body (`shared_secret`).

### Leader behaviour

Reads are answered from the receiving manager's local state machine; a follower or learner can lag slightly behind the leader. Mutations commit through Raft:

| Column value | Meaning |
|---|---|
| `leader (409)` | Checked up front. A non-leader answers `409` with `{"error": "not the leader ...", "leader": {"leader_id", "leader_hostname", "leader_addr"}}`. Resend to `leader_addr`'s API. |
| `leader (503)` | Not pre-checked. On a non-leader the Raft write fails and the call answers `503` `{"error": "raft write failed: ..."}`. Send to the leader (`GET /api/v1/raft/state`, field `current_leader`). |
| `any` | Served by whichever manager receives it. |

### Errors

Errors are JSON `{"error": "<message>"}`, sometimes with extra fields (`preserved_current`, `leader`). Codes used: `400` invalid input, `401`/`403` authentication, `404` unknown node/volume/file/chunk, `409` conflict or not leader, `503` Raft write failed (no quorum, or not the leader). Specific `409` cases:

- Volume create when the id already exists.
- Volume update (`set-*`) racing a concurrent update of the same volume: re-read the volume and retry the change.
- `POST /config` version conflict: re-read and retry.
- Node decommission while the node still holds replicas; unquarantine of a node that is not quarantined; promote of a node that is already a voter; maintenance toggle on a quarantined node, disable on a node not in Maintenance, or refused by the state machine.
- Any leader-only call sent to a non-leader.

### Common parameters

- Node ids are decimal `u64` strings (`node_id = hash(hostname)`).
- A `path_prefix` of `""` or `"/"` is treated as empty (matches every file) and requires `allow_empty_prefix: true`.
- Tier names are lowercase `hot`, `warm`, `cold`, `tape`.

## Endpoint index

| Method | Path | Auth | Leader | Purpose |
|---|---|---|---|---|
| GET | `/healthz` | no | any | Liveness (`ok`) |
| GET | `/readyz` | no | any | Readiness (200 or 503) |
| GET | `/metrics` | no | any | Prometheus text |
| GET | `/api/v1/health` | no | any | Simple health JSON |
| GET | `/api/v1/cluster/status` | no | any | Cluster summary |
| GET | `/api/v1/stats` | no | any | Aggregate metrics |
| GET | `/api/v1/memory` | no | any | Manager memory ladder |
| GET | `/api/v1/raft/state` | no | any | Raft view of this manager |
| POST | `/api/v1/raft/add-learner` | yes + body secret | leader (409) | Add learner |
| POST | `/api/v1/raft/promote` | yes + body secret | leader (409) | Learner to voter |
| POST | `/api/v1/raft/remove` | yes + body secret | leader (409) | Remove member |
| GET | `/api/v1/nodes` | no | any | List nodes |
| GET | `/api/v1/nodes/{node_id}` | no | any | Node detail |
| POST | `/api/v1/nodes/{node_id}/maintenance` | yes | leader (503) | Maintenance on/off |
| POST | `/api/v1/nodes/{node_id}/unquarantine` | yes | leader (503) | Clear quarantine |
| POST | `/api/v1/nodes/{node_id}/decommission` | yes | leader (503) | Deregister node |
| POST | `/api/v1/nodes/{node_id}/remove-dead` | yes | leader (409) | Clear a dead node's rows, then deregister |
| GET | `/api/v1/files` | no | any | List files |
| GET | `/api/v1/files/inspect` | no | any | Chunk layout of one file |
| POST | `/api/v1/files/flush_cache` | yes | leader (409) | Drop cache replicas |
| GET | `/api/v1/volumes` | no | any | List volumes |
| POST | `/api/v1/volumes` | yes | leader (503) | Create volume |
| GET | `/api/v1/volumes/{id}` | no | any | Volume detail |
| POST | `/api/v1/volumes/{id}/migrate-policy` | yes | leader (503) | Copy legacy replication |
| POST | `/api/v1/volumes/{id}/set-cache` | yes | leader (503) | Edit cache policy |
| POST | `/api/v1/volumes/{id}/set-mandatory` | yes | leader (503) | Edit mandatory replicas |
| POST | `/api/v1/volumes/{id}/set-write-safety` | yes | leader (503) | Edit write safety |
| POST | `/api/v1/volumes/{id}/set-anchor` | yes | leader (503) | Edit anchor |
| POST | `/api/v1/volumes/{id}/set-spill` | yes | leader (503) | Edit spill policy |
| GET | `/api/v1/chunks` | no | any | Chunk placement listing |
| GET | `/api/v1/chunks/{chunk_id}` | no | any | One chunk |
| GET | `/api/v1/migrations` | no | leader | Migration tasks |
| POST | `/api/v1/migrations` | yes | leader (409) | Start path migration |
| POST | `/api/v1/scrub` | yes | leader (503) | Trigger cluster scrub |
| POST | `/api/v1/scrub/orphans` | yes | leader (409) | Run the orphan reaper once now |
| GET | `/api/v1/scrub/orphans` | no | any (record is per manager) | Last orphan reap |
| GET | `/api/v1/audit` | no | any | Audit log |
| GET | `/api/v1/integrity/faults` | no | any | Integrity-fault feed |
| GET | `/api/v1/config` | no | any | Stored override |
| POST | `/api/v1/config` | yes | leader (503) | Store override |
| GET | `/api/v1/config/effective` | no | any | Flattened effective config |

`GET /api/v1/migrations` reads the scheduler's in-memory task table, which only the leader populates. Each task's `source_node` and `target_node` are decimal strings (u64 ids exceed JS's safe integer range).

## Probes and metrics

- `GET /healthz`: `200 ok` while the process answers.
- `GET /readyz`: `200 ready` when the state machine has applied at least one entry and one registered node is effectively healthy, else `503 not ready: fsm_initialised=<bool> any_healthy=<bool>`.
- `GET /metrics`: Prometheus text format. Series: `dtfs_nodes_healthy`, `dtfs_nodes_suspect`, `dtfs_nodes_down`, `dtfs_nodes_quarantined`, `dtfs_cluster_capacity_bytes`, `dtfs_cluster_used_bytes`, `dtfs_files_total`, `dtfs_chunks_total`, `dtfs_volumes_total`, `dtfs_write_leases_active`, `dtfs_audit_log_in_memory`, `dtfs_tier_migrations_completed_total`, `dtfs_migration_failed_total`, `dtfs_anchor_protected_total`. See [Monitoring](../operations/monitoring.md).
- `GET /api/v1/health`: `200 {"status": "healthy"}` whenever the manager runs.

## Cluster

### GET /api/v1/cluster/status

```json
{
  "status": "ok",
  "cluster_name": "production",
  "node": "mgr-01",
  "data_nodes": 6,
  "data_nodes_healthy": 6,
  "files": 1423,
  "files_missing_chunks": 0,
  "anchor_pending": 0,
  "total_capacity": 12884901888000,
  "used_capacity": 4294967296000,
  "config_version": 3
}
```

`status` is `degraded` when any file has lost its committed data (`files_missing_chunks > 0`; empty and never-written sparse files do not count, see [Glossary](../glossary.md)) or fewer data nodes are effectively healthy than registered, else `ok`. Health is liveness-projected from `raft.node_liveness_secs`. `anchor_pending` counts files whose anchor copy has not landed yet (asynchronous convergence, does not degrade `status`). It is computed at most once per 30 s per manager (shared with `/stats`), so it can lag by up to 30 s.

Upgrade note (v0.1.8): an empty file created before v0.1.8 whose size was never set still counts in `files_missing_chunks` (its history is unknown, so it is treated like lost data) until it is rewritten, truncated to 0 or deleted. To find such files, list files with no chunks and check `total_size`: `dtfs-cli --format json files list --limit 100000 | jq '.files[] | select(.chunk_count == 0)'`, or the same filter over `GET /api/v1/files`. A file you expect to be empty can be fixed with `truncate -s 0 <path>` on the mount. A file you expect to hold data is real loss.

### GET /api/v1/stats

Aggregate metrics for dashboards. Main fields: `health` (`healthy`, `degraded`, `critical`, `unknown`; `critical` also when any node is quarantined), `healthy_nodes`, `quarantined_nodes`, `goal_unreachable_volumes`, `goal_unreachable` (array of `{volume, tier, required, nodes_with_tier}`), `total_nodes`, `anchor_pending`, `chunk_placement` (counts per state: missing, undergoal, misplaced, stable, overgoal, pending_deletion), `infra_healthy`, `infra_total`, `manager_count`, `data_node_count`, `disk_count`, `client_count`, `file_count`, `raw_total_bytes`, `raw_used_bytes`, `net_usable_bytes`, `net_used_bytes`, `logical_bytes`, `phantom_bytes_estimate`, `tier_breakdown` (array of `{tier, disks, total_bytes, used_bytes}`), `io_read_bytes_per_sec`, `io_write_bytes_per_sec` (0 on the first call after manager start), `last_scrub`, `scrub_results_by_node` (and `last_scrub`; each entry's `node_id` is a decimal string, like `/nodes`), `config_version`. `anchor_pending`, `chunk_placement` and `logical_bytes` walk every chunk row, so each manager computes them at most once per 30 s and serves that result in between.

### GET /api/v1/memory

`{"configured": false}` when no budget is wired; otherwise `configured` (false when `hard_cap_mb = 0`), `hard_cap_bytes`, `last_rss_bytes`, `pressure_level` (`Normal`, `Warn`, `Shed`), `is_shedding`, `warn_threshold_bytes`, `shed_threshold_bytes`, `sample_interval_secs`.

### GET /api/v1/raft/state

This manager's view of Raft. Fields: `node_id`, `hostname`, `running_state` (`ok` or `fatal: ...`), `state` (`Leader`, `Follower`, `Candidate`, `Learner`), `is_leader`, `current_term`, `current_leader` (null = no quorum), `vote_term`, `vote_committed`, `last_log_index`, `last_applied_index`, `applied_log_lag`, `snapshot_index`, `voters`, `learners`, `membership` (array of `{node_id, hostname, addr, is_voter}`), `millis_since_quorum_ack` (leader only, else null), `replication` (leader only: array of `{node_id, matched_index, lag}`, else null).

### Raft membership

All three are leader-only (`409` with leader hint otherwise) and need the secret both as header and as `shared_secret` in the body (`403 {"error": "shared_secret mismatch"}`). See [Cluster Management](../operations/cluster-management.md).

| Endpoint | Body | Success response | Errors |
|---|---|---|---|
| `POST /api/v1/raft/add-learner` | `{"hostname", "addr", "shared_secret"}` | `{"status":"ok","node_id","hostname","addr","log_index"}` | `400` missing/wildcard `addr` or a hostname that collides with a registered data or client node; `503` Raft change failed |
| `POST /api/v1/raft/promote` | `{"hostname", "shared_secret"}` | `{"status":"ok","node_id","hostname","log_index"}` | `404` not a member (add-learner first); `409` already a voter; `503` |
| `POST /api/v1/raft/remove` | `{"hostname", "shared_secret"}` | `{"status":"ok","node_id","hostname","was_voter","log_index"}` | `404` not a member; `400` last voter; `503` |

## Nodes

### GET /api/v1/nodes

Array. Entry: `node_id`, `hostname`, `grpc_addr`, `location` (`region/datacenter/rack/host`), `disk_count`, `total_capacity`, `used_capacity`, `total_bytes_read`, `total_bytes_written`, `health` (liveness-projected), `role` (`Manager`, `Data`, `Client`), `last_seen_ms`, `is_leader`, `disks` (array of `{disk_id, uuid, mount_path, tier, total_bytes, used_bytes, status}`), and when present `client_ip`, `version`, `build_date`, `quarantine` (`since_ms`, `reason`, `triggering_chunk_id`, `fault_count_total`, `last_fault_at_ms`, `last_fault_summary`), `raft_member` (managers). Configured but unregistered voters and clients known only through write leases are synthesised into the list.

### GET /api/v1/nodes/{node_id}

`node_id`, `hostname`, `grpc_addr`, `location` (object with `region`, `datacenter`, `rack`, `host`), `disks`, `total_capacity`, `used_capacity`, `health`, `role`. `400` if the id is not a `u64`, `404` if unknown.

### POST /api/v1/nodes/{node_id}/maintenance

Body `{"enable": true}`. Response `{"node_id", "maintenance": <bool>}`. `404` unknown node; `409` when the node is quarantined (unquarantine first), on `{"enable": false}` for a node not in Maintenance, or when the state machine refuses. See [Maintenance Mode](../operations/maintenance-mode.md).

### POST /api/v1/nodes/{node_id}/unquarantine

Body `{"operator_id": "<label>", "reason_acknowledged": "<at least 16 characters>"}`. Response `{"node_id", "ok": true}`. `400` empty operator or short reason; `404` unknown node; `409` node is not quarantined.

### POST /api/v1/nodes/{node_id}/decommission

Body `{"operator_id": "<label>", "reason": "<text, optional>"}`. Response `{"node_id", "ok": true}`. `409` while any file still has a replica on the node (drain or re-replicate first); `404` unknown node.

### POST /api/v1/nodes/{node_id}/remove-dead

Leader only (`409` not leader). Body `{"operator_id": "<label, required>", "reason": "<text>", "accept_data_loss": false, "dry_run": false}`. The node must be `Down` or `Failed` and silent for at least `down_after_missed * heartbeat_interval_secs` (`409` otherwise; `404` unknown node). Rows with another live replica are removed through the guarded `RemoveChunkLocation`; rows that would strand the volume anchor stay (`anchor_pending`); rows with no live replica are `data_loss_candidates`, removed only when `accept_data_loss` is true. The node is deregistered once no rows remain.

Response: `{"node_id", "dry_run", "accept_data_loss", "removable", "anchor_pending", "data_loss_candidates", "sample": {"removable": [...], "anchor_pending": [...], "data_loss": [...]}}` (samples are up to 5 `path#chunk_index` strings), plus, when not a dry run, `removed`, `data_loss_removed`, `failed`, `remaining_rows`, `deregistered` and `deregister_error` on a failed deregister. Safe to repeat.

## Files

### GET /api/v1/files

Query: `limit` (default 1000, clamped to 1..100000), `offset` (default 0), `prefix` (path prefix). Ordered by path.

```json
{
  "count": 1423,
  "total": 12,
  "returned": 10,
  "truncated": true,
  "files": [
    { "path": "/movies/a.mkv", "volume_id": "default", "chunk_count": 3,
      "total_size": 402653184, "created_at": 1710842400000, "updated_at": 1710928800000 }
  ]
}
```

`count` is all files in the cluster, `total` is the number matching `prefix`, `returned` the page size, `truncated` is true when more matches exist beyond `offset + returned`. Page by increasing `offset` until `truncated` is false.

### GET /api/v1/files/inspect

Query `path` (required). Response: `path`, `volume_id`, `total_size`, `chunk_size`, `chunk_count`, `chunks` (array of `{chunk_index, chunk_id, size, node_ids, replicas: [{node_id, disk_id, tier}]}`). `404 {"error": "file not found: ..."}`. `total_size` is the file size readers see (same as `GetFileLocations` and readdir): the explicit size when one was set (truncate, fallocate), else the end of the last chunk (`chunk_index * chunk_size + size`; missing chunks in between read as zeros). The list endpoint's `total_size` is stored chunk bytes instead.

### POST /api/v1/files/flush_cache

Drops cache replicas under a path prefix; mandatory and anchor replicas are never dropped. Leader-only (`409`).

| Body field | Type | Default | Meaning |
|---|---|---|---|
| `path_prefix` | string | required | `""`/`"/"` needs `allow_empty_prefix` |
| `cache_tiers` | list of tier | `["warm","hot"]` | Tiers eligible to flush |
| `cooldown_secs` | u64 | `300` | No re-promotion for this long |
| `max_tasks` | usize | `10000` | Cap on replica deletions per call |
| `allow_empty_prefix` | bool | `false` | Safety gate |

Response: `status` (`accepted`), `path_prefix`, `cache_tiers`, `cooldown_secs`, `files_matched`, `chunks_examined`, `replicas_dropped`, `mandatory_refusals`, `truncated`, `unreachable_nodes`, `rpc_failures`, `raft_failures`. A `200` can still report partial failure: check `rpc_failures`, `raft_failures`, `mandatory_refusals` and `truncated` and re-run if non-zero. `400` for an invalid tier or a gated empty prefix.

## Volumes

Volume ids are lowercase names with spaces replaced by `-`. The field reference is in [Volume Configuration](../configuration/volume-config.md).

### GET /api/v1/volumes

`{"count": N, "volumes": [ ... ]}`. Each element is the volume projection below plus `file_count` and `total_size`. A volume referenced by files but without a stored config is listed with only `volume_id`, `file_count`, `total_size`.

### GET /api/v1/volumes/{id}

```json
{
  "volume_id": "default",
  "name": "default",
  "chunk_size_bytes": 4194304,
  "replication": { "hot": 3, "warm": 2, "cold": 1, "tape": 1 },
  "mandatory_replicas": { "hot": 0, "warm": 0, "cold": 1, "tape": 0 },
  "cache_replicas": {
    "hot":  { "min": 0, "max": 0 }, "warm": { "min": 1, "max": 1 },
    "cold": { "min": 0, "max": 0 }, "tape": { "min": 0, "max": 0 },
    "per_dc": true, "promote_reads": 2, "promote_window_secs": 14400, "cooldown_secs": 300
  },
  "anchor": { "datacenter": "leli", "required_tier": "cold" },
  "write_safety": { "min_replicas_before_ack": 2, "write_timeout_secs": 30 },
  "spill": { "allow": null, "forbidden_tiers": [], "forbidden_datacenters": [] }
}
```

`mandatory_replicas` shows the effective floor (the legacy `replication` while the stored field is all zero). `anchor` is null when unset. `404` for an unknown id.

### POST /api/v1/volumes

| Body field | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | required | Becomes the id (lowercased, spaces to `-`) |
| `chunk_size_mb` | u32 | `[defaults].chunk_size_mb` | Honoured per volume. Must be 1 to 64 (`0` = default); otherwise `400 {"error": "chunk_size_mb N is out of range: must be 1..=64 MiB"}`. |
| `replication_hot`, `replication_warm`, `replication_cold` | u32 | `[defaults.replication]` | Legacy per-tier counts |
| `anchor_datacenter` | string | none (`[defaults].anchor_datacenter` is not applied here) | Anchor datacenter |
| `force` | bool | `false` | Allow an anchor datacenter that no registered node reports |

Response `{"volume_id", "name", "status": "created"}`. `409` if the id exists, including when a concurrent create wins. The tape count starts at 0. `400` if `anchor_datacenter` is unknown to the cluster (case-insensitive) and `force` is not `true`. Unknown body fields are ignored. Write safety comes from `[defaults.write_safety]`; mandatory and cache policy start empty (set them with the endpoints below).

### Volume edit endpoints

All take a JSON body in which omitted fields keep their value, and answer `{"volume_id", "changed": <bool>, "config": <volume projection>}` (`changed:false` is a no-op). Common errors: `404` unknown volume, `400` validation message, `409` when a concurrent update of the same volume wins (re-read with `GET /api/v1/volumes/{id}`, reapply, retry), `503` Raft failure.

| Endpoint | Body fields | `400` cases |
|---|---|---|
| `set-cache` | `warm_min`, `warm_max`, `hot_min`, `hot_max`, `cold_min`, `cold_max`, `tape_min`, `tape_max` (u32); `per_dc` (bool); `promote_reads` (u32); `promote_window_secs`, `cooldown_secs` (u64) | `max < min` for any tier |
| `set-mandatory` | `hot_replicas`, `warm_replicas`, `cold_replicas`, `tape_replicas` (u32); unknown fields rejected | result all zero; zero on the anchor's `required_tier`; non-zero `tape` without a tape disk |
| `set-write-safety` | `min_replicas_before_ack`, `write_timeout_secs` (u32) | value below 1 |
| `set-anchor` | `datacenter` (string; `""` clears the anchor), `required_tier` (`hot`/`warm`/`cold`/`tape`/`""`), `force` (bool) | unknown tier; `mandatory_replicas.<tier> = 0`; datacenter unknown to the cluster without `force: true` |
| `set-spill` | `allow` (bool), `forbidden_tiers` (list), `forbidden_datacenters` (list) | unknown tier name |

`POST /api/v1/volumes/{id}/migrate-policy` (no body) copies the legacy `replication` into `mandatory_replicas` once. Response `{"volume_id", "changed", "mandatory_replicas"}`; idempotent.

## Chunks

### GET /api/v1/chunks

Query: `state` = `missing`, `undergoal`, `misplaced`, `overgoal`, `pending_deletion` (omit for all; other values `400`), `limit` (default 200, clamped to 1..5000).

```json
{ "total": 2, "limit": 200, "truncated": false,
  "chunks": [ { "chunk_id": "ab12...", "path": "/media/a.mkv", "volume_id": "default",
                "chunk_index": 0, "state": "undergoal",
                "replicas": [ { "node_id": "1", "disk_id": 0, "tier": "warm", "datacenter": "toro",
                                "node_health": "healthy", "disk_status": "healthy" } ] } ] }
```

`total` counts all matches before truncation. Replica fields are null when the node or disk is no longer registered. `node_health` and `disk_status` are lowercase strings.

### GET /api/v1/chunks/{chunk_id}

Hex id, case-insensitive (`400` otherwise, `404` when no file references it). Response `{chunk_id, state, pending_deletion, files: [{path, chunk_index}], replicas: [...]}`.

## Migrations

### POST /api/v1/migrations

Enqueue tier or datacenter moves for every chunk under a path prefix. Leader-only (`409`).

| Body field | Type | Default | Meaning |
|---|---|---|---|
| `path_prefix` | string | required | `""`/`"/"` needs `allow_empty_prefix` |
| `target_tier` | string | required | `hot`, `warm`, `cold`, `tape` (`400` otherwise) |
| `target_dc` | string | none | Constrain the target node's datacenter |
| `allow_empty_prefix` | bool | `false` | Safety gate |
| `max_tasks` | usize | `10000` | Soft cap |

Response: `status` (`accepted`), `path_prefix`, `target_tier`, `target_dc`, `files_matched`, `chunks_total`, `already_satisfied`, `tasks_scheduled`, `no_source`, `no_target`, `truncated_chunks`, `examples`. The anchor replica is never moved off the anchor. See [Migration](../storage/migration.md).

### GET /api/v1/migrations

`{"count": N, "tasks": [ ... ]}`: pending and recent tasks held by the scheduler (leader).

## Scrub

### POST /api/v1/scrub

Body `{"level": "quick" | "deep"}` (default `deep`). Every data node runs the scrub once on its next heartbeat. Per-target scrubs are not implemented: any `target_type` other than `all` or `target` other than `*` returns `400`. Response `{"status": "accepted", "target_type": "all", "target": "*", "level", "id"}`. Results appear in `GET /api/v1/stats` (`scrub_results_by_node`, `last_scrub`). See [Scrubbing](../storage/scrubbing.md).

### POST /api/v1/scrub/orphans

Runs the orphan reaper once, now, on the leader, with a one-time grace. Body `{"grace_secs": <u64, optional>, "dry_run": <bool, default false>}`; `grace_secs` defaults to the configured `scrub.orphan_grace_secs`. The sweep is the periodic one (same guards: leader re-check before every delete, disk-keyed rows, data-node mtime guard); a dry run deletes nothing and proposes no Raft write (it also skips the in-state-missing reconcile/quarantine), it only counts candidates. Returns `202` `{"status": "accepted", "trigger": "manual", "grace_secs", "dry_run"}` and runs in the background.

| Status | When |
|---|---|
| `400` | `grace_secs` below the manual floor of 3600 s |
| `409` `{"error": "an orphan reap is already running"}` | a reap (manual or timer) is running; the timer skips its tick while a manual run is active |
| `409` `{"error": "...not the leader...", "leader": {...}}` | sent to a follower |
| `401` / `403` | `cluster.shared_secret` set and the header missing / wrong |

Audited as `scrub.orphans` (target `grace_secs=N dry_run=B`) once with `accepted` and once when the run ends with `candidates=N reaped=N` or `error: ...`. Each deleted chunk keeps its own `orphan.reaped` row.

### GET /api/v1/scrub/orphans

`{"running", "is_leader", "configured_grace_secs", "manual_grace_floor_secs": 3600, "last_run"}`. `last_run` is `null` or the last (or running) reap on this manager, manual or timer: `trigger` (`manual` | `timer`), `grace_secs`, `dry_run`, `started_ms`, `finished_ms` (`null` while running), `candidates` (reap candidates past grace), `reaped`, `bytes_freed` (`null` on a dry run: `ListChunks` reports no sizes), `nodes` (per node id: `listed`, `candidates`, `reaped`, `delete_failed`, `bytes_freed`, `error`) and `error`. A sweep that did not run or was cut short reports `error` (`sweep skipped: not leader`, `sweep skipped: state machine not caught up`, `sweep skipped: empty namespace view`, `aborted: lost leadership after reaping N`), never a clean `reaped: 0`. The record is process-local: query the leader.

## Audit and integrity

### GET /api/v1/audit

Query: `limit` (default 50), `action` (prefix filter). Newest first. Entry: `timestamp` (Unix seconds), `actor`, `action`, `target`, `result`, `summary`, `origin_manager` (the last two are populated when `[audit] replicate_via_raft` is on). See [Audit Log](../security/audit-log.md).

### GET /api/v1/integrity/faults

Same query (`limit` default 200) and entry shape, filtered to integrity actions (`node.quarantine`, `node.unquarantine`, `read.integrity_fault`, `orphan.in_state_missing`). Always reads the replicated audit log (where `read.integrity_fault` and `node.quarantine` land) and, with `audit.replicate_via_raft = false`, merges in this manager's local ring, newest first.

## Config

See [Cluster-wide Configuration Override](../configuration/cluster-config.md) for semantics. No daemon applies the stored override at runtime.

### GET /api/v1/config

`{"version": <u64>, "config": <stored override, secrets as "***">, "defaults": <[defaults] with override applied>, "tiering": <[tiering] with override applied>}`.

### POST /api/v1/config

Body `{"config_json": "<JSON string>", "write_to_disk": false, "acknowledge_cluster_wide": false}`. The incoming JSON is deep-merged onto the stored override (omitted keys keep their stored value; an empty `config_json` resets it). Response `{"version", "status": "stored", "note", "write_to_disk": <bool actually written>}`: the override is stored cluster-wide in Raft and takes effect after each daemon restarts.

- `400`: invalid JSON, node-scoped key in the override, merged config does not type-check, or `write_to_disk` without `acknowledge_cluster_wide`. Body carries `"preserved_current": true`.
- `409`: another update advanced the version first (re-read, retry).
- `write_to_disk` rewrites `/etc/dtfs/manager.toml` on this manager only (atomic, mode 0600). A failed disk write returns `500` with `{"error", "version", "raft_stored": true, "write_to_disk": false}` and audits result `partial`: the Raft write has already happened, the file was not changed.

### GET /api/v1/config/effective

`{"scope": "manager", "node_hostname", "entries": [ {name, scope, scope_kind, current, default, is_default} ]}`. Secrets (`shared_secret`, `secret`, `token`, `password`) and alert webhook URLs are returned as `"***"` when non-empty. `default` is an indicative baseline and may differ from the compiled-in defaults; use the configuration pages.

## Join

There is no REST join. Data nodes and clients register over gRPC `RegisterNode`; see [Cluster Join](../security/cluster-join.md).
