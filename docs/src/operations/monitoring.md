# Monitoring

DTFS exposes metrics and health endpoints for integration with monitoring systems such as Prometheus, Grafana, and custom dashboards.

## Health Endpoints

All on the manager REST port (default 9402), no authentication for reads.

| Endpoint | Meaning |
|----------|---------|
| `GET /healthz` | Liveness: `200 ok` whenever the process answers. |
| `GET /readyz` | Readiness: `200` only when the state machine has applied at least one log entry **and** at least one node is healthy; otherwise `503` with the reason. A fresh cluster with no data node yet is not ready. |
| `GET /api/v1/health` | `{"status":"healthy"}`, same liveness meaning. `dtfs-cli health` wraps it. |
| `GET /api/v1/raft/state` | Raft view of this manager: `running_state` (`ok` or `fatal: ...`), `state`, `is_leader`, `current_leader` (null = no quorum), `voters`, `learners`, `membership`, `applied_log_lag`, `millis_since_quorum_ack`, and on the leader per-peer `replication` lag. |
| `GET /api/v1/cluster/status` | Cluster summary, see below. |
| `GET /api/v1/nodes` | Array of nodes with `health`, `role`, `version`, `build_date`, `disk_count`, capacity and `last_seen_ms`. |

`/api/v1/cluster/status`:

```json
{
  "status": "ok",
  "cluster_name": "production",
  "node": "mgr-01",
  "data_nodes": 12,
  "data_nodes_healthy": 12,
  "files": 48293,
  "files_missing_chunks": 0,
  "anchor_pending": 0,
  "total_capacity": 107374182400,
  "used_capacity": 53687091200,
  "config_version": 42
}
```

`status` is `degraded` when any file has lost its committed data
(`files_missing_chunks > 0`) or fewer data nodes are healthy than registered.
Empty files and sparse files that were never written have no chunks and do
not count ([Glossary](../glossary.md)).
`anchor_pending` counts files whose anchor copy has not landed yet (asynchronous
convergence, not data loss, does not degrade `status`). The post-deploy gate
`scripts/dtfs-cluster-healthcheck.sh` asserts exactly these fields.

## Prometheus Metrics

Only the manager exposes `GET /metrics` (port 9402, text exposition format). Data
nodes and FUSE clients have no metrics endpoint in this revision; monitor them
through the manager's node table, `journalctl`, and host-level exporters
(node_exporter, SMART).

```yaml
scrape_configs:
  - job_name: 'dtfs-managers'
    static_configs:
      - targets: ['mgr-01:9402', 'mgr-02:9402', 'mgr-03:9402']
```

Metrics are computed from the local state machine, so every manager serves the same
cluster-wide values; scrape all of them to also detect a manager that is down or
lagging.

| Metric | Type | Description |
|--------|------|-------------|
| `dtfs_files_total` | gauge | Files tracked |
| `dtfs_chunks_total` | gauge | Chunk-location entries (replicas), not unique chunks |
| `dtfs_volumes_total` | gauge | Configured volumes |
| `dtfs_nodes_healthy` | gauge | Nodes in `Healthy` state (data nodes, managers registered in the node table, clients) |
| `dtfs_nodes_suspect` | gauge | Nodes `Suspect` |
| `dtfs_nodes_down` | gauge | Nodes `Down` |
| `dtfs_nodes_quarantined` | gauge | Nodes `Quarantined` after an integrity fault |
| `dtfs_cluster_capacity_bytes` | gauge | Total capacity reported by nodes |
| `dtfs_cluster_used_bytes` | gauge | Used capacity |
| `dtfs_write_leases_active` | gauge | Held single-writer leases |
| `dtfs_anchor_protected_total` | counter | Tiering decisions refused to protect the anchor |
| `dtfs_tier_migrations_completed_total` | counter | Promote and demote events in the in-memory audit ring |
| `dtfs_migration_failed_total` | counter | Migration tasks that exhausted retries (audit ring) |
| `dtfs_audit_log_in_memory` | gauge | Entries in the capped audit ring |

The leader also exports the cache series: `dtfs_cache_disk_used_bytes`,
`dtfs_cache_disk_total_bytes` and `dtfs_cache_oldest_last_read_age_seconds`
(`{node,disk,dc,tier}`, refreshed every 60 s), `dtfs_cache_removed_total{reason}`,
`dtfs_cache_fill_total{dc}`, `dtfs_cache_fill_bytes_total{dc}`,
`dtfs_cache_scheduled_total{dc,kind}`, `dtfs_link_admitted_bytes_total{to_dc,kind}`
and `dtfs_link_tokens_bytes{to_dc}`. The counters are per manager process:
they restart at 0 with the manager, a new leader starts from 0 and a former
leader's counters stop moving, so alert on `increase()` per instance. Only the
current leader exports the `dtfs_cache_disk_*` and oldest-age gauges.
See [Observing the cache](../storage/tiering.md#observing-the-cache).

The last three counters above are derived from a bounded audit ring, so they can drop
when old entries are evicted; alert on `increase()` over short windows, not on
absolute values. A node in `Maintenance` is counted in none of the node gauges.
Anything else (Raft lag, per-disk usage, scrub results) comes from the REST
endpoints above or `GET /api/v1/stats`.

## InfluxDB push and dashboards

The Prometheus section above is unchanged. Separately, every role (manager, data node,
FUSE client) can push its own internal metrics to InfluxDB v1 or v2 as line protocol,
which is what the dashboards in `deploy/grafana/` read.

### Enable

Add a `[metrics]` block with one or more `[[metrics.servers]]` to `manager.toml`,
`data.toml` and `fuse.toml`. Without a server nothing is pushed. Keys and defaults:
[Manager `[metrics]`](../configuration/manager.md#metrics-optional) (the data node and
FUSE pages point there). The push carries the global tags `role` (`manager`, `data`,
`fuse`), `node` (`[node].hostname`, for FUSE `[location].host`) and `dc`.

`[metrics]` is per process, read at startup only, and not distributed through the
cluster. Two modes (`mode`):

- `direct` (default): the process pushes to its own `[[metrics.servers]]`; without one
  it pushes nothing. The host needs the block and a network path to InfluxDB.
- `proxy` (data nodes and FUSE clients only, no `[[metrics.servers]]`): the process
  sends its line-protocol batches to a manager with the `PushMetrics` gRPC call, and the
  manager forwards them with its own. Only the managers need InfluxDB settings and
  access. Data nodes start at the manager that last accepted (first the first one of
  `[cluster]`), FUSE clients at their active manager; on a connect error, `Unavailable`, `Unimplemented`,
  `FailedPrecondition`, `DeadlineExceeded`, `Cancelled` or `Unknown` the node tries the next
  configured manager. When every manager fails, the batches stay in the node's bounded
  queue (`buffer_max_kb`, drops counted in `dtfs_metrics_dropped_batches{server="proxy"}`)
  for the next flush. With `cluster.shared_secret` set the call carries the
  `dtfs-mgr-rpc` HMAC (binding: BLAKE3 of the payload). Mixed versions: a manager
  older than this feature answers `Unimplemented` and the node moves on; at least one
  manager must be upgraded and have servers.

A manager accepts proxied batches only when it has at least one `[[metrics.servers]]`
(else `FailedPrecondition`). It refuses an unknown encoding, a payload that decompresses
past 16 MiB, non-UTF-8 text and any non-empty line not starting with `dtfs_`, and appends
an accepted batch to every server's queue. Those queues are the same `buffer_max_kb`
buffers, so raise it on proxy managers to roughly nodes x snapshot size x
`flush_interval_secs` / `push_interval_secs` (more if InfluxDB outages should be
bridged).

`flush_interval_secs` (default `0` = every snapshot) takes snapshots every
`push_interval_secs` but sends only every flush interval (rounded up to whole
snapshots), joining the queued batches into bodies of at most 4 MiB (one POST or
`PushMetrics` call per destination per flush unless that is exceeded).
`compression = "gzip"` gzips the InfluxDB POST body (`Content-Encoding: gzip`) or the
`PushMetrics` payload.

Configure all managers, not one: the leader-only series (`raft_follower_lag`,
`raft_millis_since_quorum_ack`, scheduler gauges) come from whichever manager is leader
and go blank when leadership moves to an unconfigured one, and in proxy mode every
configured manager is a failover target for the nodes.

### Line protocol

- Measurement `dtfs_<name>`; tags are `role`, `node`, `dc` plus the metric's own labels
  (tags with an empty value are omitted). A label named `role`, `node` or `dc` would
  duplicate a global tag, so the encoder emits it as `label_<name>`; the catalogue labels
  below avoid those names (`src_dc`, `target_dc`, `reader_dc`).
- Counters and gauges have one integer field `value`. Counters are cumulative since
  process start: rate them (InfluxQL `non_negative_derivative(..., 1s)`, Flux
  `derivative(nonNegative: true)`), so a restart does not show as a negative spike.
- Histograms have the cumulative fields `count` and `sum` (microseconds for `*_us`) and
  the per-push-interval fields `p50`, `p90`, `p99`, `max`. The quantiles are power-of-two
  bucket upper bounds clamped to the interval maximum (a few tens of percent coarse), and
  are absent for an interval with no observation.

### Import the dashboards

Two sets with the same five dashboards, panels and variables (`dc`, `node`, `disk`):

| Folder | Query language | Grafana datasource |
|---|---|---|
| `deploy/grafana/influxdb-v1/` | InfluxQL | InfluxDB, query language InfluxQL (InfluxDB v1) |
| `deploy/grafana/influxdb-v2/` | Flux | InfluxDB, query language Flux (InfluxDB v2); pick the bucket in the `bucket` variable |

In Grafana: Dashboards, New, Import, upload the JSON files of one folder, and choose the
datasource in the `DS` variable at the top of each dashboard. Dashboards: `write-path`
(FUSE write, PutChunk, client allocate/commit, data-node put stages, Raft log fsync, to
answer whether the Raft fsync is the write bottleneck), `read-path`, `raft-manager`,
`data-node` (per disk) and `memory-health`. The files use dashboard schema version 41
(the `DASHBOARD_SCHEMA_VERSION` of Grafana 11.6). `tests/grafana_dashboards.rs` in
`dtfs-common` fails when a dashboard queries a measurement that no crate registers, or
when the two sets drift apart.

### Overhead model

The hot path only does relaxed atomic adds (counter, gauge, one histogram bucket); no
locks, allocation or I/O. Handles are registered once and cached. Label values are
bounded: fixed enums (FUSE ops, errnos, gRPC and REST route names from allowlists,
anything else is `other`) or capped sets (FUSE tier/DC pairs, replication peers, forward
ops) that fold into `other` past the cap. Encoding runs on a background task every
`push_interval_secs`, the send every flush interval. Batches wait in a per-server (or
`proxy`) FIFO capped at `buffer_max_kb`; the oldest batches are dropped first and counted in
`dtfs_metrics_dropped_batches{server}`. Nothing ever blocks a request.

### Metric catalogue

Measurement = `dtfs_` + name. C = counter, G = gauge, H = histogram. Every series also
carries `role`, `node`, `dc`. The manager's Raft, state-machine and memory gauges are
sampled every 5 s.

**All roles**

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `metrics_dropped_batches` | C | `server` | Batches dropped from a full retry queue (`server="proxy"` in proxy mode) |
| `metrics_proxy_sent_bytes` | C | | Data/FUSE in proxy mode: payload bytes (as sent, compressed with gzip) a manager accepted |
| `metrics_proxy_received_batches` | C | | Manager: `PushMetrics` batches accepted |
| `metrics_proxy_received_bytes` | C | | Manager: payload bytes (as received) of accepted batches |
| `metrics_proxy_rejected` | C | `reason` | Manager: refused `PushMetrics` batches; `auth`, `no_servers`, `bad_encoding`, `too_large`, `bad_line` |

**Manager**

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `raft_log_write_us` | H | | Raft log append plus fsync |
| `raft_append_entries` | H | | Entries per log append |
| `raft_append_bytes` | H | | Bytes per log append |
| `raft_vote_write_us` | H | | Vote persistence |
| `raft_apply_us` | H | | State-machine apply of one log entry (the leader's response wait excluded) |
| `raft_client_write_us` | H | | `client_write` on the local Raft: proposal to applied (leader wait excluded) |
| `raft_write_or_forward_us` | H | | Write served locally or forwarded to the leader |
| `raft_forward_us` | H | | Forward round trip (follower side) |
| `raft_forward_failed` | C | | Failed forwards |
| `raft_forward_received` | C | | Forwarded writes received (leader side) |
| `raft_forwarded` | C | `op` | Forwards by ClientRequest variant |
| `raft_current_term`, `raft_last_log_index`, `raft_last_applied`, `raft_snapshot_index` | G | | Raft positions |
| `raft_apply_lag` | G | | `last_log_index - last_applied` of this manager |
| `raft_follower_lag` | G | `peer` | Leader only (0 elsewhere): `last_log_index` minus that follower's matched index |
| `raft_millis_since_quorum_ack` | G | | Leader only (0 elsewhere): ms since the last quorum acknowledgement |
| `raft_is_leader`, `raft_has_leader` | G | | 1/0 |
| `raft_leader_changes` | C | | Leader changes seen by this manager |
| `grpc_server_requests`, `grpc_server_errors` | C | `method` (+`code` on errors) | ManagerService / Raft transport calls |
| `grpc_server_us` | H | `method` | gRPC handler latency |
| `grpc_server_inflight` | G | `method` | In-flight calls |
| `rest_requests` | C | `route`, `status` (`2xx`..) | REST requests |
| `rest_us` | H | `route` | REST latency |
| `write_alloc_chunks`, `write_commit_chunks` | H | | Chunks per allocate / commit call |
| `write_alloc_rejects`, `write_commit_rejects` | C | `reason` | Rejected allocations / commits |
| `write_lease_granted`, `write_lease_denied` | C | | Write leases |
| `scheduler_tasks_created` | C | `kind` | Repair/replication/anchor-lag tasks created |
| `scheduler_tasks_finished` | C | `status` (`done`, `failed`) | Finished tasks |
| `scheduler_replication_outcomes` | C | `outcome` | Replication scheduling outcomes |
| `scheduler_tick_us` | H | | Scheduler tick |
| `scheduler_repair_budget_hits` | C | | Repair budget exhausted |
| `tiering_tick_us`, `precache_tick_us`, `tiering_capacity_evict_tick_us`, `tiering_retention_tick_us` | H | | Tiering planners |
| `tiering_promote_copies`, `precache_copies` | C | `target_dc` | Cache copies scheduled by planner |
| `precache_scheduled_bytes` | C | | Bytes scheduled by pre-cache |
| `cache_fill_commits`, `cache_fill_bytes` | C | `reader_dc` | Fill-on-read registrations |
| `cache_removed` | C | `reason` | Cache copies removed |
| `orphan_reap_runs` | C | `result` | Orphan reap runs |
| `orphan_reap_candidates`, `orphan_reap_chunks`, `orphan_reap_freed_bytes` | C | | Reaper totals |
| `orphan_reap_us` | H | | Reap run duration |
| `monitor_node_transitions` | C | `to` | Node health transitions |
| `sm_files`, `sm_nodes` | G | | State-machine sizes |
| `sm_nodes_by_health` | G | `health` | Nodes per health state |
| `memory_rss_bytes`, `memory_hard_cap_bytes`, `memory_warn_bytes`, `memory_shed_bytes` | G | | RSS and ladder thresholds |
| `memory_pressure_level` | G | | 0 normal, 1 warn, 2 shed |

**Data node**

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `data_put_total_us` | H | `disk`, `tier` | PutChunk, whole call |
| `data_put_recv_us`, `data_put_tmp_write_us`, `data_put_hash_us`, `data_put_fsync_dat_us`, `data_put_rename_us`, `data_put_meta_us`, `data_put_commit_us` | H | `disk`, `tier` | Put stages |
| `data_put_chunk_bytes`, `data_get_chunk_bytes` | H | `disk`, `tier` | Chunk sizes |
| `data_put_bytes_total`, `data_puts_total` | C | `disk`, `tier` | Bytes and count stored |
| `data_get_read_us`, `data_get_send_us`, `data_get_verify_us`, `data_get_total_us` | H | `disk`, `tier` | Get stages and total |
| `data_get_bytes_total`, `data_gets_total` | C | `disk`, `tier` | Bytes and count served |
| `data_get_data_loss_total` | C | `disk`, `tier` | Reads that failed verification |
| `data_scrub_chunks_total`, `data_scrub_bytes_total`, `data_scrub_throttle_us_total` | C | `disk`, `tier` | Scrub progress, time spent throttled |
| `data_scrub_corrupt_total`, `data_scrub_missing_total`, `data_scrub_size_mismatch_total`, `data_scrub_meta_repaired_total`, `data_scrub_io_errors_total` | C | `disk`, `tier` | Scrub findings |
| `data_quarantines_total` | C | `disk`, `tier` | Chunks quarantined |
| `data_disk_total_bytes`, `data_disk_used_bytes`, `data_disk_free_bytes`, `data_disk_failed` | G | `disk`, `tier` | Disk capacity, failed flag |
| `data_put_inflight`, `data_get_inflight` | G | | In-flight RPCs |
| `data_put_slot_wait_us` | H | | Wait for a put slot |
| `data_put_shed_total` | C | | Puts shed under memory pressure |
| `data_put_rejects_total` | C | `method`, `code` | Rejected puts by gRPC code |
| `data_get_not_found_total` | C | | Gets for an unknown chunk |
| `data_heartbeat_us` | H | | Heartbeat to the manager |
| `data_heartbeat_failures_total`, `data_heartbeat_rejected_total` | C | | Heartbeat failures / refusals |
| `data_mem_rss_bytes`, `data_mem_cap_bytes`, `data_mem_pressure_level` | G | | RSS, cap, 0/1/2 pressure |
| `data_store_chunks`, `data_store_bytes` | G | | Store totals |
| `data_replicate_out_bytes_total`, `data_replicate_out_total`, `data_replicate_out_failures_total` | C | `peer` | Outgoing replication |
| `data_replicate_out_us` | H | `peer` | Outgoing replication latency |
| `data_grpc_requests_total` | C | `method` | gRPC requests |
| `data_grpc_latency_us` | H | `method` | gRPC latency |
| `data_grpc_inflight` | G | `method` | In-flight gRPC calls |
| `data_grpc_errors_total` | C | `method`, `code` | gRPC calls that returned an error, by status code |

**FUSE client** (`role=fuse`; the client library metrics are pushed by the same process)

| Name | Type | Labels | Meaning |
|---|---|---|---|
| `fuse_op_us` | H | `op` | Latency per FUSE op |
| `fuse_op_errors` | C | `op`, `errno` | Failed ops (`other` for unlisted errnos) |
| `fuse_meta_cache_hits`, `fuse_meta_cache_misses` | C | | Metadata cache |
| `fuse_meta_cache_entries` | G | | Metadata cache size |
| `fuse_chunk_cache_hits`, `fuse_chunk_cache_hit_bytes` | C | `layer` (`readahead`, `disk`) | Chunk cache hits |
| `fuse_chunk_cache_misses` | C | | Chunk cache misses |
| `fuse_readahead_issued`, `fuse_readahead_failed`, `fuse_readahead_bytes` | C | | Readahead activity |
| `fuse_readahead_get_us` | H | | Readahead GetChunk latency |
| `fuse_readahead_evicted_bytes` | C | `reason` (`budget`, `drop`) | Readahead bytes discarded |
| `fuse_readahead_cache_bytes`, `fuse_readahead_budget_bytes`, `fuse_readahead_pending` | G | | Readahead cache state |
| `fuse_readahead_mode` | G | `mode` | Current readahead mode (1) |
| `fuse_readahead_window_chunks` | G | | Prefetch window |
| `fuse_singleflight_merges` | C | `kind` | Reads merged onto an in-flight fetch |
| `fuse_singleflight_owner_failed` | C | | Merged fetch whose owner failed |
| `fuse_read_failovers` | C | `reason` (`integrity`, `timeout`, `error`) | Replica failovers |
| `fuse_read_verify_failures` | C | | Client-side BLAKE3 mismatches |
| `fuse_get_chunk_us`, `fuse_put_chunk_us` | H | `tier`, `src_dc` | Chunk transfer latency |
| `fuse_get_chunk_bytes`, `fuse_put_chunk_bytes` | C | `tier`, `src_dc` | Chunk transfer bytes |
| `fuse_get_chunk_failures`, `fuse_put_chunk_failures` | C | `tier`, `src_dc` | Chunk transfer failures |
| `fuse_read_replica_chosen` | C | `tier`, `src_dc` | Replica selection |
| `fuse_disk_cache_bytes`, `fuse_disk_cache_max_bytes` | G | | Disk cache size and bound |
| `fuse_write_buffered_bytes`, `fuse_write_dirty_cap_bytes` | G | | Write buffer and its cap |
| `fuse_write_cap_waits` | C | | Writers blocked at the dirty cap |
| `fuse_write_cap_wait_us` | H | | Time blocked at the dirty cap |
| `fuse_async_inflight_batches` | G | | Background upload batches |
| `fuse_async_batch_commit_us` | H | | Background batch commit |
| `fuse_commits` | C | `trigger` (`batch_full`, `timer`, `dirty_cap`, `close`) | Commits by trigger |
| `fuse_cache_fill_issued`, `fuse_cache_fill_failed` | C | | Fill-on-read |
| `fuse_put_ladder_retries` | C | | PutChunk target ladder retries |
| `client_allocate_chunks_us`, `client_commit_chunk_locations_us` | H | | Manager calls from the client |
| `client_allocate_chunks_failures`, `client_commit_chunk_locations_failures` | C | | Their failures |
| `client_put_chunk_retries` | C | | PutChunk retries |

The FUSE client exports no RSS or memory-pressure gauge; the `memory-health` dashboard
shows its cache sizes against their bounds instead.

## Node Health Monitoring

The manager tracks data node health through periodic heartbeats:

| Configuration | Default | Description |
|---------------|---------|-------------|
| `monitoring.heartbeat_interval_secs` | 5 | Heartbeat interval |
| `monitoring.suspect_after_missed` | 3 | Missed heartbeats before a node becomes suspect (15 s with defaults) |
| `monitoring.down_after_missed` | 6 | Missed heartbeats before node is marked down (30s default) |

### Health State Transitions

```
Healthy -> Suspect (after 3 missed heartbeats)
Suspect -> Healthy (heartbeat received)
Suspect -> Down    (after 6 total missed heartbeats)
Down    -> Healthy (node restarts and heartbeats resume)
```

Operator-set `Maintenance` and `Quarantined` (and `Draining`) are sticky: the monitor never marks such a node `Down`, and a heartbeat does not clear them. Only data nodes are tracked by the monitor.

Only the Raft leader judges liveness, and it counts missed heartbeats from the later of the node's last committed heartbeat and the moment it became leader. After a leader change (for example a rolling manager restart, during which no heartbeat can commit) a data node is therefore not marked `Suspect`/`Down` until the new leader has itself waited the full interval; a node that really died during the election is detected up to one interval later than otherwise.

When a node transitions to `Down`, the manager begins re-replicating its chunks to maintain the configured replication factor.

## Key Dashboards

Grafana panels worth building on the manager metrics and a small JSON poller of the
REST endpoints:

- **Cluster:** capacity used vs total, node health gauges, files and replicas,
  `anchor_pending`, `files_missing_chunks`.
- **Per node** (from `/api/v1/nodes` or `/api/v1/stats`): disk usage per disk,
  bytes read and written, last scrub result per level.
- **Raft:** which manager is leader (`/api/v1/raft/state`), `applied_log_lag`,
  term changes (rare in a healthy cluster), per-peer replication lag.
- **Host level (not DTFS):** disk SMART, free space, memory vs the systemd
  `MemoryMax` of each role, clock offset.

## Alerting Integration

See [Alerting](./alerting.md) for configuring alerts based on these metrics. Common alert conditions include:

- Node health transitions (Healthy -> Suspect -> Down).
- Disk capacity exceeding 80% or 90%.
- Scrub discovering corrupt chunks (a quarantined node: `dtfs_nodes_quarantined > 0`).
- Raft leader changes (flapping).

## WebUI

The manager includes a built-in WebUI served on the same port as the REST API (default: `9402`). The WebUI provides a visual cluster dashboard with:

- Real-time node and disk status.
- File browser.
- Configuration viewer.
- Audit log viewer.

Access the WebUI at `http://<manager>:9402/` in a web browser.

## Log Monitoring

The daemons log through `tracing` in the default text format to stdout, so under
systemd the logs are in the journal (`journalctl -u dtfs-manager -u dtfs-data`).
Verbosity is set with `RUST_LOG` (units default to `dtfs=info`; override in a
drop-in). JSON output is not implemented in this revision, see
[Log Shipping](./log-shipping.md).
