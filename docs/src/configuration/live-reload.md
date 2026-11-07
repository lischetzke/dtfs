# Live Reload

DTFS supports live configuration reload for a subset of options. Changes to these options take effect without restarting any process. This page explains which options support live reload, and how the mechanism works.

## Options That Support Live Reload

The following tables summarize every option that can be changed at runtime. Options not listed here require a process restart.

### Manager Node (`manager.toml`)

| Section | Option | TOML Path |
|---------|--------|-----------|
| `[api]` | `cors_origins` | `api.cors_origins` |
| `[defaults]` | `chunk_size_mb` | `defaults.chunk_size_mb` |
| `[defaults.write_safety]` | `min_replicas_before_ack` | `defaults.write_safety.min_replicas_before_ack` |
| `[defaults.write_safety]` | `write_timeout_secs` | `defaults.write_safety.write_timeout_secs` |
| `[defaults.replication]` | `hot` | `defaults.replication.hot` |
| `[defaults.replication]` | `warm` | `defaults.replication.warm` |
| `[defaults.replication]` | `cold` | `defaults.replication.cold` |
| `[tiering]` | `enabled` | `tiering.enabled` |
| `[tiering]` | `hot_threshold_reads_per_hour` | `tiering.hot_threshold_reads_per_hour` |
| `[tiering]` | `cold_threshold_days` | `tiering.cold_threshold_days` |
| `[tiering]` | `frozen_threshold_days` | `tiering.frozen_threshold_days` |
| `[tiering]` | `migration_bandwidth_limit_mbps` | `tiering.migration_bandwidth_limit_mbps` |
| `[tiering]` | `evaluation_interval_secs` | `tiering.evaluation_interval_secs` |
| `[monitoring]` | `heartbeat_interval_secs` | `monitoring.heartbeat_interval_secs` (accepted, but the health monitor reads it at startup: restart the managers) |
| `[monitoring]` | `suspect_after_missed` | `monitoring.suspect_after_missed` (same) |
| `[monitoring]` | `down_after_missed` | `monitoring.down_after_missed` (same) |

### Data Node (`data.toml`)

| Section | Option | TOML Path |
|---------|--------|-----------|
| `[cache]` | `max_memory_mb` | `cache.max_memory_mb` |
| `[cache]` | `policy` | `cache.policy` |
| `[scrub]` | `quick_interval_hours` | `scrub.quick_interval_hours` |
| `[scrub]` | `deep_interval_hours` | `scrub.deep_interval_hours` |
| `[scrub]` | `quick_cron` | `scrub.quick_cron` |
| `[scrub]` | `deep_cron` | `scrub.deep_cron` |
| `[scrub]` | `max_io_percent` | `scrub.max_io_percent` |

### FUSE Client (`fuse.toml`)

| Section | Option | TOML Path |
|---------|--------|-----------|
| `[readahead]` | `mode` | `readahead.mode` |
| `[readahead]` | `prefetch_chunks` | `readahead.prefetch_chunks` |
| `[readahead]` | `prefetch_next_files` | `readahead.prefetch_next_files` |
| `[cache]` | `local_cache_max_gb` | `cache.local_cache_max_gb` |
| `[partition_safety]` | `mode` | `partition_safety.mode` |
| `[partition_safety]` | `quorum_loss_timeout_secs` | `partition_safety.quorum_loss_timeout_secs` |
| `[partition_safety]` | `show_warning` | `partition_safety.show_warning` |

### Options That Do NOT Support Live Reload

The following categories of options require a process restart to take effect:

- **Node identity** -- `[node]` section (id, hostname, data_dir)
- **Topology location** -- `[location]` section
- **Cluster membership** -- `[cluster]` section (name, shared_secret, bootstrap, manager_addr)
- **Network listeners** -- `[raft]`, `[grpc]`, and `[api]` listen addresses
- **TLS** -- `[tls]` section (mode, cert_file, key_file, ca_file)
- **Disk layout** -- `[[disks]]` section (path, tier)
- **Mount point** -- `[mount]` section
- **Cache directory** -- `cache.local_cache_dir` (the path itself; the size limit is reloadable)

## How Live Reload Works

DTFS uses two mechanisms for live configuration reload, depending on where the setting lives.

### Cluster-wide Settings (via Raft)

Cluster-wide settings (defaults, tiering, monitoring, and per-volume configuration) are stored in the Raft-replicated state machine.

The reload flow is:

1. An administrator changes a setting via the REST API or Web UI.
2. The leader manager proposes the change as a Raft log entry.
3. Once committed, the new configuration is stored in the state machine.

### Local Settings (via File Watch)

Some local TOML settings (such as cache policy on a data node or readahead mode on a FUSE client) are reloadable by sending `SIGHUP` to the process or by editing the TOML file while file-watching is active. On receiving the signal, the process re-reads the TOML file and applies the changed reloadable options.

## Triggering a Reload

There are three ways to trigger a live reload:

1. **REST API / Web UI** -- Change a cluster-wide setting. The manager handles distribution automatically.
2. **SIGHUP** -- Send `SIGHUP` to a manager, data node, or FUSE client process to re-read the local TOML file and apply any changed reloadable options.
3. **CLI** -- Use `dtfsctl config reload` to send a reload signal to a local DTFS process.

## Verifying a Reload

After triggering a reload, verify that the new configuration has been applied:

- **Cluster-wide**: Query `GET /api/v1/config/effective` on each manager.
- **Local**: Check the process logs for a confirmation message, or query the node's status endpoint.
