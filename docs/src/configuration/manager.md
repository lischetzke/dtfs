# Manager Configuration

File: `/etc/dtfs/manager.toml`. Source of truth: `crates/dtfs-common/src/config.rs` (`ManagerConfig`). A complete, parse-tested example is `deploy/examples/manager.toml`.

## Conventions

- **Scope** is `node` (legitimately different per host) or `cluster` (must be identical on every manager). See [Scope](./scope.md).
- **Reload**: no manager option is re-read while the process runs. Every change needs a restart (see [Live Reload](./live-reload.md)).
- **Validated**: the process refuses to start when the check fails (exit non-zero, reason in the log).
- Required tables: `[node]`, `[location]`, `[cluster]`, `[raft]`, `[grpc]`, `[api]`, `[tls]`, `[defaults]`, `[tiering]`, `[monitoring]`. A required table may be empty when every key in it has a default. Optional tables: `[alerts]`, `[audit]`, `[scrub]`, `[placement]`, `[memory]`, `[updater]` (CLI only).
- Unknown keys are ignored silently, so a misspelt key falls back to its default. Check spelling against this page.
- Startup checks, in order: `cluster.shared_secret` non-empty, `[memory]`, `[placement]`, `[grpc]`, intervals (`[tiering]`, `[monitoring]`, `[raft]` timings), `[tls]` resolution.

## `[node]`

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `hostname` | string | required | Unique across every daemon (managers, data nodes, clients). Must equal this node's `[[cluster.initial_voters]]` `hostname`. | node |
| `id` | string | `"auto"` | Leave `auto`. | node |
| `data_dir` | path | `/var/lib/dtfs` | Writable. Holds the Raft log (RocksDB), snapshots and metadata. | node |

`node_id = hash(hostname)`. Two daemons with the same hostname overwrite each other's registry entry. On a composite host give every role its own name (manager `dtfs-dc1-mgr01`, data node `dtfs-dc1-data01`, FUSE client `dtfs-dc1-data01_cln`).

## `[location]`

All four keys are required strings: `region`, `datacenter`, `rack`, `host`. Scope: node. Datacenter strings are compared case-insensitively where the anchor invariant is evaluated.

## `[cluster]`

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `name` | string | required | Identical on all managers. | cluster |
| `shared_secret` | string | required | **Validated**: empty is refused unless `allow_insecure_empty_secret = true`. Identical on every manager, data node and client. Keep the file mode 0640. | cluster |
| `allow_insecure_empty_secret` | bool | `false` | Lab only. With an empty secret, REST, join and RPC authentication are skipped. | cluster |
| `require_raft_auth` | bool | `false` | `true`: Raft RPCs without a valid `x-dtfs-rpc-ts`/`x-dtfs-rpc-mac` stamp are rejected. `false`: unsigned RPCs are accepted with a rate-limited warning (rolling-update safe); a stamped-but-invalid RPC is always rejected. No effect with an empty secret. | cluster |
| `accept_legacy_forward_secret` | bool | `true` | Leader side of `ForwardClientWrite`. `true`: a forward without the `x-dtfs-rpc-*` stamp is accepted when its legacy raw `shared_secret` field matches (pre-HMAC managers, rolling-update safe). `false`: such forwards are refused with `PERMISSION_DENIED`. Stamped forwards are always verified. No effect with an empty secret. Set `false` once every manager runs a build that signs forwards. | cluster |
| `bootstrap` | bool | `false` | `true` on exactly one manager, first start only. Never on a node joining an existing cluster (creates a second, divergent group). | node |
| `initial_voters` | array of tables | `[]` | See below. | cluster |

`[[cluster.initial_voters]]`:

| Key | Type | Default | Constraint |
|---|---|---|---|
| `hostname` | string | required | Exactly the peer's `[node].hostname`. A manager that finds no entry for itself cannot join. |
| `addr` | string | required | Peer-routable manager gRPC `host:port` (its `grpc.listen_addr` as seen by other nodes). Never `0.0.0.0`. |
| `region`, `datacenter`, `rack` | string | `""` | Display hints for the node table only. |

The list is consulted at bootstrap and as the peer-address pool afterwards. Editing it does not join a running cluster: use `dtfs-cli raft add-learner` / `promote` / `remove` ([Cluster Management](../operations/cluster-management.md)). Keep it equal to the real membership.

```toml
[cluster]
name = "production"
shared_secret = "a-strong-random-secret"
require_raft_auth = true
bootstrap = true            # first manager, first start only

[[cluster.initial_voters]]
hostname = "mgr-01"
addr = "10.0.0.1:9401"
[[cluster.initial_voters]]
hostname = "mgr-02"
addr = "10.0.0.2:9401"
[[cluster.initial_voters]]
hostname = "mgr-03"
addr = "10.0.0.3:9401"
```

## `[raft]`

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `election_timeout_ms` | u64 | `400` | **Validated**: > 0. The election timeout is randomised in `[value, 2 x value]`. | cluster |
| `heartbeat_interval_ms` | u64 | `150` | **Validated**: > 0 and strictly below `election_timeout_ms`. | cluster |
| `snapshot_threshold` | u64 | `10000` | Applied entries between snapshots. | cluster |
| `max_in_snapshot_log_to_keep` | u64 | `1000` | Committed entries kept behind a snapshot. | node |
| `rocksdb_keep_log_file_num` | u64 | `4` | RocksDB info-LOG files retained. | node |
| `rocksdb_max_manifest_bytes` | u64 | `67108864` | MANIFEST rotation size (bytes). | node |
| `rocksdb_write_buffer_mb` | u64 | `64` | Memtable size per column family (MiB). | node |
| `rocksdb_block_cache_mb` | u64 | `64` | Shared block cache (MiB). | node |
| `node_liveness_secs` | u64 | `30` | A node not seen for this long reads as `Suspect` (and `Down` past 4x) in the read API. FUSE `client.heartbeat_interval_secs` must stay well below it. | node |
| `failed_task_retention_secs` | u64 | `1800` | How long a `Failed` migration task is kept before pruning. | node |
| `done_task_retention_secs` | u64 | `300` | Same for `Done` tasks. | node |

## `[grpc]`

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `listen_addr` | string | required | `host:port`. Serves ManagerService and the Raft transport. | node |
| `max_message_size_mb` | u32 | `256` | **Validated**: > 0. Data nodes and clients should use at least the same value. | node |
| `max_concurrent_chunk_writes` | usize | `8` | Data-node setting; ignored on managers. | node |

## `[api]`

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `listen_addr` | string | `0.0.0.0:9402` | REST API and WebUI. A bind failure exits the process. | node |

## `[tls]`

Mutual TLS for all manager gRPC (ManagerService, Raft transport, outbound connections). See [TLS](../security/tls.md).

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `mode` | string | `"auto"` | **Validated**: `auto`, `require`, `disabled` (case-insensitive; aliases `disable`, `off`, `required`). Any other value refuses startup. `auto`: mTLS when all three files are set and exist; plaintext when none is set; startup error when only some are set; files set but absent on disk log a warning and stay plaintext. `require`: all three must be set and load, else startup fails. | node |
| `cert_file`, `key_file`, `ca_file` | path | unset | PEM files. | node |

Roll TLS out only after every node runs a TLS-capable build, and enable it on all roles together.

## `[defaults]`

Seeds the auto-created `default` volume and the defaults for volumes created through the API. Read at startup. Changing it later does not modify existing volumes.

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `chunk_size_mb` | u32 | `4` | Chunk size for new volumes, 1 to 64 MiB (startup refuses other values; existing volumes are not touched, a warning is logged for any outside the range). FUSE `client.chunk_size_mb` must equal the chunk size of the volumes it writes. | cluster |
| `anchor_datacenter` | string | unset | When set, every chunk keeps at least one replica in this datacenter. Unset: no anchor. | cluster |

`[defaults.write_safety]` (if the table is present, both keys are required):

| Key | Type | Default | Constraint |
|---|---|---|---|
| `min_replicas_before_ack` | u32 | `2` | A write is acknowledged only after this many distinct `(node_id, disk_id)` replicas are durable; a commit with fewer is rejected. Allocation fills the floor across the volume's maintained tiers; the floor is clamped only to the healthy maintained node capacity, never to 0. The topology must be able to hold this many replicas. See [Write Safety](../storage/replication.md#write-safety). |
| `write_timeout_secs` | u32 | `30` | Seconds to wait for acknowledgements. |

`[defaults.replication]`. When the table is absent the built-in goal is cold 1, warm 2, hot 0, tape 0 (hot and tape are opt-in, so a cold+warm cluster can reach its goal). If the table is present, `hot`, `warm`, `cold` are required and `tape` defaults to `1`:

| Key | Type | Default (table absent) | Default (table present) |
|---|---|---|---|
| `hot` | u32 | `0` | required |
| `warm` | u32 | `2` | required |
| `cold` | u32 | `1` | required |
| `tape` | u32 | `0` | `1` |

A volume that requires copies on a tier with no registered disks (or more copies than nodes carrying that tier) can never leave `undergoal`. The manager logs one warning and audits `volume.goal_unreachable` per volume and tier at startup (after 120 s), on volume create and on `set-mandatory`; `GET /api/v1/stats` returns `goal_unreachable_volumes` and a `goal_unreachable` list (`volume`, `tier`, `required`, `nodes_with_tier`), shown as a dashboard signal chip. Placement classification is unchanged.

A replication count creates replicas only if something maintains them: the volume's `mandatory_replicas` (under-replication scheduler) or cache promotion (`promote_reads`). See [Replication](../storage/replication.md) and [Volume Configuration](./volume-config.md).

## `[tiering]`

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `enabled` | bool | `true` | | cluster |
| `hot_threshold_reads_per_hour` | u32 | `5` | | cluster |
| `cold_threshold_days` | u32 | `14` | | cluster |
| `frozen_threshold_days` | u32 | `60` | Keep above `cold_threshold_days`. | cluster |
| `migration_bandwidth_limit_mbps` | u32 | `200` | `0` = unlimited. | cluster |
| `evaluation_interval_secs` | u64 | `3600` | **Validated**: > 0. | cluster |
| `enable_cache_redundancy_escalation` | bool | `false` | `true`: the cache planner may raise a chunk's cache replicas from `min` toward `max` once reads reach 2 x `promote_reads`. | cluster |

## `[monitoring]`

The leader's health monitor reads these at startup: suspect after `heartbeat_interval_secs x suspect_after_missed`, down after `heartbeat_interval_secs x down_after_missed` (forced at least one interval above suspect). Data nodes heartbeat every 5 s (fixed); keep `heartbeat_interval_secs = 5`.

| Key | Type | Default | Constraint | Scope |
|---|---|---|---|---|
| `heartbeat_interval_secs` | u64 | `5` | **Validated**: > 0. | cluster |
| `suspect_after_missed` | u32 | `3` | **Validated**: > 0. | cluster |
| `down_after_missed` | u32 | `6` | **Validated**: > 0. Keep above `suspect_after_missed`. | cluster |

## `[scrub]` (optional)

Cluster scrub and integrity policy. The manager sends it to every data node in each heartbeat response; it overrides the data node's local `[scrub]` fallback. It is read from this file at startup, so change it on every manager and restart.

| Key | Type | Default | Notes |
|---|---|---|---|
| `quick_interval_secs` | u64 | `21600` (6 h) | Stat, size and `.meta` presence. Values below 60 are raised to 60. |
| `deep_interval_secs` | u64 | `604800` (7 d) | Full BLAKE3 verify. Values below 60 are raised to 60. |
| `orphan_interval_secs` | u64 | `86400` | Data-node `ListChunks` versus metadata cross-check. |
| `orphan_grace_secs` | u64 | `86400` | Grace before an orphan is reaped, measured against the `.dat` mtime. Minimum `21600` (6 h); lower values fail startup. |
| `max_io_percent` | u32 | `10` | Scrub I/O throttle. |
| `verify_on_read` | bool | `true` | Hash every read before bytes leave the data node. Disabling is the only mode in which reads can return corrupt bytes. |
| `quarantine_on_fault` | bool | `true` | `true`: a single integrity fault quarantines the faulting node, both on the data node (self-quarantine) and on the manager (fault reported in a heartbeat). `false`: faults are recorded and reported, nothing is quarantined. |
| `auto_unquarantine_secs` | u64 | `0` | Cooldown for automatic unquarantine; `0` = manual only. |

## `[placement]` (optional)

| Key | Type | Default | Constraint |
|---|---|---|---|
| `min_free_bytes` | u64 | `1073741824` | Per-disk free-space reserve. |
| `min_free_pct` | f64 | `5.0` | **Validated**: `0.0..=100.0`. Reserve is `max(min_free_bytes, total x pct%)`. A disk under its reserve leaves the placement ring. |
| `allow_capacity_spill` | bool | `true` | When the mandated tier is full, spill to the fastest tier with room (audited, rebalanced later). The anchor copy is never spilled. `false`: fail the write. Per-volume override: `volumes set-spill`. |

## `[memory]` (optional)

Per-process RAM budget and pressure ladder. `hard_cap_mb = 0` (default, or block absent) disables it. On the manager, once RSS reaches the shed level the scheduler skips background ticks (repair, migration, rebalance), self-limited so repair cannot stall indefinitely. Warn takes no manager action. systemd `MemoryMax` stays the hard backstop; on a composite host the sum of all roles' caps must stay below host RAM.

| Key | Type | Default | Constraint |
|---|---|---|---|
| `hard_cap_mb` | u64 | `0` | **Validated**: `0` or at least 128. |
| `warn_threshold_pct` | u8 | `80` | **Validated**: 1..=100 and below `shed_threshold_pct`. |
| `shed_threshold_pct` | u8 | `92` | **Validated**: at most 100. |
| `sample_interval_secs` | u64 | `5` | `0` is treated as 1. |

## `[audit]` (optional)

| Key | Type | Default | Notes |
|---|---|---|---|
| `replicate_via_raft` | bool | `false` | `true`: audit entries are replicated through Raft so every manager serves the same `/api/v1/audit`. Enable only after every manager runs a binary that understands the entry type, then restart the managers. |

## `[[alerts.webhooks]]` (optional)

| Key | Type | Default | Notes |
|---|---|---|---|
| `url` | string | required | Masked in `/api/v1/config*` output. |
| `secret` | string | `""` | Per-sink shared secret. Masked in `/api/v1/config*` output. |
| `kinds` | list of string | `[]` | Empty = all alert kinds. |
| `dedup_secs` | u64 | `300` | Duplicate suppression window. |

See [Alerting](../operations/alerting.md).

## `[updater]` (optional, CLI only)

Read by [`dtfs-cli update`](../api-reference/cli-reference.md#update); the daemon ignores it. The same block works in `data.toml` and `fuse.toml`.

| Key | Type | Default | Notes |
|---|---|---|---|
| `repo_url` | string | `https://github.com/lischetzke/dtfs` | GitHub repo URL (github.com only). Precedence: `--repo-url`, this key, `DTFS_REPO_URL`, built-in default. |
| `token` | string | `""` | Optional GitHub token (rate limits or a private fork), sent as `Authorization: Bearer`. Precedence: `--token`, `DTFS_REPO_TOKEN`, this key. |
| `minisign_public_key` | string | `""` | Minisign public key (base64 line or full `.pub` text). `DTFS_MINISIGN_PUBKEY` overrides it. When set, `SHA256SUMS` must carry a valid signature and list the tarball's hash, else the install is refused. Empty: unverified, a warning is printed. |
| `install_dir` | string | `/usr/local/bin` | Overridden by `--install-dir`. |

Keep the file mode 0640: `token` and `cluster.shared_secret` share it.
