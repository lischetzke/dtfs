# Configuration Templating

Sources: `deploy/examples/{manager,data,fuse}.toml` (exhaustive, every default
shown, parse-tested by `cargo test -p dtfs-common`),
`crates/dtfs-common/src/config.rs`, `crates/dtfs-{manager,data,fuse}/src/main.rs`,
[Cluster vs Node-scoped Keys](../configuration/scope.md).

Start every template from the matching example file and keep only what you set;
the key-by-key reference is [Manager](../configuration/manager.md),
[Data Node](../configuration/data-node.md) and
[FUSE Client](../configuration/fuse-client.md).

Two parser facts that shape a template:

- Unknown keys are ignored silently (no `deny_unknown_fields`). A mistyped key
  falls back to its default instead of failing. A role must template from a fixed
  key list and, after a deploy, compare effective values:
  `GET /api/v1/config/effective` (manager) lists every key with current and
  default value.
- Section tables listed as required below must exist even when every key
  inside has a default (write the header with no keys). A syntax error or a
  missing required key makes the daemon exit at start, which under systemd is a
  restart loop every 5 s. Render to a temp file, parse-check it, then move it
  into place.

## Required sections and keys per role

"Required" means the parse fails without it. Everything else has a compiled-in
default.

| Role | Required tables | Required keys | Everything else |
|---|---|---|---|
| manager | `[node] [location] [cluster] [raft] [grpc] [api] [tls] [defaults] [tiering] [monitoring]` | `node.hostname`; `location.{region,datacenter,rack,host}`; `cluster.name`; `cluster.shared_secret`; `grpc.listen_addr` | optional, see example |
| data | `[node] [location] [cluster] [grpc] [tls] [cache] [scrub]` and at least one `[[disks]]` | `node.hostname`; `location.*`; `cluster.manager_addr`; `cluster.shared_secret`; `grpc.listen_addr`; per disk `path`, `tier` | optional, see example |
| client | `[client] [location] [mount] [readahead] [cache] [partition_safety]` | `client.manager_addr`; `location.*`; `mount.mountpoint` | optional, see example |

Optional blocks that may be omitted entirely: manager `[alerts] [audit] [scrub]
[placement] [memory] [updater]`; data `[memory] [placement] [updater]
[[tape_devices]]`; client `[tls] [write] [memory] [updater]`. If a sub-table
header is written, its non-defaulted fields become required (an empty
`[defaults.write_safety]` fails to parse).

## Values that must be identical on every node

| Key | Applies to | Why |
|---|---|---|
| `cluster.shared_secret` | all managers, all data nodes, all clients | HMAC for registration, mutating RPCs, mutating REST and Raft RPCs. A client with an empty secret is not refused but stays invisible in the node table and cannot do authenticated mutations. |
| `cluster.name` | all managers | Raft cluster name. |
| `[[cluster.initial_voters]]` (same list, same order of entries is irrelevant) | all managers | Peer addresses used for self-registration and follower-to-leader forwarding, even though membership itself comes from the Raft log after bootstrap. |
| `cluster.require_raft_auth` | all managers | Mixed values make the strict managers reject the lenient ones' unsigned Raft RPCs; flip only as in [Rolling Update](./rolling-update.md#flag-flips-after-an-update). |
| `[raft]` timings (`election_timeout_ms`, `heartbeat_interval_ms`, `snapshot_threshold`) | all managers | cluster-scoped |
| `[defaults]`, `[tiering]`, `[monitoring]`, `[alerts]`, `[audit]`, `[scrub]` | all managers | cluster-scoped policy; drift between managers gives different behaviour after a leader change |
| `defaults.chunk_size_mb` (manager) and `client.chunk_size_mb` (client) | managers and clients | A client chunk size that differs from the volume's corrupts multi-chunk reads. Both default to 4 MB. |
| `[grpc] max_message_size_mb` / client `max_message_size_mb` | all | Client value must be at least the manager and data value. |
| `[tls] mode` and CA | all roles | Enable TLS only cluster-wide, in one rollout, after every node runs a TLS-capable build. |

Anchor and tier policy (`defaults.anchor_datacenter`, replication, write safety)
only affects volumes created after the change; existing volumes are changed with
the volume API ([Volume Configuration](../configuration/volume-config.md)), not by
re-rendering TOML.

## Values that must differ per node

| Key | Rule |
|---|---|
| manager `node.hostname` | unique among all daemons. `node_id = hash(hostname)`. Must equal the `hostname` of this manager's own `[[cluster.initial_voters]]` entry. |
| data `node.hostname` | unique among all daemons; must differ from a co-located manager's hostname. |
| client `location.host` | the client has no `[node]` table; it registers under this string, which must be unique cluster-wide. Convention for a client on a data host: `<data-hostname>_cln`. |
| `location.{region,datacenter,rack,host}` | real topology. Daemons on the same physical box share all four so reads prefer local replicas (the client then uses its own `host` value). `datacenter` strings drive anchor placement and are compared case-insensitively. |
| `[[cluster.initial_voters]] addr` | the peer-routable `host:9401` of that manager. Never `0.0.0.0`. |
| `cluster.bootstrap` | `true` on exactly one manager and only for the first start of a brand new cluster; `false` on every other manager and when re-rendering later. |
| `[[disks]]` | per host, ordered, append-only, see [Decommission and Replace](./decommission-replace.md). |
| `grpc.listen_addr` (data) | the address other nodes dial; use the routable address when the host has several interfaces. |

Two daemons with the same hostname overwrite each other's registry entry, with
no error. A role should assert uniqueness across the whole inventory at template
time.

## Identity and bootstrap inputs

- `node.id`: leave `"auto"`.
- A manager that is not bootstrapped starts as an empty learner (term 0, no
  leader) until it is joined with `dtfs-cli raft add-learner`. Rendering
  `bootstrap = true` to "fix" it creates a second, divergent cluster.
- The bootstrap manager needs `initial_voters` listing the voters to seed. The
  list is read once; later membership changes use the membership API.

## Secrets handling

- Generate once per cluster: `openssl rand -hex 32` (64 hex characters). The
  installer refuses to continue when no secure RNG exists.
- Keep it in Ansible Vault (for example `vault_dtfs_shared_secret`) and render it
  into the three TOML files; never into command lines, because
  `dtfs-install.sh --secret` and `dtfs-cli --secret` put it in the process
  list. Prefer the environment: `DTFS_SHARED_SECRET` is read by the installer and
  by `dtfs-cli` (`--secret` flag also exists) and does not appear in `ps`.
- Use `no_log: true` on every task that renders or passes it.
- Modes: 0640, owner `dtfs` for manager and data, `root` for fuse (see
  [Filesystem Layout](./filesystem-layout.md)).
- An empty secret is refused at startup by the manager and the data node unless
  `allow_insecure_empty_secret = true` (lab only). Never template that flag in
  production.
- Rotation is not a rolling operation: every node must change together. Treat a
  rotation as an outage window.
- The optional `[updater] token` (a GitLab deploy or access token) is a second
  secret in the same files; it is read only by `dtfs-cli update` and is not
  needed when the role pushes binaries itself, so leave it out.

## Validation performed at startup

All of these abort the process with a message naming the key. A role should run
the same checks at template time (or at least surface them from `journalctl -u`
in its failure output).

| Check | Roles | Rule |
|---|---|---|
| shared secret | manager, data | non-empty unless `allow_insecure_empty_secret` |
| `[memory]` | all | `hard_cap_mb` is 0 or at least 128; `warn_threshold_pct` between 1 and 100; `shed_threshold_pct` at most 100 and above `warn_threshold_pct` |
| message size | all | `max_message_size_mb` greater than 0 |
| `[placement] min_free_pct` | manager, data | within 0 to 100 |
| intervals | manager | `tiering.evaluation_interval_secs`, `monitoring.heartbeat_interval_secs`, `suspect_after_missed`, `down_after_missed` all greater than 0; `raft.election_timeout_ms` and `heartbeat_interval_ms` greater than 0 and heartbeat strictly below election |
| intervals | client | `cache.prune_interval_secs` and `partition_safety.quorum_loss_timeout_secs` greater than 0 |
| TLS | all | `mode = "require"` fails without all three files; `auto` silently stays plaintext without them |
| disk position | data | each disk's `.dtfs-disk-id` must equal its 0-based `[[disks]]` position |
| Raft state | manager | refuses to start when a corrupt snapshot was discarded but the log is already purged, or when the snapshot is older than the purged log (the message tells you to wipe `raft-log` and `raft-sm` and re-join as a learner from a healthy peer) |
| bootstrap voter entry | manager | a bootstrap manager without an `initial_voters` entry matching its own hostname logs "no matching initial_voters entry for self" and falls back to its gRPC listen address |

Also keep `monitoring.down_after_missed` above `suspect_after_missed`, and
`tiering.frozen_threshold_days` above `cold_threshold_days`; the example file
states these as rules even though only the zero checks above are enforced.

## Applying config changes

A config change needs a restart of that daemon unless the key is live-reloadable
([Live Reload](../configuration/live-reload.md)). The role should restart on
change through a handler, in the same node order as an update (managers
learners, followers, leader; then data; then clients), one node at a time, using
the health gates in [Rolling Update](./rolling-update.md). Never template a
manager config change into all managers in one parallel run.
