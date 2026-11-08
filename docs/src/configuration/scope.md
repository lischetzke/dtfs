# Config Scope: Cluster vs Node

Every `manager.toml` key is classified as **cluster** (must be identical on every manager) or **node** (legitimately different per host). The WebUI `/ui/config` page and `GET /api/v1/config/effective` expose the classification as `scope_kind`.

| Scope | Keys (manager) |
|---|---|
| cluster | `cluster.*`, `defaults.*`, `tiering.*`, `monitoring.*`, `alerts.*`, `audit.*`, `scrub.*`, `raft.election_timeout_ms`, `raft.heartbeat_interval_ms`, `raft.snapshot_threshold` |
| node | everything else: `node.*`, `location.*`, `grpc.*`, `api.*`, `tls.*`, `placement.*`, `memory.*`, remaining `raft.*` keys |

Data-node and FUSE-client files are node-scoped in full. The cluster scrub policy reaches data nodes through the manager's `[scrub]` block, not through their own file.

`cluster.bootstrap` is classified as cluster but is `true` on exactly one manager by design; treat it as per-host.

The classifier is `config_key_scope` in `crates/dtfs-manager/src/api/config.rs`. A key that is not in its prefix table is `node`, the safe default. A new `ManagerConfig` sub-table that should be cluster-wide needs a row there.

## Why it matters

Managers read their own file at startup. A cluster key that differs between managers produces different behaviour depending on which manager is leader (scheduler, tiering, monitor) or which one answers a request. After editing a cluster key, change it on every manager and restart them one at a time (learners, followers, then the leader).

## Editing through the API

| Action | Request | Required |
|---|---|---|
| Store an override in Raft | `POST /api/v1/config` with `write_to_disk:false` | nothing extra |
| Also rewrite `/etc/dtfs/manager.toml` on the manager that handled the request | `POST /api/v1/config` with `write_to_disk:true` | `acknowledge_cluster_wide:true` |

A body that contains a node-scoped key is rejected with `400` (`node-scoped keys cannot be set cluster-wide`). Details and limits: [Cluster-wide Configuration](./cluster-config.md) and [REST API](../api-reference/rest-api.md#config).
