# Cluster vs node-scoped config (Bug BD)

Every key under `ManagerConfig` belongs to one of two scopes:

| Scope | Where it must match | Examples |
|---|---|---|
| **Cluster** | Identical on every manager | `cluster.*`, `defaults.*`, `tiering.*`, `monitoring.*`, `alerts.*`, `audit.*`, `scrub.*`, `raft.election_timeout_ms`, `raft.heartbeat_interval_ms`, `raft.snapshot_threshold` |
| **Node** | Intentionally per-host | `node.hostname`, `node.id`, `node.data_dir`, `location.*`, `grpc.*`, `api.*`, `tls.*` |

The WebUI **/ui/config** page surfaces this split with two tabs.
Cluster opens by default. Search filters within the active tab.

## Editing

| Action | Endpoint | Required |
|---|---|---|
| Live-reload only (in-memory) | `POST /api/v1/config` `write_to_disk:false` | no ack |
| Persist to `/etc/dtfs/manager.toml` | `POST /api/v1/config` `write_to_disk:true` | `acknowledge_cluster_wide:true` |

The boundary check at `api.rs::update_config` enforces the ack
flag. The WebUI Cluster tab sets it automatically. Editing a
node-scoped key from a single manager and persisting it locally
WILL drift relative to peers — manage with care.

## Classification source of truth

The classifier `config_key_scope(dotted) -> &'static str` in
`crates/dtfs-manager/src/api.rs`. Test
`bug_bd_1_classification_table` pins the 14 prefix table. New
sub-configs added to `ManagerConfig` MUST add a row to that table
or they fall back to `node` (safer default).

## Deferred (v2)

- Cross-manager federation: `/api/v1/nodes/{id}/config` proxy so
  the Node tab can show peer managers' configs.
- Raft-driven cluster propagation:
  `ClientRequest::SetClusterConfig` variant so cluster-key changes
  land on every manager without operator SSH.
