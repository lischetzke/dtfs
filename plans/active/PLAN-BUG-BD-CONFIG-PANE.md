# Bug BC — Config pane scope split (cluster-wide vs node-specific)

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

The AZ-5 effective-config endpoint and WebUI pane currently expose
every key in `ManagerConfig` flat. Operator can edit values that MUST
be identical across all manager nodes (e.g. `cluster.name`,
`defaults.replication`) and values that are intentionally per-host
(`node.hostname`, `grpc.listen_addr`, `tls.cert_path`). Editing a
per-host key via the pane writes it to ONE manager's
`/etc/dtfs/manager.toml`, silently producing config drift across
peers.

## 0. Decisions

- **Default view = cluster-wide keys only.** Cluster keys must be
  identical on every manager; safe to edit once + propagate.
- **Node tab = current manager's own config.** Switcher selects which
  manager's pane to view. v1 shows only the manager the WebUI is
  served from (no cross-manager federation). v2 (future) proxies via
  `/api/v1/nodes/{id}/config`.
- **Data-node config not yet in pane.** Out of scope for this plan;
  follow-up after `/api/v1/nodes/{id}/config` proxy exists.
- **Write boundary stays strict.** Cluster-key writes go through the
  existing `POST /api/v1/config` (which writes leader's
  manager.toml). v1: operator must SSH-replicate to peers, OR we wire
  a `--replicate` flag that writes via Raft instead — defer to v2.
- **Classification is authoritative in code, not config.** Single
  source of truth: `config_key_scope(key) -> ConfigScope` enum in
  `api.rs`. WebUI consumes the `scope_kind` field on each row.

## 1. Schema (additive)

`get_config_effective` row shape today:

```json
{ "name": "manager.defaults.chunk_size_mb",
  "scope": "manager",
  "current": 128,
  "default": 128,
  "is_default": true }
```

Adds:

```json
{ ...,
  "scope_kind": "cluster" | "node" }
```

`scope_kind` derived from the dot-path. Mapping table in § 2.

## 2. Classification table

| Key prefix | scope_kind | Reason |
|---|---|---|
| `manager.cluster.*` | cluster | Cluster identity, shared_secret, initial_voters — must match. |
| `manager.defaults.*` | cluster | Replication factors, chunk_size, write_safety, anchor_dc — cluster policy. |
| `manager.tiering.*` | cluster | Thresholds + cadence — cluster policy. |
| `manager.monitoring.*` | cluster | Heartbeat / down-thresholds — must match for Raft + health. |
| `manager.alerts.*` | cluster | Operator alert sinks — single source. |
| `manager.audit.*` | cluster | Audit replication policy. |
| `manager.raft.election_timeout_ms` | cluster | Raft timing MUST match across peers. |
| `manager.raft.heartbeat_interval_ms` | cluster | Same. |
| `manager.raft.snapshot_threshold` | cluster | Same. |
| `manager.node.*` | node | hostname, id, data_dir — per-host identity. |
| `manager.location.*` | node | DC/rack/host position — per-host. |
| `manager.grpc.*` | node | listen_addr, advertise_addr — per-host network. |
| `manager.api.*` | node | listen_addr — per-host network. |
| `manager.tls.*` | node | cert/key material — per-host. |

Unknown prefix → `"node"` (safer default — operator sees it but
WebUI doesn't surface it under "Cluster"). Add to table when new
sub-config arrives.

## 3. Tasks

### ⬜ BC-1 — `config_key_scope` helper + row tagging

* **Files:** `crates/dtfs-manager/src/api.rs`.
* **Implementation:** add
  ```rust
  enum ConfigScope { Cluster, Node }
  fn config_key_scope(dotted: &str) -> ConfigScope { ... }
  ```
  Walk the dotted name in `flatten_config`, attach `scope_kind` to
  every row.
* **Tests:** unit `config_key_scope_classification_table` exercising
  all 14 prefixes above + an unknown-key → Node fallback.
* **Done when:** `cargo test -p dtfs-manager` green + curl
  `/api/v1/config/effective` shows `scope_kind` on every row.

### ⬜ BC-2 — WebUI tab switcher

* **Files:** `crates/dtfs-manager/src/webui.rs::renderConfig`.
* **Implementation:** two segmented buttons above the search box:
  "Cluster (N)" + "Node: <hostname> (M)". Default = Cluster. Filter
  row list by `scope_kind`. Counts in the button labels. Keep the
  existing search box; it filters within the active tab.
* **Done when:** WebUI loads with Cluster tab active, switch to
  Node tab shows only per-host keys, search filters within tab.

### ⬜ BC-3 — Write-guard at API boundary

* **Files:** `crates/dtfs-manager/src/api.rs::update_config`.
* **Implementation:** before writing the merged config to disk,
  diff against current; reject the request if any modified key is
  `Cluster`-scoped AND the request body has no
  `acknowledge_cluster_wide: true` flag. WebUI sets the flag only
  on the Cluster tab's save action.
* **Tests:** unit
  `update_config_rejects_cluster_key_without_ack`.
* **Done when:** POST without ack returns 400 listing the cluster
  keys that would have been touched; with ack succeeds.

### ⬜ BC-4 — Doc

* **Files:** `docs/src/configuration/index.md` (or
  `docs/src/configuration/scope.md` new).
* **Implementation:** one page explaining the table in § 2 and
  walking the operator through the "edit cluster-wide vs per-host"
  flow. Reference Bug BC.
* **Done when:** mdBook builds.

## 4. Verification log

> Append per-task. Empty at plan authorship.

## 5. Deferred

- **v2 cross-manager federation.** Add `/api/v1/nodes/{id}/config`
  proxy so the Node tab can switch between mgr01/02/03. Today
  shows only the manager the WebUI request landed on.
- **Cluster-key write propagation.** Today writes to one manager's
  `manager.toml`. Future: optionally write the change via Raft
  (new `ClientRequest::SetClusterConfig` variant) so all managers
  pick it up on next heartbeat without operator SSH dance.
- **Data-node config in pane.** Same federation issue. Add after
  proxy lands.

## 6. Commit-message convention

```
BC-<n>: <one-line summary, <72 chars>>

<body>

Plan: plans/active/PLAN-BUG-BC-CONFIG-SCOPE.md (BC-<n> done)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```
