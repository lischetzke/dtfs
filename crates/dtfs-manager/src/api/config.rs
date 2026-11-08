//! REST handlers: cluster config get/update/effective. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.
#![allow(unused_imports)]

use super::*;

pub(super) async fn get_config(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;
    // Raft-stored cluster override (set via the WebUI editor / `dtfs-cli
    // config set`). May be sparse, e.g. `{"tiering":{"evaluation_interval_secs":60}}`.
    let mut raw_override = if sm.cluster_config.config_json.is_empty() {
        Value::Null
    } else {
        serde_json::from_str(&sm.cluster_config.config_json).unwrap_or(Value::Null)
    };
    // CO-2-1 (security) — this endpoint is unauthenticated and echoes the
    // stored override verbatim under `config`. If an operator ever saved
    // `cluster.shared_secret` cluster-wide it lives here in cleartext; redact
    // it to the "***" sentinel before serving, same as the flattened
    // effective view. (The tiering merge below reads its own `tiering`
    // sub-object, which carries no secrets.)
    redact_secret_leaves(&mut raw_override);
    // BUG-CM — read-back MUST project the override onto the node's startup
    // config; otherwise a saved cluster-wide change is invisible here and the
    // editor looks broken. Serialise the whole TieringConfig (all fields, not a
    // hand-picked subset) and overlay the override's `tiering` sub-object.
    let mut tiering = serde_json::to_value(&state.config.tiering).unwrap_or(Value::Null);
    if let Some(over_tiering) = raw_override.get("tiering") {
        json_deep_merge(&mut tiering, over_tiering);
    }
    // CO-5-9 — the `defaults` block MUST also project the override on top,
    // exactly like `tiering` above (BUG-CM pattern). Pre-fix it was built from
    // raw `state.config.defaults`, so a cluster-wide `defaults.*` override was
    // invisible on read-back. Serialise the whole VolumeDefaults and overlay
    // the override's `defaults` sub-object.
    let mut defaults = serde_json::to_value(&state.config.defaults).unwrap_or(Value::Null);
    if let Some(over_defaults) = raw_override.get("defaults") {
        json_deep_merge(&mut defaults, over_defaults);
    }
    Json(json!({
        "version": sm.cluster_config.version,
        "config": raw_override,
        "defaults": defaults,
        "tiering": tiering,
    }))
}

#[derive(Deserialize)]
pub(super) struct ConfigUpdateRequest {
    pub(super) config_json: String,
    /// AZ-5 — when true, also persist the merged config back to
    /// `/etc/dtfs/manager.toml` (this manager only). No daemon reads the
    /// stored Raft override; only the TOML, at the next restart.
    #[serde(default)]
    pub(super) write_to_disk: bool,
    /// Bug BD-3 — operator must explicitly acknowledge any write
    /// that affects cluster-wide keys. The WebUI sets this only on
    /// the Cluster-tab save action. Without it, the endpoint
    /// refuses to write, preventing accidental drift caused by
    /// an operator editing a cluster-wide key from a per-host
    /// view that lacks federation.
    #[serde(default)]
    pub(super) acknowledge_cluster_wide: bool,
}

/// Path of the manager config file rewritten by `write_to_disk`.
pub(super) const MANAGER_CONFIG_PATH: &str = "/etc/dtfs/manager.toml";

/// REV-4-3 — render the validated merged config as the TOML that would
/// replace `manager.toml`, and prove it parses back through the SAME
/// deserialiser the daemon uses at startup (`load_config` is
/// `toml::from_str`).
///
/// Returns the TOML text, or a human-readable reason to refuse the write.
/// Anything that cannot be re-parsed here would make the manager fail to
/// load its own config on the next restart, so it never reaches the disk.
pub(super) fn render_manager_toml(cfg: &dtfs_common::config::ManagerConfig) -> Result<String, String> {
    let rendered = toml::to_string_pretty(cfg)
        .map_err(|e| format!("config could not be serialised to TOML: {e}"))?;
    toml::from_str::<dtfs_common::config::ManagerConfig>(&rendered)
        .map_err(|e| format!("rendered TOML does not parse back as a manager config: {e}"))?;
    Ok(rendered)
}

/// REV-4-3 — replace the config file at `path` with `rendered`.
///
/// The write goes to a sibling temp file that is `sync_all`ed before the
/// rename, so a crash mid-write can never leave a truncated config the
/// manager cannot start from. Any error removes the temp file and leaves
/// the target exactly as it was.
pub(super) fn write_manager_toml_atomic(path: &std::path::Path, rendered: &str) -> Result<(), String> {
    let tmp = path.with_extension("toml.az5.tmp");
    let write = || -> std::io::Result<()> {
        use std::io::Write;
        // 0600: the file carries cluster.shared_secret.
        let _ = std::fs::remove_file(&tmp); // a stale tmp would keep its old mode
        let mut oo = std::fs::OpenOptions::new();
        oo.write(true).create(true).truncate(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut oo, 0o600);
        let mut f = oo.open(&tmp)?;
        f.write_all(rendered.as_bytes())?;
        f.sync_all()?;
        drop(f);
        std::fs::rename(&tmp, path)
    };
    write().map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        format!("could not write {}: {e}", path.display())
    })
}

/// Deep-merge `incoming` onto the stored cluster override. Empty `incoming`
/// (explicit reset) stays empty; unparsable stored JSON is treated as empty.
pub(super) fn merge_onto_stored_override(stored: &str, incoming: &str) -> String {
    if incoming.is_empty() {
        return String::new();
    }
    let mut base: Value = serde_json::from_str(stored).unwrap_or(Value::Null);
    let Ok(over) = serde_json::from_str::<Value>(incoming) else {
        return incoming.to_string();
    };
    if !base.is_object() {
        return incoming.to_string();
    }
    json_deep_merge(&mut base, &over);
    serde_json::to_string(&base).unwrap_or_else(|_| incoming.to_string())
}

pub(super) async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ConfigUpdateRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // AZ-5-A3 — boundary check: parse the incoming JSON against
    // a strict ManagerConfig before touching Raft so a typo or
    // out-of-range value returns 400 with the pre-attempt
    // (current) value, leaving the live config + on-disk file
    // untouched.
    // CO-2-1 (security) — the read path redacts secret leaves to the "***"
    // sentinel, so a round-tripped editor payload carries "***" for any secret
    // the operator did not change. Strip those leaves from the override before
    // it reaches Raft, so the deep-merge keeps the real shared_secret instead
    // of overwriting it with the literal sentinel. Done first, so the parsed
    // value below and the on-disk write both see the sanitised JSON.
    let config_json = if body.config_json.is_empty() {
        body.config_json.clone()
    } else {
        match serde_json::from_str::<serde_json::Value>(&body.config_json) {
            Ok(mut v) => {
                strip_secret_sentinels(&mut v);
                restore_array_sentinels(&mut v, &effective_config_value(&state).await);
                // The override replicates to every manager; a per-host key
                // (node.hostname, api.listen_addr, ...) must not.
                let bad = non_cluster_keys(&v);
                if !bad.is_empty() {
                    warn!(keys = ?bad, "config update rejected: node-scoped keys");
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!("node-scoped keys cannot be set cluster-wide: {}", bad.join(", ")),
                            "preserved_current": true,
                        })),
                    ));
                }
                serde_json::to_string(&v).unwrap_or_else(|_| body.config_json.clone())
            }
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": format!("invalid config JSON: {e}"),
                        "preserved_current": true,
                    })),
                ));
            }
        }
    };

    // The editor sends only the keys it edited, but UpdateConfig replaces the
    // stored override wholesale: merge onto the stored override first so
    // earlier settings survive.
    let config_json = {
        let stored = state.sm_data.read().await.cluster_config.config_json.clone();
        merge_onto_stored_override(&stored, &config_json)
    };

    // CO-4-3 — strict-validate the override by deep-merging it onto the node's
    // startup config and requiring the MERGED result to deserialize as a full
    // `ManagerConfig`. `UpdateConfig` REPLACES the cluster override wholesale
    // (see state_machine.rs), so `startup + this override` is exactly the
    // effective config that this save produces — and exactly what we persist.
    // A sparse override on its own can never parse as ManagerConfig (required
    // identity fields are absent), so the pre-fix code's lone
    // `from_str::<ManagerConfig>(override)` always failed and fell back to
    // writing raw JSON into the .toml. Validating the merge instead catches a
    // bad-type / out-of-range value (-> 400, live + on-disk config untouched)
    // and yields the canonical TOML to persist below.
    let merged_config: Option<dtfs_common::config::ManagerConfig> = if config_json.is_empty() {
        None
    } else {
        match merge_and_validate_override(&state.config, &config_json) {
            Ok(cfg) => Some(cfg),
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": e,
                        "preserved_current": true,
                    })),
                ));
            }
        }
    };

    // Bug BD-3 — guard cluster-wide writes. We treat any non-empty
    // payload that the WebUI would touch under the Cluster tab as
    // cluster-wide; the ack flag confirms the operator understands
    // this propagates to every manager. Node-only writes are accepted
    // without ack but only when `write_to_disk` is false (the Raft-stored
    // override is not applied live; manager.toml is the durability boundary).
    if body.write_to_disk && !body.acknowledge_cluster_wide {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "acknowledge_cluster_wide:true required for write_to_disk:true. \
                          Cluster-scoped values must match across every manager; the WebUI \
                          Cluster tab sets this automatically.",
                "preserved_current": true,
            })),
        ));
    }
    // REV-4-3 — a write_to_disk request replaces the file the manager parses
    // at startup, so render the TOML and prove it re-parses through the
    // daemon's own deserialiser BEFORE the Raft write. Refusing here leaves
    // both the live config and the file on disk exactly as they were; a
    // rendering that cannot be read back is a manager that does not restart.
    let pending_toml: Option<String> = match (body.write_to_disk, merged_config.as_ref()) {
        (true, Some(cfg)) => match render_manager_toml(cfg) {
            Ok(t) => Some(t),
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    Json(json!({
                        "error": format!("refusing write_to_disk: {e}"),
                        "preserved_current": true,
                    })),
                ));
            }
        },
        _ => None,
    };

    // CO-5-10 — read the base version and carry it as `expected_version` so
    // the apply can CAS. Two saves racing off the same base both target
    // `base + 1`; the apply rejects the loser instead of letting it
    // blind-overwrite the winner (the old read-then-write lost updates).
    let base_version = {
        let sm = state.sm_data.read().await;
        sm.cluster_config.version
    };
    let version = base_version + 1;

    let req = ClientRequest::UpdateConfig { now_ms: crate::metadata::stamp_now_ms(),
        config_json: config_json.clone(),
        version,
        expected_version: base_version,
    };

    let resp = state
        .raft
        .client_write(req)
        .await
        .map_err(|e| (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": format!("raft write failed: {e}")})),
        ))?;

    // CO-5-10 — a version-conflict rejection comes back as a committed
    // ClientResponse::Error (the apply ran, saw the mismatch, declined the
    // overwrite). Surface it as 409 so the editor re-reads and retries
    // instead of believing its lost write landed.
    if let dtfs_raft::ClientResponse::Error { message } = resp.data {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": message,
                "preserved_current": true,
            })),
        ));
    }

    // AZ-5-A2 / CO-4-3 / REV-4-3 — persist to disk via temp file + fsync +
    // atomic rename. The text is the validated MERGED `ManagerConfig`
    // rendered above — never the raw override JSON, which is sparse and not
    // a valid manager.toml — and it has already been proven to re-parse
    // through the startup deserialiser, so the manager can always restart
    // from what we write.
    let mut wrote_to_disk = false;
    if let Some(ref rendered) = pending_toml {
        let path = std::path::Path::new(MANAGER_CONFIG_PATH);
        match write_manager_toml_atomic(path, rendered) {
            Ok(()) => wrote_to_disk = true,
            Err(e) => warn!(error = %e, "AZ-5-A2 write_to_disk failed"),
        }
    }

    state.audit_log.log(
        "api",
        "config.update",
        &format!("version={version} write_to_disk={wrote_to_disk}"),
        "success",
    );

    Ok(Json(json!({
        "version": version,
        "status": "stored",
        "note": "stored cluster-wide in Raft as a record; daemons do not read it. Only write_to_disk changes behaviour: it rewrites this manager's TOML, effective after its restart",
        "write_to_disk": wrote_to_disk,
    })))
}

/// AZ-5-A1 — effective cluster config endpoint. Returns one entry per
/// flat `[section.key]` path with `name / scope / current / default`
/// so the WebUI /ui/config pane can render every knob (including
/// those at their default value).
///
/// `ManagerConfig` has no `Default` impl (sub-configs like NodeConfig
/// require operator-supplied identity), so the "default" column is
/// derived from a hand-curated baseline JSON below. Sub-configs that
/// the operator MUST set per-host (node, location, cluster, raft,
/// grpc, api, tls) render with `default = null` — the WebUI treats
/// null defaults as "operator must set; no upstream default".
pub(super) async fn get_config_effective(
    State(state): State<Arc<AppState>>,
) -> Json<Value> {
    // BUG-CM — effective = startup config with the Raft cluster override
    // projected on top, so saved cluster-wide changes read back as `current`
    // (else the editor reports stale `is_default` values).
    let current = effective_config_value(&state).await;
    let defaults = manager_config_defaults_baseline();

    let mut rows: Vec<Value> = Vec::new();
    flatten_config(&current, &defaults, "manager", "", &mut rows);

    Json(json!({
        "scope": "manager",
        "node_hostname": state.config.node.hostname.clone(),
        "entries": rows,
    }))
}

/// AZ-5-A1 — hand-curated default values for every ManagerConfig key
/// that has a documented upstream default. Per-host-required keys
/// (node identity, cluster membership, TLS material) emit `null`,
/// which the WebUI renders as "(operator must set)".
pub(super) fn manager_config_defaults_baseline() -> Value {
    json!({
        "node": {
            "hostname": null,
            "id": null,
            "data_dir": "/var/lib/dtfs/manager",
        },
        "location": {
            "region": "lab",
            "datacenter": null,
            "rack": null,
            "host": null,
        },
        "cluster": {
            "name": "dtfs-cluster",
            "bootstrap": false,
            "shared_secret": null,
            "initial_voters": [],
        },
        "raft": {
            "election_timeout_ms": 5000,
            "heartbeat_interval_ms": 1500,
            "snapshot_threshold": 5000,
        },
        "grpc": {
            "listen_addr": "0.0.0.0:9401",
            "advertise_addr": null,
        },
        "api": {
            "listen_addr": "0.0.0.0:9402",
        },
        "tls": {
            "enabled": false,
            "cert_path": null,
            "key_path": null,
            "ca_path": null,
        },
        "defaults": {
            "chunk_size_mb": 128,
            "replication": {"hot": 0, "warm": 2, "cold": 1, "tape": 1},
            "write_safety": {
                "min_replicas_before_ack": 1,
                "write_timeout_secs": 30,
            },
            "anchor_datacenter": null,
        },
        "tiering": {
            "enabled": true,
            "hot_threshold_reads_per_hour": 5,
            "cold_threshold_days": 14,
            "frozen_threshold_days": 90,
            "evaluation_interval_secs": 3600,
            "migration_bandwidth_limit_mbps": 200,
            "enable_cache_redundancy_escalation": false,
        },
        "monitoring": {
            "heartbeat_interval_secs": 5,
            "suspect_after_missed": 3,
            "down_after_missed": 6,
        },
        "alerts": {
            "sinks": [],
        },
        "audit": {
            "replicate_via_raft": false,
        },
    })
}

/// Bug BD-1 — classify a flattened ManagerConfig key into the
/// scope that decides whether the WebUI surfaces it under "Cluster"
/// (must be identical across every manager — safe to edit + propagate)
/// or under "Node" (intentionally per-host).
///
/// Anything that isn't in the table below defaults to `Node` —
/// safer, since the cluster tab would otherwise expose an unknown
/// key as cluster-wide and operators could change it expecting
/// global effect. Add new prefixes to the table when new sub-configs
/// arrive.
pub(crate) fn config_key_scope(dotted: &str) -> &'static str {
    // Trim the leading scope label produced by flatten_config
    // ("manager.", "data.", "fuse." …) before matching.
    let key = match dotted.find('.') {
        Some(i) => &dotted[i + 1..],
        None => dotted,
    };
    const CLUSTER_PREFIXES: &[&str] = &[
        "cluster.",
        "defaults.",
        "tiering.",
        "monitoring.",
        "alerts.",
        "audit.",
        "scrub.",
        "raft.election_timeout_ms",
        "raft.heartbeat_interval_ms",
        "raft.snapshot_threshold",
    ];
    for p in CLUSTER_PREFIXES {
        if key.starts_with(p) || key == p.trim_end_matches('.') {
            return "cluster";
        }
    }
    "node"
}

/// AZ-5-A1 helper — walk two parallel JSON values and produce one
/// flat row per leaf. `scope` is the top-level prefix
/// ("manager", "data", "fuse", or a hostname for per-host views).
pub(super) fn flatten_config(
    current: &Value,
    defaults: &Value,
    scope: &str,
    prefix: &str,
    out: &mut Vec<Value>,
) {
    use serde_json::Value as V;
    match current {
        V::Object(map) => {
            for (k, v) in map {
                let next_prefix = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                let def_v = defaults.get(k).unwrap_or(&V::Null);
                flatten_config(v, def_v, scope, &next_prefix, out);
            }
        }
        _ => {
            let dotted = format!("{scope}.{prefix}");
            let kind = config_key_scope(&dotted);
            // CO-2-1 (security) — the config read path is unauthenticated and
            // the WebUI renders every row's `current` value. Never emit a
            // secret leaf (the `shared_secret` that gates raft membership /
            // node registration) in cleartext; render it as the sentinel
            // "***", which the editor treats as "unchanged" on write-back.
            // Only a non-empty secret is masked, so an unset key still reads
            // as null and `is_default` stays meaningful.
            let secret = config_key_is_secret(prefix);
            let mut current_out: V = current.clone();
            if secret && matches!(current, V::String(s) if !s.is_empty()) {
                current_out = V::String(CONFIG_SECRET_SENTINEL.to_string());
            } else if current.is_array() {
                // Arrays are one leaf; mask secrets/URLs inside their elements
                // (alerts.webhooks[].secret / .url).
                redact_secret_leaves(&mut current_out);
                if prefix.ends_with("webhooks") {
                    mask_webhook_urls(&mut current_out);
                }
            }
            let masked = current_out != *current;
            // is_default compares the (possibly redacted) emitted value, so a
            // masked secret never round-trips its cleartext via this flag.
            let is_default = !masked && current == defaults;
            out.push(json!({
                "name": dotted,
                "scope": scope,
                // Bug BD-1 — WebUI tab dispatch reads this field.
                "scope_kind": kind,
                "current": current_out,
                "default": defaults,
                "is_default": is_default,
            }));
        }
    }
}

/// CO-2-1 — sentinel rendered in place of a redacted secret in the config
/// read path. A config WRITE that posts this value back means "keep the
/// existing secret" (see `update_config` / the Raft `UpdateConfig` apply).
pub(crate) const CONFIG_SECRET_SENTINEL: &str = "***";

/// CO-2-1 — true when the flattened config leaf at `prefix` (the dotted path
/// without the scope label, e.g. `cluster.shared_secret`) carries secret
/// material that must never be emitted in cleartext. Today that is only the
/// cluster membership secret; the TLS block holds file *paths*
/// (`tls.cert_file` / `tls.key_file` / `tls.ca_file`), not inline key bytes,
/// so no key material is serialised here. Extend this list if an inline-secret
/// config field is ever added.
pub(super) fn config_key_is_secret(prefix: &str) -> bool {
    let leaf = prefix.rsplit('.').next().unwrap_or(prefix);
    matches!(leaf, "shared_secret" | "secret" | "token" | "password")
}

/// CO-2-1 — recursively drop any secret leaf (per `config_key_is_secret`)
/// whose value is the redaction sentinel `"***"` from an incoming config
/// override. The read path renders unchanged secrets as the sentinel, so an
/// editor round-trip posts "***" back; dropping the key here means the
/// downstream deep-merge preserves the real stored secret instead of
/// overwriting it with the literal placeholder. A genuinely new secret value
/// (anything other than the sentinel) passes through untouched.
pub(super) fn strip_secret_sentinels(v: &mut Value) {
    if let Value::Object(map) = v {
        map.retain(|k, child| {
            if config_key_is_secret(k)
                && child.as_str() == Some(CONFIG_SECRET_SENTINEL)
            {
                return false;
            }
            true
        });
        for child in map.values_mut() {
            strip_secret_sentinels(child);
        }
    }
}

/// CO-2-1 — recursively mask any non-empty secret leaf (per
/// `config_key_is_secret`) to the redaction sentinel `"***"`, in place. Used
/// by `get_config`, which echoes the raw Raft override verbatim, so a
/// cluster-set `shared_secret` is never served in cleartext from this
/// unauthenticated endpoint. An empty/non-string secret value is left as-is.
pub(super) fn redact_secret_leaves(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for (k, child) in map.iter_mut() {
                let masked = config_key_is_secret(k)
                    && matches!(child, Value::String(s) if !s.is_empty());
                if masked {
                    *child = Value::String(CONFIG_SECRET_SENTINEL.to_string());
                } else {
                    redact_secret_leaves(child);
                    // Webhook URLs routinely embed tokens (Slack, Discord, ...).
                    if k == "webhooks" {
                        mask_webhook_urls(child);
                    }
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact_secret_leaves),
        _ => {}
    }
}

fn mask_webhook_urls(webhooks: &mut Value) {
    for item in webhooks.as_array_mut().into_iter().flatten() {
        if let Some(u) = item.get_mut("url") {
            if matches!(u, Value::String(s) if !s.is_empty()) {
                *u = Value::String(CONFIG_SECRET_SENTINEL.to_string());
            }
        }
    }
}

/// Dotted paths of the override's leaves whose `config_key_scope` is not cluster.
pub(super) fn non_cluster_keys(v: &Value) -> Vec<String> {
    fn walk(v: &Value, prefix: &str, out: &mut Vec<String>) {
        match v {
            Value::Object(m) => {
                for (k, c) in m {
                    let p = if prefix.is_empty() { k.clone() } else { format!("{prefix}.{k}") };
                    walk(c, &p, out);
                }
            }
            _ if config_key_scope(&format!("manager.{prefix}")) != "cluster" => out.push(prefix.to_string()),
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(v, "", &mut out);
    out
}

/// A posted array (replaced wholesale by the deep-merge) may carry the "***"
/// sentinel for a secret the operator did not change; put the stored value
/// back, matched by array index.
pub(super) fn restore_array_sentinels(new: &mut Value, old: &Value) {
    match (new, old) {
        (Value::Object(n), Value::Object(o)) => {
            for (k, nv) in n.iter_mut() {
                if let Some(ov) = o.get(k) {
                    restore_array_sentinels(nv, ov);
                }
            }
        }
        (Value::Array(n), Value::Array(o)) => {
            for (nv, ov) in n.iter_mut().zip(o) {
                restore_array_sentinels(nv, ov);
            }
        }
        (n @ Value::String(_), o @ Value::String(_))
            if n.as_str() == Some(CONFIG_SECRET_SENTINEL) =>
        {
            *n = o.clone();
        }
        _ => {}
    }
}

/// BUG-CM — recursively overlay `over` onto `base`. Objects merge key by
/// key (recursing into shared keys, inserting keys only in `over`); every
/// non-object value (scalar or array) is replaced wholesale by `over`. Keys
/// present only in `base` are preserved, so a sparse override such as
/// `{"tiering":{"evaluation_interval_secs":60}}` updates just that leaf and
/// leaves its siblings intact.
pub(super) fn json_deep_merge(base: &mut Value, over: &Value) {
    use serde_json::Value as V;
    match (base, over) {
        (V::Object(b), V::Object(o)) => {
            for (k, ov) in o {
                json_deep_merge(b.entry(k.clone()).or_insert(V::Null), ov);
            }
        }
        (b, o) => *b = o.clone(),
    }
}

/// CO-4-3 — deep-merge a (possibly sparse) override JSON onto a base
/// `ManagerConfig` (the node's startup config) and require the MERGED result
/// to deserialize as a full `ManagerConfig`. Returns the typed merge on
/// success, or a human-readable 400 reason on bad JSON / type error. The
/// caller persists `toml::to_string_pretty` of the returned config — never the
/// raw override. Because `UpdateConfig` replaces the cluster override
/// wholesale, `startup + override` is the post-save effective config.
pub(super) fn merge_and_validate_override(
    base: &dtfs_common::config::ManagerConfig,
    override_json: &str,
) -> Result<dtfs_common::config::ManagerConfig, String> {
    let over: Value = serde_json::from_str(override_json)
        .map_err(|e| format!("invalid config JSON: {e}"))?;
    let mut merged = serde_json::to_value(base)
        .map_err(|e| format!("could not serialise base config: {e}"))?;
    json_deep_merge(&mut merged, &over);
    serde_json::from_value::<dtfs_common::config::ManagerConfig>(merged)
        .map_err(|e| format!("config override does not type-check as ManagerConfig: {e}"))
}

/// BUG-CM — the effective manager config: the node's startup `state.config`
/// with the Raft-stored cluster override (`cluster_config.config_json`, set
/// via the editor) projected on top. The config read endpoints MUST go
/// through this, or a saved cluster-wide change is invisible on read-back and
/// the editor appears not to work.
pub(super) async fn effective_config_value(state: &Arc<AppState>) -> Value {
    let mut current = serde_json::to_value(&state.config).unwrap_or(Value::Null);
    let override_json = {
        let sm = state.sm_data.read().await;
        sm.cluster_config.config_json.clone()
    };
    if !override_json.is_empty() {
        if let Ok(over) = serde_json::from_str::<Value>(&override_json) {
            json_deep_merge(&mut current, &over);
        }
    }
    current
}

// ---------------------------------------------------------------------------
// Aggregate stats endpoint (consumed by WebUI home page)
// ---------------------------------------------------------------------------

/// Bug AL — compute true rolling I/O rate by diffing cumulative counters
/// against a process-local previous snapshot. Returns (read_bps,
/// write_bps). The pre-fix code divided cumulative bytes by
/// `now - min(last_seen_ms)` which was nonsensical (the staleness of
/// the slowest-updated node, NOT a time window). With record_read /
/// record_write now actually wired up in the data-node `put_chunk` /
/// `get_chunk` handlers (also Bug AL), the cumulative counters
/// finally grow under load.
///
/// Semantics:
/// * First call after manager start (or after a restart) returns
///   `(0, 0)` because there's no previous snapshot to diff against.
/// * Subsequent calls return `(bytes_since_last_call / seconds_since_last_call)`,
///   summed across all data nodes.
/// * If a node disappears between calls (decommission, network split),
///   its bytes are simply dropped from the next delta — the rate
///   doesn't go negative.
/// * If the manager's `/api/v1/stats` is hit very rapidly (e.g.
///   prometheus + WebUI both scraping at ~5s), the cached
///   `last_read_bps` / `last_write_bps` are returned for any call
///   whose window is < 1s, so the rate is stable instead of jumping
///   to a 100×-spiky number.
pub(super) fn compute_io_rate(
    tracker: &mut IoRateTracker,
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> (u64, u64) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    compute_io_rate_at(tracker, nodes, now_ms)
}
