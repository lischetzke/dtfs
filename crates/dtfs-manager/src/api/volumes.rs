//! REST handlers: volume CRUD and the set-* policy endpoints. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.
#![allow(unused_imports)]

use super::*;

pub(super) async fn list_volumes(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;

    // Aggregate file counts and sizes per volume from the files map.
    let mut usage: std::collections::HashMap<String, (usize, u64)> =
        std::collections::HashMap::new();
    for meta in sm.files.values() {
        let entry = usage.entry(meta.volume_id.clone()).or_insert((0, 0));
        entry.0 += 1;
        // Group by chunk_index so replicas don't multiply the reported size.
        let mut by_index: std::collections::BTreeMap<u64, u64> =
            std::collections::BTreeMap::new();
        for c in &meta.chunks {
            let e = by_index.entry(c.chunk_index).or_insert(0);
            *e = (*e).max(c.size_bytes);
        }
        entry.1 += by_index.values().sum::<u64>();
    }

    // Merge with the volume configs stored in Raft state, falling back to
    // usage-only entries for volumes that haven't been formally configured.
    let mut volume_ids: std::collections::BTreeSet<String> =
        sm.volumes.keys().cloned().collect();
    volume_ids.extend(usage.keys().cloned());

    let volume_list: Vec<Value> = volume_ids
        .iter()
        .map(|vid| list_volumes_row_json(sm.volumes.get(vid), vid,
            usage.get(vid).copied().unwrap_or((0, 0))))
        .collect();

    Json(json!({
        "count": volume_list.len(),
        "volumes": volume_list,
    }))
}

/// Bug AV (AV-4-2) — pure helper for the per-volume row in
/// `GET /api/v1/volumes`. Embeds the full AV-3-5
/// `volume_config_to_json` projection (mandatory_replicas,
/// cache_replicas, anchor, …) and merges the aggregation fields
/// (`file_count`, `total_size`) so a single endpoint serves both the
/// usage view and the policy view. Legacy volumes (no AV fields set)
/// render with their default-zero values via `effective_*` helpers, so
/// older snapshots don't crash.
pub(crate) fn list_volumes_row_json(
    cfg: Option<&dtfs_common::types::VolumeConfig>,
    vid: &str,
    usage: (usize, u64),
) -> Value {
    let (file_count, total_size) = usage;
    if let Some(cfg) = cfg {
        let mut row = volume_config_to_json(cfg);
        if let Some(obj) = row.as_object_mut() {
            obj.insert("file_count".into(), json!(file_count));
            obj.insert("total_size".into(), json!(total_size));
        }
        row
    } else {
        json!({
            "volume_id": vid,
            "file_count": file_count,
            "total_size": total_size,
        })
    }
}

#[derive(Deserialize)]
pub(super) struct CreateVolumeRequest {
    pub(super) name: String,
    #[serde(default)]
    pub(super) chunk_size_mb: Option<u32>,
    #[serde(default)]
    pub(super) replication_hot: Option<u32>,
    #[serde(default)]
    pub(super) replication_warm: Option<u32>,
    #[serde(default)]
    pub(super) replication_cold: Option<u32>,
    /// P0-10: optional anchor DC. When set, all chunks on this volume keep
    /// at least one replica in this DC (and tiering refuses to migrate the
    /// last anchor copy).
    #[serde(default)]
    pub(super) anchor_datacenter: Option<String>,
}

/// CO-4-14 — canonical volume id derived from the operator-supplied name.
/// Factored out so the create-vs-upsert existence check is unit-testable
/// against a `StateMachineData` without standing up the full handler.
pub(super) fn volume_id_from_name(name: &str) -> String {
    name.to_lowercase().replace(' ', "-")
}

pub(super) async fn create_volume(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateVolumeRequest>,
) -> Result<Json<Value>, StatusCode> {
    use dtfs_common::types::{TierReplication, VolumeConfig, VolumeId, WriteSafety};

    let vol_id = volume_id_from_name(&body.name);

    // CO-4-14 — create is create, not upsert. Re-POSTing an existing volume
    // name must NOT blindly UpsertVolume: that wipes any set-mandatory /
    // cache / anchor / write-safety tuning applied since the first create
    // (apply is a blind BTreeMap insert). Reject with 409 instead; operators
    // change an existing volume through the dedicated tuning endpoints.
    if state.sm_data.read().await.volumes.contains_key(&vol_id) {
        return Err(StatusCode::CONFLICT);
    }

    let config = VolumeConfig {
        id: VolumeId::new(vol_id.clone()),
        name: body.name.clone(),
        chunk_size_bytes: body.chunk_size_mb
            .unwrap_or(state.config.defaults.chunk_size_mb) as u64
            * 1024 * 1024,
        replication: TierReplication {
            hot:  body.replication_hot.unwrap_or(state.config.defaults.replication.hot),
            warm: body.replication_warm.unwrap_or(state.config.defaults.replication.warm),
            cold: body.replication_cold.unwrap_or(state.config.defaults.replication.cold),
            tape: 1,
        },
        write_safety: WriteSafety {
            min_replicas_before_ack: state.config.defaults.write_safety.min_replicas_before_ack,
            write_timeout_secs:      state.config.defaults.write_safety.write_timeout_secs,
        },
        anchor_datacenter: body.anchor_datacenter.clone(),
        // Bug AV — new fields default to empty so the legacy
        // construction path lands a back-compat volume. Operators on the
        // new policy upgrade via `dtfs-cli volume migrate-policy`
        // (AV-1-10) and `volume set-cache` (AV-3-5).
        mandatory_replicas: dtfs_common::types::TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 },
        cache_replicas: dtfs_common::types::CacheReplication::default(),
        anchor: None,
        spill: dtfs_common::types::SpillPolicy::default(),
    };

    let req = ClientRequest::UpsertVolume { config };
    state.raft.client_write(req).await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    state.audit_log.log("api", "volume.create", &vol_id, "success");
    info!("Volume created: {}", vol_id);

    Ok(Json(json!({ "volume_id": vol_id, "name": body.name, "status": "created" })))
}

// ---------------------------------------------------------------------------
// Bug AV (AV-1-10) — POST /api/v1/volumes/{id}/migrate-policy
// ---------------------------------------------------------------------------

/// Bug AV — clone the legacy `replication` field into the new
/// `mandatory_replicas` field for the named volume. Idempotent:
/// running it twice (or against a volume that's already been
/// hand-tuned to the new schema) is a no-op. Legacy `replication`
/// is preserved as a rollback path.
pub(super) async fn migrate_volume_policy_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    // Resolve current config under one read lock.
    let current = {
        let sm = state.sm_data.read().await;
        sm.volumes.get(&volume_id).cloned()
    };
    let Some(current) = current else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };

    let migrated = current.migrated_to_mandatory_schema();

    // Detect a no-op so we can tell the operator whether anything
    // changed (handy in scripts).
    let changed = current != migrated;

    if changed {
        let req = ClientRequest::UpsertVolume { config: migrated.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e,
                "migrate_volume_policy Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log(
            "api",
            "volume.migrate_policy",
            &volume_id,
            "success",
        );
    } else {
        state.audit_log.log(
            "api",
            "volume.migrate_policy",
            &volume_id,
            "noop",
        );
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed":   changed,
        "mandatory_replicas": {
            "hot":  migrated.mandatory_replicas.hot,
            "warm": migrated.mandatory_replicas.warm,
            "cold": migrated.mandatory_replicas.cold,
            "tape": migrated.mandatory_replicas.tape,
        },
    })))
}

// ---------------------------------------------------------------------------
// Bug AV (AV-3-5) — GET /api/v1/volumes/{id} + POST .../set-cache
// ---------------------------------------------------------------------------

/// AV-3-5 — return the full VolumeConfig including the new
/// mandatory_replicas / cache_replicas / anchor fields. Closes
/// the AV-1-10 carryover for single-volume reads. The list
/// endpoint (`GET /api/v1/volumes`) still uses its
/// aggregation-only projection because broadening it risks
/// breaking existing WebUI/CLI clients that assume the legacy
/// shape; consumers wanting the full policy should call this
/// endpoint per-volume.
pub(super) async fn get_volume_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;
    let sm = state.sm_data.read().await;
    let Some(cfg) = sm.volumes.get(&volume_id) else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };
    Ok(Json(volume_config_to_json(cfg)))
}

/// AV-3-5 pure helper — render a `VolumeConfig` to the JSON shape
/// the GET and set-cache endpoints return. Pulled out so the
/// shape can be unit-tested without spinning an axum runtime.
// Bug BF-9 / BF-10 — back-compat surface that echoes the deprecated
// schedule fields verbatim so older clients still parse the response.
// Suppress the deprecation read warning at the function boundary.
#[allow(deprecated)]
pub(crate) fn volume_config_to_json(
    cfg: &dtfs_common::types::VolumeConfig,
) -> serde_json::Value {
    let eff_mandatory = cfg.effective_mandatory_replicas();
    let anchor = cfg.effective_anchor();
    json!({
        "volume_id": cfg.id,
        "name": cfg.name,
        "chunk_size_bytes": cfg.chunk_size_bytes,
        "replication": {
            "hot":  cfg.replication.hot,
            "warm": cfg.replication.warm,
            "cold": cfg.replication.cold,
            "tape": cfg.replication.tape,
        },
        "mandatory_replicas": {
            "hot":  eff_mandatory.hot,
            "warm": eff_mandatory.warm,
            "cold": eff_mandatory.cold,
            "tape": eff_mandatory.tape,
        },
        "cache_replicas": {
            "hot":  { "min": cfg.cache_replicas.hot.min,  "max": cfg.cache_replicas.hot.max  },
            "warm": { "min": cfg.cache_replicas.warm.min, "max": cfg.cache_replicas.warm.max },
            "cold": { "min": cfg.cache_replicas.cold.min, "max": cfg.cache_replicas.cold.max },
            "tape": { "min": cfg.cache_replicas.tape.min, "max": cfg.cache_replicas.tape.max },
            "per_dc": cfg.cache_replicas.per_dc,
            "promote_reads": cfg.cache_replicas.promote_reads,
            "promote_window_secs": cfg.cache_replicas.promote_window_secs,
            "cooldown_secs": cfg.cache_replicas.cooldown_secs,
        },
        "anchor": anchor.as_ref().map(|a| json!({
            "datacenter": a.datacenter,
            "required_tier": a.required_tier.map(|t| format!("{:?}", t).to_lowercase()),
        })),
        "write_safety": {
            "min_replicas_before_ack": cfg.write_safety.min_replicas_before_ack,
            "write_timeout_secs":      cfg.write_safety.write_timeout_secs,
        },
        // Bug BZ — per-volume capacity-spill exclusions.
        "spill": {
            "allow":                 cfg.spill.allow,
            "forbidden_tiers":       cfg.spill.forbidden_tiers,
            "forbidden_datacenters": cfg.spill.forbidden_datacenters,
        },
    })
}

/// AV-3-5 — request body for `POST /api/v1/volumes/{id}/set-cache`.
/// Every field is optional; only the supplied ones are merged.
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetCacheRequest {
    pub warm_min: Option<u32>,
    pub warm_max: Option<u32>,
    pub hot_min: Option<u32>,
    pub hot_max: Option<u32>,
    // Bug BT (follow-on) — full per-tier cache editing. Cold/tape cache is
    // atypical (cold is usually mandatory, tape isn't a cache target) but
    // exposing it completes the volume editor.
    #[serde(default)]
    pub cold_min: Option<u32>,
    #[serde(default)]
    pub cold_max: Option<u32>,
    #[serde(default)]
    pub tape_min: Option<u32>,
    #[serde(default)]
    pub tape_max: Option<u32>,
    pub per_dc: Option<bool>,
    pub promote_reads: Option<u32>,
    pub promote_window_secs: Option<u64>,
    pub cooldown_secs: Option<u64>,
}

/// AV-3-5 pure helper — apply the partial update onto a clone of
/// the current volume config. Returns the updated config; the
/// caller decides whether to Raft-write it (skip if no change).
/// Validates `max >= min` per tier; rejects with an error string
/// describing the offending tier.
pub(crate) fn apply_set_cache(
    current: &dtfs_common::types::VolumeConfig,
    req: &SetCacheRequest,
) -> Result<dtfs_common::types::VolumeConfig, String> {
    let mut next = current.clone();

    if let Some(m) = req.warm_min { next.cache_replicas.warm.min = m; }
    if let Some(m) = req.warm_max { next.cache_replicas.warm.max = m; }
    if let Some(m) = req.hot_min  { next.cache_replicas.hot.min  = m; }
    if let Some(m) = req.hot_max  { next.cache_replicas.hot.max  = m; }
    if let Some(m) = req.cold_min { next.cache_replicas.cold.min = m; }
    if let Some(m) = req.cold_max { next.cache_replicas.cold.max = m; }
    if let Some(m) = req.tape_min { next.cache_replicas.tape.min = m; }
    if let Some(m) = req.tape_max { next.cache_replicas.tape.max = m; }
    if let Some(v) = req.per_dc   { next.cache_replicas.per_dc   = v; }
    if let Some(v) = req.promote_reads
        { next.cache_replicas.promote_reads = v; }
    if let Some(v) = req.promote_window_secs
        { next.cache_replicas.promote_window_secs = v; }
    if let Some(v) = req.cooldown_secs
        { next.cache_replicas.cooldown_secs = v; }

    // Validate min<=max per tier — `validate()` lives on
    // CacheTierConfig already.
    next.cache_replicas.hot.validate()
        .map_err(|e| format!("hot tier: {e}"))?;
    next.cache_replicas.warm.validate()
        .map_err(|e| format!("warm tier: {e}"))?;
    next.cache_replicas.cold.validate()
        .map_err(|e| format!("cold tier: {e}"))?;
    next.cache_replicas.tape.validate()
        .map_err(|e| format!("tape tier: {e}"))?;

    Ok(next)
}

pub(super) async fn set_volume_cache_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
    Json(body): Json<SetCacheRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    let current = {
        let sm = state.sm_data.read().await;
        sm.volumes.get(&volume_id).cloned()
    };
    let Some(current) = current else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };

    let next = apply_set_cache(&current, &body)
        .map_err(|e| (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        ))?;

    let changed = current != next;
    if changed {
        let req = ClientRequest::UpsertVolume { config: next.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e,
                "set_volume_cache Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log(
            "api", "volume.set_cache", &volume_id, "success",
        );
    } else {
        state.audit_log.log(
            "api", "volume.set_cache", &volume_id, "noop",
        );
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed": changed,
        "config": volume_config_to_json(&next),
    })))
}

// ---------------------------------------------------------------------------
// CF-3a — POST /api/v1/volumes/{id}/set-write-safety
// ---------------------------------------------------------------------------

/// CF-3a — request body for
/// `POST /api/v1/volumes/{id}/set-write-safety`. Every field is
/// optional; supplied fields are merged onto the current
/// `write_safety`. Omitted fields keep their current value
/// (consistent with AV-3-5's `set-cache` semantics). This is the
/// runtime reconciliation tool for the durability-ack policy; today
/// the only way to change it is a full volume upsert.
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetWriteSafetyRequest {
    pub min_replicas_before_ack: Option<u32>,
    pub write_timeout_secs: Option<u32>,
}

/// CF-3a pure helper — merge a partial write_safety update onto a
/// clone of the current volume config. Returns the updated config;
/// the caller decides whether to Raft-write it (skip if no change).
///
/// Validation rules:
/// 1. `min_replicas_before_ack >= 1` if supplied (0 means "ack
///    before any replica is durable" which is nonsense — reject).
/// 2. `write_timeout_secs >= 1` if supplied (a zero timeout would
///    immediately abort every write).
pub(crate) fn apply_set_write_safety(
    current: &dtfs_common::types::VolumeConfig,
    req: &SetWriteSafetyRequest,
) -> Result<dtfs_common::types::VolumeConfig, String> {
    let mut next = current.clone();

    if let Some(v) = req.min_replicas_before_ack {
        if v < 1 {
            return Err(
                "min_replicas_before_ack must be >= 1".to_string()
            );
        }
        next.write_safety.min_replicas_before_ack = v;
    }
    if let Some(v) = req.write_timeout_secs {
        if v < 1 {
            return Err(
                "write_timeout_secs must be >= 1".to_string()
            );
        }
        next.write_safety.write_timeout_secs = v;
    }

    Ok(next)
}

pub(super) async fn set_volume_write_safety_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
    Json(body): Json<SetWriteSafetyRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    let current = {
        let sm = state.sm_data.read().await;
        sm.volumes.get(&volume_id).cloned()
    };
    let Some(current) = current else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };

    let next = apply_set_write_safety(&current, &body)
        .map_err(|e| (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        ))?;

    let changed = current != next;
    if changed {
        let req = ClientRequest::UpsertVolume { config: next.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e,
                "set_volume_write_safety Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log(
            "api", "volume.set_write_safety", &volume_id, "success",
        );
    } else {
        state.audit_log.log(
            "api", "volume.set_write_safety", &volume_id, "noop",
        );
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed": changed,
        "config": volume_config_to_json(&next),
    })))
}

// ---------------------------------------------------------------------------
// Bug AV (AV-4-1a) — POST /api/v1/volumes/{id}/set-mandatory
// ---------------------------------------------------------------------------

/// AV-4-1a — request body for
/// `POST /api/v1/volumes/{id}/set-mandatory`. Every field is
/// optional; supplied fields are merged onto the current
/// `mandatory_replicas`. Omitted fields keep their current value
/// (consistent with AV-3-5's `set-cache` semantics).
///
/// `deny_unknown_fields` so a wrong-shape body (e.g. a nested
/// `{"mandatory_replicas":{...}}` object) is a loud 422 rather than
/// deserializing to all-None and silently no-op'ing away a
/// durability-policy change. An empty `{}` body still deserializes
/// (all-None -> dispatcher detects no-op and skips the Raft write).
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetMandatoryRequest {
    pub hot_replicas: Option<u32>,
    pub warm_replicas: Option<u32>,
    pub cold_replicas: Option<u32>,
    pub tape_replicas: Option<u32>,
}

/// AV-4-1a pure helper — merge a partial mandatory_replicas
/// update onto the current volume config + validate the result.
///
/// Validation rules:
/// 1. **Reject all-zero result.** After the merge, at least one
///    mandatory tier must be > 0. All-zero would re-enable the
///    legacy-fallback path inside `effective_mandatory_replicas()`,
///    which is technically valid but is almost never what the
///    operator wanted — they should drop the volume + recreate
///    it explicitly if that's the intent.
/// 2. **Reject if anchor invariant unsatisfiable.** When the
///    volume has `anchor.required_tier = Some(tier)`, the
///    corresponding `mandatory_replicas.<tier>` must be > 0
///    (otherwise no chunk can ever satisfy the anchor).
///
/// Returns the new `VolumeConfig` on success; the dispatcher
/// decides whether to Raft-write it (skip when current == next).
pub(crate) fn apply_set_mandatory(
    current: &dtfs_common::types::VolumeConfig,
    req: &SetMandatoryRequest,
) -> Result<dtfs_common::types::VolumeConfig, String> {
    let mut next = current.clone();

    // AV-4-1a hardening — on a legacy volume (mandatory_replicas still
    // all-zero, so effective_mandatory_replicas() falls back to the legacy
    // `replication` floor), seed the mandatory map from that EFFECTIVE floor
    // BEFORE applying the partial overwrite. Otherwise a partial edit (e.g.
    // bump cold) silently zeroes every un-set tier's legacy floor (e.g. drops
    // the warm working-set RF) by shadowing the fallback. Seeding makes the
    // partial update layer onto the real floor instead of onto zeros; a volume
    // already on the new schema (any tier > 0) is left untouched.
    {
        let m = &current.mandatory_replicas;
        if m.hot == 0 && m.warm == 0 && m.cold == 0 && m.tape == 0 {
            next.mandatory_replicas = current.effective_mandatory_replicas().clone();
        }
    }

    if let Some(v) = req.hot_replicas  { next.mandatory_replicas.hot  = v; }
    if let Some(v) = req.warm_replicas { next.mandatory_replicas.warm = v; }
    if let Some(v) = req.cold_replicas { next.mandatory_replicas.cold = v; }
    if let Some(v) = req.tape_replicas { next.mandatory_replicas.tape = v; }

    // Validation 1: reject all-zero result.
    let m = &next.mandatory_replicas;
    if m.hot == 0 && m.warm == 0 && m.cold == 0 && m.tape == 0 {
        return Err(
            "all-zero mandatory_replicas would orphan the durability floor; \
             drop + recreate the volume if that's intentional".to_string(),
        );
    }

    // Validation 2: anchor.required_tier must stay satisfiable.
    if let Some(anchor) = next.effective_anchor() {
        if let Some(tier) = anchor.required_tier {
            let count = match tier {
                dtfs_common::types::DiskTier::Hot  => m.hot,
                dtfs_common::types::DiskTier::Warm => m.warm,
                dtfs_common::types::DiskTier::Cold => m.cold,
                dtfs_common::types::DiskTier::Tape => m.tape,
            };
            if count == 0 {
                return Err(format!(
                    "anchor.required_tier = {:?} but mandatory_replicas.{} = 0; \
                     anchor would be permanently unsatisfiable",
                    tier,
                    format!("{:?}", tier).to_lowercase(),
                ));
            }
        }
    }

    Ok(next)
}

/// Bug BM-3 — reject a mandatory tape-tier replica count that no
/// topology can ever satisfy. `mandatory_replicas` is enforced by the
/// under-replication scheduler, which would otherwise queue a failing
/// replication task for every chunk on the volume forever. The check is
/// evaluated against the *current* cluster topology at API time (a tape
/// disk added later lets a re-save succeed).
pub(crate) fn validate_tape_satisfiable(
    mandatory_tape: u32,
    has_tape_disk: bool,
) -> Result<(), String> {
    if mandatory_tape > 0 && !has_tape_disk {
        return Err(format!(
            "mandatory_replicas.tape={mandatory_tape} but no tape-tier disk is \
             registered in the cluster; the under-replication scheduler could \
             never satisfy it. Add a tape-tier disk first, or set \
             mandatory_replicas.tape=0."
        ));
    }
    Ok(())
}

/// Bug BP-5 — drop a `replication.tape` target a cluster with no tape disks
/// can never satisfy. Unlike `mandatory_replicas.tape` (BM-3, hard reject),
/// the legacy `replication.tape` is non-fatal scheduler drift, so we
/// normalize it to 0 at save time (mutating `cfg`) rather than rejecting.
/// Returns true when it changed the config. Idempotent.
pub(crate) fn normalize_tape_replication(
    cfg: &mut dtfs_common::types::VolumeConfig,
    has_tape_disk: bool,
) -> bool {
    if !has_tape_disk && cfg.replication.tape > 0 {
        cfg.replication.tape = 0;
        true
    } else {
        false
    }
}

/// Bug BM-3 — does any registered data node expose a tape-tier disk?
pub(super) fn cluster_has_tape_disk(
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> bool {
    nodes.values().any(|n| {
        n.disks
            .iter()
            .any(|d| d.tier == dtfs_common::types::DiskTier::Tape)
    })
}

/// AV-5-12 — request body for `POST /api/v1/volumes/{id}/set-anchor`.
/// Every field is optional; only the supplied ones are merged.
///
/// `datacenter` empty-string clears the anchor (sets to None).
/// `required_tier` accepts "hot" / "warm" / "cold" / "tape" / "" to
/// clear back to any-tier.
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetAnchorRequest {
    pub datacenter: Option<String>,
    pub required_tier: Option<String>,
}

/// AV-5-12 pure helper — merge a partial anchor update onto the
/// current volume config + validate. Sets `anchor` field; preserves
/// legacy `anchor_datacenter` (kept for back-compat).
///
/// Validation: when `required_tier` is `Some(t)` and `mandatory_replicas.<t>`
/// is 0, refuse — the anchor would be unsatisfiable forever.
pub(crate) fn apply_set_anchor(
    current: &dtfs_common::types::VolumeConfig,
    req: &SetAnchorRequest,
) -> Result<dtfs_common::types::VolumeConfig, String> {
    use dtfs_common::types::{AnchorPolicy, DiskTier};
    let mut next = current.clone();

    let current_anchor = next
        .effective_anchor()
        .unwrap_or(AnchorPolicy {
            datacenter: String::new(),
            required_tier: None,
        });

    let new_dc = req
        .datacenter
        .clone()
        .unwrap_or_else(|| current_anchor.datacenter.clone());
    let new_tier: Option<DiskTier> = match req.required_tier.as_deref() {
        None => current_anchor.required_tier,
        Some("") => None,
        Some("hot") => Some(DiskTier::Hot),
        Some("warm") => Some(DiskTier::Warm),
        Some("cold") => Some(DiskTier::Cold),
        Some("tape") => Some(DiskTier::Tape),
        Some(other) => return Err(format!("unknown required_tier: {other}")),
    };

    // Empty datacenter clears the anchor.
    if new_dc.is_empty() {
        next.anchor = None;
        next.anchor_datacenter = None;
        return Ok(next);
    }

    // Validate: required_tier reachable.
    if let Some(t) = new_tier {
        let mandatory_for_tier = next.effective_mandatory_replicas().for_tier(t);
        if mandatory_for_tier == 0 {
            return Err(format!(
                "anchor.required_tier = {:?} but mandatory_replicas.{} = 0; \
                 raise mandatory_replicas first or pick a different tier",
                t,
                format!("{:?}", t).to_lowercase()
            ));
        }
    }

    next.anchor = Some(AnchorPolicy {
        datacenter: new_dc.clone(),
        required_tier: new_tier,
    });
    next.anchor_datacenter = Some(new_dc);
    Ok(next)
}

pub(super) async fn set_volume_anchor_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
    Json(body): Json<SetAnchorRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    let current = {
        let sm = state.sm_data.read().await;
        sm.volumes.get(&volume_id).cloned()
    };
    let Some(current) = current else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };

    let next = apply_set_anchor(&current, &body)
        .map_err(|e| (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        ))?;

    let changed = current != next;
    if changed {
        let req = ClientRequest::UpsertVolume { config: next.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e,
                "set_volume_anchor Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log("api", "volume.set_anchor", &volume_id, "success");
    } else {
        state.audit_log.log("api", "volume.set_anchor", &volume_id, "noop");
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed": changed,
        "config": volume_config_to_json(&next),
    })))
}

// ---------------------------------------------------------------------------
// Bug BZ — POST /api/v1/volumes/{id}/set-spill — per-volume capacity-spill
// exclusions (allow override + forbidden tiers/datacenters).
// ---------------------------------------------------------------------------

/// Bug BZ — request body for `POST /api/v1/volumes/{id}/set-spill`. Every
/// field optional; supplied fields replace the corresponding spill setting.
/// `forbidden_tiers` accepts lowercase tier names; an empty list clears the
/// exclusion.
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetSpillRequest {
    pub allow: Option<bool>,
    pub forbidden_tiers: Option<Vec<String>>,
    pub forbidden_datacenters: Option<Vec<String>>,
}

/// Bug BZ pure helper — merge a partial spill-policy update + validate the
/// tier names.
pub(crate) fn apply_set_spill(
    current: &dtfs_common::types::VolumeConfig,
    req: &SetSpillRequest,
) -> Result<dtfs_common::types::VolumeConfig, String> {
    let mut next = current.clone();
    if let Some(a) = req.allow {
        next.spill.allow = Some(a);
    }
    if let Some(tiers) = &req.forbidden_tiers {
        let mut norm: Vec<String> = Vec::new();
        for t in tiers {
            let tl = t.trim().to_ascii_lowercase();
            if tl.is_empty() {
                continue;
            }
            if !matches!(tl.as_str(), "hot" | "warm" | "cold" | "tape") {
                return Err(format!("unknown tier in forbidden_tiers: {t}"));
            }
            if !norm.contains(&tl) {
                norm.push(tl);
            }
        }
        next.spill.forbidden_tiers = norm;
    }
    if let Some(dcs) = &req.forbidden_datacenters {
        let mut norm: Vec<String> = Vec::new();
        for d in dcs {
            let dt = d.trim().to_string();
            if dt.is_empty() {
                continue;
            }
            if !norm.contains(&dt) {
                norm.push(dt);
            }
        }
        next.spill.forbidden_datacenters = norm;
    }
    Ok(next)
}

pub(super) async fn set_volume_spill_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
    Json(body): Json<SetSpillRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    let current = {
        let sm = state.sm_data.read().await;
        sm.volumes.get(&volume_id).cloned()
    };
    let Some(current) = current else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };

    let next = apply_set_spill(&current, &body)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?;

    let changed = current != next;
    if changed {
        let req = ClientRequest::UpsertVolume { config: next.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e, "set_volume_spill Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log("api", "volume.set_spill", &volume_id, "success");
    } else {
        state.audit_log.log("api", "volume.set_spill", &volume_id, "noop");
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed": changed,
        "config": volume_config_to_json(&next),
    })))
}

pub(super) async fn set_volume_mandatory_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
    Json(body): Json<SetMandatoryRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    let current = {
        let sm = state.sm_data.read().await;
        sm.volumes.get(&volume_id).cloned()
    };
    let Some(current) = current else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("volume not found: {volume_id}")})),
        ));
    };

    let mut next = apply_set_mandatory(&current, &body)
        .map_err(|e| (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        ))?;

    // Bug BM-3 — reject an unsatisfiable mandatory tape constraint
    // against the current topology. Bug BP-5 — and normalize a phantom
    // `replication.tape` to 0 on the same save (non-fatal drift).
    {
        let sm = state.sm_data.read().await;
        let has_tape = cluster_has_tape_disk(&sm.nodes);
        validate_tape_satisfiable(next.mandatory_replicas.tape, has_tape)
            .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?;
        if normalize_tape_replication(&mut next, has_tape) {
            warn!(volume = %volume_id,
                "Bug BP-5: dropped replication.tape -> 0 (no tape disks in cluster)");
        }
    }

    let changed = current != next;
    if changed {
        let req = ClientRequest::UpsertVolume { config: next.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e,
                "set_volume_mandatory Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log(
            "api", "volume.set_mandatory", &volume_id, "success",
        );
    } else {
        state.audit_log.log(
            "api", "volume.set_mandatory", &volume_id, "noop",
        );
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed": changed,
        "config": volume_config_to_json(&next),
    })))
}

// ---------------------------------------------------------------------------
// Audit log endpoint
// ---------------------------------------------------------------------------
