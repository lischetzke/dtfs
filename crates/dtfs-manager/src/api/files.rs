//! REST handlers: file listing/inspect, scrub trigger, migrations, cache flush. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.
#![allow(unused_imports)]

use super::*;

#[derive(Deserialize)]
pub(super) struct InspectFileQuery {
    pub(super) path: String,
}

/// Bug BE helper — return the resolved chunk list for a path, with
/// each chunk's hash + the node_ids holding it. Used by release-suite
/// cases that need to corrupt or count replicas without scraping
/// the WebUI Explorer.
/// REL-1-2 — resolve a replica's tier from the owning node's disk list so
/// /api/v1/files/inspect can distinguish a cold (mandatory/anchor) replica
/// from a warm (cache) one even on a multi-tier node. "unknown" if the node
/// or disk is no longer registered.
pub(super) fn tier_of_disk(node: Option<&dtfs_raft::NodeInfo>, disk_id: u32) -> String {
    node.and_then(|n| n.disks.iter().find(|d| d.disk_id == disk_id))
        .map(|d| format!("{:?}", d.tier).to_ascii_lowercase())
        .unwrap_or_else(|| "unknown".to_string())
}

pub(super) async fn inspect_file_endpoint(
    State(state): State<Arc<AppState>>,
    Query(q): Query<InspectFileQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let sm = state.sm_data.read().await;
    let file = sm.files.get(&q.path).ok_or_else(|| (
        StatusCode::NOT_FOUND,
        Json(json!({"error": format!("file not found: {}", q.path)})),
    ))?;
    // ChunkLocationEntry is one row per replica — group by chunk_index
    // so the response has one entry per logical chunk with the full
    // node_id set holding it.
    let mut by_index: std::collections::BTreeMap<u64, (String, u64, Vec<(String, u32)>)> =
        std::collections::BTreeMap::new();
    for c in &file.chunks {
        let e = by_index
            .entry(c.chunk_index)
            .or_insert_with(|| (c.chunk_hash.clone(), c.size_bytes, Vec::new()));
        e.2.push((c.node_id.clone(), c.disk_id));
    }
    // REL-1-2 — emit per-replica {node_id, disk_id, tier} alongside the bare
    // node_ids (kept for back-compat) so a caller can tell a cold (mandatory/
    // anchor) replica from a warm (cache) one on a multi-tier node.
    let chunks: Vec<Value> = by_index
        .into_iter()
        .map(|(idx, (hash, size, reps))| {
            let node_ids: Vec<String> = reps.iter().map(|(n, _)| n.clone()).collect();
            let replicas: Vec<Value> = reps
                .iter()
                .map(|(n, disk)| {
                    let node = n.parse::<u64>().ok().and_then(|nid| sm.nodes.get(&nid));
                    json!({
                        "node_id": n,
                        "disk_id": disk,
                        "tier":    tier_of_disk(node, *disk),
                    })
                })
                .collect();
            json!({
                "chunk_index": idx,
                "chunk_id":    hash,
                "size":        size,
                "node_ids":    node_ids,
                "replicas":    replicas,
            })
        })
        .collect();
    let total_size: u64 = file
        .logical_size
        .unwrap_or_else(|| file.chunks.iter().map(|c| c.size_bytes).sum());
    Ok(Json(json!({
        "path":        &file.path,
        "volume_id":   &file.volume_id,
        "total_size":  total_size,
        "chunk_size":  file.chunk_size,
        "chunks":      chunks,
        "chunk_count": file.chunks.iter()
            .map(|c| c.chunk_index)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
    })))
}

// ---------------------------------------------------------------------------
// File endpoints
// ---------------------------------------------------------------------------

/// Logical bytes of a file's chunk rows: replicas share a `chunk_index`, so
/// take the largest size per unique index rather than summing every row.
pub(super) fn unique_chunk_bytes(chunks: &[dtfs_raft::ChunkLocationEntry]) -> u64 {
    unique_chunk_stats(chunks).1
}

/// (unique chunk count, logical bytes); see `unique_chunk_bytes`.
pub(super) fn unique_chunk_stats(chunks: &[dtfs_raft::ChunkLocationEntry]) -> (usize, u64) {
    let mut by_index: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
    for c in chunks {
        let e = by_index.entry(c.chunk_index).or_insert(0);
        *e = (*e).max(c.size_bytes);
    }
    (by_index.len(), by_index.values().sum())
}

#[derive(Deserialize, Default)]
pub(super) struct ListFilesQuery {
    pub(super) limit: Option<usize>,
    pub(super) offset: Option<usize>,
    pub(super) prefix: Option<String>,
}

pub(super) const LIST_FILES_DEFAULT_LIMIT: usize = 1000;
pub(super) const LIST_FILES_MAX_LIMIT: usize = 100_000;

pub(super) async fn list_files(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ListFilesQuery>,
) -> Json<Value> {
    let sm = state.sm_data.read().await;
    let limit = q.limit.unwrap_or(LIST_FILES_DEFAULT_LIMIT).clamp(1, LIST_FILES_MAX_LIMIT);
    let offset = q.offset.unwrap_or(0);
    let prefix = q.prefix.as_deref().unwrap_or("");

    // `files` is ordered by path, so offset paging is stable between calls.
    let matching = || sm.files.iter().filter(|(p, _)| p.starts_with(prefix));
    let total = matching().count();
    let files: Vec<Value> = matching()
        .skip(offset)
        .take(limit)
        .map(|(path, meta)| {
            let (chunk_count, total_size) = unique_chunk_stats(&meta.chunks);
            json!({
                "path": path,
                "volume_id": meta.volume_id,
                "chunk_count": chunk_count,
                "total_size": total_size,
                "created_at": meta.created_at_ms,
                "updated_at": meta.updated_at_ms,
            })
        })
        .collect();

    Json(json!({
        // `count` keeps its old meaning (all files); `total` is the filtered count.
        "count": sm.files.len(),
        "total": total,
        "returned": files.len(),
        "truncated": offset + files.len() < total,
        "files": files,
    }))
}

// ---------------------------------------------------------------------------
// Volume endpoints
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub(super) struct ScrubRequest {
    pub(super) target: Option<String>,
    pub(super) target_type: Option<String>, // "disk", "file", "chunk", or "all"
    pub(super) level: Option<String>,       // "quick" or "deep"
}

/// CF-8 — manual scrub trigger. Raft-replicates a `PendingScrub` so the
/// heartbeat handler on every manager relays it to the data nodes
/// (which dedupe by id and run the scrub immediately). Pre-CF-8 this
/// handler wrote an audit row and dispatched nothing.
pub(super) async fn trigger_scrub(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ScrubRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let level = body.level.as_deref().unwrap_or("deep");
    if !matches!(level, "quick" | "deep") {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("level must be \"quick\" or \"deep\", got {level:?}")})),
        ));
    }
    // Per-target scrub is not implemented — be honest instead of
    // accepting-and-ignoring a target (the pre-CF-8 behaviour).
    let target_type = body.target_type.as_deref().unwrap_or("all");
    let target = body.target.as_deref().unwrap_or("*");
    if target_type != "all" || target != "*" {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "per-target scrub is not implemented — omit target/target_type for a full scrub of all disks"})),
        ));
    }

    let id = uuid::Uuid::new_v4().to_string();
    let requested_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    state
        .raft
        .client_write(ClientRequest::TriggerScrub {
            id: id.clone(),
            level: level.to_string(),
            requested_at_ms,
        })
        .await
        .map_err(|e| {
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            )
        })
        .and_then(|r| {
            crate::scheduler::client_response_ok(&r.data)
                .map_err(|m| err_body(StatusCode::CONFLICT, format!("scrub trigger refused: {m}")))
        })?;

    state.audit_log.log(
        "api",
        &format!("scrub.trigger.{level}"),
        &format!("all=* id={id}"),
        "accepted",
    );

    info!("Scrub triggered via API: level={}, id={}", level, id);

    Ok(Json(json!({
        "status": "accepted",
        "target_type": target_type,
        "target": target,
        "level": level,
        "id": id,
    })))
}

// ---------------------------------------------------------------------------
// Manual data migration — POST /api/v1/migrations, GET /api/v1/migrations
//
// Operators can ask the cluster to move every chunk under a path prefix to a
// chosen tier and (optionally) a chosen datacenter. The endpoint enqueues
// replication tasks via the existing scheduler — the tasks then ride the
// normal Pending → Copying → Verifying → Done lifecycle, so they show up in
// the migration_state_json snapshot like any auto-scheduled task.
//
// Semantics: "at least one replica satisfies target_tier (and target_dc when
// set)". Existing replicas are kept; the request is additive. If a chunk
// already has a satisfying replica, it's counted as `already_satisfied` and
// no task is scheduled.
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub(super) struct StartMigrationRequest {
    /// Path prefix to match. Empty string means "every file" — guarded by
    /// requiring `allow_empty_prefix=true` so an operator cannot accidentally
    /// stampede the whole cluster.
    pub(super) path_prefix: String,
    /// Target tier — one of "hot", "warm", "cold", "tape".
    pub(super) target_tier: String,
    /// Optional target datacenter. When set, the chosen target node must be
    /// in this DC (e.g. "dc-c").
    #[serde(default)]
    pub(super) target_dc: Option<String>,
    /// Required when `path_prefix` is empty to avoid a footgun where a
    /// trailing slash typo migrates the entire volume.
    #[serde(default)]
    pub(super) allow_empty_prefix: bool,
    /// Soft cap on tasks scheduled in a single call. Defaults to 10 000;
    /// extra chunks are reported under `truncated_chunks` and a follow-up
    /// call can pick them up.
    #[serde(default)]
    pub(super) max_tasks: Option<usize>,
}

/// "", whitespace and "/" all match every file; the allow_empty_prefix gate
/// must treat them alike.
pub(super) fn prefix_is_empty(p: &str) -> bool {
    matches!(p.trim(), "" | "/")
}

pub(super) fn parse_tier(s: &str) -> Option<DiskTier> {
    match s.to_ascii_lowercase().as_str() {
        "hot" => Some(DiskTier::Hot),
        "warm" => Some(DiskTier::Warm),
        "cold" => Some(DiskTier::Cold),
        "tape" => Some(DiskTier::Tape),
        _ => None,
    }
}

pub(super) async fn start_path_migration(
    State(state): State<Arc<AppState>>,
    Json(body): Json<StartMigrationRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let target_tier = parse_tier(&body.target_tier).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": format!(
                    "invalid target_tier '{}': expected one of hot|warm|cold|tape",
                    body.target_tier
                ),
            })),
        )
    })?;

    if prefix_is_empty(&body.path_prefix) && !body.allow_empty_prefix {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "empty path_prefix would match every file — set allow_empty_prefix=true to confirm",
            })),
        ));
    }

    // Scheduling reads leader-local state and writes through Raft; a follower
    // would report tasks_scheduled while the saves fail.
    check_is_leader(&state)?;

    let max_tasks = body.max_tasks.unwrap_or(10_000);

    let report = state
        .scheduler
        .schedule_path_migration(
            &body.path_prefix,
            target_tier,
            body.target_dc.as_deref(),
            max_tasks,
        )
        .await;

    if report.busy {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "scheduler busy advancing migrations, retry shortly" })),
        ));
    }

    state.audit_log.log(
        "api",
        "migration.path",
        &format!(
            "prefix={} tier={:?} dc={:?}",
            body.path_prefix, target_tier, body.target_dc
        ),
        &format!(
            "matched={} satisfied={} scheduled={} no_source={} no_target={}",
            report.files_matched,
            report.already_satisfied,
            report.tasks_scheduled,
            report.no_source,
            report.no_target,
        ),
    );

    info!(
        prefix = %body.path_prefix,
        tier = ?target_tier,
        dc = ?body.target_dc,
        scheduled = report.tasks_scheduled,
        "Manual path migration accepted"
    );

    Ok(Json(json!({
        "status": "accepted",
        "path_prefix": body.path_prefix,
        "target_tier": format!("{:?}", target_tier).to_lowercase(),
        "target_dc": body.target_dc,
        "files_matched": report.files_matched,
        "chunks_total": report.chunks_total,
        "already_satisfied": report.already_satisfied,
        "tasks_scheduled": report.tasks_scheduled,
        "no_source": report.no_source,
        "no_target": report.no_target,
        "truncated_chunks": report.truncated_chunks,
        "examples": report.examples,
    })))
}

pub(super) async fn list_migrations(State(state): State<Arc<AppState>>) -> Json<Value> {
    let snapshot = state.scheduler.migration_snapshot().await;
    Json(json!({
        "count": snapshot.len(),
        "tasks": snapshot,
    }))
}

// ---------------------------------------------------------------------------
// Bug AV (AV-1-9) — POST /api/v1/files/flush_cache
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub(super) struct FlushCacheRequest {
    /// Path prefix to flush. Empty matches every file in the cluster
    /// (requires `allow_empty_prefix=true`).
    pub(super) path_prefix: String,
    /// Tiers to flush. Defaults to `["warm", "hot"]` when unset — the
    /// typical non-mandatory cache tiers. Tiers configured as
    /// mandatory for a file's volume are filtered out by the planner
    /// regardless of this list.
    #[serde(default)]
    pub(super) cache_tiers: Option<Vec<String>>,
    /// Cooldown in seconds blocking re-promotion. Default 300 (5 min).
    #[serde(default)]
    pub(super) cooldown_secs: Option<u64>,
    /// Cap on in-flight delete actions per call. Default 10 000;
    /// overage reported under `truncated`.
    #[serde(default)]
    pub(super) max_tasks: Option<usize>,
    /// Safety gate against accidental "flush everything". Required
    /// when `path_prefix` is empty.
    #[serde(default)]
    pub(super) allow_empty_prefix: bool,
}

pub(super) async fn flush_cache_endpoint(
    State(state): State<Arc<AppState>>,
    Json(body): Json<FlushCacheRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    // REV-1-1 — leader-only. The flush deletes chunk bytes with a direct
    // data-node RPC and then records the removal through Raft; served by a
    // follower the delete lands and the metadata write does not, so the
    // location entries survive pointing at bytes that are gone.
    check_is_leader(&state)?;

    // Safety gate: empty prefix matches every file.
    if prefix_is_empty(&body.path_prefix) && !body.allow_empty_prefix {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "empty path_prefix would flush every file — set allow_empty_prefix=true to confirm",
            })),
        ));
    }

    // Parse cache_tiers. Default = warm + hot (the typical
    // opportunistic tiers); operator can override to e.g. [warm] only.
    let tiers: Vec<dtfs_common::types::DiskTier> = match body.cache_tiers {
        Some(list) => {
            let mut parsed = Vec::with_capacity(list.len());
            for t in &list {
                match parse_tier(t) {
                    Some(d) => parsed.push(d),
                    None => return Err((
                        StatusCode::BAD_REQUEST,
                        Json(json!({
                            "error": format!(
                                "invalid cache tier '{}': expected one of hot|warm|cold|tape",
                                t
                            ),
                        })),
                    )),
                }
            }
            parsed
        }
        None => vec![
            dtfs_common::types::DiskTier::Warm,
            dtfs_common::types::DiskTier::Hot,
        ],
    };

    let cooldown_secs = body.cooldown_secs.unwrap_or(300);
    let max_tasks    = body.max_tasks.unwrap_or(10_000);

    let report = state
        .scheduler
        .flush_cache_for_path(&body.path_prefix, &tiers, cooldown_secs, max_tasks)
        .await;

    state.audit_log.log(
        "api",
        "cache.flush",
        &format!(
            "prefix={} tiers={:?} cooldown={}s",
            body.path_prefix, tiers, cooldown_secs
        ),
        &format!(
            "matched={} dropped={} refusals={} truncated={}",
            report.files_matched,
            report.replicas_dropped,
            report.mandatory_refusals,
            report.truncated,
        ),
    );

    info!(
        prefix = %body.path_prefix,
        tiers = ?tiers,
        cooldown_secs,
        dropped = report.replicas_dropped,
        refusals = report.mandatory_refusals,
        "Cache flush accepted"
    );

    Ok(Json(json!({
        "status": "accepted",
        "path_prefix": body.path_prefix,
        "cache_tiers": tiers.iter()
            .map(|t| format!("{:?}", t).to_lowercase())
            .collect::<Vec<_>>(),
        "cooldown_secs": cooldown_secs,
        "files_matched":        report.files_matched,
        "chunks_examined":      report.chunks_examined,
        "replicas_dropped":     report.replicas_dropped,
        "mandatory_refusals":   report.mandatory_refusals,
        "truncated":            report.truncated,
        "unreachable_nodes":    report.unreachable_nodes,
        "rpc_failures":         report.rpc_failures,
        "raft_failures":        report.raft_failures,
    })))
}

// ---------------------------------------------------------------------------
// P1-7 + P1-10: Prometheus /metrics + health probes
// ---------------------------------------------------------------------------
