//! REST handlers: read-only chunk placement diagnostics and manager memory
//! state. All GET; no Raft write, no auth change (LIS-18 exempts GET).
#![allow(unused_imports)]

use super::*;
use crate::placement_state::{for_each_chunk_placement, ChunkPlacementView, PlacementState};

const DEFAULT_LIMIT: usize = 200;
const MAX_LIMIT: usize = 5000;

#[derive(Debug, Deserialize)]
pub(super) struct ListChunksQuery {
    pub(super) state: Option<String>,
    pub(super) limit: Option<usize>,
}

fn state_name(s: PlacementState) -> &'static str {
    match s {
        PlacementState::Missing => "missing",
        PlacementState::Undergoal => "undergoal",
        PlacementState::Misplaced => "misplaced",
        PlacementState::Overgoal => "overgoal",
        PlacementState::Stable => "stable",
    }
}

/// `None` filter = every chunk; `Some(Pending)` = chunks with dangling rows.
enum StateFilter {
    Any,
    State(PlacementState),
    PendingDeletion,
}

fn parse_state_filter(s: Option<&str>) -> Result<StateFilter, String> {
    Ok(match s {
        None => StateFilter::Any,
        Some("missing") => StateFilter::State(PlacementState::Missing),
        Some("undergoal") => StateFilter::State(PlacementState::Undergoal),
        Some("misplaced") => StateFilter::State(PlacementState::Misplaced),
        Some("overgoal") => StateFilter::State(PlacementState::Overgoal),
        Some("pending_deletion") => StateFilter::PendingDeletion,
        Some(other) => {
            return Err(format!(
                "state must be one of missing|undergoal|misplaced|overgoal|pending_deletion, got {other:?}"
            ))
        }
    })
}

/// One replica row with node health / disk status, regardless of liveness
/// (unknown node or disk -> nulls).
fn replica_json(sm: &StateMachineData, r: &dtfs_raft::ChunkLocationEntry) -> Value {
    let node = r.node_id.parse::<u64>().ok().and_then(|id| sm.nodes.get(&id));
    let disk = node.and_then(|n| n.disk(r.disk_id));
    json!({
        "node_id": r.node_id,
        "disk_id": r.disk_id,
        "tier": disk.map(|d| d.tier),
        "datacenter": node.map(|n| n.location.datacenter.clone()),
        "node_health": node.map(|n| n.health),
        "disk_status": disk.map(|d| d.status),
    })
}

fn chunk_row_json(sm: &StateMachineData, v: &ChunkPlacementView<'_>) -> Value {
    json!({
        "chunk_id": v.chunk_id,
        "path": v.path,
        "volume_id": v.volume_id,
        "chunk_index": v.chunk_index,
        "state": state_name(v.state),
        "replicas": v.rows.iter().map(|r| replica_json(sm, r)).collect::<Vec<_>>(),
    })
}

/// Pure core of `GET /api/v1/chunks`. `total` counts matches before the
/// `limit` truncation.
pub(super) fn chunks_list_json(
    sm: &StateMachineData,
    state: Option<&str>,
    limit: Option<usize>,
) -> Result<Value, String> {
    let filter = parse_state_filter(state)?;
    let limit = limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let mut total = 0usize;
    let mut rows = Vec::new();
    for_each_chunk_placement(sm, |v| {
        let hit = match filter {
            StateFilter::Any => true,
            StateFilter::State(s) => v.state == s,
            StateFilter::PendingDeletion => v.dangling,
        };
        if !hit {
            return;
        }
        total += 1;
        if rows.len() < limit {
            rows.push(chunk_row_json(sm, &v));
        }
    });
    Ok(json!({
        "total": total,
        "limit": limit,
        "truncated": total > rows.len(),
        "chunks": rows,
    }))
}

/// Pure core of `GET /api/v1/chunks/{chunk_id}`. `Err` = bad id (400),
/// `Ok(None)` = unreferenced (404).
pub(super) fn chunk_lookup_json(
    sm: &StateMachineData,
    chunk_id: &str,
) -> Result<Option<Value>, String> {
    if chunk_id.is_empty()
        || chunk_id.len() > 128
        || !chunk_id.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err("chunk_id must be 1-128 hex characters".to_string());
    }
    let wanted = chunk_id.to_ascii_lowercase();
    let mut files = Vec::new();
    let mut replicas: Vec<Value> = Vec::new();
    let mut seen: std::collections::BTreeSet<(String, u32)> = Default::default();
    let mut seen_files: std::collections::BTreeSet<(String, u64)> = Default::default();
    for (path, f) in sm.files.iter() {
        for c in f.chunks.iter().filter(|c| c.chunk_hash.to_ascii_lowercase() == wanted) {
            if seen_files.insert((path.clone(), c.chunk_index)) {
                files.push(json!({ "path": path, "chunk_index": c.chunk_index }));
            }
            if seen.insert((c.node_id.clone(), c.disk_id)) {
                replicas.push(replica_json(sm, c));
            }
        }
    }
    if files.is_empty() {
        return Ok(None);
    }
    // Placement state comes from the first referencing logical chunk;
    // all references share the volume goal in practice.
    let mut placement: Option<(&'static str, bool)> = None;
    for_each_chunk_placement(sm, |v| {
        if placement.is_none() && v.chunk_id.to_ascii_lowercase() == wanted {
            placement = Some((state_name(v.state), v.dangling));
        }
    });
    let (state, dangling) = placement.unwrap_or(("missing", false));
    Ok(Some(json!({
        "chunk_id": wanted,
        "state": state,
        "pending_deletion": dangling,
        "files": files,
        "replicas": replicas,
    })))
}

/// Pure core of `GET /api/v1/memory`.
pub(super) fn memory_json(budget: Option<&dtfs_common::memory::MemoryBudget>) -> Value {
    match budget {
        None => json!({ "configured": false }),
        Some(b) => json!({
            "configured": b.hard_cap_bytes() > 0,
            "hard_cap_bytes": b.hard_cap_bytes(),
            "last_rss_bytes": b.last_rss_bytes(),
            "pressure_level": format!("{:?}", b.pressure_level()),
            "is_shedding": b.is_shedding(),
            "warn_threshold_bytes": b.warn_threshold_bytes(),
            "shed_threshold_bytes": b.shed_threshold_bytes(),
            "sample_interval_secs": b.sample_interval().as_secs(),
        }),
    }
}

/// `GET /api/v1/chunks?state=&limit=`
pub(super) async fn list_chunks(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ListChunksQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // Sync block: the SM read guard is dropped before any await.
    let out = {
        let sm = state.sm_data.read().await;
        chunks_list_json(&sm, q.state.as_deref(), q.limit)
    };
    out.map(Json)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))))
}

/// `GET /api/v1/chunks/{chunk_id}`
pub(super) async fn get_chunk(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(chunk_id): axum::extract::Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let out = {
        let sm = state.sm_data.read().await;
        chunk_lookup_json(&sm, &chunk_id)
    };
    match out {
        Err(e) => Err((StatusCode::BAD_REQUEST, Json(json!({ "error": e })))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("chunk {chunk_id} is not referenced by any file") })),
        )),
        Ok(Some(v)) => Ok(Json(v)),
    }
}

/// `GET /api/v1/memory`
pub(super) async fn get_memory(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(memory_json(state.memory.as_deref()))
}
