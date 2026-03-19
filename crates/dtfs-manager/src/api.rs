//! Manager REST API (Axum).
//!
//! Provides cluster management endpoints consumed by the WebUI, CLI,
//! and external integrations.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use dtfs_common::config::ManagerConfig;
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ClientRequest, DtfsRaft};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::info;

use crate::audit::AuditLog;
use crate::webui;

/// Shared application state for API handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: ManagerConfig,
    pub raft: Arc<DtfsRaft>,
    pub sm_data: Arc<RwLock<StateMachineData>>,
    pub audit_log: Arc<AuditLog>,
}

/// Build the full API router (REST endpoints + WebUI).
pub fn build_router(state: Arc<AppState>) -> Router {
    let webui_state = Arc::new(webui::WebUiState {
        cluster_name: state.config.cluster.name.clone(),
        manager_hostname: state.config.node.hostname.clone(),
    });

    let api_routes = Router::new()
        // Cluster
        .route("/api/v1/cluster/status", get(cluster_status))
        .route("/api/v1/health", get(health_check))
        // Nodes
        .route("/api/v1/nodes", get(list_nodes))
        .route("/api/v1/nodes/{node_id}", get(get_node))
        .route(
            "/api/v1/nodes/{node_id}/maintenance",
            post(set_maintenance),
        )
        // Files
        .route("/api/v1/files", get(list_files))
        // Volumes
        .route("/api/v1/volumes", get(list_volumes))
        // Audit
        .route("/api/v1/audit", get(get_audit_log))
        // Config
        .route("/api/v1/config", get(get_config).post(update_config))
        // Scrub
        .route("/api/v1/scrub", post(trigger_scrub))
        .with_state(state);

    // Merge API routes (higher priority) with WebUI (fallback).
    api_routes.merge(webui::router(webui_state))
}

/// Run the API server.
pub async fn run_api_server(
    config: ManagerConfig,
    raft: Arc<DtfsRaft>,
    sm_data: Arc<RwLock<StateMachineData>>,
    audit_log: Arc<AuditLog>,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        config: config.clone(),
        raft,
        sm_data,
        audit_log,
    });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&config.api.listen_addr).await?;
    tracing::info!("Manager REST API listening on {}", config.api.listen_addr);

    axum::serve(listener, app).await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Cluster endpoints
// ---------------------------------------------------------------------------

async fn cluster_status(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;
    let node_count = sm.nodes.len();
    let file_count = sm.files.len();
    let healthy_count = sm
        .nodes
        .values()
        .filter(|n| n.health == dtfs_common::types::HealthStatus::Healthy)
        .count();

    let total_capacity: u64 = sm.nodes.values().map(|n| n.total_capacity).sum();
    let used_capacity: u64 = sm.nodes.values().map(|n| n.used_capacity).sum();

    Json(json!({
        "status": "ok",
        "cluster_name": state.config.cluster.name,
        "node": state.config.node.hostname,
        "data_nodes": node_count,
        "data_nodes_healthy": healthy_count,
        "files": file_count,
        "total_capacity": total_capacity,
        "used_capacity": used_capacity,
        "config_version": sm.cluster_config.version,
    }))
}

async fn health_check() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({"status": "healthy"})))
}

// ---------------------------------------------------------------------------
// Node endpoints
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct NodeResponse {
    node_id: String,
    hostname: String,
    grpc_addr: String,
    location: String,
    disk_count: usize,
    total_capacity: u64,
    used_capacity: u64,
    health: String,
    role: String,
}

impl NodeResponse {
    fn from_entry(id: u64, info: &dtfs_raft::NodeInfo) -> Self {
        Self {
            node_id: id.to_string(),
            hostname: info.hostname.clone(),
            grpc_addr: info.grpc_addr.clone(),
            location: format!(
                "{}/{}/{}/{}",
                info.location.region,
                info.location.datacenter,
                info.location.rack,
                info.location.host,
            ),
            disk_count: info.disks.len(),
            total_capacity: info.total_capacity,
            used_capacity: info.used_capacity,
            health: format!("{:?}", info.health),
            role: format!("{:?}", info.role),
        }
    }
}

async fn list_nodes(State(state): State<Arc<AppState>>) -> Json<Vec<NodeResponse>> {
    let sm = state.sm_data.read().await;
    let nodes: Vec<NodeResponse> = sm
        .nodes
        .iter()
        .map(|(id, info)| NodeResponse::from_entry(*id, info))
        .collect();
    Json(nodes)
}

async fn get_node(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
) -> Result<Json<Value>, StatusCode> {
    let nid: u64 = node_id.parse().map_err(|_| StatusCode::BAD_REQUEST)?;
    let sm = state.sm_data.read().await;

    let info = sm.nodes.get(&nid).ok_or(StatusCode::NOT_FOUND)?;

    let disks: Vec<Value> = info
        .disks
        .iter()
        .map(|d| {
            json!({
                "disk_id": d.disk_id,
                "uuid": d.uuid,
                "mount_path": d.mount_path,
                "tier": format!("{:?}", d.tier),
                "total_bytes": d.total_bytes,
                "used_bytes": d.used_bytes,
                "status": format!("{:?}", d.status),
            })
        })
        .collect();

    Ok(Json(json!({
        "node_id": nid.to_string(),
        "hostname": info.hostname,
        "grpc_addr": info.grpc_addr,
        "location": {
            "region": info.location.region,
            "datacenter": info.location.datacenter,
            "rack": info.location.rack,
            "host": info.location.host,
        },
        "disks": disks,
        "total_capacity": info.total_capacity,
        "used_capacity": info.used_capacity,
        "health": format!("{:?}", info.health),
        "role": format!("{:?}", info.role),
    })))
}

#[derive(Deserialize)]
struct MaintenanceRequest {
    enable: bool,
}

async fn set_maintenance(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    Json(body): Json<MaintenanceRequest>,
) -> Result<Json<Value>, StatusCode> {
    let nid: u64 = node_id.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    let status_str = if body.enable {
        "maintenance"
    } else {
        "healthy"
    };

    let req = ClientRequest::UpdateNodeStatus {
        node_id: nid,
        status: status_str.to_string(),
    };

    state
        .raft
        .client_write(req)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    state.audit_log.log(
        "api",
        "maintenance.toggle",
        &format!("node={nid}"),
        if body.enable { "enabled" } else { "disabled" },
    );

    info!(
        "Maintenance mode {} for node {}",
        if body.enable { "enabled" } else { "disabled" },
        nid
    );

    Ok(Json(json!({
        "node_id": nid.to_string(),
        "maintenance": body.enable,
    })))
}

// ---------------------------------------------------------------------------
// File endpoints
// ---------------------------------------------------------------------------

async fn list_files(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;

    let files: Vec<Value> = sm
        .files
        .iter()
        .take(1000) // Limit response size
        .map(|(path, meta)| {
            let total_size: u64 = meta.chunks.iter().map(|c| c.size_bytes).sum();
            json!({
                "path": path,
                "volume_id": meta.volume_id,
                "chunk_count": meta.chunks.len(),
                "total_size": total_size,
                "created_at": meta.created_at_ms,
                "updated_at": meta.updated_at_ms,
            })
        })
        .collect();

    Json(json!({
        "count": sm.files.len(),
        "files": files,
    }))
}

// ---------------------------------------------------------------------------
// Volume endpoints
// ---------------------------------------------------------------------------

async fn list_volumes(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;

    // Aggregate volumes from file metadata.
    let mut volumes: std::collections::HashMap<String, (usize, u64)> =
        std::collections::HashMap::new();
    for meta in sm.files.values() {
        let entry = volumes.entry(meta.volume_id.clone()).or_insert((0, 0));
        entry.0 += 1; // file count
        entry.1 += meta.chunks.iter().map(|c| c.size_bytes).sum::<u64>(); // total size
    }

    let volume_list: Vec<Value> = volumes
        .iter()
        .map(|(vid, (file_count, total_size))| {
            json!({
                "volume_id": vid,
                "file_count": file_count,
                "total_size": total_size,
            })
        })
        .collect();

    Json(json!({
        "count": volume_list.len(),
        "volumes": volume_list,
    }))
}

// ---------------------------------------------------------------------------
// Audit log endpoint
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct AuditQuery {
    limit: Option<usize>,
    action: Option<String>,
}

async fn get_audit_log(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditQuery>,
) -> Json<Vec<Value>> {
    let filter = crate::audit::AuditFilter {
        limit: Some(query.limit.unwrap_or(50)),
        action: query.action,
        ..Default::default()
    };

    let entries = state.audit_log.query(&filter);
    let result: Vec<Value> = entries
        .into_iter()
        .map(|e| {
            json!({
                "timestamp": e.timestamp,
                "actor": e.actor,
                "action": e.action,
                "target": e.target,
                "result": e.result,
            })
        })
        .collect();

    Json(result)
}

// ---------------------------------------------------------------------------
// Config endpoints
// ---------------------------------------------------------------------------

async fn get_config(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;
    Json(json!({
        "version": sm.cluster_config.version,
        "config": if sm.cluster_config.config_json.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&sm.cluster_config.config_json).unwrap_or(Value::Null)
        },
        "defaults": {
            "chunk_size_mb": state.config.defaults.chunk_size_mb,
            "write_safety": {
                "min_replicas_before_ack": state.config.defaults.write_safety.min_replicas_before_ack,
                "write_timeout_secs": state.config.defaults.write_safety.write_timeout_secs,
            },
            "replication": {
                "hot": state.config.defaults.replication.hot,
                "warm": state.config.defaults.replication.warm,
                "cold": state.config.defaults.replication.cold,
            },
        },
        "tiering": {
            "enabled": state.config.tiering.enabled,
            "hot_threshold_reads_per_hour": state.config.tiering.hot_threshold_reads_per_hour,
            "cold_threshold_days": state.config.tiering.cold_threshold_days,
            "migration_bandwidth_limit_mbps": state.config.tiering.migration_bandwidth_limit_mbps,
        },
    }))
}

#[derive(Deserialize)]
struct ConfigUpdateRequest {
    config_json: String,
}

async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ConfigUpdateRequest>,
) -> Result<Json<Value>, StatusCode> {
    let version = {
        let sm = state.sm_data.read().await;
        sm.cluster_config.version + 1
    };

    let req = ClientRequest::UpdateConfig {
        config_json: body.config_json,
        version,
    };

    state
        .raft
        .client_write(req)
        .await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    state.audit_log.log(
        "api",
        "config.update",
        &format!("version={version}"),
        "success",
    );

    Ok(Json(json!({
        "version": version,
        "status": "applied",
    })))
}

// ---------------------------------------------------------------------------
// Scrub trigger endpoint
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ScrubRequest {
    target: Option<String>,
    target_type: Option<String>, // "disk", "file", "chunk", or "all"
    level: Option<String>,       // "quick" or "deep"
}

async fn trigger_scrub(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ScrubRequest>,
) -> Json<Value> {
    let target_type = body.target_type.as_deref().unwrap_or("all");
    let level = body.level.as_deref().unwrap_or("deep");
    let target = body.target.as_deref().unwrap_or("*");

    state.audit_log.log(
        "api",
        &format!("scrub.trigger.{level}"),
        &format!("{target_type}={target}"),
        "accepted",
    );

    info!(
        "Scrub triggered via API: type={}, target={}, level={}",
        target_type, target, level
    );

    Json(json!({
        "status": "accepted",
        "target_type": target_type,
        "target": target,
        "level": level,
    }))
}
