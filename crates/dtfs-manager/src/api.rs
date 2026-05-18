//! Manager REST API (Axum).
//!
//! Provides cluster management endpoints consumed by the WebUI, CLI,
//! and external integrations.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use dtfs_common::config::ManagerConfig;
use dtfs_common::topology::Location;
use dtfs_common::types::{DiskStatus, DiskTier, HealthStatus, NodeRole};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ClientRequest, DtfsRaft, NodeInfo as RaftNodeInfo};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tracing::info;

use crate::audit::AuditLog;
use crate::join::JoinHandler;
use crate::scheduler::TaskScheduler;
use crate::webui;

/// Shared application state for API handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: ManagerConfig,
    pub raft: Arc<DtfsRaft>,
    pub sm_data: Arc<RwLock<StateMachineData>>,
    pub audit_log: Arc<AuditLog>,
    pub join: Arc<JoinHandler>,
    /// Scheduler used to enqueue manual path-prefix tier/DC migrations
    /// triggered from the WebUI / CLI (`POST /api/v1/migrations`).
    pub scheduler: Arc<TaskScheduler>,
}

/// Build the full API router (REST endpoints + WebUI).
pub fn build_router(state: Arc<AppState>) -> Router {
    let webui_state = Arc::new(webui::WebUiState {
        cluster_name: state.config.cluster.name.clone(),
        manager_hostname: state.config.node.hostname.clone(),
    });

    let api_routes = Router::new()
        // P1-7 Prometheus scrape endpoint
        .route("/metrics", get(prometheus_metrics))
        // P1-10 K8s/systemd health probes
        .route("/healthz", get(healthz_probe))
        .route("/readyz", get(readyz_probe))
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
        .route("/api/v1/volumes", get(list_volumes).post(create_volume))
        // Audit
        .route("/api/v1/audit", get(get_audit_log))
        // Config
        .route("/api/v1/config", get(get_config).post(update_config))
        // Scrub
        .route("/api/v1/scrub", post(trigger_scrub))
        // Manual data migration (operator-triggered tier/DC moves of a path
        // prefix). GET lists pending+recent tasks; POST enqueues new ones.
        .route(
            "/api/v1/migrations",
            get(list_migrations).post(start_path_migration),
        )
        // Stats (aggregate cluster metrics for WebUI home page)
        .route("/api/v1/stats", get(get_stats))
        // HMAC join (phase 1: challenge, phase 2: Raft register)
        .route("/api/v1/join/request", post(join_request_api))
        .route("/api/v1/join/complete", post(join_complete_api))
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
    join: Arc<JoinHandler>,
    scheduler: Arc<TaskScheduler>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let state = Arc::new(AppState {
        config: config.clone(),
        raft,
        sm_data,
        audit_log,
        join,
        scheduler,
    });

    let app = build_router(state);

    let listener = tokio::net::TcpListener::bind(&config.api.listen_addr).await?;
    tracing::info!("Manager REST API listening on {}", config.api.listen_addr);

    // Bug U — graceful shutdown: existing connections finish, no new ones accepted.
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await?;
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
// Join (HMAC handshake — `join::JoinHandler`)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct JoinRequestApi {
    nonce: String,
    hmac_hex: String,
    hostname: String,
}

async fn join_request_api(
    State(state): State<Arc<AppState>>,
    Json(body): Json<JoinRequestApi>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let hmac = hex::decode(body.hmac_hex.trim()).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("invalid hmac_hex: {e}")})),
        )
    })?;
    let r = state
        .join
        .handle_join_request(&body.nonce, &hmac, &body.hostname);
    Ok(Json(json!({
        "accepted": r.accepted,
        "challenge": r.challenge,
        "ca_cert_pem": String::from_utf8_lossy(&r.ca_cert_pem),
        "rejection_reason": r.rejection_reason,
    })))
}

#[derive(Debug, Deserialize)]
struct JoinDiskApi {
    disk_id: u32,
    uuid: String,
    mount_path: String,
    tier: String,
    #[serde(default)]
    total_bytes: u64,
    #[serde(default)]
    used_bytes: u64,
    #[serde(default)]
    status: String,
}

#[derive(Debug, Deserialize)]
struct JoinCompleteApi {
    node_id: u64,
    hostname: String,
    grpc_addr: String,
    #[serde(default)]
    region: String,
    #[serde(default)]
    datacenter: String,
    #[serde(default)]
    rack: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    disks: Vec<JoinDiskApi>,
}

fn tier_join(s: &str) -> DiskTier {
    match s.to_ascii_lowercase().as_str() {
        "warm" => DiskTier::Warm,
        "cold" => DiskTier::Cold,
        "tape" => DiskTier::Tape,
        _ => DiskTier::Hot,
    }
}

fn disk_status_join(s: &str) -> DiskStatus {
    match s.to_ascii_lowercase().as_str() {
        "degraded" => DiskStatus::Degraded,
        "draining" => DiskStatus::Draining,
        "failed" => DiskStatus::Failed,
        "removed" => DiskStatus::Removed,
        _ => DiskStatus::Healthy,
    }
}

async fn join_complete_api(
    State(state): State<Arc<AppState>>,
    Json(body): Json<JoinCompleteApi>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let disks: Vec<dtfs_common::types::DiskInfo> = body
        .disks
        .iter()
        .map(|d| dtfs_common::types::DiskInfo {
            disk_id: d.disk_id,
            uuid: d.uuid.clone(),
            mount_path: d.mount_path.clone(),
            tier: tier_join(&d.tier),
            total_bytes: d.total_bytes,
            used_bytes: d.used_bytes,
            status: disk_status_join(&d.status),
        })
        .collect();
    let total_capacity: u64 = disks.iter().map(|d| d.total_bytes).sum();
    let used_capacity: u64 = disks.iter().map(|d| d.used_bytes).sum();

    let info = RaftNodeInfo {
        role: NodeRole::Data,
        hostname: body.hostname.clone(),
        grpc_addr: body.grpc_addr.clone(),
        location: Location::new(
            &body.region,
            &body.datacenter,
            &body.rack,
            &body.host,
        ),
        disks,
        total_capacity,
        used_capacity,
        health: HealthStatus::Healthy,
        last_seen_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0),
        total_bytes_read: 0,
        total_bytes_written: 0,
    };

    let completion = state
        .join
        .complete_join(body.node_id, info)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": e.to_string()})),
            )
        })?;

    Ok(Json(json!({
        "success": completion.success,
        "config_version": completion.config_version,
        "config_json": completion.config_json,
    })))
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
    /// Unix milliseconds when this node was last observed alive.
    /// For data nodes: latest heartbeat. For managers: synthesised
    /// from the cluster config + the API server's "now" (since this
    /// API only responds while it's alive). For FUSE clients:
    /// approximate via the most-recent write_lease for any of their
    /// owner_id hostnames.
    last_seen_ms: u64,
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
            last_seen_ms: info.last_seen_ms,
        }
    }
}

async fn list_nodes(State(state): State<Arc<AppState>>) -> Json<Vec<NodeResponse>> {
    let sm = state.sm_data.read().await;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    // 1. Data + manager nodes registered in Raft (have heartbeat /
    //    join records).
    let mut nodes: Vec<NodeResponse> = sm
        .nodes
        .iter()
        .map(|(id, info)| NodeResponse::from_entry(*id, info))
        .collect();

    // 2. WebUI bug — managers don't self-register through the
    //    data-node heartbeat path so they were missing from this list
    //    and from Home → "Data & Manager Nodes" and from Topology.
    //    Synthesise entries from the cluster's initial-voters list;
    //    if a manager's hostname is also in `sm.nodes` (e.g. because
    //    a manager also runs as a data node on toro-plex), skip the
    //    synthetic entry.
    let known_hostnames: std::collections::HashSet<&str> = sm
        .nodes
        .values()
        .map(|n| n.hostname.as_str())
        .collect();
    for v in &state.config.cluster.initial_voters {
        if known_hostnames.contains(v.hostname.as_str()) {
            continue;
        }
        let id = dtfs_common::node_id_from_hostname(&v.hostname);
        nodes.push(NodeResponse {
            node_id: id.to_string(),
            hostname: v.hostname.clone(),
            grpc_addr: format!("http://{}", v.addr),
            location: format!("?/?/?/{}", v.hostname),
            disk_count: 0,
            total_capacity: 0,
            used_capacity: 0,
            health: "Healthy".to_string(),
            role: "Manager".to_string(),
            // The API server replying is itself a manager — peers are
            // assumed alive within the cluster heartbeat window. A
            // future fix wires per-manager last-seen via Raft membership.
            last_seen_ms: now_ms,
        });
    }

    // 3. FUSE clients — approximate from active write_leases. The
    //    `owner_id` format is "<hostname>:<pid>:<fh>"; the hostname
    //    is the FUSE client. We surface one entry per unique
    //    hostname, with last_seen = latest lease updated_at_ms.
    let mut client_last_seen: std::collections::HashMap<String, u64> = Default::default();
    for lease in sm.write_leases.values() {
        if let Some(host) = lease.owner_id.split(':').next() {
            if host.is_empty() {
                continue;
            }
            let e = client_last_seen.entry(host.to_string()).or_insert(0);
            if lease.expires_at_ms > *e {
                *e = lease.expires_at_ms;
            }
        }
    }
    for (host, last_seen) in client_last_seen {
        if known_hostnames.contains(host.as_str()) {
            continue;
        }
        let id = dtfs_common::node_id_from_hostname(&host);
        nodes.push(NodeResponse {
            node_id: id.to_string(),
            hostname: host.clone(),
            grpc_addr: String::new(),
            location: format!("?/?/?/{}", host),
            disk_count: 0,
            total_capacity: 0,
            used_capacity: 0,
            health: "Healthy".to_string(),
            role: "Client".to_string(),
            last_seen_ms: last_seen,
        });
    }

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
            // Replica entries (one per node holding a copy) all share a
            // chunk_index. Sum size per unique chunk_index, not per row.
            let mut sizes_by_index: std::collections::BTreeMap<u64, u64> =
                std::collections::BTreeMap::new();
            for c in &meta.chunks {
                let e = sizes_by_index.entry(c.chunk_index).or_insert(0);
                *e = (*e).max(c.size_bytes);
            }
            let total_size: u64 = sizes_by_index.values().sum();
            let chunk_count = sizes_by_index.len();
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
        "count": sm.files.len(),
        "files": files,
    }))
}

// ---------------------------------------------------------------------------
// Volume endpoints
// ---------------------------------------------------------------------------

async fn list_volumes(State(state): State<Arc<AppState>>) -> Json<Value> {
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
        .map(|vid| {
            let (file_count, total_size) = usage.get(vid).copied().unwrap_or((0, 0));
            if let Some(cfg) = sm.volumes.get(vid) {
                json!({
                    "volume_id": vid,
                    "name": cfg.name,
                    "chunk_size_bytes": cfg.chunk_size_bytes,
                    "replication": {
                        "hot": cfg.replication.hot,
                        "warm": cfg.replication.warm,
                        "cold": cfg.replication.cold,
                        "tape": cfg.replication.tape,
                    },
                    "write_safety": {
                        "min_replicas_before_ack": cfg.write_safety.min_replicas_before_ack,
                        "write_timeout_secs": cfg.write_safety.write_timeout_secs,
                    },
                    "file_count": file_count,
                    "total_size": total_size,
                })
            } else {
                json!({
                    "volume_id": vid,
                    "file_count": file_count,
                    "total_size": total_size,
                })
            }
        })
        .collect();

    Json(json!({
        "count": volume_list.len(),
        "volumes": volume_list,
    }))
}

#[derive(Deserialize)]
struct CreateVolumeRequest {
    name: String,
    #[serde(default)]
    chunk_size_mb: Option<u32>,
    #[serde(default)]
    replication_hot: Option<u32>,
    #[serde(default)]
    replication_warm: Option<u32>,
    #[serde(default)]
    replication_cold: Option<u32>,
    /// P0-10: optional anchor DC. When set, all chunks on this volume keep
    /// at least one replica in this DC (and tiering refuses to migrate the
    /// last anchor copy).
    #[serde(default)]
    anchor_datacenter: Option<String>,
}

async fn create_volume(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CreateVolumeRequest>,
) -> Result<Json<Value>, StatusCode> {
    use dtfs_common::types::{TierReplication, VolumeConfig, VolumeId, WriteSafety};

    let vol_id = body.name.to_lowercase().replace(' ', "-");
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
    };

    let req = ClientRequest::UpsertVolume { config };
    state.raft.client_write(req).await
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    state.audit_log.log("api", "volume.create", &vol_id, "success");
    info!("Volume created: {}", vol_id);

    Ok(Json(json!({ "volume_id": vol_id, "name": body.name, "status": "created" })))
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
// Aggregate stats endpoint (consumed by WebUI home page)
// ---------------------------------------------------------------------------

/// Compute a "seconds since the cluster started serving I/O" window.
/// Used as a denominator for the cumulative-bytes counters reported
/// in /api/v1/stats.io_read_bytes_per_sec etc. We use the cluster
/// uptime since the earliest-seen node's first heartbeat — a crude
/// long-window average that's plenty for the WebUI's purposes.
fn compute_io_window_secs(
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let mut earliest_ms = now_ms;
    for n in nodes.values() {
        // Approximate "first seen" as last_seen minus a default of
        // 60s window. We don't track first_seen explicitly; using
        // last_seen as both bounds gives 0 → guard with 1s.
        if n.last_seen_ms > 0 && n.last_seen_ms < earliest_ms {
            earliest_ms = n.last_seen_ms;
        }
    }
    let span_ms = now_ms.saturating_sub(earliest_ms);
    (span_ms / 1000).max(1)
}

async fn get_stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    use dtfs_common::types::{DiskTier, HealthStatus, NodeRole};

    let sm = state.sm_data.read().await;

    // ---- Node counts -----------------------------------------------------
    // Managers never register through the data-node heartbeat path so
    // `sm.nodes` only contains data nodes. The authoritative manager set
    // is the initial voters list (operator-supplied at bootstrap; matches
    // the Raft membership after any subsequent add/remove). Using it here
    // makes `manager_count` consistent with the WebUI's expectation even
    // though `sm.nodes.values().filter(role==Manager)` is always 0.
    let manager_count = state.config.cluster.initial_voters.len();

    let data_node_count = sm
        .nodes
        .values()
        .filter(|n| n.role == NodeRole::Data)
        .count();
    let disk_count: usize = sm.nodes.values().map(|n| n.disks.len()).sum();

    // FUSE clients don't have a registration RPC yet (no client heartbeats);
    // approximate active clients via the unique hostnames of currently-held
    // write leases (owner_id format = "<hostname>:<pid>:<fh>"). A read-only
    // client without an open writer won't appear, but for the soak the
    // workload generators always have a writer open somewhere. Treat as a
    // lower bound; sm.client_count remains for forward compatibility when
    // an explicit client RPC lands.
    let client_count = {
        let mut hosts = std::collections::HashSet::new();
        for lease in sm.write_leases.values() {
            if let Some(host) = lease.owner_id.split(':').next() {
                if !host.is_empty() {
                    hosts.insert(host.to_string());
                }
            }
        }
        hosts.len() as u64
    };

    // ---- Capacity aggregates --------------------------------------------
    let raw_total: u64 = sm.nodes.values().map(|n| n.total_capacity).sum();
    let raw_used: u64 = sm.nodes.values().map(|n| n.used_capacity).sum();

    // Per-tier breakdown of capacity + used so the WebUI can show how data
    // is distributed across hot/warm/cold/tape. Used is currently computed
    // per-node (not per-disk), so per-tier used is best-effort: we
    // proportionally split a node's `used_capacity` across its disks by
    // their capacity share within the node.
    let mut tier_total: std::collections::HashMap<DiskTier, u64> = Default::default();
    let mut tier_used: std::collections::HashMap<DiskTier, u64> = Default::default();
    let mut tier_disks: std::collections::HashMap<DiskTier, u32> = Default::default();
    for n in sm.nodes.values() {
        let node_cap: u64 = n.disks.iter().map(|d| d.total_bytes).sum();
        if node_cap == 0 {
            continue;
        }
        for d in &n.disks {
            *tier_total.entry(d.tier).or_default() += d.total_bytes;
            *tier_disks.entry(d.tier).or_default() += 1;
            // Apportion the node's used_capacity by this disk's share of
            // the node's total. Loses precision on multi-tier nodes if
            // usage is uneven across tiers — best signal available until
            // data nodes start reporting per-disk used in heartbeats.
            let share = d.total_bytes as f64 / node_cap as f64;
            *tier_used.entry(d.tier).or_default() +=
                (n.used_capacity as f64 * share) as u64;
        }
    }
    let tier_breakdown: Vec<Value> = [
        DiskTier::Hot,
        DiskTier::Warm,
        DiskTier::Cold,
        DiskTier::Tape,
    ]
    .iter()
    .map(|t| {
        let label = match t {
            DiskTier::Hot => "hot",
            DiskTier::Warm => "warm",
            DiskTier::Cold => "cold",
            DiskTier::Tape => "tape",
        };
        json!({
            "tier":  label,
            "disks": tier_disks.get(t).copied().unwrap_or(0),
            "total_bytes": tier_total.get(t).copied().unwrap_or(0),
            "used_bytes":  tier_used.get(t).copied().unwrap_or(0),
        })
    })
    .collect();

    // Net usable = raw_total / hot_replication_factor (conservative estimate).
    let hot_rf = state.config.defaults.replication.hot.max(1) as u64;
    let net_usable = raw_total / hot_rf;
    let net_used = raw_used / hot_rf;

    // WebUI bug — was hardcoded to 0. Aggregate cumulative bytes across
    // all data nodes and amortise over the heartbeat-observed window to
    // get a crude bytes/sec figure. Better than nothing; a future fix
    // tracks a true rolling-window rate per node.
    let io_window_secs = compute_io_window_secs(&sm.nodes);
    let total_bytes_read_all: u64 =
        sm.nodes.values().map(|n| n.total_bytes_read).sum();
    let total_bytes_written_all: u64 =
        sm.nodes.values().map(|n| n.total_bytes_written).sum();
    let io_read_bps = total_bytes_read_all / io_window_secs;
    let io_write_bps = total_bytes_written_all / io_window_secs;

    // Overall cluster health.
    let total_nodes = sm.nodes.len();
    let healthy_nodes = sm
        .nodes
        .values()
        .filter(|n| n.health == HealthStatus::Healthy)
        .count();
    let degraded_nodes = sm
        .nodes
        .values()
        .filter(|n| {
            matches!(
                n.health,
                HealthStatus::Suspect | HealthStatus::Degraded
            )
        })
        .count();
    let health = if total_nodes == 0 {
        "unknown"
    } else if degraded_nodes > 0 || healthy_nodes < total_nodes {
        if healthy_nodes == 0 {
            "critical"
        } else {
            "degraded"
        }
    } else {
        "healthy"
    };

    // Per-node scrub results (fix for issue #8 — previously only the single
    // most-recent cluster-wide result was kept, silently dropping all others).
    let scrub_results_by_node: Vec<Value> = sm
        .scrub_results
        .values()
        .map(|s| {
            json!({
                "node_id":         s.node_id,
                "disk_path":       s.disk_path,
                "level":           s.level,
                "completed_at_ms": s.completed_at_ms,
                "chunks_checked":  s.chunks_checked,
                "chunks_healthy":  s.chunks_healthy,
                "chunks_corrupt":  s.chunks_corrupt,
                "chunks_missing":  s.chunks_missing,
                "duration_secs":   s.duration_secs,
            })
        })
        .collect();

    // Backward-compatible `last_scrub` field: the most recent result across
    // all nodes, or null when no scrubs have run yet.
    let last_scrub = sm
        .scrub_results
        .values()
        .max_by_key(|s| s.completed_at_ms)
        .map(|s| {
            json!({
                "node_id":         s.node_id,
                "disk_path":       s.disk_path,
                "level":           s.level,
                "completed_at_ms": s.completed_at_ms,
                "chunks_checked":  s.chunks_checked,
                "chunks_healthy":  s.chunks_healthy,
                "chunks_corrupt":  s.chunks_corrupt,
                "chunks_missing":  s.chunks_missing,
                "duration_secs":   s.duration_secs,
            })
        });

    Json(json!({
        "health":                health,
        "healthy_nodes":         healthy_nodes,
        "total_nodes":           total_nodes,
        "manager_count":         manager_count,
        "data_node_count":       data_node_count,
        "disk_count":            disk_count,
        "client_count":          client_count,
        // Legacy field kept so older WebUI builds don't break; matches
        // sm.client_count for forward compat when client heartbeats land.
        "registered_clients":    sm.client_count,
        "file_count":            sm.files.len(),
        "raw_total_bytes":       raw_total,
        "raw_used_bytes":        raw_used,
        "net_usable_bytes":      net_usable,
        "net_used_bytes":        net_used,
        "tier_breakdown":        tier_breakdown,
        "io_read_bytes_per_sec":  io_read_bps,
        "io_write_bytes_per_sec": io_write_bps,
        "last_scrub":            last_scrub,
        "scrub_results_by_node": scrub_results_by_node,
        "config_version":        sm.cluster_config.version,
    }))
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
struct StartMigrationRequest {
    /// Path prefix to match. Empty string means "every file" — guarded by
    /// requiring `allow_empty_prefix=true` so an operator cannot accidentally
    /// stampede the whole cluster.
    path_prefix: String,
    /// Target tier — one of "hot", "warm", "cold", "tape".
    target_tier: String,
    /// Optional target datacenter. When set, the chosen target node must be
    /// in this DC (e.g. "dc-c").
    #[serde(default)]
    target_dc: Option<String>,
    /// Required when `path_prefix` is empty to avoid a footgun where a
    /// trailing slash typo migrates the entire volume.
    #[serde(default)]
    allow_empty_prefix: bool,
    /// Soft cap on tasks scheduled in a single call. Defaults to 10 000;
    /// extra chunks are reported under `truncated_chunks` and a follow-up
    /// call can pick them up.
    #[serde(default)]
    max_tasks: Option<usize>,
}

fn parse_tier(s: &str) -> Option<DiskTier> {
    match s.to_ascii_lowercase().as_str() {
        "hot" => Some(DiskTier::Hot),
        "warm" => Some(DiskTier::Warm),
        "cold" => Some(DiskTier::Cold),
        "tape" => Some(DiskTier::Tape),
        _ => None,
    }
}

async fn start_path_migration(
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

    if body.path_prefix.is_empty() && !body.allow_empty_prefix {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "empty path_prefix would match every file — set allow_empty_prefix=true to confirm",
            })),
        ));
    }

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

async fn list_migrations(State(state): State<Arc<AppState>>) -> Json<Value> {
    let snapshot = state.scheduler.migration_snapshot().await;
    Json(json!({
        "count": snapshot.len(),
        "tasks": snapshot,
    }))
}

// ---------------------------------------------------------------------------
// P1-7 + P1-10: Prometheus /metrics + health probes
// ---------------------------------------------------------------------------

/// `GET /metrics` — Prometheus text-format scrape.
async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
) -> (axum::http::StatusCode, [(&'static str, &'static str); 1], String) {
    let sm = state.sm_data.read().await;

    let total_files = sm.files.len() as u64;
    let total_volumes = sm.volumes.len() as u64;
    let total_chunks: u64 = sm.files.values().map(|f| f.chunks.len() as u64).sum();
    let active_leases = sm.write_leases.len() as u64;

    let mut healthy = 0u64;
    let mut suspect = 0u64;
    let mut down = 0u64;
    let mut total_capacity: u64 = 0;
    let mut used_capacity: u64 = 0;
    for info in sm.nodes.values() {
        match info.health {
            dtfs_common::types::HealthStatus::Healthy => healthy += 1,
            dtfs_common::types::HealthStatus::Suspect => suspect += 1,
            dtfs_common::types::HealthStatus::Down => down += 1,
            _ => {}
        }
        total_capacity = total_capacity.saturating_add(info.total_capacity);
        used_capacity = used_capacity.saturating_add(info.used_capacity);
    }

    let mut anchor_protected: u64 = 0;
    let mut migration_failed: u64 = 0;
    let mut migration_completed: u64 = 0;
    for entry in &sm.audit_log {
        let s = entry.summary.as_str();
        if s.contains("tier.anchor_protected") {
            anchor_protected += 1;
        }
        if s.contains("migration.failed") {
            migration_failed += 1;
        }
        if s.contains("tier.promote") || s.contains("tier.demote") {
            migration_completed += 1;
        }
    }

    let raft_log_size = sm.audit_log.len() as u64;

    let body = format!(
        "# HELP dtfs_files_total Number of files tracked by the cluster.\n\
         # TYPE dtfs_files_total gauge\n\
         dtfs_files_total {total_files}\n\
         # HELP dtfs_chunks_total Total chunk-location entries.\n\
         # TYPE dtfs_chunks_total gauge\n\
         dtfs_chunks_total {total_chunks}\n\
         # HELP dtfs_volumes_total Number of configured volumes.\n\
         # TYPE dtfs_volumes_total gauge\n\
         dtfs_volumes_total {total_volumes}\n\
         # HELP dtfs_nodes_healthy Number of nodes in Healthy state.\n\
         # TYPE dtfs_nodes_healthy gauge\n\
         dtfs_nodes_healthy {healthy}\n\
         # HELP dtfs_nodes_suspect Number of nodes in Suspect state.\n\
         # TYPE dtfs_nodes_suspect gauge\n\
         dtfs_nodes_suspect {suspect}\n\
         # HELP dtfs_nodes_down Number of nodes in Down state.\n\
         # TYPE dtfs_nodes_down gauge\n\
         dtfs_nodes_down {down}\n\
         # HELP dtfs_cluster_capacity_bytes Total cluster capacity in bytes.\n\
         # TYPE dtfs_cluster_capacity_bytes gauge\n\
         dtfs_cluster_capacity_bytes {total_capacity}\n\
         # HELP dtfs_cluster_used_bytes Used cluster capacity in bytes.\n\
         # TYPE dtfs_cluster_used_bytes gauge\n\
         dtfs_cluster_used_bytes {used_capacity}\n\
         # HELP dtfs_write_leases_active Currently held single-writer leases.\n\
         # TYPE dtfs_write_leases_active gauge\n\
         dtfs_write_leases_active {active_leases}\n\
         # HELP dtfs_anchor_protected_total Tiering decisions refused to protect anchor DC.\n\
         # TYPE dtfs_anchor_protected_total counter\n\
         dtfs_anchor_protected_total {anchor_protected}\n\
         # HELP dtfs_tier_migrations_completed_total Completed tier migrations (promote+demote).\n\
         # TYPE dtfs_tier_migrations_completed_total counter\n\
         dtfs_tier_migrations_completed_total {migration_completed}\n\
         # HELP dtfs_migration_failed_total Migration tasks that exhausted retries.\n\
         # TYPE dtfs_migration_failed_total counter\n\
         dtfs_migration_failed_total {migration_failed}\n\
         # HELP dtfs_audit_log_in_memory Audit log entries held in the FSM (ring-buffered).\n\
         # TYPE dtfs_audit_log_in_memory gauge\n\
         dtfs_audit_log_in_memory {raft_log_size}\n",
    );

    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

/// `GET /healthz` — liveness probe. Always 200 if the process responds.
async fn healthz_probe() -> (axum::http::StatusCode, &'static str) {
    (axum::http::StatusCode::OK, "ok\n")
}

/// `GET /readyz` — readiness probe. 200 only when at least one data node is
/// healthy AND the state machine has applied at least one log entry (so the
/// manager has finished its Raft warmup); 503 otherwise so K8s/systemd can
/// route around an in-recovery manager.
async fn readyz_probe(State(state): State<Arc<AppState>>) -> (axum::http::StatusCode, String) {
    let sm = state.sm_data.read().await;
    let any_healthy = sm
        .nodes
        .values()
        .any(|n| n.health == dtfs_common::types::HealthStatus::Healthy);
    let fsm_initialised = sm.last_applied_log.is_some();
    drop(sm);

    if fsm_initialised && any_healthy {
        (axum::http::StatusCode::OK, "ready\n".to_string())
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "not ready: fsm_initialised={fsm_initialised} any_healthy={any_healthy}\n"
            ),
        )
    }
}
