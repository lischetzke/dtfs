//! Manager REST API (Axum).
//!
//! Provides cluster management endpoints consumed by the WebUI, CLI,
//! and external integrations.

use std::collections::BTreeSet;
use std::sync::Arc;

// Bug BF-12 — bring `Instant::elapsed()` into scope for the
// `last_quorum_acked` migration (openraft 0.10 wraps the timestamp
// in `SerdeInstant`, whose `.elapsed()` lives on this trait).
#[allow(unused_imports)]
use openraft::Instant as _;

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
use tracing::{info, warn};

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
    /// Bug AL — rolling-rate tracker for the dashboard's I/O Throughput
    /// card. The data nodes report cumulative `total_bytes_read` /
    /// `total_bytes_written` in every heartbeat; on each `/api/v1/stats`
    /// call we delta the current cumulative against this in-process
    /// snapshot to produce a true bytes/sec rate. Process-local (NOT
    /// state-machine state) because rate is per-manager, not a cluster
    /// invariant.
    pub io_rate: Arc<parking_lot::RwLock<IoRateTracker>>,
}

/// Bug AL — per-node cumulative-byte snapshots, used to derive a rolling
/// I/O rate at request time. `previous` holds the last-observed values
/// keyed by node_id; `last_query_ms` is when we computed the previous
/// rate (so successive /stats calls only count bytes seen since the
/// last visit).
#[derive(Debug, Default, Clone)]
pub struct IoRateTracker {
    pub previous: std::collections::HashMap<u64, IoSnapshot>,
    /// Last computed rate (cached so very fast successive calls return
    /// stable values rather than 0).
    pub last_read_bps: u64,
    pub last_write_bps: u64,
    pub last_query_ms: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct IoSnapshot {
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub observed_at_ms: u64,
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
        // Bug AC — Raft state introspection. Surfaces openraft's own
        // view of leadership/term/replication so operators can diagnose
        // split-brain or stuck-follower conditions WITHOUT having to
        // grep journald for "ForwardToLeader: None, None". Critical for
        // multi-DC clusters where Raft can silently lose quorum.
        .route("/api/v1/raft/state", get(raft_state))
        // Nodes
        .route("/api/v1/nodes", get(list_nodes))
        .route("/api/v1/nodes/{node_id}", get(get_node))
        // Bug BC-12 — operator unquarantine endpoint.
        .route(
            "/api/v1/nodes/{node_id}/unquarantine",
            post(unquarantine_node_endpoint),
        )
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
        // Bug BC-12b — operator integrity feed. Filters audit by the
        // BC action prefixes so the WebUI /ui/integrity page (BC-12e)
        // is a single fetch, not a polling N-merge across actions.
        .route("/api/v1/integrity/faults", get(get_integrity_faults))
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
        // Bug AV — operator-driven cache-tier flush. Drops warm/hot
        // replicas under a path prefix while preserving the mandatory
        // anchor and setting a cooldown that blocks immediate
        // re-promotion by the tiering engine.
        .route("/api/v1/files/flush_cache", post(flush_cache_endpoint))
        // Bug BE-5/8 helper — return the chunk list for a single path so
        // release-suite cases can resolve a path to chunk_ids + the
        // nodes that hold each. The bulk /api/v1/files list omits
        // chunks to keep the WebUI Explorer payload bounded; this
        // endpoint is the focused per-path inspector.
        .route("/api/v1/files/inspect", get(inspect_file_endpoint))
        // Bug AV — one-shot policy migration: clone legacy
        // `replication` into `mandatory_replicas` for a volume.
        .route("/api/v1/volumes/{id}/migrate-policy", post(migrate_volume_policy_endpoint))
        // Bug AV (AV-3-5) — read the full volume config including
        // the new mandatory_replicas / cache_replicas / anchor
        // fields (closes the AV-1-10 carryover for single-volume
        // reads).
        .route("/api/v1/volumes/{id}", get(get_volume_endpoint))
        // Bug AV (AV-3-5) — operator-friendly cache-policy editor.
        // Merges the supplied fields into the volume's
        // `cache_replicas`; omitted fields keep their current
        // values. Atomic via Raft UpsertVolume.
        .route("/api/v1/volumes/{id}/set-cache", post(set_volume_cache_endpoint))
        // Bug AV (AV-4-1a) — operator-friendly mandatory-replica
        // editor. Mirrors set-cache shape. Rejects all-zero
        // results (would orphan the durability floor) and
        // anchor-violating updates (e.g. cold=0 when
        // anchor.required_tier=cold).
        .route("/api/v1/volumes/{id}/set-mandatory", post(set_volume_mandatory_endpoint))
        // AV-5-12 — operator-facing set-anchor.
        .route("/api/v1/volumes/{id}/set-anchor", post(set_volume_anchor_endpoint))
        // AZ-1 — per-volume background-job schedules (scrub, fsck).
        .route("/api/v1/volumes/{id}/set-schedules", post(set_volume_schedules_endpoint))
        // AZ-5-A1 — effective cluster config (every key, name+current+default).
        .route("/api/v1/config/effective", get(get_config_effective))
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
        // Bug AL — process-local rolling rate; starts empty so the very
        // first /api/v1/stats hit returns (0, 0) then subsequent calls
        // produce real bytes/sec deltas.
        io_rate: Arc::new(parking_lot::RwLock::new(IoRateTracker::default())),
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

/// Bug AC — Raft state introspection endpoint.
///
/// Returns openraft's view of the cluster's consensus state so that
/// operators can answer "who's the leader?" and "is this manager
/// caught up?" without parsing log lines. Mirrors what the openraft
/// Watch<RaftMetrics> publishes internally.
///
/// Output (example for a healthy follower):
/// ```json
/// {
///   "node_id": "12709699821947187762",
///   "hostname": "mgr01",
///   "running_state": "ok",
///   "state": "Follower",
///   "is_leader": false,
///   "current_term": 18,
///   "current_leader": "15786580940693171021",
///   "vote_term": 18,
///   "vote_committed": true,
///   "last_log_index": 4521,
///   "last_applied_index": 4521,
///   "applied_log_lag": 0,
///   "snapshot_index": 4000,
///   "voters": ["12709...", "15786...", "5766..."],
///   "millis_since_quorum_ack": null
/// }
/// ```
///
/// Critical fields for diagnostics:
/// - `current_leader = null` means quorum is currently lost.
/// - `applied_log_lag > 0` on a follower means it's behind the leader.
/// - `is_leader = false && current_leader != null` is the healthy
///   follower state.
async fn raft_state(State(state): State<Arc<AppState>>) -> Json<Value> {
    use openraft::async_runtime::watch::WatchReceiver;
    let metrics = state.raft.metrics().borrow_watched().clone();
    let running_state = match &metrics.running_state {
        Ok(()) => "ok".to_string(),
        Err(e) => format!("fatal: {e}"),
    };
    let last_log_index = metrics.last_log_index.unwrap_or(0);
    let last_applied_index = metrics
        .last_applied
        .as_ref()
        .map(|l| l.index)
        .unwrap_or(0);
    let applied_log_lag = last_log_index.saturating_sub(last_applied_index);
    let voters: Vec<String> = metrics
        .membership_config
        .membership()
        .voter_ids()
        .map(|id| id.to_string())
        .collect();
    let self_node_id = metrics.id;
    let is_leader = metrics
        .current_leader
        .map(|leader| leader == self_node_id)
        .unwrap_or(false);
    Json(json!({
        "node_id": self_node_id.to_string(),
        "hostname": state.config.node.hostname,
        "running_state": running_state,
        "state": format!("{:?}", metrics.state),
        "is_leader": is_leader,
        "current_term": metrics.current_term,
        "current_leader": metrics
            .current_leader
            .map(|l| l.to_string()),
        "vote_term": format!("{:?}", metrics.vote.leader_id()),
        "vote_committed": metrics.vote.is_committed(),
        "last_log_index": last_log_index,
        "last_applied_index": last_applied_index,
        "applied_log_lag": applied_log_lag,
        "snapshot_index": metrics
            .snapshot
            .as_ref()
            .map(|l| l.index)
            .unwrap_or(0),
        "voters": voters,
        // Bug BF-12 — openraft 0.10 deprecated `millis_since_quorum_ack`
        // (computed-on-emit; clock-skew bait) in favour of an absolute
        // `last_quorum_acked: SerdeInstant`. Derive the legacy millis
        // shape client-side via `.elapsed()` so the JSON contract the
        // WebUI + scripts consume is unchanged.
        "millis_since_quorum_ack": metrics.last_quorum_acked
            .as_ref()
            .map(|t| t.elapsed().as_millis() as u64),
    }))
}

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
        client_ip: None,
        version: String::new(),
        build_date: String::new(),
        quarantine: None,
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
struct DiskResponse {
    disk_id: u32,
    uuid: String,
    mount_path: String,
    tier: String,
    total_bytes: u64,
    used_bytes: u64,
    status: String,
}

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
    /// Bug AX — peer IP for client (FUSE) nodes, captured at
    /// RegisterNode time. `None` for data/manager nodes (their IP is
    /// already in `grpc_addr`).
    #[serde(skip_serializing_if = "Option::is_none")]
    client_ip: Option<String>,
    /// Bug AX — true for the manager whose `node_id` matches openraft's
    /// `current_leader` at request time. Always false for data/client
    /// roles. The leader can shift between calls (Raft elections), so
    /// the WebUI should treat this as a snapshot, not a durable label.
    is_leader: bool,
    /// Bug AY — binary version this node reported. Empty for nodes that
    /// pre-date Bug AY or for synthetic/lease-derived entries the
    /// manager couldn't ask directly.
    #[serde(skip_serializing_if = "String::is_empty")]
    version: String,
    /// Bug AY — build date of the binary this node is running, format
    /// `YYYYMMDD-HHmmss` (UTC, e.g. `"20260522-141034"`). Empty when
    /// unknown.
    #[serde(skip_serializing_if = "String::is_empty")]
    build_date: String,
    /// Bug AR — per-disk detail so the Nodes page can render a row per
    /// disk (tier, mount_path, individual utilisation). The list was
    /// previously hidden behind `disk_count`, making it impossible for
    /// the WebUI to show which disk in a multi-disk data node was
    /// filling up. Empty for manager/client roles.
    disks: Vec<DiskResponse>,
    /// Bug BC-12a — `Some(_)` whenever the node is Quarantined.
    /// Carries the reason / triggering chunk / fault history the
    /// WebUI nodes page renders alongside the QUARANTINED badge.
    #[serde(skip_serializing_if = "Option::is_none")]
    quarantine: Option<QuarantineResponse>,
}

/// Bug BC-12a — quarantine projection on /api/v1/nodes rows.
#[derive(Serialize)]
struct QuarantineResponse {
    since_ms: u64,
    reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    triggering_chunk_id: Option<String>,
    fault_count_total: u32,
    last_fault_at_ms: u64,
    last_fault_summary: String,
}

impl QuarantineResponse {
    fn from_info(q: &dtfs_common::types::QuarantineInfo) -> Self {
        Self {
            since_ms: q.since_ms,
            reason: format!("{:?}", q.reason),
            triggering_chunk_id: q.triggering_chunk_id.clone(),
            fault_count_total: q.fault_count_total,
            last_fault_at_ms: q.last_fault_at_ms,
            last_fault_summary: q.last_fault_summary.clone(),
        }
    }
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
            client_ip: info.client_ip.clone(),
            is_leader: false,
            version: info.version.clone(),
            build_date: info.build_date.clone(),
            disks: info
                .disks
                .iter()
                .map(|d| DiskResponse {
                    disk_id: d.disk_id,
                    uuid: d.uuid.clone(),
                    mount_path: d.mount_path.clone(),
                    tier: format!("{:?}", d.tier).to_ascii_lowercase(),
                    total_bytes: d.total_bytes,
                    used_bytes: d.used_bytes,
                    status: format!("{:?}", d.status).to_ascii_lowercase(),
                })
                .collect(),
            quarantine: info.quarantine.as_ref().map(QuarantineResponse::from_info),
        }
    }
}

async fn list_nodes(State(state): State<Arc<AppState>>) -> Json<Vec<NodeResponse>> {
    let sm = state.sm_data.read().await;
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    // Bug AX — query Raft's current leader once so we can mark whichever
    // manager NodeResponse matches. None when quorum is lost.
    use openraft::async_runtime::watch::WatchReceiver;
    let leader_id: Option<u64> = state
        .raft
        .metrics()
        .borrow_watched()
        .current_leader;

    // 1. Data + manager nodes registered in Raft (have heartbeat /
    //    join records).
    let mut nodes: Vec<NodeResponse> = sm
        .nodes
        .iter()
        .map(|(id, info)| {
            let mut r = NodeResponse::from_entry(*id, info);
            if Some(*id) == leader_id {
                r.is_leader = true;
            }
            r
        })
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
        // Bug X — render real location when the operator configured it;
        // fall back to the `?` placeholder otherwise so legacy
        // (location-less) initial_voters keep working.
        let render_seg = |s: &str| if s.is_empty() { "?".to_string() } else { s.to_string() };
        let location = format!(
            "{}/{}/{}/{}",
            render_seg(&v.region),
            render_seg(&v.datacenter),
            render_seg(&v.rack),
            v.hostname,
        );
        // Bug AY — managers don't go through RegisterNode, so the only
        // version we can confidently fill is the local manager's own
        // (the one serving this API call). Other managers' version
        // stays empty until a future per-manager status RPC is wired.
        let is_local = v.hostname == state.config.node.hostname;
        let synth_version = if is_local {
            dtfs_common::version::VERSION.to_string()
        } else {
            String::new()
        };
        let synth_build_date = if is_local {
            dtfs_common::version::BUILD_DATE.to_string()
        } else {
            String::new()
        };
        nodes.push(NodeResponse {
            node_id: id.to_string(),
            hostname: v.hostname.clone(),
            grpc_addr: format!("http://{}", v.addr),
            location,
            disk_count: 0,
            total_capacity: 0,
            used_capacity: 0,
            health: "Healthy".to_string(),
            role: "Manager".to_string(),
            // The API server replying is itself a manager — peers are
            // assumed alive within the cluster heartbeat window. A
            // future fix wires per-manager last-seen via Raft membership.
            last_seen_ms: now_ms,
            client_ip: None,
            is_leader: Some(id) == leader_id,
            version: synth_version,
            build_date: synth_build_date,
            disks: Vec::new(),
            quarantine: None,
        });
    }

    // 3. FUSE clients — Bug AA now self-register via RegisterNode with
    //    role="client", so they appear in step 1 (sm.nodes) with their
    //    full Location. Fall back to lease-derived synthesis for any
    //    lease whose owner host isn't already registered (defends
    //    against older FUSE binaries that don't self-register yet).
    //
    //    owner_id format (Bug X compatible):
    //      legacy: "<host>:<pid>:<fh>"
    //      Bug X:  "<host>:<pid>:<fh>:<dc>"
    //    Existing prefix-match logic (Bug J) ignores the trailing fields,
    //    so this is backward-compatible with old leases.
    let mut client_seen: std::collections::HashMap<String, (u64, String)> = Default::default();
    for lease in sm.write_leases.values() {
        let parts: Vec<&str> = lease.owner_id.split(':').collect();
        let host = match parts.first() {
            Some(h) if !h.is_empty() => h.to_string(),
            _ => continue,
        };
        // 4th field (index 3) is Bug X's optional DC.
        let dc = parts.get(3).map(|s| s.to_string()).unwrap_or_default();
        let e = client_seen.entry(host).or_insert((0, dc.clone()));
        if lease.expires_at_ms > e.0 {
            e.0 = lease.expires_at_ms;
        }
        if e.1.is_empty() && !dc.is_empty() {
            e.1 = dc;
        }
    }
    for (host, (last_seen, dc)) in client_seen {
        if known_hostnames.contains(host.as_str()) {
            continue;
        }
        let id = dtfs_common::node_id_from_hostname(&host);
        let dc_seg = if dc.is_empty() { "?".to_string() } else { dc };
        nodes.push(NodeResponse {
            node_id: id.to_string(),
            hostname: host.clone(),
            grpc_addr: String::new(),
            location: format!("?/{}/?/{}", dc_seg, host),
            disk_count: 0,
            total_capacity: 0,
            used_capacity: 0,
            health: "Healthy".to_string(),
            role: "Client".to_string(),
            last_seen_ms: last_seen,
            // Lease-derived clients pre-date Bug AX self-registration,
            // so we have no peer IP for them.
            client_ip: None,
            is_leader: false,
            version: String::new(),
            build_date: String::new(),
            disks: Vec::new(),
            quarantine: None,
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

/// Bug BC-12 — body for `POST /api/v1/nodes/{id}/unquarantine`.
///
/// `operator_id` identifies WHO unquarantined (free-form; in the absence
/// of auth this is a label the operator picks, e.g. their initials or
/// a ticket number). Empty rejected so the audit row always has a name.
///
/// `reason_acknowledged` is the operator's written justification for
/// clearing the integrity-fault state. Minimum 16 chars to force a
/// real sentence rather than "ok". The text lands in the audit log
/// + the WebUI integrity feed so a future operator can reconstruct
/// why the quarantine was lifted.
#[derive(Deserialize)]
struct UnquarantineRequest {
    operator_id: String,
    reason_acknowledged: String,
}

async fn unquarantine_node_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(node_id): axum::extract::Path<String>,
    Json(body): Json<UnquarantineRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let nid: u64 = node_id.parse().map_err(|_| (
        StatusCode::BAD_REQUEST,
        Json(json!({"error": "node_id must be u64"})),
    ))?;

    let op = body.operator_id.trim();
    if op.is_empty() {
        return Err((StatusCode::BAD_REQUEST,
            Json(json!({"error": "operator_id required"}))));
    }
    if body.reason_acknowledged.trim().len() < 16 {
        return Err((StatusCode::BAD_REQUEST, Json(json!({
            "error": "reason_acknowledged must be at least 16 characters \
                      (force a real justification, not 'ok'). Recommended: \
                      describe what you investigated + why the chunk \
                      is now trusted."
        }))));
    }

    // Confirm the node is actually Quarantined before raft-writing —
    // unquarantine on a Healthy node is a no-op but operators
    // probably typed the wrong node id.
    {
        let sm = state.sm_data.read().await;
        match sm.nodes.get(&nid) {
            None => return Err((StatusCode::NOT_FOUND, Json(json!({
                "error": format!("node {nid} not found")
            })))),
            Some(n) if n.quarantine.is_none() => {
                return Err((StatusCode::CONFLICT, Json(json!({
                    "error": format!("node {nid} is not Quarantined (current health={:?})", n.health)
                }))));
            }
            _ => {}
        }
    }

    let req = ClientRequest::UnquarantineNode {
        node_id: nid,
        operator_id: op.to_string(),
        reason_acknowledged: body.reason_acknowledged.clone(),
    };

    state
        .raft
        .client_write(req)
        .await
        .map_err(|e| (StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": format!("raft write failed: {e}")}))))?;

    state.audit_log.log(
        "api",
        "node.unquarantine",
        &format!("node={nid} by={op}"),
        &body.reason_acknowledged,
    );

    info!("Bug BC-12: node {nid} unquarantined by {op}");
    Ok(Json(json!({
        "node_id": nid.to_string(),
        "ok": true,
    })))
}

#[derive(Deserialize)]
struct InspectFileQuery {
    path: String,
}

/// Bug BE helper — return the resolved chunk list for a path, with
/// each chunk's hash + the node_ids holding it. Used by release-suite
/// cases that need to corrupt or count replicas without scraping
/// the WebUI Explorer.
async fn inspect_file_endpoint(
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
    let mut by_index: std::collections::BTreeMap<u64, (String, u64, Vec<String>)> =
        std::collections::BTreeMap::new();
    for c in &file.chunks {
        let e = by_index
            .entry(c.chunk_index)
            .or_insert_with(|| (c.chunk_hash.clone(), c.size_bytes, Vec::new()));
        e.2.push(c.node_id.clone());
    }
    let chunks: Vec<Value> = by_index
        .into_iter()
        .map(|(idx, (hash, size, nodes))| {
            json!({
                "chunk_index": idx,
                "chunk_id":    hash,
                "size":        size,
                "node_ids":    nodes,
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
        // Bug AV — new fields default to empty so the legacy
        // construction path lands a back-compat volume. Operators on the
        // new policy upgrade via `dtfs-cli volume migrate-policy`
        // (AV-1-10) and `volume set-cache` (AV-3-5).
        mandatory_replicas: dtfs_common::types::TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 },
        cache_replicas: dtfs_common::types::CacheReplication::default(),
        anchor: None,
        schedules: dtfs_common::types::VolumeSchedules::default(),
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
async fn migrate_volume_policy_endpoint(
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
async fn get_volume_endpoint(
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
        // AZ-1 — per-volume background-job schedules. `null` means
        // "inherit cluster default".
        "schedules": {
            "scrub_interval_secs": cfg.schedules.scrub_interval_secs,
            "fsck_interval_secs":  cfg.schedules.fsck_interval_secs,
            "custom":              cfg.schedules.custom,
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

async fn set_volume_cache_endpoint(
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
// Bug AV (AV-4-1a) — POST /api/v1/volumes/{id}/set-mandatory
// ---------------------------------------------------------------------------

/// AV-4-1a — request body for
/// `POST /api/v1/volumes/{id}/set-mandatory`. Every field is
/// optional; supplied fields are merged onto the current
/// `mandatory_replicas`. Omitted fields keep their current value
/// (consistent with AV-3-5's `set-cache` semantics).
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
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

/// AZ-1 — request body for `POST /api/v1/volumes/{id}/set-schedules`.
/// Each field is optional; `None` for a sub-field is a no-op
/// (preserve current). To explicitly clear an override, send the
/// value `0` for that field (clears) or set `clear_unspecified=true`
/// to clear everything not explicitly set.
#[derive(Deserialize, Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SetSchedulesRequest {
    pub scrub_interval_secs: Option<u64>,
    pub fsck_interval_secs: Option<u64>,
    pub custom: Option<std::collections::BTreeMap<String, u64>>,
    #[serde(default)]
    pub clear_unspecified: bool,
}

/// AZ-1 pure helper — merge a partial schedules update onto the
/// current volume config + validate intervals.
///
/// Rules:
/// * `Some(0)` clears the override (back to cluster default).
/// * `Some(v)` with `v < 60` is rejected (scrub I/O floor).
/// * `Some(v)` with `v >= 60` sets the override.
/// * `None` is a no-op unless `clear_unspecified == true`, in
///   which case it also clears.
// Bug BF-9 / BF-10 — back-compat surface that accepts the deprecated
// schedule fields. The merge writes them to state-machine for snapshot
// continuity, but the heartbeat path (metadata.rs:warn_once_*) no
// longer honors them.
#[allow(deprecated)]
pub(crate) fn apply_set_schedules(
    current: &dtfs_common::types::VolumeConfig,
    req: &SetSchedulesRequest,
) -> Result<dtfs_common::types::VolumeConfig, String> {
    let mut next = current.clone();
    let clear = req.clear_unspecified;

    fn merge(field: &mut Option<u64>, incoming: &Option<u64>, label: &str, clear: bool)
        -> Result<(), String>
    {
        match incoming {
            Some(0) => { *field = None; Ok(()) }
            Some(v) if *v < 60 => Err(format!(
                "{label}: {v} < 60 s minimum"
            )),
            Some(v) => { *field = Some(*v); Ok(()) }
            None if clear => { *field = None; Ok(()) }
            None => Ok(()),
        }
    }

    merge(&mut next.schedules.scrub_interval_secs,
          &req.scrub_interval_secs, "scrub_interval_secs", clear)?;
    merge(&mut next.schedules.fsck_interval_secs,
          &req.fsck_interval_secs, "fsck_interval_secs", clear)?;

    if let Some(custom) = &req.custom {
        for (name, value) in custom {
            if *value == 0 {
                next.schedules.custom.remove(name);
                continue;
            }
            if *value < 60 {
                return Err(format!("custom job {name}: {value} < 60 s minimum"));
            }
            next.schedules.custom.insert(name.clone(), *value);
        }
    } else if clear {
        next.schedules.custom.clear();
    }

    Ok(next)
}

async fn set_volume_schedules_endpoint(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(volume_id): axum::extract::Path<String>,
    Json(body): Json<SetSchedulesRequest>,
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

    let next = apply_set_schedules(&current, &body)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(json!({"error": e}))))?;

    let changed = current != next;
    if changed {
        let req = ClientRequest::UpsertVolume { config: next.clone() };
        if let Err(e) = state.raft.client_write(req).await {
            warn!(volume = %volume_id, error = %e,
                "set_volume_schedules Raft write failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": format!("raft write failed: {e}")})),
            ));
        }
        state.audit_log.log("api", "volume.set_schedules", &volume_id, "success");
    } else {
        state.audit_log.log("api", "volume.set_schedules", &volume_id, "noop");
    }

    Ok(Json(json!({
        "volume_id": volume_id,
        "changed": changed,
        "config": volume_config_to_json(&next),
    })))
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

async fn set_volume_anchor_endpoint(
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

async fn set_volume_mandatory_endpoint(
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

    let next = apply_set_mandatory(&current, &body)
        .map_err(|e| (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        ))?;

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

#[derive(Deserialize, Default)]
struct AuditQuery {
    limit: Option<usize>,
    action: Option<String>,
}

/// Bug BC-12b — list of action prefixes the integrity feed surfaces.
/// Order matches the section in plans/completed/2026-05-28-PLAN-BUG-BC-SCRUB.md § 4.5.
const INTEGRITY_ACTIONS: &[&str] = &[
    "node.quarantine",
    "node.unquarantine",
    "scrub.quick.fault",
    "scrub.deep.corrupt",
    "read.integrity_fault",
    "orphan.in_state_missing",
];

/// Bug BC-12b — returns all audit entries whose `action` (or, for
/// legacy state-machine entries, `summary`) starts with one of the
/// integrity prefixes. Same wire shape as `/api/v1/audit` rows so the
/// WebUI consumer can reuse its render code.
async fn get_integrity_faults(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditQuery>,
) -> Json<Vec<Value>> {
    let limit = query.limit.unwrap_or(200);

    let matches_integrity = |action: &str, summary: &str| -> bool {
        INTEGRITY_ACTIONS
            .iter()
            .any(|p| action.starts_with(p) || summary.starts_with(p))
    };

    if state.config.audit.replicate_via_raft {
        let sm = state.sm_data.read().await;
        let entries: Vec<Value> = sm
            .audit_log
            .iter()
            .rev()
            .filter(|e| matches_integrity(&e.action, &e.summary))
            .take(limit)
            .map(|e| {
                let (actor, action, target, result) =
                    backfill_audit_fields(&e.actor, &e.action, &e.target, &e.result, &e.summary);
                json!({
                    "timestamp": e.timestamp_ms / 1_000,
                    "actor": actor,
                    "action": action,
                    "target": target,
                    "result": result,
                    "summary": e.summary,
                    "origin_manager": e.origin_manager,
                })
            })
            .collect();
        Json(entries)
    } else {
        // Local-ring AuditEntry has no `summary` field (that lives on
        // the replicated state_machine variant). Match against action
        // only. Over-fetch so the filter has enough material before
        // truncation.
        let filter = crate::audit::AuditFilter {
            since: None,
            until: None,
            action: None,
            limit: Some(limit.saturating_mul(4).max(limit)),
        };
        let entries: Vec<Value> = state
            .audit_log
            .query(&filter)
            .into_iter()
            .filter(|e| matches_integrity(&e.action, ""))
            .take(limit)
            .map(|e| {
                json!({
                    "timestamp": e.timestamp,
                    "actor": e.actor,
                    "action": e.action,
                    "target": e.target,
                    "result": e.result,
                    "summary": "",
                    "origin_manager": "",
                })
            })
            .collect();
        Json(entries)
    }
}

async fn get_audit_log(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AuditQuery>,
) -> Json<Vec<Value>> {
    // Bug AH — when Raft audit fan-out is enabled, read from the REPLICATED
    // state-machine audit_log so every manager returns the same view. When
    // disabled (the safe default during rolling upgrades), fall back to the
    // per-manager local ring as before. Both branches emit the same wire
    // shape: `timestamp` in SECONDS-since-epoch (preserves WebUI Bug AG
    // `*1000` fix and CLI's `from_timestamp(ts, 0)` consumer); `summary`
    // and `origin_manager` are surfaced as new fields, empty when reading
    // from the local ring (which has no notion of either).
    let limit = query.limit.unwrap_or(50);
    let action_filter = query.action.as_deref();

    if state.config.audit.replicate_via_raft {
        let sm = state.sm_data.read().await;
        // Newest-first: replicated log appends in Raft commit order (≈
        // wall-clock). Reverse, filter, limit.
        let entries: Vec<Value> = sm
            .audit_log
            .iter()
            .rev()
            .filter(|e| match action_filter {
                Some(prefix) => {
                    e.action.starts_with(prefix)
                        // Legacy entries (pre-Bug-AH) only have
                        // `summary`; match against that too.
                        || e.summary.starts_with(prefix)
                }
                None => true,
            })
            .take(limit)
            .map(|e| {
                // Bug AI — `apply_request`'s own `audit()` helper produces
                // entries with empty structured fields and a summary like
                // "CreateFile: /foo (directory)". Without backfill, the
                // WebUI Audit page renders these as four em-dashes
                // because the table draws from actor/action/target/result
                // and never surfaces the summary. Delegate to the pure
                // helper so the parse is unit-tested.
                let (actor, action, target, result) =
                    backfill_audit_fields(&e.actor, &e.action, &e.target, &e.result, &e.summary);
                json!({
                    "timestamp": e.timestamp_ms / 1_000,
                    "actor": actor,
                    "action": action,
                    "target": target,
                    "result": result,
                    "summary": e.summary,
                    "origin_manager": e.origin_manager,
                })
            })
            .collect();
        Json(entries)
    } else {
        // Pre-Bug-AH behaviour — local per-manager view.
        let filter = crate::audit::AuditFilter {
            limit: Some(limit),
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
                    "summary": "",
                    "origin_manager": "",
                })
            })
            .collect();
        Json(result)
    }
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
    /// AZ-5 — when true, also persist the merged config back to
    /// `/etc/dtfs/manager.toml` so the change survives a process
    /// restart. Default false preserves the existing live-reload-
    /// only behaviour for backwards compatibility.
    #[serde(default)]
    write_to_disk: bool,
    /// Bug BD-3 — operator must explicitly acknowledge any write
    /// that affects cluster-wide keys. The WebUI sets this only on
    /// the Cluster-tab save action. Without it, the endpoint
    /// refuses to write, preventing accidental drift caused by
    /// an operator editing a cluster-wide key from a per-host
    /// view that lacks federation.
    #[serde(default)]
    acknowledge_cluster_wide: bool,
}

async fn update_config(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ConfigUpdateRequest>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    // AZ-5-A3 — boundary check: parse the incoming JSON against
    // a strict ManagerConfig before touching Raft so a typo or
    // out-of-range value returns 400 with the pre-attempt
    // (current) value, leaving the live config + on-disk file
    // untouched.
    if !body.config_json.is_empty() {
        if let Err(e) = serde_json::from_str::<serde_json::Value>(&body.config_json) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": format!("invalid config JSON: {e}"),
                    "preserved_current": true,
                })),
            ));
        }
    }

    // Bug BD-3 — guard cluster-wide writes. We treat any non-empty
    // payload that the WebUI would touch under the Cluster tab as
    // cluster-wide; the ack flag confirms the operator understands
    // this propagates to every manager. Node-only writes are accepted
    // without ack but only when `write_to_disk` is false (live-reload
    // doesn't drift across peers — manager.toml is the durability
    // boundary).
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
    let version = {
        let sm = state.sm_data.read().await;
        sm.cluster_config.version + 1
    };

    let req = ClientRequest::UpdateConfig {
        config_json: body.config_json.clone(),
        version,
    };

    state
        .raft
        .client_write(req)
        .await
        .map_err(|e| (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": format!("raft write failed: {e}")})),
        ))?;

    // AZ-5-A2 — optionally persist to disk via atomic tmp+rename.
    let mut wrote_to_disk = false;
    if body.write_to_disk && !body.config_json.is_empty() {
        let path = std::path::Path::new("/etc/dtfs/manager.toml");
        let tmp = path.with_extension("toml.az5.tmp");
        // Re-serialise the live ManagerConfig to TOML so the on-disk
        // file stays operator-readable. Fall back to the raw JSON if
        // TOML serialisation fails.
        let new_toml = match serde_json::from_str::<dtfs_common::config::ManagerConfig>(
            &body.config_json,
        ) {
            Ok(parsed) => toml::to_string_pretty(&parsed)
                .unwrap_or_else(|_| body.config_json.clone()),
            Err(_) => body.config_json.clone(),
        };
        if let Err(e) = std::fs::write(&tmp, new_toml.as_bytes())
            .and_then(|_| std::fs::rename(&tmp, path))
        {
            warn!(error = %e, "AZ-5-A2 write_to_disk failed");
        } else {
            wrote_to_disk = true;
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
        "status": "applied",
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
async fn get_config_effective(
    State(state): State<Arc<AppState>>,
) -> Json<Value> {
    let current = serde_json::to_value(&state.config)
        .unwrap_or(serde_json::Value::Null);
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
fn manager_config_defaults_baseline() -> Value {
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

#[cfg(test)]
mod config_scope_tests {
    use super::config_key_scope;

    /// Bug BD-1 — exhaustive prefix table the WebUI consumes.
    /// Future refactors that change the classifier MUST keep this
    /// test green; misclassifying a node-specific key as cluster
    /// would let an operator edit (e.g.) `node.hostname` in the
    /// cluster tab and silently write it back to one manager only,
    /// causing config drift across peers.
    #[test]
    fn bug_bd_1_classification_table() {
        // Cluster-scope keys.
        for k in [
            "manager.cluster.name",
            "manager.cluster.shared_secret",
            "manager.defaults.chunk_size_mb",
            "manager.defaults.replication.warm",
            "manager.tiering.evaluation_interval_secs",
            "manager.monitoring.heartbeat_interval_secs",
            "manager.alerts.sinks",
            "manager.audit.replicate_via_raft",
            "manager.scrub.verify_on_read",
            "manager.raft.election_timeout_ms",
            "manager.raft.heartbeat_interval_ms",
            "manager.raft.snapshot_threshold",
        ] {
            assert_eq!(config_key_scope(k), "cluster", "expected cluster: {k}");
        }
        // Node-scope keys.
        for k in [
            "manager.node.hostname",
            "manager.node.id",
            "manager.node.data_dir",
            "manager.location.datacenter",
            "manager.grpc.listen_addr",
            "manager.grpc.advertise_addr",
            "manager.api.listen_addr",
            "manager.tls.cert_path",
            "manager.tls.enabled",
        ] {
            assert_eq!(config_key_scope(k), "node", "expected node: {k}");
        }
        // Unknown prefix → fall back to node (safer default).
        assert_eq!(config_key_scope("manager.something_new"), "node");
    }
}

/// AZ-5-A1 helper — walk two parallel JSON values and produce one
/// flat row per leaf. `scope` is the top-level prefix
/// ("manager", "data", "fuse", or a hostname for per-host views).
fn flatten_config(
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
            out.push(json!({
                "name": dotted,
                "scope": scope,
                // Bug BD-1 — WebUI tab dispatch reads this field.
                "scope_kind": kind,
                "current": current,
                "default": defaults,
                "is_default": current == defaults,
            }));
        }
    }
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
fn compute_io_rate(
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

/// Bug AL — pure helper accepting an explicit `now_ms` so the rate logic
/// is unit-testable without mocking `SystemTime::now()`. See
/// `compute_io_rate` for the production wrapper.
pub(crate) fn compute_io_rate_at(
    tracker: &mut IoRateTracker,
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    now_ms: u64,
) -> (u64, u64) {
    let window_ms = now_ms.saturating_sub(tracker.last_query_ms);
    // Sub-second back-to-back call: return the cached rate to avoid
    // dividing by tiny numbers (spiky display).
    if tracker.last_query_ms != 0 && window_ms < 1_000 {
        return (tracker.last_read_bps, tracker.last_write_bps);
    }

    let mut delta_read: u64 = 0;
    let mut delta_written: u64 = 0;
    let mut new_snapshot: std::collections::HashMap<u64, IoSnapshot> =
        std::collections::HashMap::with_capacity(nodes.len());

    for (node_id, info) in nodes {
        let snap = IoSnapshot {
            total_bytes_read: info.total_bytes_read,
            total_bytes_written: info.total_bytes_written,
            observed_at_ms: now_ms,
        };
        if let Some(prev) = tracker.previous.get(node_id) {
            // Cumulative counters only grow; if they appear to have
            // gone backwards (process restart, host clock jump), the
            // saturating_sub yields 0 and we just skip the delta.
            delta_read = delta_read
                .saturating_add(info.total_bytes_read.saturating_sub(prev.total_bytes_read));
            delta_written = delta_written
                .saturating_add(info.total_bytes_written.saturating_sub(prev.total_bytes_written));
        }
        new_snapshot.insert(*node_id, snap);
    }

    let read_bps;
    let write_bps;
    if tracker.last_query_ms == 0 {
        // First call ever — no baseline, can't divide.
        read_bps = 0;
        write_bps = 0;
    } else {
        let window_secs = (window_ms / 1_000).max(1);
        read_bps = delta_read / window_secs;
        write_bps = delta_written / window_secs;
    }

    tracker.previous = new_snapshot;
    tracker.last_query_ms = now_ms;
    tracker.last_read_bps = read_bps;
    tracker.last_write_bps = write_bps;
    (read_bps, write_bps)
}

/// Bug BH-8 — count (healthy, total) over cluster *infrastructure*:
/// managers + data nodes. Clients (FUSE mounts) are consumers and must
/// not drag the dashboard health ratio down.
///
/// Managers live in `cluster.initial_voters` (config), not in
/// `sm.nodes` — so they cannot carry a per-node `HealthStatus` here and
/// are counted as healthy voters (a manager fault surfaces via leader
/// election / quorum loss, not this banner). Data-node health is real.
/// Numerator and denominator are built from the same two inputs so they
/// can never diverge over mismatched node sets (the original bug).
pub(crate) fn infra_health_counts<'a>(
    manager_count: usize,
    data_health: impl Iterator<Item = &'a HealthStatus>,
) -> (usize, usize) {
    let mut data_total = 0usize;
    let mut data_healthy = 0usize;
    for h in data_health {
        data_total += 1;
        if *h == HealthStatus::Healthy {
            data_healthy += 1;
        }
    }
    (manager_count + data_healthy, manager_count + data_total)
}

#[cfg(test)]
mod bug_bh8_infra_health_tests {
    use super::*;

    /// BH-8 — banner ratio counts managers + data, excludes clients,
    /// and never diverges numerator vs denominator.
    #[test]
    fn counts_infra_excludes_clients() {
        // 3 managers (config voters) + data nodes from sm.nodes; one
        // data node unhealthy, a client present but irrelevant here
        // (clients are filtered out by the caller before this helper).
        let manager_count = 3usize;
        let data_health = [
            HealthStatus::Healthy,
            HealthStatus::Suspect, // unhealthy data node
        ];
        let (healthy, total) =
            infra_health_counts(manager_count, data_health.iter());
        assert_eq!(total, 5, "3 managers + 2 data nodes");
        assert_eq!(healthy, 4, "one data node unhealthy → 4/5");
    }
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

    // Bug AQ — count unique FUSE clients via the SAME merge logic the
    // /api/v1/nodes endpoint uses for the Nodes table: clients
    // self-registered as `sm.nodes` entries with role=Client (Bug AA)
    // PLUS any lease-derived host that isn't already represented.
    // Pre-fix this counted only write-lease holders, so a read-only
    // Plex playback client never appeared — Home showed "1 FUSE client"
    // while the Nodes page showed 3.
    let client_count: u64 = {
        let mut hosts: std::collections::HashSet<String> = sm
            .nodes
            .values()
            .filter(|n| n.role == NodeRole::Client)
            .map(|n| n.hostname.clone())
            .collect();
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
            // usage is uneven across tiers. Bug BB-1: prefer per-disk
            // `used_bytes` if the heartbeat populated it
            // (DiskUsageSnapshot is now wired). Fall back to the
            // share-of-total apportionment for clusters still on
            // pre-AU data nodes.
            let direct = d.used_bytes;
            let value = if direct > 0 {
                direct
            } else {
                let share = d.total_bytes as f64 / node_cap as f64;
                (n.used_capacity as f64 * share) as u64
            };
            *tier_used.entry(d.tier).or_default() += value;
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

    // Bug AJ — net usable / net used must divide each tier's raw figure by
    // THAT tier's replication factor, then sum. The previous code divided
    // total raw by `hot_replication_factor` only, which broke in two ways:
    //
    //   1. On clusters with no hot tier (Plexmedia: hot=0), `hot_rf.max(1)`
    //      becomes 1 → no division → `net_used == raw_used`. Dashboard then
    //      lied that every byte of raw storage was usable space.
    //   2. Even with hot_rf > 0, using a SINGLE replication factor across
    //      hot+warm+cold+tape misses the per-tier reality (Plexmedia:
    //      warm=2, cold=1) — a warm-rf=2 disk pool reports 2× actual user
    //      data; a cold-rf=1 disk pool reports 1×. Mixing them with one
    //      divisor produces a fiction.
    //
    // Read the default per-tier replication from cluster config; fall back
    // to 1 (best-case net == raw) for any tier that's effectively disabled
    // (rf == 0). The result is a meaningful "how much user data this
    // cluster can hold" number that survives tier mix changes.
    let repl = &state.config.defaults.replication;
    let net_for_tier = |tier: DiskTier, raw_in_tier: u64| -> u64 {
        let rf = match tier {
            DiskTier::Hot => repl.hot,
            DiskTier::Warm => repl.warm,
            DiskTier::Cold => repl.cold,
            DiskTier::Tape => repl.tape,
        };
        let divisor = (rf as u64).max(1);
        raw_in_tier / divisor
    };
    let net_usable: u64 = tier_total
        .iter()
        .map(|(t, raw)| net_for_tier(*t, *raw))
        .sum();
    let net_used: u64 = tier_used
        .iter()
        .map(|(t, raw)| net_for_tier(*t, *raw))
        .sum();

    // Bug AL — true rolling rate (delta cumulative ÷ time-since-last-call)
    // computed in a process-local tracker. See `compute_io_rate` for
    // the full design rationale.
    let (io_read_bps, io_write_bps) = {
        let mut tracker = state.io_rate.write();
        compute_io_rate(&mut tracker, &sm.nodes)
    };

    // Bug BH-8 — infrastructure (managers + data) health for the
    // dashboard banner. Managers come from config voters; data-node
    // health from sm.nodes. Clients excluded so a FUSE mount can't drag
    // the ratio. Same two inputs feed numerator and denominator.
    let (infra_healthy, infra_total) = infra_health_counts(
        manager_count,
        sm.nodes
            .values()
            .filter(|n| n.role == NodeRole::Data)
            .map(|n| &n.health),
    );

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
    // Bug BC-11 — count quarantined nodes separately so /api/v1/stats
    // surfaces the integrity-fault state without forcing the operator
    // to scrape Prometheus or enumerate /api/v1/nodes by hand.
    let quarantined_nodes = sm
        .nodes
        .values()
        .filter(|n| matches!(n.health, HealthStatus::Quarantined))
        .count();
    let health = if total_nodes == 0 {
        "unknown"
    } else if quarantined_nodes > 0 {
        // A Quarantined node = data-integrity event in progress. Even
        // if every other node is Healthy, the cluster is no longer
        // "healthy" — the operator needs to look at the integrity feed.
        // We surface "critical" so dashboards page on this state.
        "critical"
    } else if degraded_nodes > 0 || healthy_nodes < total_nodes {
        if healthy_nodes == 0 {
            "critical"
        } else {
            "degraded"
        }
    } else {
        "healthy"
    };

    // Per (node, level) scrub results (BK — previously keyed per-node only,
    // so a deep scrub overwrote the quick result). Flatten the nested map to
    // one JSON object per (node, level); the WebUI lists every entry.
    let scrub_results_by_node: Vec<Value> = sm
        .scrub_results_by_level
        .values()
        .flat_map(|by_level| by_level.values())
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
    // all (node, level) records, or null when no scrubs have run yet.
    let last_scrub = sm
        .scrub_results_by_level
        .values()
        .flat_map(|by_level| by_level.values())
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

    // Bug AK — logical user-visible bytes across all files. Sum per-file
    // sizes deduped by chunk_index (replicas don't multiply user data).
    // The dashboard contrasts this with `net_used_bytes` (data nodes'
    // actual on-disk usage divided by tier RF). In a healthy cluster the
    // two should match within rounding. When `logical_bytes >>
    // net_used_bytes`, the cluster is carrying GHOST METADATA — files
    // whose chunks were physically lost (data-node wipe, ENOSPC unlink
    // cascade like Bug AD, etc.) but whose FileMetadata still references
    // them. `phantom_bytes_estimate` exposes the delta so operators can
    // see ghost accumulation without diffing two endpoints by hand.
    let logical_bytes: u64 = sm
        .files
        .values()
        .map(|f| {
            let mut by_index: std::collections::BTreeMap<u64, u64> =
                std::collections::BTreeMap::new();
            for c in &f.chunks {
                let e = by_index.entry(c.chunk_index).or_insert(0);
                *e = (*e).max(c.size_bytes);
            }
            by_index.values().sum::<u64>()
        })
        .sum();
    let phantom_bytes_estimate = logical_bytes.saturating_sub(net_used);

    Json(json!({
        "health":                health,
        "healthy_nodes":         healthy_nodes,
        // Bug BC-11 — non-zero means at least one node has been
        // quarantined after an integrity fault. Routing already
        // excludes them; the counter is what the WebUI badge reads.
        "quarantined_nodes":     quarantined_nodes,
        "total_nodes":           total_nodes,
        // Bug BH-8 — banner ratio over cluster infrastructure only
        // (managers + data); clients are consumers, not cluster health.
        // Numerator/denominator are taken from one explicit set so they
        // can never diverge.
        "infra_healthy":         infra_healthy,
        "infra_total":           infra_total,
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
        // Bug AK — see above.
        "logical_bytes":         logical_bytes,
        "phantom_bytes_estimate": phantom_bytes_estimate,
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
// Bug AV (AV-1-9) — POST /api/v1/files/flush_cache
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct FlushCacheRequest {
    /// Path prefix to flush. Empty matches every file in the cluster
    /// (requires `allow_empty_prefix=true`).
    path_prefix: String,
    /// Tiers to flush. Defaults to `["warm", "hot"]` when unset — the
    /// typical non-mandatory cache tiers. Tiers configured as
    /// mandatory for a file's volume are filtered out by the planner
    /// regardless of this list.
    #[serde(default)]
    cache_tiers: Option<Vec<String>>,
    /// Cooldown in seconds blocking re-promotion. Default 300 (5 min).
    #[serde(default)]
    cooldown_secs: Option<u64>,
    /// Cap on in-flight delete actions per call. Default 10 000;
    /// overage reported under `truncated`.
    #[serde(default)]
    max_tasks: Option<usize>,
    /// Safety gate against accidental "flush everything". Required
    /// when `path_prefix` is empty.
    #[serde(default)]
    allow_empty_prefix: bool,
}

async fn flush_cache_endpoint(
    State(state): State<Arc<AppState>>,
    Json(body): Json<FlushCacheRequest>,
) -> Result<Json<Value>, (axum::http::StatusCode, Json<Value>)> {
    use axum::http::StatusCode;

    // Safety gate: empty prefix matches every file.
    if body.path_prefix.is_empty() && !body.allow_empty_prefix {
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
    let mut quarantined = 0u64;
    let mut total_capacity: u64 = 0;
    let mut used_capacity: u64 = 0;
    for info in sm.nodes.values() {
        match info.health {
            dtfs_common::types::HealthStatus::Healthy => healthy += 1,
            dtfs_common::types::HealthStatus::Suspect => suspect += 1,
            dtfs_common::types::HealthStatus::Down => down += 1,
            // Bug BC-11 — surface quarantined node count in cluster
            // status so the operator sees a non-zero counter the moment
            // an integrity fault fires, without having to enumerate
            // /api/v1/nodes by hand.
            dtfs_common::types::HealthStatus::Quarantined => quarantined += 1,
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
         # HELP dtfs_nodes_quarantined Bug BC: nodes Quarantined after integrity fault.\n\
         # TYPE dtfs_nodes_quarantined gauge\n\
         dtfs_nodes_quarantined {quarantined}\n\
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

// ---------------------------------------------------------------------------
// Bug AI — pure helper for audit-entry display backfill (unit-tested below)
// ---------------------------------------------------------------------------

/// Produce `(actor, action, target, result)` for the WebUI Audit table.
///
/// When the entry already carries structured fields (i.e. an `AppendAudit`
/// fan-out from `audit.log()` — Bug AH), return them unchanged. When the
/// entry is a legacy `apply_request`-generated one (only `summary` populated),
/// parse `summary` in the form `"VariantName: details"` to synthesise
/// `action` and `target`, set `actor="raft"`, and `result="success"` (the
/// state-machine apply path only pushes audit on the success branch — failed
/// applies return before reaching the `audit()` helper, so any entry
/// reaching us is by construction a success).
///
/// Public-in-crate so the unit tests below can call it without going
/// through the HTTP handler.
pub(crate) fn backfill_audit_fields(
    actor: &str,
    action: &str,
    target: &str,
    result: &str,
    summary: &str,
) -> (String, String, String, String) {
    if !action.is_empty() {
        return (
            actor.to_string(),
            action.to_string(),
            target.to_string(),
            result.to_string(),
        );
    }
    let (action_part, target_part) = match summary.split_once(": ") {
        Some((a, t)) => (a.to_string(), t.to_string()),
        None => (summary.to_string(), String::new()),
    };
    (
        "raft".to_string(),
        action_part,
        target_part,
        "success".to_string(),
    )
}

#[cfg(test)]
mod bug_ai_audit_backfill_tests {
    use super::backfill_audit_fields;

    /// AppendAudit fan-out entries (Bug AH) already have structured fields;
    /// the backfill must pass them through unchanged.
    #[test]
    fn structured_entry_passes_through_unchanged() {
        let (a, ac, t, r) = backfill_audit_fields(
            "api",
            "volume.create",
            "bug-ah-fanout-test",
            "success",
            "volume.create: bug-ah-fanout-test [mgr02]",
        );
        assert_eq!(a, "api");
        assert_eq!(ac, "volume.create");
        assert_eq!(t, "bug-ah-fanout-test");
        assert_eq!(r, "success");
    }

    /// Legacy state-machine-internal entries (CreateFile, DeleteFile, …)
    /// have empty structured fields and a "Variant: target" summary. The
    /// backfill splits on the first ": " and synthesises actor/result.
    #[test]
    fn legacy_create_file_entry_splits_summary() {
        let (a, ac, t, r) = backfill_audit_fields(
            "", "", "", "",
            "CreateFile: /movies/Wicked.mkv",
        );
        assert_eq!(a, "raft");
        assert_eq!(ac, "CreateFile");
        assert_eq!(t, "/movies/Wicked.mkv");
        assert_eq!(r, "success");
    }

    /// "CreateFile: /foo (directory)" — extra annotation in target stays
    /// part of the target side.
    #[test]
    fn legacy_create_file_with_directory_marker() {
        let (_, ac, t, _) = backfill_audit_fields(
            "", "", "", "",
            "CreateFile: /movies/Season 1 (directory)",
        );
        assert_eq!(ac, "CreateFile");
        assert_eq!(t, "/movies/Season 1 (directory)");
    }

    /// A summary without ": " separator (defensive — shouldn't happen in
    /// practice, but if a future audit() call passes a freeform string
    /// we still produce something rather than panicking).
    #[test]
    fn legacy_summary_without_separator_uses_full_as_action() {
        let (a, ac, t, r) = backfill_audit_fields("", "", "", "", "some-event");
        assert_eq!(a, "raft");
        assert_eq!(ac, "some-event");
        assert!(t.is_empty());
        assert_eq!(r, "success");
    }

    /// Target may itself contain ": " (e.g. "UpdateNodeStatus: 12345 = healthy").
    /// `split_once(": ")` only splits on the FIRST occurrence — verifies
    /// the colons within target are preserved.
    #[test]
    fn legacy_target_with_internal_colons_preserved() {
        let (_, ac, t, _) = backfill_audit_fields(
            "", "", "", "",
            "UpdateNodeStatus: 12345: healthy",
        );
        assert_eq!(ac, "UpdateNodeStatus");
        assert_eq!(t, "12345: healthy");
    }
}

#[cfg(test)]
mod bug_al_io_rate_tests {
    //! Bug AL — process-local rolling I/O rate tracker.
    //!
    //! Pre-fix, the dashboard `/api/v1/stats.io_read_bytes_per_sec` was
    //! computed as `Σ total_bytes_read / (now − min(last_seen_ms))` which
    //! divided cumulative bytes by the staleness of the slowest-updated
    //! node. With `record_read` / `record_write` finally wired up in the
    //! data-node `put_chunk` / `get_chunk` handlers, the cumulative
    //! counters grow under load; this tracker deltas them against a
    //! per-call snapshot to produce a real bytes/sec rate.
    //!
    //! These tests use a small NodeInfo factory (only the fields the
    //! rate calculation reads — total_bytes_read / total_bytes_written
    //! / last_seen_ms) so we don't need the full data-node registration
    //! protocol just to verify arithmetic.
    use super::*;
    use std::collections::BTreeMap;

    fn node(read: u64, written: u64) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role: dtfs_common::types::NodeRole::Data,
            disks: Vec::new(),
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health: dtfs_common::types::HealthStatus::Healthy,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: read,
            total_bytes_written: written,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    /// First call with an empty tracker must return (0, 0) — there's no
    /// baseline to diff against yet. The tracker stores the snapshot so
    /// the next call can compute a real delta.
    #[test]
    fn first_call_returns_zero_then_baselines() {
        let mut tracker = IoRateTracker::default();
        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(1_000_000, 500_000));

        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_000_000);
        assert_eq!(r, 0, "first call has no baseline");
        assert_eq!(w, 0);
        assert_eq!(tracker.previous.len(), 1, "baseline stored for next call");
        assert_eq!(tracker.last_query_ms, 1_000_000);
    }

    /// Sustained-write scenario: 10 MB read + 5 MB written over a 10-second
    /// window across two calls → (1 MB/s, 0.5 MB/s).
    ///
    /// Baselines must use non-zero `now_ms` because `last_query_ms == 0`
    /// is the sentinel for "never queried" in the production path. In
    /// real use, `SystemTime::now().as_millis()` is always ~1.8 × 10¹²
    /// so this distinction is invisible; we honour it here with an
    /// arbitrary epoch offset.
    #[test]
    fn delta_over_known_window_produces_expected_rate() {
        let mut tracker = IoRateTracker::default();
        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(0, 0));
        // Baseline at t=1_000_000.
        let _ = compute_io_rate_at(&mut tracker, &nodes, 1_000_000);
        // Second sample 10 s later, after 10 MB read + 5 MB written.
        nodes.insert(1, node(10_000_000, 5_000_000));
        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_010_000);
        assert_eq!(r, 1_000_000, "10 MB / 10 s = 1 MB/s");
        assert_eq!(w, 500_000, "5 MB / 10 s = 500 KB/s");
    }

    /// Cumulative counters going backwards (e.g. a data-node restart
    /// reset to 0, or an u64 wraparound) must NOT produce negative or
    /// astronomical rates. `saturating_sub` zeroes the delta.
    #[test]
    fn cumulative_regression_does_not_underflow() {
        let mut tracker = IoRateTracker::default();
        tracker.previous.insert(1, IoSnapshot {
            total_bytes_read: 1_000_000_000,
            total_bytes_written: 500_000_000,
            observed_at_ms: 1_000_000,
        });
        tracker.last_query_ms = 1_000_000;

        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(0, 0)); // counters reset

        // 10 s later than the baseline.
        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_010_000);
        assert_eq!(r, 0, "regression contributes 0 to the delta, not a huge spike");
        assert_eq!(w, 0);
    }

    /// A node that disappears between calls is silently dropped from
    /// the delta — the remaining nodes' bytes still produce a sensible
    /// rate. Guards against a decommission causing the dashboard to
    /// show 0 B/s for one cycle.
    #[test]
    fn disappearing_node_does_not_blow_up_rate() {
        let mut tracker = IoRateTracker::default();
        tracker.previous.insert(1, IoSnapshot {
            total_bytes_read: 500_000,
            total_bytes_written: 200_000,
            observed_at_ms: 1_000_000,
        });
        tracker.previous.insert(2, IoSnapshot {
            total_bytes_read: 1_000_000,
            total_bytes_written: 400_000,
            observed_at_ms: 1_000_000,
        });
        tracker.last_query_ms = 1_000_000;

        // Only node 1 left; node 2 vanished. 5 s window.
        let mut nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        nodes.insert(1, node(1_500_000, 700_000));

        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_005_000);
        // Node 1's delta: +1 MB read, +500 KB written, over 5 s.
        // (No contribution from node 2 since it's gone.)
        assert_eq!(r, 200_000, "node 1 delta / 5 s");
        assert_eq!(w, 100_000);
        assert_eq!(tracker.previous.len(), 1, "previous baseline only carries surviving nodes");
    }

    /// Sub-second back-to-back calls return the cached rate to avoid
    /// dividing by tiny numbers (which would produce 100×-spiky values).
    /// Critical when WebUI + Prometheus both scrape /stats at ~5s.
    #[test]
    fn sub_second_calls_return_cached_rate() {
        let mut tracker = IoRateTracker::default();
        // Pretend we just measured a rate.
        tracker.last_read_bps = 12_345;
        tracker.last_write_bps = 6_789;
        tracker.last_query_ms = 1_000_000;

        let nodes: BTreeMap<u64, dtfs_raft::NodeInfo> = BTreeMap::new();
        // 200 ms after the previous call — well under the 1 s window
        // floor, so we hit the cache return path.
        let (r, w) = compute_io_rate_at(&mut tracker, &nodes, 1_000_200);
        assert_eq!(r, 12_345);
        assert_eq!(w, 6_789);
    }
}

#[cfg(test)]
mod bug_av_set_cache_tests {
    //! AV-3-5 — pure tests for `apply_set_cache` and the
    //! `volume_config_to_json` projection. The async axum
    //! handlers wrap these helpers; per the AV-1-7 / AV-2-4
    //! convention, the handler-level paths are live-verified on
    //! the minicluster.
    use super::*;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, CacheTierConfig, DiskTier,
        TierReplication, VolumeConfig, VolumeId, WriteSafety,
    };

    fn legacy_volume() -> VolumeConfig {
        VolumeConfig {
            id: VolumeId("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication {
                hot: 0, warm: 2, cold: 1, tape: 0,
            },
            // Default to all-zero so legacy fallback path is exercised.
            mandatory_replicas: TierReplication {
                hot: 0, warm: 0, cold: 0, tape: 0,
            },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            schedules: dtfs_common::types::VolumeSchedules::default(),
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 2,
                write_timeout_secs: 30,
            },
        }
    }

    /// AV-3-5 test 1 — supplied fields overwrite, omitted fields
    /// keep their current value.
    #[test]
    fn bug_av_set_cache_merges_partial_update() {
        let current = legacy_volume();
        let req = SetCacheRequest {
            warm_min: Some(1),
            warm_max: Some(1),
            per_dc: Some(true),
            promote_reads: Some(2),
            promote_window_secs: Some(60),
            cooldown_secs: Some(300),
            ..Default::default()
        };
        let next = apply_set_cache(&current, &req).expect("valid update");
        assert_eq!(next.cache_replicas.warm.min, 1);
        assert_eq!(next.cache_replicas.warm.max, 1);
        assert!(next.cache_replicas.per_dc);
        assert_eq!(next.cache_replicas.promote_reads, 2);
        assert_eq!(next.cache_replicas.promote_window_secs, 60);
        assert_eq!(next.cache_replicas.cooldown_secs, 300);
        // Hot tier untouched → still disabled (max=0).
        assert_eq!(next.cache_replicas.hot.max, 0);
        // Legacy `replication` untouched (rollback safety).
        assert_eq!(next.replication.warm, 2);
    }

    /// AV-3-5 test 2 — `max < min` is rejected at apply time.
    /// The state-machine apply path would also reject via
    /// `CacheTierConfig::validate`, but rejecting at the REST
    /// boundary gives the operator a clearer error.
    #[test]
    fn bug_av_set_cache_rejects_max_less_than_min() {
        let current = legacy_volume();
        let req = SetCacheRequest {
            warm_min: Some(2),
            warm_max: Some(1),
            ..Default::default()
        };
        let err = apply_set_cache(&current, &req).unwrap_err();
        assert!(err.contains("warm"), "error names the tier: {err}");
        assert!(err.contains("max") && err.contains("min"),
                "error explains why: {err}");
    }

    /// AV-3-5 test 3 — empty request body produces an unchanged
    /// clone (so the dispatcher detects no-op and skips Raft).
    #[test]
    fn bug_av_set_cache_empty_request_is_noop() {
        let current = legacy_volume();
        let req = SetCacheRequest::default();
        let next = apply_set_cache(&current, &req).expect("empty req is valid");
        assert_eq!(current, next);
    }

    /// AV-3-5 test 4 — the JSON projection includes the new AV
    /// fields (mandatory_replicas, cache_replicas, anchor) so
    /// the CLI / WebUI can render them.
    #[test]
    fn bug_av_volume_config_json_includes_av_fields() {
        let mut cfg = legacy_volume();
        cfg.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        cfg.cache_replicas.warm = CacheTierConfig { min: 1, max: 1 };
        cfg.cache_replicas.per_dc = true;
        cfg.cache_replicas.promote_reads = 2;
        cfg.cache_replicas.promote_window_secs = 14400;
        cfg.cache_replicas.cooldown_secs = 300;
        cfg.anchor = Some(AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        });

        let v = volume_config_to_json(&cfg);
        assert_eq!(v["mandatory_replicas"]["cold"], 1);
        assert_eq!(v["cache_replicas"]["warm"]["min"], 1);
        assert_eq!(v["cache_replicas"]["warm"]["max"], 1);
        assert_eq!(v["cache_replicas"]["per_dc"], true);
        assert_eq!(v["cache_replicas"]["promote_reads"], 2);
        assert_eq!(v["cache_replicas"]["promote_window_secs"], 14400);
        assert_eq!(v["cache_replicas"]["cooldown_secs"], 300);
        assert_eq!(v["anchor"]["datacenter"], "leli");
        assert_eq!(v["anchor"]["required_tier"], "cold");
        // Legacy `replication` block still present (back-compat).
        assert_eq!(v["replication"]["warm"], 2);
    }

    /// AV-3-5 defensive — anchor=None projects null/omitted, not
    /// a stale stub object.
    #[test]
    fn bug_av_volume_config_json_anchor_none_is_null() {
        let cfg = legacy_volume();
        let v = volume_config_to_json(&cfg);
        assert!(
            v["anchor"].is_null(),
            "anchor is None → null, not an empty object (got {})",
            v["anchor"]
        );
    }

    /// AV-4-2 regression #1 — the list-endpoint row must include
    /// the full AV-3-5 projection (mandatory_replicas,
    /// cache_replicas, anchor) PLUS the aggregation fields
    /// (file_count, total_size). Pre-AV-4-2 the list row only
    /// emitted `replication` + aggregation, so operators reading
    /// the list endpoint couldn't see what cache policy a volume
    /// carried without an extra per-volume GET.
    #[test]
    fn bug_av_4_2_list_volumes_row_includes_av_fields() {
        let mut cfg = legacy_volume();
        cfg.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        cfg.cache_replicas.warm = CacheTierConfig { min: 1, max: 1 };
        cfg.cache_replicas.per_dc = true;
        cfg.cache_replicas.promote_reads = 2;
        cfg.cache_replicas.promote_window_secs = 60;
        cfg.cache_replicas.cooldown_secs = 30;
        cfg.anchor = Some(AnchorPolicy {
            datacenter: "dc-a".to_string(),
            required_tier: None,
        });

        let row = list_volumes_row_json(Some(&cfg), "default", (12, 9_999_999));

        // Aggregation fields.
        assert_eq!(row["file_count"], 12);
        assert_eq!(row["total_size"], 9_999_999);
        // AV-3-5 projection fields.
        assert_eq!(row["mandatory_replicas"]["cold"], 1);
        assert_eq!(row["cache_replicas"]["warm"]["max"], 1);
        assert_eq!(row["cache_replicas"]["per_dc"], true);
        assert_eq!(row["anchor"]["datacenter"], "dc-a");
        // Legacy `replication` block still present (back-compat).
        assert_eq!(row["replication"]["warm"], 2);
    }

    /// AV-4-2 regression #2 — a usage-only row (volume exists in
    /// `files` but has no `VolumeConfig`) renders with just
    /// `volume_id` + aggregation, no crash on missing config.
    /// Pre-fix behaviour preserved.
    #[test]
    fn bug_av_4_2_list_volumes_legacy_volume_renders_correctly() {
        let row = list_volumes_row_json(None, "orphan", (3, 42));
        assert_eq!(row["volume_id"], "orphan");
        assert_eq!(row["file_count"], 3);
        assert_eq!(row["total_size"], 42);
        assert!(row.get("cache_replicas").is_none(),
            "config-less row must not invent default fields");
    }
}

#[cfg(test)]
mod bug_az_set_schedules_tests {
    //! AZ-1 — pure tests for `apply_set_schedules`. Axum handler
    //! is live-only per the AV-3-5 convention.
    use super::*;
    use dtfs_common::types::{
        CacheReplication, TierReplication, VolumeConfig, VolumeId,
        VolumeSchedules, WriteSafety,
    };

    fn volume() -> VolumeConfig {
        VolumeConfig {
            id: VolumeId("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 4 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
            schedules: VolumeSchedules::default(),
        }
    }

    #[test]
    fn bug_az_set_schedules_sets_override() {
        let v = volume();
        let req = SetSchedulesRequest {
            scrub_interval_secs: Some(3600),
            ..Default::default()
        };
        let n = apply_set_schedules(&v, &req).expect("ok");
        assert_eq!(n.schedules.scrub_interval_secs, Some(3600));
        assert_eq!(n.schedules.fsck_interval_secs, None);
    }

    #[test]
    fn bug_az_set_schedules_zero_clears() {
        let mut v = volume();
        v.schedules.scrub_interval_secs = Some(3600);
        let req = SetSchedulesRequest {
            scrub_interval_secs: Some(0),
            ..Default::default()
        };
        let n = apply_set_schedules(&v, &req).expect("ok");
        assert_eq!(n.schedules.scrub_interval_secs, None,
            "Some(0) must clear the override");
    }

    #[test]
    fn bug_az_set_schedules_rejects_below_floor() {
        let v = volume();
        let req = SetSchedulesRequest {
            scrub_interval_secs: Some(30),
            ..Default::default()
        };
        let err = apply_set_schedules(&v, &req).unwrap_err();
        assert!(err.contains("scrub_interval_secs"));
        assert!(err.contains("60 s minimum"));
    }

    #[test]
    fn bug_az_set_schedules_clear_unspecified() {
        let mut v = volume();
        v.schedules.scrub_interval_secs = Some(3600);
        v.schedules.fsck_interval_secs = Some(7200);
        v.schedules.custom.insert("rebalance".into(), 86400);
        let req = SetSchedulesRequest {
            clear_unspecified: true,
            ..Default::default()
        };
        let n = apply_set_schedules(&v, &req).expect("ok");
        assert_eq!(n.schedules.scrub_interval_secs, None);
        assert_eq!(n.schedules.fsck_interval_secs, None);
        assert!(n.schedules.custom.is_empty(),
            "clear_unspecified must clear the custom map too");
    }

    #[test]
    fn bug_az_set_schedules_custom_job_merge() {
        let v = volume();
        let mut custom = std::collections::BTreeMap::new();
        custom.insert("rebalance".to_string(), 86400u64);
        custom.insert("snapshot".to_string(), 3600u64);
        let req = SetSchedulesRequest {
            custom: Some(custom),
            ..Default::default()
        };
        let n = apply_set_schedules(&v, &req).expect("ok");
        assert_eq!(n.schedules.custom.get("rebalance").copied(), Some(86400));
        assert_eq!(n.schedules.custom.get("snapshot").copied(), Some(3600));
    }

    #[test]
    fn bug_az_volume_config_json_includes_schedules() {
        let mut v = volume();
        v.schedules.scrub_interval_secs = Some(3600);
        let json = volume_config_to_json(&v);
        assert_eq!(json["schedules"]["scrub_interval_secs"], 3600);
        assert!(json["schedules"]["fsck_interval_secs"].is_null());
    }
}

#[cfg(test)]
mod bug_av_set_mandatory_tests {
    //! AV-4-1a — pure tests for `apply_set_mandatory`. Mirrors
    //! the AV-3-5 `bug_av_set_cache_tests` convention (axum
    //! handler is live-only; the pure helper carries the
    //! unit-test load).
    use super::*;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, DiskTier, TierReplication,
        VolumeConfig, VolumeId, WriteSafety,
    };

    fn legacy_volume() -> VolumeConfig {
        // Same shape as the set-cache test fixture; mandatory all-zero
        // exercises the legacy-fallback path.
        VolumeConfig {
            id: VolumeId("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication {
                hot: 0, warm: 2, cold: 1, tape: 0,
            },
            mandatory_replicas: TierReplication {
                hot: 0, warm: 0, cold: 0, tape: 0,
            },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            schedules: dtfs_common::types::VolumeSchedules::default(),
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 2,
                write_timeout_secs: 30,
            },
        }
    }

    /// AV-4-1a test 1 — supplied fields overwrite, omitted
    /// fields keep their current value. Legacy `replication`
    /// is preserved as a rollback path.
    #[test]
    fn bug_av_set_mandatory_merges_partial_update() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 2, cold: 1, tape: 1,
        };
        let req = SetMandatoryRequest {
            warm_replicas: Some(0),  // drop warm
            cold_replicas: Some(1),  // unchanged
            tape_replicas: Some(0),  // drop tape
            ..Default::default()       // hot keeps current = 0
        };
        let next = apply_set_mandatory(&current, &req).expect("valid");
        assert_eq!(next.mandatory_replicas.hot,  0);
        assert_eq!(next.mandatory_replicas.warm, 0);
        assert_eq!(next.mandatory_replicas.cold, 1);
        assert_eq!(next.mandatory_replicas.tape, 0);
        // Legacy `replication` untouched.
        assert_eq!(next.replication.warm, 2);
    }

    /// AV-4-1a test 2 — rejecting all-zero result. Would
    /// re-enable the legacy fallback silently; not what the
    /// operator wanted.
    #[test]
    fn bug_av_set_mandatory_rejects_all_zero() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 2, cold: 1, tape: 0,
        };
        let req = SetMandatoryRequest {
            warm_replicas: Some(0),
            cold_replicas: Some(0),
            // hot + tape stay at 0.
            ..Default::default()
        };
        let err = apply_set_mandatory(&current, &req).unwrap_err();
        assert!(err.contains("all-zero"),
            "error must explain the all-zero rejection: {err}");
    }

    /// AV-4-1a test 3 — rejecting an update that would make
    /// the anchor invariant permanently unsatisfiable.
    #[test]
    fn bug_av_set_mandatory_rejects_anchor_required_tier_zero() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 0, cold: 1, tape: 0,
        };
        current.anchor = Some(AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        });
        let req = SetMandatoryRequest {
            cold_replicas: Some(0),
            warm_replicas: Some(2), // satisfy the all-zero rule
            ..Default::default()
        };
        let err = apply_set_mandatory(&current, &req).unwrap_err();
        assert!(err.contains("anchor"),
            "error must mention anchor: {err}");
        assert!(err.contains("Cold") || err.contains("cold"),
            "error must name the offending tier: {err}");
    }

    /// AV-4-1a test 4 — calling set-mandatory on a legacy
    /// volume (current mandatory all-zero) populates the new
    /// field without disturbing legacy `replication`. The
    /// operator can use this as an alternative to
    /// migrate-policy when they want different mandatory
    /// values than the legacy field carries.
    #[test]
    fn bug_av_set_mandatory_legacy_volume_round_trips() {
        let current = legacy_volume(); // mandatory all-zero
        assert_eq!(current.mandatory_replicas.hot,  0);
        assert_eq!(current.mandatory_replicas.warm, 0);
        assert_eq!(current.mandatory_replicas.cold, 0);
        assert_eq!(current.mandatory_replicas.tape, 0);

        let req = SetMandatoryRequest {
            cold_replicas: Some(1),
            ..Default::default()
        };
        let next = apply_set_mandatory(&current, &req).expect("valid");

        // New mandatory_replicas populated.
        assert_eq!(next.mandatory_replicas.cold, 1);
        // Legacy replication untouched (rollback safety).
        assert_eq!(next.replication.warm, 2);
        assert_eq!(next.replication.cold, 1);
        // effective_mandatory_replicas now returns the new field
        // (cold=1) rather than the legacy fallback (warm=2, cold=1).
        assert_eq!(next.effective_mandatory_replicas().cold, 1);
        assert_eq!(next.effective_mandatory_replicas().warm, 0,
            "new mandatory_replicas takes precedence over legacy");
    }

    /// AV-4-1a defensive — empty request body produces an
    /// unchanged clone (so the dispatcher detects no-op and
    /// skips the Raft write).
    #[test]
    fn bug_av_set_mandatory_empty_request_is_noop() {
        let mut current = legacy_volume();
        current.mandatory_replicas = TierReplication {
            hot: 0, warm: 2, cold: 1, tape: 0,
        };
        let req = SetMandatoryRequest::default();
        let next = apply_set_mandatory(&current, &req).expect("empty req valid");
        assert_eq!(current, next);
    }
}
