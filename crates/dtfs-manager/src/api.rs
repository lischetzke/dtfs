//! Manager REST API (Axum).
//!
//! Provides cluster management endpoints consumed by the WebUI, CLI,
//! and external integrations.
//!
//! LIS-18 / Sec R1: mutating `/api/v1/*` routes require
//! `[cluster].shared_secret` via `X-Dtfs-Shared-Secret` (or
//! `Authorization: Bearer`) when the secret is configured. Read-only
//! GETs stay open for trusted-network dashboards. See `rest_auth`.

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
    middleware,
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

mod audit;
mod chunks;
mod cluster;
mod config;
mod files;
mod nodes;
mod raft;
mod volumes;
pub(crate) use audit::*;
pub(crate) use chunks::*;
pub(crate) use cluster::*;
pub(crate) use config::*;
pub(crate) use files::*;
pub(crate) use nodes::*;
pub(crate) use raft::*;
pub(crate) use volumes::*;

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
    /// Manager-local memory budget surfaced by `GET /api/v1/memory`.
    pub memory: Option<Arc<dtfs_common::memory::MemoryBudget>>,
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
        // CF-2 — online raft membership change. Leader-only, gated on
        // the cluster shared secret (these alter the consensus group
        // itself). `initial_voters` in the config is bootstrap-only;
        // growing/shrinking a live cluster goes through these.
        .route("/api/v1/raft/add-learner", post(raft_add_learner))
        .route("/api/v1/raft/promote", post(raft_promote))
        .route("/api/v1/raft/remove", post(raft_remove))
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
        // Operator decommission: remove a drained/dead node from the registry.
        .route(
            "/api/v1/nodes/{node_id}/decommission",
            post(decommission_node_endpoint),
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
        // CF-3a — operator-friendly write-safety editor. Merges the
        // supplied min_replicas_before_ack / write_timeout_secs into
        // the volume's `write_safety`; omitted fields keep their
        // current value. Atomic via Raft UpsertVolume. (Enforcement
        // of the ack threshold is CF-3b.)
        .route("/api/v1/volumes/{id}/set-write-safety", post(set_volume_write_safety_endpoint))
        // Bug AV (AV-4-1a) — operator-friendly mandatory-replica
        // editor. Mirrors set-cache shape. Rejects all-zero
        // results (would orphan the durability floor) and
        // anchor-violating updates (e.g. cold=0 when
        // anchor.required_tier=cold).
        .route("/api/v1/volumes/{id}/set-mandatory", post(set_volume_mandatory_endpoint))
        // AV-5-12 — operator-facing set-anchor.
        .route("/api/v1/volumes/{id}/set-anchor", post(set_volume_anchor_endpoint))
        // Bug BZ — per-volume capacity-spill exclusions.
        .route("/api/v1/volumes/{id}/set-spill", post(set_volume_spill_endpoint))
        // AZ-5-A1 — effective cluster config (every key, name+current+default).
        .route("/api/v1/config/effective", get(get_config_effective))
        // Stats (aggregate cluster metrics for WebUI home page)
        .route("/api/v1/stats", get(get_stats))
        // Read-only diagnostics: chunk placement listing/lookup + the
        // manager-local memory ladder state.
        .route("/api/v1/chunks", get(list_chunks))
        .route("/api/v1/chunks/{chunk_id}", get(get_chunk))
        .route("/api/v1/memory", get(get_memory))
        // HMAC join (phase 1: challenge, phase 2: Raft register)
        .route("/api/v1/join/request", post(join_request_api))
        .route("/api/v1/join/complete", post(join_complete_api))
        // LIS-18 — shared-secret gate on mutating REST (fail closed when
        // `[cluster].shared_secret` is set). Join HMAC + GET probes exempt.
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            crate::rest_auth::require_mutation_auth,
        ))
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
    memory: Option<Arc<dtfs_common::memory::MemoryBudget>>,
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
        memory,
    });

    let app = build_router(state);

    // Bug CK — clear, typed bind error (e.g. Tailscale interface not up at
    // boot → EADDRNOTAVAIL) so the failure propagates to a non-zero exit.
    let listener = tokio::net::TcpListener::bind(&config.api.listen_addr)
        .await
        .map_err(|e| crate::bind_error("REST API", &config.api.listen_addr, e))?;
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

#[cfg(test)]
mod tests;
