#![allow(dead_code)]
#![allow(unused_imports)]
// Mitigate rustc 1.94.x ICE in dead_code / join module analysis (see ISSUES_TO_FIX.md).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod api;
#[allow(dead_code)]
mod audit;
#[allow(dead_code)]
mod cluster;
mod join;
mod metadata;
mod monitor;
mod placement;
mod scheduler;
mod server;
mod tiering;
mod webui;

use audit::AuditLog;
use cluster::ClusterManager;
use monitor::HealthMonitor;
use placement::PlacementEngine;
use scheduler::TaskScheduler;
use tiering::TieringEngine;

#[derive(Parser, Debug)]
#[command(name = "dtfs-manager", about = "DTFS Manager Node")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/etc/dtfs/manager.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("dtfs=info".parse()?))
        .init();

    let args = Args::parse();
    tracing::info!("Starting DTFS Manager with config: {}", args.config);

    let config: dtfs_common::config::ManagerConfig =
        dtfs_common::config::load_config(std::path::Path::new(&args.config))?;

    tracing::info!(
        "Manager {} at {} ({})",
        config.node.hostname,
        config.grpc.listen_addr,
        config.location
    );

    // --- Raft initialisation ---

    // 1. Open the RocksDB log store.
    let log_store_path = config.node.data_dir.join("raft-log");
    let log_store = dtfs_raft::log_store::LogStore::new(&log_store_path)?;
    tracing::info!("Raft log store opened at {:?}", log_store_path);

    // 2. Create the in-memory state machine.
    let state_machine = dtfs_raft::state_machine::StateMachineStore::new();
    let sm_data = state_machine.data().clone();

    // 3. Build the OpenRaft config.
    let raft_config = openraft::Config {
        cluster_name: config.cluster.name.clone(),
        election_timeout_min: config.raft.election_timeout_ms,
        election_timeout_max: config.raft.election_timeout_ms * 2,
        heartbeat_interval: config.raft.heartbeat_interval_ms,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(
            config.raft.snapshot_threshold,
        ),
        ..Default::default()
    };
    let raft_config = Arc::new(raft_config.validate()?);

    // 4. Create the Raft node.
    let network_factory = dtfs_raft::network::NetworkFactory;
    let raft = openraft::Raft::new(
        // Use a deterministic node id derived from the hostname hash.
        dtfs_common::node_id_from_hostname(&config.node.hostname),
        raft_config,
        network_factory,
        log_store,
        state_machine,
    )
    .await?;
    let raft = Arc::new(raft);

    // 5. Bootstrap if this is the initial cluster node.
    if config.cluster.bootstrap {
        let node_id = dtfs_common::node_id_from_hostname(&config.node.hostname);
        let node = dtfs_raft::DtfsNode {
            addr: config.grpc.listen_addr.clone(),
            hostname: config.node.hostname.clone(),
        };

        let mut members = BTreeMap::new();
        members.insert(node_id, node);

        match raft.initialize(members).await {
            Ok(_) => tracing::info!("Raft cluster bootstrapped with node {}", node_id),
            Err(e) => {
                // Already initialised is fine (idempotent restart).
                tracing::warn!("Raft init: {} (may already be initialised)", e);
            }
        }
    }

    // --- Shared services ---

    let audit_log = Arc::new(AuditLog::default());
    // ClusterManager no longer holds a Raft reference; heartbeat tracking is
    // its only responsibility. Raft writes are performed by HealthMonitor.
    let cluster_mgr = Arc::new(ClusterManager::new());
    let placement_engine = Arc::new(PlacementEngine::new());
    let join_handler = Arc::new(join::JoinHandler::new(
        config.cluster.shared_secret.clone(),
        config.cluster.name.clone(),
        raft.clone(),
        audit_log.clone(),
    ));

    // --- Task scheduler background task ---

    let task_scheduler = Arc::new(TaskScheduler::new(
        raft.clone(),
        sm_data.clone(),
        audit_log.clone(),
    ));

    let scheduler_ref = task_scheduler.clone();
    let scheduler_handle = tokio::spawn(async move {
        scheduler_ref.run().await;
    });

    // --- Tiering engine background task ---

    let tiering_engine = Arc::new(TieringEngine::new(
        config.tiering.clone(),
        task_scheduler.clone(),
        audit_log.clone(),
        sm_data.clone(),
    ));

    let tiering_ref = tiering_engine.clone();
    let tiering_handle = tokio::spawn(async move {
        tiering_ref.run().await;
    });

    // --- Health monitor background task ---

    let health_monitor = HealthMonitor::new(
        cluster_mgr.clone(),
        Duration::from_secs(10),
        task_scheduler.clone(),
        sm_data.clone(),
        raft.clone(),
    );
    let monitor_handle = tokio::spawn(async move {
        health_monitor.run().await;
    });

    // --- Metadata service (Raft-backed) ---

    let metadata_service = metadata::MetadataService::new(
        config.clone(),
        raft.clone(),
        sm_data.clone(),
        placement_engine.clone(),
        cluster_mgr.clone(), // Fix #2: wire heartbeat tracking into the gRPC handler
    );

    // --- Start servers ---

    let grpc_handle =
        tokio::spawn(server::run_grpc_server(config.clone(), metadata_service, raft.clone()));
    let api_handle = tokio::spawn(api::run_api_server(
        config.clone(),
        raft.clone(),
        sm_data.clone(),
        audit_log.clone(),
        join_handler.clone(),
    ));

    audit_log.log(
        "system",
        "manager.start",
        &config.node.hostname,
        "success",
    );

    tokio::select! {
        result = grpc_handle => {
            tracing::error!("gRPC server exited: {:?}", result);
        }
        result = api_handle => {
            tracing::error!("API server exited: {:?}", result);
        }
        _ = monitor_handle => {
            tracing::error!("Health monitor exited unexpectedly");
        }
        _ = scheduler_handle => {
            tracing::error!("Task scheduler exited unexpectedly");
        }
        _ = tiering_handle => {
            tracing::error!("Tiering engine exited unexpectedly");
        }
    }

    Ok(())
}
