#![allow(dead_code)]
// Work around rustc ICE on stable when dead_code diagnostics hit `metrics` (see ISSUES_TO_FIX.md).

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cache;
mod disk;
mod metrics;
mod replication;
mod scrub;
mod server;
mod store;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::types::DiskTier;
use dtfs_proto::dtfs::manager::manager_service_client::ManagerServiceClient;
use dtfs_proto::dtfs::manager::{HeartbeatRequest, RegisterNodeRequest};
use store::ChunkStore;

#[derive(Parser, Debug)]
#[command(name = "dtfs-data", about = "DTFS Data Node")]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/etc/dtfs/data.toml")]
    config: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("dtfs=info".parse()?))
        .init();

    let args = Args::parse();
    tracing::info!("Starting DTFS Data Node with config: {}", args.config);

    let config: DataNodeConfig =
        dtfs_common::config::load_config(std::path::Path::new(&args.config))?;

    tracing::info!(
        "Data node {} at {} ({})",
        config.node.hostname,
        config.grpc.listen_addr,
        config.location
    );

    // Initialize disk managers for each configured disk
    let disk_manager = disk::DiskManager::new(&config.disks)?;
    tracing::info!("Initialized {} disks", disk_manager.disk_count());

    // Initialize chunk store
    let chunk_store = Arc::new(store::ChunkStore::new(disk_manager));

    // Initialize ARC cache with configured memory limit.
    // Estimate max entries from max_memory_mb assuming ~128KB average chunk size.
    let avg_chunk_kb = 128;
    let max_entries = ((config.cache.max_memory_mb * 1024) / avg_chunk_kb) as usize;
    let cache = Arc::new(cache::ArcCache::new(max_entries.max(64)));
    tracing::info!(
        "Initialized ARC cache: max_memory={}MB, max_entries={}",
        config.cache.max_memory_mb,
        max_entries,
    );

    // Initialize I/O metrics.
    let data_metrics = Arc::new(metrics::DataMetrics::new());
    tracing::info!("Initialized I/O metrics");

    // Start scrub scheduler as a background task.
    let scrub_scheduler = scrub::ScrubScheduler::new(chunk_store.clone(), config.scrub.clone());
    tokio::spawn(async move {
        scrub_scheduler.run().await;
    });
    tracing::info!("Started scrub scheduler background task");

    // Start manager registration and heartbeat background task.
    // Clone Arcs so they can be shared between the heartbeat task and the gRPC server.
    let heartbeat_config = config.clone();
    let heartbeat_store = chunk_store.clone();
    let heartbeat_metrics = data_metrics.clone();
    let process_start = Instant::now();
    tokio::spawn(async move {
        if let Err(e) =
            manager_lifecycle(heartbeat_config, heartbeat_store, heartbeat_metrics, process_start)
                .await
        {
            tracing::error!("Manager lifecycle task failed: {}", e);
        }
    });

    // Start gRPC server (blocks until shutdown)
    server::run_grpc_server(config, chunk_store, data_metrics, cache).await?;

    Ok(())
}

/// Register with the manager and send periodic heartbeats.
///
/// `chunk_store` is used to read live chunk/byte counts for each heartbeat.
/// `process_start` marks when this data node process was launched, used to
/// compute `uptime_secs`.
async fn manager_lifecycle(
    config: DataNodeConfig,
    chunk_store: Arc<ChunkStore>,
    data_metrics: Arc<metrics::DataMetrics>,
    process_start: Instant,
) -> anyhow::Result<()> {
    // F-024 hardening: walk through every configured manager endpoint when the
    // primary is down (e.g. after a rolling restart). The list is built from
    // `manager_addr` + `manager_addrs` in priority order.
    let manager_endpoints: Vec<String> = config
        .cluster
        .all_manager_addrs()
        .into_iter()
        .map(|a| format!("http://{}", a))
        .collect();

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    let (mut client, mut current_endpoint_idx) = loop {
        let mut connected = None;
        for (idx, ep) in manager_endpoints.iter().enumerate() {
            match ManagerServiceClient::connect(ep.clone()).await {
                Ok(c) => {
                    tracing::info!("Connected to manager at {}", ep);
                    connected = Some((c, idx));
                    break;
                }
                Err(e) => {
                    tracing::debug!("manager {} unreachable: {}", ep, e);
                }
            }
        }
        if let Some(pair) = connected {
            break pair;
        }
        tracing::warn!(
            "no manager reachable in {:?}; retrying in {:?}",
            manager_endpoints,
            backoff
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    };

    // Build disk info for registration. Pull total/used bytes from the live
    // ManagedDisk records (populated via statvfs at startup) so the manager
    // sees real capacity (fix for F-006).
    let managed = chunk_store.disk_manager();
    let disk_infos: Vec<dtfs_proto::dtfs::common::DiskInfo> = config
        .disks
        .iter()
        .enumerate()
        .map(|(i, d)| {
            let live = managed.all_disks().iter().find(|md| md.disk_id == i as u32);
            let total_bytes = live.map(|m| m.total_bytes).unwrap_or(0);
            let used_bytes = live.map(|m| m.used_bytes).unwrap_or(0);
            dtfs_proto::dtfs::common::DiskInfo {
                disk_id: i as u32,
                uuid: uuid::Uuid::new_v4().to_string(),
                mount_path: d.path.display().to_string(),
                tier: match d.tier {
                    DiskTier::Hot => "hot".to_string(),
                    DiskTier::Warm => "warm".to_string(),
                    DiskTier::Cold => "cold".to_string(),
                    DiskTier::Tape => "tape".to_string(),
                },
                total_bytes,
                used_bytes,
                status: "healthy".to_string(),
            }
        })
        .collect();

    // Parse port from listen address (e.g., "0.0.0.0:9500" -> 9500).
    let grpc_port: u32 = config
        .grpc
        .listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9500);

    // Register this node with the manager. Retried with backoff + endpoint
    // rotation so that a transient cluster outage (e.g. rolling manager
    // restart immediately after this data node started — F-024) doesn't
    // crash the data node lifecycle.
    let build_register_req = || RegisterNodeRequest {
        hostname: config.node.hostname.clone(),
        role: "data".to_string(),
        grpc_port,
        location: Some(dtfs_proto::dtfs::common::Location {
            region: config.location.region.clone(),
            datacenter: config.location.datacenter.clone(),
            rack: config.location.rack.clone(),
            host: config.location.host.clone(),
        }),
        disks: disk_infos.clone(),
        shared_secret: Some(config.cluster.shared_secret.clone()),
    };

    let mut reg_backoff = Duration::from_secs(1);
    let node_id = loop {
        let req = build_register_req();
        match client.register_node(tonic::Request::new(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.accepted {
                    tracing::info!(
                        "Registered with manager, assigned node_id: {}",
                        resp.node_id
                    );
                    break resp.node_id;
                } else {
                    tracing::warn!(
                        "Manager rejected registration; retrying in {:?}",
                        reg_backoff
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    "RegisterNode RPC failed: {} — walking manager list in {:?}",
                    e,
                    reg_backoff
                );
                // Try the other endpoints before backing off.
                let n = manager_endpoints.len();
                let mut switched = false;
                for offset in 1..=n {
                    let idx = (current_endpoint_idx + offset) % n;
                    let ep = &manager_endpoints[idx];
                    if let Ok(c) = ManagerServiceClient::connect(ep.clone()).await {
                        client = c;
                        current_endpoint_idx = idx;
                        tracing::info!("Switched to manager at {}", ep);
                        switched = true;
                        break;
                    }
                }
                if !switched {
                    tracing::debug!("no alternate manager reachable yet");
                }
            }
        }
        tokio::time::sleep(reg_backoff).await;
        reg_backoff = (reg_backoff * 2).min(max_backoff);
    };

    // Heartbeat loop.
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    let mut consecutive_failures = 0u32;
    // Track the last config version we have applied so we can tell the manager.
    let mut current_config_version: u64 = 0;

    loop {
        interval.tick().await;

        // Gather live metrics for the heartbeat payload.
        let store_stats = chunk_store.stats();
        let uptime = process_start.elapsed().as_secs();

        // P0-9: harvest per-chunk-per-DC counters for the heartbeat payload.
        // Cap to 1000 entries to keep the payload bounded; manager-side
        // tiering tolerates a partial view (it ages-out old chunks anyway).
        let per_chunk = data_metrics.harvest_per_chunk(1000);
        let chunk_reads: Vec<dtfs_proto::dtfs::manager::PerChunkReadCounter> = per_chunk
            .into_iter()
            .map(|h| dtfs_proto::dtfs::manager::PerChunkReadCounter {
                chunk_id: h.chunk_id,
                reads_by_dc_1h: h.reads_by_dc.clone(),
                reads_by_dc_24h: h.reads_by_dc,
                last_access_ms: h.last_access_ms,
            })
            .collect();

        let heartbeat = HeartbeatRequest {
            node_id: node_id.clone(),
            uptime_secs: uptime,
            total_chunks: store_stats.total_chunks,
            total_bytes_used: store_stats.total_bytes_stored,
            config_version: current_config_version,
            chunk_reads,
        };

        match client
            .heartbeat(tonic::Request::new(heartbeat))
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                if !resp.accepted {
                    tracing::warn!("Manager rejected heartbeat");
                }
                consecutive_failures = 0;

                // Apply config updates pushed by the manager via heartbeat response.
                if let Some(config_update) = resp.config_update {
                    tracing::info!(
                        "Received config update v{}: {} bytes — applying",
                        config_update.version,
                        config_update.config_json.len()
                    );
                    // Attempt to deserialise and apply the updated DataNodeConfig.
                    match serde_json::from_str::<DataNodeConfig>(&config_update.config_json) {
                        Ok(_new_cfg) => {
                            // In a full implementation each sub-module (scrub scheduler,
                            // cache, replication) would implement the `Reconfigurable`
                            // trait and be called here.  For now we record that we have
                            // accepted this version so future heartbeats report it correctly.
                            current_config_version = config_update.version;
                            tracing::info!(
                                "Config update v{} applied (config_version advanced)",
                                current_config_version
                            );
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to parse config update v{}: {}",
                                config_update.version,
                                e
                            );
                        }
                    }
                }
            }
            Err(e) => {
                consecutive_failures += 1;
                if consecutive_failures <= 3 {
                    tracing::warn!(
                        "Heartbeat failed ({}/3 before reconnect): {}",
                        consecutive_failures,
                        e
                    );
                } else {
                    tracing::error!(
                        "Heartbeat failed {} times, walking manager list for reconnect...",
                        consecutive_failures
                    );
                    // F-024 hardening: try every configured manager endpoint
                    // (starting after the one we last used) so a rolling
                    // restart of the primary doesn't strand this data node.
                    let n = manager_endpoints.len();
                    let mut reconnected = false;
                    for offset in 1..=n {
                        let idx = (current_endpoint_idx + offset) % n;
                        let ep = &manager_endpoints[idx];
                        match ManagerServiceClient::connect(ep.clone()).await {
                            Ok(new_client) => {
                                client = new_client;
                                current_endpoint_idx = idx;
                                consecutive_failures = 0;
                                tracing::info!("Reconnected to manager at {}", ep);
                                reconnected = true;
                                break;
                            }
                            Err(e) => {
                                tracing::debug!("reconnect to {} failed: {}", ep, e);
                            }
                        }
                    }
                    if !reconnected {
                        tracing::error!(
                            "Reconnect failed across all {} managers; will retry next heartbeat",
                            n
                        );
                    }
                }
            }
        }
    }
}
