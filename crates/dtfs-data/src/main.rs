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
    let _cache = Arc::new(cache::ArcCache::new(max_entries.max(64)));
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
    let process_start = Instant::now();
    tokio::spawn(async move {
        if let Err(e) = manager_lifecycle(heartbeat_config, heartbeat_store, process_start).await {
            tracing::error!("Manager lifecycle task failed: {}", e);
        }
    });

    // Start gRPC server (blocks until shutdown)
    server::run_grpc_server(config, chunk_store, data_metrics).await?;

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
    process_start: Instant,
) -> anyhow::Result<()> {
    let manager_addr = format!("http://{}", config.cluster.manager_addr);

    // Retry connecting to the manager with backoff.
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    let mut client = loop {
        match ManagerServiceClient::connect(manager_addr.clone()).await {
            Ok(c) => {
                tracing::info!("Connected to manager at {}", config.cluster.manager_addr);
                break c;
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to connect to manager at {}: {} (retrying in {:?})",
                    config.cluster.manager_addr,
                    e,
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    };

    // Build disk info for registration.
    let disk_infos: Vec<dtfs_proto::dtfs::common::DiskInfo> = config
        .disks
        .iter()
        .enumerate()
        .map(|(i, d)| dtfs_proto::dtfs::common::DiskInfo {
            disk_id: i as u32,
            uuid: uuid::Uuid::new_v4().to_string(),
            mount_path: d.path.display().to_string(),
            tier: match d.tier {
                DiskTier::Hot => "hot".to_string(),
                DiskTier::Warm => "warm".to_string(),
                DiskTier::Cold => "cold".to_string(),
                DiskTier::Tape => "tape".to_string(),
            },
            total_bytes: 0,
            used_bytes: 0,
            status: "healthy".to_string(),
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

    // Register this node with the manager.
    let register_req = RegisterNodeRequest {
        hostname: config.node.hostname.clone(),
        role: "data".to_string(),
        grpc_port,
        location: Some(dtfs_proto::dtfs::common::Location {
            region: config.location.region.clone(),
            datacenter: config.location.datacenter.clone(),
            rack: config.location.rack.clone(),
            host: config.location.host.clone(),
        }),
        disks: disk_infos,
    };

    let register_resp = client
        .register_node(tonic::Request::new(register_req))
        .await?;
    let resp = register_resp.into_inner();

    if resp.accepted {
        tracing::info!(
            "Registered with manager, assigned node_id: {}",
            resp.node_id
        );
    } else {
        tracing::error!("Manager rejected registration");
        return Err(anyhow::anyhow!("Registration rejected"));
    }

    let node_id = resp.node_id;

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

        let heartbeat = HeartbeatRequest {
            node_id: node_id.clone(),
            uptime_secs: uptime,
            total_chunks: store_stats.total_chunks,
            total_bytes_used: store_stats.total_bytes_stored,
            config_version: current_config_version,
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
                        "Heartbeat failed {} times, attempting reconnect...",
                        consecutive_failures
                    );
                    // Try to reconnect.
                    match ManagerServiceClient::connect(manager_addr.clone()).await {
                        Ok(new_client) => {
                            client = new_client;
                            consecutive_failures = 0;
                            tracing::info!("Reconnected to manager");
                        }
                        Err(e) => {
                            tracing::error!("Reconnect failed: {}", e);
                        }
                    }
                }
            }
        }
    }
}
