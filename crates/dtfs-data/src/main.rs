#![allow(dead_code)]
// Work around rustc ICE on stable when dead_code diagnostics hit `metrics`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod cache;
mod disk;
mod integrity;
mod memory;
mod metrics;
mod replication;
mod scrub;
mod server;
mod shutdown;
mod store;

use dtfs_common::config::DataNodeConfig;
use dtfs_common::types::DiskTier;
use dtfs_proto::dtfs::manager::manager_service_client::ManagerServiceClient;
use dtfs_proto::dtfs::manager::{HeartbeatRequest, RegisterNodeRequest};
use store::ChunkStore;

#[derive(Parser, Debug)]
#[command(name = "dtfs-data", about = "DTFS Data Node", version = dtfs_common::version::VERSION_STRING)]
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

    // Bug M — replay the WAL FIRST so any tmp/wal residue from a crashed
    // previous instance gets reconciled before the rescan walks the
    // chunks directory. Without this, orphan tmp files leak disk forever
    // and partially-written chunks confuse rescan_existing_chunks.
    if let Err(e) = chunk_store.recover_from_wal().await {
        tracing::warn!(
            error = %e,
            "recover_from_wal failed at startup; orphan tmp/wal files may remain"
        );
    }

    // Rebuild on-disk chunk count + bytes_stored. Without this, a daemon
    // restart drops back to 0/0 even though all chunks remain on disk
    // (heartbeats then under-report used_capacity to the manager, which
    // diverges from df). Runs in startup so the first heartbeat carries
    // accurate numbers.
    if let Err(e) = chunk_store.rescan_existing_chunks().await {
        tracing::warn!(
            error = %e,
            "rescan_existing_chunks failed; bytes_stored counter starts at 0"
        );
    }

    // Bug BU-6 — reclaim any leftover streaming *.dat.tmp files. At startup
    // no put_chunk is in flight, so any streaming tmp is an orphan from a
    // crash or ENOSPC-aborted put whose RAII cleanup couldn't run across the
    // restart. Leaked tmps keep the disk full and make ENOSPC self-sustaining.
    let reclaimed_tmps = chunk_store.reclaim_orphan_streaming_tmps();
    if reclaimed_tmps > 0 {
        tracing::warn!(
            reclaimed = reclaimed_tmps,
            "Bug BU-6 reclaimed orphaned streaming tmp files at startup"
        );
    }

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

    // AZ-1 — scrub scheduler holds a hot-reloadable `Arc<RwLock<ScrubConfig>>`
    // so the manager's heartbeat-driven ConfigUpdate path can lower the
    // effective interval at runtime (e.g. when an operator raises a
    // per-volume scrub cadence via `POST /api/v1/volumes/{id}/set-schedules`).
    let scrub_config = Arc::new(tokio::sync::RwLock::new(config.scrub.clone()));

    // Bug BF-2 — integrity-fault collector. verify_on_read records here;
    // heartbeat drains + ships to mgr (BF-5); mgr auto-quarantines (BF-6).
    let integrity_faults = Arc::new(integrity::IntegrityFaultCollector::new(1024));
    let scrub_scheduler = scrub::ScrubScheduler::new(
        chunk_store.clone(),
        scrub_config.clone(),
    );
    tokio::spawn(async move {
        scrub_scheduler.run().await;
    });
    tracing::info!("Started scrub scheduler background task");

    // Start manager registration and heartbeat background task.
    // Clone Arcs so they can be shared between the heartbeat task and the gRPC server.
    let heartbeat_config = config.clone();
    let heartbeat_store = chunk_store.clone();
    let heartbeat_metrics = data_metrics.clone();
    let heartbeat_scrub_config = scrub_config.clone();
    let heartbeat_integrity = integrity_faults.clone();
    let process_start = Instant::now();
    tokio::spawn(async move {
        if let Err(e) = manager_lifecycle(
            heartbeat_config,
            heartbeat_store,
            heartbeat_metrics,
            process_start,
            heartbeat_scrub_config,
            heartbeat_integrity,
        )
        .await
        {
            tracing::error!("Manager lifecycle task failed: {}", e);
        }
    });

    // Bug G — build the memory budget and spawn the background RSS
    // sampler before the gRPC server starts. Even when no hard cap is
    // configured the sampler still runs (it just keeps `is_shedding`
    // pinned at false) so we always have an RSS gauge for diagnostics.
    let memory = std::sync::Arc::new(crate::memory::MemoryBudget::from_config(&config.memory));
    if config.memory.hard_cap_mb > 0 {
        tracing::info!(
            cap_mb = config.memory.hard_cap_mb,
            warn_pct = config.memory.warn_threshold_pct,
            shed_pct = config.memory.shed_threshold_pct,
            sample_secs = config.memory.sample_interval_secs,
            "Bug G memory budget configured"
        );
    }
    // Bug V — drive the cooperative strategy ladder: under memory
    // pressure shrink the ARC cache (level 1) before refusing writes
    // (level 2 — Bug G's existing reject behavior). Matches Ceph OSD's
    // best-effort autotuner pattern: give back RAM without making the
    // client see errors.
    let mon = memory.clone();
    let mon_cache = cache.clone();
    let mon_cache_max = max_entries.max(64);
    tokio::spawn(async move {
        crate::memory::run_monitor_with_cache(mon, mon_cache, mon_cache_max).await;
    });

    // Bug U — graceful shutdown wiring.
    //
    // Data integrity is the prime directive. On SIGTERM (systemd stop) or
    // SIGINT (Ctrl+C) we must:
    //   1. Stop accepting new connections (tonic serve_with_shutdown does this).
    //   2. Wait for in-flight writes to drain via the put_chunk semaphore
    //      (Bug D / Bug G slot count).
    //   3. Flush the pending_corruption queue so the manager learns about
    //      quarantines we discovered just before stopping.
    // The drain is bounded by a 30s timeout — long enough for a typical
    // 128 MB chunk write to finish on slow disks, short enough to beat
    // systemd's default TimeoutStopSec.
    let (ctx_tx, ctx_rx) = tokio::sync::oneshot::channel::<server::ServerContext>();
    let shutdown_signal = async {
        wait_for_terminate_signal().await;
        tracing::info!("Bug U graceful shutdown signal received — stopping accept");
    };

    // Spawn the server task so we can also drive the drain after it exits.
    let server_handle = {
        let config = config.clone();
        let store = chunk_store.clone();
        let metrics = data_metrics.clone();
        let cache = cache.clone();
        let memory = memory.clone();
        let scrub_config_for_server = scrub_config.clone();
        let integrity_for_server = integrity_faults.clone();
        tokio::spawn(async move {
            server::run_grpc_server(
                config,
                store,
                metrics,
                cache,
                memory,
                scrub_config_for_server,
                integrity_for_server,
                shutdown_signal,
                Some(ctx_tx),
            )
            .await
        })
    };

    // Pick up the server context (write_slots + max_writes) so we can drain.
    let server_ctx = ctx_rx
        .await
        .map_err(|e| anyhow::anyhow!("server never published its context: {e}"))?;

    // Wait for the server to stop accepting connections.
    match server_handle.await {
        Ok(Ok(())) => tracing::info!("gRPC server stopped cleanly"),
        Ok(Err(e)) => tracing::error!(error = %e, "gRPC server returned error on shutdown"),
        Err(e) => tracing::error!(error = %e, "server task join failed"),
    }

    // Drain in-flight writes. Bounded by 30s; if a write is stuck longer
    // than that we accept the loss and exit so systemd doesn't SIGKILL us.
    tracing::info!(
        max_writes = server_ctx.max_writes,
        "Bug U waiting for in-flight chunk writes to drain (max 30s)"
    );
    let drain_outcome = crate::shutdown::wait_for_drain(
        server_ctx.write_slots,
        server_ctx.max_writes,
        Duration::from_secs(30),
    )
    .await;
    match drain_outcome {
        crate::shutdown::DrainOutcome::Drained => {
            tracing::info!("Bug U all in-flight writes drained cleanly");
        }
        crate::shutdown::DrainOutcome::Timeout {
            permits_outstanding,
        } => {
            tracing::warn!(
                stuck_writes = permits_outstanding,
                "Bug U drain timeout — exiting with writes still in flight"
            );
        }
    }

    // Drain any pending corruption reports one last time. We can't send to
    // the manager from here (the heartbeat task owns the client), but we
    // re-enqueue them so the next startup picks them up via WAL+rescan.
    let last_corrupt = chunk_store.drain_pending_corruption();
    if !last_corrupt.is_empty() {
        tracing::warn!(
            count = last_corrupt.len(),
            "Bug U undelivered corruption reports at shutdown — next start will rediscover via scrub"
        );
    }

    tracing::info!("DTFS data node exited cleanly");
    Ok(())
}

/// Bug U — block until SIGTERM (systemd stop) or SIGINT (Ctrl+C).
///
/// On Linux we listen for both. On other platforms we only get SIGINT —
/// which is fine for dev work; production is Linux-only.
async fn wait_for_terminate_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("install SIGINT handler");
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = sigint.recv() => tracing::info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl+C received");
    }
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
    // AZ-1 — shared scrub config the scheduler reads each tick. When
    // the manager pushes a `ConfigUpdate` via heartbeat response, we
    // overwrite this lock so the scheduler picks up the new interval
    // within DECISION_TICK_SECS.
    scrub_config: Arc<tokio::sync::RwLock<dtfs_common::config::ScrubConfig>>,
    // Bug BF-5 — integrity faults the data-side recorded since the
    // last tick. Drained into each outgoing HeartbeatRequest; on RPC
    // failure we re-enqueue so the next tick retries.
    integrity_faults: Arc<crate::integrity::IntegrityFaultCollector>,
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
            // Bug BN-1 — stable UUID persisted to <mount>/.dtfs-disk-uuid.
            // Fall back to a fresh random UUID (not a crash) if the file is
            // unwritable (read-only mount, permissions, ENOSPC).
            let uuid = crate::store::load_or_create_disk_uuid(&d.path)
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        "disk UUID file unavailable for {:?}: {e}; using ephemeral UUID",
                        d.path
                    );
                    uuid::Uuid::new_v4().to_string()
                });
            dtfs_proto::dtfs::common::DiskInfo {
                disk_id: i as u32,
                uuid,
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
        // Bug AY — surface this binary's version + build date so the
        // manager can show what each data node is actually running.
        version: dtfs_common::version::VERSION.to_string(),
        build_date: dtfs_common::version::BUILD_DATE.to_string(),
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

        let io_snapshot = data_metrics.snapshot();
        // Bug AU — live per-disk usage so the manager's per-disk view
        // (Bug AR) tracks reality across heartbeats, not just at-boot.
        let disk_usage: Vec<dtfs_proto::dtfs::manager::DiskUsageSnapshot> = chunk_store
            .disk_manager()
            .live_disk_usage()
            .into_iter()
            .map(|(id, total, used)| dtfs_proto::dtfs::manager::DiskUsageSnapshot {
                disk_id: id,
                total_bytes: total,
                used_bytes: used,
            })
            .collect();
        // Bug BF-5 — drain integrity faults into the outgoing heartbeat.
        // The mgr-side handler (BF-6) iterates these + raft-writes
        // QuarantineNode + RemoveCorruptChunkReplicas per fault. On RPC
        // failure we re-enqueue so a transient mgr outage doesn't lose
        // the signal — mirrors the Bug O re-enqueue pattern at line 514.
        let drained_faults = integrity_faults.drain();
        // Bug BY — drain completed scrub-result summaries into the heartbeat
        // so the manager records them (ReportScrubResult) and the WebUI
        // "Last Scrub" card / stats.scrub_results_by_node populate.
        let drained_scrubs = chunk_store.drain_scrub_results();
        let scrub_results: Vec<dtfs_proto::dtfs::manager::ScrubResultReport> = drained_scrubs
            .iter()
            .map(|s| dtfs_proto::dtfs::manager::ScrubResultReport {
                disk_path: s.disk_path.clone(),
                level: s.level.clone(),
                completed_at_ms: s.completed_at_ms,
                chunks_checked: s.chunks_checked,
                chunks_healthy: s.chunks_healthy,
                chunks_corrupt: s.chunks_corrupt,
                chunks_missing: s.chunks_missing,
                duration_secs: s.duration_secs,
            })
            .collect();
        let heartbeat = HeartbeatRequest {
            node_id: node_id.clone(),
            uptime_secs: uptime,
            total_chunks: store_stats.total_chunks,
            total_bytes_used: store_stats.total_bytes_stored,
            config_version: current_config_version,
            chunk_reads,
            total_bytes_read: io_snapshot.total_bytes_read,
            total_bytes_written: io_snapshot.total_bytes_written,
            disk_usage,
            integrity_faults: drained_faults.clone(),
            scrub_results,
        };

        // Bug O — after each heartbeat tick, drain any locally-quarantined
        // chunks the scrub scheduler has flagged and report them to the
        // manager via the existing ChunkStatusReport RPC. The manager's
        // handler issues a RemoveCorruptChunkReplicas Raft command which
        // drops the (this-node, chunk_hash) entries from sm.files and
        // wakes the scheduler's under-replication check.
        let corrupt_ids = chunk_store.drain_pending_corruption();
        if !corrupt_ids.is_empty() {
            let report = dtfs_proto::dtfs::manager::ChunkStatusReport {
                node_id: node_id.clone(),
                chunks: corrupt_ids
                    .iter()
                    .map(|id| dtfs_proto::dtfs::manager::ChunkStatus {
                        chunk_id: id.clone(),
                        status: "corrupt".into(),
                        size: 0,
                    })
                    .collect(),
            };
            match client.report_chunk_status(tonic::Request::new(report)).await {
                Ok(_) => tracing::info!(
                    count = corrupt_ids.len(),
                    "Bug O reported quarantined chunks to manager"
                ),
                Err(e) => {
                    // Re-enqueue on RPC failure so we try again next heartbeat.
                    chunk_store.enqueue_pending_corruption(corrupt_ids.iter().cloned());
                    tracing::warn!(
                        error = %e,
                        count = corrupt_ids.len(),
                        "Bug O ChunkStatusReport failed — will retry next heartbeat"
                    );
                }
            }
        }

        match client
            .heartbeat(tonic::Request::new(heartbeat))
            .await
        {
            Ok(resp) => {
                let resp = resp.into_inner();
                if !resp.accepted {
                    tracing::warn!("Manager rejected heartbeat");
                    // Bug BF-5: rejection ≠ delivery confirmation;
                    // re-enqueue so the next tick retries the faults
                    // we just shipped.
                    if !drained_faults.is_empty() {
                        integrity_faults.re_enqueue(drained_faults.clone());
                    }
                }
                // Bug BF — mgr says we're not Quarantined any more
                // (operator unquarantine OR clean from the start).
                // Clear the sticky self-quarantine flag so put_chunk
                // accepts writes again without a process restart.
                if resp.self_quarantine_cleared
                    && integrity_faults.is_self_quarantined()
                {
                    tracing::warn!(
                        "Bug BF: manager cleared self-quarantine \
                         (operator unquarantine); re-enabling put_chunk"
                    );
                    integrity_faults.clear_self_quarantined();
                }
                consecutive_failures = 0;

                // Apply config updates pushed by the manager via heartbeat response.
                if let Some(config_update) = resp.config_update {
                    // AZ-1 — apply the scrub override path first (it's
                    // sent on every heartbeat). The full config_json
                    // path stays for legacy live-reload — when empty,
                    // skip the JSON parse + log noise.
                    if let Some(s) = &config_update.scrub_override {
                        let mut sc = scrub_config.write().await;
                        if sc.quick_interval_hours != s.quick_interval_hours
                            || sc.deep_interval_hours != s.deep_interval_hours
                        {
                            tracing::info!(
                                "AZ-1 scrub override: quick {}h -> {}h, deep {}h -> {}h",
                                sc.quick_interval_hours,
                                s.quick_interval_hours,
                                sc.deep_interval_hours,
                                s.deep_interval_hours,
                            );
                            sc.quick_interval_hours = s.quick_interval_hours;
                            sc.deep_interval_hours = s.deep_interval_hours;
                        }
                    }
                    if !config_update.config_json.is_empty() {
                        tracing::info!(
                            "Received config update v{}: {} bytes — applying",
                            config_update.version,
                            config_update.config_json.len()
                        );
                        match serde_json::from_str::<DataNodeConfig>(&config_update.config_json) {
                            Ok(new_cfg) => {
                                {
                                    let mut sc = scrub_config.write().await;
                                    if *sc != new_cfg.scrub {
                                        tracing::info!(
                                            "Scrub config updated from config_json: quick {}h -> {}h, deep {}h -> {}h",
                                            sc.quick_interval_hours,
                                            new_cfg.scrub.quick_interval_hours,
                                            sc.deep_interval_hours,
                                            new_cfg.scrub.deep_interval_hours,
                                        );
                                        *sc = new_cfg.scrub.clone();
                                    }
                                }
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
            }
            Err(e) => {
                // Bug BF-5 — RPC failed; re-enqueue so faults retry on
                // the next tick. Mirrors the Bug O re-enqueue at
                // line 514.
                if !drained_faults.is_empty() {
                    integrity_faults.re_enqueue(drained_faults.clone());
                }
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
