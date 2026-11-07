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

/// DATA-6 — bound every control-plane RPC (Heartbeat, ReportChunkStatus,
/// RegisterNode) with a deadline so a manager that accepts the TCP
/// connection but never answers (partition, wedged leader, etc.) doesn't
/// hang the heartbeat loop indefinitely — it times out and the loop's
/// existing reconnect/backoff logic takes over.
const CONTROL_RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// DATA-8 — `ChunkStatusReport` is unbounded: after a large scrub finds
/// many corrupt chunks (or a rolling-restart backlog), a single report
/// could carry tens of thousands of ids in one gRPC message. Split into
/// fixed-size batches so no single call is oversized.
const CHUNK_STATUS_REPORT_BATCH_SIZE: usize = 1000;

/// Split `ids` into chunks of at most `n` each, preserving order. `n == 0`
/// degenerates to one batch (never divides by zero / infinite-loops).
fn batches(ids: Vec<String>, n: usize) -> Vec<Vec<String>> {
    if n == 0 {
        return vec![ids];
    }
    ids.chunks(n).map(|c| c.to_vec()).collect()
}

fn control_request<T>(body: T) -> tonic::Request<T> {
    let mut req = tonic::Request::new(body);
    req.set_timeout(CONTROL_RPC_TIMEOUT);
    req
}

/// CR-5-1 (PROTO-1) / LIS-56 — stamp `x-dtfs-rpc-*` metadata on a mutating
/// `dtfs-mgr-rpc` request (Heartbeat / ReportChunkStatus) from this data
/// node's own `[cluster].shared_secret`. Empty secret = no-op (legacy skip,
/// matches `require_mgr_rpc_auth`'s empty-secret bypass on the manager).
fn stamp_mgr_rpc_request<T>(secret: &str, method: &str, binding: &str, body: T) -> tonic::Request<T> {
    use dtfs_common::crypto::{stamp_mgr_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS};
    let mut req = control_request(body);
    if let Some((ts, mac)) = stamp_mgr_rpc_auth(secret, method, binding) {
        if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
            req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
            req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
        }
    }
    req
}

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

    // LIS-55 / LIS-20 — fail-closed empty shared_secret (no silent admit
    // on Put/Delete/ReplicateChunk auth skip). Lab escape hatch:
    // cluster.allow_insecure_empty_secret = true.
    config
        .cluster
        .validate_shared_secret()
        .map_err(|e| anyhow::anyhow!(e))?;
    // CR-6-1 (CMN-1) — reject an unusable memory ladder at startup
    // instead of flapping Warn/Shed on every sample.
    config.memory.validate().map_err(|e| anyhow::anyhow!(e))?;
    // CR2-8-1 (CMN-3) — reject an unusable message-size cap at startup.
    config.grpc.validate().map_err(|e| anyhow::anyhow!(e))?;

    tracing::info!(
        "Data node {} at {} ({})",
        config.node.hostname,
        config.grpc.listen_addr,
        config.location
    );

    // CF-6 — resolve [tls] once; reused by the gRPC server, the manager
    // heartbeat lifecycle, and outbound chunk replication.
    let tls_ctx = dtfs_common::tls::TlsContext::from_config(&config.tls)?;
    tls_ctx.log_startup("dtfs-data", &config.tls.mode);

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
    // DATA-START-1 — the rescan stat()s every `.dat` on every disk. On the
    // 50 TB LELI archive that is millions of stats on an HDD, i.e. minutes
    // to hours during which (pre-fix) the gRPC server had not bound and the
    // node had not registered — the whole node read as Down for the length
    // of an inventory walk on every restart. Run it in the background: the
    // server binds and heartbeats immediately with counters that start at
    // 0 and snap to the on-disk truth when the walk completes
    // (`rescan_existing_chunks` folds in puts that landed meanwhile).
    {
        let rescan_store = chunk_store.clone();
        tokio::spawn(async move {
            if let Err(e) = rescan_store.rescan_existing_chunks().await {
                tracing::warn!(
                    error = %e,
                    "rescan_existing_chunks failed; bytes_stored counter stays at the live delta"
                );
            }
        });
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

    // REV-4-5 — no server-side ARC cache is constructed. Nothing in the
    // workspace ever populated it (`ArcCache::put` had no caller): reads
    // stream straight off disk for Bug-I/U2 memory parity, so the cache
    // was permanently empty. That made the CF-5 ladder's "shrink ARC
    // cache" rung a no-op while `cache.max_memory_mb` was still reserved
    // out of `memory.hard_cap_mb` when sizing put_chunk concurrency —
    // phantom accounting that collapsed writes to one in-flight chunk on
    // a 1 GB-capped node. `config.cache` is now inert on this role.

    // Initialize I/O metrics.
    let data_metrics = Arc::new(metrics::DataMetrics::new());
    tracing::info!("Initialized I/O metrics");

    // AZ-1 — scrub scheduler holds a hot-reloadable `Arc<RwLock<ScrubConfig>>`
    // so the manager's heartbeat-driven ConfigUpdate path can lower the
    // effective interval at runtime (cluster-wide `[scrub]` config change).
    let scrub_config = Arc::new(tokio::sync::RwLock::new(config.scrub.clone()));

    // Bug BF-2 — integrity-fault collector. verify_on_read records here;
    // heartbeat drains + ships to mgr (BF-5); mgr auto-quarantines (BF-6).
    let integrity_faults = Arc::new(integrity::IntegrityFaultCollector::new(1024));
    // CF-8 — manual scrub trigger path: the heartbeat task receives a
    // ScrubRequestNow from the manager and pushes the level here; the
    // scheduler loop selects on it and runs the scrub immediately.
    let (scrub_now_tx, scrub_now_rx) =
        tokio::sync::mpsc::channel::<dtfs_common::types::ScrubLevel>(4);
    let scrub_scheduler = scrub::ScrubScheduler::new(
        chunk_store.clone(),
        scrub_config.clone(),
    );
    tokio::spawn(async move {
        scrub_scheduler.run(scrub_now_rx).await;
    });
    tracing::info!("Started scrub scheduler background task");

    // Start manager registration and heartbeat background task.
    // Clone Arcs so they can be shared between the heartbeat task and the gRPC server.
    let heartbeat_config = config.clone();
    let heartbeat_store = chunk_store.clone();
    let heartbeat_metrics = data_metrics.clone();
    let heartbeat_scrub_config = scrub_config.clone();
    let heartbeat_integrity = integrity_faults.clone();
    let heartbeat_tls = tls_ctx.clone();
    let process_start = Instant::now();
    tokio::spawn(async move {
        if let Err(e) = manager_lifecycle(
            heartbeat_config,
            heartbeat_tls,
            heartbeat_store,
            heartbeat_metrics,
            process_start,
            heartbeat_scrub_config,
            heartbeat_integrity,
            scrub_now_tx,
        )
        .await
        {
            tracing::error!(error = %e, "Manager lifecycle task failed");
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
    // REV-4-5 — plain RSS sampler. The Bug V rung this used to drive
    // (`run_monitor_with_cache`) shrank a cache nothing ever populated,
    // so it could only ever evict zero entries; spawning it advertised a
    // ladder step the node does not have. The data node's real
    // cheapest-first rungs are streaming-to-disk on both the write and
    // read paths (Bug I / U2) and, last resort, the opt-in put_chunk
    // shed (Bug G) that this sampler drives.
    let mon = memory.clone();
    tokio::spawn(async move {
        mon.run_monitor().await;
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
        let memory = memory.clone();
        let scrub_config_for_server = scrub_config.clone();
        let integrity_for_server = integrity_faults.clone();
        let tls = tls_ctx.clone();
        tokio::spawn(async move {
            server::run_grpc_server(
                config,
                tls,
                store,
                metrics,
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

/// DATA-REG-1 — register (or re-register) with the manager cluster until
/// accepted, rotating endpoints on RPC failure. Returns the assigned node
/// id. Called once at startup and again whenever a heartbeat answers
/// NOT_FOUND (the state machine no longer knows this node: a rebuilt
/// cluster, a decommission that was later reverted, a rolled-back
/// snapshot). Pre-fix registration ran exactly once per process, so a node
/// dropped from `sm.nodes` heartbeated an unknown id until someone
/// restarted the daemon.
async fn register_with_manager(
    client: &mut ManagerServiceClient<tonic::transport::Channel>,
    current_endpoint_idx: &mut usize,
    manager_endpoints: &[String],
    tls: &dtfs_common::tls::TlsContext,
    build_register_req: &(dyn Fn() -> RegisterNodeRequest + Sync),
    max_backoff: Duration,
) -> String {
    let mut reg_backoff = Duration::from_secs(1);
    loop {
        let req = build_register_req();
        match client.register_node(control_request(req)).await {
            Ok(resp) => {
                let resp = resp.into_inner();
                if resp.accepted {
                    tracing::info!(
                        "Registered with manager, assigned node_id: {}",
                        resp.node_id
                    );
                    return resp.node_id;
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
                    let idx = (*current_endpoint_idx + offset) % n;
                    let ep = &manager_endpoints[idx];
                    if let Ok(c) = tls.connect(ep).await.map(ManagerServiceClient::new) {
                        *client = c;
                        *current_endpoint_idx = idx;
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
    }
}

/// Register with the manager and send periodic heartbeats.
///
/// `chunk_store` is used to read live chunk/byte counts for each heartbeat.
/// `process_start` marks when this data node process was launched, used to
/// compute `uptime_secs`.
async fn manager_lifecycle(
    config: DataNodeConfig,
    // CF-6 — resolved [tls] context for manager connections.
    tls: dtfs_common::tls::TlsContext,
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
    // CF-8 — manual scrub trigger relay into the scrub scheduler.
    scrub_now_tx: tokio::sync::mpsc::Sender<dtfs_common::types::ScrubLevel>,
) -> anyhow::Result<()> {
    // CF-8 — dedupe state: each ScrubRequestNow id runs at most once per
    // process lifetime (the manager re-sends the latest request on every
    // heartbeat; it is replaced, never cleared).
    let mut last_scrub_request_id: Option<String> = None;
    // F-024 hardening: walk through every configured manager endpoint when the
    // primary is down (e.g. after a rolling restart). The list is built from
    // `manager_addr` + `manager_addrs` in priority order.
    let manager_endpoints: Vec<String> = config
        .cluster
        .all_manager_addrs()
        .into_iter()
        .map(|a| tls.endpoint_url(&a))
        .collect();

    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);

    let (mut client, mut current_endpoint_idx) = loop {
        let mut connected = None;
        for (idx, ep) in manager_endpoints.iter().enumerate() {
            match tls.connect(ep).await.map(ManagerServiceClient::new) {
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

    let mut node_id = register_with_manager(
        &mut client,
        &mut current_endpoint_idx,
        &manager_endpoints,
        &tls,
        &build_register_req,
        max_backoff,
    )
    .await;

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
        // CO-5-6 — keep the drained harvest so we can re-enqueue it if the
        // heartbeat carrying these counters fails to send (harvest zeroes
        // the live counters, so a dropped heartbeat loses the reads).
        let per_chunk = data_metrics.harvest_per_chunk(1000);
        let chunk_reads: Vec<dtfs_proto::dtfs::manager::PerChunkReadCounter> = per_chunk
            .iter()
            .map(|h| dtfs_proto::dtfs::manager::PerChunkReadCounter {
                chunk_id: h.chunk_id.clone(),
                reads_by_dc_1h: h.reads_by_dc.clone(),
                reads_by_dc_24h: h.reads_by_dc.clone(),
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
            // DATA-8 — send in fixed-size batches; on the first failed
            // batch, re-enqueue that batch plus every batch after it
            // (the unsent remainder) so nothing is lost and nothing
            // already accepted by the manager is re-sent.
            let batch_count = corrupt_ids.len().div_ceil(CHUNK_STATUS_REPORT_BATCH_SIZE);
            let mut batch_iter = batches(corrupt_ids.clone(), CHUNK_STATUS_REPORT_BATCH_SIZE).into_iter();
            let mut sent_ok = 0usize;
            while let Some(batch) = batch_iter.next() {
                let batch_len = batch.len();
                let report = dtfs_proto::dtfs::manager::ChunkStatusReport {
                    node_id: node_id.clone(),
                    chunks: batch
                        .iter()
                        .map(|id| dtfs_proto::dtfs::manager::ChunkStatus {
                            chunk_id: id.clone(),
                            status: "corrupt".into(),
                            size: 0,
                        })
                        .collect(),
                };
                let report_req = stamp_mgr_rpc_request(
                    &config.cluster.shared_secret,
                    "ReportChunkStatus",
                    &node_id,
                    report,
                );
                match client.report_chunk_status(report_req).await {
                    Ok(resp) => {
                        if resp.into_inner().accepted {
                            sent_ok += 1;
                            tracing::info!(
                                count = batch_len,
                                batch = sent_ok,
                                total_batches = batch_count,
                                "Bug O reported quarantined chunks to manager"
                            );
                        } else {
                            // CO-3-7 — `accepted == false` means the manager could
                            // not commit the de-location (e.g. a follower failed to
                            // forward to the leader). Treat it exactly like a
                            // transport error: re-enqueue this batch plus every
                            // batch after it (unsent remainder), or the corrupt
                            // replica stays advertised healthy and re-replication
                            // never queues until scrub independently re-detects it.
                            let mut remainder = batch;
                            remainder.extend(batch_iter.by_ref().flatten());
                            let remainder_len = remainder.len();
                            chunk_store.enqueue_pending_corruption(remainder.into_iter());
                            tracing::warn!(
                                count = remainder_len,
                                "Bug O / CO-3-7 ChunkStatusReport not accepted — will retry next heartbeat"
                            );
                            break;
                        }
                    }
                    Err(e) => {
                        // Re-enqueue this batch plus the unsent remainder on
                        // RPC failure so we try again next heartbeat, without
                        // re-sending batches the manager already accepted.
                        let mut remainder = batch;
                        remainder.extend(batch_iter.by_ref().flatten());
                        let remainder_len = remainder.len();
                        chunk_store.enqueue_pending_corruption(remainder.into_iter());
                        tracing::warn!(
                            error = %e,
                            count = remainder_len,
                            "Bug O ChunkStatusReport failed — will retry next heartbeat"
                        );
                        break;
                    }
                }
            }
        }

        let heartbeat_req = stamp_mgr_rpc_request(
            &config.cluster.shared_secret,
            "Heartbeat",
            &node_id,
            heartbeat,
        );
        match client
            .heartbeat(heartbeat_req)
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

                // CF-8 — manual scrub trigger relayed by the manager.
                // Dedupe by id (it is re-sent every heartbeat) and skip
                // stale requests (e.g. re-delivered across a data-node
                // restart long after the operator triggered).
                if let Some(sr) = &resp.scrub_request {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    if scrub::manual_scrub_due(
                        &sr.id,
                        sr.requested_at_ms,
                        last_scrub_request_id.as_deref(),
                        now_ms,
                        scrub::MANUAL_SCRUB_MAX_AGE_SECS,
                    ) {
                        let level = match sr.level.as_str() {
                            "quick" => Some(dtfs_common::types::ScrubLevel::Quick),
                            "deep" => Some(dtfs_common::types::ScrubLevel::Deep),
                            other => {
                                tracing::warn!(
                                    "CF-8 manual scrub request {} has unknown level {:?} — ignoring",
                                    sr.id,
                                    other
                                );
                                None
                            }
                        };
                        if let Some(level) = level {
                            // Mark handled even if the channel is full —
                            // a dropped duplicate must not retrigger
                            // forever on every heartbeat.
                            last_scrub_request_id = Some(sr.id.clone());
                            match scrub_now_tx.try_send(level) {
                                Ok(()) => tracing::info!(
                                    "CF-8 manual {} scrub request {} accepted — forwarding to scheduler",
                                    sr.level,
                                    sr.id
                                ),
                                Err(e) => tracing::warn!(
                                    "CF-8 manual scrub request {} dropped (scheduler busy): {}",
                                    sr.id,
                                    e
                                ),
                            }
                        }
                    }
                }

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
                            || sc.quick_interval_secs != s.quick_interval_secs
                            || sc.deep_interval_secs != s.deep_interval_secs
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
                            // CR-6-3 (PROTO-2) — exact-secs companions; `0`
                            // from an older manager means "not sent", so
                            // the scheduler falls back to the hours field.
                            sc.quick_interval_secs = s.quick_interval_secs;
                            sc.deep_interval_secs = s.deep_interval_secs;
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
                // DATA-REG-1 — the manager no longer knows this node id
                // (MGR-HB-1 maps the apply-level "node not found" to
                // NOT_FOUND). Re-register instead of heartbeating a ghost.
                if e.code() == tonic::Code::NotFound {
                    tracing::warn!(
                        node_id = %node_id,
                        error = %e,
                        "DATA-REG-1: manager does not know this node any more; re-registering"
                    );
                    if !drained_scrubs.is_empty() {
                        chunk_store.restore_scrub_results(drained_scrubs);
                    }
                    if !per_chunk.is_empty() {
                        data_metrics.restore_per_chunk(per_chunk);
                    }
                    node_id = register_with_manager(
                        &mut client,
                        &mut current_endpoint_idx,
                        &manager_endpoints,
                        &tls,
                        &build_register_req,
                        max_backoff,
                    )
                    .await;
                    consecutive_failures = 0;
                    continue;
                }
                // CO-5-6 — the same heartbeat carried drained scrub
                // results and harvested per-chunk read counters; both
                // drain (zero) their sources as the payload is built, so
                // a failed send loses them. Re-enqueue both so the next
                // heartbeat ships them. Mirrors the Bug O / BF-5 pattern.
                if !drained_scrubs.is_empty() {
                    chunk_store.restore_scrub_results(drained_scrubs);
                }
                if !per_chunk.is_empty() {
                    data_metrics.restore_per_chunk(per_chunk);
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
                        match tls.connect(ep).await.map(ManagerServiceClient::new) {
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

#[cfg(test)]
mod data_6_control_deadline_tests {
    use super::control_request;

    #[test]
    fn control_request_carries_a_grpc_timeout_deadline() {
        // DATA-6 — `set_timeout` stamps the `grpc-timeout` metadata key
        // tonic reads to enforce the deadline; assert it is actually
        // present so a future refactor can't silently drop the timeout.
        let req = control_request(42u32);
        assert!(req.metadata().get("grpc-timeout").is_some());
    }
}

#[cfg(test)]
mod data_8_batches_tests {
    use super::batches;

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| i.to_string()).collect()
    }

    #[test]
    fn splits_into_expected_batch_count() {
        let out = batches(ids(2500), 1000);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].len(), 1000);
        assert_eq!(out[1].len(), 1000);
        assert_eq!(out[2].len(), 500);
        // Order preserved.
        assert_eq!(out[0][0], "0");
        assert_eq!(out[2][499], "2499");
    }

    #[test]
    fn empty_input_yields_no_batches() {
        assert!(batches(Vec::new(), 1000).is_empty());
    }

    #[test]
    fn failure_at_batch_2_of_3_reenqueues_exactly_the_unsent_remainder() {
        // 2500 ids -> 3 batches of 1000/1000/500. A failure ON batch 2
        // (index 1) must re-enqueue batch 2 + batch 3 = 1000 + 500 = 1500,
        // never batch 1 (already accepted by the manager).
        let all = batches(ids(2500), 1000);
        assert_eq!(all.len(), 3);
        let mut iter = all.into_iter();
        let _batch1_sent_ok = iter.next().unwrap();
        let failed_batch = iter.next().unwrap();
        let mut remainder = failed_batch;
        remainder.extend(iter.flatten());
        assert_eq!(remainder.len(), 1500);
    }
}
