#![allow(dead_code)]
#![allow(unused_imports)]
// Mitigate rustc 1.94.x ICE in dead_code / join module analysis.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod alerts;
mod api;
#[allow(dead_code)]
mod audit;
#[allow(dead_code)]
mod cluster;
mod join;
mod metadata;
mod monitor;
mod placement;
mod placement_state;
mod scheduler;
mod server;
mod shutdown;
mod tiering;
mod webui;

use audit::AuditLog;
use cluster::ClusterManager;
use monitor::HealthMonitor;
use placement::PlacementEngine;
use scheduler::TaskScheduler;
use tiering::TieringEngine;

#[derive(Parser, Debug)]
#[command(name = "dtfs-manager", about = "DTFS Manager Node", version = dtfs_common::version::VERSION_STRING)]
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

    let log_store_path = config.node.data_dir.join("raft-log");
    let sm_dir = config.node.data_dir.join("raft-sm");

    // Bug AC — poisoned-Raft-state detector. Before handing the local
    // state to openraft, peek at the on-disk vote/log/snapshot triple.
    // If we crashed into a stuck-candidate state (term inflated wildly
    // by repeated self-vote elections while isolated, log lagging
    // behind by hundreds of entries), wipe the local state so that on
    // the next iteration we rejoin via InstallSnapshot from the
    // current leader.
    //
    // We rename rather than delete so the operator can forensically
    // inspect what was poisoned. Renamed dirs are not auto-deleted.
    //
    // Detection criteria (all must hold):
    //   * vote.term >= 100              — many failed elections
    //   * vote NOT committed            — we never won
    //   * vote.node == self             — voting for ourselves (isolation)
    //   * applied_index <= snapshot_idx  + 10  — barely caught up
    //   * log.last_index - applied > 500 — huge replay backlog
    //
    // If ANY criterion is missing, we leave the state alone — false
    // positive risk is real (e.g. a long-running healthy follower
    // might briefly have a high uncommitted vote during an election).
    if let Err(e) =
        maybe_recover_poisoned_raft_state(&log_store_path, &sm_dir).await
    {
        tracing::warn!(error = %e, "Bug AC poisoned-state check failed; proceeding anyway");
    }

    // 1. Open the RocksDB log store with bounded info-LOG / MANIFEST (BP-1).
    let log_store = dtfs_raft::log_store::LogStore::new_with_opts(
        &log_store_path,
        dtfs_raft::log_store::RocksDbLogOpts {
            keep_log_file_num: config.raft.rocksdb_keep_log_file_num,
            max_manifest_bytes: config.raft.rocksdb_max_manifest_bytes,
        },
    )?;
    tracing::info!("Raft log store opened at {:?}", log_store_path);

    // 2. Create the state machine. Snapshots are persisted under
    //    `<data_dir>/raft-sm/` so that a manager restart can restore Raft
    //    membership before any log replay starts (F-024 — without this,
    //    membership is lost as soon as the membership log entry is purged).
    let state_machine = dtfs_raft::state_machine::StateMachineStore::open_at(&sm_dir)?;
    tracing::info!("Raft state machine snapshot dir: {:?}", sm_dir);
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
        // Bug BP-1 — bound how many committed log entries are retained
        // after a snapshot so the on-disk `.sst` set stays small (an
        // implicit large default let ~10 000 tiny SSTs accumulate and
        // slowed RocksDB open to minutes — ISSUE-SCAN-2026-06-02).
        max_in_snapshot_log_to_keep: config.raft.max_in_snapshot_log_to_keep,
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
        let self_id = dtfs_common::node_id_from_hostname(&config.node.hostname);

        // Bug AC — the bootstrap node's advertised addr MUST be the
        // peer-routable IP:port, NOT `grpc.listen_addr` which is the
        // wildcard `0.0.0.0:9401`. Other Raft peers dial this exact
        // string; "0.0.0.0:9401" routes back to themselves and Raft
        // messages never reach this node. Pre-fix symptom: bootstrap
        // node showed empty WebUI forever because mgr02/03 couldn't
        // talk to it via Raft, so no log entries ever applied.
        //
        // Prefer the operator-specified addr from initial_voters; fall
        // back to grpc.listen_addr with a WARN if absent (legacy
        // single-node configs).
        let self_routable_addr = config
            .cluster
            .initial_voters
            .iter()
            .find(|v| v.hostname == config.node.hostname)
            .map(|v| v.addr.clone())
            .unwrap_or_else(|| {
                tracing::warn!(
                    listen_addr = %config.grpc.listen_addr,
                    "Bug AC bootstrap: no matching initial_voters entry for self; falling back to grpc.listen_addr — \
                     other managers may not be able to reach this node"
                );
                config.grpc.listen_addr.clone()
            });
        if self_routable_addr.starts_with("0.0.0.0")
            || self_routable_addr.starts_with("[::]")
            || self_routable_addr.starts_with("0:")
        {
            tracing::error!(
                addr = %self_routable_addr,
                "Bug AC bootstrap: self-addr is a wildcard ({}). Other managers WILL NOT be able to reach this node via Raft. Set [[cluster.initial_voters]] addr to the routable IP:port for this host.",
                self_routable_addr
            );
        }

        let self_node = dtfs_raft::DtfsNode {
            addr: self_routable_addr,
            hostname: config.node.hostname.clone(),
        };

        let mut members = BTreeMap::new();
        members.insert(self_id, self_node);

        // Add the configured initial voters so the cluster opens with all
        // expected managers at once rather than as a single-voter group with
        // no way for followers to join (fix for F-009).
        for voter in &config.cluster.initial_voters {
            let id = dtfs_common::node_id_from_hostname(&voter.hostname);
            if id == self_id {
                continue;
            }
            members.insert(
                id,
                dtfs_raft::DtfsNode {
                    addr: voter.addr.clone(),
                    hostname: voter.hostname.clone(),
                },
            );
        }

        let voter_count = members.len();
        match raft.initialize(members).await {
            Ok(_) => tracing::info!(
                "Raft cluster bootstrapped with {} voter(s), self={}",
                voter_count,
                self_id,
            ),
            Err(e) => {
                // Already initialised is fine (idempotent restart).
                tracing::warn!("Raft init: {} (may already be initialised)", e);
            }
        }

        // F-024: trigger an early snapshot after a short warm-up so the
        // membership change at log index 0 lands in a persistent on-disk
        // snapshot before the threshold is naturally reached. Without this,
        // a brand-new cluster that restarts before `snapshot_threshold`
        // logs accumulate has no snapshot to load and the FSM (including
        // membership) comes back empty.
        let raft_for_snap = raft.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
            if let Err(e) = raft_for_snap.trigger().snapshot().await {
                tracing::warn!("post-bootstrap trigger_snapshot failed: {}", e);
            } else {
                tracing::info!("post-bootstrap snapshot triggered");
            }
        });

        // Bug Z — upsert the "default" volume so anchor enforcement and
        // per-tier replication actually apply. Without this, allocate_chunks
        // can't read a `VolumeConfig` and the auto-implicit volume has
        // `anchor_datacenter = None`, so every chunk lands on whichever
        // node round-robin happens to pick. Plexmedia operator sets
        // `defaults.anchor_datacenter = "leli"` so data-primary01 holds a
        // copy of every chunk.
        //
        // AV-4-8 fix — the upsert is **insert-if-missing**, not
        // unconditional. Pre-fix every manager boot called
        // `UpsertVolume { config: bootstrap_default }` and the state-
        // machine's apply path is a blind `volumes.insert(id, config)`,
        // so any operator-supplied `set-cache` / `set-mandatory` on
        // `default` was silently destroyed on the next manager restart.
        // We now check the SM first and skip the upsert when `default`
        // already exists; a fresh cluster (no existing entry) still
        // gets the bootstrap volume.
        let raft_for_volume = raft.clone();
        let sm_data_for_bootstrap = sm_data.clone();
        let default_volume_replication = config.defaults.replication.clone();
        let default_chunk_size_bytes =
            (config.defaults.chunk_size_mb as u64).saturating_mul(1024 * 1024);
        let default_write_safety = config.defaults.write_safety.clone();
        let default_anchor = config.defaults.anchor_datacenter.clone();
        tokio::spawn(async move {
            // Wait for raft to elect a leader before proposing.
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;

            // AV-4-8 — skip when the operator already configured the
            // default volume (typical post-bootstrap restart).
            if sm_data_for_bootstrap.read().await.volumes.contains_key("default") {
                tracing::info!(
                    "Bug Z default volume already present; skipping bootstrap upsert (operator config preserved)"
                );
                return;
            }

            let vol_cfg = dtfs_common::types::VolumeConfig {
                id: dtfs_common::types::VolumeId::new("default".to_string()),
                name: "default".to_string(),
                chunk_size_bytes: default_chunk_size_bytes,
                replication: default_volume_replication,
                write_safety: default_write_safety,
                anchor_datacenter: default_anchor.clone(),
                // Bug AV — new fields default to empty/disabled so the
                // bootstrap-default volume stays on the legacy policy.
                // Operators upgrade explicitly via
                // `dtfs-cli volume migrate-policy default`.
                mandatory_replicas: dtfs_common::types::TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 },
                cache_replicas: dtfs_common::types::CacheReplication::default(),
                anchor: None,
                schedules: dtfs_common::types::VolumeSchedules::default(),
            };
            match raft_for_volume
                .client_write(dtfs_raft::ClientRequest::UpsertVolume { config: vol_cfg })
                .await
            {
                Ok(_) => tracing::info!(
                    anchor = ?default_anchor,
                    "Bug Z default volume upserted with anchor enforcement (fresh cluster)"
                ),
                Err(e) => {
                    tracing::warn!(error = %e, "Bug Z default volume upsert failed at bootstrap")
                }
            }
        });
    }

    // --- Shared services ---

    // P0-8: persist audit entries to a daily-rotated JSONL archive so the
    // 10k-entry ring buffer doesn't silently drop history.
    //
    // Bug AH — opt-in fan-out: when `audit.replicate_via_raft = true` in
    // manager.toml, every audit event ALSO gets proposed as a
    // `ClientRequest::AppendAudit` Raft write so all managers see the
    // same `/api/v1/audit` view. Default is OFF to make the initial
    // rolling deploy safe: an older follower can't deserialize the new
    // `AppendAudit` variant, so we wait until every voter is upgraded
    // before flipping the flag. The local ring + JSONL archive always
    // capture events for forensics regardless of the fan-out state.
    let audit_archive_dir = config.node.data_dir.join("audit");
    let audit_log = if config.audit.replicate_via_raft {
        tracing::info!(
            origin_manager = %config.node.hostname,
            "Bug AH audit Raft fan-out ENABLED (replicate_via_raft=true)"
        );
        Arc::new(AuditLog::new_with_archive_and_fanout(
            10_000,
            audit_archive_dir,
            raft.clone(),
            config.node.hostname.clone(),
        ))
    } else {
        tracing::info!(
            "Bug AH audit Raft fan-out DISABLED (set [audit] replicate_via_raft=true after upgrading all voters)"
        );
        Arc::new(AuditLog::new_with_archive(10_000, audit_archive_dir))
    };

    // P1-8: alert dispatcher (webhook sinks from config).
    let alert_dispatcher = Arc::new(alerts::AlertDispatcher::new(
        config.cluster.name.clone(),
        config.node.hostname.clone(),
        config.alerts.webhooks.clone(),
    ));
    if alert_dispatcher.sink_count() > 0 {
        tracing::info!(
            sinks = alert_dispatcher.sink_count(),
            "alert dispatcher armed"
        );
    }
    // ClusterManager no longer holds a Raft reference; heartbeat tracking is
    // its only responsibility. Raft writes are performed by HealthMonitor.
    let cluster_mgr = Arc::new(ClusterManager::new());
    let placement_engine = Arc::new(PlacementEngine::new(config.placement.clone()));
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

    // Bug BC-7 — orphan_scrub loop. Cross-checks on-disk chunks
    // against state_machine.chunk_locations + reaps the diff after
    // a grace period. Interval + grace come from the new
    // [scrub] block in manager.toml; defaults: 1d / 1d.
    let orphan_ref = task_scheduler.clone();
    let orphan_interval = config.scrub.orphan_interval_secs;
    let orphan_grace = config.scrub.orphan_grace_secs;
    let _orphan_handle = tokio::spawn(async move {
        orphan_ref.run_orphan_scrub(orphan_interval, orphan_grace).await;
    });

    // Bug BF-7 — auto-unquarantine sweep. Disabled when
    // auto_unquarantine_secs == 0 (default; operator-only). The tick
    // cadence is one-tenth of the cooldown (clamped to >= 30s) so the
    // window doesn't drift by more than ~10 %.
    let auto_unq_ref = task_scheduler.clone();
    let cooldown = config.scrub.auto_unquarantine_secs;
    let tick_secs = (cooldown / 10).max(30);
    let _auto_unq_handle = tokio::spawn(async move {
        auto_unq_ref
            .run_auto_unquarantine_sweep(tick_secs, cooldown)
            .await;
    });

    // P0-1: Lease GC — sweeps expired write leases every 1s through Raft.
    let lease_gc_raft = raft.clone();
    let _lease_gc_handle = tokio::spawn(async move {
        TaskScheduler::run_lease_gc(lease_gc_raft).await;
    });

    // --- Tiering engine background task ---

    // P1-8: wire the alert dispatcher into the scheduler before any work starts.
    task_scheduler.set_alerts(alert_dispatcher.clone());

    let tiering_engine = Arc::new(
        TieringEngine::new(
            config.tiering.clone(),
            task_scheduler.clone(),
            audit_log.clone(),
            sm_data.clone(),
        )
        .with_alerts(alert_dispatcher.clone()),
    );

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
    )
    .with_alerts(alert_dispatcher.clone()); // P1-8
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
    )
    .with_tiering(tiering_engine.clone()); // P0-9: receive per-chunk-per-DC reads

    // --- Start servers ---

    // Bug U — broadcast shutdown signal: SIGTERM/SIGINT fires it once, every
    // server task observes it and stops accepting new connections.
    let shutdown_signal = crate::shutdown::ShutdownSignal::new();
    let grpc_shutdown = shutdown_signal.clone();
    let api_shutdown = shutdown_signal.clone();

    let grpc_handle = tokio::spawn(server::run_grpc_server(
        config.clone(),
        metadata_service,
        raft.clone(),
        async move { grpc_shutdown.wait_for_shutdown().await },
    ));
    let api_handle = tokio::spawn(api::run_api_server(
        config.clone(),
        raft.clone(),
        sm_data.clone(),
        audit_log.clone(),
        join_handler.clone(),
        task_scheduler.clone(),
        async move { api_shutdown.wait_for_shutdown().await },
    ));

    audit_log.log(
        "system",
        "manager.start",
        &config.node.hostname,
        "success",
    );

    // Bug BR-1 — self-register this manager's version + build_date + a live
    // last_seen_ms into shared Raft state. Managers are Raft peers, not
    // RegisterNode callers, so historically every peer's /api/v1/nodes
    // showed a blank version for all but the serving manager, and peer
    // liveness was synthesized as "always now" (api.rs). Registering via the
    // same RegisterNode RPC the data nodes use makes managers first-class
    // entries in sm.nodes: step-1 of the nodes projection renders their real
    // version and routes their health through effective_health (BP-3).
    let self_reg_config = config.clone();
    let _self_register_handle = tokio::spawn(async move {
        manager_self_register_lifecycle(self_reg_config).await;
    });

    // Bug U — spawn a dedicated signal watcher that flips the shutdown
    // signal. We don't put the signal future inside `select!` directly
    // because we want ALL of (gRPC, API, monitors) to run to graceful
    // completion together, not racing.
    let sig_signal = shutdown_signal.clone();
    let sig_handle = tokio::spawn(async move {
        crate::shutdown::wait_for_terminate_signal().await;
        tracing::info!("Bug U shutdown signal received — broadcasting to servers");
        sig_signal.trigger();
    });

    // Wait for either:
    //   - SIGTERM/SIGINT triggers shutdown → both servers exit via graceful
    //     shutdown → we proceed to cleanup.
    //   - A background task panics → log and bail.
    tokio::select! {
        result = grpc_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("gRPC server stopped"),
                Ok(Err(e)) => tracing::error!(error = %e, "gRPC server returned error"),
                Err(e) => tracing::error!(error = %e, "gRPC server task panicked"),
            }
        }
        result = api_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("API server stopped"),
                Ok(Err(e)) => tracing::error!(error = %e, "API server returned error"),
                Err(e) => tracing::error!(error = %e, "API server task panicked"),
            }
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

    // Make sure the signal was at least triggered (covers the
    // "background task died" path → we still want a clean exit).
    if !shutdown_signal.is_fired() {
        shutdown_signal.trigger();
    }
    sig_handle.abort();

    // Bug U cleanup steps. Each is best-effort and short-circuited on
    // failure so that a stuck Raft snapshot doesn't hold the manager
    // hostage forever — systemd's TimeoutStopSec would SIGKILL us anyway.

    // 1. Trigger final Raft snapshot. Without this, an idle cluster
    //    accumulates log entries between snapshot thresholds; if the next
    //    start fails to read the log we lose everything since the last
    //    snapshot. 5s timeout is plenty for an in-memory state machine.
    let snap_raft = raft.clone();
    let snap_result = tokio::time::timeout(Duration::from_secs(5), async move {
        snap_raft.trigger().snapshot().await
    })
    .await;
    match snap_result {
        Ok(Ok(())) => tracing::info!("Bug U final Raft snapshot triggered cleanly"),
        Ok(Err(e)) => tracing::warn!(error = %e, "Bug U final snapshot trigger failed"),
        Err(_) => tracing::warn!("Bug U final snapshot trigger timed out (5s)"),
    }

    // 2. Flush + record clean-shutdown audit event. AuditLog persists to
    //    daily-rotated JSONL so this is durable across restarts.
    audit_log.log(
        "system",
        "manager.stop",
        &config.node.hostname,
        "clean",
    );

    tracing::info!("DTFS manager exited cleanly");
    Ok(())
}

/// Bug BR-1 — publish this manager's version/build_date + a live
/// last_seen_ms into shared Raft state.
///
/// Managers are Raft peers, not RegisterNode callers, so their identity
/// never entered the shared node registry: peers rendered a blank version
/// and a synthesized "always now" last_seen on each other's /api/v1/nodes.
/// This task self-registers through the exact RegisterNode RPC the data
/// nodes use (role="manager"), then refreshes on a cadence shorter than
/// `raft.node_liveness_secs` so a downed manager ages to non-Healthy via
/// effective_health (BP-3) like any other node.
///
/// Re-register is audit-idempotent (Bug BJ node_info_material_eq): the
/// steady-state keepalive adds no audit rows; only a real version change
/// (an upgrade) logs once. Registering against the peer pool converges as
/// soon as a leader exists — a follower's raft_write returns Unavailable,
/// so we walk to the next endpoint just like a data node does.
async fn manager_self_register_lifecycle(config: dtfs_common::config::ManagerConfig) {
    use dtfs_proto::dtfs::manager::manager_service_client::ManagerServiceClient;
    use dtfs_proto::dtfs::manager::RegisterNodeRequest;

    let grpc_port: u32 = config
        .grpc
        .listen_addr
        .rsplit(':')
        .next()
        .and_then(|p| p.parse().ok())
        .unwrap_or(9401);

    // Manager peer endpoints to register through, plus a loopback fallback
    // so a single-node / legacy config (no initial_voters) still registers
    // itself against its own listener.
    let mut endpoints: Vec<String> = config
        .cluster
        .initial_voters
        .iter()
        .map(|v| format!("http://{}", v.addr))
        .collect();
    let loopback = format!("http://127.0.0.1:{grpc_port}");
    if !endpoints.iter().any(|e| e.ends_with(&format!(":{grpc_port}"))) {
        endpoints.push(loopback.clone());
    }
    if endpoints.is_empty() {
        endpoints.push(loopback);
    }

    let build_req = || RegisterNodeRequest {
        hostname: config.node.hostname.clone(),
        role: "manager".to_string(),
        grpc_port,
        location: Some(dtfs_proto::dtfs::common::Location {
            region: config.location.region.clone(),
            datacenter: config.location.datacenter.clone(),
            rack: config.location.rack.clone(),
            host: config.location.host.clone(),
        }),
        disks: Vec::new(),
        shared_secret: Some(config.cluster.shared_secret.clone()),
        version: dtfs_common::version::VERSION.to_string(),
        build_date: dtfs_common::version::BUILD_DATE.to_string(),
    };

    // ~3 beats per liveness window, floored at 3s. The interval's immediate
    // first tick performs the startup registration.
    let interval_secs = (config.raft.node_liveness_secs / 3).max(3);
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut idx = 0usize;
    loop {
        ticker.tick().await;
        let n = endpoints.len();
        let mut ok = false;
        for offset in 0..n {
            let ep = &endpoints[(idx + offset) % n];
            match ManagerServiceClient::connect(ep.clone()).await {
                Ok(mut c) => match c.register_node(tonic::Request::new(build_req())).await {
                    Ok(resp) => {
                        if resp.into_inner().accepted {
                            idx = (idx + offset) % n;
                            ok = true;
                            break;
                        }
                        tracing::debug!(
                            endpoint = %ep,
                            "Bug BR-1 self-register rejected (shared_secret mismatch?)"
                        );
                    }
                    Err(e) => tracing::debug!(
                        endpoint = %ep,
                        error = %e,
                        "Bug BR-1 self-register RPC failed; trying next manager"
                    ),
                },
                Err(e) => tracing::debug!(
                    endpoint = %ep,
                    error = %e,
                    "Bug BR-1 manager endpoint unreachable"
                ),
            }
        }
        if !ok {
            tracing::debug!(
                "Bug BR-1 self-register: no manager reachable this tick; retrying next tick"
            );
        }
    }
}

/// Bug AC — pre-init poisoned-Raft-state detector + auto-recovery.
///
/// Inspects the local log store + state machine snapshot without
/// handing them to openraft. If the triple is "poisoned" (term has
/// inflated by hundreds of failed self-vote elections while
/// isolated, with the state machine miles behind the log), renames
/// the directories to backups so the next openraft initialization
/// starts fresh. The leader then snapshot-installs us back into the
/// cluster.
///
/// This trades a small startup delay (peek a few RocksDB keys) for
/// eliminating the most common Raft-stuck failure mode in multi-DC
/// clusters under intermittent network conditions.
async fn maybe_recover_poisoned_raft_state(
    log_store_path: &std::path::Path,
    sm_dir: &std::path::Path,
) -> anyhow::Result<()> {
    use std::fs;
    use std::time::SystemTime;

    if !log_store_path.exists() || !sm_dir.exists() {
        // First boot — nothing to recover.
        return Ok(());
    }

    // Peek log + vote without opening as RaftLogStorage. We open the
    // log store read-only here, then drop the handle BEFORE openraft
    // re-opens it. Avoids long-lived dual handles.
    let (last_log_id, vote) = {
        let ls = dtfs_raft::log_store::LogStore::new(log_store_path)?;
        let llid = ls.peek_last_log_id().ok().flatten();
        let v = ls.peek_vote().ok().flatten();
        // ls dropped here; RocksDB releases its lock.
        (llid, v)
    };

    // Peek last_applied from the on-disk snapshot. The snapshot file
    // contains the state machine's last_log_id at the time it was
    // taken; the running state machine catches up from there via
    // the log.
    let snapshot_applied_idx = {
        let sm = dtfs_raft::state_machine::StateMachineStore::open_at(sm_dir)?;
        let data = sm.data().read().await;
        let idx = data.last_applied_log.as_ref().map(|l| l.index).unwrap_or(0);
        drop(data);
        // sm dropped here.
        idx
    };

    let last_log_index = last_log_id.as_ref().map(|l| l.index).unwrap_or(0);
    let last_log_term = last_log_id
        .as_ref()
        .map(|l| {
            // Term lives inside LeaderId — extract via Debug fallback so
            // we don't depend on internal accessor stability.
            // LogId display: term-index; vote term is usually the same shape.
            // openraft 0.10 has `l.leader_id.term` but the API may shift; pull
            // it through serde-stable Display:
            let s = format!("{:?}", l);
            // "LogId { leader_id: LeaderId { term: 19, ... }, index: 100 }"
            s.split("term: ")
                .nth(1)
                .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0)
        })
        .unwrap_or(0);
    let vote_term = vote
        .as_ref()
        .map(|v| {
            // Vote's LeaderId is the same shape.
            let s = format!("{:?}", v);
            s.split("term: ")
                .nth(1)
                .and_then(|rest| rest.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0)
        })
        .unwrap_or(0);
    let vote_committed = vote
        .as_ref()
        .map(|v| v.is_committed())
        .unwrap_or(false);
    let applied_log_lag = last_log_index.saturating_sub(snapshot_applied_idx);

    // Detection: ALL of the following must be true.
    let too_many_failed_elections = vote_term >= 100;
    let term_jumped_way_above_log = vote_term > last_log_term + 50;
    let huge_applied_lag = applied_log_lag > 500;
    let vote_never_committed = !vote_committed;

    tracing::info!(
        vote_term,
        vote_committed,
        last_log_index,
        last_log_term,
        snapshot_applied_idx,
        applied_log_lag,
        "Bug AC pre-init Raft state peek"
    );

    if !(too_many_failed_elections
        && term_jumped_way_above_log
        && huge_applied_lag
        && vote_never_committed)
    {
        // Healthy state — leave as is.
        return Ok(());
    }

    tracing::error!(
        vote_term,
        last_log_index,
        last_log_term,
        snapshot_applied_idx,
        applied_log_lag,
        "Bug AC POISONED RAFT STATE DETECTED — wiping local raft-log + raft-sm so this manager rejoins via InstallSnapshot from the leader"
    );

    // Rename rather than delete so an operator can inspect what was wiped.
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let log_backup = log_store_path.with_extension(format!("poisoned.{ts}"));
    let sm_backup = sm_dir.with_extension(format!("poisoned.{ts}"));
    if let Err(e) = fs::rename(log_store_path, &log_backup) {
        tracing::error!(error = %e, "Bug AC failed to rename raft-log; manual recovery required");
        anyhow::bail!("rename raft-log: {e}");
    }
    if let Err(e) = fs::rename(sm_dir, &sm_backup) {
        tracing::error!(error = %e, "Bug AC failed to rename raft-sm; manual recovery required");
        // Try to put the log back so we don't leave half-recovered state.
        let _ = fs::rename(&log_backup, log_store_path);
        anyhow::bail!("rename raft-sm: {e}");
    }
    tracing::warn!(
        log_backup = %log_backup.display(),
        sm_backup = %sm_backup.display(),
        "Bug AC poisoned raft state moved to backup; fresh state will be initialized + snapshot-installed from leader"
    );
    Ok(())
}
