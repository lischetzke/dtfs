use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod alerts;
mod api;
mod audit;
mod goal_check;
mod membership;
mod metadata;
mod monitor;
mod placement;
mod placement_state;
mod precache;
mod rest_auth;
mod scheduler;
mod server;
mod shutdown;
mod tiering;
mod webui;

use audit::AuditLog;
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
    // LIS-20 empty shared_secret, CR-6-1 memory ladder / placement pct,
    // CR2-8-1 message-size cap, zero intervals: one set, shared with the
    // config API (issue #129).
    config.validate_all().map_err(|e| anyhow::anyhow!(e))?;

    tracing::info!(
        "Manager {} at {} ({})",
        config.node.hostname,
        config.grpc.listen_addr,
        config.location
    );

    // CF-6 — resolve [tls] once; reused by the Raft transport, the gRPC
    // server, and every manager-side client connection.
    let tls_ctx = dtfs_common::tls::TlsContext::from_config(&config.tls)?;
    tls_ctx.log_startup("dtfs-manager", &config.tls.mode);

    // --- Raft initialisation ---

    let log_store_path = config.node.data_dir.join("raft-log");
    let sm_dir = config.node.data_dir.join("raft-sm");

    // Issue #145 — no automatic wipe of raft-log / raft-sm here. A voter that
    // forgets its vote and acknowledged entries can help elect a leader that
    // lacks committed entries; an inflated term on an intact log heals on its
    // own once the node reaches the leader. Manual recovery:
    // docs/src/operations/cluster-management.md.

    // 1. Open the RocksDB log store with bounded info-LOG / MANIFEST (BP-1).
    let log_store = dtfs_raft::log_store::LogStore::new_with_opts(
        &log_store_path,
        dtfs_raft::log_store::RocksDbLogOpts {
            keep_log_file_num: config.raft.rocksdb_keep_log_file_num,
            max_manifest_bytes: config.raft.rocksdb_max_manifest_bytes,
            write_buffer_mb: config.raft.rocksdb_write_buffer_mb,
            block_cache_mb: config.raft.rocksdb_block_cache_mb,
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

    // RAFT-6 / CR2-7-6 — refuse to boot as a voter if `open_at` just
    // discarded a corrupt/unreadable snapshot (blank state machine, BP-2)
    // AND this node's log has already been purged past some point. That
    // combination means the discarded snapshot was the only surviving
    // copy of everything before the purge point; winning an election in
    // that state would apply only the log's surviving suffix onto an
    // empty state machine and lose it cluster-wide on the next snapshot.
    // A genuinely fresh, never-bootstrapped node (blank SM, log never
    // purged) is unaffected.
    let sm_is_blank = { sm_data.read().await.last_applied_log.is_none() };
    let last_purged = log_store.peek_last_purged()?;
    if let Err(msg) = dtfs_raft::log_store::blank_sm_startup_allowed(sm_is_blank, last_purged.clone()) {
        anyhow::bail!(msg);
    }
    // Stale (non-blank) SM behind the purge point: same hazard, see
    // `stale_sm_startup_allowed`.
    let sm_last_applied = { sm_data.read().await.last_applied_log.clone() };
    if let Err(msg) = dtfs_raft::log_store::stale_sm_startup_allowed(
        sm_last_applied.as_ref(),
        last_purged.as_ref(),
    ) {
        anyhow::bail!(msg);
    }

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
    let network_factory = dtfs_raft::network::NetworkFactory {
        tls: tls_ctx.clone(),
        shared_secret: config.cluster.shared_secret.clone(),
        self_id: dtfs_common::node_id_from_hostname(&config.node.hostname),
    };
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
                tracing::warn!(error = %e, "Raft init failed (may already be initialised)");
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
                tracing::warn!(error = %e, "post-bootstrap trigger_snapshot failed");
            } else {
                tracing::info!("post-bootstrap snapshot triggered");
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
    let placement_engine = Arc::new(PlacementEngine::new(config.placement.clone()));

    // CF-5 — per-process memory budget + RSS sampler. When the operator
    // sets [memory] hard_cap_mb > 0 the sampler watches RSS and publishes a
    // pressure level; the scheduler polls it and skips background ticks at
    // the Warn level (cheapest ladder rung — re-replication/migration scans
    // hold the largest transient state). Cap 0 = unbounded: the sampler
    // still runs for the RSS gauge but the level stays pinned Normal. The
    // systemd MemoryHigh/MemoryMax drop-ins remain the hard backstop.
    let memory_budget =
        Arc::new(dtfs_common::memory::MemoryBudget::from_config(&config.memory));
    if config.memory.hard_cap_mb > 0 {
        tracing::info!(
            cap_mb = config.memory.hard_cap_mb,
            warn_pct = config.memory.warn_threshold_pct,
            shed_pct = config.memory.shed_threshold_pct,
            sample_secs = config.memory.sample_interval_secs,
            "CF-5 memory budget configured"
        );
    }
    let mem_mon = memory_budget.clone();
    tokio::spawn(async move {
        mem_mon.run_monitor().await;
    });

    // --- Task scheduler background task ---

    let task_scheduler = Arc::new(TaskScheduler::new(
        raft.clone(),
        sm_data.clone(),
        audit_log.clone(),
        // CNF-2 — terminal-task retention windows for the Bug AE prune.
        config.raft.done_task_retention_secs,
        config.raft.failed_task_retention_secs,
        tls_ctx.clone(),
        config.placement.clone(),
    ));
    // Bug Z / issue #157 — the leader's scheduler tick creates the `default`
    // volume when missing (was a one-shot write 5 s after bootstrap that
    // failed without quorum and never retried).
    task_scheduler.set_default_volume(bootstrap_default_volume(&config));
    // CF-5 — let the scheduler skip ticks under memory pressure.
    task_scheduler.set_memory_budget(memory_budget.clone());
    // LIS-19 — stamp DeleteChunk RPCs (flush / orphan reap) with the cluster secret.
    task_scheduler.set_shared_secret(config.cluster.shared_secret.clone());
    // C5 — per-target-DC copy budget for every replication task.
    task_scheduler.set_links(config.tiering.links.clone());

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
    let lease_gc_sm = sm_data.clone();
    let _lease_gc_handle = tokio::spawn(async move {
        TaskScheduler::run_lease_gc(lease_gc_raft, lease_gc_sm).await;
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

    let precache_ref = tiering_engine.clone();
    let precache_handle = tokio::spawn(async move {
        precache_ref.run_precache().await;
    });

    // --- Health monitor background task ---

    // Bug CH — the monitor detects and alerts only; repair is owned by
    // the leader-gated scheduler tick (no TaskScheduler handle here).
    // Voters let it forward status commits to the leader (heartbeats
    // land on one arbitrary manager, usually a follower). Bug CJ — it
    // judges liveness from replicated state, so it no longer needs the
    // ClusterManager heartbeat map.
    if config.cluster.accept_legacy_forward_secret && !config.cluster.shared_secret.is_empty() {
        tracing::warn!(
            "cluster.accept_legacy_forward_secret = true: follower forwards still carry the raw \
             shared_secret (for old leaders during a rolling update); set it to false on every \
             manager once all run this version"
        );
    }
    let health_monitor = HealthMonitor::new(
        Duration::from_secs(10),
        &config.monitoring,
        sm_data.clone(),
        raft.clone(),
        config.cluster.initial_voters.clone(),
        tls_ctx.clone(),
        config.cluster.shared_secret.clone(),
        config.cluster.accept_legacy_forward_secret,
        config.grpc.max_message_size_mb,
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
        tls_ctx.clone(),
    )
    .with_tiering(tiering_engine.clone()) // P0-9: receive per-chunk-per-DC reads
    .with_audit(audit_log.clone()); // Bug BX-10: placement.spill audit rows
    metadata_service.spawn_read_report_flusher();

    // --- Start servers ---

    // Bug U — broadcast shutdown signal: SIGTERM/SIGINT fires it once, every
    // server task observes it and stops accepting new connections.
    let shutdown_signal = crate::shutdown::ShutdownSignal::new();
    let grpc_shutdown = shutdown_signal.clone();
    let api_shutdown = shutdown_signal.clone();

    let grpc_handle = tokio::spawn(server::run_grpc_server(
        config.clone(),
        tls_ctx.clone(),
        metadata_service,
        raft.clone(),
        async move { grpc_shutdown.wait_for_shutdown().await },
    ));
    let api_handle = tokio::spawn(api::run_api_server(
        config.clone(),
        raft.clone(),
        sm_data.clone(),
        audit_log.clone(),
        task_scheduler.clone(),
        Some(memory_budget.clone()),
        Some(tiering_engine.clone()),
        async move { api_shutdown.wait_for_shutdown().await },
    ));

    audit_log.log(
        "system",
        "manager.start",
        &config.node.hostname,
        "success",
    );

    // Goal-reachability check once data nodes have had time to register
    // their disks (a check at t=0 would see an empty topology).
    {
        let sm = sm_data.clone();
        let audit = audit_log.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_secs(120)).await;
            let sm = sm.read().await;
            for v in sm.volumes.values() {
                goal_check::report_unreachable_goals(&audit, v, &sm.nodes);
            }
        });
    }

    // Bug BR-1 — self-register this manager's version + build_date + a live
    // last_seen_ms into shared Raft state. Managers are Raft peers, not
    // RegisterNode callers, so historically every peer's /api/v1/nodes
    // showed a blank version for all but the serving manager, and peer
    // liveness was synthesized as "always now" (api.rs). Registering via the
    // same RegisterNode RPC the data nodes use makes managers first-class
    // entries in sm.nodes: step-1 of the nodes projection renders their real
    // version and routes their health through effective_health (BP-3).
    let self_reg_config = config.clone();
    let self_reg_tls = tls_ctx.clone();
    let _self_register_handle = tokio::spawn(async move {
        manager_self_register_lifecycle(self_reg_config, self_reg_tls).await;
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
    // Bug CK — track whether we are stopping because of a FAILURE (a server
    // returned an error / a background task died unexpectedly) versus a
    // graceful shutdown signal. A graceful stop has the servers return
    // Ok(Ok(())) after the signal fires. Any other arm here means something
    // broke — most importantly a startup bind failure (Tailscale interface
    // not up yet at boot) — and the process MUST exit non-zero so systemd's
    // Restart=on-failure retries instead of treating exit 0 as success.
    let mut server_failed = false;
    tokio::select! {
        result = grpc_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("gRPC server stopped"),
                Ok(Err(e)) => { tracing::error!(error = %e, "gRPC server returned error"); server_failed = true; }
                Err(e) => { tracing::error!(error = %e, "gRPC server task panicked"); server_failed = true; }
            }
        }
        result = api_handle => {
            match result {
                Ok(Ok(())) => tracing::info!("API server stopped"),
                Ok(Err(e)) => { tracing::error!(error = %e, "API server returned error"); server_failed = true; }
                Err(e) => { tracing::error!(error = %e, "API server task panicked"); server_failed = true; }
            }
        }
        _ = monitor_handle => {
            tracing::error!("Health monitor exited unexpectedly");
            server_failed = true;
        }
        _ = scheduler_handle => {
            tracing::error!("Task scheduler exited unexpectedly");
            server_failed = true;
        }
        _ = tiering_handle => {
            tracing::error!("Tiering engine exited unexpectedly");
            server_failed = true;
        }
        _ = precache_handle => {
            tracing::error!("Pre-cache loop exited unexpectedly");
            server_failed = true;
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

    // Bug CK — if we got here because a server/background task FAILED (not a
    // graceful signal), exit non-zero AFTER the best-effort cleanup above so
    // systemd restarts us. The common case is a boot-time bind race against
    // the Tailscale interface, which self-heals on the next retry.
    if server_failed {
        tracing::error!(
            "DTFS manager exiting non-zero after a server/background-task failure \
             (systemd Restart=on-failure will retry)"
        );
        std::process::exit(1);
    }

    tracing::info!("DTFS manager exited cleanly");
    Ok(())
}

/// True when `host` is a wildcard bind (not dialable by peers or self via
/// that literal). Used by Bug BR-1 local-endpoint selection.
fn is_wildcard_listen_host(host: &str) -> bool {
    matches!(host, "0.0.0.0" | "::" | "[::]" | "*")
}

/// Parse `host:port` (IPv6 bracket form ok) into `(host, port)`. Falls back
/// to `(raw, default_port)` when no colon is present.
fn split_host_port(addr: &str, default_port: u32) -> (String, u32) {
    if let Some((host, port)) = addr.rsplit_once(':') {
        // Avoid treating bare IPv6 without brackets as host:port.
        if host.contains(':') && !host.starts_with('[') {
            return (addr.to_string(), default_port);
        }
        let port = port.parse().unwrap_or(default_port);
        (host.to_string(), port)
    } else {
        (addr.to_string(), default_port)
    }
}

/// Bug BR-1 / LIS-77 — ordered manager endpoints for self-register.
///
/// Order matters: dead `initial_voters` entries earlier in config used to
/// blackhole the keepalive forever (no connect timeout). Prefer:
/// 1. this node's own voter addr (hostname match),
/// 2. remaining voters (config order),
/// 3. a local dial target — the specific `listen_addr` IP when not a
///    wildcard (loopback fails when gRPC binds only the Tailscale IP),
///    otherwise `127.0.0.1:<port>`.
fn self_register_endpoints(
    hostname: &str,
    listen_addr: &str,
    voters: &[dtfs_common::config::InitialVoter],
    endpoint_url: impl Fn(&str) -> String,
) -> Vec<String> {
    let (listen_host, grpc_port) = split_host_port(listen_addr, 9401);
    let mut endpoints: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    // Push helpers scoped so the mut borrow of `endpoints` ends before return
    // (rustc 1.94 E0502: closure live across `endpoints` use → ICE otherwise).
    {
        let mut push = |bare: &str| {
            let ep = endpoint_url(bare);
            if seen.insert(ep.clone()) {
                endpoints.push(ep);
            }
        };

        let self_voter = voters.iter().find(|v| v.hostname == hostname);
        if let Some(self_v) = self_voter {
            push(&self_v.addr);
        }
        for v in voters {
            if v.hostname != hostname {
                push(&v.addr);
            }
        }

        let local_bare = if is_wildcard_listen_host(&listen_host) {
            format!("127.0.0.1:{grpc_port}")
        } else {
            format!("{listen_host}:{grpc_port}")
        };
        // A loopback dial registers this manager under 127.0.0.1 (the server
        // records the peer address), so skip it when our own voter addr is known.
        let loopback_ok = self_voter.is_none() || !is_wildcard_listen_host(&listen_host);
        if loopback_ok {
            push(&local_bare);
        }
        // local_bare always lands at least one entry; no empty fallback needed.
    }
    endpoints
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
///
/// LIS-77: each connect is bounded by [`dtfs_common::tls::CONNECT_TIMEOUT`];
/// each RegisterNode RPC is wall-clock bounded so a hung peer cannot stall
/// the whole walk. Endpoint order prefers self (see
/// [`self_register_endpoints`]).
async fn manager_self_register_lifecycle(
    config: dtfs_common::config::ManagerConfig,
    tls: dtfs_common::tls::TlsContext,
) {
    use dtfs_proto::dtfs::manager::manager_service_client::ManagerServiceClient;
    use dtfs_proto::dtfs::manager::RegisterNodeRequest;

    let (_, grpc_port) = split_host_port(&config.grpc.listen_addr, 9401);
    let endpoints = self_register_endpoints(
        &config.node.hostname,
        &config.grpc.listen_addr,
        &config.cluster.initial_voters,
        |bare| tls.endpoint_url(bare),
    );

    const REGISTER_RPC_TIMEOUT: Duration = Duration::from_secs(5);

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
    // CR2-8-1 (CMN-3) — this manager-to-manager RegisterNode client used
    // tonic's 4 MB default; size it from the same configured cap as the
    // listener it's calling into.
    let max_bytes = dtfs_common::config::max_message_bytes(config.grpc.max_message_size_mb);
    loop {
        ticker.tick().await;
        let n = endpoints.len();
        let mut ok = false;
        for offset in 0..n {
            let ep = &endpoints[(idx + offset) % n];
            match tls.connect(ep).await.map(|ch| {
                ManagerServiceClient::new(ch)
                    .max_decoding_message_size(max_bytes)
                    .max_encoding_message_size(max_bytes)
            }) {
                Ok(mut c) => {
                    let mut req = tonic::Request::new(build_req());
                    req.set_timeout(REGISTER_RPC_TIMEOUT);
                    match c.register_node(req).await {
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
                    }
                }
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

/// Bug Z — the `default` volume from `[defaults]`. The new AV policy fields
/// stay empty/disabled (legacy policy); operators upgrade explicitly via
/// `dtfs-cli volume migrate-policy default`.
fn bootstrap_default_volume(config: &dtfs_common::config::ManagerConfig) -> dtfs_common::types::VolumeConfig {
    dtfs_common::types::VolumeConfig {
        id: dtfs_common::types::VolumeId::new("default".to_string()),
        name: "default".to_string(),
        chunk_size_bytes: (config.defaults.chunk_size_mb as u64).saturating_mul(1024 * 1024),
        replication: config.defaults.replication.clone(),
        write_safety: config.defaults.write_safety.clone(),
        anchor_datacenter: config.defaults.anchor_datacenter.clone(),
        mandatory_replicas: dtfs_common::types::TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 },
        cache_replicas: dtfs_common::types::CacheReplication::default(),
        anchor: None,
        spill: dtfs_common::types::SpillPolicy::default(),
        version: 0,
        precache: Default::default(),
    }
}

/// Bug CK — operator-facing error for a server listener that cannot bind.
///
/// Production trigger: a manager starts at boot before its Tailscale
/// interface is up, so the configured bind address (`100.x:9401`) is not
/// yet assigned → `EADDRNOTAVAIL`. Pre-CK the manager logged a generic
/// "transport error" and exited 0; with `Restart=on-failure` systemd
/// treated that as success and never retried, so the node had no manager
/// until a manual start. Now the bind failure is surfaced clearly and
/// propagated so the process exits non-zero (see `main`'s server-failure
/// path) and systemd retries until the interface is ready.
pub(crate) fn bind_error(
    role: &str,
    addr: impl std::fmt::Display,
    e: std::io::Error,
) -> anyhow::Error {
    use std::io::ErrorKind;
    let hint = match e.kind() {
        ErrorKind::AddrNotAvailable => {
            "bind interface is not available / bind address cannot be found — \
             the network interface may not be up yet (e.g. Tailscale not connected at boot)"
        }
        ErrorKind::AddrInUse => "bind address is already in use — another process holds this port",
        _ => "cannot bind listener to the configured address",
    };
    anyhow::anyhow!("{role} server: {hint} (addr={addr}): {e}")
}

#[cfg(test)]
mod bug_ck_bind_error_tests {
    use super::bind_error;
    use std::io::{Error, ErrorKind};

    #[test]
    fn addr_not_available_message_is_clear() {
        let msg = bind_error("gRPC", "100.64.0.1:9401", Error::from(ErrorKind::AddrNotAvailable))
            .to_string();
        assert!(msg.contains("not available"), "{msg}");
        assert!(msg.contains("bind address cannot be found"), "{msg}");
        assert!(msg.contains("interface"), "{msg}");
        assert!(msg.contains("100.64.0.1:9401"), "{msg}");
    }

    #[test]
    fn addr_in_use_is_distinct() {
        let msg =
            bind_error("REST API", "0.0.0.0:9402", Error::from(ErrorKind::AddrInUse)).to_string();
        assert!(msg.contains("already in use"), "{msg}");
        assert!(msg.contains("REST API"), "{msg}");
    }
}

#[cfg(test)]
mod lis77_br1_self_register_endpoints_tests {
    use super::{is_wildcard_listen_host, self_register_endpoints};
    use dtfs_common::config::InitialVoter;

    fn voter(hostname: &str, addr: &str) -> InitialVoter {
        InitialVoter {
            hostname: hostname.to_string(),
            addr: addr.to_string(),
            region: String::new(),
            datacenter: String::new(),
            rack: String::new(),
        }
    }

    #[test]
    fn prefers_self_before_dead_earlier_voters() {
        // Config order: dead LELI peers first (the DC1 incident shape), then
        // the live local voter. Self-register must dial self first.
        let voters = vec![
            voter("dead-mgr01", "192.0.2.1:9401"),
            voter("dead-mgr02", "192.0.2.2:9401"),
            voter("dc1-mgr01", "100.64.0.10:9401"),
        ];
        let eps = self_register_endpoints("dc1-mgr01", "100.64.0.10:9401", &voters, |a| {
            format!("http://{a}")
        });
        assert_eq!(
            eps.first().map(String::as_str),
            Some("http://100.64.0.10:9401"),
            "self voter must be first: {eps:?}"
        );
        assert!(eps.iter().any(|e| e.contains("192.0.2.1")), "{eps:?}");
        // Specific listen IP is the local dial target — not 127.0.0.1
        // (gRPC binds only the Tailscale IP on DC1).
        assert!(
            !eps.iter().any(|e| e.contains("127.0.0.1")),
            "must not prefer loopback when listen_addr is specific: {eps:?}"
        );
    }

    #[test]
    fn wildcard_listen_falls_back_to_loopback() {
        let voters = vec![voter("mgr01", "10.0.0.1:9401")];
        let eps = self_register_endpoints("other", "0.0.0.0:9401", &voters, |a| {
            format!("http://{a}")
        });
        assert!(
            eps.iter().any(|e| e == "http://127.0.0.1:9401"),
            "wildcard listen needs loopback fallback: {eps:?}"
        );
    }

    #[test]
    fn wildcard_listen_with_known_self_voter_never_dials_loopback() {
        let voters = vec![voter("mgr01", "10.0.0.1:9401")];
        let eps = self_register_endpoints("mgr01", "0.0.0.0:9401", &voters, |a| format!("http://{a}"));
        assert_eq!(eps, vec!["http://10.0.0.1:9401".to_string()]);
    }

    #[test]
    fn empty_voters_still_has_local_endpoint() {
        let eps = self_register_endpoints("solo", "10.1.2.3:9401", &[], |a| format!("http://{a}"));
        assert_eq!(eps, vec!["http://10.1.2.3:9401".to_string()]);
    }

    #[test]
    fn wildcard_helper() {
        assert!(is_wildcard_listen_host("0.0.0.0"));
        assert!(is_wildcard_listen_host("::"));
        assert!(!is_wildcard_listen_host("100.64.0.10"));
    }
}
