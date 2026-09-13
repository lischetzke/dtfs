#![allow(dead_code)]
#![allow(unused_imports)]
// Mitigate rustc 1.94.x ICE in dead_code analysis.

// DTFS FUSE Client Mount — Phase 2 (Linux: fuse3 mount).
//
// Entry point for the FUSE client binary. Loads configuration, connects
// to the DTFS cluster manager, initialises the filesystem abstraction
// layer, and mounts via fuse3 (Linux) or exits gracefully on other platforms.

use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info};

use dtfs_client::client::DtfsClient;
use dtfs_common::config::{self, FuseConfig};

mod filesystem;
mod fuse_adapter;
mod local_cache;
mod readahead;
mod writebuf;

use filesystem::DtfsFilesystem;

#[derive(Parser, Debug)]
#[command(name = "dtfs-fuse", about = "DTFS FUSE Client Mount", version = dtfs_common::version::VERSION_STRING)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "/etc/dtfs/fuse.toml")]
    config: String,

    /// Mount point (overrides config)
    #[arg(short, long)]
    mountpoint: Option<String>,
}

// F-026: force a multi-threaded runtime with at least 4 worker threads. A
// single-vCPU client had `worker_threads = num_cpus = 1`, which meant any
// task holding a Mutex stalled the entire FUSE service under sustained
// small-file load. Four workers give us interleaving headroom even on
// 1-vCPU hosts without measurably raising memory.
#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    // Initialise tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // Load configuration.
    let fuse_config: FuseConfig = config::load_config(Path::new(&args.config))
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // CR-6-1 (CMN-1) — reject an unusable memory ladder at startup
    // instead of flapping Warn/Shed on every sample.
    fuse_config
        .memory
        .validate()
        .map_err(|e| anyhow::anyhow!(e))?;
    // CR2-8-1 (CMN-3) — reject an unusable message-size cap at startup.
    fuse_config
        .client
        .validate()
        .map_err(|e| anyhow::anyhow!(e))?;

    let mountpoint = args
        .mountpoint
        .clone()
        .unwrap_or_else(|| {
            fuse_config
                .mount
                .mountpoint
                .to_str()
                .unwrap_or("/mnt/dtfs")
                .to_string()
        });

    info!(
        config = %args.config,
        mountpoint = %mountpoint,
        manager_addr = %fuse_config.client.manager_addr,
        location = %fuse_config.location,
        readahead_mode = %fuse_config.readahead.mode,
        "starting DTFS FUSE client"
    );

    // Connect to the cluster manager. F-024 hardening + P0-3 runtime failover:
    // register *all* manager endpoints with the client so subsequent RPCs can
    // fail over to a healthy manager when the active one dies mid-session.
    let client = DtfsClient::new();
    // CF-6 — resolve [tls] once and install it BEFORE the first connect so
    // every cached channel (manager and data node) carries the right
    // transport. Default config (mode "auto", no certs) stays plaintext.
    let tls_ctx = dtfs_common::tls::TlsContext::from_config(&fuse_config.tls)?;
    tls_ctx.log_startup("dtfs-fuse", &fuse_config.tls.mode);
    client.set_tls(tls_ctx.clone());
    // LIS-19 — stamp PutChunk with the same cluster secret the data node verifies.
    client.set_shared_secret(&fuse_config.client.shared_secret);
    // CMN-3 — install the configured gRPC message-size cap before the
    // first connect (channels are cached per address).
    client.set_max_message_size_mb(fuse_config.client.max_message_size_mb);
    let endpoints: Vec<String> = fuse_config
        .client
        .all_manager_addrs()
        .into_iter()
        .map(|a| tls_ctx.endpoint_url(&a))
        .collect();

    client.connect_many(&endpoints).await.with_context(|| {
        format!(
            "failed to connect to any of the configured managers: {:?}",
            endpoints
        )
    })?;

    // P0-9: attach our DC label to every GetChunk request so the data node
    // can bucket per-chunk reads by DC for tiering's auto-move-replica logic.
    client.set_requester_location(
        &fuse_config.location.datacenter,
        &fuse_config.location.host,
    );

    info!(count = endpoints.len(), "connected to manager pool");

    // Save fields needed after fuse_config is moved into DtfsFilesystem.
    let manager_addr_display = fuse_config.client.manager_addr.clone();
    let allow_other = fuse_config.mount.allow_other;

    // P0-6: clear orphan write leases from a previous crashed FUSE process
    // on this host BEFORE we start serving FUSE operations. Otherwise an
    // orphan lease blocks all writers until the 15s lease_ms elapses.
    // We pass our own PID as `except_pid` so any healthy concurrent FUSE
    // process on this host keeps its leases.
    //
    // Bug J — wrap this call in a hard timeout. The release_leases_by_host
    // RPC goes through Raft; if the cluster is busy (rolling restart,
    // Bug G shedding event, network partition) the call can take many
    // seconds, during which FUSE startup hangs and the systemd unit looks
    // permanently "active but not mounted". A 5 s ceiling lets startup
    // proceed; leftover leases will expire via the 15 s lease_ms anyway.
    let hostname_for_cleanup = fuse_config.location.host.clone();
    let current_pid = std::process::id();
    let cleanup_fut =
        client.release_leases_by_host(&hostname_for_cleanup, current_pid);
    match tokio::time::timeout(std::time::Duration::from_secs(5), cleanup_fut).await {
        Ok(Ok(resp)) => {
            if resp.released_count > 0 {
                info!(
                    host = %hostname_for_cleanup,
                    pid = current_pid,
                    released = resp.released_count,
                    "released orphan write leases from previous FUSE process"
                );
            }
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e,
                "orphan lease cleanup failed; proceeding anyway"
            );
        }
        Err(_elapsed) => {
            tracing::warn!(
                host = %hostname_for_cleanup,
                "orphan lease cleanup timed out after 5s; proceeding — orphans \
                 will expire via lease_ms"
            );
        }
    }

    // Bug AA — self-register with the manager so the topology view
    // shows this FUSE client even when it never writes (read-only Plex
    // viewers). Best-effort with a 5s timeout; failure doesn't block
    // mount because the user-visible filesystem still works.
    let register_loc = dtfs_proto::dtfs::common::Location {
        region: fuse_config.location.region.clone(),
        datacenter: fuse_config.location.datacenter.clone(),
        rack: fuse_config.location.rack.clone(),
        host: fuse_config.location.host.clone(),
    };
    let register_hostname = fuse_config.location.host.clone();
    let register_secret = Some(fuse_config.client.shared_secret.clone());
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client.register_self_as_client(&register_hostname, register_loc, register_secret),
    )
    .await
    {
        Ok(Ok(())) => info!(
            host = %register_hostname,
            "Bug AA self-registered as client"
        ),
        Ok(Err(e)) => tracing::warn!(error = %e, "Bug AA self-register failed; proceeding"),
        Err(_) => tracing::warn!("Bug AA self-register timed out; proceeding"),
    }

    // Bug BQ-1 — periodic client keepalive. The Bug AA register above is
    // one-shot; without a refresh an idle mounted client's last_seen_ms
    // freezes and effective_health (BP-3) projects it Down even though the
    // mount is alive. Mirror the data node's heartbeat loop (data
    // main.rs::manager_lifecycle): re-register on a cadence shorter than
    // raft.node_liveness_secs. Re-register is audit-idempotent (Bug BJ) so
    // steady state does not spam the audit log; on RPC error the client
    // pool advances to another manager and the next tick retries.
    let hb_secs = fuse_config.client.heartbeat_interval_secs;
    if hb_secs > 0 {
        let hb_client = client.clone();
        let hb_hostname = fuse_config.location.host.clone();
        let hb_location = fuse_config.location.clone();
        let hb_secret = Some(fuse_config.client.shared_secret.clone());
        tokio::spawn(async move {
            run_client_heartbeat(std::time::Duration::from_secs(hb_secs), move || {
                let c = hb_client.clone();
                let host = hb_hostname.clone();
                let loc = dtfs_proto::dtfs::common::Location {
                    region: hb_location.region.clone(),
                    datacenter: hb_location.datacenter.clone(),
                    rack: hb_location.rack.clone(),
                    host: hb_location.host.clone(),
                };
                let secret = hb_secret.clone();
                async move { c.register_self_as_client(&host, loc, secret).await }
            })
            .await;
        });
        info!(interval_secs = hb_secs, "Bug BQ-1 client keepalive started");
    }

    // P0-11: set up the disk-backed local cache (if configured) before
    // moving fuse_config into DtfsFilesystem.
    let local_cache_cfg = fuse_config.cache.clone();
    // CF-5 — snapshot the memory config before fuse_config is moved.
    let memory_cfg = fuse_config.memory.clone();
    let client_location = fuse_config.location.clone();
    // Bug U — keep a handle to the client (cheap Arc clone) so we can
    // release this PID's leases on the way out, after DtfsFilesystem has
    // taken ownership of its own copy.
    let shutdown_client = client.clone();
    let mut fs = DtfsFilesystem::new(
        client,
        client_location,
        fuse_config,
    );

    if let Some(cache_dir) = local_cache_cfg.local_cache_dir.clone() {
        let max_bytes = local_cache_cfg.local_cache_max_gb * 1024 * 1024 * 1024;
        let max_age = local_cache_cfg
            .max_age_days
            .map(|d| std::time::Duration::from_secs(u64::from(d) * 86400));
        match local_cache::LocalChunkCache::new(cache_dir.clone(), max_bytes, max_age).await {
            Ok(cache) => {
                let cache_arc = std::sync::Arc::new(cache);
                fs.set_local_cache(cache_arc.clone());

                // Background pruner sweeps expired entries on the configured cadence.
                let prune_interval =
                    std::time::Duration::from_secs(local_cache_cfg.prune_interval_secs);
                let _pruner = local_cache::spawn_pruner(cache_arc, prune_interval);
                info!(
                    cache_dir = %cache_dir.display(),
                    max_bytes,
                    max_age_days = ?local_cache_cfg.max_age_days,
                    "local chunk cache initialised"
                );
            }
            Err(e) => {
                tracing::warn!(
                    cache_dir = %cache_dir.display(),
                    error = %e,
                    "local chunk cache disabled — init failed"
                );
            }
        }
    }

    // CR-6-5 (FUSE-5) — wrap the filesystem in an `Arc` now that its
    // `&mut self` setup (`set_local_cache`) is done, so a small always-on
    // task can hold a clone and periodically sweep expired
    // `metadata_cache` entries (bounded eviction on insert handles the
    // size cap; this handles the staleness the size cap alone can't).
    let fs = std::sync::Arc::new(fs);
    {
        let sweep_fs = fs.clone();
        let sweep_interval = fs.metadata_ttl();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(sweep_interval);
            loop {
                ticker.tick().await;
                sweep_fs.sweep_expired_metadata_cache();
            }
        });
    }

    // CF-5 — per-process memory budget + RSS sampler. When the operator sets
    // [memory] hard_cap_mb > 0 the sampler watches RSS and publishes a
    // pressure level; a small ladder task halves the readahead/prefetch byte
    // budget at the Warn level and drops all unconsumed prefetch at Shed, so
    // a RAM-starved client gives memory back instead of being OOM/cgroup-
    // killed. Cap 0 = unbounded: the sampler still runs for the RSS gauge but
    // the level stays pinned Normal. The systemd MemoryHigh/MemoryMax drop-ins
    // remain the hard backstop.
    let memory_budget =
        std::sync::Arc::new(dtfs_common::memory::MemoryBudget::from_config(&memory_cfg));
    if memory_cfg.hard_cap_mb > 0 {
        info!(
            cap_mb = memory_cfg.hard_cap_mb,
            warn_pct = memory_cfg.warn_threshold_pct,
            shed_pct = memory_cfg.shed_threshold_pct,
            sample_secs = memory_cfg.sample_interval_secs,
            "CF-5 memory budget configured"
        );
        let controller = fs.readahead_controller();
        let ladder_budget = memory_budget.clone();
        tokio::spawn(async move {
            run_readahead_pressure_ladder(ladder_budget, controller).await;
        });
    }
    let mem_mon = memory_budget.clone();
    tokio::spawn(async move {
        mem_mon.run_monitor().await;
    });

    info!(mountpoint, "filesystem initialised");

    // FUSE3 mount is Linux-only.
    #[cfg(target_os = "linux")]
    {
        use fuse3::MountOptions;
        use fuse_adapter::DtfsFuse3Adapter;
        #[allow(unused_imports)]
        use libc;

        let adapter = DtfsFuse3Adapter::from_shared(fs);
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        let mut opts = MountOptions::default();
        opts.uid(uid)
            .gid(gid)
            .read_only(false)
            .allow_other(allow_other)
            .fs_name("dtfs");
        if allow_other {
            info!("FUSE allow_other enabled — non-root processes can access the mount (F-028)");
        }

        info!(
            mountpoint,
            uid,
            gid,
            manager = %manager_addr_display,
            "mounting DTFS via fuse3"
        );

        let handle = fuse3::raw::Session::<DtfsFuse3Adapter>::new(opts)
            .mount(adapter, std::path::Path::new(&mountpoint))
            .await
            .context("fuse3 mount failed")?;

        // Bug U — graceful shutdown for the FUSE client.
        //
        // Data integrity is the prime directive: if systemd sends SIGTERM
        // (`systemctl stop dtfs-fuse`) we MUST
        //   1. Tell the kernel to unmount the filesystem cleanly so that
        //      any cached writes flush out via FUSE flush/release ops,
        //   2. Release this PID's outstanding write leases on the manager
        //      so the next acquirer doesn't have to wait `lease_ms` for
        //      them to expire.
        // Without step 1 a SIGKILL on us would leave the mount in a
        // half-dead state ("Transport endpoint is not connected") that
        // requires manual `fusermount -uz` to clean up.
        let mut handle = handle;
        let signal_fut = wait_for_terminate_signal();
        tokio::pin!(signal_fut);

        let session_result = tokio::select! {
            res = &mut handle => {
                // External unmount (fusermount -u, or kernel umount).
                info!("FUSE session exited externally");
                res
            }
            _ = &mut signal_fut => {
                info!("Bug U signal received — calling MountHandle::unmount() for clean teardown");
                // Move the handle out of the select! borrow scope and
                // request a synchronous unmount. The kernel will issue
                // flush()/release() ops on every open inode before this
                // returns, giving writebuf a chance to push final segments.
                match tokio::time::timeout(
                    std::time::Duration::from_secs(30),
                    handle.unmount(),
                )
                .await
                {
                    Ok(Ok(())) => {
                        info!("Bug U FUSE unmount completed cleanly");
                        Ok(())
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %e, "Bug U FUSE unmount returned error — kernel may have already dropped the mount");
                        Ok(())
                    }
                    Err(_) => {
                        tracing::warn!("Bug U FUSE unmount timed out after 30s — orphan mount may require `fusermount -uz`");
                        Ok(())
                    }
                }
            }
        };
        session_result.context("fuse3 session error")?;

        // Bug U step 2: release this process's leases before exiting.
        // The kernel unmount above released local FDs but the *manager-side*
        // lease state still names this PID. Without this call, a write to
        // the same path right after we exit would block on the lease GC
        // tick (1s) plus lease_ms (15s) before a new writer could acquire.
        //
        // Bounded by 5s — same reasoning as the startup cleanup: don't
        // hold systemd hostage. Use the same hostname/PID as startup so
        // we exactly undo our own footprint.
        let exit_release = shutdown_client.release_leases_by_host(&hostname_for_cleanup, current_pid);
        match tokio::time::timeout(std::time::Duration::from_secs(5), exit_release).await {
            Ok(Ok(resp)) => {
                if resp.released_count > 0 {
                    info!(
                        host = %hostname_for_cleanup,
                        pid = current_pid,
                        released = resp.released_count,
                        "Bug U released own write leases on exit"
                    );
                }
            }
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "Bug U exit-time lease release failed");
            }
            Err(_) => {
                tracing::warn!("Bug U exit-time lease release timed out");
            }
        }

        info!("DTFS unmounted from {}", mountpoint);
    }

    #[cfg(not(target_os = "linux"))]
    {
        info!(
            "FUSE3 mount is Linux-only (running on {}). \
             Filesystem abstraction is ready but FUSE mount is not available.",
            std::env::consts::OS
        );
        info!("waiting for shutdown signal (Ctrl+C)");
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl+C")?;
    }

    info!("DTFS FUSE client exiting");
    Ok(())
}

/// Bug U — block until SIGTERM (systemd stop) or SIGINT (Ctrl+C). Logs which
/// signal woke us. The kernel sends SIGTERM on `systemctl stop`, SIGINT on
/// foreground Ctrl+C — handling both keeps dev and prod behaviour identical.
async fn wait_for_terminate_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "failed to install SIGTERM handler");
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                error!(error = %e, "failed to install SIGINT handler");
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => info!("SIGTERM received"),
            _ = sigint.recv() => info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        info!("Ctrl+C received");
    }
}

/// Bug BQ-1 — drive a periodic client keepalive. Generic over the beat
/// closure (and its error type) so it can be unit-tested without a live
/// manager. The loop never returns: each tick calls `beat()`, logs the
/// outcome at debug, and continues regardless of error so a transient
/// manager failover cannot stop the client from refreshing its liveness.
///
/// The interval's immediate first tick is consumed before the loop because
/// the caller has already performed the startup registration.
async fn run_client_heartbeat<F, Fut, E>(interval: std::time::Duration, mut beat: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), E>>,
    E: std::fmt::Display,
{
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Consume the immediate first tick — we already registered at startup.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        match beat().await {
            Ok(()) => tracing::debug!("Bug BQ-1 client keepalive ok"),
            Err(e) => tracing::debug!(error = %e, "Bug BQ-1 client keepalive failed; retry next tick"),
        }
    }
}

/// CF-5 — drive the FUSE memory-pressure ladder. Polls the budget's pressure
/// level on the sampler cadence and adjusts the readahead/prefetch cache:
///
///  * Warn → halve the live byte budget (evict down to it).
///  * Shed → pin the budget to 0 (stops new prefetch) and abort/clear all
///    unconsumed prefetch.
///  * Normal → restore the operator-configured budget.
///
/// Only acts on level changes to avoid hammering the cache on a stable
/// system. Best-effort, never rejects a read (§ A.5). Pure ladder logic is
/// factored into `pressure_target_budget` so it can be unit-tested.
async fn run_readahead_pressure_ladder(
    budget: std::sync::Arc<dtfs_common::memory::MemoryBudget>,
    controller: readahead::ReadAheadCacheController,
) {
    use dtfs_common::memory::PressureLevel;
    let mut tick = tokio::time::interval(budget.sample_interval());
    let mut last_level = PressureLevel::Normal;
    loop {
        tick.tick().await;
        let level = budget.pressure_level();
        if level == last_level {
            continue;
        }
        let target = pressure_target_budget(level, controller.configured_budget());
        // H2 — pin `prefetch_enabled` BEFORE shrinking/dropping, so Shed's
        // `drop_unconsumed()` below is not immediately undone by a read
        // that lands between the drop and the next tick: `on_chunk_read`
        // stops spawning new prefetch entirely at Shed. `shrink_to_bytes`
        // alone only evicts already-stored bytes, it does not stop new
        // spawns.
        controller.set_prefetch_enabled(level != PressureLevel::Shed);
        // Pin the live budget so no new prefetch can refill past the
        // target while we are under pressure.
        let evicted = controller.shrink_to_bytes(target);
        if level == PressureLevel::Shed {
            // Also abort in-flight prefetch tasks — shrink_to_bytes only
            // evicts already-stored bytes, not pending fetches.
            controller.drop_unconsumed();
        }
        info!(
            level = ?level,
            target_bytes = target,
            evicted,
            cache_bytes = controller.cache_bytes(),
            prefetch_enabled = controller.prefetch_enabled(),
            "CF-5 memory pressure ladder adjusted readahead budget"
        );
        last_level = level;
    }
}

/// CF-5 — pure ladder map from pressure level to readahead byte budget.
/// Normal restores the operator budget; Warn halves it; Shed is handled by
/// `drop_unconsumed` (returns 0 here as a defensive floor).
fn pressure_target_budget(
    level: dtfs_common::memory::PressureLevel,
    configured: usize,
) -> usize {
    use dtfs_common::memory::PressureLevel;
    match level {
        PressureLevel::Normal => configured,
        PressureLevel::Warn => configured / 2,
        PressureLevel::Shed => 0,
    }
}

#[cfg(test)]
mod cf5_ladder_tests {
    use super::pressure_target_budget;
    use dtfs_common::memory::PressureLevel;

    /// CF-5 — the FUSE ladder halves the readahead budget at Warn, floors it
    /// at Shed, and restores the full operator budget at Normal. Mirrors the
    /// data-node ARC ladder shape (Bug V) for the prefetch cache.
    #[test]
    fn cf5_pressure_target_budget_ladder() {
        let configured = 256 * 1024 * 1024;
        assert_eq!(pressure_target_budget(PressureLevel::Normal, configured), configured);
        assert_eq!(
            pressure_target_budget(PressureLevel::Warn, configured),
            configured / 2
        );
        assert_eq!(pressure_target_budget(PressureLevel::Shed, configured), 0);
    }
}

#[cfg(test)]
mod bq1_heartbeat_tests {
    use super::run_client_heartbeat;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    // Bug BQ-1 — the keepalive loop must fire on cadence AND keep going
    // across an RPC error, so a transient manager failover doesn't stop the
    // client from staying live. Drives a paused clock so the test is
    // deterministic (no wall-clock sleeps).
    #[tokio::test(start_paused = true)]
    async fn keepalive_fires_on_cadence_and_survives_errors() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let interval = Duration::from_secs(10);
        let task = tokio::spawn(async move {
            run_client_heartbeat(interval, move || {
                let n = c.fetch_add(1, Ordering::SeqCst);
                async move {
                    // Every 3rd beat simulates a manager RPC failure; the
                    // loop must survive it and keep beating.
                    if n % 3 == 2 {
                        Err("simulated manager RPC failure")
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        });
        // Let the task reach its first post-startup tick().await.
        tokio::task::yield_now().await;
        for _ in 0..6 {
            tokio::time::advance(interval).await;
            tokio::task::yield_now().await;
        }
        task.abort();
        let n = calls.load(Ordering::SeqCst);
        assert!(
            n >= 5,
            "expected >=5 keepalive beats across 6 intervals, got {n}"
        );
    }
}
