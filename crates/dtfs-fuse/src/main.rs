#![allow(dead_code)]
#![allow(unused_imports)]
// Mitigate rustc 1.94.x ICE in dead_code analysis (see ISSUES_TO_FIX.md #12).

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
mod migration;
mod readahead;
mod writebuf;

use filesystem::DtfsFilesystem;

#[derive(Parser, Debug)]
#[command(name = "dtfs-fuse", about = "DTFS FUSE Client Mount")]
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
    let endpoints: Vec<String> = fuse_config
        .client
        .all_manager_addrs()
        .into_iter()
        .map(|a| format!("http://{}", a))
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

    // P0-11: set up the disk-backed local cache (if configured) before
    // moving fuse_config into DtfsFilesystem.
    let local_cache_cfg = fuse_config.cache.clone();
    let client_location = fuse_config.location.clone();
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

    info!(mountpoint, "filesystem initialised");

    // FUSE3 mount is Linux-only.
    #[cfg(target_os = "linux")]
    {
        use fuse3::MountOptions;
        use fuse_adapter::DtfsFuse3Adapter;
        #[allow(unused_imports)]
        use libc;

        let adapter = DtfsFuse3Adapter::new(fs);
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

        // Block until the filesystem is unmounted (e.g., via `fusermount -u`
        // or process signal).
        handle.await.context("fuse3 session error")?;

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
