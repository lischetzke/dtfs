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

#[tokio::main]
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

    // Connect to the cluster manager.
    let client = DtfsClient::new();
    let manager_addr = format!("http://{}", fuse_config.client.manager_addr);

    info!(addr = %manager_addr, "connecting to manager");
    client
        .connect(&manager_addr)
        .await
        .context("failed to connect to DTFS manager")?;

    info!("connected to manager successfully");

    // Save fields needed after fuse_config is moved into DtfsFilesystem.
    let manager_addr_display = fuse_config.client.manager_addr.clone();

    // Initialise the filesystem abstraction.
    let client_location = fuse_config.location.clone();
    let fs = DtfsFilesystem::new(
        client,
        client_location,
        fuse_config,
    );

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
            .allow_other(false)
            .fs_name("dtfs");

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
