// DTFS FUSE Client Mount — Phase 2.
//
// Entry point for the FUSE client binary. Loads configuration, connects
// to the DTFS cluster manager, initialises the filesystem abstraction
// layer, and (on Linux) would mount via fuse3.

use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info};

use dtfs_client::client::DtfsClient;
use dtfs_common::config::{self, FuseConfig};

mod filesystem;
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

    // Initialise the filesystem abstraction.
    let client_location = fuse_config.location.clone();
    let _fs = DtfsFilesystem::new(
        client,
        client_location,
        fuse_config,
    );

    info!(mountpoint, "filesystem initialised");

    // FUSE3 mount is Linux-only. On other platforms we print a message
    // and exit gracefully.
    if cfg!(target_os = "linux") {
        info!(
            mountpoint,
            "FUSE3 mount support will be enabled on Linux builds"
        );
        // TODO: Phase 2 Linux — call fuse3::Session::new() and mount.
        // For now, wait for a signal to keep the process alive.
        info!("waiting for shutdown signal (Ctrl+C)");
        tokio::signal::ctrl_c()
            .await
            .context("failed to listen for Ctrl+C")?;
        info!("received shutdown signal, unmounting");
    } else {
        info!(
            "FUSE3 mount is Linux-only. Running on {}, filesystem abstraction \
             is ready but FUSE mount is not available. Use the gRPC client \
             API or run on Linux for FUSE access.",
            std::env::consts::OS
        );
    }

    info!("DTFS FUSE client exiting");
    Ok(())
}
