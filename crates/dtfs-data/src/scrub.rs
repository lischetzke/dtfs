// Quick + deep scrub engine.
// Periodic integrity checking with configurable intervals and manual triggers.

use std::sync::Arc;

use dtfs_common::config::ScrubConfig;
use dtfs_common::types::ScrubLevel;

use crate::store::ChunkStore;

/// Background scrub scheduler that runs quick and deep scrubs on configured
/// intervals against all disks managed by the chunk store.
pub struct ScrubScheduler {
    store: Arc<ChunkStore>,
    config: ScrubConfig,
}

impl ScrubScheduler {
    /// Create a new scrub scheduler.
    pub fn new(store: Arc<ChunkStore>, config: ScrubConfig) -> Self {
        Self { store, config }
    }

    /// Run the background scrub loop.
    ///
    /// This runs indefinitely, executing quick scrubs every
    /// `quick_interval_hours` and deep scrubs every `deep_interval_hours`.
    /// Between chunk checks it sleeps to respect `max_io_percent`.
    pub async fn run(&self) {
        let quick_interval = tokio::time::Duration::from_secs(
            self.config.quick_interval_hours * 3600,
        );
        let deep_interval = tokio::time::Duration::from_secs(
            self.config.deep_interval_hours * 3600,
        );

        let mut quick_ticker = tokio::time::interval(quick_interval);
        let mut deep_ticker = tokio::time::interval(deep_interval);

        // Skip the first immediate tick for both so we don't scrub on startup.
        quick_ticker.tick().await;
        deep_ticker.tick().await;

        tracing::info!(
            "Scrub scheduler started: quick every {}h, deep every {}h, max I/O {}%",
            self.config.quick_interval_hours,
            self.config.deep_interval_hours,
            self.config.max_io_percent,
        );

        loop {
            tokio::select! {
                _ = quick_ticker.tick() => {
                    self.run_scrub(ScrubLevel::Quick).await;
                }
                _ = deep_ticker.tick() => {
                    self.run_scrub(ScrubLevel::Deep).await;
                }
            }
        }
    }

    /// Execute a scrub of the given level on all disks.
    async fn run_scrub(&self, level: ScrubLevel) {
        let level_str = match level {
            ScrubLevel::Quick => "quick",
            ScrubLevel::Deep => "deep",
        };

        tracing::info!("Starting {} scrub on all disks", level_str);

        let disks = self.store.disk_paths();

        for disk_path in &disks {
            // Throttle I/O between disks based on max_io_percent.
            // A simple approach: if max_io_percent is 10%, sleep 90% of a
            // small time slice between disk scrubs.
            let io_percent = self.config.max_io_percent.max(1).min(100) as f64;
            let sleep_fraction = (100.0 - io_percent) / io_percent;
            let throttle_delay = tokio::time::Duration::from_millis(
                (100.0 * sleep_fraction) as u64,
            );

            match self.store.scrub_disk(disk_path, level).await {
                Ok(result) => {
                    tracing::info!(
                        "{} scrub on {}: checked={}, healthy={}, corrupt={}, missing={}, size_mismatch={}, duration={:.2}s",
                        level_str,
                        disk_path.display(),
                        result.chunks_checked,
                        result.chunks_healthy,
                        result.chunks_corrupt,
                        result.chunks_missing,
                        result.chunks_size_mismatch,
                        result.duration_secs,
                    );

                    if result.chunks_corrupt > 0 || result.chunks_missing > 0 {
                        tracing::warn!(
                            "Scrub found issues on {}: {} corrupt, {} missing",
                            disk_path.display(),
                            result.chunks_corrupt,
                            result.chunks_missing,
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(
                        "Scrub failed on {}: {}",
                        disk_path.display(),
                        e,
                    );
                }
            }

            // Throttle before next disk.
            tokio::time::sleep(throttle_delay).await;
        }

        tracing::info!("Completed {} scrub on all disks", level_str);
    }
}
