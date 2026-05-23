// Quick + deep scrub engine.
// Periodic integrity checking with configurable intervals and manual triggers.

use std::sync::Arc;

use dtfs_common::config::ScrubConfig;
use dtfs_common::types::ScrubLevel;
use tokio::sync::RwLock;

use crate::store::ChunkStore;

/// AZ-1 — decision-tick cadence for the scrub loop. The loop wakes
/// every `DECISION_TICK_SECS` and consults the live `ScrubConfig` to
/// decide whether the (now - last_run) gap has reached the configured
/// interval. Replaces the legacy fixed-interval ticker so a config
/// hot-reload (e.g. operator drops a volume's scrub_interval_secs)
/// takes effect within `DECISION_TICK_SECS`, not at next restart.
const DECISION_TICK_SECS: u64 = 60;

/// Background scrub scheduler that runs quick and deep scrubs on configured
/// intervals against all disks managed by the chunk store. AZ-1: the
/// `config` field is an `Arc<RwLock<ScrubConfig>>` so the manager's
/// heartbeat-driven config-update path can lower the effective interval
/// at runtime (e.g. when an operator raises a per-volume scrub cadence).
pub struct ScrubScheduler {
    store: Arc<ChunkStore>,
    config: Arc<RwLock<ScrubConfig>>,
}

impl ScrubScheduler {
    /// Create a new scrub scheduler with a hot-reloadable config handle.
    pub fn new(store: Arc<ChunkStore>, config: Arc<RwLock<ScrubConfig>>) -> Self {
        Self { store, config }
    }

    /// Construct a scheduler from an owned config (test / legacy callers).
    /// Wraps the value in `Arc<RwLock<...>>` so the loop's hot-reload
    /// semantics still hold.
    pub fn new_owned(store: Arc<ChunkStore>, config: ScrubConfig) -> Self {
        Self { store, config: Arc::new(RwLock::new(config)) }
    }

    /// Run the background scrub loop. Wakes every `DECISION_TICK_SECS`,
    /// reads the live `ScrubConfig`, and runs a quick / deep scrub
    /// when the configured interval has elapsed since the last run.
    pub async fn run(&self) {
        let (q_h, d_h, io_pct) = {
            let c = self.config.read().await;
            (c.quick_interval_hours, c.deep_interval_hours, c.max_io_percent)
        };
        tracing::info!(
            "Scrub scheduler started: quick every {}h, deep every {}h, max I/O {}% (live-reloadable)",
            q_h,
            d_h,
            io_pct,
        );

        let mut ticker = tokio::time::interval(
            tokio::time::Duration::from_secs(DECISION_TICK_SECS),
        );
        // Skip the first immediate tick so a fresh process doesn't
        // scrub on startup.
        ticker.tick().await;
        let start = std::time::Instant::now();
        let mut last_quick: Option<std::time::Instant> = None;
        let mut last_deep: Option<std::time::Instant> = None;

        loop {
            ticker.tick().await;
            let (quick_secs, deep_secs) = {
                let c = self.config.read().await;
                (
                    c.quick_interval_hours.saturating_mul(3600),
                    c.deep_interval_hours.saturating_mul(3600),
                )
            };
            let now = std::time::Instant::now();
            let quick_due = match last_quick {
                Some(t) => now.duration_since(t).as_secs() >= quick_secs,
                None => now.duration_since(start).as_secs() >= quick_secs,
            };
            let deep_due = match last_deep {
                Some(t) => now.duration_since(t).as_secs() >= deep_secs,
                None => now.duration_since(start).as_secs() >= deep_secs,
            };
            if quick_due {
                self.run_scrub(ScrubLevel::Quick).await;
                last_quick = Some(std::time::Instant::now());
            }
            if deep_due {
                self.run_scrub(ScrubLevel::Deep).await;
                last_deep = Some(std::time::Instant::now());
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
            let io_percent = self.config.read().await.max_io_percent.max(1).min(100) as f64;
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

                    // Bug O — quarantine the offending local chunks
                    // and enqueue them so the next heartbeat can tell
                    // the manager. The manager's Raft state machine
                    // drops these (node_id, chunk_hash) entries, the
                    // scheduler's check_under_replicated then queues
                    // a re-replicate from a healthy peer.
                    if !result.corrupt_chunk_ids.is_empty() {
                        tracing::warn!(
                            disk = %disk_path.display(),
                            count = result.corrupt_chunk_ids.len(),
                            "Bug O scrub quarantining locally-corrupt chunks + enqueuing for manager report"
                        );
                        for cid in &result.corrupt_chunk_ids {
                            if let Err(e) =
                                crate::store::quarantine_chunk_on_disk(disk_path, cid).await
                            {
                                tracing::warn!(
                                    chunk_id = %cid,
                                    error = %e,
                                    "Bug O quarantine failed (non-fatal; entry still reported)"
                                );
                            }
                        }
                        self.store
                            .enqueue_pending_corruption(result.corrupt_chunk_ids.iter().cloned());
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
