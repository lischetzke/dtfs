// Quick + deep scrub engine.
// Periodic integrity checking with configurable intervals and manual triggers.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dtfs_common::config::ScrubConfig;
use dtfs_common::types::ScrubLevel;
use tokio::sync::RwLock;

use crate::store::ChunkStore;

/// AZ-1 — decision-tick cadence for the scrub loop. The loop wakes
/// every `DECISION_TICK_SECS` and consults the live `ScrubConfig` to
/// decide whether the (now - last_run) gap has reached the configured
/// interval. Replaces the legacy fixed-interval ticker so a config
/// hot-reload (e.g. operator changes the cluster-wide scrub interval)
/// takes effect within `DECISION_TICK_SECS`, not at next restart.
const DECISION_TICK_SECS: u64 = 60;

/// Sleep owed after `elapsed` of scrub work so scrub uses about
/// `max_io_percent` of the disk: `elapsed * (100 - p) / p`, p clamped to
/// 1..=100 (100 and above -> no sleep).
pub fn throttle_delay(elapsed: std::time::Duration, max_io_percent: u8) -> std::time::Duration {
    let p = u32::from(max_io_percent).clamp(1, 100);
    elapsed * (100 - p) / p
}

#[cfg(test)]
mod throttle_tests {
    use super::throttle_delay;
    use std::time::Duration;

    #[test]
    fn full_speed_never_sleeps() {
        assert_eq!(throttle_delay(Duration::from_millis(50), 100), Duration::ZERO);
        assert_eq!(throttle_delay(Duration::from_millis(50), 200), Duration::ZERO);
    }

    #[test]
    fn ten_percent_sleeps_nine_times_work() {
        assert_eq!(throttle_delay(Duration::from_millis(100), 10), Duration::from_millis(900));
    }

    #[test]
    fn zero_clamps_to_one_percent() {
        assert_eq!(throttle_delay(Duration::from_millis(10), 0), Duration::from_millis(990));
    }

    #[test]
    fn fifty_percent_sleeps_equal_work() {
        assert_eq!(throttle_delay(Duration::from_millis(40), 50), Duration::from_millis(40));
    }
}

/// CF-8 — staleness window for manual scrub requests. The manager
/// re-sends the latest `ScrubRequestNow` on every heartbeat forever
/// (it is replaced, never cleared); per-process id dedupe handles the
/// steady state, and this window stops a *restarted* data node from
/// surprise-running a scrub the operator triggered long ago.
pub const MANUAL_SCRUB_MAX_AGE_SECS: u64 = 3600;

/// CF-8 — should this manual scrub request run now?
///
/// `true` iff the id is non-empty, differs from the last id this
/// process handled, and the request is younger than `max_age_secs`.
pub fn manual_scrub_due(
    req_id: &str,
    requested_at_ms: u64,
    last_handled_id: Option<&str>,
    now_ms: u64,
    max_age_secs: u64,
) -> bool {
    if req_id.is_empty() || last_handled_id == Some(req_id) {
        return false;
    }
    now_ms.saturating_sub(requested_at_ms) <= max_age_secs.saturating_mul(1000)
}

/// CR-6-3 (PROTO-2) — prefer an exact-seconds cadence over the legacy
/// hours field: `secs` is non-zero only when the manager's heartbeat
/// carried the CR-6-3 `ScrubConfigOverride.quick_interval_secs` /
/// `deep_interval_secs` fields (a manager on this build or newer); an
/// older manager (or before the first heartbeat) leaves `secs` at `0`,
/// falling back to the pre-existing `hours * 3600` (ceil-to-1h)
/// behaviour.
pub fn effective_interval_secs(secs: u64, hours: u64) -> u64 {
    if secs > 0 {
        secs
    } else {
        hours.saturating_mul(3600)
    }
}

fn level_name(level: ScrubLevel) -> &'static str {
    match level {
        ScrubLevel::Quick => "quick",
        ScrubLevel::Deep => "deep",
    }
}

fn marker_path(disk: &Path, level: ScrubLevel) -> PathBuf {
    disk.join(".dtfs").join(format!("last_{}_scrub_ms", level_name(level)))
}

fn read_marker(disk: &Path, level: ScrubLevel) -> Option<u64> {
    std::fs::read_to_string(marker_path(disk, level)).ok()?.trim().parse().ok()
}

/// Persist `ms` as the last `level` scrub time of `disk` (tmp, sync_all, rename).
fn write_marker(disk: &Path, level: ScrubLevel, ms: u64) -> std::io::Result<()> {
    let path = marker_path(disk, level);
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(ms.to_string().as_bytes())?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, &path)
}

/// Write the `level` marker on every disk; a failure is logged, the pass still counts.
fn write_markers(disks: &[PathBuf], level: ScrubLevel, ms: u64) {
    for d in disks {
        if let Err(e) = write_marker(d, level, ms) {
            tracing::warn!(disk = %d.display(), level = level_name(level), error = %e, "scrub marker write failed");
        }
    }
}

/// #117 — the wall-clock ms the `level` interval counts from, persisted
/// across restarts: the oldest marker over `disks`. A disk without a marker
/// gets `now_ms` written as its baseline, so a node restarted more often
/// than the interval still reaches it (and a fresh install does not scrub
/// at once).
pub fn load_last_scrub_ms(disks: &[PathBuf], level: ScrubLevel, now_ms: u64) -> u64 {
    let mut oldest = now_ms;
    let mut missing = Vec::new();
    for d in disks {
        match read_marker(d, level) {
            Some(ms) => oldest = oldest.min(ms),
            None => missing.push(d.clone()),
        }
    }
    write_markers(&missing, level, now_ms);
    oldest
}

/// Is a scrub due `interval_secs` after `last_ms`? A clock stepped back
/// before `last_ms` waits (saturating), it never scrubs early.
pub fn scrub_due(now_ms: u64, last_ms: u64, interval_secs: u64) -> bool {
    now_ms.saturating_sub(last_ms) >= interval_secs.saturating_mul(1000)
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

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

    /// Run the background scrub loop. Wakes every `DECISION_TICK_SECS`,
    /// reads the live `ScrubConfig`, and runs a quick / deep scrub
    /// when the configured interval has elapsed since the last run.
    /// CF-8: also selects on `scrub_now` — a manual trigger relayed by
    /// the manager via heartbeat — and runs that level immediately
    /// (which resets the level's interval timer, like any other run).
    pub async fn run(&self, mut scrub_now: tokio::sync::mpsc::Receiver<ScrubLevel>) {
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
        // #117 — interval timers are persisted per disk, so a restart does
        // not push the next scrub out by a full interval.
        let disks = self.store.disk_paths();
        let mut last_quick = load_last_scrub_ms(&disks, ScrubLevel::Quick, wall_ms());
        let mut last_deep = load_last_scrub_ms(&disks, ScrubLevel::Deep, wall_ms());

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let (quick_secs, deep_secs) = {
                        let c = self.config.read().await;
                        (effective_interval_secs(c.quick_interval_secs, c.quick_interval_hours),
                         effective_interval_secs(c.deep_interval_secs, c.deep_interval_hours))
                    };
                    if scrub_due(wall_ms(), last_quick, quick_secs) {
                        self.run_scrub(ScrubLevel::Quick).await;
                        last_quick = wall_ms();
                        write_markers(&disks, ScrubLevel::Quick, last_quick);
                    }
                    if scrub_due(wall_ms(), last_deep, deep_secs) {
                        self.run_scrub(ScrubLevel::Deep).await;
                        last_deep = wall_ms();
                        write_markers(&disks, ScrubLevel::Deep, last_deep);
                    }
                }
                Some(level) = scrub_now.recv() => {
                    // CF-8 — operator-triggered run; counts as a regular
                    // run for the interval timer of that level.
                    tracing::info!("CF-8 manual {:?} scrub starting (operator trigger)", level);
                    self.run_scrub(level).await;
                    let ms = wall_ms();
                    match level {
                        ScrubLevel::Quick => last_quick = ms,
                        ScrubLevel::Deep => last_deep = ms,
                    }
                    write_markers(&disks, level, ms);
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
            // max_io_percent throttling happens per chunk inside scrub_disk_throttled.
            let io_percent = self.config.read().await.max_io_percent.min(100) as u8;

            match self.store.scrub_disk_throttled(disk_path, level, io_percent).await {
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

                    // Bug BY — queue the scrub RESULT summary so the next
                    // heartbeat ships it to the manager (the WebUI "Last
                    // Scrub" card + stats.scrub_results_by_node consume it).
                    let completed_at_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    self.store.enqueue_scrub_result(crate::store::ScrubResultSummary {
                        disk_path: disk_path.display().to_string(),
                        level: level_str.to_string(),
                        completed_at_ms,
                        chunks_checked: result.chunks_checked,
                        chunks_healthy: result.chunks_healthy,
                        chunks_corrupt: result.chunks_corrupt,
                        chunks_missing: result.chunks_missing,
                        duration_secs: result.duration_secs,
                    });

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
                        // Quarantine + enqueue already happened inline in
                        // scrub_disk, under each chunk's lock.
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

        }

        tracing::info!("Completed {} scrub on all disks", level_str);
    }
}

#[cfg(test)]
mod cf8_manual_scrub_tests {
    use super::manual_scrub_due;

    const HOUR_MS: u64 = 3600 * 1000;

    #[test]
    fn fresh_new_id_runs() {
        assert!(manual_scrub_due("t1", 1_000, None, 2_000, 3600));
        // a different id than the last handled one also runs
        assert!(manual_scrub_due("t2", 1_000, Some("t1"), 2_000, 3600));
    }

    #[test]
    fn same_id_dedupes() {
        // the manager re-sends the request on every heartbeat — only the
        // first delivery may run
        assert!(!manual_scrub_due("t1", 1_000, Some("t1"), 2_000, 3600));
    }

    #[test]
    fn stale_request_ignored() {
        // restarted data node sees a request from >1h ago: skip
        let now = 10 * HOUR_MS;
        assert!(!manual_scrub_due("t1", now - HOUR_MS - 1, None, now, 3600));
        // boundary: exactly max_age old still runs
        assert!(manual_scrub_due("t1", now - HOUR_MS, None, now, 3600));
    }

    #[test]
    fn empty_id_never_runs() {
        assert!(!manual_scrub_due("", 1_000, None, 2_000, 3600));
    }
}

#[cfg(test)]
mod cr_6_3_effective_interval_secs_tests {
    use super::effective_interval_secs;

    #[test]
    fn nonzero_secs_wins_over_hours() {
        // manager sent quick_interval_secs = 600 (10 min) — must NOT be
        // ceiled to 1h via the legacy hours field.
        assert_eq!(effective_interval_secs(600, 6), 600);
    }

    #[test]
    fn zero_secs_falls_back_to_hours() {
        // older manager / no override yet: secs is 0, derive from hours.
        assert_eq!(effective_interval_secs(0, 6), 6 * 3600);
    }
}

#[cfg(test)]
mod persisted_due_tests {
    use super::*;

    const H: u64 = 3600 * 1000;
    const WEEK_SECS: u64 = 168 * 3600;

    #[test]
    fn fresh_disk_baselines_now_and_is_not_due() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join(".dtfs")).unwrap();
        let disks = vec![d.path().to_path_buf()];
        let now = 1_000 * H;
        let last = load_last_scrub_ms(&disks, ScrubLevel::Deep, now);
        assert_eq!(last, now);
        assert!(!scrub_due(now, last, WEEK_SECS));
        assert_eq!(read_marker(d.path(), ScrubLevel::Deep), Some(now), "baseline persisted");
    }

    #[test]
    fn restart_long_after_last_pass_is_due() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join(".dtfs")).unwrap();
        let disks = vec![d.path().to_path_buf()];
        write_marker(d.path(), ScrubLevel::Deep, 1_000 * H).unwrap();
        // Restarted 200 h later: the persisted time counts, not process start.
        let now = 1_200 * H;
        let last = load_last_scrub_ms(&disks, ScrubLevel::Deep, now);
        assert_eq!(last, 1_000 * H);
        assert!(scrub_due(now, last, WEEK_SECS));
    }

    #[test]
    fn restart_shortly_after_pass_is_not_due_and_boundary_holds() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join(".dtfs")).unwrap();
        let disks = vec![d.path().to_path_buf()];
        write_marker(d.path(), ScrubLevel::Deep, 1_000 * H).unwrap();
        let last = load_last_scrub_ms(&disks, ScrubLevel::Deep, 1_001 * H);
        assert_eq!(last, 1_000 * H);
        assert!(!scrub_due(1_000 * H + WEEK_SECS * 1000 - 1, last, WEEK_SECS));
        assert!(scrub_due(1_000 * H + WEEK_SECS * 1000, last, WEEK_SECS));
        // Clock stepped back before the marker: wait, never scrub early.
        assert!(!scrub_due(999 * H, last, WEEK_SECS));
    }

    #[test]
    fn oldest_marker_wins_and_levels_are_separate() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        for d in [&a, &b] {
            std::fs::create_dir_all(d.path().join(".dtfs")).unwrap();
        }
        write_marker(a.path(), ScrubLevel::Deep, 900 * H).unwrap();
        write_marker(b.path(), ScrubLevel::Deep, 950 * H).unwrap();
        let disks = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        assert_eq!(load_last_scrub_ms(&disks, ScrubLevel::Deep, 1_000 * H), 900 * H);
        assert_eq!(load_last_scrub_ms(&disks, ScrubLevel::Quick, 1_000 * H), 1_000 * H);
    }
}
