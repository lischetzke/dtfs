// Role-agnostic memory budget + proactive backpressure (Bug G core).
//
// Originally lived in dtfs-data (Bug G / Bug V). CF-5 hoisted the
// role-agnostic core here so dtfs-manager and dtfs-fuse can run the same
// RSS sampler + pressure ladder without a copy-paste second sampler. The
// data node keeps its ARC-cache-specific monitor (`run_monitor_with_cache`)
// in `dtfs-data/src/memory.rs`; manager + fuse poll `pressure_level()` and
// drive their own cheap ladder rung (skip scheduler ticks / shrink the
// readahead cache).
//
// Operators set a per-process RAM ceiling (e.g. `hard_cap_mb = 2048`). A
// background sampler reads `/proc/self/status` every few seconds and
// publishes a `PressureLevel` once RSS climbs past the configured
// thresholds; cooperative consumers poll it and give RAM back before the
// process is OOM-killed.
//
//  * `hard_cap_mb = 0` → unbounded (no backpressure, level pinned Normal).
//  * `warn_threshold_pct` → log + Warn level.
//  * `shed_threshold_pct` → Shed level + (data-node only) refuse put_chunk.
//
// The pure helpers have no I/O and are unit-tested. The async monitor + the
// RSS read live behind small wrappers so the static logic is verifiable
// without spinning up a process.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::MemoryConfig;

/// Bug V — current pressure level, derived from the latest RSS reading.
///
/// The strategy ladder maps levels to cooperative actions (Ceph OSD style)
/// so a role defends its memory budget without rejecting requests until the
/// very last rung:
///
/// | Level    | RSS                  | Data action            | Manager action  | FUSE action                |
/// |----------|----------------------|------------------------|-----------------|----------------------------|
/// | Normal   | < warn_threshold     | Full ARC cache         | Run ticks       | Full readahead budget      |
/// | Warn     | [warn, shed)         | Shrink ARC to 50%      | Skip ticks      | Half readahead budget      |
/// | Shed     | >= shed_threshold    | Shrink ARC to 10%      | Skip ticks      | Drop unconsumed readahead  |
///
/// Levels are stored as a `u8` in atomic memory so all observers see the
/// same snapshot without locking. The current ordering MUST stay stable
/// because `record_sample` casts via integer arithmetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PressureLevel {
    Normal = 0,
    Warn = 1,
    Shed = 2,
}

impl PressureLevel {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => PressureLevel::Normal,
            1 => PressureLevel::Warn,
            _ => PressureLevel::Shed,
        }
    }
}

/// Per-process memory budget. Cheap to clone (just `Arc`).
#[derive(Debug)]
pub struct MemoryBudget {
    hard_cap_bytes: u64,
    warn_threshold_bytes: u64,
    shed_threshold_bytes: u64,
    sample_interval: Duration,
    shedding: AtomicBool,
    last_rss_bytes: AtomicU64,
    warn_logged: AtomicBool,
    /// Bug V — current pressure level as u8; readers downcast via
    /// `PressureLevel::from_u8`. Atomic so the monitor publishes without
    /// holding a mutex and consumers (cache shrinker, scrub pauser) see
    /// the latest level without contention.
    level: AtomicU8,
}

impl MemoryBudget {
    /// Build a budget from a parsed config. `hard_cap_mb = 0` disables
    /// every check — `is_shedding()` will always return false and
    /// `recommend_chunk_write_slots` returns the caller's preferred
    /// default.
    pub fn from_config(cfg: &MemoryConfig) -> Self {
        let hard_cap_bytes = (cfg.hard_cap_mb as u64).saturating_mul(1024 * 1024);
        let warn_threshold_bytes = bytes_for_pct(hard_cap_bytes, cfg.warn_threshold_pct);
        let shed_threshold_bytes = bytes_for_pct(hard_cap_bytes, cfg.shed_threshold_pct);
        Self {
            hard_cap_bytes,
            warn_threshold_bytes,
            shed_threshold_bytes,
            sample_interval: Duration::from_secs(cfg.sample_interval_secs.max(1)),
            shedding: AtomicBool::new(false),
            last_rss_bytes: AtomicU64::new(0),
            warn_logged: AtomicBool::new(false),
            level: AtomicU8::new(PressureLevel::Normal as u8),
        }
    }

    /// Bug V — current pressure level. Cooperative consumers (ARC cache
    /// shrinker, scrub scheduler, prefetch dropper) poll this every few
    /// seconds and adjust their behavior. The shedding flag (`is_shedding`)
    /// remains the hard backstop that refuses new writes at the highest
    /// level.
    pub fn pressure_level(&self) -> PressureLevel {
        PressureLevel::from_u8(self.level.load(Ordering::Acquire))
    }

    /// `true` once RSS has climbed past `shed_threshold_pct`. Cleared
    /// when RSS falls back below the *warn* threshold (hysteresis — we
    /// don't flap on small fluctuations).
    pub fn is_shedding(&self) -> bool {
        self.shedding.load(Ordering::Acquire)
    }

    /// Recommend an initial concurrent-put-chunk slot count that fits
    /// under the configured budget. `chunk_size_bytes` is the manager's
    /// configured chunk size; `static_overhead_bytes` is whatever else
    /// is guaranteed to be live (ARC cache, tonic, raft, etc.).
    pub fn recommend_chunk_write_slots(
        &self,
        chunk_size_bytes: u64,
        static_overhead_bytes: u64,
        fallback: usize,
    ) -> usize {
        if self.hard_cap_bytes == 0 {
            return fallback;
        }
        recommend_chunk_write_slots(
            self.hard_cap_bytes,
            chunk_size_bytes,
            static_overhead_bytes,
            fallback,
        )
    }

    /// Most-recent observed RSS in bytes. 0 if the sampler hasn't run
    /// yet.
    pub fn last_rss_bytes(&self) -> u64 {
        self.last_rss_bytes.load(Ordering::Acquire)
    }

    /// The poll cadence (`sample_interval_secs`, floored at 1s). Consumers
    /// that drive their own monitor loop (manager scheduler, fuse cache
    /// shrinker) tick on this so their cadence matches the sampler.
    pub fn sample_interval(&self) -> Duration {
        self.sample_interval
    }

    /// Apply a freshly-sampled RSS reading. Sets the shedding flag if
    /// we've crossed the shed threshold; clears it once we've dropped
    /// back below the warn threshold (hysteresis).
    ///
    /// Bug V — also updates the pressure level so cooperative consumers
    /// (cache shrinker, scrub pauser) can poll and react before the
    /// hard reject threshold is reached.
    pub fn record_sample(&self, rss_bytes: u64) {
        self.last_rss_bytes.store(rss_bytes, Ordering::Release);
        if self.hard_cap_bytes == 0 {
            // Unbounded — every flag stays off and level pinned at Normal.
            self.shedding.store(false, Ordering::Release);
            self.level
                .store(PressureLevel::Normal as u8, Ordering::Release);
            return;
        }

        let prev_shedding = self.shedding.load(Ordering::Acquire);
        let new_shedding = if prev_shedding {
            // Hysteresis: stay in shedding until we drop below WARN.
            rss_bytes >= self.warn_threshold_bytes
        } else {
            rss_bytes >= self.shed_threshold_bytes
        };

        // Bug V — derive pressure level from the same hysteretic rules:
        //   * Once shedding, stay >=Warn until we drop below warn threshold.
        //   * Once >=warn but <shed, sit at Warn.
        //   * Else Normal.
        let new_level = if new_shedding {
            PressureLevel::Shed
        } else if rss_bytes >= self.warn_threshold_bytes {
            PressureLevel::Warn
        } else {
            PressureLevel::Normal
        };
        self.level.store(new_level as u8, Ordering::Release);

        if new_shedding != prev_shedding {
            self.shedding.store(new_shedding, Ordering::Release);
            if new_shedding {
                tracing::warn!(
                    rss_mb = rss_bytes / 1024 / 1024,
                    shed_threshold_mb = self.shed_threshold_bytes / 1024 / 1024,
                    cap_mb = self.hard_cap_bytes / 1024 / 1024,
                    "memory budget: SHEDDING (RSS past shed threshold)"
                );
            } else {
                tracing::info!(
                    rss_mb = rss_bytes / 1024 / 1024,
                    warn_threshold_mb = self.warn_threshold_bytes / 1024 / 1024,
                    "memory budget: shedding cleared (RSS back below warn threshold)"
                );
                // Reset warn-once latch so future warns fire again.
                self.warn_logged.store(false, Ordering::Release);
            }
            return;
        }

        // Not transitioning. If we're between WARN and SHED, log once.
        if !prev_shedding && rss_bytes >= self.warn_threshold_bytes {
            if !self.warn_logged.swap(true, Ordering::AcqRel) {
                tracing::warn!(
                    rss_mb = rss_bytes / 1024 / 1024,
                    warn_threshold_mb = self.warn_threshold_bytes / 1024 / 1024,
                    cap_mb = self.hard_cap_bytes / 1024 / 1024,
                    "memory budget: RSS past warn threshold — approaching shed point"
                );
            }
        } else if !prev_shedding && rss_bytes < self.warn_threshold_bytes {
            // Recovered all the way down; rearm the warn-once latch.
            self.warn_logged.store(false, Ordering::Release);
        }
    }

    /// Background loop that samples RSS every `sample_interval` and
    /// updates the shedding flag + pressure level. Intended to be
    /// `tokio::spawn`'d once at startup. Consumers poll `pressure_level()`
    /// from their own loops.
    pub async fn run_monitor(self: Arc<Self>) {
        // hard_cap_bytes == 0 → nothing to enforce, but still report
        // RSS for visibility.
        let mut tick = tokio::time::interval(self.sample_interval);
        loop {
            tick.tick().await;
            match read_proc_self_rss_bytes() {
                Ok(rss) => self.record_sample(rss),
                Err(e) => {
                    tracing::debug!(error = %e, "memory budget: could not read RSS");
                }
            }
        }
    }

    pub fn hard_cap_bytes(&self) -> u64 {
        self.hard_cap_bytes
    }
}

// ---------------------------------------------------------------------------
// Pure helpers — unit-tested separately from the async monitor.
// ---------------------------------------------------------------------------

/// `pct` of `total`, saturating at the total (so 200% caps at 100%).
pub fn bytes_for_pct(total: u64, pct: u8) -> u64 {
    let pct = pct.min(100) as u64;
    total.saturating_mul(pct) / 100
}

/// Recommend how many concurrent put_chunk slots to allow.
///
/// Reserves `static_overhead_bytes` for the ARC cache + tonic + raft +
/// kernel pages, then divides the remaining budget by `chunk_size_bytes`
/// to figure out how many in-flight 128 MB chunks we can hold.
///
/// Guaranteed `>= 1` (we need at least one slot to make progress) and
/// `<= 256` (an absurd ceiling — beyond this, OS scheduling overhead
/// dominates and we should be sharding across nodes anyway).
pub fn recommend_chunk_write_slots(
    hard_cap_bytes: u64,
    chunk_size_bytes: u64,
    static_overhead_bytes: u64,
    fallback: usize,
) -> usize {
    if hard_cap_bytes == 0 || chunk_size_bytes == 0 {
        return fallback.max(1);
    }
    if static_overhead_bytes >= hard_cap_bytes {
        // Caller's overhead alone exceeds the cap. Best we can do is 1
        // slot and pray for fast disk flushes.
        return 1;
    }
    let available = hard_cap_bytes - static_overhead_bytes;
    let slots = (available / chunk_size_bytes) as usize;
    slots.clamp(1, 256)
}

/// Read the current process's VmRSS in bytes by parsing
/// `/proc/self/status`. Linux-only; returns an error on other OSes.
#[cfg(target_os = "linux")]
pub fn read_proc_self_rss_bytes() -> std::io::Result<u64> {
    let content = std::fs::read_to_string("/proc/self/status")?;
    parse_proc_self_status_vmrss(&content).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "VmRSS not found")
    })
}

#[cfg(not(target_os = "linux"))]
pub fn read_proc_self_rss_bytes() -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "memory budget RSS sampling is Linux-only",
    ))
}

/// Parse the `VmRSS:` line out of `/proc/self/status`. Pure function
/// (no I/O), unit-tested with synthetic input. The kernel emits VmRSS
/// in kB.
pub fn parse_proc_self_status_vmrss(content: &str) -> Option<u64> {
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // `VmRSS:    132456 kB`
            let parts: Vec<&str> = rest.split_whitespace().collect();
            if parts.len() < 2 {
                return None;
            }
            let n: u64 = parts[0].parse().ok()?;
            let unit = parts[1].to_ascii_lowercase();
            let bytes = match unit.as_str() {
                "kb" => n.saturating_mul(1024),
                "mb" => n.saturating_mul(1024 * 1024),
                "b" => n,
                _ => return None,
            };
            return Some(bytes);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;
    const GB: u64 = 1024 * MB;

    /// Bug G — operators set a hard RAM cap; the recommended put_chunk
    /// slot count must fit under that cap with the configured overhead.
    /// 2 GB cap with 600 MB overhead and 128 MB chunks → 11 slots.
    #[test]
    fn bug_g_recommend_slots_fits_under_2gb_cap() {
        let chunk = 128 * MB;
        let overhead = 600 * MB;
        let slots = recommend_chunk_write_slots(2 * GB, chunk, overhead, 8);
        // (2 * 1024 - 600) MB available = 1448 MB; 1448 / 128 = 11.
        assert_eq!(slots, 11);
        // Sanity: slots * chunk + overhead <= hard_cap.
        let usage = (slots as u64) * chunk + overhead;
        assert!(
            usage <= 2 * GB,
            "recommended budget {} bytes exceeds cap {} bytes",
            usage,
            2 * GB
        );
    }

    /// 0 = unbounded — return the caller's fallback verbatim.
    #[test]
    fn bug_g_recommend_slots_zero_cap_returns_fallback() {
        let slots = recommend_chunk_write_slots(0, 128 * MB, 600 * MB, 8);
        assert_eq!(slots, 8);
    }

    /// Ridiculously small budget — return 1 (we must make progress).
    #[test]
    fn bug_g_recommend_slots_floor_is_one() {
        let slots = recommend_chunk_write_slots(64 * MB, 128 * MB, 0, 8);
        assert_eq!(slots, 1, "even with no slots fitting, we must return >=1");
    }

    /// Overhead alone exceeds cap → still return 1.
    #[test]
    fn bug_g_recommend_slots_overhead_exceeds_cap() {
        let slots = recommend_chunk_write_slots(1 * GB, 128 * MB, 2 * GB, 8);
        assert_eq!(slots, 1);
    }

    /// 256 ceiling so a misconfigured huge cap doesn't blow up tonic.
    #[test]
    fn bug_g_recommend_slots_ceiling_is_256() {
        let slots = recommend_chunk_write_slots(1024 * GB, 1 * MB, 0, 8);
        assert_eq!(slots, 256);
    }

    /// Threshold conversion: 80% of 2 GB ≈ 1.6 GB.
    #[test]
    fn bug_g_threshold_calculation() {
        assert_eq!(bytes_for_pct(2 * GB, 80), (2 * GB) * 80 / 100);
        // Saturation: pct > 100 clamps to 100.
        assert_eq!(bytes_for_pct(2 * GB, 200), 2 * GB);
    }

    /// Sampler integration: RSS crosses shed → flag goes high; drops
    /// back below WARN → flag clears (hysteresis prevents flapping).
    #[test]
    fn bug_g_shedding_flag_hysteresis() {
        let cfg = MemoryConfig {
            hard_cap_mb: 2048,
            warn_threshold_pct: 80,
            shed_threshold_pct: 92,
            sample_interval_secs: 5,
        };
        let budget = MemoryBudget::from_config(&cfg);

        // Far below WARN — flag stays low.
        budget.record_sample(500 * MB);
        assert!(!budget.is_shedding());

        // Between WARN and SHED — flag stays low (just a warning).
        budget.record_sample(1700 * MB); // ~83% of 2 GB
        assert!(!budget.is_shedding());

        // Above SHED — flag goes high.
        budget.record_sample(1900 * MB); // ~93% of 2 GB
        assert!(budget.is_shedding());

        // Drops a little, but still above WARN — stays high (hysteresis).
        budget.record_sample(1700 * MB);
        assert!(
            budget.is_shedding(),
            "hysteresis: must stay shedding until back below warn"
        );

        // Drops below WARN — flag clears.
        budget.record_sample(1500 * MB); // ~73% of 2 GB
        assert!(!budget.is_shedding());
    }

    /// `hard_cap = 0` → shedding flag is always off.
    #[test]
    fn bug_g_unbounded_never_sheds() {
        let cfg = MemoryConfig {
            hard_cap_mb: 0,
            warn_threshold_pct: 80,
            shed_threshold_pct: 92,
            sample_interval_secs: 5,
        };
        let budget = MemoryBudget::from_config(&cfg);
        budget.record_sample(1_000_000 * MB);
        assert!(!budget.is_shedding());
    }

    /// VmRSS parser — happy path + a couple of edge cases.
    #[test]
    fn bug_g_parse_proc_self_status_vmrss() {
        let sample = "\
Name:\tdtfs-data
Pid:\t1234
VmPeak:\t  500000 kB
VmSize:\t  450000 kB
VmHWM:\t   150000 kB
VmRSS:\t   132456 kB
VmData:\t  300000 kB
";
        let bytes = parse_proc_self_status_vmrss(sample).expect("VmRSS present");
        assert_eq!(bytes, 132_456_u64 * 1024);
    }

    #[test]
    fn bug_g_parse_proc_self_status_vmrss_missing() {
        let sample = "Name:\tdtfs-data\nPid:\t1234\n";
        assert!(parse_proc_self_status_vmrss(sample).is_none());
    }

    #[test]
    fn bug_g_parse_proc_self_status_vmrss_unknown_unit() {
        // Hypothetical kernel that reports in some odd unit — return
        // None rather than crash.
        let sample = "VmRSS:\t  100 GiB\n";
        assert!(parse_proc_self_status_vmrss(sample).is_none());
    }

    /// Bug V regression — pressure_level walks Normal → Warn → Shed as
    /// RSS rises and back down with hysteresis. The cooperative consumers
    /// (cache shrinker, scrub pauser, scheduler throttle) rely on this
    /// monotonic state machine.
    #[test]
    fn bug_v_pressure_level_tracks_thresholds() {
        let cfg = MemoryConfig {
            hard_cap_mb: 2048,
            warn_threshold_pct: 80,
            shed_threshold_pct: 92,
            sample_interval_secs: 5,
        };
        let budget = MemoryBudget::from_config(&cfg);

        // Far below warn → Normal.
        budget.record_sample(500 * MB);
        assert_eq!(budget.pressure_level(), PressureLevel::Normal);

        // Between warn (80% = 1638 MB) and shed (92% = 1884 MB) → Warn.
        budget.record_sample(1700 * MB);
        assert_eq!(budget.pressure_level(), PressureLevel::Warn);

        // Above shed → Shed.
        budget.record_sample(1900 * MB);
        assert_eq!(budget.pressure_level(), PressureLevel::Shed);

        // Drop a bit but still above warn → stays at Shed via hysteresis
        // (matching is_shedding's clearing rule).
        budget.record_sample(1700 * MB);
        assert_eq!(budget.pressure_level(), PressureLevel::Shed);

        // Drop below warn → Normal (since shedding clears here too).
        budget.record_sample(1500 * MB);
        assert_eq!(budget.pressure_level(), PressureLevel::Normal);
    }

    /// Bug V regression — `hard_cap = 0` pins the level at Normal so
    /// unbounded-mode systems never spuriously shrink the cache.
    #[test]
    fn bug_v_pressure_level_unbounded_stays_normal() {
        let cfg = MemoryConfig {
            hard_cap_mb: 0,
            warn_threshold_pct: 80,
            shed_threshold_pct: 92,
            sample_interval_secs: 5,
        };
        let budget = MemoryBudget::from_config(&cfg);
        budget.record_sample(1_000_000 * MB);
        assert_eq!(budget.pressure_level(), PressureLevel::Normal);
    }
}
