// Data-node memory backpressure (Bug G / Bug V).
//
// The role-agnostic core — `MemoryBudget`, `PressureLevel`, RSS sampling,
// the pure slot-recommendation + threshold helpers, and `run_monitor` —
// moved to `dtfs_common::memory` in CF-5 so the manager and fuse roles
// share one sampler instead of a copy-paste. This module re-exports those
// items (so the rest of dtfs-data keeps using `crate::memory::*`) and adds
// the one data-specific rung the shared core can't own: the ARC-cache
// shrinker monitor.

use std::sync::Arc;

pub use dtfs_common::memory::{
    bytes_for_pct, parse_proc_self_status_vmrss, read_proc_self_rss_bytes,
    recommend_chunk_write_slots, MemoryBudget, PressureLevel,
};

/// Bug V — variant of `MemoryBudget::run_monitor` that also drives the
/// cooperative ARC-cache shrinker. At each sample tick we look at the
/// pressure level and shrink the ARC cache to a target size before any
/// hard rejection kicks in. This is the "best-effort, no-reject" behavior
/// the user asked for — matches Ceph OSD's autotuner shape.
///
/// `cache_max_entries` is the operator-configured maximum (whatever was
/// passed to `ArcCache::new`); we never grow beyond it. Lives here (not on
/// `MemoryBudget`) because it depends on dtfs-data's `ArcCache`.
pub async fn run_monitor_with_cache(
    budget: Arc<MemoryBudget>,
    cache: Arc<crate::cache::ArcCache>,
    cache_max_entries: usize,
) {
    let mut tick = tokio::time::interval(budget.sample_interval());
    let mut last_level = PressureLevel::Normal;
    loop {
        tick.tick().await;
        if let Ok(rss) = read_proc_self_rss_bytes() {
            budget.record_sample(rss);
        }
        let level = budget.pressure_level();
        // Only act on level changes (or every tick if Shed) to avoid
        // hammering the cache mutex on a stable system.
        if level != last_level || level == PressureLevel::Shed {
            let target = match level {
                PressureLevel::Normal => cache_max_entries,
                PressureLevel::Warn => cache_max_entries / 2,
                PressureLevel::Shed => cache_max_entries / 10,
            };
            let evicted = cache.shrink_to(target);
            if evicted > 0 {
                tracing::info!(
                    level = ?level,
                    evicted,
                    target,
                    cache_len = cache.len(),
                    "Bug V cooperative cache shrink under memory pressure"
                );
            }
            last_level = level;
        }
    }
}
