//! Hot/cold data classification and migration decisions.
//!
//! The `TieringEngine` periodically evaluates chunk access patterns and
//! classifies data as hot, warm, cold, or frozen. It then schedules tier
//! migration tasks via the [`TaskScheduler`] to move chunks between tiers.
//!
//! Location-aware migration: data is also moved toward DCs where it is
//! actively accessed, ensuring low-latency reads for hot/warm data.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dtfs_common::config::TieringConfig;
use dtfs_common::types::{DataTemperature, DiskTier};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::DtfsRaft;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::audit::AuditLog;
use crate::scheduler::TaskScheduler;

/// Per-chunk access profile used for tiering decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkAccessProfile {
    pub chunk_id: String,
    pub file_path: String,
    pub total_reads_1h: u32,
    pub total_reads_24h: u32,
    /// Read counts broken down by datacenter name.
    pub reads_by_dc: HashMap<String, u32>,
    pub last_access_ms: u64,
    pub current_temperature: DataTemperature,
    /// Which tier the chunk currently lives on.
    pub current_tier: DiskTier,
    /// Which node currently hosts this chunk.
    pub current_node: u64,
}

/// Tiering engine that evaluates access patterns and triggers tier migrations.
pub struct TieringEngine {
    config: RwLock<TieringConfig>,
    scheduler: Arc<TaskScheduler>,
    audit: Arc<AuditLog>,
    state_data: Arc<RwLock<StateMachineData>>,
    /// Per-chunk access statistics, reported by data nodes via heartbeats.
    access_stats: RwLock<HashMap<String, ChunkAccessProfile>>,
    /// P1-8: optional alert dispatcher. Anchor refusals fire here.
    alerts: Option<Arc<crate::alerts::AlertDispatcher>>,
}

impl TieringEngine {
    pub fn new(
        config: TieringConfig,
        scheduler: Arc<TaskScheduler>,
        audit: Arc<AuditLog>,
        state_data: Arc<RwLock<StateMachineData>>,
    ) -> Self {
        Self {
            config: RwLock::new(config),
            scheduler,
            audit,
            state_data,
            access_stats: RwLock::new(HashMap::new()),
            alerts: None,
        }
    }

    /// Inject the alert dispatcher after construction.
    pub fn with_alerts(mut self, alerts: Arc<crate::alerts::AlertDispatcher>) -> Self {
        self.alerts = Some(alerts);
        self
    }

    /// Run the tiering evaluation loop. Spawned as a background task.
    pub async fn run(&self) {
        let interval_secs = {
            let cfg = self.config.read().await;
            cfg.evaluation_interval_secs
        };

        info!(
            "Tiering engine started (evaluation interval: {}s)",
            interval_secs
        );

        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;

            let cfg = self.config.read().await;
            if !cfg.enabled {
                debug!("Tiering disabled, skipping evaluation");
                continue;
            }
            drop(cfg);

            if let Err(e) = self.evaluate().await {
                warn!("Tiering evaluation failed: {}", e);
            }
        }
    }

    /// Update access stats for a chunk (called from heartbeat processing).
    pub async fn report_access(
        &self,
        chunk_id: &str,
        file_path: &str,
        reads_1h: u32,
        reads_24h: u32,
        reads_by_dc: HashMap<String, u32>,
        current_tier: DiskTier,
        current_node: u64,
    ) {
        let now_ms = now_millis();
        let mut stats = self.access_stats.write().await;

        let profile = stats.entry(chunk_id.to_string()).or_insert_with(|| {
            ChunkAccessProfile {
                chunk_id: chunk_id.to_string(),
                file_path: file_path.to_string(),
                total_reads_1h: 0,
                total_reads_24h: 0,
                reads_by_dc: HashMap::new(),
                last_access_ms: 0,
                current_temperature: DataTemperature::Cold,
                current_tier,
                current_node,
            }
        });

        profile.total_reads_1h = reads_1h;
        profile.total_reads_24h = reads_24h;
        profile.reads_by_dc = reads_by_dc;
        if reads_1h > 0 || reads_24h > 0 {
            profile.last_access_ms = now_ms;
        }
        profile.current_tier = current_tier;
        profile.current_node = current_node;
    }

    /// Evaluate all tracked chunks and classify their temperature.
    /// Schedules tier migration tasks for chunks whose temperature doesn't
    /// match their current tier.
    async fn evaluate(&self) -> anyhow::Result<()> {
        let cfg = self.config.read().await;
        let mut stats = self.access_stats.write().await;
        let now_ms = now_millis();

        let mut tier_up = 0u32;
        let mut tier_down = 0u32;

        for (_chunk_id, profile) in stats.iter_mut() {
            let old_temp = profile.current_temperature;

            // Classify temperature based on access patterns.
            let new_temp = classify_temperature(
                profile.total_reads_1h,
                profile.total_reads_24h,
                profile.last_access_ms,
                now_ms,
                cfg.hot_threshold_reads_per_hour,
                cfg.cold_threshold_days,
                cfg.frozen_threshold_days,
            );

            profile.current_temperature = new_temp;

            if old_temp == new_temp {
                continue;
            }

            let target_tier = temperature_to_tier(new_temp);

            if target_tier == profile.current_tier {
                continue;
            }

            // Need to migrate this chunk to a different tier.
            let direction = if tier_rank(target_tier) > tier_rank(profile.current_tier) {
                tier_down += 1;
                "demote"
            } else {
                tier_up += 1;
                "promote"
            };

            info!(
                "Tiering {}: chunk {} ({:?} -> {:?}, tier {:?} -> {:?})",
                direction,
                profile.chunk_id,
                old_temp,
                new_temp,
                profile.current_tier,
                target_tier,
            );

            // Find a target node with the right tier.
            let target_node = self
                .find_target_node(target_tier, profile.current_node, &profile.reads_by_dc)
                .await;

            if let Some(target) = target_node {
                // P0-10: anchor-protect. If the volume has an anchor DC set,
                // refuse migrations that would leave 0 replicas in that DC.
                let anchor_violated = self
                    .would_violate_anchor(profile, target)
                    .await;
                if anchor_violated {
                    self.audit.log(
                        "tiering",
                        "tier.anchor_protected",
                        &format!(
                            "chunk={} path={} refused {:?}->{:?}",
                            profile.chunk_id, profile.file_path,
                            profile.current_tier, target_tier
                        ),
                        "refused",
                    );
                    if let Some(alerts) = &self.alerts {
                        alerts.fire(
                            crate::alerts::AlertKind::AnchorViolated,
                            "tier migration refused — would strand anchor DC",
                            serde_json::json!({
                                "chunk_id": profile.chunk_id,
                                "file_path": profile.file_path,
                                "source_tier": format!("{:?}", profile.current_tier),
                                "target_tier": format!("{:?}", target_tier),
                                "target_node": target,
                            }),
                        );
                    }
                    continue;
                }

                self.scheduler
                    .schedule_replication(
                        &profile.chunk_id,
                        profile.current_node,
                        target,
                        target_tier,
                    )
                    .await;

                self.audit.log(
                    "tiering",
                    &format!("tier.{direction}"),
                    &format!(
                        "chunk={} {:?}->{:?}",
                        profile.chunk_id, profile.current_tier, target_tier
                    ),
                    "scheduled",
                );
            } else {
                warn!(
                    "No suitable target node found for tier {:?} migration of chunk {}",
                    target_tier, profile.chunk_id
                );
            }
        }

        if tier_up > 0 || tier_down > 0 {
            info!(
                "Tiering evaluation complete: {} promotions, {} demotions",
                tier_up, tier_down
            );
        }

        Ok(())
    }

    /// P0-10: return `true` if moving `profile`'s chunk to `target_node`
    /// would leave the volume's anchor DC with zero replicas of this chunk.
    ///
    /// Logic:
    /// 1. Look up the file's `volume_id` from `sm.files[profile.file_path]`.
    /// 2. Look up the volume's `anchor_datacenter`.
    /// 3. If anchor is None → never violated (no constraint).
    /// 4. Count current replicas of `profile.chunk_id` whose node lives in
    ///    the anchor DC. Subtract 1 if the source node is in anchor DC
    ///    (it's the one being moved). Add 1 if the target node is in anchor.
    /// 5. If the resulting count would be 0 → violation.
    async fn would_violate_anchor(
        &self,
        profile: &ChunkAccessProfile,
        target_node: u64,
    ) -> bool {
        let sm = self.state_data.read().await;
        let volume_id = match sm.files.get(&profile.file_path) {
            Some(meta) => meta.volume_id.clone(),
            None => return false,
        };
        let anchor = match sm.volumes.get(&volume_id).and_then(|v| v.anchor_datacenter.clone()) {
            Some(a) if !a.is_empty() => a,
            _ => return false,
        };
        let file_meta = match sm.files.get(&profile.file_path) {
            Some(m) => m,
            None => return false,
        };

        // Build a quick lookup: grpc_addr → datacenter.
        let mut grpc_to_dc: HashMap<String, String> = HashMap::new();
        let mut id_to_dc: HashMap<u64, String> = HashMap::new();
        for (id, info) in sm.nodes.iter() {
            grpc_to_dc.insert(info.grpc_addr.clone(), info.location.datacenter.clone());
            id_to_dc.insert(*id, info.location.datacenter.clone());
        }

        // Count anchor-DC replicas of this chunk.
        let mut anchor_count: i64 = 0;
        for ent in file_meta.chunks.iter() {
            if ent.chunk_hash != profile.chunk_id {
                continue;
            }
            // node_id is sometimes a u64 string, sometimes a "http://..." URL.
            // Try both lookups.
            let dc = if let Some(d) = grpc_to_dc.get(&ent.node_id) {
                Some(d.clone())
            } else if let Ok(num) = ent.node_id.parse::<u64>() {
                id_to_dc.get(&num).cloned()
            } else {
                None
            };
            if dc.as_deref() == Some(anchor.as_str()) {
                anchor_count += 1;
            }
        }

        // Adjust for the move.
        let source_dc = id_to_dc.get(&profile.current_node).cloned();
        let target_dc = id_to_dc.get(&target_node).cloned();
        if source_dc.as_deref() == Some(anchor.as_str()) {
            anchor_count -= 1;
        }
        if target_dc.as_deref() == Some(anchor.as_str()) {
            anchor_count += 1;
        }

        anchor_count <= 0
    }

    /// Find a target node that has disks of the specified tier,
    /// excluding the given source node.
    async fn find_target_node(
        &self,
        target_tier: DiskTier,
        exclude_node: u64,
        reads_by_dc: &HashMap<String, u32>,
    ) -> Option<u64> {
        let sm = self.state_data.read().await;

        // Score each candidate node: prefer nodes in DCs where the chunk is
        // actively being read (higher read count = higher score).  Among
        // equal-score nodes, prefer the one with more free capacity.
        sm.nodes
            .iter()
            .filter(|(id, info)| {
                **id != exclude_node
                    && info.health == dtfs_common::types::HealthStatus::Healthy
                    && info.disks.iter().any(|d| d.tier == target_tier)
            })
            .max_by_key(|(_, info)| {
                let dc_reads = reads_by_dc
                    .get(&info.location.datacenter)
                    .copied()
                    .unwrap_or(0) as u64;
                let free_capacity = info.total_capacity.saturating_sub(info.used_capacity);
                // Primary sort key: DC read activity (scaled to outweigh capacity);
                // secondary: free capacity.
                (dc_reads * 1_000_000_000, free_capacity)
            })
            .map(|(id, _)| *id)
    }

    /// Update the tiering configuration at runtime (live reload).
    pub async fn update_config(&self, new_config: TieringConfig) {
        let mut cfg = self.config.write().await;
        *cfg = new_config;
        info!("Tiering configuration updated (live reload)");
    }

    /// Get current access statistics (for WebUI / API).
    pub async fn get_access_stats(&self) -> Vec<ChunkAccessProfile> {
        let stats = self.access_stats.read().await;
        stats.values().cloned().collect()
    }
}

/// Classify data temperature based on access patterns.
fn classify_temperature(
    reads_1h: u32,
    reads_24h: u32,
    last_access_ms: u64,
    now_ms: u64,
    hot_threshold: u32,
    cold_days: u32,
    frozen_days: u32,
) -> DataTemperature {
    if reads_1h >= hot_threshold {
        return DataTemperature::Hot;
    }

    if reads_24h >= 2 {
        return DataTemperature::Warm;
    }

    let ms_per_day: u64 = 86_400_000;
    let age_ms = now_ms.saturating_sub(last_access_ms);

    if age_ms > frozen_days as u64 * ms_per_day {
        DataTemperature::Frozen
    } else if age_ms > cold_days as u64 * ms_per_day {
        DataTemperature::Cold
    } else {
        DataTemperature::Warm
    }
}

/// Map a data temperature to the target storage tier.
///
/// Frozen data is sent to Tape (archival / LTFS) rather than Cold (fix #14).
/// The Tape tier exists precisely for long-term, write-once archival of data
/// that has not been accessed in `frozen_threshold_days`.
fn temperature_to_tier(temp: DataTemperature) -> DiskTier {
    match temp {
        DataTemperature::Hot => DiskTier::Hot,
        DataTemperature::Warm => DiskTier::Warm,
        DataTemperature::Cold => DiskTier::Cold,
        DataTemperature::Frozen => DiskTier::Tape,
    }
}

/// Numeric rank for tier comparison (lower = hotter).
fn tier_rank(tier: DiskTier) -> u8 {
    match tier {
        DiskTier::Hot => 0,
        DiskTier::Warm => 1,
        DiskTier::Cold => 2,
        DiskTier::Tape => 3,  // Coldest / archival tier
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ---------------------------------------------------------------------------
// Bug AV (AV-2-1) — per-DC rolling read-window store
// ---------------------------------------------------------------------------
//
// The legacy tiering path uses absolute counters (reads_1h, reads_24h)
// computed by the data nodes. AV needs a per-(chunk, DC) sliding
// window so the promotion engine can answer "how many reads from
// THIS DC in the last `promote_window_secs`?". We carry one ring of
// (timestamp_ms, count) samples per (chunk_id, dc); each heartbeat
// converts the observed counter delta into one new sample.
//
// Memory bound: a sample is 12 bytes; promote_window_secs default 4h;
// in the worst case the manager retains every heartbeat in that
// window. With a 30 s heartbeat that's 480 samples = ~6 KB per
// (chunk, dc). 10 000 chunks × 3 DCs = ~180 MB worst case; the
// window prune below keeps this in practice well under 50 MB.
//
// All public helpers in this section are **pure** so the
// promotion-decision tests (AV-2-2) can call them without spinning
// up a runtime.

/// One sample in the per-DC read window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadSample {
    pub ts_ms: u64,
    pub count: u32,
}

/// AV-2-1 pure kernel — count the total reads in
/// `[now_ms - window_secs*1000, now_ms]`.
///
/// `samples` may be unsorted; the helper iterates exhaustively
/// because the per-(chunk, dc) ring is small and the sort cost
/// would dominate. Counts are saturated at `u32::MAX` so a runaway
/// counter never wraps.
pub(crate) fn count_reads_in_window(
    samples: &[ReadSample],
    now_ms: u64,
    window_secs: u64,
) -> u32 {
    if samples.is_empty() || window_secs == 0 {
        return 0;
    }
    let window_ms = window_secs.saturating_mul(1_000);
    let lower = now_ms.saturating_sub(window_ms);
    let mut total: u32 = 0;
    for s in samples {
        if s.ts_ms >= lower && s.ts_ms <= now_ms {
            total = total.saturating_add(s.count);
        }
    }
    total
}

/// AV-2-1 housekeeping helper — drop samples older than the window.
/// Intended for in-place pruning of the ring-buffer that the engine
/// keeps in its `read_windows` map. Not a hot-path operation; called
/// once per heartbeat per affected (chunk, dc) pair.
#[allow(dead_code)] // used by AV-2-4 wiring; kept here so the helpers stay co-located.
pub(crate) fn prune_read_window(
    samples: &mut Vec<ReadSample>,
    now_ms: u64,
    window_secs: u64,
) {
    if window_secs == 0 {
        samples.clear();
        return;
    }
    let window_ms = window_secs.saturating_mul(1_000);
    let lower = now_ms.saturating_sub(window_ms);
    samples.retain(|s| s.ts_ms >= lower);
}

// ---------------------------------------------------------------------------
// Bug AV (AV-2-2) — pure cache-promotion decision kernel
// ---------------------------------------------------------------------------

/// AV-2-2 pure kernel — decide how many new cache replicas to
/// create for `(chunk, dc, tier)`.
///
/// Inputs:
/// * `read_count_in_window` — total reads from this DC within
///   `promote_window_secs` (output of [`count_reads_in_window`]).
/// * `tier_cfg` — the volume's `cache_replicas.<tier>` policy. A
///   `max == 0` config disables the tier entirely.
/// * `current_cache_replicas_in_dc` — existing cache replicas of
///   this chunk already present in the DC. The result is bounded so
///   we never over-create.
/// * `cooldown_until_ms` — chunk's per-index cooldown set by
///   operator flush (AV-1-4 `SetChunkCooldown`). `0` means no
///   cooldown active.
/// * `promote_reads_threshold` — `cache_replicas.promote_reads`.
///   We refuse to promote unless `read_count_in_window` reaches this.
/// * `now_ms` — caller-supplied current wall-clock (testable).
///
/// Returns the number of replicas to schedule. `0` means "do
/// nothing" (every gate path returns `0`).
///
/// The basic-promotion path caps at `min` — the redundancy
/// escalation pass that ramps toward `max` is opt-in
/// (`ENABLE_CACHE_REDUNDANCY_ESCALATION = false` in Phase 2; see
/// AV-2-4 wiring).
pub(crate) fn plan_cache_promotion(
    read_count_in_window: u32,
    tier_cfg: &dtfs_common::types::CacheTierConfig,
    current_cache_replicas_in_dc: u32,
    cooldown_until_ms: u64,
    promote_reads_threshold: u32,
    now_ms: u64,
) -> u32 {
    // Gate 1 — tier disabled.
    if tier_cfg.max == 0 {
        return 0;
    }
    // Gate 2 — chunk in cooldown (set by operator flush).
    if cooldown_until_ms > 0 && now_ms < cooldown_until_ms {
        return 0;
    }
    // Gate 3 — promotion threshold not met.
    if read_count_in_window < promote_reads_threshold {
        return 0;
    }
    // Gate 4 — already at or above the basic-promotion floor.
    // `target` is `min(min, max)` to defend against a config where
    // the operator set `min > max` (validate() would have caught it,
    // but be paranoid in the planner anyway).
    let target = tier_cfg.min.min(tier_cfg.max);
    target.saturating_sub(current_cache_replicas_in_dc)
}

// ---------------------------------------------------------------------------
// Bug AV (AV-2-3) — pure cache-promotion target selection
// ---------------------------------------------------------------------------

/// Minimal projection of a node's state needed to score it as a
/// cache-promotion target. Built by the engine from a single
/// state-machine snapshot; the pure selector below operates only on
/// this view so it's deterministically testable.
#[derive(Debug, Clone)]
pub(crate) struct NodeSelectionView {
    pub node_id: u64,
    pub datacenter: String,
    /// Set true when the engine has confirmed the node hosts at
    /// least one disk of the desired tier and that disk is healthy.
    pub has_target_tier_disk: bool,
    /// Free-capacity proxy, in bytes. Larger = preferred. The
    /// engine derives this as
    /// `node.total_capacity - node.used_capacity`.
    pub free_capacity: u64,
    /// True when the node is in `Draining` health state (operator
    /// asked it to evacuate; never schedule new replicas there).
    pub draining: bool,
    /// True when the node already holds a replica of the chunk we'd
    /// be promoting. The selector skips these so we don't duplicate
    /// a replica on the same node.
    pub already_holds_chunk: bool,
}

/// AV-2-3 pure kernel — pick the best cache-promotion target in
/// `dc` for a chunk that currently doesn't have a cache replica
/// there.
///
/// Selection rules (in this order):
/// 1. Filter to nodes in `dc` only.
/// 2. Exclude `draining = true`.
/// 3. Exclude `already_holds_chunk = true`.
/// 4. Exclude `has_target_tier_disk = false`.
/// 5. Of the remainder, pick the highest `free_capacity`. Ties
///    broken by lowest `node_id` (deterministic for tests + audit).
///
/// Returns the chosen `node_id`, or `None` if no candidate qualifies.
pub(crate) fn select_promotion_target(
    candidates: &[NodeSelectionView],
    dc: &str,
) -> Option<u64> {
    candidates
        .iter()
        .filter(|n| n.datacenter == dc)
        .filter(|n| !n.draining)
        .filter(|n| !n.already_holds_chunk)
        .filter(|n| n.has_target_tier_disk)
        // max_by_key wants Ord; pack (free_capacity, -node_id) so the
        // higher free_capacity wins, then the lower node_id wins.
        // (We invert by mapping to `u64::MAX - node_id` so larger
        // means "smaller original".)
        .max_by_key(|n| (n.free_capacity, u64::MAX - n.node_id))
        .map(|n| n.node_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_hot() {
        let temp = classify_temperature(10, 50, 1000, 2000, 5, 14, 60);
        assert_eq!(temp, DataTemperature::Hot);
    }

    #[test]
    fn test_classify_warm() {
        let temp = classify_temperature(2, 5, 1000, 2000, 5, 14, 60);
        assert_eq!(temp, DataTemperature::Warm);
    }

    #[test]
    fn test_classify_cold() {
        let ms_per_day: u64 = 86_400_000;
        let now = 100 * ms_per_day;
        let last_access = now - 20 * ms_per_day; // 20 days ago
        let temp = classify_temperature(0, 0, last_access, now, 5, 14, 60);
        assert_eq!(temp, DataTemperature::Cold);
    }

    #[test]
    fn test_classify_frozen() {
        let ms_per_day: u64 = 86_400_000;
        let now = 200 * ms_per_day;
        let last_access = now - 90 * ms_per_day; // 90 days ago
        let temp = classify_temperature(0, 0, last_access, now, 5, 14, 60);
        assert_eq!(temp, DataTemperature::Frozen);
    }

    #[test]
    fn test_temperature_to_tier() {
        assert_eq!(temperature_to_tier(DataTemperature::Hot), DiskTier::Hot);
        assert_eq!(temperature_to_tier(DataTemperature::Warm), DiskTier::Warm);
        assert_eq!(temperature_to_tier(DataTemperature::Cold), DiskTier::Cold);
        // Frozen -> Tape (archival), not Cold (fix for issue #14).
        assert_eq!(temperature_to_tier(DataTemperature::Frozen), DiskTier::Tape);
    }
}

#[cfg(test)]
mod bug_av_read_window_tests {
    //! AV-2-1 — pure tests for `count_reads_in_window` /
    //! `prune_read_window`. These run without a runtime because the
    //! helpers are stateless.
    use super::*;

    fn s(ts_ms: u64, count: u32) -> ReadSample {
        ReadSample { ts_ms, count }
    }

    /// AV-2-1 test 1 — samples inside the window contribute.
    #[test]
    fn bug_av_read_window_counts_inside_window() {
        let now = 1_000_000;
        // window = 60 s ⇒ lower bound = now - 60_000 = 940_000.
        let samples = vec![s(940_000, 1), s(960_000, 2), s(1_000_000, 4)];
        assert_eq!(count_reads_in_window(&samples, now, 60), 7);
    }

    /// AV-2-1 test 2 — samples older than the window are dropped from
    /// the count, and `prune_read_window` actually removes them.
    #[test]
    fn bug_av_read_window_drops_outside() {
        let now = 1_000_000;
        // The 800_000 sample is 200 s old, outside a 60-s window.
        let mut samples = vec![s(800_000, 99), s(960_000, 2), s(1_000_000, 4)];
        assert_eq!(count_reads_in_window(&samples, now, 60), 6);
        prune_read_window(&mut samples, now, 60);
        assert_eq!(samples.len(), 2, "pruned ring drops the 800_000 sample");
        assert!(samples.iter().all(|s| s.ts_ms >= 940_000));
    }

    /// AV-2-1 test 3 — empty input returns 0 (and doesn't underflow
    /// the saturating-sub on `now_ms - window`).
    #[test]
    fn bug_av_read_window_handles_empty() {
        assert_eq!(count_reads_in_window(&[], 1_000_000, 60), 0);
        assert_eq!(count_reads_in_window(&[], 0, 60), 0);
        // Pruning an empty ring is a no-op.
        let mut empty: Vec<ReadSample> = Vec::new();
        prune_read_window(&mut empty, 1_000_000, 60);
        assert!(empty.is_empty());
    }

    /// AV-2-1 test 4 — unsorted input is handled correctly. The
    /// helper must not assume monotonic timestamps (heartbeat
    /// dedup, clock skew, etc.).
    #[test]
    fn bug_av_read_window_handles_unsorted_input() {
        let now = 1_000_000;
        // Interleaved old + recent samples.
        let samples = vec![
            s(1_000_000, 1),
            s(500_000, 99), // older than the 60-s window
            s(960_000, 3),
            s(800_000, 50), // older
            s(950_000, 2),
        ];
        // Only 1 + 3 + 2 = 6 are inside the 60-s window.
        assert_eq!(count_reads_in_window(&samples, now, 60), 6);
    }

    /// AV-2-1 defensive — `window_secs = 0` returns 0 (every sample
    /// is "older than" a 0-second window). Edge case caught early
    /// because the type allows it.
    #[test]
    fn bug_av_read_window_zero_window_returns_zero() {
        let samples = vec![s(1_000_000, 5), s(999_999, 5)];
        assert_eq!(count_reads_in_window(&samples, 1_000_000, 0), 0);
        let mut s2 = samples.clone();
        prune_read_window(&mut s2, 1_000_000, 0);
        assert!(s2.is_empty());
    }
}

#[cfg(test)]
mod bug_av_promotion_decision_tests {
    //! AV-2-2 — pure tests for `plan_cache_promotion`. Each test
    //! exercises one gate in the decision tree (disable, cooldown,
    //! threshold, already-at-target).
    use super::*;
    use dtfs_common::types::CacheTierConfig;

    fn cfg(min: u32, max: u32) -> CacheTierConfig {
        CacheTierConfig { min, max }
    }

    /// AV-2-2 test 1 — read count below threshold → do nothing.
    #[test]
    fn bug_av_promote_first_read_below_threshold_does_nothing() {
        let n = plan_cache_promotion(
            /* read_count_in_window */ 1,
            /* tier_cfg */ &cfg(1, 1),
            /* current_cache_replicas_in_dc */ 0,
            /* cooldown_until_ms */ 0,
            /* promote_reads_threshold */ 2,
            /* now_ms */ 1_000_000,
        );
        assert_eq!(n, 0);
    }

    /// AV-2-2 test 2 — threshold met, no current replicas →
    /// returns `min`.
    #[test]
    fn bug_av_promote_threshold_met_returns_min_target_minus_current() {
        let n = plan_cache_promotion(2, &cfg(1, 1), 0, 0, 2, 1_000_000);
        assert_eq!(n, 1);
    }

    /// AV-2-2 test 3 — already at `min` → returns 0 (basic-promotion
    /// path doesn't ramp into `max`).
    #[test]
    fn bug_av_promote_already_at_min_returns_zero() {
        let n = plan_cache_promotion(10, &cfg(1, 2), 1, 0, 2, 1_000_000);
        assert_eq!(n, 0, "current already at min=1; ramp to max=2 is reserved for the redundancy-escalation pass");
    }

    /// AV-2-2 test 4 — `max == 0` disables the tier entirely.
    #[test]
    fn bug_av_promote_max_zero_disables_promotion() {
        let n = plan_cache_promotion(999, &cfg(0, 0), 0, 0, 1, 1_000_000);
        assert_eq!(n, 0);
    }

    /// AV-2-2 test 5 — chunk in cooldown blocks promotion until the
    /// cooldown expires.
    #[test]
    fn bug_av_promote_cooldown_blocks_until_expiry() {
        // 2 reads in window, threshold 2, target 1 — would promote if
        // not for the cooldown set 100 ms in the future.
        let blocked = plan_cache_promotion(2, &cfg(1, 1), 0,
            /* cooldown_until_ms */ 1_000_100,
            /* threshold */ 2,
            /* now_ms */ 1_000_000);
        assert_eq!(blocked, 0);

        // Same call after the cooldown expires unblocks the
        // promotion.
        let allowed = plan_cache_promotion(2, &cfg(1, 1), 0,
            /* cooldown_until_ms */ 1_000_100,
            /* threshold */ 2,
            /* now_ms */ 1_000_200);
        assert_eq!(allowed, 1);
    }

    /// AV-2-2 test 6 — when `min = 2` (per-DC HA), threshold met
    /// returns 2 in a single decision so the engine schedules both
    /// replicas at once.
    #[test]
    fn bug_av_promote_min_two_creates_two_at_once() {
        let n = plan_cache_promotion(5, &cfg(2, 2), 0, 0, 2, 1_000_000);
        assert_eq!(n, 2);
    }

    /// AV-2-2 defensive — a misconfigured `min > max` (validate()
    /// would normally catch this at load) is clamped to `max` by the
    /// planner so we never schedule more than the operator allowed.
    #[test]
    fn bug_av_promote_min_greater_than_max_clamps_to_max() {
        let n = plan_cache_promotion(5, &cfg(3, 1), 0, 0, 2, 1_000_000);
        assert_eq!(n, 1, "min=3 clamps to max=1");
    }
}

#[cfg(test)]
mod bug_av_target_selection_tests {
    //! AV-2-3 — pure tests for `select_promotion_target`. Each test
    //! exercises one filter clause + the free-capacity tiebreaker.
    use super::*;

    fn node(
        id: u64,
        dc: &str,
        free: u64,
        has_tier: bool,
        draining: bool,
        holds: bool,
    ) -> NodeSelectionView {
        NodeSelectionView {
            node_id: id,
            datacenter: dc.to_string(),
            has_target_tier_disk: has_tier,
            free_capacity: free,
            draining,
            already_holds_chunk: holds,
        }
    }

    /// AV-2-3 test 1 — same DC, free-capacity tiebreak picks the
    /// node with the most free space.
    #[test]
    fn bug_av_target_selects_most_free_in_dc() {
        let candidates = vec![
            node(10, "toro", 100, true, false, false),
            node(11, "toro", 500, true, false, false), // winner
            node(12, "toro", 200, true, false, false),
        ];
        let pick = select_promotion_target(&candidates, "toro");
        assert_eq!(pick, Some(11));
    }

    /// AV-2-3 test 2 — `Draining` nodes never win even when their
    /// free capacity beats everyone else's.
    #[test]
    fn bug_av_target_excludes_draining_nodes() {
        let candidates = vec![
            node(10, "toro", 9_999_999, true, /* draining */ true, false),
            node(11, "toro", 100, true, false, false), // only legitimate option
        ];
        let pick = select_promotion_target(&candidates, "toro");
        assert_eq!(pick, Some(11));
    }

    /// AV-2-3 test 3 — nodes already holding a replica of the chunk
    /// are skipped.
    #[test]
    fn bug_av_target_excludes_existing_replica_holders() {
        let candidates = vec![
            node(10, "toro", 9_999_999, true, false, /* holds */ true),
            node(11, "toro", 100, true, false, false), // only legitimate
        ];
        let pick = select_promotion_target(&candidates, "toro");
        assert_eq!(pick, Some(11));
    }

    /// AV-2-3 test 4 — no warm/target-tier node in the DC → `None`.
    /// The engine logs and gives up; the chunk is read straight from
    /// the cold copy until the policy can be satisfied.
    #[test]
    fn bug_av_target_returns_none_when_no_dc_warm_node() {
        let candidates = vec![
            // Same DC but lacks the target tier.
            node(10, "toro", 999_999, /* has_tier */ false, false, false),
            // Right tier but wrong DC.
            node(11, "nrb",  999_999, true, false, false),
        ];
        let pick = select_promotion_target(&candidates, "toro");
        assert!(pick.is_none());
    }

    /// AV-2-3 defensive — when two nodes have *identical* free
    /// capacity, the lower `node_id` wins (so the choice is
    /// deterministic across runs / audit reviews).
    #[test]
    fn bug_av_target_ties_resolve_by_lower_node_id() {
        let candidates = vec![
            node(20, "toro", 100, true, false, false),
            node(10, "toro", 100, true, false, false), // winner
            node(30, "toro", 100, true, false, false),
        ];
        let pick = select_promotion_target(&candidates, "toro");
        assert_eq!(pick, Some(10));
    }
}
