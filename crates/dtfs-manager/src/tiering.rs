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
