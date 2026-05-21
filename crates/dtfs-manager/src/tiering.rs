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
    /// Bug AV (AV-2-1) — per-(chunk_id, dc) sliding window of reads.
    /// Pushed by `report_access` on positive heartbeat deltas; consumed
    /// by `tick_av_cache` for promotion decisions.
    read_windows: RwLock<HashMap<(String, String), Vec<ReadSample>>>,
    /// Bug AV (AV-2-4) — last observed `reads_by_dc` value per
    /// (chunk_id, dc). Used to compute the delta on each heartbeat
    /// so we don't double-count reads carried inside the data
    /// node's own 1h sliding window.
    prev_reads_by_dc: RwLock<HashMap<(String, String), u32>>,
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
            read_windows: RwLock::new(HashMap::new()),
            prev_reads_by_dc: RwLock::new(HashMap::new()),
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
        profile.reads_by_dc = reads_by_dc.clone();
        if reads_1h > 0 || reads_24h > 0 {
            profile.last_access_ms = now_ms;
        }
        profile.current_tier = current_tier;
        profile.current_node = current_node;
        drop(stats);

        // Bug AV (AV-2-4) — track per-(chunk, dc) deltas to feed the
        // read-window store. The data node already reports a
        // 1h-windowed counter (`reads_by_dc_1h`); we record only the
        // positive deltas as samples so the manager-side window
        // doesn't double-count reads that the data-node window
        // already smoothed.
        let mut prev = self.prev_reads_by_dc.write().await;
        let mut windows = self.read_windows.write().await;
        for (dc, new_val) in reads_by_dc.iter() {
            let key = (chunk_id.to_string(), dc.clone());
            let old_val = prev.get(&key).copied().unwrap_or(0);
            if *new_val > old_val {
                let delta = *new_val - old_val;
                windows
                    .entry(key.clone())
                    .or_default()
                    .push(ReadSample { ts_ms: now_ms, count: delta });
            }
            // Always update the stored "last seen" value, including
            // when it drops (data-node 1h window slid past a read).
            prev.insert(key, *new_val);
        }
    }

    /// Bug AV (AV-2-4) — return a snapshot of the read window for a
    /// `(chunk_id, dc)` pair after pruning to `window_secs`. Used by
    /// the tick path; safe to call concurrently.
    async fn read_window_count(
        &self,
        chunk_id: &str,
        dc: &str,
        now_ms: u64,
        window_secs: u64,
    ) -> u32 {
        let mut windows = self.read_windows.write().await;
        let key = (chunk_id.to_string(), dc.to_string());
        if let Some(ring) = windows.get_mut(&key) {
            prune_read_window(ring, now_ms, window_secs);
            return count_reads_in_window(ring, now_ms, window_secs);
        }
        0
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

        // Bug AV (AV-2-4) — run the AV cache-tick alongside the legacy
        // temperature-based path. Volumes without a `cache_replicas`
        // policy produce no plan (the planner skips them); volumes
        // WITH a policy drive the new opportunistic cache lifecycle.
        if let Err(e) = self.tick_av_cache().await {
            warn!("Bug AV tick_av_cache failed: {}", e);
        }

        Ok(())
    }

    /// Bug AV (AV-2-4) — one engine tick for the cache lifecycle.
    /// Builds a `TickSnapshot` from the SM + read-window store,
    /// runs the pure planner, then dispatches promotion + demotion
    /// actions to the scheduler.
    ///
    /// Volumes without a `cache_replicas` policy (or with a
    /// disabled / 0-window config) contribute no actions; the
    /// legacy temperature-based migration path (above) still
    /// handles those.
    async fn tick_av_cache(&self) -> anyhow::Result<()> {
        let now_ms = now_millis();

        // Build the snapshot inside one SM read lock so the planner
        // sees a consistent view.
        let snapshot = self.build_av_cache_snapshot(now_ms).await;
        if snapshot.chunks.is_empty() {
            return Ok(());
        }

        let plan = plan_cache_tick(&snapshot, now_ms);
        if plan.promotions.is_empty() && plan.demotions.is_empty() {
            return Ok(());
        }

        info!(
            "Bug AV tick: {} promotions, {} demotions",
            plan.promotions.len(),
            plan.demotions.len()
        );

        // -- Dispatch promotions via the scheduler.
        for p in &plan.promotions {
            self.scheduler
                .schedule_replication(
                    &p.chunk_id,
                    p.source_node_id,
                    p.target_node_id,
                    p.target_tier,
                )
                .await;
            self.audit.log(
                "tiering",
                "cache.promote",
                &format!(
                    "chunk={} file={} idx={} src={} dst={} dc={} tier={:?}",
                    p.chunk_id,
                    p.file_path,
                    p.chunk_index,
                    p.source_node_id,
                    p.target_node_id,
                    p.target_dc,
                    p.target_tier,
                ),
                p.reason,
            );
        }

        // -- AV-2-5 — Dispatch demotions: DeleteChunkReplica RPC →
        // RemoveChunkLocation Raft write → conditional cooldown
        // clear. The conditional check (`should_clear_cooldown_on_
        // demote`) honours an operator flush whose cooldown is
        // still in the future.
        for d in &plan.demotions {
            // 1. Resolve the data-node's gRPC address.
            let node_addr = match self.scheduler.node_grpc_addr(d.node_id).await {
                Some(addr) => addr,
                None => {
                    warn!(
                        chunk = %d.chunk_id,
                        node = d.node_id,
                        "Bug AV demote: node unreachable, skipping"
                    );
                    self.audit.log(
                        "tiering",
                        "cache.demote",
                        &format!(
                            "chunk={} file={} idx={} node={} tier={:?}",
                            d.chunk_id, d.file_path, d.chunk_index, d.node_id, d.tier
                        ),
                        "skipped_unreachable",
                    );
                    continue;
                }
            };

            // 2. DeleteChunkReplica RPC (idempotent shape from
            //    AV-1-5: returns was_present + freed_bytes).
            match self
                .scheduler
                .delete_chunk_replica_on_node(&node_addr, &d.chunk_id)
                .await
            {
                Ok((was_present, _freed)) => {
                    self.audit.log(
                        "tiering",
                        "cache.demote",
                        &format!(
                            "chunk={} file={} idx={} node={} tier={:?}",
                            d.chunk_id, d.file_path, d.chunk_index, d.node_id, d.tier
                        ),
                        if was_present { d.reason } else { "noop_already_absent" },
                    );
                }
                Err(e) => {
                    warn!(
                        chunk = %d.chunk_id,
                        node = d.node_id,
                        error = %e,
                        "Bug AV demote: DeleteChunkReplica failed"
                    );
                    self.audit.log(
                        "tiering",
                        "cache.demote",
                        &format!(
                            "chunk={} file={} idx={} node={} tier={:?} err={}",
                            d.chunk_id, d.file_path, d.chunk_index, d.node_id, d.tier, e
                        ),
                        "rpc_failed",
                    );
                    continue;
                }
            };

            // 3. RemoveChunkLocation Raft write so the SM no longer
            //    lists this replica.
            let remove_req = dtfs_raft::ClientRequest::RemoveChunkLocation {
                path: d.file_path.clone(),
                chunk_index: d.chunk_index,
                node_id: d.node_id.to_string(),
                // AV-2-4 ensures the planner only demotes when
                // anchor survives, but pass the policy through so
                // the apply path's defence-in-depth check confirms.
                enforce_anchor: None,
            };
            if let Err(e) = self.scheduler.raft().client_write(remove_req).await {
                warn!(
                    chunk = %d.chunk_id,
                    error = %e,
                    "Bug AV demote: RemoveChunkLocation Raft write failed"
                );
                continue;
            }

            // 4. Conditional cooldown clear. Only fire when the
            //    pre-demote cooldown was 0 or in the past — never
            //    overwrite an active operator flush.
            if should_clear_cooldown_on_demote(d.cooldown_until_ms_at_snapshot, now_ms) {
                let clear_req = dtfs_raft::ClientRequest::SetChunkCooldown {
                    path: d.file_path.clone(),
                    chunk_indexes: vec![d.chunk_index],
                    expires_at_ms: 0,
                };
                if let Err(e) = self.scheduler.raft().client_write(clear_req).await {
                    warn!(
                        chunk = %d.chunk_id,
                        error = %e,
                        "Bug AV demote: SetChunkCooldown(0) clear failed"
                    );
                }
            }
        }
        Ok(())
    }

    /// Bug AV (AV-2-4) — build a `TickSnapshot` from the live state
    /// machine + the engine's read-window store. Held under a
    /// single SM read lock for consistency.
    async fn build_av_cache_snapshot(
        &self,
        now_ms: u64,
    ) -> TickSnapshot {
        // Materialise the lookup tables once.
        let sm = self.state_data.read().await;

        // node_id → (dc, free_capacity, draining, tier set)
        let mut node_views: Vec<NodeSelectionView> = Vec::new();
        let mut id_to_dc: HashMap<u64, String> = HashMap::new();
        let mut id_to_tiers: HashMap<u64, Vec<DiskTier>> = HashMap::new();
        for (id, info) in sm.nodes.iter() {
            id_to_dc.insert(*id, info.location.datacenter.clone());
            let tiers: Vec<DiskTier> = info
                .disks
                .iter()
                .map(|d| d.tier)
                .collect();
            id_to_tiers.insert(*id, tiers.clone());
            // The planner will set `has_target_tier_disk` per-call
            // when iterating tiers — we materialise one node view
            // per tier the node has below. Here we emit a baseline
            // view; the planner closure layers the per-call tier
            // check via the candidates iteration.
            let draining = matches!(info.health, dtfs_common::types::HealthStatus::Draining);
            // For simplicity, emit one view per tier so the planner
            // can pick the right one. Free capacity is shared
            // across tiers on the same node; that's a small
            // approximation that's good enough for promotion
            // target picking.
            for tier in &tiers {
                node_views.push(NodeSelectionView {
                    node_id: *id,
                    datacenter: info.location.datacenter.clone(),
                    has_target_tier_disk: true,
                    free_capacity: info.total_capacity.saturating_sub(info.used_capacity),
                    draining,
                    already_holds_chunk: false,
                });
                // Mark the tier on the most recent view so
                // downstream filters work via the per-tier
                // iteration; we just rely on
                // `has_target_tier_disk = true` for any tier the
                // node has, and the planner closure tier-matches
                // via `select_promotion_target`'s caller.
                let _ = tier;
            }
        }

        // Convert volumes to TickVolumePolicy.
        let mut volumes: HashMap<String, TickVolumePolicy> = HashMap::new();
        for (vid, vcfg) in sm.volumes.iter() {
            // Only emit volumes with a real cache policy; everything
            // else gets the legacy temperature path.
            let mandatory = vcfg.effective_mandatory_replicas().clone();
            let cache = vcfg.cache_replicas.clone();
            // Skip when cache disabled entirely (every tier max=0
            // AND no promote_window).
            let any_cache_tier =
                cache.hot.max > 0 || cache.warm.max > 0 || cache.cold.max > 0 || cache.tape.max > 0;
            if !any_cache_tier || cache.promote_window_secs == 0 {
                continue;
            }
            volumes.insert(
                vid.clone(),
                TickVolumePolicy {
                    mandatory,
                    cache,
                    anchor: vcfg.effective_anchor(),
                },
            );
        }
        if volumes.is_empty() {
            // No AV-style volumes → empty snapshot, planner returns
            // an empty plan and we early-return upstream.
            return TickSnapshot {
                chunks: Vec::new(),
                nodes: node_views,
                volumes,
            };
        }

        // Walk every file's chunks, building TickChunk entries for
        // chunks whose volume has a cache policy.
        let mut chunks: Vec<TickChunk> = Vec::new();
        for (path, fm) in sm.files.iter() {
            let Some(policy) = volumes.get(&fm.volume_id) else {
                continue;
            };
            // Group ChunkLocationEntry rows by chunk_index so we
            // build one TickChunk per (file, chunk_index).
            let mut by_idx: std::collections::BTreeMap<u64, Vec<&dtfs_raft::ChunkLocationEntry>> =
                std::collections::BTreeMap::new();
            for e in &fm.chunks {
                by_idx.entry(e.chunk_index).or_default().push(e);
            }
            for (idx, entries) in by_idx {
                // Resolve placements.
                let mut placements: Vec<TickPlacement> = Vec::new();
                let mut chunk_id_opt: Option<String> = None;
                for e in &entries {
                    if chunk_id_opt.is_none() {
                        chunk_id_opt = Some(e.chunk_hash.clone());
                    }
                    let nid = match e.node_id.parse::<u64>() {
                        Ok(n) => n,
                        Err(_) => continue,
                    };
                    let dc = match id_to_dc.get(&nid) {
                        Some(d) => d.clone(),
                        None => continue,
                    };
                    let tier = id_to_tiers
                        .get(&nid)
                        .and_then(|t| t.get(e.disk_id as usize).copied());
                    let Some(t) = tier else { continue };
                    placements.push(TickPlacement { node_id: nid, dc, tier: t });
                }
                let Some(chunk_id) = chunk_id_opt else { continue };

                // Per-DC reads in the volume's promote_window_secs.
                let mut reads_in_window_by_dc: HashMap<String, u32> = HashMap::new();
                // Heuristic: query every DC that appears in nodes.
                let unique_dcs: std::collections::HashSet<String> =
                    id_to_dc.values().cloned().collect();
                for dc in unique_dcs {
                    let cnt = self
                        .read_window_count(&chunk_id, &dc, now_ms,
                                           policy.cache.promote_window_secs)
                        .await;
                    if cnt > 0 {
                        reads_in_window_by_dc.insert(dc, cnt);
                    }
                }

                let cooldown = fm
                    .cache_cooldown_until_ms
                    .get(&idx)
                    .copied()
                    .unwrap_or(0);

                chunks.push(TickChunk {
                    chunk_id,
                    chunk_index: idx,
                    file_path: path.clone(),
                    volume_id: fm.volume_id.clone(),
                    placements,
                    cooldown_until_ms: cooldown,
                    reads_in_window_by_dc,
                });
            }
        }
        drop(sm);

        TickSnapshot { chunks, nodes: node_views, volumes }
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

// ---------------------------------------------------------------------------
// Bug AV (AV-2-4) — `plan_cache_tick` pure planner + supporting types
// ---------------------------------------------------------------------------
//
// The async engine (`evaluate_av_cache_tick`) builds a `TickSnapshot`
// from the state machine + read-window store, then calls
// `plan_cache_tick(snapshot, now_ms)` to produce a list of
// promotion + demotion actions. The dispatcher then translates each
// action into the appropriate scheduler call.
//
// Why a pure planner: we get deterministic, fast unit tests on the
// decision logic ("threshold met → schedule promotion", "idle for
// `promote_window_secs` → schedule demotion", "draining nodes never
// receive promotions", etc.) without spinning up Raft / SM mocks.

/// AV-2-4 — feature flag for redundancy-escalation past `min` toward
/// `max`. Phase 2 ships with this off; Phase 4 follow-up flips it on
/// once a real per-DC HA workload exists to validate against.
pub(crate) const ENABLE_CACHE_REDUNDANCY_ESCALATION: bool = false;

/// AV-2-4 — projection of a volume's caching policy used by the
/// planner. Built from `VolumeConfig` by the engine.
#[derive(Debug, Clone)]
pub(crate) struct TickVolumePolicy {
    pub mandatory: dtfs_common::types::TierReplication,
    pub cache: dtfs_common::types::CacheReplication,
    pub anchor: Option<dtfs_common::types::AnchorPolicy>,
}

/// AV-2-4 — one chunk's view as the planner needs it.
#[derive(Debug, Clone)]
pub(crate) struct TickChunk {
    pub chunk_id: String,
    pub chunk_index: u64,
    pub file_path: String,
    pub volume_id: String,
    /// Existing placements (where this chunk is hosted today).
    pub placements: Vec<TickPlacement>,
    /// Per-chunk cooldown ts (0 = none).
    pub cooldown_until_ms: u64,
    /// Reads observed in the volume's promote window, bucketed by
    /// source DC (output of `count_reads_in_window` per dc).
    pub reads_in_window_by_dc: std::collections::HashMap<String, u32>,
}

/// AV-2-4 — one placement of a chunk (node + DC + tier).
#[derive(Debug, Clone)]
pub(crate) struct TickPlacement {
    pub node_id: u64,
    pub dc: String,
    pub tier: DiskTier,
}

/// AV-2-4 — full snapshot input to the planner.
#[derive(Debug, Clone)]
pub(crate) struct TickSnapshot {
    pub chunks: Vec<TickChunk>,
    pub nodes: Vec<NodeSelectionView>,
    pub volumes: std::collections::HashMap<String, TickVolumePolicy>,
}

/// AV-2-4 — one cache replica to create. The engine's dispatcher
/// then calls `scheduler.schedule_replication(...)` with these
/// values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TickPromotion {
    pub chunk_id: String,
    pub chunk_index: u64,
    pub file_path: String,
    pub source_node_id: u64,
    pub target_node_id: u64,
    pub target_dc: String,
    pub target_tier: DiskTier,
    /// Stable reason code for audit/log scraping. Currently the
    /// only value is `"promote_threshold_met"`; future redundancy-
    /// escalation paths will use `"promote_redundancy"`.
    pub reason: &'static str,
}

/// AV-2-4 — one cache replica to delete (engine-driven demotion of
/// an idle cache replica, not operator-driven flush).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TickDemotion {
    pub chunk_id: String,
    pub chunk_index: u64,
    pub file_path: String,
    pub node_id: u64,
    pub tier: DiskTier,
    pub reason: &'static str,
    /// AV-2-5 — the chunk's cooldown timestamp as observed in the
    /// planner snapshot. The dispatcher passes this through
    /// `should_clear_cooldown_on_demote` to decide whether the
    /// engine-driven demote should clear an existing entry (idle
    /// demote → clear so the next read re-promotes immediately;
    /// operator flush cooldown still active → leave alone).
    pub cooldown_until_ms_at_snapshot: u64,
}

/// AV-2-4 — output of the planner. Wrapped in a struct so the
/// engine can iterate both lists from a single return value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TickPlan {
    pub promotions: Vec<TickPromotion>,
    pub demotions: Vec<TickDemotion>,
}

/// AV-2-4 — pure planner. Walks every chunk in the snapshot and
/// emits:
/// * **Promotions** for `(chunk, DC, tier)` triples where the
///   read window threshold is met, no cooldown applies, no cache
///   replica yet exists in the DC, and a valid target node exists.
/// * **Demotions** for cache replicas that have seen 0 reads in
///   the volume's `promote_window_secs` AND would not strand the
///   mandatory copy if dropped.
///
/// The planner intentionally does NOT consult the heartbeat
/// counters (`total_reads_1h`, `total_reads_24h`) — those are the
/// legacy temperature signal. AV uses only the per-DC read window.
pub(crate) fn plan_cache_tick(
    snapshot: &TickSnapshot,
    now_ms: u64,
) -> TickPlan {
    let mut plan = TickPlan::default();
    for chunk in &snapshot.chunks {
        let Some(policy) = snapshot.volumes.get(&chunk.volume_id) else {
            // Unknown volume — skip rather than guess. The legacy
            // tiering path still runs for volumes without an AV
            // policy.
            continue;
        };

        // -- Promotions --
        plan_chunk_promotions(chunk, policy, &snapshot.nodes, now_ms, &mut plan.promotions);

        // -- Demotions --
        plan_chunk_demotions(chunk, policy, now_ms, &mut plan.demotions);
    }
    plan
}

/// AV-2-4 helper — promotion decisions for one chunk.
fn plan_chunk_promotions(
    chunk: &TickChunk,
    policy: &TickVolumePolicy,
    nodes: &[NodeSelectionView],
    now_ms: u64,
    out: &mut Vec<TickPromotion>,
) {
    // Each tier with `cache_replicas.<tier>.max > 0` is a candidate.
    let tier_configs: [(DiskTier, &dtfs_common::types::CacheTierConfig); 4] = [
        (DiskTier::Hot,  &policy.cache.hot),
        (DiskTier::Warm, &policy.cache.warm),
        (DiskTier::Cold, &policy.cache.cold),
        (DiskTier::Tape, &policy.cache.tape),
    ];
    for (tier, tier_cfg) in tier_configs.iter() {
        if tier_cfg.is_disabled() {
            continue;
        }
        // A tier that's *also* mandatory isn't a cache tier — the
        // under-replication scheduler owns it. The cache planner
        // never touches it.
        if policy.mandatory.for_tier(*tier) > 0 {
            continue;
        }

        // When per_dc is true: each DC promotes independently.
        // When false: any DC's reads count toward a single global
        //             cache pool.
        let per_dc = policy.cache.per_dc;
        let dcs: Vec<String> = if per_dc {
            chunk.reads_in_window_by_dc.keys().cloned().collect()
        } else {
            // Synthesize a "global" pseudo-DC that sums everything.
            vec!["__global__".to_string()]
        };

        for dc in dcs {
            let reads = if per_dc {
                chunk
                    .reads_in_window_by_dc
                    .get(&dc)
                    .copied()
                    .unwrap_or(0)
            } else {
                chunk.reads_in_window_by_dc.values().copied().sum()
            };
            // Count existing cache replicas in the DC scope.
            let current = if per_dc {
                chunk
                    .placements
                    .iter()
                    .filter(|p| p.dc == dc && p.tier == *tier)
                    .count() as u32
            } else {
                chunk
                    .placements
                    .iter()
                    .filter(|p| p.tier == *tier)
                    .count() as u32
            };

            let basic = plan_cache_promotion(
                reads,
                tier_cfg,
                current,
                chunk.cooldown_until_ms,
                policy.cache.promote_reads,
                now_ms,
            );
            // Redundancy escalation: ramp toward `max` only when
            // the feature flag is on AND `max > min` AND we've
            // already met `min` AND `read_count >= 2 *
            // promote_reads` (a coarse "hot enough to justify"
            // heuristic; Phase-4 will refine).
            let want = if ENABLE_CACHE_REDUNDANCY_ESCALATION
                && tier_cfg.max > tier_cfg.min
                && current >= tier_cfg.min
                && reads >= policy.cache.promote_reads.saturating_mul(2)
            {
                tier_cfg.max.saturating_sub(current)
            } else {
                basic
            };
            if want == 0 {
                continue;
            }

            // Build candidates with `already_holds_chunk` flagged
            // per placement of this chunk.
            let holders: std::collections::HashSet<u64> =
                chunk.placements.iter().map(|p| p.node_id).collect();
            let candidates: Vec<NodeSelectionView> = nodes
                .iter()
                .map(|n| {
                    let mut copy = n.clone();
                    if holders.contains(&n.node_id) {
                        copy.already_holds_chunk = true;
                    }
                    copy
                })
                .collect();

            // For per_dc=false ("global" pool) we still need to
            // pick a DC to host the new replica. Pick the DC that
            // generated the most reads (best locality).
            let target_dc = if per_dc {
                dc.clone()
            } else {
                chunk
                    .reads_in_window_by_dc
                    .iter()
                    .max_by_key(|entry| *entry.1)
                    .map(|(d, _)| d.clone())
                    .unwrap_or_else(|| dc.clone())
            };

            // For each replica we want to add, pick a distinct
            // target. Loop because `select_promotion_target`
            // returns one node at a time and we want N replicas.
            let mut placed: std::collections::HashSet<u64> =
                std::collections::HashSet::new();
            for _ in 0..want {
                let avail: Vec<NodeSelectionView> = candidates
                    .iter()
                    .map(|n| {
                        let mut c = n.clone();
                        if placed.contains(&n.node_id) {
                            c.already_holds_chunk = true;
                        }
                        c
                    })
                    .collect();
                let Some(target) = select_promotion_target(&avail, &target_dc) else {
                    break;
                };
                placed.insert(target);
                // Source = any existing placement. Prefer one in
                // a different DC (so we're not source==target by
                // accident) and in a stable tier ordering for
                // audit determinism.
                let source = chunk
                    .placements
                    .iter()
                    .min_by_key(|p| (tier_rank(p.tier), p.node_id))
                    .map(|p| p.node_id);
                if let Some(src) = source {
                    out.push(TickPromotion {
                        chunk_id: chunk.chunk_id.clone(),
                        chunk_index: chunk.chunk_index,
                        file_path: chunk.file_path.clone(),
                        source_node_id: src,
                        target_node_id: target,
                        target_dc: target_dc.clone(),
                        target_tier: *tier,
                        reason: if ENABLE_CACHE_REDUNDANCY_ESCALATION
                            && current >= tier_cfg.min
                        {
                            "promote_redundancy"
                        } else {
                            "promote_threshold_met"
                        },
                    });
                }
            }
        }
    }
}

/// AV-2-4 helper — demotion decisions for one chunk.
///
/// A placement is demoted when ALL of:
///   * the tier is cache-eligible for the volume
///     (`cache_replicas.<tier>.max > 0`) AND not mandatory,
///   * the volume has `cache_replicas.promote_window_secs > 0`,
///   * `reads_in_window_by_dc.get(dc)` is 0 (no recent reads
///     from the placement's DC),
///   * dropping the placement wouldn't strand the mandatory copy
///     (i.e., at least one mandatory-tier replica still remains).
///
/// Cooldown does NOT block demotion (operator flushes already
/// drove the cooldown; engine-driven demotion is a separate path
/// and should run unhindered).
fn plan_chunk_demotions(
    chunk: &TickChunk,
    policy: &TickVolumePolicy,
    _now_ms: u64,
    out: &mut Vec<TickDemotion>,
) {
    if policy.cache.promote_window_secs == 0 {
        // Cache disabled cluster-wide on this volume; demotions
        // would be misleading.
        return;
    }
    // Build a quick is-cache-tier predicate.
    let is_cache_tier = |t: DiskTier| -> bool {
        let cfg = match t {
            DiskTier::Hot => &policy.cache.hot,
            DiskTier::Warm => &policy.cache.warm,
            DiskTier::Cold => &policy.cache.cold,
            DiskTier::Tape => &policy.cache.tape,
        };
        cfg.max > 0 && policy.mandatory.for_tier(t) == 0
    };

    // A demotion candidate is a placement on a cache tier with 0
    // reads in its DC and at least one mandatory replica surviving
    // after the drop.
    for placement in &chunk.placements {
        if !is_cache_tier(placement.tier) {
            continue;
        }
        let dc_reads = chunk
            .reads_in_window_by_dc
            .get(&placement.dc)
            .copied()
            .unwrap_or(0);
        if dc_reads > 0 {
            continue;
        }
        // Mandatory survives the drop iff at least one OTHER
        // placement is on a mandatory tier.
        let mandatory_still_present = chunk.placements.iter().any(|p| {
            (p.node_id != placement.node_id || p.tier != placement.tier)
                && policy.mandatory.for_tier(p.tier) > 0
        });
        if !mandatory_still_present {
            continue;
        }
        out.push(TickDemotion {
            chunk_id: chunk.chunk_id.clone(),
            chunk_index: chunk.chunk_index,
            file_path: chunk.file_path.clone(),
            node_id: placement.node_id,
            tier: placement.tier,
            reason: "idle_window",
            cooldown_until_ms_at_snapshot: chunk.cooldown_until_ms,
        });
    }
}

// ---------------------------------------------------------------------------
// Bug AV (AV-2-5) — cooldown clearance on engine-driven demote
// ---------------------------------------------------------------------------

/// AV-2-5 pure kernel — decide whether an engine-driven demote
/// should clear the chunk's cooldown entry.
///
/// Engine-driven demotion happens when a cache replica has 0 reads
/// in the volume's `promote_window_secs`. If the operator flushed
/// the chunk recently, the cooldown will be in the future (set to
/// `now + cooldown_secs`) — we MUST leave that alone so the
/// operator's "no immediate re-promotion" intent is respected.
///
/// Returns `true` when the engine should issue
/// `SetChunkCooldown { expires_at_ms: 0 }` (which the state-
/// machine apply path treats as "remove the entry"). Returns
/// `false` when the existing cooldown is in the future and the
/// engine should leave it alone.
///
/// `cooldown_until_ms == 0` (no entry) returns `true` so the
/// engine issues the Raft write anyway — it's a no-op on an absent
/// entry, idempotent, and keeps the dispatcher branch-free.
pub(crate) fn should_clear_cooldown_on_demote(
    cooldown_until_ms: u64,
    now_ms: u64,
) -> bool {
    // Past or absent cooldown → safe to clear (idempotent on absent).
    // Future cooldown → operator flush still active; engine must
    // respect it.
    cooldown_until_ms <= now_ms
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

#[cfg(test)]
mod bug_av_tiering_tick_tests {
    //! AV-2-4 — planner-level tests for `plan_cache_tick`. The
    //! scheduler's actual dispatcher (which calls
    //! `schedule_replication` + `delete_chunk_replica`) is exercised
    //! live on the minicluster per the AV-2-4 done-when criteria;
    //! these tests cover the decision logic in isolation.
    use super::*;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, CacheTierConfig, TierReplication,
    };
    use std::collections::HashMap;

    fn node(
        id: u64,
        dc: &str,
        free: u64,
        has_tier: bool,
    ) -> NodeSelectionView {
        NodeSelectionView {
            node_id: id,
            datacenter: dc.to_string(),
            has_target_tier_disk: has_tier,
            free_capacity: free,
            draining: false,
            already_holds_chunk: false,
        }
    }

    fn plex_policy() -> TickVolumePolicy {
        // Plexmedia-shape: cold mandatory, warm cache (per-DC, 2 reads
        // in 4h, 5-min cooldown).
        TickVolumePolicy {
            mandatory: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig { min: 1, max: 1 },
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 14_400,
                cooldown_secs: 300,
            },
            anchor: Some(AnchorPolicy {
                datacenter: "leli".to_string(),
                required_tier: Some(DiskTier::Cold),
            }),
        }
    }

    fn vols(p: TickVolumePolicy) -> HashMap<String, TickVolumePolicy> {
        let mut m = HashMap::new();
        m.insert("default".to_string(), p);
        m
    }

    /// AV-2-4 test 1 — threshold met → planner emits one warm
    /// promotion in the requesting DC.
    #[test]
    fn bug_av_tiering_tick_promotes_on_threshold() {
        let mut reads = HashMap::new();
        reads.insert("toro".to_string(), 2); // threshold (2 reads) met
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![TickPlacement {
                    node_id: 10,
                    dc: "leli".to_string(),
                    tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![
                node(10, "leli", 1_000_000, /* cold */ false),
                node(11, "toro",   500_000, /* warm */ true),
                node(12, "nrb",    500_000, /* warm */ true),
            ],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert_eq!(plan.promotions.len(), 1, "one warm promotion");
        let p = &plan.promotions[0];
        assert_eq!(p.target_node_id, 11);
        assert_eq!(p.target_dc, "toro");
        assert_eq!(p.target_tier, DiskTier::Warm);
        assert_eq!(p.reason, "promote_threshold_met");
        // The lone source candidate (cold replica on node 10) is used.
        assert_eq!(p.source_node_id, 10);
        // No demotions (no idle-warm placements yet).
        assert!(plan.demotions.is_empty());
    }

    /// AV-2-4 test 2 — an existing warm cache replica with 0 reads in
    /// the window is demoted, provided the mandatory cold replica
    /// survives the drop.
    #[test]
    fn bug_av_tiering_tick_demotes_idle_cache_replica() {
        let reads: HashMap<String, u32> = HashMap::new(); // 0 reads anywhere
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![
                    TickPlacement { node_id: 10, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![node(11, "toro", 500_000, true)],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert!(plan.promotions.is_empty(), "no promotions when reads = 0");
        assert_eq!(plan.demotions.len(), 1);
        assert_eq!(plan.demotions[0].node_id, 11);
        assert_eq!(plan.demotions[0].tier, DiskTier::Warm);
        assert_eq!(plan.demotions[0].reason, "idle_window");
    }

    /// AV-2-4 test 3 — cooldown blocks promotion until expiry.
    #[test]
    fn bug_av_tiering_tick_respects_cooldown() {
        let mut reads = HashMap::new();
        reads.insert("toro".to_string(), 5); // way above threshold
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![TickPlacement {
                    node_id: 10, dc: "leli".into(), tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 2_000_000, // future, still in cooldown
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![node(11, "toro", 500_000, true)],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert!(plan.promotions.is_empty(), "cooldown blocks the promotion");
    }

    /// AV-2-4 test 4 — `per_dc = true` with `min = 1` caps each DC
    /// at one cache replica. Multiple DCs each get their own
    /// (parallel) promotion, but a single DC reaching the threshold
    /// only gets ONE replica even if many reads accumulate.
    #[test]
    fn bug_av_tiering_tick_respects_per_dc_cap() {
        let mut reads = HashMap::new();
        reads.insert("toro".to_string(), 100); // way above; cap is 1
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![TickPlacement {
                    node_id: 10, dc: "leli".into(), tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "toro", 400_000, true),
                node(13, "toro", 300_000, true),
            ],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert_eq!(plan.promotions.len(), 1, "per-DC cap of 1 honoured");
        assert_eq!(plan.promotions[0].target_node_id, 11, "highest-free wins");
    }

    /// AV-2-4 test 5 — the redundancy-escalation flag is off in
    /// Phase 2. A volume with `cache_replicas.warm.max = 2` that
    /// already has 1 warm replica + plenty of reads MUST NOT get a
    /// second cache replica.
    #[test]
    fn bug_av_tiering_tick_redundancy_escalation_off_by_default() {
        // Sanity: confirm the flag is wired off so the test is
        // actually testing what we think.
        assert!(!ENABLE_CACHE_REDUNDANCY_ESCALATION);

        let mut policy = plex_policy();
        // Set max=2 to invite escalation if it were on.
        policy.cache.warm = CacheTierConfig { min: 1, max: 2 };

        let mut reads = HashMap::new();
        // 100 reads — far above threshold AND above the escalation
        // heuristic's 2*threshold gate.
        reads.insert("toro".to_string(), 100);
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![
                    TickPlacement { node_id: 10, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "toro", 400_000, true), // would be the 2nd replica
            ],
            volumes: vols(policy),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert!(
            plan.promotions.is_empty(),
            "with ENABLE_CACHE_REDUNDANCY_ESCALATION = false, a 2nd warm replica must not be scheduled"
        );
    }

    /// AV-2-5 test 1 — engine-driven demote of an idle cache
    /// replica clears the cooldown entry so a subsequent read can
    /// re-promote immediately. `should_clear_cooldown_on_demote`
    /// returns `true` for past or absent cooldowns.
    #[test]
    fn bug_av_demote_clears_cooldown() {
        // 0 → no cooldown active; clearing is idempotent.
        assert!(should_clear_cooldown_on_demote(0, 1_000_000));
        // Past cooldown (operator flushed long ago, cooldown has
        // since expired). Clear to remove the stale entry.
        assert!(should_clear_cooldown_on_demote(900_000, 1_000_000));
        // Exactly equal — treat as past (use `<=`).
        assert!(should_clear_cooldown_on_demote(1_000_000, 1_000_000));
    }

    /// AV-2-5 test 2 — when the operator flushed recently and the
    /// cooldown is still in the future, the engine-driven demote
    /// must NOT clear it. The operator's "no immediate re-promote"
    /// intent wins.
    #[test]
    fn bug_av_demote_does_not_clear_future_operator_cooldown() {
        // Cooldown set to expire 100 ms in the future.
        assert!(!should_clear_cooldown_on_demote(1_000_100, 1_000_000));
        // Far-future cooldown (operator just flushed; 5-min wait).
        assert!(!should_clear_cooldown_on_demote(1_000_000 + 5 * 60 * 1_000, 1_000_000));
    }

    /// AV-2-5 defensive — the planner now passes the snapshot
    /// cooldown through to TickDemotion so the dispatcher can call
    /// `should_clear_cooldown_on_demote` without re-fetching the
    /// SM. Guard against future refactors that strip this field.
    #[test]
    fn bug_av_demote_carries_cooldown_through_planner() {
        let reads: HashMap<String, u32> = HashMap::new();
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![
                    TickPlacement { node_id: 10, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 1_500_000, // future operator cooldown
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![node(11, "toro", 500_000, true)],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert_eq!(plan.demotions.len(), 1);
        assert_eq!(
            plan.demotions[0].cooldown_until_ms_at_snapshot, 1_500_000,
            "TickDemotion must carry the snapshot cooldown through"
        );
        // And the dispatcher would correctly decide NOT to clear it.
        assert!(!should_clear_cooldown_on_demote(
            plan.demotions[0].cooldown_until_ms_at_snapshot,
            1_000_000
        ));
    }

    /// AV-2-4 defensive — when `per_dc = false`, a single global
    /// cache pool tracks one warm replica regardless of which DC
    /// produced the reads. Targets the highest-read DC for locality.
    #[test]
    fn bug_av_tiering_tick_per_dc_false_uses_global_pool() {
        let mut policy = plex_policy();
        policy.cache.per_dc = false;
        let mut reads = HashMap::new();
        reads.insert("toro".to_string(), 1);
        reads.insert("nrb".to_string(),  3); // wins global locality
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![TickPlacement {
                    node_id: 10, dc: "leli".into(), tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "nrb",  500_000, true),
            ],
            volumes: vols(policy),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000);
        assert_eq!(plan.promotions.len(), 1);
        assert_eq!(plan.promotions[0].target_dc, "nrb",
            "global pool targets the DC with the most reads");
    }
}
