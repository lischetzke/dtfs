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

/// CO-4-4 — one legacy-temperature migration decision, collected under
/// the `access_stats` lock and dispatched after the lock is dropped so the
/// evaluate loop never holds the lock across a dispatch/query await.
struct LegacyMigration {
    chunk_id: String,
    file_path: String,
    current_node: u64,
    current_tier: DiskTier,
    target_tier: DiskTier,
    reads_by_dc: HashMap<String, u32>,
    /// true = demote (toward colder tier), false = promote.
    demote: bool,
    old_temp: DataTemperature,
    new_temp: DataTemperature,
}

/// CO-4-6 — does this volume run an active AV cache policy? When true the
/// volume's chunks are driven SOLELY by the AV cache tick; the legacy
/// temperature-profile demote/promote SKIPS them so an operator flush
/// (future cooldown) is not undone by a legacy promote re-creating the
/// warm replica. Mirrors the `build_av_cache_snapshot` filter: at least
/// one cache tier with `max > 0` AND a non-zero `promote_window_secs`.
pub(crate) fn volume_has_active_cache_policy(
    cache: &dtfs_common::types::CacheReplication,
) -> bool {
    let any_tier =
        cache.hot.max > 0 || cache.warm.max > 0 || cache.cold.max > 0 || cache.tape.max > 0;
    any_tier && cache.promote_window_secs > 0
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
    /// Each heartbeat appends a fresh `ReadSample` carrying the
    /// per-harvest delta (AV-4-1c fix); `tick_av_cache` reads
    /// these for promotion decisions.
    read_windows: RwLock<HashMap<(String, String), Vec<ReadSample>>>,
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

            // Bug CN — bound the tiering in-memory state to the live working
            // set. `access_stats` + `read_windows` are append-keyed by chunk_id
            // and were only pruned for chunks still present; a deleted /
            // re-uploaded chunk (content-addressed → new id) left its entries
            // behind forever, so a folder-readahead promote load over many
            // chunks grew them without bound. Sweep dead-chunk entries each
            // tick. Cheap (one sm.files pass) and off the request hot path.
            //
            // REV-1-1 — the sweep runs FIRST and UNCONDITIONALLY, ahead of
            // both the `enabled` gate and the new leader gate. `report_access`
            // feeds these maps on every manager that serves a heartbeat,
            // whether or not tiering is enabled and whether or not this node
            // is the leader, so gating the prune on either would leave them
            // growing forever on exactly the nodes that never evaluate.
            self.sweep_dead_chunk_state().await;

            let cfg = self.config.read().await;
            if !cfg.enabled {
                debug!("Tiering disabled, skipping evaluation");
                continue;
            }
            drop(cfg);

            // REV-1-1 — leader-only. `evaluate` / `tick_av_cache` dispatch
            // DIRECT data-node delete RPCs (the AV demote path) that no Raft
            // rejection can undo, and a follower's read-window is always
            // empty — so an ungated follower plans an idle demote for every
            // cache replica cluster-wide, deletes the bytes, and then fails
            // to record the removal.
            if !self.scheduler.is_leader() {
                debug!("REV-1-1: tiering evaluation skipped: not leader");
                continue;
            }

            if let Err(e) = self.evaluate().await {
                warn!("Tiering evaluation failed: {}", e);
            }
        }
    }

    /// Bug CN — drop `access_stats` / `read_windows` entries for chunks no
    /// longer in the namespace, bounding both maps to the live working set.
    async fn sweep_dead_chunk_state(&self) {
        let live: std::collections::HashSet<String> = {
            let sm = self.state_data.read().await;
            sm.files
                .values()
                .flat_map(|fm| fm.chunks.iter().map(|c| c.chunk_hash.clone()))
                .collect()
        };
        let (stats_pruned, windows_pruned) = {
            let mut stats = self.access_stats.write().await;
            let mut windows = self.read_windows.write().await;
            retain_live_chunk_state(&mut stats, &mut windows, &live)
        };
        if stats_pruned > 0 || windows_pruned > 0 {
            debug!(
                stats_pruned,
                windows_pruned, "Bug CN: swept dead-chunk tiering state"
            );
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

        // Bug AV (AV-4-1c fix) — feed per-(chunk, dc) reads into the
        // read-window store. The data-node's `harvest_per_chunk`
        // reports the count of reads SINCE THE LAST HARVEST and
        // swap-resets the counter to 0; the manager must treat the
        // incoming value as a per-heartbeat delta and push it
        // directly. The earlier AV-2-4 implementation compared the
        // incoming value to a stored "prev" and pushed `new - old`,
        // which silently dropped every read after the first
        // heartbeat (the harvest reset makes "new" smaller than
        // "old" on every subsequent heartbeat). Regression-tested
        // by `bug_av_4_1c_heartbeat_treated_as_delta_not_cumulative`.
        let mut windows = self.read_windows.write().await;
        apply_heartbeat_reads(chunk_id, &reads_by_dc, now_ms, &mut windows);
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
        let now_ms = now_millis();

        // CO-4-6 — resolve, once per tick, which volumes run an active AV
        // cache policy and the file_path -> volume_id map. A volume with a
        // live `cache_replicas` policy is driven SOLELY by `tick_av_cache`;
        // the legacy temperature path must skip its chunks, or after an
        // operator flush (which sets a FUTURE cooldown) the legacy promote
        // re-creates the warm replica during the cooldown window. Read the
        // SM once here so we never hold the SM lock across the dispatch
        // awaits below.
        let (av_cache_volumes, path_to_volume) = {
            let sm = self.state_data.read().await;
            let av: std::collections::HashSet<String> = sm
                .volumes
                .iter()
                .filter(|(_, v)| volume_has_active_cache_policy(&v.cache_replicas))
                .map(|(id, _)| id.clone())
                .collect();
            let paths: HashMap<String, String> = sm
                .files
                .iter()
                .map(|(p, fm)| (p.clone(), fm.volume_id.clone()))
                .collect();
            (av, paths)
        };

        // CO-4-4 — collect every legacy migration decision UNDER the
        // `access_stats` write lock (classification mutates the profile),
        // then DROP the lock before any dispatch/query await. Pre-fix the
        // loop held the write lock across `find_target_node` /
        // `would_violate_anchor` / `schedule_replication` awaits, so a
        // large evaluate stalled every concurrent `report_access`
        // (heartbeat) for the whole dispatch duration.
        let (decisions, deferred): (Vec<LegacyMigration>, usize) = {
            let cfg = self.config.read().await;
            let mut stats = self.access_stats.write().await;
            let mut out = Vec::new();
            let mut deferred = 0usize;
            for (_chunk_id, profile) in stats.iter_mut() {
                let old_temp = profile.current_temperature;
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
                // CO-4-6 — skip the legacy path for chunks whose volume has
                // an active AV cache policy; `tick_av_cache` owns them.
                if let Some(vid) = path_to_volume.get(&profile.file_path) {
                    if av_cache_volumes.contains(vid) {
                        continue;
                    }
                }
                // REV-4-8 — bound the legacy path. It schedules a migration
                // for essentially every chunk that gets read at all: on the
                // Plex workload a chunk crosses Cold -> Warm on its FIRST
                // read, so one tick could enqueue one migration per chunk read
                // since the last tick, an unbounded flood driven straight off
                // read traffic. The AV cache engine (`tick_av_cache`) owns
                // demand-driven placement; this path only covers volumes with
                // no `cache_replicas` policy, so a slow trickle is enough.
                //
                // The deferral is deliberately NOT recorded in the profile:
                // restoring `old_temp` makes the next tick re-derive exactly
                // this decision, so the backlog drains at a fixed rate instead
                // of being silently dropped.
                if out.len() >= MAX_LEGACY_MIGRATIONS_PER_TICK {
                    profile.current_temperature = old_temp;
                    deferred += 1;
                    continue;
                }

                let demote = tier_rank(target_tier) > tier_rank(profile.current_tier);
                out.push(LegacyMigration {
                    chunk_id: profile.chunk_id.clone(),
                    file_path: profile.file_path.clone(),
                    current_node: profile.current_node,
                    current_tier: profile.current_tier,
                    target_tier,
                    reads_by_dc: profile.reads_by_dc.clone(),
                    demote,
                    old_temp,
                    new_temp,
                });
            }
            (out, deferred)
        };

        if deferred > 0 {
            // AGENTS.md: never cap silently.
            warn!(
                planned = decisions.len(),
                deferred,
                max = MAX_LEGACY_MIGRATIONS_PER_TICK,
                "REV-4-8: legacy tier-migration budget reached for this tick; \
                 the remaining decisions re-derive on the next tick"
            );
        }

        let mut tier_up = 0u32;
        let mut tier_down = 0u32;
        for d in &decisions {
            let direction = if d.demote {
                tier_down += 1;
                "demote"
            } else {
                tier_up += 1;
                "promote"
            };

            info!(
                "Tiering {}: chunk {} ({:?} -> {:?}, tier {:?} -> {:?})",
                direction,
                d.chunk_id,
                d.old_temp,
                d.new_temp,
                d.current_tier,
                d.target_tier,
            );

            // Find a target node with the right tier.
            let target_node = self
                .find_target_node(d.target_tier, d.current_node, &d.reads_by_dc)
                .await;

            if let Some(target) = target_node {
                // P0-10: anchor-protect. If the volume has an anchor DC set,
                // refuse migrations that would leave 0 replicas in that DC.
                let anchor_violated = self
                    .would_violate_anchor_for(
                        &d.file_path,
                        &d.chunk_id,
                        d.current_node,
                        target,
                    )
                    .await;
                if anchor_violated {
                    self.audit.log(
                        "tiering",
                        "tier.anchor_protected",
                        &format!(
                            "chunk={} path={} refused {:?}->{:?}",
                            d.chunk_id, d.file_path, d.current_tier, d.target_tier
                        ),
                        "refused",
                    );
                    if let Some(alerts) = &self.alerts {
                        alerts.fire(
                            crate::alerts::AlertKind::AnchorViolated,
                            "tier migration refused — would strand anchor DC",
                            serde_json::json!({
                                "chunk_id": d.chunk_id,
                                "file_path": d.file_path,
                                "source_tier": format!("{:?}", d.current_tier),
                                "target_tier": format!("{:?}", d.target_tier),
                                "target_node": target,
                            }),
                        );
                    }
                    continue;
                }

                self.scheduler
                    .schedule_replication(
                        &d.chunk_id,
                        d.current_node,
                        target,
                        d.target_tier,
                    )
                    .await;

                self.audit.log(
                    "tiering",
                    &format!("tier.{direction}"),
                    &format!(
                        "chunk={} {:?}->{:?}",
                        d.chunk_id, d.current_tier, d.target_tier
                    ),
                    "scheduled",
                );
            } else {
                warn!(
                    "No suitable target node found for tier {:?} migration of chunk {}",
                    d.target_tier, d.chunk_id
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

        // AV-4-5 — read the runtime flag under the same `config`
        // RwLock used by `update_config` so a `reload_config` call
        // takes effect on the next tick.
        let enable_redundancy_escalation = self
            .config
            .read()
            .await
            .enable_cache_redundancy_escalation;
        let plan = plan_cache_tick(&snapshot, now_ms, enable_redundancy_escalation);

        // CO-1-2 [HIGH anchor] — surface any demote the planner refused
        // because it would have stranded the volume's anchor DC. The
        // planner decides the refusal purely (`demote_preserves_anchor`);
        // here we emit the `tier.anchor_protected` audit + AnchorViolated
        // alert so an operator sees the protection fire. Logged before the
        // early-return so a tick that ONLY produced refusals is still
        // visible.
        for r in &plan.anchor_protected {
            self.audit.log(
                "tiering",
                "tier.anchor_protected",
                &format!(
                    "chunk={} file={} idx={} node={} disk={} tier={:?} refused idle-demote",
                    r.chunk_id, r.file_path, r.chunk_index, r.node_id, r.disk_id, r.tier
                ),
                "refused",
            );
            if let Some(alerts) = &self.alerts {
                alerts.fire(
                    crate::alerts::AlertKind::AnchorViolated,
                    "cache demote refused — would strand anchor DC",
                    serde_json::json!({
                        "chunk_id": r.chunk_id,
                        "file_path": r.file_path,
                        "chunk_index": r.chunk_index,
                        "node_id": r.node_id,
                        "disk_id": r.disk_id,
                        "tier": format!("{:?}", r.tier),
                    }),
                );
            }
        }

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

        // -- AV-2-5 — Dispatch demotions. CO-1-3 — Raft-remove-FIRST
        // ordering: RemoveChunkLocation Raft write → DeleteChunkReplica
        // RPC → conditional cooldown clear. De-locating in the SM before
        // touching bytes makes the demote atomic at the metadata layer:
        // on a Raft failure we issue no byte delete (the live replica is
        // untouched), and on a post-commit RPC failure the leftover
        // on-disk bytes are now a "chunk on a node's disk with no
        // location entry for that node" that the CO-3-2 orphan reaper
        // (`chunk_delocated_on_node` / `orphan_scrub_tick`) collects
        // after the durable-mtime grace. We NEVER roll back a committed
        // Raft remove. The reverse order (byte-delete first) could leave
        // bytes gone but the location still advertised as live, which
        // mis-serves reads / strands the anchor. The conditional check
        // (`should_clear_cooldown_on_demote`) honours an operator flush
        // whose cooldown is still in the future.
        // AV-5-5 — grace window must cover the full replication
        // cycle plus a margin so a just-completed PutChunk has time
        // to be observed (or operator-flushed) before the next
        // idle-demote tick yanks it. Replication: scheduler tick =
        // 30s × 4 states ≈ 120s; add `promote_window_secs` headroom
        // (so reads accumulated during replication remain "fresh"
        // when the planner re-evaluates).
        let promote_window_max = self
            .state_data
            .read()
            .await
            .volumes
            .values()
            .map(|v| v.cache_replicas.promote_window_secs)
            .max()
            .unwrap_or(60);
        let grace_ms = promote_window_max
            .saturating_add(180)
            .saturating_mul(1_000);
        for d in &plan.demotions {
            // AV-5-5 dispatcher re-check #1: between the planner's
            // snapshot and now, a replication.complete may have
            // landed a fresh ChunkLocationEntry for this file. Skip
            // the delete if the file's `updated_at_ms` is within
            // the grace window — otherwise we yank a just-added
            // warm replica and the cache lifecycle yo-yos.
            let recently_updated = {
                let sm = self.state_data.read().await;
                sm.files
                    .get(&d.file_path)
                    .map(|fm| now_ms.saturating_sub(fm.updated_at_ms) < grace_ms)
                    .unwrap_or(false)
            };
            if recently_updated {
                continue;
            }
            // AV-5-5 dispatcher re-check #2: also skip when there's
            // an active replication targeting THIS (chunk, node)
            // pair. The Verifying / Completing / Done sequence can
            // span 90s+ and a stale demote plan would otherwise
            // race the PutChunk write, deleting the file off disk
            // mid-replication.
            if self
                .scheduler
                .has_active_replication_to(&d.chunk_id, d.node_id)
                .await
            {
                continue;
            }
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

            // 2+3. Raft-remove-FIRST de-location via `delocate_replica`
            //    (CO-1-3): RemoveChunkLocation Raft write (with the volume
            //    anchor as `enforce_anchor`, CO-1-2 defence-in-depth)
            //    THEN the DeleteChunkReplica byte delete. On a Raft
            //    failure NO byte delete is issued (the replica is
            //    untouched in the SM + on disk); on a post-commit RPC
            //    failure the leftover bytes are now "on a node's disk
            //    with no location entry for that node" and the CO-3-2
            //    orphan reaper reclaims them after the mtime grace. The
            //    committed Raft remove is NEVER rolled back.
            let delocate = crate::scheduler::SchedulerDelocate {
                sched: &*self.scheduler,
                remove_req: dtfs_raft::ClientRequest::RemoveChunkLocation {
                    path: d.file_path.clone(),
                    chunk_index: d.chunk_index,
                    node_id: d.node_id.to_string(),
                    // CO-1-1 — drop only this (node, disk) warm cache entry.
                    disk_id: d.disk_id,
                    enforce_anchor: d.enforce_anchor.clone(),
                },
                node_addr,
                chunk_id: d.chunk_id.clone(),
                disk_id: d.disk_id,
                tier: d.tier.to_string(),
            };
            match crate::scheduler::delocate_replica(&delocate).await {
                crate::scheduler::DelocateOutcome::RemoveFailed => {
                    warn!(
                        chunk = %d.chunk_id,
                        "Bug AV demote: RemoveChunkLocation Raft write failed"
                    );
                    // Raft remove failed: replica still live, retry next
                    // tick. Skip the cooldown bookkeeping below.
                    continue;
                }
                crate::scheduler::DelocateOutcome::Done => {
                    self.audit.log(
                        "tiering",
                        "cache.demote",
                        &format!(
                            "chunk={} file={} idx={} node={} tier={:?}",
                            d.chunk_id, d.file_path, d.chunk_index, d.node_id, d.tier
                        ),
                        d.reason,
                    );
                }
                crate::scheduler::DelocateOutcome::RemovedBytesLeftover => {
                    warn!(
                        chunk = %d.chunk_id,
                        node = d.node_id,
                        "Bug AV demote: DeleteChunkReplica failed after Raft remove (orphan reaper will reclaim leftover bytes)"
                    );
                    self.audit.log(
                        "tiering",
                        "cache.demote",
                        &format!(
                            "chunk={} file={} idx={} node={} tier={:?}",
                            d.chunk_id, d.file_path, d.chunk_index, d.node_id, d.tier
                        ),
                        "rpc_failed_after_remove",
                    );
                    // Fall through: the SM entry is gone (de-located), so
                    // the cooldown bookkeeping below still applies.
                }
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
        for (id, info) in sm.nodes.iter() {
            id_to_dc.insert(*id, info.location.datacenter.clone());
            // CO-4-5 — emit one view per (node, distinct serviceable
            // tier). `node_selection_view` computes `has_target_tier_disk`
            // honestly per tier and excludes Draining/Down/Failed/
            // Quarantined nodes, so the promotion planner only ever
            // targets a node that can actually hold the cache replica.
            let mut seen_tiers: std::collections::HashSet<DiskTier> =
                std::collections::HashSet::new();
            for disk in &info.disks {
                if !seen_tiers.insert(disk.tier) {
                    continue;
                }
                let view = node_selection_view(*id, info, disk.tier);
                if view.has_target_tier_disk {
                    node_views.push(view);
                }
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
            let _ = path;
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
                    // CNF-1 — stable disk_id key, never Vec index.
                    let tier = sm
                        .nodes
                        .get(&nid)
                        .and_then(|n| n.disk(e.disk_id))
                        .map(|d| d.tier);
                    let Some(t) = tier else { continue };
                    placements.push(TickPlacement { node_id: nid, disk_id: e.disk_id, dc, tier: t });
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
                    file_updated_at_ms: fm.updated_at_ms,
                });
            }
        }
        drop(sm);

        TickSnapshot { chunks, nodes: node_views, volumes }
    }

    /// P0-10: return `true` if moving `file_path`'s chunk to `target_node`
    /// would leave the volume's anchor DC with zero replicas of this chunk.
    ///
    /// Logic:
    /// 1. Look up the file's `volume_id` from `sm.files[file_path]`.
    /// 2. Resolve the volume's anchor via `effective_anchor()` (CO-4-7:
    ///    reads the resolved `AnchorPolicy` so a volume that set the new
    ///    `anchor` field — not just the legacy `anchor_datacenter` string
    ///    — is honoured). Only the anchor *datacenter* is enforced here;
    ///    a tier-pinned anchor (`required_tier`) is still enforced solely
    ///    on the source-removal path (`RemoveChunkLocation`
    ///    `enforce_anchor`), since this additive copy never drops bytes.
    /// 3. If anchor is None → never violated (no constraint).
    /// 4. The tiering path schedules an ADDITIVE copy
    ///    (`schedule_replication`, source never removed — § A.6 #6), so
    ///    `anchor_violated_after_copy(.., is_additive=true)` does NOT
    ///    subtract the source. Pre-CO-4-7 this modelled the copy as a
    ///    move and a sole-anchor LELI->TORO promote was wrongly refused.
    ///
    /// CO-4-4 — takes the chunk's identity by value (was `&ChunkAccessProfile`)
    /// so the caller can drop the `access_stats` lock before this SM-reading
    /// await runs.
    async fn would_violate_anchor_for(
        &self,
        file_path: &str,
        chunk_id: &str,
        current_node: u64,
        target_node: u64,
    ) -> bool {
        let sm = self.state_data.read().await;
        let volume_id = match sm.files.get(file_path) {
            Some(meta) => meta.volume_id.clone(),
            None => return false,
        };
        let anchor = match sm
            .volumes
            .get(&volume_id)
            .and_then(|v| v.effective_anchor())
        {
            Some(a) if !a.datacenter.is_empty() => a.datacenter,
            _ => return false,
        };
        let file_meta = match sm.files.get(file_path) {
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

        // Datacenter of every current replica of this chunk.
        let mut replica_dcs: Vec<String> = Vec::new();
        for ent in file_meta.chunks.iter() {
            if ent.chunk_hash != chunk_id {
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
            if let Some(d) = dc {
                replica_dcs.push(d);
            }
        }

        let source_dc = id_to_dc.get(&current_node).cloned();
        let target_dc = id_to_dc.get(&target_node).cloned();
        anchor_violated_after_copy(
            &replica_dcs,
            &anchor,
            source_dc.as_deref(),
            target_dc.as_deref(),
            // The tiering migration is additive (copy, no source removal).
            true,
        )
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

/// REV-4-8 — maximum legacy (temperature-based) tier migrations planned in
/// one tick.
///
/// The legacy path classifies every profile the heartbeats reported and, on
/// any temperature change that implies a different tier, schedules a
/// replication. On the Plex workload nearly every chunk that gets read at all
/// crosses Cold -> Warm on its first read, so a single tick could enqueue one
/// migration per chunk read since the last tick — an unbounded flood off read
/// traffic, straight into the (now bounded) scheduler task map.
///
/// The path is bounded rather than removed: `build_av_cache_snapshot` skips
/// volumes without a `cache_replicas` policy, so deleting it would silently
/// strip tiering from those volumes.
pub(crate) const MAX_LEGACY_MIGRATIONS_PER_TICK: usize = 8;

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

/// CO-4-17 — hard cap on the number of `(chunk_id, dc)` rings the
/// read-window store keeps, and on the samples retained per ring.
/// `apply_heartbeat_reads` enforces both so a flood of distinct chunk
/// reads (or a single hot chunk read every heartbeat) cannot grow the
/// store without bound between the once-per-tick dead-chunk sweep. The
/// key cap mirrors the data-node's `MAX_CHUNK_DC_ENTRIES`.
pub(crate) const MAX_READ_WINDOW_KEYS: usize = 10_000;
/// CO-4-17 — per-ring sample cap. A 30 s heartbeat over a 4 h promote
/// window is ~480 samples; this keeps a runaway window (e.g. a stuck
/// clock or a window never queried) from growing unbounded.
pub(crate) const MAX_READ_WINDOW_SAMPLES: usize = 1_024;

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

/// Bug AV (AV-4-1c) — pure helper that ingests a per-heartbeat
/// `reads_by_dc` payload into the read-window store. The data-
/// node's `harvest_per_chunk` returns the count of reads SINCE
/// the last harvest and swap-resets the counter to 0, so each
/// incoming value is a delta — push it directly. Zero counts
/// are ignored (no point creating an empty bucket).
///
/// Extracted as a free function so `report_access` becomes a
/// thin wrapper and the delta-semantics live in one
/// unit-testable place. Regressions surface in
/// `bug_av_4_1c_heartbeat_treated_as_delta_not_cumulative` and
/// friends.
pub(crate) fn apply_heartbeat_reads(
    chunk_id: &str,
    reads_by_dc: &HashMap<String, u32>,
    now_ms: u64,
    windows: &mut HashMap<(String, String), Vec<ReadSample>>,
) {
    for (dc, count) in reads_by_dc.iter() {
        if *count == 0 {
            continue;
        }
        let key = (chunk_id.to_string(), dc.clone());
        // CO-4-17 — bound the number of rings. When the store is at the
        // key cap and this is a NEW key, evict the ring whose newest
        // sample is the oldest (least recently active). Without this the
        // map only shrank in the once-per-tick dead-chunk sweep, so a
        // flood of distinct chunk reads inside one tick grew it without
        // bound.
        if !windows.contains_key(&key) && windows.len() >= MAX_READ_WINDOW_KEYS {
            if let Some(stalest) = windows
                .iter()
                .min_by_key(|(_, ring)| ring.iter().map(|s| s.ts_ms).max().unwrap_or(0))
                .map(|(k, _)| k.clone())
            {
                windows.remove(&stalest);
            }
        }
        let ring = windows.entry(key).or_default();
        ring.push(ReadSample { ts_ms: now_ms, count: *count });
        // CO-4-17 — bound the per-ring sample count. A ring is normally
        // pruned to `window_secs` on query (`read_window_count`), but a
        // window never queried (e.g. a chunk whose volume has no cache
        // policy) would otherwise append forever. Drop the oldest
        // samples once the ring exceeds the cap.
        if ring.len() > MAX_READ_WINDOW_SAMPLES {
            let drop_n = ring.len() - MAX_READ_WINDOW_SAMPLES;
            ring.drain(0..drop_n);
        }
    }
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

/// Bug CN — retain only the tiering state for chunks still present in the
/// namespace. `access_stats` (keyed chunk_id) and `read_windows` (keyed
/// `(chunk_id, dc)`) both grow per chunk read and were only pruned for LIVE
/// chunks during the tick, so entries for deleted / re-uploaded chunks
/// (content-addressed → new id) leaked forever. Sweeping both against the live
/// chunk set bounds them to the working set. Pure (no I/O) so the bound is
/// unit-tested directly. Returns `(access_stats_pruned, read_windows_pruned)`.
fn retain_live_chunk_state(
    access_stats: &mut HashMap<String, ChunkAccessProfile>,
    read_windows: &mut HashMap<(String, String), Vec<ReadSample>>,
    live: &std::collections::HashSet<String>,
) -> (usize, usize) {
    let a0 = access_stats.len();
    access_stats.retain(|cid, _| live.contains(cid));
    let w0 = read_windows.len();
    read_windows.retain(|(cid, _), _| live.contains(cid));
    (a0 - access_stats.len(), w0 - read_windows.len())
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
/// escalation pass that ramps toward `max` is opt-in via the
/// runtime `TieringConfig::enable_cache_redundancy_escalation`
/// flag (default `false`; AV-4-5 promoted it from a compile-time
/// const so operators can flip it without rebuilding).
pub(crate) fn plan_cache_promotion(
    read_count_in_window: u32,
    tier_cfg: &dtfs_common::types::CacheTierConfig,
    current_cache_replicas_in_dc: u32,
    cooldown_until_ms: u64,
    promote_reads_threshold: u32,
    now_ms: u64,
) -> u32 {
    // Gate 0 (Bug BV-2) — `promote_reads == 0` means promotion DISABLED,
    // not "promote on every read". Without this, gate 3's
    // `read_count_in_window < 0` is always false (u32), so a chunk
    // promotes on a single read — or even zero reads — turning an
    // accidentally-zeroed threshold into a hyperactive tiering engine.
    // `0` is the documented disabled sentinel, matching the engine-tick
    // gate on `promote_window_secs == 0`.
    if promote_reads_threshold == 0 {
        return 0;
    }
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
    /// The tier this view was materialised for. The engine emits one
    /// view per (node, serviceable tier); the promotion planner only
    /// considers views whose `tier` matches the tier it is filling, so
    /// a cold-only node is never offered as a warm-cache target.
    pub tier: DiskTier,
    /// Set true when the engine has confirmed the node hosts at
    /// least one disk of `tier` and that disk is serviceable (not
    /// Failed/Removed).
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

/// CO-4-7 — pure anchor check for a copy operation. `replica_dcs` is
/// the datacenter of every current replica of the chunk. The result is
/// whether the anchor-DC replica count *after* the copy would be 0:
///
/// - additive copy (`is_additive=true`, e.g. cache promotion via
///   `schedule_replication` — the source is never removed, § A.6 #6):
///   the source is NOT subtracted; only a target landing in the anchor
///   DC adds one. Pre-CO-4-7 the promote path modelled this as a move
///   and subtracted the source, so a sole-anchor LELI->TORO promote
///   computed anchor_count 0 and was wrongly refused (+ AnchorViolated
///   spam).
/// - move/demote (`is_additive=false`): the source replica leaves, so a
///   source in the anchor DC subtracts one (genuine anchor protection
///   for tier-demote / migration — unchanged).
///
/// Returns true iff the post-copy anchor count would be 0 (violation).
pub(crate) fn anchor_violated_after_copy(
    replica_dcs: &[String],
    anchor_dc: &str,
    source_dc: Option<&str>,
    target_dc: Option<&str>,
    is_additive: bool,
) -> bool {
    if anchor_dc.is_empty() {
        return false;
    }
    let mut anchor_count: i64 = replica_dcs
        .iter()
        .filter(|dc| dc.as_str() == anchor_dc)
        .count() as i64;
    // Only a move removes the source replica from its DC.
    if !is_additive && source_dc == Some(anchor_dc) {
        anchor_count -= 1;
    }
    if target_dc == Some(anchor_dc) {
        anchor_count += 1;
    }
    anchor_count <= 0
}

/// CO-4-5 — project a node into a `NodeSelectionView` for a specific
/// `target_tier`. `has_target_tier_disk` is true iff the node hosts at
/// least one serviceable (not Failed/Removed) disk of that tier, and
/// `draining` is set for any health state that disqualifies the node
/// from receiving a new replica (Draining/Down/Failed/Quarantined) so
/// the pure selector skips it. Pre-CO-4-5 the snapshot set
/// `has_target_tier_disk` unconditionally and only excluded Draining,
/// so promotions targeted cold-only or dead nodes and fast-failed
/// forever (the cache replica was never created).
pub(crate) fn node_selection_view(
    node_id: u64,
    info: &dtfs_raft::NodeInfo,
    target_tier: DiskTier,
) -> NodeSelectionView {
    use dtfs_common::types::{DiskStatus, HealthStatus};
    let has_target_tier_disk = info.disks.iter().any(|d| {
        d.tier == target_tier
            && !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed)
    });
    // Any health state that means "don't place a new replica here".
    // `draining` is the selector's catch-all exclusion flag.
    let excluded = matches!(
        info.health,
        HealthStatus::Draining
            | HealthStatus::Down
            | HealthStatus::Failed
            | HealthStatus::Quarantined
    );
    NodeSelectionView {
        node_id,
        datacenter: info.location.datacenter.clone(),
        tier: target_tier,
        has_target_tier_disk,
        free_capacity: info.total_capacity.saturating_sub(info.used_capacity),
        draining: excluded,
        already_holds_chunk: false,
    }
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

/// AV-4-5 — deprecated marker. The redundancy-escalation gate is now
/// the runtime `TieringConfig::enable_cache_redundancy_escalation`
/// flag (per-cluster, hot-reloadable). The const is kept for
/// documentation; new code paths MUST take the bool as a parameter
/// instead of reading this constant.
#[deprecated(note = "AV-4-5: use TieringConfig::enable_cache_redundancy_escalation runtime flag instead")]
#[allow(dead_code)]
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
    /// AV-5-5 — `FileMetadata.updated_at_ms` at snapshot time. The
    /// demote planner uses this as a freshly-added grace window:
    /// placements on a file whose metadata was touched in the last
    /// `promote_window_secs` are deferred from idle-demote so a just-
    /// completed cache replication isn't immediately undone by the
    /// next tick's planner (and an operator flush has time to see
    /// the SM entry).
    #[doc(hidden)]
    pub file_updated_at_ms: u64,
}

/// AV-2-4 — one placement of a chunk (node + DC + tier).
#[derive(Debug, Clone)]
pub(crate) struct TickPlacement {
    pub node_id: u64,
    /// CO-1-1 — the disk this placement lives on. Replica identity is
    /// (node_id, disk_id); a demote must target this exact disk so the
    /// cold anchor co-located on another disk of the same node survives.
    pub disk_id: u32,
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
    /// CO-1-1 — the disk holding the cache replica being demoted. The
    /// dispatcher passes it to the data-node delete and RemoveChunkLocation
    /// so only this (node, disk) copy is removed; a co-located cold anchor
    /// on a different disk of the same node is never touched.
    pub disk_id: u32,
    pub tier: DiskTier,
    pub reason: &'static str,
    /// AV-2-5 — the chunk's cooldown timestamp as observed in the
    /// planner snapshot. The dispatcher passes this through
    /// `should_clear_cooldown_on_demote` to decide whether the
    /// engine-driven demote should clear an existing entry (idle
    /// demote → clear so the next read re-promotes immediately;
    /// operator flush cooldown still active → leave alone).
    pub cooldown_until_ms_at_snapshot: u64,
    /// CO-1-2 — the volume's resolved anchor policy, passed straight
    /// through to the `RemoveChunkLocation` apply as `enforce_anchor`
    /// (defence-in-depth). The planner already refuses any demote that
    /// would strand the anchor (`demote_preserves_anchor`); this gives
    /// the state-machine apply path a second chance to refuse if the SM
    /// changed between snapshot and apply. `None` = no anchor policy.
    pub enforce_anchor: Option<dtfs_common::types::AnchorPolicy>,
}

/// CO-1-2 — a demote the planner refused because it would have
/// stranded the volume's anchor DC. Surfaced so the dispatcher (which
/// holds the audit log) can emit a `tier.anchor_protected` audit; the
/// refusal itself is decided purely in `demote_preserves_anchor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnchorProtectedDemote {
    pub chunk_id: String,
    pub chunk_index: u64,
    pub file_path: String,
    pub node_id: u64,
    pub disk_id: u32,
    pub tier: DiskTier,
}

/// AV-2-4 — output of the planner. Wrapped in a struct so the
/// engine can iterate both lists from a single return value.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TickPlan {
    pub promotions: Vec<TickPromotion>,
    pub demotions: Vec<TickDemotion>,
    /// CO-1-2 — demotes refused to protect the anchor DC.
    pub anchor_protected: Vec<AnchorProtectedDemote>,
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
    enable_redundancy_escalation: bool,
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
        plan_chunk_promotions(
            chunk,
            policy,
            &snapshot.nodes,
            now_ms,
            enable_redundancy_escalation,
            &mut plan.promotions,
        );

        // -- Demotions --
        plan_chunk_demotions(
            chunk,
            policy,
            now_ms,
            &mut plan.demotions,
            &mut plan.anchor_protected,
        );
    }
    plan
}

/// AV-2-4 helper — promotion decisions for one chunk.
fn plan_chunk_promotions(
    chunk: &TickChunk,
    policy: &TickVolumePolicy,
    nodes: &[NodeSelectionView],
    now_ms: u64,
    enable_redundancy_escalation: bool,
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
            let want = if enable_redundancy_escalation
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
            // CO-4-5 — only consider node views materialised for the
            // tier we're filling; a node's cold-disk view must never
            // satisfy a warm-cache promotion.
            let candidates: Vec<NodeSelectionView> = nodes
                .iter()
                .filter(|n| n.tier == *tier)
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
                        reason: if enable_redundancy_escalation
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

/// CO-1-2 pure kernel — would dropping `victim` from `chunk`'s
/// placements still leave the volume's anchor satisfied?
///
/// Returns `true` when it is safe to demote (anchor preserved or no
/// anchor policy), `false` when `victim` is the last placement that
/// satisfies the anchor (so the demote MUST be refused). The check
/// mirrors `would_violate_anchor_for` but operates on the planner's
/// `TickPlacement` view and uses `AnchorPolicy::is_satisfied_by`
/// (datacenter + optional `required_tier`).
pub(crate) fn demote_preserves_anchor(
    chunk: &TickChunk,
    victim: &TickPlacement,
    anchor: Option<&dtfs_common::types::AnchorPolicy>,
) -> bool {
    let Some(anchor) = anchor else {
        return true; // no anchor policy → no constraint
    };
    if anchor.datacenter.is_empty() {
        return true;
    }
    // Identity is (node_id, disk_id) — the demote removes exactly that
    // replica (CO-1-1). Any OTHER placement that satisfies the anchor
    // keeps the invariant.
    chunk.placements.iter().any(|p| {
        let is_victim = p.node_id == victim.node_id && p.disk_id == victim.disk_id;
        !is_victim && anchor.is_satisfied_by(&p.dc, Some(p.tier))
    })
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
    now_ms: u64,
    out: &mut Vec<TickDemotion>,
    anchor_protected: &mut Vec<AnchorProtectedDemote>,
) {
    if policy.cache.promote_window_secs == 0 {
        // Cache disabled cluster-wide on this volume; demotions
        // would be misleading.
        return;
    }
    // AV-5-5 — freshly-added grace. If the file's metadata was
    // touched within the last `promote_window_secs` (e.g. a
    // just-completed cache replication landed a new
    // ChunkLocationEntry), defer the idle-demote for one window
    // so:
    //  * an operator `flush_cache` between replication.complete
    //    and the next tick actually sees the warm replica in SM,
    //  * the planner doesn't yo-yo schedule-replicate ->
    //    immediately-demote on a chunk that's still warming up.
    let grace_ms = policy.cache.promote_window_secs.saturating_mul(1_000);
    if grace_ms > 0
        && chunk.file_updated_at_ms > 0
        && now_ms.saturating_sub(chunk.file_updated_at_ms) < grace_ms
    {
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
        // CO-1-2 [HIGH anchor] — never strand the LELI cold anchor. The
        // mandatory-tier check above does NOT cover the anchor invariant
        // (§ A.3.4): a chunk's ONLY anchor-DC replica can sit on a cache
        // tier while the mandatory copy lives in another DC, and the
        // idle-demote would then drop the last anchor copy. Skip the
        // placement-removal unless another placement still satisfies the
        // volume's `effective_anchor`. The dispatcher additionally passes
        // the anchor as `enforce_anchor` on the RemoveChunkLocation apply
        // (defence-in-depth, matching the AV-2-4 pattern).
        if !demote_preserves_anchor(chunk, placement, policy.anchor.as_ref()) {
            anchor_protected.push(AnchorProtectedDemote {
                chunk_id: chunk.chunk_id.clone(),
                chunk_index: chunk.chunk_index,
                file_path: chunk.file_path.clone(),
                node_id: placement.node_id,
                disk_id: placement.disk_id,
                tier: placement.tier,
            });
            continue;
        }
        out.push(TickDemotion {
            chunk_id: chunk.chunk_id.clone(),
            chunk_index: chunk.chunk_index,
            file_path: chunk.file_path.clone(),
            node_id: placement.node_id,
            disk_id: placement.disk_id,
            tier: placement.tier,
            reason: "idle_window",
            cooldown_until_ms_at_snapshot: chunk.cooldown_until_ms,
            // CO-1-2 — carry the anchor through for the apply-path re-check.
            enforce_anchor: policy.anchor.clone(),
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

    // Bug CN — `retain_live_chunk_state` drops `access_stats` + `read_windows`
    // entries for chunks no longer in the namespace, bounding both maps to the
    // live working set (prod 2026-06-10: dead-chunk tiering state leaked under
    // folder-readahead promote load and helped wedge the leader).
    #[test]
    fn bug_cn_retain_live_chunk_state_drops_dead_chunks() {
        use std::collections::{HashMap, HashSet};
        fn prof(cid: &str) -> ChunkAccessProfile {
            ChunkAccessProfile {
                chunk_id: cid.to_string(),
                file_path: String::new(),
                total_reads_1h: 0,
                total_reads_24h: 0,
                reads_by_dc: HashMap::new(),
                last_access_ms: 0,
                current_temperature: DataTemperature::Warm,
                current_tier: DiskTier::Warm,
                current_node: 0,
            }
        }
        fn reads(dc: &str, n: u32) -> HashMap<String, u32> {
            let mut m = HashMap::new();
            m.insert(dc.to_string(), n);
            m
        }
        let mut stats: HashMap<String, ChunkAccessProfile> = HashMap::new();
        stats.insert("live".into(), prof("live"));
        stats.insert("dead".into(), prof("dead"));

        let mut windows: HashMap<(String, String), Vec<ReadSample>> = HashMap::new();
        apply_heartbeat_reads("live", &reads("dc-a", 2), 1_000, &mut windows);
        apply_heartbeat_reads("dead", &reads("dc-a", 3), 1_000, &mut windows);

        let live: HashSet<String> = ["live".to_string()].into_iter().collect();
        let (sp, wp) = retain_live_chunk_state(&mut stats, &mut windows, &live);

        assert_eq!(sp, 1, "dead access_stats entry pruned");
        assert_eq!(wp, 1, "dead read_windows entry pruned");
        assert!(stats.contains_key("live") && !stats.contains_key("dead"));
        assert!(windows.contains_key(&("live".into(), "dc-a".into())));
        assert!(!windows.contains_key(&("dead".into(), "dc-a".into())));
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

    // ─────────────────────────────────────────────────────────────
    // Bug AV (AV-4-1c) — heartbeat-delta semantics
    // ─────────────────────────────────────────────────────────────
    //
    // Pre-fix, `report_access` treated the incoming heartbeat
    // `reads_by_dc` as a CUMULATIVE counter and pushed `new - old`
    // as the sample. The data-node's `harvest_per_chunk` already
    // returns a per-harvest delta (the counter is swap-reset to
    // 0 after each harvest), so the manager dropped every read
    // after the first heartbeat per chunk. The cache-promotion
    // threshold never fired in production and the AV-4-1 live
    // demo silently failed every run.
    //
    // The fix lives in `apply_heartbeat_reads` (free function);
    // these tests pin its semantics so a refactor can't regress
    // the contract.

    fn hb(entries: &[(&str, u32)]) -> std::collections::HashMap<String, u32> {
        let mut m = std::collections::HashMap::new();
        for (k, v) in entries { m.insert(k.to_string(), *v); }
        m
    }

    /// Regression #1 — three heartbeats reporting `(2, 3, 1)`
    /// reads-since-last-harvest in `dc-a` must accumulate to
    /// 6 total samples in the window. Pre-fix the manager would
    /// have stored 2 + max(0, 3-2) + max(0, 1-3) = 2 + 1 + 0 = 3,
    /// losing half the reads.
    #[test]
    fn bug_av_4_1c_heartbeat_treated_as_delta_not_cumulative() {
        let mut windows = std::collections::HashMap::new();
        let chunk = "x";
        apply_heartbeat_reads(chunk, &hb(&[("dc-a", 2)]), 1_000, &mut windows);
        apply_heartbeat_reads(chunk, &hb(&[("dc-a", 3)]), 6_000, &mut windows);
        apply_heartbeat_reads(chunk, &hb(&[("dc-a", 1)]), 11_000, &mut windows);

        let key = (chunk.to_string(), "dc-a".to_string());
        let samples = windows.get(&key).expect("dc-a bucket exists");
        let total: u32 = samples.iter().map(|s| s.count).sum();
        assert_eq!(
            total, 6,
            "three heartbeats of (2, 3, 1) reads must accumulate to 6; \
             treating the heartbeat value as a cumulative counter would \
             have produced 3 (silent regression of the cache-promotion \
             pipeline)"
        );
        // And the window count helper must see all 6 too.
        let counted = count_reads_in_window(samples, 11_000, 60);
        assert_eq!(counted, 6);
    }

    /// Regression #2 — a heartbeat carrying a zero count for a
    /// DC must NOT create an empty bucket. (Empty buckets would
    /// confuse the planner's `chunks.reads_in_window_by_dc.keys()
    /// .collect()` iteration into "saw activity here" when there
    /// was none.)
    #[test]
    fn bug_av_4_1c_zero_count_does_not_create_bucket() {
        let mut windows = std::collections::HashMap::new();
        apply_heartbeat_reads("x", &hb(&[("dc-a", 0)]), 1_000, &mut windows);
        assert!(
            windows.get(&("x".to_string(), "dc-a".to_string())).is_none(),
            "zero-count heartbeat must not create an empty bucket"
        );
    }

    /// Regression #3 — multiple DCs in the same heartbeat each
    /// land in their own bucket. Guards against accidental
    /// aggregation across DCs.
    #[test]
    fn bug_av_4_1c_per_dc_buckets_isolated() {
        let mut windows = std::collections::HashMap::new();
        apply_heartbeat_reads(
            "x",
            &hb(&[("dc-a", 2), ("dc-b", 5)]),
            1_000,
            &mut windows,
        );
        let dc_a = windows.get(&("x".to_string(), "dc-a".to_string())).unwrap();
        let dc_b = windows.get(&("x".to_string(), "dc-b".to_string())).unwrap();
        assert_eq!(dc_a.iter().map(|s| s.count).sum::<u32>(), 2);
        assert_eq!(dc_b.iter().map(|s| s.count).sum::<u32>(), 5);
    }

    /// Regression #4 — repeated heartbeats for the same chunk
    /// from DIFFERENT chunks must not collide. Guards the
    /// (chunk_id, dc) compound key.
    #[test]
    fn bug_av_4_1c_chunk_id_isolates_buckets() {
        let mut windows = std::collections::HashMap::new();
        apply_heartbeat_reads("chunk-a", &hb(&[("dc-a", 2)]), 1_000, &mut windows);
        apply_heartbeat_reads("chunk-b", &hb(&[("dc-a", 3)]), 1_000, &mut windows);
        assert_eq!(
            windows.get(&("chunk-a".to_string(), "dc-a".to_string()))
                .unwrap().iter().map(|s| s.count).sum::<u32>(),
            2,
        );
        assert_eq!(
            windows.get(&("chunk-b".to_string(), "dc-a".to_string()))
                .unwrap().iter().map(|s| s.count).sum::<u32>(),
            3,
        );
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

    /// Bug BV-2 — `promote_reads == 0` is DISABLED, not promote-on-every-read.
    /// Even a large read count (and a non-zero target tier) must not promote.
    #[test]
    fn bug_bv2_zero_threshold_disables_promotion() {
        // High read count, healthy tier, no current replicas — pre-fix this
        // promoted to `min`; with Gate 0 it stays 0.
        let n = plan_cache_promotion(
            /* read_count_in_window */ 100,
            /* tier_cfg */ &cfg(2, 2),
            /* current_cache_replicas_in_dc */ 0,
            /* cooldown_until_ms */ 0,
            /* promote_reads_threshold */ 0,
            /* now_ms */ 1_000_000,
        );
        assert_eq!(n, 0, "promote_reads=0 disables promotion");
        // And zero reads with zero threshold also stays disabled.
        assert_eq!(plan_cache_promotion(0, &cfg(2, 2), 0, 0, 0, 1_000_000), 0);
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
            // `select_promotion_target` ignores `tier`; the value is
            // irrelevant to these direct-selector tests.
            tier: DiskTier::Warm,
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
            // The plex/tier tests in this module all promote to Warm.
            tier: DiskTier::Warm,
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
                    disk_id: 0,
                    dc: "leli".to_string(),
                    tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![
                node(10, "leli", 1_000_000, /* cold */ false),
                node(11, "toro",   500_000, /* warm */ true),
                node(12, "nrb",    500_000, /* warm */ true),
            ],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
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
                // CO-1-1 / Bug BH — the cold anchor (disk 0) and the warm
                // cache copy (disk 1) are CO-LOCATED on node 10. The demote
                // must target the exact (node 10, disk 1) warm copy.
                placements: vec![
                    TickPlacement { node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 10, disk_id: 1, dc: "leli".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![node(10, "leli", 500_000, true)],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
        assert!(plan.promotions.is_empty(), "no promotions when reads = 0");
        assert_eq!(plan.demotions.len(), 1);
        assert_eq!(plan.demotions[0].node_id, 10);
        // CO-1-1 — the demote targets the warm disk (1), never the cold
        // anchor disk (0) on the same node.
        assert_eq!(plan.demotions[0].disk_id, 1);
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
                    node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 2_000_000, // future, still in cooldown
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![node(11, "toro", 500_000, true)],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
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
                    node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "toro", 400_000, true),
                node(13, "toro", 300_000, true),
            ],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
        assert_eq!(plan.promotions.len(), 1, "per-DC cap of 1 honoured");
        assert_eq!(plan.promotions[0].target_node_id, 11, "highest-free wins");
    }

    /// AV-2-4 test 5 (renamed for AV-4-5) — with the runtime
    /// escalation flag OFF, a volume with `cache_replicas.warm.max
    /// = 2` that already has 1 warm replica + plenty of reads
    /// MUST NOT get a second cache replica. The planner caps at
    /// `min`.
    #[test]
    fn bug_av_tiering_tick_redundancy_escalation_off() {
        let mut policy = plex_policy();
        // Set max=2 to invite escalation if the flag were on.
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
                    TickPlacement { node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, disk_id: 0, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "toro", 400_000, true), // would be the 2nd replica
            ],
            volumes: vols(policy),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
        assert!(
            plan.promotions.is_empty(),
            "with redundancy escalation flag OFF, a 2nd warm replica must not be scheduled"
        );
    }

    /// AV-4-5 test 1 — same snapshot, flag flipped ON. Planner must
    /// schedule the 2nd warm replica with the `promote_redundancy`
    /// reason code.
    #[test]
    fn bug_av_tiering_tick_redundancy_escalation_on() {
        let mut policy = plex_policy();
        policy.cache.warm = CacheTierConfig { min: 1, max: 2 };

        let mut reads = HashMap::new();
        reads.insert("toro".to_string(), 100);
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![
                    TickPlacement { node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, disk_id: 0, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "toro", 400_000, true), // gains the 2nd replica
            ],
            volumes: vols(policy),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, true);
        assert_eq!(plan.promotions.len(), 1,
            "flag ON must schedule the 2nd warm replica");
        assert_eq!(plan.promotions[0].target_node_id, 12,
            "the un-held warm node is picked");
        assert_eq!(plan.promotions[0].reason, "promote_redundancy",
            "escalation reason code");
    }

    /// AV-4-5 test 2 — the escalation flag does NOT bypass the
    /// already-existing safety checks. When no candidate warm-tier
    /// node exists in the DC (the only candidate already holds the
    /// chunk), no extra promotion is scheduled even though `max >
    /// min` and reads are far above the threshold.
    #[test]
    fn bug_av_tiering_redundancy_escalation_respects_anchor() {
        let mut policy = plex_policy();
        policy.cache.warm = CacheTierConfig { min: 1, max: 2 };

        let mut reads = HashMap::new();
        reads.insert("toro".to_string(), 100);
        let snapshot = TickSnapshot {
            chunks: vec![TickChunk {
                chunk_id: "h0".to_string(),
                chunk_index: 0,
                file_path: "/movies/x.mkv".to_string(),
                volume_id: "default".to_string(),
                placements: vec![
                    TickPlacement { node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, disk_id: 0, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            // Only 1 warm-tier node in toro, and it already holds
            // the chunk. Planner must not pick it again.
            nodes: vec![node(11, "toro", 500_000, true)],
            volumes: vols(policy),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, true);
        assert!(
            plan.promotions.is_empty(),
            "no spare warm-tier node in toro → no escalation, even with flag on"
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
                    TickPlacement { node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold },
                    TickPlacement { node_id: 11, disk_id: 0, dc: "toro".into(), tier: DiskTier::Warm },
                ],
                cooldown_until_ms: 1_500_000, // future operator cooldown
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![node(11, "toro", 500_000, true)],
            volumes: vols(plex_policy()),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
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
                    node_id: 10, disk_id: 0, dc: "leli".into(), tier: DiskTier::Cold,
                }],
                cooldown_until_ms: 0,
                reads_in_window_by_dc: reads,
                file_updated_at_ms: 0,
            }],
            nodes: vec![
                node(11, "toro", 500_000, true),
                node(12, "nrb",  500_000, true),
            ],
            volumes: vols(policy),
        };
        let plan = plan_cache_tick(&snapshot, 1_000_000, false);
        assert_eq!(plan.promotions.len(), 1);
        assert_eq!(plan.promotions[0].target_dc, "nrb",
            "global pool targets the DC with the most reads");
    }
}

#[cfg(test)]
mod audit_co_tests {
    //! CO-4-5 — `node_selection_view` honest tier + health check, so
    //! the cache-promotion planner stops targeting cold-only / dead
    //! nodes (which fast-fail forever, never creating the replica).
    use super::*;
    use dtfs_common::topology::Location;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole};
    use dtfs_raft::NodeInfo;

    fn disk(disk_id: u32, tier: DiskTier, status: DiskStatus) -> DiskInfo {
        DiskInfo {
            disk_id,
            uuid: format!("uuid-{disk_id}"),
            mount_path: format!("/mnt/d{disk_id}"),
            tier,
            total_bytes: 1_000,
            used_bytes: 0,
            status,
        }
    }

    // `dtfs_raft::NodeInfo` carries no `id` field (the node id is the
    // `StateMachineData.nodes` map key, passed separately to
    // `node_selection_view`).
    fn node(id: u64, dc: &str, health: HealthStatus, disks: Vec<DiskInfo>) -> NodeInfo {
        NodeInfo {
            role: NodeRole::Data,
            hostname: format!("node{id}"),
            grpc_addr: format!("http://10.0.0.{id}:9500"),
            location: Location::new("r", dc, "rack", &format!("host{id}")),
            disks,
            total_capacity: 1_000_000,
            used_capacity: 0,
            health,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    /// CO-4-5 — a cold-only node's Warm view reports
    /// `has_target_tier_disk=false`; a node with a serviceable warm
    /// disk reports true. Pre-fix the snapshot set this true
    /// unconditionally, so cold-only A looked like a valid warm target.
    #[test]
    fn co_4_5_cold_only_node_has_no_warm_target_disk() {
        let a = node(
            10,
            "leli",
            HealthStatus::Healthy,
            vec![disk(0, DiskTier::Cold, DiskStatus::Healthy)],
        );
        let b = node(
            11,
            "leli",
            HealthStatus::Healthy,
            vec![disk(0, DiskTier::Warm, DiskStatus::Healthy)],
        );
        let va = node_selection_view(10, &a, DiskTier::Warm);
        let vb = node_selection_view(11, &b, DiskTier::Warm);
        assert!(
            !va.has_target_tier_disk,
            "cold-only node must not advertise a warm target disk"
        );
        assert!(vb.has_target_tier_disk, "warm node must advertise warm");
        // So the pure selector picks B, never the cold-only A.
        let pick = select_promotion_target(&[va, vb], "leli");
        assert_eq!(pick, Some(11));
    }

    /// CO-4-5 — a Failed/Removed disk of the target tier doesn't count.
    #[test]
    fn co_4_5_failed_warm_disk_is_not_serviceable() {
        let n = node(
            10,
            "toro",
            HealthStatus::Healthy,
            vec![disk(0, DiskTier::Warm, DiskStatus::Failed)],
        );
        assert!(!node_selection_view(10, &n, DiskTier::Warm).has_target_tier_disk);
        let n2 = node(
            10,
            "toro",
            HealthStatus::Healthy,
            vec![disk(0, DiskTier::Warm, DiskStatus::Removed)],
        );
        assert!(!node_selection_view(10, &n2, DiskTier::Warm).has_target_tier_disk);
    }

    /// CO-4-5 — Down/Failed/Quarantined nodes are excluded
    /// (`draining=true` → selector skips). Only Draining was excluded
    /// pre-fix, so promotions targeted dead nodes.
    #[test]
    fn co_4_5_dead_nodes_excluded_from_selection() {
        for bad in [
            HealthStatus::Down,
            HealthStatus::Failed,
            HealthStatus::Quarantined,
            HealthStatus::Draining,
        ] {
            let n = node(
                10,
                "toro",
                bad,
                vec![disk(0, DiskTier::Warm, DiskStatus::Healthy)],
            );
            let v = node_selection_view(10, &n, DiskTier::Warm);
            assert!(v.draining, "{bad:?} node must be flagged excluded");
            assert_eq!(
                select_promotion_target(&[v], "toro"),
                None,
                "{bad:?} node must not be selected as a promotion target"
            );
        }
    }

    /// CO-4-7 — a sole-anchor additive promote LELI->TORO is NOT a
    /// violation: the cold LELI copy stays (additive), so the anchor DC
    /// still has 1 replica afterwards. Pre-fix the source was subtracted
    /// (modelled as a move) → anchor_count 0 → wrongly refused.
    #[test]
    fn co_4_7_additive_promote_does_not_strand_sole_anchor() {
        let replica_dcs = vec!["leli".to_string()];
        let violated = anchor_violated_after_copy(
            &replica_dcs,
            "leli",
            Some("leli"), // source (cold anchor)
            Some("toro"), // target (warm cache)
            true,         // additive copy
        );
        assert!(!violated, "additive promote must not strand the LELI anchor");
    }

    /// CO-4-7 — guard: a genuine MOVE off the sole anchor IS a
    /// violation (anchor protection for demote/migration preserved).
    #[test]
    fn co_4_7_move_off_sole_anchor_is_violation() {
        let replica_dcs = vec!["leli".to_string()];
        let violated = anchor_violated_after_copy(
            &replica_dcs,
            "leli",
            Some("leli"),
            Some("toro"),
            false, // move: source leaves
        );
        assert!(violated, "moving the sole anchor copy away must be refused");
    }

    /// CO-4-7 — a copy that lands IN the anchor DC is fine; a non-anchor
    /// move that keeps a second anchor replica is fine.
    #[test]
    fn co_4_7_anchor_count_arithmetic() {
        // Two anchor replicas, move one away -> still 1 -> ok.
        assert!(!anchor_violated_after_copy(
            &["leli".into(), "leli".into()],
            "leli",
            Some("leli"),
            Some("toro"),
            false,
        ));
        // No anchor replica yet, additive copy lands in anchor -> ok.
        assert!(!anchor_violated_after_copy(
            &["toro".into()],
            "leli",
            Some("toro"),
            Some("leli"),
            true,
        ));
        // No anchor replica, copy does not land in anchor -> violation.
        assert!(anchor_violated_after_copy(
            &["toro".into()],
            "leli",
            Some("toro"),
            Some("nrb"),
            true,
        ));
        // Empty anchor dc -> never a constraint.
        assert!(!anchor_violated_after_copy(&[], "", None, None, false));
    }

    // ---- CO-1-2 [HIGH anchor] — demote must not strand the anchor ----

    use dtfs_common::types::AnchorPolicy;

    fn placement(node_id: u64, disk_id: u32, dc: &str, tier: DiskTier) -> TickPlacement {
        TickPlacement { node_id, disk_id, dc: dc.to_string(), tier }
    }

    fn chunk_with(placements: Vec<TickPlacement>) -> TickChunk {
        TickChunk {
            chunk_id: "c1".into(),
            chunk_index: 0,
            file_path: "/f".into(),
            volume_id: "v1".into(),
            placements,
            cooldown_until_ms: 0,
            reads_in_window_by_dc: HashMap::new(),
            file_updated_at_ms: 0,
        }
    }

    /// CO-1-2 — the chunk's ONLY anchor-DC replica is the warm cache copy
    /// (mandatory copy lives in another DC). Demoting it would strand the
    /// LELI anchor, so `demote_preserves_anchor` must return false.
    #[test]
    fn co_1_2_demote_of_sole_anchor_replica_is_refused() {
        // LELI warm cache copy (the only LELI replica) + TORO mandatory.
        let leli_cache = placement(10, 1, "leli", DiskTier::Warm);
        let toro_mand = placement(20, 0, "toro", DiskTier::Cold);
        let chunk = chunk_with(vec![leli_cache.clone(), toro_mand]);
        let anchor = AnchorPolicy { datacenter: "leli".into(), required_tier: None };
        assert!(
            !demote_preserves_anchor(&chunk, &leli_cache, Some(&anchor)),
            "demoting the only LELI replica must be refused"
        );
    }

    /// CO-1-2 — a second LELI replica keeps the anchor satisfied, so the
    /// demote of one LELI cache copy is allowed.
    #[test]
    fn co_1_2_demote_safe_when_another_anchor_replica_survives() {
        let leli_cache = placement(10, 1, "leli", DiskTier::Warm);
        let leli_anchor = placement(11, 0, "leli", DiskTier::Cold);
        let chunk = chunk_with(vec![leli_cache.clone(), leli_anchor]);
        let anchor = AnchorPolicy { datacenter: "leli".into(), required_tier: None };
        assert!(
            demote_preserves_anchor(&chunk, &leli_cache, Some(&anchor)),
            "another LELI replica keeps the anchor satisfied"
        );
    }

    /// CO-1-2 — a tier-pinned anchor (cold in LELI) is not satisfied by a
    /// surviving LELI WARM replica, so demoting the LELI cold copy is
    /// refused even though another LELI replica exists.
    #[test]
    fn co_1_2_tier_pinned_anchor_respects_required_tier() {
        let leli_cold = placement(10, 0, "leli", DiskTier::Cold);
        let leli_warm = placement(10, 1, "leli", DiskTier::Warm);
        let chunk = chunk_with(vec![leli_cold.clone(), leli_warm]);
        let anchor = AnchorPolicy {
            datacenter: "leli".into(),
            required_tier: Some(DiskTier::Cold),
        };
        assert!(
            !demote_preserves_anchor(&chunk, &leli_cold, Some(&anchor)),
            "the surviving LELI warm copy does not satisfy a cold-pinned anchor"
        );
    }

    /// CO-1-2 — no anchor policy → never a constraint.
    #[test]
    fn co_1_2_no_anchor_policy_never_blocks() {
        let leli = placement(10, 1, "leli", DiskTier::Warm);
        let chunk = chunk_with(vec![leli.clone()]);
        assert!(demote_preserves_anchor(&chunk, &leli, None));
        let empty = AnchorPolicy { datacenter: String::new(), required_tier: None };
        assert!(demote_preserves_anchor(&chunk, &leli, Some(&empty)));
    }

    /// CO-1-2 — the planner records the refused demote in
    /// `anchor_protected` (so the dispatcher can audit it) and does NOT
    /// emit it as a demotion.
    #[test]
    fn co_1_2_planner_refuses_and_records_anchor_protected() {
        use dtfs_common::types::{CacheReplication, CacheTierConfig, TierReplication};
        // Volume: warm cache enabled (max=1), TORO mandatory cold, anchor
        // = LELI. The chunk's only LELI replica is the warm cache copy.
        let mut cache = CacheReplication::default();
        cache.warm = CacheTierConfig { min: 0, max: 1 };
        cache.promote_window_secs = 60;
        cache.promote_reads = 2;
        // Mandatory = cold only (warm/hot/tape 0) so warm is a pure cache
        // tier; TierReplication::default() is {3,2,1,1}, NOT all-zero.
        let mandatory = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let policy = TickVolumePolicy {
            mandatory,
            cache,
            anchor: Some(AnchorPolicy { datacenter: "leli".into(), required_tier: None }),
        };
        let mut volumes = HashMap::new();
        volumes.insert("v1".to_string(), policy);

        // Idle (no reads) chunk, file old enough to clear the grace window.
        let mut chunk = chunk_with(vec![
            placement(10, 1, "leli", DiskTier::Warm), // sole LELI = warm cache
            placement(20, 0, "toro", DiskTier::Cold), // mandatory elsewhere
        ]);
        chunk.file_updated_at_ms = 1; // far in the past relative to now_ms
        let snapshot = TickSnapshot {
            chunks: vec![chunk],
            nodes: Vec::new(),
            volumes,
        };
        let plan = plan_cache_tick(&snapshot, 10_000_000, false);
        assert!(
            plan.demotions.is_empty(),
            "the anchor-stranding demote must NOT be scheduled"
        );
        assert_eq!(
            plan.anchor_protected.len(),
            1,
            "the refusal must be recorded for the dispatcher to audit"
        );
        assert_eq!(plan.anchor_protected[0].node_id, 10);
        assert_eq!(plan.anchor_protected[0].disk_id, 1);
    }

    // ---- CO-1-3 — demote dispatch de-location ordering ----

    /// CO-1-3 — the demote dispatch routes its de-location through the same
    /// `delocate_replica` contract the scheduler uses: the
    /// `RemoveChunkLocation` Raft write (carrying the volume `enforce_anchor`)
    /// commits BEFORE any byte delete, and on a forced Raft-write failure NO
    /// `DeleteChunkReplica` RPC is issued (no phantom replica). A recording
    /// mock stands in for the live `SchedulerDelocate`, capturing the anchor
    /// the demote would thread through.
    #[tokio::test]
    async fn co_1_3_demote_removes_before_byte_delete_carries_anchor() {
        use crate::scheduler::{delocate_replica, DelocateOps, DelocateOutcome};
        use std::sync::Mutex;

        struct DemoteDelocate {
            calls: Mutex<Vec<&'static str>>,
            // The anchor the demote dispatch threads into RemoveChunkLocation.
            enforce_anchor: Option<AnchorPolicy>,
            remove_fails: bool,
        }
        impl DelocateOps for DemoteDelocate {
            async fn remove_location(&self) -> anyhow::Result<()> {
                self.calls.lock().unwrap().push("remove");
                // The demote MUST carry the volume anchor for the apply-path
                // defence-in-depth re-check (CO-1-2).
                assert!(
                    self.enforce_anchor.is_some(),
                    "demote RemoveChunkLocation must carry enforce_anchor"
                );
                if self.remove_fails {
                    Err(anyhow::anyhow!("forced raft remove failure"))
                } else {
                    Ok(())
                }
            }
            async fn delete_bytes(&self) -> anyhow::Result<()> {
                self.calls.lock().unwrap().push("delete");
                Ok(())
            }
        }

        let anchor = Some(AnchorPolicy { datacenter: "leli".into(), required_tier: None });

        // Happy path: remove then delete, in order.
        let ok = DemoteDelocate {
            calls: Mutex::new(Vec::new()),
            enforce_anchor: anchor.clone(),
            remove_fails: false,
        };
        assert_eq!(delocate_replica(&ok).await, DelocateOutcome::Done);
        assert_eq!(*ok.calls.lock().unwrap(), vec!["remove", "delete"]);

        // Forced Raft failure: no byte delete is issued (no phantom replica).
        let fail = DemoteDelocate {
            calls: Mutex::new(Vec::new()),
            enforce_anchor: anchor,
            remove_fails: true,
        };
        assert_eq!(delocate_replica(&fail).await, DelocateOutcome::RemoveFailed);
        assert_eq!(
            *fail.calls.lock().unwrap(),
            vec!["remove"],
            "a failed demote Raft remove must not delete any bytes"
        );
    }

    // ---- CO-4-6 — legacy temperature path skips AV-cache volumes ----

    /// CO-4-6 — a volume with at least one cache tier (`max > 0`) and a
    /// non-zero `promote_window_secs` is "active": the legacy temperature
    /// path must skip its chunks. A volume with cache disabled (all
    /// `max == 0`, or `promote_window_secs == 0`) is NOT active and the
    /// legacy path still handles it.
    #[test]
    fn co_4_6_active_cache_policy_detection() {
        use dtfs_common::types::{CacheReplication, CacheTierConfig};
        // Active: warm cache max=1, window=60.
        let mut active = CacheReplication::default();
        active.warm = CacheTierConfig { min: 0, max: 1 };
        active.promote_window_secs = 60;
        assert!(volume_has_active_cache_policy(&active));

        // Window 0 → not active even with a cache tier.
        let mut no_window = CacheReplication::default();
        no_window.warm = CacheTierConfig { min: 0, max: 1 };
        no_window.promote_window_secs = 0;
        assert!(!volume_has_active_cache_policy(&no_window));

        // No cache tier (all max=0) → not active.
        let mut no_tier = CacheReplication::default();
        no_tier.promote_window_secs = 60;
        assert!(!volume_has_active_cache_policy(&no_tier));

        // Default (empty) → not active.
        assert!(!volume_has_active_cache_policy(&CacheReplication::default()));
    }

    /// CO-4-17 — `apply_heartbeat_reads` bounds the read-window store.
    /// A flood of distinct chunk ids must not grow the key set past the
    /// cap, and a single hot chunk read every heartbeat must not grow
    /// its ring past the per-ring sample cap. Pre-fix neither was
    /// bounded inside `apply_heartbeat_reads` (only the once-per-tick
    /// dead-chunk sweep + per-query prune touched the store).
    #[test]
    fn co_4_17_read_windows_key_set_stays_bounded() {
        let mut windows: HashMap<(String, String), Vec<ReadSample>> = HashMap::new();
        let total = MAX_READ_WINDOW_KEYS + 1_000;
        for i in 0..total as u64 {
            let chunk = format!("{i:064x}");
            let mut reads = HashMap::new();
            reads.insert("dc-a".to_string(), 1u32);
            apply_heartbeat_reads(&chunk, &reads, 1_000 + i, &mut windows);
        }
        assert!(
            windows.len() <= MAX_READ_WINDOW_KEYS,
            "read_windows key set must stay <= MAX (got {})",
            windows.len()
        );
    }

    #[test]
    fn co_4_17_read_window_ring_stays_bounded() {
        let mut windows: HashMap<(String, String), Vec<ReadSample>> = HashMap::new();
        let chunk = "hotchunk";
        let mut reads = HashMap::new();
        reads.insert("dc-a".to_string(), 1u32);
        // 10k heartbeats for one hot chunk: without the per-ring cap the
        // ring would hold 10k samples.
        for i in 0..10_000u64 {
            apply_heartbeat_reads(chunk, &reads, 1_000 + i, &mut windows);
        }
        let ring = windows.get(&(chunk.to_string(), "dc-a".to_string())).unwrap();
        assert!(
            ring.len() <= MAX_READ_WINDOW_SAMPLES,
            "per-ring samples must stay <= MAX (got {})",
            ring.len()
        );
        // The retained samples are the most recent ones (oldest dropped).
        assert_eq!(
            ring.last().unwrap().ts_ms,
            1_000 + 9_999,
            "newest sample must be retained"
        );
    }
}
