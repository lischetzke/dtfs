//! Background task scheduler — Phase 4 (Resilience).
//!
//! Drives periodic and on-demand background tasks such as re-replication of
//! under-replicated chunks, tier migration, and chunk redistribution across
//! the cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dtfs_common::types::{DiskStatus, DiskTier, HealthStatus, MigrationStatus, NodeRole, TierReplication};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ChunkLocationEntry, ClientRequest, DtfsRaft};
use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::{ReplicateChunkRequest, VerifyChunkRequest};
// Bug AV — DeleteChunkRequest used by `delete_chunk_replica_on_node`. The
// proto's existing `DeleteChunk` RPC is the dispatcher target; AV-1-5
// extended its response shape with `was_present` + `freed_bytes`.
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::audit::AuditLog;

/// Interval between scheduler ticks.
const SCHEDULER_TICK: Duration = Duration::from_secs(30);

/// Maximum number of consecutive failures (per task per state) before the task
/// is marked Failed. With a 30s tick, 6 attempts ≈ 3 minutes of trying. After
/// exhaustion the task is moved to Failed and an audit entry recorded (P0-4).
const MAX_MIGRATION_ATTEMPTS: u32 = 6;

/// A replication task tracked by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationTask {
    pub task_id: String,
    pub chunk_id: String,
    pub source_node: u64,
    pub target_node: u64,
    pub tier: DiskTier,
    pub status: MigrationStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    /// Number of consecutive failed attempts in the current state (P0-4).
    /// Reset to 0 on each successful state transition; once it hits
    /// `MAX_MIGRATION_ATTEMPTS` the task is marked `Failed`.
    /// `#[serde(default)]` keeps existing snapshots loadable.
    #[serde(default)]
    pub attempts: u32,
    /// Last error message — kept for audit logging on failure.
    #[serde(default)]
    pub last_error: Option<String>,
}

/// Serializable collection of active migration tasks, stored in Raft state
/// via `ClientRequest::UpdateConfig`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MigrationState {
    tasks: HashMap<String, ReplicationTask>,
    next_task_id: u64,
}

/// Background task scheduler that drives re-replication and migration.
pub struct TaskScheduler {
    raft: Arc<DtfsRaft>,
    state_data: Arc<RwLock<StateMachineData>>,
    audit: Arc<AuditLog>,
    /// P1-8: optional alert dispatcher. MigrationFailed fires here.
    alerts: parking_lot::RwLock<Option<Arc<crate::alerts::AlertDispatcher>>>,
    /// CF-5: optional memory budget. When set and RSS reaches the Warn
    /// pressure level, the scheduler skips its background ticks
    /// (re-replication / migration / rebalance) so the manager gives RAM
    /// back instead of being OOM/cgroup-killed. None = no cap configured
    /// (the common case) and ticks always run.
    memory: parking_lot::RwLock<Option<Arc<dtfs_common::memory::MemoryBudget>>>,
}

impl TaskScheduler {
    /// Create a new task scheduler.
    pub fn new(
        raft: Arc<DtfsRaft>,
        state_data: Arc<RwLock<StateMachineData>>,
        audit: Arc<AuditLog>,
    ) -> Self {
        Self {
            raft,
            state_data,
            audit,
            alerts: parking_lot::RwLock::new(None),
            memory: parking_lot::RwLock::new(None),
        }
    }

    /// Inject the alert dispatcher (called after main.rs has wired one).
    pub fn set_alerts(&self, alerts: Arc<crate::alerts::AlertDispatcher>) {
        *self.alerts.write() = Some(alerts);
    }

    /// CF-5 — inject the memory budget so the scheduler can skip background
    /// ticks under memory pressure (called after main.rs builds the budget).
    pub fn set_memory_budget(&self, budget: Arc<dtfs_common::memory::MemoryBudget>) {
        *self.memory.write() = Some(budget);
    }

    /// CF-5 — true when a memory budget is configured and RSS has reached the
    /// Warn pressure level. The scheduler's background loop checks this before
    /// each tick and skips the work to give RAM back (the cheapest ladder rung
    /// for the manager: re-replication/migration scans hold the largest
    /// transient state). Returns false when no cap is configured.
    fn under_memory_pressure(&self) -> bool {
        use dtfs_common::memory::PressureLevel;
        match &*self.memory.read() {
            Some(b) => b.pressure_level() != PressureLevel::Normal,
            None => false,
        }
    }

    /// Run the scheduler background loop. Intended to be spawned as a tokio task.
    pub async fn run(&self) {
        info!("Task scheduler started (tick interval: {:?})", SCHEDULER_TICK);

        let mut interval = tokio::time::interval(SCHEDULER_TICK);
        loop {
            interval.tick().await;

            // CF-5 — under memory pressure, skip the tick. Re-replication
            // and migration scans build the largest transient state in the
            // manager; pausing them is the cheapest ladder rung to give RAM
            // back before the systemd cgroup kills us. Work resumes
            // automatically once RSS drops back below the warn threshold.
            if self.under_memory_pressure() {
                tracing::warn!(
                    "Scheduler tick skipped: under memory pressure (CF-5 ladder)"
                );
                continue;
            }

            if let Err(e) = self.tick().await {
                warn!("Scheduler tick failed: {}", e);
            }
        }
    }

    /// P0-1: lease garbage-collection loop. Sweeps expired write leases
    /// every second by proposing `SweepExpiredLeases` through Raft. Without
    /// this, an orphan lease (FUSE crashed, network partition, kill -9)
    /// blocks all writers to that path until the lease's lease_ms elapses.
    pub async fn run_lease_gc(raft: Arc<DtfsRaft>) {
        info!("Lease GC started (1s tick)");
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            let now_ms = now_millis();
            if let Err(e) = raft
                .client_write(dtfs_raft::ClientRequest::SweepExpiredLeases { now_ms })
                .await
            {
                // Non-leader managers will get a forward error; that's fine
                // — only the leader needs to drive GC.
                tracing::trace!("SweepExpiredLeases write failed (non-leader?): {e}");
            }
        }
    }

    /// A single scheduler tick: find under-replicated chunks and advance
    /// in-progress migration tasks.
    ///
    /// Bug AC — leader-gated. Followers' Raft writes silently forward to
    /// the leader (or fail entirely when leader is unknown). Running the
    /// scheduler on every manager wastes effort: 3 managers all
    /// independently call `ReplicateChunk` on the source data node for
    /// the same chunk, all observe success, all try to write
    /// UpdateChunkLocations — only the leader's write commits, the
    /// others silently fail. This caused the per-task 3x amplification
    /// during Bug AB diagnosis. Skipping on followers also means a
    /// leader-loss event freezes scheduler work cleanly (no
    /// half-finished state on stale followers) until the next election.
    async fn tick(&self) -> anyhow::Result<()> {
        // Skip on followers — only the current leader drives migrations.
        // openraft's metrics watch is a Receiver — borrow_watched gives
        // a borrow without consuming the receiver.
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        let self_id = metrics.id;
        let is_leader = metrics
            .current_leader
            .map(|leader| leader == self_id)
            .unwrap_or(false);
        if !is_leader {
            tracing::trace!(
                "Scheduler tick skipped: not leader (current_leader={:?})",
                metrics.current_leader
            );
            return Ok(());
        }

        let mut migration_state = self.load_migration_state().await;

        // 1. Check for under-replicated chunks and schedule new tasks.
        self.check_under_replicated(&mut migration_state).await;

        // 1b. Bug AV (AV-3-3) — check anchor lag and schedule repairs
        // for chunks whose anchor invariant isn't yet satisfied. Used
        // primarily by warm-first writes (AV-3-1/3-2) where the
        // mandatory anchor copy is created asynchronously.
        self.check_anchor_lag(&mut migration_state).await;

        // 1c. Bug BX-11 — rebalance: drop EXCESS spill replicas from
        // over-replicated chunks so a capacity-spilled chunk converges back
        // to goal once the mandated tier has been backfilled (by 1./1b.).
        // Only acts above mandatory_total + cache_allowance and never below a
        // mandated tier / the anchor (pure planner + enforce_anchor on apply),
        // so it can't under-replicate or fight the tiering engine.
        self.check_misplaced_rebalance().await;

        // 2. Advance existing migration tasks through their state machine.
        self.advance_tasks(&mut migration_state).await;

        // 3. Persist the updated migration state back to Raft.
        self.save_migration_state(&migration_state).await;

        Ok(())
    }

    /// Schedule a replication task for a specific chunk.
    ///
    /// Creates a new task in `Pending` state that will be picked up on the
    /// next scheduler tick.
    pub async fn schedule_replication(
        &self,
        chunk_id: &str,
        source_node: u64,
        target_node: u64,
        tier: DiskTier,
    ) {
        let mut state = self.load_migration_state().await;

        // Avoid duplicate tasks for the same chunk + target.
        let already_scheduled = state.tasks.values().any(|t| {
            t.chunk_id == chunk_id
                && t.target_node == target_node
                && t.status != MigrationStatus::Done
                && t.status != MigrationStatus::Failed
        });

        if already_scheduled {
            debug!(
                chunk_id,
                target_node, "Replication task already scheduled, skipping"
            );
            return;
        }

        let now_ms = now_millis();
        let task_id = format!("repl-{}", state.next_task_id);
        state.next_task_id += 1;

        let task = ReplicationTask {
            task_id: task_id.clone(),
            chunk_id: chunk_id.to_string(),
            source_node,
            target_node,
            tier,
            status: MigrationStatus::Pending,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            attempts: 0,
            last_error: None,
        };

        info!(
            task_id = %task_id,
            chunk_id = %chunk_id,
            source = source_node,
            target = target_node,
            "Scheduled replication task"
        );

        self.audit.log(
            "scheduler",
            "replication.schedule",
            &format!("chunk={chunk_id} src={source_node} dst={target_node}"),
            "pending",
        );

        state.tasks.insert(task_id, task);
        self.save_migration_state(&state).await;
    }

    /// AV-5-5 — return true when a replication task targeting `chunk_id`
    /// on `target_node` is still in flight (any status except Done /
    /// Failed). The AV-2-5 demote dispatcher consults this to avoid
    /// racing a just-completed PutChunk against a stale planner-emitted
    /// demote (the planner's snapshot can predate the
    /// UpdateChunkLocations Raft write that records the new replica).
    pub(crate) async fn has_active_replication_to(
        &self,
        chunk_id: &str,
        target_node: u64,
    ) -> bool {
        let state = self.load_migration_state().await;
        state.tasks.values().any(|t| {
            t.chunk_id == chunk_id
                && t.target_node == target_node
                && t.status != MigrationStatus::Done
                && t.status != MigrationStatus::Failed
        })
    }

    /// Check the cluster state for under-replicated chunks and schedule
    /// re-replication tasks for them.
    async fn check_under_replicated(&self, migration_state: &mut MigrationState) {
        let sm = self.state_data.read().await;

        // Collect healthy DATA nodes and their live tiers. Bug CH — the
        // role filter is explicit (managers/clients register with no
        // disks, but relying on the empty tier list alone is fragile),
        // and a Failed/Removed disk no longer advertises its tier as a
        // placement target.
        let healthy_nodes: Vec<(u64, Vec<DiskTier>)> = sm
            .nodes
            .iter()
            .filter(|(_, info)| {
                info.role == NodeRole::Data && info.health == HealthStatus::Healthy
            })
            .map(|(id, info)| {
                let tiers: Vec<DiskTier> = info
                    .disks
                    .iter()
                    .filter(|d| !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed))
                    .map(|d| d.tier)
                    .collect();
                (*id, tiers)
            })
            .collect();

        // Walk all files; for each LOGICAL chunk check the PER-TIER replica
        // count against the volume's mandatory per-tier requirement.
        //
        // Bug BX-4 — the previous version derived a single `chunk_tier` from
        // one replica row and checked only that tier's count, so a mandated
        // tier with ZERO replicas (e.g. a chunk that landed only on cold under
        // a warm=2,cold=1 volume because warm was full at write) was never
        // noticed and never backfilled. We now group rows by chunk_index and
        // schedule a copy for every tier that is short of its mandatory count.
        //
        // Bug AV (AV-1-8) — under-replication enforces ONLY mandatory tiers;
        // cache tiers are the tiering engine's job.
        for (_path, file_meta) in &sm.files {
            if file_meta.is_directory || file_meta.symlink_target.is_some() {
                continue;
            }
            let mandatory: TierReplication = sm
                .volumes
                .get(&file_meta.volume_id)
                .map(|v| v.effective_mandatory_replicas().clone())
                .unwrap_or(TierReplication { hot: 3, warm: 0, cold: 0, tape: 0 });

            let mut by_idx: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
                std::collections::BTreeMap::new();
            for c in &file_meta.chunks {
                by_idx.entry(c.chunk_index).or_default().push(c);
            }

            for (_idx, entries) in &by_idx {
                // Bug CH — count a replica only when it is actually
                // serviceable (node not Down/Failed, disk not
                // Failed/Removed). Pre-CH the only cold replica sitting
                // on a dead node still satisfied cold=1, so node loss
                // was never backfilled by this (leader-gated) tick —
                // which is what the runaway monitor evacuation path
                // existed to paper over.
                let tier_of = |e: &ChunkLocationEntry| -> Option<DiskTier> {
                    let nid: u64 = e.node_id.parse().ok()?;
                    live_replica_tier(sm.nodes.get(&nid), e.disk_id)
                };
                let count_on = |t: DiskTier| entries.iter().filter(|e| tier_of(e) == Some(t)).count() as u32;

                let chunk_hash = match entries.first() {
                    Some(e) => e.chunk_hash.clone(),
                    None => continue,
                };
                let existing_nodes: Vec<u64> =
                    entries.iter().filter_map(|e| e.node_id.parse().ok()).collect();

                // Healthy source = any existing replica on a healthy node.
                let source_node_id: u64 = existing_nodes
                    .iter()
                    .copied()
                    .find(|nid| {
                        sm.nodes
                            .get(nid)
                            .map(|n| n.health == HealthStatus::Healthy)
                            .unwrap_or(false)
                    })
                    .unwrap_or(0);
                if source_node_id == 0 {
                    continue; // no healthy source to copy from
                }

                // Schedule a copy for every mandated tier that is short.
                for (tier, _deficit) in under_replicated_tiers(count_on, &mandatory) {
                    // Bug BH — exclude a node that already holds this chunk ON
                    // THE TARGET TIER, but a node holding it on a DIFFERENT
                    // tier is still a valid target (single-node multi-tier
                    // topology: data01 warm+cold).
                    let tier_holders: std::collections::HashSet<u64> = entries
                        .iter()
                        .filter(|e| tier_of(e) == Some(tier))
                        .filter_map(|e| e.node_id.parse().ok())
                        .collect();

                    let Some((target, _)) = healthy_nodes
                        .iter()
                        .find(|(id, tiers)| !tier_holders.contains(id) && tiers.contains(&tier))
                    else {
                        continue;
                    };

                    let already = migration_state.tasks.values().any(|t| {
                        t.chunk_id == chunk_hash
                            && t.target_node == *target
                            && t.tier == tier
                            && t.status != MigrationStatus::Done
                            && t.status != MigrationStatus::Failed
                    });
                    if already {
                        continue;
                    }

                    let now_ms = now_millis();
                    let task_id = format!("repl-{}", migration_state.next_task_id);
                    migration_state.next_task_id += 1;
                    let task = ReplicationTask {
                        task_id: task_id.clone(),
                        chunk_id: chunk_hash.clone(),
                        source_node: source_node_id,
                        target_node: *target,
                        tier,
                        status: MigrationStatus::Pending,
                        created_at_ms: now_ms,
                        updated_at_ms: now_ms,
                        attempts: 0,
                        last_error: None,
                    };
                    info!(
                        task_id = %task_id,
                        chunk = %chunk_hash,
                        source = source_node_id,
                        target = *target,
                        tier = ?tier,
                        "Bug BX-4 auto-scheduled re-replication for a short mandated tier"
                    );
                    migration_state.tasks.insert(task_id, task);
                }
            }
        }
    }

    /// Bug AV (AV-3-3) — Auto-anchor-migration check.
    ///
    /// For every chunk whose volume has an anchor policy, verifies
    /// that at least one replica satisfies the anchor invariant
    /// (in the anchor DC; if `required_tier` is set, on that tier).
    /// If not, schedules a `ReplicationTask` targeting a healthy
    /// node in the anchor DC with the required tier.
    ///
    /// This is what makes the warm-first write pattern safe: the
    /// FUSE client (AV-3-1/3-2) ACKs on a warm placement, then
    /// this background pass schedules the cold (anchor) copy. The
    /// volume's mandatory-replica scheduler
    /// (`check_under_replicated`) would also notice the missing
    /// cold copy, but that path only fires when the
    /// `mandatory_replicas.<tier>` counter is below threshold —
    /// the anchor invariant is a separate concern (which DC the
    /// replica lives in, not how many of them exist).
    /// Bug BX-11 — relocate-by-removal pass. Drops ONE excess spill replica
    /// per over-replicated chunk (via the pure `plan_excess_spill_removal`,
    /// which guards mandatory tiers + anchor + cache allowance), issuing
    /// `RemoveChunkLocation` with `enforce_anchor` as defence-in-depth.
    /// Bounded per tick; leader-gated by the caller. Combined with the
    /// per-tier backfill (BX-4) + anchor-lag, a capacity-spilled chunk
    /// converges back to its correct placement once capacity returns.
    async fn check_misplaced_rebalance(&self) {
        const MAX_REMOVALS_PER_TICK: usize = 50;

        // Phase 1 — under a read lock, decide the safe removals.
        let removals: Vec<(String, u64, u64, Option<dtfs_common::types::AnchorPolicy>, String)> = {
            let sm = self.state_data.read().await;
            let mut out = Vec::new();
            'outer: for (path, fm) in &sm.files {
                if fm.is_directory || fm.symlink_target.is_some() {
                    continue;
                }
                let Some(vol) = sm.volumes.get(&fm.volume_id) else {
                    continue;
                };
                let mandatory = vol.effective_mandatory_replicas().clone();
                let anchor = vol.effective_anchor();
                let cache_allow = vol.cache_replicas.hot.max
                    + vol.cache_replicas.warm.max
                    + vol.cache_replicas.cold.max
                    + vol.cache_replicas.tape.max;

                let mut by_idx: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
                    std::collections::BTreeMap::new();
                for c in &fm.chunks {
                    by_idx.entry(c.chunk_index).or_default().push(c);
                }
                for (idx, entries) in by_idx {
                    let reps: Vec<(u64, String, DiskTier)> = entries
                        .iter()
                        .filter_map(|e| {
                            let nid = e.node_id.parse::<u64>().ok()?;
                            let info = sm.nodes.get(&nid)?;
                            let tier = info.disks.get(e.disk_id as usize).map(|d| d.tier)?;
                            Some((nid, info.location.datacenter.clone(), tier))
                        })
                        .collect();
                    if let Some(node) =
                        plan_excess_spill_removal(&reps, &mandatory, anchor.as_ref(), cache_allow)
                    {
                        let chunk_hash =
                            entries.first().map(|e| e.chunk_hash.clone()).unwrap_or_default();
                        out.push((path.clone(), idx, node, anchor.clone(), chunk_hash));
                        if out.len() >= MAX_REMOVALS_PER_TICK {
                            break 'outer;
                        }
                    }
                }
            }
            out
        };

        // Phase 2 — issue the Raft removals (lock released).
        for (path, idx, node, enforce, chunk_hash) in removals {
            let req = ClientRequest::RemoveChunkLocation {
                path: path.clone(),
                chunk_index: idx,
                node_id: node.to_string(),
                enforce_anchor: enforce,
            };
            match self.raft.client_write(req).await {
                Ok(_) => {
                    info!(
                        path = %path,
                        idx,
                        node,
                        chunk = %chunk_hash,
                        "Bug BX-11 removed an excess spill replica (rebalance toward goal)"
                    );
                    self.audit.log(
                        "scheduler",
                        "rebalance.remove_excess",
                        &format!("chunk={chunk_hash} path={path} idx={idx} node={node}"),
                        "removed",
                    );
                }
                Err(e) => warn!(
                    path = %path,
                    idx,
                    node,
                    error = %e,
                    "Bug BX-11 excess-replica removal failed"
                ),
            }
        }
    }

    async fn check_anchor_lag(&self, migration_state: &mut MigrationState) {
        let sm = self.state_data.read().await;

        // Build per-node summary for the planner.
        let nodes: Vec<AnchorRepairNode> = sm
            .nodes
            .iter()
            .map(|(id, info)| AnchorRepairNode {
                node_id: *id,
                datacenter: info.location.datacenter.clone(),
                healthy: info.health == HealthStatus::Healthy,
                tiers: info.disks.iter().map(|d| d.tier).collect(),
            })
            .collect();

        // Quick lookup: existing task fingerprints (chunk_id, target_node)
        // for in-progress tasks so we don't double-schedule.
        // Bug BO-2: keep Failed tasks in the fingerprint set so a chunk that
        // just exhausted its attempts is NOT immediately re-queued on the
        // next 30s tick. A Failed task lingers for FAILED_TASK_RETENTION_MS
        // (30 min, Bug AE prune); only after it is GC'd does check_anchor_lag
        // schedule a fresh attempt. Previously Failed was excluded here, so
        // the same chunk was re-queued every tick → tight loop + audit spam.
        let existing_tasks: std::collections::HashSet<(String, u64)> =
            migration_state
                .tasks
                .values()
                .filter(|t| task_blocks_requeue(t.status))
                .map(|t| (t.chunk_id.clone(), t.target_node))
                .collect();

        // Walk every file and run the per-chunk planner.
        for (file_path, file_meta) in &sm.files {
            let Some(volume) = sm.volumes.get(&file_meta.volume_id) else {
                continue;
            };
            let Some(anchor) = volume.effective_anchor() else {
                continue;
            };

            // Group chunks by chunk_index so we evaluate each chunk
            // (not each replica row) exactly once.
            let mut by_idx: std::collections::BTreeMap<
                u64,
                Vec<&dtfs_raft::ChunkLocationEntry>,
            > = std::collections::BTreeMap::new();
            for c in &file_meta.chunks {
                by_idx.entry(c.chunk_index).or_default().push(c);
            }

            for (chunk_index, entries) in by_idx {
                // Resolve the placements + tiers for this chunk.
                let placements: Vec<AnchorRepairPlacement> = entries
                    .iter()
                    .filter_map(|e| {
                        let nid = e.node_id.parse::<u64>().ok()?;
                        let info = sm.nodes.get(&nid)?;
                        let tier = info.disks.get(e.disk_id as usize).map(|d| d.tier)?;
                        Some(AnchorRepairPlacement {
                            node_id: nid,
                            dc: info.location.datacenter.clone(),
                            tier,
                        })
                    })
                    .collect();

                // The chunk_id (BLAKE3 hash) is the same across
                // replicas for a given chunk_index; pick the first.
                let Some(chunk_id) = entries.first().map(|e| e.chunk_hash.clone()) else {
                    continue;
                };

                if let Some(action) = plan_anchor_lag_repair(
                    &anchor,
                    &placements,
                    &nodes,
                    &chunk_id,
                    &existing_tasks,
                ) {
                    // Source picks a healthy existing replica (any
                    // tier); the dispatcher tolerates cross-tier
                    // copies.
                    let now_ms = now_millis();
                    let task_id =
                        format!("anchor-{}", migration_state.next_task_id);
                    migration_state.next_task_id += 1;

                    let task = ReplicationTask {
                        task_id: task_id.clone(),
                        chunk_id: chunk_id.clone(),
                        source_node: action.source_node,
                        target_node: action.target_node,
                        tier: action.target_tier,
                        status: MigrationStatus::Pending,
                        created_at_ms: now_ms,
                        updated_at_ms: now_ms,
                        attempts: 0,
                        last_error: None,
                    };
                    info!(
                        task_id = %task_id,
                        chunk = %chunk_id,
                        file = %file_path,
                        idx = chunk_index,
                        source = action.source_node,
                        target = action.target_node,
                        tier = ?action.target_tier,
                        dc = %anchor.datacenter,
                        "Bug AV anchor.lag_repair: scheduling anchor migration"
                    );
                    self.audit.log(
                        "scheduler",
                        "anchor.lag_repair",
                        &format!(
                            "chunk={} file={} idx={} target={} tier={:?} dc={}",
                            chunk_id, file_path, chunk_index,
                            action.target_node, action.target_tier,
                            anchor.datacenter
                        ),
                        "scheduled",
                    );
                    migration_state.tasks.insert(task_id, task);
                }
            }
        }
    }

    /// Advance each in-progress migration task through its state machine:
    /// `Pending -> Copying -> Verifying -> Completing -> Done`.
    ///
    /// NOTE: Source data is NEVER removed until the target has been verified.
    async fn advance_tasks(&self, state: &mut MigrationState) {
        let now_ms = now_millis();

        let task_ids: Vec<String> = state.tasks.keys().cloned().collect();

        for task_id in task_ids {
            let task = match state.tasks.get_mut(&task_id) {
                Some(t) => t,
                None => continue,
            };

            match task.status {
                MigrationStatus::Pending => {
                    // Transition to Copying — reset the attempt counter for
                    // the new state.
                    info!(
                        task_id = %task.task_id,
                        chunk = %task.chunk_id,
                        "Migration: Pending -> Copying"
                    );
                    task.status = MigrationStatus::Copying;
                    task.attempts = 0;
                    task.last_error = None;
                    task.updated_at_ms = now_ms;
                }

                MigrationStatus::Copying => {
                    // Bug CH — fast-fail tasks whose target can never
                    // receive the copy (not registered / not a data node /
                    // dead / no live disk of the task's tier). The
                    // 2026-06-06 monitor storm persisted 36k tasks
                    // targeting managers and the FUSE client; without
                    // this guard each burns MAX_MIGRATION_ATTEMPTS
                    // ReplicateChunk RPCs against e.g. a manager's raft
                    // port. Cheap map lookup, so it runs before the
                    // full-namespace orphan scan below. Failed entries
                    // are pruned by the Bug AE retention window; a
                    // genuinely missing tier is rescheduled against a
                    // valid target by check_under_replicated.
                    let target_check = {
                        let sm = self.state_data.read().await;
                        validate_task_target(sm.nodes.get(&task.target_node), task.tier)
                    };
                    if let Err(reason) = target_check {
                        warn!(
                            task_id = %task.task_id,
                            chunk = %task.chunk_id,
                            target = task.target_node,
                            "Bug CH: dropping migration task with invalid target — {}",
                            reason
                        );
                        task.status = MigrationStatus::Failed;
                        task.last_error = Some(reason.to_string());
                        task.updated_at_ms = now_ms;
                        self.audit.log(
                            "scheduler",
                            "migration.invalid_target",
                            &format!(
                                "task={} chunk={} target={}",
                                task.task_id, task.chunk_id, task.target_node
                            ),
                            reason,
                        );
                        continue;
                    }

                    // Each tick, re-resolve the source live: pick any healthy
                    // node that currently has this chunk. This unsticks tasks
                    // that were scheduled against a node that has since gone
                    // Down (fix for F-015 — the previous code retried the
                    // originally-pinned source forever, even when other
                    // replicas were healthier).
                    //
                    // P0-4: after MAX_MIGRATION_ATTEMPTS failed ticks (≈ 3 min
                    // at 30s tick), give up and mark Failed so an operator
                    // gets paged via the audit log + alerting pipeline rather
                    // than the task lingering forever.
                    //
                    // Bug AE — before any retry, verify the chunk_id still
                    // exists in some file's chunks list. After a data-node
                    // disk wipe (or operator unlink + GC), the original
                    // file is gone but the migration task lingers in
                    // migration_state_json forever. resolve_healthy_source
                    // returns None → "no healthy source replica" → 6
                    // retries over 3 min → Failed. With 125+ such orphans
                    // every 30s tick spams the logs AND hammers data nodes
                    // with ReplicateChunk("Chunk not found") that's pure
                    // load. Drop these immediately (no retries) and let
                    // the prune step below garbage-collect them.
                    if !self.chunk_exists_anywhere(&task.chunk_id).await {
                        let reason = "chunk no longer exists (orphan task from \
                                      pre-wipe or unlinked file)";
                        warn!(
                            task_id = %task.task_id,
                            chunk = %task.chunk_id,
                            "Bug AE: dropping orphan migration task — {}",
                            reason
                        );
                        task.status = MigrationStatus::Failed;
                        task.last_error = Some(reason.to_string());
                        task.updated_at_ms = now_ms;
                        self.audit.log(
                            "scheduler",
                            "migration.orphan_dropped",
                            &format!("task={} chunk={}", task.task_id, task.chunk_id),
                            reason,
                        );
                        continue;
                    }

                    let target_addr = self.node_grpc_addr(task.target_node).await;
                    // Bug BO-1: for a same-node cross-tier anchor copy, don't
                    // exclude the target — it IS the only holder of the chunk
                    // (on its other tier), and excluding it strands the copy.
                    let exclude = anchor_copy_source_exclude(task.source_node, task.target_node);
                    let resolved_source = self
                        .resolve_healthy_source(&task.chunk_id, exclude)
                        .await;

                    let outcome: Result<(), String> = match (resolved_source, target_addr) {
                        (Some((src_node, src_addr)), Some(tgt_addr)) => {
                            if src_node != task.source_node {
                                info!(
                                    task_id = %task.task_id,
                                    chunk = %task.chunk_id,
                                    old_src = task.source_node,
                                    new_src = src_node,
                                    "Re-replication: source moved to a healthier replica",
                                );
                                task.source_node = src_node;
                            }
                            let chunk_id = task.chunk_id.clone();
                            let tier_str = task.tier.to_string();
                            match self.replicate_chunk(&src_addr, &chunk_id, &tgt_addr, &tier_str).await {
                                Ok(()) => {
                                    info!(
                                        task_id = %task.task_id,
                                        chunk = %chunk_id,
                                        src = %src_addr,
                                        dst = %tgt_addr,
                                        "Migration: Copying -> Verifying (ReplicateChunk OK)"
                                    );
                                    task.status = MigrationStatus::Verifying;
                                    task.attempts = 0;
                                    task.last_error = None;
                                    task.updated_at_ms = now_ms;
                                    continue;
                                }
                                Err(e) => Err(format!("ReplicateChunk error: {e}")),
                            }
                        }
                        (None, _) => Err("no healthy source replica".to_string()),
                        (_, None) => Err(format!(
                            "target node {} unresolvable",
                            task.target_node
                        )),
                    };

                    if let Err(reason) = outcome {
                        task.attempts = task.attempts.saturating_add(1);
                        task.last_error = Some(reason.clone());
                        task.updated_at_ms = now_ms;

                        // Bug AF — when the source data node specifically
                        // reports "Chunk not found", the bytes are gone
                        // from THAT node (disk wipe, fs reset, manual rm,
                        // etc.). resolve_healthy_source still returns the
                        // same node next tick because the file metadata
                        // hasn't been updated to remove it as a replica.
                        // Without a fast-fail, the scheduler retries this
                        // identical RPC every 30s × MAX_MIGRATION_ATTEMPTS
                        // (= 3 min), and each RPC opens a streaming gRPC
                        // to the source — which hammers the data node and
                        // contends with foreground put_chunk traffic from
                        // FUSE clients. On Plexmedia 4h soak v2 (verified
                        // 2026-05-19 07:30 UTC) this contention pushed the
                        // 60s put_chunk RPC timeout, causing every write
                        // to fail and FUSE to "unlink ghost entry" all
                        // newly-created files.
                        //
                        // Promote "Chunk not found" to instant-Failed.
                        // The 30-min Failed retention window keeps the
                        // task visible for operator inspection; the prune
                        // step clears it once an audit-driven action is
                        // taken (manual unlink, scrub, or restore from
                        // off-cluster backup).
                        if rpc_error_indicates_chunk_missing(&reason) {
                            task.attempts = MAX_MIGRATION_ATTEMPTS;
                        }
                        if task.attempts >= MAX_MIGRATION_ATTEMPTS {
                            warn!(
                                task_id = %task.task_id,
                                chunk = %task.chunk_id,
                                attempts = task.attempts,
                                reason = %reason,
                                "Migration: Copying -> Failed (max attempts exhausted)"
                            );
                            task.status = MigrationStatus::Failed;
                            self.audit.log(
                                "scheduler",
                                "migration.failed",
                                &format!(
                                    "task={} chunk={} attempts={}",
                                    task.task_id, task.chunk_id, task.attempts
                                ),
                                &reason,
                            );
                            if let Some(alerts) = self.alerts.read().clone() {
                                alerts.fire(
                                    crate::alerts::AlertKind::MigrationFailed,
                                    "migration task exhausted retries",
                                    serde_json::json!({
                                        "task_id": task.task_id,
                                        "chunk_id": task.chunk_id,
                                        "attempts": task.attempts,
                                        "reason": reason,
                                        "source_node": task.source_node,
                                        "target_node": task.target_node,
                                    }),
                                );
                            }
                        } else {
                            warn!(
                                task_id = %task.task_id,
                                chunk = %task.chunk_id,
                                attempt = task.attempts,
                                max = MAX_MIGRATION_ATTEMPTS,
                                reason = %reason,
                                "Copying retry queued",
                            );
                        }
                    }
                }

                MigrationStatus::Verifying => {
                    // Issue VerifyChunk RPC on the target node.
                    // Source data is NOT removed yet — only after verification passes.
                    let target_addr = self.node_grpc_addr(task.target_node).await;
                    match target_addr {
                        Some(tgt_addr) => {
                            let chunk_id = task.chunk_id.clone();
                            match self.verify_chunk(&tgt_addr, &chunk_id).await {
                                Ok(true) => {
                                    info!(
                                        task_id = %task.task_id,
                                        chunk = %chunk_id,
                                        "Migration: Verifying -> Completing (checksum OK)"
                                    );
                                    task.status = MigrationStatus::Completing;
                                    task.updated_at_ms = now_ms;
                                }
                                Ok(false) => {
                                    warn!(
                                        task_id = %task.task_id,
                                        chunk = %chunk_id,
                                        "VerifyChunk failed (checksum mismatch); retrying copy"
                                    );
                                    task.status = MigrationStatus::Copying;
                                    task.updated_at_ms = now_ms;
                                }
                                Err(e) => {
                                    warn!(
                                        task_id = %task.task_id,
                                        chunk = %chunk_id,
                                        "VerifyChunk RPC error, will retry: {}",
                                        e
                                    );
                                    task.updated_at_ms = now_ms;
                                }
                            }
                        }
                        None => {
                            warn!(
                                task_id = %task.task_id,
                                "Cannot resolve target node {} address for verify; failing task",
                                task.target_node
                            );
                            task.status = MigrationStatus::Failed;
                            task.updated_at_ms = now_ms;
                        }
                    }
                }

                MigrationStatus::Completing => {
                    // Target is verified.  Register the new replica location in
                    // the Raft state machine, then mark the task Done.
                    // The source remains a valid replica; over-replication
                    // cleanup happens in a separate pass.
                    let chunk_id = task.chunk_id.clone();
                    let target_node = task.target_node;
                    let tier = task.tier;

                    // Find the file path that owns this chunk (needed for
                    // UpdateChunkLocations).
                    let file_path = self.find_file_for_chunk(&chunk_id).await;

                    if let Some(path) = file_path {
                        // Bug AB — `merge_chunk_location_entries` REPLACES
                        // all existing entries for a chunk_index with the
                        // incoming set (this matches FUSE flush_segments
                        // semantics, where the client builds the full
                        // replica set). The scheduler was passing only
                        // the new replica entry → previous replicas were
                        // wiped → under-replication check immediately
                        // re-fired → infinite Copying → Verifying → Done
                        // loop on the same chunk. Fix: read the existing
                        // entries for this chunk_index, ADD the new
                        // target, dedupe by node_id, pass the full set.
                        let (chunk_index, full_chunks) = {
                            let sm = self.state_data.read().await;
                            let file = sm.files.get(&path);
                            let chunk_index = file
                                .and_then(|f| {
                                    f.chunks
                                        .iter()
                                        .find(|c| c.chunk_hash == chunk_id)
                                        .map(|c| c.chunk_index)
                                })
                                .unwrap_or(0);
                            // Collect every existing entry for this chunk_index
                            // (i.e. all current replicas).
                            let mut entries: Vec<ChunkLocationEntry> = file
                                .map(|f| {
                                    f.chunks
                                        .iter()
                                        .filter(|c| c.chunk_index == chunk_index)
                                        .cloned()
                                        .collect()
                                })
                                .unwrap_or_default();
                            // Add the new target if it's not already present.
                            // AV-5-9 — pick the disk_id that matches the
                            // replication task's tier on the target node.
                            // Pre-fix this was hardcoded `0`, so when the
                            // target's disk[0] wasn't the tier we asked
                            // for, the planner read the entry back as the
                            // wrong tier and the cache lifecycle silently
                            // misclassified replicas.
                            let target_str = target_node.to_string();
                            let resolved_disk_id: u32 = sm
                                .nodes
                                .get(&target_node)
                                .and_then(|n| {
                                    n.disks
                                        .iter()
                                        .position(|d| d.tier == tier)
                                        .map(|p| p as u32)
                                })
                                .unwrap_or(0);
                            // Bug BH — dedup by (node_id, disk_id), NOT
                            // node_id alone. On a no-spare-node multi-tier
                            // topology the anchor-cold target already holds
                            // the chunk on its warm disk; a node-only guard
                            // dropped the new cold entry, so the cold anchor
                            // was never recorded → infinite anchor-lag-repair
                            // loop + mandatory_strand flush refusals.
                            let already_present = replica_already_present(
                                &entries,
                                &target_str,
                                resolved_disk_id,
                            );
                            if !already_present {
                                entries.push(ChunkLocationEntry {
                                    chunk_index,
                                    chunk_hash: chunk_id.clone(),
                                    node_id: target_str,
                                    disk_id: resolved_disk_id,
                                    size_bytes: 0,
                                });
                            }
                            (chunk_index, entries)
                        };
                        let _ = chunk_index; // bound below if needed
                        let req = ClientRequest::UpdateChunkLocations {
                            path,
                            chunks: full_chunks,
                            // Bug P — re-replication is system-driven,
                            // not user-driven, so no client lease applies.
                            // Empty lease_id skips the verification path.
                            lease_id: String::new(),
                        };
                        // Bug AB diagnostic — promote the silent debug
                        // log to WARN so a stuck-in-loop scheduler is
                        // visible without per-tick instrumentation.
                        if let Err(e) = self.raft.client_write(req).await {
                            warn!(
                                task_id = %task.task_id,
                                error = %e,
                                "Bug AB UpdateChunkLocations Raft write failed; new replica not registered yet"
                            );
                        }
                    }

                    info!(
                        task_id = %task.task_id,
                        chunk = %chunk_id,
                        target_node,
                        tier = ?tier,
                        "Migration: Completing -> Done (new replica registered)"
                    );
                    task.status = MigrationStatus::Done;
                    task.updated_at_ms = now_ms;

                    self.audit.log(
                        "scheduler",
                        "replication.complete",
                        &format!(
                            "chunk={} src={} dst={}",
                            task.chunk_id, task.source_node, task.target_node
                        ),
                        "success",
                    );
                }

                MigrationStatus::Done | MigrationStatus::Failed => {
                    // Terminal states, nothing to do.
                }
            }
        }

        // Prune completed tasks older than 5 minutes.
        //
        // Bug AE — also prune Failed tasks older than 30 minutes. Before
        // this fix, Failed tasks lived in migration_state_json forever
        // (only Done was retained==false). After a disk wipe or other
        // mass-orphan event, 100+ Failed tasks accumulated and bloated
        // the Raft snapshot + cluttered the /api/v1/migrations UI. 30
        // min is long enough for an operator alerted on
        // `migration.failed` to inspect the task before it disappears
        // and short enough to clear historical garbage within one snapshot
        // interval (1 h default).
        let before = state.tasks.len();
        state.tasks.retain(|_, t| should_retain_task(t.status, t.updated_at_ms, now_ms));
        let pruned = before.saturating_sub(state.tasks.len());
        if pruned > 0 {
            info!(
                pruned,
                remaining = state.tasks.len(),
                "Bug AE: pruned terminal migration tasks"
            );
        }
    }

    /// Load the migration state from the dedicated Raft field.
    ///
    /// Uses `migration_state_json` instead of `cluster_config.config_json` so
    /// that scheduler bookkeeping never clobbers user-visible cluster
    /// configuration (fix for issue #3).
    async fn load_migration_state(&self) -> MigrationState {
        let sm = self.state_data.read().await;
        if sm.migration_state_json.is_empty() {
            return MigrationState::default();
        }
        serde_json::from_str(&sm.migration_state_json).unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // Data-node RPC helpers
    // -----------------------------------------------------------------------

    /// Look up the gRPC address of a node from the Raft state machine.
    pub(crate) async fn node_grpc_addr(&self, node_id: u64) -> Option<String> {
        let sm = self.state_data.read().await;
        sm.nodes.get(&node_id).map(|info| info.grpc_addr.clone())
    }

    /// AV-2-5 — expose the Raft handle so the tiering engine (which
    /// owns a reference to the scheduler) can propose
    /// `RemoveChunkLocation` + `SetChunkCooldown` for engine-driven
    /// demotions. This mirrors the established API surface used by
    /// the manager's REST handlers — the alternative (mirror each
    /// Raft request shape as a scheduler method) would just bloat
    /// the surface without adding safety.
    pub(crate) fn raft(&self) -> &std::sync::Arc<DtfsRaft> {
        &self.raft
    }

    /// Pick any healthy node that currently has a copy of `chunk_id`,
    /// excluding `exclude` (the migration target). Returns `(node_id,
    /// grpc_addr)` for the chosen source, or `None` if no healthy replica
    /// exists. (Helper for F-015.)
    /// Resolve a healthy node that holds `chunk_id`, optionally excluding
    /// one node id. Bug BO-1: `exclude` is `None` for a same-node
    /// cross-tier anchor copy (`source_node == target_node`) — the planner
    /// (`plan_anchor_lag_repair`, BH) legitimately targets a node that
    /// already holds the chunk on another tier, so excluding the target
    /// would strand the copy with "no healthy source replica". All other
    /// call sites pass `Some(target_node)` (unchanged behaviour).
    async fn resolve_healthy_source(
        &self,
        chunk_id: &str,
        exclude: Option<u64>,
    ) -> Option<(u64, String)> {
        let sm = self.state_data.read().await;
        let mut candidates: Vec<u64> = Vec::new();
        for file in sm.files.values() {
            for c in &file.chunks {
                if c.chunk_hash == chunk_id {
                    if let Ok(nid) = c.node_id.parse::<u64>() {
                        if Some(nid) != exclude && !candidates.contains(&nid) {
                            candidates.push(nid);
                        }
                    }
                }
            }
        }
        for nid in candidates {
            if let Some(info) = sm.nodes.get(&nid) {
                if info.health == HealthStatus::Healthy {
                    return Some((nid, info.grpc_addr.clone()));
                }
            }
        }
        None
    }

    /// Ask the source node to replicate `chunk_id` to `target_addr`,
    /// landing the replica on `target_tier` ("hot"/"warm"/"cold"/
    /// "tape"). AV-5-9: empty tier preserves legacy behaviour (target
    /// falls back to "any healthy disk"), non-empty pins the
    /// destination tier so cache promotions actually land on warm.
    async fn replicate_chunk(
        &self,
        source_addr: &str,
        chunk_id: &str,
        target_addr: &str,
        target_tier: &str,
    ) -> anyhow::Result<()> {
        // Bug W (part 2) — `source_addr` is `NodeInfo.grpc_addr`, which the
        // RegisterNode path stores already as a full URL ("http://ip:port").
        // The old code prepended "http://" unconditionally and produced
        // `http://http://...:9500` — tonic rejected this with a generic
        // "transport error" that silenced every migration in Plexmedia.
        let endpoint = normalize_endpoint(source_addr);
        let mut client = DataServiceClient::connect(endpoint).await
            .map_err(|e| anyhow::anyhow!("connect to source {}: {}", source_addr, e))?;

        let resp = client
            .replicate_chunk(ReplicateChunkRequest {
                chunk_id: chunk_id.to_string(),
                target_address: target_addr.to_string(),
                target_tier: target_tier.to_string(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("ReplicateChunk RPC: {}", e))?
            .into_inner();

        if resp.success {
            Ok(())
        } else {
            Err(anyhow::anyhow!("ReplicateChunk returned error: {}", resp.error))
        }
    }

    /// Ask the target node to verify the integrity of `chunk_id`.
    /// Returns `true` if the chunk is valid, `false` if the checksum failed.
    async fn verify_chunk(&self, target_addr: &str, chunk_id: &str) -> anyhow::Result<bool> {
        // Bug W (part 2) — same fix as `replicate_chunk`: NodeInfo.grpc_addr
        // already carries the scheme. Don't double-prefix.
        let endpoint = normalize_endpoint(target_addr);
        let mut client = DataServiceClient::connect(endpoint).await
            .map_err(|e| anyhow::anyhow!("connect to target {}: {}", target_addr, e))?;

        let resp = client
            .verify_chunk(VerifyChunkRequest {
                chunk_id: chunk_id.to_string(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("VerifyChunk RPC: {}", e))?
            .into_inner();

        if resp.valid {
            Ok(true)
        } else {
            warn!("VerifyChunk returned invalid: {}", resp.error);
            Ok(false)
        }
    }

    /// Bug AV — call `DeleteChunkReplica` on a data node. Mirrors
    /// `replicate_chunk` / `verify_chunk`: normalises the stored
    /// `NodeInfo.grpc_addr` (already-formed URL via Bug W),
    /// translates transport / RPC errors with context, and surfaces
    /// the idempotent response shape directly.
    ///
    /// Returns `(was_present, freed_bytes)` from the data node's
    /// idempotent delete (AV-1-5). A `was_present == false` is a
    /// normal no-op (retry, or chunk never landed on this node) and
    /// must NOT be treated as an error.
    pub(crate) async fn delete_chunk_replica_on_node(
        &self,
        node_addr: &str,
        chunk_id: &str,
    ) -> anyhow::Result<(bool, u64)> {
        let endpoint = normalize_endpoint(node_addr);
        let mut client = DataServiceClient::connect(endpoint).await
            .map_err(|e| anyhow::anyhow!("connect to data node {}: {}", node_addr, e))?;

        let resp = client
            .delete_chunk(dtfs_proto::dtfs::data::DeleteChunkRequest {
                chunk_id: chunk_id.to_string(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("DeleteChunkReplica RPC: {}", e))?
            .into_inner();

        if !resp.success {
            // The handler treats absent chunks as was_present=false
            // (success=true). A success=false response is a real
            // failure on the data node side.
            return Err(anyhow::anyhow!(
                "DeleteChunkReplica returned success=false on {}",
                node_addr
            ));
        }
        Ok((resp.was_present, resp.freed_bytes))
    }

    /// Find the file path that contains a given chunk hash.
    async fn find_file_for_chunk(&self, chunk_hash: &str) -> Option<String> {
        let sm = self.state_data.read().await;
        for (path, file_meta) in &sm.files {
            if file_meta.chunks.iter().any(|c| c.chunk_hash == chunk_hash) {
                return Some(path.clone());
            }
        }
        None
    }

    /// Bug AE — quick existence check for a chunk hash across every file in
    /// the metadata tree. Returns true iff at least one `FileMetadata`
    /// references the chunk. Used to detect orphan migration tasks: when
    /// the underlying file has been unlinked or the source disk wiped, the
    /// task's chunk_id is no longer reachable from any live file and we
    /// should stop retrying it. Delegates to the pure helper
    /// `chunk_in_any_file` so the predicate logic is unit-testable.
    async fn chunk_exists_anywhere(&self, chunk_hash: &str) -> bool {
        let sm = self.state_data.read().await;
        chunk_in_any_file(chunk_hash, &sm.files)
    }

    /// Persist the migration state via the dedicated `UpdateMigrationState`
    /// Raft command so that it never overwrites `cluster_config` (fix #3).
    async fn save_migration_state(&self, state: &MigrationState) {
        let json = match serde_json::to_string(state) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize migration state: {}", e);
                return;
            }
        };

        let req = ClientRequest::UpdateMigrationState { json };

        if let Err(e) = self.raft.client_write(req).await {
            // Bug AB diagnostic — followers will see this often (expected
            // because they forward to leader and the call returns
            // ForwardToLeader rather than committing locally). But when
            // EVERY save fails for an extended period the cluster is
            // leaderless and migration tasks loop infinitely re-running
            // ReplicateChunk for the same chunk every tick. Promoted from
            // debug to warn so the symptom is visible in standard log
            // levels — operators should investigate when this is the
            // only manager logging "Migration: Copying -> Verifying"
            // for the same task_id every 30s.
            warn!("Bug AB save_migration_state failed: {} (may not be leader)", e);
        }
    }

    // -----------------------------------------------------------------------
    // Manual / operator-driven path migration
    // -----------------------------------------------------------------------

    /// Plan and enqueue a manual data migration for every chunk whose owning
    /// file path starts with `path_prefix`. For each chunk the engine ensures
    /// at least one replica satisfies `target_tier` (and, when set,
    /// `target_dc`). Existing replicas are left in place — this is additive,
    /// not destructive, so a follow-up scrub or tiering pass can reclaim
    /// space later without risking data loss during the move.
    ///
    /// Semantics:
    /// * **Already satisfied** — a chunk that already has a replica on a node
    ///   whose disk tier matches `target_tier` and whose datacenter matches
    ///   `target_dc` (when set). Counted, not rescheduled.
    /// * **Scheduled** — one new replication task per chunk needing a copy,
    ///   inserted via [`schedule_replication`](Self::schedule_replication).
    /// * **No source** — chunk has no live healthy replica; can't be copied.
    /// * **No target** — no healthy node matches the tier/DC constraint and
    ///   does not already hold the chunk; needs a fresh node or a relaxed
    ///   constraint.
    ///
    /// `max_tasks` caps the number of new replication tasks enqueued in a
    /// single call so an "all files → tape" command can't lock the
    /// scheduler. Anything past the cap is reported under `truncated_chunks`
    /// and the operator can re-run the call to pick up where it left off
    /// (already-scheduled chunks become `already_satisfied` on the next run
    /// after the task completes).
    pub async fn schedule_path_migration(
        &self,
        path_prefix: &str,
        target_tier: DiskTier,
        target_dc: Option<&str>,
        max_tasks: usize,
    ) -> PathMigrationReport {
        // Snapshot file metadata + node table under one read lock so all
        // decisions are made against a consistent view.
        let (matching_files, nodes_snapshot): (
            Vec<(String, Vec<ChunkLocationEntry>)>,
            HashMap<u64, NodeSummary>,
        ) = {
            let sm = self.state_data.read().await;
            let files: Vec<(String, Vec<ChunkLocationEntry>)> = sm
                .files
                .iter()
                .filter(|(path, _)| path.starts_with(path_prefix))
                .map(|(path, meta)| (path.clone(), meta.chunks.clone()))
                .collect();
            let nodes: HashMap<u64, NodeSummary> = sm
                .nodes
                .iter()
                .map(|(id, info)| {
                    (
                        *id,
                        NodeSummary {
                            grpc_addr: info.grpc_addr.clone(),
                            datacenter: info.location.datacenter.clone(),
                            healthy: info.health == HealthStatus::Healthy,
                            tiers: info.disks.iter().map(|d| d.tier).collect(),
                        },
                    )
                })
                .collect();
            (files, nodes)
        };

        let (mut report, plan) = plan_path_migration(
            &matching_files,
            &nodes_snapshot,
            target_tier,
            target_dc,
            max_tasks,
        );

        // Dispatch the planned tasks. `schedule_replication` already
        // deduplicates so a retry of this call is safe.
        for action in &plan {
            self.schedule_replication(
                &action.chunk_hash,
                action.source_node,
                action.target_node,
                action.tier,
            )
            .await;
        }
        report.tasks_scheduled = plan.len();

        info!(
            prefix = %path_prefix,
            tier = ?target_tier,
            dc = ?target_dc,
            files = report.files_matched,
            chunks = report.chunks_total,
            scheduled = report.tasks_scheduled,
            satisfied = report.already_satisfied,
            no_source = report.no_source,
            no_target = report.no_target,
            truncated = report.truncated_chunks,
            "schedule_path_migration complete"
        );

        report
    }

    /// Lightweight snapshot of all migration tasks (used by the WebUI
    /// "Migrations" panel and `GET /api/v1/migrations`).
    pub async fn migration_snapshot(&self) -> Vec<serde_json::Value> {
        let state = self.load_migration_state().await;
        let mut out: Vec<_> = state
            .tasks
            .values()
            .map(|t| {
                serde_json::json!({
                    "task_id":    t.task_id,
                    "chunk_id":   t.chunk_id,
                    "source_node": t.source_node,
                    "target_node": t.target_node,
                    "tier":        format!("{:?}", t.tier).to_ascii_lowercase(),
                    "status":      format!("{:?}", t.status).to_ascii_lowercase(),
                    "attempts":    t.attempts,
                    "last_error":  t.last_error,
                    "created_at_ms": t.created_at_ms,
                    "updated_at_ms": t.updated_at_ms,
                })
            })
            .collect();
        // Bug AW — sort by created_at_ms (set-once) DESC, tiebreak by
        // natural-numeric task_id ASC. Bug AM's earlier fix sorted on
        // updated_at_ms, but that field is bumped on every scheduler
        // tick for any task in flight, so running tasks shuffled position
        // on each 5s WebUI refresh. created_at_ms is immutable after
        // insertion → newest task on top, frozen position thereafter.
        // Natural-numeric tiebreak ("repl-2" before "repl-10") replaces
        // the lexicographic compare that put "repl-10" between "repl-1"
        // and "repl-2".
        sort_migration_snapshot(&mut out);
        out
    }

    // -----------------------------------------------------------------------
    // Bug AV — operator-driven cache-tier flush (AV-1-7)
    // -----------------------------------------------------------------------

    /// Bug AV — flush opportunistic cache replicas for every file whose path
    /// starts with `path_prefix`. For each chunk:
    ///
    /// 1. Resolve mandatory tiers for the chunk's volume (via
    ///    `VolumeConfig::effective_mandatory_replicas` from AV-1-2).
    /// 2. Identify candidate replicas — those on tiers in `cache_tiers`
    ///    AND not in any mandatory tier with rf > 0 for this volume.
    /// 3. Verify the chunk still has at least one mandatory-tier replica
    ///    after the proposed removals. If not, audit a refusal and skip.
    /// 4. Issue `DeleteChunkReplica` on each candidate node (AV-1-6),
    ///    then `RemoveChunkLocation` Raft writes (AV-1-3) to drop the
    ///    SM entries, then a single `SetChunkCooldown` Raft write
    ///    (AV-1-4) covering every affected chunk_index.
    ///
    /// Returns a [`FlushCacheReport`] mirroring `PathMigrationReport`'s
    /// shape so the REST + CLI handlers can present a uniform summary.
    pub async fn flush_cache_for_path(
        &self,
        path_prefix: &str,
        cache_tiers: &[DiskTier],
        cooldown_secs: u64,
        max_tasks: usize,
    ) -> FlushCacheReport {
        // Snapshot SM under one read lock so all planning decisions are
        // made against a consistent view (same pattern as
        // `schedule_path_migration`).
        let snapshot: FlushSnapshot = {
            let sm = self.state_data.read().await;
            FlushSnapshot::from_sm(&sm, path_prefix)
        };

        let (actions, mut report) =
            plan_flush_cache(&snapshot, cache_tiers, max_tasks);

        // AV-1-11 — audit + alert on refusals BEFORE the dispatch loop
        // so the operator sees the warning even when nothing was
        // actually deletable (e.g. every chunk would have stranded the
        // anchor). One audit entry per refused chunk; one deduped
        // alert per call when refusals > 0.
        if !report.refusals.is_empty() {
            for refusal in &report.refusals {
                self.audit.log(
                    "scheduler",
                    "cache.flush_refused",
                    &format!(
                        "file={} idx={} reason={}",
                        refusal.file_path, refusal.chunk_index, refusal.reason
                    ),
                    "refused",
                );
            }
            if let Some(alerts) = self.alerts.read().clone() {
                let sample: Vec<serde_json::Value> = report
                    .refusals
                    .iter()
                    .take(20)
                    .map(|r| {
                        serde_json::json!({
                            "file_path": r.file_path,
                            "chunk_index": r.chunk_index,
                            "reason": r.reason,
                        })
                    })
                    .collect();
                alerts.fire(
                    crate::alerts::AlertKind::AnchorViolated,
                    "flush_cache refused: would have stranded mandatory replica(s)",
                    serde_json::json!({
                        "subsystem": "cache_flush",
                        "path_prefix": path_prefix,
                        "refusal_count": report.refusals.len(),
                        "files_matched": report.files_matched,
                        "chunks_examined": report.chunks_examined,
                        "sample": sample,
                    }),
                );
            }
        }

        if actions.is_empty() {
            // Still emit the completion summary so audit timelines tell
            // the full story (operator sees they ran a flush; planner
            // produced 0 actions but possibly N refusals).
            self.audit.log(
                "scheduler",
                "cache.flush_complete",
                &format!(
                    "prefix={} replicas_dropped=0 mandatory_refusals={}",
                    path_prefix, report.mandatory_refusals
                ),
                "success",
            );
            return report;
        }

        // Dispatch deletes. We don't currently bound the parallelism per
        // source node — the typical operator-driven flush is at-most-a-few-
        // hundred chunks. If we ever start seeing 10k-chunk flushes, gate
        // this with a per-source Semaphore (max 8 in-flight per node).
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cooldown_until_ms = now_ms.saturating_add(cooldown_secs * 1000);

        // Group affected chunk_indexes per file_path for one
        // SetChunkCooldown write per file at the end.
        let mut cooldowns: HashMap<String, std::collections::BTreeSet<u64>> =
            HashMap::new();

        for action in &actions {
            let Some(node_addr) = self.node_grpc_addr(action.node_id).await else {
                report.unreachable_nodes += 1;
                continue;
            };
            match self
                .delete_chunk_replica_on_node(&node_addr, &action.chunk_id)
                .await
            {
                Ok((was_present, _freed)) => {
                    // Audit each replica delete.
                    self.audit.log(
                        "scheduler",
                        "cache.flush_replica",
                        &format!(
                            "chunk={} file={} idx={} node={}",
                            action.chunk_id,
                            action.file_path,
                            action.chunk_index,
                            action.node_id
                        ),
                        if was_present { "success" } else { "noop" },
                    );

                    // Drop the SM entry. Anchor enforcement is threaded
                    // through so the apply path refuses if a race left
                    // us about to strand the anchor (defence in depth —
                    // the planner already filtered).
                    let req = ClientRequest::RemoveChunkLocation {
                        path: action.file_path.clone(),
                        chunk_index: action.chunk_index,
                        node_id: action.node_id.to_string(),
                        enforce_anchor: snapshot.anchor.clone(),
                    };
                    if let Err(e) = self.raft.client_write(req).await {
                        warn!(
                            chunk = %action.chunk_id,
                            error = %e,
                            "RemoveChunkLocation Raft write failed during flush"
                        );
                        report.raft_failures += 1;
                        continue;
                    }
                    cooldowns
                        .entry(action.file_path.clone())
                        .or_default()
                        .insert(action.chunk_index);
                    report.replicas_dropped += 1;
                }
                Err(e) => {
                    warn!(
                        chunk = %action.chunk_id,
                        node = action.node_id,
                        error = %e,
                        "DeleteChunkReplica RPC failed during flush"
                    );
                    report.rpc_failures += 1;
                }
            }
        }

        // One SetChunkCooldown per file covering every affected
        // chunk_index. Tiering engine refuses re-promotion while these
        // are in the future.
        for (path, idxs) in cooldowns {
            let req = ClientRequest::SetChunkCooldown {
                path,
                chunk_indexes: idxs.into_iter().collect(),
                expires_at_ms: cooldown_until_ms,
            };
            if let Err(e) = self.raft.client_write(req).await {
                warn!(error = %e, "SetChunkCooldown Raft write failed during flush");
                report.raft_failures += 1;
            }
        }

        self.audit.log(
            "scheduler",
            "cache.flush_complete",
            &format!(
                "prefix={} replicas_dropped={} mandatory_refusals={}",
                path_prefix, report.replicas_dropped, report.mandatory_refusals
            ),
            "success",
        );

        report
    }

    // ── Bug BF-7 — auto-unquarantine cooldown sweep ──────────────────────

    /// Run the auto-unquarantine loop forever. Each tick scans every
    /// Quarantined node; if BOTH `(now - q.since_ms) > cooldown_secs`
    /// AND `(now - q.last_fault_at_ms) > cooldown_secs`, raft-write
    /// `UnquarantineNode` with operator_id="auto-cooldown".
    ///
    /// `cooldown_secs = 0` (the default) DISABLES the loop — operator-
    /// only unquarantine matches the data-consistency-first stance.
    pub async fn run_auto_unquarantine_sweep(
        &self,
        tick_secs: u64,
        cooldown_secs: u64,
    ) {
        if cooldown_secs == 0 {
            info!(
                "Bug BF-7: auto-unquarantine disabled \
                 (manager.scrub.auto_unquarantine_secs=0; manual only)"
            );
            return;
        }
        info!(
            tick_secs,
            cooldown_secs,
            "Bug BF-7: auto-unquarantine sweep starting"
        );
        let mut tick = tokio::time::interval(Duration::from_secs(tick_secs.max(30)));
        tick.tick().await; // skip immediate
        loop {
            tick.tick().await;
            match self.auto_unquarantine_tick(cooldown_secs).await {
                Ok(n) if n > 0 => info!(
                    cleared = n,
                    "Bug BF-7: auto-unquarantine cleared nodes after cooldown"
                ),
                Ok(_) => debug!("Bug BF-7: auto-unquarantine tick clean"),
                Err(e) => warn!("Bug BF-7: auto-unquarantine tick failed: {e}"),
            }
        }
    }

    /// One sweep. Returns the number of nodes auto-unquarantined.
    pub async fn auto_unquarantine_tick(&self, cooldown_secs: u64) -> anyhow::Result<usize> {
        use dtfs_common::types::HealthStatus;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cooldown_ms = cooldown_secs.saturating_mul(1000);

        // Collect eligible (node_id, since, last_fault) snapshots.
        let eligible: Vec<u64> = {
            let sm = self.state_data.read().await;
            sm.nodes
                .iter()
                .filter_map(|(id, n)| {
                    if !matches!(n.health, HealthStatus::Quarantined) {
                        return None;
                    }
                    let q = n.quarantine.as_ref()?;
                    let age = now_ms.saturating_sub(q.since_ms);
                    let since_last_fault = now_ms.saturating_sub(q.last_fault_at_ms);
                    if age > cooldown_ms && since_last_fault > cooldown_ms {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut cleared = 0usize;
        for node_id in eligible {
            let req = ClientRequest::UnquarantineNode {
                node_id,
                operator_id: "auto-cooldown".to_string(),
                reason_acknowledged: format!(
                    "auto-cooldown after {}s clean (Bug BF-7)",
                    cooldown_secs
                ),
            };
            match self.raft.client_write(req).await {
                Ok(_) => {
                    cleared += 1;
                    self.audit.log(
                        "scheduler",
                        "node.unquarantine.auto",
                        &format!("node={node_id} cooldown_secs={cooldown_secs}"),
                        "success",
                    );
                }
                Err(e) => {
                    warn!(
                        node_id,
                        error = %e,
                        "Bug BF-7: UnquarantineNode raft write failed"
                    );
                }
            }
        }
        Ok(cleared)
    }

    // ── Bug BC-7 — orphan_scrub ─────────────────────────────────────────

    /// Run the orphan_scrub loop forever. Each tick:
    /// 1. Walk every data node, call `ListChunks`, get the on-disk
    ///    `chunk_id -> .dat mtime` map.
    /// 2. Diff against the union of every file's chunk hashes
    ///    (`chunk_is_orphan`).
    /// 3. Reap each orphan whose durable mtime is at least `grace_secs`
    ///    old (`orphan_reap_due`): call `DeleteChunkReplica` on the node +
    ///    audit `orphan.reaped`. Bug CB — the grace is measured against
    ///    the chunk's on-disk mtime, NOT a manager-local first-observed
    ///    clock, so it survives restarts / leadership changes and a
    ///    deleted file's chunks reclaim even under rolling-restart churn.
    ///    An unknown mtime (pre-Bug-CB data node) defers the reap.
    pub async fn run_orphan_scrub(&self, interval_secs: u64, grace_secs: u64) {
        info!(
            interval_secs,
            grace_secs,
            "Bug BC-7: orphan_scrub loop starting"
        );
        // Bug CB — short warmup (NOT a full interval) so a freshly
        // (re)started manager lets its raft log replay settle before the
        // first scan, then create the ticker so its first (immediate) tick
        // fires right after the warmup. The pre-CB code skipped a whole
        // `interval_secs` after every restart, which — combined with the
        // old in-memory grace clock — starved reaping under rolling-restart
        // churn. In-flight puts/promotes are protected by the mtime grace
        // (orphan_reap_due), not by deferring the scan.
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(60)));
        loop {
            tick.tick().await;
            match self.orphan_scrub_tick(grace_secs).await {
                Ok(reaped) => {
                    if reaped > 0 {
                        info!(reaped, "Bug BC-7: orphan_scrub tick reaped chunks");
                    } else {
                        debug!("Bug BC-7: orphan_scrub tick clean");
                    }
                }
                Err(e) => warn!("Bug BC-7: orphan_scrub tick failed: {e}"),
            }
        }
    }

    /// One sweep of orphan_scrub. Returns the number of chunks reaped.
    pub async fn orphan_scrub_tick(&self, grace_secs: u64) -> anyhow::Result<usize> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // 1. Snapshot in-state chunk_ids from state-machine. Bug BF-8
        // also snapshots per-node expected sets so the in-state-missing
        // inverse check can fire alongside the on-disk-not-in-state
        // reap loop without taking the state-machine lock twice.
        let (data_nodes, in_state_chunks, expected_by_node, already_quarantined) = {
            let sm = self.state_data.read().await;
            let nodes: Vec<(u64, String)> = sm
                .nodes
                .iter()
                .filter(|(_, n)| matches!(n.role, dtfs_common::types::NodeRole::Data))
                .map(|(id, n)| (*id, n.grpc_addr.clone()))
                .collect();
            let mut chunks: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            let mut expected: std::collections::BTreeMap<u64, std::collections::HashSet<String>> =
                std::collections::BTreeMap::new();
            for f in sm.files.values() {
                for c in &f.chunks {
                    chunks.insert(c.chunk_hash.clone());
                    if let Ok(nid) = c.node_id.parse::<u64>() {
                        expected.entry(nid).or_default().insert(c.chunk_hash.clone());
                    }
                }
            }
            let quarantined: std::collections::HashSet<u64> = sm
                .nodes
                .iter()
                .filter(|(_, n)| matches!(
                    n.health,
                    dtfs_common::types::HealthStatus::Quarantined
                ))
                .map(|(id, _)| *id)
                .collect();
            (nodes, chunks, expected, quarantined)
        };

        let mut reaped: usize = 0;

        for (node_id, grpc_addr) in &data_nodes {
            if grpc_addr.is_empty() { continue; }
            let on_disk = match self.list_chunks_on_node(grpc_addr).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(node = %node_id, error = %e, "Bug BC-7: ListChunks failed");
                    continue;
                }
            };
            // Bug BF-8 — in-state-missing inverse. Before processing
            // the on-disk-not-in-state loop, check what the state
            // machine THINKS this node holds; quarantine the node if
            // any expected chunk is absent from its ListChunks reply.
            // Skip if the node is already Quarantined (apply path
            // accumulates fault_count_total; no need to bump on every
            // tick).
            if !already_quarantined.contains(node_id) {
                if let Some(expected) = expected_by_node.get(node_id) {
                    let missing: Vec<&String> =
                        expected.iter().filter(|c| !on_disk.contains_key(*c)).collect();
                    if !missing.is_empty() {
                        let first = missing[0].clone();
                        let info = dtfs_common::types::QuarantineInfo {
                            since_ms: now_ms,
                            reason: dtfs_common::types::QuarantineReason::OrphanScrubInStateMissing,
                            triggering_chunk_id: Some(first.clone()),
                            fault_count_total: 0,
                            last_fault_at_ms: now_ms,
                            last_fault_summary: format!(
                                "orphan_scrub: {} chunk(s) in state but not on disk; first={}",
                                missing.len(),
                                first
                            ),
                        };
                        let req = ClientRequest::QuarantineNode {
                            node_id: *node_id,
                            info,
                        };
                        if let Err(e) = self.raft.client_write(req).await {
                            warn!(
                                node = %node_id, error = %e,
                                "Bug BF-8: QuarantineNode raft write failed"
                            );
                        } else {
                            self.audit.log(
                                "scheduler",
                                "orphan.in_state_missing",
                                &format!(
                                    "node={node_id} missing={} first={}",
                                    missing.len(),
                                    first
                                ),
                                "quarantined",
                            );
                            // Continue to reap on-disk-not-in-state below
                            // — even a Quarantined node can have orphans
                            // that the reaper should still clear.
                        }
                    }
                }
            }

            for (cid, mtime_ms) in &on_disk {
                // Bug BP-4 — a chunk is an orphan candidate ONLY when NO
                // FileMetadata references its hash (`in_state_chunks` is the
                // union of every file's chunk hashes). Never reap a chunk a
                // file still points at, even if its location record looks
                // stale — that was the data-loss vector (BN-1 UUID rotation
                // dropped location bindings; the hash-referenced guard here
                // is the defence-in-depth backstop). AGENTS.md §A.6 #8.
                let is_orphan = chunk_is_orphan(&in_state_chunks, cid);

                // Bug CB — measure the reap grace against the chunk's
                // DURABLE on-disk mtime, not a manager-local "first
                // observed orphan" clock. The old in-memory tracker reset
                // on every manager restart / leadership change, so under
                // rolling-restart churn a manager never accumulated the
                // 24h grace and orphaned chunks were never reaped (disk
                // usage of deleted files never dropped). The mtime survives
                // restarts; a freshly-written chunk (recent mtime) is still
                // protected against the in-flight put/promote race.
                if !orphan_reap_due(is_orphan, *mtime_ms, now_ms, grace_secs) {
                    // Surface a known-orphan we are deferring only because
                    // its age is unknown (mtime 0 = pre-Bug-CB data node),
                    // so a permanently-stalled reap is diagnosable.
                    if is_orphan && *mtime_ms == 0 {
                        debug!(
                            node = %node_id, chunk = %cid,
                            "Bug CB: orphan deferred — data node reported no \
                             mtime (upgrade the data node to enable reaping)"
                        );
                    }
                    continue;
                }
                let age_secs = now_ms.saturating_sub(*mtime_ms) / 1000;

                // Past grace — reap.
                match self.delete_chunk_replica_on_node(grpc_addr, cid).await {
                    Ok((was_present, freed)) => {
                        let result_msg = if was_present {
                            format!("freed_bytes={freed}")
                        } else {
                            "noop_already_absent".to_string()
                        };
                        self.audit.log(
                            "scheduler",
                            "orphan.reaped",
                            &format!("node={node_id} chunk={cid} age_secs={age_secs}"),
                            &result_msg,
                        );
                        reaped += 1;
                    }
                    Err(e) => {
                        warn!(
                            node = %node_id, chunk = %cid, error = %e,
                            "Bug BC-7: orphan reap delete RPC failed"
                        );
                    }
                }
            }
        }

        Ok(reaped)
    }

    /// Bug BC-6 — invoke `ListChunks` on a single data node, drain all
    /// pages, return the combined set of chunk_ids.
    async fn list_chunks_on_node(
        &self,
        node_addr: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, u64>> {
        use dtfs_proto::dtfs::data::ListChunksRequest;
        let endpoint = normalize_endpoint(node_addr);
        let mut client = DataServiceClient::connect(endpoint).await
            .map_err(|e| anyhow::anyhow!("connect to data node {}: {}", node_addr, e))?;
        // Bug CB — value is the chunk's `.dat` mtime (Unix ms); a
        // pre-Bug-CB data node sends an empty `chunk_mtimes_ms`, so a
        // missing entry defaults to 0 ("age unknown") and the reaper
        // defers rather than dropping a possibly in-flight chunk.
        let mut out: std::collections::HashMap<String, u64> =
            std::collections::HashMap::new();
        let mut token = String::new();
        loop {
            let resp = client
                .list_chunks(ListChunksRequest {
                    page_token: token.clone(),
                    limit: 1000,
                })
                .await
                .map_err(|e| anyhow::anyhow!("ListChunks RPC: {}", e))?
                .into_inner();
            for (i, cid) in resp.chunk_ids.into_iter().enumerate() {
                let mtime = resp.chunk_mtimes_ms.get(i).copied().unwrap_or(0);
                out.insert(cid, mtime);
            }
            if resp.next_page_token.is_empty() {
                break;
            }
            token = resp.next_page_token;
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Bug AV — flush_cache_for_path pure-planner kernel
// ---------------------------------------------------------------------------

/// Bug AV — snapshot extracted from the state machine for the flush
/// planner. Contains exactly what the planner needs: files matching the
/// prefix, their volumes' effective mandatory replication policy, and a
/// node table mapping `node_id → (datacenter, [disk_tier])`. Built once
/// under a single SM read lock by the async wrapper.
#[derive(Debug, Clone)]
pub(crate) struct FlushSnapshot {
    pub files: Vec<(String, Vec<ChunkLocationEntry>, TierReplication)>, // (path, chunks, mandatory)
    pub nodes: HashMap<u64, FlushNodeSummary>,
    pub anchor: Option<dtfs_common::types::AnchorPolicy>,
}

#[derive(Debug, Clone)]
pub(crate) struct FlushNodeSummary {
    pub datacenter: String,
    pub disk_tiers: Vec<DiskTier>, // by disk_id index
}

impl FlushSnapshot {
    fn from_sm(sm: &StateMachineData, path_prefix: &str) -> Self {
        let nodes: HashMap<u64, FlushNodeSummary> = sm
            .nodes
            .iter()
            .map(|(id, info)| {
                (
                    *id,
                    FlushNodeSummary {
                        datacenter: info.location.datacenter.clone(),
                        disk_tiers: info.disks.iter().map(|d| d.tier).collect(),
                    },
                )
            })
            .collect();
        // Pick any one anchor — file-level overrides aren't a thing yet
        // (each volume has its own policy; we honour the first matching
        // file's volume in the dispatcher's per-action RemoveChunkLocation
        // call).  For the snapshot we cache the most common (default
        // volume's) anchor for simplicity.
        let anchor = sm
            .volumes
            .get("default")
            .and_then(|v| v.effective_anchor());

        let mut files = Vec::new();
        for (path, meta) in sm.files.iter() {
            if !path.starts_with(path_prefix) {
                continue;
            }
            let mandatory = sm
                .volumes
                .get(&meta.volume_id)
                .map(|v| v.effective_mandatory_replicas().clone())
                .unwrap_or_else(TierReplication::default);
            files.push((path.clone(), meta.chunks.clone(), mandatory));
        }
        Self { files, nodes, anchor }
    }
}

/// Bug AV — one replica to drop, produced by the flush planner. The
/// dispatcher feeds these one-by-one into `delete_chunk_replica_on_node`
/// + `RemoveChunkLocation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushAction {
    pub file_path: String,
    pub chunk_index: u64,
    pub chunk_id: String,
    pub node_id: u64,
}

/// Bug AV (AV-1-11) — one entry per chunk the planner declined to
/// flush because doing so would have stranded the mandatory-tier
/// replicas. The dispatcher consumes these to emit audit + alert
/// hooks (one audit per refusal; one deduped alert per call when
/// any refusals occurred).
///
/// `reason` is a stable, machine-readable string so log scrapers
/// (and S15-style scenarios) can grep without parsing free text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushRefusal {
    pub file_path: String,
    pub chunk_index: u64,
    /// Stable reason code. Currently the only refusal cause is
    /// `"mandatory_strand"`; reserved for future additions
    /// (e.g. `"cooldown_active"` if AV-2 layers another rule on top).
    pub reason: &'static str,
}

/// Bug AV — operator-facing summary of a flush_cache call. Mirrors
/// PathMigrationReport's shape so the REST + CLI presenters share UX.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct FlushCacheReport {
    pub files_matched: usize,
    pub chunks_examined: usize,
    pub replicas_dropped: usize,
    /// Number of chunks where the proposed flush would have stranded the
    /// mandatory-tier replicas. Skipped (NOT counted in
    /// `replicas_dropped`); operator should investigate.
    pub mandatory_refusals: usize,
    pub truncated: usize,
    /// Per-action runtime failure counters. Populated by the dispatcher,
    /// always 0 from the pure planner.
    #[serde(default)] pub unreachable_nodes: usize,
    #[serde(default)] pub rpc_failures: usize,
    #[serde(default)] pub raft_failures: usize,
    /// AV-1-11 — detailed per-refusal records. Kept out of the JSON
    /// wire shape (large for big prefixes; the counter is sufficient
    /// for the operator-facing report). The dispatcher consumes the
    /// list internally to emit audit log entries and an alert; tests
    /// inspect it to verify the planner's refusal classification.
    #[serde(skip)] pub(crate) refusals: Vec<FlushRefusal>,
}

/// Bug BH — true if `entries` already records a replica of this chunk on
/// the exact `(node_id, disk_id)` slot. The anchor-lag-repair completion
/// path used to dedup on `node_id` alone, which on a no-spare-node
/// multi-tier topology (target already holds the chunk on its warm disk)
/// silently dropped the new *cold* anchor entry — `disk_id` is already
/// tier-resolved (AV-5-9 `resolved_disk_id`), so `(node_id, disk_id)` is
/// the exact `(node, tier)` slot and the correct dedup key.
pub(crate) fn replica_already_present(
    entries: &[ChunkLocationEntry],
    node_id: &str,
    disk_id: u32,
) -> bool {
    entries
        .iter()
        .any(|c| c.node_id == node_id && c.disk_id == disk_id)
}

/// Bug AV — pure planner kernel. Walks the snapshot, identifies cache-
/// tier replicas eligible for deletion, refuses anything that would
/// strand a mandatory-tier replica.
///
/// `cache_tiers` is the operator-supplied list of tiers to flush. The
/// planner additionally filters out any tier that's also configured as
/// mandatory for the file's volume (so e.g. a volume with
/// `mandatory_replicas.warm = 1` won't have its warm replicas dropped
/// even if the operator passed `cache_tiers = [warm]`).
pub(crate) fn plan_flush_cache(
    snapshot: &FlushSnapshot,
    cache_tiers: &[DiskTier],
    max_tasks: usize,
) -> (Vec<FlushAction>, FlushCacheReport) {
    let mut actions: Vec<FlushAction> = Vec::new();
    let mut report = FlushCacheReport::default();

    'files: for (file_path, chunks, mandatory) in &snapshot.files {
        report.files_matched += 1;
        // Group by chunk_index so we evaluate each chunk as a unit.
        let mut by_index: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
            std::collections::BTreeMap::new();
        for c in chunks {
            by_index.entry(c.chunk_index).or_default().push(c);
        }

        for (chunk_index, entries) in by_index {
            report.chunks_examined += 1;

            // Categorise replicas as cache-eligible or mandatory.
            // A tier is mandatory when mandatory_replicas.<tier> > 0.
            let mandatory_tier = |tier: DiskTier| -> bool {
                mandatory.for_tier(tier) > 0
            };
            // Resolve tier of a stored replica via the node table.
            let entry_tier = |e: &ChunkLocationEntry| -> Option<DiskTier> {
                let nid: u64 = e.node_id.parse().ok()?;
                let node = snapshot.nodes.get(&nid)?;
                node.disk_tiers.get(e.disk_id as usize).copied()
            };

            // Survivors AFTER the proposed flush. We refuse if no
            // mandatory-tier replica would remain.
            let mut would_drop: Vec<&ChunkLocationEntry> = Vec::new();
            let mut would_remain: Vec<&ChunkLocationEntry> = Vec::new();
            for e in &entries {
                let tier = entry_tier(e);
                let is_cache = match tier {
                    Some(t) => cache_tiers.contains(&t) && !mandatory_tier(t),
                    None => false, // unresolvable tier → leave alone
                };
                if is_cache {
                    would_drop.push(e);
                } else {
                    would_remain.push(e);
                }
            }

            if would_drop.is_empty() {
                continue;
            }

            // Refuse if no mandatory-tier replica survives.
            let mandatory_survives = would_remain.iter().any(|e| {
                entry_tier(e).map(mandatory_tier).unwrap_or(false)
            });
            if !mandatory_survives {
                report.mandatory_refusals += 1;
                // AV-1-11: record the refusal so the dispatcher can
                // audit + alert. Reason is stable for log scrapers.
                report.refusals.push(FlushRefusal {
                    file_path: file_path.clone(),
                    chunk_index,
                    reason: "mandatory_strand",
                });
                continue;
            }

            // Cap on number of in-flight actions. Once we hit max_tasks
            // we stop enqueueing — operator re-runs the call to pick up
            // remaining work.
            for e in would_drop {
                if actions.len() >= max_tasks {
                    // Count remaining unseen as truncated and bail.
                    report.truncated +=
                        report.chunks_examined.saturating_sub(actions.len());
                    break 'files;
                }
                let Ok(nid) = e.node_id.parse::<u64>() else {
                    continue;
                };
                actions.push(FlushAction {
                    file_path: file_path.clone(),
                    chunk_index,
                    chunk_id: e.chunk_hash.clone(),
                    node_id: nid,
                });
            }
        }
    }

    (actions, report)
}

/// A single replication operation produced by [`plan_path_migration`]. The
/// planner is pure — it does not touch Raft, so it's testable. The async
/// `schedule_path_migration` wrapper feeds these into `schedule_replication`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedReplication {
    pub chunk_hash: String,
    pub source_node: u64,
    pub target_node: u64,
    pub tier: DiskTier,
}

/// Pure planning kernel: given the matching files + a node snapshot, decide
/// which replications need to run to satisfy the requested tier/DC. Returns
/// the report counters plus the ordered list of (chunk_hash, src, tgt) tasks
/// to schedule.
///
/// Extracted so it can be unit-tested without touching Raft. See the tests
/// at the bottom of this file.
pub(crate) fn plan_path_migration(
    matching_files: &[(String, Vec<ChunkLocationEntry>)],
    nodes: &HashMap<u64, NodeSummary>,
    target_tier: DiskTier,
    target_dc: Option<&str>,
    max_tasks: usize,
) -> (PathMigrationReport, Vec<PlannedReplication>) {
    let mut report = PathMigrationReport {
        files_matched: matching_files.len(),
        ..Default::default()
    };
    let mut plan: Vec<PlannedReplication> = Vec::new();

    // Track per-chunk targets we've committed to so two chunks with the
    // same hash and chunks within the same file don't pile onto one node.
    let mut already_targeted: std::collections::HashSet<(String, u64)> = Default::default();

    for (path, chunks) in matching_files {
        // Group replicas by chunk_index so each logical chunk is
        // considered exactly once.
        let mut by_index: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
            Default::default();
        for c in chunks {
            by_index.entry(c.chunk_index).or_default().push(c);
        }

        for (chunk_index, replicas) in by_index {
            report.chunks_total += 1;

            // Stop scheduling once the per-call budget is exhausted. We
            // still keep walking so truncated_chunks is accurate.
            if plan.len() >= max_tasks {
                report.truncated_chunks += 1;
                continue;
            }

            let chunk_hash = match replicas.first() {
                Some(r) => r.chunk_hash.clone(),
                None => continue,
            };

            // Already satisfied?
            let satisfied = replicas.iter().any(|r| {
                let nid: u64 = r.node_id.parse().unwrap_or(0);
                nodes
                    .get(&nid)
                    .map(|n| {
                        n.tiers.contains(&target_tier)
                            && target_dc
                                .map(|dc| n.datacenter == dc)
                                .unwrap_or(true)
                    })
                    .unwrap_or(false)
            });
            if satisfied {
                report.already_satisfied += 1;
                continue;
            }

            // Pick a healthy source from the existing replicas.
            let existing_nids: std::collections::HashSet<u64> = replicas
                .iter()
                .filter_map(|r| r.node_id.parse::<u64>().ok())
                .collect();
            let source_node: Option<u64> = replicas
                .iter()
                .filter_map(|r| r.node_id.parse::<u64>().ok())
                .find(|nid| nodes.get(nid).map(|n| n.healthy).unwrap_or(false));
            let Some(src) = source_node else {
                report.no_source += 1;
                report.note_example(path, chunk_index, "no_source");
                continue;
            };

            // Pick a healthy target satisfying tier + (optional) DC, not
            // already holding this chunk + not already targeted earlier.
            // Iterate by node_id ASC so plans are deterministic in tests.
            let mut candidates: Vec<u64> = nodes
                .iter()
                .filter(|(nid, n)| {
                    n.healthy
                        && n.tiers.contains(&target_tier)
                        && target_dc
                            .map(|dc| n.datacenter == dc)
                            .unwrap_or(true)
                        && !existing_nids.contains(nid)
                        && !already_targeted.contains(&(chunk_hash.clone(), **nid))
                })
                .map(|(nid, _)| *nid)
                .collect();
            candidates.sort_unstable();
            let target_node = candidates.into_iter().next();
            let Some(tgt) = target_node else {
                report.no_target += 1;
                report.note_example(path, chunk_index, "no_target");
                continue;
            };

            already_targeted.insert((chunk_hash.clone(), tgt));
            plan.push(PlannedReplication {
                chunk_hash,
                source_node: src,
                target_node: tgt,
                tier: target_tier,
            });
        }
    }

    // tasks_scheduled is set after dispatch; planner just reports the
    // length of the plan it produced.
    report.tasks_scheduled = plan.len();

    (report, plan)
}

/// Per-call summary of a manual `schedule_path_migration` invocation.
///
/// Returned over the REST API so the WebUI can show "what just happened"
/// without polling the migration list immediately.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PathMigrationReport {
    pub files_matched: usize,
    pub chunks_total: usize,
    pub already_satisfied: usize,
    pub tasks_scheduled: usize,
    pub no_source: usize,
    pub no_target: usize,
    /// Chunks past `max_tasks` we walked but did not schedule. The next call
    /// with the same prefix will pick them up.
    pub truncated_chunks: usize,
    /// Up to 5 illustrative entries so the operator can see *which* paths
    /// hit `no_source` / `no_target` without dumping the full list.
    pub examples: Vec<PathMigrationExample>,
}

impl PathMigrationReport {
    fn note_example(&mut self, path: &str, chunk_index: u64, reason: &str) {
        if self.examples.len() >= 5 {
            return;
        }
        self.examples.push(PathMigrationExample {
            path: path.to_string(),
            chunk_index,
            reason: reason.to_string(),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathMigrationExample {
    pub path: String,
    pub chunk_index: u64,
    pub reason: String,
}

/// Per-node summary used by the path-migration planner. Holds just enough
/// info to score a node for tier+DC fit without re-locking the state machine
/// for each candidate.
pub(crate) struct NodeSummary {
    pub grpc_addr: String,
    pub datacenter: String,
    pub healthy: bool,
    pub tiers: Vec<DiskTier>,
}

/// Get current time in milliseconds since UNIX epoch.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Bug W (part 2) — return a tonic-ready endpoint URL given a stored
/// `NodeInfo.grpc_addr`. New deployments store a fully-formed URL
/// (`http://10.150.0.157:9500`); pre-Bug-W deployments may have stored a
/// bare `host:port`. This helper accepts both and never produces the
/// `http://http://...` form that broke every migration.
#[inline]
pub(crate) fn normalize_endpoint(stored_addr: &str) -> String {
    if stored_addr.starts_with("http://") || stored_addr.starts_with("https://") {
        stored_addr.to_string()
    } else {
        format!("http://{}", stored_addr)
    }
}

/// Bug BP-4 — GC orphan classification. A physical chunk is an orphan
/// (eligible for reap) ONLY when no FileMetadata references its hash;
/// `in_state_chunks` is the union of every file's chunk hashes. Keying on
/// the hash (not on a per-disk location/UUID match) is the load-bearing
/// safety invariant: a chunk a file still points at must never be deleted,
/// even if its location record looks stale. AGENTS.md §A.3.3 #4 / §A.6 #8.
pub(crate) fn chunk_is_orphan(
    in_state_chunks: &std::collections::HashSet<String>,
    chunk_hash: &str,
) -> bool {
    !in_state_chunks.contains(chunk_hash)
}

/// Bug CB — is an on-disk chunk due to be reaped this tick? A chunk is
/// reaped only when it is unreferenced (`is_orphan`) AND its durable
/// on-disk `.dat` mtime is at least `grace_secs` old. Anchoring the grace
/// to the chunk's mtime (reported by ListChunks) instead of a
/// manager-local "first observed orphan" timestamp makes the grace clock
/// survive manager restarts and leadership changes, so a deleted file's
/// chunks reclaim even under rolling-restart churn — the bug where disk
/// usage of deleted files never dropped.
///
/// `chunk_mtime_ms == 0` means "age unknown" — a pre-Bug-CB data node
/// that does not populate `chunk_mtimes_ms`, or a stat failure. In that
/// case we DEFER (return false) rather than reap with no grace, so the
/// in-flight put/promote race can never drop a just-written chunk during
/// a mixed-version rollout. Reaping resumes once the data node is
/// upgraded to report mtimes.
pub(crate) fn orphan_reap_due(
    is_orphan: bool,
    chunk_mtime_ms: u64,
    now_ms: u64,
    grace_secs: u64,
) -> bool {
    if !is_orphan || chunk_mtime_ms == 0 {
        return false;
    }
    let age_secs = now_ms.saturating_sub(chunk_mtime_ms) / 1000;
    age_secs >= grace_secs
}

/// Bug BO-1 — which node id (if any) `resolve_healthy_source` must exclude
/// when copying a chunk for an anchor-lag task. For a same-node cross-tier
/// copy (`source_node == target_node`) the target is the ONLY holder of the
/// chunk (on its other tier), so excluding it strands the copy with "no
/// healthy source replica". Returns `None` in that case; `Some(target)`
/// otherwise (unchanged behaviour for cross-node copies).
pub(crate) fn anchor_copy_source_exclude(source_node: u64, target_node: u64) -> Option<u64> {
    if source_node == target_node {
        None
    } else {
        Some(target_node)
    }
}

/// Bug BO-2 — should a task with this status keep a chunk out of the
/// anchor-lag re-queue set? Only non-Done tasks block a re-queue; a Failed
/// task lingers (Bug AE 30-min retention) so the same chunk is NOT
/// immediately re-scheduled every 30s tick. (Previously Failed was excluded
/// too → tight re-queue loop + audit spam.)
pub(crate) fn task_blocks_requeue(status: MigrationStatus) -> bool {
    status != MigrationStatus::Done
}

// ---------------------------------------------------------------------------
// Bug AV (AV-3-3) — anchor-lag-repair pure-planner kernel
// ---------------------------------------------------------------------------

/// AV-3-3 — projection of a node's state used by the anchor-lag
/// planner.
#[derive(Debug, Clone)]
pub(crate) struct AnchorRepairNode {
    pub node_id: u64,
    pub datacenter: String,
    pub healthy: bool,
    pub tiers: Vec<DiskTier>,
}

/// AV-3-3 — one of a chunk's existing replicas.
#[derive(Debug, Clone)]
pub(crate) struct AnchorRepairPlacement {
    pub node_id: u64,
    pub dc: String,
    pub tier: DiskTier,
}

/// AV-3-3 — the planner's output: a single anchor-repair action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnchorRepairAction {
    pub source_node: u64,
    pub target_node: u64,
    pub target_tier: DiskTier,
}

/// AV-3-3 pure kernel — decide whether a chunk needs an anchor
/// repair, and if so, pick `source` + `target` for the replication
/// task.
///
/// Returns `Some(AnchorRepairAction)` when:
/// 1. No existing placement satisfies the anchor invariant
///    (`AnchorPolicy::is_satisfied_by`-compatible check inline).
/// 2. A healthy candidate node exists in the anchor DC with the
///    required tier (or any tier, if `required_tier` is `None`).
/// 3. The action isn't already in flight
///    (`(chunk_id, target_node) ∉ existing_tasks`).
///
/// Returns `None` when the invariant is already met, no
/// candidate exists, or the action is already scheduled.
///
/// Target tier selection: when `required_tier` is set, use it
/// exactly. Otherwise prefer cold (the durable anchor tier) if
/// any node in the anchor DC has a cold disk; else fall back to
/// any tier the candidate offers.
/// Bug BX-4 — per-tier replica shortfall for one logical chunk. Returns
/// `(tier, deficit)` for every mandated tier with fewer replicas than the
/// volume requires. Pure + testable; replaces the old single-tier-from-
/// source derivation that missed a mandated tier with ZERO replicas
/// entirely (e.g. a chunk that landed only on cold under a warm=2,cold=1
/// volume never got its warm copies scheduled).
pub(crate) fn under_replicated_tiers<F: Fn(DiskTier) -> u32>(
    count_on_tier: F,
    mandatory: &TierReplication,
) -> Vec<(DiskTier, u32)> {
    let mut out = Vec::new();
    for tier in [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold, DiskTier::Tape] {
        let want = mandatory.for_tier(tier);
        let have = count_on_tier(tier);
        if want > have {
            out.push((tier, want - have));
        }
    }
    out
}

/// Bug BX-5 — choose ONE safe excess replica to drop from an
/// over-replicated chunk so it converges toward its goal, without dropping
/// below any mandated tier or stranding the anchor, and without touching
/// legitimate cache replicas (within `cache_allowance`). Returns the
/// `node_id` to remove, or None when no safe removal exists.
///
/// Only acts when the chunk is genuinely OVER goal
/// (`total > mandatory_total + cache_allowance`) — the same threshold
/// BX-3 uses for `Overgoal` — so a chunk carrying mandated + legit cache
/// copies is left alone (the tiering engine owns cache demotion). A
/// candidate removal is accepted only if every mandated tier AND the
/// anchor remain satisfied afterwards, so this can never under-replicate
/// or strand the anchor. Pure + deterministic (first safe candidate in
/// replica order).
pub(crate) fn plan_excess_spill_removal(
    replicas: &[(u64, String, DiskTier)],
    mandatory: &TierReplication,
    anchor: Option<&dtfs_common::types::AnchorPolicy>,
    cache_allowance: u32,
) -> Option<u64> {
    let total = replicas.len() as u32;
    let mandatory_total = mandatory.hot + mandatory.warm + mandatory.cold + mandatory.tape;
    if total <= mandatory_total + cache_allowance {
        return None; // not over goal — leave cache/tiering alone
    }
    for (idx, (node, _dc, _tier)) in replicas.iter().enumerate() {
        let remaining: Vec<&(u64, String, DiskTier)> = replicas
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, r)| r)
            .collect();
        let on_tier = |t: DiskTier| remaining.iter().filter(|(_, _, tt)| *tt == t).count() as u32;
        let tiers_ok = on_tier(DiskTier::Hot) >= mandatory.hot
            && on_tier(DiskTier::Warm) >= mandatory.warm
            && on_tier(DiskTier::Cold) >= mandatory.cold
            && on_tier(DiskTier::Tape) >= mandatory.tape;
        let anchor_ok = match anchor {
            None => true,
            Some(a) => remaining.iter().any(|(_, dc, tt)| {
                dc == &a.datacenter
                    && match a.required_tier {
                        Some(rt) => *tt == rt,
                        None => true,
                    }
            }),
        };
        if tiers_ok && anchor_ok {
            return Some(*node);
        }
    }
    None
}

#[cfg(test)]
mod bx5_excess_removal_tests {
    use super::plan_excess_spill_removal;
    use dtfs_common::types::{AnchorPolicy, DiskTier, TierReplication};

    fn rep(node: u64, dc: &str, tier: DiskTier) -> (u64, String, DiskTier) {
        (node, dc.to_string(), tier)
    }

    // Spilled extra copy beyond mandatory+cache is removed, keeping mandated
    // tiers + anchor satisfied.
    #[test]
    fn removes_excess_keeps_goal() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }; // total 1
        let anchor = AnchorPolicy { datacenter: "dc-a".into(), required_tier: Some(DiskTier::Cold) };
        // cold anchor (node 1) + a spilled hot copy (node 2). allowance 0 -> over.
        let reps = vec![rep(1, "dc-a", DiskTier::Cold), rep(2, "dc-b", DiskTier::Hot)];
        // The removable one is node 2 (removing node 1 would strand the anchor).
        assert_eq!(plan_excess_spill_removal(&reps, &m, Some(&anchor), 0), Some(2));
    }

    // Within mandatory+cache allowance -> nothing removed (don't fight tiering).
    #[test]
    fn keeps_legit_cache_replica() {
        let m = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let reps = vec![rep(1, "dc-a", DiskTier::Cold), rep(2, "dc-a", DiskTier::Warm)];
        // total 2 == mandatory 1 + cache allowance 1 -> not over.
        assert_eq!(plan_excess_spill_removal(&reps, &m, None, 1), None);
    }

    // Never drops below a mandated tier: removing the only mandated copy is refused.
    #[test]
    fn refuses_to_underreplicate() {
        let m = TierReplication { hot: 0, warm: 2, cold: 0, tape: 0 }; // total 2
        // 2 warm (exactly mandated) -> total 2, not over (allowance 0) -> None.
        let reps = vec![rep(1, "dc-a", DiskTier::Warm), rep(2, "dc-b", DiskTier::Warm)];
        assert_eq!(plan_excess_spill_removal(&reps, &m, None, 0), None);
    }
}

#[cfg(test)]
mod bx4_under_replication_tests {
    use super::under_replicated_tiers;
    use dtfs_common::types::{DiskTier, TierReplication};

    // Bug BX-4 — a chunk on cold-only under warm=2,cold=1 reports warm short
    // by 2 (pre-fix: the warm shortfall was invisible).
    #[test]
    fn detects_missing_mandated_tier() {
        let mandatory = TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        let cold_only = |t: DiskTier| if t == DiskTier::Cold { 1 } else { 0 };
        assert_eq!(
            under_replicated_tiers(cold_only, &mandatory),
            vec![(DiskTier::Warm, 2)]
        );
        // Fully satisfied -> nothing short.
        let ok = |t: DiskTier| match t {
            DiskTier::Warm => 2,
            DiskTier::Cold => 1,
            _ => 0,
        };
        assert!(under_replicated_tiers(ok, &mandatory).is_empty());
        // Cache-only tiers (mandatory 0) are never reported short.
        let none = |_t: DiskTier| 0u32;
        let cache_vol = TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 };
        assert!(under_replicated_tiers(none, &cache_vol).is_empty());
    }
}

pub(crate) fn plan_anchor_lag_repair(
    anchor: &dtfs_common::types::AnchorPolicy,
    placements: &[AnchorRepairPlacement],
    nodes: &[AnchorRepairNode],
    chunk_id: &str,
    existing_tasks: &std::collections::HashSet<(String, u64)>,
) -> Option<AnchorRepairAction> {
    // Gate 1 — is the anchor already satisfied?
    let satisfied = placements.iter().any(|p| {
        p.dc == anchor.datacenter
            && match anchor.required_tier {
                Some(t) => p.tier == t,
                None => true,
            }
    });
    if satisfied {
        return None;
    }

    // Gate 2 — pick a target tier. Prefer the required_tier if set,
    // else cold (the durable anchor floor), else any tier available
    // in the anchor DC.
    let candidate_tier = anchor.required_tier.unwrap_or(DiskTier::Cold);

    // Gate 3 — find a healthy node in the anchor DC with the
    // candidate tier. Bug fix (BF-followup): exclude only nodes that
    // already hold the chunk ON THE CANDIDATE TIER. A node that
    // holds the chunk on warm AND also has a cold disk is a valid
    // target for the cold anchor copy — that single-node
    // multi-tier case is the lab topology (data01 has warm+cold)
    // and previously the holders-set excluded data01 entirely,
    // stranding the anchor-lag pass + leaving cold=0 forever.
    let tier_holders: std::collections::HashSet<u64> = placements
        .iter()
        .filter(|p| p.tier == candidate_tier)
        .map(|p| p.node_id)
        .collect();
    let target = nodes
        .iter()
        .filter(|n| n.healthy)
        .filter(|n| n.datacenter == anchor.datacenter)
        .filter(|n| n.tiers.contains(&candidate_tier))
        .filter(|n| !tier_holders.contains(&n.node_id))
        .min_by_key(|n| n.node_id)?; // deterministic by node_id

    // Gate 4 — already scheduled?
    if existing_tasks.contains(&(chunk_id.to_string(), target.node_id)) {
        return None;
    }

    // Source = any existing replica (the dispatcher's
    // `ReplicateChunk` RPC handles cross-tier reads).
    let source = placements
        .iter()
        .min_by_key(|p| p.node_id)
        .map(|p| p.node_id)?;

    Some(AnchorRepairAction {
        source_node: source,
        target_node: target.node_id,
        target_tier: candidate_tier,
    })
}

// ---------------------------------------------------------------------------
// Bug AE — orphan migration task helpers (pure logic, unit-tested below)
// ---------------------------------------------------------------------------

/// How long a `Failed` task lingers in `migration_state_json` before the
/// scheduler garbage-collects it. 30 minutes is long enough for an operator
/// alerted on `migration.failed` to inspect the task in the UI before it
/// disappears, and short enough that a mass-orphan event (e.g. a data-node
/// disk wipe) doesn't bloat the Raft snapshot for hours.
const FAILED_TASK_RETENTION_MS: u64 = 30 * 60 * 1000;

/// How long a `Done` task lingers — kept short because the new replica is
/// already visible in the file's chunk list and the historical task entry
/// is operationally uninteresting beyond a few minutes.
const DONE_TASK_RETENTION_MS: u64 = 5 * 60 * 1000;

/// Pure predicate for the scheduler's task-prune retain block. Returns true
/// when the task should stay in the migration state, false when it should
/// be dropped.
///
/// Terminal tasks (`Done`, `Failed`) are dropped once their age exceeds the
/// per-status retention window. Non-terminal tasks (`Pending`, `Copying`,
/// `Verifying`, `Completing`) are always kept — the state machine in
/// `advance_tasks` is responsible for transitioning them onward, including
/// the Bug AE orphan check that fast-forwards orphan tasks to `Failed`.
#[inline]
pub(crate) fn should_retain_task(
    status: MigrationStatus,
    updated_at_ms: u64,
    now_ms: u64,
) -> bool {
    match status {
        MigrationStatus::Done => {
            now_ms.saturating_sub(updated_at_ms) < DONE_TASK_RETENTION_MS
        }
        MigrationStatus::Failed => {
            now_ms.saturating_sub(updated_at_ms) < FAILED_TASK_RETENTION_MS
        }
        _ => true,
    }
}

/// Bug AF — pure predicate: does this RPC error string indicate the source
/// data node specifically said "Chunk not found"? When true, the scheduler
/// fast-fails the migration task (bypasses MAX_MIGRATION_ATTEMPTS) so we
/// don't burn 6 × 30s of data-node load on a chunk that's gone.
///
/// Matches both shapes seen in the wild:
///   - "ReplicateChunk error: ReplicateChunk returned error: Chunk not found: <hash>"
///   - "Chunk not found"
#[inline]
pub(crate) fn rpc_error_indicates_chunk_missing(error_message: &str) -> bool {
    error_message.contains("Chunk not found")
}

/// Pure helper for `chunk_exists_anywhere`. Returns true iff `chunk_hash`
/// appears in any `FileMetadata.chunks` entry across the supplied `files`
/// map. The async caller pulls the state-machine read lock; this function
/// stays sync so it's trivially unit-testable without a tokio runtime.
#[inline]
pub(crate) fn chunk_in_any_file(
    chunk_hash: &str,
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
) -> bool {
    files
        .values()
        .any(|f| f.chunks.iter().any(|c| c.chunk_hash == chunk_hash))
}

// ---------------------------------------------------------------------------
// Bug CH — node/disk-loss-aware replica accounting + dispatch target guard
// (pure logic, unit-tested below). See PLAN-BUG-CH /
// ISSUE-SCAN-2026-06-06-2335 for the incident these prevent.
// ---------------------------------------------------------------------------

/// Bug CH — resolve the tier of a replica row ONLY when that replica is
/// actually serviceable: node health not Down/Failed AND backing disk not
/// Failed/Removed. `check_under_replicated` counts coverage through this,
/// so losing a node (or a disk) makes its chunks tier-short and the
/// existing BX-4 backfill repairs them — replacing the monitor's deleted
/// evacuation path as the single, leader-gated repair owner.
///
/// Suspect deliberately still counts: a heartbeat ≤30s overdue must not
/// trigger a cluster-wide re-replication wave.
#[inline]
pub(crate) fn live_replica_tier(
    node: Option<&dtfs_raft::NodeInfo>,
    disk_id: u32,
) -> Option<DiskTier> {
    let node = node?;
    if matches!(node.health, HealthStatus::Down | HealthStatus::Failed) {
        return None;
    }
    let disk = node.disks.get(disk_id as usize)?;
    if matches!(disk.status, DiskStatus::Failed | DiskStatus::Removed) {
        return None;
    }
    Some(disk.tier)
}

/// Bug CH — dispatch-time guard: can this task's target possibly receive
/// the copy? Err for a target that is unregistered, not a Data node,
/// Down/Failed/Quarantined (BC: quarantined nodes are excluded from every
/// allocation path), or without a live disk of the task's tier. Suspect
/// targets are allowed — the normal attempt/retry machinery handles a
/// transiently slow node.
#[inline]
pub(crate) fn validate_task_target(
    node: Option<&dtfs_raft::NodeInfo>,
    tier: DiskTier,
) -> Result<(), &'static str> {
    let Some(node) = node else {
        return Err("target node not registered");
    };
    if node.role != NodeRole::Data {
        return Err("target node is not a data node");
    }
    if matches!(
        node.health,
        HealthStatus::Down | HealthStatus::Failed | HealthStatus::Quarantined
    ) {
        return Err("target node is down/failed/quarantined");
    }
    let has_live_tier = node.disks.iter().any(|d| {
        d.tier == tier && !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed)
    });
    if !has_live_tier {
        return Err("target node has no live disk of the task tier");
    }
    Ok(())
}

#[cfg(test)]
mod bug_ch_live_replica_tests {
    use super::live_replica_tier;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole};

    fn disk(tier: DiskTier, status: DiskStatus) -> DiskInfo {
        DiskInfo {
            disk_id: 0,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: 0,
            used_bytes: 0,
            status,
        }
    }

    fn node(health: HealthStatus, disks: Vec<DiskInfo>) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role: NodeRole::Data,
            disks,
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    // Bug CH — a replica on a Down node must not count toward mandatory
    // tier coverage (pre-fix: tier_of ignored health, so the only cold
    // replica sitting on a dead node satisfied cold=1 and node loss was
    // never repaired by the leader tick).
    #[test]
    fn down_node_replica_does_not_count() {
        let n = node(HealthStatus::Down, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
    }

    #[test]
    fn failed_node_replica_does_not_count() {
        let n = node(HealthStatus::Failed, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
    }

    // Replica on a Failed/Removed disk of an otherwise healthy node is
    // equally dead (the handle_disk_failed case, now owned by the tick).
    #[test]
    fn failed_disk_replica_does_not_count() {
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Warm, DiskStatus::Failed)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Warm, DiskStatus::Removed)]);
        assert_eq!(live_replica_tier(Some(&n), 0), None);
    }

    #[test]
    fn healthy_replica_counts() {
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), Some(DiskTier::Cold));
    }

    // Suspect = heartbeat ≤30s overdue. Still counts — a transient stall
    // must not trigger a full re-replication wave (cross-cutting
    // invariant in PLAN-BUG-CH).
    #[test]
    fn suspect_node_replica_still_counts() {
        let n = node(HealthStatus::Suspect, vec![disk(DiskTier::Warm, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 0), Some(DiskTier::Warm));
    }

    #[test]
    fn unknown_node_or_disk_does_not_count() {
        assert_eq!(live_replica_tier(None, 0), None);
        let n = node(HealthStatus::Healthy, vec![disk(DiskTier::Cold, DiskStatus::Healthy)]);
        assert_eq!(live_replica_tier(Some(&n), 7), None); // disk index out of range
    }
}

#[cfg(test)]
mod bug_ch_target_validation_tests {
    use super::validate_task_target;
    use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier, HealthStatus, NodeRole};

    fn disk(tier: DiskTier, status: DiskStatus) -> DiskInfo {
        DiskInfo {
            disk_id: 0,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: 0,
            used_bytes: 0,
            status,
        }
    }

    fn node(role: NodeRole, health: HealthStatus, disks: Vec<DiskInfo>) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: String::new(),
            role,
            disks,
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    // Bug CH — the 2026-06-06 incident left 36k tasks targeting manager
    // and client node ids (monitor round-robin over every registered
    // node). Dispatch must fail these in one tick, not burn
    // MAX_MIGRATION_ATTEMPTS gRPC calls against a manager's raft port.
    #[test]
    fn manager_target_rejected() {
        let n = node(NodeRole::Manager, HealthStatus::Healthy, vec![]);
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn client_target_rejected() {
        let n = node(NodeRole::Client, HealthStatus::Healthy, vec![]);
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn missing_node_rejected() {
        assert!(validate_task_target(None, DiskTier::Warm).is_err());
    }

    // Hot was hardcoded by the monitor; a data node without any hot disk
    // can never satisfy the task's tier.
    #[test]
    fn data_node_without_tier_rejected() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Healthy,
            vec![disk(DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Hot).is_err());
    }

    #[test]
    fn data_node_with_only_dead_disk_of_tier_rejected() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Healthy,
            vec![disk(DiskTier::Warm, DiskStatus::Failed)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn unhealthy_data_node_rejected() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Down,
            vec![disk(DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_err());
    }

    #[test]
    fn healthy_data_node_with_live_tier_accepted() {
        let n = node(
            NodeRole::Data,
            HealthStatus::Healthy,
            vec![disk(DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert!(validate_task_target(Some(&n), DiskTier::Warm).is_ok());
    }
}

#[cfg(test)]
mod endpoint_tests {
    use super::*;

    /// Bug W (part 2) — a URL-formatted stored address passes through
    /// unchanged. Prior bug double-prefixed `http://` and tonic returned
    /// `transport error` for every migration RPC.
    #[test]
    fn bug_w_endpoint_url_passthrough() {
        let stored = "http://10.150.0.157:9500";
        assert_eq!(normalize_endpoint(stored), "http://10.150.0.157:9500");
    }

    /// Pre-Bug-W stored addresses (bare host:port) still get the http://
    /// prefix so we don't break older clusters during an upgrade.
    #[test]
    fn bug_w_endpoint_bare_host_gets_scheme() {
        let stored = "10.150.0.157:9500";
        assert_eq!(normalize_endpoint(stored), "http://10.150.0.157:9500");
    }

    /// HTTPS is also passed through (future-proofing for TLS rollout).
    #[test]
    fn bug_w_endpoint_https_passthrough() {
        let stored = "https://data-warm01:9500";
        assert_eq!(normalize_endpoint(stored), "https://data-warm01:9500");
    }
}

#[cfg(test)]
mod bug_bh_dedup_tests {
    use super::*;

    fn ent(node: &str, disk: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "h".to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: 0,
        }
    }

    /// BH-1 — the dedup guard must key on (node_id, disk_id). A node that
    /// holds the chunk on disk 0 (warm) is NOT "already present" for a
    /// cold copy on disk 1. (Fails with the old node-only guard.)
    #[test]
    fn replica_present_dedupes_by_node_and_disk() {
        let entries = vec![ent("A", 0)];
        assert!(
            !replica_already_present(&entries, "A", 1),
            "same node, different disk/tier must NOT count as present"
        );
        assert!(
            replica_already_present(&entries, "A", 0),
            "exact (node, disk) match must count as present (idempotency)"
        );
        assert!(
            !replica_already_present(&entries, "B", 0),
            "different node must NOT count as present"
        );
    }

    /// BH-2 — the completion path must RECORD a same-node cold replica
    /// when the anchor target already holds the chunk on its warm disk.
    /// Mirrors the dispatcher push logic; pre-BH (node-only guard) this
    /// leaves 2 entries (cold never recorded) → infinite anchor-lag loop.
    #[test]
    fn completion_records_same_node_cold_replica() {
        // Existing warm replicas on both data nodes (warm disk = 0).
        let mut entries = vec![ent("A", 0), ent("B", 0)];
        // Anchor-cold repair targets B; its cold disk resolved to id 1.
        let target = "B";
        let resolved_cold_disk = 1u32;
        if !replica_already_present(&entries, target, resolved_cold_disk) {
            entries.push(ent(target, resolved_cold_disk));
        }
        assert_eq!(
            entries.len(),
            3,
            "cold anchor on B must be recorded alongside its warm copy"
        );
        assert!(
            entries.iter().any(|e| e.node_id == "B" && e.disk_id == 1),
            "the new B-cold entry must be present"
        );
        // Integrity invariant (A.3.3): existing warm entries untouched.
        assert!(entries.iter().any(|e| e.node_id == "A" && e.disk_id == 0));
        assert!(entries.iter().any(|e| e.node_id == "B" && e.disk_id == 0));
    }
}

#[cfg(test)]
mod path_migration_tests {
    use super::*;

    fn chunk(idx: u64, hash: &str, node: u64, disk: u32, size: u64) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: size,
        }
    }

    fn node(
        dc: &str,
        healthy: bool,
        tiers: &[DiskTier],
    ) -> NodeSummary {
        NodeSummary {
            grpc_addr: "127.0.0.1:9500".to_string(),
            datacenter: dc.to_string(),
            healthy,
            tiers: tiers.to_vec(),
        }
    }

    /// 1 file, 1 chunk currently on a hot DC-A node. Plan a move to warm
    /// tier with no DC constraint. Expect exactly one task targeting a warm
    /// node (any DC).
    #[test]
    fn plans_replication_to_target_tier() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) =
            plan_path_migration(&files, &nodes, DiskTier::Warm, None, 1000);

        assert_eq!(report.files_matched, 1);
        assert_eq!(report.chunks_total, 1);
        assert_eq!(report.already_satisfied, 0);
        assert_eq!(report.no_source, 0);
        assert_eq!(report.no_target, 0);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].source_node, 10);
        // 12 < 14 so deterministic ordering picks node 12.
        assert_eq!(plan[0].target_node, 12);
        assert_eq!(plan[0].tier, DiskTier::Warm);
    }

    /// Same setup but require target_dc = "dc-c". Target must be node 14
    /// even though node 12 is also a warm-tier candidate.
    #[test]
    fn target_dc_constraint_is_enforced() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );

        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].target_node, 14);
        assert_eq!(report.tasks_scheduled, 1);
    }

    /// If at least one existing replica already satisfies the constraint,
    /// no task should be scheduled (additive semantics).
    #[test]
    fn already_satisfied_chunks_are_skipped() {
        let files = vec![(
            "/movies/Bar.mkv".to_string(),
            vec![
                chunk(0, "h0", 10, 0, 128), // dc-a, hot
                chunk(0, "h0", 14, 0, 128), // dc-c, warm
            ],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );

        assert_eq!(report.already_satisfied, 1);
        assert_eq!(plan.len(), 0);
    }

    /// Only files starting with `path_prefix` are considered. Matching is
    /// `starts_with` against the absolute path stored in the SM.
    #[test]
    fn path_prefix_filters_files() {
        let files = vec![
            (
                "/movies/Season 1/E01.mkv".to_string(),
                vec![chunk(0, "h0", 10, 0, 128)],
            ),
            (
                "/movies/Season 2/E01.mkv".to_string(),
                vec![chunk(0, "h1", 10, 0, 128)],
            ),
            (
                "/audiobooks/Foo.m4b".to_string(),
                vec![chunk(0, "h2", 10, 0, 128)],
            ),
        ];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        // Filter to "/movies/Season 1/" — only the first file should match.
        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            None,
            1000,
        );

        // Sanity: with no prefix all three match.
        assert_eq!(report.files_matched, 3);

        // Now actually invoke the planner with a filtered slice — emulates
        // what the async wrapper does inside the read-lock.
        let filtered: Vec<_> = files
            .iter()
            .filter(|(p, _)| p.starts_with("/movies/Season 1/"))
            .cloned()
            .collect();
        let (r2, p2) =
            plan_path_migration(&filtered, &nodes, DiskTier::Warm, None, 1000);
        assert_eq!(r2.files_matched, 1);
        assert_eq!(p2.len(), 1);
        let _ = plan; // suppress unused
    }

    /// When the only existing replica is on a Down node we cannot copy
    /// from it. The chunk is reported as `no_source`.
    #[test]
    fn no_healthy_source_is_reported() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", false /* down */, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) =
            plan_path_migration(&files, &nodes, DiskTier::Warm, None, 1000);
        assert_eq!(report.no_source, 1);
        assert_eq!(report.no_target, 0);
        assert_eq!(plan.len(), 0);
        assert_eq!(report.examples.len(), 1);
        assert_eq!(report.examples[0].reason, "no_source");
    }

    /// No node in the requested DC has the requested tier disk → `no_target`.
    #[test]
    fn no_matching_target_dc_is_reported() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![chunk(0, "h0", 10, 0, 128)],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (12, node("dc-b", true, &[DiskTier::Warm])),
            // dc-c has no warm-tier node.
            (14, node("dc-c", true, &[DiskTier::Cold])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(report.no_target, 1);
        assert_eq!(plan.len(), 0);
        assert_eq!(report.examples[0].reason, "no_target");
    }

    /// Two files, one chunk each, both need a warm-DC-C copy. Targets must
    /// be different nodes so two chunks don't pile onto a single node when
    /// alternatives exist.
    #[test]
    fn spreads_chunks_across_target_nodes_when_possible() {
        let files = vec![
            (
                "/movies/A.mkv".to_string(),
                vec![chunk(0, "h0", 10, 0, 128)],
            ),
            (
                "/movies/B.mkv".to_string(),
                vec![chunk(0, "h1", 10, 0, 128)],
            ),
        ];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
            (15, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (_report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(plan.len(), 2);
        // Distinct chunk hashes, so already_targeted only kicks in within
        // the same hash. Both chunks should still get warm-DC-C targets.
        for action in &plan {
            assert!(action.target_node == 14 || action.target_node == 15);
        }
    }

    /// max_tasks limits how many tasks we plan. The rest are counted under
    /// truncated_chunks so the operator knows to re-run.
    #[test]
    fn max_tasks_caps_the_plan() {
        let files: Vec<_> = (0..5u64)
            .map(|i| {
                (
                    format!("/movies/{i}.mkv"),
                    vec![chunk(0, &format!("h{i}"), 10, 0, 128)],
                )
            })
            .collect();
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
            (15, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            2, // cap
        );
        assert_eq!(plan.len(), 2);
        assert_eq!(report.chunks_total, 5);
        assert_eq!(report.truncated_chunks, 3);
    }

    /// All replicas already on warm-DC-C → planner produces nothing.
    #[test]
    fn no_op_when_all_chunks_satisfied() {
        let files = vec![(
            "/movies/Foo.mkv".to_string(),
            vec![
                chunk(0, "h0", 14, 0, 128),
                chunk(1, "h1", 15, 0, 128),
            ],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (14, node("dc-c", true, &[DiskTier::Warm])),
            (15, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(plan.len(), 0);
        assert_eq!(report.already_satisfied, 2);
    }

    /// Multi-chunk file with multiple replicas per chunk: planner should
    /// still consider each chunk_index exactly once.
    #[test]
    fn multi_chunk_file_groups_by_chunk_index() {
        let files = vec![(
            "/movies/Big.mkv".to_string(),
            vec![
                chunk(0, "h0", 10, 0, 128),
                chunk(0, "h0", 11, 0, 128), // replica of the same chunk_index
                chunk(1, "h1", 10, 0, 128),
            ],
        )];
        let nodes: HashMap<u64, NodeSummary> = HashMap::from([
            (10, node("dc-a", true, &[DiskTier::Hot])),
            (11, node("dc-a", true, &[DiskTier::Hot])),
            (14, node("dc-c", true, &[DiskTier::Warm])),
        ]);

        let (report, plan) = plan_path_migration(
            &files,
            &nodes,
            DiskTier::Warm,
            Some("dc-c"),
            1000,
        );
        assert_eq!(report.chunks_total, 2); // chunk_index 0 + 1, not 3 rows
        assert_eq!(plan.len(), 2);
    }
}

#[cfg(test)]
mod bug_av_flush_cache_tests {
    //! Bug AV (AV-1-7) — pure-planner tests for the cache-flush
    //! routine. The async wrapper that dispatches DeleteChunkReplica +
    //! issues Raft writes is covered live (AV-1-7 "Done when"
    //! verification on the minicluster); these tests cover the
    //! decision logic.
    use super::*;

    fn ent(chunk_index: u64, hash: &str, node: u64, disk: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: 1024,
        }
    }

    fn node_summary(dc: &str, tiers: &[DiskTier]) -> FlushNodeSummary {
        FlushNodeSummary {
            datacenter: dc.to_string(),
            disk_tiers: tiers.to_vec(),
        }
    }

    fn mandatory_cold_only() -> TierReplication {
        TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 }
    }

    fn mandatory_warm_and_cold() -> TierReplication {
        TierReplication { hot: 0, warm: 1, cold: 1, tape: 0 }
    }

    /// AV-1-7 test 1 — happy path: a file with one cold (mandatory) +
    /// one warm (cache) replica has its warm replica flushed; cold
    /// preserved.
    #[test]
    fn bug_av_plan_flush_drops_warm_keeps_cold() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/wicked.mkv".to_string(),
                vec![
                    ent(0, "h", 10, 0), // node 10 = cold
                    ent(0, "h", 11, 0), // node 11 = warm
                ],
                mandatory_cold_only(),
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 1, "drops the warm replica");
        assert_eq!(actions[0].node_id, 11);
        assert_eq!(actions[0].chunk_id, "h");
        assert_eq!(actions[0].chunk_index, 0);
        assert_eq!(report.replicas_dropped, 0, "planner doesn't run the deletes — wrapper does");
        assert_eq!(report.mandatory_refusals, 0);
    }

    /// AV-1-7 test 2 — refuse to flush if doing so would leave no
    /// mandatory-tier replica. Volume mandates 1 cold, file only has
    /// warm replicas (state shouldn't normally happen, but defence
    /// in depth) → refuse.
    #[test]
    fn bug_av_plan_flush_refuses_when_anchor_only_on_warm() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/oops.mkv".to_string(),
                vec![
                    ent(0, "h", 11, 0), // node 11 = warm (only)
                    ent(0, "h", 12, 0), // node 12 = warm (only)
                ],
                mandatory_cold_only(),
            )],
            nodes: HashMap::from([
                (11u64, node_summary("toro", &[DiskTier::Warm])),
                (12u64, node_summary("nrb",  &[DiskTier::Warm])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty(), "no actions when mandatory not satisfied");
        assert_eq!(report.mandatory_refusals, 1);
    }

    /// AV-1-7 test 3 — `max_tasks` caps the planned actions; truncated
    /// counter reports the remaining unseen.
    #[test]
    fn bug_av_plan_flush_respects_max_tasks() {
        // 5 files, each with 1 chunk, each with cold + warm replicas.
        let files: Vec<_> = (0..5u64)
            .map(|i| {
                (
                    format!("/movies/f{i}.bin"),
                    vec![ent(0, &format!("h{i}"), 10, 0), ent(0, &format!("h{i}"), 11, 0)],
                    mandatory_cold_only(),
                )
            })
            .collect();
        let snapshot = FlushSnapshot {
            files,
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 3);
        assert_eq!(actions.len(), 3, "max_tasks honoured");
        assert!(report.truncated > 0, "remaining counted as truncated");
    }

    /// AV-1-7 test 4 — no cache-tier replicas → empty plan, no refusals.
    #[test]
    fn bug_av_plan_flush_empty_when_no_cache_tier_replicas() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/cold-only.mkv".to_string(),
                vec![ent(0, "h", 10, 0)], // cold only
                mandatory_cold_only(),
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty());
        assert_eq!(report.mandatory_refusals, 0);
        assert_eq!(report.files_matched, 1);
        assert_eq!(report.chunks_examined, 1);
    }

    /// AV-1-11 test 1 — every stranded chunk produces a `FlushRefusal`
    /// with the stable `"mandatory_strand"` reason code. The async
    /// dispatcher converts each entry to one `cache.flush_refused`
    /// audit row + one deduped `AnchorViolated` alert (live-verified;
    /// see verification log).
    #[test]
    fn bug_av_planner_records_refusal_per_stranded_chunk() {
        // Two stranded chunks: same file, different indexes.
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/oops.mkv".to_string(),
                vec![
                    ent(0, "h0", 11, 0), // warm only
                    ent(0, "h0", 12, 0), // warm only
                    ent(1, "h1", 11, 0), // warm only
                    ent(1, "h1", 12, 0), // warm only
                ],
                mandatory_cold_only(),
            )],
            nodes: HashMap::from([
                (11u64, node_summary("toro", &[DiskTier::Warm])),
                (12u64, node_summary("nrb",  &[DiskTier::Warm])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert!(actions.is_empty(), "nothing to drop without mandatory cold");
        assert_eq!(report.mandatory_refusals, 2);
        assert_eq!(report.refusals.len(), 2);
        for r in &report.refusals {
            assert_eq!(r.file_path, "/movies/oops.mkv");
            assert_eq!(r.reason, "mandatory_strand");
        }
        let mut idxs: Vec<u64> =
            report.refusals.iter().map(|r| r.chunk_index).collect();
        idxs.sort();
        assert_eq!(idxs, vec![0, 1]);
    }

    /// AV-1-11 test 2 — the happy path leaves `refusals` empty so the
    /// dispatcher doesn't emit a spurious alert when everything was
    /// flushable.
    #[test]
    fn bug_av_planner_no_refusals_on_happy_path() {
        let snapshot = FlushSnapshot {
            files: vec![(
                "/movies/wicked.mkv".to_string(),
                vec![ent(0, "h", 10, 0), ent(0, "h", 11, 0)],
                mandatory_cold_only(),
            )],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        assert_eq!(actions.len(), 1);
        assert!(
            report.refusals.is_empty(),
            "no refusals → dispatcher must not fire AnchorViolated alert"
        );
    }

    /// AV-1-11 test 3 — the public JSON shape of `FlushCacheReport`
    /// must not include the per-refusal list (it can be large for big
    /// prefixes; operator gets the counter + audit log entries). Guard
    /// against accidental `#[serde(skip)]` removal during refactors.
    #[test]
    fn bug_av_report_serde_skips_refusals_field() {
        let mut report = FlushCacheReport::default();
        report.refusals.push(FlushRefusal {
            file_path: "/x".to_string(),
            chunk_index: 0,
            reason: "mandatory_strand",
        });
        report.mandatory_refusals = 1;
        let json = serde_json::to_string(&report).unwrap();
        // Use the precise `"refusals":` token so we don't false-match
        // the legitimate `"mandatory_refusals":` counter.
        assert!(
            !json.contains("\"refusals\":"),
            "FlushCacheReport JSON wire shape must omit the 'refusals' field (was: {json})"
        );
        assert!(
            json.contains("\"mandatory_refusals\":1"),
            "counter must remain in JSON (was: {json})"
        );
    }

    /// AV-1-7 test 5 — two files in different volumes evaluated
    /// independently. A volume that mandates warm protects its warm
    /// replicas; a volume that doesn't mandate warm permits flushing.
    #[test]
    fn bug_av_plan_flush_multi_volume_isolation() {
        let snapshot = FlushSnapshot {
            files: vec![
                // Volume "default": cold mandatory; warm is cache → flushable.
                (
                    "/vol-default/foo.bin".to_string(),
                    vec![ent(0, "ha", 10, 0), ent(0, "ha", 11, 0)],
                    mandatory_cold_only(),
                ),
                // Volume "office": warm + cold both mandatory; warm
                // protected from flush even though caller asked for it.
                (
                    "/vol-office/bar.docx".to_string(),
                    vec![ent(0, "hb", 10, 0), ent(0, "hb", 11, 0)],
                    mandatory_warm_and_cold(),
                ),
            ],
            nodes: HashMap::from([
                (10u64, node_summary("leli", &[DiskTier::Cold])),
                (11u64, node_summary("toro", &[DiskTier::Warm])),
            ]),
            anchor: None,
        };
        let (actions, report) =
            plan_flush_cache(&snapshot, &[DiskTier::Warm], 1000);
        // Only foo.bin's warm replica drops.
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].file_path, "/vol-default/foo.bin");
        assert_eq!(report.replicas_dropped, 0, "wrapper drives this");
        assert_eq!(report.files_matched, 2);
    }
}

#[cfg(test)]
mod bug_av_anchor_lag_tests {
    //! AV-3-3 — pure tests for `plan_anchor_lag_repair`. The
    //! async dispatcher wiring (`check_anchor_lag`) is live-only
    //! per the AV-1-7 / AV-2-4 convention; these tests cover the
    //! decision logic in isolation.
    use super::*;
    use dtfs_common::types::AnchorPolicy;
    use std::collections::HashSet;

    fn node(id: u64, dc: &str, healthy: bool, tiers: &[DiskTier]) -> AnchorRepairNode {
        AnchorRepairNode {
            node_id: id,
            datacenter: dc.to_string(),
            healthy,
            tiers: tiers.to_vec(),
        }
    }

    fn placement(id: u64, dc: &str, tier: DiskTier) -> AnchorRepairPlacement {
        AnchorRepairPlacement { node_id: id, dc: dc.to_string(), tier }
    }

    /// AV-3-3 test 1 — a warm-first write has a single warm
    /// placement in `toro`. The Plexmedia anchor is
    /// `(leli, cold)`. The planner picks the only healthy LELI
    /// cold node as the repair target.
    #[test]
    fn bug_av_anchor_lag_repair_picks_anchor_dc_node() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
            node(12, "nrb",  true, &[DiskTier::Warm]),
        ];
        let existing: HashSet<(String, u64)> = HashSet::new();
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &existing);
        let a = action.expect("anchor missing → repair scheduled");
        assert_eq!(a.target_node, 10, "LELI cold node");
        assert_eq!(a.target_tier, DiskTier::Cold);
        assert_eq!(a.source_node, 11, "the only existing replica");
    }

    /// AV-3-3 test 2 — `required_tier = Cold` filters out a
    /// LELI warm node that *would* be in the anchor DC but on
    /// the wrong tier.
    #[test]
    fn bug_av_anchor_lag_repair_picks_required_tier() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            // LELI warm-only — must NOT be picked (wrong tier).
            node(15, "leli", true, &[DiskTier::Warm]),
            // LELI cold — the correct target.
            node(10, "leli", true, &[DiskTier::Cold]),
            // TORO warm — the existing source.
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        let a = action.expect("anchor repair scheduled");
        assert_eq!(a.target_node, 10);
        assert_eq!(a.target_tier, DiskTier::Cold);
    }

    /// AV-3-3 test 3 — when the anchor invariant is already
    /// satisfied (existing LELI cold replica), the planner
    /// returns `None`.
    #[test]
    fn bug_av_anchor_lag_repair_no_op_when_satisfied() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![
            placement(10, "leli", DiskTier::Cold), // anchor already present
            placement(11, "toro", DiskTier::Warm),
        ];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        assert!(action.is_none(), "anchor satisfied → no repair");
    }

    /// AV-3-3 defensive — `required_tier = None` accepts any
    /// tier in the anchor DC (preferring Cold by default per
    /// the planner's documented choice).
    #[test]
    fn bug_av_anchor_lag_repair_required_tier_none_defaults_to_cold() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: None,
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(15, "leli", true, &[DiskTier::Warm]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        let a = action.expect("required_tier=None still produces a repair");
        // Per the planner doc: defaults to Cold when required_tier
        // is None and a cold candidate exists.
        assert_eq!(a.target_tier, DiskTier::Cold);
        assert_eq!(a.target_node, 10);
    }

    /// AV-3-3 defensive — already-scheduled tasks are detected
    /// via the (chunk_id, target_node) fingerprint and don't
    /// produce a duplicate.
    #[test]
    fn bug_av_anchor_lag_repair_skips_already_scheduled() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            node(10, "leli", true, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let mut existing = HashSet::new();
        existing.insert(("h0".to_string(), 10u64));
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &existing);
        assert!(action.is_none(),
            "task already in-flight → no duplicate");
    }

    /// AV-3-3 defensive — Draining / unhealthy nodes are
    /// filtered out of the candidate set.
    #[test]
    fn bug_av_anchor_lag_repair_skips_unhealthy_target() {
        let anchor = AnchorPolicy {
            datacenter: "leli".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        let placements = vec![placement(11, "toro", DiskTier::Warm)];
        let nodes = vec![
            // Only LELI cold node is unhealthy → no candidate.
            node(10, "leli", /* healthy */ false, &[DiskTier::Cold]),
            node(11, "toro", true, &[DiskTier::Warm]),
        ];
        let action =
            plan_anchor_lag_repair(&anchor, &placements, &nodes, "h0", &HashSet::new());
        assert!(action.is_none(),
            "no healthy LELI cold node → no repair");
    }

    /// 2026-05-27 lab-found bug: when the only anchor-DC node ALSO
    /// holds the chunk on a non-anchor tier (warm), the previous
    /// `holders` filter excluded that node entirely, so no cold
    /// replica was ever scheduled. The lab minicluster topology
    /// (data01 holds both warm + cold disks in dc-a) exposed this:
    /// after writes the cold tier stayed empty forever.
    #[test]
    fn anchor_lag_repair_allows_self_when_holding_only_non_required_tier() {
        let anchor = AnchorPolicy {
            datacenter: "dc-a".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        // data01 (node_id=1) holds the chunk on warm; it ALSO has a
        // cold disk and is the only node in dc-a. Anchor cold must
        // land here.
        let placements = vec![placement(1, "dc-a", DiskTier::Warm)];
        let nodes = vec![
            node(1, "dc-a", true, &[DiskTier::Warm, DiskTier::Cold]),
            node(2, "dc-b", true, &[DiskTier::Warm, DiskTier::Cold]),
        ];
        let action = plan_anchor_lag_repair(
            &anchor, &placements, &nodes, "h0", &HashSet::new(),
        )
        .expect("anchor-DC node with cold disk MUST be a valid target \
                 even when it already holds the chunk on a different tier");
        assert_eq!(action.target_node, 1);
        assert_eq!(action.target_tier, DiskTier::Cold);
    }

    /// Inverse: when the candidate-tier copy already exists on the
    /// node, don't double-schedule.
    #[test]
    fn anchor_lag_repair_skips_self_when_holding_required_tier() {
        let anchor = AnchorPolicy {
            datacenter: "dc-a".to_string(),
            required_tier: Some(DiskTier::Cold),
        };
        // data01 already holds a cold replica → anchor satisfied.
        let placements = vec![placement(1, "dc-a", DiskTier::Cold)];
        let nodes = vec![
            node(1, "dc-a", true, &[DiskTier::Warm, DiskTier::Cold]),
        ];
        assert!(
            plan_anchor_lag_repair(
                &anchor, &placements, &nodes, "h0", &HashSet::new(),
            )
            .is_none(),
            "anchor already satisfied → no repair"
        );
    }
}

#[cfg(test)]
mod bug_ae_orphan_tests {
    //! Bug AE — orphan migration tasks and Failed-task retention.
    //!
    //! Symptom (verified on Plexmedia 4h soak v1, 2026-05-19): 125 failed +
    //! 32 stuck-Copying migration tasks all carrying `"Chunk not found"`
    //! errors. The chunks referenced no longer existed in any
    //! `FileMetadata.chunks` list — they were leftovers from a data-node
    //! disk wipe but `migration_state_json` retained the tasks across the
    //! wipe.
    //!
    //! Root cause: the prune step at the end of `advance_tasks` retained
    //! `Failed` tasks forever (only `Done` was time-bounded), AND the
    //! `Copying` branch had no fast-path for "chunk no longer exists" —
    //! it ran the full 6-retry × 30s cycle, hammering data nodes with
    //! ReplicateChunk RPCs that all returned `Chunk not found`.
    //!
    //! Fix verified by:
    //! - `bug_ae_*` tests below for the pure helpers,
    //! - live-cluster observation that the 125 Failed tasks drain within
    //!   30 minutes of the new manager binary rolling out,
    //! - 4h soak v2 expected to maintain `failed_tasks < 5` throughout.

    use super::*;
    use dtfs_raft::state_machine::FileMetadata;
    use dtfs_raft::ChunkLocationEntry;
    use std::collections::BTreeMap;

    fn file_with_chunks(path: &str, chunk_hashes: &[&str]) -> FileMetadata {
        FileMetadata {
            path: path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 128 * 1024 * 1024,
            chunks: chunk_hashes
                .iter()
                .enumerate()
                .map(|(i, h)| ChunkLocationEntry {
                    chunk_index: i as u64,
                    chunk_hash: (*h).to_string(),
                    node_id: "10".to_string(),
                    disk_id: 0,
                    size_bytes: 128 * 1024 * 1024,
                })
                .collect(),
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        }
    }

    /// Pending / Copying / Verifying / Completing must NEVER be pruned —
    /// the state machine in `advance_tasks` owns their progression.
    #[test]
    fn non_terminal_tasks_are_always_retained() {
        let now = 10 * 60 * 60 * 1000; // 10h
        for status in [
            MigrationStatus::Pending,
            MigrationStatus::Copying,
            MigrationStatus::Verifying,
            MigrationStatus::Completing,
        ] {
            // Even one full hour old — keep it.
            assert!(
                should_retain_task(status, now - 60 * 60 * 1000, now),
                "non-terminal task ({status:?}) must be retained regardless of age",
            );
        }
    }

    /// `Done` task within the 5-minute window stays; older one is pruned.
    #[test]
    fn done_tasks_pruned_after_five_minutes() {
        let now = 60 * 60 * 1000; // 1h
        let just_completed = now - 30 * 1000; // 30s ago
        let stale = now - 6 * 60 * 1000;       // 6min ago — past cutoff

        assert!(should_retain_task(MigrationStatus::Done, just_completed, now));
        assert!(!should_retain_task(MigrationStatus::Done, stale, now));
    }

    /// `Failed` task within the 30-minute window stays so operators can
    /// inspect; older one is pruned to bound state size.
    #[test]
    fn failed_tasks_pruned_after_thirty_minutes() {
        let now = 60 * 60 * 1000; // 1h
        let recent_failure = now - 5 * 60 * 1000;  // 5min ago — keep
        let stale_failure  = now - 31 * 60 * 1000; // 31min ago — prune

        assert!(should_retain_task(MigrationStatus::Failed, recent_failure, now));
        assert!(!should_retain_task(MigrationStatus::Failed, stale_failure, now));
    }

    /// Sanity: `updated_at_ms > now_ms` (clock drift / startup race) must
    /// NOT cause underflow; the task should be retained.
    #[test]
    fn updated_after_now_does_not_underflow() {
        let now = 1000;
        let future = 5000;
        // saturating_sub returns 0, < retention windows, so retained.
        assert!(should_retain_task(MigrationStatus::Done, future, now));
        assert!(should_retain_task(MigrationStatus::Failed, future, now));
    }

    /// chunk_in_any_file returns true when a chunk hash matches any file.
    #[test]
    fn chunk_in_any_file_finds_existing_chunk() {
        let mut files: BTreeMap<String, FileMetadata> = BTreeMap::new();
        files.insert(
            "/movies/a.mkv".to_string(),
            file_with_chunks("/movies/a.mkv", &["hash_a0", "hash_a1"]),
        );
        files.insert(
            "/movies/b.mkv".to_string(),
            file_with_chunks("/movies/b.mkv", &["hash_b0"]),
        );

        assert!(chunk_in_any_file("hash_a0", &files));
        assert!(chunk_in_any_file("hash_a1", &files));
        assert!(chunk_in_any_file("hash_b0", &files));
    }

    /// Returns false when no file references the chunk — this is the
    /// orphan condition that fast-paths the task to Failed.
    #[test]
    fn chunk_in_any_file_returns_false_for_orphan() {
        let mut files: BTreeMap<String, FileMetadata> = BTreeMap::new();
        files.insert(
            "/movies/a.mkv".to_string(),
            file_with_chunks("/movies/a.mkv", &["hash_a0"]),
        );

        assert!(!chunk_in_any_file("hash_orphan", &files));
        assert!(!chunk_in_any_file("", &files));
    }

    /// Empty file map — every chunk lookup is orphan. Common at
    /// fresh-cluster boot before any file is created.
    #[test]
    fn chunk_in_any_file_handles_empty_map() {
        let files: BTreeMap<String, FileMetadata> = BTreeMap::new();
        assert!(!chunk_in_any_file("any_hash", &files));
    }

    /// File present but with no chunks (e.g. truncated to zero, or symlink
    /// placeholder). Chunk lookup must return false, not panic.
    #[test]
    fn chunk_in_any_file_skips_chunkless_files() {
        let mut files: BTreeMap<String, FileMetadata> = BTreeMap::new();
        files.insert(
            "/empty.bin".to_string(),
            file_with_chunks("/empty.bin", &[]),
        );
        assert!(!chunk_in_any_file("anything", &files));
    }

    /// Bug AF — the exact error string produced when a source data node
    /// reports "Chunk not found" must be classified as fast-fail. This is
    /// the live shape observed on Plexmedia 4h soak v2 at 07:30 UTC.
    #[test]
    fn bug_af_chunk_not_found_classified_for_fast_fail() {
        let live = "ReplicateChunk error: ReplicateChunk returned error: \
                    Chunk not found: 6bea15d90aa05f7f445d5136c49618c7aa720ca13bf0f902d8518732ca2a4e46";
        assert!(rpc_error_indicates_chunk_missing(live));

        // Bare form (defensive — if the inner wrapping ever changes).
        assert!(rpc_error_indicates_chunk_missing("Chunk not found"));
    }

    /// Generic transport / network / target-resolution failures must NOT
    /// trigger the fast-fail path — those are transient and should be
    /// retried up to MAX_MIGRATION_ATTEMPTS as normal.
    #[test]
    fn bug_af_transient_errors_not_classified_for_fast_fail() {
        assert!(!rpc_error_indicates_chunk_missing("connect to source 10.0.0.1: timeout"));
        assert!(!rpc_error_indicates_chunk_missing("ReplicateChunk RPC: tonic transport error"));
        assert!(!rpc_error_indicates_chunk_missing("target node 999 unresolvable"));
        assert!(!rpc_error_indicates_chunk_missing("no healthy source replica"));
        assert!(!rpc_error_indicates_chunk_missing(""));
    }

    /// Regression-shape test against the live-cluster scenario: a Failed
    /// task that fired 31 min ago AND references a chunk that's no longer
    /// in any file is pruned (because Failed + stale).  A separate
    /// Copying-state task with the same orphan chunk is retained by the
    /// retain block (the advance_tasks branch is responsible for fast-
    /// pathing it to Failed on the next tick).
    #[test]
    fn live_scenario_125_failed_orphans_drain() {
        let now = 60 * 60 * 1000; // 1h
        let stale_failed = now - 31 * 60 * 1000;
        let stuck_copying = now - 31 * 60 * 1000;

        // Failed + stale → pruned.
        assert!(!should_retain_task(MigrationStatus::Failed, stale_failed, now));
        // Copying + stale → retained; advance_tasks owns the fast-path.
        assert!(should_retain_task(MigrationStatus::Copying, stuck_copying, now));
    }
}

/// Bug AW — split out the migration-snapshot sort comparator so we can
/// unit-test it without spinning up a scheduler. Sorts in place by
/// `created_at_ms` DESC (newest first, immutable after insertion)
/// then by natural-numeric `task_id` ASC ("repl-2" before "repl-10").
fn sort_migration_snapshot(out: &mut Vec<serde_json::Value>) {
    out.sort_by(|a, b| {
        let bts = b
            .get("created_at_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let ats = a
            .get("created_at_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        bts.cmp(&ats).then_with(|| {
            let aid = a.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            let bid = b.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            natural_task_id_cmp(aid, bid)
        })
    });
}

/// Compare two task IDs like "repl-<N>" by their trailing integer when
/// both parse, otherwise fall back to lexicographic. Keeps a burst of
/// 10+ tasks sorted as repl-1, repl-2, … repl-10 instead of
/// repl-1, repl-10, repl-2, … (the prior lex compare).
fn natural_task_id_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (trailing_u64(a), trailing_u64(b)) {
        (Some(an), Some(bn)) => an.cmp(&bn).then_with(|| a.cmp(b)),
        _ => a.cmp(b),
    }
}

fn trailing_u64(s: &str) -> Option<u64> {
    let digits: String = s.chars().rev().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.chars().rev().collect::<String>().parse::<u64>().ok()
}

#[cfg(test)]
mod bug_aw_migration_sort_tests {
    //! Bug AW — Migrations page randomised row order on each 5s refresh.
    //!
    //! Symptom: operators watching the Migrations panel during a tier
    //! storm saw rows leap around between refreshes, making the page
    //! unreadable.
    //!
    //! Root cause: the snapshot sort keyed on `updated_at_ms`, which is
    //! bumped on every scheduler tick for any task in flight. Two ticks
    //! in different orders → two different sort orders.
    //!
    //! Fix: sort on `created_at_ms` (set once at insertion) DESC,
    //! tiebreak with a natural-numeric `task_id` compare.
    use super::*;
    use serde_json::json;

    fn t(task_id: &str, created: u64, updated: u64) -> serde_json::Value {
        json!({
            "task_id": task_id,
            "created_at_ms": created,
            "updated_at_ms": updated,
        })
    }

    fn ids(v: &[serde_json::Value]) -> Vec<&str> {
        v.iter()
            .map(|x| x.get("task_id").and_then(|s| s.as_str()).unwrap_or(""))
            .collect()
    }

    /// Two scheduler ticks bump `updated_at_ms` in different orders;
    /// the pre-fix sort would flip the rows. With the fix, ordering
    /// is anchored to `created_at_ms` and is stable.
    #[test]
    fn bug_aw_running_tasks_dont_reshuffle() {
        // Tick 1: repl-1 was updated last.
        let mut tick1 = vec![
            t("repl-1", 1_000, 5_000),
            t("repl-2", 2_000, 4_000),
            t("repl-3", 3_000, 3_000),
        ];
        sort_migration_snapshot(&mut tick1);

        // Tick 2: same three tasks, scheduler touched repl-2 last
        // (different updated_at_ms ordering), creation order unchanged.
        let mut tick2 = vec![
            t("repl-1", 1_000, 6_000),
            t("repl-2", 2_000, 9_000),
            t("repl-3", 3_000, 7_000),
        ];
        sort_migration_snapshot(&mut tick2);

        // Bug AW: order is identical between ticks regardless of how
        // updated_at_ms shifted.
        assert_eq!(ids(&tick1), ids(&tick2));
        // And newest-created is on top.
        assert_eq!(ids(&tick1), vec!["repl-3", "repl-2", "repl-1"]);
    }

    /// Natural numeric tiebreak: a burst of >=10 tasks created in the
    /// same millisecond must render as repl-1..repl-10 in numeric order.
    #[test]
    fn bug_aw_natural_numeric_tiebreak() {
        let mut burst: Vec<_> = (1..=12u64)
            .map(|n| t(&format!("repl-{n}"), 5_000, 5_000))
            .collect();
        // Shuffle insertion order to prove sort doesn't depend on it.
        burst.swap(0, 11);
        burst.swap(2, 7);

        sort_migration_snapshot(&mut burst);

        let want: Vec<String> = (1..=12u64).map(|n| format!("repl-{n}")).collect();
        let got: Vec<String> = ids(&burst).into_iter().map(String::from).collect();
        assert_eq!(got, want);
    }

    /// Pre-Bug-AW lex sort would order ["repl-1","repl-10","repl-2"];
    /// the new natural compare must order ["repl-1","repl-2","repl-10"].
    #[test]
    fn bug_aw_natural_compare_helper() {
        use std::cmp::Ordering::*;
        assert_eq!(natural_task_id_cmp("repl-2", "repl-10"), Less);
        assert_eq!(natural_task_id_cmp("repl-10", "repl-2"), Greater);
        assert_eq!(natural_task_id_cmp("repl-7", "repl-7"), Equal);
    }

    /// Non-numeric IDs fall back to lex compare without panicking.
    #[test]
    fn bug_aw_non_numeric_ids_fallback() {
        use std::cmp::Ordering::*;
        assert_eq!(natural_task_id_cmp("alpha", "beta"), Less);
        assert_eq!(natural_task_id_cmp("repl-foo", "repl-bar"), Greater);
    }

    /// Tasks missing created_at_ms (older snapshots) sort to the bottom
    /// instead of randomising the head of the list.
    #[test]
    fn bug_aw_missing_created_at_sinks() {
        let mut v = vec![
            json!({"task_id": "repl-1"}), // missing created_at_ms
            t("repl-2", 1_000, 1_000),
            t("repl-3", 2_000, 2_000),
        ];
        sort_migration_snapshot(&mut v);
        assert_eq!(ids(&v), vec!["repl-3", "repl-2", "repl-1"]);
    }
}

#[cfg(test)]
mod bug_bo_tests {
    use super::*;

    /// BO-1 — a same-node cross-tier anchor copy must NOT exclude the
    /// target from the source search (the target is the only holder of the
    /// chunk, on its other tier). Cross-node copies still exclude the
    /// target.
    #[test]
    fn bo_1_same_node_source_not_excluded() {
        assert_eq!(anchor_copy_source_exclude(1, 1), None,
            "same-node warm->cold copy: target must not be excluded");
        assert_eq!(anchor_copy_source_exclude(1, 2), Some(2),
            "cross-node copy: target excluded as before");
    }

    /// BO-2 — a Failed task must keep its chunk in the re-queue-blocking
    /// set (so check_anchor_lag does not re-schedule it every tick during
    /// the 30-min retention window); only Done tasks free the chunk.
    #[test]
    fn bo_2_failed_blocks_requeue_done_does_not() {
        assert!(task_blocks_requeue(MigrationStatus::Failed),
            "Failed task must block immediate re-queue (was the tight-loop bug)");
        assert!(!task_blocks_requeue(MigrationStatus::Done),
            "Done task frees the chunk for a future schedule");
        // In-flight states also block (unchanged).
        assert!(task_blocks_requeue(MigrationStatus::Pending));
        assert!(task_blocks_requeue(MigrationStatus::Copying));
        assert!(task_blocks_requeue(MigrationStatus::Verifying));
    }

    /// BP-4 — a chunk referenced by ANY file (hash in the in-state set) is
    /// never an orphan, even if its on-disk location looks stale; only a
    /// hash with no file referent is reapable.
    #[test]
    fn bp_4_chunk_is_orphan_only_when_unreferenced() {
        let mut in_state = std::collections::HashSet::new();
        in_state.insert("referenced".to_string());
        assert!(!chunk_is_orphan(&in_state, "referenced"), "referenced chunk is NOT orphan");
        assert!(chunk_is_orphan(&in_state, "dangling"), "unreferenced chunk IS orphan");
        // Empty in-state set: everything is orphan (no files reference anything).
        let empty = std::collections::HashSet::new();
        assert!(chunk_is_orphan(&empty, "anything"));
    }

    /// Bug CB — the reap decision is a pure function of the chunk's
    /// durable mtime, so it survives manager restarts (no first-observed
    /// clock to reset). Covers: referenced never reaps, recent orphan is
    /// protected, aged orphan reaps, unknown mtime defers.
    #[test]
    fn cb_orphan_reap_due_uses_durable_mtime() {
        // Fixed clock so the test never touches Date::now (forbidden in
        // workflow scripts and avoided in pure tests for determinism).
        let now_ms: u64 = 1_700_000_000_000;
        let grace_secs: u64 = 86_400; // 24h, the stock default

        // Referenced chunk (not orphan) is NEVER reaped, even if ancient.
        assert!(
            !orphan_reap_due(false, now_ms - 10 * grace_secs * 1000, now_ms, grace_secs),
            "a referenced chunk must never be reaped"
        );

        // Orphan but freshly written (1 min old) — within grace, protected
        // against the in-flight put/promote race.
        assert!(
            !orphan_reap_due(true, now_ms - 60_000, now_ms, grace_secs),
            "a recently-written orphan must be protected by grace"
        );

        // Orphan whose .dat is older than grace — reap.
        assert!(
            orphan_reap_due(true, now_ms - (grace_secs + 10) * 1000, now_ms, grace_secs),
            "an aged orphan past grace must be reaped"
        );

        // Exactly at the grace boundary — reap (>=).
        assert!(
            orphan_reap_due(true, now_ms - grace_secs * 1000, now_ms, grace_secs),
            "grace boundary is inclusive"
        );

        // Unknown mtime (0) — a pre-Bug-CB data node or a stat failure.
        // Defer rather than reap with no grace, even though the chunk is
        // an orphan and `now - 0` is astronomically larger than grace.
        assert!(
            !orphan_reap_due(true, 0, now_ms, grace_secs),
            "unknown mtime must defer the reap, not fire it"
        );
    }

    /// Bug CB — regression for the actual symptom: the decision carries NO
    /// state across calls, so a manager that "restarts" (re-evaluates from
    /// scratch) reaches the same verdict. The pre-CB in-memory clock reset
    /// to `now` on every restart, so an aged orphan re-started its 24h
    /// countdown forever and disk was never reclaimed.
    #[test]
    fn cb_orphan_reap_survives_simulated_restart() {
        let grace_secs: u64 = 86_400;
        let mtime_ms: u64 = 1_700_000_000_000; // written once, long ago
        // First "manager process": observes the orphan well past grace.
        let now_1 = mtime_ms + (grace_secs + 1) * 1000;
        assert!(orphan_reap_due(true, mtime_ms, now_1, grace_secs));
        // Manager restarts (any in-memory tracker is gone). A later tick
        // on the fresh process still sees the same durable mtime and still
        // reaps — it does NOT restart the grace countdown.
        let now_2 = now_1 + 5_000;
        assert!(
            orphan_reap_due(true, mtime_ms, now_2, grace_secs),
            "reap verdict must not reset across a manager restart"
        );
    }
}
