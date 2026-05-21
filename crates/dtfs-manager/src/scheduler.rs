//! Background task scheduler — Phase 4 (Resilience).
//!
//! Drives periodic and on-demand background tasks such as re-replication of
//! under-replicated chunks, tier migration, and chunk redistribution across
//! the cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dtfs_common::types::{DiskTier, HealthStatus, MigrationStatus, TierReplication};
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
        }
    }

    /// Inject the alert dispatcher (called after main.rs has wired one).
    pub fn set_alerts(&self, alerts: Arc<crate::alerts::AlertDispatcher>) {
        *self.alerts.write() = Some(alerts);
    }

    /// Run the scheduler background loop. Intended to be spawned as a tokio task.
    pub async fn run(&self) {
        info!("Task scheduler started (tick interval: {:?})", SCHEDULER_TICK);

        let mut interval = tokio::time::interval(SCHEDULER_TICK);
        loop {
            interval.tick().await;

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

    /// Check the cluster state for under-replicated chunks and schedule
    /// re-replication tasks for them.
    async fn check_under_replicated(&self, migration_state: &mut MigrationState) {
        let sm = self.state_data.read().await;

        // Collect healthy nodes and their tiers.
        let healthy_nodes: Vec<(u64, Vec<DiskTier>)> = sm
            .nodes
            .iter()
            .filter(|(_, info)| {
                info.health == HealthStatus::Healthy
            })
            .map(|(id, info)| {
                let tiers: Vec<DiskTier> = info.disks.iter().map(|d| d.tier).collect();
                (*id, tiers)
            })
            .collect();

        // Walk all files and check chunk replica counts.
        for (_path, file_meta) in &sm.files {
            for chunk_loc in &file_meta.chunks {
                // Each chunk_loc entry represents one replica. Count how many
                // replicas exist for the same chunk_index.
                let replica_count = file_meta
                    .chunks
                    .iter()
                    .filter(|c| c.chunk_index == chunk_loc.chunk_index)
                    .count() as u32;

                // Look up the per-volume replication factor, falling back to
                // the cluster-wide default of 3 for the hot tier.
                let chunk_tier = {
                    // Try to determine the actual tier from the source node's
                    // disk matching the stored disk_id.
                    let source_node_id_for_tier: u64 =
                        chunk_loc.node_id.parse().unwrap_or(0);
                    sm.nodes
                        .get(&source_node_id_for_tier)
                        .and_then(|n| n.disks.get(chunk_loc.disk_id as usize))
                        .map(|d| d.tier)
                        .unwrap_or(DiskTier::Hot)
                };
                let desired_replicas = sm.volumes
                    .get(&file_meta.volume_id)
                    .map(|v| v.replication.for_tier(chunk_tier))
                    .unwrap_or(3u32);

                if replica_count < desired_replicas {
                    // Pick a HEALTHY source from the chunk's current replicas
                    // rather than blindly using `chunk_loc.node_id` (which may
                    // be the Down node that caused the under-replication in
                    // the first place). The Copying-state retry logic will
                    // re-resolve too (F-015), but scheduling with a healthy
                    // source up-front avoids a wasted retry cycle (F-016).
                    let existing_node_ids: Vec<u64> = file_meta
                        .chunks
                        .iter()
                        .filter(|c| c.chunk_index == chunk_loc.chunk_index)
                        .filter_map(|c| c.node_id.parse().ok())
                        .collect();

                    let source_node_id: u64 = existing_node_ids
                        .iter()
                        .copied()
                        .find(|nid| {
                            sm.nodes
                                .get(nid)
                                .map(|n| n.health == HealthStatus::Healthy)
                                .unwrap_or(false)
                        })
                        .unwrap_or_else(|| chunk_loc.node_id.parse().unwrap_or(0));

                    if source_node_id == 0 {
                        continue;
                    }

                    let existing_nodes: Vec<u64> = existing_node_ids;

                    let tier = chunk_tier;

                    if let Some((target, _)) = healthy_nodes
                        .iter()
                        .find(|(id, tiers)| !existing_nodes.contains(id) && tiers.contains(&tier))
                    {
                        // Check if already scheduled.
                        let already = migration_state.tasks.values().any(|t| {
                            t.chunk_id == chunk_loc.chunk_hash
                                && t.target_node == *target
                                && t.status != MigrationStatus::Done
                                && t.status != MigrationStatus::Failed
                        });

                        if !already {
                            let now_ms = now_millis();
                            let task_id =
                                format!("repl-{}", migration_state.next_task_id);
                            migration_state.next_task_id += 1;

                            let task = ReplicationTask {
                                task_id: task_id.clone(),
                                chunk_id: chunk_loc.chunk_hash.clone(),
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
                                chunk = %chunk_loc.chunk_hash,
                                source = source_node_id,
                                target = *target,
                                "Auto-scheduled re-replication (replica count {} < {})",
                                replica_count,
                                desired_replicas
                            );

                            migration_state.tasks.insert(task_id, task);
                        }
                    }
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
                    let resolved_source = self
                        .resolve_healthy_source(&task.chunk_id, task.target_node)
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
                            match self.replicate_chunk(&src_addr, &chunk_id, &tgt_addr).await {
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
                            let target_str = target_node.to_string();
                            let already_present = entries
                                .iter()
                                .any(|c| c.node_id == target_str);
                            if !already_present {
                                entries.push(ChunkLocationEntry {
                                    chunk_index,
                                    chunk_hash: chunk_id.clone(),
                                    node_id: target_str,
                                    disk_id: 0,
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
    async fn node_grpc_addr(&self, node_id: u64) -> Option<String> {
        let sm = self.state_data.read().await;
        sm.nodes.get(&node_id).map(|info| info.grpc_addr.clone())
    }

    /// Pick any healthy node that currently has a copy of `chunk_id`,
    /// excluding `exclude` (the migration target). Returns `(node_id,
    /// grpc_addr)` for the chosen source, or `None` if no healthy replica
    /// exists. (Helper for F-015.)
    async fn resolve_healthy_source(
        &self,
        chunk_id: &str,
        exclude: u64,
    ) -> Option<(u64, String)> {
        let sm = self.state_data.read().await;
        let mut candidates: Vec<u64> = Vec::new();
        for file in sm.files.values() {
            for c in &file.chunks {
                if c.chunk_hash == chunk_id {
                    if let Ok(nid) = c.node_id.parse::<u64>() {
                        if nid != exclude && !candidates.contains(&nid) {
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

    /// Ask the source node to replicate `chunk_id` to `target_addr`.
    async fn replicate_chunk(
        &self,
        source_addr: &str,
        chunk_id: &str,
        target_addr: &str,
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
    async fn delete_chunk_replica_on_node(
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
        // Bug AM — most recently updated first, then by task_id ascending
        // for stable ordering on refresh. The previous sort used only
        // updated_at_ms which is shared by every task touched in the same
        // 30s scheduler tick; HashMap iteration order then randomised the
        // display each refresh and made the page unreadable when a burst
        // of migrations all completed simultaneously.
        out.sort_by(|a, b| {
            let bts = b.get("updated_at_ms").and_then(|v| v.as_u64()).unwrap_or(0);
            let ats = a.get("updated_at_ms").and_then(|v| v.as_u64()).unwrap_or(0);
            bts.cmp(&ats).then_with(|| {
                // task_id is "repl-<N>" — natural ordering on the trailing
                // integer would be nicer but lexicographic is consistent
                // and stable, which is all we need.
                let aid = a.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
                let bid = b.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
                aid.cmp(bid)
            })
        });
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

        if actions.is_empty() {
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
