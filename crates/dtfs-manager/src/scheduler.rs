//! Background task scheduler — Phase 4 (Resilience).
//!
//! Drives periodic and on-demand background tasks such as re-replication of
//! under-replicated chunks, tier migration, and chunk redistribution across
//! the cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dtfs_common::types::{DiskTier, HealthStatus, MigrationStatus};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ChunkLocationEntry, ClientRequest, DtfsRaft};
use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::{ReplicateChunkRequest, VerifyChunkRequest};
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
        }
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
    async fn tick(&self) -> anyhow::Result<()> {
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
                        let chunk_index = {
                            let sm = self.state_data.read().await;
                            sm.files
                                .get(&path)
                                .and_then(|f| {
                                    f.chunks
                                        .iter()
                                        .find(|c| c.chunk_hash == chunk_id)
                                        .map(|c| c.chunk_index)
                                })
                                .unwrap_or(0)
                        };
                        let new_loc = ChunkLocationEntry {
                            chunk_index,
                            chunk_hash: chunk_id.clone(),
                            // Store the numeric node ID (not the gRPC address)
                            // so that `chunk_loc.node_id.parse::<u64>()` succeeds
                            // in the scheduler's under-replication checks (fix #7).
                            node_id: target_node.to_string(),
                            disk_id: 0, // Disk chosen by target data node.
                            size_bytes: 0,
                        };
                        let req = ClientRequest::UpdateChunkLocations {
                            path,
                            chunks: vec![new_loc],
                        };
                        if let Err(e) = self.raft.client_write(req).await {
                            debug!(
                                "Could not write new chunk location to Raft (may not be leader): {}",
                                e
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
        let cutoff = now_ms.saturating_sub(5 * 60 * 1000);
        state.tasks.retain(|_, t| {
            !(t.status == MigrationStatus::Done && t.updated_at_ms < cutoff)
        });
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
        let endpoint = format!("http://{}", source_addr);
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
        let endpoint = format!("http://{}", target_addr);
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
            // Not being leader is expected on followers.
            debug!("Failed to persist migration state: {} (may not be leader)", e);
        }
    }
}

/// Get current time in milliseconds since UNIX epoch.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
