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

                // Default replication factor is 3 for hot tier.
                let desired_replicas = 3u32;

                if replica_count < desired_replicas {
                    // Find the node this chunk is on.
                    let source_node_id: u64 =
                        chunk_loc.node_id.parse().unwrap_or(0);

                    // Skip if source is down.
                    if source_node_id == 0 {
                        continue;
                    }

                    // Find a healthy target node that does NOT already host
                    // this chunk.
                    let existing_nodes: Vec<u64> = file_meta
                        .chunks
                        .iter()
                        .filter(|c| c.chunk_index == chunk_loc.chunk_index)
                        .filter_map(|c| c.node_id.parse().ok())
                        .collect();

                    let tier = DiskTier::Hot; // Simplified: assume hot tier.

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
                    // Transition to Copying.
                    info!(
                        task_id = %task.task_id,
                        chunk = %task.chunk_id,
                        "Migration: Pending -> Copying"
                    );
                    task.status = MigrationStatus::Copying;
                    task.updated_at_ms = now_ms;
                }

                MigrationStatus::Copying => {
                    // Issue ReplicateChunk RPC on the source node.
                    // The source node streams the chunk to the target node.
                    let source_addr = self.node_grpc_addr(task.source_node).await;
                    let target_addr = self.node_grpc_addr(task.target_node).await;

                    match (source_addr, target_addr) {
                        (Some(src_addr), Some(tgt_addr)) => {
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
                                    task.updated_at_ms = now_ms;
                                }
                                Err(e) => {
                                    warn!(
                                        task_id = %task.task_id,
                                        chunk = %chunk_id,
                                        "ReplicateChunk failed, will retry: {}",
                                        e
                                    );
                                    // Stay in Copying; will retry next tick.
                                    task.updated_at_ms = now_ms;
                                }
                            }
                        }
                        _ => {
                            warn!(
                                task_id = %task.task_id,
                                chunk = %task.chunk_id,
                                src_node = task.source_node,
                                dst_node = task.target_node,
                                "Cannot resolve node addresses for copy; failing task"
                            );
                            task.status = MigrationStatus::Failed;
                            task.updated_at_ms = now_ms;
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
                        let new_loc = ChunkLocationEntry {
                            chunk_index: 0,   // Will be corrected by merging with existing.
                            chunk_hash: chunk_id.clone(),
                            node_id: target_node.to_string(),
                            disk_id: 0,       // Disk chosen by target data node.
                            size_bytes: 0,    // Size already stored on target.
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

    /// Load the migration state from the Raft cluster config JSON.
    async fn load_migration_state(&self) -> MigrationState {
        let sm = self.state_data.read().await;
        if sm.cluster_config.config_json.is_empty() {
            return MigrationState::default();
        }

        // The cluster_config.config_json may contain a MigrationState or some
        // other config. We try to parse it; on failure we use the default.
        serde_json::from_str(&sm.cluster_config.config_json).unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // Data-node RPC helpers
    // -----------------------------------------------------------------------

    /// Look up the gRPC address of a node from the Raft state machine.
    async fn node_grpc_addr(&self, node_id: u64) -> Option<String> {
        let sm = self.state_data.read().await;
        sm.nodes.get(&node_id).map(|info| info.grpc_addr.clone())
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

    /// Persist the migration state to Raft via UpdateConfig.
    async fn save_migration_state(&self, state: &MigrationState) {
        let config_json = match serde_json::to_string(state) {
            Ok(json) => json,
            Err(e) => {
                error!("Failed to serialize migration state: {}", e);
                return;
            }
        };

        let version = {
            let sm = self.state_data.read().await;
            sm.cluster_config.version + 1
        };

        let req = ClientRequest::UpdateConfig {
            config_json,
            version,
        };

        if let Err(e) = self.raft.client_write(req).await {
            // Not being leader is expected on followers; only warn.
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
