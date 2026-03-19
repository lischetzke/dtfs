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
use dtfs_raft::{ClientRequest, DtfsRaft};
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
                    // In a real implementation we would issue an RPC to copy
                    // the chunk from source to target. For now, we advance to
                    // Verifying on the next tick.
                    info!(
                        task_id = %task.task_id,
                        chunk = %task.chunk_id,
                        "Migration: Copying -> Verifying"
                    );
                    task.status = MigrationStatus::Verifying;
                    task.updated_at_ms = now_ms;
                }

                MigrationStatus::Verifying => {
                    // In a real implementation we would verify the checksum on
                    // the target node. Source data is NOT removed yet.
                    info!(
                        task_id = %task.task_id,
                        chunk = %task.chunk_id,
                        "Migration: Verifying -> Completing (source preserved)"
                    );
                    task.status = MigrationStatus::Completing;
                    task.updated_at_ms = now_ms;
                }

                MigrationStatus::Completing => {
                    // Target verified. Mark as done. Source data is still kept
                    // (the source is a valid replica); the old replica can be
                    // removed later if over-replicated.
                    info!(
                        task_id = %task.task_id,
                        chunk = %task.chunk_id,
                        "Migration: Completing -> Done"
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
