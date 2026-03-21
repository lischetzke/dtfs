//! Raft state machine — the cluster-wide FSM that holds all metadata.
//!
//! State is kept entirely in memory. Snapshots are serialized to JSON via
//! [`crate::snapshot`] and stored as `Cursor<Vec<u8>>` blobs.

use std::collections::BTreeMap;
use std::io::{self, Cursor};
use std::sync::Arc;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::EntryPayload;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use dtfs_common::types::VolumeConfig;

use crate::snapshot::{self, StateMachineSnapshot};
use crate::{
    merge_chunk_location_entries, ChunkLocationEntry, ClientRequest, ClientResponse, TypeConfig,
};

// Re-export type aliases for convenience.
type LogId = crate::LogId;
type StoredMembership = crate::StoredMembership;

// ---------------------------------------------------------------------------
// Audit log cap — prevents unbounded memory growth
// ---------------------------------------------------------------------------

/// Maximum number of audit entries retained in the state machine.
/// Oldest entries are evicted once this limit is exceeded.
const MAX_AUDIT_LOG_ENTRIES: usize = 10_000;

// ---------------------------------------------------------------------------
// Domain types stored inside the state machine
// ---------------------------------------------------------------------------

/// A completed scrub operation result, stored per data-node in the state
/// machine so the WebUI and `/api/v1/stats` can surface health per node.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScrubRecord {
    pub node_id: u64,
    pub disk_path: String,
    /// `"quick"` or `"deep"`
    pub level: String,
    /// Unix timestamp in milliseconds when the scrub finished.
    pub completed_at_ms: u64,
    pub chunks_checked: u64,
    pub chunks_healthy: u64,
    pub chunks_corrupt: u64,
    pub chunks_missing: u64,
    pub duration_secs: f64,
}

/// Metadata for a single file managed by the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileMetadata {
    pub path: String,
    pub volume_id: String,
    pub chunk_size: u64,
    pub chunks: Vec<ChunkLocationEntry>,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
}

/// Cluster-wide configuration (versioned).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ClusterConfig {
    pub config_json: String,
    pub version: u64,
}

/// A single entry in the audit log.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditEntry {
    pub log_index: u64,
    pub summary: String,
    pub timestamp_ms: u64,
}

// ---------------------------------------------------------------------------
// StateMachineData — the actual in-memory state
// ---------------------------------------------------------------------------

/// All state maintained by the cluster FSM.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId>,
    pub last_membership: StoredMembership,

    // ---- domain state ----
    pub files: BTreeMap<String, FileMetadata>,
    pub nodes: BTreeMap<u64, crate::NodeInfo>,
    pub cluster_config: ClusterConfig,
    pub audit_log: Vec<AuditEntry>,

    // ---- observability state ----
    /// Per-node scrub results, keyed by `node_id`.
    ///
    /// Storing one record per node ensures that a fast scrub on node B does
    /// not silently discard the result from node A (fix for issue #8 —
    /// previously only the cluster-wide "most recent" single result was kept).
    #[serde(default)]
    pub scrub_results: BTreeMap<u64, ScrubRecord>,

    /// Number of registered FUSE clients (updated by client heartbeats).
    pub client_count: u64,

    /// Volume configurations, keyed by volume_id string.
    #[serde(default)]
    pub volumes: BTreeMap<String, VolumeConfig>,

    /// Migration-scheduler state, serialised as JSON.
    ///
    /// Kept in its own field instead of `cluster_config.config_json` so that
    /// the scheduler's internal bookkeeping never clobbers user-visible
    /// cluster configuration (fix for issue #3).
    #[serde(default)]
    pub migration_state_json: String,
}

impl Default for StateMachineData {
    fn default() -> Self {
        Self {
            last_applied_log: None,
            last_membership: Default::default(),
            files: BTreeMap::new(),
            nodes: BTreeMap::new(),
            cluster_config: ClusterConfig::default(),
            audit_log: Vec::new(),
            scrub_results: BTreeMap::new(),
            client_count: 0,
            volumes: BTreeMap::new(),
            migration_state_json: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// StateMachineStore — wraps the data behind an Arc<RwLock>
// ---------------------------------------------------------------------------

/// The `RaftStateMachine` implementation for DTFS.
#[derive(Debug)]
pub struct StateMachineStore {
    /// The mutable state machine data.
    pub data: Arc<RwLock<StateMachineData>>,
    /// The last snapshot that was built/installed.
    snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
}

/// A snapshot that has been persisted (kept in memory for simplicity).
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: crate::SnapshotMeta,
    data: Vec<u8>,
}

impl StateMachineStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(StateMachineData::default())),
            snapshot: Arc::new(RwLock::new(None)),
        }
    }

    /// Read-only access to the current state (useful for queries).
    pub fn data(&self) -> &Arc<RwLock<StateMachineData>> {
        &self.data
    }
}

impl Default for StateMachineStore {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// SnapshotBuilder
// ---------------------------------------------------------------------------

/// Builds a snapshot from a frozen copy of the state machine data.
pub struct DtfsSnapshotBuilder {
    data: StateMachineData,
}

impl RaftSnapshotBuilder<TypeConfig> for DtfsSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<crate::Snapshot, io::Error> {
        let sm = &self.data;

        // Serialise the snapshot.
        let snap = StateMachineSnapshot {
            last_applied_log: sm.last_applied_log.as_ref().map(|lid| {
                (lid.leader_id.term, lid.index)
            }),
            last_membership: serde_json::to_value(&sm.last_membership).ok(),
            files: sm.files.clone(),
            nodes: sm.nodes.clone(),
            cluster_config: sm.cluster_config.clone(),
            audit_log: sm.audit_log.clone(),
            scrub_results: sm.scrub_results.clone(),
            client_count: sm.client_count,
            volumes: sm.volumes.clone(),
            migration_state_json: sm.migration_state_json.clone(),
        };

        let cursor = snapshot::serialize_snapshot(&snap)?;
        let data_bytes = cursor.into_inner();

        let snapshot_id = if let Some(ref lid) = sm.last_applied_log {
            format!("{}-{}", lid.leader_id.term, lid.index)
        } else {
            "0-0".to_string()
        };

        let meta = openraft::storage::SnapshotMeta {
            last_log_id: sm.last_applied_log.clone(),
            last_membership: sm.last_membership.clone(),
            snapshot_id,
        };

        Ok(openraft::storage::Snapshot {
            meta,
            snapshot: Cursor::new(data_bytes),
        })
    }
}

// ---------------------------------------------------------------------------
// RaftStateMachine implementation
// ---------------------------------------------------------------------------

impl RaftStateMachine<TypeConfig> for StateMachineStore {
    type SnapshotBuilder = DtfsSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId>, StoredMembership), io::Error> {
        let sm = self.data.read().await;
        Ok((sm.last_applied_log.clone(), sm.last_membership.clone()))
    }

    async fn apply<S>(&mut self, mut entries: S) -> Result<(), io::Error>
    where
        S: futures::Stream<Item = Result<openraft::storage::EntryResponder<TypeConfig>, io::Error>>
            + Unpin
            + Send,
    {
        while let Some(item) = entries.next().await {
            let (entry, responder) = item?;

            let log_id = entry.log_id.clone();
            debug!("apply: log_id={}", log_id);

            let response = {
                let mut sm = self.data.write().await;
                sm.last_applied_log = Some(log_id.clone());

                match &entry.payload {
                    EntryPayload::Blank => ClientResponse::Success,
                    EntryPayload::Membership(mem) => {
                        sm.last_membership =
                            openraft::StoredMembership::new(Some(log_id.clone()), mem.clone());
                        ClientResponse::Success
                    }
                    EntryPayload::Normal(req) => apply_request(&mut sm, &log_id, req),
                }
            };
            // sm lock is dropped here

            // If there is a client waiting (leader), send the response.
            if let Some(resp) = responder {
                resp.send(response);
            }
        }

        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let data = self.data.read().await.clone();
        DtfsSnapshotBuilder { data }
    }

    async fn begin_receiving_snapshot(&mut self) -> Result<Cursor<Vec<u8>>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &crate::SnapshotMeta,
        snapshot_cursor: Cursor<Vec<u8>>,
    ) -> Result<(), io::Error> {
        let raw = snapshot::bytes_from_cursor(snapshot_cursor)?;
        let snap_data: StateMachineSnapshot = serde_json::from_slice(&raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        // Rebuild the state machine from the snapshot.
        {
            let mut sm = self.data.write().await;
            sm.last_applied_log = meta.last_log_id.clone();
            sm.last_membership = meta.last_membership.clone();
            sm.files = snap_data.files;
            sm.nodes = snap_data.nodes;
            sm.cluster_config = snap_data.cluster_config;
            sm.audit_log = snap_data.audit_log;
            sm.scrub_results = snap_data.scrub_results;
            sm.client_count = snap_data.client_count;
            sm.volumes = snap_data.volumes;
            sm.migration_state_json = snap_data.migration_state_json;
        }

        // Store the snapshot blob for later retrieval.
        {
            let mut stored = self.snapshot.write().await;
            *stored = Some(StoredSnapshot {
                meta: meta.clone(),
                data: raw,
            });
        }

        info!(
            "installed snapshot: id={}, last_log_id={:?}",
            meta.snapshot_id, meta.last_log_id
        );

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<crate::Snapshot>, io::Error> {
        let stored = self.snapshot.read().await;
        Ok(stored.as_ref().map(|s| openraft::storage::Snapshot {
            meta: s.meta.clone(),
            snapshot: Cursor::new(s.data.clone()),
        }))
    }
}

// ---------------------------------------------------------------------------
// Business logic for applying a single ClientRequest
// ---------------------------------------------------------------------------

/// Push an audit entry and cap the log at `MAX_AUDIT_LOG_ENTRIES`.
///
/// Oldest entries are drained from the front when the cap is exceeded,
/// preventing unbounded memory growth (fix for issue #5).
fn push_audit_capped(log: &mut Vec<AuditEntry>, entry: AuditEntry) {
    log.push(entry);
    if log.len() > MAX_AUDIT_LOG_ENTRIES {
        let excess = log.len() - MAX_AUDIT_LOG_ENTRIES;
        log.drain(..excess);
    }
}

fn apply_request(
    sm: &mut StateMachineData,
    log_id: &LogId,
    req: &ClientRequest,
) -> ClientResponse {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    let audit = |summary: String| AuditEntry {
        log_index: log_id.index,
        summary,
        timestamp_ms: now_ms,
    };

    match req {
        ClientRequest::CreateFile {
            path,
            volume_id,
            chunk_size,
        } => {
            sm.files.insert(
                path.clone(),
                FileMetadata {
                    path: path.clone(),
                    volume_id: volume_id.clone(),
                    chunk_size: *chunk_size,
                    chunks: Vec::new(),
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                },
            );
            push_audit_capped(&mut sm.audit_log, audit(format!("CreateFile: {path}")));
            ClientResponse::FileCreated {
                path: path.clone(),
                volume_id: volume_id.clone(),
            }
        }

        ClientRequest::DeleteFile { path } => {
            sm.files.remove(path);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeleteFile: {path}")));
            ClientResponse::Success
        }

        ClientRequest::UpdateChunkLocations { path, chunks } => {
            if let Some(file) = sm.files.get_mut(path) {
                file.chunks = merge_chunk_location_entries(&file.chunks, chunks);
                file.updated_at_ms = now_ms;
                push_audit_capped(&mut sm.audit_log, audit(format!(
                    "UpdateChunks: {path} (merged {} incoming -> {} total)",
                    chunks.len(),
                    file.chunks.len()
                )));
                ClientResponse::Success
            } else {
                warn!("UpdateChunkLocations: file not found: {path}");
                ClientResponse::Error {
                    message: format!("file not found: {path}"),
                }
            }
        }

        ClientRequest::RegisterNode { node_id, info } => {
            sm.nodes.insert(*node_id, info.clone());
            push_audit_capped(&mut sm.audit_log, audit(format!("RegisterNode: {node_id}")));
            ClientResponse::Success
        }

        ClientRequest::DeregisterNode { node_id } => {
            sm.nodes.remove(node_id);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeregisterNode: {node_id}")));
            ClientResponse::Success
        }

        ClientRequest::UpdateNodeStatus { node_id, status } => {
            use dtfs_common::types::HealthStatus;
            if let Some(node) = sm.nodes.get_mut(node_id) {
                match status.as_str() {
                    "healthy" => node.health = HealthStatus::Healthy,
                    "suspect" => node.health = HealthStatus::Suspect,
                    "degraded" => node.health = HealthStatus::Degraded,
                    "down" => node.health = HealthStatus::Down,
                    "failed" => node.health = HealthStatus::Failed,
                    "maintenance" => node.health = HealthStatus::Maintenance,
                    "draining" => node.health = HealthStatus::Draining,
                    other => {
                        warn!("unknown node status: {other}");
                    }
                }
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("UpdateNodeStatus: {node_id} -> {status}")),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("node not found: {node_id}"),
                }
            }
        }

        ClientRequest::UpdateNodeUsage {
            node_id,
            used_capacity,
            chunk_count: _chunk_count,
        } => {
            if let Some(node) = sm.nodes.get_mut(node_id) {
                node.used_capacity = *used_capacity;
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("UpdateNodeUsage: {node_id} used={used_capacity}")),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("node not found: {node_id}"),
                }
            }
        }

        ClientRequest::UpdateConfig {
            config_json,
            version,
        } => {
            sm.cluster_config = ClusterConfig {
                config_json: config_json.clone(),
                version: *version,
            };
            push_audit_capped(&mut sm.audit_log, audit(format!("UpdateConfig: v{version}")));
            ClientResponse::Success
        }

        ClientRequest::UpsertVolume { config } => {
            let vol_id = config.id.0.clone();
            push_audit_capped(&mut sm.audit_log, audit(format!("UpsertVolume: {vol_id}")));
            sm.volumes.insert(vol_id, config.clone());
            ClientResponse::Success
        }

        ClientRequest::DeleteVolume { volume_id } => {
            sm.volumes.remove(volume_id);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeleteVolume: {volume_id}")));
            ClientResponse::Success
        }

        ClientRequest::ReportScrubResult {
            node_id,
            disk_path,
            level,
            completed_at_ms,
            chunks_checked,
            chunks_healthy,
            chunks_corrupt,
            chunks_missing,
            duration_secs,
        } => {
            let record = ScrubRecord {
                node_id: *node_id,
                disk_path: disk_path.clone(),
                level: level.clone(),
                completed_at_ms: *completed_at_ms,
                chunks_checked: *chunks_checked,
                chunks_healthy: *chunks_healthy,
                chunks_corrupt: *chunks_corrupt,
                chunks_missing: *chunks_missing,
                duration_secs: *duration_secs,
            };
            // Store per-node: each data node maintains its own latest result.
            // Previously only the single most-recent cluster-wide result was
            // kept, which caused a fast scrub on node B to silently drop node
            // A's result (fix for issue #8).
            sm.scrub_results.insert(*node_id, record);
            push_audit_capped(&mut sm.audit_log, audit(format!(
                "ScrubResult: node={node_id} disk={disk_path} level={level} \
                 checked={chunks_checked} corrupt={chunks_corrupt} missing={chunks_missing}"
            )));
            ClientResponse::Success
        }

        ClientRequest::UpdateMigrationState { json } => {
            // Dedicated field — never touches cluster_config.config_json
            // (fix for issue #3: migration state used to clobber cluster config).
            sm.migration_state_json = json.clone();
            ClientResponse::Success
        }
    }
}
