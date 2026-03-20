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

use crate::snapshot::{self, StateMachineSnapshot};
use crate::{
    ChunkLocationEntry, ClientRequest, ClientResponse, TypeConfig,
};

// Re-export type aliases for convenience.
type LogId = crate::LogId;
type StoredMembership = crate::StoredMembership;

// ---------------------------------------------------------------------------
// Domain types stored inside the state machine
// ---------------------------------------------------------------------------

/// A completed scrub operation result, stored in the state machine so the
/// WebUI and `/api/v1/stats` can surface the most recent scrub without
/// querying individual data nodes.
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
    /// The most recent scrub result reported by any data node.
    /// Updated by `ClientRequest::ReportScrubResult`; surfaced by
    /// `GET /api/v1/stats` and the WebUI home page.
    pub last_scrub: Option<ScrubRecord>,
    /// Number of registered FUSE clients (updated by client heartbeats).
    pub client_count: u64,
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
            last_scrub: None,
            client_count: 0,
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
            last_scrub: sm.last_scrub.clone(),
            client_count: sm.client_count,
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
            sm.last_scrub = snap_data.last_scrub;
            sm.client_count = snap_data.client_count;
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
            sm.audit_log
                .push(audit(format!("CreateFile: {path}")));
            ClientResponse::FileCreated {
                path: path.clone(),
                volume_id: volume_id.clone(),
            }
        }

        ClientRequest::DeleteFile { path } => {
            sm.files.remove(path);
            sm.audit_log
                .push(audit(format!("DeleteFile: {path}")));
            ClientResponse::Success
        }

        ClientRequest::UpdateChunkLocations { path, chunks } => {
            if let Some(file) = sm.files.get_mut(path) {
                file.chunks.clone_from(chunks);
                file.updated_at_ms = now_ms;
                sm.audit_log.push(audit(format!(
                    "UpdateChunks: {path} ({} chunks)",
                    chunks.len()
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
            sm.audit_log
                .push(audit(format!("RegisterNode: {node_id}")));
            ClientResponse::Success
        }

        ClientRequest::DeregisterNode { node_id } => {
            sm.nodes.remove(node_id);
            sm.audit_log
                .push(audit(format!("DeregisterNode: {node_id}")));
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
                sm.audit_log
                    .push(audit(format!("UpdateNodeStatus: {node_id} -> {status}")));
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
            sm.audit_log
                .push(audit(format!("UpdateConfig: v{version}")));
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
            // Keep only the most recent scrub result cluster-wide.
            let is_newer = sm
                .last_scrub
                .as_ref()
                .map_or(true, |prev| *completed_at_ms > prev.completed_at_ms);
            if is_newer {
                sm.last_scrub = Some(record);
            }
            sm.audit_log.push(audit(format!(
                "ScrubResult: node={node_id} disk={disk_path} level={level} \
                 checked={chunks_checked} corrupt={chunks_corrupt} missing={chunks_missing}"
            )));
            ClientResponse::Success
        }
    }
}
