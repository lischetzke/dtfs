pub mod log_store;
pub mod network;
pub mod snapshot;
pub mod state_machine;

use std::fmt;
use std::io::Cursor;

use serde::{Deserialize, Serialize};

use dtfs_common::topology::Location;
use dtfs_common::types::{DiskInfo, HealthStatus, NodeRole, VolumeConfig};

// ---------------------------------------------------------------------------
// TypeConfig — wires our application types into OpenRaft
// ---------------------------------------------------------------------------

openraft::declare_raft_types!(
    pub TypeConfig:
        D            = ClientRequest,
        R            = ClientResponse,
        NodeId       = u64,
        Node         = DtfsNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// Convenience alias used by the rest of the dtfs-manager crate.
pub type DtfsRaft = openraft::Raft<TypeConfig, state_machine::StateMachineStore>;

/// Type aliases for commonly used OpenRaft generic types parameterised with our
/// `TypeConfig`.
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type Entry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig>;
pub type StoredMembership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;

// ---------------------------------------------------------------------------
// DtfsNode — extra metadata attached to each Raft node
// ---------------------------------------------------------------------------

/// Information about a Raft node that is stored inside the Raft membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtfsNode {
    /// gRPC address (host:port) used for Raft and management RPCs.
    pub addr: String,
    /// Human-readable hostname.
    pub hostname: String,
}

impl fmt::Display for DtfsNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.hostname, self.addr)
    }
}

impl Default for DtfsNode {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:9100".to_string(),
            hostname: "localhost".to_string(),
        }
    }
}

// ---------------------------------------------------------------------------
// ClientRequest — application commands replicated through Raft
// ---------------------------------------------------------------------------

/// A chunk location entry referencing where a chunk replica lives.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkLocationEntry {
    pub chunk_index: u64,
    pub chunk_hash: String,
    pub node_id: String,
    pub disk_id: u32,
    pub size_bytes: u64,
}

impl fmt::Display for ChunkLocationEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "chunk[{}]@{}:disk{}",
            self.chunk_index, self.node_id, self.disk_id
        )
    }
}

/// Merge `incoming` into `existing` without dropping unrelated replicas or chunk indices.
///
/// Entries are keyed by `(chunk_index, chunk_hash, node_id)`; a matching triple is replaced
/// by the incoming row (e.g. updated `size_bytes`).
pub fn merge_chunk_location_entries(
    existing: &[ChunkLocationEntry],
    incoming: &[ChunkLocationEntry],
) -> Vec<ChunkLocationEntry> {
    let mut out: Vec<ChunkLocationEntry> = existing.to_vec();
    for inc in incoming {
        out.retain(|e| {
            !(e.chunk_index == inc.chunk_index
                && e.chunk_hash == inc.chunk_hash
                && e.node_id == inc.node_id)
        });
        out.push(inc.clone());
    }
    out.sort_by(|a, b| {
        a.chunk_index
            .cmp(&b.chunk_index)
            .then_with(|| a.chunk_hash.cmp(&b.chunk_hash))
            .then_with(|| a.node_id.cmp(&b.node_id))
    });
    out
}

/// Node information stored in the Raft state machine (richer than `DtfsNode`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NodeInfo {
    pub role: NodeRole,
    pub hostname: String,
    pub grpc_addr: String,
    pub location: Location,
    pub disks: Vec<DiskInfo>,
    pub total_capacity: u64,
    pub used_capacity: u64,
    pub health: HealthStatus,
}

impl fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({}, {:?})", self.hostname, self.grpc_addr, self.role)
    }
}

/// Commands that mutate the cluster state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClientRequest {
    // ---- Metadata operations ----
    CreateFile {
        path: String,
        volume_id: String,
        chunk_size: u64,
    },
    DeleteFile {
        path: String,
    },
    UpdateChunkLocations {
        path: String,
        chunks: Vec<ChunkLocationEntry>,
    },

    // ---- Cluster operations ----
    RegisterNode {
        node_id: u64,
        info: NodeInfo,
    },
    DeregisterNode {
        node_id: u64,
    },
    UpdateNodeStatus {
        node_id: u64,
        status: String,
    },

    /// Heartbeat-derived usage counters for a data node (best-effort).
    UpdateNodeUsage {
        node_id: u64,
        used_capacity: u64,
        chunk_count: u64,
    },

    // ---- Config operations ----
    UpdateConfig {
        config_json: String,
        version: u64,
    },

    /// Create or update a volume configuration.
    UpsertVolume {
        config: VolumeConfig,
    },
    /// Remove a volume configuration.
    DeleteVolume {
        volume_id: String,
    },

    // ---- Observability operations ----
    /// Record the result of a completed scrub operation.
    ///
    /// Data nodes report this after each quick or deep scrub cycle, and the
    /// CLI can inject a result after a manually-triggered scrub.  The result
    /// is stored per data-node in `scrub_results` and surfaced by
    /// `/api/v1/stats` and the WebUI home page.
    ReportScrubResult {
        node_id: u64,
        disk_path: String,
        /// `"quick"` or `"deep"`
        level: String,
        /// Unix timestamp in milliseconds when the scrub completed.
        completed_at_ms: u64,
        chunks_checked: u64,
        chunks_healthy: u64,
        chunks_corrupt: u64,
        chunks_missing: u64,
        duration_secs: f64,
    },

    /// Persist migration-scheduler state as a dedicated Raft entry.
    ///
    /// Kept separate from `UpdateConfig` so that the scheduler's internal
    /// bookkeeping never clobbers user-visible cluster configuration.
    UpdateMigrationState {
        /// JSON-serialised `MigrationState` struct from `dtfs-manager`.
        json: String,
    },
}

impl fmt::Display for ClientRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateFile { path, volume_id, .. } => {
                write!(f, "CreateFile({path}, vol={volume_id})")
            }
            Self::DeleteFile { path } => write!(f, "DeleteFile({path})"),
            Self::UpdateChunkLocations { path, chunks } => {
                write!(f, "UpdateChunks({path}, {} chunks)", chunks.len())
            }
            Self::RegisterNode { node_id, .. } => write!(f, "RegisterNode({node_id})"),
            Self::DeregisterNode { node_id } => write!(f, "DeregisterNode({node_id})"),
            Self::UpdateNodeStatus { node_id, status } => {
                write!(f, "UpdateNodeStatus({node_id}, {status})")
            }
            Self::UpdateNodeUsage {
                node_id,
                used_capacity,
                chunk_count,
            } => {
                write!(
                    f,
                    "UpdateNodeUsage({node_id}, used={used_capacity}, chunks={chunk_count})"
                )
            }
            Self::UpdateConfig { version, .. } => write!(f, "UpdateConfig(v{version})"),
            Self::UpsertVolume { config } => write!(f, "UpsertVolume({})", config.id),
            Self::DeleteVolume { volume_id } => write!(f, "DeleteVolume({volume_id})"),
            Self::ReportScrubResult { node_id, level, chunks_checked, chunks_corrupt, .. } => {
                write!(f, "ReportScrubResult(node={node_id}, {level}, checked={chunks_checked}, corrupt={chunks_corrupt})")
            }
            Self::UpdateMigrationState { .. } => write!(f, "UpdateMigrationState"),
        }
    }
}

// ---------------------------------------------------------------------------
// ClientResponse — result returned after a command is applied
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ClientResponse {
    Success,
    FileCreated { path: String, volume_id: String },
    Error { message: String },
}

impl Default for ClientResponse {
    fn default() -> Self {
        Self::Success
    }
}

impl fmt::Display for ClientResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success => write!(f, "ok"),
            Self::FileCreated { path, volume_id } => {
                write!(f, "file_created({path}, vol={volume_id})")
            }
            Self::Error { message } => write!(f, "error({message})"),
        }
    }
}

#[cfg(test)]
mod merge_tests {
    use super::{merge_chunk_location_entries, ChunkLocationEntry};

    #[test]
    fn merge_adds_replica_same_chunk_distinct_node_replaces_identical_triple() {
        let a = ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "aa".into(),
            node_id: "http://n1:1".into(),
            disk_id: 0,
            size_bytes: 10,
        };
        let b = ChunkLocationEntry {
            chunk_index: 1,
            chunk_hash: "bb".into(),
            node_id: "http://n2:1".into(),
            disk_id: 0,
            size_bytes: 20,
        };
        // Second replica for chunk "aa" on another node — keep both.
        let inc = ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "aa".into(),
            node_id: "http://n3:1".into(),
            disk_id: 0,
            size_bytes: 11,
        };
        let merged = merge_chunk_location_entries(&[a.clone(), b.clone()], &[inc.clone()]);
        assert_eq!(merged.len(), 3);
        assert!(merged.contains(&a));
        assert!(merged.contains(&b));
        assert!(merged.contains(&inc));

        // Same triple updates size (replaces prior row).
        let inc2 = ChunkLocationEntry {
            chunk_index: 0,
            chunk_hash: "aa".into(),
            node_id: "http://n3:1".into(),
            disk_id: 0,
            size_bytes: 99,
        };
        let merged2 = merge_chunk_location_entries(&merged, &[inc2.clone()]);
        assert_eq!(merged2.len(), 3);
        assert!(merged2.contains(&inc2));
        assert!(!merged2.contains(&inc));
    }
}
