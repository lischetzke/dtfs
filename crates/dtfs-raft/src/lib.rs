pub mod log_store;
pub mod network;
pub mod snapshot;
pub mod state_machine;

use std::fmt;
use std::io::Cursor;

use serde::{Deserialize, Serialize};

use dtfs_common::topology::Location;
use dtfs_common::types::{DiskInfo, HealthStatus, NodeRole};

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

    // ---- Config operations ----
    UpdateConfig {
        config_json: String,
        version: u64,
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
            Self::UpdateConfig { version, .. } => write!(f, "UpdateConfig(v{version})"),
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
