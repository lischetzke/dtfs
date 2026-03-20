use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a node in the cluster.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct NodeId(pub String);

impl NodeId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn from_string(s: String) -> Self {
        Self(s)
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Unique identifier for a chunk of data.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct ChunkId(pub [u8; 32]);

impl ChunkId {
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn as_hex(&self) -> String {
        hex_encode(&self.0)
    }

    /// Returns first two hex chars for directory partitioning.
    pub fn dir_prefix_1(&self) -> String {
        format!("{:02x}", self.0[0])
    }

    /// Returns next two hex chars for second-level directory.
    pub fn dir_prefix_2(&self) -> String {
        format!("{:02x}", self.0[1])
    }
}

impl fmt::Display for ChunkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_hex())
    }
}

/// Unique identifier for a volume.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct VolumeId(pub String);

impl VolumeId {
    pub fn new(name: String) -> Self {
        Self(name)
    }
}

impl fmt::Display for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Storage tier classification for disks.
///
/// Tiers form a performance/cost hierarchy:
/// - `Hot`  — fast SSD, highest cost, highest IOPS/throughput
/// - `Warm` — mid-tier (SSD or fast HDD)
/// - `Cold` — bulk HDD, low cost, lower throughput
/// - `Tape` — LTFS-backed tape storage for frozen/archival data (write-once,
///            very high latency, very low cost per TB, LTO-9 ≈ 300-400 MB/s sequential)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiskTier {
    Hot,
    Warm,
    Cold,
    /// LTFS (Linear Tape File System) backed storage.  Chunks on this tier
    /// are write-once; migration is append-only.  No in-place scrubbing —
    /// LTFS provides its own checksum/integrity layer.
    Tape,
}

impl fmt::Display for DiskTier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DiskTier::Hot => write!(f, "hot"),
            DiskTier::Warm => write!(f, "warm"),
            DiskTier::Cold => write!(f, "cold"),
            DiskTier::Tape => write!(f, "tape"),
        }
    }
}

/// Data temperature classification based on access patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataTemperature {
    Hot,
    Warm,
    Cold,
    Frozen,
}

/// Role of a node in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Manager,
    Data,
}

/// Health status of a node or disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HealthStatus {
    Healthy,
    Suspect,
    Degraded,
    Down,
    Failed,
    Maintenance,
    Draining,
}

/// Status of a disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiskStatus {
    Healthy,
    Degraded,
    Draining,
    Failed,
    Removed,
}

/// Information about a managed disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiskInfo {
    pub disk_id: u32,
    pub uuid: String,
    pub mount_path: String,
    pub tier: DiskTier,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub status: DiskStatus,
}

/// Information about a node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: NodeId,
    pub role: NodeRole,
    pub hostname: String,
    pub grpc_port: u16,
    pub location: crate::topology::Location,
    pub disks: Vec<DiskInfo>,
    pub total_capacity: u64,
    pub used_capacity: u64,
    pub health: HealthStatus,
}

/// Per-tier replication configuration.
///
/// Each tier can have a different replication factor reflecting the
/// cost/durability trade-off of that tier's media:
///
/// - `hot`  — 3 copies across hot SSDs (high durability, low latency)
/// - `warm` — 2 copies (balanced)
/// - `cold` — 1 copy on cold HDDs (cost-optimised; tape backup adds durability)
/// - `tape` — 1 copy; tape cartridges provide inherent sequential redundancy
///            and tape libraries typically use multiple drives for availability
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TierReplication {
    pub hot: u32,
    pub warm: u32,
    pub cold: u32,
    /// Replication factor for tape-tier chunks.  Usually 1 — the tape library
    /// itself provides durability.  Set to 2 if you have two independent tape
    /// libraries and want a second copy on each.
    #[serde(default = "TierReplication::default_tape")]
    pub tape: u32,
}

impl TierReplication {
    fn default_tape() -> u32 { 1 }

    /// Return the replication factor for the given tier.
    pub fn for_tier(&self, tier: DiskTier) -> u32 {
        match tier {
            DiskTier::Hot  => self.hot,
            DiskTier::Warm => self.warm,
            DiskTier::Cold => self.cold,
            DiskTier::Tape => self.tape,
        }
    }
}

impl Default for TierReplication {
    fn default() -> Self {
        Self {
            hot: 3,
            warm: 2,
            cold: 1,
            tape: 1,
        }
    }
}

/// Write safety configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WriteSafety {
    pub min_replicas_before_ack: u32,
    pub write_timeout_secs: u32,
}

impl Default for WriteSafety {
    fn default() -> Self {
        Self {
            min_replicas_before_ack: 2,
            write_timeout_secs: 30,
        }
    }
}

/// Volume configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VolumeConfig {
    pub id: VolumeId,
    pub name: String,
    pub chunk_size_bytes: u64,
    pub replication: TierReplication,
    pub write_safety: WriteSafety,
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 128 * 1024 * 1024, // 128MB
            replication: TierReplication::default(),
            write_safety: WriteSafety::default(),
        }
    }
}

/// Chunk metadata stored alongside chunk data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub id: ChunkId,
    pub size: u64,
    pub checksum: [u8; 32],
    pub created_at: i64,
    pub volume_id: VolumeId,
    pub replication_factor: u32,
}

/// Scrub level for integrity checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScrubLevel {
    Quick,
    Deep,
}

/// Scrub target.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScrubTarget {
    All,
    Disk(String),
    File(String),
    Chunk(ChunkId),
}

/// Scrub priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScrubPriority {
    Background,
    Urgent,
}

/// Result of a scrub operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrubResult {
    pub chunks_checked: u64,
    pub chunks_healthy: u64,
    pub chunks_corrupt: u64,
    pub chunks_missing: u64,
    pub chunks_size_mismatch: u64,
    pub duration_secs: f64,
}

/// Migration task status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationStatus {
    Pending,
    Copying,
    Verifying,
    Completing,
    Done,
    Failed,
}

/// Partition safety mode for FUSE clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PartitionMode {
    Strict,
    LocalReadOnly,
    FullLock,
}

impl Default for PartitionMode {
    fn default() -> Self {
        Self::Strict
    }
}

/// Failure domain level for replica placement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FailureDomain {
    Host,
    Rack,
    Datacenter,
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_id_hex() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xab;
        bytes[1] = 0xcd;
        let chunk_id = ChunkId::from_bytes(bytes);
        assert_eq!(chunk_id.dir_prefix_1(), "ab");
        assert_eq!(chunk_id.dir_prefix_2(), "cd");
    }

    #[test]
    fn test_node_id_display() {
        let id = NodeId::from_string("test-node-1".to_string());
        assert_eq!(id.to_string(), "test-node-1");
    }

    #[test]
    fn test_tier_replication_default() {
        let tr = TierReplication::default();
        assert_eq!(tr.hot, 3);
        assert_eq!(tr.warm, 2);
        assert_eq!(tr.cold, 1);
    }
}
