use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::topology::Location;
use crate::types::{DiskTier, PartitionMode, TierReplication, WriteSafety};

/// Manager node configuration (loaded from /etc/dtfs/manager.toml).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagerConfig {
    pub node: NodeConfig,
    pub location: Location,
    pub cluster: ClusterConfig,
    pub raft: RaftConfig,
    pub grpc: GrpcConfig,
    pub api: ApiConfig,
    pub tls: TlsConfig,
    pub defaults: DefaultsConfig,
    pub tiering: TieringConfig,
    pub monitoring: MonitoringConfig,
}

/// Data node configuration (loaded from /etc/dtfs/data.toml).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataNodeConfig {
    pub node: NodeConfig,
    pub location: Location,
    pub cluster: DataClusterConfig,
    pub grpc: GrpcConfig,
    pub tls: TlsConfig,
    pub disks: Vec<DiskConfig>,
    pub cache: CacheConfig,
    pub scrub: ScrubConfig,
    /// Optional LTFS-backed tape devices for frozen/archival storage.
    /// Leave empty (or omit) if this node has no tape drives.
    #[serde(default)]
    pub tape_devices: Vec<TapeDeviceConfig>,
}

/// FUSE client configuration (loaded from /etc/dtfs/fuse.toml).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseConfig {
    pub client: FuseClientConfig,
    pub location: Location,
    pub mount: MountConfig,
    pub readahead: ReadAheadConfig,
    pub cache: FuseCacheConfig,
    pub partition_safety: PartitionSafetyConfig,
}

// --- Sub-configs ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    #[serde(default = "default_auto")]
    pub id: String,
    pub hostname: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub shared_secret: String,
    #[serde(default)]
    pub bootstrap: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataClusterConfig {
    pub manager_addr: String,
    pub shared_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    #[serde(default = "default_raft_listen")]
    pub listen_addr: String,
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    pub listen_addr: String,
    #[serde(default = "default_max_message_size")]
    pub max_message_size_mb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_listen")]
    pub listen_addr: String,
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default = "default_tls_mode")]
    pub mode: String,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DefaultsConfig {
    #[serde(default = "default_chunk_size")]
    pub chunk_size_mb: u32,
    #[serde(default)]
    pub write_safety: WriteSafety,
    #[serde(default)]
    pub replication: TierReplication,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_hot_threshold")]
    pub hot_threshold_reads_per_hour: u32,
    #[serde(default = "default_cold_threshold")]
    pub cold_threshold_days: u32,
    #[serde(default = "default_frozen_threshold")]
    pub frozen_threshold_days: u32,
    #[serde(default = "default_migration_bandwidth")]
    pub migration_bandwidth_limit_mbps: u32,
    #[serde(default = "default_tiering_interval")]
    pub evaluation_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    #[serde(default = "default_heartbeat_secs")]
    pub heartbeat_interval_secs: u64,
    #[serde(default = "default_suspect_after")]
    pub suspect_after_missed: u32,
    #[serde(default = "default_down_after")]
    pub down_after_missed: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiskConfig {
    pub path: PathBuf,
    pub tier: DiskTier,
}

/// Configuration for a tape drive or tape library backed by LTFS.
///
/// Tape devices provide **frozen/archival** storage.  Data is written to the
/// LTFS mount point exactly like any other disk tier; the LTFS layer handles
/// the tape-specific I/O.  Only the tiering engine writes to tape — client
/// writes never go directly to a tape-tier disk.
///
/// # Prerequisites
/// - LTFS must be installed (`apt install ltfs` on Debian/Ubuntu).
/// - The tape must be LTFS-formatted (`mkltfs /dev/nst0`) and mounted
///   (`ltfs /mnt/tape-a -o devname=/dev/nst0`) before `dtfs-data` starts.
/// - For tape libraries, the changer daemon must be running.
///
/// # Example
/// ```toml
/// [[tape_devices]]
/// ltfs_path    = "/mnt/tape-a"
/// device       = "/dev/nst0"
/// library_device = "/dev/sg2"   # omit for standalone drive
/// label        = "lto9-library-a"
/// capacity_gb  = 18000          # LTO-9 native: 18 TB
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TapeDeviceConfig {
    /// LTFS mount point path (must be pre-mounted before dtfs-data starts).
    pub ltfs_path: PathBuf,
    /// Raw tape device node (e.g. `/dev/nst0`). Used for status / rewinding.
    /// Optional — omit if you only interact with the device via LTFS.
    pub device: Option<String>,
    /// Tape library changer device (e.g. `/dev/sg2`). Required only for
    /// automated tape libraries with a robotic arm. Omit for standalone drives.
    pub library_device: Option<String>,
    /// Human-readable label shown in WebUI / CLI (e.g. `"lto9-library-a"`).
    pub label: String,
    /// Capacity hint per cartridge in GiB (informational only; does not
    /// limit usage). LTO-9 native = ~18 000 GiB.
    pub capacity_gb: Option<u64>,
    /// Maximum bandwidth to use for tiering migrations to this device (MB/s).
    /// `0` means unlimited (use full tape throughput, typically 300–400 MB/s
    /// for LTO-9).
    #[serde(default)]
    pub max_write_mbps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_memory")]
    pub max_memory_mb: u64,
    #[serde(default = "default_cache_policy")]
    pub policy: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrubConfig {
    #[serde(default = "default_quick_interval")]
    pub quick_interval_hours: u64,
    #[serde(default = "default_deep_interval")]
    pub deep_interval_hours: u64,
    pub quick_cron: Option<String>,
    pub deep_cron: Option<String>,
    #[serde(default = "default_scrub_io")]
    pub max_io_percent: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseClientConfig {
    pub manager_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    pub mountpoint: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadAheadConfig {
    #[serde(default = "default_readahead_mode")]
    pub mode: String,
    #[serde(default = "default_prefetch_chunks")]
    pub prefetch_chunks: u32,
    #[serde(default = "default_prefetch_files")]
    pub prefetch_next_files: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseCacheConfig {
    pub local_cache_dir: Option<PathBuf>,
    #[serde(default = "default_local_cache_gb")]
    pub local_cache_max_gb: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionSafetyConfig {
    #[serde(default)]
    pub mode: PartitionMode,
    #[serde(default = "default_quorum_timeout")]
    pub quorum_loss_timeout_secs: u64,
    #[serde(default = "default_true")]
    pub show_warning: bool,
}

// --- Default value functions ---

fn default_auto() -> String {
    "auto".to_string()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/dtfs")
}
fn default_raft_listen() -> String {
    "0.0.0.0:9400".to_string()
}
fn default_election_timeout() -> u64 {
    400
}
fn default_heartbeat_interval() -> u64 {
    150
}
fn default_snapshot_threshold() -> u64 {
    10000
}
fn default_max_message_size() -> u32 {
    256
}
fn default_api_listen() -> String {
    "0.0.0.0:9402".to_string()
}
fn default_cors_origins() -> Vec<String> {
    vec!["*".to_string()]
}
fn default_tls_mode() -> String {
    "auto".to_string()
}
fn default_chunk_size() -> u32 {
    128
}
fn default_true() -> bool {
    true
}
fn default_hot_threshold() -> u32 {
    5
}
fn default_cold_threshold() -> u32 {
    14
}
fn default_frozen_threshold() -> u32 {
    60
}
fn default_migration_bandwidth() -> u32 {
    200
}
fn default_tiering_interval() -> u64 {
    3600
}
fn default_heartbeat_secs() -> u64 {
    5
}
fn default_suspect_after() -> u32 {
    3
}
fn default_down_after() -> u32 {
    6
}
fn default_cache_memory() -> u64 {
    8192
}
fn default_cache_policy() -> String {
    "arc".to_string()
}
fn default_quick_interval() -> u64 {
    6
}
fn default_deep_interval() -> u64 {
    168
}
fn default_scrub_io() -> u32 {
    10
}
fn default_readahead_mode() -> String {
    "folder-sequential".to_string()
}
fn default_prefetch_chunks() -> u32 {
    8
}
fn default_prefetch_files() -> u32 {
    3
}
fn default_local_cache_gb() -> u64 {
    50
}
fn default_quorum_timeout() -> u64 {
    10
}

/// Load a configuration from a TOML file.
pub fn load_config<T: for<'de> Deserialize<'de>>(path: &std::path::Path) -> anyhow::Result<T> {
    let content = std::fs::read_to_string(path)?;
    let config: T = toml::from_str(&content)?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data_node_config() {
        let toml_str = r#"
[node]
id = "auto"
hostname = "data-01.example.com"
data_dir = "/var/lib/dtfs/data"

[location]
region = "eu"
datacenter = "frankfurt"
rack = "rack-a1"
host = "data-01"

[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "test-secret"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "auto"

[[disks]]
path = "/mnt/ssd-a"
tier = "hot"

[[disks]]
path = "/mnt/hdd-a"
tier = "cold"

[cache]
max_memory_mb = 4096
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
"#;
        let config: DataNodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.disks.len(), 2);
        assert_eq!(config.disks[0].tier, DiskTier::Hot);
        assert_eq!(config.disks[1].tier, DiskTier::Cold);
        assert_eq!(config.location.datacenter, "frankfurt");
        assert_eq!(config.scrub.quick_interval_hours, 6);
    }
}
