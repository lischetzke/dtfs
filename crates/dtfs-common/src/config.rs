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
    /// P1-8: alert webhook sinks (operator-configured HTTP receivers).
    /// `#[serde(default)]` keeps existing configs loadable.
    #[serde(default)]
    pub alerts: AlertsConfig,
}

/// P1-8 alerts config.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlertsConfig {
    /// Webhook URLs and per-sink shared secrets.
    #[serde(default)]
    pub webhooks: Vec<AlertWebhook>,
}

/// One alert webhook sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertWebhook {
    pub url: String,
    #[serde(default)]
    pub secret: String,
    /// Restrict the kinds this sink receives. Empty = all.
    #[serde(default)]
    pub kinds: Vec<String>,
    #[serde(default = "default_dedup_secs")]
    pub dedup_secs: u64,
}

fn default_dedup_secs() -> u64 {
    300
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
    /// Optional list of manager peers to seed the Raft cluster with.
    ///
    /// Set this on the bootstrap node so that `raft.initialize(...)` opens
    /// the cluster with all expected voters at once. Each entry is the
    /// peer's gRPC `host:port` (as advertised in that peer's
    /// `grpc.listen_addr`). The bootstrap node's own address may be
    /// included or omitted — it is always added.
    ///
    /// Followers (`bootstrap = false`) do not need to set this; they wait to
    /// be enrolled by the bootstrap node's `raft.initialize(...)` call.
    ///
    /// Without this list, the bootstrap node initialises a single-voter
    /// cluster and no follower ever joins (see FINDINGS.md F-009).
    #[serde(default)]
    pub initial_voters: Vec<InitialVoter>,
}

/// Pre-bootstrap description of a manager peer for `[cluster].initial_voters`.
///
/// `hostname` is used to derive the same `NodeId` the peer will compute on
/// its own startup (`node_id_from_hostname`). `addr` is the gRPC `host:port`
/// the peer listens on.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitialVoter {
    pub hostname: String,
    pub addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataClusterConfig {
    /// Primary manager endpoint (`host:port`). Always required.
    pub manager_addr: String,
    pub shared_secret: String,
    /// Optional additional manager endpoints. When the primary becomes
    /// unreachable (e.g. after a rolling manager restart, F-024) the data
    /// node walks through this list to find another manager and re-register.
    /// Endpoints are formatted the same way as `manager_addr` (`host:port`).
    #[serde(default)]
    pub manager_addrs: Vec<String>,
}

impl DataClusterConfig {
    /// All manager endpoints in priority order: the primary first, followed
    /// by any extras. Duplicates are removed but order is preserved.
    pub fn all_manager_addrs(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(1 + self.manager_addrs.len());
        out.push(self.manager_addr.clone());
        for a in &self.manager_addrs {
            if !out.contains(a) {
                out.push(a.clone());
            }
        }
        out
    }
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
    /// Max in-flight `PutChunk` operations on a data node. Bounds peak
    /// memory: each in-flight write buffers ≤ one chunk (~128 MB by
    /// default). Without this cap, sustained 100 MB/s rsync to a data
    /// node OOM-kills it on 4–8 GB VMs because every concurrent gRPC
    /// stream holds a fresh chunk in memory. SOAK-RECOVERY-2026-05-18
    /// bug D.
    #[serde(default = "default_max_concurrent_chunk_writes")]
    pub max_concurrent_chunk_writes: usize,
}

fn default_max_concurrent_chunk_writes() -> usize {
    // 8 concurrent × 128 MB chunks = ~1 GB buffer ceiling. Leaves
    // plenty of headroom on an 8 GB data-node VM after the 512 MB ARC
    // cache and tonic/raft overhead.
    8
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
    /// Optional additional manager endpoints used when `manager_addr`
    /// becomes unreachable (F-024 hardening).
    #[serde(default)]
    pub manager_addrs: Vec<String>,
}

impl FuseClientConfig {
    pub fn all_manager_addrs(&self) -> Vec<String> {
        let mut out = Vec::with_capacity(1 + self.manager_addrs.len());
        out.push(self.manager_addr.clone());
        for a in &self.manager_addrs {
            if !out.contains(a) {
                out.push(a.clone());
            }
        }
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MountConfig {
    pub mountpoint: PathBuf,
    /// F-028: when true, mount with FUSE `allow_other` so non-root users
    /// (e.g. a media-server service running under its own UID) can read and
    /// write through `/mnt/dtfs`. Defaults to false to keep the historic
    /// behaviour and avoid a permissive default.
    #[serde(default)]
    pub allow_other: bool,
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
    /// P0-11: maximum age of a cached chunk before the pruner evicts it.
    /// `None` = no age-based eviction (size-only). Used to implement
    /// per-DC retention policies, e.g. "DC B keeps only files accessed in
    /// the last 90 days". Eviction respects the file's atime so chunks
    /// re-read recently are kept.
    #[serde(default)]
    pub max_age_days: Option<u32>,
    /// P0-11: interval (in seconds) between background pruner sweeps.
    /// Defaults to 1 hour.
    #[serde(default = "default_prune_interval")]
    pub prune_interval_secs: u64,
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
fn default_prune_interval() -> u64 {
    3600 // 1 hour
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

    /// Bug D — the data-node config MUST default
    /// `max_concurrent_chunk_writes` to a finite value. Without a bound,
    /// sustained streaming writes to a data node accumulate one
    /// `chunk_size` buffer per concurrent gRPC stream and OOM-kill the
    /// process on small VMs. Lab evidence (4 GB then 8 GB RAM data
    /// nodes both OOM'd under rsync); SOAK-RECOVERY-2026-05-18.md bug D.
    #[test]
    fn bug_d_default_max_concurrent_chunk_writes_is_bounded() {
        // Parse a minimal data-node config without a [grpc.max_concurrent_chunk_writes]
        // line and verify the default kicks in.
        let toml_str = r#"
[node]
hostname = "data-x"

[location]
region = "lab"
datacenter = "dc-x"
rack = "r-x"
host = "data-x"

[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "s"

[grpc]
listen_addr = "0.0.0.0:9500"

[tls]
mode = "off"

[[disks]]
path = "/mnt/hot"
tier = "hot"

[cache]
max_memory_mb = 512
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
"#;
        let config: DataNodeConfig = toml::from_str(toml_str).unwrap();
        let limit = config.grpc.max_concurrent_chunk_writes;
        assert!(limit > 0, "limit must be > 0 (got {limit})");
        // Sanity ceiling: 8 chunks × 128 MB = 1 GB; the default should
        // sit well below the smallest VM size we plan to support
        // (4 GB) leaving room for ARC + tonic + raft.
        assert!(
            limit <= 32,
            "default max_concurrent_chunk_writes too high ({limit}) — risks OOM on small data-node VMs"
        );
    }

    /// And operators MUST be able to override the cap from data.toml.
    #[test]
    fn bug_d_max_concurrent_chunk_writes_is_overridable() {
        let toml_str = r#"
[node]
hostname = "data-x"

[location]
region = "lab"
datacenter = "dc-x"
rack = "r-x"
host = "data-x"

[cluster]
manager_addr = "mgr-01:9401"
shared_secret = "s"

[grpc]
listen_addr = "0.0.0.0:9500"
max_concurrent_chunk_writes = 4

[tls]
mode = "off"

[[disks]]
path = "/mnt/hot"
tier = "hot"

[cache]
max_memory_mb = 512
policy = "arc"

[scrub]
quick_interval_hours = 6
deep_interval_hours = 168
max_io_percent = 10
"#;
        let config: DataNodeConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.grpc.max_concurrent_chunk_writes, 4);
    }
}
