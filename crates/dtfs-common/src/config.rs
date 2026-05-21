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
    /// Bug AH — audit log replication settings.
    #[serde(default)]
    pub audit: AuditConfig,
}

/// Bug AH — audit log replication configuration.
///
/// The cluster's audit events default to local-per-manager storage to avoid
/// breaking rolling upgrades: a leader on a new binary would propose
/// `ClientRequest::AppendAudit` Raft entries that older follower binaries
/// can't deserialize, leaving them stuck. Operator opt-in via
/// `replicate_via_raft = true` should happen AFTER every manager is on
/// the new code.
///
/// Deployment recipe:
///   1. Rolling-deploy the new dtfs-manager binary to all voters with
///      `replicate_via_raft = false` (the default). Each restart is safe.
///   2. Verify every voter reports the new binary via /api/v1/raft/state.
///   3. Edit /etc/dtfs/manager.toml on each voter to set
///      `[audit] replicate_via_raft = true`.
///   4. Rolling-restart again (or send SIGHUP if hot-reload is wired).
///   5. Confirm `/api/v1/audit` returns the same view from any manager.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditConfig {
    /// When true, every `audit.log()` call also proposes a
    /// `ClientRequest::AppendAudit` Raft write so the entry replicates
    /// to every manager's state-machine `audit_log`. When false (the
    /// default during the rolling-upgrade window), entries stay in the
    /// per-manager local ring + JSONL archive.
    #[serde(default)]
    pub replicate_via_raft: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            // SAFE DEFAULT: don't propose AppendAudit until operator
            // confirms all managers are upgraded.
            replicate_via_raft: false,
        }
    }
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
    /// Bug G — per-process RAM cap with proactive backpressure. When
    /// `hard_cap_mb > 0` the data node uses it to size the put_chunk
    /// concurrency and ARC cache, and a background sampler flips a
    /// shedding flag once RSS crosses the configured threshold so new
    /// writes get RESOURCE_EXHAUSTED instead of OOM-killing the process.
    /// `#[serde(default)]` so old config files keep working.
    #[serde(default)]
    pub memory: MemoryConfig,
}

/// Hard memory budget for the data node process.
///
/// `hard_cap_mb = 0` (the default) → unbounded, no backpressure, no
/// recommendation. Existing deployments that don't have a `[memory]`
/// section in data.toml keep their previous behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Total RAM budget for this data-node process, in MB. 0 disables.
    #[serde(default)]
    pub hard_cap_mb: u64,
    /// Log a warning once RSS climbs above this fraction of the cap.
    #[serde(default = "default_warn_pct")]
    pub warn_threshold_pct: u8,
    /// Start refusing new `put_chunk` writes once RSS climbs above this
    /// fraction of the cap. Should be higher than `warn_threshold_pct`.
    #[serde(default = "default_shed_pct")]
    pub shed_threshold_pct: u8,
    /// How often the background sampler reads `/proc/self/status`, in
    /// seconds. 0 is treated as 1.
    #[serde(default = "default_memory_sample_secs")]
    pub sample_interval_secs: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            hard_cap_mb: 0,
            warn_threshold_pct: default_warn_pct(),
            shed_threshold_pct: default_shed_pct(),
            sample_interval_secs: default_memory_sample_secs(),
        }
    }
}

fn default_warn_pct() -> u8 {
    80
}
fn default_shed_pct() -> u8 {
    92
}
fn default_memory_sample_secs() -> u64 {
    5
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
    /// Bug AV (AV-3-1) — write-path preferences. Optional; absent
    /// block falls through to the safe default (`first_write_tier
    /// = "cold"`) so a legacy `fuse.toml` keeps working.
    #[serde(default)]
    pub write: FuseWriteConfig,
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
///
/// Bug X — optional location fields. Pre-bootstrap voters used to render
/// in the WebUI / API node list with placeholder `?/?/?/<hostname>` because
/// managers never call `RegisterNode` and the initial-voters config had no
/// location data. Operators can now pin region/datacenter/rack here so the
/// rendered topology reflects reality. All four fields default to empty
/// strings so existing configs keep loading without changes; the API
/// synthesizer treats empty as "unknown" and renders `?` as before.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InitialVoter {
    pub hostname: String,
    pub addr: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub datacenter: String,
    #[serde(default)]
    pub rack: String,
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
    /// Bug Z — anchor datacenter for the auto-created "default" volume.
    /// When set, every chunk placement guarantees at least one replica
    /// lives in this DC. Plexmedia: `"leli"` so data-primary01 always
    /// holds a copy of every file. `None` = no anchor enforcement.
    #[serde(default)]
    pub anchor_datacenter: Option<String>,
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
    /// Bug AA — cluster shared secret. Required for client
    /// self-registration via RegisterNode(role="client"). Same secret
    /// that data nodes + manager peers use; missing/empty means the
    /// client doesn't self-register and only appears in the topology
    /// when it holds an active write lease (legacy behavior).
    #[serde(default)]
    pub shared_secret: String,
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

/// Bug AV (AV-3-1) — FUSE write-path preference for first-write
/// chunk placement.
///
/// Two modes:
///
/// * **`"cold"`** (default): the FUSE client requests that the
///   manager place the first chunk on the cold (mandatory anchor)
///   tier. Write ACK waits for the cold replica to acknowledge —
///   safer, slower in DCs with slow uplinks to the anchor DC.
/// * **`"nearest-warm"`**: FUSE requests warm placement, ACKs the
///   write as soon as a warm replica confirms, and the
///   manager's anchor-lag-repair pass (AV-3-3) schedules a cold
///   copy asynchronously. Faster ACKs at the cost of a short
///   window where the chunk lives only on cache-class storage.
///
/// Any unrecognised string falls back to `"cold"` via
/// `effective_first_write_tier`; we deliberately do NOT reject
/// unknown values at parse time so a forward-compat addition
/// (e.g. a future `"nearest-hot"`) doesn't brick existing
/// clients during a rolling upgrade.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FuseWriteConfig {
    #[serde(default = "default_first_write_tier")]
    pub first_write_tier: String,
}

impl Default for FuseWriteConfig {
    fn default() -> Self {
        Self {
            first_write_tier: default_first_write_tier(),
        }
    }
}

/// Resolved write-tier preference. Returned by
/// `FuseWriteConfig::effective_first_write_tier`; consumers
/// (`crates/dtfs-fuse/src/filesystem.rs`'s allocate_chunks call)
/// match on this rather than parsing the raw string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstWriteTier {
    /// Pin the first chunk to the cold (mandatory) tier — slower
    /// ACK, no anchor-lag window.
    Cold,
    /// Place on nearest warm-tier node; anchor migration happens
    /// asynchronously via AV-3-3.
    NearestWarm,
}

impl FuseWriteConfig {
    /// Normalise the configured string to a `FirstWriteTier`.
    /// Any unrecognised value (typo, future tier name not yet
    /// supported by this client) falls back to `Cold` — the safe
    /// default that never breaks durability invariants.
    pub fn effective_first_write_tier(&self) -> FirstWriteTier {
        match self.first_write_tier.as_str() {
            "nearest-warm" => FirstWriteTier::NearestWarm,
            // "cold" + any other string — the safe fallback.
            _ => FirstWriteTier::Cold,
        }
    }
}

fn default_first_write_tier() -> String {
    "cold".to_string()
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
    // Workspace default — see the regression test
    // `workspace_default_chunk_size_is_4_mb`.
    //
    // Pre-Bug-I we used 128 MB to amortise the cost of allocating a full
    // chunk's worth of memory per put_chunk. Now that put_chunk streams
    // directly to disk (Bug I) and the memory budget is enforced (Bug G),
    // 4 MB is the better default:
    //
    //   * Faster migrations  : 4 MB at 1 Gbps ≈ 50 ms / chunk (vs 1 s)
    //   * Smaller retries    : a RESOURCE_EXHAUSTED retry redoes 4 MB
    //   * Plex friendliness  : 4 MB ≈ 1 second of 32 Mbps video — natural
    //                          unit for a streaming reader
    //   * Finer ARC cache    : 32× more independent entries per MB budget
    //   * Better hash ring   : more chunks → finer disk distribution
    //
    // Tradeoff is more Raft state per file and more `UpdateChunkLocations`
    // commits; both are modest at lab scale and the cluster handles them
    // comfortably.
    4
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

    /// The workspace default chunk size MUST be 4 MB. The original 128 MB
    /// choice predated Bug I's streaming put_chunk and was sized to
    /// amortise per-write allocation cost. With streaming-to-disk and
    /// Bug G's runtime memory cap, smaller chunks win on every axis that
    /// matters in the lab: faster migrations, granular retries on
    /// `RESOURCE_EXHAUSTED`, better Plex playback granularity, more
    /// independent ARC cache entries, and finer hash-ring distribution.
    ///
    /// Locking this here so a careless edit doesn't quietly revert
    /// freshly-deployed clusters back to the old default. We assert
    /// both the pure helper AND the serde default on `DefaultsConfig`.
    #[test]
    fn workspace_default_chunk_size_is_4_mb() {
        // 1. The pure helper.
        assert_eq!(default_chunk_size(), 4);

        // 2. Serde default through `DefaultsConfig` — when an operator
        //    writes an empty `[defaults]` block in manager.toml the
        //    serde-default kicks in.
        let cfg: DefaultsConfig = toml::from_str("").unwrap();
        assert_eq!(
            cfg.chunk_size_mb, 4,
            "DefaultsConfig.chunk_size_mb must default to 4; got {}",
            cfg.chunk_size_mb
        );
    }

    // ---------------------------------------------------------------
    // Bug AV (AV-3-1) — FuseWriteConfig parsing + safe fallback
    // ---------------------------------------------------------------

    /// AV-3-1 test 1 — when `fuse.toml` omits the `[write]` block
    /// entirely (legacy config), `serde(default)` produces a
    /// `FuseWriteConfig { first_write_tier: "cold" }` and
    /// `effective_first_write_tier()` returns `Cold`.
    #[test]
    fn bug_av_fuse_write_config_default() {
        let cfg: FuseWriteConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.first_write_tier, "cold");
        assert_eq!(
            cfg.effective_first_write_tier(),
            FirstWriteTier::Cold
        );
        // Also via the struct's Default impl (used by serde on the
        // parent FuseConfig).
        let dflt = FuseWriteConfig::default();
        assert_eq!(dflt.first_write_tier, "cold");
    }

    /// AV-3-1 test 2 — explicit `first_write_tier = "nearest-warm"`
    /// parses and resolves to `FirstWriteTier::NearestWarm`.
    #[test]
    fn bug_av_fuse_write_config_nearest_warm_parsed() {
        let cfg: FuseWriteConfig =
            toml::from_str("first_write_tier = \"nearest-warm\"").unwrap();
        assert_eq!(cfg.first_write_tier, "nearest-warm");
        assert_eq!(
            cfg.effective_first_write_tier(),
            FirstWriteTier::NearestWarm
        );
    }

    /// AV-3-1 test 3 — an unrecognised string falls back to `Cold`
    /// at use-time. We deliberately do NOT reject at parse time so
    /// a forward-compat addition (e.g. a future `"nearest-hot"`)
    /// doesn't brick an older client during a rolling upgrade —
    /// they just write cold instead.
    #[test]
    fn bug_av_fuse_write_config_invalid_value_falls_back_to_cold() {
        let cfg: FuseWriteConfig =
            toml::from_str("first_write_tier = \"banana\"").unwrap();
        assert_eq!(
            cfg.first_write_tier, "banana",
            "raw string preserved for diagnostic logs"
        );
        assert_eq!(
            cfg.effective_first_write_tier(),
            FirstWriteTier::Cold,
            "unknown values must fall back to the safe default"
        );
    }
}
