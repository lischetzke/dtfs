use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::topology::Location;
use crate::types::{DiskTier, TierReplication, WriteSafety};

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
    /// Bug BC — cluster-wide scrub + integrity policy. Propagated to
    /// data nodes via the heartbeat `ScrubConfigOverride`. Replaces the
    /// removed per-volume schedule override and the hardcoded constants
    /// previously in `metadata.rs`.
    #[serde(default)]
    pub scrub: ClusterScrubConfig,
    /// Bug BU — free-space headroom for capacity-aware placement. A disk
    /// whose free space is below the headroom is excluded from the
    /// placement ring so new chunks route to a disk that can hold them.
    #[serde(default)]
    pub placement: PlacementConfig,
    /// CF-5 — per-process RAM cap + pressure ladder. When `hard_cap_mb > 0`
    /// a background sampler watches RSS; Warn takes no manager action
    /// (only data/FUSE roles react there), and at the Shed level the
    /// scheduler skips its background ticks (re-replication / migration /
    /// rebalance, self-limiting via `MAX_CONSECUTIVE_PRESSURE_SKIPS` — see
    /// REV-3-5) so the manager gives RAM back instead of being
    /// OOM/cgroup-killed. `#[serde(default)]` so old config files keep
    /// working (cap 0 = unbounded, no ladder).
    #[serde(default)]
    pub memory: MemoryConfig,
}

/// Bug BU — capacity-aware placement headroom.
///
/// A disk is "full for placement" when its free space drops below
/// `max(min_free_bytes, total * min_free_pct%)`. Such a disk is excluded
/// from the placement ring (manager) and rejected by `put_chunk` (data)
/// so new writes route to a disk with room instead of hard-failing on
/// ENOSPC. Defaults reserve `max(1 GiB, 5%)` per disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementConfig {
    #[serde(default = "default_min_free_bytes")]
    pub min_free_bytes: u64,
    #[serde(default = "default_min_free_pct")]
    pub min_free_pct: f64,
    /// Bug BX — when the mandated tier is globally full, spill the chunk to
    /// the fastest+nearest tier that has room (audited as `placement.spill`,
    /// flagged misplaced, rebalanced back when capacity returns) instead of
    /// failing the write. The anchor copy is NEVER spilled out of its
    /// DC/tier. Default `true`: for the bursty download-ingest workload,
    /// accepting a degraded-placement write beats dropping it. Set `false`
    /// for a durability-first, fail-loud cluster.
    #[serde(default = "default_allow_capacity_spill")]
    pub allow_capacity_spill: bool,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self {
            min_free_bytes: default_min_free_bytes(),
            min_free_pct: default_min_free_pct(),
            allow_capacity_spill: default_allow_capacity_spill(),
        }
    }
}

impl PlacementConfig {
    /// Bytes that must stay free on a disk of the given total size.
    pub fn headroom_bytes(&self, total_bytes: u64) -> u64 {
        let pct = ((total_bytes as f64) * self.min_free_pct / 100.0) as u64;
        self.min_free_bytes.max(pct)
    }

    /// True when the disk has room for a new chunk: free space stays
    /// strictly above the headroom reserve. A `total_bytes == 0` disk
    /// (capacity not yet reported) is treated as having room so a node
    /// that hasn't run statvfs yet isn't spuriously excluded.
    pub fn has_room(&self, total_bytes: u64, used_bytes: u64) -> bool {
        if total_bytes == 0 {
            return true;
        }
        let free = total_bytes.saturating_sub(used_bytes);
        free > self.headroom_bytes(total_bytes)
    }

    /// CMN-8: `min_free_pct` outside `0.0..=100.0` (or NaN) silently
    /// breaks placement — `>100` makes `headroom_bytes` exceed the disk's
    /// total size (every disk excluded from the ring); negative/NaN
    /// saturate to 0 in `headroom_bytes`' `as u64` cast, disabling the
    /// reserve entirely. Called from `main.rs` right after config load.
    pub fn validate(&self) -> Result<(), String> {
        if !(0.0..=100.0).contains(&self.min_free_pct) {
            return Err(format!(
                "[placement] min_free_pct = {} must be within 0.0..=100.0",
                self.min_free_pct
            ));
        }
        Ok(())
    }
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
    /// Bug BU — free-space headroom; `put_chunk` rejects a write to a
    /// disk under headroom with a typed `RESOURCE_EXHAUSTED` so the
    /// client can reroute, rather than streaming until the kernel ENOSPCs.
    #[serde(default)]
    pub placement: PlacementConfig,
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

impl MemoryConfig {
    /// CO-5-20 — reject a nonsensical pressure ladder before the role
    /// daemons act on it. An inverted ladder (`warn >= shed`) makes the
    /// Warn rung unreachable: the cheap mitigations (shrink cache, pause
    /// background ticks) never fire and the last-resort shed (write
    /// reject, RESOURCE_EXHAUSTED) is the first thing the operator sees.
    /// Either threshold above 100% is meaningless (RSS is a percentage of
    /// the cap). Called by the role config `validate()` on load.
    pub fn validate(&self) -> Result<(), String> {
        if (1..128).contains(&self.hard_cap_mb) {
            return Err(format!(
                "[memory] hard_cap_mb = {} is below the 128 MB floor (0 = unbounded)",
                self.hard_cap_mb
            ));
        }
        if self.warn_threshold_pct == 0 {
            return Err("[memory] warn_threshold_pct must be > 0".to_string());
        }
        if self.warn_threshold_pct > 100 {
            return Err(format!(
                "[memory] warn_threshold_pct = {} exceeds 100",
                self.warn_threshold_pct
            ));
        }
        if self.shed_threshold_pct > 100 {
            return Err(format!(
                "[memory] shed_threshold_pct = {} exceeds 100",
                self.shed_threshold_pct
            ));
        }
        if self.warn_threshold_pct >= self.shed_threshold_pct {
            return Err(format!(
                "[memory] warn_threshold_pct ({}) must be below shed_threshold_pct ({}); \
                 an inverted ladder makes the Warn rung unreachable",
                self.warn_threshold_pct, self.shed_threshold_pct
            ));
        }
        Ok(())
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

/// AZ-5 — self-updater source config, consumed by `dtfs-cli update`.
///
/// Lives in dtfs-common so the defaults + parsing are tested once. The
/// CLI pulls just this block out of any role `.toml` via a small
/// `#[serde(default)]` extractor; the daemons do not read it (serde
/// silently ignores the unknown `[updater]` table), so it is
/// intentionally NOT a field on the manager/data/fuse config structs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdaterConfig {
    /// Repo base URL releases are fetched from. A GitLab project URL
    /// (`https://<host>/<group>/<project>`, the default) resolves to the
    /// Generic Package Registry
    /// `<host>/api/v4/projects/<group>%2F<project>/packages/generic/dtfs/<tag>/dtfs-linux-amd64.tar.gz`;
    /// a github.com URL keeps the legacy
    /// `<repo_url>/releases/{latest/download,download/<tag>}/dtfs-linux-amd64.tar.gz`.
    #[serde(default = "default_updater_repo_url")]
    pub repo_url: String,
    /// Directory the binaries are installed into (atomic `install -m 0755`).
    #[serde(default = "default_updater_install_dir")]
    pub install_dir: String,
    /// GitLab token for a private project: a deploy token (`gldt-...`,
    /// scope `read_package_registry`, sent as `DEPLOY-TOKEN`) or a
    /// project/personal access token (`glpat-...`, scope `read_api`, sent
    /// as `PRIVATE-TOKEN`). Empty = anonymous (public project / GitHub).
    /// Overridden by `--token` and the `DTFS_REPO_TOKEN` env var. Lives
    /// in the same 0640 file as `cluster.shared_secret`.
    #[serde(default)]
    pub token: String,
    /// Minisign public key (base64 line, or the full `.pub` file text) the
    /// release `SHA256SUMS` must be signed with. When set, `dtfs-cli update`
    /// also downloads `SHA256SUMS.minisig`, verifies it and checks the
    /// tarball against `SHA256SUMS`; any failure refuses the install.
    /// Overridden by the `DTFS_MINISIGN_PUBKEY` env var. Empty = unverified
    /// (the in-tarball MANIFEST is then the only check; a warning is printed).
    #[serde(default)]
    pub minisign_public_key: String,
}

impl Default for UpdaterConfig {
    fn default() -> Self {
        Self {
            repo_url: default_updater_repo_url(),
            install_dir: default_updater_install_dir(),
            token: String::new(),
            minisign_public_key: String::new(),
        }
    }
}

fn default_updater_repo_url() -> String {
    crate::REPO_URL.to_string()
}
fn default_updater_install_dir() -> String {
    "/usr/local/bin".to_string()
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
    /// CF-5 — per-process RAM cap + pressure ladder. When `hard_cap_mb > 0`
    /// a background sampler watches RSS and, at the Warn level, halves the
    /// in-memory readahead/prefetch cache budget; at Shed it drops all
    /// unconsumed prefetch so a RAM-starved client stops growing instead of
    /// being OOM/cgroup-killed. `#[serde(default)]` so a legacy `fuse.toml`
    /// keeps working (cap 0 = unbounded, no ladder).
    #[serde(default)]
    pub memory: MemoryConfig,
    /// U4 read-locality — ordered datacenter/host read preference. When set,
    /// reads pick the first replica whose location (datacenter OR host)
    /// matches the earliest-listed entry, taking precedence over raw physical
    /// distance (the operator's explicit intent, e.g. a TORO client trying
    /// DC1 then DC3). Entries match case-insensitively. Empty
    /// (the default) = current distance-only behaviour. `#[serde(default)]`
    /// so a legacy `fuse.toml` keeps working.
    #[serde(default)]
    pub read_location_priority: Vec<String>,
    /// CF-6 — gRPC transport security toward managers and data nodes.
    /// `#[serde(default)]` (mode "auto", no certs = plaintext) keeps a
    /// legacy `fuse.toml` working unchanged.
    #[serde(default)]
    pub tls: TlsConfig,
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
    /// LIS-20 / Sec R3 — lab-only escape hatch. When `false` (default), an
    /// empty `shared_secret` is refused at manager startup and RegisterNode
    /// never fail-opens. Set `true` only for intentional insecure lab runs.
    #[serde(default)]
    pub allow_insecure_empty_secret: bool,
    /// Raft transport auth: when `true`, a Raft RPC without a valid
    /// `x-dtfs-rpc-*` stamp is rejected. When `false` (default, rolling-update
    /// safe) unsigned RPCs are accepted with a rate-limited warning; stamped
    /// but invalid RPCs are always rejected. Set `true` once every manager
    /// runs a build that signs Raft RPCs.
    #[serde(default)]
    pub require_raft_auth: bool,
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

impl ClusterConfig {
    /// LIS-20 / Sec R3 — refuse empty `shared_secret` unless the operator
    /// explicitly opts into insecure lab mode.
    pub fn validate_shared_secret(&self) -> Result<(), String> {
        if self.shared_secret.is_empty() && !self.allow_insecure_empty_secret {
            return Err(
                "cluster.shared_secret is empty; set a non-empty secret, or set \
                 cluster.allow_insecure_empty_secret = true for lab-only insecure mode"
                    .into(),
            );
        }
        Ok(())
    }

    /// Whether a provided RegisterNode / join secret is acceptable.
    ///
    /// Non-empty cluster secret: constant-time equality check (missing
    /// provided → reject). Empty cluster secret: only when
    /// `allow_insecure_empty_secret` was set at load (lab fail-open).
    pub fn accepts_shared_secret(&self, provided: Option<&str>) -> bool {
        if self.shared_secret.is_empty() {
            return self.allow_insecure_empty_secret;
        }
        match provided {
            Some(s) => crate::crypto::constant_time_eq(s.as_bytes(), self.shared_secret.as_bytes()),
            None => false,
        }
    }
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
    /// LIS-55 / LIS-20 parity — lab-only escape hatch. When `true`, an empty
    /// `shared_secret` is permitted at data-node startup (auth gates skip).
    /// Never enable in production.
    #[serde(default)]
    pub allow_insecure_empty_secret: bool,
}

impl DataClusterConfig {
    /// LIS-55 / LIS-20 — refuse empty `shared_secret` unless the operator
    /// explicitly opts into insecure lab mode.
    pub fn validate_shared_secret(&self) -> Result<(), String> {
        if self.shared_secret.is_empty() && !self.allow_insecure_empty_secret {
            return Err(
                "cluster.shared_secret is empty; set a non-empty secret, or set \
                 cluster.allow_insecure_empty_secret = true for lab-only insecure mode"
                    .into(),
            );
        }
        Ok(())
    }

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
    // `listen_addr` was removed: the Raft transport is served on
    // `grpc.listen_addr`. Old configs that still set it keep parsing
    // (unknown keys are ignored; see `removed_unread_keys_still_parse`).
    #[serde(default = "default_election_timeout")]
    pub election_timeout_ms: u64,
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_snapshot_threshold")]
    pub snapshot_threshold: u64,
    /// Bug BP-1 — RocksDB info-LOG retention for the raft log store.
    /// Unbounded by default in RocksDB, the `LOG.old.*` info logs grew to
    /// ~6.8 GB and filled a 16 GB manager partition (ISSUE-SCAN-2026-06-02
    /// F5). Keep only the most recent N.
    #[serde(default = "default_rocksdb_keep_log_file_num")]
    pub rocksdb_keep_log_file_num: u64,
    /// Bug BP-1 — max RocksDB MANIFEST size before it rotates. Unbounded
    /// by default, the MANIFEST grew to ~207 MB. Rotating bounds it.
    #[serde(default = "default_rocksdb_max_manifest_bytes")]
    pub rocksdb_max_manifest_bytes: u64,
    /// CNF-3 — RocksDB memtable (write buffer) size per column family for
    /// the raft log store, in MiB. Bounds write-side memory.
    #[serde(default = "default_rocksdb_write_buffer_mb")]
    pub rocksdb_write_buffer_mb: u64,
    /// CNF-3 — RocksDB block-cache size for the raft log store, in MiB.
    /// One LRU cache shared across both column families.
    #[serde(default = "default_rocksdb_block_cache_mb")]
    pub rocksdb_block_cache_mb: u64,
    /// Bug BP-1 — how many committed raft-log entries openraft keeps after
    /// a snapshot. A large/implicit-default value let ~10 000 tiny `.sst`
    /// files accumulate, slowing RocksDB open to minutes. Bounded purge
    /// keeps the on-disk log small.
    #[serde(default = "default_max_in_snapshot_log_to_keep")]
    pub max_in_snapshot_log_to_keep: u64,
    /// Bug BP-3 — liveness window (seconds). A node whose `last_seen_ms` is
    /// older than this is projected `Suspect` (and `Down` past 4×) in the
    /// read API regardless of its stored `Healthy`, so a frozen heartbeat
    /// (raft degraded) cannot keep reporting a dead node as Healthy
    /// (ISSUE-SCAN-2026-06-02 F4). Read-path only — no raft write needed.
    #[serde(default = "default_node_liveness_secs")]
    pub node_liveness_secs: u64,
    /// Bug CN (CNF-2) — how long a `Failed` migration task lingers in
    /// `migration_state_json` before the scheduler's terminal-task prune
    /// drops it. Long enough for an operator alerted on `migration.failed`
    /// to inspect the task, short enough that a mass-failure event (the
    /// 2026-06-10 flood aftermath) doesn't bloat the Raft snapshot for
    /// hours. Configurable so the release suite can exercise the prune
    /// without waiting the production 30 minutes.
    #[serde(default = "default_failed_task_retention_secs")]
    pub failed_task_retention_secs: u64,
    /// Bug CN (CNF-2) — how long a `Done` migration task lingers before
    /// the prune. Kept short: the new replica is already visible in the
    /// file's chunk list and the historical task entry is operationally
    /// uninteresting beyond a few minutes.
    #[serde(default = "default_done_task_retention_secs")]
    pub done_task_retention_secs: u64,
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

impl GrpcConfig {
    /// CMN-3 — reject an unusable message-size cap at startup instead of
    /// silently falling back to tonic's 4 MB default.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_message_size_mb == 0 {
            return Err("[grpc] max_message_size_mb must be > 0".to_string());
        }
        Ok(())
    }
}

/// CMN-3 — pure conversion from a configured MB cap to the byte count
/// tonic's `max_decoding_message_size`/`max_encoding_message_size` want.
/// Takes the raw `u32` (rather than `&GrpcConfig`) so it also serves
/// `FuseClientConfig::max_message_size_mb`, which has no `[grpc]` block of
/// its own.
pub fn max_message_bytes(max_message_size_mb: u32) -> usize {
    max_message_size_mb as usize * 1024 * 1024
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
    // `cors_origins` was removed: no CORS layer exists. Old configs that
    // still set it keep parsing.
}

/// CF-6 — gRPC transport security. Resolved by
/// `dtfs_common::tls::TlsContext::from_config`: `disabled` = plaintext;
/// `require` = the three files must load or startup fails; `auto` (default)
/// = mTLS when all three files are configured AND exist on disk, plaintext
/// otherwise (back-compat for cert-less deployments).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default = "default_tls_mode")]
    pub mode: String,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub ca_file: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            mode: default_tls_mode(),
            cert_file: None,
            key_file: None,
            ca_file: None,
        }
    }
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
    /// Bug AV (AV-4-5) — opt-in for the cache-tier redundancy
    /// escalation pass. When `false` (default) the cache planner
    /// caps each `(chunk, dc, tier)` group at `cache_replicas.<t>.min`
    /// replicas. When `true` the planner may ramp toward
    /// `cache_replicas.<t>.max` once the read count is at least
    /// `2 * promote_reads` — useful for per-DC HA when warm capacity
    /// exists. Defaults to `false` so legacy configs keep current
    /// behaviour.
    #[serde(default)]
    pub enable_cache_redundancy_escalation: bool,
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

impl TieringConfig {
    /// `tokio::time::interval(ZERO)` panics inside the spawned tiering task.
    pub fn validate(&self) -> Result<(), String> {
        nonzero("[tiering] evaluation_interval_secs", self.evaluation_interval_secs)
    }
}

/// Floor for `[scrub] orphan_grace_secs`: the orphan reaper must not race
/// the commit-to-raft-apply window of an in-flight write.
pub const MIN_ORPHAN_GRACE_SECS: u64 = 6 * 3600;

impl ClusterScrubConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.orphan_grace_secs < MIN_ORPHAN_GRACE_SECS {
            return Err(format!(
                "[scrub] orphan_grace_secs ({}) must be >= {} (6 h)",
                self.orphan_grace_secs, MIN_ORPHAN_GRACE_SECS
            ));
        }
        Ok(())
    }
}

impl MonitoringConfig {
    pub fn validate(&self) -> Result<(), String> {
        nonzero("[monitoring] heartbeat_interval_secs", self.heartbeat_interval_secs)?;
        nonzero("[monitoring] suspect_after_missed", self.suspect_after_missed as u64)?;
        nonzero("[monitoring] down_after_missed", self.down_after_missed as u64)
    }
}

impl RaftConfig {
    pub fn validate_timings(&self) -> Result<(), String> {
        nonzero("[raft] election_timeout_ms", self.election_timeout_ms)?;
        nonzero("[raft] heartbeat_interval_ms", self.heartbeat_interval_ms)?;
        if self.heartbeat_interval_ms >= self.election_timeout_ms {
            return Err(format!(
                "[raft] heartbeat_interval_ms ({}) must be below election_timeout_ms ({})",
                self.heartbeat_interval_ms, self.election_timeout_ms
            ));
        }
        Ok(())
    }
}

impl ManagerConfig {
    /// Interval/timeout checks for every block with a periodic task.
    /// Call from manager `main.rs` next to `memory.validate()`.
    pub fn validate_intervals(&self) -> Result<(), String> {
        self.tiering.validate()?;
        self.monitoring.validate()?;
        self.scrub.validate()?;
        self.raft.validate_timings()
    }
}

impl FuseConfig {
    /// Call from fuse `main.rs` next to `client.validate()`.
    pub fn validate_intervals(&self) -> Result<(), String> {
        nonzero("[cache] prune_interval_secs", self.cache.prune_interval_secs)?;
        nonzero(
            "[partition_safety] quorum_loss_timeout_secs",
            self.partition_safety.quorum_loss_timeout_secs,
        )
    }
}

fn nonzero(name: &str, v: u64) -> Result<(), String> {
    if v == 0 {
        Err(format!("{name} must be > 0"))
    } else {
        Ok(())
    }
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScrubConfig {
    #[serde(default = "default_quick_interval")]
    pub quick_interval_hours: u64,
    #[serde(default = "default_deep_interval")]
    pub deep_interval_hours: u64,
    /// CR-6-3 (PROTO-2) — exact-seconds companion to `quick_interval_hours`,
    /// populated by the manager's heartbeat `ScrubConfigOverride` (proto
    /// fields 9/10) on a build that sends them; `0` means "not set", so
    /// the scheduler falls back to `quick_interval_hours * 3600` (the
    /// pre-existing ceil-to-1h behaviour). Not meant to be hand-set in
    /// `data.toml` -- the manager's `[scrub]` cluster policy is the
    /// source of truth once the manager sends it.
    #[serde(default)]
    pub quick_interval_secs: u64,
    /// CR-6-3 (PROTO-2) — exact-seconds companion to `deep_interval_hours`.
    /// See `quick_interval_secs`.
    #[serde(default)]
    pub deep_interval_secs: u64,
    #[serde(default = "default_scrub_io")]
    pub max_io_percent: u32,
    /// Bug BC — data-side mirror of `ClusterScrubConfig.verify_on_read`.
    /// Defaults `true` so a data node starting before the first
    /// heartbeat still enforces hash-on-read.
    #[serde(default = "default_cluster_verify_on_read")]
    pub verify_on_read: bool,
    /// Bug BC — data-side mirror of `ClusterScrubConfig.quarantine_on_fault`.
    /// Defaults `true` to match the single-fault → full ban invariant.
    #[serde(default = "default_cluster_quarantine_on_fault")]
    pub quarantine_on_fault: bool,
}

/// Bug BC — cluster-wide scrub + integrity policy.
///
/// Lives on `ManagerConfig.scrub`. Heartbeat propagates the
/// resolved values to every data node, where they replace the local
/// `DataNodeConfig.scrub` block at runtime (the data-side block is
/// kept only as a startup fallback before the first heartbeat
/// arrives).
///
/// Defaults match the data-side defaults so a fresh cluster on
/// pre-BC binaries behaves identically (6h quick, 168h deep).
///
/// The integrity knobs (`verify_on_read`, `quarantine_on_fault`,
/// `auto_unquarantine_secs`) are wired by Bug BC tasks BC-8/10/12.
/// Disabling `verify_on_read` is gated by an explicit
/// acknowledgement at the API boundary (see BC-8 boundary check).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterScrubConfig {
    /// Quick scrub cadence (stat + size + meta presence). Default
    /// `default_cluster_quick_interval` = 21600 (6 h).
    #[serde(default = "default_cluster_quick_interval")]
    pub quick_interval_secs: u64,

    /// Deep scrub cadence (full BLAKE3 verify). Default
    /// `default_cluster_deep_interval` = 604800 (7 d).
    #[serde(default = "default_cluster_deep_interval")]
    pub deep_interval_secs: u64,

    /// Orphan-scrub cadence: manager-driven cross-check between data
    /// node `ListChunks` and state-machine `chunk_locations`. Default
    /// `default_cluster_orphan_interval` = 86400 (1 d).
    #[serde(default = "default_cluster_orphan_interval")]
    pub orphan_interval_secs: u64,

    /// Grace period before an orphan candidate is reaped. Catches
    /// in-flight unlink/re-promote races. Default
    /// `default_cluster_orphan_grace` = 86400 (1 d).
    #[serde(default = "default_cluster_orphan_grace")]
    pub orphan_grace_secs: u64,

    /// Throttle for deep scrub I/O — fraction of disk bandwidth the
    /// scrubber may use. Default 10.
    #[serde(default = "default_cluster_scrub_io")]
    pub max_io_percent: u32,

    /// Data consistency knob: hash-verify every read at the data
    /// node before bytes leave the process. Default **true**.
    /// Operator can set to `false` only with explicit ack at the
    /// API boundary (BC-8). Disabling is a footgun; CLAUDE.md flags
    /// the false branch as the only DTFS-supported mode where
    /// reads can deliver corrupt bytes.
    #[serde(default = "default_cluster_verify_on_read")]
    pub verify_on_read: bool,

    /// If true (default), a single integrity fault (quick mismatch,
    /// deep mismatch, verify-on-read mismatch, in-state-missing)
    /// quarantines the faulting node immediately. No threshold,
    /// no debounce. Leave true unless you know what you do.
    #[serde(default = "default_cluster_quarantine_on_fault")]
    pub quarantine_on_fault: bool,

    /// Cooldown after which a Quarantined node auto-clears if no
    /// fresh fault has arrived. 0 = manual only (default, matches
    /// data-consistency-first stance).
    #[serde(default)]
    pub auto_unquarantine_secs: u64,
}

impl Default for ClusterScrubConfig {
    fn default() -> Self {
        Self {
            quick_interval_secs:    default_cluster_quick_interval(),
            deep_interval_secs:     default_cluster_deep_interval(),
            orphan_interval_secs:   default_cluster_orphan_interval(),
            orphan_grace_secs:      default_cluster_orphan_grace(),
            max_io_percent:         default_cluster_scrub_io(),
            verify_on_read:         default_cluster_verify_on_read(),
            quarantine_on_fault:    default_cluster_quarantine_on_fault(),
            auto_unquarantine_secs: 0,
        }
    }
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
    /// Bug BQ-1 — periodic client keepalive interval (seconds). An idle
    /// mounted FUSE client re-registers on this cadence so its
    /// `last_seen_ms` stays fresh and `effective_health` (BP-3) does not
    /// project it `Down`. Must be well under `raft.node_liveness_secs`
    /// (default 30) or the node flaps `Down` between beats. `0` disables
    /// the keepalive (legacy register-once behavior).
    #[serde(default = "default_client_heartbeat_secs")]
    pub heartbeat_interval_secs: u64,
    /// Bug BW — chunk size (MB) the FUSE write buffer splits files into.
    /// MUST match the volume's `chunk_size_bytes` (manager
    /// `defaults.chunk_size_mb`, default 4 MB) — the manager records the
    /// volume chunk size in file metadata and reads compute chunk offsets
    /// from it, so a mismatch corrupts multi-chunk reads. Previously the
    /// client hardcoded 128 MB, so a >4 MB file was stored as one oversized
    /// chunk while metadata said 4 MB, and reads past the first 4 MB
    /// returned garbage. Default 4 to match the workspace volume default.
    #[serde(default = "default_client_chunk_size_mb")]
    pub chunk_size_mb: u32,
    /// FUSE-HOL — per-replica chunk-fetch timeout (seconds). A `read_chunk`
    /// that does not complete within this is abandoned and the read fails over
    /// to the next replica (extends the P0-2 fallback to cover a hung/dead node
    /// that never returns an error, not just transport errors). Without it a
    /// hung data node stalls the read — and the application's FUSE op —
    /// indefinitely. Generous default (60 s) vs a 4 MB chunk at 40 Mbps
    /// (~0.8 s); raise for very large chunks on very slow links. `0` disables
    /// the timeout (legacy unbounded behavior).
    #[serde(default = "default_chunk_read_timeout_secs")]
    pub chunk_read_timeout_secs: u64,
    /// CMN-3 — max gRPC message size (MB) for the client's manager and
    /// data-node channels. Mirrors `GrpcConfig::max_message_size_mb`
    /// (manager/data servers); the client has no `[grpc]` block of its own
    /// so this lives directly on `[client]`. `#[serde(default)]` so a
    /// legacy `fuse.toml` keeps working.
    #[serde(default = "default_max_message_size")]
    pub max_message_size_mb: u32,
}

impl FuseClientConfig {
    /// CMN-3 — reject an unusable message-size cap at startup instead of
    /// silently falling back to tonic's 4 MB default.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_message_size_mb == 0 {
            return Err("[client] max_message_size_mb must be > 0".to_string());
        }
        Ok(())
    }

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
    /// Bug CE — soft byte budget (MB) for the in-memory prefetch cache. The
    /// cache evicts to stay under this regardless of chunk size, so a
    /// whole-file read on a RAM-starved client cannot OOM FUSE. Was an
    /// unbounded 256-entry count cap.
    #[serde(default = "default_readahead_cache_mb")]
    pub cache_mb: u64,
    /// WI-05 — maximum simultaneous prefetch `GetChunk` streams. The
    /// prefetch WINDOW stays `prefetch_chunks` deep; this bounds how many
    /// of those fetches are on the wire at once. On a 40 Mbps cross-DC path
    /// 8 concurrent 4 MB streams delay the nearest chunk about 8x (about
    /// 6.4 s instead of about 0.8 s) and hold `prefetch_chunks * chunk_size`
    /// in heap at once. `0` degrades to serial prefetch (`.max(1)`), never
    /// "unbounded".
    #[serde(default = "default_prefetch_max_inflight")]
    pub prefetch_max_inflight: u32,
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
    /// WI-01 — number of complete, chunk-aligned chunks the write buffer
    /// holds back before flushing to the cluster, amortizing the manager
    /// `AllocateChunks` RPC + `CommitChunkLocations` Raft write over a batch
    /// instead of firing once per chunk. Clamped into `1..=32` by
    /// `WriteBuffer::new_with_batch`; `1` reproduces the pre-WI-01
    /// per-chunk-flush behaviour exactly (the rollback value). WI-01 / H1 —
    /// the EFFECTIVE batch is additionally clamped to
    /// `min(configured, 64 MB / chunk_size_mb)`
    /// (`writebuf::effective_batch_chunks`, `FLUSH_BATCH_MAX_BYTES` = 64 MB),
    /// so the un-committed ceiling per open write is AT MOST 64 MB regardless
    /// of `chunk_size_mb` (default 4 * 4 MB = 16 MB), and a volume with
    /// chunks of 64 MB or more flushes per chunk — see CLAUDE.md A.3.2.
    #[serde(default = "default_flush_batch_chunks")]
    pub flush_batch_chunks: u32,
}

impl Default for FuseWriteConfig {
    fn default() -> Self {
        Self {
            first_write_tier: default_first_write_tier(),
            flush_batch_chunks: default_flush_batch_chunks(),
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

fn default_flush_batch_chunks() -> u32 {
    4
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
    /// Validated non-zero at startup but not acted on at runtime.
    #[serde(default = "default_quorum_timeout")]
    pub quorum_loss_timeout_secs: u64,
}

// --- Default value functions ---

fn default_auto() -> String {
    "auto".to_string()
}
fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/dtfs")
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
fn default_rocksdb_keep_log_file_num() -> u64 {
    4
}
fn default_rocksdb_max_manifest_bytes() -> u64 {
    64 * 1024 * 1024
}
fn default_rocksdb_write_buffer_mb() -> u64 {
    64
}
fn default_rocksdb_block_cache_mb() -> u64 {
    64
}
fn default_max_in_snapshot_log_to_keep() -> u64 {
    1000
}
fn default_failed_task_retention_secs() -> u64 {
    // Matches the pre-CNF-2 hardcoded FAILED_TASK_RETENTION_MS (30 min).
    30 * 60
}
fn default_done_task_retention_secs() -> u64 {
    // Matches the pre-CNF-2 hardcoded DONE_TASK_RETENTION_MS (5 min).
    5 * 60
}
fn default_node_liveness_secs() -> u64 {
    // ~6 missed 5 s heartbeats. Long enough to ride out a brief GC pause or
    // leader election, short enough that a truly dead node is flagged fast.
    30
}
/// CMN-3 — shared MB default for every gRPC message-size cap (manager/data
/// `[grpc]`, fuse `[client]`). `pub` so `dtfs-client` can seed
/// `DtfsClient`'s in-memory default without duplicating the literal.
pub fn default_max_message_size() -> u32 {
    256
}
fn default_api_listen() -> String {
    "0.0.0.0:9402".to_string()
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
fn default_min_free_bytes() -> u64 {
    1024 * 1024 * 1024 // 1 GiB
}
fn default_min_free_pct() -> f64 {
    5.0
}
fn default_allow_capacity_spill() -> bool {
    // Bug BX — availability-first default: never drop a write because the
    // mandated tier is momentarily full; spill + reconcile. Anchor exempt.
    true
}
fn default_client_chunk_size_mb() -> u32 {
    // Bug BW — match the workspace volume default (`default_chunk_size` = 4 MB)
    // so client chunking agrees with the chunk size recorded in file metadata.
    4
}
fn default_chunk_read_timeout_secs() -> u64 {
    // FUSE-HOL — generous vs a 4 MB chunk at 40 Mbps (~0.8 s) but bounds a
    // hung/dead replica so a read fails over instead of hanging forever.
    60
}
fn default_client_heartbeat_secs() -> u64 {
    // Bug BQ-1 — well under default_node_liveness_secs (30) so an idle
    // client re-registers ~3x per liveness window and never flaps Down.
    10
}
fn default_suspect_after() -> u32 {
    3
}
fn default_down_after() -> u32 {
    6
}
fn default_quick_interval() -> u64 {
    6
}
fn default_deep_interval() -> u64 {
    168
}

// Bug BC — defaults for `ClusterScrubConfig`. Hour-equivalents of the
// legacy data-side defaults so an upgraded cluster behaves identically.
fn default_cluster_quick_interval() -> u64 {
    6 * 3600
}
fn default_cluster_deep_interval() -> u64 {
    168 * 3600
}
fn default_cluster_orphan_interval() -> u64 {
    24 * 3600
}
fn default_cluster_orphan_grace() -> u64 {
    24 * 3600
}
fn default_cluster_scrub_io() -> u32 {
    10
}
fn default_cluster_verify_on_read() -> bool {
    true
}
fn default_cluster_quarantine_on_fault() -> bool {
    true
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
fn default_prefetch_max_inflight() -> u32 {
    2
}
fn default_readahead_cache_mb() -> u64 {
    64
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

    // ---------------------------------------------------------------
    // deploy/examples/*.toml parse guard
    //
    // The example configs are the operator-facing canonical reference
    // (CLAUDE.md § B.6): every option, optional ones commented out.
    // They MUST parse against the current structs — if a config struct
    // gains a required field or renames a section, these tests fail
    // until the matching example (and docs) are updated in the same
    // commit.
    // ---------------------------------------------------------------

    fn example_path(name: &str) -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../deploy/examples")
            .join(name)
    }

    #[test]
    fn example_manager_toml_parses() {
        let cfg: ManagerConfig = load_config(&example_path("manager.toml"))
            .expect("deploy/examples/manager.toml must parse against ManagerConfig");
        assert_eq!(cfg.node.hostname, "dtfs-example-mgr01");
        assert_eq!(cfg.cluster.initial_voters.len(), 3);
        // The example's self-entry must satisfy the Bug AC identity
        // contract it documents: [node].hostname appears in initial_voters.
        assert!(
            cfg.cluster.initial_voters.iter().any(|v| v.hostname == cfg.node.hostname),
            "example must model the hostname<->initial_voters match"
        );
        // CF-5 — the commented-out [memory] block leaves the role on the
        // unbounded default (cap 0, no ladder).
        assert_eq!(cfg.memory.hard_cap_mb, 0);
        assert_eq!(cfg.memory.warn_threshold_pct, default_warn_pct());
        assert_eq!(cfg.memory.shed_threshold_pct, default_shed_pct());
        // CNF-2 — the commented-out retention options keep the pre-CNF-2
        // hardcoded behaviour (Failed 30 min, Done 5 min).
        assert_eq!(cfg.raft.failed_task_retention_secs, 30 * 60);
        assert_eq!(cfg.raft.done_task_retention_secs, 5 * 60);
        // LIS-20 — example ships a non-empty secret; escape hatch off.
        assert!(!cfg.cluster.shared_secret.is_empty());
        assert!(!cfg.cluster.allow_insecure_empty_secret);
        cfg.cluster
            .validate_shared_secret()
            .expect("example manager.toml must pass shared_secret validate");
    }

    /// LIS-20 / Sec R3 — empty shared_secret without the escape hatch is
    /// refused (fail-closed). Silent admit of any RegisterNode is the bug.
    #[test]
    fn lis20_empty_shared_secret_rejected_by_default() {
        let cfg = ClusterConfig {
            name: "t".into(),
            shared_secret: String::new(),
            allow_insecure_empty_secret: false,
            require_raft_auth: false,
            bootstrap: false,
            initial_voters: vec![],
        };
        let err = cfg.validate_shared_secret().expect_err("empty must fail");
        assert!(
            err.contains("shared_secret is empty"),
            "unexpected err: {err}"
        );
        assert!(
            !cfg.accepts_shared_secret(None),
            "RegisterNode must reject when secret unset"
        );
        assert!(
            !cfg.accepts_shared_secret(Some("anything")),
            "RegisterNode must reject any provided secret when unset"
        );
    }

    /// LIS-20 — explicit lab escape hatch allows empty secret; RegisterNode
    /// then fail-opens by design (operator opted in).
    #[test]
    fn lis20_allow_insecure_empty_secret_escape_hatch() {
        let cfg = ClusterConfig {
            name: "t".into(),
            shared_secret: String::new(),
            allow_insecure_empty_secret: true,
            require_raft_auth: false,
            bootstrap: false,
            initial_voters: vec![],
        };
        cfg.validate_shared_secret()
            .expect("escape hatch must allow empty");
        assert!(cfg.accepts_shared_secret(None));
        assert!(cfg.accepts_shared_secret(Some("x")));
    }

    /// LIS-20 — non-empty secret still requires an exact match.
    #[test]
    fn lis20_nonempty_secret_requires_match() {
        let cfg = ClusterConfig {
            name: "t".into(),
            shared_secret: "s3cret".into(),
            allow_insecure_empty_secret: false,
            require_raft_auth: false,
            bootstrap: false,
            initial_voters: vec![],
        };
        cfg.validate_shared_secret().unwrap();
        assert!(cfg.accepts_shared_secret(Some("s3cret")));
        assert!(!cfg.accepts_shared_secret(Some("wrong")));
        assert!(!cfg.accepts_shared_secret(None));
    }

    #[test]
    fn example_data_toml_parses() {
        let cfg: DataNodeConfig = load_config(&example_path("data.toml"))
            .expect("deploy/examples/data.toml must parse against DataNodeConfig");
        assert_eq!(cfg.node.hostname, "dtfs-example-data01");
        assert!(!cfg.disks.is_empty());
        // LIS-55 / LIS-20 — example ships a non-empty secret; escape hatch off.
        assert!(!cfg.cluster.shared_secret.is_empty());
        assert!(!cfg.cluster.allow_insecure_empty_secret);
        cfg.cluster
            .validate_shared_secret()
            .expect("example data.toml must pass shared_secret validate");
    }

    /// LIS-55 — data-node empty shared_secret refused without escape hatch.
    #[test]
    fn lis55_data_empty_shared_secret_rejected_by_default() {
        let cfg = DataClusterConfig {
            manager_addr: "mgr:9401".into(),
            shared_secret: String::new(),
            manager_addrs: vec![],
            allow_insecure_empty_secret: false,
        };
        let err = cfg.validate_shared_secret().expect_err("empty must fail");
        assert!(
            err.contains("shared_secret is empty"),
            "unexpected err: {err}"
        );
    }

    /// LIS-55 — data-node lab escape hatch allows empty secret.
    #[test]
    fn lis55_data_allow_insecure_empty_secret_escape_hatch() {
        let cfg = DataClusterConfig {
            manager_addr: "mgr:9401".into(),
            shared_secret: String::new(),
            manager_addrs: vec![],
            allow_insecure_empty_secret: true,
        };
        cfg.validate_shared_secret()
            .expect("escape hatch must allow empty");
    }

    #[test]
    fn example_fuse_toml_parses() {
        let cfg: FuseConfig = load_config(&example_path("fuse.toml"))
            .expect("deploy/examples/fuse.toml must parse against FuseConfig");
        // The client identity is location.host and must model the "_cln"
        // uniqueness convention the file documents.
        assert!(cfg.location.host.ends_with("_cln"));
        // CF-5 — the commented-out [memory] block leaves the FUSE client on
        // the unbounded default (cap 0, no ladder).
        assert_eq!(cfg.memory.hard_cap_mb, 0);
        assert_eq!(cfg.memory.warn_threshold_pct, default_warn_pct());
        assert_eq!(cfg.memory.shed_threshold_pct, default_shed_pct());
        // U4 — the commented-out read_location_priority leaves reads on the
        // distance-only default (empty list).
        assert!(cfg.read_location_priority.is_empty());
    }

    /// U4 — an explicit read_location_priority list parses and overrides the
    /// empty default. Gates the FuseConfig field + serde(default) plumbing.
    #[test]
    fn u4_read_location_priority_parses() {
        let cfg: FuseConfig = toml::from_str(
            "read_location_priority = [\"dc1\", \"dc3\"]\n\
             [client]\nmanager_addr = \"10.0.0.1:9401\"\n\
             [location]\nregion = \"eu\"\ndatacenter = \"toro\"\nrack = \"r1\"\nhost = \"c_cln\"\n\
             [mount]\nmountpoint = \"/mnt/dtfs\"\n\
             [readahead]\n[cache]\n[partition_safety]\n",
        )
        .expect("read_location_priority must parse");
        assert_eq!(cfg.read_location_priority, vec!["dc1", "dc3"]);
    }

    /// CF-5 — an explicit `[memory]` block on the manager/fuse roles parses
    /// and overrides the unbounded default, mirroring the data-node block.
    /// `MemoryConfig` is shared across all three roles (one struct, one
    /// parser) so a per-role parse test would be redundant; we assert the
    /// shared override path + the absent-block default path here.
    #[test]
    fn cf5_memory_block_parses_and_overrides() {
        // Absent block → unbounded default.
        let empty: MemoryConfig = toml::from_str("").unwrap();
        assert_eq!(empty.hard_cap_mb, 0);
        assert_eq!(empty.warn_threshold_pct, default_warn_pct());
        assert_eq!(empty.shed_threshold_pct, default_shed_pct());
        assert_eq!(empty.sample_interval_secs, default_memory_sample_secs());

        // Explicit block → operator values win.
        let cfg: MemoryConfig = toml::from_str(
            "hard_cap_mb = 1500\nwarn_threshold_pct = 70\nshed_threshold_pct = 90\nsample_interval_secs = 3\n",
        )
        .unwrap();
        assert_eq!(cfg.hard_cap_mb, 1500);
        assert_eq!(cfg.warn_threshold_pct, 70);
        assert_eq!(cfg.shed_threshold_pct, 90);
        assert_eq!(cfg.sample_interval_secs, 3);
    }

    /// CO-5-20 — `MemoryConfig::validate` rejects an inverted/over-100
    /// pressure ladder and accepts the compiled-in default.
    #[test]
    fn co_5_20_memory_config_validate() {
        // Default ladder (warn 80, shed 92) is valid.
        assert!(MemoryConfig::default().validate().is_ok());

        // Inverted ladder: warn above shed → Warn rung unreachable.
        let inverted = MemoryConfig {
            warn_threshold_pct: 90,
            shed_threshold_pct: 80,
            ..Default::default()
        };
        assert!(inverted.validate().is_err());

        // Equal thresholds are also rejected (warn must be strictly below).
        let equal = MemoryConfig {
            warn_threshold_pct: 85,
            shed_threshold_pct: 85,
            ..Default::default()
        };
        assert!(equal.validate().is_err());

        // A threshold above 100% is meaningless.
        let over = MemoryConfig {
            warn_threshold_pct: 70,
            shed_threshold_pct: 120,
            ..Default::default()
        };
        assert!(over.validate().is_err());
    }

    #[test]
    fn memory_validate_floor_and_zero_warn() {
        let low = MemoryConfig { hard_cap_mb: 127, ..Default::default() };
        assert!(low.validate().unwrap_err().contains("128 MB floor"));
        assert!(MemoryConfig { hard_cap_mb: 128, ..Default::default() }.validate().is_ok());
        assert!(MemoryConfig { hard_cap_mb: 0, ..Default::default() }.validate().is_ok());
        let zero = MemoryConfig { warn_threshold_pct: 0, ..Default::default() };
        assert_eq!(zero.validate().unwrap_err(), "[memory] warn_threshold_pct must be > 0");
    }

    #[test]
    fn removed_unread_keys_still_parse() {
        // raft.listen_addr and api.cors_origins were never read; configs
        // written before their removal must keep loading.
        let r: RaftConfig = toml::from_str("listen_addr = \"0.0.0.0:9400\"\nsnapshot_threshold = 7\n").unwrap();
        assert_eq!(r.snapshot_threshold, 7);
        let a: ApiConfig = toml::from_str("listen_addr = \"0.0.0.0:1\"\ncors_origins = [\"*\"]\n").unwrap();
        assert_eq!(a.listen_addr, "0.0.0.0:1");
        // Scrub quick_cron/deep_cron and partition_safety mode/show_warning
        // were never read either; the data [cache] table is ignored whole.
        let s: ScrubConfig = toml::from_str("quick_cron = \"0 3 * * *\"\ndeep_cron = \"0 4 * * 0\"\n").unwrap();
        assert_eq!(s.quick_interval_hours, 6);
        let p: PartitionSafetyConfig =
            toml::from_str("mode = \"strict\"\nshow_warning = true\nquorum_loss_timeout_secs = 7\n").unwrap();
        assert_eq!(p.quorum_loss_timeout_secs, 7);
    }

    #[test]
    fn interval_validators_reject_zero() {
        let mut t: TieringConfig = toml::from_str("").unwrap();
        assert!(t.validate().is_ok());
        t.evaluation_interval_secs = 0;
        assert_eq!(t.validate().unwrap_err(), "[tiering] evaluation_interval_secs must be > 0");
        let mut m: MonitoringConfig = toml::from_str("").unwrap();
        assert!(m.validate().is_ok());
        m.heartbeat_interval_secs = 0;
        assert!(m.validate().is_err());
        let mut r: RaftConfig = toml::from_str("").unwrap();
        assert!(r.validate_timings().is_ok());
        r.heartbeat_interval_ms = r.election_timeout_ms;
        assert!(r.validate_timings().is_err());
        r.heartbeat_interval_ms = 0;
        assert!(r.validate_timings().is_err());
        let mut c: FuseCacheConfig = toml::from_str("").unwrap();
        assert!(c.prune_interval_secs > 0);
        c.prune_interval_secs = 0;
        assert_eq!(nonzero("[cache] prune_interval_secs", c.prune_interval_secs).unwrap_err(), "[cache] prune_interval_secs must be > 0");
    }

    /// CR-6-1 (CMN-8) — `PlacementConfig::validate` rejects a `min_free_pct`
    /// outside `0.0..=100.0` and accepts the compiled-in default.
    #[test]
    fn cr_6_1_placement_config_validate() {
        assert!(PlacementConfig::default().validate().is_ok());

        let too_high = PlacementConfig {
            min_free_pct: 150.0,
            ..Default::default()
        };
        assert!(too_high.validate().is_err());

        let negative = PlacementConfig {
            min_free_pct: -1.0,
            ..Default::default()
        };
        assert!(negative.validate().is_err());

        let nan = PlacementConfig {
            min_free_pct: f64::NAN,
            ..Default::default()
        };
        assert!(nan.validate().is_err());
    }

    // Bug BU — headroom classification at the threshold boundary.
    #[test]
    fn bu3_placement_has_room_boundary() {
        // 20 GiB disk, default reserve = max(1 GiB, 5% of 20 GiB = 1 GiB) = 1 GiB.
        let cfg = PlacementConfig::default();
        let total = 20 * 1024 * 1024 * 1024u64;
        let reserve = cfg.headroom_bytes(total);
        assert_eq!(reserve, 1024 * 1024 * 1024, "5% of 20GiB == 1GiB == min_free_bytes");

        // Free == reserve -> NOT room (strict >).
        let used_at_reserve = total - reserve;
        assert!(!cfg.has_room(total, used_at_reserve), "free == reserve is full");
        // Free just above reserve -> room.
        assert!(cfg.has_room(total, used_at_reserve - 1));
        // Disk literally full -> no room.
        assert!(!cfg.has_room(total, total));
        // Unreported capacity (total 0) -> treated as room (not yet measured).
        assert!(cfg.has_room(0, 0));
    }

    // Bug BU — percentage reserve dominates on a large disk.
    #[test]
    fn bu3_placement_pct_dominates_on_large_disk() {
        let cfg = PlacementConfig::default(); // 1 GiB / 5%
        let total = 1024 * 1024 * 1024 * 1024u64; // 1 TiB
        // 5% of 1 TiB ~= 52 GiB > 1 GiB, so the pct reserve wins.
        assert_eq!(cfg.headroom_bytes(total), (total as f64 * 0.05) as u64);
    }

    // AZ-5 — updater config: empty block yields defaults; values override.
    #[test]
    fn updater_config_defaults() {
        let empty: UpdaterConfig = toml::from_str("").unwrap();
        assert_eq!(empty.repo_url, crate::REPO_URL);
        assert_eq!(empty.install_dir, "/usr/local/bin");
        // Default impl agrees with the empty-parse path.
        assert_eq!(UpdaterConfig::default().repo_url, crate::REPO_URL);
        assert_eq!(UpdaterConfig::default().install_dir, "/usr/local/bin");
    }

    #[test]
    fn updater_config_override() {
        let cfg: UpdaterConfig = toml::from_str(
            "repo_url = \"https://github.com/acme/fork\"\ninstall_dir = \"/opt/dtfs/bin\"\n",
        )
        .unwrap();
        assert_eq!(cfg.repo_url, "https://github.com/acme/fork");
        assert_eq!(cfg.install_dir, "/opt/dtfs/bin");
        assert_eq!(cfg.token, "");
    }

    #[test]
    fn updater_config_token() {
        let cfg: UpdaterConfig =
            toml::from_str("token = \"gldt-example\"\n").unwrap();
        assert_eq!(cfg.token, "gldt-example");
        assert_eq!(cfg.repo_url, crate::REPO_URL);
    }

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

    /// WI-01 — when `[write]` omits `flush_batch_chunks` (legacy config or
    /// only `first_write_tier` set), `serde(default)` must produce `4`.
    #[test]
    fn wi01_flush_batch_chunks_defaults_to_four() {
        let cfg: FuseWriteConfig = toml::from_str("").unwrap();
        assert_eq!(cfg.flush_batch_chunks, 4);

        let cfg: FuseWriteConfig =
            toml::from_str("first_write_tier = \"nearest-warm\"").unwrap();
        assert_eq!(
            cfg.flush_batch_chunks, 4,
            "setting first_write_tier alone must not disturb the batch default"
        );

        let dflt = FuseWriteConfig::default();
        assert_eq!(dflt.flush_batch_chunks, 4);
    }

    /// WI-01 — an explicit `flush_batch_chunks` parses through untouched;
    /// clamping into `1..=32` happens in `WriteBuffer::new_with_batch`, not
    /// at parse time.
    #[test]
    fn wi01_flush_batch_chunks_parses_explicit_value() {
        let cfg: FuseWriteConfig =
            toml::from_str("flush_batch_chunks = 1").unwrap();
        assert_eq!(cfg.flush_batch_chunks, 1, "1 is the documented rollback value");
    }

    /// Bug BC-1 — an empty `[scrub]` block (or its complete absence)
    /// must yield the documented defaults. The data-integrity
    /// invariants (`verify_on_read=true`, `quarantine_on_fault=true`,
    /// `auto_unquarantine_secs=0`) are load-bearing for the rest of
    /// Bug BC: if the default ever flips, a fresh cluster would start
    /// silently delivering corrupt bytes on read.
    #[test]
    fn bug_bc_1_cluster_scrub_config_defaults() {
        let empty: ClusterScrubConfig = toml::from_str("").unwrap();
        assert_eq!(empty.quick_interval_secs,    6 * 3600);
        assert_eq!(empty.deep_interval_secs,     168 * 3600);
        assert_eq!(empty.orphan_interval_secs,   24 * 3600);
        assert_eq!(empty.orphan_grace_secs,      24 * 3600);
        assert_eq!(empty.max_io_percent,         10);
        assert!(empty.verify_on_read,
            "verify_on_read must default true (data consistency invariant)");
        assert!(empty.quarantine_on_fault,
            "quarantine_on_fault must default true (single-fault → full ban)");
        assert_eq!(empty.auto_unquarantine_secs, 0,
            "auto_unquarantine_secs must default 0 (manual only)");

        let default = ClusterScrubConfig::default();
        assert_eq!(default, empty, "Default impl must match the serde-default deserialiser");
    }

    /// Bug BC-1 — overrides round-trip cleanly and the integrity
    /// knobs are honoured even when an operator sets them to the
    /// risky values.
    #[test]
    fn orphan_grace_below_floor_is_rejected() {
        let mut c = ClusterScrubConfig::default();
        assert!(c.validate().is_ok());
        c.orphan_grace_secs = MIN_ORPHAN_GRACE_SECS;
        assert!(c.validate().is_ok(), "boundary is inclusive");
        c.orphan_grace_secs = MIN_ORPHAN_GRACE_SECS - 1;
        let e = c.validate().unwrap_err();
        assert!(e.contains("orphan_grace_secs"), "{e}");
    }

    #[test]
    fn bug_bc_1_cluster_scrub_config_overrides_apply() {
        let cfg: ClusterScrubConfig = toml::from_str(r#"
quick_interval_secs    = 600
deep_interval_secs     = 3600
orphan_interval_secs   = 7200
orphan_grace_secs      = 21600
max_io_percent         = 25
verify_on_read         = false
quarantine_on_fault    = false
auto_unquarantine_secs = 3600
"#).unwrap();
        assert_eq!(cfg.quick_interval_secs, 600);
        assert_eq!(cfg.deep_interval_secs, 3600);
        assert_eq!(cfg.orphan_interval_secs, 7200);
        assert_eq!(cfg.orphan_grace_secs, 21600);
        assert_eq!(cfg.max_io_percent, 25);
        assert!(!cfg.verify_on_read,
            "operator may disable verify_on_read; the API-level ack guard \
             (Bug BC-8) is what protects against silent toggles");
        assert!(!cfg.quarantine_on_fault);
        assert_eq!(cfg.auto_unquarantine_secs, 3600);
    }

    // CR2-8-1 (CMN-3) — grpc.max_message_size_mb propagation + validation.

    #[test]
    fn cr2_8_1_max_message_bytes_converts_mb_to_bytes() {
        assert_eq!(max_message_bytes(256), 256 * 1024 * 1024);
        assert_eq!(max_message_bytes(32), 32 * 1024 * 1024);
        assert_eq!(max_message_bytes(0), 0);
    }

    #[test]
    fn cr2_8_1_grpc_config_validate_rejects_zero() {
        let mut cfg = GrpcConfig {
            listen_addr: "0.0.0.0:9500".to_string(),
            max_message_size_mb: 0,
            max_concurrent_chunk_writes: default_max_concurrent_chunk_writes(),
        };
        assert!(cfg.validate().is_err());
        cfg.max_message_size_mb = 32;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn cr2_8_1_fuse_client_config_max_message_size_mb_defaults_and_validates() {
        let cfg: FuseClientConfig = toml::from_str(r#"
manager_addr = "mgr-01:9401"
shared_secret = "s"
"#).unwrap();
        assert_eq!(cfg.max_message_size_mb, default_max_message_size());
        assert!(cfg.validate().is_ok());

        let bad: FuseClientConfig = toml::from_str(r#"
manager_addr = "mgr-01:9401"
shared_secret = "s"
max_message_size_mb = 0
"#).unwrap();
        assert!(bad.validate().is_err());
    }
}
