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

    /// Parse a 64-hex-char string into a ChunkId. Returns an error
    /// string on bad length or non-hex characters.
    pub fn from_hex(s: &str) -> Result<Self, String> {
        if s.len() != 64 {
            return Err(format!("expected 64 hex chars, got {}", s.len()));
        }
        if !s.is_ascii() {
            return Err("expected ASCII hex chars".to_string());
        }
        let mut out = [0u8; 32];
        for i in 0..32 {
            let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
                .map_err(|e| format!("bad hex at byte {}: {}", i, e))?;
            out[i] = byte;
        }
        Ok(Self(out))
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

impl DiskTier {
    /// Bug BX — media speed rank, lower = faster. Used to order capacity
    /// spill candidates so a chunk that can't fit on its mandated tier
    /// lands on the FASTEST tier with room (hot before warm before cold
    /// before tape) rather than just "any tier".
    pub fn speed_rank(&self) -> u8 {
        match self {
            DiskTier::Hot => 0,
            DiskTier::Warm => 1,
            DiskTier::Cold => 2,
            DiskTier::Tape => 3,
        }
    }
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
    /// Bug AA — FUSE client self-registers via RegisterNode so its
    /// location is visible in the topology view even when it has no
    /// active leases. Treated like a non-storage node by the placement
    /// engine (excluded from chunk placement) but appears in the
    /// `/api/v1/nodes` listing.
    Client,
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
    /// Bug BC — a data integrity fault was observed on this node.
    /// While Quarantined, the manager excludes the node from EVERY
    /// serving path: read source, replication source, allocate
    /// target. Heartbeats still accepted so we know when the
    /// process recovers. State stays Quarantined until an operator
    /// explicitly unquarantines via WebUI; the corresponding
    /// `NodeInfo.quarantine: Option<QuarantineInfo>` carries the
    /// reason + triggering chunk + history for the operator.
    Quarantined,
}

/// Bug BC — reason the node was quarantined. Compact enum so the
/// WebUI badge can render a short label; the human-readable
/// summary lives on `QuarantineInfo.last_fault_summary`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuarantineReason {
    QuickScrubSizeMismatch,
    QuickScrubMissingDat,
    DeepScrubHashMismatch,
    VerifyOnReadMismatch,
    OrphanScrubInStateMissing,
    OperatorRequested,
}

/// Bug BC — quarantine metadata attached to `NodeInfo.quarantine`
/// while `HealthStatus == Quarantined`. Persists through Raft
/// snapshots via `#[serde(default)]` on `NodeInfo.quarantine`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuarantineInfo {
    pub since_ms: u64,
    pub reason: QuarantineReason,
    #[serde(default)]
    pub triggering_chunk_id: Option<String>,
    #[serde(default)]
    pub fault_count_total: u32,
    pub last_fault_at_ms: u64,
    pub last_fault_summary: String,
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
    /// Bug BC — `Some(_)` iff `health == Quarantined`. Snapshot-safe:
    /// `#[serde(default)]` lets older snapshots without this field
    /// load as `None`.
    #[serde(default)]
    pub quarantine: Option<QuarantineInfo>,
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

// ---------------------------------------------------------------------------
// Bug AV — two-class tier lifecycle (mandatory anchor + opportunistic cache)
//
// `TierReplication` above is reused as `mandatory_replicas` (enforced by the
// under-replication scheduler). The opportunistic cache class is expressed
// by `CacheReplication` below — per-DC `(min, max)` caps with a promotion
// trigger and a flush cooldown.
//
// See: plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md for the full design.
// ---------------------------------------------------------------------------

/// Bug AV — per-tier cache configuration. `min` is the per-DC promotion
/// target (when the read threshold fires, the tiering engine ramps the
/// DC's cache replica count to `min`). `max` is the per-DC absolute cap;
/// it equals `min` for the typical "1 cache replica per DC" use case but
/// can be set higher to permit a future redundancy-escalation pass for
/// fault tolerance within a DC.
///
/// **Two defaults at play.**
///
/// * `CacheTierConfig::default()` returns `(0, 0)` — the tier is
///   disabled. This is what serde resolves to when the TOML parent
///   doesn't mention this tier at all (so an empty
///   `[cache_replicas]` block disables every tier).
/// * The `#[serde(default = …)]` attributes on the individual fields
///   make missing fields within a present struct default to `1, 1`.
///   So `[cache_replicas.warm]` with no inner keys deserialises to
///   `(1, 1)` — "tier explicitly enabled, default per-DC cap of 1".
///
/// This dual-default lets operators express "warm enabled" with a
/// single-line subsection without having to spell out `min = 1 max = 1`,
/// while still keeping the implicit-disabled behaviour when the tier
/// section is absent entirely.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheTierConfig {
    #[serde(default = "CacheTierConfig::default_min")]
    pub min: u32,
    #[serde(default = "CacheTierConfig::default_max")]
    pub max: u32,
}

impl CacheTierConfig {
    fn default_min() -> u32 { 1 }
    fn default_max() -> u32 { 1 }

    /// Bug AV — validate the invariant `max >= min`. Called by the config
    /// loader so a misconfigured volume is rejected with a clear error at
    /// load time rather than producing surprising tiering behaviour at
    /// runtime. Tier-disabled state `(0, 0)` is valid.
    pub fn validate(&self) -> Result<(), String> {
        if self.max < self.min {
            return Err(format!(
                "cache tier config: max ({}) must be >= min ({})",
                self.max, self.min
            ));
        }
        Ok(())
    }

    /// Returns true when this tier is disabled (no opportunistic
    /// promotion will ever happen here).
    pub fn is_disabled(&self) -> bool {
        self.max == 0
    }
}

impl Default for CacheTierConfig {
    fn default() -> Self {
        // Disabled when the parent doesn't mention this tier.
        Self { min: 0, max: 0 }
    }
}

/// Bug AV — opportunistic cache replication policy for a volume. Cache
/// replicas are created by the tiering engine in response to read
/// activity (`promote_reads` reads within `promote_window_secs` from a
/// single DC), bounded by per-tier `min`/`max`, with a cooldown after
/// operator-driven flushes to prevent immediate re-promotion.
///
/// All fields are `#[serde(default)]` so an empty `[cache_replicas]`
/// block parses successfully and leaves every tier disabled (the safe
/// back-compat path for pre-AV volumes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CacheReplication {
    /// Hot-tier cache. Atypical (hot is usually mandatory) but legal.
    #[serde(default)] pub hot:  CacheTierConfig,
    /// Warm-tier cache. The most common use case (warm SSDs as
    /// read-through cache for cold archive).
    #[serde(default)] pub warm: CacheTierConfig,
    /// Cold-tier cache. Almost always 0 — cold is normally mandatory.
    #[serde(default)] pub cold: CacheTierConfig,
    /// Tape-tier cache. Reserved as 0; tape doesn't make sense as a
    /// cache target.
    #[serde(default)] pub tape: CacheTierConfig,

    /// When true, `min`/`max` are applied **per DC** where reads
    /// originate. When false, they are cluster-wide caps. Default
    /// true — matches the typical multi-DC locality intent.
    #[serde(default = "CacheReplication::default_per_dc")]
    pub per_dc: bool,

    /// Promotion threshold: a chunk is eligible for promotion in a DC
    /// when it has seen at least this many reads from that DC within
    /// `promote_window_secs`. Default 2.
    #[serde(default = "CacheReplication::default_promote_reads")]
    pub promote_reads: u32,

    /// Rolling read window for `promote_reads`, in seconds. Default
    /// 14 400 (= 4 hours).
    #[serde(default = "CacheReplication::default_promote_window_secs")]
    pub promote_window_secs: u64,

    /// After an operator-driven `flush_cache`, the tiering engine
    /// refuses to re-promote affected chunks for this many seconds.
    /// Default 300 (= 5 minutes).
    #[serde(default = "CacheReplication::default_cooldown_secs")]
    pub cooldown_secs: u64,
}

impl CacheReplication {
    fn default_per_dc()            -> bool { true }
    fn default_promote_reads()     -> u32  { 2 }
    fn default_promote_window_secs() -> u64 { 4 * 3600 }
    fn default_cooldown_secs()     -> u64  { 5 * 60 }

    /// Per-tier accessor mirroring `TierReplication::for_tier`.
    pub fn for_tier(&self, tier: DiskTier) -> &CacheTierConfig {
        match tier {
            DiskTier::Hot  => &self.hot,
            DiskTier::Warm => &self.warm,
            DiskTier::Cold => &self.cold,
            DiskTier::Tape => &self.tape,
        }
    }

    /// Validate every tier's `(min, max)` pair. Returns the first error
    /// encountered with a tier prefix so the operator can find the
    /// offending stanza.
    pub fn validate(&self) -> Result<(), String> {
        for (label, cfg) in [
            ("hot",  &self.hot),
            ("warm", &self.warm),
            ("cold", &self.cold),
            ("tape", &self.tape),
        ] {
            cfg.validate().map_err(|e| format!("{label}: {e}"))?;
        }
        Ok(())
    }

    /// True when no tier permits any cache replica (i.e. all
    /// `(min, max)` pairs are `(0, 0)`). Used by the scheduler to
    /// short-circuit cache evaluation for legacy volumes.
    pub fn all_disabled(&self) -> bool {
        self.hot.is_disabled() && self.warm.is_disabled()
            && self.cold.is_disabled() && self.tape.is_disabled()
    }
}

/// Bug AV — anchor policy. Replaces the legacy
/// `VolumeConfig.anchor_datacenter: Option<String>` with a tier-aware
/// variant: `required_tier` constrains the anchor to a specific tier
/// (e.g. "the cold copy in LELI is the anchor"). When `required_tier`
/// is `None`, any tier in `datacenter` satisfies the invariant — that
/// matches the legacy string behaviour and is the implicit conversion
/// target for the back-compat helper below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorPolicy {
    pub datacenter: String,
    /// When `None`: any replica in `datacenter` satisfies the anchor.
    /// When `Some(t)`: the anchor replica MUST be in tier `t`.
    #[serde(default)]
    pub required_tier: Option<DiskTier>,
}

impl AnchorPolicy {
    /// Bug AV — construct an AnchorPolicy from the legacy
    /// `anchor_datacenter: Option<String>` field. Used by
    /// `VolumeConfig::effective_anchor()` (AV-1-2) to give a single
    /// resolved view to the scheduler/tiering engine regardless of
    /// which schema version produced the volume.
    pub fn from_legacy(datacenter: String) -> Self {
        Self { datacenter, required_tier: None }
    }

    /// Returns true when `tier` and `datacenter` would together satisfy
    /// this anchor policy. `tier=None` is treated as "tier unknown" and
    /// fails any policy that requires a specific tier.
    pub fn is_satisfied_by(&self, dc: &str, tier: Option<DiskTier>) -> bool {
        if !dc.eq_ignore_ascii_case(&self.datacenter) {
            return false;
        }
        match self.required_tier {
            None => true,                  // any-tier match
            Some(req) => tier == Some(req), // tier must match
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
    /// **Legacy**. The pre-Bug-AV single replication policy. Still
    /// the read fallback when `mandatory_replicas` is empty (i.e.
    /// all fields are 0). Operators on the new policy should set
    /// `mandatory_replicas` and leave this alone for rollback safety.
    pub replication: TierReplication,
    pub write_safety: WriteSafety,
    /// P0-10 / **Legacy**. If set, every chunk placement on this
    /// volume guarantees at least one replica lives in the named
    /// datacenter. The new tier-aware `anchor` field below
    /// supersedes this when both are set.
    ///
    /// `#[serde(default)]` keeps existing volume snapshots loadable.
    #[serde(default)]
    pub anchor_datacenter: Option<String>,

    // ── Bug AV — two-class tier lifecycle ──────────────────────────

    /// Bug AV — required (always-on) per-tier replication factors.
    /// Enforced by the under-replication scheduler. When every field
    /// is 0 (the back-compat default), the scheduler falls back to
    /// the legacy `replication` field via
    /// `effective_mandatory_replicas()`.
    ///
    /// Uses a custom serde-default of `(0, 0, 0, 0)` rather than
    /// `TierReplication::default()` (which is `(3, 2, 1, 1)`) — when
    /// an old snapshot is loaded that doesn't carry this field, we
    /// MUST land in the "all zero, defer to legacy" state, not in a
    /// surprise "double-replicate everything" state.
    #[serde(default = "VolumeConfig::default_mandatory_replicas")]
    pub mandatory_replicas: TierReplication,
    /// Bug AV — opportunistic per-DC cache per tier. Populated only
    /// when the operator opts in; default is "all disabled" so legacy
    /// volumes get exactly the pre-Bug-AV behaviour.
    #[serde(default)]
    pub cache_replicas: CacheReplication,
    /// Bug AV — tier-aware anchor policy. Supersedes
    /// `anchor_datacenter` when set; `effective_anchor()` does the
    /// resolution.
    #[serde(default)]
    pub anchor: Option<AnchorPolicy>,

    /// Bug AZ-1 — per-volume background-job schedules. Currently
    /// covers scrub cadence; future: fsck and operator-defined jobs.
    /// `None` on any sub-field means "inherit cluster default".
    #[serde(default)]
    pub schedules: VolumeSchedules,

    /// Bug BZ — per-volume capacity-spill policy. Controls whether and
    /// WHERE the manager may spill this volume's chunks when the mandated
    /// tier is full (Bug BX-2). `#[serde(default)]` keeps old snapshots
    /// loadable (default = inherit the cluster `allow_capacity_spill`, no
    /// tier/DC exclusions).
    #[serde(default)]
    pub spill: SpillPolicy,
}

/// Bug BZ — per-volume capacity-spill exclusions. When the mandated tier
/// is globally full, Bug BX-2 spills a chunk to the fastest+nearest tier
/// with room. This policy lets an operator forbid specific spill targets
/// per volume — e.g. "never spill the cold archive onto hot SSD" or "never
/// spill into datacenter dc-b". The anchor copy is never spilled regardless
/// (Bug BX-2 invariant); these rules constrain the NON-anchor spill targets.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct SpillPolicy {
    /// Per-volume override of the cluster `[placement] allow_capacity_spill`.
    /// `None` = inherit the cluster default; `Some(false)` = this volume
    /// fails the write rather than spilling; `Some(true)` = spill allowed.
    #[serde(default)]
    pub allow: Option<bool>,
    /// Tiers this volume must NEVER spill onto (lowercase: "hot"/"warm"/
    /// "cold"/"tape"). A candidate disk on a forbidden tier is dropped from
    /// the spill ranking.
    #[serde(default)]
    pub forbidden_tiers: Vec<String>,
    /// Datacenters this volume must NEVER spill into. A candidate disk on a
    /// node in a forbidden datacenter is dropped from the spill ranking.
    #[serde(default)]
    pub forbidden_datacenters: Vec<String>,
}

impl SpillPolicy {
    /// Bug BZ — is spill to `(datacenter, tier)` permitted for this volume?
    /// Tier comparison is case-insensitive against the lowercase tier name.
    pub fn allows_target(&self, datacenter: &str, tier: DiskTier) -> bool {
        let tier_name = tier.to_string(); // Display = lowercase
        if self.forbidden_tiers.iter().any(|t| t.eq_ignore_ascii_case(&tier_name)) {
            return false;
        }
        if self
            .forbidden_datacenters
            .iter()
            .any(|d| d.eq_ignore_ascii_case(datacenter))
        {
            return false;
        }
        true
    }
}

/// Bug AZ-1 — per-volume background-job cadence overrides. Lets
/// operators run scrub more often on hot volumes (e.g. Plex media
/// libraries with frequent reads) than the cluster-wide default
/// without dialling up scrub I/O on archival volumes.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VolumeSchedules {
    /// Bug BF-9 — REMOVED FROM THE ACTIVE PATH. Cluster-wide
    /// `manager.scrub.quick_interval_secs` governs scrub cadence
    /// (data nodes scrub by disk, not by volume). The field is
    /// kept for snapshot back-compat + `#[serde(skip_serializing_if =
    /// "Option::is_none")]` so fresh writes don't emit it. A
    /// warn-once log fires on first observation per process.
    #[deprecated(note = "Bug BF-9: removed from active path; set [scrub] block instead")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scrub_interval_secs: Option<u64>,
    /// Bug BF-10 — RESERVED, NEVER CONSUMED. No fsck scheduler
    /// exists. Scheduled for removal in next minor. Warn-once
    /// log fires on first observation per process.
    #[deprecated(note = "Bug BF-10: reserved-but-unused; scheduled for removal")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fsck_interval_secs: Option<u64>,
    /// Bug BF-10 — RESERVED, NEVER CONSUMED. No scheduler honours
    /// named-job entries. Scheduled for removal in next minor.
    #[deprecated(note = "Bug BF-10: reserved-but-unused; scheduled for removal")]
    #[serde(default)]
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub custom: std::collections::BTreeMap<String, u64>,
}

impl VolumeConfig {
    /// Bug AV — serde-default for `mandatory_replicas`. See the
    /// field-level comment for why this isn't `TierReplication::default()`.
    fn default_mandatory_replicas() -> TierReplication {
        TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 }
    }

    /// Bug AV — return the effective mandatory replication policy:
    /// `mandatory_replicas` when it has any positive RF, otherwise the
    /// legacy `replication` field. Lets the under-replication scheduler
    /// honour the new schema while still working unchanged for legacy
    /// volumes that haven't been migrated.
    pub fn effective_mandatory_replicas(&self) -> &TierReplication {
        let m = &self.mandatory_replicas;
        if m.hot > 0 || m.warm > 0 || m.cold > 0 || m.tape > 0 {
            m
        } else {
            &self.replication
        }
    }

    /// Bug AV (AV-1-10) — produce a new VolumeConfig where the
    /// legacy `replication` field has been copied into
    /// `mandatory_replicas` so the under-replication scheduler reads
    /// the new field. Idempotent: if `mandatory_replicas` already
    /// has any positive RF, the original config is returned
    /// unchanged (the operator has already migrated or hand-tuned
    /// the new shape).
    ///
    /// The legacy `replication` field is left untouched — keeps
    /// rollback available (an operator who wants to revert just
    /// zeros mandatory_replicas and the fallback kicks in).
    /// `anchor_datacenter` legacy field is similarly preserved.
    pub fn migrated_to_mandatory_schema(&self) -> Self {
        let m = &self.mandatory_replicas;
        let already_migrated =
            m.hot > 0 || m.warm > 0 || m.cold > 0 || m.tape > 0;
        if already_migrated {
            return self.clone();
        }
        Self {
            mandatory_replicas: self.replication.clone(),
            ..self.clone()
        }
    }

    /// Bug AV — return the effective anchor policy:
    /// 1. `anchor` if `Some` (new tier-aware shape wins).
    /// 2. else `anchor_datacenter` as an any-tier policy
    ///    (`required_tier = None`).
    /// 3. else `None` — no anchor enforcement.
    ///
    /// Returns a owned `AnchorPolicy` because the legacy fallback has
    /// to construct one on the fly. Cheap: small struct, allocates a
    /// single String clone.
    pub fn effective_anchor(&self) -> Option<AnchorPolicy> {
        if let Some(a) = &self.anchor {
            return Some(a.clone());
        }
        self.anchor_datacenter
            .as_ref()
            .map(|dc| AnchorPolicy::from_legacy(dc.clone()))
    }
}

impl Default for VolumeConfig {
    fn default() -> Self {
        Self {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 128 * 1024 * 1024, // 128MB
            replication: TierReplication::default(),
            write_safety: WriteSafety::default(),
            anchor_datacenter: None,
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            schedules: VolumeSchedules::default(),
            spill: SpillPolicy::default(),
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
    /// Bug O — chunk hashes (hex) flagged as corrupt or missing during
    /// the scrub. The data node's scrub scheduler quarantines these
    /// locally then sends them to the manager so the Raft state-machine
    /// drops the matching replica entries from `sm.files` and the
    /// scheduler can queue re-replication. `#[serde(default)]` so older
    /// snapshots / heartbeat payloads load.
    #[serde(default)]
    pub corrupt_chunk_ids: Vec<String>,
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

#[cfg(test)]
mod bug_bz_spill_policy_tests {
    use super::{DiskTier, SpillPolicy};

    // Bug BZ — forbidden tiers + datacenters block spill targets; an empty
    // policy permits everything; tier match is case-insensitive.
    #[test]
    fn allows_target_respects_exclusions() {
        let p = SpillPolicy {
            allow: None,
            forbidden_tiers: vec!["hot".into()],
            forbidden_datacenters: vec!["dc-b".into()],
        };
        assert!(!p.allows_target("dc-a", DiskTier::Hot), "forbidden tier blocked");
        assert!(!p.allows_target("dc-b", DiskTier::Warm), "forbidden dc blocked");
        assert!(p.allows_target("dc-a", DiskTier::Warm), "allowed target permitted");
        // Default policy forbids nothing.
        let open = SpillPolicy::default();
        assert!(open.allows_target("dc-z", DiskTier::Hot));
        // Case-insensitive tier name.
        let p2 = SpillPolicy { forbidden_tiers: vec!["HOT".into()], ..Default::default() };
        assert!(!p2.allows_target("dc-a", DiskTier::Hot));
    }
}

#[cfg(test)]
mod bug_av_types_tests {
    //! Bug AV (AV-1-1) — tests for the new two-class tier lifecycle
    //! types: CacheTierConfig, CacheReplication, AnchorPolicy.
    //!
    //! These tests use real TOML deserialisation because the configs
    //! these types come from are TOML files (manager.toml's
    //! `[defaults.cache_replicas]` block) — testing the actual format
    //! is more informative than serde_json round-tripping.
    use super::*;

    /// AV-1-1 test 1 — when a TOML config has `[cache_replicas]` present
    /// but doesn't mention any tier subsection, every tier defaults to
    /// disabled `(0, 0)`. This is the safe back-compat path: a legacy
    /// volume with the new struct serialised as `Default::default()`
    /// produces no cache promotion.
    #[test]
    fn bug_av_cache_tier_disabled_when_not_mentioned() {
        let toml_str = r#"
            promote_reads = 2
        "#;
        let parsed: CacheReplication = toml::from_str(toml_str).unwrap();
        assert_eq!(parsed.hot,  CacheTierConfig { min: 0, max: 0 });
        assert_eq!(parsed.warm, CacheTierConfig { min: 0, max: 0 });
        assert_eq!(parsed.cold, CacheTierConfig { min: 0, max: 0 });
        assert_eq!(parsed.tape, CacheTierConfig { min: 0, max: 0 });
        // ... but non-tier defaults still come through.
        assert_eq!(parsed.promote_reads, 2);
        assert!(parsed.per_dc); // default true
        // all_disabled() reports correctly.
        assert!(parsed.all_disabled());
    }

    /// AV-1-1 test 2 — when an operator writes `[warm]` (an empty
    /// subsection), the warm tier deserialises to `(1, 1)`: the field
    /// `#[serde(default = …)]` attributes default the missing `min` and
    /// `max` keys to 1, NOT 0. This matters because operators commonly
    /// want to express "enable warm cache with the defaults" in one
    /// line without spelling out `min = 1 max = 1`.
    #[test]
    fn bug_av_cache_tier_explicit_defaults_to_one_one() {
        let toml_str = r#"
            [warm]
        "#;
        let parsed: CacheReplication = toml::from_str(toml_str).unwrap();
        assert_eq!(parsed.warm, CacheTierConfig { min: 1, max: 1 });
        // Other tiers remain disabled.
        assert_eq!(parsed.hot,  CacheTierConfig { min: 0, max: 0 });
        assert_eq!(parsed.cold, CacheTierConfig { min: 0, max: 0 });
        assert_eq!(parsed.tape, CacheTierConfig { min: 0, max: 0 });
        // is_disabled() reflects the new state.
        assert!(!parsed.warm.is_disabled());
        assert!(parsed.hot.is_disabled());
    }

    /// AV-1-1 test 3 — explicit `min`/`max` survives a ser→de round
    /// trip with the values intact.
    #[test]
    fn bug_av_cache_tier_explicit_min_max_round_trip() {
        let original = CacheTierConfig { min: 1, max: 2 };
        let toml_str = toml::to_string(&original).unwrap();
        let round_tripped: CacheTierConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(round_tripped, original);
    }

    /// AV-1-1 test 4 — a fully populated `CacheReplication` (every
    /// tier mentioned, non-default scheduling parameters) survives
    /// ser→de cleanly. Guards against accidental serde-attribute
    /// regressions on any of the named fields.
    #[test]
    fn bug_av_cache_replication_full_serde_round_trip() {
        let original = CacheReplication {
            hot:  CacheTierConfig { min: 0, max: 0 },
            warm: CacheTierConfig { min: 1, max: 2 },
            cold: CacheTierConfig { min: 0, max: 0 },
            tape: CacheTierConfig { min: 0, max: 0 },
            per_dc: false,
            promote_reads: 5,
            promote_window_secs: 7200,
            cooldown_secs: 600,
        };
        let toml_str = toml::to_string(&original).unwrap();
        let round_tripped: CacheReplication = toml::from_str(&toml_str).unwrap();
        assert_eq!(round_tripped, original);
    }

    /// AV-1-1 test 5 — the `validate()` helper rejects `max < min`.
    /// Serde itself is permissive (it'll happily deserialise the bad
    /// values); the config loader is expected to call `validate()` and
    /// surface a meaningful error so the operator sees the problem at
    /// load time rather than as confusing runtime behaviour.
    #[test]
    fn bug_av_cache_tier_rejects_max_less_than_min() {
        let bad = CacheTierConfig { min: 3, max: 1 };
        let err = bad.validate().unwrap_err();
        assert!(err.contains("max (1)"),  "error mentions max: {err}");
        assert!(err.contains("min (3)"),  "error mentions min: {err}");

        // Tier-disabled (0, 0) is valid.
        assert!(CacheTierConfig { min: 0, max: 0 }.validate().is_ok());
        // min == max is valid (the typical case).
        assert!(CacheTierConfig { min: 1, max: 1 }.validate().is_ok());
        // max > min is valid (redundancy headroom).
        assert!(CacheTierConfig { min: 1, max: 4 }.validate().is_ok());

        // CacheReplication::validate() prefixes the error with the tier
        // label so the operator can find the offending stanza.
        let rep = CacheReplication {
            warm: CacheTierConfig { min: 2, max: 1 },
            ..Default::default()
        };
        let err = rep.validate().unwrap_err();
        assert!(err.starts_with("warm:"), "error labels the tier: {err}");
    }

    /// AV-1-1 test 6 — `AnchorPolicy::from_legacy` converts the legacy
    /// `anchor_datacenter: Option<String>` shape into a tier-agnostic
    /// AnchorPolicy. Required by AV-1-2's
    /// `VolumeConfig::effective_anchor()` so the scheduler sees a
    /// uniform policy view regardless of which schema produced the
    /// volume.
    #[test]
    fn bug_av_anchor_policy_legacy_string_compat() {
        let p = AnchorPolicy::from_legacy("leli".to_string());
        assert_eq!(p.datacenter, "leli");
        assert_eq!(p.required_tier, None);
        // The any-tier match accepts any tier in the named DC.
        assert!(p.is_satisfied_by("leli", Some(DiskTier::Warm)));
        assert!(p.is_satisfied_by("leli", Some(DiskTier::Cold)));
        assert!(p.is_satisfied_by("leli", None));   // tier-unknown ok
        // But not other DCs.
        assert!(!p.is_satisfied_by("toro", Some(DiskTier::Warm)));
    }

    /// CR-3-7 / CMN-5 — the datacenter compare is case-insensitive, so an
    /// anchor configured as "leli" is satisfied by a node registered under
    /// "LELI" (or any other casing). Without this, the anchor invariant is
    /// permanently unsatisfiable whenever config/registration casing
    /// diverges.
    #[test]
    fn cr_3_7_anchor_policy_dc_compare_is_case_insensitive() {
        let p = AnchorPolicy::from_legacy("leli".to_string());
        assert!(p.is_satisfied_by("LELI", Some(DiskTier::Cold)));
        assert!(p.is_satisfied_by("LeLi", None));
        assert!(!p.is_satisfied_by("toro", Some(DiskTier::Warm)));
    }

    /// AV-1-1 test 7 — the new struct form round-trips through TOML
    /// and `is_satisfied_by` enforces the tier constraint.
    #[test]
    fn bug_av_anchor_policy_struct_round_trip() {
        let original = AnchorPolicy {
            datacenter: "leli".into(),
            required_tier: Some(DiskTier::Cold),
        };
        let toml_str = toml::to_string(&original).unwrap();
        let round_tripped: AnchorPolicy = toml::from_str(&toml_str).unwrap();
        assert_eq!(round_tripped, original);

        // Tier-pinned policy: only cold-in-leli satisfies.
        assert!(round_tripped.is_satisfied_by("leli", Some(DiskTier::Cold)));
        // Warm in the right DC is rejected — tier doesn't match.
        assert!(!round_tripped.is_satisfied_by("leli", Some(DiskTier::Warm)));
        // Right DC but tier unknown is rejected when required_tier is Some.
        assert!(!round_tripped.is_satisfied_by("leli", None));
        // Wrong DC is rejected regardless of tier.
        assert!(!round_tripped.is_satisfied_by("toro", Some(DiskTier::Cold)));
    }

    // ---- AV-1-2 — VolumeConfig extension tests --------------------

    /// Build a default-ish VolumeConfig and tweak via the helper.
    /// Tests use this so future field additions on VolumeConfig don't
    /// force every test to be rewritten.
    fn vc_base() -> VolumeConfig {
        VolumeConfig::default()
    }

    /// AV-1-2 test 1 — when `mandatory_replicas` is all-zero (the
    /// back-compat shape applied to legacy snapshots), the helper
    /// returns the legacy `replication` field. This is the
    /// guarantee that lets us land the new schema dark without
    /// behaviour change.
    #[test]
    fn bug_av_volume_legacy_replication_falls_back() {
        let mut v = vc_base();
        v.replication = TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 };
        // mandatory all 0 — the default-from-VolumeConfig::default()
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 };
        let eff = v.effective_mandatory_replicas();
        assert_eq!(eff.hot,  3);
        assert_eq!(eff.warm, 2);
        assert_eq!(eff.cold, 1);
        assert_eq!(eff.tape, 1);
    }

    /// AV-1-2 test 2 — once the operator sets any positive
    /// mandatory RF, the helper returns the new field. The legacy
    /// `replication` is preserved untouched (rollback path).
    #[test]
    fn bug_av_volume_new_mandatory_takes_precedence() {
        let mut v = vc_base();
        v.replication = TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 };
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let eff = v.effective_mandatory_replicas();
        assert_eq!(eff.cold, 1);
        assert_eq!(eff.warm, 0); // mandatory's warm wins (=0), not legacy's (=2)
    }

    /// AV-1-2 test 3 — legacy `anchor_datacenter` (the old Option<String>)
    /// resolves into an any-tier AnchorPolicy via the
    /// `effective_anchor()` helper.
    #[test]
    fn bug_av_anchor_legacy_string_promoted() {
        let mut v = vc_base();
        v.anchor_datacenter = Some("leli".to_string());
        v.anchor = None;
        let p = v.effective_anchor().expect("anchor present");
        assert_eq!(p.datacenter, "leli");
        assert_eq!(p.required_tier, None);
    }

    /// AV-1-2 test 4 — the new struct form supersedes the legacy
    /// string when both are present (the migration window: operator
    /// has set `anchor` but hasn't cleaned up `anchor_datacenter`).
    #[test]
    fn bug_av_anchor_struct_overrides_legacy_string() {
        let mut v = vc_base();
        v.anchor_datacenter = Some("leli".into());
        v.anchor = Some(AnchorPolicy {
            datacenter: "leli".into(),
            required_tier: Some(DiskTier::Cold),
        });
        let p = v.effective_anchor().expect("anchor present");
        assert_eq!(p.required_tier, Some(DiskTier::Cold));
    }

    /// AV-1-2 test 5 — both legacy and new anchor unset → no anchor
    /// enforcement.
    #[test]
    fn bug_av_anchor_neither_returns_none() {
        let v = vc_base();
        assert!(v.effective_anchor().is_none());
    }

    /// AV-1-10 — first migration call clones `replication` into
    /// `mandatory_replicas`, leaving the legacy field intact for
    /// rollback. After migration, `effective_mandatory_replicas()`
    /// returns the new field (which equals the old field's value).
    #[test]
    fn bug_av_migrate_legacy_replication_clones_to_mandatory() {
        let mut v = vc_base();
        v.replication = TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 };
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 0, tape: 0 };
        let m = v.migrated_to_mandatory_schema();
        assert_eq!(m.mandatory_replicas, TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 });
        // Legacy field preserved for rollback.
        assert_eq!(m.replication,        TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 });
        // effective_* now picks the new field.
        assert_eq!(m.effective_mandatory_replicas().for_tier(DiskTier::Cold), 1);
    }

    /// AV-1-10 — migration is idempotent. Running it twice (or
    /// running it against a volume the operator has already
    /// hand-tuned the new shape on) is a no-op and doesn't clobber
    /// the post-migration values with the pre-migration legacy
    /// field.
    #[test]
    fn bug_av_migrate_idempotent_when_both_present() {
        let mut v = vc_base();
        v.replication = TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 };
        // Operator already hand-tuned mandatory to a NEW shape
        // (e.g. cold-only AV style):
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let m = v.migrated_to_mandatory_schema();
        // Mandatory preserved exactly — the migration MUST NOT
        // overwrite with the pre-AV legacy values.
        assert_eq!(m.mandatory_replicas, TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 });
        assert_eq!(m.replication,        TierReplication { hot: 3, warm: 2, cold: 1, tape: 1 });
    }

    /// AV-1-8 — under-replication scheduler uses
    /// `effective_mandatory_replicas()`. When the operator sets
    /// `mandatory_replicas` to the typical AV pattern (cold=1, warm=0,
    /// hot=0), the effective RF for warm is 0 — meaning the
    /// under-replication scheduler will NOT try to maintain warm
    /// replicas. Warm replicas are instead owned by the tiering
    /// engine (cache_replicas in Phase 2).
    ///
    /// This test guards the integration point: same volume queried
    /// for both warm and cold returns the new-mandatory values, NOT
    /// the legacy `replication.warm=2`.
    #[test]
    fn bug_av_under_replication_ignores_cache_tiers() {
        let mut v = vc_base();
        // Legacy replication says warm=2 — pre-AV scheduler would
        // try to maintain 2 warm copies.
        v.replication = TierReplication { hot: 0, warm: 2, cold: 1, tape: 0 };
        // New mandatory says cold=1 only — warm is a cache tier
        // and shouldn't be auto-filled by under-replication.
        v.mandatory_replicas = TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 };
        let eff = v.effective_mandatory_replicas();
        assert_eq!(eff.for_tier(DiskTier::Cold), 1, "cold mandatory");
        assert_eq!(eff.for_tier(DiskTier::Warm), 0,
            "warm not mandatory → under-replication scheduler stays out");
        assert_eq!(eff.for_tier(DiskTier::Hot),  0);
        assert_eq!(eff.for_tier(DiskTier::Tape), 0);
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::*;

    // CO-5-12 — ChunkId::from_hex must Err (never panic) on a 64-byte
    // non-ASCII input; the byte-length check passes but `&s[i*2..i*2+2]`
    // slices through a multibyte char boundary and panics today.
    #[test]
    fn co_5_12_from_hex_rejects_multibyte_64_bytes() {
        // "€" is 3 bytes in UTF-8 (boundaries at 0,3,6,...). 21 of them =
        // 63 bytes + one ASCII byte = 64 bytes, so the len==64 guard
        // passes. The second slice `&s[2..4]` (i=1) straddles the first
        // "€" (bytes 0..3) -> char-boundary panic on current code.
        let s = format!("{}a", "€".repeat(21));
        assert_eq!(s.len(), 64, "fixture must be 64 bytes to pass the length guard");
        let res = ChunkId::from_hex(&s);
        assert!(res.is_err(), "non-hex multibyte input must be a clean Err, not a panic");

        // A genuine 64-hex string still parses Ok (no regression).
        let valid = "ab".repeat(32);
        assert_eq!(valid.len(), 64);
        assert!(ChunkId::from_hex(&valid).is_ok());
    }
}
