pub mod log_store;
pub mod network;
pub mod snapshot;
pub mod state_machine;

use std::fmt;
use std::io::Cursor;

use serde::{Deserialize, Serialize};

use dtfs_common::topology::Location;
use dtfs_common::types::{AnchorPolicy, DiskInfo, HealthStatus, NodeRole, VolumeConfig};

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

/// Merge `incoming` into `existing` without dropping unrelated chunk indices.
///
/// F-030: the FUSE client sends the *complete* replica set for every chunk
/// index touched by a single flush (one `UpdateChunkLocations` carries every
/// replica for the rewritten chunk). When a write produces *new content* at
/// an existing chunk index — e.g. SQLite rewriting page 1 of its database —
/// the chunk hash changes, so the previous content lives on as a stale row.
/// Later reads would then see whichever hash wins the BTreeMap sort and
/// silently return old data, which is what made every SQLite INSERT after
/// the first one invisible to subsequent SELECTs.
///
/// New semantics: for every `chunk_index` present in `incoming`, drop all
/// existing entries at that index and replace them with the incoming set.
/// Other chunk indices are untouched, so adding replicas for chunk N does
/// not disturb chunks M ≠ N.
///
/// Adding a single replica for a chunk that already has replicas is still
/// handled correctly because the caller always builds the *full* new set of
/// replicas at flush time (`flush_segments_to_cluster`).
pub fn merge_chunk_location_entries(
    existing: &[ChunkLocationEntry],
    incoming: &[ChunkLocationEntry],
) -> Vec<ChunkLocationEntry> {
    use std::collections::BTreeSet;
    let touched: BTreeSet<u64> = incoming.iter().map(|e| e.chunk_index).collect();
    let mut out: Vec<ChunkLocationEntry> = existing
        .iter()
        .filter(|e| !touched.contains(&e.chunk_index))
        .cloned()
        .collect();
    out.extend(incoming.iter().cloned());
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
    /// Unix milliseconds when the manager last observed this node alive
    /// (heartbeat received, RPC succeeded, etc.). Persisted via Raft so
    /// every manager surfaces the same "last seen" timestamp in the
    /// WebUI, even if the operator switches between mgr-01/02/03.
    /// `#[serde(default)]` keeps older snapshots loadable.
    #[serde(default)]
    pub last_seen_ms: u64,
    /// Cumulative bytes this node has served via read RPCs since the
    /// data-node process started. The manager diffs across heartbeats
    /// to compute a per-second rate for the WebUI I/O card.
    #[serde(default)]
    pub total_bytes_read: u64,
    /// Cumulative bytes written into this node via `put_chunk` since
    /// the data-node process started.
    #[serde(default)]
    pub total_bytes_written: u64,
    /// Bug AX — peer IP captured at RegisterNode time. Populated only for
    /// clients (FUSE mounts) where `grpc_addr` is intentionally empty
    /// (Bug AA — clients run no gRPC listener) and the operator still
    /// needs to see *where* the mount is talking from. Data / manager
    /// nodes carry their IP inside `grpc_addr`, so this stays `None` for
    /// them. `#[serde(default)]` keeps older snapshots loadable.
    #[serde(default)]
    pub client_ip: Option<String>,
    /// Bug AY — binary version string this node reported at RegisterNode
    /// time (e.g. `"v0.0.0-dev"`). Empty when the node pre-dates Bug AY.
    /// `#[serde(default)]` keeps older snapshots loadable.
    #[serde(default)]
    pub version: String,
    /// Bug AY — build date of the binary this node is running, format
    /// `YYYYMMDD-HHmmss` (UTC, e.g. `"20260522-141034"`). Comes from
    /// `dtfs_common::version::BUILD_DATE` baked in at compile time.
    /// Empty when the node pre-dates Bug AY.
    #[serde(default)]
    pub build_date: String,
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
        /// Bug P — the lease_id the writer is committing under. When
        /// non-empty, the apply path verifies it matches
        /// `sm.write_leases[path].lease_id` and rejects with
        /// `WriteLeaseConflict` otherwise. Empty string preserves the
        /// pre-fix behaviour (no lease verification) so existing
        /// snapshots / older clients continue to work.
        #[serde(default)]
        lease_id: String,
    },

    /// Bug O — remove every (node_id, chunk_hash) entry the data node
    /// has flagged as corrupt or missing during scrub. The scheduler's
    /// `check_under_replicated` pass then notices the file is below its
    /// configured replication factor and queues a re-replication task
    /// from a healthy peer.
    ///
    /// `#[serde(default)]` on the new variant is unnecessary at the
    /// enum level (variants are tagged); the contained fields are
    /// always present, no compat concern.
    RemoveCorruptChunkReplicas {
        /// Stringified u64 node id (matches `ChunkLocationEntry.node_id`).
        node_id: String,
        /// BLAKE3 hex chunk hashes that the data node confirmed are
        /// corrupt or missing on its disks.
        chunk_hashes: Vec<String>,
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
        /// WebUI bug: cumulative byte counters carried up from the
        /// data-node heartbeat. The manager stores them in NodeInfo
        /// so /api/v1/stats can compute a per-second rate. New fields
        /// are `#[serde(default)]` so older Raft log entries replay.
        #[serde(default)]
        total_bytes_read: u64,
        #[serde(default)]
        total_bytes_written: u64,
        /// Unix milliseconds; manager-side wall-clock at heartbeat
        /// receipt. Used to compute the bytes/sec rate AND to populate
        /// NodeInfo.last_seen_ms for the WebUI.
        #[serde(default)]
        observed_at_ms: u64,
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

    // ---- Extended POSIX surface (F-034 .. F-038) ----

    /// Atomically rename a file in the namespace.
    RenameFile { from: String, to: String },
    /// Set the logical size of a file. Trailing chunks past `new_size`
    /// are dropped from metadata; the truncate path also clears them on
    /// the data nodes.
    SetFileSize { path: String, new_size: u64 },
    /// Create a symbolic link.
    CreateSymlink { path: String, target: String },
    /// Create a hard link from `source` to `dest`. The FSM duplicates the
    /// source's chunk list under the new path.
    CreateLink { source: String, dest: String },

    // ---- Distributed fcntl coordination (F-039) ----

    /// Try to acquire a POSIX byte-range lock through the cluster.
    AcquireLock {
        path: String,
        owner_id: String,
        start: u64,
        end: u64,
        write: bool,
        lease_ms: u64,
        now_ms: u64,
    },
    /// Release a previously-acquired lock (matched by owner_id+range).
    ReleaseLock {
        path: String,
        owner_id: String,
        start: u64,
        end: u64,
    },

    // ---- Single-writer leases (P0-1) ----
    /// Acquire an exclusive write lease for a path. `proposed_lease_id` is
    /// generated by the manager before proposing so the apply stays
    /// deterministic across replicas (every replica sees the same id).
    AcquireWriteLease {
        path: String,
        owner_id: String,
        lease_ms: u64,
        now_ms: u64,
        proposed_lease_id: String,
    },
    /// Extend an active write lease by `extension_ms`. New expiry is
    /// `now_ms + extension_ms` (not `expires_at_ms + extension_ms`) so the
    /// per-lease budget is bounded under load.
    RenewWriteLease {
        path: String,
        lease_id: String,
        extension_ms: u64,
        now_ms: u64,
    },
    /// Release a held write lease (lease_id must match).
    ReleaseWriteLease {
        path: String,
        lease_id: String,
    },
    /// Garbage-collect orphan leases from a crashed FUSE process. Releases
    /// every lease whose `owner_id` starts with `hostname_prefix` AND whose
    /// `:pid:` segment does NOT match `except_pid` (the current FUSE PID).
    ReleaseLeasesByHost {
        hostname_prefix: String,
        except_pid: u32,
    },
    /// Sweep all leases with `expires_at_ms <= now_ms`. Called by the manager
    /// GC tokio task on its 1s tick.
    SweepExpiredLeases {
        now_ms: u64,
    },

    // ---- Bug AU — refresh per-disk usage on each heartbeat ----

    /// Bug AU — apply the live per-disk usage snapshot from a data
    /// node heartbeat. Replaces `NodeInfo.disks[i].total_bytes` and
    /// `used_bytes` for any disk_id present in the snapshot, leaving
    /// other fields (uuid, mount_path, tier, status) untouched.
    /// Snapshots that reference an unknown disk_id are dropped (the
    /// disk may have been removed since the heartbeat was sent).
    /// Without this command the per-disk view stayed at the
    /// register-time values; aggregate node-level totals were updated
    /// via `UpdateNodeUsage` but the per-disk breakdown surfaced by
    /// `/api/v1/nodes` (Bug AR) was stale.
    UpdateNodeDiskUsage {
        node_id: u64,
        per_disk: Vec<(u32, u64, u64)>, // (disk_id, total_bytes, used_bytes)
    },

    // ---- Bug AH — replicated audit log fan-out ----

    /// Bug AH — append a per-manager local audit event into the cluster-wide
    /// replicated audit log. Every manager's local `AuditLog::log()` call
    /// submits one of these so `/api/v1/audit` returns the same view from
    /// every voter (previously each manager only saw the events its own
    /// process generated).
    ///
    /// `timestamp_ms` is supplied by the originator (NOT regenerated at
    /// apply time) so the rendered ordering matches wall-clock at the
    /// source. The state machine apply path stores both the structured
    /// fields and a `summary` derived from `"{action}: {target}"` for
    /// display in tools that only understand the legacy summary shape.
    AppendAudit {
        actor: String,
        action: String,
        target: String,
        result: String,
        origin_manager: String,
        timestamp_ms: u64,
    },

    // ---- Bug AV — operator-driven cache replica removal ----

    /// Bug AV — drop a single `ChunkLocationEntry` from a file's
    /// `chunks` list. Used by the cache-flush path
    /// (`flush_cache_for_path`, AV-1-7) AFTER the underlying
    /// `DeleteChunkReplica` data-node RPC has succeeded, so the SM's
    /// view stays consistent with what physically exists on disk.
    ///
    /// **Anchor safety.** When `enforce_anchor` is `Some(policy)`, the
    /// apply path refuses the operation if dropping this entry would
    /// leave the affected chunk with zero replicas satisfying the
    /// policy. The check fires per chunk_index, not per file — a
    /// multi-chunk file may have some chunks anchor-satisfied and some
    /// not; only the actually-stranding case is rejected.
    ///
    /// Idempotent: removing an entry that isn't present is a no-op
    /// (the data-node delete may have raced with a prior identical
    /// call).
    RemoveChunkLocation {
        path: String,
        chunk_index: u64,
        /// String form to match `ChunkLocationEntry.node_id` (u64 as
        /// string for protobuf interop).
        node_id: String,
        /// When Some, refuse if the resulting state would strand the
        /// anchor for this chunk_index. Threading the policy through
        /// the apply path is cleaner than re-reading the volume here
        /// (apply is meant to be a pure function of `req + state`).
        enforce_anchor: Option<AnchorPolicy>,
    },

    /// Bug AV — set or extend cache cooldown for one or more
    /// chunk_indexes of a file. Called by the cache-flush path
    /// (`flush_cache_for_path`, AV-1-7) after a flush completes so the
    /// tiering engine refuses to re-promote the freshly-dropped
    /// replicas for `cache_replicas.cooldown_secs` seconds. The set
    /// operation is **monotonic**: a concurrent flush + tiering demote
    /// can race, and we always want the FURTHER-out timestamp to win
    /// (operator's flush intent dominates).
    ///
    /// Setting `expires_at_ms = 0` is the convention for "clear the
    /// cooldown" — used by the tiering-engine demote path (AV-2-5)
    /// so a natural idle→demote→read cycle re-promotes immediately,
    /// while operator-driven flushes set future timestamps.
    /// Specifically: the apply path clears the entry when
    /// `expires_at_ms == 0`, otherwise takes `max(current, new)`.
    SetChunkCooldown {
        path: String,
        chunk_indexes: Vec<u64>,
        expires_at_ms: u64,
    },
}

impl fmt::Display for ClientRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateFile { path, volume_id, .. } => {
                write!(f, "CreateFile({path}, vol={volume_id})")
            }
            Self::DeleteFile { path } => write!(f, "DeleteFile({path})"),
            Self::UpdateChunkLocations { path, chunks, lease_id } => {
                let lease_marker = if lease_id.is_empty() {
                    "no-lease".to_string()
                } else {
                    format!("lease={lease_id}")
                };
                write!(f, "UpdateChunks({path}, {} chunks, {lease_marker})", chunks.len())
            }
            Self::RemoveCorruptChunkReplicas { node_id, chunk_hashes } => write!(
                f,
                "RemoveCorruptChunkReplicas(node={node_id}, {} chunks)",
                chunk_hashes.len()
            ),
            Self::RegisterNode { node_id, .. } => write!(f, "RegisterNode({node_id})"),
            Self::DeregisterNode { node_id } => write!(f, "DeregisterNode({node_id})"),
            Self::UpdateNodeStatus { node_id, status } => {
                write!(f, "UpdateNodeStatus({node_id}, {status})")
            }
            Self::UpdateNodeUsage {
                node_id,
                used_capacity,
                chunk_count,
                total_bytes_read,
                total_bytes_written,
                observed_at_ms: _,
            } => {
                write!(
                    f,
                    "UpdateNodeUsage({node_id}, used={used_capacity}, chunks={chunk_count}, read={total_bytes_read}, written={total_bytes_written})"
                )
            }
            Self::UpdateConfig { version, .. } => write!(f, "UpdateConfig(v{version})"),
            Self::UpsertVolume { config } => write!(f, "UpsertVolume({})", config.id),
            Self::DeleteVolume { volume_id } => write!(f, "DeleteVolume({volume_id})"),
            Self::ReportScrubResult { node_id, level, chunks_checked, chunks_corrupt, .. } => {
                write!(f, "ReportScrubResult(node={node_id}, {level}, checked={chunks_checked}, corrupt={chunks_corrupt})")
            }
            Self::UpdateMigrationState { .. } => write!(f, "UpdateMigrationState"),
            Self::RenameFile { from, to } => write!(f, "RenameFile({from} -> {to})"),
            Self::SetFileSize { path, new_size } => {
                write!(f, "SetFileSize({path}, {new_size})")
            }
            Self::CreateSymlink { path, target } => {
                write!(f, "CreateSymlink({path} -> {target})")
            }
            Self::CreateLink { source, dest } => {
                write!(f, "CreateLink({source} -> {dest})")
            }
            Self::AcquireLock { path, owner_id, start, end, write, .. } => write!(
                f,
                "AcquireLock({path}, owner={owner_id}, [{start},{end}), write={write})"
            ),
            Self::ReleaseLock { path, owner_id, start, end } => {
                write!(f, "ReleaseLock({path}, owner={owner_id}, [{start},{end}))")
            }
            Self::AcquireWriteLease { path, owner_id, lease_ms, .. } => write!(
                f,
                "AcquireWriteLease({path}, owner={owner_id}, lease_ms={lease_ms})"
            ),
            Self::RenewWriteLease { path, lease_id, extension_ms, .. } => write!(
                f,
                "RenewWriteLease({path}, lease_id={lease_id}, ext_ms={extension_ms})"
            ),
            Self::ReleaseWriteLease { path, lease_id } => {
                write!(f, "ReleaseWriteLease({path}, lease_id={lease_id})")
            }
            Self::ReleaseLeasesByHost { hostname_prefix, except_pid } => write!(
                f,
                "ReleaseLeasesByHost(host={hostname_prefix}, except_pid={except_pid})"
            ),
            Self::SweepExpiredLeases { now_ms } => {
                write!(f, "SweepExpiredLeases(now_ms={now_ms})")
            }
            Self::AppendAudit { actor, action, target, .. } => {
                write!(f, "AppendAudit({actor}, {action}, {target})")
            }
            Self::UpdateNodeDiskUsage { node_id, per_disk } => {
                write!(f, "UpdateNodeDiskUsage(node={node_id}, {} disks)", per_disk.len())
            }
            Self::RemoveChunkLocation { path, chunk_index, node_id, enforce_anchor } => {
                let anchor_marker = match enforce_anchor {
                    Some(p) => match p.required_tier {
                        Some(t) => format!(", anchor={}@{}", p.datacenter, t),
                        None    => format!(", anchor={}", p.datacenter),
                    },
                    None => String::new(),
                };
                write!(f, "RemoveChunkLocation({path}, idx={chunk_index}, node={node_id}{anchor_marker})")
            }
            Self::SetChunkCooldown { path, chunk_indexes, expires_at_ms } => {
                write!(f, "SetChunkCooldown({path}, {} chunks, until={expires_at_ms}ms)",
                    chunk_indexes.len())
            }
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
    /// F-039: `AcquireLock` returns either Granted or a conflict description.
    LockGranted,
    LockConflict {
        owner_id: String,
        start: u64,
        end: u64,
        write: bool,
    },
    /// P0-1: write-lease acquired (or renewed) successfully.
    WriteLeaseGranted {
        lease_id: String,
        expires_at_ms: u64,
    },
    /// P0-1: another writer already holds the lease.
    WriteLeaseConflict {
        owner_id: String,
        expires_at_ms: u64,
    },
    /// P0-1: bulk release/sweep result.
    WriteLeasesReleased {
        count: u32,
    },
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
            Self::LockGranted => write!(f, "lock_granted"),
            Self::LockConflict { owner_id, start, end, write } => {
                write!(
                    f,
                    "lock_conflict(owner={owner_id}, [{start},{end}), write={write})"
                )
            }
            Self::WriteLeaseGranted { lease_id, expires_at_ms } => write!(
                f,
                "write_lease_granted(id={lease_id}, expires={expires_at_ms})"
            ),
            Self::WriteLeaseConflict { owner_id, expires_at_ms } => write!(
                f,
                "write_lease_conflict(owner={owner_id}, expires={expires_at_ms})"
            ),
            Self::WriteLeasesReleased { count } => {
                write!(f, "write_leases_released({count})")
            }
        }
    }
}

#[cfg(test)]
mod merge_tests {
    use super::{merge_chunk_location_entries, ChunkLocationEntry};

    fn entry(idx: u64, hash: &str, node: &str, size: u64) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: hash.into(),
            node_id: node.into(),
            disk_id: 0,
            size_bytes: size,
        }
    }

    #[test]
    fn merge_replaces_all_entries_at_touched_chunk_index() {
        // chunk 0 currently has 3 replicas with hash "aa"
        let a1 = entry(0, "aa", "http://n1:1", 10);
        let a2 = entry(0, "aa", "http://n2:1", 10);
        let a3 = entry(0, "aa", "http://n3:1", 10);
        // chunk 1 is untouched
        let other = entry(1, "bb", "http://n2:1", 20);

        // The client rewrites chunk 0 with new content hash "cc" across the
        // same three nodes — this is the SQLite "INSERT then INSERT again"
        // case: same chunk_index, new payload.
        let incoming = vec![
            entry(0, "cc", "http://n1:1", 30),
            entry(0, "cc", "http://n2:1", 30),
            entry(0, "cc", "http://n3:1", 30),
        ];

        let merged = merge_chunk_location_entries(
            &[a1, a2, a3, other.clone()],
            &incoming,
        );

        // The stale "aa" rows must be gone; otherwise the manager's
        // get_file_locations would still return them and the reader would
        // see pre-rewrite content (F-030).
        assert!(
            !merged.iter().any(|e| e.chunk_hash == "aa"),
            "old chunk content must be replaced when chunk_index is rewritten"
        );
        assert_eq!(
            merged.iter().filter(|e| e.chunk_index == 0).count(),
            3,
            "exactly the 3 new replicas for chunk 0"
        );
        assert!(merged.contains(&other), "untouched chunk index 1 preserved");
    }

    #[test]
    fn merge_leaves_other_chunks_alone() {
        let a = entry(0, "aa", "http://n1:1", 10);
        let b = entry(1, "bb", "http://n1:1", 20);
        // Only chunk 1 is rewritten.
        let merged = merge_chunk_location_entries(&[a.clone(), b], &[entry(1, "bb2", "http://n1:1", 21)]);
        assert!(merged.contains(&a), "chunk 0 unaffected");
        assert_eq!(merged.iter().filter(|e| e.chunk_index == 1).count(), 1);
    }
}
