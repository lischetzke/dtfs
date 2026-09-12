//! Raft state machine — the cluster-wide FSM that holds all metadata.
//!
//! State is kept entirely in memory. Snapshots are serialized to JSON via
//! [`crate::snapshot`] and stored as `Cursor<Vec<u8>>` blobs.

use std::collections::BTreeMap;
use std::io::{self, Cursor, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::EntryPayload;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use dtfs_common::types::{AnchorPolicy, VolumeConfig};

use crate::snapshot::{self, StateMachineSnapshot};
use crate::{
    legacy_disk_id, merge_chunk_location_entries, ChunkLocationEntry, ClientRequest,
    ClientResponse, TypeConfig,
};

// Re-export type aliases for convenience.
type LogId = crate::LogId;
type StoredMembership = crate::StoredMembership;

// ---------------------------------------------------------------------------
// Audit log cap — prevents unbounded memory growth
// ---------------------------------------------------------------------------

/// Maximum number of audit entries retained in the state machine.
/// Oldest entries are evicted once this limit is exceeded.
const MAX_AUDIT_LOG_ENTRIES: usize = 10_000;

// ---------------------------------------------------------------------------
// Domain types stored inside the state machine
// ---------------------------------------------------------------------------

/// A completed scrub operation result, stored per data-node in the state
/// machine so the WebUI and `/api/v1/stats` can surface health per node.
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
    /// F-035 — when set, this entry is a symbolic link whose target is the
    /// stored string. `chunks` is empty for symlinks.
    #[serde(default)]
    pub symlink_target: Option<String>,
    /// F-037 — explicit logical size override. Some flows (sparse files via
    /// truncate-grow, fallocate) need a size that isn't derived from the
    /// chunk list. `None` falls back to the sum of chunk sizes.
    #[serde(default)]
    pub logical_size: Option<u64>,
    /// Marker that this entry represents an empty directory rather than a
    /// regular file. Set by `mkdir` (CreateFile with a trailing slash).
    /// Synthetic directories implied by child file paths are computed at
    /// lookup time via [`StateMachineData::lookup_path`].
    /// `#[serde(default)]` keeps older snapshots loadable.
    #[serde(default)]
    pub is_directory: bool,

    /// Bug AV — per-chunk-index cache cooldown timestamps in
    /// milliseconds-since-epoch. When `cache_cooldown_until_ms[idx]`
    /// is in the future, the tiering engine refuses to re-promote that
    /// chunk's cache replicas (suppresses immediate re-promotion after
    /// an operator-driven flush). Entries are cleared on tiering-engine
    /// demote so a natural idle→demote→read cycle promotes immediately;
    /// operator flushes set future timestamps that the engine respects.
    /// `BTreeMap` (not HashMap) so the serialised form is deterministic
    /// across snapshots.
    /// `#[serde(default)]` keeps older snapshots loadable — empty map
    /// for pre-AV files.
    #[serde(default)]
    pub cache_cooldown_until_ms: std::collections::BTreeMap<u64, u64>,
}

/// F-039 — One distributed POSIX byte-range lock held in the cluster FSM.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DistributedLock {
    pub owner_id: String,
    pub start: u64,
    pub end: u64,
    pub write: bool,
    /// Wall-clock ms when this lock expires unless the owner renews.
    pub expires_at_ms: u64,
}

/// P0-1 — One single-writer lease on a path. Exclusive; one per path.
///
/// Prevents two FUSE clients writing the same file concurrently (the
/// last-flush-wins race that silently overwrote interleaved chunks).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct WriteLease {
    /// Caller-supplied owner id, recommended format `<hostname>:<pid>:<fh>`.
    pub owner_id: String,
    /// Opaque token returned to the FUSE client; must be presented on
    /// renew/release. Pre-computed by the manager so the apply is
    /// deterministic across Raft replicas.
    pub lease_id: String,
    /// Absolute expiry, Unix ms.
    pub expires_at_ms: u64,
}

/// Cluster-wide configuration (versioned).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ClusterConfig {
    pub config_json: String,
    pub version: u64,
}

/// A single entry in the audit log.
///
/// The historical shape was `{log_index, summary, timestamp_ms}` — populated
/// only by Raft state-mutating commands themselves (via `apply_request`'s
/// `audit(...)` helper). Bug AH extends the shape with structured fields so
/// the per-manager local audit ring (`crates/dtfs-manager/src/audit.rs`) can
/// fan its events out through Raft and have all voters see the same view via
/// `/api/v1/audit`. All new fields are `#[serde(default)]` so older snapshots
/// load unchanged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditEntry {
    pub log_index: u64,
    pub summary: String,
    pub timestamp_ms: u64,
    /// Bug AH — who initiated the event ("scheduler", "api", "system", or
    /// the user/principal name once auth lands). Empty for legacy entries.
    #[serde(default)]
    pub actor: String,
    /// Bug AH — dotted action category, e.g. `"replication.complete"`,
    /// `"migration.failed"`, `"volume.create"`. Empty for legacy entries.
    #[serde(default)]
    pub action: String,
    /// Bug AH — free-form target/subject, e.g. a chunk id, file path,
    /// task id, node id. Empty for legacy entries.
    #[serde(default)]
    pub target: String,
    /// Bug AH — outcome marker: `"success"`, `"pending"`, or a short
    /// reason string for failures. Empty for legacy entries.
    #[serde(default)]
    pub result: String,
    /// Bug AH — hostname of the manager that originated the event. Helps
    /// operators understand which manager's scheduler / API handler
    /// produced an entry; mirrored into `summary` for display. Empty for
    /// legacy entries (where the originating manager is unrecoverable).
    #[serde(default)]
    pub origin_manager: String,
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
    /// Per-node scrub results, keyed by `node_id`.
    ///
    /// Storing one record per node ensures that a fast scrub on node B does
    /// not silently discard the result from node A (fix for issue #8 —
    /// previously only the cluster-wide "most recent" single result was kept).
    #[serde(default)]
    pub scrub_results: BTreeMap<u64, ScrubRecord>,

    /// Latest scrub result per `(node_id, level)` (BK). The legacy
    /// `scrub_results` map above is single-per-node, so a deep scrub
    /// overwrites the quick result (and vice versa). This nested map
    /// retains the latest of each level: outer key = node_id, inner key =
    /// level (`"quick"` / `"deep"`). The legacy map is kept populated with
    /// the node's most-recent record for one release of snapshot
    /// back-compat (an older manager binary mid-roll still renders it).
    #[serde(default)]
    pub scrub_results_by_level: BTreeMap<u64, BTreeMap<String, ScrubRecord>>,

    /// CF-8 — latest manual scrub trigger, replicated so the heartbeat
    /// handler on ANY manager relays it to data nodes (they heartbeat
    /// to whichever manager they connected to — Bug CC lesson).
    /// Replaced by each new trigger, never cleared; data nodes dedupe
    /// by `id` and ignore stale `requested_at_ms`, so permanent
    /// presence is harmless. `#[serde(default)]` per § A.3.1.
    #[serde(default)]
    pub pending_scrub: Option<crate::PendingScrub>,

    /// Number of registered FUSE clients (updated by client heartbeats).
    pub client_count: u64,

    /// Volume configurations, keyed by volume_id string.
    #[serde(default)]
    pub volumes: BTreeMap<String, VolumeConfig>,

    /// Migration-scheduler state, serialised as JSON.
    ///
    /// Kept in its own field instead of `cluster_config.config_json` so that
    /// the scheduler's internal bookkeeping never clobbers user-visible
    /// cluster configuration (fix for issue #3).
    #[serde(default)]
    pub migration_state_json: String,

    /// F-039: per-path distributed POSIX byte-range locks. Each entry is
    /// the live lock set for one file. Expired entries are evicted lazily
    /// on the next AcquireLock/ReleaseLock call referencing that path.
    #[serde(default)]
    pub posix_locks: BTreeMap<String, Vec<DistributedLock>>,

    /// P0-1: exclusive single-writer leases. One entry per path; presence
    /// blocks other writers from opening for write. Expired entries are
    /// reaped by a manager-side GC task that proposes `SweepExpiredLeases`
    /// every 1s; FUSE clients renew at half-life during flush.
    #[serde(default)]
    pub write_leases: BTreeMap<String, WriteLease>,
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
            scrub_results: BTreeMap::new(),
            scrub_results_by_level: BTreeMap::new(),
            pending_scrub: None,
            client_count: 0,
            volumes: BTreeMap::new(),
            migration_state_json: String::new(),
            posix_locks: BTreeMap::new(),
            write_leases: BTreeMap::new(),
        }
    }
}

/// One entry returned by [`StateMachineData::list_directory_entries`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedEntry {
    pub name: String,
    pub is_directory: bool,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub size_bytes: u64,
}

impl StateMachineData {
    /// List the entries in a directory.
    ///
    /// `prefix_path` is the directory path with or without a trailing slash;
    /// the helper normalises it. Three behaviours that fix Bug E from
    /// `plans/incidents/SOAK-RECOVERY-2026-05-18.md`:
    ///
    /// 1. If both an explicit mkdir entry (`/foo`) AND child files
    ///    (`/foo/bar.mkv`) exist, the returned entry is always reported as
    ///    `is_directory = true` — independent of BTreeMap iteration order.
    /// 2. Directory entries report a placeholder size (4096) rather than
    ///    the sum of their children, so `ls -l` doesn't show a directory
    ///    as a 7.9 GB regular file.
    /// 3. Synthetic directories (no mkdir entry, only child paths) are
    ///    still listed correctly.
    pub fn list_directory_entries(&self, prefix_path: &str) -> Vec<ListedEntry> {
        let prefix = if prefix_path.ends_with('/') {
            prefix_path.to_string()
        } else {
            format!("{}/", prefix_path)
        };
        // Aggregate by first-path-component within the prefix.
        let mut by_name: BTreeMap<String, ListedEntry> = BTreeMap::new();

        for (path, meta) in self.files.range(prefix.clone()..) {
            if !path.starts_with(&prefix) {
                break;
            }
            let relative = &path[prefix.len()..];
            if relative.is_empty() {
                continue;
            }
            // Two flavours of "directory" contribution:
            //   (a) The relative has a `/` → this entry sits BELOW a
            //       subdirectory named by everything before the slash.
            //   (b) The relative has no `/` → this entry IS the named
            //       child; its `is_directory` field determines the type.
            let (name, is_dir, file_size) = match relative.find('/') {
                Some(slash) => (relative[..slash].to_string(), true, 0u64),
                None => {
                    // Sum child sizes by chunk_index so replicas don't
                    // multiply the reported size.
                    let mut sizes_by_index: BTreeMap<u64, u64> = BTreeMap::new();
                    for c in &meta.chunks {
                        let e = sizes_by_index.entry(c.chunk_index).or_insert(0);
                        *e = (*e).max(c.size_bytes);
                    }
                    let logical_size: u64 = meta
                        .logical_size
                        .unwrap_or_else(|| sizes_by_index.values().sum());
                    (relative.to_string(), meta.is_directory, logical_size)
                }
            };

            let updated = by_name.entry(name.clone()).or_insert(ListedEntry {
                name: name.clone(),
                is_directory: false,
                created_at_ms: meta.created_at_ms,
                updated_at_ms: meta.updated_at_ms,
                size_bytes: 0,
            });
            // OR semantics — once any contributor says it's a directory,
            // the aggregate is a directory.
            updated.is_directory = updated.is_directory || is_dir;
            updated.updated_at_ms = updated.updated_at_ms.max(meta.updated_at_ms);
            // Files: take the largest size we've seen for this name
            // (handles the case where children get aggregated).
            // Directories: leave size as 0, we override on the way out.
            if !is_dir {
                updated.size_bytes = updated.size_bytes.max(file_size);
            }
        }
        // Directories report a single-block placeholder so `ls -l` doesn't
        // claim the dir is a giant file.
        for entry in by_name.values_mut() {
            if entry.is_directory {
                entry.size_bytes = 4096;
            }
        }
        by_name.into_values().collect()
    }

    /// Look up a path, returning either the canonical FileMetadata or a
    /// synthesised one for directories that are *implied* by child paths.
    ///
    /// Canonical hits: `sm.files.get(path)` is `Some`.
    ///
    /// Synthetic hits: there is no entry at `path` itself, but there *is*
    /// at least one entry under the prefix `path + "/"`. In that case we
    /// return a synthesised `FileMetadata` with `is_directory = true` and
    /// `chunks = []` so the caller can reply with directory attributes.
    ///
    /// This is the canonical "does this path exist for posix purposes"
    /// helper — used by the manager's `get_file_locations` handler and
    /// the rest of the metadata RPCs to fix bugs A and B from
    /// `plans/incidents/SOAK-RECOVERY-2026-05-18.md`.
    pub fn lookup_path(&self, path: &str) -> Option<FileMetadata> {
        // 1. Direct hit.
        if let Some(meta) = self.files.get(path) {
            return Some(meta.clone());
        }
        // 2. mkdir-style entry stored at `path + "/"`. mkdir historically
        //    appends a trailing slash to signal directory semantics; the
        //    fix in `apply_request` normalises away the slash on insert,
        //    but we keep this fallback for snapshots taken before the
        //    fix landed.
        let with_slash = format!("{}/", path);
        if let Some(meta) = self.files.get(&with_slash) {
            return Some(FileMetadata {
                is_directory: true,
                ..meta.clone()
            });
        }
        // 3. Synthetic: any entry exists below this prefix → directory.
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{}/", path)
        };
        if self
            .files
            .range(prefix.clone()..)
            .next()
            .map(|(k, _)| k.starts_with(&prefix))
            .unwrap_or(false)
        {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            return Some(FileMetadata {
                path: path.to_string(),
                volume_id: String::new(),
                chunk_size: 0,
                chunks: Vec::new(),
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                symlink_target: None,
                logical_size: None,
                is_directory: true,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            });
        }
        None
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
    /// Directory used to persist snapshots (so membership + FSM survive restart).
    /// `None` means in-memory only (tests).
    snapshot_dir: Option<PathBuf>,
}

/// A snapshot that has been persisted (kept in memory for fast retrieval).
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: crate::SnapshotMeta,
    data: Vec<u8>,
}

/// File-on-disk format that pairs the snapshot meta with the raw bytes.
/// Stored as bincode inside `<snapshot_dir>/snapshot.bin`. Used to recover
/// the FSM (especially the Raft membership) across manager restarts —
/// without this, a manager that loses the last membership log entry to
/// compaction comes back as a member-less Learner with no leader (root
/// cause of F-024).
#[derive(Debug, Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: crate::SnapshotMeta,
    /// Raw bytes — the same payload `install_snapshot` produces / consumes.
    data: Vec<u8>,
}

impl StateMachineStore {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(StateMachineData::default())),
            snapshot: Arc::new(RwLock::new(None)),
            snapshot_dir: None,
        }
    }

    /// Open (or create) a state machine that persists snapshots under `dir`.
    /// If a previous snapshot exists at `<dir>/snapshot.json` it is restored
    /// into the FSM immediately so that membership and metadata survive
    /// process restart.
    pub fn open_at(dir: impl AsRef<Path>) -> Result<Self, io::Error> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let store = Self {
            data: Arc::new(RwLock::new(StateMachineData::default())),
            snapshot: Arc::new(RwLock::new(None)),
            snapshot_dir: Some(dir.clone()),
        };

        // Bug BP-2 — clean any stray `.tmp` from an interrupted write so a
        // partial temp can never be mistaken for a snapshot.
        let _ = std::fs::remove_file(dir.join("snapshot.bin.tmp"));

        let path = dir.join("snapshot.bin");
        if path.exists() {
            // Bug BP-2 — a snapshot truncated by a disk-full / crash mid-write
            // (0-byte or partial) must NOT hard-error the open (that wedged
            // the manager on `Read Snapshot EOF`, ISSUE-SCAN-2026-06-02 F3).
            // Treat an unreadable snapshot as absent: log, delete it, and
            // start blank — openraft log-catchup from a healthy peer brings
            // the node current.
            let decoded: Option<(PersistedSnapshot, StateMachineSnapshot)> =
                match std::fs::read(&path) {
                    Ok(bytes) => match bincode::deserialize::<PersistedSnapshot>(&bytes) {
                        Ok(persisted) => match serde_json::from_slice::<StateMachineSnapshot>(
                            &persisted.data,
                        ) {
                            Ok(snap) => Some((persisted, snap)),
                            Err(e) => {
                                tracing::warn!(error = %e, path = ?path,
                                    "Bug BP-2: snapshot inner payload corrupt; \
                                     discarding + catching up via log replay");
                                None
                            }
                        },
                        Err(e) => {
                            tracing::warn!(error = %e, path = ?path,
                                "Bug BP-2: snapshot envelope corrupt/truncated; \
                                 discarding + catching up via log replay");
                            None
                        }
                    },
                    Err(e) => {
                        tracing::warn!(error = %e, path = ?path,
                            "Bug BP-2: snapshot unreadable; discarding");
                        None
                    }
                };
            let Some((persisted, snap)) = decoded else {
                let _ = std::fs::remove_file(&path);
                return Ok(store);
            };
            let mut sm = store
                .data
                .try_write()
                .expect("fresh store, no contention");
            sm.last_applied_log = persisted.meta.last_log_id.clone();
            sm.last_membership = persisted.meta.last_membership.clone();
            sm.files = snap.files;
            sm.nodes = snap.nodes;
            sm.cluster_config = snap.cluster_config;
            sm.audit_log = snap.audit_log;
            sm.scrub_results = snap.scrub_results;
            sm.scrub_results_by_level = snap.scrub_results_by_level;
            migrate_scrub_results_by_level(&mut sm);
            sm.client_count = snap.client_count;
            sm.volumes = snap.volumes;
            sm.migration_state_json = snap.migration_state_json;
            sm.posix_locks = snap.posix_locks;
            sm.write_leases = snap.write_leases;
            // CO-4-11 — assign even when None so a reloaded snapshot is the
            // source of truth for pending_scrub on restart.
            sm.pending_scrub = snap.pending_scrub;
            drop(sm);
            let mut slot = store
                .snapshot
                .try_write()
                .expect("fresh store, no contention");
            *slot = Some(StoredSnapshot {
                meta: persisted.meta.clone(),
                data: persisted.data,
            });
            drop(slot);
            info!(
                "loaded persisted snapshot: {} (last_log_id={:?}, voters={})",
                persisted.meta.snapshot_id,
                persisted.meta.last_log_id,
                persisted.meta.last_membership.voter_ids().count(),
            );
        }

        Ok(store)
    }

    /// Read-only access to the current state (useful for queries).
    pub fn data(&self) -> &Arc<RwLock<StateMachineData>> {
        &self.data
    }

    /// Atomically write the latest snapshot (meta + bytes) to disk so the FSM
    /// survives manager restart (F-024).
    fn persist_snapshot_to_disk(
        dir: &Path,
        meta: &crate::SnapshotMeta,
        bytes: &[u8],
    ) -> Result<(), io::Error> {
        let persisted = PersistedSnapshot {
            meta: meta.clone(),
            data: bytes.to_vec(),
        };
        let buf = bincode::serialize(&persisted)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tmp = dir.join("snapshot.bin.tmp");
        let final_path = dir.join("snapshot.bin");
        // Bug BP-2 — crash/ENOSPC-safe write: fsync the tmp file BEFORE the
        // rename, then fsync the directory AFTER, so a power loss / disk-full
        // mid-write can only ever leave the old committed snapshot or a
        // stray `.tmp` — never a truncated `snapshot.bin`. (ISSUE-SCAN
        // 2026-06-02 F3: a disk-full snapshot write left a 0-byte
        // snapshot.bin that wedged the manager on `Read Snapshot EOF`.)
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &final_path)?;
        // fsync the directory so the rename itself is durable.
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
        debug!(
            "persisted snapshot to {:?}: id={} bytes={}",
            final_path,
            meta.snapshot_id,
            buf.len()
        );
        Ok(())
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
    snapshot_dir: Option<PathBuf>,
    last_snapshot: Arc<RwLock<Option<StoredSnapshot>>>,
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
            scrub_results: sm.scrub_results.clone(),
            scrub_results_by_level: sm.scrub_results_by_level.clone(),
            client_count: sm.client_count,
            volumes: sm.volumes.clone(),
            migration_state_json: sm.migration_state_json.clone(),
            posix_locks: sm.posix_locks.clone(),
            write_leases: sm.write_leases.clone(),
            pending_scrub: sm.pending_scrub.clone(),
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

        // Cache the freshly built snapshot in memory so `get_current_snapshot`
        // returns the up-to-date blob (leader needs this to send to followers).
        {
            let mut slot = self.last_snapshot.write().await;
            *slot = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data_bytes.clone(),
            });
        }

        // Persist to disk so a manager restart can reload membership + state
        // before the Raft log replay starts (F-024).
        if let Some(dir) = &self.snapshot_dir {
            if let Err(e) = StateMachineStore::persist_snapshot_to_disk(dir, &meta, &data_bytes) {
                warn!("failed to persist snapshot to disk: {}", e);
            }
        }

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
        DtfsSnapshotBuilder {
            data,
            snapshot_dir: self.snapshot_dir.clone(),
            last_snapshot: self.snapshot.clone(),
        }
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
            sm.scrub_results = snap_data.scrub_results;
            sm.scrub_results_by_level = snap_data.scrub_results_by_level;
            migrate_scrub_results_by_level(&mut sm);
            sm.client_count = snap_data.client_count;
            sm.volumes = snap_data.volumes;
            sm.migration_state_json = snap_data.migration_state_json;
            sm.posix_locks = snap_data.posix_locks;
            sm.write_leases = snap_data.write_leases;
            // CO-4-11 — assign even when None so an installed snapshot replaces
            // a follower's stale pending_scrub with the source's value.
            sm.pending_scrub = snap_data.pending_scrub;
        }

        // Store the snapshot blob for later retrieval.
        {
            let mut stored = self.snapshot.write().await;
            *stored = Some(StoredSnapshot {
                meta: meta.clone(),
                data: raw.clone(),
            });
        }

        // Persist to disk so this follower can restart cleanly (F-024).
        if let Some(dir) = &self.snapshot_dir {
            if let Err(e) = Self::persist_snapshot_to_disk(dir, meta, &raw) {
                warn!("failed to persist installed snapshot: {}", e);
            }
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

/// Push an audit entry and cap the log at `MAX_AUDIT_LOG_ENTRIES`.
///
/// Oldest entries are drained from the front when the cap is exceeded,
/// preventing unbounded memory growth (fix for issue #5).
fn push_audit_capped(log: &mut Vec<AuditEntry>, entry: AuditEntry) {
    log.push(entry);
    if log.len() > MAX_AUDIT_LOG_ENTRIES {
        let excess = log.len() - MAX_AUDIT_LOG_ENTRIES;
        log.drain(..excess);
    }
}

/// Bug BK — one-shot migration on snapshot restore: when the nested
/// `scrub_results_by_level` map is empty but the legacy single-per-node
/// `scrub_results` map has entries (a snapshot written by a pre-BK
/// manager binary), seed the nested map from the legacy records keyed by
/// each record's own `level`. Idempotent: a no-op once the nested map is
/// populated.
fn migrate_scrub_results_by_level(sm: &mut StateMachineData) {
    if !sm.scrub_results_by_level.is_empty() {
        return;
    }
    for (node_id, record) in &sm.scrub_results {
        sm.scrub_results_by_level
            .entry(*node_id)
            .or_default()
            .insert(record.level.clone(), record.clone());
    }
}

/// Bug BJ — material (identity/topology) equality for the RegisterNode
/// audit-dedup. Compares only the fields a genuine re-registration would
/// change; volatile fields (`last_seen_ms`, usage counters, `health`,
/// `quarantine`) are excluded because they tick on every registration
/// and would otherwise make every re-register look "changed" and defeat
/// the dedup. `quarantine`/`health` are owned by other apply paths
/// (QuarantineNode / UpdateNodeStatus), not RegisterNode, so excluding
/// them here cannot suppress a legitimate state change.
fn node_info_material_eq(a: &crate::NodeInfo, b: &crate::NodeInfo) -> bool {
    a.role == b.role
        && a.hostname == b.hostname
        && a.grpc_addr == b.grpc_addr
        && a.location == b.location
        && disks_identity_eq(&a.disks, &b.disks)
        && a.version == b.version
        && a.build_date == b.build_date
        && a.client_ip == b.client_ip
}

/// Bug BJ — compare disk *physical identity*, ignoring fields that churn
/// across a normal re-registration. A data node reports its disks on
/// every RegisterNode, and several DiskInfo fields are NOT stable
/// identity:
///   - `used_bytes` drifts as the node stores/serves data,
///   - `disk_id` is an enumeration index that can be reassigned, and
///   - `uuid` is regenerated by the data node on every boot (observed
///     live 2026-05-28: each restart produced fresh per-disk uuids), so
///     it is per-process, not persistent disk identity,
/// and the disk *order* in the Vec can vary between boots. Comparing the
/// whole `Vec<DiskInfo>` (or keying on uuid/disk_id) therefore flagged
/// every restart as "materially changed" and re-spammed the audit log.
///
/// The stable physical identity available here is the **mount path**
/// (+ tier). We match disks by `mount_path` order-independently and
/// compare only the durable fields. Disk add/remove, retier, resize
/// (`total_bytes`), or a `status` flip (e.g. Failed) are still material.
/// (NOTE: the data node SHOULD persist disk uuids; its per-boot uuid
/// regeneration is a separate defect — see BJ plan deviations.)
fn disks_identity_eq(
    a: &[dtfs_common::types::DiskInfo],
    b: &[dtfs_common::types::DiskInfo],
) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut av: Vec<&dtfs_common::types::DiskInfo> = a.iter().collect();
    let mut bv: Vec<&dtfs_common::types::DiskInfo> = b.iter().collect();
    av.sort_by(|x, y| x.mount_path.cmp(&y.mount_path));
    bv.sort_by(|x, y| x.mount_path.cmp(&y.mount_path));
    av.iter().zip(bv.iter()).all(|(x, y)| {
        x.mount_path == y.mount_path
            && x.tier == y.tier
            && x.total_bytes == y.total_bytes
            && x.status == y.status
    })
}

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
        // Bug AH — Raft state-machine-internal entries don't carry the
        // structured fields; the summary IS the description. Fan-out
        // entries from manager-local AuditLog populate these via the
        // `AppendAudit` variant below.
        actor: String::new(),
        action: String::new(),
        target: String::new(),
        result: String::new(),
        origin_manager: String::new(),
    };
    // Bug BH-9 — a state-machine-internal audit row that DOES carry the
    // structured fields, so the integrity feed renders distinct TARGET
    // vs RESULT/SUMMARY columns instead of `backfill_audit_fields`
    // splitting one summary string into both. Shares the log_index /
    // timestamp bookkeeping with `audit` above.
    let audit_structured =
        |action: &str, target: String, result: String, summary: String| AuditEntry {
            log_index: log_id.index,
            summary,
            timestamp_ms: now_ms,
            actor: "system".to_string(),
            action: action.to_string(),
            target,
            result,
            origin_manager: String::new(),
        };

    match req {
        ClientRequest::CreateFile {
            path,
            volume_id,
            chunk_size,
        } => {
            // Bug A — normalize the storage key. FUSE's `mkdir` historically
            // signals "create a directory entry" by appending a trailing
            // slash. The SM used to store that verbatim under "/foo/", which
            // then made `getattr("/foo")` (the canonical lookup) return
            // ENOENT. We now strip the slash and set `is_directory = true`,
            // so the canonical key matches subsequent lookups while the
            // semantic intent is preserved on the entry.
            //
            // Regression test: posix_path_regression::
            //   bug_a_mkdir_trailing_slash_is_lookup_able_without_slash.
            let is_dir = path.ends_with('/') && path != "/";
            let canonical = if is_dir {
                path.trim_end_matches('/').to_string()
            } else {
                path.clone()
            };
            // CO-3-1 — CreateFile must apply deterministically: two replicas
            // (or a replay) applying the same log entry have to reach
            // byte-identical state, so the timestamps it stamps cannot come
            // from each machine's own `SystemTime::now()`. The CreateFile
            // command carries no proposer wall-clock (the eventual real fix is
            // to thread one in, like AcquireWriteLease.now_ms), so the
            // committed `log_id.index` -- identical on every replica -- is used
            // as a deterministic logical timestamp for the new entry + its
            // audit row. NOTE: an empty file therefore reports a small
            // (near-epoch) created/updated_at until the first
            // UpdateChunkLocations stamps a real wall-clock; this is the
            // accepted cost of making apply deterministic without a wire
            // change.
            let det_now = log_id.index;
            // CO-4-9 — CreateFile on an existing non-empty regular file must
            // NOT silently truncate it (a re-open / O_CREAT on an already-
            // populated path). Keep the existing entry intact -- chunks,
            // created_at_ms, chunk_size, volume_id, logical_size and cooldowns
            // all stay consistent with the on-disk layout -- and only refresh
            // updated_at_ms. Touching chunk_size/volume_id here would
            // re-map chunk_index<->byte-offset for the preserved chunks and
            // serve wrong bytes. A directory, symlink, or empty file is
            // (re)created fresh as before.
            let preserve = match sm.files.get(&canonical) {
                Some(existing) => {
                    !is_dir
                        && !existing.is_directory
                        && existing.symlink_target.is_none()
                        && !existing.chunks.is_empty()
                }
                None => false,
            };
            if preserve {
                let existing = sm.files.get_mut(&canonical).expect("checked above");
                existing.updated_at_ms = det_now;
            } else {
                sm.files.insert(
                    canonical.clone(),
                    FileMetadata {
                        path: canonical.clone(),
                        volume_id: volume_id.clone(),
                        chunk_size: *chunk_size,
                        chunks: Vec::new(),
                        created_at_ms: det_now,
                        updated_at_ms: det_now,
                        symlink_target: None,
                        logical_size: None,
                        is_directory: is_dir,
                        cache_cooldown_until_ms: std::collections::BTreeMap::new(),
                    },
                );
            }
            push_audit_capped(
                &mut sm.audit_log,
                AuditEntry {
                    log_index: log_id.index,
                    summary: format!(
                        "CreateFile: {canonical}{}",
                        if is_dir { " (directory)" } else { "" }
                    ),
                    timestamp_ms: det_now,
                    actor: String::new(),
                    action: String::new(),
                    target: String::new(),
                    result: String::new(),
                    origin_manager: String::new(),
                },
            );
            ClientResponse::FileCreated {
                path: canonical,
                volume_id: volume_id.clone(),
            }
        }

        ClientRequest::DeleteFile { path } => {
            sm.files.remove(path);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeleteFile: {path}")));
            ClientResponse::Success
        }

        ClientRequest::UpdateChunkLocations { path, chunks, lease_id } => {
            // Bug P — when the writer carries a lease_id, the manager
            // verifies it matches the currently-held lease. Pre-fix a
            // stale writer whose lease had been GC'd could still commit
            // chunks alongside a fresh writer's, interleaving bytes.
            // Empty lease_id preserves legacy callers' behaviour.
            // CO-4-15 — an empty lease_id must NOT let a foreign writer
            // introduce NEW chunk content while another writer holds the
            // lease. We reject only when the lease-less commit changes content
            // (some incoming `(chunk_index, chunk_hash)` is not already
            // present in the file). System-driven, lease-less replica
            // backfills (scheduler re-replication / tiering) only re-register
            // EXISTING `(index, hash)` pairs on new nodes, so they pass — they
            // must keep working while a client holds the write lease, or
            // under-replication loops forever (scheduler.rs Bug AB).
            if lease_id.is_empty() {
                if let Some(held) = sm.write_leases.get(path) {
                    let introduces_new_content = match sm.files.get(path) {
                        Some(file) => chunks.iter().any(|inc| {
                            !file.chunks.iter().any(|cur| {
                                cur.chunk_index == inc.chunk_index
                                    && cur.chunk_hash == inc.chunk_hash
                            })
                        }),
                        // No metadata yet => any incoming entry is new content.
                        None => !chunks.is_empty(),
                    };
                    if introduces_new_content {
                        warn!(
                            "UpdateChunkLocations REJECTED for {path}: empty lease_id introduces new content while a lease is held (owner={})",
                            held.owner_id
                        );
                        return ClientResponse::WriteLeaseConflict {
                            owner_id: held.owner_id.clone(),
                            expires_at_ms: held.expires_at_ms,
                        };
                    }
                }
            } else {
                match sm.write_leases.get(path) {
                    Some(held) if held.lease_id == *lease_id => {
                        // OK — same writer.
                    }
                    Some(held) => {
                        warn!(
                            "UpdateChunkLocations REJECTED for {path}: stale lease_id={lease_id} (current={})",
                            held.lease_id
                        );
                        return ClientResponse::WriteLeaseConflict {
                            owner_id: held.owner_id.clone(),
                            expires_at_ms: held.expires_at_ms,
                        };
                    }
                    None => {
                        warn!(
                            "UpdateChunkLocations REJECTED for {path}: lease_id={lease_id} but no lease currently held"
                        );
                        return ClientResponse::WriteLeaseConflict {
                            owner_id: String::new(),
                            expires_at_ms: 0,
                        };
                    }
                }
            }

            if let Some(file) = sm.files.get_mut(path) {
                file.chunks = merge_chunk_location_entries(&file.chunks, chunks);
                file.updated_at_ms = now_ms;
                push_audit_capped(&mut sm.audit_log, audit(format!(
                    "UpdateChunks: {path} (merged {} incoming -> {} total)",
                    chunks.len(),
                    file.chunks.len()
                )));
                ClientResponse::Success
            } else {
                warn!("UpdateChunkLocations: file not found: {path}");
                ClientResponse::Error {
                    message: format!("file not found: {path}"),
                }
            }
        }

        ClientRequest::RemoveCorruptChunkReplicas { node_id, chunk_hashes, disk_id } => {
            // Bug O — sweep every file's chunk list, dropping any entry
            // that matches (this node_id, one of these chunk_hashes).
            // The scheduler's check_under_replicated pass will then
            // queue re-replication from a healthy peer.
            //
            // CO-1-1 — replica identity is `(node_id, disk_id)`. The data
            // node reports corruption per disk, so drop only the corrupt
            // disk's copy; a healthy co-located copy on another disk/tier
            // of the same node MUST survive. `u32::MAX` (legacy / pre-CO-1-1
            // log entry) keeps the old node-wide sweep.
            let legacy_node_wide = *disk_id == crate::legacy_disk_id();
            let needle: std::collections::HashSet<&str> =
                chunk_hashes.iter().map(|s| s.as_str()).collect();
            let mut dropped: u64 = 0;
            let mut affected_files: u64 = 0;
            for (_path, file) in sm.files.iter_mut() {
                let before = file.chunks.len();
                file.chunks.retain(|c| {
                    !(c.node_id == *node_id
                        && needle.contains(c.chunk_hash.as_str())
                        && (legacy_node_wide || c.disk_id == *disk_id))
                });
                let after = file.chunks.len();
                if after < before {
                    dropped += (before - after) as u64;
                    affected_files += 1;
                    file.updated_at_ms = now_ms;
                }
            }
            push_audit_capped(
                &mut sm.audit_log,
                audit(format!(
                    "RemoveCorruptChunkReplicas: node={node_id} disk={disk_id} chunks={} dropped_entries={} affected_files={}",
                    chunk_hashes.len(),
                    dropped,
                    affected_files,
                )),
            );
            ClientResponse::Success
        }

        ClientRequest::RegisterNode { node_id, info } => {
            use dtfs_common::types::HealthStatus;
            // Bug BJ — audit only on a NEW node or a materially-changed
            // re-registration. node_id is deterministic
            // (node_id_from_hostname), so a re-register reuses the id and
            // overwrites sm.nodes with near-identical info; auditing it
            // unconditionally spammed the feed and evicted real events
            // against the MAX_AUDIT_LOG_ENTRIES cap. Still refresh
            // sm.nodes so last_seen_ms / usage update.
            //
            // RAFT-1 — the sole producer of RegisterNode (metadata.rs) always
            // builds `health: Healthy, quarantine: None`, so a re-register
            // (every process restart) wiped a Quarantined/Maintenance/
            // Draining node back to Healthy, bypassing BF-6 (only
            // UnquarantineNode may clear Quarantined) and re-entering the
            // node into read/allocate/replication pools with its integrity
            // fault unresolved. Carry the sticky state forward from `prior`
            // before inserting.
            let prior_state = sm.nodes.get(node_id).cloned();
            let mut info = info.clone();
            let mut sticky_state: Option<&'static str> = None;
            if let Some(p) = &prior_state {
                if p.quarantine.is_some() {
                    info.quarantine = p.quarantine.clone();
                    info.health = HealthStatus::Quarantined;
                    sticky_state = Some("Quarantined");
                } else if matches!(p.health, HealthStatus::Maintenance | HealthStatus::Draining) {
                    info.health = p.health.clone();
                    sticky_state = Some(if p.health == HealthStatus::Maintenance {
                        "Maintenance"
                    } else {
                        "Draining"
                    });
                }
            }
            let prior = sm.nodes.insert(*node_id, info.clone());
            let materially_changed = match &prior {
                None => true,
                Some(old) => !node_info_material_eq(old, &info),
            };
            if materially_changed {
                push_audit_capped(&mut sm.audit_log, audit(format!("RegisterNode: {node_id}")));
            }
            if let Some(state) = sticky_state {
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("RegisterNode: {node_id} (sticky state preserved: {state})")),
                );
            }
            ClientResponse::Success
        }

        ClientRequest::DeregisterNode { node_id } => {
            // Integrity (A.6 #5): never drop a node that still holds chunk
            // replicas. Removing it from sm.nodes would dangle its location
            // entries and silently lower the replication factor with no
            // re-replication — the operator must drain (migrate the replicas
            // off) first. The decommission API pre-checks the same, but the
            // apply is the single authoritative writer, so re-check here to
            // close the TOCTOU window.
            // ChunkLocationEntry.node_id is the stringified node id.
            let nid_str = node_id.to_string();
            let holds_replica = sm
                .files
                .values()
                .any(|f| f.chunks.iter().any(|c| c.node_id == nid_str));
            if holds_replica {
                ClientResponse::Error {
                    message: format!(
                        "DeregisterNode refused: node {node_id} still holds chunk replicas; drain/re-replicate first"
                    ),
                }
            } else if sm.nodes.remove(node_id).is_some() {
                push_audit_capped(&mut sm.audit_log, audit(format!("DeregisterNode: {node_id}")));
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("DeregisterNode: unknown node {node_id}"),
                }
            }
        }

        ClientRequest::UpdateNodeStatus { node_id, status } => {
            use dtfs_common::types::HealthStatus;
            if let Some(node) = sm.nodes.get_mut(node_id) {
                // Bug BF-6 — refuse to overwrite Quarantined via the
                // generic UpdateNodeStatus path. Only UnquarantineNode
                // (which also clears the quarantine record) can flip a
                // Quarantined node back to Healthy. Without this
                // guard a stale heartbeat from a manager that hasn't
                // applied the QuarantineNode log entry yet would
                // clobber the integrity-fault state.
                if node.quarantine.is_some() && status.as_str() != "down" && status.as_str() != "failed" {
                    debug!(
                        node_id,
                        attempted = %status,
                        "Bug BF-6: refusing UpdateNodeStatus on Quarantined node"
                    );
                    return ClientResponse::Success;
                }
                // Bug BN-2 — only audit a genuine health transition. The
                // health monitor + heartbeat path apply UpdateNodeStatus
                // ("healthy") on every cycle; auditing each one spammed the
                // log (~24 rows/min) and evicted real events against the cap.
                let old_health = node.health;
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
                if node.health != old_health {
                    push_audit_capped(
                        &mut sm.audit_log,
                        audit(format!(
                            "UpdateNodeStatus: {node_id} {old_health:?} -> {status}"
                        )),
                    );
                }
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("node not found: {node_id}"),
                }
            }
        }

        ClientRequest::QuarantineNode { node_id, info } => {
            use dtfs_common::types::HealthStatus;
            if let Some(node) = sm.nodes.get_mut(node_id) {
                node.health = HealthStatus::Quarantined;
                let hostname = node.hostname.clone();
                // Accumulate fault_count_total across faults instead of
                // resetting on every new quarantine — the WebUI badge
                // surfaces the running total so operators see whether
                // a node is repeatedly faulting.
                let prior_count = node.quarantine
                    .as_ref()
                    .map(|q| q.fault_count_total)
                    .unwrap_or(0);
                let mut merged = info.clone();
                merged.fault_count_total = prior_count + 1;
                node.quarantine = Some(merged);
                // Bug BH-9 — emit with structured fields so the integrity
                // feed shows node id + hostname in TARGET and the fault
                // detail (reason / chunk / fault_count) in RESULT/SUMMARY,
                // instead of `backfill_audit_fields` dumping the whole
                // string into both columns.
                let chunk = info
                    .triggering_chunk_id
                    .clone()
                    .unwrap_or_else(|| "-".into());
                let detail = format!(
                    "reason={:?} chunk={} fault_count={}",
                    info.reason,
                    chunk,
                    prior_count + 1,
                );
                push_audit_capped(
                    &mut sm.audit_log,
                    audit_structured(
                        "node.quarantine",
                        format!("{node_id} ({hostname})"),
                        detail.clone(),
                        format!("node.quarantine: {node_id} {detail}"),
                    ),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error { message: format!("node not found: {node_id}") }
            }
        }

        ClientRequest::UnquarantineNode { node_id, operator_id, reason_acknowledged } => {
            use dtfs_common::types::HealthStatus;
            if let Some(node) = sm.nodes.get_mut(node_id) {
                node.quarantine = None;
                node.health = HealthStatus::Healthy;
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!(
                        "node.unquarantine: {node_id} by={operator_id} ack={reason_acknowledged}"
                    )),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error { message: format!("node not found: {node_id}") }
            }
        }

        ClientRequest::UpdateNodeUsage {
            node_id,
            used_capacity,
            chunk_count: _chunk_count,
            total_bytes_read,
            total_bytes_written,
            observed_at_ms,
        } => {
            if let Some(node) = sm.nodes.get_mut(node_id) {
                node.used_capacity = *used_capacity;
                node.total_bytes_read = *total_bytes_read;
                node.total_bytes_written = *total_bytes_written;
                if *observed_at_ms > 0 {
                    node.last_seen_ms = *observed_at_ms;
                }
                // Bug BN-2 — no audit row. Usage counters + last_seen_ms are
                // pure telemetry (exposed live via /api/v1/nodes); data nodes
                // send this every 5 s, so auditing it flooded the log and
                // evicted genuine integrity events against the cap.
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
            expected_version,
        } => {
            // CO-5-10 — compare-and-set on the cluster-config version. The
            // proposer reads the current version, builds `version =
            // current + 1` and carries `expected_version = current`. Two
            // saves racing off the same base both carry the same
            // `expected_version`; only the first to apply matches the live
            // version, the second is rejected so its blind overwrite can't
            // silently clobber the winner. The `UPDATE_CONFIG_NO_CAS`
            // sentinel (a legacy/replayed entry pre-CO-5-10, serde default)
            // skips the check; a real base version (incl. 0, a fresh
            // cluster's first write) is always checked.
            if *expected_version != crate::UPDATE_CONFIG_NO_CAS
                && *expected_version != sm.cluster_config.version
            {
                return ClientResponse::Error {
                    message: format!(
                        "config version conflict: expected {expected_version}, \
                         current {}",
                        sm.cluster_config.version
                    ),
                };
            }
            sm.cluster_config = ClusterConfig {
                config_json: config_json.clone(),
                version: *version,
            };
            push_audit_capped(&mut sm.audit_log, audit(format!("UpdateConfig: v{version}")));
            ClientResponse::Success
        }

        ClientRequest::UpsertVolume { config } => {
            let vol_id = config.id.0.clone();
            push_audit_capped(&mut sm.audit_log, audit(format!("UpsertVolume: {vol_id}")));
            sm.volumes.insert(vol_id, config.clone());
            ClientResponse::Success
        }

        ClientRequest::DeleteVolume { volume_id } => {
            sm.volumes.remove(volume_id);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeleteVolume: {volume_id}")));
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
            // BK — store per (node, level) so a deep scrub does not
            // overwrite the quick result (and vice versa). The legacy
            // per-node map is kept populated with the node's most-recent
            // record for one release of snapshot back-compat (an older
            // manager binary mid-roll still renders something).
            sm.scrub_results_by_level
                .entry(*node_id)
                .or_default()
                .insert(level.clone(), record.clone());
            sm.scrub_results.insert(*node_id, record);
            push_audit_capped(&mut sm.audit_log, audit(format!(
                "ScrubResult: node={node_id} disk={disk_path} level={level} \
                 checked={chunks_checked} corrupt={chunks_corrupt} missing={chunks_missing}"
            )));
            ClientResponse::Success
        }

        ClientRequest::UpdateMigrationState { json } => {
            // Dedicated field — never touches cluster_config.config_json
            // (fix for issue #3: migration state used to clobber cluster config).
            sm.migration_state_json = json.clone();
            ClientResponse::Success
        }

        // ---- Extended POSIX surface (F-034 .. F-038) ----

        ClientRequest::RenameFile { from, to } => {
            if from == to {
                return ClientResponse::Success;
            }
            // RAFT-2 — a plain FUSE `rename_file(from, to)` used to move only
            // the exact `from` key; a directory's children (stored under
            // `from/...` keys, real or synthetic) were left stranded under
            // the old prefix. Renaming a directory into its own subtree, or
            // onto an existing non-empty directory, must be refused rather
            // than silently corrupt the tree.
            let from_prefix = format!("{from}/");
            if to.starts_with(&from_prefix) {
                return ClientResponse::Error {
                    message: format!(
                        "RenameFile: cannot rename '{from}' into its own subtree '{to}'"
                    ),
                };
            }
            let to_prefix = format!("{to}/");
            let to_has_children = sm
                .files
                .range(to_prefix.clone()..)
                .take_while(|(k, _)| k.starts_with(&to_prefix))
                .next()
                .is_some();
            if to_has_children {
                return ClientResponse::Error {
                    message: format!(
                        "RenameFile: destination '{to}' is a non-empty directory"
                    ),
                };
            }
            // Collect the child keys BEFORE mutating sm.files (BTreeMap
            // iteration must not overlap a removal/insertion into itself).
            let child_keys: Vec<String> = sm
                .files
                .range(from_prefix.clone()..)
                .take_while(|(k, _)| k.starts_with(&from_prefix))
                .map(|(k, _)| k.clone())
                .collect();
            let exact_source = sm.files.remove(from);
            if exact_source.is_none() && child_keys.is_empty() {
                return ClientResponse::Error {
                    message: format!("RenameFile: source '{from}' not found"),
                };
            }
            if let Some(mut meta) = exact_source {
                meta.path = to.clone();
                meta.updated_at_ms = now_ms;
                sm.files.insert(to.clone(), meta);
                // Migrate any active locks on the path too.
                if let Some(locks) = sm.posix_locks.remove(from) {
                    sm.posix_locks.insert(to.clone(), locks);
                }
                // CO-4-10 — the write lease must follow the rename so the
                // holder can keep committing chunks on the new path under the
                // same lease_id (mirrors the posix_locks migration above).
                // The destination's metadata was just replaced, so drop any
                // stale lease there first; then move the source's lease over
                // (if any). Renaming a leased source onto a leased dest is a
                // pathological collision -- the source writer wins, matching
                // the metadata that now lives at `to`.
                let moved = sm.write_leases.remove(from);
                match moved {
                    Some(lease) => {
                        sm.write_leases.insert(to.clone(), lease);
                    }
                    None => {
                        sm.write_leases.remove(to);
                    }
                }
            }
            // Re-key every child (real or a synthetic sub-directory's own
            // children) from `from/<suffix>` to `to/<suffix>`.
            let mut moved_children: u64 = 0;
            for old_key in child_keys {
                let suffix = &old_key[from.len()..];
                let new_key = format!("{to}{suffix}");
                if let Some(mut meta) = sm.files.remove(&old_key) {
                    meta.path = new_key.clone();
                    meta.updated_at_ms = now_ms;
                    sm.files.insert(new_key.clone(), meta);
                }
                if let Some(locks) = sm.posix_locks.remove(&old_key) {
                    sm.posix_locks.insert(new_key.clone(), locks);
                }
                match sm.write_leases.remove(&old_key) {
                    Some(lease) => {
                        sm.write_leases.insert(new_key.clone(), lease);
                    }
                    None => {
                        sm.write_leases.remove(&new_key);
                    }
                }
                moved_children += 1;
            }
            push_audit_capped(
                &mut sm.audit_log,
                audit(format!(
                    "RenameFile: {from} -> {to} ({moved_children} children moved)"
                )),
            );
            ClientResponse::Success
        }

        ClientRequest::SetFileSize { path, new_size } => {
            if let Some(meta) = sm.files.get_mut(path) {
                meta.logical_size = Some(*new_size);
                meta.updated_at_ms = now_ms;
                // Truncate-shrink: drop chunks fully past the new EOF.
                let chunk_size = meta.chunk_size.max(1);
                let keep_idx = ((*new_size).div_ceil(chunk_size)).saturating_sub(0);
                meta.chunks
                    .retain(|c| (c.chunk_index as u64) < keep_idx || *new_size == 0 && false);
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("SetFileSize: {path} -> {new_size}")),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("SetFileSize: '{path}' not found"),
                }
            }
        }

        ClientRequest::CreateSymlink { path, target } => {
            sm.files.insert(
                path.clone(),
                FileMetadata {
                    path: path.clone(),
                    volume_id: "default".to_string(),
                    chunk_size: 0,
                    chunks: Vec::new(),
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                    symlink_target: Some(target.clone()),
                    logical_size: Some(target.len() as u64),
                    is_directory: false,
                    cache_cooldown_until_ms: std::collections::BTreeMap::new(),
                },
            );
            push_audit_capped(
                &mut sm.audit_log,
                audit(format!("Symlink: {path} -> {target}")),
            );
            ClientResponse::Success
        }

        ClientRequest::CreateLink { source, dest } => {
            // Hard link: clone the source metadata under the dest path.
            // The chunk list is shared by value (chunk_ids are content-addressed
            // so the same bytes remain accessible from both names).
            if let Some(src_meta) = sm.files.get(source).cloned() {
                let mut new_meta = src_meta;
                new_meta.path = dest.clone();
                new_meta.created_at_ms = now_ms;
                new_meta.updated_at_ms = now_ms;
                sm.files.insert(dest.clone(), new_meta);
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("Link: {source} -> {dest}")),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("Link: source '{source}' not found"),
                }
            }
        }

        // ---- Distributed POSIX byte-range locks (F-039) ----

        ClientRequest::AcquireLock {
            path,
            owner_id,
            start,
            end,
            write,
            lease_ms,
            now_ms,
        } => {
            let table = sm.posix_locks.entry(path.clone()).or_default();
            // Evict expired entries first.
            table.retain(|l| l.expires_at_ms > *now_ms);
            // Check conflict.
            for l in table.iter() {
                if &l.owner_id == owner_id {
                    continue;
                }
                if *start >= l.end || *end <= l.start {
                    continue;
                }
                if l.write || *write {
                    return ClientResponse::LockConflict {
                        owner_id: l.owner_id.clone(),
                        start: l.start,
                        end: l.end,
                        write: l.write,
                    };
                }
            }
            // Drop any overlapping locks owned by the same owner so we
            // upgrade/extend cleanly.
            table.retain(|l| {
                !(l.owner_id == *owner_id && *start < l.end && l.start < *end)
            });
            table.push(DistributedLock {
                owner_id: owner_id.clone(),
                start: *start,
                end: *end,
                write: *write,
                expires_at_ms: now_ms.saturating_add(*lease_ms),
            });
            ClientResponse::LockGranted
        }

        ClientRequest::ReleaseLock {
            path,
            owner_id,
            start,
            end,
        } => {
            if let Some(table) = sm.posix_locks.get_mut(path) {
                table.retain(|l| {
                    !(l.owner_id == *owner_id && l.start >= *start && l.end <= *end)
                });
                if table.is_empty() {
                    sm.posix_locks.remove(path);
                }
            }
            ClientResponse::Success
        }

        // ---- P0-1: single-writer leases ----
        ClientRequest::AcquireWriteLease {
            path,
            owner_id,
            lease_ms,
            now_ms,
            proposed_lease_id,
        } => {
            // Evict expired entries (lazy GC; the manager also runs a 1s
            // ticker that proposes SweepExpiredLeases for active reaping).
            if let Some(existing) = sm.write_leases.get(path) {
                if existing.expires_at_ms <= *now_ms {
                    sm.write_leases.remove(path);
                }
            }

            // Check for live conflict.
            if let Some(existing) = sm.write_leases.get(path) {
                if existing.owner_id != *owner_id {
                    return ClientResponse::WriteLeaseConflict {
                        owner_id: existing.owner_id.clone(),
                        expires_at_ms: existing.expires_at_ms,
                    };
                }
                // Same owner re-acquiring: replace with refreshed expiry but
                // keep the existing lease_id so the caller's prior token
                // still validates (idempotency).
                let lease_id = existing.lease_id.clone();
                let new_expires = now_ms.saturating_add(*lease_ms);
                sm.write_leases.insert(
                    path.clone(),
                    WriteLease {
                        owner_id: owner_id.clone(),
                        lease_id: lease_id.clone(),
                        expires_at_ms: new_expires,
                    },
                );
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!(
                        "AcquireWriteLease (re-acquire): {path} owner={owner_id}"
                    )),
                );
                return ClientResponse::WriteLeaseGranted {
                    lease_id,
                    expires_at_ms: new_expires,
                };
            }

            // No conflict — grant fresh.
            let expires_at_ms = now_ms.saturating_add(*lease_ms);
            sm.write_leases.insert(
                path.clone(),
                WriteLease {
                    owner_id: owner_id.clone(),
                    lease_id: proposed_lease_id.clone(),
                    expires_at_ms,
                },
            );
            push_audit_capped(
                &mut sm.audit_log,
                audit(format!("AcquireWriteLease: {path} owner={owner_id}")),
            );
            ClientResponse::WriteLeaseGranted {
                lease_id: proposed_lease_id.clone(),
                expires_at_ms,
            }
        }

        ClientRequest::RenewWriteLease {
            path,
            lease_id,
            extension_ms,
            now_ms,
        } => {
            if let Some(existing) = sm.write_leases.get_mut(path) {
                if existing.lease_id != *lease_id {
                    return ClientResponse::WriteLeaseConflict {
                        owner_id: existing.owner_id.clone(),
                        expires_at_ms: existing.expires_at_ms,
                    };
                }
                if existing.expires_at_ms <= *now_ms {
                    // Lease already expired and was about to be GC'd; the
                    // GC may have already run. Treat as conflict so the
                    // caller re-acquires cleanly.
                    return ClientResponse::WriteLeaseConflict {
                        owner_id: existing.owner_id.clone(),
                        expires_at_ms: existing.expires_at_ms,
                    };
                }
                let new_expires = now_ms.saturating_add(*extension_ms);
                existing.expires_at_ms = new_expires;
                return ClientResponse::WriteLeaseGranted {
                    lease_id: lease_id.clone(),
                    expires_at_ms: new_expires,
                };
            }
            ClientResponse::WriteLeaseConflict {
                owner_id: String::new(),
                expires_at_ms: 0,
            }
        }

        ClientRequest::ReleaseWriteLease { path, lease_id } => {
            if let Some(existing) = sm.write_leases.get(path) {
                if existing.lease_id == *lease_id {
                    sm.write_leases.remove(path);
                    push_audit_capped(
                        &mut sm.audit_log,
                        audit(format!("ReleaseWriteLease: {path}")),
                    );
                    return ClientResponse::WriteLeasesReleased { count: 1 };
                }
            }
            ClientResponse::WriteLeasesReleased { count: 0 }
        }

        ClientRequest::ReleaseLeasesByHost {
            hostname_prefix,
            except_pid,
            now_ms,
        } => {
            // owner_id format is `<hostname>:<pid>:<fh>`. Match the hostname
            // prefix, exclude the caller's own pid, and — Bug CD — drop ONLY
            // leases that are already expired (`expires_at_ms <= now_ms`).
            // A live concurrent FUSE client renews at half-life, so its lease
            // is never expired and is never dropped here; a crashed/orphan
            // client stops renewing and lapses, so its lease is reaped. The
            // restarting writer's own not-yet-expired orphan on a given path
            // is handled by AcquireWriteLease's lazy expired-eviction. Pre-CD
            // this dropped EVERY non-caller lease on the host, nuking healthy
            // concurrent writers mid-write (their next commit hit
            // WriteLeaseConflict -> EIO).
            let prefix = format!("{hostname_prefix}:");
            let except_segment = format!(":{except_pid}:");
            let to_drop: Vec<String> = sm
                .write_leases
                .iter()
                .filter(|(_, l)| {
                    l.owner_id.starts_with(&prefix)
                        && !l.owner_id.contains(&except_segment)
                        && l.expires_at_ms <= *now_ms
                })
                .map(|(p, _)| p.clone())
                .collect();
            let count = to_drop.len() as u32;
            for p in &to_drop {
                sm.write_leases.remove(p);
            }
            if count > 0 {
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!(
                        "ReleaseLeasesByHost: host={hostname_prefix} except_pid={except_pid} count={count}"
                    )),
                );
            }
            ClientResponse::WriteLeasesReleased { count }
        }

        ClientRequest::SweepExpiredLeases { now_ms } => {
            let to_drop: Vec<String> = sm
                .write_leases
                .iter()
                .filter(|(_, l)| l.expires_at_ms <= *now_ms)
                .map(|(p, _)| p.clone())
                .collect();
            let count = to_drop.len() as u32;
            for p in &to_drop {
                sm.write_leases.remove(p);
            }
            if count > 0 {
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("SweepExpiredLeases: count={count}")),
                );
            }
            ClientResponse::WriteLeasesReleased { count }
        }

        ClientRequest::ReportChunkReads { .. } => {
            // Not state-machine state. The manager service intercepts this
            // variant in `forward_client_write` and applies it directly to the
            // leader's in-memory tiering engine; it must never reach Raft
            // apply. This arm exists only for match exhaustiveness.
            ClientResponse::Success
        }

        ClientRequest::TriggerScrub { id, level, requested_at_ms } => {
            // CF-8 — record (replace) the pending manual scrub request.
            // Every manager's heartbeat handler relays it; data nodes
            // dedupe by id, so replacement is the only cleanup needed.
            sm.pending_scrub = Some(crate::PendingScrub {
                id: id.clone(),
                level: level.clone(),
                requested_at_ms: *requested_at_ms,
            });
            ClientResponse::Success
        }

        ClientRequest::UpdateNodeDiskUsage { node_id, per_disk } => {
            // Bug AU — refresh per-disk total_bytes + used_bytes from
            // the heartbeat. Snapshots referencing an unknown disk_id
            // are dropped (the operator may have removed a disk since
            // the heartbeat was sent). Aggregate node-level totals are
            // recomputed from the updated disks so /api/v1/stats
            // tier_breakdown reflects fresh per-tier usage too.
            if let Some(info) = sm.nodes.get_mut(node_id) {
                for (disk_id, total, used) in per_disk {
                    if let Some(disk) = info.disks.iter_mut().find(|d| d.disk_id == *disk_id) {
                        disk.total_bytes = *total;
                        disk.used_bytes = *used;
                    }
                }
                info.total_capacity = info.disks.iter().map(|d| d.total_bytes).sum();
                info.used_capacity  = info.disks.iter().map(|d| d.used_bytes ).sum();
            }
            ClientResponse::Success
        }

        ClientRequest::AppendAudit {
            actor,
            action,
            target,
            result,
            origin_manager,
            timestamp_ms,
        } => {
            // Bug AH — fan-out from manager-local AuditLog. Build the
            // entry from the originator's structured fields; synthesise a
            // `summary` so legacy display paths (CLI's `dtfs-cli audit`,
            // raft-internal logs) keep working without code changes.
            let summary = if target.is_empty() {
                format!("{action} [{origin_manager}]")
            } else {
                format!("{action}: {target} [{origin_manager}]")
            };
            let entry = AuditEntry {
                log_index: log_id.index,
                summary,
                // Preserve the originator's wall-clock; Raft apply time is
                // typically very close but can lag by network RTT, and
                // operators want events ordered by when they happened at
                // the source.
                timestamp_ms: *timestamp_ms,
                actor: actor.clone(),
                action: action.clone(),
                target: target.clone(),
                result: result.clone(),
                origin_manager: origin_manager.clone(),
            };
            push_audit_capped(&mut sm.audit_log, entry);
            ClientResponse::Success
        }

        ClientRequest::RemoveChunkLocation {
            path,
            chunk_index,
            node_id,
            disk_id,
            enforce_anchor,
        } => {
            // Bug AV — operator-driven cache replica removal. Mutates
            // `FileMetadata.chunks` to drop a single entry, with an
            // optional anchor-strand check. CO-1-1 — keyed on the exact
            // (node_id, disk_id) replica; `u32::MAX` = legacy node-wide.
            apply_remove_chunk_location(
                sm,
                path,
                *chunk_index,
                node_id,
                *disk_id,
                enforce_anchor.as_ref(),
            )
        }

        ClientRequest::SetChunkCooldown {
            path,
            chunk_indexes,
            expires_at_ms,
        } => {
            // Bug AV — set/clear cache cooldown timestamps. Monotonic
            // (max of existing and new); 0 clears.
            apply_set_chunk_cooldown(sm, path, chunk_indexes, *expires_at_ms)
        }
    }
}

/// Bug AV — apply path for `SetChunkCooldown`. Factored out for testing.
/// `expires_at_ms == 0` clears the cooldown entry; any other value is
/// merged via max() so concurrent flushes monotonically extend.
fn apply_set_chunk_cooldown(
    sm: &mut StateMachineData,
    path: &str,
    chunk_indexes: &[u64],
    expires_at_ms: u64,
) -> ClientResponse {
    let Some(file) = sm.files.get_mut(path) else {
        // Idempotent: unknown file is a no-op success.
        return ClientResponse::Success;
    };
    for &idx in chunk_indexes {
        if expires_at_ms == 0 {
            file.cache_cooldown_until_ms.remove(&idx);
        } else {
            let existing = file.cache_cooldown_until_ms.get(&idx).copied().unwrap_or(0);
            // Monotonic: take the later expiry. An operator flush at
            // T+100 followed by another at T+50 must leave T+100,
            // not regress to T+50.
            file.cache_cooldown_until_ms.insert(idx, existing.max(expires_at_ms));
        }
    }
    ClientResponse::Success
}

/// Bug AV — apply path for `RemoveChunkLocation`, factored out for
/// testability + readability. Returns the same ClientResponse the
/// inline arm would.
fn apply_remove_chunk_location(
    sm: &mut StateMachineData,
    path: &str,
    chunk_index: u64,
    node_id: &str,
    disk_id: u32,
    enforce_anchor: Option<&AnchorPolicy>,
) -> ClientResponse {
    let file = match sm.files.get(path) {
        Some(f) => f,
        // Idempotent: unknown file → success no-op so a retry after
        // a CrashRestart doesn't spam errors.
        None => return ClientResponse::Success,
    };

    // CO-1-1 — replica identity is `(node_id, disk_id)` (A.3.4 / Bug BH).
    // A chunk may legally hold a mandatory replica AND a cache replica on
    // ONE node across DIFFERENT disks/tiers; matching on `node_id` alone
    // dropped BOTH. Match the exact disk when one is given; the legacy
    // sentinel (`u32::MAX`, from a pre-CO-1-1 snapshot/log entry) falls
    // back to the old node-wide match on `(chunk_index, node_id)`.
    let legacy_node_wide = disk_id == crate::legacy_disk_id();
    let matches = |c: &ChunkLocationEntry| -> bool {
        c.chunk_index == chunk_index
            && c.node_id == node_id
            && (legacy_node_wide || c.disk_id == disk_id)
    };

    // Check the entry is even present. Idempotent on absent.
    let entry_present = file.chunks.iter().any(matches);
    if !entry_present {
        return ClientResponse::Success;
    }

    // Anchor safety: compute the post-removal state for this
    // chunk_index and verify the anchor would still be satisfied by
    // at least one remaining replica.
    if let Some(policy) = enforce_anchor {
        let survives_anchor = file.chunks.iter().any(|c| {
            // The entry/entries we're about to drop don't count.
            if matches(c) {
                return false;
            }
            // Only same chunk_index entries are relevant — different
            // chunks aren't affected by removing this one.
            if c.chunk_index != chunk_index {
                return false;
            }
            // Resolve dc + tier via the node table.
            let nid: u64 = match c.node_id.parse() {
                Ok(n) => n,
                Err(_) => return false,
            };
            let Some(ninfo) = sm.nodes.get(&nid) else { return false };
            // REV-3-7 — a replica on a Down/Failed node, or on a
            // Failed/Removed disk, cannot satisfy the anchor: the bytes are
            // not servable. The scheduler's coverage counting
            // (`live_replica_tier`) has always said so; this survivor scan
            // used to disagree, which let a removal through on the strength
            // of a replica that no longer exists.
            if matches!(
                ninfo.health,
                dtfs_common::types::HealthStatus::Down | dtfs_common::types::HealthStatus::Failed
            ) {
                return false;
            }
            let dc = ninfo.location.datacenter.as_str();
            // CNF-1 — resolve by the stable disk_id key, never Vec index.
            let tier = ninfo.disk(c.disk_id).filter(|d| {
                !matches!(
                    d.status,
                    dtfs_common::types::DiskStatus::Failed
                        | dtfs_common::types::DiskStatus::Removed
                )
            }).map(|d| d.tier);
            if tier.is_none() {
                return false;
            }
            policy.is_satisfied_by(dc, tier)
        });
        if !survives_anchor {
            return ClientResponse::Error {
                message: format!(
                    "RemoveChunkLocation refused: would strand anchor for {path}#{chunk_index} \
                     (policy: dc={}{})",
                    policy.datacenter,
                    match policy.required_tier {
                        Some(t) => format!(", required_tier={t}"),
                        None => String::new(),
                    },
                ),
            };
        }
    }

    // Safe to drop. Mutate.
    let file_mut = sm.files.get_mut(path).expect("checked above");
    let before = file_mut.chunks.len();
    file_mut.chunks.retain(|c| !matches(c));
    let after = file_mut.chunks.len();
    // CO-1-1 — a disk-targeted removal drops EXACTLY the one (node,disk)
    // entry; only the legacy node-wide sentinel may drop several
    // co-located entries at once.
    debug_assert!(
        before.saturating_sub(after) == 1 || legacy_node_wide,
        "disk-targeted RemoveChunkLocation must drop exactly one entry"
    );
    file_mut.updated_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    ClientResponse::Success
}

// ---------------------------------------------------------------------------
// Tests — snapshot persistence (regression test for F-024)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod snapshot_persistence_tests {
    use super::*;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

    fn tmp_dir() -> tempfile::TempDir {
        tempfile::tempdir().expect("create tempdir")
    }

    /// Build a tiny FSM populated with some files + nodes and a synthetic
    /// `last_applied_log` so we can detect that a restart restores it.
    fn populate_sample_state(data: &mut StateMachineData) {
        use openraft::vote::RaftLeaderId;
        let leader_id =
            openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        let log_id = openraft::LogId::new(leader_id, 100);
        data.last_applied_log = Some(log_id);
        data.files.insert(
            "/a.bin".into(),
            FileMetadata {
                path: "/a.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            },
        );
        data.cluster_config.version = 7;
        data.cluster_config.config_json = "{\"v\":7}".into();
    }

    #[tokio::test]
    async fn build_snapshot_persists_and_reopen_restores_state() {
        let dir = tmp_dir();
        let path = dir.path().to_path_buf();

        // Build a state machine, populate it, and produce a snapshot.
        {
            let mut sm = StateMachineStore::open_at(&path).expect("open");
            {
                let mut data = sm.data.write().await;
                populate_sample_state(&mut data);
            }
            let mut builder = sm.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.expect("build");
            assert!(snap.meta.last_log_id.is_some());
        }

        // Disk file should now exist.
        let on_disk = path.join("snapshot.bin");
        assert!(
            on_disk.exists(),
            "snapshot.bin must be written to disk so restart can restore FSM"
        );

        // Reopen with a fresh store at the same path — should recover state.
        let restored = StateMachineStore::open_at(&path).expect("reopen");
        let data = restored.data.read().await;
        assert_eq!(
            data.files.len(),
            1,
            "files BTreeMap restored from snapshot"
        );
        assert!(
            data.files.contains_key("/a.bin"),
            "the specific file we created is back"
        );
        assert_eq!(data.cluster_config.version, 7, "cluster config restored");
        assert!(
            data.last_applied_log.is_some(),
            "last_applied_log restored — without this Raft skips log replay and stays empty"
        );
    }

    #[tokio::test]
    async fn open_at_empty_dir_is_blank_state_machine() {
        let dir = tmp_dir();
        let sm = StateMachineStore::open_at(dir.path()).expect("open empty");
        let data = sm.data.read().await;
        assert!(data.files.is_empty());
        assert!(data.last_applied_log.is_none());
    }

    /// Bug BP-2 — a truncated/0-byte `snapshot.bin` (disk-full mid-write)
    /// must NOT hard-error `open_at`; it is treated as absent, removed, and
    /// the store starts blank so openraft catches up via log replay.
    #[tokio::test]
    async fn bp_2_truncated_snapshot_treated_as_absent() {
        let dir = tmp_dir();
        // Write a 0-byte snapshot.bin (the exact F3 corruption shape).
        std::fs::write(dir.path().join("snapshot.bin"), b"").expect("write empty");
        let sm = StateMachineStore::open_at(dir.path())
            .expect("open must NOT error on a truncated snapshot");
        {
            let data = sm.data.read().await;
            assert!(data.files.is_empty(), "blank state after discarding corrupt snapshot");
        }
        // The corrupt file is removed so it cannot wedge subsequent opens.
        assert!(
            !dir.path().join("snapshot.bin").exists(),
            "corrupt snapshot.bin must be deleted"
        );
        // A partial (non-bincode) payload is likewise tolerated.
        std::fs::write(dir.path().join("snapshot.bin"), b"\x00\x01garbage").expect("write junk");
        assert!(
            StateMachineStore::open_at(dir.path()).is_ok(),
            "partial snapshot envelope tolerated"
        );
    }

    /// Bug BP-2 — a stray `snapshot.bin.tmp` left by an interrupted write is
    /// ignored on open and does not become the active snapshot.
    #[tokio::test]
    async fn bp_2_stray_tmp_ignored_on_open() {
        let dir = tmp_dir();
        std::fs::write(dir.path().join("snapshot.bin.tmp"), b"partial").expect("write tmp");
        let sm = StateMachineStore::open_at(dir.path()).expect("open ignores .tmp");
        {
            let data = sm.data.read().await;
            assert!(data.files.is_empty());
        }
        assert!(
            !dir.path().join("snapshot.bin.tmp").exists(),
            "stray .tmp is cleaned up on open"
        );
    }

    #[tokio::test]
    async fn install_snapshot_persists_to_disk() {
        let dir = tmp_dir();

        // Source SM produces a snapshot.
        let snapshot_bytes;
        let snapshot_meta;
        {
            let mut src = StateMachineStore::open_at(dir.path()).expect("open src");
            {
                let mut data = src.data.write().await;
                populate_sample_state(&mut data);
            }
            let mut builder = src.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.expect("build");
            snapshot_meta = snap.meta.clone();
            snapshot_bytes = snap.snapshot.into_inner();
        }

        // A fresh SM in a different dir simulates a follower receiving the snapshot.
        let follower_dir = tmp_dir();
        let mut follower = StateMachineStore::open_at(follower_dir.path()).expect("open follower");
        follower
            .install_snapshot(&snapshot_meta, Cursor::new(snapshot_bytes))
            .await
            .expect("install");

        // The follower must now have the file *and* must have written
        // snapshot.bin so its own restart works.
        let path = follower_dir.path().join("snapshot.bin");
        assert!(path.exists(), "install_snapshot must persist to disk");

        // Closing and reopening the follower restores the state.
        drop(follower);
        let reopened = StateMachineStore::open_at(follower_dir.path()).expect("reopen follower");
        let data = reopened.data.read().await;
        assert_eq!(data.files.len(), 1);
        assert_eq!(data.cluster_config.version, 7);
    }

    /// Bug AZ-3 — `FileMetadata.chunk_size` is per-file and snapshotted at
    /// `CreateFile` time. An operator UpsertVolume call that changes
    /// `VolumeConfig.chunk_size_bytes` MUST NOT retro-modify any existing
    /// `FileMetadata.chunk_size`. The read path resolves chunk indices
    /// against the per-file value
    /// (`dtfs-fuse/src/filesystem.rs::resolve_chunk_size`), so leaving
    /// FileMetadata alone is the load-bearing invariant — without it,
    /// files written before the resize become unreadable because
    /// `offset / chunk_size` produces an out-of-range chunk_index.
    #[tokio::test]
    async fn bug_az_3_volume_chunk_size_change_does_not_touch_existing_file() {
        use dtfs_common::types::{
            CacheReplication, TierReplication, VolumeConfig, VolumeId, WriteSafety,
        };
        use openraft::vote::RaftLeaderId;

        fn mk_log_id(idx: u64) -> LogId {
            let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
            openraft::LogId::new(leader, idx)
        }

        let dir = tempfile::TempDir::new().expect("tmpdir");
        let store = StateMachineStore::open_at(dir.path()).expect("open sm");
        let mut sm = store.data.write().await;

        let volume = VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            schedules: dtfs_common::types::VolumeSchedules::default(),
            spill: dtfs_common::types::SpillPolicy::default(),
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
        };
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::UpsertVolume { config: volume.clone() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::CreateFile {
                path: "/foo.bin".to_string(),
                volume_id: "default".to_string(),
                chunk_size: 16 * 1024 * 1024,
            },
        );

        let mut shrunk = volume.clone();
        shrunk.chunk_size_bytes = 4 * 1024 * 1024;
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::UpsertVolume { config: shrunk },
        );

        let file = sm.files.get("/foo.bin").expect("file persisted");
        assert_eq!(
            file.chunk_size,
            16 * 1024 * 1024,
            "FileMetadata.chunk_size must NOT track volume.chunk_size_bytes; \
             the read path uses the per-file value to compute chunk indices",
        );
        let vol = sm.volumes.get("default").expect("volume present");
        assert_eq!(vol.chunk_size_bytes, 4 * 1024 * 1024);
    }

    /// Bug AZ-3 — the inverse: a NEW file created after the resize picks up
    /// the NEW volume.chunk_size_bytes. Pins the contract that the
    /// CreateFile caller (FUSE / dtfs-cli) reads volume.chunk_size_bytes
    /// at write time and snapshots it onto the file.
    #[tokio::test]
    async fn bug_az_3_new_file_after_resize_uses_new_chunk_size() {
        use dtfs_common::types::{
            CacheReplication, TierReplication, VolumeConfig, VolumeId, WriteSafety,
        };
        use openraft::vote::RaftLeaderId;

        fn mk_log_id(idx: u64) -> LogId {
            let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
            openraft::LogId::new(leader, idx)
        }

        let dir = tempfile::TempDir::new().expect("tmpdir");
        let store = StateMachineStore::open_at(dir.path()).expect("open sm");
        let mut sm = store.data.write().await;

        let mut volume = VolumeConfig {
            id: VolumeId::new("default".to_string()),
            name: "default".to_string(),
            chunk_size_bytes: 16 * 1024 * 1024,
            replication: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            mandatory_replicas: TierReplication { hot: 0, warm: 0, cold: 1, tape: 0 },
            cache_replicas: CacheReplication::default(),
            anchor: None,
            schedules: dtfs_common::types::VolumeSchedules::default(),
            spill: dtfs_common::types::SpillPolicy::default(),
            anchor_datacenter: None,
            write_safety: WriteSafety {
                min_replicas_before_ack: 1,
                write_timeout_secs: 30,
            },
        };
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::UpsertVolume { config: volume.clone() },
        );

        volume.chunk_size_bytes = 4 * 1024 * 1024;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UpsertVolume { config: volume.clone() },
        );

        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::CreateFile {
                path: "/post-resize.bin".to_string(),
                volume_id: "default".to_string(),
                chunk_size: 4 * 1024 * 1024,
            },
        );

        let file = sm.files.get("/post-resize.bin").expect("file persisted");
        assert_eq!(
            file.chunk_size,
            4 * 1024 * 1024,
            "post-resize file must use the NEW chunk_size_bytes"
        );
    }
}

// ---------------------------------------------------------------------------
// Regression tests for three live-lab bugs uncovered during the 2026-05-18
// soak recovery (see plans/incidents/SOAK-RECOVERY-2026-05-18.md):
//
//   Bug A — empty `mkdir` doesn't persist as a lookup-able directory. FUSE
//           sends `CreateFile("/foo/")` (with a trailing slash to signal
//           "directory") but the state machine stores the entry verbatim
//           under key `"/foo/"`. Subsequent `getattr("/foo")` (no slash)
//           returns ENOENT because the canonical lookup key doesn't match.
//
//   Bug B — synthetic directories from existing file paths are not
//           lookup-able. `readdir("/")` correctly shows "foo" as a directory
//           when `sm.files` contains "/foo/bar.txt", but `lookup("/foo")`
//           returns ENOENT because no entry literally exists at "/foo".
//           Symptom: rsync mkdirs a folder, writes a file, then mkstemp()
//           in the same folder fails because the parent dir "disappears".
//
//   Bug C — rename of a freshly-created (chunkless) file is lost. FUSE
//           creates "/dir/.tmp" via `CreateFile`, writes data through
//           `UpdateChunkLocations`, then `RenameFile("/dir/.tmp",
//           "/dir/final")`. Without bug A's fix, the directory the file
//           lives in is fragile; the rename has been observed to leave the
//           cluster in a state where neither the old nor the new key is
//           lookup-able. The fix is verifiable with a pure state-machine
//           round-trip.
//
// All three tests are written to FAIL against the current code so the fix
// is verifiable. They are kept in their own module so the existing
// snapshot tests are unaffected.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod posix_path_regression {
    use super::*;

    fn mk_log_id(idx: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, idx)
    }

    /// CF-8 — TriggerScrub apply records the request; a later trigger
    /// replaces it (replacement is the only cleanup the design needs).
    #[test]
    fn cf8_trigger_scrub_sets_and_replaces_pending() {
        let mut sm = StateMachineData::default();
        assert!(sm.pending_scrub.is_none());
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::TriggerScrub {
                id: "t1".into(),
                level: "quick".into(),
                requested_at_ms: 111,
            },
        );
        let p = sm.pending_scrub.clone().expect("pending set");
        assert_eq!(
            (p.id.as_str(), p.level.as_str(), p.requested_at_ms),
            ("t1", "quick", 111)
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::TriggerScrub {
                id: "t2".into(),
                level: "deep".into(),
                requested_at_ms: 222,
            },
        );
        assert_eq!(sm.pending_scrub.expect("replaced").id, "t2");
    }

    /// CF-8 / § A.3.1 — a snapshot written before `pending_scrub`
    /// existed must load with the field defaulting to None.
    #[test]
    fn cf8_pending_scrub_old_snapshot_defaults_none() {
        let sm = StateMachineData::default();
        let mut v = serde_json::to_value(&sm).expect("serialize");
        v.as_object_mut().unwrap().remove("pending_scrub"); // pre-CF-8 shape
        let restored: StateMachineData =
            serde_json::from_value(v).expect("old snapshot loads");
        assert!(restored.pending_scrub.is_none());
    }

    /// CO-3-1 — CreateFile stamps a deterministic logical timestamp
    /// (`log_id.index`) into the new entry + its audit row, NOT each
    /// replica's wall-clock, so the same log entry yields byte-identical
    /// state on every replica and across replay.
    #[test]
    fn co_3_1_create_file_timestamps_are_log_index_deterministic() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(7),
            &ClientRequest::CreateFile {
                path: "/det.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let meta = sm.files.get("/det.bin").expect("created");
        assert_eq!(meta.created_at_ms, 7);
        assert_eq!(meta.updated_at_ms, 7);
        let entry = sm.audit_log.last().expect("audit pushed");
        assert_eq!(entry.timestamp_ms, 7);
        assert_eq!(entry.log_index, 7);
    }

    /// Bug CD — `ReleaseLeasesByHost` must reap only EXPIRED host leases. A
    /// live concurrent FUSE client (same host, different pid, lease still
    /// valid because it renews at half-life) MUST keep its lease; only the
    /// orphan (expired) lease is dropped. Pre-CD the apply dropped every
    /// non-caller lease on the host and nuked the live writer mid-write.
    #[test]
    fn bug_cd_release_by_host_spares_live_peer() {
        let mut sm = StateMachineData::default();
        let now_ms = 1_000_000u64;
        // Live concurrent writer on the same host, different pid, unexpired.
        sm.write_leases.insert(
            "/media/live.mkv".into(),
            WriteLease {
                owner_id: "toro-plex:222:7".into(),
                lease_id: "live".into(),
                expires_at_ms: now_ms + 10_000,
            },
        );
        // Orphan from a crashed prior process on the same host: expired.
        sm.write_leases.insert(
            "/media/orphan.mkv".into(),
            WriteLease {
                owner_id: "toro-plex:111:3".into(),
                lease_id: "orphan".into(),
                expires_at_ms: now_ms - 1,
            },
        );

        let resp = apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::ReleaseLeasesByHost {
                hostname_prefix: "toro-plex".into(),
                except_pid: 999, // the caller's own (new) pid; matches neither
                now_ms,
            },
        );

        assert!(
            matches!(resp, ClientResponse::WriteLeasesReleased { count: 1 }),
            "exactly one (expired orphan) lease should be released, got {resp:?}"
        );
        assert!(
            sm.write_leases.contains_key("/media/live.mkv"),
            "live concurrent writer's lease must NOT be dropped"
        );
        assert!(
            !sm.write_leases.contains_key("/media/orphan.mkv"),
            "expired orphan lease must be reaped"
        );
    }

    /// Bug A — `mkdir("/foo")` arrives at the state machine as
    /// `CreateFile { path: "/foo/" }`. After apply, the directory MUST be
    /// looked-up-able under the canonical key `"/foo"` and MUST advertise
    /// itself as a directory.
    #[test]
    fn bug_a_mkdir_trailing_slash_is_lookup_able_without_slash() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::CreateFile {
                path: "/corpus/".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );

        // Canonical lookup key must exist (no trailing slash).
        assert!(
            sm.files.contains_key("/corpus"),
            "directory must be findable by canonical path /corpus (got keys: {:?})",
            sm.files.keys().collect::<Vec<_>>(),
        );
        let meta = sm.files.get("/corpus").expect("just asserted");
        assert!(
            meta.is_directory,
            "FileMetadata for an empty mkdir must set is_directory=true"
        );
        assert!(
            meta.chunks.is_empty(),
            "directory entry has no chunks"
        );
    }

    /// Bug A (corollary) — a regular file create must NOT be marked as a
    /// directory, even though it also has empty chunks at create time.
    #[test]
    fn bug_a_regular_create_is_not_a_directory() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::CreateFile {
                path: "/foo/bar.txt".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let meta = sm
            .files
            .get("/foo/bar.txt")
            .expect("file must exist at its canonical path");
        assert!(
            !meta.is_directory,
            "regular file must not be tagged as a directory"
        );
    }

    /// Bug B — readdir on "/foo" should be lookup-able once any file
    /// exists under that prefix, even if no explicit mkdir was ever done.
    /// This models rsync's pattern: it writes ".plexmatch" directly under
    /// "/series/" without an mkdir, then tries to mkstemp another file in
    /// the same "directory" and the parent lookup fails.
    ///
    /// The state machine exposes a `lookup_path` helper (added by the fix)
    /// that returns Some(...) for canonical paths AND for synthetic
    /// directories implied by existing file paths.
    #[test]
    fn bug_b_synthetic_directory_is_lookup_able() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::CreateFile {
                path: "/series/episode-01.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );

        // The episode file itself looks up fine.
        let f = sm
            .lookup_path("/series/episode-01.mkv")
            .expect("explicit file path looks up");
        assert!(!f.is_directory);

        // The implicit parent directory MUST also be lookup-able.
        let dir = sm
            .lookup_path("/series")
            .expect("synthetic parent directory must lookup, but it returned None");
        assert!(
            dir.is_directory,
            "synthetic parent must report is_directory=true"
        );
    }

    /// Bug C — rename of a freshly-created file (with chunks already
    /// recorded via UpdateChunkLocations) must atomically move the
    /// metadata to the new key, and the old key must no longer be present.
    #[test]
    fn bug_c_rename_after_create_moves_metadata_to_new_key() {
        let mut sm = StateMachineData::default();
        // Step 1: rsync mkstemp -- create the temp file.
        apply_request(
            &mut sm,
            &mk_log_id(4),
            &ClientRequest::CreateFile {
                path: "/movies/.foo.GQhlto".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );
        // Step 2: rsync writes data; chunk locations land via UpdateChunkLocations.
        apply_request(
            &mut sm,
            &mk_log_id(5),
            &ClientRequest::UpdateChunkLocations {
                path: "/movies/.foo.GQhlto".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "abc123".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: String::new(),
            },
        );
        // Sanity: the temp file is fully formed.
        let before = sm
            .files
            .get("/movies/.foo.GQhlto")
            .expect("temp file present before rename");
        assert_eq!(before.chunks.len(), 1);

        // Step 3: rsync renames temp -> final.
        let resp = apply_request(
            &mut sm,
            &mk_log_id(6),
            &ClientRequest::RenameFile {
                from: "/movies/.foo.GQhlto".into(),
                to: "/movies/foo.mkv".into(),
            },
        );
        assert!(
            matches!(resp, ClientResponse::Success),
            "rename must succeed for a freshly-created+written file"
        );

        // Old key is gone; new key exists with the same chunks; the
        // FileMetadata.path field reflects the new name.
        assert!(
            !sm.files.contains_key("/movies/.foo.GQhlto"),
            "old key must be removed after rename"
        );
        let after = sm
            .files
            .get("/movies/foo.mkv")
            .expect("new key must exist after rename");
        assert_eq!(after.path, "/movies/foo.mkv");
        assert_eq!(
            after.chunks.len(),
            1,
            "chunk locations carried over by rename"
        );
        assert_eq!(after.chunks[0].chunk_hash, "abc123");

        // And the parent directory is still lookup-able (Bug B linkage —
        // synthetic dirs survive a rename of any child).
        let parent = sm
            .lookup_path("/movies")
            .expect("parent dir lookup after rename");
        assert!(parent.is_directory);
    }

    /// DeregisterNode integrity guard: a node that still holds chunk replicas
    /// MUST NOT be removed (would dangle its location entries and silently
    /// lower the replication factor). Once drained it deregisters cleanly;
    /// an unknown node is a no-op Error.
    #[test]
    fn deregister_node_refuses_while_holding_replicas_then_succeeds_when_drained() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 10, info: mk_data_node_with_disk(0) },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::CreateFile {
                path: "/f.bin".into(),
                volume_id: "default".into(),
                chunk_size: 4 * 1024 * 1024,
            },
        );
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::UpdateChunkLocations {
                path: "/f.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h0".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: String::new(),
            },
        );

        // Refused while node 10 still holds the replica.
        let refused =
            apply_request(&mut sm, &mk_log_id(4), &ClientRequest::DeregisterNode { node_id: 10 });
        match refused {
            ClientResponse::Error { message } => {
                assert!(message.contains("still holds chunk replicas"), "got: {message}")
            }
            other => panic!("expected Error while holding replicas, got {other:?}"),
        }
        assert!(sm.nodes.contains_key(&10), "node must remain while it holds replicas");

        // Drain the replica, then deregister succeeds and removes the node.
        apply_request(
            &mut sm,
            &mk_log_id(5),
            &ClientRequest::RemoveCorruptChunkReplicas {
                node_id: "10".into(),
                chunk_hashes: vec!["h0".into()],
                disk_id: crate::legacy_disk_id(),
            },
        );
        let ok =
            apply_request(&mut sm, &mk_log_id(6), &ClientRequest::DeregisterNode { node_id: 10 });
        assert!(matches!(ok, ClientResponse::Success), "drained node must deregister");
        assert!(!sm.nodes.contains_key(&10), "node removed after deregister");

        // Unknown node -> Error (idempotent safety, no panic).
        let unknown =
            apply_request(&mut sm, &mk_log_id(7), &ClientRequest::DeregisterNode { node_id: 99 });
        assert!(matches!(unknown, ClientResponse::Error { .. }), "unknown node deregister is Error");
    }

    /// Bug E — `list_directory_entries` must report a path as a directory
    /// when EITHER an explicit mkdir entry OR a child file path exists,
    /// independent of BTreeMap iteration order.
    ///
    /// Symptom on the lab: after mkdir + writing files inside, `ls -l`
    /// showed the directory as a regular file with size equal to the sum
    /// of children — `find` saw it as a non-directory and never recursed.
    #[test]
    fn bug_e_dir_with_mkdir_marker_and_children_is_a_directory_with_4k_size() {
        let mut sm = StateMachineData::default();
        // 1. mkdir /series  (FUSE sends with trailing slash)
        apply_request(
            &mut sm,
            &mk_log_id(10),
            &ClientRequest::CreateFile {
                path: "/series/".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );
        // 2. write /series/episode-01.mkv (10 GB worth of "chunks")
        apply_request(
            &mut sm,
            &mk_log_id(11),
            &ClientRequest::CreateFile {
                path: "/series/episode-01.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 128 * 1024 * 1024,
            },
        );
        apply_request(
            &mut sm,
            &mk_log_id(12),
            &ClientRequest::UpdateChunkLocations {
                path: "/series/episode-01.mkv".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    // 10 GB single chunk for the test
                    size_bytes: 10 * 1024 * 1024 * 1024,
                }],
                lease_id: String::new(),
            },
        );

        // List from `/`: expect ONE entry named "series" that's a
        // directory with size 4096.
        let entries = sm.list_directory_entries("/");
        assert_eq!(
            entries.len(),
            1,
            "ls / should return a single entry, got {:?}",
            entries
        );
        let e = &entries[0];
        assert_eq!(e.name, "series");
        assert!(
            e.is_directory,
            "directory must be reported as is_directory=true, got {:?}",
            e
        );
        assert_eq!(
            e.size_bytes, 4096,
            "directory size must be 4096 (one block), not the sum of children — got {} (= {} MB)",
            e.size_bytes,
            e.size_bytes / 1_048_576
        );

        // And `ls /series/` should return the episode as a regular file
        // with its real size.
        let inside = sm.list_directory_entries("/series");
        let episode = inside
            .iter()
            .find(|x| x.name == "episode-01.mkv")
            .expect("episode must be present in /series listing");
        assert!(!episode.is_directory);
        assert_eq!(episode.size_bytes, 10 * 1024 * 1024 * 1024);
    }

    /// Bug E (synthetic dir variant) — even without an explicit mkdir,
    /// a directory implied by a child file path must list as a directory
    /// from its parent. (Catches the case where rsync writes files
    /// straight into a folder without ever calling mkdir.)
    #[test]
    fn bug_e_synthetic_dir_lists_as_directory() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(20),
            &ClientRequest::CreateFile {
                path: "/movies/foo.mkv".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let entries = sm.list_directory_entries("/");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "movies");
        assert!(entries[0].is_directory);
        assert_eq!(entries[0].size_bytes, 4096);
    }

    // ----------- Bug O — scrub auto-repair ---------------------------

    /// When the data node reports chunks as corrupt via
    /// `RemoveCorruptChunkReplicas`, the matching (node_id, chunk_hash)
    /// entries are dropped from every file's chunk list. The
    /// scheduler's `check_under_replicated` pass then sees the file
    /// has fewer than RF replicas and queues a copy from a healthy
    /// peer.
    #[test]
    fn bug_o_remove_corrupt_chunk_replicas_drops_matching_entries() {
        let mut sm = StateMachineData::default();
        sm.files.insert(
            "/a.bin".into(),
            FileMetadata {
                path: "/a.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "corrupt-hash".into(),
                        node_id: "10".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "corrupt-hash".into(),
                        node_id: "11".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                ],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            },
        );

        apply_request(
            &mut sm,
            &mk_log_id(99),
            &ClientRequest::RemoveCorruptChunkReplicas {
                node_id: "10".into(),
                chunk_hashes: vec!["corrupt-hash".into()],
                disk_id: legacy_disk_id(),
            },
        );

        // Node 10's replica is gone; node 11's replica survives.
        let a = sm.files.get("/a.bin").unwrap();
        assert_eq!(a.chunks.len(), 1);
        assert_eq!(a.chunks[0].node_id, "11");
        assert_eq!(a.chunks[0].chunk_hash, "corrupt-hash");
    }

    /// Bug P — `UpdateChunkLocations` MUST reject a commit whose
    /// `lease_id` doesn't match the currently-held lease for the
    /// path. Pre-fix a stale writer (whose lease the manager had
    /// already GC'd) could still commit its chunks alongside a fresh
    /// writer's, interleaving the file's bytes per chunk_index.
    #[test]
    fn bug_p_update_chunk_locations_rejects_stale_lease_id() {
        let mut sm = StateMachineData::default();
        // 1. Create the file.
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::CreateFile {
                path: "/x.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        // 2. Writer-A acquires the lease.
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::AcquireWriteLease {
                path: "/x.bin".into(),
                owner_id: "host-a:pid-1:fh-9".into(),
                lease_ms: 15_000,
                now_ms: 1_000_000,
                proposed_lease_id: "lease-a-uuid".into(),
            },
        );
        let lease_a = sm
            .write_leases
            .get("/x.bin")
            .map(|l| l.lease_id.clone())
            .expect("writer-A holds the lease");

        // 3. Writer-A's lease is reaped (simulate GC sweep at lease+1).
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::SweepExpiredLeases {
                now_ms: 1_000_000 + 16_000,
            },
        );
        assert!(sm.write_leases.get("/x.bin").is_none());

        // 4. Writer-B acquires fresh.
        apply_request(
            &mut sm,
            &mk_log_id(4),
            &ClientRequest::AcquireWriteLease {
                path: "/x.bin".into(),
                owner_id: "host-b:pid-2:fh-1".into(),
                lease_ms: 15_000,
                now_ms: 1_000_000 + 20_000,
                proposed_lease_id: "lease-b-uuid".into(),
            },
        );
        let lease_b = sm
            .write_leases
            .get("/x.bin")
            .map(|l| l.lease_id.clone())
            .expect("writer-B holds the lease now");
        assert_ne!(lease_a, lease_b);

        // 5. Writer-A's belated commit must be REJECTED.
        let resp = apply_request(
            &mut sm,
            &mk_log_id(5),
            &ClientRequest::UpdateChunkLocations {
                path: "/x.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "stale-bytes".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: lease_a.clone(),
            },
        );
        assert!(
            matches!(resp, ClientResponse::WriteLeaseConflict { .. }),
            "stale lease must be rejected with WriteLeaseConflict; got {:?}",
            resp
        );
        // And the file's chunks must NOT have been mutated by A's stale commit.
        let f = sm.files.get("/x.bin").unwrap();
        assert!(
            f.chunks.is_empty(),
            "file must have no chunks committed by the stale writer"
        );

        // 6. Writer-B's commit with the right lease_id succeeds.
        let resp = apply_request(
            &mut sm,
            &mk_log_id(6),
            &ClientRequest::UpdateChunkLocations {
                path: "/x.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "fresh-bytes".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 4096,
                }],
                lease_id: lease_b,
            },
        );
        assert!(matches!(resp, ClientResponse::Success));
        let f = sm.files.get("/x.bin").unwrap();
        assert_eq!(f.chunks.len(), 1);
        assert_eq!(f.chunks[0].chunk_hash, "fresh-bytes");
    }

    /// Bug P — empty lease_id preserves legacy callers (system-driven
    /// re-replication, older clients). The commit succeeds without
    /// lease verification.
    #[test]
    fn bug_p_empty_lease_id_skips_verification_legacy_compat() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::CreateFile {
                path: "/y.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
            },
        );
        let resp = apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UpdateChunkLocations {
                path: "/y.bin".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "legacy".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 1024,
                }],
                lease_id: String::new(),
            },
        );
        assert!(
            matches!(resp, ClientResponse::Success),
            "empty lease_id must skip verification (legacy compat)"
        );
    }

    /// Bug O — unrelated chunks on the same node MUST be preserved.
    #[test]
    fn bug_o_remove_does_not_drop_unrelated_chunks_on_same_node() {
        let mut sm = StateMachineData::default();
        sm.files.insert(
            "/c.bin".into(),
            FileMetadata {
                path: "/c.bin".into(),
                volume_id: "default".into(),
                chunk_size: 1024,
                chunks: vec![
                    ChunkLocationEntry {
                        chunk_index: 0,
                        chunk_hash: "h-bad".into(),
                        node_id: "20".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                    ChunkLocationEntry {
                        chunk_index: 1,
                        chunk_hash: "h-good".into(),
                        node_id: "20".into(),
                        disk_id: 0,
                        size_bytes: 1024,
                    },
                ],
                created_at_ms: 1,
                updated_at_ms: 1,
                symlink_target: None,
                logical_size: None,
                is_directory: false,
                cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            },
        );
        apply_request(
            &mut sm,
            &mk_log_id(100),
            &ClientRequest::RemoveCorruptChunkReplicas {
                node_id: "20".into(),
                chunk_hashes: vec!["h-bad".into()],
                disk_id: legacy_disk_id(),
            },
        );
        let c = sm.files.get("/c.bin").unwrap();
        assert_eq!(c.chunks.len(), 1);
        assert_eq!(c.chunks[0].chunk_hash, "h-good");
    }

    // ---- Bug AH — replicated audit log fan-out ----

    /// `AppendAudit` must push a fully-populated entry onto `sm.audit_log`
    /// with structured fields preserved and a synthesised `summary` for
    /// legacy display.
    #[test]
    fn bug_ah_append_audit_pushes_entry_with_structured_fields() {
        let mut sm = StateMachineData::default();
        let before = sm.audit_log.len();

        let resp = apply_request(
            &mut sm,
            &mk_log_id(7),
            &ClientRequest::AppendAudit {
                actor: "scheduler".into(),
                action: "replication.complete".into(),
                target: "chunk=abc src=10 dst=11".into(),
                result: "success".into(),
                origin_manager: "mgr02".into(),
                timestamp_ms: 1_779_000_000_000,
            },
        );
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm.audit_log.len(), before + 1);

        let entry = sm.audit_log.last().unwrap();
        assert_eq!(entry.actor, "scheduler");
        assert_eq!(entry.action, "replication.complete");
        assert_eq!(entry.target, "chunk=abc src=10 dst=11");
        assert_eq!(entry.result, "success");
        assert_eq!(entry.origin_manager, "mgr02");
        assert_eq!(entry.timestamp_ms, 1_779_000_000_000);
        // Synthesised summary preserves the legacy display path.
        assert!(entry.summary.contains("replication.complete"));
        assert!(entry.summary.contains("chunk=abc src=10 dst=11"));
        assert!(entry.summary.contains("mgr02"));
        // log_index is the Raft log index at apply time.
        assert_eq!(entry.log_index, 7);
    }

    /// `AppendAudit` with an empty target must still produce a sensible
    /// summary (no trailing `": "`). Covers system events like
    /// `manager.start` which carry only an actor/action/result.
    #[test]
    fn bug_ah_append_audit_targetless_summary_is_clean() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(8),
            &ClientRequest::AppendAudit {
                actor: "system".into(),
                action: "manager.start".into(),
                target: String::new(),
                result: "success".into(),
                origin_manager: "mgr01".into(),
                timestamp_ms: 100,
            },
        );
        let entry = sm.audit_log.last().unwrap();
        assert_eq!(entry.summary, "manager.start [mgr01]");
        assert!(entry.target.is_empty());
    }

    /// Bug AH compat — an `AuditEntry` serialised before the structured
    /// fields existed (only `log_index` + `summary` + `timestamp_ms`)
    /// must deserialise cleanly via `#[serde(default)]` on the new fields.
    /// This guards every existing snapshot on disk.
    #[test]
    fn bug_ah_audit_entry_backward_compat_deserialise() {
        let legacy = r#"{"log_index":42,"summary":"DeleteFile: /x","timestamp_ms":1234}"#;
        let entry: AuditEntry = serde_json::from_str(legacy).expect("legacy shape must deserialise");
        assert_eq!(entry.log_index, 42);
        assert_eq!(entry.summary, "DeleteFile: /x");
        assert_eq!(entry.timestamp_ms, 1234);
        // New fields default to empty strings — safe for the API
        // serializer to surface unchanged.
        assert!(entry.actor.is_empty());
        assert!(entry.action.is_empty());
        assert!(entry.target.is_empty());
        assert!(entry.result.is_empty());
        assert!(entry.origin_manager.is_empty());
    }

    /// `AppendAudit` is subject to the same `MAX_AUDIT_LOG_ENTRIES` cap
    /// as native audit pushes (CreateFile, RegisterNode, etc.) — both
    /// paths flow through `push_audit_capped`. Verifies the cap holds
    /// under a flood from the fan-out path.
    #[test]
    fn bug_ah_append_audit_respects_global_cap() {
        let mut sm = StateMachineData::default();
        // Fill close to the cap with native audit events first.
        for i in 0..(MAX_AUDIT_LOG_ENTRIES as u64) {
            apply_request(
                &mut sm,
                &mk_log_id(i + 1),
                &ClientRequest::CreateFile {
                    path: format!("/seed/{i}"),
                    volume_id: "default".into(),
                    chunk_size: 1024,
                },
            );
        }
        assert_eq!(sm.audit_log.len(), MAX_AUDIT_LOG_ENTRIES);

        // Now flood with AppendAudit — the cap must still hold.
        for i in 0..50 {
            apply_request(
                &mut sm,
                &mk_log_id(MAX_AUDIT_LOG_ENTRIES as u64 + i + 1),
                &ClientRequest::AppendAudit {
                    actor: "scheduler".into(),
                    action: "flood".into(),
                    target: format!("evt={i}"),
                    result: "success".into(),
                    origin_manager: "mgr03".into(),
                    timestamp_ms: 0,
                },
            );
        }
        assert_eq!(sm.audit_log.len(), MAX_AUDIT_LOG_ENTRIES);
        // The newest entries should be the fan-out ones (FIFO eviction
        // by `push_audit_capped`).
        let last = sm.audit_log.last().unwrap();
        assert_eq!(last.action, "flood");
        assert_eq!(last.origin_manager, "mgr03");
    }

    // ---- AV-1-3 — RemoveChunkLocation tests -----------------------

    /// Build a minimal SM with one file + one chunk and the named
    /// nodes pre-registered. Each node gets a single disk of the
    /// requested tier in the requested DC. Used by the AV-1-3 tests.
    fn sm_with_chunk(
        file_path: &str,
        chunk_index: u64,
        chunk_hash: &str,
        replicas: &[(u64, &str, dtfs_common::types::DiskTier)],
    ) -> StateMachineData {
        use dtfs_common::topology::Location;
        use dtfs_common::types::DiskStatus;

        let mut sm = StateMachineData::default();
        for (nid, dc, tier) in replicas {
            sm.nodes.insert(*nid, crate::NodeInfo {
                role: dtfs_common::types::NodeRole::Data,
                hostname: format!("node-{nid}"),
                grpc_addr: String::new(),
                location: Location::new("lab", dc, "rack-1", &format!("node-{nid}")),
                disks: vec![dtfs_common::types::DiskInfo {
                    disk_id: 0,
                    uuid: String::new(),
                    mount_path: String::new(),
                    tier: *tier,
                    total_bytes: 0,
                    used_bytes: 0,
                    status: DiskStatus::Healthy,
                }],
                total_capacity: 0,
                used_capacity: 0,
                health: dtfs_common::types::HealthStatus::Healthy,
                last_seen_ms: 0,
                total_bytes_read: 0,
                total_bytes_written: 0,
                client_ip: None,
                version: String::new(),
                build_date: String::new(),
                quarantine: None,
            });
        }
        sm.files.insert(file_path.to_string(), FileMetadata {
            path: file_path.to_string(),
            volume_id: "default".to_string(),
            chunk_size: 1024,
            chunks: replicas
                .iter()
                .map(|(nid, _, _)| ChunkLocationEntry {
                    chunk_index,
                    chunk_hash: chunk_hash.to_string(),
                    node_id: nid.to_string(),
                    disk_id: 0,
                    size_bytes: 1024,
                })
                .collect(),
            created_at_ms: 1,
            updated_at_ms: 1,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        });
        sm
    }

    /// REV-3-7 — the anchor survivor scan must use the same liveness rule as
    /// the scheduler's coverage counting (`live_replica_tier`). A replica on a
    /// Down node is not servable, so it cannot be the thing that makes a
    /// removal safe. Pre-fix the two disagreed: `check_under_replicated` saw a
    /// deficit and scheduled a backfill, and this scan let the rebalance drop
    /// the fresh copy again on the strength of the dead anchor — forever.
    #[test]
    fn rev_3_7_a_dead_replica_cannot_satisfy_the_anchor() {
        use dtfs_common::types::{AnchorPolicy, DiskTier, HealthStatus};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "limburg", DiskTier::Cold), // the anchor copy
            (11, "toro", DiskTier::Warm),
        ]);
        let policy = AnchorPolicy {
            datacenter: "limburg".to_string(),
            required_tier: Some(DiskTier::Cold),
        };

        // With the anchor node healthy, dropping the warm copy is fine.
        let mut healthy = sm.clone();
        let resp = apply_request(&mut healthy, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: Some(policy.clone()),
            });
        assert!(matches!(resp, ClientResponse::Success));

        // Now the anchor node is Down: its replica is not servable, so the
        // same removal must be refused instead of counting on it.
        sm.nodes.get_mut(&10).unwrap().health = HealthStatus::Down;
        let resp = apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: Some(policy.clone()),
            });
        match resp {
            ClientResponse::Error { message } => {
                assert!(message.contains("would strand anchor"), "got: {message}");
            }
            other => panic!("expected refusal while the anchor node is Down, got {other:?}"),
        }
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 2, "nothing removed");

        // Same rule for a Failed/Removed DISK under a healthy node.
        let mut bad_disk = sm.clone();
        bad_disk.nodes.get_mut(&10).unwrap().health = HealthStatus::Healthy;
        bad_disk.nodes.get_mut(&10).unwrap().disks[0].status =
            dtfs_common::types::DiskStatus::Failed;
        let resp = apply_request(&mut bad_disk, &mk_log_id(3),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: 0,
                enforce_anchor: Some(policy),
            });
        assert!(
            matches!(resp, ClientResponse::Error { .. }),
            "a Failed anchor disk cannot authorise the removal either"
        );
    }

    /// AV-1-3 test 1 — happy path: drop one entry, others survive.
    #[test]
    fn bug_av_remove_chunk_location_drops_entry() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "dc-a", DiskTier::Cold),
            (11, "dc-b", DiskTier::Warm),
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.chunks.len(), 1);
        assert_eq!(f.chunks[0].node_id, "10");
    }

    /// AV-1-3 test 2 — unknown file → idempotent no-op success.
    #[test]
    fn bug_av_remove_chunk_location_unknown_file_is_noop() {
        let mut sm = StateMachineData::default();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/nope".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert!(sm.files.is_empty());
    }

    /// AV-1-3 test 3 — unknown (chunk_index, node) → idempotent no-op.
    #[test]
    fn bug_av_remove_chunk_location_unknown_node_is_noop() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "99".into(),  // not present
                disk_id: legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.chunks.len(), 1, "no entry dropped");
    }

    /// AV-1-3 test 4 — anchor-strand protection: refuse to drop the
    /// last anchor-satisfying replica.
    #[test]
    fn bug_av_remove_chunk_location_refuses_to_strand_anchor() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "toro", DiskTier::Warm),
        ]);
        // Attempt to drop the only leli-resident replica — last anchor copy.
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(AnchorPolicy {
                    datacenter: "leli".into(),
                    required_tier: None,  // any-tier-in-leli policy
                }),
            });
        match resp {
            ClientResponse::Error { message } => {
                assert!(message.contains("strand anchor"), "msg: {message}");
                assert!(message.contains("leli"));
            }
            other => panic!("expected Error, got {other:?}"),
        }
        // SM unchanged.
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.chunks.len(), 2);
    }

    /// AV-1-3 test 5 — tier-aware anchor: with `required_tier=Cold`,
    /// dropping a warm replica in the anchor DC is fine (cold replica
    /// elsewhere satisfies); but dropping the only cold replica IS
    /// stranded (because the remaining warm-in-leli doesn't match the
    /// required tier).
    #[test]
    fn bug_av_remove_chunk_location_anchor_tier_required_check() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let policy = AnchorPolicy {
            datacenter: "leli".into(),
            required_tier: Some(DiskTier::Cold),
        };

        // (a) Setup: one cold-in-leli + one warm-in-leli.
        //     Drop the warm replica → still 1 cold-in-leli → OK.
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "leli", DiskTier::Warm),
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(), // warm
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(policy.clone()),
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 1);

        // (b) Same setup; drop the cold replica → only warm-in-leli
        //     remains, which doesn't satisfy required_tier=Cold → REFUSE.
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "leli", DiskTier::Warm),
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),  // cold
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(policy),
            });
        assert!(matches!(resp, ClientResponse::Error { .. }));
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 2, "unchanged");
    }

    /// CNF-1 — the enforce-anchor survivor check must resolve the disk by
    /// the STABLE `DiskInfo.disk_id`, not Vec position. With the cold
    /// survivor's only disk carrying disk_id=1 at Vec index 0 (reindexed
    /// disk list), dropping the warm replica is safe and must succeed
    /// (pre-fix the positional lookup missed, tier resolved to None, and
    /// the safe removal was refused).
    #[test]
    fn cnf1_remove_chunk_location_anchor_resolves_disk_by_stable_id() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),
            (11, "leli", DiskTier::Warm),
        ]);
        // Reindex node 10's disk list: its only disk now carries the
        // stable disk_id=1 (still at Vec index 0); the chunk entry
        // references the stable id.
        sm.nodes.get_mut(&10).unwrap().disks[0].disk_id = 1;
        for c in &mut sm.files.get_mut("/x").unwrap().chunks {
            if c.node_id == "10" {
                c.disk_id = 1;
            }
        }
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(), // drop warm; cold survivor anchors
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(AnchorPolicy {
                    datacenter: "leli".into(),
                    required_tier: Some(DiskTier::Cold),
                }),
            });
        assert!(matches!(resp, ClientResponse::Success), "got {resp:?}");
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 1);
    }

    // ---- AV-1-4 — SetChunkCooldown tests --------------------------

    /// AV-1-4 test 1 — single-chunk cooldown set on a file with no
    /// prior cooldown.
    #[test]
    fn bug_av_set_cooldown_single_chunk() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 1_000_000,
            });
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.cache_cooldown_until_ms.get(&0), Some(&1_000_000));
    }

    /// AV-1-4 test 2 — multiple chunk_indexes in one apply call.
    #[test]
    fn bug_av_set_cooldown_multi_chunk() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0, 1, 2, 7],
                expires_at_ms: 5_000_000,
            });
        let f = sm.files.get("/x").unwrap();
        assert_eq!(f.cache_cooldown_until_ms.len(), 4);
        for idx in [0u64, 1, 2, 7] {
            assert_eq!(f.cache_cooldown_until_ms.get(&idx), Some(&5_000_000));
        }
    }

    /// AV-1-4 test 3 — monotonic merge: a later flush with an
    /// earlier expiry does NOT regress; a later flush with a later
    /// expiry extends.
    #[test]
    fn bug_av_set_cooldown_monotonic() {
        use dtfs_common::types::DiskTier;
        let mut sm = sm_with_chunk("/x", 0, "h", &[(10, "dc-a", DiskTier::Cold)]);
        // First flush sets t=1000
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 1000,
            });
        // Second flush with EARLIER expiry must NOT regress
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 500,
            });
        assert_eq!(sm.files.get("/x").unwrap().cache_cooldown_until_ms.get(&0), Some(&1000));
        // Third flush with later expiry extends
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 2000,
            });
        assert_eq!(sm.files.get("/x").unwrap().cache_cooldown_until_ms.get(&0), Some(&2000));
        // expires_at_ms = 0 clears.
        apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::SetChunkCooldown {
                path: "/x".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 0,
            });
        assert!(sm.files.get("/x").unwrap().cache_cooldown_until_ms.is_empty());
    }

    /// AV-1-4 test 4 — unknown file → idempotent no-op success.
    #[test]
    fn bug_av_set_cooldown_unknown_file_is_noop() {
        let mut sm = StateMachineData::default();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::SetChunkCooldown {
                path: "/nope".into(),
                chunk_indexes: vec![0],
                expires_at_ms: 1000,
            });
        assert!(matches!(resp, ClientResponse::Success));
    }

    /// AV-1-3 test 6 — any-tier anchor policy allows dropping a
    /// warm-in-anchor-DC replica as long as another replica in the
    /// anchor DC survives (any tier).
    #[test]
    fn bug_av_remove_chunk_location_anchor_any_tier_allowed() {
        use dtfs_common::types::{AnchorPolicy, DiskTier};
        let mut sm = sm_with_chunk("/x", 0, "h", &[
            (10, "leli", DiskTier::Cold),  // anchor survivor
            (11, "leli", DiskTier::Warm),  // dropping this
        ]);
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "11".into(),
                disk_id: legacy_disk_id(),
                enforce_anchor: Some(AnchorPolicy {
                    datacenter: "leli".into(),
                    required_tier: None,  // any tier
                }),
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(sm.files.get("/x").unwrap().chunks.len(), 1);
    }

    // ---- AV-1-2 — FileMetadata.cache_cooldown_until_ms tests ------

    /// AV-1-2 test 6 — `FileMetadata::cache_cooldown_until_ms` is
    /// `#[serde(default)]` so old snapshots (from before AV) load
    /// cleanly with an empty BTreeMap and no behaviour change.
    #[test]
    fn bug_av_filemeta_cooldown_serde_default() {
        // Old-snapshot JSON (no `cache_cooldown_until_ms` field).
        let legacy_json = r#"{
            "path": "/old.bin",
            "volume_id": "default",
            "chunk_size": 1024,
            "chunks": [],
            "created_at_ms": 100,
            "updated_at_ms": 200
        }"#;
        let f: FileMetadata = serde_json::from_str(legacy_json).expect("loads");
        assert_eq!(f.path, "/old.bin");
        assert!(f.cache_cooldown_until_ms.is_empty(),
            "missing field defaults to empty map");
    }

    /// AV-1-2 test 7 — a populated cooldown map survives ser→de
    /// round-trip with deterministic key ordering.
    #[test]
    fn bug_av_filemeta_old_snapshot_loads() {
        let mut original = FileMetadata {
            path: "/new.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
            chunks: Vec::new(),
            created_at_ms: 100,
            updated_at_ms: 200,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        };
        original.cache_cooldown_until_ms.insert(0, 1_000_000);
        original.cache_cooldown_until_ms.insert(2, 2_000_000);

        let json = serde_json::to_string(&original).unwrap();
        let round_tripped: FileMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, original);
        // BTreeMap iteration order is by key — deterministic across runs.
        let keys: Vec<_> = round_tripped.cache_cooldown_until_ms.keys().copied().collect();
        assert_eq!(keys, vec![0, 2]);
    }

    // ── Bug BC-10 — Quarantine state-machine apply ──────────────────

    fn mk_quarantinable_node() -> crate::NodeInfo {
        crate::NodeInfo {
            role: dtfs_common::types::NodeRole::Data,
            hostname: "data-1".into(),
            grpc_addr: "data-1:9500".into(),
            location: dtfs_common::topology::Location::new("lab", "dc-a", "r1", "h1"),
            disks: Vec::new(),
            total_capacity: 0,
            used_capacity: 0,
            health: dtfs_common::types::HealthStatus::Healthy,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn mk_quarantine_info(reason: dtfs_common::types::QuarantineReason) -> dtfs_common::types::QuarantineInfo {
        dtfs_common::types::QuarantineInfo {
            since_ms: 1_000,
            reason,
            triggering_chunk_id: Some("aabbccdd".into()),
            fault_count_total: 0,
            last_fault_at_ms: 1_000,
            last_fault_summary: "bytes mismatched expected hash".into(),
        }
    }

    // ── Bug BJ — RegisterNode audit idempotency ────────────────────

    fn count_register_audits(sm: &StateMachineData) -> usize {
        sm.audit_log
            .iter()
            .filter(|e| e.summary.starts_with("RegisterNode:"))
            .count()
    }

    #[test]
    fn bug_bj_register_node_new_emits_audit() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        assert_eq!(count_register_audits(&sm), 1, "first register emits one row");
    }

    #[test]
    fn bug_bj_register_node_reregister_identical_no_audit() {
        let mut sm = StateMachineData::default();
        let info = mk_quarantinable_node();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: info.clone() },
        );
        // Re-register: only volatile fields differ (heartbeat-driven).
        let mut info2 = info;
        info2.last_seen_ms = 999_999;
        info2.used_capacity = 12_345;
        info2.total_bytes_read = 555;
        info2.total_bytes_written = 666;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 7, info: info2 },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "identical re-register (only volatile fields changed) must add no audit row"
        );
    }

    #[test]
    fn bug_bj_register_node_material_change_emits_audit() {
        let mut sm = StateMachineData::default();
        let info = mk_quarantinable_node();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: info.clone() },
        );
        let mut info2 = info;
        info2.version = "v0.0.1-new".into(); // material identity field
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 7, info: info2 },
        );
        assert_eq!(
            count_register_audits(&sm),
            2,
            "a materially-changed re-register (version) must add exactly one row"
        );
    }

    // ── RAFT-1 — RegisterNode must carry forward sticky Quarantined/
    // Maintenance/Draining state instead of resetting to Healthy ─────

    #[test]
    fn raft_1_reregister_keeps_quarantined() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::QuarantineNode {
                node_id: 7,
                info: mk_quarantine_info(QuarantineReason::DeepScrubHashMismatch),
            },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Quarantined);

        // Re-register (e.g. process restart) with the producer's
        // unconditional health: Healthy, quarantine: None.
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        let node = sm.nodes.get(&7).expect("node still registered");
        assert_eq!(node.health, HealthStatus::Quarantined,
            "re-register must not clear Quarantined");
        assert!(node.quarantine.is_some(),
            "QuarantineInfo must survive re-register");
    }

    #[test]
    fn raft_1_reregister_keeps_maintenance() {
        use dtfs_common::types::HealthStatus;
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UpdateNodeStatus { node_id: 7, status: "maintenance".into() },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Maintenance);
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Maintenance,
            "re-register must not clear Maintenance");
    }

    #[test]
    fn raft_1_reregister_healthy_stays_healthy() {
        use dtfs_common::types::HealthStatus;
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() },
        );
        assert_eq!(sm.nodes.get(&7).unwrap().health, HealthStatus::Healthy);
    }

    // Bug BR-1 — managers now self-register (role="manager") to publish
    // their version/build_date and a live last_seen_ms into shared state.
    // The same audit-idempotency that protects data-node re-registration
    // must hold for the manager keepalive: a steady-state beat (only
    // last_seen_ms changes) adds no audit row, while a real upgrade
    // (version change) logs exactly once. Guards the new manager keepalive
    // from spamming the audit feed.
    #[test]
    fn bug_br1_manager_self_register_versioned_and_idempotent() {
        let mut sm = StateMachineData::default();
        let mut info = mk_quarantinable_node();
        info.role = dtfs_common::types::NodeRole::Manager;
        info.hostname = "mgr01".into();
        info.grpc_addr = "10.0.0.1:9401".into();
        info.version = "v1.2.3".into();
        info.build_date = "20260602-000000".into();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 42, info: info.clone() },
        );
        assert_eq!(
            sm.nodes.get(&42).map(|n| n.version.as_str()),
            Some("v1.2.3"),
            "manager self-register must store version in shared state"
        );
        assert_eq!(count_register_audits(&sm), 1, "first manager register emits one row");

        // Keepalive beat: only last_seen_ms moves.
        let mut beat = info.clone();
        beat.last_seen_ms = 123_456;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 42, info: beat },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "manager keepalive re-register must not spam the audit log"
        );

        // Upgrade: version changes once.
        let mut upgraded = info;
        upgraded.version = "v1.2.4".into();
        apply_request(
            &mut sm,
            &mk_log_id(3),
            &ClientRequest::RegisterNode { node_id: 42, info: upgraded },
        );
        assert_eq!(
            count_register_audits(&sm),
            2,
            "a manager version change (upgrade) must log exactly one row"
        );
    }

    // Bug BY — applying a ReportScrubResult (now produced by the data node
    // via heartbeat) records the result per (node, level) so the WebUI
    // "Last Scrub" card + stats.scrub_results_by_node populate. A deep
    // result does not overwrite the quick one (BK).
    #[test]
    fn bug_by_report_scrub_result_populates_state() {
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::ReportScrubResult {
                node_id: 5,
                disk_path: "/mnt/data01-warm".into(),
                level: "quick".into(),
                completed_at_ms: 1_700_000_000_000,
                chunks_checked: 160,
                chunks_healthy: 160,
                chunks_corrupt: 0,
                chunks_missing: 0,
                duration_secs: 0.05,
            },
        );
        let rec = sm
            .scrub_results_by_level
            .get(&5)
            .and_then(|m| m.get("quick"))
            .expect("quick record stored per (node, level)");
        assert_eq!(rec.chunks_checked, 160);
        assert_eq!(rec.level, "quick");
        assert!(sm.scrub_results.contains_key(&5), "legacy per-node map also populated");

        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::ReportScrubResult {
                node_id: 5,
                disk_path: "/mnt/data01-warm".into(),
                level: "deep".into(),
                completed_at_ms: 1_700_000_001_000,
                chunks_checked: 314,
                chunks_healthy: 314,
                chunks_corrupt: 0,
                chunks_missing: 0,
                duration_secs: 1.2,
            },
        );
        assert_eq!(
            sm.scrub_results_by_level.get(&5).unwrap().len(),
            2,
            "quick + deep both retained (BK), not overwritten"
        );
    }

    fn mk_data_node_with_disk(used_bytes: u64) -> crate::NodeInfo {
        let mut n = mk_quarantinable_node();
        n.disks = vec![dtfs_common::types::DiskInfo {
            disk_id: 0,
            uuid: "uuid-0".into(),
            mount_path: "/mnt/d0".into(),
            tier: dtfs_common::types::DiskTier::Warm,
            total_bytes: 1_000_000,
            used_bytes,
            status: dtfs_common::types::DiskStatus::Healthy,
        }];
        n
    }

    #[test]
    fn bug_bj_register_node_disk_usage_change_no_audit() {
        // A data node reports live per-disk used_bytes on every register;
        // a drift there must NOT count as a material change.
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_data_node_with_disk(100) },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_data_node_with_disk(999_999) },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "only disk used_bytes changed → no new audit row"
        );
    }

    #[test]
    fn bug_bj_register_node_disk_status_change_emits_audit() {
        // A disk status flip (e.g. Failed) IS material.
        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_data_node_with_disk(100) },
        );
        let mut changed = mk_data_node_with_disk(100);
        changed.disks[0].status = dtfs_common::types::DiskStatus::Failed;
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 7, info: changed },
        );
        assert_eq!(
            count_register_audits(&sm),
            2,
            "a disk status change must add an audit row"
        );
    }

    #[test]
    fn bug_bj_register_node_disk_reorder_no_audit() {
        use dtfs_common::types::{DiskInfo, DiskStatus, DiskTier};
        let warm = DiskInfo {
            disk_id: 0,
            uuid: "u-warm".into(),
            mount_path: "/mnt/w".into(),
            tier: DiskTier::Warm,
            total_bytes: 100,
            used_bytes: 10,
            status: DiskStatus::Healthy,
        };
        let cold = DiskInfo {
            disk_id: 1,
            uuid: "u-cold".into(),
            mount_path: "/mnt/c".into(),
            tier: DiskTier::Cold,
            total_bytes: 100,
            used_bytes: 5,
            status: DiskStatus::Healthy,
        };
        let mut n1 = mk_quarantinable_node();
        n1.disks = vec![warm.clone(), cold.clone()];
        // Re-register: disks reversed, disk_id reassigned, used_bytes
        // drifted, AND uuids regenerated (the data node mints fresh disk
        // uuids per boot) — same physical mounts, so NOT material.
        let mut cold_b = cold;
        cold_b.disk_id = 0;
        cold_b.used_bytes = 99;
        cold_b.uuid = "u-cold-boot2".into();
        let mut warm_b = warm;
        warm_b.disk_id = 1;
        warm_b.used_bytes = 42;
        warm_b.uuid = "u-warm-boot2".into();
        let mut n2 = mk_quarantinable_node();
        n2.disks = vec![cold_b, warm_b];

        let mut sm = StateMachineData::default();
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: n1 },
        );
        apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::RegisterNode { node_id: 7, info: n2 },
        );
        assert_eq!(
            count_register_audits(&sm),
            1,
            "disk reorder + disk_id swap + usage drift → no new audit row"
        );
    }

    #[test]
    fn bug_bc_10_quarantine_node_sets_health_and_info() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(42, mk_quarantinable_node());

        let info = mk_quarantine_info(QuarantineReason::VerifyOnReadMismatch);
        let resp = apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode { node_id: 42, info: info.clone() },
        );
        assert!(matches!(resp, ClientResponse::Success));

        let n = sm.nodes.get(&42).expect("node still present");
        assert_eq!(n.health, HealthStatus::Quarantined);
        let q = n.quarantine.as_ref().expect("quarantine record present");
        assert_eq!(q.reason, QuarantineReason::VerifyOnReadMismatch);
        assert_eq!(q.triggering_chunk_id.as_deref(), Some("aabbccdd"));
        assert_eq!(q.fault_count_total, 1, "first fault accumulates to 1");

        // Audit row should mention quarantine + reason.
        let last = sm.audit_log.last().unwrap();
        assert_eq!(last.summary, "node.quarantine: 42 reason=VerifyOnReadMismatch chunk=aabbccdd fault_count=1");

        // Bug BH-9 — the row must carry STRUCTURED fields so the
        // integrity feed renders TARGET (node id + hostname) distinctly
        // from RESULT/SUMMARY (the fault detail), not the same blob in
        // both columns.
        assert_eq!(last.actor, "system");
        assert_eq!(last.action, "node.quarantine");
        assert_eq!(last.target, "42 (data-1)", "TARGET = node id + hostname only");
        assert!(
            !last.target.contains("reason=") && !last.target.contains("chunk="),
            "TARGET must not carry fault detail: {}",
            last.target
        );
        assert!(
            last.result.contains("reason=VerifyOnReadMismatch")
                && last.result.contains("chunk=aabbccdd"),
            "RESULT must carry the fault detail: {}",
            last.result
        );
    }

    #[test]
    fn bug_bc_10_quarantine_accumulates_fault_count() {
        use dtfs_common::types::QuarantineReason;
        let mut sm = StateMachineData::default();
        sm.nodes.insert(7, mk_quarantinable_node());

        for i in 1..=3 {
            apply_request(
                &mut sm,
                &mk_log_id(i),
                &ClientRequest::QuarantineNode {
                    node_id: 7,
                    info: mk_quarantine_info(QuarantineReason::DeepScrubHashMismatch),
                },
            );
        }
        let q = sm.nodes.get(&7).unwrap().quarantine.as_ref().unwrap();
        assert_eq!(q.fault_count_total, 3,
            "three faults must accumulate, not reset on each apply");
    }

    #[test]
    fn bug_bc_10_unquarantine_clears_and_restores_health() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(9, mk_quarantinable_node());
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode {
                node_id: 9,
                info: mk_quarantine_info(QuarantineReason::QuickScrubMissingDat),
            },
        );
        assert_eq!(sm.nodes.get(&9).unwrap().health, HealthStatus::Quarantined);

        let resp = apply_request(
            &mut sm,
            &mk_log_id(2),
            &ClientRequest::UnquarantineNode {
                node_id: 9,
                operator_id: "alice".into(),
                reason_acknowledged: "disk swapped + bitrot test passed".into(),
            },
        );
        assert!(matches!(resp, ClientResponse::Success));

        let n = sm.nodes.get(&9).unwrap();
        assert_eq!(n.health, HealthStatus::Healthy,
            "unquarantine must restore Healthy, not leave the node in Suspect");
        assert!(n.quarantine.is_none(),
            "quarantine record must be cleared");

        let last = sm.audit_log.last().unwrap();
        assert!(last.summary.contains("node.unquarantine: 9"),
            "audit must record the unquarantine event: {}", last.summary);
        assert!(last.summary.contains("by=alice"));
        assert!(last.summary.contains("ack=disk swapped"));
    }

    /// Bug BC-11 — once a node is Quarantined, the canonical
    /// `health == Healthy` filter used by `placement::rebuild_ring`,
    /// `scheduler::resolve_healthy_source`, and `api::allocate_chunks`
    /// MUST exclude it. We exercise the filter directly here so a
    /// future refactor that flips one of those sites to a negative
    /// `health != Down` predicate (which would leak Quarantined into
    /// the serving pool) is caught immediately.
    #[test]
    fn bug_bc_11_quarantined_node_excluded_from_healthy_filter() {
        use dtfs_common::types::{HealthStatus, QuarantineReason};
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, mk_quarantinable_node());
        sm.nodes.insert(2, mk_quarantinable_node());
        sm.nodes.insert(3, mk_quarantinable_node());

        // Quarantine node 2.
        apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode {
                node_id: 2,
                info: mk_quarantine_info(QuarantineReason::VerifyOnReadMismatch),
            },
        );

        let healthy: Vec<u64> = sm
            .nodes
            .iter()
            .filter(|(_, n)| n.health == HealthStatus::Healthy)
            .map(|(id, _)| *id)
            .collect();
        let mut sorted = healthy.clone();
        sorted.sort();
        assert_eq!(sorted, vec![1, 3],
            "Quarantined node 2 must NOT appear in the healthy serving \
             pool. If this assertion ever fires after a placement or \
             scheduler refactor, the cluster is at risk of routing reads \
             or replication to a node we know is corrupt.");
    }

    #[test]
    fn bug_bc_10_quarantine_missing_node_returns_error() {
        use dtfs_common::types::QuarantineReason;
        let mut sm = StateMachineData::default();
        let resp = apply_request(
            &mut sm,
            &mk_log_id(1),
            &ClientRequest::QuarantineNode {
                node_id: 999,
                info: mk_quarantine_info(QuarantineReason::OrphanScrubInStateMissing),
            },
        );
        assert!(matches!(resp, ClientResponse::Error { .. }));
    }

    // ── Bug BK — scrub results per (node, level) ────────────────────

    fn mk_scrub_req(node_id: u64, level: &str, completed_at_ms: u64) -> ClientRequest {
        ClientRequest::ReportScrubResult {
            node_id,
            disk_path: "/mnt/d0".into(),
            level: level.into(),
            completed_at_ms,
            chunks_checked: 10,
            chunks_healthy: 10,
            chunks_corrupt: 0,
            chunks_missing: 0,
            duration_secs: 1.0,
        }
    }

    #[test]
    fn scrub_quick_then_deep_both_retained() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &mk_scrub_req(7, "quick", 1_000));
        apply_request(&mut sm, &mk_log_id(2), &mk_scrub_req(7, "deep", 2_000));
        let by_level = sm.scrub_results_by_level.get(&7).expect("node 7 present");
        assert!(by_level.contains_key("quick"), "quick result retained");
        assert!(by_level.contains_key("deep"), "deep result retained");
        assert_eq!(by_level.len(), 2, "both levels coexist (deep did not overwrite quick)");
        // Same level re-reports → overwrite (latest of each level).
        apply_request(&mut sm, &mk_log_id(3), &mk_scrub_req(7, "quick", 3_000));
        let by_level = sm.scrub_results_by_level.get(&7).unwrap();
        assert_eq!(by_level.len(), 2);
        assert_eq!(by_level.get("quick").unwrap().completed_at_ms, 3_000,
            "latest quick replaces the earlier quick");
    }

    #[test]
    fn scrub_snapshot_roundtrip_preserves_levels() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &mk_scrub_req(7, "quick", 1_000));
        apply_request(&mut sm, &mk_log_id(2), &mk_scrub_req(7, "deep", 2_000));
        // Round-trip the nested map through serde (snapshot uses JSON).
        let json = serde_json::to_string(&sm.scrub_results_by_level).unwrap();
        let restored: std::collections::BTreeMap<u64, std::collections::BTreeMap<String, ScrubRecord>> =
            serde_json::from_str(&json).unwrap();
        let by_level = restored.get(&7).expect("node 7 survives round-trip");
        assert!(by_level.contains_key("quick") && by_level.contains_key("deep"),
            "both levels survive snapshot serialize/deserialize");
    }

    #[test]
    fn scrub_legacy_snapshot_migrates() {
        // Simulate a pre-BK snapshot: only the legacy single-per-node map is
        // populated; the nested map is empty. migrate_* must seed it.
        let mut sm = StateMachineData::default();
        sm.scrub_results.insert(7, ScrubRecord {
            node_id: 7,
            disk_path: "/mnt/d0".into(),
            level: "deep".into(),
            completed_at_ms: 5_000,
            chunks_checked: 3,
            chunks_healthy: 3,
            chunks_corrupt: 0,
            chunks_missing: 0,
            duration_secs: 0.5,
        });
        assert!(sm.scrub_results_by_level.is_empty());
        migrate_scrub_results_by_level(&mut sm);
        let by_level = sm.scrub_results_by_level.get(&7).expect("migrated");
        assert!(by_level.contains_key("deep"),
            "legacy record migrates into nested map keyed by its own level");
        // Idempotent: a second call must not clobber freshly-applied entries.
        apply_request(&mut sm, &mk_log_id(1), &mk_scrub_req(7, "quick", 6_000));
        migrate_scrub_results_by_level(&mut sm);
        assert_eq!(sm.scrub_results_by_level.get(&7).unwrap().len(), 2,
            "migration is a no-op once the nested map is populated");
    }

    // ── Bug BN-2 — heartbeat audit spam suppression ─────────────────

    fn count_audit_prefix(sm: &StateMachineData, prefix: &str) -> usize {
        sm.audit_log.iter().filter(|e| e.summary.starts_with(prefix)).count()
    }

    #[test]
    fn bug_bn2_update_node_usage_emits_no_audit() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() });
        let before = sm.audit_log.len();
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::UpdateNodeUsage {
            node_id: 7, used_capacity: 123, chunk_count: 4,
            total_bytes_read: 5, total_bytes_written: 6, observed_at_ms: 1_000,
        });
        assert_eq!(sm.audit_log.len(), before,
            "UpdateNodeUsage is telemetry — it must add no audit row");
    }

    #[test]
    fn bug_bn2_update_node_status_no_audit_without_transition() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() });
        // mk_quarantinable_node() is Healthy; applying "healthy" twice is a
        // no-op transition each time.
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::UpdateNodeStatus { node_id: 7, status: "healthy".into() });
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateNodeStatus { node_id: 7, status: "healthy".into() });
        assert_eq!(count_audit_prefix(&sm, "UpdateNodeStatus:"), 0,
            "Healthy -> Healthy must not audit");
    }

    #[test]
    fn bug_bn2_update_node_status_audits_real_transition() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RegisterNode { node_id: 7, info: mk_quarantinable_node() });
        apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::UpdateNodeStatus { node_id: 7, status: "suspect".into() });
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateNodeStatus { node_id: 7, status: "healthy".into() });
        assert_eq!(count_audit_prefix(&sm, "UpdateNodeStatus:"), 2,
            "Healthy->Suspect then Suspect->Healthy = two transitions = two rows");
    }

    // ── RAFT-2 — RenameFile must move directory children, not just the
    // exact key ──────────────────────────────────────────────────────

    #[test]
    fn raft_2_rename_directory_moves_children() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile {
            path: "/a/".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile {
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::CreateFile {
            path: "/a/b/y".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::RenameFile { from: "/a".into(), to: "/c".into() });

        assert!(sm.files.contains_key("/c"), "dir marker moved");
        assert!(sm.files.contains_key("/c/x"), "child file moved");
        assert!(sm.files.contains_key("/c/b/y"), "grandchild moved");
        assert_eq!(sm.files.get("/c/x").unwrap().path, "/c/x",
            "moved child's own path field must be updated");
        assert!(!sm.files.contains_key("/a"), "old dir marker gone");
        assert!(!sm.files.contains_key("/a/x"), "old child gone");
        assert!(!sm.files.contains_key("/a/b/y"), "old grandchild gone");
    }

    #[test]
    fn raft_2_rename_synthetic_directory_moves_children() {
        let mut sm = StateMachineData::default();
        // No mkdir marker for "/a" — only a file underneath it, so "/a" is
        // a synthetic directory (Bug B).
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile {
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let resp = apply_request(&mut sm, &mk_log_id(2),
            &ClientRequest::RenameFile { from: "/a".into(), to: "/c".into() });
        assert!(matches!(resp, ClientResponse::Success),
            "synthetic-dir source with children must succeed, got {resp:?}");
        assert!(sm.files.contains_key("/c/x"));
        assert!(!sm.files.contains_key("/a/x"));
    }

    #[test]
    fn raft_2_rename_onto_nonempty_directory_is_refused() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile {
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile {
            path: "/b/y".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        let resp = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::RenameFile { from: "/a".into(), to: "/b".into() });
        assert!(matches!(resp, ClientResponse::Error { .. }),
            "renaming onto a non-empty directory must be refused, got {resp:?}");
        assert!(sm.files.contains_key("/a/x"), "source untouched on refusal");
        assert!(sm.files.contains_key("/b/y"), "destination untouched on refusal");
    }

    #[test]
    fn raft_2_rename_lease_follows_child() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile {
            path: "/a/x".into(), volume_id: "default".into(), chunk_size: 4096,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::AcquireWriteLease {
            path: "/a/x".into(), owner_id: "writer-1".into(), lease_ms: 60_000, now_ms: 1_000,
            proposed_lease_id: "lease-1".into(),
        });
        assert!(sm.write_leases.contains_key("/a/x"));
        apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::RenameFile { from: "/a".into(), to: "/c".into() });
        assert!(!sm.write_leases.contains_key("/a/x"), "old lease key gone");
        let lease = sm.write_leases.get("/c/x").expect("lease follows the renamed child");
        assert_eq!(lease.owner_id, "writer-1");
    }
}

// ---------------------------------------------------------------------------
// Audit CO findings — red tests that encode the desired post-fix behaviour.
// Each fn is prefixed with its finding id. These are written to FAIL against
// the current (unfixed) code; a later fix turns them green. No production
// code is modified by this module.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod audit_co_tests {
    use super::*;
    use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};

    fn mk_log_id(idx: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, idx)
    }

    // ----- CO-1-1 ----------------------------------------------------------
    // RemoveChunkLocation now carries a `disk_id`. A chunk may legally hold a
    // mandatory replica AND a cache replica on the SAME node_id across
    // DIFFERENT disks/tiers (AGENTS.md A.3.4 / Bug BH); pre-fix the retain
    // keyed on (chunk_index, node_id) only and dropped BOTH (and the
    // production `debug_assert "drop exactly one entry"` panicked first).
    // Post-fix removal targets the exact (node, disk) and the co-located
    // replica survives.
    fn sm_with_colocated_disks() -> StateMachineData {
        // One file, one chunk_index, two replicas on the SAME node_id (10)
        // but different disk_id (0 cold anchor, 1 warm cache).
        let mut sm = StateMachineData::default();
        sm.files.insert("/x".to_string(), FileMetadata {
            path: "/x".to_string(),
            volume_id: "default".to_string(),
            chunk_size: 1024,
            chunks: vec![
                ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".to_string(),
                    node_id: "10".to_string(),
                    disk_id: 0,
                    size_bytes: 1024,
                },
                ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".to_string(),
                    node_id: "10".to_string(),
                    disk_id: 1,
                    size_bytes: 1024,
                },
            ],
            created_at_ms: 1,
            updated_at_ms: 1,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        });
        sm
    }

    #[test]
    fn co_1_1_remove_chunk_location_overbroad_drops_colocated_entry() {
        // Drop the WARM disk-1 replica; the cold disk-0 anchor must survive.
        let mut sm = sm_with_colocated_disks();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: 1,
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        // Only the targeted (node 10, disk 1) entry is removed; the
        // co-located same-node disk-0 cold anchor survives.
        let chunks = &sm.files.get("/x").unwrap().chunks;
        assert_eq!(
            chunks.len(),
            1,
            "RemoveChunkLocation must drop only the targeted (node,disk) entry, \
             leaving the co-located same-node different-disk replica intact",
        );
        assert_eq!(chunks[0].disk_id, 0, "the disk-0 cold anchor must be the survivor");
    }

    #[test]
    fn co_1_1_remove_chunk_location_legacy_sentinel_is_node_wide() {
        // The legacy sentinel (u32::MAX, what an old snapshot/log entry
        // deserializes to) keeps the pre-CO-1-1 node-wide behaviour: both
        // co-located entries on node 10 are dropped.
        let mut sm = sm_with_colocated_disks();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::RemoveChunkLocation {
                path: "/x".into(),
                chunk_index: 0,
                node_id: "10".into(),
                disk_id: crate::legacy_disk_id(),
                enforce_anchor: None,
            });
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(
            sm.files.get("/x").unwrap().chunks.len(),
            0,
            "legacy node-wide removal drops every (chunk_index, node) entry",
        );
    }

    #[test]
    fn co_1_1_remove_chunk_location_disk_id_defaults_to_legacy_sentinel() {
        // A.3.1 snapshot/log compat — a RemoveChunkLocation deserialized from
        // a pre-CO-1-1 log entry (no disk_id field) gets legacy_disk_id()
        // = u32::MAX and therefore applies node-wide.
        let json = r#"{"RemoveChunkLocation":{"path":"/x","chunk_index":0,"node_id":"10","enforce_anchor":null}}"#;
        let req: ClientRequest = serde_json::from_str(json).expect("legacy RemoveChunkLocation decodes");
        match &req {
            ClientRequest::RemoveChunkLocation { disk_id, .. } => {
                assert_eq!(*disk_id, crate::legacy_disk_id());
                assert_eq!(*disk_id, u32::MAX);
            }
            other => panic!("expected RemoveChunkLocation, got {other:?}"),
        }
        let mut sm = sm_with_colocated_disks();
        let resp = apply_request(&mut sm, &mk_log_id(1), &req);
        assert!(matches!(resp, ClientResponse::Success));
        assert_eq!(
            sm.files.get("/x").unwrap().chunks.len(),
            0,
            "a legacy (no disk_id) RemoveChunkLocation must apply node-wide",
        );
    }

    // ----- CO-3-1 ----------------------------------------------------------
    // apply_request stamps SystemTime::now() into replicated state
    // (FileMetadata.created_at_ms/updated_at_ms + AuditEntry.timestamp_ms)
    // instead of a now_ms carried in the command. Two replicas applying the
    // SAME CreateFile therefore diverge byte-for-byte. Correct: identical.
    #[test]
    fn co_3_1_create_file_apply_is_deterministic_across_replicas() {
        let req = ClientRequest::CreateFile {
            path: "/det.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        };
        let mut a = StateMachineData::default();
        apply_request(&mut a, &mk_log_id(1), &req);

        // A real wall-clock gap between the two replays.
        std::thread::sleep(std::time::Duration::from_millis(5));

        let mut b = StateMachineData::default();
        apply_request(&mut b, &mk_log_id(1), &req);

        let a_bytes = serde_json::to_vec(&a).expect("serialize a");
        let b_bytes = serde_json::to_vec(&b).expect("serialize b");
        assert_eq!(
            a_bytes, b_bytes,
            "two replicas applying the SAME CreateFile must reach byte-identical \
             state; today now() is stamped at apply time so timestamps diverge",
        );
    }

    // ----- CO-4-9 ----------------------------------------------------------
    // CreateFile on an existing path inserts a fresh FileMetadata with empty
    // chunks, wiping the chunk list. Correct: an existing file's chunks are
    // preserved (CreateFile of an existing path must not silently truncate).
    #[test]
    fn co_4_9_create_file_existing_path_preserves_chunks() {
        let mut sm = StateMachineData::default();
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::CreateFile {
            path: "/keep.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::UpdateChunkLocations {
            path: "/keep.bin".into(),
            chunks: vec![ChunkLocationEntry {
                chunk_index: 0,
                chunk_hash: "h".into(),
                node_id: "10".into(),
                disk_id: 0,
                size_bytes: 1024,
            }],
            lease_id: String::new(),
        });
        assert_eq!(sm.files.get("/keep.bin").unwrap().chunks.len(), 1);

        // Re-create the same path.
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::CreateFile {
            path: "/keep.bin".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        assert_eq!(
            sm.files.get("/keep.bin").unwrap().chunks.len(),
            1,
            "CreateFile on an existing path must not wipe the chunk list",
        );
    }

    // ----- CO-4-10 ---------------------------------------------------------
    // RenameFile moves posix_locks but NOT write_leases, so a commit on the
    // new path under the still-valid lease is rejected with WriteLeaseConflict.
    // Correct: the lease follows the rename and the commit succeeds.
    #[test]
    fn co_4_10_rename_file_migrates_write_lease() {
        let mut sm = StateMachineData::default();
        // Grant a lease on /a.
        let granted = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::AcquireWriteLease {
                path: "/a".into(),
                owner_id: "host:1:7".into(),
                lease_ms: 60_000,
                now_ms: 1_000,
                proposed_lease_id: "lease-xyz".into(),
            });
        let lease_id = match granted {
            ClientResponse::WriteLeaseGranted { lease_id, .. } => lease_id,
            other => panic!("expected WriteLeaseGranted, got {other:?}"),
        };
        // Create the file so UpdateChunkLocations has a target after rename.
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile {
            path: "/a".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        // Rename /a -> /b.
        apply_request(&mut sm, &mk_log_id(3), &ClientRequest::RenameFile {
            from: "/a".into(),
            to: "/b".into(),
        });
        // Commit chunks on /b under the original lease.
        let resp = apply_request(&mut sm, &mk_log_id(4),
            &ClientRequest::UpdateChunkLocations {
                path: "/b".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 1024,
                }],
                lease_id,
            });
        assert!(
            matches!(resp, ClientResponse::Success),
            "the write lease must follow RenameFile so the holder can keep \
             committing under it; got {resp:?}",
        );
    }

    // ----- CO-4-11 ---------------------------------------------------------
    // pending_scrub is not part of StateMachineSnapshot, so build_snapshot
    // drops it and install_snapshot leaves the follower's stale value. Correct:
    // an installed snapshot makes the follower's pending_scrub match the source.
    #[tokio::test]
    async fn co_4_11_pending_scrub_survives_snapshot_install() {
        let src_dir = tempfile::tempdir().expect("tmp src");
        let snapshot_bytes;
        let snapshot_meta;
        let source_pending;
        {
            let mut src = StateMachineStore::open_at(src_dir.path()).expect("open src");
            {
                let mut data = src.data.write().await;
                apply_request(&mut data, &mk_log_id(1), &ClientRequest::TriggerScrub {
                    id: "src-scrub".into(),
                    level: "deep".into(),
                    requested_at_ms: 9_999,
                });
                source_pending = data.pending_scrub.clone();
            }
            let mut builder = src.get_snapshot_builder().await;
            let snap = builder.build_snapshot().await.expect("build");
            snapshot_meta = snap.meta.clone();
            snapshot_bytes = snap.snapshot.into_inner();
        }
        assert!(source_pending.is_some(), "precondition: source has a pending scrub");

        // Follower pre-seeded with a DIFFERENT pending_scrub.
        let fol_dir = tempfile::tempdir().expect("tmp follower");
        let mut follower = StateMachineStore::open_at(fol_dir.path()).expect("open follower");
        {
            let mut data = follower.data.write().await;
            apply_request(&mut data, &mk_log_id(1), &ClientRequest::TriggerScrub {
                id: "stale-follower".into(),
                level: "quick".into(),
                requested_at_ms: 1,
            });
        }
        follower
            .install_snapshot(&snapshot_meta, Cursor::new(snapshot_bytes))
            .await
            .expect("install");

        let data = follower.data.read().await;
        assert_eq!(
            data.pending_scrub, source_pending,
            "installing the source's snapshot must replace the follower's \
             pending_scrub with the source's value; today the field is not \
             part of StateMachineSnapshot so the stale follower value survives",
        );
    }

    // ----- CO-4-15 ---------------------------------------------------------
    // An empty lease_id bypasses the write-lease check on UpdateChunkLocations,
    // so a writer with no token can commit while holder A's lease is live.
    // Correct: with a live lease held by A, a lease-less commit is rejected.
    #[test]
    fn co_4_15_empty_lease_id_does_not_bypass_held_lease() {
        let mut sm = StateMachineData::default();
        // Holder A acquires the lease on /f.
        apply_request(&mut sm, &mk_log_id(1), &ClientRequest::AcquireWriteLease {
            path: "/f".into(),
            owner_id: "hostA:1:7".into(),
            lease_ms: 60_000,
            now_ms: 1_000,
            proposed_lease_id: "lease-A".into(),
        });
        apply_request(&mut sm, &mk_log_id(2), &ClientRequest::CreateFile {
            path: "/f".into(),
            volume_id: "default".into(),
            chunk_size: 1024,
        });
        // A different writer commits with an EMPTY lease_id while A holds it.
        let resp = apply_request(&mut sm, &mk_log_id(3),
            &ClientRequest::UpdateChunkLocations {
                path: "/f".into(),
                chunks: vec![ChunkLocationEntry {
                    chunk_index: 0,
                    chunk_hash: "h".into(),
                    node_id: "10".into(),
                    disk_id: 0,
                    size_bytes: 1024,
                }],
                lease_id: String::new(),
            });
        assert!(
            matches!(resp, ClientResponse::WriteLeaseConflict { .. }),
            "a commit with an empty lease_id must NOT bypass a live lease held \
             by another writer; got {resp:?}",
        );
    }

    // ----- CO-5-1 ----------------------------------------------------------
    // AcquireWriteLease computes `now_ms + lease_ms` unchecked, so lease_ms =
    // u64::MAX panics (debug overflow). Correct: no panic; grant a finite,
    // future expiry (saturating).
    #[test]
    fn co_5_1_acquire_write_lease_huge_lease_ms_does_not_overflow() {
        let mut sm = StateMachineData::default();
        let resp = apply_request(&mut sm, &mk_log_id(1),
            &ClientRequest::AcquireWriteLease {
                path: "/big".into(),
                owner_id: "host:1:7".into(),
                lease_ms: u64::MAX,
                now_ms: 1_000,
                proposed_lease_id: "lease-big".into(),
            });
        match resp {
            ClientResponse::WriteLeaseGranted { expires_at_ms, .. } => {
                assert!(
                    expires_at_ms > 1_000,
                    "expiry must be a finite future instant, got {expires_at_ms}",
                );
            }
            other => panic!("expected WriteLeaseGranted, got {other:?}"),
        }
    }

    // ----- CO-5-10 ---------------------------------------------------------
    // update_config read-then-writes `version = current + 1` and the apply
    // used to blind-overwrite. Two saves racing off the same base version
    // both target the same new version; without a CAS the second silently
    // loses the first's update. With `expected_version`, the apply rejects
    // the loser instead of clobbering the winner.
    #[test]
    fn co_5_10_update_config_rejects_stale_expected_version() {
        let mut sm = StateMachineData::default();
        assert_eq!(sm.cluster_config.version, 0);

        // Both writers read base version 0 and build version 1.
        let first = ClientRequest::UpdateConfig {
            config_json: r#"{"a":1}"#.into(),
            version: 1,
            expected_version: 0,
        };
        let second = ClientRequest::UpdateConfig {
            config_json: r#"{"b":2}"#.into(),
            version: 1,
            expected_version: 0,
        };

        let r1 = apply_request(&mut sm, &mk_log_id(1), &first);
        assert!(matches!(r1, ClientResponse::Success));
        assert_eq!(sm.cluster_config.version, 1);
        assert_eq!(sm.cluster_config.config_json, r#"{"a":1}"#);

        // The loser carries expected_version 0, which no longer matches the
        // live version 1 -> rejected, no overwrite.
        let r2 = apply_request(&mut sm, &mk_log_id(2), &second);
        assert!(
            matches!(r2, ClientResponse::Error { .. }),
            "stale-version UpdateConfig must be rejected, got {r2:?}",
        );
        assert_eq!(sm.cluster_config.version, 1, "loser must not bump version");
        assert_eq!(
            sm.cluster_config.config_json, r#"{"a":1}"#,
            "loser must not clobber the winner's config",
        );
    }

    // CO-5-10 compat (A.3.1): a UpdateConfig serialised before the
    // `expected_version` field existed must still deserialise (defaulting to
    // the UPDATE_CONFIG_NO_CAS sentinel) and apply WITHOUT a CAS check, so
    // replaying an old Raft log / snapshot never wedges on a phantom version
    // conflict.
    #[test]
    fn co_5_10_update_config_legacy_no_expected_version_applies() {
        let legacy = r#"{"UpdateConfig":{"config_json":"{}","version":7}}"#;
        let req: ClientRequest =
            serde_json::from_str(legacy).expect("legacy UpdateConfig must deserialise");
        match &req {
            ClientRequest::UpdateConfig { expected_version, version, .. } => {
                assert_eq!(
                    *expected_version,
                    crate::UPDATE_CONFIG_NO_CAS,
                    "missing field defaults to the no-CAS sentinel",
                );
                assert_eq!(*version, 7);
            }
            other => panic!("expected UpdateConfig, got {other:?}"),
        }

        // Apply against a non-zero live version; legacy entry (sentinel)
        // must skip the CAS and apply.
        let mut sm = StateMachineData::default();
        sm.cluster_config.version = 4;
        let resp = apply_request(&mut sm, &mk_log_id(1), &req);
        assert!(
            matches!(resp, ClientResponse::Success),
            "legacy entry must apply without a CAS check, got {resp:?}",
        );
        assert_eq!(sm.cluster_config.version, 7);
    }

    // CO-5-10 — the version-0 boundary: two writes racing off a fresh
    // cluster both read base version 0 and carry expected_version 0. With a
    // sentinel-based skip (not a 0-based skip), the second is still CAS-
    // checked against the now-live version 1 and rejected, so the first
    // write is not lost on a brand-new cluster.
    #[test]
    fn co_5_10_concurrent_first_write_from_version_zero_is_cas_checked() {
        let mut sm = StateMachineData::default();
        assert_eq!(sm.cluster_config.version, 0);

        let first = ClientRequest::UpdateConfig {
            config_json: r#"{"a":1}"#.into(),
            version: 1,
            expected_version: 0,
        };
        let second = ClientRequest::UpdateConfig {
            config_json: r#"{"b":2}"#.into(),
            version: 1,
            expected_version: 0,
        };

        let r1 = apply_request(&mut sm, &mk_log_id(1), &first);
        assert!(matches!(r1, ClientResponse::Success));
        assert_eq!(sm.cluster_config.version, 1);

        let r2 = apply_request(&mut sm, &mk_log_id(2), &second);
        assert!(
            matches!(r2, ClientResponse::Error { .. }),
            "a second version-0-based write must be rejected once version is 1, got {r2:?}",
        );
        assert_eq!(sm.cluster_config.config_json, r#"{"a":1}"#);
    }
}
