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
use tracing::{debug, error, info, warn};

use dtfs_common::types::{AnchorPolicy, VolumeConfig};

use crate::snapshot::{self, StateMachineSnapshot};
use crate::{
    merge_chunk_location_entries, ChunkLocationEntry, ClientRequest, ClientResponse, TypeConfig,
};
#[cfg(test)]
use crate::legacy_disk_id;

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

        // Persist to disk FIRST so a manager restart can reload membership +
        // state (F-024). A failure must surface: openraft purges the log
        // after a successful build, and a snapshot that never hit disk would
        // leave the restarted node with a purged log and no state.
        if let Some(dir) = &self.snapshot_dir {
            if let Err(e) = StateMachineStore::persist_snapshot_to_disk(dir, &meta, &data_bytes) {
                error!(error = %e, snapshot_id = %meta.snapshot_id,
                    "failed to persist built snapshot; not publishing it");
                return Err(e);
            }
        }

        // Cache the freshly built snapshot in memory so `get_current_snapshot`
        // returns the up-to-date blob (leader needs this to send to followers).
        {
            let mut slot = self.last_snapshot.write().await;
            *slot = Some(StoredSnapshot {
                meta: meta.clone(),
                data: data_bytes.clone(),
            });
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

        // Persist before touching any in-memory state (F-024): on failure the
        // old state stays intact and openraft sees the error.
        if let Some(dir) = &self.snapshot_dir {
            if let Err(e) = Self::persist_snapshot_to_disk(dir, meta, &raw) {
                error!(error = %e, snapshot_id = %meta.snapshot_id,
                    "failed to persist installed snapshot; state unchanged");
                return Err(e);
            }
        }

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

    // RAFT-4 — `audit` above stamps the replica-local wall clock, which is
    // fine for arms that don't otherwise need determinism. The seven
    // proposer-stamped arms (UpdateChunkLocations, RemoveCorruptChunkReplicas,
    // RenameFile, SetFileSize, CreateSymlink, CreateLink, RemoveChunkLocation)
    // must apply as a pure function of (req, state): `audit_at` takes an
    // explicit timestamp instead of capturing the outer `now_ms`, so those
    // arms can route both `updated_at_ms` and their own audit row through the
    // proposer's `now_ms` (falling back to `log_id.index` for a legacy/`0`
    // entry, CO-3-1 pattern) and stay byte-identical across replicas.
    let audit_at = |summary: String, ts: u64| AuditEntry {
        log_index: log_id.index,
        summary,
        timestamp_ms: ts,
        actor: String::new(),
        action: String::new(),
        target: String::new(),
        result: String::new(),
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
            // RAFT-7 — a bare `sm.files.remove(path)` leaked the file's
            // write lease + posix locks forever (re-creating the same path
            // within `lease_ms` hit a stale WriteLeaseConflict), and never
            // checked for children, so FUSE's `rmdir` -> `unlink` deleted
            // only the directory marker while the tree underneath stayed
            // live — `rm -r` reported success with files remaining.
            let prefix = format!("{path}/");
            let has_children = sm
                .files
                .range(prefix.clone()..)
                .take_while(|(k, _)| k.starts_with(&prefix))
                .next()
                .is_some();
            if has_children {
                return ClientResponse::Error {
                    message: format!("DeleteFile: '{path}' directory not empty"),
                };
            }
            sm.files.remove(path);
            sm.write_leases.remove(path);
            sm.posix_locks.remove(path);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeleteFile: {path}")));
            ClientResponse::Success
        }

        ClientRequest::UpdateChunkLocations { path, chunks, lease_id, now_ms: req_now_ms } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
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
                file.updated_at_ms = eff_now_ms;
                // RAFT-3 — `logical_size` is set once by SetFileSize (e.g.
                // O_TRUNC → SetFileSize(0)) and never revisited, so a
                // truncate-then-rewrite (cp, rsync --inplace, shell `>`,
                // SQLite journal recycle) committed chunks past the
                // truncated size but every reader kept seeing the stale
                // (usually 0) size. Grow logical_size to cover the extent
                // implied by the merged chunks, but never derive a size out
                // of thin air for a file that has none set yet (None stays
                // None; a chunk-only file still reports the summed-chunks
                // fallback in metadata.rs).
                let extent = file
                    .chunks
                    .iter()
                    .map(|c| c.chunk_index * file.chunk_size + c.size_bytes)
                    .max();
                if let Some(extent) = extent {
                    file.logical_size = file.logical_size.map(|l| l.max(extent));
                }
                push_audit_capped(&mut sm.audit_log, audit_at(format!(
                    "UpdateChunks: {path} (merged {} incoming -> {} total)",
                    chunks.len(),
                    file.chunks.len()
                ), eff_now_ms));
                ClientResponse::Success
            } else {
                warn!(path = %path, "UpdateChunkLocations: file not found");
                ClientResponse::Error {
                    message: format!("file not found: {path}"),
                }
            }
        }

        ClientRequest::RemoveCorruptChunkReplicas { node_id, chunk_hashes, disk_id, now_ms: req_now_ms } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
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
                    file.updated_at_ms = eff_now_ms;
                }
            }
            push_audit_capped(
                &mut sm.audit_log,
                audit_at(format!(
                    "RemoveCorruptChunkReplicas: node={node_id} disk={disk_id} chunks={} dropped_entries={} affected_files={}",
                    chunk_hashes.len(),
                    dropped,
                    affected_files,
                ), eff_now_ms),
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
                        warn!(status = %other, "unknown node status");
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
            version: _, // RAFT-8 — apply always derives its own version.
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
            // RAFT-8 — the CAS check above only pins the EXPECTED base
            // version; the stored `version` came straight from the wire, so
            // a buggy/malicious proposer could regress it (e.g. resubmit an
            // old value) even though the base matched. The apply is the
            // sole authority: it always derives the new version itself.
            let new_version = sm.cluster_config.version + 1;
            sm.cluster_config = ClusterConfig {
                config_json: config_json.clone(),
                version: new_version,
            };
            push_audit_capped(&mut sm.audit_log, audit(format!("UpdateConfig: v{new_version}")));
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
            // RAFT-9 — serde_json serialises NaN/inf as `null`, which then
            // fails to deserialise as f64 on the very next snapshot load
            // (every `install_snapshot` fails, discarding the whole
            // snapshot -> RAFT-6). The RPC is CLI/REST-injectable, so clamp
            // any non-finite value at the apply boundary rather than trust
            // the reporter (the real data node always sends finite values).
            let duration_secs = if duration_secs.is_finite() { *duration_secs } else { 0.0 };
            let record = ScrubRecord {
                node_id: *node_id,
                disk_path: disk_path.clone(),
                level: level.clone(),
                completed_at_ms: *completed_at_ms,
                chunks_checked: *chunks_checked,
                chunks_healthy: *chunks_healthy,
                chunks_corrupt: *chunks_corrupt,
                chunks_missing: *chunks_missing,
                duration_secs,
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

        ClientRequest::RenameFile { from, to, now_ms: req_now_ms } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
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
                meta.updated_at_ms = eff_now_ms;
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
                    meta.updated_at_ms = eff_now_ms;
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
                audit_at(format!(
                    "RenameFile: {from} -> {to} ({moved_children} children moved)"
                ), eff_now_ms),
            );
            ClientResponse::Success
        }

        ClientRequest::SetFileSize { path, new_size, now_ms: req_now_ms } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
            if let Some(meta) = sm.files.get_mut(path) {
                meta.logical_size = Some(*new_size);
                meta.updated_at_ms = eff_now_ms;
                // Truncate-shrink: drop chunks fully past the new EOF.
                let chunk_size = meta.chunk_size.max(1);
                let keep_idx = (*new_size).div_ceil(chunk_size);
                meta.chunks.retain(|c| c.chunk_index < keep_idx);
                push_audit_capped(
                    &mut sm.audit_log,
                    audit_at(format!("SetFileSize: {path} -> {new_size}"), eff_now_ms),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("SetFileSize: '{path}' not found"),
                }
            }
        }

        ClientRequest::CreateSymlink { path, target, now_ms: req_now_ms } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
            sm.files.insert(
                path.clone(),
                FileMetadata {
                    path: path.clone(),
                    volume_id: "default".to_string(),
                    chunk_size: 0,
                    chunks: Vec::new(),
                    created_at_ms: eff_now_ms,
                    updated_at_ms: eff_now_ms,
                    symlink_target: Some(target.clone()),
                    logical_size: Some(target.len() as u64),
                    is_directory: false,
                    cache_cooldown_until_ms: std::collections::BTreeMap::new(),
                },
            );
            push_audit_capped(
                &mut sm.audit_log,
                audit_at(format!("Symlink: {path} -> {target}"), eff_now_ms),
            );
            ClientResponse::Success
        }

        ClientRequest::CreateLink { source, dest, now_ms: req_now_ms } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
            // Hard link: clone the source metadata under the dest path.
            // The chunk list is shared by value (chunk_ids are content-addressed
            // so the same bytes remain accessible from both names).
            if let Some(src_meta) = sm.files.get(source).cloned() {
                let mut new_meta = src_meta;
                new_meta.path = dest.clone();
                new_meta.created_at_ms = eff_now_ms;
                new_meta.updated_at_ms = eff_now_ms;
                sm.files.insert(dest.clone(), new_meta);
                push_audit_capped(
                    &mut sm.audit_log,
                    audit_at(format!("Link: {source} -> {dest}"), eff_now_ms),
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
            now_ms: req_now_ms,
        } => {
            let eff_now_ms = if *req_now_ms == 0 { log_id.index } else { *req_now_ms };
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
                eff_now_ms,
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
    now_ms: u64,
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

    // CR-3-2/MGR-2 (defence-in-depth, independent of enforce_anchor) —
    // a replica is "live" when its node is not Down/Failed and its disk
    // is not Failed/Removed (same test as the scheduler's
    // `live_replica_tier` / the anchor scan above). If the row being
    // removed IS live, refuse when no OTHER live replica of this
    // chunk_index survives — a dead row can always be removed (it isn't
    // serving anything), but the last serviceable copy must never be
    // dropped, anchor policy or not. Runs after the anchor check so a
    // volume with an anchor policy keeps its more specific "would strand
    // anchor" message when both checks would refuse the same removal.
    let is_live = |c: &ChunkLocationEntry| -> bool {
        let Ok(nid) = c.node_id.parse::<u64>() else {
            return false;
        };
        let Some(ninfo) = sm.nodes.get(&nid) else {
            return false;
        };
        if matches!(
            ninfo.health,
            dtfs_common::types::HealthStatus::Down | dtfs_common::types::HealthStatus::Failed
        ) {
            return false;
        }
        ninfo
            .disk(c.disk_id)
            .map(|d| {
                !matches!(
                    d.status,
                    dtfs_common::types::DiskStatus::Failed | dtfs_common::types::DiskStatus::Removed
                )
            })
            .unwrap_or(false)
    };
    let removing_a_live_replica = file.chunks.iter().any(|c| matches(c) && is_live(c));
    if removing_a_live_replica {
        let other_live_survives = file
            .chunks
            .iter()
            .any(|c| !matches(c) && c.chunk_index == chunk_index && is_live(c));
        if !other_live_survives {
            return ClientResponse::Error {
                message: format!(
                    "RemoveChunkLocation refused: would remove the last live replica of {path}#{chunk_index}"
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
    // RAFT-4 — was a fresh `SystemTime::now()` per replica; now the
    // proposer-stamped (or `log_id.index`-fallback) deterministic value.
    file_mut.updated_at_ms = now_ms;
    ClientResponse::Success
}

// ---------------------------------------------------------------------------
// Tests — snapshot persistence (regression test for F-024)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Audit CO findings — red tests that encode the desired post-fix behaviour.
// Each fn is prefixed with its finding id. These are written to FAIL against
// the current (unfixed) code; a later fix turns them green. No production
// code is modified by this module.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
