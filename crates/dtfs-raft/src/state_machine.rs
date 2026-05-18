//! Raft state machine — the cluster-wide FSM that holds all metadata.
//!
//! State is kept entirely in memory. Snapshots are serialized to JSON via
//! [`crate::snapshot`] and stored as `Cursor<Vec<u8>>` blobs.

use std::collections::BTreeMap;
use std::io::{self, Cursor};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::EntryPayload;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use dtfs_common::types::VolumeConfig;

use crate::snapshot::{self, StateMachineSnapshot};
use crate::{
    merge_chunk_location_entries, ChunkLocationEntry, ClientRequest, ClientResponse, TypeConfig,
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuditEntry {
    pub log_index: u64,
    pub summary: String,
    pub timestamp_ms: u64,
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
    /// `tests/proxmox/SOAK-RECOVERY-2026-05-18.md`:
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
    /// `tests/proxmox/SOAK-RECOVERY-2026-05-18.md`.
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

        let path = dir.join("snapshot.bin");
        if path.exists() {
            let bytes = std::fs::read(&path)?;
            let persisted: PersistedSnapshot = bincode::deserialize(&bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            // Replay the snapshot contents into the in-memory FSM.
            let snap: StateMachineSnapshot = serde_json::from_slice(&persisted.data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
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
            sm.client_count = snap.client_count;
            sm.volumes = snap.volumes;
            sm.migration_state_json = snap.migration_state_json;
            sm.posix_locks = snap.posix_locks;
            sm.write_leases = snap.write_leases;
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
        std::fs::write(&tmp, &buf)?;
        std::fs::rename(&tmp, &final_path)?;
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
            client_count: sm.client_count,
            volumes: sm.volumes.clone(),
            migration_state_json: sm.migration_state_json.clone(),
            posix_locks: sm.posix_locks.clone(),
            write_leases: sm.write_leases.clone(),
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
            sm.client_count = snap_data.client_count;
            sm.volumes = snap_data.volumes;
            sm.migration_state_json = snap_data.migration_state_json;
            sm.posix_locks = snap_data.posix_locks;
            sm.write_leases = snap_data.write_leases;
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
            sm.files.insert(
                canonical.clone(),
                FileMetadata {
                    path: canonical.clone(),
                    volume_id: volume_id.clone(),
                    chunk_size: *chunk_size,
                    chunks: Vec::new(),
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                    symlink_target: None,
                    logical_size: None,
                    is_directory: is_dir,
                },
            );
            push_audit_capped(
                &mut sm.audit_log,
                audit(format!(
                    "CreateFile: {canonical}{}",
                    if is_dir { " (directory)" } else { "" }
                )),
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

        ClientRequest::UpdateChunkLocations { path, chunks } => {
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

        ClientRequest::RegisterNode { node_id, info } => {
            sm.nodes.insert(*node_id, info.clone());
            push_audit_capped(&mut sm.audit_log, audit(format!("RegisterNode: {node_id}")));
            ClientResponse::Success
        }

        ClientRequest::DeregisterNode { node_id } => {
            sm.nodes.remove(node_id);
            push_audit_capped(&mut sm.audit_log, audit(format!("DeregisterNode: {node_id}")));
            ClientResponse::Success
        }

        ClientRequest::UpdateNodeStatus { node_id, status } => {
            use dtfs_common::types::HealthStatus;
            if let Some(node) = sm.nodes.get_mut(node_id) {
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
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("UpdateNodeStatus: {node_id} -> {status}")),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("node not found: {node_id}"),
                }
            }
        }

        ClientRequest::UpdateNodeUsage {
            node_id,
            used_capacity,
            chunk_count: _chunk_count,
        } => {
            if let Some(node) = sm.nodes.get_mut(node_id) {
                node.used_capacity = *used_capacity;
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("UpdateNodeUsage: {node_id} used={used_capacity}")),
                );
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
        } => {
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
            // Store per-node: each data node maintains its own latest result.
            // Previously only the single most-recent cluster-wide result was
            // kept, which caused a fast scrub on node B to silently drop node
            // A's result (fix for issue #8).
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
            // Remove the source's metadata, rewrite its path, re-insert under the new key.
            if let Some(mut meta) = sm.files.remove(from) {
                meta.path = to.clone();
                meta.updated_at_ms = now_ms;
                sm.files.insert(to.clone(), meta);
                // Migrate any active locks on the path too.
                if let Some(locks) = sm.posix_locks.remove(from) {
                    sm.posix_locks.insert(to.clone(), locks);
                }
                push_audit_capped(
                    &mut sm.audit_log,
                    audit(format!("RenameFile: {from} -> {to}")),
                );
                ClientResponse::Success
            } else {
                ClientResponse::Error {
                    message: format!("RenameFile: source '{from}' not found"),
                }
            }
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
                expires_at_ms: *now_ms + *lease_ms,
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
                let new_expires = *now_ms + *lease_ms;
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
            let expires_at_ms = *now_ms + *lease_ms;
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
                let new_expires = *now_ms + *extension_ms;
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
        } => {
            // Recommended owner_id format is `<hostname>:<pid>:<fh>`; match
            // the hostname prefix and ensure the PID segment differs from
            // `except_pid` (the live FUSE process). This drops orphan leases
            // left by a crashed previous FUSE process while keeping leases
            // from healthy concurrent mounts on the same host.
            let prefix = format!("{hostname_prefix}:");
            let except_segment = format!(":{except_pid}:");
            let to_drop: Vec<String> = sm
                .write_leases
                .iter()
                .filter(|(_, l)| {
                    l.owner_id.starts_with(&prefix) && !l.owner_id.contains(&except_segment)
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
    }
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
}

// ---------------------------------------------------------------------------
// Regression tests for three live-lab bugs uncovered during the 2026-05-18
// soak recovery (see tests/proxmox/SOAK-RECOVERY-2026-05-18.md):
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
}
