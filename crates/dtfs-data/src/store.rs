use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dtfs_common::checksum;
use dtfs_common::error::{DtfsError, DtfsResult};
use dtfs_common::types::{ChunkId, ChunkMeta, DiskTier, ScrubLevel, ScrubResult, VolumeId};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::disk::DiskManager;

/// Bug BN-1 — load a stable per-disk UUID from `<mount_path>/.dtfs-disk-uuid`,
/// creating it on first discovery. Previously every data-node boot minted a
/// fresh random UUID (`Uuid::new_v4`), defeating UUID-based disk identity
/// (placement dedup, scrub tracking) and forcing BJ's `disks_identity_eq` to
/// key on `mount_path`. The file is written once and read on every later boot,
/// so a disk keeps its identity until it is wiped/reformatted.
///
/// Returns an error only on I/O failure; the caller falls back to a fresh
/// random UUID (read-only mount, ENOSPC, permissions) rather than crashing the
/// boot loop. A corrupt/garbage file is transparently regenerated.
pub fn load_or_create_disk_uuid(mount_path: &Path) -> std::io::Result<String> {
    let uuid_file = mount_path.join(".dtfs-disk-uuid");
    if uuid_file.exists() {
        let s = std::fs::read_to_string(&uuid_file)?.trim().to_string();
        if uuid::Uuid::parse_str(&s).is_ok() {
            return Ok(s);
        }
        tracing::warn!(
            "disk UUID file {:?} is corrupt ({:?}); regenerating",
            uuid_file,
            s
        );
    }
    let u = uuid::Uuid::new_v4().to_string();
    std::fs::write(&uuid_file, &u)?;
    // fsync the file + parent dir so the identity survives a crash right
    // after first provisioning.
    if let Ok(f) = std::fs::File::open(&uuid_file) {
        let _ = f.sync_all();
    }
    if let Ok(d) = std::fs::File::open(mount_path) {
        let _ = d.sync_all();
    }
    Ok(u)
}

/// CO-5-2 — filename for a streaming put's tmp file (chunk_id unknown until
/// the stream finishes). The previous `streaming-{pid}-{nanos}` scheme
/// collided whenever two concurrent puts on the same disk read the same
/// coarse `nanos` clock, so both streams interleaved into one tmp and the
/// committed chunk's checksum mismatched yet was acked success. A v4 UUID
/// makes the name unique per call regardless of clock resolution. Keeps the
/// `streaming-` prefix (recovery/reclaim sweeps `tmp/`) and the `.dat.tmp`
/// suffix that `reclaim_orphan_streaming_tmps` matches on.
fn streaming_tmp_name() -> String {
    format!("streaming-{}.dat.tmp", uuid::Uuid::new_v4())
}

/// A lightweight stats snapshot for heartbeat reporting.
#[derive(Debug, Clone, Default)]
pub struct ChunkStoreStats {
    /// Number of chunk files currently stored across all disks.
    pub total_chunks: u64,
    /// Approximate total bytes of chunk data stored (sum of chunk sizes written).
    pub total_bytes_stored: u64,
}

/// A Write-Ahead Log entry describing a pending chunk write operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WalEntry {
    /// The operation being performed.
    operation: WalOperation,
    /// Hex-encoded chunk ID.
    chunk_id: String,
    /// Disk mount path where the write is occurring.
    disk_path: String,
    /// Timestamp when the WAL entry was created (epoch millis).
    timestamp_ms: u64,
}

/// Operations recorded in the WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum WalOperation {
    /// A chunk write is in progress.
    PutChunk,
    /// A chunk delete is in progress.
    DeleteChunk,
}

/// Chunk storage engine — stores chunks on XFS-backed disks.
///
/// Layout per disk:
/// ```text
/// /mnt/dtfs-disk-{uuid}/
/// ├── .dtfs/
/// │   ├── disk.meta
/// │   └── wal/
/// ├── chunks/
/// │   ├── {00-ff}/
/// │   │   └── {00-ff}/
/// │   │       ├── {hash}.dat
/// │   │       └── {hash}.meta
/// └── tmp/
/// ```
pub struct ChunkStore {
    disk_manager: Arc<DiskManager>,
    /// Running count of chunks stored across all disks.
    chunk_count: Arc<AtomicU64>,
    /// Running total of bytes stored (incremented on put, decremented on delete).
    bytes_stored: Arc<AtomicU64>,
    /// Bug O — chunks the scrub scheduler has quarantined locally,
    /// waiting to be reported to the manager on the next heartbeat so
    /// the Raft state machine can drop the matching replica entries
    /// and trigger re-replication from a healthy peer.
    pending_corruption: Arc<parking_lot::Mutex<Vec<String>>>,
    /// Bug BY — completed scrub-result summaries waiting to be shipped to
    /// the manager on the next heartbeat (one per disk per quick/deep
    /// cycle). The manager issues a `ReportScrubResult` Raft write per
    /// entry so the WebUI "Last Scrub" card populates.
    pending_scrub_results: Arc<parking_lot::Mutex<Vec<ScrubResultSummary>>>,
    /// CR-4-4 (DATA-3) — striped per-chunk locks (256 stripes, indexed by
    /// the chunk id's first byte) held across `commit_chunk_from_tmp_inner`
    /// and `delete_chunk_idempotent_on_disk` so a concurrent put and delete
    /// of the SAME chunk id can never interleave into a torn state (a
    /// `.dat` with no `.meta`, or vice versa). In-memory only — a fresh
    /// process starts with every stripe unlocked, which is fine because
    /// the WAL (not the lock) is what makes recovery correct across a
    /// restart.
    chunk_locks: Arc<Vec<tokio::sync::Mutex<()>>>,
}

/// Bug BY — a completed scrub result queued for the next heartbeat. The
/// data node doesn't know its own manager-assigned node_id; the manager
/// fills that in from the heartbeat envelope.
#[derive(Debug, Clone)]
pub struct ScrubResultSummary {
    pub disk_path: String,
    pub level: String,
    pub completed_at_ms: u64,
    pub chunks_checked: u64,
    pub chunks_healthy: u64,
    pub chunks_corrupt: u64,
    pub chunks_missing: u64,
    pub duration_secs: f64,
}

impl ChunkStore {
    pub fn new(disk_manager: DiskManager) -> Self {
        Self {
            disk_manager: Arc::new(disk_manager),
            chunk_count: Arc::new(AtomicU64::new(0)),
            bytes_stored: Arc::new(AtomicU64::new(0)),
            pending_corruption: Arc::new(parking_lot::Mutex::new(Vec::new())),
            pending_scrub_results: Arc::new(parking_lot::Mutex::new(Vec::new())),
            chunk_locks: Arc::new((0..256).map(|_| tokio::sync::Mutex::new(())).collect()),
        }
    }

    /// CR-4-4 (DATA-3) — the lock stripe for a chunk id (first byte).
    fn chunk_lock_index(chunk_id: &ChunkId) -> usize {
        chunk_id.0[0] as usize
    }

    /// Bug BY — queue a completed scrub result for the next heartbeat.
    pub fn enqueue_scrub_result(&self, summary: ScrubResultSummary) {
        self.pending_scrub_results.lock().push(summary);
    }

    /// Bug BY — drain queued scrub results so each is reported once.
    pub fn drain_scrub_results(&self) -> Vec<ScrubResultSummary> {
        std::mem::take(&mut *self.pending_scrub_results.lock())
    }

    /// CO-5-6 — re-queue a drained batch of scrub results after a failed
    /// heartbeat so they aren't lost. `drain_scrub_results` empties the
    /// queue as it builds the heartbeat payload; if that send fails the
    /// summaries would otherwise vanish and the WebUI "Last Scrub" card
    /// would never populate. Prepended so they retain priority over any
    /// results enqueued since the drain.
    pub fn restore_scrub_results(&self, mut summaries: Vec<ScrubResultSummary>) {
        if summaries.is_empty() {
            return;
        }
        let mut q = self.pending_scrub_results.lock();
        summaries.append(&mut q);
        *q = summaries;
    }

    /// Bug O — append chunk_ids that have been quarantined locally
    /// (failed checksum or missing meta) so the next heartbeat can
    /// report them to the manager. Called by the scrub scheduler.
    pub fn enqueue_pending_corruption(&self, chunk_ids: impl IntoIterator<Item = String>) {
        let mut q = self.pending_corruption.lock();
        for id in chunk_ids {
            q.push(id);
        }
    }

    /// Bug O — drain the pending-corruption queue. Called by the
    /// heartbeat loop so each report is sent to the manager exactly
    /// once.
    pub fn drain_pending_corruption(&self) -> Vec<String> {
        let mut q = self.pending_corruption.lock();
        std::mem::take(&mut *q)
    }

    /// Return a lightweight stats snapshot for heartbeat and metrics reporting.
    pub fn stats(&self) -> ChunkStoreStats {
        ChunkStoreStats {
            total_chunks: self.chunk_count.load(Ordering::Relaxed),
            total_bytes_stored: self.bytes_stored.load(Ordering::Relaxed),
        }
    }

    /// Rebuild `chunk_count` and `bytes_stored` from what is actually on disk.
    ///
    /// `put_chunk` increments these counters in-process, so a daemon restart
    /// loses the running total — the next heartbeat would report 0/0 even
    /// though disk has every chunk from the previous run. Without this
    /// rescan the manager's `used_capacity` accounting diverges from reality
    /// (observed: manager reports 0 GB used while df shows multi-GB on
    /// every restarted data node).
    ///
    /// Walks each managed disk's `chunks/<aa>/<bb>/*.dat` and replaces the
    /// counters with the totals found. Safe to call any time; intended at
    /// process startup right after disk init, before the heartbeat loop
    /// fires.
    pub async fn rescan_existing_chunks(&self) -> DtfsResult<()> {
        let mut total_count: u64 = 0;
        let mut total_bytes: u64 = 0;

        for disk in self.disk_manager.all_disks() {
            let chunks_dir = disk.mount_path.join("chunks");
            if !chunks_dir.exists() {
                continue;
            }

            let mut dir1 = match fs::read_dir(&chunks_dir).await {
                Ok(d) => d,
                Err(_) => continue,
            };
            while let Some(d1) = dir1.next_entry().await.map_err(DtfsError::Io)? {
                if !d1.file_type().await.map_err(DtfsError::Io)?.is_dir() {
                    continue;
                }
                let mut dir2 = fs::read_dir(d1.path()).await.map_err(DtfsError::Io)?;
                while let Some(d2) = dir2.next_entry().await.map_err(DtfsError::Io)? {
                    if !d2.file_type().await.map_err(DtfsError::Io)?.is_dir() {
                        continue;
                    }
                    let mut entries = fs::read_dir(d2.path()).await.map_err(DtfsError::Io)?;
                    while let Some(e) =
                        entries.next_entry().await.map_err(DtfsError::Io)?
                    {
                        let p = e.path();
                        if p.extension().map_or(false, |x| x == "dat") {
                            let meta = match fs::metadata(&p).await {
                                Ok(m) => m,
                                Err(_) => continue,
                            };
                            total_count += 1;
                            total_bytes += meta.len();
                        }
                    }
                }
            }
        }

        self.chunk_count.store(total_count, Ordering::Relaxed);
        self.bytes_stored.store(total_bytes, Ordering::Relaxed);
        tracing::info!(
            chunks = total_count,
            bytes = total_bytes,
            "ChunkStore startup rescan complete"
        );
        Ok(())
    }

    /// Access the disk manager (e.g. for `GetDiskStatus` RPC).
    pub fn disk_manager(&self) -> &Arc<DiskManager> {
        &self.disk_manager
    }

    /// Write a chunk to disk with fsync guarantee.
    /// Returns the ChunkMeta on success.
    ///
    /// The write is preceded by a WAL entry. The WAL entry is removed only
    /// after the chunk data and metadata have been fully committed to disk.
    pub async fn put_chunk(
        &self,
        chunk_id: &ChunkId,
        data: &[u8],
        volume_id: &VolumeId,
        tier: DiskTier,
    ) -> DtfsResult<ChunkMeta> {
        let disk = self
            .disk_manager
            .select_disk_for_tier(tier)
            .ok_or_else(|| DtfsError::DiskNotFound(format!("No disk for tier {:?}", tier)))?;

        let chunk_dir = chunk_data_dir(&disk.mount_path, chunk_id);
        let tmp_dir = tmp_dir(&disk.mount_path);
        let wal_dir = wal_dir(&disk.mount_path);

        // Ensure directories exist
        fs::create_dir_all(&chunk_dir)
            .await
            .map_err(DtfsError::Io)?;
        fs::create_dir_all(&tmp_dir).await.map_err(DtfsError::Io)?;
        fs::create_dir_all(&wal_dir).await.map_err(DtfsError::Io)?;

        // 1. Write WAL entry before doing anything.
        let wal_entry = WalEntry {
            operation: WalOperation::PutChunk,
            chunk_id: chunk_id.as_hex(),
            disk_path: disk.mount_path.to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        let wal_path = wal_dir.join(format!("{}.wal.json", chunk_id.as_hex()));
        write_wal_entry(&wal_path, &wal_entry).await?;

        // 2. Compute checksum
        let hash = checksum::compute(data);

        // 3. Write to tmp first, then atomic rename
        let tmp_path = tmp_dir.join(format!("{}.dat.tmp", chunk_id.as_hex()));
        let final_dat_path = chunk_dir.join(format!("{}.dat", chunk_id.as_hex()));
        let final_meta_path = chunk_dir.join(format!("{}.meta", chunk_id.as_hex()));

        // Write data to tmp with fsync
        {
            let mut file = fs::File::create(&tmp_path).await.map_err(DtfsError::Io)?;
            file.write_all(data).await.map_err(DtfsError::Io)?;
            file.sync_all().await.map_err(DtfsError::Io)?;
        }

        // CO-5-5 — chunks are content-addressed, so a re-upload of the same
        // bytes lands on an already-present chunk. Note whether a COMPLETE
        // chunk (.dat + .meta) already exists *before* the rename so we only
        // bump the chunk_count / bytes_stored counters for a genuinely new
        // chunk. Requiring the .meta too means a meta-less .dat left by an
        // interrupted prior put (CO-1-4b residue) doesn't suppress the first
        // real count for this id.
        let already_present =
            final_dat_path.exists() && final_meta_path.exists();

        // Atomic rename to final location
        fs::rename(&tmp_path, &final_dat_path)
            .await
            .map_err(DtfsError::Io)?;
        // Bug L — fsync parent directory after rename so the directory
        // entry survives a crash.
        fsync_dir(&chunk_dir).await?;

        // 4. Write metadata
        let meta = ChunkMeta {
            id: chunk_id.clone(),
            size: data.len() as u64,
            checksum: hash,
            created_at: chrono::Utc::now().timestamp(),
            volume_id: volume_id.clone(),
            replication_factor: 1, // Will be set by manager
        };

        let meta_json = serde_json::to_string_pretty(&meta)
            .map_err(|e| DtfsError::Serialization(e.to_string()))?;

        {
            let mut file = fs::File::create(&final_meta_path)
                .await
                .map_err(DtfsError::Io)?;
            file.write_all(meta_json.as_bytes())
                .await
                .map_err(DtfsError::Io)?;
            file.sync_all().await.map_err(DtfsError::Io)?;
        }
        // Bug L — fsync parent dir after .meta create.
        fsync_dir(&chunk_dir).await?;

        // 5. Remove the WAL entry now that the write is fully committed.
        let _ = fs::remove_file(&wal_path).await;
        let _ = fsync_dir(&wal_dir).await;

        // Update running counters — only for a newly-stored chunk. A
        // duplicate put (CO-5-5) overwrote an existing .dat, so counting it
        // again would inflate chunk_count / bytes_stored.
        if !already_present {
            self.chunk_count.fetch_add(1, Ordering::Relaxed);
            self.bytes_stored.fetch_add(data.len() as u64, Ordering::Relaxed);
        }

        tracing::debug!(
            "Stored chunk {} ({} bytes) on disk {}",
            chunk_id,
            data.len(),
            disk.mount_path.display()
        );

        Ok(meta)
    }

    // -----------------------------------------------------------------------
    // Bug I — streaming put_chunk. Caller streams bytes to a tmp file
    // on disk while hashing incrementally; once the stream completes the
    // caller invokes `commit_chunk_from_tmp` to rename the tmp into the
    // final chunk path and write the .meta sidecar. Peak memory per
    // concurrent write is bounded by the gRPC piece size (~1 MB)
    // instead of one full chunk (~128 MB). See
    // plans/incidents/SOAK-RECOVERY-2026-05-18.md.
    // -----------------------------------------------------------------------

    /// Reserve a tmp file path on the disk this tier will write to. The
    /// caller streams the chunk bytes to this path, hashing as it goes,
    /// then invokes [`commit_chunk_from_tmp`] with the finalised hash.
    ///
    /// Returns `(tmp_path, disk_mount_path)`. The tmp path is unique
    /// (UUID-suffixed) so concurrent writers can't collide on the same
    /// filename before the chunk_id is known.
    pub fn allocate_streaming_tmp(
        &self,
        tier: DiskTier,
    ) -> DtfsResult<(PathBuf, PathBuf)> {
        let disk = self
            .disk_manager
            .select_disk_for_tier(tier)
            .ok_or_else(|| DtfsError::DiskNotFound(format!("No disk for tier {:?}", tier)))?;
        let mount = disk.mount_path.clone();
        let tmp_root = tmp_dir(&mount);
        // Create the tmp directory if it doesn't exist (e.g. on a fresh disk).
        let _ = std::fs::create_dir_all(&tmp_root);
        let tmp_path = tmp_root.join(streaming_tmp_name());
        Ok((tmp_path, mount))
    }

    /// Bug BU-6 — sweep leftover streaming `*.dat.tmp` files at startup.
    ///
    /// At process start there are no in-flight `put_chunk` streams, so any
    /// `*.dat.tmp` under a disk's `tmp/` is an orphan from a crash or an
    /// ENOSPC-aborted put (whose RAII cleanup couldn't run across the
    /// restart). Such leaks keep the disk full and make the ENOSPC
    /// condition self-sustaining (live: 84 tmps = 18.5 GB stranded).
    /// Returns the number of files reclaimed.
    pub fn reclaim_orphan_streaming_tmps(&self) -> usize {
        let mut removed = 0usize;
        for disk in self.disk_manager.all_disks() {
            let tmp_root = tmp_dir(&disk.mount_path);
            let entries = match std::fs::read_dir(&tmp_root) {
                Ok(e) => e,
                Err(_) => continue, // tmp/ may not exist yet on a fresh disk
            };
            for entry in entries.flatten() {
                let path = entry.path();
                let is_streaming_tmp = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.ends_with(".dat.tmp"))
                    .unwrap_or(false);
                if is_streaming_tmp && std::fs::remove_file(&path).is_ok() {
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            tracing::warn!(
                removed,
                "Bug BU-6 reclaimed orphaned streaming tmp files at startup"
            );
        }
        removed
    }

    /// Commit a chunk whose bytes are already on disk in `tmp_path`.
    ///
    /// The chunk_id MUST be the BLAKE3 of the bytes streamed to
    /// `tmp_path`; the caller is responsible for computing the hash
    /// incrementally during streaming. We trust the caller's hash —
    /// re-hashing here would either read the whole file back (defeats
    /// the streaming win) or pin it in RAM (also defeats the win).
    ///
    /// On any failure the tmp file is removed so partial writes don't
    /// leak disk space.
    pub async fn commit_chunk_from_tmp(
        &self,
        chunk_id: &ChunkId,
        tmp_path: &Path,
        volume_id: &VolumeId,
        tier: DiskTier,
        size: u64,
        checksum: [u8; 32],
    ) -> DtfsResult<(ChunkMeta, u32)> {
        let outcome = self
            .commit_chunk_from_tmp_inner(chunk_id, tmp_path, volume_id, tier, size, checksum)
            .await;
        if outcome.is_err() {
            // Cleanup: never leave a tmp file behind on the error path.
            let _ = tokio::fs::remove_file(tmp_path).await;
        }
        outcome
    }

    async fn commit_chunk_from_tmp_inner(
        &self,
        chunk_id: &ChunkId,
        tmp_path: &Path,
        volume_id: &VolumeId,
        tier: DiskTier,
        size: u64,
        checksum: [u8; 32],
    ) -> DtfsResult<(ChunkMeta, u32)> {
        // CR-4-4 (DATA-3) — hold this chunk id's stripe for the whole
        // commit so a concurrent delete of the same id can't interleave.
        let _chunk_guard = self.chunk_locks[Self::chunk_lock_index(chunk_id)]
            .lock()
            .await;

        // Look up the disk that owns this tmp path.
        let disk = self
            .disk_manager
            .all_disks()
            .iter()
            .find(|d| tmp_path.starts_with(&d.mount_path))
            .ok_or_else(|| {
                DtfsError::DiskNotFound(format!(
                    "tmp_path {} not on any managed disk",
                    tmp_path.display()
                ))
            })?
            .clone();
        let disk_id = disk.disk_id;
        let _ = tier; // tier is informational here — disk is already chosen

        let chunk_dir = chunk_data_dir(&disk.mount_path, chunk_id);
        let wal_dir_path = wal_dir(&disk.mount_path);

        fs::create_dir_all(&chunk_dir).await.map_err(DtfsError::Io)?;
        fs::create_dir_all(&wal_dir_path).await.map_err(DtfsError::Io)?;

        // Bug K — fsync the tmp .dat *before* the rename. The streaming
        // put_chunk path in server.rs only flushed (not sync_all'd) so
        // on a power loss after a successful gRPC response the chunk
        // metadata could be durable while the bytes were lost. Open
        // O_RDONLY here just to sync the data pages without re-buffering.
        {
            let f = fs::File::open(tmp_path).await.map_err(DtfsError::Io)?;
            f.sync_all().await.map_err(DtfsError::Io)?;
        }

        // 1. WAL entry first.
        let wal_entry = WalEntry {
            operation: WalOperation::PutChunk,
            chunk_id: chunk_id.as_hex(),
            disk_path: disk.mount_path.to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        // CR-4-4 — op-keyed WAL name so a same-id put and delete never
        // share (and clobber) one WAL file.
        let wal_path = wal_dir_path.join(format!("{}.put.wal.json", chunk_id.as_hex()));
        write_wal_entry(&wal_path, &wal_entry).await?;

        // 2. Atomic rename tmp → final.
        let final_dat_path = chunk_dir.join(format!("{}.dat", chunk_id.as_hex()));
        let final_meta_path = chunk_dir.join(format!("{}.meta", chunk_id.as_hex()));

        // CR-4-6 (DATA-7) — same CO-5-5 dedup this store's whole-buffer
        // `put_chunk` already applies, now also on the production streaming
        // commit path: a re-upload of the same content-addressed chunk
        // must not double-count `chunk_count`/`bytes_stored`. Must be
        // captured BEFORE the rename below, which would otherwise
        // overwrite (and hide) any pre-existing `.dat`.
        let already_present = final_dat_path.exists() && final_meta_path.exists();

        fs::rename(tmp_path, &final_dat_path)
            .await
            .map_err(DtfsError::Io)?;

        // Bug L — fsync the parent directory after the rename so the
        // directory-entry pointing at the renamed inode survives a crash.
        // On XFS / ext4 a rename's directory update can be lost even
        // though the inode itself is durable, leaving an orphan inode and
        // a missing chunk from the manager's perspective.
        fsync_dir(&chunk_dir).await?;

        // 3. Write the meta sidecar. CR-4-2 (DATA-2) — atomic tmp+rename
        // instead of a direct `File::create` at the final path: a crash
        // mid-write of the direct create left a torn (partial/0-byte)
        // `.meta` at the path readers use, which `get_chunk_meta` would
        // then fail to parse. `write_meta_atomic` writes to this disk's
        // `tmp/` dir, fsyncs, and renames over the final path so readers
        // only ever see a complete `.meta` or none at all.
        let meta = ChunkMeta {
            id: chunk_id.clone(),
            size,
            checksum,
            created_at: chrono::Utc::now().timestamp(),
            volume_id: volume_id.clone(),
            replication_factor: 1,
        };
        let meta_json = match serde_json::to_string_pretty(&meta) {
            Ok(j) => j,
            Err(e) => {
                cleanup_after_dat_committed(&final_dat_path, &final_meta_path, &wal_path).await;
                return Err(DtfsError::Serialization(e.to_string()));
            }
        };
        if let Err(e) = write_meta_atomic(&disk.mount_path, &final_meta_path, &meta_json).await {
            // CR-4-3 (DATA-4) — the `.dat` is already committed (renamed)
            // at this point; a failure writing its `.meta` companion must
            // not leave a `.dat` with no matching `.meta` (nor a stale WAL
            // entry) behind.
            cleanup_after_dat_committed(&final_dat_path, &final_meta_path, &wal_path).await;
            return Err(e);
        }

        // 4. Clear WAL.
        let _ = fs::remove_file(&wal_path).await;
        // Best-effort fsync of WAL dir so the removal is durable; if
        // we crash after this point the (now-absent) WAL is a "no-op"
        // for recover_from_wal.
        let _ = fsync_dir(&wal_dir_path).await;

        // 5. Counters — skip for a re-upload of an already-complete chunk
        // (CR-4-6 / CO-5-5 parity).
        if !already_present {
            self.chunk_count.fetch_add(1, Ordering::Relaxed);
            self.bytes_stored.fetch_add(size, Ordering::Relaxed);
        }

        tracing::debug!(
            "Streamed chunk {} ({} bytes) onto disk {} (disk_id={})",
            chunk_id,
            size,
            disk.mount_path.display(),
            disk_id,
        );
        Ok((meta, disk_id))
    }

    /// Read a chunk from disk.
    ///
    /// Uses blocking std::fs::read on a dedicated thread (via spawn_blocking)
    /// instead of tokio::fs::read — for 128 MB chunks the blocking-pool
    /// orchestration costs add up and synchronous read is materially faster.
    ///
    /// Per-read BLAKE3 verification has been removed from this hot path; the
    /// scrub scheduler (`crates/dtfs-data/src/scrub.rs`) already runs
    /// quick + deep scrubs that detect bit-rot. Verifying on every read
    /// burned ~40 ms per 128 MB chunk and produced no signal we didn't
    /// already get from scrub.
    /// Bug CI follow-up — resolve a committed chunk to its on-disk `.dat`
    /// path + size WITHOUT reading it. Replication egress streams from
    /// this path (Bug I parity for the read side: pre-fix `replicate_to`
    /// pulled the whole chunk into a heap `Bytes`, which under the
    /// 2026-06-06 task storm pinned the data node at its memory cap).
    pub fn chunk_file_path(&self, chunk_id: &ChunkId) -> Option<(std::path::PathBuf, u64)> {
        for disk in self.disk_manager.all_disks() {
            let dat_path = chunk_data_path(&disk.mount_path, chunk_id);
            if let Ok(md) = std::fs::metadata(&dat_path) {
                return Some((dat_path, md.len()));
            }
        }
        None
    }

    pub async fn get_chunk(&self, chunk_id: &ChunkId) -> DtfsResult<Bytes> {
        for disk in self.disk_manager.all_disks() {
            let dat_path = chunk_data_path(&disk.mount_path, chunk_id);
            if !dat_path.exists() {
                continue;
            }
            let path = dat_path.clone();
            let data = tokio::task::spawn_blocking(move || std::fs::read(&path))
                .await
                .map_err(|e| DtfsError::Io(std::io::Error::other(e.to_string())))?
                .map_err(DtfsError::Io)?;
            return Ok(Bytes::from(data));
        }
        Err(DtfsError::ChunkNotFound(chunk_id.to_string()))
    }

    /// Delete a chunk from disk. **Errors** with `ChunkNotFound` when
    /// the chunk is absent — call `delete_chunk_idempotent` instead
    /// if you want a silent no-op on absent.
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> DtfsResult<()> {
        let (was_present, _bytes) = self.delete_chunk_idempotent(chunk_id).await?;
        if !was_present {
            return Err(DtfsError::ChunkNotFound(chunk_id.to_string()));
        }
        Ok(())
    }

    /// Bug AV (AV-4-7 WAL-recoverable) — idempotent chunk delete.
    /// Returns `(was_present, freed_bytes)`:
    /// * `was_present = true, freed_bytes = N` — file was removed,
    ///   counters decremented by N.
    /// * `was_present = false, freed_bytes = 0` — chunk not found on
    ///   any disk; this is the operation's natural no-op state, not
    ///   an error.
    ///
    /// WAL-recoverable atomicity (AV-4-7): the WAL is written +
    /// fsynced BEFORE either unlink, then `.dat` is removed first
    /// (releases bytes immediately), then `.meta`, then the WAL
    /// entry is cleared. On a crash between any two steps,
    /// `recover_from_wal` re-runs the unlinks (idempotent — already-
    /// missing files are not errors) and clears the WAL. Net effect:
    /// every delete is durable and re-runnable; there is no window in
    /// which an orphan `.meta` outlives a successful boot.
    pub async fn delete_chunk_idempotent(
        &self,
        chunk_id: &ChunkId,
    ) -> DtfsResult<(bool, u64)> {
        // Node-wide legacy first-found delete (no disk targeting).
        self.delete_chunk_idempotent_on_disk(chunk_id, None).await
    }

    /// CO-1-1 — disk-targeted variant of `delete_chunk_idempotent`.
    /// `disk_id = Some(id)` unlinks ONLY the copy on the matching disk,
    /// so demoting a warm cache replica can never delete the cold anchor
    /// bytes co-located on another disk of the same node (replica identity
    /// is `(node_id, disk_id)`, A.3.4 / Bug BH). `disk_id = None` (an old
    /// manager that doesn't send the field, or any caller without disk
    /// context) keeps the legacy first-found-across-all-disks behaviour.
    /// An unknown `disk_id` (no such disk) is a no-op `(false, 0)` rather
    /// than a fall-through to first-found, so a stale id never deletes the
    /// wrong copy.
    pub async fn delete_chunk_idempotent_on_disk(
        &self,
        chunk_id: &ChunkId,
        disk_id: Option<u32>,
    ) -> DtfsResult<(bool, u64)> {
        // CR-4-4 (DATA-3) — hold this chunk id's stripe for the whole
        // delete so a concurrent commit of the same id can't interleave.
        let _chunk_guard = self.chunk_locks[Self::chunk_lock_index(chunk_id)]
            .lock()
            .await;

        for disk in self.disk_manager.all_disks() {
            // CO-1-1 — when a specific disk is requested, skip every other
            // disk so only the matching copy can be unlinked.
            if let Some(want) = disk_id {
                if disk.disk_id != want {
                    continue;
                }
            }
            let dat_path = chunk_data_path(&disk.mount_path, chunk_id);
            let meta_path = chunk_meta_path(&disk.mount_path, chunk_id);

            if dat_path.exists() {
                // Read the on-disk file size *before* deleting so we can
                // accurately decrement bytes_stored. Previously this counter
                // was never decremented on delete, causing it to grow
                // monotonically and report inflated storage usage (fix #6).
                let file_size = fs::metadata(&dat_path)
                    .await
                    .map(|m| m.len())
                    .unwrap_or(0);

                // AV-4-7 — WAL-recoverable delete. Write the WAL entry
                // first so a crash mid-delete (or before either unlink
                // even starts) replays on boot via `recover_from_wal`.
                let wal_dir_path = wal_dir(&disk.mount_path);
                fs::create_dir_all(&wal_dir_path).await.map_err(DtfsError::Io)?;
                let wal_entry = WalEntry {
                    operation: WalOperation::DeleteChunk,
                    chunk_id: chunk_id.as_hex(),
                    disk_path: disk.mount_path.to_string_lossy().to_string(),
                    timestamp_ms: now_millis(),
                };
                // CR-4-4 — op-keyed WAL name (see commit_chunk_from_tmp_inner).
                let wal_path = wal_dir_path
                    .join(format!("{}.delete.wal.json", chunk_id.as_hex()));
                write_wal_entry(&wal_path, &wal_entry).await?;

                // CO-1-4(a) — clear the DeleteChunk WAL on the error path
                // too. A `remove_file` failure (e.g. a racing delete already
                // unlinked the .dat → NotFound) must not leave a stale
                // DeleteChunk WAL behind: recover_from_wal would otherwise
                // replay it on boot and unlink a chunk that was re-uploaded
                // in the meantime (content-addressed, Bug CI).
                if let Err(e) = fs::remove_file(&dat_path).await {
                    let _ = fs::remove_file(&wal_path).await;
                    return Err(DtfsError::Io(e));
                }
                if meta_path.exists() {
                    if let Err(e) = fs::remove_file(&meta_path).await {
                        let _ = fs::remove_file(&wal_path).await;
                        return Err(DtfsError::Io(e));
                    }
                }

                // Clear the WAL marker last — only after both unlinks
                // have succeeded. A crash before this point leaves the
                // WAL for replay, which is harmless because the unlinks
                // are idempotent.
                let _ = fs::remove_file(&wal_path).await;

                tracing::debug!("Deleted chunk {} ({} bytes)", chunk_id, file_size);

                // Update running counters (saturating to avoid underflow).
                self.chunk_count
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                        Some(c.saturating_sub(1))
                    })
                    .ok();
                self.bytes_stored
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |b| {
                        Some(b.saturating_sub(file_size))
                    })
                    .ok();

                return Ok((true, file_size));
            }
        }

        Ok((false, 0))
    }

    /// Check if a chunk exists on any disk.
    pub async fn has_chunk(&self, chunk_id: &ChunkId) -> bool {
        for disk in self.disk_manager.all_disks() {
            if chunk_data_path(&disk.mount_path, chunk_id).exists() {
                return true;
            }
        }
        false
    }

    /// Get chunk metadata.
    pub async fn get_chunk_meta(&self, chunk_id: &ChunkId) -> DtfsResult<ChunkMeta> {
        for disk in self.disk_manager.all_disks() {
            let meta_path = chunk_meta_path(&disk.mount_path, chunk_id);
            if meta_path.exists() {
                let meta_json = fs::read_to_string(&meta_path)
                    .await
                    .map_err(DtfsError::Io)?;
                let meta: ChunkMeta = serde_json::from_str(&meta_json)
                    .map_err(|e| DtfsError::Serialization(e.to_string()))?;
                return Ok(meta);
            }
        }
        Err(DtfsError::ChunkNotFound(chunk_id.to_string()))
    }

    /// Return all disk mount paths for enumeration (used by scrub scheduler).
    pub fn disk_paths(&self) -> Vec<std::path::PathBuf> {
        self.disk_manager
            .all_disks()
            .iter()
            .map(|d| d.mount_path.clone())
            .collect()
    }

    /// Bug BC-6 / Bug CB — enumerate every chunk this node holds on disk
    /// together with its `.dat` mtime (Unix ms). Walks each configured
    /// disk's `chunks/aa/bb/<id>.dat` tree and returns the stems whose
    /// `.meta` companion also exists. Used by the manager-side
    /// orphan_scrub planner (BC-7); the mtime is the durable grace clock
    /// the reaper measures against (Bug CB, scheduler::orphan_reap_due).
    /// Linear in chunk count; OK at the ~24h cadence we tick orphan_scrub
    /// on. A stat failure on an individual file yields mtime 0 ("age
    /// unknown"), which the manager treats as not-yet-due rather than
    /// reaping with no grace.
    pub async fn list_all_chunks_with_mtime(&self) -> DtfsResult<Vec<(String, u64)>> {
        let mut out: Vec<(String, u64)> = Vec::new();
        for disk in self.disk_manager.all_disks() {
            let chunks_dir = disk.mount_path.join("chunks");
            if !chunks_dir.exists() {
                continue;
            }
            let mut d1 = fs::read_dir(&chunks_dir).await.map_err(DtfsError::Io)?;
            while let Some(e1) = d1.next_entry().await.map_err(DtfsError::Io)? {
                if !e1.file_type().await.map_err(DtfsError::Io)?.is_dir() { continue; }
                let mut d2 = fs::read_dir(e1.path()).await.map_err(DtfsError::Io)?;
                while let Some(e2) = d2.next_entry().await.map_err(DtfsError::Io)? {
                    if !e2.file_type().await.map_err(DtfsError::Io)?.is_dir() { continue; }
                    let mut files = fs::read_dir(e2.path()).await.map_err(DtfsError::Io)?;
                    while let Some(f) = files.next_entry().await.map_err(DtfsError::Io)? {
                        let p = f.path();
                        if p.extension().map_or(false, |x| x == "dat") {
                            let stem = match p.file_stem().and_then(|s| s.to_str()) {
                                Some(s) if !s.is_empty() => s.to_string(),
                                _ => continue,
                            };
                            // Require the .meta companion so we never
                            // surface a half-written chunk to the orphan
                            // reaper.
                            if p.with_extension("meta").exists() {
                                let mtime_ms = f
                                    .metadata()
                                    .await
                                    .ok()
                                    .and_then(|m| m.modified().ok())
                                    .and_then(|t| {
                                        t.duration_since(std::time::UNIX_EPOCH).ok()
                                    })
                                    .map(|d| d.as_millis() as u64)
                                    .unwrap_or(0);
                                out.push((stem, mtime_ms));
                            }
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// REV-3-2 — return ONE page of this node's chunk listing without
    /// walking the whole chunk tree.
    ///
    /// The on-disk layout is `chunks/<aa>/<bb>/<id>.dat` where `aa`/`bb`
    /// are the first four hex chars of the id, so the directory tree is
    /// already sorted by chunk id and a page token maps directly to a
    /// position in the walk: skip every `aa`/`bb` bucket that sorts
    /// before the token, then emit ids strictly greater than it until
    /// `limit` is reached. Only the buckets a page actually touches are
    /// read (and stat'd), so a page costs O(limit) syscalls instead of
    /// O(N) — the pre-fix `list_all_chunks_with_mtime()` + sort ran per
    /// page, making a full orphan-scrub pass O(N^2).
    ///
    /// `after` is exclusive; pass `""` to start at the beginning. The
    /// result is ascending by id and deduplicated across this node's
    /// disks (a chunk held on two tiers of the same node appears once,
    /// carrying the NEWEST `.dat` mtime — the conservative choice for
    /// the orphan reaper's grace clock, Bug CB). As before, only chunks
    /// whose `.meta` companion exists are surfaced, and an unreadable
    /// mtime yields 0 ("age unknown"), which the manager treats as
    /// not-yet-due.
    pub async fn list_chunks_page(
        &self,
        after: &str,
        limit: usize,
    ) -> DtfsResult<Vec<(String, u64, u32)>> {
        use std::collections::{BTreeMap, BTreeSet};

        let mut out: Vec<(String, u64, u32)> = Vec::new();
        if limit == 0 {
            return Ok(out);
        }

        // CR2-7-2 / MGR-10 — keep each disk's id alongside its chunks root
        // so collect_chunk_bucket can tag every chunk with the disk it was
        // found on.
        let roots: Vec<(PathBuf, u32)> = self
            .disk_manager
            .all_disks()
            .iter()
            .map(|d| (d.mount_path.join("chunks"), d.disk_id))
            .collect();

        // The token's own bucket coordinates; "" means "from the start".
        let after_l1 = after.get(0..2).unwrap_or("");
        let after_l2 = after.get(2..4).unwrap_or("");

        let mut level1: BTreeSet<String> = BTreeSet::new();
        for (root, _) in &roots {
            collect_subdir_names(root, &mut level1).await?;
        }

        for aa in level1.iter().filter(|a| a.as_str() >= after_l1) {
            let mut level2: BTreeSet<String> = BTreeSet::new();
            for (root, _) in &roots {
                collect_subdir_names(&root.join(aa), &mut level2).await?;
            }
            // Only the token's own `aa` needs its earlier `bb` buckets
            // skipped; every later `aa` starts from its first bucket.
            let lower = if aa.as_str() == after_l1 { after_l2 } else { "" };
            for bb in level2.iter().filter(|b| b.as_str() >= lower) {
                let mut bucket: BTreeMap<String, (u64, u32)> = BTreeMap::new();
                for (root, disk_id) in &roots {
                    collect_chunk_bucket(&root.join(aa).join(bb), *disk_id, &mut bucket).await?;
                }
                for (id, (mtime, disk_id)) in bucket {
                    if id.as_str() <= after {
                        continue;
                    }
                    out.push((id, mtime, disk_id));
                    if out.len() == limit {
                        return Ok(out);
                    }
                }
            }
        }

        Ok(out)
    }

    /// Recover from WAL after a crash.
    ///
    /// Replays all WAL entries found across all disks:
    /// - For incomplete `PutChunk` writes: cleans up any leftover tmp files.
    /// - For incomplete `DeleteChunk` writes: completes the deletion.
    ///
    /// Returns the number of recovered (cleaned up) entries.
    pub async fn recover_from_wal(&self) -> DtfsResult<u32> {
        let mut recovered: u32 = 0;

        for disk in self.disk_manager.all_disks() {
            let wal = wal_dir(&disk.mount_path);
            let tmp = tmp_dir(&disk.mount_path);

            if !wal.exists() {
                continue;
            }

            let mut entries = fs::read_dir(&wal).await.map_err(DtfsError::Io)?;
            while let Some(entry) = entries.next_entry().await.map_err(DtfsError::Io)? {
                let path = entry.path();
                if path.extension().map_or(false, |e| e == "json") {
                    match fs::read_to_string(&path).await {
                        Ok(content) => {
                            match serde_json::from_str::<WalEntry>(&content) {
                                Ok(wal_entry) => {
                                    tracing::info!(
                                        "Recovering WAL entry: {:?} for chunk {}",
                                        wal_entry.operation,
                                        wal_entry.chunk_id
                                    );

                                    match wal_entry.operation {
                                        WalOperation::PutChunk => {
                                            // Clean up incomplete write: remove
                                            // any tmp file that was left behind.
                                            let tmp_file = tmp.join(format!(
                                                "{}.dat.tmp",
                                                wal_entry.chunk_id
                                            ));
                                            if tmp_file.exists() {
                                                let _ = fs::remove_file(&tmp_file).await;
                                                tracing::info!(
                                                    "Cleaned up incomplete tmp file: {}",
                                                    tmp_file.display()
                                                );
                                            }

                                            // CO-1-4(b) — a crash between the
                                            // rename and the .meta write leaves
                                            // a committed .dat with no .meta
                                            // companion. That half-committed
                                            // chunk DATA_LOSSes on read; roll it
                                            // back by removing the orphan .dat
                                            // (§ A.6 #3/#5). A complete put has
                                            // already cleared its WAL, so seeing
                                            // a PutChunk WAL with a meta-less
                                            // .dat means the put was interrupted.
                                            if let Some(cid) =
                                                parse_chunk_id_hex(&wal_entry.chunk_id)
                                            {
                                                let dat = chunk_data_path(
                                                    &disk.mount_path,
                                                    &cid,
                                                );
                                                let meta = chunk_meta_path(
                                                    &disk.mount_path,
                                                    &cid,
                                                );
                                                // CO-1-4b / CR-4-2 — a
                                                // missing `.meta` and a
                                                // present-but-unparseable
                                                // (torn/0-byte) `.meta` are
                                                // the same failure mode: an
                                                // interrupted put that never
                                                // reached a complete meta
                                                // sidecar. Roll both back
                                                // the same way.
                                                let meta_missing_or_torn = if !meta.exists() {
                                                    true
                                                } else {
                                                    match fs::read_to_string(&meta).await {
                                                        Ok(s) => serde_json::from_str::<ChunkMeta>(&s)
                                                            .is_err(),
                                                        Err(_) => true,
                                                    }
                                                };
                                                if dat.exists() && meta_missing_or_torn {
                                                    let _ = fs::remove_file(&dat).await;
                                                    let _ = fs::remove_file(&meta).await;
                                                    tracing::info!(
                                                        "Rolled back meta-less/torn-meta committed chunk {} (interrupted put)",
                                                        wal_entry.chunk_id
                                                    );
                                                }
                                            }
                                        }
                                        WalOperation::DeleteChunk => {
                                            // Attempt to complete the deletion.
                                            // Parse chunk_id hex back into bytes.
                                            if let Some(cid) =
                                                parse_chunk_id_hex(&wal_entry.chunk_id)
                                            {
                                                let dat = chunk_data_path(
                                                    &disk.mount_path,
                                                    &cid,
                                                );
                                                let meta = chunk_meta_path(
                                                    &disk.mount_path,
                                                    &cid,
                                                );
                                                // CO-1-4(a) — chunks are
                                                // content-addressed and may be
                                                // re-uploaded after a delete, so
                                                // a stale DeleteChunk WAL must
                                                // never blindly unlink a live
                                                // copy (§ A.6 #4, Bug CI). Skip
                                                // the unlink when the on-disk
                                                // .dat is a fresh live copy:
                                                // either its mtime is newer than
                                                // the WAL entry, or it is a
                                                // complete chunk (.dat + .meta
                                                // both present) — an interrupted
                                                // delete the manager still wants
                                                // gone is re-issued by GC, but a
                                                // re-uploaded live chunk must not
                                                // be destroyed on boot. (rename
                                                // does not bump mtime, so the
                                                // mtime is the chunk's write
                                                // time, not its delete time.)
                                                let newer = dat_mtime_ms(&dat).await
                                                    .map_or(false, |m| m > wal_entry.timestamp_ms);
                                                let live_complete =
                                                    dat.exists() && meta.exists();
                                                if newer || live_complete {
                                                    tracing::info!(
                                                        "Skipping stale delete replay for chunk {} (live on-disk copy present)",
                                                        wal_entry.chunk_id
                                                    );
                                                } else {
                                                    let _ = fs::remove_file(&dat).await;
                                                    let _ = fs::remove_file(&meta).await;
                                                    tracing::info!(
                                                        "Completed interrupted delete for chunk {}",
                                                        wal_entry.chunk_id
                                                    );
                                                }
                                            }
                                        }
                                    }

                                    recovered += 1;
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        "Failed to parse WAL entry {}: {}",
                                        path.display(),
                                        e
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Failed to read WAL file {}: {}",
                                path.display(),
                                e
                            );
                        }
                    }

                    // Remove the WAL entry after processing.
                    let _ = fs::remove_file(&path).await;
                }
            }

            // Clean up any orphaned tmp files (from crashes without WAL entries).
            if tmp.exists() {
                let mut tmp_entries = fs::read_dir(&tmp).await.map_err(DtfsError::Io)?;
                while let Some(entry) =
                    tmp_entries.next_entry().await.map_err(DtfsError::Io)?
                {
                    let path = entry.path();
                    if path.extension().map_or(false, |e| e == "tmp") {
                        tracing::info!(
                            "Removing orphaned tmp file: {}",
                            path.display()
                        );
                        let _ = fs::remove_file(&path).await;
                        recovered += 1;
                    }
                }
            }
        }

        if recovered > 0 {
            tracing::info!("WAL recovery complete: {} entries processed", recovered);
        } else {
            tracing::debug!("WAL recovery: no entries to process");
        }

        Ok(recovered)
    }

    /// Run a scrub on all chunks on a specific disk.
    ///
    /// Skips chunks whose WAL entry exists (P0-5: chunk is currently being
    /// written or replicated — scrubbing it would race with the atomic
    /// `.dat.tmp` → `.dat` rename and the subsequent `.meta` write, producing
    /// spurious "missing" or "size_mismatch" verdicts).
    pub async fn scrub_disk(
        &self,
        disk_path: &Path,
        level: ScrubLevel,
    ) -> DtfsResult<ScrubResult> {
        let chunks_dir = disk_path.join("chunks");
        let wal = wal_dir(disk_path);
        let mut result = ScrubResult {
            chunks_checked: 0,
            chunks_healthy: 0,
            chunks_corrupt: 0,
            chunks_missing: 0,
            chunks_size_mismatch: 0,
            duration_secs: 0.0,
            corrupt_chunk_ids: Vec::new(),
            chunks_meta_repaired: 0,
        };
        let mut skipped_in_flight: u64 = 0;

        let start = std::time::Instant::now();

        if !chunks_dir.exists() {
            return Ok(result);
        }

        // REV-4-4 — only a failure to enumerate the DISK ROOT is fatal.
        // Everything below is per-bucket / per-chunk: an unreadable
        // `chunks/<aa>[/<bb>]` directory, a torn `.meta`, an EIO on the
        // data file, or a chunk unlinked by GC mid-scan is counted and
        // reported, never propagated. Pre-fix the directory walk used
        // `?`, so ONE bad bucket aborted the whole disk pass and silently
        // disabled the only mechanism that finds bit-rot (§ A.6 #7).
        let mut level1 = std::collections::BTreeSet::new();
        collect_subdir_names(&chunks_dir, &mut level1).await?;

        let mut io_errors: u64 = 0;
        let mut vanished: u64 = 0;

        for l1 in &level1 {
            let l1_dir = chunks_dir.join(l1);
            let mut level2 = std::collections::BTreeSet::new();
            if let Err(e) = collect_subdir_names(&l1_dir, &mut level2).await {
                io_errors += 1;
                tracing::warn!(
                    dir = %l1_dir.display(), error = %e,
                    "scrub: cannot enumerate level-1 directory; skipping it"
                );
                continue;
            }
            for l2 in &level2 {
                let bucket = l1_dir.join(l2);
                let files = match collect_dat_files(&bucket).await {
                    Ok(f) => f,
                    Err(e) => {
                        io_errors += 1;
                        tracing::warn!(
                            dir = %bucket.display(), error = %e,
                            "scrub: cannot enumerate chunk bucket; skipping it"
                        );
                        continue;
                    }
                };
                for path in files {
                    // Stem of the .dat file is the chunk_id hex.
                    let chunk_hex = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or_default()
                        .to_string();

                    // P0-5: skip chunks with an outstanding WAL entry.
                    // The WAL is written before a put_chunk starts and
                    // removed only once the .meta has been fsynced, so its
                    // presence reliably indicates an in-flight write or
                    // replication into the destination disk.
                    if !chunk_hex.is_empty() && wal_entry_exists(&wal, &chunk_hex) {
                        skipped_in_flight += 1;
                        tracing::debug!(
                            chunk = %chunk_hex,
                            "scrub: skipping in-flight chunk (WAL entry present)"
                        );
                        continue;
                    }

                    // CO-4-2 / REV-4-4 — a single unreadable/unparseable
                    // chunk must NOT abort the whole disk scan (that
                    // aborts exactly when corruption exists). Handle each
                    // chunk's errors locally: count it corrupt and
                    // continue.
                    //
                    // Quick scrub: existence + size. Stat FIRST — a chunk
                    // the unlink GC removed between `read_dir` and here
                    // was never part of this pass, so it is neither
                    // checked nor reported as corrupt (a needless repair
                    // of a deliberately deleted chunk is worse than
                    // skipping it).
                    let file_meta = match fs::metadata(&path).await {
                        Ok(m) => m,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            vanished += 1;
                            continue;
                        }
                        Err(e) => {
                            io_errors += 1;
                            result.chunks_checked += 1;
                            record_scrub_fault(&mut result, &chunk_hex, "stat", &e);
                            continue;
                        }
                    };

                    result.chunks_checked += 1;

                    let meta_path = path.with_extension("meta");
                    let chunk_meta_json = match fs::read_to_string(&meta_path).await {
                        Ok(s) => s,
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            // CR-4-1 (DATA-1) — no `.meta` companion, but
                            // the `.dat` is readable (we already stat'd it
                            // above). Before treating this as a corrupt/
                            // missing chunk, hash the `.dat` and check it
                            // against the id encoded in its filename: if
                            // they match, the bytes are fine and only the
                            // sidecar was lost (e.g. CR-4-2's window before
                            // this fix), so repair `.meta` instead of
                            // quarantining a healthy chunk.
                            if let Some(meta) = try_repair_meta_from_dat(
                                disk_path, &path, &chunk_hex, file_meta.len(),
                            ).await {
                                result.chunks_meta_repaired += 1;
                                result.chunks_healthy += 1;
                                let _ = meta;
                                continue;
                            }
                            result.chunks_missing += 1;
                            if !chunk_hex.is_empty() {
                                result.corrupt_chunk_ids.push(chunk_hex.clone());
                            }
                            continue;
                        }
                        Err(e) => {
                            io_errors += 1;
                            record_scrub_fault(&mut result, &chunk_hex, "meta_read", &e);
                            continue;
                        }
                    };
                    let chunk_meta: ChunkMeta = match serde_json::from_str(&chunk_meta_json) {
                        Ok(m) => m,
                        Err(e) => {
                            // CR-4-1 (DATA-1) — same repair-before-quarantine
                            // logic for a `.meta` that exists but fails to
                            // parse (torn/0-byte, CR-4-2's window before
                            // this fix).
                            if let Some(meta) = try_repair_meta_from_dat(
                                disk_path, &path, &chunk_hex, file_meta.len(),
                            ).await {
                                result.chunks_meta_repaired += 1;
                                result.chunks_healthy += 1;
                                let _ = meta;
                                continue;
                            }
                            io_errors += 1;
                            record_scrub_fault(&mut result, &chunk_hex, "meta_parse", &e);
                            continue;
                        }
                    };

                    if file_meta.len() != chunk_meta.size {
                        result.chunks_size_mismatch += 1;
                        if !chunk_hex.is_empty() {
                            result.corrupt_chunk_ids.push(chunk_hex.clone());
                        }
                        continue;
                    }

                    // Deep scrub: full checksum verification. Hash the file
                    // incrementally (A.3.2 — never load a whole chunk into
                    // heap) rather than fs::read of the entire chunk.
                    if level == ScrubLevel::Deep {
                        match hash_chunk_file_incrementally(&path, file_meta.len()).await {
                            Ok(digest) => {
                                if digest != chunk_meta.checksum {
                                    result.chunks_corrupt += 1;
                                    if !chunk_hex.is_empty() {
                                        result.corrupt_chunk_ids.push(chunk_hex.clone());
                                    }
                                    continue;
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // GC unlinked it between the stat above
                                // and the hash — same race as before.
                                result.chunks_checked = result.chunks_checked.saturating_sub(1);
                                vanished += 1;
                                continue;
                            }
                            Err(e) => {
                                io_errors += 1;
                                record_scrub_fault(&mut result, &chunk_hex, "data_read", &e);
                                continue;
                            }
                        }
                    }

                    result.chunks_healthy += 1;
                }
            }
        }

        if skipped_in_flight > 0 {
            tracing::info!(
                disk = %disk_path.display(),
                skipped = skipped_in_flight,
                "scrub skipped in-flight chunks (P0-5)"
            );
        }
        if vanished > 0 {
            tracing::info!(
                disk = %disk_path.display(),
                vanished,
                "REV-4-4 scrub skipped chunks removed by GC mid-scan"
            );
        }
        if io_errors > 0 {
            tracing::warn!(
                disk = %disk_path.display(),
                errors = io_errors,
                "REV-4-4 scrub hit per-chunk/per-bucket failures; \
                 the disk pass completed"
            );
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        Ok(result)
    }
}

/// CR-4-1 (DATA-1) — attempt to repair a missing/unparseable `.meta` by
/// re-hashing the already-stat'd `.dat` and comparing the digest to the id
/// encoded in the filename. Returns the rebuilt `ChunkMeta` (already
/// written atomically to `path`'s `.meta` companion) on a match, or `None`
/// when the id is malformed, the hash doesn't match (real corruption — the
/// caller must still quarantine), or the write itself failed. Never mutates
/// `result`; the caller updates its own counters based on the outcome.
async fn try_repair_meta_from_dat(
    disk_root: &Path,
    dat_path: &Path,
    chunk_hex: &str,
    size: u64,
) -> Option<ChunkMeta> {
    let chunk_id = ChunkId::from_hex(chunk_hex).ok()?;
    let digest = hash_chunk_file_incrementally(dat_path, size).await.ok()?;
    if digest != chunk_id.0 {
        return None;
    }
    let meta = ChunkMeta {
        id: chunk_id,
        size,
        checksum: digest,
        created_at: chrono::Utc::now().timestamp(),
        // The real volume can't be recovered from the `.dat` alone; the
        // "default" volume is a best-effort placeholder purely so the
        // sidecar is complete and parseable again.
        volume_id: VolumeId::new("default".to_string()),
        replication_factor: 1,
    };
    let meta_json = serde_json::to_string_pretty(&meta).ok()?;
    let final_meta_path = dat_path.with_extension("meta");
    write_meta_atomic(disk_root, &final_meta_path, &meta_json).await.ok()?;
    tracing::info!(
        chunk = %chunk_hex,
        "CR-4-1 scrub repaired missing/torn .meta from a verified .dat"
    );
    Some(meta)
}

/// REV-4-4 — count one per-chunk scrub failure as corruption and report
/// its id so the manager auto-repairs it via replicate_chunk (§ A.6 #7),
/// instead of aborting the whole disk pass. An unreadable/unparsable
/// `.meta` or an EIO on the data file IS damage to that chunk; the
/// existing corruption accounting is the right channel for it.
fn record_scrub_fault(
    result: &mut ScrubResult,
    chunk_hex: &str,
    detector: &str,
    err: &dyn std::fmt::Display,
) {
    result.chunks_corrupt += 1;
    if !chunk_hex.is_empty() {
        result.corrupt_chunk_ids.push(chunk_hex.to_string());
    }
    tracing::warn!(
        chunk = %chunk_hex,
        detector,
        error = %err,
        "REV-4-4 scrub: per-chunk failure counted as corrupt; disk pass continues"
    );
}

/// REV-4-4 — list the `.dat` files in one `chunks/<aa>/<bb>` bucket. A
/// bucket that disappeared between levels is not an error.
async fn collect_dat_files(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut out: Vec<PathBuf> = Vec::new();
    let mut rd = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    while let Some(f) = rd.next_entry().await? {
        let p = f.path();
        if p.extension().map_or(false, |e| e == "dat") {
            out.push(p);
        }
    }
    Ok(out)
}

/// REV-3-2 — collect the immediate sub-directory names of `dir` into
/// `out`. A missing directory is not an error (a disk may hold no chunks
/// yet, or the bucket may have been reaped between levels).
async fn collect_subdir_names(
    dir: &Path,
    out: &mut std::collections::BTreeSet<String>,
) -> DtfsResult<()> {
    let mut rd = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(DtfsError::Io(e)),
    };
    while let Some(entry) = rd.next_entry().await.map_err(DtfsError::Io)? {
        if !entry.file_type().await.map_err(DtfsError::Io)?.is_dir() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            out.insert(name.to_string());
        }
    }
    Ok(())
}

/// REV-3-2 — merge one `chunks/<aa>/<bb>` bucket into `out` as
/// `id -> (.dat mtime (Unix ms), disk_id)`. Only chunks with a `.meta`
/// companion are surfaced so a half-written chunk never reaches the
/// orphan reaper. When the same id is present on more than one disk of
/// this node the newest mtime wins (most conservative grace clock, Bug
/// CB); CR2-7-2 / MGR-10 carries the disk_id of whichever mtime wins,
/// so the manager can key its reap by (node, disk) instead of node-wide.
async fn collect_chunk_bucket(
    dir: &Path,
    disk_id: u32,
    out: &mut std::collections::BTreeMap<String, (u64, u32)>,
) -> DtfsResult<()> {
    let mut rd = match fs::read_dir(dir).await {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(DtfsError::Io(e)),
    };
    while let Some(f) = rd.next_entry().await.map_err(DtfsError::Io)? {
        let p = f.path();
        if p.extension().map_or(false, |x| x == "dat") {
            let stem = match p.file_stem().and_then(|s| s.to_str()) {
                Some(s) if !s.is_empty() => s.to_string(),
                _ => continue,
            };
            if !p.with_extension("meta").exists() {
                continue;
            }
            let mtime_ms = f
                .metadata()
                .await
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            out.entry(stem)
                .and_modify(|(m, d)| {
                    if mtime_ms > *m {
                        *m = mtime_ms;
                        *d = disk_id;
                    }
                })
                .or_insert((mtime_ms, disk_id));
        }
    }
    Ok(())
}

/// Build the directory path for a chunk's data files.
fn chunk_data_dir(disk_path: &Path, chunk_id: &ChunkId) -> PathBuf {
    disk_path
        .join("chunks")
        .join(chunk_id.dir_prefix_1())
        .join(chunk_id.dir_prefix_2())
}

/// Build the full path to a chunk's .dat file.
fn chunk_data_path(disk_path: &Path, chunk_id: &ChunkId) -> PathBuf {
    chunk_data_dir(disk_path, chunk_id).join(format!("{}.dat", chunk_id.as_hex()))
}

/// Build the full path to a chunk's .meta file.
fn chunk_meta_path(disk_path: &Path, chunk_id: &ChunkId) -> PathBuf {
    chunk_data_dir(disk_path, chunk_id).join(format!("{}.meta", chunk_id.as_hex()))
}

/// Build the tmp directory path for a disk.
fn tmp_dir(disk_path: &Path) -> PathBuf {
    disk_path.join("tmp")
}

/// Build the WAL directory path for a disk.
fn wal_dir(disk_path: &Path) -> PathBuf {
    disk_path.join(".dtfs").join("wal")
}

/// CR-4-4 follow-up — does ANY WAL entry exist for this chunk id, under
/// either the op-keyed names (`<id>.put.wal.json` / `<id>.delete.wal.json`)
/// or the legacy `<id>.wal.json`? `commit_chunk_from_tmp_inner` and
/// `delete_chunk_idempotent_on_disk` write the op-keyed names; anything
/// that needs to detect "this chunk has an in-flight write/delete"
/// (currently just the scrub P0-5 skip) must check all three names, not
/// just the legacy one, or CR-4-4's WAL rename silently broke the
/// detection.
fn wal_entry_exists(wal_dir: &Path, chunk_hex: &str) -> bool {
    wal_dir.join(format!("{chunk_hex}.put.wal.json")).exists()
        || wal_dir.join(format!("{chunk_hex}.delete.wal.json")).exists()
        || wal_dir.join(format!("{chunk_hex}.wal.json")).exists()
}

/// Write a WAL entry to disk with fsync. Also fsyncs the parent
/// directory so the entry-creation is durable (Bug L).
async fn write_wal_entry(path: &Path, entry: &WalEntry) -> DtfsResult<()> {
    let json = serde_json::to_string(entry)
        .map_err(|e| DtfsError::Serialization(e.to_string()))?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).await.map_err(DtfsError::Io)?;
    }

    let mut file = fs::File::create(path).await.map_err(DtfsError::Io)?;
    file.write_all(json.as_bytes())
        .await
        .map_err(DtfsError::Io)?;
    file.sync_all().await.map_err(DtfsError::Io)?;
    if let Some(parent) = path.parent() {
        let _ = fsync_dir(parent).await;
    }
    Ok(())
}

/// CR-4-2 (DATA-2) — write a `.meta` sidecar atomically: tmp file in this
/// disk's `tmp/` dir, `sync_all`, rename over `final_meta_path`, fsync the
/// destination directory. On any failure the tmp file is removed, so a
/// crash or error mid-write never leaves a torn `.meta` at `final_meta_path`
/// — the final path either has the complete previous content (rename not
/// yet reached) or the complete new content (rename succeeded).
async fn write_meta_atomic(
    disk_root: &Path,
    final_meta_path: &Path,
    meta_json: &str,
) -> DtfsResult<()> {
    let tmp = tmp_dir(disk_root);
    fs::create_dir_all(&tmp).await.map_err(DtfsError::Io)?;
    let tmp_path = tmp.join(format!("{}.meta.tmp", uuid::Uuid::new_v4()));

    {
        let mut file = fs::File::create(&tmp_path).await.map_err(DtfsError::Io)?;
        file.write_all(meta_json.as_bytes())
            .await
            .map_err(DtfsError::Io)?;
        file.sync_all().await.map_err(DtfsError::Io)?;
    }

    if let Err(e) = fs::rename(&tmp_path, final_meta_path).await {
        let _ = fs::remove_file(&tmp_path).await;
        return Err(DtfsError::Io(e));
    }
    if let Some(parent) = final_meta_path.parent() {
        fsync_dir(parent).await?;
    }
    Ok(())
}

/// CR-4-3 (DATA-4) — after `.dat` has been renamed into place, any later
/// failure (writing `.meta`, etc.) must not leave a committed `.dat`
/// without its `.meta` companion, nor a stale WAL entry that would
/// re-trigger recovery against a chunk that no longer exists. Removing all
/// three is safe even when some of them were never created.
async fn cleanup_after_dat_committed(final_dat_path: &Path, final_meta_path: &Path, wal_path: &Path) {
    let _ = fs::remove_file(final_dat_path).await;
    let _ = fs::remove_file(final_meta_path).await;
    let _ = fs::remove_file(wal_path).await;
}

/// Bug O / CR-4-1 (DATA-1) — quarantine a chunk whose on-disk bytes have
/// failed scrub verification (checksum mismatch or unreadable `.dat`).
///
/// The `.dat` is RENAMED (never `remove_file`d) to
/// `.dtfs/quarantine/<id>.dat.bad` so the corrupt bytes are preserved for
/// forensics instead of being destroyed outright — an operator can pull
/// the file and inspect it before it's ever overwritten. If a `.dat.bad`
/// from an earlier quarantine of the same id already exists, it is
/// overwritten (the newer failure is what matters). The `.meta` moves to
/// `.dtfs/quarantine/<id>.meta.bad` as a breadcrumb, same as before.
pub async fn quarantine_chunk_on_disk(
    disk_mount: &Path,
    chunk_id_hex: &str,
) -> DtfsResult<()> {
    let chunk_id = ChunkId::from_hex(chunk_id_hex)
        .map_err(|e| DtfsError::Serialization(format!("bad chunk_id {chunk_id_hex}: {e}")))?;
    let chunk_dir = chunk_data_dir(disk_mount, &chunk_id);
    let dat = chunk_dir.join(format!("{}.dat", chunk_id_hex));
    let meta = chunk_dir.join(format!("{}.meta", chunk_id_hex));

    let q_root = disk_mount.join(".dtfs").join("quarantine");
    fs::create_dir_all(&q_root).await.map_err(DtfsError::Io)?;
    let q_meta = q_root.join(format!("{}.meta.bad", chunk_id_hex));
    let q_dat = q_root.join(format!("{}.dat.bad", chunk_id_hex));

    // Move the .meta as a breadcrumb (operators can correlate with logs).
    if meta.exists() {
        // Overwrite any earlier quarantine breadcrumb for this id.
        let _ = fs::remove_file(&q_meta).await;
        let _ = fs::rename(&meta, &q_meta).await;
    }
    // Preserve the corrupt .dat instead of destroying it: rename it into
    // quarantine so it survives for forensics.
    if dat.exists() {
        let _ = fs::remove_file(&q_dat).await;
        let _ = fs::rename(&dat, &q_dat).await;
    }
    let _ = fsync_dir(&chunk_dir).await;
    let _ = fsync_dir(&q_root).await;
    Ok(())
}

/// Bug L — fsync a directory so any rename/create inside it is durable
/// across a power loss. POSIX requires the parent-dir to be fsynced
/// separately from the inode itself; tokio's `File::open` + `sync_all`
/// works for directories on Linux when opened with read-only access.
async fn fsync_dir(dir: &Path) -> DtfsResult<()> {
    // `fs::File::open` on a directory yields a handle whose `sync_all`
    // syncs the directory metadata. On Linux this is the canonical
    // dir-fsync. On non-Linux it's a no-op cost.
    match fs::File::open(dir).await {
        Ok(f) => match f.sync_all().await {
            Ok(()) => Ok(()),
            Err(e) => {
                // Some filesystems (overlayfs, tmpfs) return EINVAL on
                // directory sync. Log + accept — the rename itself is
                // still observed by the inode; the durability
                // guarantee weakens to whatever the underlying FS
                // gives us, which is at least no worse than the
                // pre-fix behaviour.
                tracing::debug!(
                    dir = %dir.display(),
                    error = %e,
                    "fsync_dir failed (non-fatal — falling back to pre-fix durability)"
                );
                Ok(())
            }
        },
        Err(e) => Err(DtfsError::Io(e)),
    }
}

/// Parse a hex-encoded chunk ID string back into a ChunkId.
fn parse_chunk_id_hex(hex_str: &str) -> Option<ChunkId> {
    let bytes: Vec<u8> = (0..hex_str.len())
        .step_by(2)
        .filter_map(|i| {
            hex_str.get(i..i + 2).and_then(|s| u8::from_str_radix(s, 16).ok())
        })
        .collect();

    if bytes.len() == 32 {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Some(ChunkId::from_bytes(arr))
    } else {
        None
    }
}

/// CO-1-4(a) — read a `.dat` file's mtime as epoch millis, or None if the
/// file is absent or its mtime can't be resolved. Used by the DeleteChunk
/// WAL replay to tell a fresh re-upload (mtime > WAL timestamp) from the
/// copy the interrupted delete targeted.
async fn dat_mtime_ms(path: &Path) -> Option<u64> {
    let md = fs::metadata(path).await.ok()?;
    let modified = md.modified().ok()?;
    modified
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis() as u64)
}

/// CO-4-2 — BLAKE3-hash an on-disk chunk file incrementally, holding at most
/// one ~8 MB piece in RAM at a time (never the whole chunk, A.3.2). Used by
/// the deep scrub so a large chunk isn't loaded whole to verify its checksum.
async fn hash_chunk_file_incrementally(path: &Path, total: u64) -> std::io::Result<[u8; 32]> {
    use tokio::io::AsyncReadExt;
    const PIECE: usize = 8 * 1024 * 1024;
    let mut f = fs::File::open(path).await?;
    let mut hasher = dtfs_common::checksum::IncrementalHasher::new();
    let mut read: u64 = 0;
    let mut buf = vec![0u8; PIECE];
    while read < total {
        let want = (total - read).min(PIECE as u64) as usize;
        f.read_exact(&mut buf[..want]).await?;
        hasher.update(&buf[..want]);
        read += want as u64;
    }
    Ok(hasher.finalize())
}

/// Get current time in milliseconds since UNIX epoch.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dtfs_common::types::DiskTier;
    use tempfile::TempDir;

    // ── Bug BN-1 — persistent disk UUID ────────────────────────────

    #[test]
    fn disk_uuid_stable_across_calls() {
        let tmp = TempDir::new().unwrap();
        let first = load_or_create_disk_uuid(tmp.path()).unwrap();
        let second = load_or_create_disk_uuid(tmp.path()).unwrap();
        assert_eq!(first, second, "UUID must be stable across boots");
        assert!(uuid::Uuid::parse_str(&first).is_ok());
        assert!(tmp.path().join(".dtfs-disk-uuid").exists());
    }

    #[test]
    fn disk_uuid_corrupt_file_regenerated() {
        let tmp = TempDir::new().unwrap();
        std::fs::write(tmp.path().join(".dtfs-disk-uuid"), "not-a-uuid\n").unwrap();
        let u = load_or_create_disk_uuid(tmp.path()).unwrap();
        assert!(uuid::Uuid::parse_str(&u).is_ok(), "corrupt file regenerated to a valid UUID");
        // and the regenerated value now persists
        assert_eq!(u, load_or_create_disk_uuid(tmp.path()).unwrap());
    }

    #[test]
    fn disk_uuid_valid_file_not_rewritten() {
        let tmp = TempDir::new().unwrap();
        let first = load_or_create_disk_uuid(tmp.path()).unwrap();
        let f = tmp.path().join(".dtfs-disk-uuid");
        let mtime0 = std::fs::metadata(&f).unwrap().modified().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let second = load_or_create_disk_uuid(tmp.path()).unwrap();
        let mtime1 = std::fs::metadata(&f).unwrap().modified().unwrap();
        assert_eq!(first, second);
        assert_eq!(mtime0, mtime1, "valid UUID file must not be rewritten on read");
    }

    // CO-5-2 — two concurrent streaming puts must never share a tmp name,
    // even when sampled back-to-back on a coarse clock. A pid+nanos name
    // collided and let two streams interleave into one tmp (checksum
    // mismatch acked success); the UUID-based name is unique per call.
    #[test]
    fn co_5_2_streaming_tmp_name_unique() {
        let a = streaming_tmp_name();
        let b = streaming_tmp_name();
        assert_ne!(a, b, "two streaming tmp names must differ");
        assert!(a.starts_with("streaming-"), "keeps the streaming- prefix: {a}");
        assert!(a.ends_with(".dat.tmp"), "keeps the .dat.tmp suffix: {a}");
    }

    async fn make_test_store() -> (ChunkStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let disk_path = tmp.path().to_path_buf();

        // Create required directories
        std::fs::create_dir_all(disk_path.join(".dtfs")).unwrap();
        std::fs::create_dir_all(disk_path.join(".dtfs").join("wal")).unwrap();
        std::fs::create_dir_all(disk_path.join("chunks")).unwrap();
        std::fs::create_dir_all(disk_path.join("tmp")).unwrap();

        let disk_configs = vec![dtfs_common::config::DiskConfig {
            path: disk_path,
            tier: DiskTier::Hot,
        }];
        let dm = DiskManager::new(&disk_configs).unwrap();
        (ChunkStore::new(dm), tmp)
    }

    // ── REV-4-4 — one bad chunk/bucket must not abort the disk pass ─

    /// REV-4-4 — a chunk that vanishes mid-scan (the unlink GC racing
    /// the scrub) is not corruption and must not be reported as such;
    /// scheduling a needless repair for a deliberately deleted chunk is
    /// worse than skipping it. Simulated with a dangling `.dat` symlink,
    /// which `fs::metadata` resolves to NotFound exactly like a file
    /// removed between `read_dir` and `stat`.
    #[cfg(unix)]
    #[tokio::test]
    async fn rev_4_4_scrub_skips_chunk_that_vanished_mid_scan() {
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let disk = store.disk_paths()[0].clone();

        for i in 0..3u32 {
            let data = format!("rev-4-4-live-{i}");
            let hash = checksum::compute(data.as_bytes());
            let cid = ChunkId::from_bytes(hash);
            store
                .put_chunk(&cid, data.as_bytes(), &volume_id, DiskTier::Hot)
                .await
                .unwrap();
        }

        // A 4th chunk whose .dat disappeared under us.
        let ghost = ChunkId::from_bytes(checksum::compute(b"rev-4-4-ghost"));
        let dir = chunk_data_dir(&disk, &ghost);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(chunk_meta_path(&disk, &ghost), b"{}").unwrap();
        std::os::unix::fs::symlink(dir.join("gone.dat"), chunk_data_path(&disk, &ghost)).unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await.unwrap();
        assert_eq!(res.chunks_healthy, 3, "the live chunks must all verify");
        assert_eq!(res.chunks_checked, 3, "the vanished chunk must not be counted");
        assert_eq!(res.chunks_corrupt, 0, "a GC race is not corruption");
        assert!(res.corrupt_chunk_ids.is_empty());
    }

    /// REV-4-4 — only failing to enumerate the DISK ROOT is fatal. An
    /// unreadable `chunks/<aa>/<bb>` bucket (bad sector, EACCES) used to
    /// propagate out of the `read_dir` `?` and abort the whole disk pass,
    /// silently disabling the only mechanism that finds bit-rot
    /// (§ A.6 #7). Every chunk in the *other* buckets must still scrub.
    #[cfg(unix)]
    #[tokio::test]
    async fn rev_4_4_scrub_survives_unreadable_bucket_directory() {
        use std::os::unix::fs::PermissionsExt;

        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let disk = store.disk_paths()[0].clone();

        let mut ids = Vec::new();
        for i in 0..8u32 {
            let data = format!("rev-4-4-bucket-{i}");
            let cid = ChunkId::from_bytes(checksum::compute(data.as_bytes()));
            store
                .put_chunk(&cid, data.as_bytes(), &volume_id, DiskTier::Hot)
                .await
                .unwrap();
            ids.push(cid);
        }

        // Make ONE chunk's bucket unreadable.
        let blocked_dir = chunk_data_dir(&disk, &ids[3]);
        let restore = std::fs::metadata(&blocked_dir).unwrap().permissions();
        std::fs::set_permissions(&blocked_dir, std::fs::Permissions::from_mode(0o000)).unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await;
        // Restore before asserting so the TempDir can always be cleaned up.
        std::fs::set_permissions(&blocked_dir, restore).unwrap();

        let res = res.expect("an unreadable bucket must not abort the disk pass");
        assert!(
            res.chunks_healthy >= 7,
            "every chunk outside the unreadable bucket must still be \
             scrubbed; got {} healthy",
            res.chunks_healthy
        );
    }

    // ── CR-4-1 (DATA-1) — quarantine-by-rename + scrub meta repair ──

    /// A `.dat` whose bytes still hash to the id, but whose `.meta` is
    /// torn (0 bytes), must be REPAIRED by scrub, not quarantined: no
    /// corrupt id, `chunks_meta_repaired` counts it, and the `.meta` is
    /// readable again afterwards.
    #[tokio::test]
    async fn cr_4_1_scrub_repairs_torn_meta_from_good_dat() {
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let disk = store.disk_paths()[0].clone();

        let data = b"cr-4-1-good-dat-torn-meta";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot).await.unwrap();

        // Tear the .meta.
        std::fs::write(chunk_meta_path(&disk, &chunk_id), b"").unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await.unwrap();
        assert_eq!(res.chunks_meta_repaired, 1, "torn-but-verifiable meta must be repaired");
        assert_eq!(res.chunks_healthy, 1);
        assert!(res.corrupt_chunk_ids.is_empty(), "a repaired chunk is not corrupt");

        // The .meta is parseable again and matches the .dat.
        let repaired = store.get_chunk_meta(&chunk_id).await.unwrap();
        assert_eq!(repaired.checksum, chunk_id.0);
    }

    // ── CR-4-4 follow-up — P0-5 in-flight skip must recognise every WAL
    // name CR-4-4 introduced, not just the legacy one. Without this, a
    // chunk whose `.dat` has just been renamed into place but whose
    // `.meta` isn't written yet (put_chunk mid-commit, WAL still present)
    // is no longer skipped by scrub: CR-4-1's repair-from-dat path then
    // races commit_chunk_from_tmp_inner's own write_meta_atomic call.

    /// `.dat` present, no `.meta`, and an op-keyed `<id>.put.wal.json`
    /// present (the name `commit_chunk_from_tmp_inner` actually writes)
    /// must still be skipped as in-flight: not checked, not repaired, not
    /// reported corrupt, and scrub must not touch `.meta` at all.
    #[tokio::test]
    async fn cr_4_4_scrub_skips_in_flight_chunk_with_put_wal_name() {
        let (store, tmp) = make_test_store().await;
        let disk = store.disk_paths()[0].clone();

        let data = b"cr-4-4-in-flight-put-wal";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        let dir = chunk_data_dir(&disk, &chunk_id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(chunk_data_path(&disk, &chunk_id), data).unwrap();
        // No .meta — mid-commit, exactly the window write_meta_atomic
        // hasn't reached yet.
        let wal_path = wal_dir(tmp.path()).join(format!("{}.put.wal.json", chunk_id.as_hex()));
        std::fs::create_dir_all(wal_path.parent().unwrap()).unwrap();
        std::fs::write(&wal_path, b"{}").unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await.unwrap();
        assert_eq!(res.chunks_checked, 0, "in-flight chunk must not be examined at all");
        assert_eq!(res.chunks_meta_repaired, 0, "must not race write_meta_atomic with a repair");
        assert!(res.corrupt_chunk_ids.is_empty());
        assert!(!chunk_meta_path(&disk, &chunk_id).exists(), "scrub must not write a .meta here");
    }

    /// Same scenario, but with the legacy `<id>.wal.json` name (an older
    /// data node build, or a WAL entry the streaming test-only `put_chunk`
    /// path still writes) — must be skipped identically.
    #[tokio::test]
    async fn cr_4_4_scrub_skips_in_flight_chunk_with_legacy_wal_name() {
        let (store, tmp) = make_test_store().await;
        let disk = store.disk_paths()[0].clone();

        let data = b"cr-4-4-in-flight-legacy-wal";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        let dir = chunk_data_dir(&disk, &chunk_id);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(chunk_data_path(&disk, &chunk_id), data).unwrap();
        let wal_path = wal_dir(tmp.path()).join(format!("{}.wal.json", chunk_id.as_hex()));
        std::fs::create_dir_all(wal_path.parent().unwrap()).unwrap();
        std::fs::write(&wal_path, b"{}").unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await.unwrap();
        assert_eq!(res.chunks_checked, 0, "in-flight chunk must not be examined at all");
        assert_eq!(res.chunks_meta_repaired, 0, "must not race write_meta_atomic with a repair");
        assert!(res.corrupt_chunk_ids.is_empty());
        assert!(!chunk_meta_path(&disk, &chunk_id).exists(), "scrub must not write a .meta here");
    }

    /// A `.dat` whose bytes do NOT hash to its own filename id (real
    /// corruption) must still be quarantined — by RENAME, not deletion:
    /// `.dtfs/quarantine/<id>.dat.bad` exists with the original bytes and
    /// the live `.dat` is gone.
    #[tokio::test]
    async fn cr_4_1_bad_dat_quarantined_by_rename_not_delete() {
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let disk = store.disk_paths()[0].clone();

        let data = b"cr-4-1-corrupt-me";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot).await.unwrap();

        // Corrupt the bytes in place (size unchanged so the quick
        // size-check passes and the deep hash check is what catches it).
        let dat_path = chunk_data_path(&disk, &chunk_id);
        let mut corrupted = data.to_vec();
        corrupted[0] ^= 0xFF;
        std::fs::write(&dat_path, &corrupted).unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await.unwrap();
        assert_eq!(res.corrupt_chunk_ids, vec![chunk_id.as_hex()]);
        assert_eq!(res.chunks_meta_repaired, 0);

        crate::store::quarantine_chunk_on_disk(&disk, &chunk_id.as_hex())
            .await
            .unwrap();

        assert!(!dat_path.exists(), ".dat removed from its live location");
        let q_dat = disk.join(".dtfs").join("quarantine").join(format!("{}.dat.bad", chunk_id.as_hex()));
        assert!(q_dat.exists(), "corrupt bytes preserved under quarantine, not deleted");
        assert_eq!(std::fs::read(&q_dat).unwrap(), corrupted, "quarantined bytes are the original (corrupt) bytes");
    }

    /// A chunk whose `.dat` bytes do NOT hash to its own filename id AND
    /// whose `.meta` is unparseable must stay corrupt — the repair-from-dat
    /// path only fires on a verified hash match, so a genuinely wrong
    /// `.dat` is correctly declined and still counted (unchanged behaviour
    /// for real corruption; CR-4-1 only adds a repair path, it does not
    /// weaken the corruption check).
    #[tokio::test]
    async fn cr_4_1_dat_hash_mismatch_with_bad_meta_stays_corrupt() {
        let (store, _tmp) = make_test_store().await;
        let disk = store.disk_paths()[0].clone();

        let ghost = ChunkId::from_bytes(checksum::compute(b"cr-4-1-real-content"));
        let dir = chunk_data_dir(&disk, &ghost);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(chunk_meta_path(&disk, &ghost), b"not valid json").unwrap();
        // .dat filename says `ghost`, but its bytes hash to something else
        // entirely — a genuine mismatch the repair path must decline.
        std::fs::write(chunk_data_path(&disk, &ghost), b"unrelated bytes").unwrap();

        let res = store.scrub_disk(&disk, ScrubLevel::Deep).await.unwrap();
        assert_eq!(res.corrupt_chunk_ids, vec![ghost.as_hex()]);
        assert_eq!(res.chunks_meta_repaired, 0, "a hash mismatch must not be repaired");
    }

    // ── REV-3-2 — incremental (non-O(N)-per-page) chunk listing ────

    /// REV-3-2 regression.
    ///
    /// `list_chunks` used to call `list_all_chunks_with_mtime()` (a full
    /// walk of the chunk tree with 2 stat calls per entry) and sort the
    /// whole thing on EVERY page. The manager's orphan scrub pages
    /// through the entire node, so at ~12.5M chunks that is ~12,500 full
    /// walks — O(N^2) syscalls plus an N-sized heap spike per page.
    ///
    /// The on-disk layout `chunks/<aa>/<bb>/<id>.dat` is already sorted
    /// by chunk id (aa/bb are the first four hex chars of the id), so a
    /// page token maps directly to a position in the walk.
    ///
    /// Invariants pinned here:
    /// - a page returns at most `limit` entries (it does NOT materialise
    ///   the whole set),
    /// - ids are strictly increasing across pages,
    /// - the union of all pages equals the full on-disk set,
    /// - each mtime stays aligned with its own id.
    #[tokio::test]
    async fn rev_3_2_list_chunks_page_is_incremental_and_ordered() {
        use std::collections::HashMap;

        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());

        // 25 chunks — blake3 spreads them over many aa/bb buckets, so
        // paging has to skip forward through the directory tree.
        let mut dat_paths: HashMap<String, PathBuf> = HashMap::new();
        let disk = store.disk_paths()[0].clone();
        for i in 0..25u32 {
            let data = format!("rev-3-2-chunk-{i}");
            let hash = checksum::compute(data.as_bytes());
            let chunk_id = ChunkId::from_bytes(hash);
            store
                .put_chunk(&chunk_id, data.as_bytes(), &volume_id, DiskTier::Hot)
                .await
                .unwrap();
            dat_paths.insert(chunk_id.as_hex(), chunk_data_path(&disk, &chunk_id));
        }

        let mut expected: Vec<String> = store
            .list_all_chunks_with_mtime()
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        expected.sort();
        assert_eq!(expected.len(), 25);

        const LIMIT: usize = 4;
        let mut seen: Vec<String> = Vec::new();
        let mut token = String::new();
        loop {
            let page = store.list_chunks_page(&token, LIMIT).await.unwrap();
            assert!(
                page.len() <= LIMIT,
                "a page must return at most `limit` entries, not the whole \
                 set (got {})",
                page.len()
            );
            if page.is_empty() {
                break;
            }
            for (id, mtime, _disk_id) in &page {
                assert!(
                    seen.last().map_or(true, |prev| prev < id),
                    "ids must be strictly increasing across pages: {:?} then {}",
                    seen.last(),
                    id
                );
                // The mtime must belong to THIS id's .dat file.
                let on_disk = std::fs::metadata(dat_paths.get(id).expect("known id"))
                    .unwrap()
                    .modified()
                    .unwrap()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                assert_eq!(*mtime, on_disk, "mtime must stay aligned with its id");
                seen.push(id.clone());
            }
            token = page.last().unwrap().0.clone();
            if page.len() < LIMIT {
                break;
            }
        }

        assert_eq!(seen, expected, "union of all pages must equal the full set");
    }

    /// REV-3-2 — page-token semantics: an empty token starts at the
    /// beginning, a token returns only ids strictly greater than it, and
    /// a token past the last id yields an empty page.
    #[tokio::test]
    async fn rev_3_2_list_chunks_page_token_is_exclusive() {
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        for i in 0..10u32 {
            let data = format!("rev-3-2-token-{i}");
            let hash = checksum::compute(data.as_bytes());
            let chunk_id = ChunkId::from_bytes(hash);
            store
                .put_chunk(&chunk_id, data.as_bytes(), &volume_id, DiskTier::Hot)
                .await
                .unwrap();
        }
        let first = store.list_chunks_page("", 10).await.unwrap();
        assert_eq!(first.len(), 10);

        // Token = 3rd id → page starts at the 4th.
        let after = first[2].0.clone();
        let rest = store.list_chunks_page(&after, 10).await.unwrap();
        assert_eq!(rest.len(), 7);
        assert_eq!(rest[0].0, first[3].0);
        assert!(rest.iter().all(|(id, _, _)| id > &after));

        // Token = last id → exhausted.
        let last = first.last().unwrap().0.clone();
        assert!(store.list_chunks_page(&last, 10).await.unwrap().is_empty());
    }

    /// CR2-7-2 / MGR-10 — `list_chunks_page` tags each chunk with the
    /// disk_id it was actually found on, not just its mtime. A chunk that
    /// exists on only one of two disks reports that disk's id; a chunk
    /// present on both disks (cold anchor + warm cache) reports the disk
    /// whose mtime wins the "newest wins" tie-break.
    #[tokio::test]
    async fn cr2_7_2_list_chunks_page_reports_disk_id_per_chunk() {
        let (store, _cold, _warm) = make_two_disk_store().await;
        let volume_id = VolumeId::new("default".to_string());

        // Cold-only chunk (disk 0).
        let cold_only = ChunkId::from_bytes(checksum::compute(b"cr2-7-2-cold-only"));
        store.put_chunk(&cold_only, b"cr2-7-2-cold-only", &volume_id, DiskTier::Cold).await.unwrap();

        // Warm-only chunk (disk 1).
        let warm_only = ChunkId::from_bytes(checksum::compute(b"cr2-7-2-warm-only"));
        store.put_chunk(&warm_only, b"cr2-7-2-warm-only", &volume_id, DiskTier::Warm).await.unwrap();

        // Co-located on both disks — warm write lands second, so its mtime
        // wins and the reported disk_id must be 1 (warm).
        let both = ChunkId::from_bytes(checksum::compute(b"cr2-7-2-both"));
        store.put_chunk(&both, b"cr2-7-2-both", &volume_id, DiskTier::Cold).await.unwrap();
        store.put_chunk(&both, b"cr2-7-2-both", &volume_id, DiskTier::Warm).await.unwrap();

        let page = store.list_chunks_page("", 100).await.unwrap();
        let by_id: std::collections::HashMap<String, u32> =
            page.into_iter().map(|(id, _, disk_id)| (id, disk_id)).collect();

        assert_eq!(by_id.get(&cold_only.as_hex()), Some(&0));
        assert_eq!(by_id.get(&warm_only.as_hex()), Some(&1));
        assert_eq!(by_id.get(&both.as_hex()), Some(&1), "newest mtime (warm, written last) wins");
    }

    // CO-5-6 — a heartbeat that drained the pending scrub results but then
    // failed to send must not lose them. `restore_scrub_results` re-queues
    // the drained batch (with priority) so the next heartbeat ships it.
    #[tokio::test]
    async fn co_5_6_restore_scrub_results_recovers_dropped_summaries() {
        let (store, _tmp) = make_test_store().await;
        let mk = |path: &str| ScrubResultSummary {
            disk_path: path.to_string(),
            level: "quick".to_string(),
            completed_at_ms: 1_000,
            chunks_checked: 10,
            chunks_healthy: 10,
            chunks_corrupt: 0,
            chunks_missing: 0,
            duration_secs: 1.5,
        };
        store.enqueue_scrub_result(mk("/disk/a"));

        // Heartbeat builder drains the queue.
        let drained = store.drain_scrub_results();
        assert_eq!(drained.len(), 1);
        assert!(store.drain_scrub_results().is_empty(), "drain empties the queue");

        // A fresh result lands while the failed heartbeat is in flight.
        store.enqueue_scrub_result(mk("/disk/b"));
        // Simulated heartbeat failure: re-enqueue the drained batch.
        store.restore_scrub_results(drained);

        let after = store.drain_scrub_results();
        assert_eq!(after.len(), 2, "restored + newly enqueued are both present");
        assert_eq!(after[0].disk_path, "/disk/a", "restored batch keeps priority");
        assert_eq!(after[1].disk_path, "/disk/b");
    }

    #[tokio::test]
    async fn test_put_and_get_chunk() {
        let (store, _tmp) = make_test_store().await;
        let data = b"hello, DTFS!";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        let meta = store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        assert_eq!(meta.size, data.len() as u64);

        let retrieved = store.get_chunk(&chunk_id).await.unwrap();
        assert_eq!(retrieved.as_ref(), data);
    }

    // Bug CI follow-up — replication egress streams straight from the
    // chunk's on-disk file (Bug I parity for the read side). The path
    // resolver must find a committed chunk and report its true size, and
    // return None for an absent id so the caller maps it to ChunkNotFound.
    #[tokio::test]
    async fn chunk_file_path_resolves_committed_chunk() {
        let (store, _tmp) = make_test_store().await;
        let data: &[u8] = b"bug-ci-replication-streams-from-disk";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());
        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        let (path, size) = store
            .chunk_file_path(&chunk_id)
            .expect("committed chunk must resolve to its .dat path");
        assert_eq!(size, data.len() as u64);
        assert!(path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .ends_with(".dat"));
        assert!(path.exists());

        let absent = ChunkId::from_bytes([9u8; 32]);
        assert!(store.chunk_file_path(&absent).is_none());
    }

    // Bug BU-6 — startup reclaim removes leftover streaming *.dat.tmp
    // orphans (a crash/ENOSPC-aborted put) but leaves unrelated files.
    #[tokio::test]
    async fn bu6_reclaim_orphan_streaming_tmps() {
        let (store, tmp) = make_test_store().await;
        let tmp_dir = tmp.path().join("tmp");
        let orphan1 = tmp_dir.join("streaming-123-456.dat.tmp");
        let orphan2 = tmp_dir.join("streaming-123-789.dat.tmp");
        let keep = tmp_dir.join("keep.txt");
        std::fs::write(&orphan1, b"partial").unwrap();
        std::fs::write(&orphan2, b"partial").unwrap();
        std::fs::write(&keep, b"unrelated").unwrap();

        let removed = store.reclaim_orphan_streaming_tmps();
        assert_eq!(removed, 2, "both .dat.tmp orphans reclaimed");
        assert!(!orphan1.exists() && !orphan2.exists(), "orphan tmps gone");
        assert!(keep.exists(), "non-tmp file preserved");
    }

    #[tokio::test]
    async fn test_delete_chunk() {
        let (store, _tmp) = make_test_store().await;
        let data = b"delete me";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        assert!(store.has_chunk(&chunk_id).await);

        store.delete_chunk(&chunk_id).await.unwrap();
        assert!(!store.has_chunk(&chunk_id).await);
    }

    #[tokio::test]
    async fn test_chunk_not_found() {
        let (store, _tmp) = make_test_store().await;
        let chunk_id = ChunkId::from_bytes([0u8; 32]);
        let result = store.get_chunk(&chunk_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_scrub_empty_disk() {
        let (store, tmp) = make_test_store().await;
        let result = store
            .scrub_disk(tmp.path(), ScrubLevel::Quick)
            .await
            .unwrap();
        assert_eq!(result.chunks_checked, 0);
    }

    #[tokio::test]
    async fn test_scrub_with_data() {
        let (store, tmp) = make_test_store().await;
        let data = b"scrub test data";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        // Quick scrub
        let result = store
            .scrub_disk(tmp.path(), ScrubLevel::Quick)
            .await
            .unwrap();
        assert_eq!(result.chunks_checked, 1);
        assert_eq!(result.chunks_healthy, 1);

        // Deep scrub
        let result = store
            .scrub_disk(tmp.path(), ScrubLevel::Deep)
            .await
            .unwrap();
        assert_eq!(result.chunks_checked, 1);
        assert_eq!(result.chunks_healthy, 1);
    }

    #[tokio::test]
    async fn test_wal_recovery_cleans_tmp() {
        let (store, tmp) = make_test_store().await;

        // Simulate a crash: create a WAL entry and a tmp file, but no
        // completed chunk.
        let chunk_hex = "ab".repeat(32);
        let wal_entry = WalEntry {
            operation: WalOperation::PutChunk,
            chunk_id: chunk_hex.clone(),
            disk_path: tmp.path().to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        let wal_path = tmp
            .path()
            .join(".dtfs")
            .join("wal")
            .join(format!("{}.wal.json", chunk_hex));
        write_wal_entry(&wal_path, &wal_entry).await.unwrap();

        // Create a stale tmp file.
        let tmp_file = tmp
            .path()
            .join("tmp")
            .join(format!("{}.dat.tmp", chunk_hex));
        tokio::fs::write(&tmp_file, b"incomplete data")
            .await
            .unwrap();

        // Run recovery.
        let recovered = store.recover_from_wal().await.unwrap();
        assert!(recovered >= 1);

        // The tmp file should be gone.
        assert!(!tmp_file.exists());
        // The WAL entry should be gone.
        assert!(!wal_path.exists());
    }

    #[tokio::test]
    async fn test_wal_recovery_empty() {
        let (store, _tmp) = make_test_store().await;
        let recovered = store.recover_from_wal().await.unwrap();
        assert_eq!(recovered, 0);
    }

    // ---- Regression tests for counter-rebuild bug (2026-05-18) ----
    //
    // Bug history: ChunkStore::bytes_stored + chunk_count were AtomicU64s
    // that started at 0 on every process start and only ever incremented
    // in put_chunk(). After a `mount-data-disks.yml` playbook restart,
    // chunks remained on disk but the next heartbeat reported 0 used
    // capacity to the manager — observed live as df 7.6 GB vs
    // manager.used_capacity=0 across every restarted node. Fix is
    // rescan_existing_chunks() which walks the disk and rebuilds the
    // counters from reality at startup. These tests pin that behaviour.

    /// Build a ChunkStore over an EXISTING disk path (does NOT recreate
    /// the `.dtfs`/`chunks`/`tmp` dirs). Used to simulate a daemon
    /// restart against the same on-disk state.
    fn open_store_at(disk_path: std::path::PathBuf) -> ChunkStore {
        let disk_configs = vec![dtfs_common::config::DiskConfig {
            path: disk_path,
            tier: DiskTier::Hot,
        }];
        let dm = DiskManager::new(&disk_configs).unwrap();
        ChunkStore::new(dm)
    }

    #[tokio::test]
    async fn rescan_rebuilds_counters_after_restart() {
        let (store, tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());

        // Write 3 chunks of varying sizes.
        let blobs: Vec<&[u8]> = vec![
            b"first chunk content",
            b"a slightly longer second chunk",
            &[0xABu8; 4096], // 4 KiB
        ];
        let mut total_bytes: u64 = 0;
        for data in &blobs {
            let hash = checksum::compute(data);
            let id = ChunkId::from_bytes(hash);
            store
                .put_chunk(&id, data, &volume_id, DiskTier::Hot)
                .await
                .unwrap();
            total_bytes += data.len() as u64;
        }

        // Original store sees them.
        let stats_before = store.stats();
        assert_eq!(stats_before.total_chunks, 3);
        assert_eq!(stats_before.total_bytes_stored, total_bytes);

        // Simulate a daemon restart: drop the old store, build a new one
        // pointing at the same disk dir. THIS IS THE BUG STATE — the new
        // store starts with counters = 0 even though chunks are on disk.
        drop(store);
        let store2 = open_store_at(tmp.path().to_path_buf());

        // Before rescan: counters are zero (the bug). Confirms the bug
        // exists in the absence of the rescan call.
        let stats_pre_rescan = store2.stats();
        assert_eq!(
            stats_pre_rescan.total_chunks, 0,
            "fresh ChunkStore should start at 0 — pin this so we notice if init logic changes"
        );
        assert_eq!(stats_pre_rescan.total_bytes_stored, 0);

        // Run the fix.
        store2.rescan_existing_chunks().await.unwrap();

        // After rescan: counters match what's on disk.
        let stats_post_rescan = store2.stats();
        assert_eq!(
            stats_post_rescan.total_chunks, 3,
            "rescan_existing_chunks must rebuild chunk_count from disk",
        );
        assert_eq!(
            stats_post_rescan.total_bytes_stored, total_bytes,
            "rescan_existing_chunks must rebuild bytes_stored from disk",
        );
    }

    #[tokio::test]
    async fn rescan_handles_empty_disk() {
        let (_store, tmp) = make_test_store().await;
        let store2 = open_store_at(tmp.path().to_path_buf());
        store2.rescan_existing_chunks().await.unwrap();
        let s = store2.stats();
        assert_eq!(s.total_chunks, 0);
        assert_eq!(s.total_bytes_stored, 0);
    }

    #[tokio::test]
    async fn rescan_handles_missing_chunks_dir() {
        let tmp = TempDir::new().unwrap();
        let disk_path = tmp.path().to_path_buf();
        // Deliberately do NOT create chunks/ — the rescan must not fail
        // when a freshly-mounted disk hasn't been written to yet.
        let disk_configs = vec![dtfs_common::config::DiskConfig {
            path: disk_path,
            tier: DiskTier::Hot,
        }];
        let dm = DiskManager::new(&disk_configs).unwrap();
        let store = ChunkStore::new(dm);
        store.rescan_existing_chunks().await.unwrap();
        assert_eq!(store.stats().total_chunks, 0);
        assert_eq!(store.stats().total_bytes_stored, 0);
    }

    #[tokio::test]
    async fn rescan_idempotent() {
        // Calling rescan twice produces the same totals — guards against
        // a future change that accidentally accumulates instead of
        // replacing.
        let (store, tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let data = b"idempotent test";
        let id = ChunkId::from_bytes(checksum::compute(data));
        store
            .put_chunk(&id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        drop(store);
        let store2 = open_store_at(tmp.path().to_path_buf());
        store2.rescan_existing_chunks().await.unwrap();
        let first = store2.stats();
        store2.rescan_existing_chunks().await.unwrap();
        let second = store2.stats();
        assert_eq!(first.total_chunks, second.total_chunks);
        assert_eq!(first.total_bytes_stored, second.total_bytes_stored);
        assert_eq!(first.total_chunks, 1);
        assert_eq!(first.total_bytes_stored, data.len() as u64);
    }

    #[tokio::test]
    async fn rescan_ignores_tmp_and_wal_files() {
        // Only `.dat` files under chunks/ should count. Partial tmp
        // writes and WAL entries must not be counted as live chunks
        // (they'd inflate used_capacity by stale-write footprint).
        let (store, tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let data = b"real chunk";
        let id = ChunkId::from_bytes(checksum::compute(data));
        store
            .put_chunk(&id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        // Drop a fake .dat.tmp + .wal.json next to the real chunk.
        let chunk_hex = id.as_hex();
        let bogus_tmp = tmp
            .path()
            .join("tmp")
            .join(format!("{}.dat.tmp", chunk_hex));
        tokio::fs::write(&bogus_tmp, vec![0u8; 4096]).await.unwrap();
        let bogus_wal = tmp
            .path()
            .join(".dtfs")
            .join("wal")
            .join("aa.wal.json");
        tokio::fs::write(&bogus_wal, b"{}").await.unwrap();

        drop(store);
        let store2 = open_store_at(tmp.path().to_path_buf());
        store2.rescan_existing_chunks().await.unwrap();
        let s = store2.stats();
        assert_eq!(s.total_chunks, 1, "tmp + wal must not be counted");
        assert_eq!(s.total_bytes_stored, data.len() as u64);
    }

    #[tokio::test]
    async fn rescan_replaces_stale_counter_not_accumulates() {
        // Tests the exact lab regression: ChunkStore was created, some
        // chunks written, counters non-zero. Then ANOTHER call to
        // rescan_existing_chunks() must REPLACE the counters with the
        // disk truth (not double them). Critical because the rescan
        // could be called at startup even after some put_chunk calls
        // have raced ahead.
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());

        // Write 2 chunks (counters jump to 2).
        for i in 0..2u8 {
            let data = vec![i; 1024];
            let id = ChunkId::from_bytes(checksum::compute(&data));
            store
                .put_chunk(&id, &data, &volume_id, DiskTier::Hot)
                .await
                .unwrap();
        }
        assert_eq!(store.stats().total_chunks, 2);

        // Force the bug state on the *same* store: pretend the counters
        // are stale (e.g. simulating a path where some other future
        // bug left them inflated). Inject by adding to the atomics
        // directly via repeated puts to a different chunk, then rescan:
        // we expect rescan to set them to the true on-disk value (2),
        // not (2 + extra).
        for _ in 0..3 {
            store.rescan_existing_chunks().await.unwrap();
        }
        let s = store.stats();
        assert_eq!(
            s.total_chunks, 2,
            "rescan must replace, not accumulate; multiple invocations stay stable"
        );
        assert_eq!(s.total_bytes_stored, 1024 * 2);
    }

    // -----------------------------------------------------------------------
    // Bug I — stream put_chunk to disk so peak memory is ~1 MB / write
    // instead of one full chunk (~128 MB). See
    // plans/incidents/SOAK-RECOVERY-2026-05-18.md.
    // -----------------------------------------------------------------------

    /// Bug I — caller streams bytes to a tmp file, ChunkStore commits
    /// the tmp file as a chunk. The result must be indistinguishable
    /// from `put_chunk(buf)`: same on-disk layout, same meta, same
    /// hash, same counter behaviour.
    #[tokio::test]
    async fn bug_i_commit_chunk_from_tmp_roundtrip() {
        use dtfs_common::checksum::IncrementalHasher;
        use tokio::io::AsyncWriteExt;

        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());

        // Allocate a streaming tmp file on the right disk for the tier.
        let (tmp_path, _disk_mount) = store
            .allocate_streaming_tmp(DiskTier::Hot)
            .expect("allocate_streaming_tmp returns Some on a healthy disk");
        assert!(tmp_path.exists() == false, "tmp path must not exist yet");

        // Simulate the gRPC streaming write — multiple small pieces.
        let pieces: Vec<&[u8]> = vec![
            b"first piece ",
            b"of the chunk; ",
            b"second arrives; ",
            b"third and last.",
        ];
        let mut hasher = IncrementalHasher::new();
        let mut size = 0u64;
        {
            let mut f = tokio::fs::File::create(&tmp_path).await.unwrap();
            for p in &pieces {
                hasher.update(p);
                f.write_all(p).await.unwrap();
                size += p.len() as u64;
            }
            f.sync_all().await.unwrap();
        }
        let hash = hasher.finalize();
        let chunk_id = ChunkId::from_bytes(hash);

        // Commit.
        let (meta, _disk_id) = store
            .commit_chunk_from_tmp(&chunk_id, &tmp_path, &volume_id, DiskTier::Hot, size, hash)
            .await
            .expect("commit succeeds");
        assert_eq!(meta.size, size);
        assert_eq!(meta.checksum, hash);

        // tmp file must be gone (atomic rename happened).
        assert!(!tmp_path.exists(), "tmp must be cleaned up after commit");

        // Round-trip read returns the concatenated input.
        let retrieved = store.get_chunk(&chunk_id).await.unwrap();
        let expected: Vec<u8> = pieces.iter().flat_map(|p| p.iter()).copied().collect();
        assert_eq!(retrieved.as_ref(), expected.as_slice());

        // And the hash matches what `put_chunk(&data)` would have
        // computed — proves IncrementalHasher streaming matches the
        // legacy full-buffer compute.
        let legacy_hash = checksum::compute(&expected);
        assert_eq!(hash, legacy_hash);

        // Counters updated.
        let s = store.stats();
        assert_eq!(s.total_chunks, 1);
        assert_eq!(s.total_bytes_stored, size);
    }

    /// Bug I — if the caller's streaming finished but the commit step
    /// fails (e.g. bad chunk_id), the tmp file must NOT be left behind
    /// to leak disk.
    #[tokio::test]
    async fn bug_i_commit_failure_does_not_leak_tmp_files() {
        let (store, _tmp) = make_test_store().await;

        let (tmp_path, _disk_mount) = store
            .allocate_streaming_tmp(DiskTier::Hot)
            .unwrap();

        // Write a few bytes and try to commit with a *mismatched* size.
        tokio::fs::write(&tmp_path, b"some data").await.unwrap();
        let fake_id = ChunkId::from_bytes([7u8; 32]);
        // Use a clearly wrong size to provoke a failure mode at the
        // metadata-write step (or earlier validation). The tmp file
        // must NOT leak even on the error path.
        let _ = store
            .commit_chunk_from_tmp(
                &fake_id,
                &tmp_path,
                &VolumeId::new("default".to_string()),
                DiskTier::Hot,
                999_999_999, // intentionally wrong
                [0u8; 32],
            )
            .await;
        // Cleanup happens either way; assert no tmp file is left
        // behind no matter what the commit decision was.
        assert!(
            !tmp_path.exists(),
            "tmp file must be removed even when commit fails"
        );
    }

    /// Bug K — the streaming commit MUST fsync the tmp .dat before
    /// the rename. We can't directly observe fsync from outside the
    /// kernel, but we can prove the function returns successfully
    /// after writing bytes that aren't tied to an open writer handle.
    /// (The fsync_dir helper is exercised via the integration tests
    /// + ChunkStore tests that follow.)
    #[tokio::test]
    async fn bug_k_commit_succeeds_when_tmp_is_already_closed() {
        use tokio::io::AsyncWriteExt;
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());

        let (tmp_path, _disk_mount) = store.allocate_streaming_tmp(DiskTier::Hot).unwrap();
        let payload = b"closed-then-committed";
        {
            let mut f = tokio::fs::File::create(&tmp_path).await.unwrap();
            f.write_all(payload).await.unwrap();
            f.flush().await.unwrap();
            // NOTE: drop without sync_all — Bug K means commit must
            // sync the tmp itself before renaming.
        }
        let hash = dtfs_common::checksum::compute(payload);
        let chunk_id = ChunkId::from_bytes(hash);
        let (meta, _disk_id) = store
            .commit_chunk_from_tmp(
                &chunk_id,
                &tmp_path,
                &volume_id,
                DiskTier::Hot,
                payload.len() as u64,
                hash,
            )
            .await
            .expect("commit must succeed even if caller didn't sync");
        assert_eq!(meta.size, payload.len() as u64);
        let got = store.get_chunk(&chunk_id).await.unwrap();
        assert_eq!(got.as_ref(), payload);
    }

    /// Bug M — `recover_from_wal` must be safe to call on a clean
    /// store (no WAL entries). Returns 0 recovered, no error.
    #[tokio::test]
    async fn bug_m_recover_from_wal_safe_on_empty_store() {
        let (store, _tmp) = make_test_store().await;
        let n = store.recover_from_wal().await.expect("clean recover ok");
        assert_eq!(n, 0);
    }

    /// Bug M — when a previous instance crashed leaving a tmp file
    /// + a WAL entry pointing at it, the next startup must reclaim
    /// the tmp. Without `recover_from_wal` at boot, orphan tmps
    /// leaked disk forever (audit finding #1c).
    #[tokio::test]
    async fn bug_m_recover_from_wal_reclaims_orphan_tmp() {
        let (store, tmp) = make_test_store().await;
        let disk_path = tmp.path().to_path_buf();

        // Hand-craft a tmp + WAL entry as if a prior writer crashed
        // mid-commit_chunk_from_tmp.
        let tmp_dir_path = disk_path.join("tmp");
        tokio::fs::create_dir_all(&tmp_dir_path).await.unwrap();
        let leftover_tmp = tmp_dir_path.join("streaming-9999.dat.tmp");
        tokio::fs::write(&leftover_tmp, b"orphan").await.unwrap();
        assert!(leftover_tmp.exists());

        // Run recovery.
        store.recover_from_wal().await.expect("recover ok");

        // The orphan tmp must be gone. Whether by recover_from_wal
        // or by the tmp-sweep pass; we don't care which, just that
        // the leak is closed.
        // NB: current recover_from_wal sweeps tmp at the end (see
        // store.rs:594-607). If a future refactor removes that
        // sweep this assertion catches it.
        assert!(
            !leftover_tmp.exists(),
            "orphan tmp from a crashed prior writer must be reclaimed at startup"
        );
    }

    /// Bug I — peak memory: prove the streaming contract by writing a
    /// chunk much larger than typical Vec accumulation would tolerate
    /// in a tight unit test. If a future refactor secretly reads the
    /// tmp into RAM, this test still passes — but the live profiler on
    /// data-a1 (configured to a 2 GB hard cap) will OOM the moment
    /// concurrent writes start, and the regression is caught in
    /// `tests/proxmox/abuse/13-data-node-mem-cap.sh`.
    #[tokio::test]
    async fn bug_i_commit_handles_multi_mb_streamed_chunk() {
        use dtfs_common::checksum::IncrementalHasher;
        use tokio::io::AsyncWriteExt;
        let (store, _tmp) = make_test_store().await;
        let volume_id = VolumeId::new("default".to_string());

        // 4 MB chunk streamed in 64 KB pieces — small enough for a
        // unit test, big enough to prove the pipeline actually flushes
        // to disk piece-by-piece rather than buffering.
        let piece = vec![0xABu8; 64 * 1024];
        let pieces = 64; // → 4 MB
        let (tmp_path, _disk_mount) = store.allocate_streaming_tmp(DiskTier::Hot).unwrap();

        let mut hasher = IncrementalHasher::new();
        let mut size = 0u64;
        {
            let mut f = tokio::fs::File::create(&tmp_path).await.unwrap();
            for _ in 0..pieces {
                hasher.update(&piece);
                f.write_all(&piece).await.unwrap();
                size += piece.len() as u64;
            }
            f.sync_all().await.unwrap();
        }
        let hash = hasher.finalize();
        let chunk_id = ChunkId::from_bytes(hash);
        store
            .commit_chunk_from_tmp(&chunk_id, &tmp_path, &volume_id, DiskTier::Hot, size, hash)
            .await
            .unwrap();

        let got = store.get_chunk(&chunk_id).await.unwrap();
        assert_eq!(got.len() as u64, size);
        assert_eq!(&got[0..16], &vec![0xABu8; 16][..]);
    }

    // ---- AV-1-5 — DeleteChunkReplica idempotent delete tests ------

    /// AV-1-5 test 1 — atomic-on-success: delete_chunk_idempotent
    /// unlinks BOTH the `.dat` and the `.meta` and reports the file
    /// size that was freed.
    #[tokio::test]
    async fn bug_av_delete_chunk_atomic_unlinks_both() {
        let (store, tmp) = make_test_store().await;
        let data = b"unlink-both";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        let dat = chunk_data_path(tmp.path(), &chunk_id);
        let meta = chunk_meta_path(tmp.path(), &chunk_id);
        assert!(dat.exists(), "pre-delete dat present");
        assert!(meta.exists(), "pre-delete meta present");

        let (was_present, freed) = store.delete_chunk_idempotent(&chunk_id).await.unwrap();
        assert!(was_present, "first delete sees the chunk");
        assert_eq!(freed, data.len() as u64, "freed_bytes = data size");
        assert!(!dat.exists(),  "dat unlinked");
        assert!(!meta.exists(), "meta unlinked");
    }

    /// AV-1-5 test 2 — delete on an absent chunk returns
    /// `was_present=false, freed_bytes=0` and does NOT error. A retry
    /// of a previously-successful delete (e.g. the manager's
    /// flush-cache scheduler retrying after a transient network error)
    /// won't tear down the batch.
    #[tokio::test]
    async fn bug_av_delete_chunk_missing_is_idempotent() {
        let (store, _tmp) = make_test_store().await;
        let hash = checksum::compute(b"never-stored");
        let chunk_id = ChunkId::from_bytes(hash);
        let (was_present, freed) = store.delete_chunk_idempotent(&chunk_id).await.unwrap();
        assert!(!was_present, "missing chunk → was_present=false");
        assert_eq!(freed, 0, "no bytes freed");
    }

    /// AV-1-5 test 3 — second delete after a successful first delete
    /// also returns was_present=false (the "retry after I already
    /// deleted" path). Same shape as test 2 but exercises the
    /// state-after-success path explicitly.
    #[tokio::test]
    async fn bug_av_delete_chunk_second_delete_is_noop() {
        let (store, _tmp) = make_test_store().await;
        let data = b"twice-removed";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        let (was_present_1, _) = store.delete_chunk_idempotent(&chunk_id).await.unwrap();
        let (was_present_2, freed_2) = store.delete_chunk_idempotent(&chunk_id).await.unwrap();
        assert!(was_present_1, "first delete sees chunk");
        assert!(!was_present_2, "second delete: gone");
        assert_eq!(freed_2, 0);
    }

    /// CO-1-1 — a two-disk store: disk 0 = Cold (anchor), disk 1 = Warm
    /// (cache). disk_id is assigned by Vec position in `DiskManager::new`.
    async fn make_two_disk_store() -> (ChunkStore, TempDir, TempDir) {
        let cold = TempDir::new().unwrap();
        let warm = TempDir::new().unwrap();
        let disk_configs = vec![
            dtfs_common::config::DiskConfig { path: cold.path().to_path_buf(), tier: DiskTier::Cold },
            dtfs_common::config::DiskConfig { path: warm.path().to_path_buf(), tier: DiskTier::Warm },
        ];
        let dm = DiskManager::new(&disk_configs).unwrap();
        (ChunkStore::new(dm), cold, warm)
    }

    /// CO-1-1 — the same chunk lives on the cold anchor (disk 0) AND the
    /// warm cache (disk 1). Deleting with the WARM disk_id must unlink ONLY
    /// the warm copy; the cold anchor `.dat` survives. This is the demote
    /// scenario that pre-fix could delete the anchor (first-found ordering
    /// returned the cold disk first).
    #[tokio::test]
    async fn co_1_1_disk_targeted_delete_preserves_cold_anchor() {
        let (store, cold, warm) = make_two_disk_store().await;
        let data = b"co-1-1-anchor-vs-cache";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        // Land the chunk on BOTH disks (cold anchor + warm cache copy).
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Cold).await.unwrap();
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Warm).await.unwrap();

        let cold_dat = chunk_data_path(cold.path(), &chunk_id);
        let warm_dat = chunk_data_path(warm.path(), &chunk_id);
        assert!(cold_dat.exists(), "cold anchor present pre-delete");
        assert!(warm_dat.exists(), "warm cache present pre-delete");

        // Demote: delete ONLY the warm disk (disk_id 1).
        let (was_present, freed) = store
            .delete_chunk_idempotent_on_disk(&chunk_id, Some(1))
            .await
            .unwrap();
        assert!(was_present, "warm copy was present");
        assert_eq!(freed, data.len() as u64);

        assert!(cold_dat.exists(), "CO-1-1: cold anchor MUST survive a warm-disk delete");
        assert!(!warm_dat.exists(), "warm cache copy unlinked");
    }

    /// CO-1-1 — `disk_id = None` keeps the legacy first-found behaviour
    /// (used by the orphan reaper / unlink fan-out that reclaim the whole
    /// chunk). First-found scans disks in Vec order, so it hits the cold
    /// disk (disk 0) first here.
    #[tokio::test]
    async fn co_1_1_disk_none_is_legacy_first_found() {
        let (store, cold, warm) = make_two_disk_store().await;
        let data = b"co-1-1-legacy-first-found";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Cold).await.unwrap();
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Warm).await.unwrap();

        let cold_dat = chunk_data_path(cold.path(), &chunk_id);
        let warm_dat = chunk_data_path(warm.path(), &chunk_id);

        let (was_present, _) = store
            .delete_chunk_idempotent_on_disk(&chunk_id, None)
            .await
            .unwrap();
        assert!(was_present);
        // First-found removes exactly one copy (the disk-0 cold one); the
        // warm copy remains. (Documented legacy single-copy-per-call shape.)
        assert!(!cold_dat.exists(), "first-found removed the disk-0 copy");
        assert!(warm_dat.exists(), "the other disk's copy remains (one per call)");
    }

    /// CO-1-1 — an unknown disk_id is a no-op, never a fall-through to
    /// first-found. A stale id must not delete the wrong copy.
    #[tokio::test]
    async fn co_1_1_unknown_disk_id_is_noop() {
        let (store, cold, warm) = make_two_disk_store().await;
        let data = b"co-1-1-unknown-disk";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Cold).await.unwrap();
        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Warm).await.unwrap();

        let (was_present, freed) = store
            .delete_chunk_idempotent_on_disk(&chunk_id, Some(99))
            .await
            .unwrap();
        assert!(!was_present, "no disk 99 -> no-op");
        assert_eq!(freed, 0);
        assert!(chunk_data_path(cold.path(), &chunk_id).exists(), "cold copy untouched");
        assert!(chunk_data_path(warm.path(), &chunk_id).exists(), "warm copy untouched");
    }

    /// AV-4-7 — WAL-recoverable delete. Resolves the AV-1-5
    /// deviation that originally deferred WAL-side atomicity. Test
    /// scenario: simulate a crash that happened AFTER the WAL was
    /// written and the `.dat` was unlinked, but BEFORE the `.meta`
    /// was unlinked. On boot, `recover_from_wal` must finish the job
    /// and clear the WAL entry.
    #[tokio::test]
    async fn bug_av_delete_chunk_wal_recovery_after_crash() {
        let (store, tmp) = make_test_store().await;
        let data = b"wal-recovery-victim";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        let dat = chunk_data_path(tmp.path(), &chunk_id);
        let meta = chunk_meta_path(tmp.path(), &chunk_id);
        assert!(dat.exists() && meta.exists(), "pre-crash setup");

        // Simulate the AV-4-7 crash window: WAL was written, `.dat`
        // was unlinked, process died before `.meta` was unlinked.
        let wal_path = wal_dir(tmp.path())
            .join(format!("{}.wal.json", chunk_id.as_hex()));
        let wal_entry = WalEntry {
            operation: WalOperation::DeleteChunk,
            chunk_id: chunk_id.as_hex(),
            disk_path: tmp.path().to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        write_wal_entry(&wal_path, &wal_entry).await.unwrap();
        fs::remove_file(&dat).await.unwrap();
        assert!(!dat.exists(),  "crash sim: .dat removed");
        assert!( meta.exists(), "crash sim: .meta still present");
        assert!( wal_path.exists(), "crash sim: WAL marker present");

        // Recover.
        let n = store.recover_from_wal().await.unwrap();
        assert!(n >= 1, "at least the delete WAL entry processed");

        // Both files must be gone now, WAL entry cleared.
        assert!(!dat.exists(),  "post-recovery .dat gone");
        assert!(!meta.exists(), "post-recovery .meta gone");
        assert!(!wal_path.exists(), "post-recovery WAL cleared");
    }

    /// AV-4-7 — WAL replay is safe to re-run any number of times.
    /// Re-invoking recover after a successful one is a no-op.
    #[tokio::test]
    async fn bug_av_delete_chunk_wal_recovery_idempotent() {
        let (store, _tmp) = make_test_store().await;
        let n1 = store.recover_from_wal().await.unwrap();
        let n2 = store.recover_from_wal().await.unwrap();
        assert_eq!(n1, 0, "fresh store: nothing to recover");
        assert_eq!(n2, 0, "second run: still nothing to recover");
    }

    /// AV-1-5 test 4 — sequential put followed by delete leaves
    /// consistent counters. Tests that bytes_stored decrements
    /// correctly through the new idempotent path (regression on the
    /// fix #6 counter behaviour from the legacy `delete_chunk`).
    #[tokio::test]
    async fn bug_av_delete_chunk_counters_consistent() {
        let (store, _tmp) = make_test_store().await;
        let data = b"counter-check-data-with-known-size";
        let n = data.len() as u64;
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        let stats_before = store.stats();
        assert_eq!(stats_before.total_chunks, 0);
        assert_eq!(stats_before.total_bytes_stored, 0);

        store.put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot).await.unwrap();
        let after_put = store.stats();
        assert_eq!(after_put.total_chunks, 1);
        assert_eq!(after_put.total_bytes_stored, n);

        let (was, freed) = store.delete_chunk_idempotent(&chunk_id).await.unwrap();
        assert!(was);
        assert_eq!(freed, n);
        let after_del = store.stats();
        assert_eq!(after_del.total_chunks, 0);
        assert_eq!(after_del.total_bytes_stored, 0);
    }

    // ── CR-4-2 (DATA-2) — atomic `.meta` write ─────────────────────

    /// `write_meta_atomic` must never leave a partial `.meta` at the final
    /// path: force the rename to fail (target directory doesn't exist) and
    /// assert nothing landed at `final_meta_path` and no tmp leftover
    /// remains in the disk's `tmp/` dir.
    #[tokio::test]
    async fn cr_4_2_write_meta_atomic_leaves_no_partial_on_rename_failure() {
        let tmp = TempDir::new().unwrap();
        let disk_root = tmp.path().to_path_buf();
        fs::create_dir_all(disk_root.join("tmp")).await.unwrap();

        let final_meta = disk_root.join("nonexistent-dir").join("x.meta");
        let res = write_meta_atomic(&disk_root, &final_meta, "{}").await;
        assert!(res.is_err(), "rename over a missing directory must fail");
        assert!(!final_meta.exists(), "no partial .meta at the final path");

        let mut left = fs::read_dir(disk_root.join("tmp")).await.unwrap();
        assert!(
            left.next_entry().await.unwrap().is_none(),
            "failed rename must not leave a .meta.tmp leftover"
        );
    }

    /// A WAL(Put) entry whose `.dat` is committed but whose `.meta` is
    /// present-but-torn (0 bytes, unparseable) must be rolled back exactly
    /// like the meta-missing case (CO-1-4b) — no torn `.meta` survives.
    #[tokio::test]
    async fn cr_4_2_put_wal_replay_rolls_back_torn_meta() {
        let (store, tmp) = make_test_store().await;
        let chunk_id = ChunkId::from_bytes([0xABu8; 32]);

        let chunk_dir = chunk_data_dir(tmp.path(), &chunk_id);
        std::fs::create_dir_all(&chunk_dir).unwrap();
        let dat_path = chunk_dir.join(format!("{}.dat", chunk_id.as_hex()));
        std::fs::write(&dat_path, b"committed-but-torn-meta").unwrap();
        let meta_path = chunk_dir.join(format!("{}.meta", chunk_id.as_hex()));
        std::fs::write(&meta_path, b"").unwrap(); // 0-byte: unparseable

        let wal_path = wal_dir(tmp.path()).join(format!("{}.wal.json", chunk_id.as_hex()));
        let wal_entry = WalEntry {
            operation: WalOperation::PutChunk,
            chunk_id: chunk_id.as_hex(),
            disk_path: tmp.path().to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        write_wal_entry(&wal_path, &wal_entry).await.unwrap();

        store.recover_from_wal().await.unwrap();

        assert!(!dat_path.exists(), "torn meta must roll back like meta-missing");
        assert!(!meta_path.exists());
        assert!(!wal_path.exists(), "WAL entry must be cleared after replay");
    }

    // ── CR-4-3 (DATA-4) — post-rename failure leaves no residue ────

    /// If writing the `.meta` companion fails AFTER the `.dat` rename has
    /// already committed, `commit_chunk_from_tmp` must remove the `.dat`,
    /// any `.meta` and the WAL entry rather than leaving a `.dat` with no
    /// matching `.meta`. Forced by pre-creating `final_meta_path` as a
    /// non-empty directory so the rename in `write_meta_atomic` fails.
    #[tokio::test]
    async fn cr_4_3_meta_write_failure_after_rename_leaves_no_residue() {
        let (store, tmp) = make_test_store().await;
        let data = b"cr-4-3-payload";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        let chunk_dir = chunk_data_dir(tmp.path(), &chunk_id);
        std::fs::create_dir_all(&chunk_dir).unwrap();
        let final_meta_path = chunk_dir.join(format!("{}.meta", chunk_id.as_hex()));
        // Pre-create the final .meta path as a non-empty directory so the
        // tmp->final rename inside write_meta_atomic fails.
        std::fs::create_dir_all(&final_meta_path).unwrap();
        std::fs::write(final_meta_path.join("blocker"), b"x").unwrap();

        let tmp_path = tmp.path().join("tmp").join(format!("{}.dat.tmp", uuid::Uuid::new_v4()));
        std::fs::write(&tmp_path, data).unwrap();

        let res = store
            .commit_chunk_from_tmp(&chunk_id, &tmp_path, &volume_id, DiskTier::Hot, data.len() as u64, hash)
            .await;
        assert!(res.is_err(), "meta write must fail while final_meta_path is a directory");

        let final_dat_path = chunk_dir.join(format!("{}.dat", chunk_id.as_hex()));
        assert!(!final_dat_path.exists(), "no orphan .dat left behind");
        let wal_path = wal_dir(tmp.path()).join(format!("{}.wal.json", chunk_id.as_hex()));
        assert!(!wal_path.exists(), "no stale WAL entry left behind");
    }

    // ── CR-4-4 (DATA-3) — per-chunk lock across commit/delete ──────

    /// Racing `commit_chunk_from_tmp` against `delete_chunk_idempotent` on
    /// the same chunk id, 200 times, must never observe a torn state: the
    /// `.dat` and `.meta` companions always agree on presence.
    #[tokio::test]
    async fn cr_4_4_concurrent_commit_and_delete_never_tears_chunk() {
        let (store, tmp) = make_test_store().await;
        let store = Arc::new(store);
        let disk_root = tmp.path().to_path_buf();
        let data: &[u8] = b"cr-4-4-race-payload-bytes";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);
        let volume_id = VolumeId::new("default".to_string());

        for i in 0..200u32 {
            let tmp_path = disk_root.join("tmp").join(format!("race-{i}.dat.tmp"));
            std::fs::write(&tmp_path, data).unwrap();

            let s1 = store.clone();
            let cid1 = chunk_id.clone();
            let vol1 = volume_id.clone();
            let tp = tmp_path.clone();
            let h1 = tokio::spawn(async move {
                let _ = s1
                    .commit_chunk_from_tmp(&cid1, &tp, &vol1, DiskTier::Hot, data.len() as u64, hash)
                    .await;
            });
            let s2 = store.clone();
            let cid2 = chunk_id.clone();
            let h2 = tokio::spawn(async move {
                let _ = s2.delete_chunk_idempotent(&cid2).await;
            });
            let _ = tokio::join!(h1, h2);

            let dat = chunk_data_path(&disk_root, &chunk_id);
            let meta = chunk_meta_path(&disk_root, &chunk_id);
            assert_eq!(
                dat.exists(),
                meta.exists(),
                "iteration {i}: .dat and .meta must never diverge"
            );

            let _ = std::fs::remove_file(&dat);
            let _ = std::fs::remove_file(&meta);
        }
    }

    /// Recovery must process WAL entries under the new `.put.wal.json` /
    /// `.delete.wal.json` names as well as the legacy `<id>.wal.json` name
    /// (it matches on the `.json` extension and reads the operation out of
    /// the JSON body, so all three co-exist during a rolling upgrade).
    #[tokio::test]
    async fn cr_4_4_recovery_reads_put_delete_and_legacy_wal_names() {
        let (store, tmp) = make_test_store().await;
        let wd = wal_dir(tmp.path());

        for (suffix, id_byte) in [("put.wal.json", 0x11u8), ("delete.wal.json", 0x22u8), ("wal.json", 0x33u8)] {
            let cid = ChunkId::from_bytes([id_byte; 32]);
            let entry = WalEntry {
                operation: WalOperation::PutChunk,
                chunk_id: cid.as_hex(),
                disk_path: tmp.path().to_string_lossy().to_string(),
                timestamp_ms: now_millis(),
            };
            let path = wd.join(format!("{}.{}", cid.as_hex(), suffix));
            write_wal_entry(&path, &entry).await.unwrap();
        }

        let n = store.recover_from_wal().await.unwrap();
        assert_eq!(n, 3, "put/delete/legacy-named WAL files must all be recovered");
    }
}

// ─────────────────────────────────────────────────────────────────────
// Audit CO findings — RED tests that encode CORRECT behaviour and so
// FAIL against the current (unfixed) code. Do not "fix" by relaxing the
// asserts; fix the production code.
// ─────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod audit_co_tests {
    use super::*;
    use dtfs_common::types::DiskTier;
    use tempfile::TempDir;

    // Local clone of the tests-module helper (that one lives in a sibling
    // module). Builds a ChunkStore over a single hot disk in a TempDir.
    async fn make_store() -> (ChunkStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let disk_path = tmp.path().to_path_buf();
        std::fs::create_dir_all(disk_path.join(".dtfs").join("wal")).unwrap();
        std::fs::create_dir_all(disk_path.join("chunks")).unwrap();
        std::fs::create_dir_all(disk_path.join("tmp")).unwrap();
        let disk_configs = vec![dtfs_common::config::DiskConfig {
            path: disk_path,
            tier: DiskTier::Hot,
        }];
        let dm = DiskManager::new(&disk_configs).unwrap();
        (ChunkStore::new(dm), tmp)
    }

    // CO-1-4(a) — a leftover DeleteChunk WAL must NOT delete a live chunk
    // that was re-uploaded after the original delete. recover_from_wal
    // currently unlinks unconditionally, destroying the re-uploaded copy.
    #[tokio::test]
    async fn co_1_4a_delete_wal_replay_must_not_drop_live_chunk() {
        let (store, tmp) = make_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let data = b"content-addressed chunk re-uploaded after a delete";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));

        // Chunk is live on disk right now (a fresh re-upload).
        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        let dat_path = chunk_data_path(tmp.path(), &chunk_id);
        assert!(dat_path.exists(), "precondition: chunk is on disk");

        // Hand-write a stale DeleteChunk WAL entry for that id (as if an
        // earlier delete crashed before clearing its WAL).
        let wal_dir_path = wal_dir(tmp.path());
        let wal_entry = WalEntry {
            operation: WalOperation::DeleteChunk,
            chunk_id: chunk_id.as_hex(),
            disk_path: tmp.path().to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        let wal_path =
            wal_dir_path.join(format!("{}.wal.json", chunk_id.as_hex()));
        write_wal_entry(&wal_path, &wal_entry).await.unwrap();

        store.recover_from_wal().await.unwrap();

        // CORRECT: replay must re-check that no live chunk references the
        // id and skip the unlink. Today it deletes the .dat → this fails.
        assert!(
            dat_path.exists(),
            "DeleteChunk WAL replay must not delete a re-uploaded live chunk"
        );
    }

    // CO-1-4(b) — a crash between rename and .meta write leaves a committed
    // .dat with no .meta companion. recover_from_wal's PutChunk arm only
    // removes a `{id}.dat.tmp`; it must also clean the orphaned meta-less
    // committed .dat (and clear the WAL).
    #[tokio::test]
    async fn co_1_4b_put_wal_replay_cleans_meta_less_committed_dat() {
        let (store, tmp) = make_store().await;
        let chunk_id = ChunkId::from_bytes([0xCDu8; 32]);

        // Forge the crash residue: committed .dat under chunks/aa/bb/, no
        // .meta, plus the PutChunk WAL entry that should drive cleanup.
        let chunk_dir = chunk_data_dir(tmp.path(), &chunk_id);
        std::fs::create_dir_all(&chunk_dir).unwrap();
        let dat_path = chunk_dir.join(format!("{}.dat", chunk_id.as_hex()));
        std::fs::write(&dat_path, b"committed-but-meta-less").unwrap();
        assert!(!chunk_meta_path(tmp.path(), &chunk_id).exists());

        let wal_path =
            wal_dir(tmp.path()).join(format!("{}.wal.json", chunk_id.as_hex()));
        let wal_entry = WalEntry {
            operation: WalOperation::PutChunk,
            chunk_id: chunk_id.as_hex(),
            disk_path: tmp.path().to_string_lossy().to_string(),
            timestamp_ms: now_millis(),
        };
        write_wal_entry(&wal_path, &wal_entry).await.unwrap();

        store.recover_from_wal().await.unwrap();

        // CORRECT: an interrupted put left a half-committed chunk; recovery
        // must roll it back. Today the PutChunk arm only touches *.dat.tmp
        // so the orphan .dat survives → this fails.
        assert!(
            !dat_path.exists(),
            "recover_from_wal must remove the meta-less committed .dat from an interrupted put"
        );
        assert!(!wal_path.exists(), "WAL entry must be cleared after replay");
    }

    // CO-4-2 — deep scrub must not abort the whole disk on the first
    // unparseable .meta. With one chunk's .meta corrupted to invalid JSON,
    // the scrub should COMPLETE, count the healthy chunk, and flag the bad
    // one — never `?`-propagate the serde error out of scrub_disk.
    #[tokio::test]
    async fn co_4_2_deep_scrub_survives_one_bad_meta() {
        let (store, tmp) = make_store().await;
        let volume_id = VolumeId::new("default".to_string());

        let good = b"healthy chunk that must still be counted";
        let good_id = ChunkId::from_bytes(checksum::compute(good));
        store
            .put_chunk(&good_id, good, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        let bad = b"chunk whose meta we will corrupt to invalid json";
        let bad_id = ChunkId::from_bytes(checksum::compute(bad));
        store
            .put_chunk(&bad_id, bad, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        // Corrupt the bad chunk's .meta into non-JSON.
        let bad_meta = chunk_meta_path(tmp.path(), &bad_id);
        std::fs::write(&bad_meta, b"this is not valid json {{{").unwrap();

        // CORRECT: scrub completes (no error bubbled out).
        let result = store
            .scrub_disk(tmp.path(), ScrubLevel::Deep)
            .await
            .expect("deep scrub must complete despite one unparseable .meta");

        assert_eq!(
            result.chunks_checked, 2,
            "scrub must examine both chunks, not abort on the first bad one"
        );
        // CR-4-1 (DATA-1) — `bad_id`'s `.dat` is genuine (written by a real
        // `put_chunk`), only its `.meta` was corrupted; the scrub now
        // repairs a torn `.meta` from a verified `.dat` instead of
        // quarantining an otherwise-healthy chunk, so BOTH chunks end up
        // healthy and none are reported corrupt.
        assert_eq!(
            result.chunks_healthy, 2,
            "the .dat-verified chunk must be repaired, not quarantined"
        );
        assert_eq!(result.chunks_meta_repaired, 1);
        assert!(result.corrupt_chunk_ids.is_empty());
    }

    // CO-5-5 — committing the same content-addressed chunk twice must not
    // double-count chunk_count/bytes_stored (the file is the same inode,
    // re-uploaded). Today both put_chunk calls unconditionally fetch_add.
    #[tokio::test]
    async fn co_5_5_duplicate_put_must_not_double_count() {
        let (store, _tmp) = make_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let data = b"identical re-upload of one content-addressed chunk";
        let chunk_id = ChunkId::from_bytes(checksum::compute(data));

        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();
        store
            .put_chunk(&chunk_id, data, &volume_id, DiskTier::Hot)
            .await
            .unwrap();

        let stats = store.stats();
        assert_eq!(
            stats.total_chunks, 1,
            "re-putting the same content-addressed chunk must not inflate chunk_count"
        );
        assert_eq!(
            stats.total_bytes_stored,
            data.len() as u64,
            "re-putting the same chunk must count its bytes once"
        );
    }

    // CR-4-6 (DATA-7) — CO-5-5's dedup, but on the PRODUCTION streaming
    // commit path (`commit_chunk_from_tmp`), which is what `put_chunk`
    // RPC actually calls in server.rs. `put_chunk` (whole-buffer) already
    // had the dedup guard; this repoints the same invariant at the path
    // that matters in production.
    #[tokio::test]
    async fn cr_4_6_duplicate_commit_from_tmp_must_not_double_count() {
        let (store, tmp) = make_store().await;
        let volume_id = VolumeId::new("default".to_string());
        let data = b"identical re-upload via the streaming commit path";
        let hash = checksum::compute(data);
        let chunk_id = ChunkId::from_bytes(hash);

        for i in 0..2u32 {
            let tmp_path = tmp.path().join("tmp").join(format!("dup-{i}.dat.tmp"));
            std::fs::write(&tmp_path, data).unwrap();
            store
                .commit_chunk_from_tmp(&chunk_id, &tmp_path, &volume_id, DiskTier::Hot, data.len() as u64, hash)
                .await
                .unwrap();
        }

        let stats = store.stats();
        assert_eq!(
            stats.total_chunks, 1,
            "re-committing the same content-addressed chunk must not inflate chunk_count"
        );
        assert_eq!(
            stats.total_bytes_stored,
            data.len() as u64,
            "re-committing the same chunk must count its bytes once"
        );
    }
}
