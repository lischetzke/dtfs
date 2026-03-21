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
}

impl ChunkStore {
    pub fn new(disk_manager: DiskManager) -> Self {
        Self {
            disk_manager: Arc::new(disk_manager),
            chunk_count: Arc::new(AtomicU64::new(0)),
            bytes_stored: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Return a lightweight stats snapshot for heartbeat and metrics reporting.
    pub fn stats(&self) -> ChunkStoreStats {
        ChunkStoreStats {
            total_chunks: self.chunk_count.load(Ordering::Relaxed),
            total_bytes_stored: self.bytes_stored.load(Ordering::Relaxed),
        }
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

        // Atomic rename to final location
        fs::rename(&tmp_path, &final_dat_path)
            .await
            .map_err(DtfsError::Io)?;

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

        // 5. Remove the WAL entry now that the write is fully committed.
        let _ = fs::remove_file(&wal_path).await;

        // Update running counters.
        self.chunk_count.fetch_add(1, Ordering::Relaxed);
        self.bytes_stored.fetch_add(data.len() as u64, Ordering::Relaxed);

        tracing::debug!(
            "Stored chunk {} ({} bytes) on disk {}",
            chunk_id,
            data.len(),
            disk.mount_path.display()
        );

        Ok(meta)
    }

    /// Read a chunk from disk.
    pub async fn get_chunk(&self, chunk_id: &ChunkId) -> DtfsResult<Bytes> {
        // Search across all disks for the chunk
        for disk in self.disk_manager.all_disks() {
            let dat_path = chunk_data_path(&disk.mount_path, chunk_id);
            if dat_path.exists() {
                let data = fs::read(&dat_path).await.map_err(DtfsError::Io)?;

                // Verify checksum on read
                let meta_path = chunk_meta_path(&disk.mount_path, chunk_id);
                if meta_path.exists() {
                    let meta_json = fs::read_to_string(&meta_path)
                        .await
                        .map_err(DtfsError::Io)?;
                    let meta: ChunkMeta = serde_json::from_str(&meta_json)
                        .map_err(|e| DtfsError::Serialization(e.to_string()))?;

                    if !checksum::verify(&data, &meta.checksum) {
                        return Err(DtfsError::ChunkCorrupt {
                            id: chunk_id.to_string(),
                            expected: hex_encode(&meta.checksum),
                            actual: hex_encode(&checksum::compute(&data)),
                        });
                    }
                }

                return Ok(Bytes::from(data));
            }
        }

        Err(DtfsError::ChunkNotFound(chunk_id.to_string()))
    }

    /// Delete a chunk from disk.
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> DtfsResult<()> {
        for disk in self.disk_manager.all_disks() {
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

                fs::remove_file(&dat_path).await.map_err(DtfsError::Io)?;
                if meta_path.exists() {
                    fs::remove_file(&meta_path)
                        .await
                        .map_err(DtfsError::Io)?;
                }

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

                return Ok(());
            }
        }

        Err(DtfsError::ChunkNotFound(chunk_id.to_string()))
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
                                                let _ = fs::remove_file(&dat).await;
                                                let _ = fs::remove_file(&meta).await;
                                                tracing::info!(
                                                    "Completed interrupted delete for chunk {}",
                                                    wal_entry.chunk_id
                                                );
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
    pub async fn scrub_disk(
        &self,
        disk_path: &Path,
        level: ScrubLevel,
    ) -> DtfsResult<ScrubResult> {
        let chunks_dir = disk_path.join("chunks");
        let mut result = ScrubResult {
            chunks_checked: 0,
            chunks_healthy: 0,
            chunks_corrupt: 0,
            chunks_missing: 0,
            chunks_size_mismatch: 0,
            duration_secs: 0.0,
        };

        let start = std::time::Instant::now();

        if !chunks_dir.exists() {
            return Ok(result);
        }

        // Walk the chunk directory tree
        let mut dir1_entries = fs::read_dir(&chunks_dir).await.map_err(DtfsError::Io)?;
        while let Some(dir1) = dir1_entries.next_entry().await.map_err(DtfsError::Io)? {
            if !dir1.file_type().await.map_err(DtfsError::Io)?.is_dir() {
                continue;
            }
            let mut dir2_entries = fs::read_dir(dir1.path()).await.map_err(DtfsError::Io)?;
            while let Some(dir2) = dir2_entries.next_entry().await.map_err(DtfsError::Io)? {
                if !dir2.file_type().await.map_err(DtfsError::Io)?.is_dir() {
                    continue;
                }
                let mut file_entries = fs::read_dir(dir2.path()).await.map_err(DtfsError::Io)?;
                while let Some(entry) =
                    file_entries.next_entry().await.map_err(DtfsError::Io)?
                {
                    let path = entry.path();
                    if path.extension().map_or(false, |e| e == "dat") {
                        result.chunks_checked += 1;

                        let meta_path = path.with_extension("meta");
                        if !meta_path.exists() {
                            result.chunks_missing += 1;
                            continue;
                        }

                        // Quick scrub: check existence + size
                        let file_meta =
                            fs::metadata(&path).await.map_err(DtfsError::Io)?;
                        let chunk_meta_json = fs::read_to_string(&meta_path)
                            .await
                            .map_err(DtfsError::Io)?;
                        let chunk_meta: ChunkMeta =
                            serde_json::from_str(&chunk_meta_json)
                                .map_err(|e| DtfsError::Serialization(e.to_string()))?;

                        if file_meta.len() != chunk_meta.size {
                            result.chunks_size_mismatch += 1;
                            continue;
                        }

                        // Deep scrub: full checksum verification
                        if level == ScrubLevel::Deep {
                            let data = fs::read(&path).await.map_err(DtfsError::Io)?;
                            if !checksum::verify(&data, &chunk_meta.checksum) {
                                result.chunks_corrupt += 1;
                                continue;
                            }
                        }

                        result.chunks_healthy += 1;
                    }
                }
            }
        }

        result.duration_secs = start.elapsed().as_secs_f64();
        Ok(result)
    }
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

/// Write a WAL entry to disk with fsync.
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
    Ok(())
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
}
