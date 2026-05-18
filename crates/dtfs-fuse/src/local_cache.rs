// P0-11: disk-backed FUSE chunk cache with size + (optional) age eviction.
//
// Design intent for media workloads:
//   * Primary eviction is SIZE-based, oldest-atime-first (LRU).
//   * Age TTL is a SAFETY UPPER BOUND, defaulting to 90 days. Operators
//     can set it to None to disable time-based eviction entirely.
//   * Scanner-style reads (Plex library scan: stat + tiny header read)
//     must NOT refresh the LRU position of a chunk, or they'd make every
//     touched chunk look "recently used" and defeat eviction. We only
//     bump atime when the caller is reading a substantial slice of the
//     chunk (>= `ATIME_UPDATE_MIN_BYTES`); small reads return the cached
//     bytes without touching atime so the next eviction sees the true
//     last-viewer access time.
//   * Readahead-mirrored chunks (next-episode prefetches) DO get a fresh
//     atime via `put` so they sit near the top of the LRU and survive
//     scanner activity in between viewings.
//
// Atomic writes: a torn write would poison the cache, so each entry is
// written to `<...>.partial`, fsynced, and renamed. Reads only look at
// `.dat` files so partial writes are never observed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::fs as tfs;
use tokio::io::AsyncWriteExt;

/// Reads below this byte count don't refresh the cached chunk's atime
/// (scanner/header-only access). Set to 1 MiB by default: a Plex library
/// scanner typically reads just the file header (a few hundred KB) while
/// a media playback session pulls megabytes of contiguous data.
pub const ATIME_UPDATE_MIN_BYTES: usize = 1024 * 1024;

/// One disk-backed chunk cache.
pub struct LocalChunkCache {
    dir: PathBuf,
    max_bytes: u64,
    max_age: Option<Duration>,
    /// LRU index: mtime_ms -> chunk_id. We use BTreeMap so the oldest entry
    /// is at the head and eviction is O(log N).
    lru: RwLock<BTreeMap<u64, String>>,
    /// Per-chunk size in bytes (drives total-bytes accounting).
    sizes: dashmap::DashMap<String, u64>,
    /// Aggregate bytes stored. AtomicU64 because put/evict can run from
    /// different tasks.
    total_bytes: AtomicU64,
}

impl LocalChunkCache {
    /// Initialise a new cache rooted at `dir`. Scans existing files on disk
    /// to seed the LRU index so a restart doesn't lose accounting of
    /// already-cached chunks.
    pub async fn new(
        dir: PathBuf,
        max_bytes: u64,
        max_age: Option<Duration>,
    ) -> anyhow::Result<Self> {
        tfs::create_dir_all(&dir).await?;
        let cache = Self {
            dir,
            max_bytes,
            max_age,
            lru: RwLock::new(BTreeMap::new()),
            sizes: dashmap::DashMap::new(),
            total_bytes: AtomicU64::new(0),
        };
        cache.reindex_from_disk().await?;
        Ok(cache)
    }

    /// Walk the cache directory and rebuild the in-memory index. Called on
    /// startup so the size/age accounting is correct after a restart.
    async fn reindex_from_disk(&self) -> anyhow::Result<()> {
        let mut total: u64 = 0;
        let mut lru = self.lru.write();
        let mut dirs1 = match tfs::read_dir(&self.dir).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        while let Some(d1) = dirs1.next_entry().await? {
            if !d1.file_type().await?.is_dir() {
                continue;
            }
            let mut dirs2 = tfs::read_dir(d1.path()).await?;
            while let Some(d2) = dirs2.next_entry().await? {
                if !d2.file_type().await?.is_dir() {
                    continue;
                }
                let mut entries = tfs::read_dir(d2.path()).await?;
                while let Some(e) = entries.next_entry().await? {
                    let path = e.path();
                    if path.extension().map_or(false, |x| x == "dat") {
                        let meta = e.metadata().await?;
                        let size = meta.len();
                        let mtime_ms = meta
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                            let id = stem.to_string();
                            self.sizes.insert(id.clone(), size);
                            lru.insert(mtime_ms.saturating_mul(1000).saturating_add(0), id);
                            total += size;
                        }
                    }
                }
            }
        }
        self.total_bytes.store(total, Ordering::Relaxed);
        tracing::info!(
            count = self.sizes.len(),
            bytes = total,
            "local cache reindexed"
        );
        Ok(())
    }

    fn path_for(&self, chunk_id: &str) -> PathBuf {
        let (aa, bb) = if chunk_id.len() >= 4 {
            (&chunk_id[..2], &chunk_id[2..4])
        } else {
            ("00", "00")
        };
        self.dir.join(aa).join(bb).join(format!("{chunk_id}.dat"))
    }

    /// Retrieve a cached chunk WITHOUT updating its LRU position.
    /// Use this for scanner-style accesses (header reads, stat-walks).
    ///
    /// Note: `tokio::fs::read()` triggers an OS-level atime update on
    /// most filesystems. We capture the pre-read atime + restore it
    /// after, so scanner reads truly leave the LRU position alone.
    pub async fn peek(&self, chunk_id: &str) -> Option<Bytes> {
        let path = self.path_for(chunk_id);
        if !path.exists() {
            return None;
        }
        let pre = tfs::metadata(&path).await.ok().and_then(|m| m.accessed().ok());
        let data = match tfs::read(&path).await {
            Ok(d) => d,
            Err(_) => return None,
        };
        if let Some(t) = pre {
            // Restore the pre-read atime so the OS read-side update
            // doesn't leak into LRU ordering.
            let _ = filetime::set_file_atime(&path, filetime::FileTime::from_system_time(t));
        }
        Some(Bytes::from(data))
    }

    /// Retrieve a cached chunk and (conditionally) refresh its atime so
    /// the size-based LRU eviction sees it as recently used.
    ///
    /// `bytes_hint` is the byte count the caller intends to consume from
    /// this fetch. If `bytes_hint >= ATIME_UPDATE_MIN_BYTES` we treat the
    /// read as a "view" event and bump atime to now. Otherwise it's a
    /// scanner read and we explicitly RESTORE the pre-read atime — the
    /// OS auto-updates atime on read() so without the restore even
    /// scanner reads would refresh LRU position and defeat the design.
    pub async fn get(&self, chunk_id: &str, bytes_hint: usize) -> Option<Bytes> {
        let path = self.path_for(chunk_id);
        if !path.exists() {
            return None;
        }
        let pre = tfs::metadata(&path).await.ok().and_then(|m| m.accessed().ok());
        let data = match tfs::read(&path).await {
            Ok(d) => d,
            Err(_) => return None,
        };
        if bytes_hint >= ATIME_UPDATE_MIN_BYTES {
            // Viewer read — set atime to now.
            let now = SystemTime::now();
            let _ = filetime::set_file_atime(
                &path,
                filetime::FileTime::from_system_time(now),
            );
        } else if let Some(t) = pre {
            // Scanner read — restore pre-read atime so LRU position
            // stays anchored to last VIEWER access.
            let _ = filetime::set_file_atime(&path, filetime::FileTime::from_system_time(t));
        }
        Some(Bytes::from(data))
    }

    /// Insert a chunk into the cache. Atomic via tmp+rename. Triggers
    /// size-based eviction if we exceed `max_bytes`.
    pub async fn put(&self, chunk_id: &str, data: &[u8]) -> anyhow::Result<()> {
        let final_path = self.path_for(chunk_id);
        if let Some(parent) = final_path.parent() {
            tfs::create_dir_all(parent).await?;
        }
        let tmp_path = final_path.with_extension("partial");

        {
            let mut f = tfs::File::create(&tmp_path).await?;
            f.write_all(data).await?;
            f.sync_all().await?;
        }
        tfs::rename(&tmp_path, &final_path).await?;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let len = data.len() as u64;

        // Update accounting.
        let prev = self.sizes.insert(chunk_id.to_string(), len).unwrap_or(0);
        if prev != len {
            self.total_bytes.fetch_add(len, Ordering::Relaxed);
            self.total_bytes.fetch_sub(prev, Ordering::Relaxed);
        }
        {
            let mut lru = self.lru.write();
            // Use mtime_ms * 1000 + small jitter to avoid collisions when
            // multiple chunks land in the same ms.
            let key = now_ms.saturating_mul(1000).saturating_add(
                xxhash_rust::xxh3::xxh3_64(chunk_id.as_bytes()) % 1000,
            );
            lru.insert(key, chunk_id.to_string());
        }

        self.evict_to_size().await;
        Ok(())
    }

    /// Evict entries until total_bytes ≤ max_bytes, oldest atime first.
    ///
    /// We deliberately query each candidate's actual file atime rather
    /// than rely on the in-memory `lru` BTreeMap's insertion order:
    /// atime is what `get()` updates on viewer reads (and intentionally
    /// does NOT update on scanner reads), so the on-disk atime is the
    /// authoritative source of truth for "most recently viewed". This
    /// makes LRU robust to scanner storms — a Plex library walk doesn't
    /// move recently-viewed chunks toward eviction.
    async fn evict_to_size(&self) {
        while self.total_bytes.load(Ordering::Relaxed) > self.max_bytes {
            // Build a snapshot of (atime, chunk_id) for everything we
            // know about, pick the oldest, and drop it.
            let ids: Vec<String> = self.sizes.iter().map(|e| e.key().clone()).collect();
            if ids.is_empty() {
                break;
            }
            let mut oldest: Option<(SystemTime, String, u64)> = None;
            for id in &ids {
                let path = self.path_for(id);
                let meta = match tfs::metadata(&path).await {
                    Ok(m) => m,
                    Err(_) => {
                        // File missing under our tracking; drop from index.
                        self.sizes.remove(id);
                        continue;
                    }
                };
                let atime = meta.accessed().unwrap_or(UNIX_EPOCH);
                let len = meta.len();
                if oldest.as_ref().map_or(true, |(ta, _, _)| atime < *ta) {
                    oldest = Some((atime, id.clone(), len));
                }
            }
            let (_, chunk_id, len) = match oldest {
                Some(v) => v,
                None => break,
            };
            let path = self.path_for(&chunk_id);
            if tfs::remove_file(&path).await.is_ok() {
                self.total_bytes.fetch_sub(len, Ordering::Relaxed);
                self.sizes.remove(&chunk_id);
                // Also drop any matching entry from the in-memory `lru`
                // map (best-effort housekeeping; the map is no longer
                // authoritative but we keep it tidy).
                let mut lru = self.lru.write();
                let stale: Vec<u64> = lru
                    .iter()
                    .filter(|(_, v)| **v == chunk_id)
                    .map(|(k, _)| *k)
                    .collect();
                for k in stale {
                    lru.remove(&k);
                }
            } else {
                // Couldn't remove; bail to avoid an infinite loop.
                break;
            }
        }
    }

    /// Background pruner: walk the cache and remove any entry whose `atime`
    /// is older than `max_age`. Idempotent; safe to call concurrently with
    /// `put`/`get`.
    pub async fn prune_by_age(&self) -> anyhow::Result<u64> {
        let max_age = match self.max_age {
            Some(d) => d,
            None => return Ok(0),
        };
        let now = SystemTime::now();
        let mut removed: u64 = 0;
        let ids: Vec<String> = self.sizes.iter().map(|e| e.key().clone()).collect();
        for id in ids {
            let path = self.path_for(&id);
            let meta = match tfs::metadata(&path).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let atime = meta.accessed().unwrap_or_else(|_| now);
            let age = now.duration_since(atime).unwrap_or_default();
            if age > max_age {
                let len = meta.len();
                if tfs::remove_file(&path).await.is_ok() {
                    self.sizes.remove(&id);
                    self.total_bytes.fetch_sub(len, Ordering::Relaxed);
                    removed += 1;
                }
            }
        }
        if removed > 0 {
            tracing::info!(
                removed,
                max_age_secs = max_age.as_secs(),
                "local cache age-pruner removed entries"
            );
        }
        Ok(removed)
    }

    pub fn current_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    pub fn entry_count(&self) -> usize {
        self.sizes.len()
    }
}

/// Spawn a background task that periodically prunes age-expired entries.
/// Returns the JoinHandle (caller can drop to detach).
pub fn spawn_pruner(
    cache: std::sync::Arc<LocalChunkCache>,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip the first immediate tick.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(e) = cache.prune_by_age().await {
                tracing::warn!("local cache prune failed: {e}");
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn chunk_id(n: u32) -> String {
        format!("{:064x}", n as u128)
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10 * 1024 * 1024, None)
            .await
            .unwrap();
        let id = chunk_id(1);
        let data = vec![42u8; 4096];
        cache.put(&id, &data).await.unwrap();
        let got = cache.get(&id, data.len()).await.unwrap();
        assert_eq!(got.as_ref(), &data[..]);
        assert_eq!(cache.current_bytes(), 4096);
        assert_eq!(cache.entry_count(), 1);
    }

    /// Regression: scanner-style reads (small bytes_hint) MUST NOT
    /// refresh the cached chunk's atime — otherwise a Plex library walk
    /// would make every chunk look recently accessed and the size LRU
    /// would evict the wrong things.
    #[tokio::test]
    async fn scanner_read_does_not_update_atime() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(
            tmp.path().to_path_buf(),
            10 * 1024 * 1024,
            None,
        )
        .await
        .unwrap();
        let id = chunk_id(1);
        cache.put(&id, &vec![1u8; 4096]).await.unwrap();

        // Read the atime right after put.
        let path = cache.path_for(&id);
        let atime_before = tfs::metadata(&path)
            .await
            .unwrap()
            .accessed()
            .unwrap();

        // Wait so the comparison is meaningful, then do a "scanner" read.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = cache
            .get(&id, /* bytes_hint */ 64 * 1024)
            .await
            .expect("scanner read still returns data");

        let atime_after = tfs::metadata(&path)
            .await
            .unwrap()
            .accessed()
            .unwrap();
        assert_eq!(
            atime_before, atime_after,
            "scanner-sized read (< ATIME_UPDATE_MIN_BYTES) must not bump atime"
        );
    }

    /// Viewer-style reads (>= ATIME_UPDATE_MIN_BYTES) DO refresh atime so
    /// the chunk stays at the top of the size-LRU.
    #[tokio::test]
    async fn viewer_read_updates_atime() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(
            tmp.path().to_path_buf(),
            10 * 1024 * 1024,
            None,
        )
        .await
        .unwrap();
        let id = chunk_id(1);
        cache.put(&id, &vec![1u8; 4096]).await.unwrap();

        let path = cache.path_for(&id);
        let atime_before = tfs::metadata(&path).await.unwrap().accessed().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let _ = cache
            .get(&id, /* bytes_hint */ ATIME_UPDATE_MIN_BYTES + 1)
            .await
            .expect("viewer read returns data");

        let atime_after = tfs::metadata(&path).await.unwrap().accessed().unwrap();
        assert!(
            atime_after > atime_before,
            "viewer-sized read should bump atime"
        );
    }

    /// peek() never updates atime, regardless of bytes_hint.
    #[tokio::test]
    async fn peek_never_updates_atime() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(
            tmp.path().to_path_buf(),
            10 * 1024 * 1024,
            None,
        )
        .await
        .unwrap();
        let id = chunk_id(1);
        cache.put(&id, &vec![1u8; 4096]).await.unwrap();
        let path = cache.path_for(&id);
        let atime_before = tfs::metadata(&path).await.unwrap().accessed().unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = cache.peek(&id).await.unwrap();
        let atime_after = tfs::metadata(&path).await.unwrap().accessed().unwrap();
        assert_eq!(atime_before, atime_after);
    }

    /// Size-eviction LRU uses atime, not insertion order. After a scanner
    /// reads a chunk, it must still be the eviction candidate if it was
    /// the oldest VIEWER access — i.e. scanner reads on chunk-1 don't
    /// save it from eviction when chunk-2/3/4 are real viewer reads.
    #[tokio::test]
    async fn size_eviction_uses_viewer_atime_not_scanner() {
        let tmp = tempfile::tempdir().unwrap();
        // Cap at 10 KB — first put fills it. Fifth put evicts oldest.
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10_000, None)
            .await
            .unwrap();

        cache.put(&chunk_id(1), &vec![1u8; 2500]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        cache.put(&chunk_id(2), &vec![2u8; 2500]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        cache.put(&chunk_id(3), &vec![3u8; 2500]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        cache.put(&chunk_id(4), &vec![4u8; 2500]).await.unwrap();

        // Scanner reads chunk 1 (smallest read). atime stays old.
        let _ = cache.get(&chunk_id(1), 32 * 1024).await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        // Real viewer reads chunk 2 (refreshes atime).
        let _ = cache
            .get(&chunk_id(2), ATIME_UPDATE_MIN_BYTES + 1)
            .await
            .unwrap();

        // Now put chunk 5 → eviction must drop the OLDEST viewer atime,
        // which is chunk 1 (despite the scanner having "touched" it just
        // before the viewer read of chunk 2).
        cache.put(&chunk_id(5), &vec![5u8; 2500]).await.unwrap();

        assert!(cache.current_bytes() <= 10_000);
        // The viewer-warm chunk 2 must survive; the scanner-only chunk 1
        // must be the one evicted.
        assert!(cache.peek(&chunk_id(2)).await.is_some(), "viewer-warm chunk must stay");
        assert!(cache.peek(&chunk_id(1)).await.is_none(), "scanner-only chunk should be evicted");
    }

    #[tokio::test]
    async fn size_eviction() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10_000, None)
            .await
            .unwrap();
        // Put 5 × 3 KB = 15 KB total → ought to evict at least 2 oldest.
        for i in 0..5 {
            let data = vec![i as u8; 3000];
            cache.put(&chunk_id(i), &data).await.unwrap();
        }
        assert!(cache.current_bytes() <= 10_000);
        assert!(cache.entry_count() < 5);
    }

    #[tokio::test]
    async fn age_eviction() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = Arc::new(
            LocalChunkCache::new(
                tmp.path().to_path_buf(),
                10 * 1024 * 1024,
                Some(Duration::from_millis(50)),
            )
            .await
            .unwrap(),
        );
        cache.put(&chunk_id(1), b"hello").await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let removed = cache.prune_by_age().await.unwrap();
        assert_eq!(removed, 1);
        assert_eq!(cache.entry_count(), 0);
    }

    #[tokio::test]
    async fn reindex_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 1 << 20, None)
                .await
                .unwrap();
            cache.put(&chunk_id(7), &vec![1u8; 2048]).await.unwrap();
            cache.put(&chunk_id(8), &vec![2u8; 4096]).await.unwrap();
            assert_eq!(cache.entry_count(), 2);
        }
        // Re-open: should rediscover.
        let cache2 = LocalChunkCache::new(tmp.path().to_path_buf(), 1 << 20, None)
            .await
            .unwrap();
        assert_eq!(cache2.entry_count(), 2);
        assert!(cache2.current_bytes() >= 6144);
    }
}
