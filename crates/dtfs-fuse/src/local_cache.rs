// P0-11: disk-backed FUSE chunk cache with size + age eviction.
//
// Sits between the in-memory readahead and the network fetch. Chunks are
// stored in `<dir>/aa/bb/<chunk_id>.dat` (mirroring the data-node sharding)
// and an in-memory index tracks total size + per-entry mtime so eviction
// is O(log N) by either dimension.
//
// Atomic writes: a torn write would poison the cache, so each entry is
// written to `<...>.partial`, fsynced, and renamed. Reads only look at
// `.dat` files so partial writes are never observed.
//
// Eviction:
//   * Size: when total bytes exceeds `max_bytes`, drop entries by oldest
//     mtime until under threshold.
//   * Age: a background pruner walks the cache periodically and deletes
//     entries whose `atime` is older than `max_age` (None = no TTL).
//
// Atime tracking: on every cache hit we set the file's atime via
// `filetime::set_file_atime`. The age pruner reads each file's atime to
// decide eviction so chunks that are actively being read are NEVER evicted
// even if their original write time is past max_age.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use parking_lot::RwLock;
use tokio::fs as tfs;
use tokio::io::AsyncWriteExt;

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

    /// Retrieve a cached chunk. Updates the file's atime so the age-pruner
    /// keeps actively-read chunks even past the TTL.
    pub async fn get(&self, chunk_id: &str) -> Option<Bytes> {
        let path = self.path_for(chunk_id);
        if !path.exists() {
            return None;
        }
        let data = match tfs::read(&path).await {
            Ok(d) => d,
            Err(_) => return None,
        };
        // Update atime on success.
        let now = SystemTime::now();
        let _ = filetime::set_file_atime(&path, filetime::FileTime::from_system_time(now));
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

    /// Evict entries (oldest first) until total_bytes ≤ max_bytes.
    async fn evict_to_size(&self) {
        while self.total_bytes.load(Ordering::Relaxed) > self.max_bytes {
            // Pop the oldest entry.
            let (key, chunk_id) = {
                let mut lru = self.lru.write();
                match lru.pop_first() {
                    Some((k, v)) => (k, v),
                    None => break,
                }
            };
            let _ = key;
            let path = self.path_for(&chunk_id);
            if let Ok(meta) = tfs::metadata(&path).await {
                let _ = tfs::remove_file(&path).await;
                self.total_bytes.fetch_sub(meta.len(), Ordering::Relaxed);
                self.sizes.remove(&chunk_id);
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
        let got = cache.get(&id).await.unwrap();
        assert_eq!(got.as_ref(), &data[..]);
        assert_eq!(cache.current_bytes(), 4096);
        assert_eq!(cache.entry_count(), 1);
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
