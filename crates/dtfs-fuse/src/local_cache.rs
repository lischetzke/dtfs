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
use dtfs_common::checksum::IncrementalHasher;
use dtfs_common::types::ChunkId;
use parking_lot::RwLock;
use tokio::fs as tfs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Reads below this byte count don't refresh the cached chunk's atime
/// (scanner/header-only access). Set to 1 MiB by default: a Plex library
/// scanner typically reads just the file header (a few hundred KB) while
/// a media playback session pulls megabytes of contiguous data.
pub const ATIME_UPDATE_MIN_BYTES: usize = 1024 * 1024;

/// REV-2-4: piece size for the verify-on-read hash. We hash straight out of
/// the destination buffer in slices of this size, so verification adds NO
/// extra whole-chunk (or even piece-sized) allocation — A.3.2 parity with the
/// data node's incremental read-serve hash.
const VERIFY_PIECE_SIZE: usize = 8 * 1024 * 1024;

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
    /// REV-3-12: number of full stat sweeps of the tracked entry set that
    /// `evict_to_size` has performed. One call must cost at most ONE sweep no
    /// matter how many victims it drops.
    eviction_sweeps: AtomicU64,
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
            eviction_sweeps: AtomicU64::new(0),
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

    /// REV-2-4 — read a cached chunk file and BLAKE3-verify it before it is
    /// handed to any reader.
    ///
    /// The data node BLAKE3s every chunk it serves (`verify_on_read`, A.3.3
    /// §6) precisely so bit rot never reaches the reader. This cache sits IN
    /// FRONT of the cluster, so without the same check a chunk that rotted in
    /// the client's own cache dir — or was truncated by a full disk, or left
    /// behind by a crashed writer — would be served as good data and the
    /// healthy replicas would never be consulted. Chunks are content-addressed,
    /// so the chunk id IS the expected hash; no extra metadata is needed.
    ///
    /// Hashing is incremental over the destination buffer (the one
    /// chunk-sized allocation we have to make anyway — no second copy, no
    /// scratch buffer), keeping A.3.2 parity. On mismatch the entry is evicted
    /// and `None` is returned so the caller treats it as a MISS and falls
    /// through to the cluster.
    async fn read_verified(&self, chunk_id: &str, path: &Path) -> Option<Bytes> {
        let expected = match ChunkId::from_hex(chunk_id) {
            Ok(c) => c.0,
            Err(e) => {
                tracing::warn!(
                    chunk_id,
                    error = %e,
                    "local cache: chunk id is not a content address; refusing to serve unverifiable cache entry"
                );
                return None;
            }
        };

        let mut f = tfs::File::open(path).await.ok()?;
        let total = f.metadata().await.ok()?.len() as usize;
        let mut data = vec![0u8; total];
        let mut hasher = IncrementalHasher::new();
        let mut off = 0usize;
        while off < total {
            let want = (total - off).min(VERIFY_PIECE_SIZE);
            if f.read_exact(&mut data[off..off + want]).await.is_err() {
                return None;
            }
            hasher.update(&data[off..off + want]);
            off += want;
        }

        if hasher.finalize() != expected {
            // Integrity fault, NOT an ordinary cache miss.
            tracing::error!(
                chunk_id,
                path = %path.display(),
                bytes = total,
                "local cache INTEGRITY FAULT: cached chunk failed BLAKE3 verify (bit rot / torn write); evicting entry and falling through to a cluster replica"
            );
            self.evict_entry(chunk_id).await;
            return None;
        }
        Some(Bytes::from(data))
    }

    /// Remove one entry from disk and from the size/LRU accounting.
    /// Returns false only when the file exists but could not be removed.
    async fn evict_entry(&self, chunk_id: &str) -> bool {
        let path = self.path_for(chunk_id);
        match tfs::remove_file(&path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return false,
        }
        if let Some((_, len)) = self.sizes.remove(chunk_id) {
            self.total_bytes.fetch_sub(len, Ordering::Relaxed);
        }
        let mut lru = self.lru.write();
        let stale: Vec<u64> = lru
            .iter()
            .filter(|(_, v)| v.as_str() == chunk_id)
            .map(|(k, _)| *k)
            .collect();
        for k in stale {
            lru.remove(&k);
        }
        true
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
        // REV-2-4 — verify before serving; a rotten entry is a MISS.
        let data = self.read_verified(chunk_id, &path).await?;
        if let Some(t) = pre {
            // Restore the pre-read atime so the OS read-side update
            // doesn't leak into LRU ordering.
            let _ = filetime::set_file_atime(&path, filetime::FileTime::from_system_time(t));
        }
        Some(data)
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
        // REV-2-4 — verify before serving; a rotten entry is a MISS.
        let data = self.read_verified(chunk_id, &path).await?;
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
        Some(data)
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
    ///
    /// REV-3-12: the atime snapshot is taken ONCE per call. The previous shape
    /// re-stat'ed every tracked entry for every victim, so a burst of
    /// insertions cost O(N*M) stat syscalls awaited inline on the FUSE read
    /// path (a visible stall mid-playback with a large cache). Policy is
    /// unchanged — same oldest-atime-first order, just computed in one sweep
    /// and then applied to as many victims as the cap needs.
    async fn evict_to_size(&self) {
        if self.total_bytes.load(Ordering::Relaxed) <= self.max_bytes {
            return;
        }
        self.eviction_sweeps.fetch_add(1, Ordering::Relaxed);

        // ONE sweep: stat every tracked entry exactly once.
        let ids: Vec<String> = self.sizes.iter().map(|e| e.key().clone()).collect();
        let mut candidates: Vec<(SystemTime, String)> = Vec::with_capacity(ids.len());
        for id in ids {
            match tfs::metadata(self.path_for(&id)).await {
                Ok(m) => candidates.push((m.accessed().unwrap_or(UNIX_EPOCH), id)),
                Err(_) => {
                    // Tracked but gone from disk: drop it from the index AND
                    // from the byte total, otherwise the cap could never be
                    // reached again.
                    if let Some((_, len)) = self.sizes.remove(&id) {
                        self.total_bytes.fetch_sub(len, Ordering::Relaxed);
                    }
                }
            }
        }
        candidates.sort_by(|a, b| a.0.cmp(&b.0));

        for (_, id) in candidates {
            if self.total_bytes.load(Ordering::Relaxed) <= self.max_bytes {
                break;
            }
            // A failed removal (permissions, busy) just leaves the entry in
            // place; move on to the next-oldest instead of spinning.
            let _ = self.evict_entry(&id).await;
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

    /// REV-3-12 observability: total full stat sweeps performed by
    /// `evict_to_size` since startup.
    pub(crate) fn eviction_sweeps(&self) -> u64 {
        self.eviction_sweeps.load(Ordering::Relaxed)
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

    /// Content-addressed id for `data` — a chunk id IS the BLAKE3 of the
    /// chunk bytes, which is what the cache verifies on read (REV-2-4).
    fn cid(data: &[u8]) -> String {
        dtfs_common::checksum::compute(data)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    /// REV-2-4: a cached chunk whose bytes rotted on the client's own disk
    /// MUST NOT be served. Chunks are content-addressed, so the id is the
    /// expected BLAKE3 — a mismatch is an integrity fault: evict the entry
    /// and report a MISS so the caller falls through to a healthy replica.
    #[tokio::test]
    async fn rev_2_4_corrupt_cached_chunk_is_evicted_and_returns_miss() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10 * 1024 * 1024, None)
            .await
            .unwrap();

        let good = vec![7u8; 4096];
        let good_id = cid(&good);
        cache.put(&good_id, &good).await.unwrap();

        let rotten = vec![9u8; 8192];
        let rotten_id = cid(&rotten);
        cache.put(&rotten_id, &rotten).await.unwrap();

        // Flip a single bit on disk, as bit rot / a torn write would.
        let path = cache.path_for(&rotten_id);
        let mut raw = std::fs::read(&path).unwrap();
        raw[1234] ^= 0xff;
        std::fs::write(&path, &raw).unwrap();

        assert!(
            cache.get(&rotten_id, rotten.len()).await.is_none(),
            "corrupt cache entry must report a MISS, never serve rotten bytes"
        );
        assert!(
            !path.exists(),
            "corrupt cache entry must be evicted from disk"
        );
        assert!(
            cache.peek(&rotten_id).await.is_none(),
            "evicted corrupt entry must stay gone"
        );

        // The healthy neighbour is untouched and still byte-exact.
        let got = cache.get(&good_id, good.len()).await.unwrap();
        assert_eq!(got.as_ref(), &good[..]);
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.current_bytes(), 4096);
    }

    /// REV-3-12: one `evict_to_size` call that must drop several victims
    /// performs exactly ONE stat sweep of the tracked set, not one sweep per
    /// victim (the O(N*M) stall on the FUSE read path).
    #[tokio::test]
    async fn rev_3_12_evict_to_size_sweeps_once_for_many_victims() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10_000, None)
            .await
            .unwrap();

        // Six 1500-byte entries = 9000 bytes, under the 10 KB cap so no
        // eviction happens yet. Distinct atimes, oldest first.
        let mut ids = Vec::new();
        for i in 0..6u8 {
            let data = vec![i; 1500];
            let id = cid(&data);
            cache.put(&id, &data).await.unwrap();
            ids.push(id);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(cache.entry_count(), 6);
        let sweeps_before = cache.eviction_sweeps();

        // One 5000-byte insert pushes the total to 14000 → three of the
        // oldest entries must go in this single call.
        let big = vec![200u8; 5000];
        let big_id = cid(&big);
        cache.put(&big_id, &big).await.unwrap();

        assert!(
            cache.current_bytes() <= 10_000,
            "eviction must bring the cache under the cap, got {}",
            cache.current_bytes()
        );
        assert_eq!(
            cache.eviction_sweeps() - sweeps_before,
            1,
            "evict_to_size must sweep once per call, not once per victim"
        );
        // LRU policy unchanged: the three oldest atimes went, the three
        // newest plus the fresh insert survived.
        for old in &ids[..3] {
            assert!(
                cache.peek(old).await.is_none(),
                "oldest-atime entry should have been evicted"
            );
        }
        for keep in &ids[3..] {
            assert!(
                cache.peek(keep).await.is_some(),
                "newer entry must survive eviction"
            );
        }
        assert!(
            cache.peek(&big_id).await.is_some(),
            "fresh insert must survive"
        );
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10 * 1024 * 1024, None)
            .await
            .unwrap();
        let data = vec![42u8; 4096];
        let id = cid(&data);
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
        let data = vec![1u8; 4096];
        let id = cid(&data);
        cache.put(&id, &data).await.unwrap();

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
        let data = vec![1u8; 4096];
        let id = cid(&data);
        cache.put(&id, &data).await.unwrap();

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
        let data = vec![1u8; 4096];
        let id = cid(&data);
        cache.put(&id, &data).await.unwrap();
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

        let ids: Vec<String> = (1..=5u8).map(|i| cid(&vec![i; 2500])).collect();
        for i in 0..4usize {
            cache
                .put(&ids[i], &vec![(i + 1) as u8; 2500])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Scanner reads chunk 1 (smallest read). atime stays old.
        let _ = cache.get(&ids[0], 32 * 1024).await.unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        // Real viewer reads chunk 2 (refreshes atime).
        let _ = cache
            .get(&ids[1], ATIME_UPDATE_MIN_BYTES + 1)
            .await
            .unwrap();

        // Now put chunk 5 → eviction must drop the OLDEST viewer atime,
        // which is chunk 1 (despite the scanner having "touched" it just
        // before the viewer read of chunk 2).
        cache.put(&ids[4], &vec![5u8; 2500]).await.unwrap();

        assert!(cache.current_bytes() <= 10_000);
        // The viewer-warm chunk 2 must survive; the scanner-only chunk 1
        // must be the one evicted.
        assert!(cache.peek(&ids[1]).await.is_some(), "viewer-warm chunk must stay");
        assert!(cache.peek(&ids[0]).await.is_none(), "scanner-only chunk should be evicted");
    }

    #[tokio::test]
    async fn size_eviction() {
        let tmp = tempfile::tempdir().unwrap();
        let cache = LocalChunkCache::new(tmp.path().to_path_buf(), 10_000, None)
            .await
            .unwrap();
        // Put 5 × 3 KB = 15 KB total → ought to evict at least 2 oldest.
        for i in 0..5u8 {
            let data = vec![i; 3000];
            cache.put(&cid(&data), &data).await.unwrap();
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
        cache.put(&cid(b"hello"), b"hello").await.unwrap();
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
            let a = vec![1u8; 2048];
            let b = vec![2u8; 4096];
            cache.put(&cid(&a), &a).await.unwrap();
            cache.put(&cid(&b), &b).await.unwrap();
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
