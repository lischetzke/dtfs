// Configurable read-ahead engine — Phase 2 (FUSE Client).
//
// Prefetches upcoming chunks in the background based on detected access
// patterns (sequential file reads, folder-sequential reads, etc.). Cached
// data is served from memory on subsequent reads, eliminating round-trips
// to data nodes.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use tokio::task::JoinHandle;
use tracing::{debug, instrument, warn};

use dtfs_client::client::DtfsClient;

/// Read-ahead mode determines the prefetch strategy.
#[derive(Debug, Clone)]
pub enum ReadAheadMode {
    /// Prefetch the next N chunks within the same file.
    FileSequential {
        prefetch_chunks: u32,
    },
    /// Prefetch the next N chunks in this file AND the first chunks of the
    /// next M files in the same directory (sorted lexicographically).
    FolderSequential {
        prefetch_chunks: u32,
        prefetch_next_files: u32,
    },
    /// Prefetch only within the current file, no cross-file prefetching.
    FileOnly {
        prefetch_chunks: u32,
    },
    /// Read-ahead disabled.
    Disabled,
}

impl ReadAheadMode {
    /// Parse a mode string from configuration into a `ReadAheadMode`.
    pub fn from_config(mode: &str, prefetch_chunks: u32, prefetch_next_files: u32) -> Self {
        match mode {
            "file-sequential" => ReadAheadMode::FileSequential { prefetch_chunks },
            "folder-sequential" => ReadAheadMode::FolderSequential {
                prefetch_chunks,
                prefetch_next_files,
            },
            "file-only" => ReadAheadMode::FileOnly { prefetch_chunks },
            "disabled" => ReadAheadMode::Disabled,
            other => {
                warn!(mode = other, "unknown readahead mode, defaulting to folder-sequential");
                ReadAheadMode::FolderSequential {
                    prefetch_chunks,
                    prefetch_next_files,
                }
            }
        }
    }

    /// Number of chunks to prefetch ahead of the current read position.
    fn prefetch_chunks(&self) -> u32 {
        match self {
            ReadAheadMode::FileSequential { prefetch_chunks } => *prefetch_chunks,
            ReadAheadMode::FolderSequential { prefetch_chunks, .. } => *prefetch_chunks,
            ReadAheadMode::FileOnly { prefetch_chunks } => *prefetch_chunks,
            ReadAheadMode::Disabled => 0,
        }
    }
}

/// Key for cached chunk data: "node_addr|chunk_id".
fn cache_key(node_addr: &str, chunk_id: &str) -> String {
    format!("{node_addr}|{chunk_id}")
}

/// Background read-ahead engine.
///
/// When a chunk is read, the engine inspects the access pattern and
/// speculatively fetches upcoming chunks in background tokio tasks. Fetched
/// data is stored in an in-memory `DashMap` cache and can be retrieved
/// with [`get_cached`](Self::get_cached).
pub struct ReadAheadEngine {
    mode: ReadAheadMode,
    client: DtfsClient,
    /// Currently pending prefetch tasks keyed by cache key.
    pending: Arc<DashMap<String, JoinHandle<()>>>,
    /// Cached prefetched chunk data keyed by cache key.
    cache: Arc<DashMap<String, Bytes>>,
    /// Bug CE — running total of bytes held in `cache`, kept in sync with
    /// every insert/evict so the cache is bounded by SIZE, not entry count.
    /// A whole chunk can be up to 128 MB; the old 256-entry count bound x
    /// whole-chunk values OOM-killed the 1 GB FUSE client on a full-file read.
    cache_bytes: Arc<std::sync::atomic::AtomicUsize>,
    /// Soft byte budget for `cache`. Inserts evict arbitrary entries until
    /// the total fits (best-effort; never rejects a read — § A.5).
    ///
    /// CF-5 — runtime-adjustable (was a fixed `usize`) so the memory-pressure
    /// ladder can halve it at the Warn level and restore it on recovery. An
    /// `Arc<AtomicUsize>` so the cheap `ReadAheadCacheController` handle can
    /// shrink it without owning the engine (the engine is moved into the
    /// fuse adapter at mount time).
    max_cache_bytes: Arc<AtomicUsize>,
    /// P0-11 / "next episode survives" — when set, prefetched chunks are
    /// also mirrored into the persistent disk cache so they survive
    /// in-memory eviction, FUSE restart, and scanner storms. Setting an
    /// initial atime on the disk-cache entry keeps the next-episode
    /// chunks near the top of the size-LRU.
    local_cache: parking_lot::RwLock<Option<Arc<crate::local_cache::LocalChunkCache>>>,
}

impl ReadAheadEngine {
    /// Create a new read-ahead engine. `max_cache_bytes` is the soft byte
    /// budget for the in-memory prefetch cache (Bug CE).
    pub fn new(mode: ReadAheadMode, client: DtfsClient, max_cache_bytes: usize) -> Self {
        Self {
            mode,
            client,
            pending: Arc::new(DashMap::new()),
            cache: Arc::new(DashMap::new()),
            cache_bytes: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            max_cache_bytes: Arc::new(AtomicUsize::new(max_cache_bytes)),
            local_cache: parking_lot::RwLock::new(None),
        }
    }

    /// CF-5 — a cheap clonable handle that lets a background task drive the
    /// memory-pressure ladder (shrink the byte budget, drop unconsumed
    /// prefetch) without owning the engine, which is moved into the fuse
    /// adapter at mount time. Captures the Arcs the engine already shares
    /// internally plus the operator-configured budget so Normal can restore
    /// the full size.
    pub fn cache_controller(&self) -> ReadAheadCacheController {
        ReadAheadCacheController {
            cache: Arc::clone(&self.cache),
            cache_bytes: Arc::clone(&self.cache_bytes),
            pending: Arc::clone(&self.pending),
            max_cache_bytes: Arc::clone(&self.max_cache_bytes),
            configured_budget: self.max_cache_bytes.load(Ordering::Relaxed),
        }
    }

    /// Bug CE — insert into the byte-bounded cache, evicting arbitrary other
    /// entries until the running byte total fits `max_cache_bytes`. Associated
    /// fn (not `&self`) so the prefetch task can call it with cloned `Arc`s.
    /// Keeps at least the just-stored entry; best-effort, never rejects.
    fn store_in_cache(
        cache: &DashMap<String, Bytes>,
        cache_bytes: &std::sync::atomic::AtomicUsize,
        max_cache_bytes: usize,
        key: String,
        data: Bytes,
    ) {
        use std::sync::atomic::Ordering;
        let incoming = data.len();
        if let Some(old) = cache.insert(key.clone(), data) {
            cache_bytes.fetch_sub(old.len(), Ordering::Relaxed);
        }
        cache_bytes.fetch_add(incoming, Ordering::Relaxed);
        // Evict until under budget, never dropping the entry just stored.
        while cache_bytes.load(Ordering::Relaxed) > max_cache_bytes {
            let victim = cache
                .iter()
                .find(|e| e.key() != &key)
                .map(|e| e.key().clone());
            match victim {
                Some(k) => {
                    if let Some((_, v)) = cache.remove(&k) {
                        cache_bytes.fetch_sub(v.len(), Ordering::Relaxed);
                    }
                }
                None => break, // only the fresh entry remains
            }
        }
    }

    /// Bytes currently held in the prefetch cache (Bug CE accounting).
    pub fn cache_bytes(&self) -> usize {
        self.cache_bytes.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Attach a disk-backed cache. Prefetched chunks will be mirrored to
    /// it after a successful fetch.
    pub fn set_local_cache(&self, cache: Arc<crate::local_cache::LocalChunkCache>) {
        *self.local_cache.write() = Some(cache);
    }

    /// Called when a chunk is read. Triggers background prefetch of upcoming
    /// chunks based on the configured read-ahead mode.
    ///
    /// # Parameters
    /// - `file_path`: path of the file being read
    /// - `chunk_index`: zero-based index of the chunk just read
    /// - `total_chunks`: total number of chunks in the file
    /// - `chunk_locations`: list of `(chunk_id, node_addresses)` for all chunks
    ///   in the file, ordered by chunk index
    #[instrument(skip(self, chunk_locations), fields(path = %file_path, chunk = chunk_index, total = total_chunks))]
    pub fn on_chunk_read(
        &self,
        file_path: &str,
        chunk_index: u32,
        total_chunks: u32,
        chunk_locations: &[(String, Vec<String>)],
    ) {
        let n = self.mode.prefetch_chunks();
        if n == 0 {
            return;
        }

        // Prefetch the next `n` chunks after the one just read.
        let start = chunk_index + 1;
        let end = total_chunks.min(start + n);

        for idx in start..end {
            if let Some((chunk_id, node_addrs)) = chunk_locations.get(idx as usize) {
                if node_addrs.is_empty() {
                    continue;
                }
                // Pick the first available node address (location-aware
                // selection happens at a higher level).
                let addr = &node_addrs[0];
                let key = cache_key(addr, chunk_id);

                // Skip if already cached or being fetched.
                if self.cache.contains_key(&key) || self.pending.contains_key(&key) {
                    continue;
                }

                self.spawn_prefetch(key, addr.clone(), chunk_id.clone());
            }
        }

        debug!(
            file_path,
            chunk_index,
            prefetch_start = start,
            prefetch_end = end,
            "readahead triggered"
        );
    }

    /// Get a previously prefetched chunk from the cache.
    ///
    /// Returns `Some(data)` if the chunk was prefetched and is available,
    /// or `None` if it has not been fetched yet.
    pub fn get_cached(&self, node_addr: &str, chunk_id: &str) -> Option<Bytes> {
        let key = cache_key(node_addr, chunk_id);
        self.cache.get(&key).map(|entry| entry.value().clone())
    }

    /// Insert a chunk that the foreground read path just fetched. Without
    /// this, every FUSE read of a fresh chunk (128 KB at a time across a
    /// 128 MB chunk) re-issues a full chunk fetch — observed ~1.2 MB/s.
    /// Storing the chunk here lets the next FUSE read hit the cache.
    pub fn insert_cached(&self, node_addr: &str, chunk_id: &str, data: Bytes) {
        let key = cache_key(node_addr, chunk_id);
        Self::store_in_cache(
            &self.cache,
            &self.cache_bytes,
            self.max_cache_bytes.load(Ordering::Relaxed),
            key,
            data,
        );
    }

    /// Remove a cached entry (e.g., after it has been consumed).
    pub fn evict(&self, node_addr: &str, chunk_id: &str) {
        let key = cache_key(node_addr, chunk_id);
        if let Some((_, v)) = self.cache.remove(&key) {
            self.cache_bytes
                .fetch_sub(v.len(), std::sync::atomic::Ordering::Relaxed);
        }
        // Also cancel pending task if any.
        if let Some((_, handle)) = self.pending.remove(&key) {
            handle.abort();
        }
    }

    /// Cancel all pending prefetch tasks and clear the cache.
    pub fn clear(&self) {
        for entry in self.pending.iter() {
            entry.value().abort();
        }
        self.pending.clear();
        self.cache.clear();
        self.cache_bytes
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Number of entries currently in the prefetch cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Spawn a background task to fetch a single chunk.
    fn spawn_prefetch(&self, key: String, node_addr: String, chunk_id: String) {
        // Bug CE — skip prefetch when the byte budget is already exhausted.
        if self.cache_bytes() >= self.max_cache_bytes.load(Ordering::Relaxed) {
            debug!(key, "readahead cache full (bytes), skipping prefetch");
            return;
        }

        let client = self.client.clone();
        let cache = Arc::clone(&self.cache);
        let cache_bytes = Arc::clone(&self.cache_bytes);
        let max_cache_bytes = self.max_cache_bytes.load(Ordering::Relaxed);
        let pending_key = key.clone();
        let pending_ref = Arc::clone(&self.pending);
        let local_cache = self.local_cache.read().clone();

        let handle = tokio::spawn(async move {
            match client.read_chunk(&node_addr, &chunk_id).await {
                Ok(data) => {
                    debug!(
                        chunk_id = %chunk_id,
                        node = %node_addr,
                        size = data.len(),
                        "readahead prefetch complete"
                    );
                    // Bug CE — byte-bounded insert (was an unbounded
                    // `cache.insert`, the worst offender for the OOM).
                    Self::store_in_cache(
                        &cache,
                        &cache_bytes,
                        max_cache_bytes,
                        pending_key.clone(),
                        data.clone(),
                    );
                    // Mirror to disk cache so the prefetched chunk
                    // survives in-mem eviction + FUSE restart. Best-
                    // effort; disk-cache failures are logged, not fatal.
                    if let Some(lc) = local_cache {
                        if let Err(e) = lc.put(&chunk_id, &data).await {
                            debug!(
                                chunk_id = %chunk_id,
                                error = %e,
                                "readahead disk-cache mirror failed (non-fatal)"
                            );
                        }
                    }
                }
                Err(e) => {
                    // Prefetch failures are non-fatal — the read path will
                    // fetch on demand.
                    debug!(
                        chunk_id = %chunk_id,
                        node = %node_addr,
                        error = %e,
                        "readahead prefetch failed (non-fatal)"
                    );
                }
            }
            // Remove from pending set.
            pending_ref.remove(&pending_key);
        });

        self.pending.insert(key, handle);
    }
}

impl Drop for ReadAheadEngine {
    fn drop(&mut self) {
        // Abort all pending tasks on drop.
        for entry in self.pending.iter() {
            entry.value().abort();
        }
    }
}

/// CF-5 — cheap clonable handle to the readahead engine's cache, used by the
/// FUSE memory-pressure ladder. The engine is moved into the fuse adapter at
/// mount time, so a background sampler task can't hold `&ReadAheadEngine`;
/// this captures the same internal `Arc`s and drives the two ladder rungs:
///
///  * Warn → `shrink_to_bytes(configured / 2)`: halve the live byte budget
///    and evict down to it. Prefetch still runs, just bounded smaller.
///  * Shed → `drop_unconsumed()`: cancel pending prefetch + clear the cache.
///  * Normal → `shrink_to_bytes(configured)`: restore the operator budget.
#[derive(Clone)]
pub struct ReadAheadCacheController {
    cache: Arc<DashMap<String, Bytes>>,
    cache_bytes: Arc<AtomicUsize>,
    pending: Arc<DashMap<String, JoinHandle<()>>>,
    max_cache_bytes: Arc<AtomicUsize>,
    /// The operator-configured budget (Bug CE `cache_mb`), captured at
    /// construction so `Normal` can restore it after pressure clears.
    configured_budget: usize,
}

impl ReadAheadCacheController {
    /// The operator-configured byte budget (Bug CE `cache_mb`).
    pub fn configured_budget(&self) -> usize {
        self.configured_budget
    }

    /// Current live byte budget.
    pub fn current_budget(&self) -> usize {
        self.max_cache_bytes.load(Ordering::Relaxed)
    }

    /// Bytes currently held in the prefetch cache.
    pub fn cache_bytes(&self) -> usize {
        self.cache_bytes.load(Ordering::Relaxed)
    }

    /// Set the live byte budget to `target` and evict arbitrary entries until
    /// the cache fits. Best-effort, never rejects a read (§ A.5). Returns the
    /// number of entries evicted.
    pub fn shrink_to_bytes(&self, target: usize) -> usize {
        self.max_cache_bytes.store(target, Ordering::Relaxed);
        let mut evicted = 0usize;
        while self.cache_bytes.load(Ordering::Relaxed) > target {
            let victim = self.cache.iter().next().map(|e| e.key().clone());
            match victim {
                Some(k) => {
                    if let Some((_, v)) = self.cache.remove(&k) {
                        self.cache_bytes.fetch_sub(v.len(), Ordering::Relaxed);
                        evicted += 1;
                    }
                }
                None => break,
            }
        }
        evicted
    }

    /// Shed rung — cancel every pending prefetch and clear the in-memory
    /// cache. The disk-backed local cache (P0-11) is untouched, so warm
    /// chunks still survive; only the volatile RAM cache is dropped.
    pub fn drop_unconsumed(&self) {
        for entry in self.pending.iter() {
            entry.value().abort();
        }
        self.pending.clear();
        self.cache.clear();
        self.cache_bytes.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_readahead_mode_from_config() {
        let mode = ReadAheadMode::from_config("disabled", 8, 3);
        assert!(matches!(mode, ReadAheadMode::Disabled));
        assert_eq!(mode.prefetch_chunks(), 0);

        let mode = ReadAheadMode::from_config("file-sequential", 4, 0);
        assert!(matches!(
            mode,
            ReadAheadMode::FileSequential { prefetch_chunks: 4 }
        ));

        let mode = ReadAheadMode::from_config("folder-sequential", 8, 3);
        assert!(matches!(
            mode,
            ReadAheadMode::FolderSequential {
                prefetch_chunks: 8,
                prefetch_next_files: 3,
            }
        ));

        let mode = ReadAheadMode::from_config("file-only", 2, 0);
        assert!(matches!(
            mode,
            ReadAheadMode::FileOnly { prefetch_chunks: 2 }
        ));

        // Unknown defaults to folder-sequential.
        let mode = ReadAheadMode::from_config("unknown", 8, 3);
        assert!(matches!(mode, ReadAheadMode::FolderSequential { .. }));
    }

    #[test]
    fn test_cache_key_format() {
        assert_eq!(
            cache_key("node1:9000", "abc123"),
            "node1:9000|abc123"
        );
    }

    /// Bug CE — the in-memory prefetch cache must be bounded by BYTES, not
    /// entry count. With a 4 MB budget, inserting eight 1 MB chunks must
    /// evict down to <= 4 MB (the old count bound kept all 8 MB, which on
    /// the 1 GB client with whole-chunk values OOM-killed FUSE).
    #[test]
    fn bug_ce_cache_is_byte_bounded() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 4 * 1024 * 1024);
        let one_mb = Bytes::from(vec![0u8; 1024 * 1024]);
        for i in 0..8 {
            engine.insert_cached("node:9000", &format!("chunk{i}"), one_mb.clone());
        }
        assert!(
            engine.cache_bytes() <= 4 * 1024 * 1024,
            "cache must stay within the 4 MB byte budget, got {} bytes",
            engine.cache_bytes()
        );
        assert!(
            engine.cache_bytes() > 0,
            "cache should retain the most-recent chunks, not drop everything"
        );
        // Byte accounting stays consistent with the entry count.
        assert!(
            engine.cache_len() <= 4,
            "at most four 1 MB chunks fit in a 4 MB budget, got {} entries",
            engine.cache_len()
        );
    }

    /// CF-5 — the controller's `shrink_to_bytes` halves the live budget and
    /// evicts down to it; `drop_unconsumed` clears the cache entirely. This
    /// is the FUSE memory-pressure ladder's Warn and Shed rungs.
    #[test]
    fn cf5_controller_shrink_and_drop() {
        let client = DtfsClient::new();
        let engine = ReadAheadEngine::new(ReadAheadMode::Disabled, client, 8 * 1024 * 1024);
        let one_mb = Bytes::from(vec![0u8; 1024 * 1024]);
        for i in 0..8 {
            engine.insert_cached("node:9000", &format!("chunk{i}"), one_mb.clone());
        }
        let ctrl = engine.cache_controller();
        assert_eq!(ctrl.configured_budget(), 8 * 1024 * 1024);
        assert!(ctrl.cache_bytes() > 4 * 1024 * 1024);

        // Warn rung — halve the budget; cache must evict down to <= 4 MB.
        let evicted = ctrl.shrink_to_bytes(4 * 1024 * 1024);
        assert!(evicted > 0, "halving the budget must evict entries");
        assert!(
            ctrl.cache_bytes() <= 4 * 1024 * 1024,
            "cache must fit the halved 4 MB budget, got {} bytes",
            ctrl.cache_bytes()
        );

        // Shed rung — drop everything.
        ctrl.drop_unconsumed();
        assert_eq!(ctrl.cache_bytes(), 0, "drop_unconsumed clears the cache");
        assert_eq!(engine.cache_len(), 0);

        // Normal rung — restore the operator budget.
        ctrl.shrink_to_bytes(ctrl.configured_budget());
        assert_eq!(ctrl.current_budget(), 8 * 1024 * 1024);
    }
}
