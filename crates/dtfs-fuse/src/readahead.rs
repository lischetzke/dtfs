// Configurable read-ahead engine — Phase 2 (FUSE Client).
//
// Prefetches upcoming chunks in the background based on detected access
// patterns (sequential file reads, folder-sequential reads, etc.). Cached
// data is served from memory on subsequent reads, eliminating round-trips
// to data nodes.

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
    /// Maximum number of cached entries before eviction starts.
    max_cache_entries: usize,
}

impl ReadAheadEngine {
    /// Create a new read-ahead engine.
    pub fn new(mode: ReadAheadMode, client: DtfsClient, max_cache_entries: usize) -> Self {
        Self {
            mode,
            client,
            pending: Arc::new(DashMap::new()),
            cache: Arc::new(DashMap::new()),
            max_cache_entries,
        }
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
        if self.cache.len() >= self.max_cache_entries {
            // Simple eviction: drop a single old entry to make room.
            if let Some(entry) = self.cache.iter().next() {
                let k = entry.key().clone();
                drop(entry);
                self.cache.remove(&k);
            }
        }
        let key = cache_key(node_addr, chunk_id);
        self.cache.insert(key, data);
    }

    /// Remove a cached entry (e.g., after it has been consumed).
    pub fn evict(&self, node_addr: &str, chunk_id: &str) {
        let key = cache_key(node_addr, chunk_id);
        self.cache.remove(&key);
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
    }

    /// Number of entries currently in the prefetch cache.
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Spawn a background task to fetch a single chunk.
    fn spawn_prefetch(&self, key: String, node_addr: String, chunk_id: String) {
        // Simple eviction: if cache is full, skip this prefetch.
        if self.cache.len() >= self.max_cache_entries {
            debug!(key, "readahead cache full, skipping prefetch");
            return;
        }

        let client = self.client.clone();
        let cache = Arc::clone(&self.cache);
        let pending_key = key.clone();
        let pending_ref = Arc::clone(&self.pending);

        let handle = tokio::spawn(async move {
            match client.read_chunk(&node_addr, &chunk_id).await {
                Ok(data) => {
                    debug!(
                        chunk_id = %chunk_id,
                        node = %node_addr,
                        size = data.len(),
                        "readahead prefetch complete"
                    );
                    cache.insert(pending_key.clone(), data);
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
}
