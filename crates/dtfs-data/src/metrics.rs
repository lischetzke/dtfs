// I/O statistics and access frequency tracking.
// Reports per-chunk access stats to manager for hot/cold classification.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

/// Snapshot of current metrics values.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_reads: u64,
    pub total_writes: u64,
    pub total_bytes_read: u64,
    pub total_bytes_written: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// P0-9: harvested per-chunk-per-DC read counter for the next heartbeat.
#[derive(Debug, Clone)]
pub struct PerChunkReadHarvest {
    pub chunk_id: String,
    /// DC name → reads since last harvest.
    pub reads_by_dc: HashMap<String, u32>,
    pub last_access_ms: u64,
}

/// Thread-safe I/O metrics for the data node.
pub struct DataMetrics {
    pub total_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub total_bytes_read: AtomicU64,
    pub total_bytes_written: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    /// P0-9: per-chunk read counters bucketed by requester DC. Drained on
    /// each `harvest_per_chunk()` call (called from the heartbeat builder).
    /// Bounded to MAX_CHUNK_DC_ENTRIES; on overflow, the oldest entries are
    /// evicted to keep memory predictable.
    per_chunk_reads: DashMap<String, DashMap<String, AtomicU64>>,
    /// Last access timestamp per chunk_id (UTC ms).
    per_chunk_last_access: DashMap<String, AtomicU64>,
}

/// Cap on per-chunk read-tracking. Each entry is roughly
/// 64 bytes (chunk_id) + 16 bytes per DC. 10k entries ≈ 1 MB worst case.
const MAX_CHUNK_DC_ENTRIES: usize = 10_000;

impl DataMetrics {
    /// Create a new zeroed metrics instance.
    pub fn new() -> Self {
        Self {
            total_reads: AtomicU64::new(0),
            total_writes: AtomicU64::new(0),
            total_bytes_read: AtomicU64::new(0),
            total_bytes_written: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            per_chunk_reads: DashMap::new(),
            per_chunk_last_access: DashMap::new(),
        }
    }

    /// Record a read operation of `bytes` size.
    pub fn record_read(&self, bytes: u64) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
    }

    /// P0-9: record a per-chunk read attributed to a requester DC.
    /// Empty `requester_dc` is recorded under the `"unknown"` bucket so the
    /// counter is never lost (server-side TieringEngine can ignore it).
    pub fn record_chunk_read(&self, chunk_id: &str, requester_dc: &str, now_ms: u64) {
        let dc = if requester_dc.is_empty() {
            "unknown"
        } else {
            requester_dc
        };

        // Update last-access first (cheap path).
        self.per_chunk_last_access
            .entry(chunk_id.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .store(now_ms, Ordering::Relaxed);

        // Eviction: if the table is at capacity AND this chunk isn't already
        // tracked, drop the entry with the oldest last_access to make room.
        if !self.per_chunk_reads.contains_key(chunk_id)
            && self.per_chunk_reads.len() >= MAX_CHUNK_DC_ENTRIES
        {
            if let Some(oldest) = self
                .per_chunk_last_access
                .iter()
                .filter(|e| self.per_chunk_reads.contains_key(e.key()))
                .min_by_key(|e| e.value().load(Ordering::Relaxed))
                .map(|e| e.key().clone())
            {
                self.per_chunk_reads.remove(&oldest);
            }
        }

        let by_dc = self
            .per_chunk_reads
            .entry(chunk_id.to_string())
            .or_insert_with(DashMap::new);
        by_dc
            .entry(dc.to_string())
            .or_insert_with(|| AtomicU64::new(0))
            .fetch_add(1, Ordering::Relaxed);
    }

    /// P0-9: drain the per-chunk-per-DC counters and return the top-N entries
    /// by recent activity, suitable for inclusion in a heartbeat payload.
    /// Counters are zeroed after harvest so each report reflects activity
    /// since the previous heartbeat.
    pub fn harvest_per_chunk(&self, max_entries: usize) -> Vec<PerChunkReadHarvest> {
        // Snapshot all keys.
        let chunks: Vec<String> =
            self.per_chunk_reads.iter().map(|e| e.key().clone()).collect();

        let mut harvest: Vec<PerChunkReadHarvest> = chunks
            .into_iter()
            .filter_map(|chunk_id| {
                let dcs = match self.per_chunk_reads.get(&chunk_id) {
                    Some(d) => d,
                    None => return None,
                };
                let mut reads_by_dc = HashMap::new();
                let mut total: u64 = 0;
                for kv in dcs.iter() {
                    let count = kv.value().swap(0, Ordering::Relaxed) as u32;
                    if count > 0 {
                        total += count as u64;
                        reads_by_dc.insert(kv.key().clone(), count);
                    }
                }
                drop(dcs);
                let last_access_ms = self
                    .per_chunk_last_access
                    .get(&chunk_id)
                    .map(|a| a.value().load(Ordering::Relaxed))
                    .unwrap_or(0);
                if reads_by_dc.is_empty() {
                    return None;
                }
                let _ = total;
                Some(PerChunkReadHarvest {
                    chunk_id,
                    reads_by_dc,
                    last_access_ms,
                })
            })
            .collect();

        // Sort by recency, then truncate.
        harvest.sort_by(|a, b| b.last_access_ms.cmp(&a.last_access_ms));
        harvest.truncate(max_entries);
        harvest
    }

    /// Record a write operation of `bytes` size.
    pub fn record_write(&self, bytes: u64) {
        self.total_writes.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record a cache hit.
    pub fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss.
    pub fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Take a consistent snapshot of current metric values.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_reads: self.total_reads.load(Ordering::Relaxed),
            total_writes: self.total_writes.load(Ordering::Relaxed),
            total_bytes_read: self.total_bytes_read.load(Ordering::Relaxed),
            total_bytes_written: self.total_bytes_written.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
        }
    }
}

impl Default for DataMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_read() {
        let m = DataMetrics::new();
        m.record_read(1024);
        m.record_read(2048);
        let snap = m.snapshot();
        assert_eq!(snap.total_reads, 2);
        assert_eq!(snap.total_bytes_read, 3072);
    }

    #[test]
    fn test_record_write() {
        let m = DataMetrics::new();
        m.record_write(512);
        let snap = m.snapshot();
        assert_eq!(snap.total_writes, 1);
        assert_eq!(snap.total_bytes_written, 512);
    }

    #[test]
    fn test_cache_hit_miss() {
        let m = DataMetrics::new();
        m.record_cache_hit();
        m.record_cache_hit();
        m.record_cache_miss();
        let snap = m.snapshot();
        assert_eq!(snap.cache_hits, 2);
        assert_eq!(snap.cache_misses, 1);
    }

    #[test]
    fn test_default() {
        let m = DataMetrics::default();
        let snap = m.snapshot();
        assert_eq!(snap.total_reads, 0);
        assert_eq!(snap.total_writes, 0);
    }
}
