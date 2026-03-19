// I/O statistics and access frequency tracking.
// Reports per-chunk access stats to manager for hot/cold classification.

use std::sync::atomic::{AtomicU64, Ordering};

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

/// Thread-safe I/O metrics for the data node.
pub struct DataMetrics {
    pub total_reads: AtomicU64,
    pub total_writes: AtomicU64,
    pub total_bytes_read: AtomicU64,
    pub total_bytes_written: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
}

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
        }
    }

    /// Record a read operation of `bytes` size.
    pub fn record_read(&self, bytes: u64) {
        self.total_reads.fetch_add(1, Ordering::Relaxed);
        self.total_bytes_read.fetch_add(bytes, Ordering::Relaxed);
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
