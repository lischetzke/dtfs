//! Issue #159 phase 4 - FUSE client metrics.
//!
//! Every handle is registered once (`OnceLock`); the hot path only does
//! atomics plus `Instant::now()`. Labels come from small fixed sets (op,
//! errno, cache layer, trigger); the only dynamic label values (tier, DC)
//! go through `SrcMetrics`, a bounded per-(tier, dc) handle cache.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Instant;

use dtfs_common::metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

/// FUSE ops that get a latency histogram and error counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    Lookup,
    Getattr,
    Setattr,
    Open,
    Release,
    Read,
    Write,
    Flush,
    Fsync,
    Mkdir,
    Unlink,
    Rmdir,
    Create,
    Readdir,
    Readdirplus,
    Rename,
}

const OPS: [Op; 16] = [
    Op::Lookup,
    Op::Getattr,
    Op::Setattr,
    Op::Open,
    Op::Release,
    Op::Read,
    Op::Write,
    Op::Flush,
    Op::Fsync,
    Op::Mkdir,
    Op::Unlink,
    Op::Rmdir,
    Op::Create,
    Op::Readdir,
    Op::Readdirplus,
    Op::Rename,
];

impl Op {
    pub fn name(self) -> &'static str {
        match self {
            Op::Lookup => "lookup",
            Op::Getattr => "getattr",
            Op::Setattr => "setattr",
            Op::Open => "open",
            Op::Release => "release",
            Op::Read => "read",
            Op::Write => "write",
            Op::Flush => "flush",
            Op::Fsync => "fsync",
            Op::Mkdir => "mkdir",
            Op::Unlink => "unlink",
            Op::Rmdir => "rmdir",
            Op::Create => "create",
            Op::Readdir => "readdir",
            Op::Readdirplus => "readdirplus",
            Op::Rename => "rename",
        }
    }

    fn idx(self) -> usize {
        self as usize
    }
}

/// Errnos the adapter can return, one label each; anything else is "other".
const ERRNOS: [(i32, &str); 9] = [
    (libc::ENOENT, "ENOENT"),
    (libc::EIO, "EIO"),
    (libc::EEXIST, "EEXIST"),
    (libc::ENOTEMPTY, "ENOTEMPTY"),
    (libc::EINVAL, "EINVAL"),
    (libc::EBUSY, "EBUSY"),
    (libc::ENOSPC, "ENOSPC"),
    (libc::EACCES, "EACCES"),
    (libc::EPERM, "EPERM"),
];

/// Index into `ERRNOS`, `ERRNOS.len()` for an unlisted errno.
fn errno_idx(errno: i32) -> usize {
    ERRNOS.iter().position(|(e, _)| *e == errno).unwrap_or(ERRNOS.len())
}

pub fn errno_label(errno: i32) -> &'static str {
    ERRNOS.get(errno_idx(errno)).map_or("other", |(_, l)| l)
}

/// Handles for one (tier, source DC) pair.
pub struct SrcMetrics {
    pub get_us: Arc<Histogram>,
    pub get_bytes: Arc<Counter>,
    pub get_fail: Arc<Counter>,
    pub put_us: Arc<Histogram>,
    pub put_bytes: Arc<Counter>,
    pub put_fail: Arc<Counter>,
    pub chosen: Arc<Counter>,
}

pub struct FuseMetrics {
    op_us: Vec<Arc<Histogram>>,
    op_err: Vec<Arc<Counter>>,
    pub meta_hit: Arc<Counter>,
    pub meta_miss: Arc<Counter>,
    pub meta_entries: Arc<Gauge>,
    pub cache_hit_mem: Arc<Counter>,
    pub cache_hit_mem_bytes: Arc<Counter>,
    pub cache_hit_disk: Arc<Counter>,
    pub cache_hit_disk_bytes: Arc<Counter>,
    pub cache_miss: Arc<Counter>,
    pub ra_issued: Arc<Counter>,
    pub ra_failed: Arc<Counter>,
    pub ra_get_us: Arc<Histogram>,
    pub ra_bytes: Arc<Counter>,
    pub ra_evicted_bytes: Arc<Counter>,
    pub ra_dropped_bytes: Arc<Counter>,
    pub ra_join: Arc<Counter>,
    pub ra_cache_bytes: Arc<Gauge>,
    pub ra_budget_bytes: Arc<Gauge>,
    pub ra_pending: Arc<Gauge>,
    pub sf_fg_join: Arc<Counter>,
    pub sf_fg_owner_failed: Arc<Counter>,
    pub failover_integrity: Arc<Counter>,
    pub failover_timeout: Arc<Counter>,
    pub failover_error: Arc<Counter>,
    pub verify_fail: Arc<Counter>,
    pub disk_cache_bytes: Arc<Gauge>,
    pub disk_cache_max_bytes: Arc<Gauge>,
    pub dirty_bytes: Arc<Gauge>,
    pub dirty_cap_bytes: Arc<Gauge>,
    pub cap_waits: Arc<Counter>,
    pub cap_wait_us: Arc<Histogram>,
    pub bg_inflight: Arc<Gauge>,
    pub bg_commit_us: Arc<Histogram>,
    pub trig_full: Arc<Counter>,
    pub trig_timer: Arc<Counter>,
    pub trig_dirty_cap: Arc<Counter>,
    pub trig_close: Arc<Counter>,
    pub fill_issued: Arc<Counter>,
    pub fill_failed: Arc<Counter>,
    pub put_ladder_retries: Arc<Counter>,
    src: RwLock<HashMap<String, Arc<SrcMetrics>>>,
}

/// More distinct (tier, dc) pairs than this fold into "other".
const MAX_SRC_KEYS: usize = 32;

pub fn fm() -> &'static FuseMetrics {
    static M: OnceLock<FuseMetrics> = OnceLock::new();
    M.get_or_init(|| {
        let mut op_us = Vec::new();
        let mut op_err = Vec::new();
        for op in OPS {
            op_us.push(histogram("fuse_op_us", &[("op", op.name())]));
            for (_, l) in ERRNOS.iter().map(|(e, l)| (*e, *l)).chain([(0, "other")]) {
                op_err.push(counter("fuse_op_errors", &[("op", op.name()), ("errno", l)]));
            }
        }
        FuseMetrics {
            op_us,
            op_err,
            meta_hit: counter("fuse_meta_cache_hits", &[]),
            meta_miss: counter("fuse_meta_cache_misses", &[]),
            meta_entries: gauge("fuse_meta_cache_entries", &[]),
            cache_hit_mem: counter("fuse_chunk_cache_hits", &[("layer", "readahead")]),
            cache_hit_mem_bytes: counter("fuse_chunk_cache_hit_bytes", &[("layer", "readahead")]),
            cache_hit_disk: counter("fuse_chunk_cache_hits", &[("layer", "disk")]),
            cache_hit_disk_bytes: counter("fuse_chunk_cache_hit_bytes", &[("layer", "disk")]),
            cache_miss: counter("fuse_chunk_cache_misses", &[]),
            ra_issued: counter("fuse_readahead_issued", &[]),
            ra_failed: counter("fuse_readahead_failed", &[]),
            ra_get_us: histogram("fuse_readahead_get_us", &[]),
            ra_bytes: counter("fuse_readahead_bytes", &[]),
            ra_evicted_bytes: counter("fuse_readahead_evicted_bytes", &[("reason", "budget")]),
            ra_dropped_bytes: counter("fuse_readahead_evicted_bytes", &[("reason", "drop")]),
            ra_join: counter("fuse_singleflight_merges", &[("kind", "prefetch_join")]),
            ra_cache_bytes: gauge("fuse_readahead_cache_bytes", &[]),
            ra_budget_bytes: gauge("fuse_readahead_budget_bytes", &[]),
            ra_pending: gauge("fuse_readahead_pending", &[]),
            sf_fg_join: counter("fuse_singleflight_merges", &[("kind", "foreground")]),
            sf_fg_owner_failed: counter("fuse_singleflight_owner_failed", &[]),
            failover_integrity: counter("fuse_read_failovers", &[("reason", "integrity")]),
            failover_timeout: counter("fuse_read_failovers", &[("reason", "timeout")]),
            failover_error: counter("fuse_read_failovers", &[("reason", "error")]),
            verify_fail: counter("fuse_read_verify_failures", &[]),
            disk_cache_bytes: gauge("fuse_disk_cache_bytes", &[]),
            disk_cache_max_bytes: gauge("fuse_disk_cache_max_bytes", &[]),
            dirty_bytes: gauge("fuse_write_buffered_bytes", &[]),
            dirty_cap_bytes: gauge("fuse_write_dirty_cap_bytes", &[]),
            cap_waits: counter("fuse_write_cap_waits", &[]),
            cap_wait_us: histogram("fuse_write_cap_wait_us", &[]),
            bg_inflight: gauge("fuse_async_inflight_batches", &[]),
            bg_commit_us: histogram("fuse_async_batch_commit_us", &[]),
            trig_full: counter("fuse_commits", &[("trigger", "batch_full")]),
            trig_timer: counter("fuse_commits", &[("trigger", "timer")]),
            trig_dirty_cap: counter("fuse_commits", &[("trigger", "dirty_cap")]),
            trig_close: counter("fuse_commits", &[("trigger", "close")]),
            fill_issued: counter("fuse_cache_fill_issued", &[]),
            fill_failed: counter("fuse_cache_fill_failed", &[]),
            put_ladder_retries: counter("fuse_put_ladder_retries", &[]),
            src: RwLock::new(HashMap::new()),
        }
    })
}

impl FuseMetrics {
    /// Record one finished op: latency, and the errno when it failed.
    pub fn op_done(&self, op: Op, t0: Instant, errno: Option<i32>) {
        self.op_us[op.idx()].observe_duration(t0.elapsed());
        if let Some(e) = errno {
            self.op_err[op.idx() * (ERRNOS.len() + 1) + errno_idx(e)].inc();
        }
    }

    /// Handles for a (tier, dc) pair; empty values read "unknown". The
    /// registration happens once per pair (bounded by `MAX_SRC_KEYS`), so
    /// callers pay a read-lock lookup per chunk, never a registration.
    pub fn src(&self, tier: &str, dc: &str) -> Arc<SrcMetrics> {
        let key = format!("{}|{}", tier, dc);
        if let Some(s) = self.src.read().unwrap().get(&key) {
            return s.clone();
        }
        let mut map = self.src.write().unwrap();
        let (key, tier, dc) = if map.len() >= MAX_SRC_KEYS && !map.contains_key(&key) {
            ("other|other".to_string(), "other", "other")
        } else {
            (key, if tier.is_empty() { "unknown" } else { tier }, if dc.is_empty() { "unknown" } else { dc })
        };
        map.entry(key)
            .or_insert_with(|| {
                let l = [("tier", tier), ("dc", dc)];
                Arc::new(SrcMetrics {
                    get_us: histogram("fuse_get_chunk_us", &l),
                    get_bytes: counter("fuse_get_chunk_bytes", &l),
                    get_fail: counter("fuse_get_chunk_failures", &l),
                    put_us: histogram("fuse_put_chunk_us", &l),
                    put_bytes: counter("fuse_put_chunk_bytes", &l),
                    put_fail: counter("fuse_put_chunk_failures", &l),
                    chosen: counter("fuse_read_replica_chosen", &l),
                })
            })
            .clone()
    }
}

/// True at most once per `every_ms` for the shared `last` stamp (0 = never sampled).
pub fn due(last: &AtomicU64, now_ms: u64, every_ms: u64) -> bool {
    let l = last.load(Ordering::Relaxed);
    if l != 0 && now_ms.saturating_sub(l) < every_ms {
        return false;
    }
    last.store(now_ms.max(1), Ordering::Relaxed);
    true
}

/// Gauge sampling throttle: once a second.
pub fn gauges_due() -> bool {
    static START: OnceLock<Instant> = OnceLock::new();
    static LAST: AtomicU64 = AtomicU64::new(0);
    // +1 so the first call is never 0 (the "never sampled" stamp).
    due(&LAST, START.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1, 1000)
}

/// Run an adapter op body, recording its latency and error errno.
pub async fn timed<T>(op: Op, fut: impl Future<Output = fuse3::Result<T>>) -> fuse3::Result<T> {
    let t0 = Instant::now();
    let r = fut.await;
    fm().op_done(op, t0, r.as_ref().err().map(|e| i32::from(*e)));
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errno_label_maps_known_and_unknown() {
        assert_eq!(errno_label(libc::ENOENT), "ENOENT");
        assert_eq!(errno_label(libc::ENOTEMPTY), "ENOTEMPTY");
        assert_eq!(errno_label(libc::ENOSPC), "ENOSPC");
        assert_eq!(errno_label(libc::EXDEV), "other");
        assert_eq!(errno_label(0), "other");
        assert_eq!(errno_idx(libc::EXDEV), ERRNOS.len());
    }

    #[test]
    fn op_index_matches_table_position() {
        for (i, op) in OPS.iter().enumerate() {
            assert_eq!(op.idx(), i, "{}", op.name());
        }
        assert_eq!(OPS.len(), 16);
    }

    #[test]
    fn errored_op_counts_exact_errno_and_latency() {
        let m = fm();
        let c = |op: Op, l: &str| counter("fuse_op_errors", &[("op", op.name()), ("errno", l)]);
        let (e0, o0, h0) = (c(Op::Rmdir, "ENOTEMPTY").get(), c(Op::Rmdir, "EIO").get(), histogram("fuse_op_us", &[("op", "rmdir")]).count());
        m.op_done(Op::Rmdir, Instant::now(), Some(libc::ENOTEMPTY));
        assert_eq!(c(Op::Rmdir, "ENOTEMPTY").get(), e0 + 1);
        assert_eq!(c(Op::Rmdir, "EIO").get(), o0, "other errno untouched");
        m.op_done(Op::Rmdir, Instant::now(), None);
        assert_eq!(c(Op::Rmdir, "ENOTEMPTY").get(), e0 + 1, "success adds no error");
        assert_eq!(histogram("fuse_op_us", &[("op", "rmdir")]).count(), h0 + 2);
    }

    #[test]
    fn cache_counters_hit_and_miss_are_distinct() {
        let m = fm();
        // Global counters shared with parallel tests: assert growth only.
        let (h, mi) = (m.cache_hit_disk.get(), m.cache_miss.get());
        m.cache_hit_disk.inc();
        assert!(m.cache_hit_disk.get() > h);
        m.cache_miss.inc();
        assert!(m.cache_miss.get() > mi);
    }

    #[test]
    fn due_throttles_to_the_interval() {
        let last = AtomicU64::new(0);
        assert!(due(&last, 5, 1000), "first sample always runs");
        assert!(!due(&last, 999, 1000), "inside the interval");
        assert!(due(&last, 1005, 1000), "boundary: exactly one interval later");
        assert!(!due(&last, 1006, 1000));
    }

    #[test]
    fn src_handles_are_cached_and_bounded() {
        let m = fm();
        let a = m.src("hot", "leli");
        assert!(Arc::ptr_eq(&a, &m.src("hot", "leli")));
        assert!(!Arc::ptr_eq(&a, &m.src("cold", "leli")));
        for i in 0..(MAX_SRC_KEYS * 2) {
            m.src("t", &format!("dc{i}"));
        }
        assert!(m.src.read().unwrap().len() <= MAX_SRC_KEYS + 1);
    }
}
