//! Issue #159 -- data node metrics handles. Every handle is registered once
//! (per disk inside `ChunkStore`, per method / peer on first use, the rest in
//! `OnceLock`s) and cached; the hot path only does atomic ops and
//! `Instant::now()`. Labels come from small fixed sets: disk id, tier, method,
//! gRPC code, peer address (capped). `DataMetrics` (the heartbeat counters) is
//! untouched and keeps feeding the heartbeat payload.

use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use dtfs_common::metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};
use tonic::codegen::http;
use tonic::Code;

type C = Arc<Counter>;
type G = Arc<Gauge>;
type H = Arc<Histogram>;

/// Per-disk handles, created once by `ChunkStore::new` (position = disk_id).
pub struct DiskObs {
    // PutChunk stages. recv/tmp_write/hash/total are recorded by the gRPC
    // handler, the rest by `commit_chunk_from_tmp`.
    pub put_total_us: H,
    pub put_recv_us: H,
    pub put_tmp_write_us: H,
    pub put_hash_us: H,
    pub put_fsync_dat_us: H,
    pub put_rename_us: H,
    /// `.meta` tmp write + fsync + rename + chunk-dir fsync.
    pub put_meta_us: H,
    pub put_commit_us: H,
    pub put_chunk_bytes: H,
    pub put_bytes: C,
    pub puts: C,
    // GetChunk. read/send are per streamed piece.
    pub get_read_us: H,
    pub get_send_us: H,
    pub get_verify_us: H,
    pub get_total_us: H,
    pub get_chunk_bytes: H,
    pub get_bytes: C,
    pub gets: C,
    pub get_data_loss: C,
    // Scrub.
    pub scrub_chunks: C,
    pub scrub_bytes: C,
    pub scrub_corrupt: C,
    pub scrub_missing: C,
    pub scrub_size_mismatch: C,
    pub scrub_meta_repaired: C,
    pub scrub_io_errors: C,
    pub scrub_throttle_us: C,
    pub quarantines: C,
    // Sampled gauges.
    pub disk_total_bytes: G,
    pub disk_used_bytes: G,
    pub disk_free_bytes: G,
    pub disk_failed: G,
}

impl DiskObs {
    pub fn new(disk_id: u32, tier: &str) -> Self {
        let id = disk_id.to_string();
        let l: &[(&'static str, &str)] = &[("disk", &id), ("tier", tier)];
        Self {
            put_total_us: histogram("data_put_total_us", l),
            put_recv_us: histogram("data_put_recv_us", l),
            put_tmp_write_us: histogram("data_put_tmp_write_us", l),
            put_hash_us: histogram("data_put_hash_us", l),
            put_fsync_dat_us: histogram("data_put_fsync_dat_us", l),
            put_rename_us: histogram("data_put_rename_us", l),
            put_meta_us: histogram("data_put_meta_us", l),
            put_commit_us: histogram("data_put_commit_us", l),
            put_chunk_bytes: histogram("data_put_chunk_bytes", l),
            put_bytes: counter("data_put_bytes_total", l),
            puts: counter("data_puts_total", l),
            get_read_us: histogram("data_get_read_us", l),
            get_send_us: histogram("data_get_send_us", l),
            get_verify_us: histogram("data_get_verify_us", l),
            get_total_us: histogram("data_get_total_us", l),
            get_chunk_bytes: histogram("data_get_chunk_bytes", l),
            get_bytes: counter("data_get_bytes_total", l),
            gets: counter("data_gets_total", l),
            get_data_loss: counter("data_get_data_loss_total", l),
            scrub_chunks: counter("data_scrub_chunks_total", l),
            scrub_bytes: counter("data_scrub_bytes_total", l),
            scrub_corrupt: counter("data_scrub_corrupt_total", l),
            scrub_missing: counter("data_scrub_missing_total", l),
            scrub_size_mismatch: counter("data_scrub_size_mismatch_total", l),
            scrub_meta_repaired: counter("data_scrub_meta_repaired_total", l),
            scrub_io_errors: counter("data_scrub_io_errors_total", l),
            scrub_throttle_us: counter("data_scrub_throttle_us_total", l),
            quarantines: counter("data_quarantines_total", l),
            disk_total_bytes: gauge("data_disk_total_bytes", l),
            disk_used_bytes: gauge("data_disk_used_bytes", l),
            disk_free_bytes: gauge("data_disk_free_bytes", l),
            disk_failed: gauge("data_disk_failed", l),
        }
    }
}

/// One counter per gRPC status code (0..=16), registered on first use.
pub struct CodeCounters {
    name: &'static str,
    method: String,
    slots: [OnceLock<C>; 17],
}

impl CodeCounters {
    pub fn new(name: &'static str, method: &str) -> Self {
        Self { name, method: method.to_string(), slots: std::array::from_fn(|_| OnceLock::new()) }
    }
    fn slot(&self, code: Code) -> &OnceLock<C> {
        &self.slots[(code as i32).clamp(0, 16) as usize]
    }
    pub fn inc(&self, code: Code) {
        self.slot(code)
            .get_or_init(|| counter(self.name, &[("method", &self.method), ("code", &format!("{code:?}"))]))
            .inc();
    }
    #[cfg(test)]
    pub fn get(&self, code: Code) -> u64 {
        self.slot(code).get().map_or(0, |c| c.get())
    }
}

/// Decrements the gauge on drop, so a dropped request future still counts out.
pub struct Inflight(G);
impl Inflight {
    pub fn new(g: &G) -> Self {
        g.add(1);
        Self(g.clone())
    }
}
impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.add(-1);
    }
}

/// Node-wide handles.
pub struct NodeObs {
    pub put_inflight: G,
    pub get_inflight: G,
    pub put_slot_wait_us: H,
    /// Puts refused because the memory budget was shedding.
    pub put_shed: C,
    pub put_rejects: CodeCounters,
    pub get_not_found: C,
    pub heartbeat_us: H,
    pub heartbeat_failures: C,
    pub heartbeat_rejected: C,
    pub mem_rss_bytes: G,
    pub mem_cap_bytes: G,
    /// 0 normal, 1 warn, 2 shed.
    pub mem_pressure: G,
    pub store_chunks: G,
    pub store_bytes: G,
}

pub fn node() -> &'static NodeObs {
    static N: OnceLock<NodeObs> = OnceLock::new();
    N.get_or_init(|| NodeObs {
        put_inflight: gauge("data_put_inflight", &[]),
        get_inflight: gauge("data_get_inflight", &[]),
        put_slot_wait_us: histogram("data_put_slot_wait_us", &[]),
        put_shed: counter("data_put_shed_total", &[]),
        put_rejects: CodeCounters::new("data_put_rejects_total", "PutChunk"),
        get_not_found: counter("data_get_not_found_total", &[]),
        heartbeat_us: histogram("data_heartbeat_us", &[]),
        heartbeat_failures: counter("data_heartbeat_failures_total", &[]),
        heartbeat_rejected: counter("data_heartbeat_rejected_total", &[]),
        mem_rss_bytes: gauge("data_mem_rss_bytes", &[]),
        mem_cap_bytes: gauge("data_mem_cap_bytes", &[]),
        mem_pressure: gauge("data_mem_pressure_level", &[]),
        store_chunks: gauge("data_store_chunks", &[]),
        store_bytes: gauge("data_store_bytes", &[]),
    })
}

/// Awaits a heartbeat RPC and records its latency and failure.
pub async fn timed_heartbeat<T, E>(fut: impl std::future::Future<Output = Result<T, E>>) -> Result<T, E> {
    let t = Instant::now();
    let r = fut.await;
    let n = node();
    n.heartbeat_us.observe_duration(t.elapsed());
    if r.is_err() {
        n.heartbeat_failures.inc();
    }
    r
}

// ---- replication (outbound, per peer) ----

pub struct PeerObs {
    pub out_bytes: C,
    pub out_us: H,
    pub out_ok: C,
    pub out_failures: C,
}

/// Peers beyond this share the "other" entry (the address is caller-chosen).
const MAX_PEERS: usize = 64;

/// `host:port` of a replication target, scheme and path stripped.
pub fn peer_label(addr: &str) -> &str {
    let a = addr.split_once("://").map_or(addr, |(_, r)| r);
    a.split('/').next().unwrap_or(a)
}

pub fn peer(addr: &str) -> Arc<PeerObs> {
    static PEERS: OnceLock<DashMap<String, Arc<PeerObs>>> = OnceLock::new();
    static COUNT: AtomicUsize = AtomicUsize::new(0);
    let peers = PEERS.get_or_init(DashMap::new);
    let mut label = peer_label(addr);
    if let Some(p) = peers.get(label) {
        return p.clone();
    }
    if COUNT.load(Relaxed) >= MAX_PEERS {
        label = "other";
        if let Some(p) = peers.get(label) {
            return p.clone();
        }
    }
    peers
        .entry(label.to_string())
        .or_insert_with(|| {
            COUNT.fetch_add(1, Relaxed);
            let l: &[(&'static str, &str)] = &[("peer", label)];
            Arc::new(PeerObs {
                out_bytes: counter("data_replicate_out_bytes_total", l),
                out_us: histogram("data_replicate_out_us", l),
                out_ok: counter("data_replicate_out_total", l),
                out_failures: counter("data_replicate_out_failures_total", l),
            })
        })
        .clone()
}

impl PeerObs {
    pub fn record(&self, elapsed: Duration, bytes: u64, ok: bool) {
        self.out_us.observe_duration(elapsed);
        if ok {
            self.out_ok.inc();
            self.out_bytes.add(bytes);
        } else {
            self.out_failures.inc();
        }
    }
}

// ---- gRPC server layer ----

pub struct MethodObs {
    pub requests: C,
    pub latency_us: H,
    pub inflight: G,
    pub errors: CodeCounters,
}

const MAX_METHODS: usize = 32;

/// Last path segment of a gRPC path (`/pkg.Service/Method`).
pub fn method_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

pub(crate) fn method(path: &str) -> Arc<MethodObs> {
    static METHODS: OnceLock<DashMap<String, Arc<MethodObs>>> = OnceLock::new();
    let methods = METHODS.get_or_init(DashMap::new);
    let mut name = method_name(path);
    if let Some(m) = methods.get(name) {
        return m.clone();
    }
    if methods.len() >= MAX_METHODS {
        name = "other";
        if let Some(m) = methods.get(name) {
            return m.clone();
        }
    }
    methods
        .entry(name.to_string())
        .or_insert_with(|| {
            let l: &[(&'static str, &str)] = &[("method", name)];
            Arc::new(MethodObs {
                requests: counter("data_grpc_requests_total", l),
                latency_us: histogram("data_grpc_latency_us", l),
                inflight: gauge("data_grpc_inflight", l),
                errors: CodeCounters::new("data_grpc_errors_total", name),
            })
        })
        .clone()
}

/// Status of a response from its headers. Errors returned before the body
/// starts are "trailers-only" and carry `grpc-status` here; a stream that
/// fails mid-way reports it in trailers, which this does not see.
pub fn grpc_code(h: &http::HeaderMap) -> Code {
    h.get("grpc-status")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<i32>().ok())
        .map_or(Code::Ok, Code::from_i32)
}

#[derive(Clone, Copy)]
pub struct GrpcObsLayer;

impl<S> tower::Layer<S> for GrpcObsLayer {
    type Service = GrpcObsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        GrpcObsService { inner }
    }
}

#[derive(Clone)]
pub struct GrpcObsService<S> {
    inner: S,
}

impl<S, ReqB, ResB> tower::Service<http::Request<ReqB>> for GrpcObsService<S>
where
    S: tower::Service<http::Request<ReqB>, Response = http::Response<ResB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqB: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<ReqB>) -> Self::Future {
        let m = method(req.uri().path());
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        Box::pin(async move {
            m.requests.inc();
            let _g = Inflight::new(&m.inflight);
            let t = Instant::now();
            let r = inner.call(req).await;
            m.latency_us.observe_duration(t.elapsed());
            match &r {
                Ok(resp) => {
                    let c = grpc_code(resp.headers());
                    if c != Code::Ok {
                        m.errors.inc(c);
                    }
                }
                Err(_) => m.errors.inc(Code::Unknown),
            }
            r
        })
    }
}

// ---- sampled gauges ----

/// Refresh the per-disk, store and memory gauges (statvfs per disk; cheap).
pub fn sample_gauges(store: &crate::store::ChunkStore, memory: &crate::memory::MemoryBudget) {
    let dm = store.disk_manager();
    for (id, total, used) in dm.live_disk_usage() {
        if let Some(o) = store.disk_obs(id) {
            o.disk_total_bytes.set(total as i64);
            o.disk_used_bytes.set(used as i64);
            o.disk_free_bytes.set(total.saturating_sub(used) as i64);
        }
    }
    for d in dm.all_disks() {
        if let Some(o) = store.disk_obs(d.disk_id) {
            o.disk_failed.set((dm.disk_status(d) == dtfs_common::types::DiskStatus::Failed) as i64);
        }
    }
    let s = store.stats();
    let n = node();
    n.store_chunks.set(s.total_chunks as i64);
    n.store_bytes.set(s.total_bytes_stored as i64);
    n.mem_rss_bytes.set(memory.last_rss_bytes() as i64);
    n.mem_cap_bytes.set(memory.hard_cap_bytes() as i64);
    n.mem_pressure.set(memory.pressure_level() as u8 as i64);
}

pub async fn run_gauge_sampler(store: Arc<crate::store::ChunkStore>, memory: Arc<crate::memory::MemoryBudget>) {
    let mut tick = tokio::time::interval(Duration::from_secs(15));
    loop {
        tick.tick().await;
        sample_gauges(&store, &memory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_counters_count_per_code_and_exact_code() {
        let c = CodeCounters::new("t_code_counters_total", "X");
        c.inc(Code::DataLoss);
        c.inc(Code::DataLoss);
        c.inc(Code::NotFound);
        assert_eq!((c.get(Code::DataLoss), c.get(Code::NotFound)), (2, 1));
        assert_eq!(c.get(Code::Internal), 0);
    }

    #[test]
    fn peer_label_strips_scheme_and_path() {
        assert_eq!(peer_label("http://10.0.0.1:9500"), "10.0.0.1:9500");
        assert_eq!(peer_label("https://h:1/x"), "h:1");
        assert_eq!(peer_label("h:1"), "h:1");
    }

    #[test]
    fn peer_record_splits_ok_and_failure() {
        let p = peer("http://obs-test-peer:1");
        p.record(Duration::from_millis(1), 100, true);
        p.record(Duration::from_millis(1), 999, false);
        assert_eq!((p.out_ok.get(), p.out_failures.get(), p.out_bytes.get()), (1, 1, 100));
    }

    #[test]
    fn method_name_takes_last_segment() {
        assert_eq!(method_name("/dtfs.data.DataService/PutChunk"), "PutChunk");
        assert_eq!(method_name("PutChunk"), "PutChunk");
    }

    #[test]
    fn grpc_code_reads_header_and_defaults_ok() {
        let mut h = http::HeaderMap::new();
        assert_eq!(grpc_code(&h), Code::Ok);
        h.insert("grpc-status", "15".parse().unwrap());
        assert_eq!(grpc_code(&h), Code::DataLoss);
        h.insert("grpc-status", "garbage".parse().unwrap());
        assert_eq!(grpc_code(&h), Code::Ok);
    }

    #[test]
    fn inflight_guard_counts_out_on_drop() {
        let g: G = Arc::default();
        {
            let _a = Inflight::new(&g);
            let _b = Inflight::new(&g);
            assert_eq!(g.get(), 2);
        }
        assert_eq!(g.get(), 0);
    }
}
