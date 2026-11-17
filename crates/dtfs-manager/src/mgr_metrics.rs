//! Issue #159 — manager metrics. Handles are cached (registration locks and
//! allocates); the request path only does Relaxed atomics. Labels come from
//! small fixed sets (gRPC method, route template, error code, reason, DC).
//! Nothing here runs inside `apply_request`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock, Mutex, OnceLock, RwLock as StdRwLock};
use std::time::{Duration, Instant};

use dtfs_common::metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::DtfsRaft;
use futures::future::BoxFuture;
use tokio::sync::RwLock;
use tonic::codegen::http::{HeaderMap, Request, Response};
use tower::{Layer, Service};

/// Label-keyed handle cache. Lookups take a read lock; a new key registers
/// once. Past `cap` distinct keys everything shares the `other` entry, so a
/// hostile peer cannot grow the registry.
pub(crate) struct Cached<T> {
    map: StdRwLock<HashMap<String, Arc<T>>>,
    cap: usize,
}

impl<T> Cached<T> {
    pub(crate) fn new(cap: usize) -> Self {
        Self { map: StdRwLock::new(HashMap::new()), cap }
    }

    pub(crate) fn get(&self, key: &str, make: impl FnOnce(&str) -> Arc<T>) -> Arc<T> {
        if let Some(h) = self.map.read().unwrap_or_else(|e| e.into_inner()).get(key) {
            return h.clone();
        }
        let mut w = self.map.write().unwrap_or_else(|e| e.into_inner());
        let key = if w.len() >= self.cap && !w.contains_key(key) { "other" } else { key };
        w.entry(key.to_string()).or_insert_with(|| make(key)).clone()
    }
}

// ---------------------------------------------------------------------------
// gRPC server layer (ManagerService and the Raft transport share the listener)
// ---------------------------------------------------------------------------

/// `ManagerService` methods of proto/manager.proto. The label is always one
/// of these (or `RaftRpc` / `other`), never text from the request: the layer
/// runs before auth, so a caller must not be able to mint label values.
const KNOWN_METHODS: [&str; 21] = [
    "CreateFile", "GetFileLocations", "DeleteFile", "ListDirectory", "AllocateChunks",
    "CommitChunkLocations", "RenameFile", "SetFileSize", "CreateSymlink", "ReadSymlink",
    "CreateLink", "ClusterCapacity", "AcquireWriteLease", "RenewWriteLease",
    "ReleaseWriteLease", "ReleaseLeasesByHost", "RegisterNode", "Heartbeat",
    "ReportChunkStatus", "ForwardClientWrite", "CommitCacheFill",
];

/// `"/dtfs.manager.ManagerService/CreateFile"` -> `"CreateFile"`; the Raft
/// transport -> `"RaftRpc"`; anything else -> `"other"`.
pub(crate) fn method_of_path(path: &str) -> &'static str {
    if path == "/dtfs.manager.RaftTransport/RaftRpc" {
        return "RaftRpc";
    }
    path.strip_prefix("/dtfs.manager.ManagerService/")
        .and_then(|m| KNOWN_METHODS.iter().find(|k| **k == m))
        .copied()
        .unwrap_or("other")
}

pub(crate) fn code_label(c: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match c {
        Ok => "ok",
        Cancelled => "cancelled",
        Unknown => "unknown",
        InvalidArgument => "invalid_argument",
        DeadlineExceeded => "deadline_exceeded",
        NotFound => "not_found",
        AlreadyExists => "already_exists",
        PermissionDenied => "permission_denied",
        ResourceExhausted => "resource_exhausted",
        FailedPrecondition => "failed_precondition",
        Aborted => "aborted",
        OutOfRange => "out_of_range",
        Unimplemented => "unimplemented",
        Internal => "internal",
        Unavailable => "unavailable",
        DataLoss => "data_loss",
        Unauthenticated => "unauthenticated",
    }
}

/// The `grpc-status` of a trailers-only response (every handler `Err(Status)`).
/// `None` when absent (success, or a streamed response whose status is in the
/// trailers) or unparsable.
pub(crate) fn status_code(h: &HeaderMap) -> Option<tonic::Code> {
    let n: i32 = h.get("grpc-status")?.to_str().ok()?.parse().ok()?;
    Some(tonic::Code::from(n))
}

struct GrpcHandles {
    method: String,
    requests: Arc<Counter>,
    us: Arc<Histogram>,
    inflight: Arc<Gauge>,
    /// Indexed by `tonic::Code as usize` (0..=16), registered on first error.
    errors: [OnceLock<Arc<Counter>>; 17],
}

impl GrpcHandles {
    fn new(method: &str) -> Self {
        let l = [("method", method)];
        Self {
            method: method.to_string(),
            requests: counter("grpc_server_requests", &l),
            us: histogram("grpc_server_us", &l),
            inflight: gauge("grpc_server_inflight", &l),
            errors: std::array::from_fn(|_| OnceLock::new()),
        }
    }

    fn error(&self, code: tonic::Code) -> &Counter {
        self.errors[(code as i32 as usize).min(16)].get_or_init(|| {
            counter("grpc_server_errors", &[("method", &self.method), ("code", code_label(code))])
        })
    }
}

static GRPC: LazyLock<Cached<GrpcHandles>> = LazyLock::new(|| Cached::new(128));

struct InFlight(Arc<Gauge>);
impl InFlight {
    fn new(g: Arc<Gauge>) -> Self {
        g.add(1);
        Self(g)
    }
}
impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.add(-1);
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct GrpcMetricsLayer;

impl<S> Layer<S> for GrpcMetricsLayer {
    type Service = GrpcMetrics<S>;
    fn layer(&self, inner: S) -> GrpcMetrics<S> {
        GrpcMetrics(inner)
    }
}

#[derive(Clone)]
pub(crate) struct GrpcMetrics<S>(S);

impl<S, B, RB> Service<Request<B>> for GrpcMetrics<S>
where
    S: Service<Request<B>, Response = Response<RB>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let h = GRPC.get(method_of_path(req.uri().path()), |m| Arc::new(GrpcHandles::new(m)));
        let clone = self.0.clone();
        let mut inner = std::mem::replace(&mut self.0, clone);
        Box::pin(async move {
            h.requests.inc();
            let _g = InFlight::new(h.inflight.clone());
            let t0 = Instant::now();
            let res = inner.call(req).await;
            // For streamed responses this is time to first headers.
            h.us.observe_duration(t0.elapsed());
            if let Ok(r) = &res {
                if let Some(code) = status_code(r.headers()) {
                    if code != tonic::Code::Ok {
                        h.error(code).inc();
                    }
                }
            }
            res
        })
    }
}

// ---------------------------------------------------------------------------
// REST middleware
// ---------------------------------------------------------------------------

/// `"2xx"` etc.; anything outside 100..=599 is `"other"`.
pub(crate) fn status_class(code: u16) -> &'static str {
    match code {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

const CLASSES: [&str; 6] = ["1xx", "2xx", "3xx", "4xx", "5xx", "other"];

struct RestHandles {
    us: Arc<Histogram>,
    by_class: [Arc<Counter>; 6],
}

impl RestHandles {
    fn new(route: &str) -> Self {
        Self {
            us: histogram("rest_us", &[("route", route)]),
            by_class: std::array::from_fn(|i| {
                counter("rest_requests", &[("route", route), ("status", CLASSES[i])])
            }),
        }
    }
}

static REST: LazyLock<Cached<RestHandles>> = LazyLock::new(|| Cached::new(256));

/// Labelled with the matched route template, never the raw path.
pub(crate) async fn rest_metrics(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map_or("other", |m| m.as_str());
    let h = REST.get(route, |r| Arc::new(RestHandles::new(r)));
    let t0 = Instant::now();
    let resp = next.run(req).await;
    h.us.observe_duration(t0.elapsed());
    let class = status_class(resp.status().as_u16());
    if let Some(i) = CLASSES.iter().position(|c| *c == class) {
        h.by_class[i].inc();
    }
    resp
}

// ---------------------------------------------------------------------------
// Fixed-name handles used by metadata.rs / scheduler / monitor
// ---------------------------------------------------------------------------

pub(crate) struct Mgr {
    pub raft_forward_us: Arc<Histogram>,
    pub raft_forward_failed: Arc<Counter>,
    pub raft_write_or_forward_us: Arc<Histogram>,
    pub forward_received: Arc<Counter>,
    pub alloc_chunks: Arc<Histogram>,
    pub commit_chunks: Arc<Histogram>,
    pub lease_granted: Arc<Counter>,
    pub lease_denied: Arc<Counter>,
    pub tasks_finished_done: Arc<Counter>,
    pub tasks_finished_failed: Arc<Counter>,
    pub sched_tick_us: Arc<Histogram>,
    pub sched_budget_hits: Arc<Counter>,
    pub orphan_runs_ok: Arc<Counter>,
    pub orphan_runs_failed: Arc<Counter>,
    pub orphan_candidates: Arc<Counter>,
    pub orphan_reaped: Arc<Counter>,
    pub orphan_freed_bytes: Arc<Counter>,
    pub orphan_run_us: Arc<Histogram>,
    pub tiering_tick_us: Arc<Histogram>,
    pub precache_tick_us: Arc<Histogram>,
    pub evict_tick_us: Arc<Histogram>,
    pub retention_tick_us: Arc<Histogram>,
    pub precache_bytes: Arc<Counter>,
    pub leader_changes: Arc<Counter>,
}

pub(crate) fn m() -> &'static Mgr {
    static M: OnceLock<Mgr> = OnceLock::new();
    M.get_or_init(|| Mgr {
        raft_forward_us: histogram("raft_forward_us", &[]),
        raft_forward_failed: counter("raft_forward_failed", &[]),
        raft_write_or_forward_us: histogram("raft_write_or_forward_us", &[]),
        forward_received: counter("raft_forward_received", &[]),
        alloc_chunks: histogram("write_alloc_chunks", &[]),
        commit_chunks: histogram("write_commit_chunks", &[]),
        lease_granted: counter("write_lease_granted", &[]),
        lease_denied: counter("write_lease_denied", &[]),
        tasks_finished_done: counter("scheduler_tasks_finished", &[("status", "done")]),
        tasks_finished_failed: counter("scheduler_tasks_finished", &[("status", "failed")]),
        sched_tick_us: histogram("scheduler_tick_us", &[]),
        sched_budget_hits: counter("scheduler_repair_budget_hits", &[]),
        orphan_runs_ok: counter("orphan_reap_runs", &[("result", "ok")]),
        orphan_runs_failed: counter("orphan_reap_runs", &[("result", "error")]),
        orphan_candidates: counter("orphan_reap_candidates", &[]),
        orphan_reaped: counter("orphan_reap_chunks", &[]),
        orphan_freed_bytes: counter("orphan_reap_freed_bytes", &[]),
        orphan_run_us: histogram("orphan_reap_us", &[]),
        tiering_tick_us: histogram("tiering_tick_us", &[]),
        precache_tick_us: histogram("precache_tick_us", &[]),
        evict_tick_us: histogram("tiering_capacity_evict_tick_us", &[]),
        retention_tick_us: histogram("tiering_retention_tick_us", &[]),
        precache_bytes: counter("precache_scheduled_bytes", &[]),
        leader_changes: counter("raft_leader_changes", &[]),
    })
}

/// `name{label=<value>}` counters, one cached handle per value.
pub(crate) struct CounterVec {
    name: &'static str,
    label: &'static str,
    cache: Cached<Counter>,
}

impl CounterVec {
    fn new(name: &'static str, label: &'static str, cap: usize) -> Self {
        Self { name, label, cache: Cached::new(cap) }
    }

    pub(crate) fn with(&self, v: &str) -> Arc<Counter> {
        self.cache.get(v, |k| counter(self.name, &[(self.label, k)]))
    }
}

macro_rules! counter_vec {
    ($id:ident, $name:expr, $label:expr, $cap:expr) => {
        pub(crate) static $id: LazyLock<CounterVec> = LazyLock::new(|| CounterVec::new($name, $label, $cap));
    };
}

counter_vec!(FORWARDED, "raft_forwarded", "op", 64);
counter_vec!(COMMIT_REJECTS, "write_commit_rejects", "reason", 16);
counter_vec!(ALLOC_REJECTS, "write_alloc_rejects", "reason", 16);
counter_vec!(TASKS_CREATED, "scheduler_tasks_created", "kind", 16);
counter_vec!(SCHED_OUTCOMES, "scheduler_replication_outcomes", "outcome", 16);
counter_vec!(PROMOTE_COPIES, "tiering_promote_copies", "dc", 64);
counter_vec!(PRECACHE_COPIES, "precache_copies", "dc", 64);
counter_vec!(CACHE_REMOVED, "cache_removed", "reason", 16);
counter_vec!(CACHE_FILL_COMMITS, "cache_fill_commits", "dc", 64);
counter_vec!(CACHE_FILL_BYTES, "cache_fill_bytes", "dc", 64);
counter_vec!(NODE_TRANSITIONS, "monitor_node_transitions", "to", 16);

// ---------------------------------------------------------------------------
// Leader-only gauges: zeroed when this manager stops being leader
// ---------------------------------------------------------------------------

static LEADER_GAUGES: LazyLock<Mutex<HashMap<String, Arc<Gauge>>>> = LazyLock::new(Default::default);

/// Registered once per `(name, labels)`; `clear_leader_gauges` zeroes them all.
/// Cold path only (per tick), it takes a lock.
pub(crate) fn leader_gauge(name: &'static str, labels: &[(&'static str, &str)]) -> Arc<Gauge> {
    let key = format!("{name}{labels:?}");
    let mut g = LEADER_GAUGES.lock().unwrap_or_else(|e| e.into_inner());
    g.entry(key).or_insert_with(|| gauge(name, labels)).clone()
}

pub(crate) fn clear_leader_gauges() {
    for g in LEADER_GAUGES.lock().unwrap_or_else(|e| e.into_inner()).values() {
        g.set(0);
    }
}

// ---------------------------------------------------------------------------
// Sampler (one task, off the hot path)
// ---------------------------------------------------------------------------

pub(crate) const SAMPLE_SECS: u64 = 5;

/// Leader changed to a different, known leader since the previous sample.
pub(crate) fn leader_changed(prev: Option<u64>, cur: Option<u64>) -> bool {
    cur.is_some() && cur != prev
}

/// Entries a follower is behind the leader's log; an unreported peer counts
/// from index 0.
pub(crate) fn follower_lag(last_log: u64, matched: Option<u64>) -> u64 {
    last_log.saturating_sub(matched.unwrap_or(0))
}

pub(crate) fn spawn_sampler(
    raft: Arc<DtfsRaft>,
    sm: Arc<RwLock<StateMachineData>>,
    memory: Arc<dtfs_common::memory::MemoryBudget>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(SAMPLE_SECS));
        let mut prev_leader: Option<u64> = None;
        let mut seen_health: std::collections::BTreeSet<String> = Default::default();
        loop {
            tick.tick().await;
            sample_raft(&raft, &mut prev_leader);
            sample_state(&sm, &mut seen_health).await;
            sample_memory(&memory);
        }
    })
}

fn sample_raft(raft: &DtfsRaft, prev_leader: &mut Option<u64>) {
    use openraft::async_runtime::watch::WatchReceiver;
    use openraft::Instant as _;
    let mt =raft.metrics().borrow_watched().clone();
    let last_log = mt.last_log_index.unwrap_or(0);
    let applied = mt.last_applied.as_ref().map_or(0, |l| l.index);
    let is_leader = mt.current_leader == Some(mt.id);
    gauge("raft_current_term", &[]).set(mt.current_term as i64);
    gauge("raft_last_log_index", &[]).set(last_log as i64);
    gauge("raft_last_applied", &[]).set(applied as i64);
    gauge("raft_apply_lag", &[]).set(last_log.saturating_sub(applied) as i64);
    gauge("raft_snapshot_index", &[]).set(mt.snapshot.as_ref().map_or(0, |l| l.index) as i64);
    gauge("raft_is_leader", &[]).set(is_leader as i64);
    gauge("raft_has_leader", &[]).set(mt.current_leader.is_some() as i64);
    if leader_changed(*prev_leader, mt.current_leader) {
        // The first observation after start is a change too.
        m().leader_changes.inc();
    }
    if mt.current_leader.is_some() {
        *prev_leader = mt.current_leader;
    }
    if is_leader {
        if let Some(rep) = &mt.replication {
            for (peer, lid) in rep {
                let lag = follower_lag(last_log, lid.as_ref().map(|l| l.index));
                leader_gauge("raft_follower_lag", &[("peer", &peer.to_string())]).set(lag as i64);
            }
        }
        if let Some(t) = &mt.last_quorum_acked {
            leader_gauge("raft_millis_since_quorum_ack", &[]).set(t.elapsed().as_millis() as i64);
        }
    } else {
        clear_leader_gauges();
    }
}

async fn sample_state(sm: &RwLock<StateMachineData>, seen: &mut std::collections::BTreeSet<String>) {
    let mut by_health: BTreeMap<String, i64> = BTreeMap::new();
    let (files, nodes_total);
    {
        let sm = sm.read().await;
        files = sm.files.len() as i64;
        nodes_total = sm.nodes.len() as i64;
        for n in sm.nodes.values() {
            *by_health.entry(format!("{:?}", n.health).to_lowercase()).or_default() += 1;
        }
    }
    gauge("sm_files", &[]).set(files);
    gauge("sm_nodes", &[]).set(nodes_total);
    seen.extend(by_health.keys().cloned());
    for h in seen.iter() {
        gauge("sm_nodes_by_health", &[("health", h)]).set(by_health.get(h).copied().unwrap_or(0));
    }
}

fn sample_memory(b: &dtfs_common::memory::MemoryBudget) {
    gauge("memory_rss_bytes", &[]).set(b.last_rss_bytes() as i64);
    gauge("memory_hard_cap_bytes", &[]).set(b.hard_cap_bytes() as i64);
    gauge("memory_warn_bytes", &[]).set(b.warn_threshold_bytes() as i64);
    gauge("memory_shed_bytes", &[]).set(b.shed_threshold_bytes() as i64);
    gauge("memory_pressure_level", &[]).set(b.pressure_level() as i64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::codegen::http::HeaderValue;

    #[test]
    fn method_of_path_maps_known_methods() {
        assert_eq!(method_of_path("/dtfs.manager.ManagerService/CreateFile"), "CreateFile");
        assert_eq!(method_of_path("/dtfs.manager.RaftTransport/RaftRpc"), "RaftRpc");
    }

    #[test]
    fn method_of_path_unknown_paths_are_other() {
        assert_eq!(method_of_path("nopath"), "other");
        assert_eq!(method_of_path("/dtfs.manager.ManagerService/"), "other");
        assert_eq!(method_of_path("/dtfs.manager.ManagerService/Evil123"), "other");
        assert_eq!(method_of_path("/other.Svc/CreateFile"), "other");
    }

    #[test]
    fn known_methods_match_the_proto() {
        let proto = include_str!("../../../proto/manager.proto");
        let n = proto.lines().filter(|l| l.trim_start().starts_with("rpc ")).count();
        assert_eq!(n, KNOWN_METHODS.len());
        for m in KNOWN_METHODS {
            assert!(proto.contains(&format!("rpc {m}(")), "{m}");
        }
    }

    #[test]
    fn code_labels_are_exact() {
        assert_eq!(code_label(tonic::Code::Unavailable), "unavailable");
        assert_eq!(code_label(tonic::Code::FailedPrecondition), "failed_precondition");
        assert_eq!(code_label(tonic::Code::DataLoss), "data_loss");
    }

    #[test]
    fn status_code_reads_grpc_status_header() {
        let mut h = HeaderMap::new();
        assert_eq!(status_code(&h), None);
        h.insert("grpc-status", HeaderValue::from_static("14"));
        assert_eq!(status_code(&h), Some(tonic::Code::Unavailable));
        h.insert("grpc-status", HeaderValue::from_static("junk"));
        assert_eq!(status_code(&h), None);
    }

    #[test]
    fn status_class_boundaries() {
        assert_eq!(status_class(199), "1xx");
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(599), "5xx");
        assert_eq!(status_class(600), "other");
        assert_eq!(status_class(99), "other");
    }

    #[test]
    fn leader_changed_only_on_new_known_leader() {
        assert!(leader_changed(None, Some(1)));
        assert!(leader_changed(Some(1), Some(2)));
        assert!(!leader_changed(Some(1), Some(1)));
        assert!(!leader_changed(Some(1), None));
    }

    #[test]
    fn follower_lag_saturates() {
        assert_eq!(follower_lag(100, Some(90)), 10);
        assert_eq!(follower_lag(100, None), 100);
        assert_eq!(follower_lag(5, Some(9)), 0);
    }

    #[test]
    fn cached_caps_distinct_keys() {
        let c: Cached<Counter> = Cached::new(2);
        let a = c.get("a", |_| Arc::default());
        c.get("b", |_| Arc::default());
        let o1 = c.get("c", |_| Arc::default());
        let o2 = c.get("d", |_| Arc::default());
        assert!(Arc::ptr_eq(&o1, &o2), "overflow keys share the other entry");
        assert!(Arc::ptr_eq(&a, &c.get("a", |_| Arc::default())));
    }

    #[test]
    fn leader_gauges_zero_on_clear() {
        let g = leader_gauge("test_leader_gauge", &[("peer", "9")]);
        g.set(7);
        assert!(Arc::ptr_eq(&g, &leader_gauge("test_leader_gauge", &[("peer", "9")])));
        clear_leader_gauges();
        assert_eq!(g.get(), 0);
    }

    /// Inner service answering with a trailers-only gRPC error.
    #[derive(Clone)]
    struct Fail(tonic::Code);
    impl Service<Request<()>> for Fail {
        type Response = Response<()>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Response<()>, Self::Error>>;
        fn poll_ready(&mut self, _: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: Request<()>) -> Self::Future {
            let mut r = Response::new(());
            r.headers_mut().insert("grpc-status", HeaderValue::from_str(&(self.0 as i32).to_string()).unwrap());
            std::future::ready(Ok(r))
        }
    }

    #[tokio::test]
    async fn layer_counts_request_latency_and_labels_error_code() {
        let mut svc = GrpcMetricsLayer.layer(Fail(tonic::Code::Unavailable));
        let req = || Request::builder().uri("/dtfs.manager.ManagerService/CreateLink").body(()).unwrap();
        svc.call(req()).await.unwrap();
        svc.call(req()).await.unwrap();
        let l = [("method", "CreateLink")];
        assert_eq!(counter("grpc_server_requests", &l).get(), 2);
        assert_eq!(gauge("grpc_server_inflight", &l).get(), 0);
        let ok = [("method", "CreateLink"), ("code", "unavailable")];
        assert_eq!(counter("grpc_server_errors", &ok).get(), 2);
        let other = [("method", "CreateLink"), ("code", "internal")];
        assert_eq!(counter("grpc_server_errors", &other).get(), 0);
    }

    #[tokio::test]
    async fn layer_does_not_count_ok_as_error() {
        let mut svc = GrpcMetricsLayer.layer(Fail(tonic::Code::Ok));
        svc.call(Request::builder().uri("/attacker/Mint1").body(()).unwrap()).await.unwrap();
        let ok = [("method", "other"), ("code", "ok")];
        assert_eq!(counter("grpc_server_errors", &ok).get(), 0);
        assert!(counter("grpc_server_requests", &[("method", "other")]).get() >= 1);
        assert_eq!(counter("grpc_server_requests", &[("method", "Mint1")]).get(), 0);
    }
}
