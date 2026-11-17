//! Issue #159 — in-process metrics (atomic counters/gauges/log2 histograms)
//! pushed to InfluxDB v1/v2 as line protocol. The hot path is Relaxed atomics
//! only; registration and the push loop are cold. Credentials are never logged.

use crate::config::{MetricsConfig, MetricsKind, MetricsServer};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Bucket i holds values with `64 - leading_zeros == i`: 0, 1, 2..=3, 4..=7, ...
/// The last bucket absorbs everything above (2^38 us is ~3 days).
pub const BUCKETS: usize = 40;

#[derive(Default)]
pub struct Counter(AtomicU64);
impl Counter {
    pub fn inc(&self) {
        self.0.fetch_add(1, Relaxed);
    }
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Relaxed)
    }
}

#[derive(Default)]
pub struct Gauge(AtomicI64);
impl Gauge {
    pub fn set(&self, v: i64) {
        self.0.store(v, Relaxed);
    }
    pub fn add(&self, v: i64) {
        self.0.fetch_add(v, Relaxed);
    }
    pub fn get(&self) -> i64 {
        self.0.load(Relaxed)
    }
}

pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }
}

pub fn bucket_index(v: u64) -> usize {
    ((64 - v.leading_zeros()) as usize).min(BUCKETS - 1)
}

/// Largest value that lands in bucket `i`.
pub fn bucket_upper(i: usize) -> u64 {
    if i == 0 {
        0
    } else if i >= BUCKETS - 1 {
        u64::MAX
    } else {
        (1u64 << i) - 1
    }
}

impl Histogram {
    pub fn observe(&self, v: u64) {
        self.buckets[bucket_index(v)].fetch_add(1, Relaxed);
        self.count.fetch_add(1, Relaxed);
        self.sum.fetch_add(v, Relaxed);
        self.max.fetch_max(v, Relaxed);
    }
    pub fn count(&self) -> u64 {
        self.count.load(Relaxed)
    }
    /// Records microseconds.
    pub fn observe_duration(&self, d: Duration) {
        self.observe(d.as_micros().min(u64::MAX as u128) as u64);
    }
    /// `max` is per push interval: it is reset by the pusher's snapshot.
    fn snapshot(&self) -> HistSnap {
        HistSnap {
            count: self.count.load(Relaxed),
            sum: self.sum.load(Relaxed),
            max: self.max.swap(0, Relaxed),
            buckets: std::array::from_fn(|i| self.buckets[i].load(Relaxed)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistSnap {
    pub count: u64,
    pub sum: u64,
    pub max: u64,
    pub buckets: [u64; BUCKETS],
}

/// Upper bound of the bucket where the cumulative count crosses `pct`% of the
/// total. Returns 0 when `total` is 0.
pub fn quantile(buckets: &[u64; BUCKETS], total: u64, pct: u64) -> u64 {
    if total == 0 {
        return 0;
    }
    let rank = (total * pct).div_ceil(100).max(1);
    let mut cum = 0;
    for (i, n) in buckets.iter().enumerate() {
        cum += n;
        if cum >= rank {
            return bucket_upper(i);
        }
    }
    u64::MAX
}

type Tags = Vec<(String, String)>;

#[derive(Clone)]
enum Metric {
    Counter(Arc<Counter>),
    Gauge(Arc<Gauge>),
    Histogram(Arc<Histogram>),
}

type Registry = Mutex<BTreeMap<(String, Tags), Metric>>;

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(Default::default)
}

static GLOBAL_TAGS: OnceLock<Tags> = OnceLock::new();

/// Set once at startup; later calls are ignored.
pub fn set_global_tags(role: &str, node: &str, dc: &str) {
    let _ = GLOBAL_TAGS.set(vec![
        ("role".into(), role.into()),
        ("node".into(), node.into()),
        ("dc".into(), dc.into()),
    ]);
}

fn register(name: &str, labels: &[(&'static str, &str)], make: Metric) -> Metric {
    let mut tags: Tags = labels.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    tags.sort();
    let mut r = registry().lock().unwrap_or_else(|e| e.into_inner());
    r.entry((name.to_string(), tags)).or_insert(make).clone()
}

/// Cache the returned Arc; this takes a lock.
pub fn counter(name: &str, labels: &[(&'static str, &str)]) -> Arc<Counter> {
    match register(name, labels, Metric::Counter(Arc::default())) {
        Metric::Counter(c) => c,
        _ => Arc::default(), // name reused with another kind: detached, never pushed
    }
}

pub fn gauge(name: &str, labels: &[(&'static str, &str)]) -> Arc<Gauge> {
    match register(name, labels, Metric::Gauge(Arc::default())) {
        Metric::Gauge(g) => g,
        _ => Arc::default(),
    }
}

pub fn histogram(name: &str, labels: &[(&'static str, &str)]) -> Arc<Histogram> {
    match register(name, labels, Metric::Histogram(Arc::default())) {
        Metric::Histogram(h) => h,
        _ => Arc::default(),
    }
}

pub enum Value {
    Counter(u64),
    Gauge(i64),
    Hist(HistSnap),
}

pub struct Sample {
    pub name: String,
    pub tags: Tags,
    pub value: Value,
}

fn snapshot_registry() -> Vec<Sample> {
    let r = registry().lock().unwrap_or_else(|e| e.into_inner());
    r.iter()
        .map(|((name, tags), m)| Sample {
            name: name.clone(),
            tags: tags.clone(),
            value: match m {
                Metric::Counter(c) => Value::Counter(c.get()),
                Metric::Gauge(g) => Value::Gauge(g.get()),
                Metric::Histogram(h) => Value::Hist(h.snapshot()),
            },
        })
        .collect()
}

fn esc(s: &str, extra_eq: bool) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        if c == ',' || c == ' ' || (extra_eq && c == '=') {
            o.push('\\');
        }
        o.push(c);
    }
    o
}

/// `dtfs_<name>,k=v,... field=1i,... <ts>`. Tags are sorted; empty values
/// are omitted (InfluxDB rejects them).
pub fn encode_line(name: &str, tags: &[(String, String)], fields: &[(&str, i64)], ts_ns: u128) -> String {
    let mut t: Vec<&(String, String)> = tags.iter().filter(|(_, v)| !v.is_empty()).collect();
    t.sort();
    let mut s = format!("dtfs_{}", esc(name, false));
    for (k, v) in t {
        s.push_str(&format!(",{}={}", esc(k, true), esc(v, true)));
    }
    s.push(' ');
    let f: Vec<String> = fields.iter().map(|(k, v)| format!("{k}={v}i")).collect();
    s.push_str(&f.join(","));
    s.push_str(&format!(" {ts_ns}\n"));
    s
}

/// Keeps the previous histogram snapshots so quantiles cover one interval.
#[derive(Default)]
pub struct Encoder {
    prev: HashMap<(String, Tags), HistSnap>,
}

impl Encoder {
    pub fn encode(&mut self, samples: &[Sample], global: &[(String, String)], ts_ns: u128) -> String {
        let mut out = String::new();
        for s in samples {
            let mut tags = s.tags.clone();
            tags.extend(global.iter().cloned());
            let i = |v: u64| v.min(i64::MAX as u64) as i64;
            match &s.value {
                Value::Counter(v) => out.push_str(&encode_line(&s.name, &tags, &[("value", i(*v))], ts_ns)),
                Value::Gauge(v) => out.push_str(&encode_line(&s.name, &tags, &[("value", *v)], ts_ns)),
                Value::Hist(h) => {
                    let key = (s.name.clone(), s.tags.clone());
                    let prev = self.prev.insert(key, h.clone());
                    let mut delta = h.buckets;
                    let mut dcount = h.count;
                    if let Some(p) = &prev {
                        for (d, pb) in delta.iter_mut().zip(p.buckets.iter()) {
                            *d = d.saturating_sub(*pb);
                        }
                        dcount = h.count.saturating_sub(p.count);
                    }
                    let mut f = vec![("count", i(h.count)), ("sum", i(h.sum))];
                    if dcount > 0 {
                        // Clamp to the interval max: a bucket bound can exceed what was seen.
                        let q = |p| quantile(&delta, dcount, p).min(h.max);
                        f.extend([("p50", i(q(50))), ("p90", i(q(90))), ("p99", i(q(99))), ("max", i(h.max))]);
                    }
                    out.push_str(&encode_line(&s.name, &tags, &f, ts_ns));
                }
            }
        }
        out
    }
}

/// Bounded FIFO of pending batches, capped by total bytes.
pub struct BatchQueue {
    cap: usize,
    bytes: usize,
    q: VecDeque<String>,
}

impl BatchQueue {
    pub fn new(cap_bytes: usize) -> Self {
        Self { cap: cap_bytes, bytes: 0, q: VecDeque::new() }
    }
    /// Returns how many batches were dropped (oldest first; the new batch
    /// itself when it alone exceeds the cap).
    pub fn push(&mut self, b: String) -> u64 {
        self.bytes += b.len();
        self.q.push_back(b);
        let mut dropped = 0;
        while self.bytes > self.cap {
            match self.q.pop_front() {
                Some(old) => {
                    self.bytes -= old.len();
                    dropped += 1;
                }
                None => break,
            }
        }
        dropped
    }
    pub fn front(&self) -> Option<&String> {
        self.q.front()
    }
    pub fn pop(&mut self) {
        if let Some(old) = self.q.pop_front() {
            self.bytes -= old.len();
        }
    }
    pub fn len(&self) -> usize {
        self.q.len()
    }
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }
}

pub enum Auth {
    None,
    Basic(String, String),
    Token(String),
}

/// Where and how to POST a batch. `url` carries no query and no secret.
pub struct PushRequest {
    pub url: String,
    pub query: Vec<(&'static str, String)>,
    pub auth: Auth,
}

pub fn build_request(s: &MetricsServer) -> PushRequest {
    let base = s.url.trim_end_matches('/');
    let g = |o: &Option<String>| o.clone().unwrap_or_default();
    match s.kind {
        MetricsKind::InfluxdbV1 => PushRequest {
            url: format!("{base}/write"),
            query: vec![("db", g(&s.database)), ("precision", "ns".into())],
            auth: match s.username.as_deref() {
                Some(u) if !u.is_empty() => Auth::Basic(u.to_string(), g(&s.password)),
                _ => Auth::None,
            },
        },
        MetricsKind::InfluxdbV2 => PushRequest {
            url: format!("{base}/api/v2/write"),
            query: vec![("org", g(&s.org)), ("bucket", g(&s.bucket)), ("precision", "ns".into())],
            auth: Auth::Token(g(&s.token)),
        },
    }
}

async fn post(client: &reqwest::Client, req: &PushRequest, body: String) -> bool {
    let mut rb = client.post(&req.url).query(&req.query).body(body);
    rb = match &req.auth {
        Auth::None => rb,
        Auth::Basic(u, p) => rb.basic_auth(u, Some(p)),
        Auth::Token(t) => rb.header("Authorization", format!("Token {t}")),
    };
    matches!(rb.send().await, Ok(r) if r.status().is_success())
}

/// Starts the push loop; `None` when no servers are configured. Needs a
/// tokio runtime. Call after `cfg.validate()`.
pub fn spawn_pusher(cfg: MetricsConfig) -> Option<tokio::task::JoinHandle<()>> {
    if cfg.servers.is_empty() {
        return None;
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()
        .ok()?;
    Some(tokio::spawn(async move {
        let dropped_ctr: Vec<_> = cfg
            .servers
            .iter()
            .map(|s| counter("metrics_dropped_batches", &[("server", &s.name)]))
            .collect();
        let reqs: Vec<_> = cfg.servers.iter().map(build_request).collect();
        let mut queues: Vec<_> = cfg.servers.iter().map(|_| BatchQueue::new(cfg.buffer_max_kb as usize * 1024)).collect();
        let mut failing = vec![false; cfg.servers.len()];
        let mut enc = Encoder::default();
        let global = GLOBAL_TAGS.get().cloned().unwrap_or_default();
        let mut tick = tokio::time::interval(Duration::from_secs(cfg.push_interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let batch = enc.encode(&snapshot_registry(), &global, ts);
            for (i, srv) in cfg.servers.iter().enumerate() {
                if !batch.is_empty() {
                    let d = queues[i].push(batch.clone());
                    dropped_ctr[i].add(d);
                }
                while let Some(b) = queues[i].front() {
                    if post(&client, &reqs[i], b.clone()).await {
                        queues[i].pop();
                        if failing[i] {
                            failing[i] = false;
                            tracing::warn!(server = %srv.name, "metrics push recovered");
                        }
                    } else {
                        if !failing[i] {
                            failing[i] = true;
                            tracing::warn!(server = %srv.name, "metrics push failing; batches are buffered and retried");
                        }
                        break;
                    }
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_boundaries() {
        assert_eq!(bucket_index(0), 0);
        assert_eq!(bucket_index(1), 1);
        assert_eq!(bucket_index(2), 2);
        assert_eq!(bucket_index(3), 2);
        assert_eq!(bucket_index(4), 3);
        for k in 1..(BUCKETS as u32 - 1) {
            assert_eq!(bucket_index((1u64 << k) - 1), k as usize, "2^{k}-1");
            assert_eq!(bucket_index(1u64 << k), k as usize + 1, "2^{k}");
        }
        assert_eq!(bucket_index(u64::MAX), BUCKETS - 1);
        assert_eq!(bucket_upper(0), 0);
        assert_eq!(bucket_upper(3), 7);
        assert_eq!(bucket_upper(BUCKETS - 1), u64::MAX);
    }

    #[test]
    fn counter_gauge_and_registry_identity() {
        let a = counter("t_ident", &[("k", "v")]);
        a.inc();
        a.add(4);
        let b = counter("t_ident", &[("k", "v")]);
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(b.get(), 5);
        assert!(!Arc::ptr_eq(&a, &counter("t_ident", &[("k", "w")])));
        let g = gauge("t_gauge", &[]);
        g.set(10);
        g.add(-15);
        assert_eq!(g.get(), -5);
    }

    fn snap(h: &Histogram) -> HistSnap {
        h.snapshot()
    }

    #[test]
    fn delta_quantiles_and_empty_delta() {
        let h = Histogram::default();
        for _ in 0..98 {
            h.observe(10); // bucket 4, upper 15
        }
        h.observe(1000); // bucket 10, upper 1023
        h.observe(1000);
        let s = Value::Hist(snap(&h));
        let mut enc = Encoder::default();
        let sample = |v| vec![Sample { name: "h".into(), tags: vec![], value: v }];
        let out = enc.encode(&sample(s), &[], 1);
        assert_eq!(out, "dtfs_h count=100i,sum=2980i,p50=15i,p90=15i,p99=1000i,max=1000i 1\n");
        // Second interval: only fast values; the old slow ones must not count.
        for _ in 0..10 {
            h.observe(3);
        }
        let out = enc.encode(&sample(Value::Hist(snap(&h))), &[], 2);
        assert_eq!(out, "dtfs_h count=110i,sum=3010i,p50=3i,p90=3i,p99=3i,max=3i 2\n");
        // Idle interval: cumulative fields only.
        let out = enc.encode(&sample(Value::Hist(snap(&h))), &[], 3);
        assert_eq!(out, "dtfs_h count=110i,sum=3010i 3\n");
    }

    #[test]
    fn line_escaping_and_exact_output() {
        let tags = vec![
            ("z".to_string(), "a b".to_string()),
            ("a,k=".to_string(), "x,y=z".to_string()),
            ("empty".to_string(), String::new()),
        ];
        let l = encode_line("m n,o=p", &tags, &[("value", -3)], 42);
        assert_eq!(l, "dtfs_m\\ n\\,o=p,a\\,k\\==x\\,y\\=z,z=a\\ b value=-3i 42\n");
    }

    #[test]
    fn counter_and_gauge_lines_carry_global_tags() {
        let mut enc = Encoder::default();
        let s = vec![
            Sample { name: "c".into(), tags: vec![], value: Value::Counter(7) },
            Sample { name: "g".into(), tags: vec![("op".into(), "x".into())], value: Value::Gauge(-1) },
        ];
        let g = vec![("role".to_string(), "manager".to_string())];
        assert_eq!(
            enc.encode(&s, &g, 9),
            "dtfs_c,role=manager value=7i 9\ndtfs_g,op=x,role=manager value=-1i 9\n"
        );
    }

    #[test]
    fn queue_drops_oldest_at_byte_cap() {
        let mut q = BatchQueue::new(10);
        assert_eq!(q.push("aaaa".into()), 0);
        assert_eq!(q.push("bbbb".into()), 0); // 8 bytes
        assert_eq!(q.push("cccc".into()), 1); // 12 > 10: drop "aaaa"
        assert_eq!(q.len(), 2);
        assert_eq!(q.front().unwrap(), "bbbb");
        assert_eq!(q.push("x".repeat(11)), 3); // oversized batch evicts everything, itself included
        assert!(q.is_empty());
        q.push("dd".into());
        q.pop();
        assert!(q.is_empty());
    }

    fn server(kind: MetricsKind) -> MetricsServer {
        MetricsServer {
            name: "s".into(),
            kind,
            url: "http://h:8086/".into(),
            database: Some("db".into()),
            username: Some("user".into()),
            password: Some("hunter2".into()),
            org: Some("o".into()),
            bucket: Some("b".into()),
            token: Some("tok-secret".into()),
        }
    }

    #[test]
    fn request_builder_keeps_credentials_out_of_url() {
        let v1 = build_request(&server(MetricsKind::InfluxdbV1));
        assert_eq!(v1.url, "http://h:8086/write");
        assert_eq!(v1.query, vec![("db", "db".to_string()), ("precision", "ns".to_string())]);
        assert!(matches!(&v1.auth, Auth::Basic(u, p) if u == "user" && p == "hunter2"));
        let v2 = build_request(&server(MetricsKind::InfluxdbV2));
        assert_eq!(v2.url, "http://h:8086/api/v2/write");
        assert_eq!(
            v2.query,
            vec![("org", "o".to_string()), ("bucket", "b".to_string()), ("precision", "ns".to_string())]
        );
        assert!(matches!(&v2.auth, Auth::Token(t) if t == "tok-secret"));
        for r in [&v1, &v2] {
            let all = format!("{}{:?}", r.url, r.query);
            assert!(!all.contains("hunter2") && !all.contains("tok-secret"));
        }
        let mut s = server(MetricsKind::InfluxdbV1);
        s.username = None;
        assert!(matches!(build_request(&s).auth, Auth::None));
    }
}
