//! Issue #159 — in-process metrics (atomic counters/gauges/log2 histograms)
//! pushed to InfluxDB v1/v2 as line protocol, directly or (data/FUSE
//! `mode = "proxy"`) through a manager's `PushMetrics`. The hot path is
//! Relaxed atomics only; registration and the push loop are cold.
//! Credentials and payloads are never logged.

use crate::config::{MetricsCompression, MetricsConfig, MetricsKind, MetricsMode, MetricsServer};
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
            // A label named like a global tag would give a duplicate tag key, which InfluxDB rejects
            // (whole batch on v2): keep the global tag, rename the label.
            let mut tags: Vec<(String, String)> = s
                .tags
                .iter()
                .map(|(k, v)| if global.iter().any(|(g, _)| g == k) { (format!("label_{k}"), v.clone()) } else { (k.clone(), v.clone()) })
                .collect();
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
        self.trim()
    }
    /// Puts an unsent body back in front; returns batches dropped (oldest
    /// first) to stay under the cap.
    pub fn requeue_front(&mut self, b: String) -> u64 {
        self.bytes += b.len();
        self.q.push_front(b);
        self.trim()
    }
    /// Pops the oldest batches joined into one body of at most `max` bytes,
    /// split between batches; a single batch above `max` goes alone.
    pub fn take_body(&mut self, max: usize) -> Option<String> {
        let mut body = self.q.pop_front()?;
        self.bytes -= body.len();
        while self.q.front().is_some_and(|n| body.len() + n.len() <= max) {
            let n = self.q.pop_front().unwrap_or_default();
            self.bytes -= n.len();
            body.push_str(&n);
        }
        Some(body)
    }
    fn trim(&mut self) -> u64 {
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

/// `Err` carries the HTTP status and the (trimmed) response body, or the
/// transport error without its URL; neither holds a secret.
async fn post(client: &reqwest::Client, req: &PushRequest, body: Vec<u8>, gzip: bool) -> Result<(), (Option<u16>, String)> {
    let mut rb = client.post(&req.url).query(&req.query).body(body);
    if gzip {
        rb = rb.header("Content-Encoding", "gzip");
    }
    rb = match &req.auth {
        Auth::None => rb,
        Auth::Basic(u, p) => rb.basic_auth(u, Some(p)),
        Auth::Token(t) => rb.header("Authorization", format!("Token {t}")),
    };
    match rb.send().await {
        Ok(r) if r.status().is_success() => Ok(()),
        Ok(r) => {
            let status = r.status();
            let text = r.text().await.unwrap_or_default();
            Err((Some(status.as_u16()), format!("HTTP {status}: {}", text.chars().take(200).collect::<String>())))
        }
        Err(e) => Err((None, e.without_url().to_string())),
    }
}

/// A body the server refused as malformed or too large: retrying the same
/// bytes can never succeed, so it is dropped instead of blocking the queue.
fn rejected_for_good(status: Option<u16>) -> bool {
    matches!(status, Some(400 | 413 | 422))
}

/// Largest uncompressed POST / PushMetrics body: queued batches are joined
/// up to this, split between batches.
pub const MAX_BODY_BYTES: usize = 4 << 20;
/// Hard cap on a decompressed `PushMetrics` payload (gzip bomb guard).
pub const MAX_PROXY_DECODED_BYTES: usize = 16 << 20;
/// `metrics_dropped_batches` server label in proxy mode.
pub const PROXY_DEST: &str = "proxy";

pub fn gzip(data: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    // Writes into a Vec cannot fail.
    let _ = e.write_all(data);
    e.finish().unwrap_or_default()
}

/// Decompresses at most `cap` bytes: `BadEncoding` on corrupt input,
/// `TooLarge` past the cap. Never allocates past the cap.
pub fn gunzip_capped(data: &[u8], cap: usize) -> Result<Vec<u8>, ProxyReject> {
    use std::io::Read;
    let mut d = flate2::read::GzDecoder::new(data);
    let mut out = Vec::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = d.read(&mut buf).map_err(|_| ProxyReject::BadEncoding)?;
        if n == 0 {
            return Ok(out);
        }
        if out.len() + n > cap {
            return Err(ProxyReject::TooLarge);
        }
        if out.capacity() < out.len() + n {
            let want = (out.capacity() * 2).max(out.len() + n).min(cap);
            out.reserve_exact(want - out.len());
        }
        out.extend_from_slice(&buf[..n]);
    }
}

/// Why a manager refused a `PushMetrics` batch (`metrics_proxy_rejected{reason}`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyReject {
    Auth,
    NoServers,
    BadEncoding,
    TooLarge,
    BadLine,
}

impl ProxyReject {
    pub fn reason(self) -> &'static str {
        match self {
            Self::Auth => "auth",
            Self::NoServers => "no_servers",
            Self::BadEncoding => "bad_encoding",
            Self::TooLarge => "too_large",
            Self::BadLine => "bad_line",
        }
    }
}

/// MAC binding of a `PushMetrics` request: hex BLAKE3 of the payload as sent.
pub fn proxy_binding(payload: &[u8]) -> String {
    blake3::hash(payload).to_hex().to_string()
}

/// Manager side of `PushMetrics`: decode (`""` or `"gzip"`, capped at
/// [`MAX_PROXY_DECODED_BYTES`]) and check that it is UTF-8 and that every
/// non-empty line is a `dtfs_` point. The result ends with a newline.
pub fn decode_proxied(payload: &[u8], encoding: &str) -> Result<String, ProxyReject> {
    let raw = match encoding {
        "" if payload.len() > MAX_PROXY_DECODED_BYTES => return Err(ProxyReject::TooLarge),
        "" => payload.to_vec(),
        "gzip" => gunzip_capped(payload, MAX_PROXY_DECODED_BYTES)?,
        _ => return Err(ProxyReject::BadEncoding),
    };
    let mut s = String::from_utf8(raw).map_err(|_| ProxyReject::BadLine)?;
    if !s.lines().all(|l| l.is_empty() || l.starts_with("dtfs_")) {
        return Err(ProxyReject::BadLine);
    }
    if !s.is_empty() && !s.ends_with('\n') {
        s.push('\n'); // else the next batch's first line would join this one's last
    }
    Ok(s)
}

/// Proxy sender: a status code after which the node tries the next manager
/// (down, old manager without the RPC, manager without servers, timeout).
/// Auth and payload refusals would repeat on every manager.
pub fn proxy_try_next(code: tonic::Code) -> bool {
    use tonic::Code::*;
    matches!(code, Unavailable | Unimplemented | FailedPrecondition | DeadlineExceeded | Cancelled | Unknown)
}

/// Snapshots per send: `ceil(flush / push)`, 1 when `flush` is 0.
pub fn flush_every(push_interval_secs: u64, flush_interval_secs: u64) -> u64 {
    if flush_interval_secs == 0 { 1 } else { flush_interval_secs.div_ceil(push_interval_secs.max(1)) }
}

/// Pending batches per destination (one per `[[metrics.servers]]`, or one
/// `proxy`), shared by the push loop and a manager's `PushMetrics` handler.
pub struct Outbox {
    queues: Mutex<Vec<BatchQueue>>,
    dropped: Vec<Arc<Counter>>,
}

impl Outbox {
    pub fn new(dests: &[&str], cap_bytes: usize) -> Self {
        Self {
            queues: Mutex::new(dests.iter().map(|_| BatchQueue::new(cap_bytes)).collect()),
            dropped: dests.iter().map(|d| counter("metrics_dropped_batches", &[("server", d)])).collect(),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<BatchQueue>> {
        self.queues.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Queues `batch` for every destination, counting drop-oldest evictions.
    pub fn push(&self, batch: &str) {
        if batch.is_empty() {
            return;
        }
        let mut q = self.lock();
        for (i, q) in q.iter_mut().enumerate() {
            self.dropped[i].add(q.push(batch.to_string()));
        }
    }
    /// Next body of at most [`MAX_BODY_BYTES`] for destination `i`.
    pub fn take_body(&self, i: usize) -> Option<String> {
        self.lock().get_mut(i)?.take_body(MAX_BODY_BYTES)
    }
    fn requeue(&self, i: usize, body: String) {
        if let Some(q) = self.lock().get_mut(i) {
            self.dropped[i].add(q.requeue_front(body));
        }
    }
}

/// Proxy transport (data node, FUSE): sends `(payload, encoding)` to a
/// manager via `PushMetrics`, failing over through the managers.
pub type ProxySend = Arc<
    dyn Fn(Vec<u8>, &'static str) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
        + Send
        + Sync,
>;

pub fn proxy_send<F, Fut>(f: F) -> ProxySend
where
    F: Fn(Vec<u8>, &'static str) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    Arc::new(move |p, e| {
        Box::pin(f(p, e)) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>
    })
}

enum Dest {
    Direct(reqwest::Client, Vec<PushRequest>),
    Proxy(ProxySend),
}

/// Starts the push loop and returns its outbox; `None` when nothing is
/// pushed (direct with no servers, or proxy without a sender). Needs a
/// tokio runtime. Call after `cfg.validate()`.
pub fn spawn_pusher(cfg: MetricsConfig, proxy: Option<ProxySend>) -> Option<Arc<Outbox>> {
    let (dest, names): (Dest, Vec<&str>) = match (cfg.mode, proxy) {
        (MetricsMode::Proxy, Some(p)) => (Dest::Proxy(p), vec![PROXY_DEST]),
        (MetricsMode::Proxy, None) => return None,
        (MetricsMode::Direct, _) if cfg.servers.is_empty() => return None,
        (MetricsMode::Direct, _) => {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(cfg.timeout_secs))
                .build()
                .ok()?;
            (Dest::Direct(client, cfg.servers.iter().map(build_request).collect()), cfg.servers.iter().map(|s| s.name.as_str()).collect())
        }
    };
    let outbox = Arc::new(Outbox::new(&names, cfg.buffer_max_kb as usize * 1024));
    let names: Vec<String> = names.iter().map(|n| n.to_string()).collect();
    let ob = outbox.clone();
    tokio::spawn(async move {
        let gz = cfg.compression == MetricsCompression::Gzip;
        let sent_bytes = matches!(dest, Dest::Proxy(_)).then(|| counter("metrics_proxy_sent_bytes", &[]));
        let every = flush_every(cfg.push_interval_secs, cfg.flush_interval_secs);
        let mut failing = vec![false; names.len()];
        let mut enc = Encoder::default();
        let global = GLOBAL_TAGS.get().cloned().unwrap_or_default();
        let mut tick = tokio::time::interval(Duration::from_secs(cfg.push_interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for n in 1u64.. {
            tick.tick().await;
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            ob.push(&enc.encode(&snapshot_registry(), &global, ts));
            if n % every != 0 {
                continue;
            }
            for (i, name) in names.iter().enumerate() {
                while let Some(body) = ob.take_body(i) {
                    let payload = if gz { gzip(body.as_bytes()) } else { body.as_bytes().to_vec() };
                    let res = match &dest {
                        Dest::Direct(client, reqs) => {
                            post(client, &reqs[i], payload, gz).await
                        }
                        Dest::Proxy(send) => {
                            let len = payload.len() as u64;
                            let r = send(payload, if gz { "gzip" } else { "" }).await;
                            if let (Ok(()), Some(c)) = (&r, &sent_bytes) {
                                c.add(len);
                            }
                            r.map_err(|e| (None, e))
                        }
                    };
                    match res {
                        Err((status, e)) if rejected_for_good(status) => {
                            // dropped, not requeued: counted like a queue overflow
                            ob.dropped[i].inc();
                            tracing::warn!(server = %name, error = %e, "metrics batch rejected by the server, dropped");
                        }
                        Ok(()) if failing[i] => {
                            failing[i] = false;
                            tracing::warn!(server = %name, "metrics push recovered");
                        }
                        Ok(()) => {}
                        Err((_, e)) => {
                            ob.requeue(i, body);
                            if !failing[i] {
                                failing[i] = true;
                                tracing::warn!(server = %name, error = %e, "metrics push failing; batches are buffered and retried");
                            }
                            break;
                        }
                    }
                }
            }
        }
    });
    Some(outbox)
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
    fn label_named_like_a_global_tag_is_renamed_not_duplicated() {
        let mut enc = Encoder::default();
        let s = vec![Sample { name: "c".into(), tags: vec![("dc".into(), "toro".into())], value: Value::Counter(1) }];
        let g = vec![("dc".to_string(), "leli".to_string())];
        // InfluxDB: "duplicate tags" for `dc=toro,dc=leli`
        assert_eq!(enc.encode(&s, &g, 9), "dtfs_c,dc=leli,label_dc=toro value=1i 9\n");
    }

    #[test]
    fn only_malformed_body_statuses_are_dropped_not_retried() {
        for s in [400, 413, 422] {
            assert!(rejected_for_good(Some(s)), "{s}");
        }
        // auth, rate limit, server errors and transport errors are retried
        for s in [Some(401), Some(403), Some(404), Some(429), Some(500), Some(503), None] {
            assert!(!rejected_for_good(s), "{s:?}");
        }
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

    #[test]
    fn take_body_splits_at_4_mib_between_batches() {
        let half = MAX_BODY_BYTES / 2;
        let mut q = BatchQueue::new(64 << 20);
        q.push("a".repeat(half));
        q.push("b".repeat(half));
        q.push("c".into());
        let b = q.take_body(MAX_BODY_BYTES).unwrap();
        assert_eq!(b.len(), MAX_BODY_BYTES, "two halves fill exactly 4 MiB");
        assert_eq!(q.take_body(MAX_BODY_BYTES).unwrap(), "c");
        assert!(q.take_body(MAX_BODY_BYTES).is_none());
        // One byte over: split between the batches, never inside one.
        q.push("a".repeat(half));
        q.push("b".repeat(half + 1));
        assert_eq!(q.take_body(MAX_BODY_BYTES).unwrap().len(), half);
        assert_eq!(q.take_body(MAX_BODY_BYTES).unwrap().len(), half + 1);
        // A single batch above the limit goes alone.
        q.push("x".repeat(MAX_BODY_BYTES + 1));
        q.push("y".into());
        assert_eq!(q.take_body(MAX_BODY_BYTES).unwrap().len(), MAX_BODY_BYTES + 1);
        assert_eq!(q.take_body(MAX_BODY_BYTES).unwrap(), "y");
        assert!(q.is_empty());
    }

    #[test]
    fn requeue_front_keeps_order_and_cap() {
        let mut q = BatchQueue::new(10);
        q.push("bbbb".into());
        assert_eq!(q.requeue_front("aaaa".into()), 0);
        assert_eq!(q.take_body(100).unwrap(), "aaaabbbb");
        q.push("cccccc".into());
        assert_eq!(q.requeue_front("aaaaa".into()), 1, "11 > 10: the requeued (oldest) body is dropped");
        assert_eq!(q.front().unwrap(), "cccccc");
    }

    #[test]
    fn flush_accumulates_snapshots_into_one_body() {
        assert_eq!(flush_every(10, 0), 1);
        assert_eq!(flush_every(10, 10), 1);
        assert_eq!(flush_every(10, 30), 3);
        assert_eq!(flush_every(10, 25), 3, "rounded up to whole snapshots");
        let ob = Outbox::new(&["t_flush_a", "t_flush_b"], 1 << 20);
        for s in ["dtfs_a v=1i 1\n", "dtfs_a v=2i 2\n", "dtfs_a v=3i 3\n"] {
            ob.push(s);
        }
        ob.push("");
        for i in 0..2 {
            assert_eq!(ob.take_body(i).unwrap(), "dtfs_a v=1i 1\ndtfs_a v=2i 2\ndtfs_a v=3i 3\n");
            assert!(ob.take_body(i).is_none());
        }
        assert!(ob.take_body(2).is_none(), "unknown destination");
    }

    #[test]
    fn outbox_counts_drops_per_destination() {
        let ob = Outbox::new(&["t_drop_dest"], 8);
        ob.push("aaaa");
        ob.push("bbbb");
        ob.push("cccc");
        assert_eq!(counter("metrics_dropped_batches", &[("server", "t_drop_dest")]).get(), 1);
        ob.requeue(0, "dddddddd".into()); // 16 > 8: the requeued (oldest) body goes
        assert_eq!(counter("metrics_dropped_batches", &[("server", "t_drop_dest")]).get(), 2);
        assert_eq!(ob.take_body(0).unwrap(), "bbbbcccc");
    }

    #[test]
    fn gzip_round_trip() {
        let s = "dtfs_x,role=data value=1i 1\n".repeat(1000);
        let z = gzip(s.as_bytes());
        assert!(z.len() < s.len() / 10);
        assert_eq!(gunzip_capped(&z, MAX_PROXY_DECODED_BYTES).unwrap(), s.as_bytes());
        assert_eq!(decode_proxied(&z, "gzip").unwrap(), s);
    }

    #[test]
    fn gunzip_cap_boundary() {
        let at = gzip(&vec![b'a'; MAX_PROXY_DECODED_BYTES]);
        assert_eq!(gunzip_capped(&at, MAX_PROXY_DECODED_BYTES).unwrap().len(), MAX_PROXY_DECODED_BYTES);
        let over = gzip(&vec![b'a'; MAX_PROXY_DECODED_BYTES + 1]);
        assert_eq!(gunzip_capped(&over, MAX_PROXY_DECODED_BYTES), Err(ProxyReject::TooLarge));
        assert_eq!(decode_proxied(&over, "gzip"), Err(ProxyReject::TooLarge));
        assert_eq!(decode_proxied(&vec![b'a'; MAX_PROXY_DECODED_BYTES + 1], ""), Err(ProxyReject::TooLarge));
        assert_eq!(gunzip_capped(b"not gzip", 100), Err(ProxyReject::BadEncoding));
    }

    #[test]
    fn decode_proxied_line_checks() {
        assert_eq!(decode_proxied(b"dtfs_a v=1i 1\n\ndtfs_b v=2i 2", "").unwrap(), "dtfs_a v=1i 1\n\ndtfs_b v=2i 2\n");
        assert_eq!(decode_proxied(b"", "").unwrap(), "");
        assert_eq!(decode_proxied(b"dtfs_a v=1i 1\ncpu v=1i 1\n", ""), Err(ProxyReject::BadLine));
        assert_eq!(decode_proxied(b" dtfs_a v=1i 1\n", ""), Err(ProxyReject::BadLine));
        assert_eq!(decode_proxied(b"dtfs_a v=\xff 1\n", ""), Err(ProxyReject::BadLine), "not UTF-8");
        assert_eq!(decode_proxied(b"dtfs_a v=1i 1\n", "zstd"), Err(ProxyReject::BadEncoding));
        assert_eq!(decode_proxied(b"dtfs_a v=1i 1\n", "gzip"), Err(ProxyReject::BadEncoding), "plain bytes labelled gzip");
        let reasons: Vec<_> = [ProxyReject::Auth, ProxyReject::NoServers, ProxyReject::BadEncoding, ProxyReject::TooLarge, ProxyReject::BadLine]
            .iter()
            .map(|r| r.reason())
            .collect();
        assert_eq!(reasons, ["auth", "no_servers", "bad_encoding", "too_large", "bad_line"]);
    }

    #[test]
    fn proxy_failover_decision_per_code() {
        use tonic::Code::*;
        for c in [Unavailable, Unimplemented, FailedPrecondition, DeadlineExceeded, Cancelled, Unknown] {
            assert!(proxy_try_next(c), "{c:?}");
        }
        for c in [
            Ok,
            InvalidArgument,
            NotFound,
            AlreadyExists,
            PermissionDenied,
            ResourceExhausted,
            Aborted,
            OutOfRange,
            Internal,
            DataLoss,
            Unauthenticated,
        ] {
            assert!(!proxy_try_next(c), "{c:?}");
        }
    }

    #[test]
    fn proxy_binding_is_blake3_hex_of_payload() {
        assert_eq!(proxy_binding(b"abc"), blake3::hash(b"abc").to_hex().to_string());
        assert_eq!(proxy_binding(b"abc").len(), 64);
        assert_ne!(proxy_binding(b"abc"), proxy_binding(b"abd"));
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
