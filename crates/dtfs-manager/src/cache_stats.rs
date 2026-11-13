//! Cache observability: leader-local counters (memory only, reset on restart,
//! so plain Prometheus counters) and the per-disk view behind
//! `GET /api/v1/cache/status` and the `dtfs_cache_*` series on `/metrics`.
//! Locks here are never held across an await.

use std::collections::BTreeMap;
use std::time::Duration;

use dtfs_common::types::DiskTier;
use dtfs_raft::state_machine::StateMachineData;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use crate::scheduler::link_budget::LinkBacklog;
use crate::scheduler::TaskScheduler;

/// Per-disk `ListOldestChunks` budget; a slow node must not stall the status call.
const ORACLE_TIMEOUT: Duration = Duration::from_secs(2);
/// The leader refreshes the cached disk view (for `/metrics`) this often.
pub(crate) const VIEW_REFRESH_MS: u64 = 60_000;

/// Why a cache copy was removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Removed {
    EvictCapacity,
    Retention,
    IdleDemote,
}

const REMOVED_LABELS: [&str; 3] = ["evict_capacity", "retention", "idle_demote"];

/// Which planner scheduled a cache copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Scheduled {
    Promote,
    Precache,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Counters {
    pub removed: [u64; 3],
    /// Lowercased DC -> (commits, bytes).
    pub fill: BTreeMap<String, (u64, u64)>,
    /// Lowercased target DC -> copies scheduled.
    pub promote: BTreeMap<String, u64>,
    pub precache: BTreeMap<String, u64>,
}

impl Counters {
    pub(crate) fn to_json(&self) -> Value {
        let removed: BTreeMap<&str, u64> = REMOVED_LABELS.iter().copied().zip(self.removed).collect();
        let fill: BTreeMap<&String, Value> =
            self.fill.iter().map(|(dc, (n, b))| (dc, json!({"commits": n, "bytes": b}))).collect();
        let mut scheduled: BTreeMap<&String, Value> = BTreeMap::new();
        for dc in self.promote.keys().chain(self.precache.keys()) {
            scheduled.insert(
                dc,
                json!({"promote": self.promote.get(dc).unwrap_or(&0), "precache": self.precache.get(dc).unwrap_or(&0)}),
            );
        }
        json!({"removed": removed, "fill": fill, "scheduled": scheduled})
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiskRow {
    pub node_id: u64,
    pub hostname: String,
    pub dc: String,
    pub disk_id: u32,
    pub tier: String,
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub oldest_last_read_ms: Option<u64>,
    pub oldest_error: Option<String>,
}

impl DiskRow {
    pub(crate) fn used_pct(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            self.used_bytes as f64 * 100.0 / self.total_bytes as f64
        }
    }

    fn age_secs(&self, now_ms: u64) -> Option<u64> {
        self.oldest_last_read_ms.map(|t| now_ms.saturating_sub(t) / 1000)
    }

    pub(crate) fn to_json(&self, now_ms: u64) -> Value {
        json!({
            "node_id": self.node_id,
            "hostname": self.hostname,
            "dc": self.dc,
            "disk_id": self.disk_id,
            "tier": self.tier,
            "total_bytes": self.total_bytes,
            "used_bytes": self.used_bytes,
            "used_pct": self.used_pct(),
            "oldest_last_read_ms": self.oldest_last_read_ms,
            "oldest_last_read_age_secs": self.age_secs(now_ms),
            "oldest_error": self.oldest_error,
        })
    }
}

pub(crate) struct CacheStats {
    counters: Mutex<Counters>,
    /// (refreshed at ms, rows) of the last disk view; `/metrics` renders this.
    view: Mutex<(u64, Vec<DiskRow>)>,
}

/// Process-wide: the writers sit in the tiering engine and the metadata
/// service, which share no struct.
pub(crate) static STATS: CacheStats = CacheStats::new();

impl CacheStats {
    pub(crate) const fn new() -> Self {
        Self {
            counters: Mutex::new(Counters {
                removed: [0; 3],
                fill: BTreeMap::new(),
                promote: BTreeMap::new(),
                precache: BTreeMap::new(),
            }),
            view: Mutex::new((0, Vec::new())),
        }
    }

    pub(crate) fn note_removed(&self, r: Removed) {
        self.counters.lock().removed[r as usize] += 1;
    }

    pub(crate) fn note_fill(&self, dc: &str, bytes: u64) {
        let mut c = self.counters.lock();
        let e = c.fill.entry(dc.to_ascii_lowercase()).or_default();
        e.0 += 1;
        e.1 += bytes;
    }

    pub(crate) fn note_scheduled(&self, kind: Scheduled, dc: &str) {
        let mut c = self.counters.lock();
        let m = match kind {
            Scheduled::Promote => &mut c.promote,
            Scheduled::Precache => &mut c.precache,
        };
        *m.entry(dc.to_ascii_lowercase()).or_default() += 1;
    }

    pub(crate) fn counters(&self) -> Counters {
        self.counters.lock().clone()
    }

    pub(crate) fn set_view(&self, at_ms: u64, rows: Vec<DiskRow>) {
        *self.view.lock() = (at_ms, rows);
    }

    pub(crate) fn view(&self) -> (u64, Vec<DiskRow>) {
        self.view.lock().clone()
    }

    pub(crate) fn view_refreshed_at_ms(&self) -> u64 {
        self.view.lock().0
    }
}

/// One row per hot/warm disk of every node. The oldest last-read of each disk
/// is a live `ListOldestChunks(limit 1)`, issued concurrently; an error or a
/// timeout leaves it `None` with the reason in `oldest_error`. The state
/// machine lock is released before any RPC.
pub(crate) async fn collect_disks(sm: &RwLock<StateMachineData>, sched: &TaskScheduler) -> Vec<DiskRow> {
    let mut targets: Vec<(DiskRow, String)> = {
        let sm = sm.read().await;
        sm.nodes
            .iter()
            .flat_map(|(nid, n)| {
                n.disks.iter().filter(|d| matches!(d.tier, DiskTier::Hot | DiskTier::Warm)).map(move |d| {
                    let row = DiskRow {
                        node_id: *nid,
                        hostname: n.hostname.clone(),
                        dc: n.location.datacenter.clone(),
                        disk_id: d.disk_id,
                        tier: d.tier.to_string(),
                        total_bytes: d.total_bytes,
                        used_bytes: d.used_bytes,
                        oldest_last_read_ms: None,
                        oldest_error: None,
                    };
                    (row, n.grpc_addr.clone())
                })
            })
            .collect()
    };
    targets.sort_by_key(|(r, _)| (r.node_id, r.disk_id));
    futures::future::join_all(targets.into_iter().map(|(mut row, addr)| async move {
        match tokio::time::timeout(ORACLE_TIMEOUT, sched.list_oldest_chunks_on_node(&addr, row.disk_id, 1, 0, None)).await {
            Ok(Ok(v)) => row.oldest_last_read_ms = v.first().map(|c| c.last_read_ms),
            Ok(Err(e)) => row.oldest_error = Some(e.message().to_string()),
            Err(_) => row.oldest_error = Some(format!("no answer within {}s", ORACLE_TIMEOUT.as_secs())),
        }
        row
    }))
    .await
}

/// `dtfs_cache_*` and `dtfs_link_*` series. Disk series come from the cached
/// view (leader only; empty on a follower), so a scrape never fans out RPCs.
pub(crate) fn render_metrics(now_ms: u64, c: &Counters, disks: &[DiskRow], links: &[LinkBacklog]) -> String {
    use std::fmt::Write;
    let mut o = String::new();
    let head = |o: &mut String, name: &str, help: &str, ty: &str| {
        let _ = write!(o, "# HELP {name} {help}\n# TYPE {name} {ty}\n");
    };
    let lbl = |d: &DiskRow| format!("node=\"{}\",disk=\"{}\",dc=\"{}\",tier=\"{}\"", d.hostname, d.disk_id, d.dc, d.tier);
    head(&mut o, "dtfs_cache_disk_used_bytes", "Used bytes of a hot/warm disk (leader view, refreshed every 60 s).", "gauge");
    for d in disks {
        let _ = writeln!(o, "dtfs_cache_disk_used_bytes{{{}}} {}", lbl(d), d.used_bytes);
    }
    head(&mut o, "dtfs_cache_disk_total_bytes", "Total bytes of a hot/warm disk.", "gauge");
    for d in disks {
        let _ = writeln!(o, "dtfs_cache_disk_total_bytes{{{}}} {}", lbl(d), d.total_bytes);
    }
    head(&mut o, "dtfs_cache_oldest_last_read_age_seconds", "Age of the least recently read chunk on the disk; absent when unknown.", "gauge");
    for d in disks {
        if let Some(a) = d.age_secs(now_ms) {
            let _ = writeln!(o, "dtfs_cache_oldest_last_read_age_seconds{{{}}} {a}", lbl(d));
        }
    }
    head(&mut o, "dtfs_cache_removed_total", "Cache copies removed by this leader, by reason.", "counter");
    for (l, n) in REMOVED_LABELS.iter().zip(c.removed) {
        let _ = writeln!(o, "dtfs_cache_removed_total{{reason=\"{l}\"}} {n}");
    }
    head(&mut o, "dtfs_cache_fill_total", "Fill-on-read copies committed, by reader DC.", "counter");
    for (dc, (n, _)) in &c.fill {
        let _ = writeln!(o, "dtfs_cache_fill_total{{dc=\"{dc}\"}} {n}");
    }
    head(&mut o, "dtfs_cache_fill_bytes_total", "Bytes of fill-on-read copies committed, by reader DC.", "counter");
    for (dc, (_, b)) in &c.fill {
        let _ = writeln!(o, "dtfs_cache_fill_bytes_total{{dc=\"{dc}\"}} {b}");
    }
    head(&mut o, "dtfs_cache_scheduled_total", "Promotion and pre-cache copies scheduled, by target DC.", "counter");
    for (kind, m) in [("promote", &c.promote), ("precache", &c.precache)] {
        for (dc, n) in m {
            let _ = writeln!(o, "dtfs_cache_scheduled_total{{dc=\"{dc}\",kind=\"{kind}\"}} {n}");
        }
    }
    head(&mut o, "dtfs_link_admitted_bytes_total", "Bytes admitted onto a metered link, by kind (cache or repair).", "counter");
    for l in links {
        let _ = writeln!(o, "dtfs_link_admitted_bytes_total{{to_dc=\"{}\",kind=\"cache\"}} {}", l.to_dc, l.admitted_cache_bytes);
        let _ = writeln!(o, "dtfs_link_admitted_bytes_total{{to_dc=\"{}\",kind=\"repair\"}} {}", l.to_dc, l.admitted_repair_bytes);
    }
    head(&mut o, "dtfs_link_tokens_bytes", "Link budget balance (negative while repair has borrowed).", "gauge");
    for l in links {
        let _ = writeln!(o, "dtfs_link_tokens_bytes{{to_dc=\"{}\"}} {}", l.to_dc, l.tokens_bytes);
    }
    o
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_are_exact_per_reason_and_dc() {
        let s = CacheStats::new();
        s.note_removed(Removed::EvictCapacity);
        s.note_removed(Removed::IdleDemote);
        s.note_removed(Removed::IdleDemote);
        s.note_fill("TORO", 100);
        s.note_fill("toro", 50);
        s.note_fill("nrb", 7);
        s.note_scheduled(Scheduled::Promote, "Toro");
        s.note_scheduled(Scheduled::Precache, "toro");
        s.note_scheduled(Scheduled::Precache, "toro");
        let c = s.counters();
        assert_eq!(c.removed, [1, 0, 2]);
        assert_eq!(c.fill["toro"], (2, 150));
        assert_eq!(c.fill["nrb"], (1, 7));
        let j = c.to_json();
        assert_eq!(j["removed"], json!({"evict_capacity": 1, "retention": 0, "idle_demote": 2}));
        assert_eq!(j["fill"]["toro"], json!({"commits": 2, "bytes": 150}));
        assert_eq!(j["scheduled"]["toro"], json!({"promote": 1, "precache": 2}));
    }

    fn row(oldest: Option<u64>) -> DiskRow {
        DiskRow {
            node_id: 4,
            hostname: "data-warm01".into(),
            dc: "toro".into(),
            disk_id: 7,
            tier: "warm".into(),
            total_bytes: 1000,
            used_bytes: 250,
            oldest_last_read_ms: oldest,
            oldest_error: oldest.is_none().then(|| "boom".to_string()),
        }
    }

    #[test]
    fn disk_json_age_and_pct_and_null_on_error() {
        let j = row(Some(1_000_000)).to_json(1_000_000 + 14 * 86_400_000);
        assert_eq!(j["used_pct"], json!(25.0));
        assert_eq!(j["oldest_last_read_age_secs"], json!(14 * 86_400));
        assert_eq!(j["oldest_error"], Value::Null);
        let j = row(None).to_json(5);
        assert_eq!(j["oldest_last_read_ms"], Value::Null);
        assert_eq!(j["oldest_last_read_age_secs"], Value::Null);
        assert_eq!(j["oldest_error"], "boom");
        // A clock behind the stamp saturates to 0, never wraps.
        assert_eq!(row(Some(10)).to_json(5)["oldest_last_read_age_secs"], json!(0));
    }

    #[test]
    fn metrics_carry_every_new_series() {
        let s = CacheStats::new();
        s.note_removed(Removed::Retention);
        s.note_fill("toro", 64);
        let links = vec![LinkBacklog {
            to_dc: "toro".into(),
            max_mbps: 40,
            tokens_bytes: -5,
            skipped_requests: 0,
            skipped_bytes: 0,
            admitted_cache_bytes: 11,
            admitted_repair_bytes: 22,
        }];
        let body = render_metrics(1_000_000 + 3_000, &s.counters(), &[row(Some(1_000_000)), row(None)], &links);
        for want in [
            "dtfs_cache_disk_used_bytes{node=\"data-warm01\",disk=\"7\",dc=\"toro\",tier=\"warm\"} 250",
            "dtfs_cache_disk_total_bytes{node=\"data-warm01\",disk=\"7\",dc=\"toro\",tier=\"warm\"} 1000",
            "dtfs_cache_oldest_last_read_age_seconds{node=\"data-warm01\",disk=\"7\",dc=\"toro\",tier=\"warm\"} 3",
            "dtfs_cache_removed_total{reason=\"retention\"} 1",
            "dtfs_cache_removed_total{reason=\"evict_capacity\"} 0",
            "dtfs_cache_fill_total{dc=\"toro\"} 1",
            "dtfs_cache_fill_bytes_total{dc=\"toro\"} 64",
            "dtfs_link_admitted_bytes_total{to_dc=\"toro\",kind=\"cache\"} 11",
            "dtfs_link_admitted_bytes_total{to_dc=\"toro\",kind=\"repair\"} 22",
            "dtfs_link_tokens_bytes{to_dc=\"toro\"} -5",
        ] {
            assert!(body.contains(want), "missing {want}\n{body}");
        }
        // The unknown-oldest disk emits no age series (only the one with a value).
        assert_eq!(body.matches("dtfs_cache_oldest_last_read_age_seconds{").count(), 1);
    }
}
