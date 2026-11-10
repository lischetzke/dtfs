//! Audit logging — Phase 3 (Clustering).
//!
//! Records security-relevant and administrative events such as node joins,
//! configuration changes, and access control decisions for compliance and
//! debugging purposes.
//!
//! Three layers, from cheapest to most durable:
//!
//! 1. **Local in-memory ring buffer** (this struct's `entries`). Fast queries
//!    inside the manager process; bounded to `max_entries` (default 10 000).
//! 2. **Daily-rotated JSONL archive** on local disk (`archive_dir`, P0-8).
//!    Survives ring-buffer eviction; per-manager only.
//! 3. **Replicated Raft audit log** (Bug AH). When `raft` is set, every
//!    `log()` call additionally spawns a tokio task that proposes a
//!    `ClientRequest::AppendAudit` write. The state machine applies it on
//!    every voter, so `/api/v1/audit` (which now reads from
//!    `StateMachineData.audit_log`) returns the same view from any manager.
//!    Spawn-and-forget with a 5 s timeout: audit fan-out failures never
//!    block the request that triggered the audit, and the local
//!    in-memory + JSONL paths still capture the event durably for
//!    forensics if Raft is unhealthy.

use std::collections::VecDeque;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// CR2-7-4 / MGR-14 — cap on concurrent in-flight `AppendAudit` Raft
/// proposals spawned by `AuditLog::log()`. Without a bound, flushing a
/// 10k-entry backlog (e.g. after a Raft outage) spawned thousands of
/// concurrent `client_write`s over the 40 Mbps LELI link at once. A `log()`
/// call whose fan-out slot is exhausted is dropped (never blocked) — the
/// local ring + JSONL archive stay authoritative for that event.
const AUDIT_FANOUT_MAX_INFLIGHT: usize = 16;

/// A single audit log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: i64,
    pub actor: String,
    pub action: String,
    pub target: String,
    pub result: String,
}

/// Optional filter for querying audit entries.
#[derive(Debug, Default)]
pub struct AuditFilter {
    /// Only return entries at or after this timestamp.
    pub since: Option<i64>,
    /// Only return entries at or before this timestamp.
    pub until: Option<i64>,
    /// Only return entries matching this action prefix.
    pub action: Option<String>,
    /// Maximum number of entries to return.
    pub limit: Option<usize>,
}

/// In-memory ring-buffer audit log with optional disk archive AND optional
/// cluster-wide Raft fan-out (Bug AH).
pub struct AuditLog {
    // CR2-7-4 / MGR-14 — a VecDeque ring so eviction is O(1)
    // pop_front() instead of Vec::remove(0)'s O(n) shift.
    entries: parking_lot::RwLock<VecDeque<AuditEntry>>,
    max_entries: usize,
    /// If set, every `log()` call appends the entry to
    /// `<archive_dir>/audit-YYYYMMDD.jsonl` so events survive ring-buffer
    /// eviction. Errors during archive writes are logged but do not fail the
    /// audit path (P0-8).
    archive_dir: Option<PathBuf>,
    /// Bug AH — when set, every `log()` call additionally spawns a
    /// tokio task that proposes `ClientRequest::AppendAudit` so all
    /// managers see the event via the replicated state-machine
    /// `audit_log`. Errors (ForwardToLeader during election, transport
    /// failure, timeout) are logged at debug level and dropped: the
    /// local ring + JSONL archive remain authoritative for forensics.
    fanout: Option<RaftFanout>,
}

/// CR2-7-4 / MGR-14 — the actual Raft dispatch is abstracted behind this
/// trait so the bounded-fan-out semaphore gate in `RaftFanout`/`AuditLog`
/// is unit-testable with a mock sink, without a live Raft node.
#[async_trait::async_trait]
trait FanoutSink: Send + Sync {
    async fn dispatch(&self, entry: &AuditEntry);
}

/// Bug AH — sends `AppendAudit` through the real Raft node.
struct RaftSink {
    raft: Arc<dtfs_raft::DtfsRaft>,
    /// Hostname of this manager, embedded as `origin_manager` so operators
    /// can tell which voter produced the event.
    origin_manager: String,
}

#[async_trait::async_trait]
impl FanoutSink for RaftSink {
    /// Propose `ClientRequest::AppendAudit` and wait up to 5 s for the
    /// Raft commit. On any error (ForwardToLeader on a follower, transport
    /// failure, timeout) we log at debug level and drop — the originator's
    /// local ring/archive still has the event for forensics. Audit fan-out
    /// is best-effort; never block a real request on its completion.
    async fn dispatch(&self, entry: &AuditEntry) {
        let req = dtfs_raft::ClientRequest::AppendAudit {
            actor: entry.actor.clone(),
            action: entry.action.clone(),
            target: entry.target.clone(),
            result: entry.result.clone(),
            origin_manager: self.origin_manager.clone(),
            // AuditEntry.timestamp is seconds-since-epoch; the replicated
            // entry stores ms (consistent with every other Raft-side
            // timestamp). Multiply at the boundary.
            timestamp_ms: (entry.timestamp as u64).saturating_mul(1_000),
        };
        let fut = self.raft.client_write(req);
        match tokio::time::timeout(Duration::from_secs(5), fut).await {
            Ok(Ok(_)) => { /* committed; followers will apply via Raft */ }
            Ok(Err(e)) => {
                // ForwardToLeader is the common case on followers — openraft
                // 0.10 in this codebase does NOT auto-forward; the leader's
                // own task is the one that proposes for everyone. Drop
                // quietly on followers; the leader fans out for them
                // anyway via its own AuditLog (every manager logs the
                // same event redundantly into the local ring, and the
                // leader's fan-out task wins). Bug AC verified the
                // ForwardToLeader-as-Unavailable shape so this match is
                // intentionally permissive.
                tracing::debug!(error = %e, "Bug AH audit fan-out failed (non-leader or transport)");
            }
            Err(_) => {
                tracing::debug!("Bug AH audit fan-out timed out after 5 s");
            }
        }
    }
}

/// Bug AH — bundle of state needed to propose an `AppendAudit` Raft write.
/// Cloned into the spawned fan-out task so the public `AuditLog::log()` stays
/// non-async and non-blocking.
///
/// CR2-7-4 / MGR-14 — `semaphore` bounds the number of `dispatch` tasks
/// in flight at once to `AUDIT_FANOUT_MAX_INFLIGHT`; `dropped` counts
/// `log()` calls that skipped fan-out because the bound was reached.
/// Both are `Arc`-shared so every clone (one per `log()` call) sees the
/// same bound and the same counter.
#[derive(Clone)]
struct RaftFanout {
    sink: Arc<dyn FanoutSink>,
    semaphore: Arc<tokio::sync::Semaphore>,
    dropped: Arc<AtomicU64>,
}

impl RaftFanout {
    fn new(sink: Arc<dyn FanoutSink>) -> Self {
        Self {
            sink,
            semaphore: Arc::new(tokio::sync::Semaphore::new(AUDIT_FANOUT_MAX_INFLIGHT)),
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    fn new_raft(raft: Arc<dtfs_raft::DtfsRaft>, origin_manager: String) -> Self {
        Self::new(Arc::new(RaftSink { raft, origin_manager }))
    }
}

impl AuditLog {
    /// Create a new audit log with the given maximum capacity (in-memory only).
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: parking_lot::RwLock::new(VecDeque::with_capacity(
                max_entries.min(1024),
            )),
            max_entries,
            archive_dir: None,
            fanout: None,
        }
    }

    /// Create a new audit log that also archives every entry to a daily-rotated
    /// JSONL file under `archive_dir`. The directory is created on first use.
    pub fn new_with_archive(max_entries: usize, archive_dir: PathBuf) -> Self {
        Self {
            entries: parking_lot::RwLock::new(VecDeque::with_capacity(
                max_entries.min(1024),
            )),
            max_entries,
            archive_dir: Some(archive_dir),
            fanout: None,
        }
    }

    /// Bug AH — convenience constructor: archive + Raft fan-out wired
    /// at creation time. Use this when the raft handle is available
    /// before the AuditLog is built (the common case in main.rs after
    /// the Bug AC startup ordering).
    pub fn new_with_archive_and_fanout(
        max_entries: usize,
        archive_dir: PathBuf,
        raft: Arc<dtfs_raft::DtfsRaft>,
        origin_manager: String,
    ) -> Self {
        Self {
            entries: parking_lot::RwLock::new(VecDeque::with_capacity(
                max_entries.min(1024),
            )),
            max_entries,
            archive_dir: Some(archive_dir),
            fanout: Some(RaftFanout::new_raft(raft, origin_manager)),
        }
    }

    /// Return the number of `log()` calls that skipped Raft fan-out because
    /// `AUDIT_FANOUT_MAX_INFLIGHT` in-flight `dispatch` tasks were already
    /// running. `0` when Raft fan-out is disabled or never saturated.
    /// CR2-7-4 / MGR-14 observability hook.
    #[cfg(test)]
    pub fn fanout_dropped_count(&self) -> u64 {
        self.fanout
            .as_ref()
            .map(|f| f.dropped.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Record an audit event.
    ///
    /// Three side effects, all best-effort:
    ///   1. Append to local in-memory ring (bounded by `max_entries`).
    ///   2. Append to daily-rotated JSONL archive on local disk if
    ///      `archive_dir` is set.
    ///   3. Spawn a tokio task that fans the event out to all managers
    ///      via Raft if `fanout` is set (Bug AH). The task is
    ///      detached — `log()` returns immediately so the caller is
    ///      never blocked on Raft latency or leader election.
    pub fn log(&self, actor: &str, action: &str, target: &str, result: &str) {
        let entry = AuditEntry {
            timestamp: chrono::Utc::now().timestamp(),
            actor: actor.to_string(),
            action: action.to_string(),
            target: target.to_string(),
            result: result.to_string(),
        };

        // Append to disk archive first (best-effort; errors don't fail audit).
        if let Some(dir) = &self.archive_dir {
            if let Err(e) = append_to_archive(dir, &entry) {
                tracing::error!(error = %e, "audit archive write failed");
            }
        }

        // Bug AH — fan out through Raft so every manager sees the event.
        // Spawn-and-forget; the local ring + archive keep the event safe
        // even if the Raft write never completes (split-brain, network
        // partition, etc.).
        //
        // CR2-7-4 / MGR-14 — bound concurrent in-flight dispatch tasks via
        // `try_acquire_owned`. A permit-holding `log()` spawns exactly one
        // task; a `log()` that finds the bound exhausted skips fan-out
        // entirely rather than queuing (never block the caller) — the
        // local ring + JSONL archive remain authoritative for that event.
        if let Some(fanout) = self.fanout.clone() {
            match fanout.semaphore.clone().try_acquire_owned() {
                Ok(permit) => {
                    let entry_for_fanout = entry.clone();
                    let sink = fanout.sink.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        sink.dispatch(&entry_for_fanout).await;
                    });
                }
                Err(_) => {
                    fanout.dropped.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        action = %entry.action,
                        "CR2-7-4 audit fan-out in-flight bound reached; \
                         skipping Raft replication for this entry"
                    );
                }
            }
        }

        let mut entries = self.entries.write();
        if entries.len() >= self.max_entries {
            entries.pop_front();
        }
        entries.push_back(entry);
    }

    /// Query audit entries using an optional filter.
    ///
    /// CF-4a — when a `limit` is set we return the NEWEST `limit` matching
    /// entries, newest-first, mirroring the replicated branch in
    /// `api::get_audit_log` (`sm.audit_log.iter().rev().filter(..).take`).
    /// Pre-CF-4a this truncated to the OLDEST N in insertion order, so a
    /// default-config cluster (local ring, `replicate_via_raft = false`)
    /// silently hid recent activity behind the limit while a replicated
    /// cluster showed it. Iterate the ring back-to-front so both branches
    /// agree.
    pub fn query(&self, filter: &AuditFilter) -> Vec<AuditEntry> {
        let entries = self.entries.read();
        let matches = |e: &AuditEntry| {
            if let Some(since) = filter.since {
                if e.timestamp < since {
                    return false;
                }
            }
            if let Some(until) = filter.until {
                if e.timestamp > until {
                    return false;
                }
            }
            if let Some(ref action) = filter.action {
                if !e.action.starts_with(action.as_str()) {
                    return false;
                }
            }
            true
        };

        let limit = filter.limit.unwrap_or(usize::MAX);
        entries
            .iter()
            .rev()
            .filter(|e| matches(e))
            .take(limit)
            .cloned()
            .collect()
    }

    /// Return the total number of entries currently stored.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new(10_000)
    }
}

/// Append a single audit entry to the daily-rotated JSONL archive under `dir`.
/// File name pattern: `audit-YYYYMMDD.jsonl` (UTC date). Errors propagate up to
/// the caller, which logs them but does not abort the audit path.
fn append_to_archive(dir: &std::path::Path, entry: &AuditEntry) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let date = chrono::DateTime::from_timestamp(entry.timestamp, 0)
        .map(|dt| dt.format("%Y%m%d").to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let path = dir.join(format!("audit-{date}.jsonl"));

    let line = serde_json::to_string(entry)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Query archived entries from disk, scanning daily files in `archive_dir`.
/// Returns up to `filter.limit` matching entries, oldest first.
#[cfg(test)]
pub fn query_archive(
    archive_dir: &std::path::Path,
    filter: &AuditFilter,
) -> std::io::Result<Vec<AuditEntry>> {
    use std::io::BufRead;
    if !archive_dir.exists() {
        return Ok(Vec::new());
    }
    let mut files: Vec<PathBuf> = std::fs::read_dir(archive_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("audit-") && n.ends_with(".jsonl"))
                .unwrap_or(false)
        })
        .collect();
    files.sort();

    let mut results: Vec<AuditEntry> = Vec::new();
    let limit = filter.limit.unwrap_or(usize::MAX);
    'outer: for path in files {
        let f = std::fs::File::open(&path)?;
        let reader = std::io::BufReader::new(f);
        for line in reader.lines() {
            let line = match line {
                Ok(l) if !l.is_empty() => l,
                _ => continue,
            };
            let entry: AuditEntry = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if let Some(since) = filter.since {
                if entry.timestamp < since {
                    continue;
                }
            }
            if let Some(until) = filter.until {
                if entry.timestamp > until {
                    continue;
                }
            }
            if let Some(action) = &filter.action {
                if !entry.action.starts_with(action.as_str()) {
                    continue;
                }
            }
            results.push(entry);
            if results.len() >= limit {
                break 'outer;
            }
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_archive_appends_and_rotates_by_date() {
        let tmp = tempfile::tempdir().unwrap();
        let archive_path = tmp.path().to_path_buf();
        let log = AuditLog::new_with_archive(5, archive_path.clone());
        log.log("test", "action.a", "tgt1", "ok");
        log.log("test", "action.b", "tgt2", "ok");

        let files: Vec<_> = std::fs::read_dir(&archive_path)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect();
        assert_eq!(files.len(), 1, "expected one daily archive file");

        let queried = query_archive(
            &archive_path,
            &AuditFilter {
                action: Some("action.".to_string()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(queried.len(), 2);
        assert_eq!(queried[0].action, "action.a");
        assert_eq!(queried[1].action, "action.b");
    }

    #[test]
    fn test_archive_survives_ring_eviction() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::new_with_archive(2, tmp.path().to_path_buf());
        log.log("t", "a", "t", "r");
        log.log("t", "b", "t", "r");
        log.log("t", "c", "t", "r"); // evicts "a" from in-memory
        log.log("t", "d", "t", "r"); // evicts "b" from in-memory

        assert_eq!(log.len(), 2);
        let archived = query_archive(tmp.path(), &AuditFilter::default()).unwrap();
        assert_eq!(archived.len(), 4); // all four survive on disk
    }

    /// Bug AH — `AuditLog::new()` (no Raft) must remain runnable from a
    /// synchronous test. The fan-out path is gated on `self.fanout` so
    /// when it's None we don't call `tokio::spawn` (which requires a
    /// runtime). Guards against future refactors that might
    /// unconditionally spawn.
    #[test]
    fn bug_ah_log_without_fanout_does_not_require_runtime() {
        let log = AuditLog::new(4);
        // If this calls tokio::spawn() unconditionally, the test panics
        // with "there is no reactor running".
        log.log("system", "test", "tgt", "ok");
        assert_eq!(log.len(), 1);
    }

    /// Bug AH — the same invariant for the archive-only constructor.
    #[test]
    fn bug_ah_log_with_archive_only_does_not_require_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let log = AuditLog::new_with_archive(4, tmp.path().to_path_buf());
        log.log("system", "test", "tgt", "ok");
        assert_eq!(log.len(), 1);
    }

    /// CF-4a — `query()` with a `limit` MUST return the NEWEST N matching
    /// entries, newest-first, to match the replicated-branch behaviour in
    /// `api::get_audit_log` (`sm.audit_log.iter().rev().take(limit)`).
    /// Pre-CF-4a it truncated to the OLDEST N (insertion order), so a
    /// default-config cluster silently hid recent activity behind the
    /// limit. Push more than N entries, query with limit=N, assert we get
    /// the most-recent N newest-first.
    #[test]
    fn cf_4a_query_limit_returns_newest_first() {
        let log = AuditLog::new(100);
        for i in 0..10 {
            log.log("api", &format!("action.{i}"), "tgt", "ok");
        }
        let got = log.query(&AuditFilter {
            limit: Some(3),
            ..Default::default()
        });
        let actions: Vec<&str> = got.iter().map(|e| e.action.as_str()).collect();
        assert_eq!(
            actions,
            vec!["action.9", "action.8", "action.7"],
            "limit must return the newest N entries, newest-first"
        );
    }

    /// CF-4a — the action prefix filter must compose with the newest-N
    /// limit: select the newest N entries that match the prefix, not the
    /// newest N overall then filter.
    #[test]
    fn cf_4a_query_limit_with_action_filter_returns_newest_matching() {
        let log = AuditLog::new(100);
        // Interleave matching and non-matching actions.
        for i in 0..10 {
            let action = if i % 2 == 0 {
                format!("config.{i}")
            } else {
                format!("node.{i}")
            };
            log.log("api", &action, "tgt", "ok");
        }
        let got = log.query(&AuditFilter {
            limit: Some(2),
            action: Some("config.".to_string()),
            ..Default::default()
        });
        let actions: Vec<&str> = got.iter().map(|e| e.action.as_str()).collect();
        assert_eq!(
            actions,
            vec!["config.8", "config.6"],
            "newest matching entries first, filtered before limit"
        );
    }

    /// CR2-7-4 / MGR-14 — a mock `FanoutSink` that tracks how many
    /// `dispatch` calls are concurrently in flight, so the semaphore bound
    /// is testable without a live Raft node.
    struct CountingSink {
        current: Arc<std::sync::atomic::AtomicUsize>,
        max_seen: Arc<std::sync::atomic::AtomicUsize>,
        total_dispatched: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl FanoutSink for CountingSink {
        async fn dispatch(&self, _entry: &AuditEntry) {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.current.fetch_add(1, SeqCst) + 1;
            self.max_seen.fetch_max(now, SeqCst);
            // Hold the "slot" long enough that a burst of log() calls
            // overlaps in flight.
            tokio::time::sleep(Duration::from_millis(30)).await;
            self.current.fetch_sub(1, SeqCst);
            self.total_dispatched.fetch_add(1, SeqCst);
        }
    }

    /// CR2-7-4 / MGR-14 — 100 concurrent `log()` calls with Raft fan-out
    /// enabled must never have more than `AUDIT_FANOUT_MAX_INFLIGHT`
    /// dispatch tasks running at once, and every call that could not get a
    /// slot must be counted as dropped rather than blocking `log()` (all
    /// 100 calls return immediately, synchronously, below).
    #[tokio::test]
    async fn mgr_14_fanout_bounded_to_semaphore_capacity() {
        let current = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let max_seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let total_dispatched = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let sink = Arc::new(CountingSink {
            current: current.clone(),
            max_seen: max_seen.clone(),
            total_dispatched: total_dispatched.clone(),
        });

        let mut log = AuditLog::new(1000);
        log.fanout = Some(RaftFanout::new(sink));

        for i in 0..100 {
            // Synchronous call — must never block waiting for a fan-out slot.
            log.log("api", &format!("action.{i}"), "tgt", "ok");
        }

        // Let the spawned dispatch tasks (and their sleeps) run to completion.
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(
            max_seen.load(std::sync::atomic::Ordering::SeqCst) <= AUDIT_FANOUT_MAX_INFLIGHT,
            "at most {AUDIT_FANOUT_MAX_INFLIGHT} dispatch tasks may run concurrently, saw {}",
            max_seen.load(std::sync::atomic::Ordering::SeqCst)
        );
        let dispatched = total_dispatched.load(std::sync::atomic::Ordering::SeqCst) as u64;
        let dropped = log.fanout_dropped_count();
        assert_eq!(
            dispatched + dropped,
            100,
            "every log() call either dispatched or was counted as dropped"
        );
        assert!(dropped > 0, "a burst of 100 calls must exhaust the {AUDIT_FANOUT_MAX_INFLIGHT}-slot bound");
        assert_eq!(log.len(), 100, "the local ring keeps every entry regardless of fan-out");
    }

    /// CR2-7-4 / MGR-14 — the ring eviction order is unchanged by the
    /// `Vec` -> `VecDeque` swap: the OLDEST entry is dropped first.
    #[test]
    fn mgr_14_vecdeque_ring_evicts_oldest_first() {
        let log = AuditLog::new(3);
        log.log("t", "a", "t", "r");
        log.log("t", "b", "t", "r");
        log.log("t", "c", "t", "r");
        log.log("t", "d", "t", "r"); // evicts "a"

        let all = log.query(&AuditFilter::default());
        let actions: Vec<&str> = all.iter().map(|e| e.action.as_str()).collect();
        assert_eq!(actions, vec!["d", "c", "b"], "newest-first, oldest ('a') evicted");
    }
}
