//! Capacity eviction: when a hot/warm data disk reaches `[tiering]
//! cache_high_watermark_pct`, remove its least recently read cache copies
//! until the disk is back at `cache_low_watermark_pct`. Leader-only, run from
//! the 30 s pre-cache loop. Removes cache replicas only, with the idle
//! demote's safety checks (cache tier, mandatory survivor, anchor); never a
//! `precache.pins` copy.
//!
//! Retention demotion (same loop, every 10 min per disk): cache copies of a
//! volume with `cache_replicas.retention_secs` set that the disk's last-read
//! index reports unread for that long go, with the same per-row checks;
//! pre-cache protected copies and pins stay.

use std::collections::{BTreeMap, HashMap, HashSet};

use dtfs_common::types::DiskTier;
use dtfs_proto::dtfs::data::OldestChunk;
use dtfs_raft::state_machine::StateMachineData;
use tracing::{debug, info, warn};

use super::{
    demote_preserves_anchor, is_cache_tier, mandatory_survives, now_millis, tick_placement,
    tick_volume_policies, TickChunk, TickVolumePolicy, TieringEngine,
};

/// Cap on the evictions planned for one disk per round (30 s).
pub(crate) const MAX_EVICTIONS_PER_ROUND: usize = 512;
/// `ListOldestChunks` limit (also the data node's cap).
const LIST_LIMIT: u32 = 4096;
/// Pages (of `LIST_LIMIT`) read per disk per round: 32k entries.
const MAX_PAGES: usize = 8;

/// Keyset cursor `(last_read_ms, chunk_id)`, exclusive.
type Cursor = (u64, String);

/// Page one disk's `ListOldestChunks` and plan each page, accumulating into
/// one plan. Continues only while the plan is short of `target_bytes` and of
/// `MAX_EVICTIONS_PER_ROUND`, the page was full and fewer than `MAX_PAGES`
/// were read. A page whose first entry is not strictly after the cursor (an
/// old data node ignoring it) is dropped and ends paging. A fetch error on
/// the first page is returned; on a later page it ends paging, keeping the plan.
async fn plan_paged<F, Fut, P, PFut>(mut fetch: F, mut plan_page: P, target_bytes: u64) -> Result<Vec<CapacityEviction>, tonic::Status>
where
    F: FnMut(Option<Cursor>) -> Fut,
    Fut: std::future::Future<Output = Result<Vec<OldestChunk>, tonic::Status>>,
    P: FnMut(Vec<OldestChunk>, u64) -> PFut,
    PFut: std::future::Future<Output = Vec<CapacityEviction>>,
{
    let mut out: Vec<CapacityEviction> = Vec::new();
    let mut freed = 0u64;
    let mut cursor: Option<Cursor> = None;
    for n in 0..MAX_PAGES {
        let page = match fetch(cursor.clone()).await {
            Ok(p) => p,
            Err(e) if n == 0 => return Err(e),
            Err(e) => {
                debug!(code = ?e.code(), "ListOldestChunks page failed, paging stopped");
                break;
            }
        };
        if let (Some(c), Some(f)) = (&cursor, page.first()) {
            if (f.last_read_ms, &f.chunk_id) <= (c.0, &c.1) {
                break;
            }
        }
        let full = page.len() >= LIST_LIMIT as usize;
        let next = page.last().map(|l| (l.last_read_ms, l.chunk_id.clone()));
        if !page.is_empty() {
            let ev = plan_page(page, target_bytes.saturating_sub(freed)).await;
            freed = ev.iter().fold(freed, |a, e| a.saturating_add(e.size_bytes));
            out.extend(ev);
        }
        if !full || freed >= target_bytes || out.len() >= MAX_EVICTIONS_PER_ROUND {
            break;
        }
        cursor = next;
    }
    out.truncate(MAX_EVICTIONS_PER_ROUND);
    Ok(out)
}

enum PlanKind {
    /// Bytes still to free.
    Capacity(u64),
    /// Now (ms).
    Retention(u64),
}

/// One location row an eviction removes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EvictRow {
    pub file_path: String,
    pub chunk_index: u64,
    pub enforce_anchor: Option<dtfs_common::types::AnchorPolicy>,
}

/// One cache copy (a chunk on one (node, disk)) to evict: every row that
/// references the hash on that disk, so the byte delete actually runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CapacityEviction {
    pub chunk_id: String,
    pub last_read_ms: u64,
    pub size_bytes: u64,
    pub tier: DiskTier,
    /// 1 = unprotected copy, 2 = protected only by a pre-cache cursor.
    pub pass: u8,
    pub rows: Vec<EvictRow>,
}

/// Pre-cache leader memory the planner honours.
pub(crate) struct EvictProtection<'a> {
    /// Every protected (chunk, node, disk), pins included.
    pub protected: &'a HashSet<(String, u64, u32)>,
    /// (chunk, target node) pairs with a live pre-cache task.
    pub scheduled: &'a HashSet<(String, u64)>,
    /// Pin-protected subset: never evicted.
    pub pinned: &'a HashSet<(String, u64, u32)>,
}

/// True when a disk's filesystem usage is at or above `high_pct`.
pub(crate) fn over_high_watermark(total_bytes: u64, used_bytes: u64, high_pct: u8) -> bool {
    high_pct > 0 && total_bytes > 0 && used_bytes as u128 * 100 >= high_pct as u128 * total_bytes as u128
}

/// Bytes to free to bring the disk down to `low_pct`.
pub(crate) fn bytes_to_free(total_bytes: u64, used_bytes: u64, low_pct: u8) -> u64 {
    used_bytes.saturating_sub((total_bytes as u128 * low_pct as u128 / 100) as u64)
}

/// How long a disk whose usage report has not changed since its last eviction
/// round is left alone (covers a truly unchanged disk, e.g. failed byte deletes).
pub(crate) const EVICT_STALE_REPORT_MS: u64 = 5 * 60 * 1000;

/// A disk's last round that evicted something.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvictRecord {
    /// `DiskInfo.used_bytes` that round planned against.
    pub used_at_round: u64,
    pub evicted_at_ms: u64,
}

/// True while the disk's usage report predates the last eviction round: the
/// heartbeat has not applied the freed bytes yet, so planning again would
/// evict the same amount a second time.
pub(crate) fn eviction_report_stale(rec: Option<&EvictRecord>, used_now: u64, now_ms: u64) -> bool {
    rec.is_some_and(|r| r.used_at_round == used_now && now_ms.saturating_sub(r.evicted_at_ms) < EVICT_STALE_REPORT_MS)
}

/// Every (file, chunk_index) whose rows carry one of `hashes`, keyed by hash.
/// Placements are only the rows of that hash (a row of another hash at the
/// same index is different content and never a survivor).
pub(crate) fn eviction_chunks(sm: &StateMachineData, hashes: &HashSet<String>) -> HashMap<String, Vec<TickChunk>> {
    let mut out: HashMap<String, Vec<TickChunk>> = HashMap::new();
    for (path, fm) in sm.files.iter() {
        let mut by_key: BTreeMap<(u64, &str), Vec<&dtfs_raft::ChunkLocationEntry>> = BTreeMap::new();
        for e in fm.chunks.iter().filter(|e| hashes.contains(&e.chunk_hash)) {
            by_key.entry((e.chunk_index, e.chunk_hash.as_str())).or_default().push(e);
        }
        for ((idx, hash), entries) in by_key {
            out.entry(hash.to_string()).or_default().push(TickChunk {
                chunk_id: hash.to_string(),
                chunk_index: idx,
                file_path: path.clone(),
                volume_id: fm.volume_id.clone(),
                placements: entries.iter().filter_map(|e| tick_placement(sm, e)).collect(),
                cooldown_until_ms: fm.cache_cooldown_until_ms.get(&idx).copied().unwrap_or(0),
                reads_in_window_by_dc: HashMap::new(),
                file_updated_at_ms: fm.updated_at_ms.max(fm.replica_added_at_ms),
                idle_reads_by_dc: None,
            });
        }
    }
    out
}

/// The row's volume has `retention_secs` set, it elapsed since `last_read_ms`,
/// and the file is past the idle demote's fresh-copy grace (#146:
/// `promote_window_secs` + 180 s since its last update or replica add).
pub(crate) fn retention_elapsed(tc: &TickChunk, policy: &TickVolumePolicy, now_ms: u64, last_read_ms: u64) -> bool {
    let retention_ms = policy.cache.retention_secs.saturating_mul(1000);
    let grace_ms = policy.cache.promote_window_secs.saturating_add(180).saturating_mul(1000);
    retention_ms > 0
        && now_ms.saturating_sub(last_read_ms) >= retention_ms
        && !(tc.file_updated_at_ms > 0 && now_ms.saturating_sub(tc.file_updated_at_ms) < grace_ms)
}

/// The eviction of `chunk_id` from (node_id, disk_id) when EVERY row on that
/// disk passes the idle demote's checks (and, with `retention_at = Some((now,
/// last_read))`, `retention_elapsed`); `None` otherwise or for a stale id.
fn evictable(
    node_id: u64,
    disk_id: u32,
    chunk_id: &str,
    chunks: &HashMap<String, Vec<TickChunk>>,
    volumes: &HashMap<String, TickVolumePolicy>,
    removed: &[(String, u64, u64, u32)],
    retention_at: Option<(u64, u64)>,
) -> Option<(DiskTier, Vec<EvictRow>)> {
    let mut rows = Vec::new();
    let mut tier = None;
    for tc in chunks.get(chunk_id)? {
        for p in tc.placements.iter().filter(|p| p.node_id == node_id && p.disk_id == disk_id) {
            let policy = volumes.get(&tc.volume_id)?;
            if !is_cache_tier(policy, p.tier) || !mandatory_survives(tc, p, policy) {
                return None;
            }
            if retention_at.is_some_and(|(now, last)| !retention_elapsed(tc, policy, now, last)) {
                return None;
            }
            let already: Vec<(u64, u32)> = removed
                .iter()
                .filter(|r| r.0 == tc.file_path && r.1 == tc.chunk_index)
                .map(|r| (r.2, r.3))
                .collect();
            if !demote_preserves_anchor(tc, p, policy.anchor.as_ref(), &already) {
                return None;
            }
            tier = Some(p.tier);
            rows.push(EvictRow {
                file_path: tc.file_path.clone(),
                chunk_index: tc.chunk_index,
                enforce_anchor: policy.anchor.clone(),
            });
        }
    }
    Some((tier?, rows))
}

/// Plan the capacity eviction of one disk. Pure. `candidates` come oldest
/// first from `ListOldestChunks`; evictions accumulate in that order until
/// `target_bytes` are freed, at most `MAX_EVICTIONS_PER_ROUND`. Pass 1 skips
/// pre-cache protected copies; pass 2 (only when pass 1 is short) also takes
/// copies protected by a cursor. Pins are never evicted. `removed` carries
/// (file, idx, node, disk) planned earlier this round, across disks, so the
/// anchor check never counts a copy that is also going.
pub(crate) fn plan_capacity_eviction(
    node_id: u64,
    disk_id: u32,
    candidates: &[OldestChunk],
    chunks: &HashMap<String, Vec<TickChunk>>,
    volumes: &HashMap<String, TickVolumePolicy>,
    prot: &EvictProtection,
    target_bytes: u64,
    removed: &mut Vec<(String, u64, u64, u32)>,
) -> Vec<CapacityEviction> {
    plan_disk(node_id, disk_id, candidates, chunks, volumes, prot, target_bytes, &[1, 2], None, removed)
}

/// Plan the retention demotion of one disk. Pure. A candidate goes when every
/// row on (node, disk) passes `plan_capacity_eviction`'s checks AND belongs to
/// a volume whose `retention_secs` elapsed since the candidate's `last_read_ms`
/// (`retention_elapsed`). Pass 1 only: protected (cursor / scheduled) and
/// pinned copies stay. No byte target, at most `MAX_EVICTIONS_PER_ROUND`.
pub(crate) fn plan_retention_demotion(
    node_id: u64,
    disk_id: u32,
    candidates: &[OldestChunk],
    chunks: &HashMap<String, Vec<TickChunk>>,
    volumes: &HashMap<String, TickVolumePolicy>,
    prot: &EvictProtection,
    now_ms: u64,
    removed: &mut Vec<(String, u64, u64, u32)>,
) -> Vec<CapacityEviction> {
    plan_disk(node_id, disk_id, candidates, chunks, volumes, prot, u64::MAX, &[1], Some(now_ms), removed)
}

#[allow(clippy::too_many_arguments)]
fn plan_disk(
    node_id: u64,
    disk_id: u32,
    candidates: &[OldestChunk],
    chunks: &HashMap<String, Vec<TickChunk>>,
    volumes: &HashMap<String, TickVolumePolicy>,
    prot: &EvictProtection,
    target_bytes: u64,
    passes: &[u8],
    retention_now: Option<u64>,
    removed: &mut Vec<(String, u64, u64, u32)>,
) -> Vec<CapacityEviction> {
    let mut out: Vec<CapacityEviction> = Vec::new();
    let mut freed = 0u64;
    for &pass in passes {
        for c in candidates {
            if freed >= target_bytes || out.len() >= MAX_EVICTIONS_PER_ROUND {
                return out;
            }
            let key = (c.chunk_id.clone(), node_id, disk_id);
            if prot.pinned.contains(&key) {
                continue;
            }
            let protected = prot.protected.contains(&key) || prot.scheduled.contains(&(c.chunk_id.clone(), node_id));
            // Pass 1 takes the unprotected copies, pass 2 only the cursor-protected rest.
            if protected != (pass == 2) {
                continue;
            }
            let retention_at = retention_now.map(|now| (now, c.last_read_ms));
            let Some((tier, rows)) = evictable(node_id, disk_id, &c.chunk_id, chunks, volumes, removed, retention_at) else {
                continue;
            };
            removed.extend(rows.iter().map(|r| (r.file_path.clone(), r.chunk_index, node_id, disk_id)));
            freed = freed.saturating_add(c.size_bytes);
            out.push(CapacityEviction {
                chunk_id: c.chunk_id.clone(),
                last_read_ms: c.last_read_ms,
                size_bytes: c.size_bytes,
                tier,
                pass,
                rows,
            });
        }
    }
    out
}

struct DiskToEvict {
    node_id: u64,
    addr: String,
    disk_id: u32,
    total: u64,
    used: u64,
}

impl TieringEngine {
    /// One capacity-eviction round. No-op while `cache_high_watermark_pct` is 0.
    pub(super) async fn capacity_evict_tick(&self) {
        let (enabled, high, low) = {
            let c = self.config.read().await;
            (c.enabled, c.cache_high_watermark_pct, c.cache_low_watermark_pct)
        };
        if !enabled || high == 0 || !self.scheduler.is_leader() {
            return;
        }
        let now_ms = now_millis();
        let disks: Vec<DiskToEvict> = {
            let sm = self.state_data.read().await;
            let mut records = self.evict_records.lock();
            let mut v = Vec::new();
            for (nid, n) in sm.nodes.iter() {
                for d in &n.disks {
                    let key = (*nid, d.disk_id);
                    // The disk must itself be a usable replica (node not Down/Failed/
                    // Maintenance/Quarantined, disk not Failed/Removed).
                    if !(matches!(d.tier, DiskTier::Hot | DiskTier::Warm)
                        && crate::scheduler::survivor_replica_tier(Some(n), d.disk_id).is_some()
                        && over_high_watermark(d.total_bytes, d.used_bytes, high))
                    {
                        records.remove(&key);
                        continue;
                    }
                    if eviction_report_stale(records.get(&key), d.used_bytes, now_ms) {
                        debug!(node_id = nid, disk_id = d.disk_id, "capacity eviction: usage report predates the last round, disk skipped");
                        continue;
                    }
                    records.remove(&key);
                    v.push(DiskToEvict {
                        node_id: *nid,
                        addr: n.grpc_addr.clone(),
                        disk_id: d.disk_id,
                        total: d.total_bytes,
                        used: d.used_bytes,
                    });
                }
            }
            v
        };
        if disks.is_empty() {
            return;
        }

        let named = dtfs_raft::state_machine::cluster_understands_named_remove(&*self.state_data.read().await);
        let (protected, pinned, scheduled) = {
            let st = self.precache.lock();
            (st.protected.clone(), st.pinned.clone(), st.scheduled.clone())
        };
        let prot = EvictProtection { protected: &protected, scheduled: &scheduled, pinned: &pinned };
        let removed = parking_lot::Mutex::new(Vec::new());
        let mut plans: Vec<(DiskToEvict, Vec<CapacityEviction>)> = Vec::new();
        for d in disks {
            let target = bytes_to_free(d.total, d.used, low);
            let res = plan_paged(
                |cur| self.list_page(&d, 0, cur),
                |page, remaining| self.plan_page(&d, page, &prot, &removed, PlanKind::Capacity(remaining)),
                target,
            )
            .await;
            if let Some(ev) = note_list_result(&self.evict_skip_logged, "capacity eviction", &d, res) {
                plans.push((d, ev));
            }
        }

        let active_pairs = self.scheduler.active_replication_pairs().await;
        for (d, evictions) in plans {
            if evictions.is_empty() {
                continue;
            }
            if !self.scheduler.is_leader() {
                return;
            }
            let now_ms = now_millis();
            let (count, bytes) = self.dispatch_removals(&d, &evictions, &active_pairs, named, ("cache.evict_capacity", "evicted")).await;
            if count > 0 {
                self.evict_records
                    .lock()
                    .insert((d.node_id, d.disk_id), EvictRecord { used_at_round: d.used, evicted_at_ms: now_ms });
                let used_pct = d.used as u128 * 100 / d.total.max(1) as u128;
                warn!(node_id = d.node_id, disk_id = d.disk_id, used_pct = used_pct as u64, evicted = count, bytes, "capacity eviction: cache copies evicted above high watermark");
            }
        }
    }

    /// One retention round: for every hot/warm survivor disk not visited in
    /// `RETENTION_INTERVAL_MS`, list the chunks unread for the shortest
    /// `retention_secs` and demote the cache copies whose volume retention
    /// elapsed (`plan_retention_demotion`). A disk that answers is "capable":
    /// the ring-based idle demote leaves its retention-volume copies alone.
    /// No-op while no volume sets `retention_secs`.
    pub(super) async fn retention_demote_tick(&self) {
        if !self.config.read().await.enabled || !self.scheduler.is_leader() {
            return;
        }
        let now_ms = now_millis();
        let (min_retention_secs, disks) = {
            let sm = self.state_data.read().await;
            let min = sm.volumes.values().map(|v| v.cache_replicas.retention_secs).filter(|r| *r > 0).min();
            let Some(min) = min else { return };
            let mut last_run = self.retention_last_run.lock();
            let mut v = Vec::new();
            for (nid, n) in sm.nodes.iter() {
                for d in &n.disks {
                    let key = (*nid, d.disk_id);
                    if !(matches!(d.tier, DiskTier::Hot | DiskTier::Warm)
                        && crate::scheduler::survivor_replica_tier(Some(n), d.disk_id).is_some())
                        || !retention_due(last_run.get(&key).copied(), now_ms)
                    {
                        continue;
                    }
                    last_run.insert(key, now_ms);
                    v.push(DiskToEvict {
                        node_id: *nid,
                        addr: n.grpc_addr.clone(),
                        disk_id: d.disk_id,
                        total: d.total_bytes,
                        used: d.used_bytes,
                    });
                }
            }
            (min, v)
        };
        if disks.is_empty() {
            return;
        }
        let older_than_ms = now_ms.saturating_sub(min_retention_secs.saturating_mul(1000)).max(1);
        let named = dtfs_raft::state_machine::cluster_understands_named_remove(&*self.state_data.read().await);
        let (protected, pinned, scheduled) = {
            let st = self.precache.lock();
            (st.protected.clone(), st.pinned.clone(), st.scheduled.clone())
        };
        let prot = EvictProtection { protected: &protected, scheduled: &scheduled, pinned: &pinned };
        let removed = parking_lot::Mutex::new(Vec::new());
        let mut plans: Vec<(DiskToEvict, Vec<CapacityEviction>)> = Vec::new();
        for d in disks {
            let res = plan_paged(
                |cur| self.list_page(&d, older_than_ms, cur),
                |page, _| self.plan_page(&d, page, &prot, &removed, PlanKind::Retention(now_ms)),
                u64::MAX,
            )
            .await;
            let key = (d.node_id, d.disk_id);
            match note_list_result(&self.retention_skip_logged, "retention demote", &d, res) {
                Some(ev) => {
                    self.retention_capable.lock().insert(key, now_ms);
                    plans.push((d, ev));
                }
                None => {
                    // The ring-based idle demote takes this disk back.
                    self.retention_capable.lock().remove(&key);
                }
            }
        }

        let active_pairs = self.scheduler.active_replication_pairs().await;
        for (d, demotions) in plans {
            if demotions.is_empty() {
                continue;
            }
            if !self.scheduler.is_leader() {
                return;
            }
            let (count, bytes) = self.dispatch_removals(&d, &demotions, &active_pairs, named, ("cache.demote", "retention_elapsed")).await;
            if count > 0 {
                info!(node_id = d.node_id, disk_id = d.disk_id, demoted = count, bytes, "retention demote: cache copies unread past their volume retention removed");
            }
        }
    }

    async fn list_page(&self, d: &DiskToEvict, older_than_ms: u64, cursor: Option<Cursor>) -> Result<Vec<OldestChunk>, tonic::Status> {
        self.scheduler.list_oldest_chunks_on_node(&d.addr, d.disk_id, LIST_LIMIT, older_than_ms, cursor).await
    }

    /// Plan one page against a fresh state-machine snapshot.
    async fn plan_page(
        &self,
        d: &DiskToEvict,
        page: Vec<OldestChunk>,
        prot: &EvictProtection<'_>,
        removed: &parking_lot::Mutex<Vec<(String, u64, u64, u32)>>,
        kind: PlanKind,
    ) -> Vec<CapacityEviction> {
        let hashes: HashSet<String> = page.iter().map(|o| o.chunk_id.clone()).collect();
        let (chunks, volumes) = {
            let sm = self.state_data.read().await;
            (eviction_chunks(&sm, &hashes), tick_volume_policies(&sm))
        };
        let mut removed = removed.lock();
        match kind {
            PlanKind::Capacity(target) => plan_capacity_eviction(d.node_id, d.disk_id, &page, &chunks, &volumes, prot, target, &mut removed),
            PlanKind::Retention(now_ms) => plan_retention_demotion(d.node_id, d.disk_id, &page, &chunks, &volumes, prot, now_ms, &mut removed),
        }
    }

    /// De-locate every planned copy of one disk (Raft remove first, byte delete
    /// after, via `delocate_replica`) and audit each as `audit.0`, result
    /// `audit.1` on success. Skips a copy a live copy task targets. Returns the
    /// copies and bytes removed.
    async fn dispatch_removals(
        &self,
        d: &DiskToEvict,
        evictions: &[CapacityEviction],
        active_pairs: &HashSet<(String, u64)>,
        named: bool,
        audit: (&str, &'static str),
    ) -> (u64, u64) {
        let now_ms = now_millis();
        let (mut count, mut bytes) = (0u64, 0u64);
        for e in evictions {
            // A copy task targeting this node may be writing the same id.
            if active_pairs.contains(&(e.chunk_id.clone(), d.node_id)) {
                continue;
            }
            let mut result = audit.1;
            for r in &e.rows {
                let delocate = crate::scheduler::SchedulerDelocate {
                    sched: &*self.scheduler,
                    remove_req: dtfs_raft::ClientRequest::RemoveChunkLocation {
                        now_ms: now_millis(),
                        path: r.file_path.clone(),
                        chunk_index: r.chunk_index,
                        node_id: d.node_id.to_string(),
                        disk_id: d.disk_id,
                        enforce_anchor: r.enforce_anchor.clone(),
                        chunk_hash: named.then(|| e.chunk_id.clone()),
                    },
                    node_addr: d.addr.clone(),
                    chunk_id: e.chunk_id.clone(),
                    disk_id: d.disk_id,
                    tier: e.tier.to_string(),
                };
                match crate::scheduler::delocate_replica(&delocate).await {
                    crate::scheduler::DelocateOutcome::Done => {}
                    crate::scheduler::DelocateOutcome::RemovedBytesLeftover => {
                        warn!(chunk_id = %e.chunk_id, node_id = d.node_id, disk_id = d.disk_id, action = audit.0, "cache removal: byte delete failed after Raft remove (orphan reaper reclaims it)");
                        result = "rpc_failed_after_remove";
                    }
                    crate::scheduler::DelocateOutcome::RemoveFailed => {
                        warn!(chunk_id = %e.chunk_id, node_id = d.node_id, disk_id = d.disk_id, file = %r.file_path, action = audit.0, "cache removal: RemoveChunkLocation refused or failed, copy kept");
                        result = "remove_failed";
                        break;
                    }
                }
            }
            self.audit.log(
                "tiering",
                audit.0,
                &format!(
                    "chunk={} node={} disk={} tier={:?} last_read_age_secs={} pass={}",
                    e.chunk_id,
                    d.node_id,
                    d.disk_id,
                    e.tier,
                    now_ms.saturating_sub(e.last_read_ms) / 1000,
                    e.pass
                ),
                result,
            );
            if result != "remove_failed" {
                count += 1;
                bytes += e.size_bytes;
            }
        }
        (count, bytes)
    }
}

/// Cadence of the retention pass per (node, disk): the data node's last-read index only refreshes an entry every 10 min anyway.
pub(crate) const RETENTION_INTERVAL_MS: u64 = 10 * 60 * 1000;
/// A disk counts as retention-capable this long after its last successful
/// `ListOldestChunks`.
pub(crate) const RETENTION_CAPABLE_FRESH_MS: u64 = 30 * 60 * 1000;

/// True when the disk's last retention pass is `RETENTION_INTERVAL_MS` old (or never ran).
pub(crate) fn retention_due(last_run_ms: Option<u64>, now_ms: u64) -> bool {
    last_run_ms.is_none_or(|t| now_ms.saturating_sub(t) >= RETENTION_INTERVAL_MS)
}

/// (node, disk) whose last-read index answered within `RETENTION_CAPABLE_FRESH_MS`.
pub(crate) fn fresh_capable_disks(capable: &HashMap<(u64, u32), u64>, now_ms: u64) -> HashSet<(u64, u32)> {
    capable
        .iter()
        .filter(|(_, t)| now_ms.saturating_sub(**t) <= RETENTION_CAPABLE_FRESH_MS)
        .map(|(k, _)| *k)
        .collect()
}

/// `Some(list)` on success (and forget earlier skip logs for the disk); on an
/// error log once per (disk, code): info for the expected Unimplemented /
/// FailedPrecondition / Unavailable, warn otherwise.
fn note_list_result<T>(
    logged: &parking_lot::Mutex<HashSet<(u64, u32, i32)>>,
    what: &str,
    d: &DiskToEvict,
    res: Result<T, tonic::Status>,
) -> Option<T> {
    match res {
        Ok(c) => {
            logged.lock().retain(|k| (k.0, k.1) != (d.node_id, d.disk_id));
            Some(c)
        }
        Err(s) => {
            let first = logged.lock().insert((d.node_id, d.disk_id, s.code() as i32));
            let expected = matches!(
                s.code(),
                tonic::Code::Unimplemented | tonic::Code::FailedPrecondition | tonic::Code::Unavailable
            );
            if first && expected {
                info!(node_id = d.node_id, disk_id = d.disk_id, code = ?s.code(), what, "ListOldestChunks unavailable, disk skipped");
            } else if first {
                warn!(node_id = d.node_id, disk_id = d.disk_id, code = ?s.code(), error = %s.message(), what, "ListOldestChunks failed, disk skipped");
            } else {
                debug!(node_id = d.node_id, disk_id = d.disk_id, code = ?s.code(), what, "disk skipped");
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tiering::TickPlacement;
    use dtfs_common::topology::Location;
    use dtfs_common::types::{
        AnchorPolicy, CacheReplication, CacheTierConfig, DiskInfo, DiskStatus, HealthStatus, NodeRole, TierReplication,
    };

    const N: u64 = 2; // the full disk lives on node 2, disk 0 (toro warm)

    fn policy(warm_mandatory: u32) -> TickVolumePolicy {
        TickVolumePolicy {
            mandatory: TierReplication { hot: 0, warm: warm_mandatory, cold: 1, tape: 0 },
            cache: CacheReplication {
                hot: CacheTierConfig::default(),
                warm: CacheTierConfig { min: 1, max: 1 },
                cold: CacheTierConfig::default(),
                tape: CacheTierConfig::default(),
                per_dc: true,
                promote_reads: 2,
                promote_window_secs: 14_400,
                cooldown_secs: 300,
                retention_secs: 0,
            },
            // Any-tier anchor: a leli warm cache copy can be the only anchor copy.
            anchor: Some(AnchorPolicy { datacenter: "LELI".into(), required_tier: None }),
        }
    }

    fn vols() -> HashMap<String, TickVolumePolicy> {
        [("v".to_string(), policy(0)), ("m".to_string(), policy(1))].into_iter().collect()
    }

    fn pl(node_id: u64, dc: &str, tier: DiskTier, live: bool) -> TickPlacement {
        TickPlacement { node_id, disk_id: 0, dc: dc.into(), tier, live }
    }

    /// leli cold anchor/mandatory on node 1 + the toro warm cache copy on node 2.
    fn std_placements() -> Vec<TickPlacement> {
        vec![pl(1, "leli", DiskTier::Cold, true), pl(N, "toro", DiskTier::Warm, true)]
    }

    fn tc(hash: &str, path: &str, vol: &str, placements: Vec<TickPlacement>) -> TickChunk {
        TickChunk {
            chunk_id: hash.into(),
            chunk_index: 0,
            file_path: path.into(),
            volume_id: vol.into(),
            placements,
            cooldown_until_ms: 0,
            reads_in_window_by_dc: HashMap::new(),
            file_updated_at_ms: 0,
            idle_reads_by_dc: None,
        }
    }

    fn cand(hash: &str, last_read_ms: u64, size: u64) -> OldestChunk {
        OldestChunk { chunk_id: hash.into(), last_read_ms, size_bytes: size }
    }

    struct Prot {
        protected: HashSet<(String, u64, u32)>,
        scheduled: HashSet<(String, u64)>,
        pinned: HashSet<(String, u64, u32)>,
    }

    impl Prot {
        fn none() -> Self {
            Prot { protected: HashSet::new(), scheduled: HashSet::new(), pinned: HashSet::new() }
        }
        fn view(&self) -> EvictProtection<'_> {
            EvictProtection { protected: &self.protected, scheduled: &self.scheduled, pinned: &self.pinned }
        }
    }

    fn plan(cands: &[OldestChunk], chunks: &HashMap<String, Vec<TickChunk>>, prot: &Prot, target: u64) -> Vec<CapacityEviction> {
        plan_capacity_eviction(N, 0, cands, chunks, &vols(), &prot.view(), target, &mut Vec::new())
    }

    fn std_world(hashes: &[&str]) -> HashMap<String, Vec<TickChunk>> {
        hashes.iter().map(|h| (h.to_string(), vec![tc(h, &format!("/f/{h}"), "v", std_placements())])).collect()
    }

    fn ids(ev: &[CapacityEviction]) -> Vec<&str> {
        ev.iter().map(|e| e.chunk_id.as_str()).collect()
    }

    #[test]
    fn watermark_arithmetic() {
        assert!(!over_high_watermark(1000, 899, 90));
        assert!(over_high_watermark(1000, 900, 90));
        assert!(!over_high_watermark(1000, 1000, 0), "off");
        assert!(!over_high_watermark(0, 0, 90), "capacity not reported");
        assert_eq!(bytes_to_free(1000, 950, 80), 150);
        assert_eq!(bytes_to_free(1000, 700, 80), 0);
    }

    #[test]
    fn report_stale_until_usage_changes_or_five_minutes() {
        let rec = EvictRecord { used_at_round: 900, evicted_at_ms: 1_000 };
        assert!(!eviction_report_stale(None, 900, 1_000), "no record");
        assert!(!eviction_report_stale(Some(&rec), 899, 1_001), "fresh report");
        assert!(!eviction_report_stale(Some(&rec), 901, 1_001), "fresh report, grew");
        assert!(eviction_report_stale(Some(&rec), 900, 1_000), "same report, same instant");
        assert!(eviction_report_stale(Some(&rec), 900, 1_000 + EVICT_STALE_REPORT_MS - 1), "same report within 5 min");
        assert!(!eviction_report_stale(Some(&rec), 900, 1_000 + EVICT_STALE_REPORT_MS), "5 min elapsed");
        // Clock stepped back: still stale (saturating), not a fresh round.
        assert!(eviction_report_stale(Some(&rec), 900, 0));
    }

    #[test]
    fn evicts_oldest_first_and_stops_at_target() {
        let chunks = std_world(&["a", "b", "c"]);
        let cands = [cand("a", 1, 10), cand("b", 2, 10), cand("c", 3, 10)];
        let ev = plan(&cands, &chunks, &Prot::none(), 15);
        assert_eq!(ids(&ev), ["a", "b"]);
        assert!(ev.iter().all(|e| e.pass == 1 && e.tier == DiskTier::Warm));
        assert_eq!(ev[0].rows, vec![EvictRow { file_path: "/f/a".into(), chunk_index: 0, enforce_anchor: policy(0).anchor }]);
        assert_eq!(ids(&plan(&cands, &chunks, &Prot::none(), 10)), ["a"]);
        assert!(plan(&cands, &chunks, &Prot::none(), 0).is_empty());
    }

    #[test]
    fn cursor_protected_copies_go_only_in_pass_two() {
        let chunks = std_world(&["a", "b", "c"]);
        let cands = [cand("a", 1, 10), cand("b", 2, 10), cand("c", 3, 10)];
        let mut prot = Prot::none();
        prot.protected.insert(("a".into(), N, 0));
        prot.scheduled.insert(("b".into(), N));
        // Pass 1 meets the target: the protected a and b stay.
        assert_eq!(ids(&plan(&cands, &chunks, &prot, 10)), ["c"]);
        // Short after pass 1: pass 2 takes the protected ones, oldest first.
        let ev = plan(&cands, &chunks, &prot, 25);
        assert_eq!(ids(&ev), ["c", "a", "b"]);
        assert_eq!(ev.iter().map(|e| e.pass).collect::<Vec<_>>(), [1, 2, 2]);
    }

    #[test]
    fn round_is_capped() {
        let hs: Vec<String> = (0..600).map(|i| format!("h{i:03}")).collect();
        let refs: Vec<&str> = hs.iter().map(String::as_str).collect();
        let chunks = std_world(&refs);
        let cands: Vec<OldestChunk> = hs.iter().enumerate().map(|(i, h)| cand(h, i as u64, 1)).collect();
        let ev = plan(&cands, &chunks, &Prot::none(), u64::MAX);
        assert_eq!(ev.len(), MAX_EVICTIONS_PER_ROUND);
        assert_eq!(ev[511].chunk_id, "h511");
    }

    /// Each refused shape sits between two evictable candidates; only those two go.
    fn assert_skipped(chunks: HashMap<String, Vec<TickChunk>>, prot: &Prot) {
        let mut chunks = chunks;
        chunks.extend(std_world(&["ok1", "ok2"]));
        let cands = [cand("ok1", 1, 10), cand("x", 2, 10), cand("ok2", 3, 10)];
        assert_eq!(ids(&plan(&cands, &chunks, prot, u64::MAX)), ["ok1", "ok2"]);
    }

    fn one(hash: &str, chunks: Vec<TickChunk>) -> HashMap<String, Vec<TickChunk>> {
        [(hash.to_string(), chunks)].into_iter().collect()
    }

    #[test]
    fn mandatory_copy_is_skipped() {
        // Volume m keeps warm=1 mandatory: the warm row is not a cache copy.
        assert_skipped(one("x", vec![tc("x", "/m/x", "m", std_placements())]), &Prot::none());
    }

    #[test]
    fn only_anchor_dc_copy_is_skipped() {
        // Mandatory cold copy in nrb, the only leli copy is the warm one on the full disk.
        let p = vec![pl(3, "nrb", DiskTier::Cold, true), pl(N, "leli", DiskTier::Warm, true)];
        assert_skipped(one("x", vec![tc("x", "/f/x", "v", p)]), &Prot::none());
    }

    #[test]
    fn anchor_pending_chunk_is_skipped() {
        // No live leli copy at all yet (anchor-lag pending).
        let p = vec![pl(3, "nrb", DiskTier::Cold, true), pl(N, "toro", DiskTier::Warm, true)];
        assert_skipped(one("x", vec![tc("x", "/f/x", "v", p)]), &Prot::none());
    }

    #[test]
    fn last_live_replica_is_skipped() {
        assert_skipped(one("x", vec![tc("x", "/f/x", "v", vec![pl(N, "toro", DiskTier::Warm, true)])]), &Prot::none());
        // The other copy is dead: still the last live one.
        let p = vec![pl(1, "leli", DiskTier::Cold, false), pl(N, "toro", DiskTier::Warm, true)];
        assert_skipped(one("x", vec![tc("x", "/f/x", "v", p)]), &Prot::none());
    }

    #[test]
    fn shared_hash_with_a_mandatory_row_is_skipped() {
        let rows = vec![tc("x", "/f/x", "v", std_placements()), tc("x", "/m/x", "m", std_placements())];
        assert_skipped(one("x", rows), &Prot::none());
        // Both rows cache copies: one eviction removing both rows.
        let rows = vec![tc("x", "/f/x", "v", std_placements()), tc("x", "/g/x", "v", std_placements())];
        let ev = plan(&[cand("x", 1, 10)], &one("x", rows), &Prot::none(), 10);
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].rows.len(), 2);
    }

    #[test]
    fn pinned_copy_is_never_evicted() {
        let mut prot = Prot::none();
        prot.protected.insert(("x".into(), N, 0));
        prot.pinned.insert(("x".into(), N, 0));
        assert_skipped(std_world(&["x"]), &prot);
    }

    #[test]
    fn stale_id_is_skipped() {
        // Unknown hash, and a hash with no row on this disk.
        assert_skipped(HashMap::new(), &Prot::none());
        let p = vec![pl(1, "leli", DiskTier::Cold, true), pl(5, "toro", DiskTier::Warm, true)];
        assert_skipped(one("x", vec![tc("x", "/f/x", "v", p)]), &Prot::none());
    }

    #[test]
    fn anchor_check_counts_copies_removed_earlier_in_the_round() {
        // Two leli warm copies (nodes 4 and 2), mandatory in nrb: one may go, not both.
        let p = vec![pl(3, "nrb", DiskTier::Cold, true), pl(4, "leli", DiskTier::Warm, true), pl(N, "leli", DiskTier::Warm, true)];
        let chunks = one("x", vec![tc("x", "/f/x", "v", p)]);
        let prot = Prot::none();
        let mut removed = Vec::new();
        let first = plan_capacity_eviction(4, 0, &[cand("x", 1, 10)], &chunks, &vols(), &prot.view(), 10, &mut removed);
        assert_eq!(first.len(), 1);
        let second = plan_capacity_eviction(N, 0, &[cand("x", 1, 10)], &chunks, &vols(), &prot.view(), 10, &mut removed);
        assert!(second.is_empty(), "the second leli copy is the last anchor copy");
    }

    // --- retention ----------------------------------------------------------

    const R: u64 = 86_400; // retention_secs
    const T: u64 = 10_000_000_000; // now_ms

    fn ret_vols() -> HashMap<String, TickVolumePolicy> {
        vols()
            .into_iter()
            .map(|(k, mut p)| {
                p.cache.retention_secs = R;
                (k, p)
            })
            .collect()
    }

    fn plan_ret_with(cands: &[OldestChunk], chunks: &HashMap<String, Vec<TickChunk>>, vols: &HashMap<String, TickVolumePolicy>, prot: &Prot) -> Vec<CapacityEviction> {
        plan_retention_demotion(N, 0, cands, chunks, vols, &prot.view(), T, &mut Vec::new())
    }

    fn plan_ret(cands: &[OldestChunk], chunks: &HashMap<String, Vec<TickChunk>>, prot: &Prot) -> Vec<CapacityEviction> {
        plan_ret_with(cands, chunks, &ret_vols(), prot)
    }

    /// Unread for exactly the retention.
    fn aged(hash: &str) -> OldestChunk {
        cand(hash, T - R * 1000, 10)
    }

    /// Each refused shape sits between two demotable candidates; only those two go.
    fn assert_ret_skipped(chunks: HashMap<String, Vec<TickChunk>>, prot: &Prot) {
        let mut chunks = chunks;
        chunks.extend(std_world(&["ok1", "ok2"]));
        let cands = [aged("ok1"), aged("x"), aged("ok2")];
        assert_eq!(ids(&plan_ret(&cands, &chunks, prot)), ["ok1", "ok2"]);
    }

    #[test]
    fn retention_demotes_at_exactly_elapsed_and_keeps_one_ms_short() {
        let chunks = std_world(&["a", "b"]);
        let cands = [aged("a"), cand("b", T - R * 1000 + 1, 10)];
        let ev = plan_ret(&cands, &chunks, &Prot::none());
        assert_eq!(ids(&ev), ["a"]);
        assert_eq!(ev[0].rows, vec![EvictRow { file_path: "/f/a".into(), chunk_index: 0, enforce_anchor: policy(0).anchor }]);
        // No retention on the volume: the ring-based idle demote owns it.
        assert!(plan_ret_with(&cands, &chunks, &vols(), &Prot::none()).is_empty());
    }

    #[test]
    fn retention_keeps_mandatory_anchor_and_last_live_copies() {
        assert_ret_skipped(one("x", vec![tc("x", "/m/x", "m", std_placements())]), &Prot::none());
        let only_leli = vec![pl(3, "nrb", DiskTier::Cold, true), pl(N, "leli", DiskTier::Warm, true)];
        assert_ret_skipped(one("x", vec![tc("x", "/f/x", "v", only_leli)]), &Prot::none());
        let anchor_pending = vec![pl(3, "nrb", DiskTier::Cold, true), pl(N, "toro", DiskTier::Warm, true)];
        assert_ret_skipped(one("x", vec![tc("x", "/f/x", "v", anchor_pending)]), &Prot::none());
        let dead_other = vec![pl(1, "leli", DiskTier::Cold, false), pl(N, "toro", DiskTier::Warm, true)];
        assert_ret_skipped(one("x", vec![tc("x", "/f/x", "v", dead_other)]), &Prot::none());
    }

    #[test]
    fn retention_keeps_a_hash_shared_with_a_mandatory_or_non_retention_row() {
        let rows = vec![tc("x", "/f/x", "v", std_placements()), tc("x", "/m/x", "m", std_placements())];
        assert_ret_skipped(one("x", rows), &Prot::none());
        // Volume "n" has no retention: its row keeps the copy.
        let mut vols = ret_vols();
        vols.insert("n".into(), policy(0));
        let rows = vec![tc("x", "/f/x", "v", std_placements()), tc("x", "/n/x", "n", std_placements())];
        assert!(plan_ret_with(&[aged("x")], &one("x", rows), &vols, &Prot::none()).is_empty());
    }

    #[test]
    fn retention_never_takes_protected_scheduled_or_pinned_copies() {
        for prot in [
            Prot { protected: [("x".to_string(), N, 0)].into(), ..Prot::none() },
            Prot { scheduled: [("x".to_string(), N)].into(), ..Prot::none() },
            Prot { protected: [("x".to_string(), N, 0)].into(), pinned: [("x".to_string(), N, 0)].into(), ..Prot::none() },
        ] {
            assert_ret_skipped(std_world(&["x"]), &prot);
        }
    }

    #[test]
    fn retention_respects_the_fresh_copy_grace() {
        // Grace = promote_window_secs (14 400) + 180 s since the file's last update / replica add.
        let grace_ms = (14_400 + 180) * 1000;
        let fresh = |updated: u64| {
            let mut t = tc("x", "/f/x", "v", std_placements());
            t.file_updated_at_ms = updated;
            one("x", vec![t])
        };
        assert!(plan_ret(&[aged("x")], &fresh(T - grace_ms + 1), &Prot::none()).is_empty());
        assert_eq!(ids(&plan_ret(&[aged("x")], &fresh(T - grace_ms), &Prot::none())), ["x"]);
    }

    /// The ring-based idle demote would be gated off (new leader, a ring evicted
    /// at the key cap); the node-local retention pass does not consult it.
    #[test]
    fn retention_ignores_the_ring_tenure_and_eviction_gates() {
        assert!(!crate::tiering::idle_demote_allowed(T, T - 1_000, T - 1_000, R));
        assert_eq!(ids(&plan_ret(&[aged("a")], &std_world(&["a"]), &Prot::none())), ["a"]);
    }

    #[test]
    fn retention_is_capped_per_round() {
        let hs: Vec<String> = (0..600).map(|i| format!("h{i:03}")).collect();
        let refs: Vec<&str> = hs.iter().map(String::as_str).collect();
        let cands: Vec<OldestChunk> = hs.iter().map(|h| aged(h)).collect();
        assert_eq!(plan_ret(&cands, &std_world(&refs), &Prot::none()).len(), MAX_EVICTIONS_PER_ROUND);
    }

    /// Emulates the data node: entries sorted by (last_read_ms, id), strictly
    /// after the cursor, first `LIST_LIMIT`. `honor_cursor=false` = old node.
    fn serve(all: &[OldestChunk], cur: Option<Cursor>, honor_cursor: bool) -> Vec<OldestChunk> {
        all.iter()
            .filter(|c| !honor_cursor || cur.as_ref().is_none_or(|k| (c.last_read_ms, &c.chunk_id) > (k.0, &k.1)))
            .take(LIST_LIMIT as usize)
            .cloned()
            .collect()
    }

    /// `n_bad` mandatory-only entries (never evictable) then evictable `ok*` ones.
    fn paged_world(n_bad: usize, n_ok: usize) -> (Vec<OldestChunk>, HashMap<String, Vec<TickChunk>>) {
        let mut all = Vec::new();
        let mut chunks = HashMap::new();
        for i in 0..n_bad + n_ok {
            let h = if i < n_bad { format!("bad{i:06}") } else { format!("ok{i:06}") };
            // Volume "m" makes the warm copy mandatory, so it is never evictable.
            chunks.insert(h.clone(), vec![tc(&h, &format!("/f/{h}"), if i < n_bad { "m" } else { "v" }, std_placements())]);
            all.push(cand(&h, 1000 + i as u64, 10));
        }
        (all, chunks)
    }

    async fn run_paged(all: &[OldestChunk], chunks: &HashMap<String, Vec<TickChunk>>, honor: bool, target: u64) -> (Vec<CapacityEviction>, usize) {
        let vs = vols();
        let fetches = std::cell::Cell::new(0usize);
        let removed = std::cell::RefCell::new(Vec::new());
        let prot = Prot::none();
        let ev = plan_paged(
            |cur| {
                fetches.set(fetches.get() + 1);
                std::future::ready(Ok(serve(all, cur, honor)))
            },
            |page, remaining| {
                std::future::ready(plan_capacity_eviction(N, 0, &page, chunks, &vs, &prot.view(), remaining, &mut removed.borrow_mut()))
            },
            target,
        )
        .await
        .unwrap();
        (ev, fetches.get())
    }

    #[tokio::test]
    async fn eviction_reaches_page_two_past_ineligible_entries() {
        let (all, chunks) = paged_world(LIST_LIMIT as usize, 10);
        let (ev, fetches) = run_paged(&all, &chunks, true, 50).await;
        assert_eq!(ev.len(), 5, "evictable entries on page 2 were reached");
        assert!(ev.iter().all(|e| e.chunk_id.starts_with("ok")));
        assert_eq!(fetches, 2);
    }

    #[tokio::test]
    async fn repeated_page_from_an_old_node_stops_paging() {
        let (all, chunks) = paged_world(LIST_LIMIT as usize, 10);
        let (ev, fetches) = run_paged(&all, &chunks, false, 50).await;
        assert!(ev.is_empty());
        assert_eq!(fetches, 2, "second page repeats the first: dropped, no third fetch");
    }

    #[tokio::test]
    async fn paging_is_capped_at_eight_pages() {
        let (all, chunks) = paged_world(LIST_LIMIT as usize * 9, 10);
        let (ev, fetches) = run_paged(&all, &chunks, true, 50).await;
        assert!(ev.is_empty(), "evictable entries sit beyond the 32k window");
        assert_eq!(fetches, MAX_PAGES);
    }

    #[tokio::test]
    async fn short_page_stops_paging() {
        let (all, chunks) = paged_world(3, 3);
        let (_, fetches) = run_paged(&all, &chunks, true, 1_000).await;
        assert_eq!(fetches, 1);
    }

    #[test]
    fn retention_cadence_and_capable_freshness() {
        assert!(retention_due(None, 0));
        assert!(!retention_due(Some(1_000), 1_000 + RETENTION_INTERVAL_MS - 1));
        assert!(retention_due(Some(1_000), 1_000 + RETENTION_INTERVAL_MS));
        let m: HashMap<(u64, u32), u64> = [((1, 0), 1_000), ((2, 0), 999)].into();
        let fresh = fresh_capable_disks(&m, 1_000 + RETENTION_CAPABLE_FRESH_MS);
        assert_eq!(fresh, [(1, 0)].into());
    }

    // --- SM view ----------------------------------------------------------

    fn node(id: u64, dc: &str, tier: DiskTier, health: HealthStatus) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            role: NodeRole::Data,
            hostname: format!("n{id}"),
            grpc_addr: format!("http://10.0.0.{id}:9500"),
            location: Location::new("r", dc, "rack", &format!("h{id}")),
            disks: vec![DiskInfo {
                disk_id: 0,
                uuid: format!("u{id}"),
                mount_path: "/d".into(),
                tier,
                total_bytes: 1000,
                used_bytes: 0,
                status: DiskStatus::Healthy,
            }],
            total_capacity: 1000,
            used_capacity: 0,
            health,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn file(path: &str, holders: &[u64]) -> dtfs_raft::state_machine::FileMetadata {
        let chunks: Vec<_> = holders
            .iter()
            .map(|h| serde_json::json!({"chunk_index": 0, "chunk_hash": "x", "node_id": h.to_string(), "disk_id": 0, "size_bytes": 10}))
            .collect();
        serde_json::from_value(serde_json::json!({
            "path": path, "volume_id": "v", "chunk_size": 10, "chunks": chunks,
            "created_at_ms": 0, "updated_at_ms": 0,
        }))
        .unwrap()
    }

    #[test]
    fn survivor_in_maintenance_blocks_eviction() {
        let mut sm = StateMachineData::default();
        sm.nodes.insert(1, node(1, "leli", DiskTier::Cold, HealthStatus::Healthy));
        sm.nodes.insert(N, node(N, "toro", DiskTier::Warm, HealthStatus::Healthy));
        sm.files.insert("/f/x".into(), file("/f/x", &[1, N]));
        let hashes: HashSet<String> = ["x".to_string(), "gone".to_string()].into_iter().collect();
        let chunks = eviction_chunks(&sm, &hashes);
        assert!(!chunks.contains_key("gone"));
        assert_eq!(ids(&plan(&[cand("x", 1, 10)], &chunks, &Prot::none(), 10)), ["x"]);
        sm.nodes.get_mut(&1).unwrap().health = HealthStatus::Maintenance;
        let chunks = eviction_chunks(&sm, &hashes);
        assert!(plan(&[cand("x", 1, 10)], &chunks, &Prot::none(), 10).is_empty());
    }

    // --- property ---------------------------------------------------------

    struct Rng(u64);
    impl Rng {
        fn next(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// Random placements, policies and candidate lists over several full disks
    /// in one round: after removing every planned row, each chunk keeps its live
    /// mandatory replicas and, if it had one, a live anchor copy; no mandatory
    /// row and no pinned copy is ever planned. Odd seeds plan capacity
    /// eviction, even seeds retention demotion (random `retention_secs`, 0 =
    /// unset): there additionally no protected copy and no row whose volume
    /// retention has not elapsed is planned.
    #[test]
    fn property_evictions_keep_mandatory_and_anchor() {
        let dcs = ["leli", "toro", "nrb"];
        let tiers = [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold];
        const NOW_P: u64 = 3_000;
        for seed in 1..600u64 {
            let retention_mode = seed % 2 == 0;
            let mut r = Rng(seed * 0x9E37_79B9_7F4A_7C15);
            let mut vols = HashMap::new();
            for v in ["a", "b", "c"] {
                let mut p = policy(r.next(2) as u32);
                p.cache.retention_secs = r.next(3);
                p.mandatory.hot = r.next(2) as u32;
                p.cache.hot = CacheTierConfig { min: 0, max: r.next(2) as u32 };
                p.anchor = match r.next(3) {
                    0 => None,
                    1 => Some(AnchorPolicy { datacenter: "leli".into(), required_tier: None }),
                    _ => Some(AnchorPolicy { datacenter: "Leli".into(), required_tier: Some(DiskTier::Cold) }),
                };
                vols.insert(v.to_string(), p);
            }
            // Nodes 1..=6, fixed dc/tier per node; disk 0 only.
            let node_dc: Vec<&str> = (0..7).map(|_| dcs[r.next(3) as usize]).collect();
            let node_tier: Vec<DiskTier> = (0..7).map(|_| tiers[r.next(3) as usize]).collect();
            let node_live: Vec<bool> = (0..7).map(|_| r.next(5) != 0).collect();
            let mut chunks: HashMap<String, Vec<TickChunk>> = HashMap::new();
            for h in 0..12 {
                let hash = format!("h{h}");
                for f in 0..(1 + r.next(2)) {
                    let mut ps = Vec::new();
                    for n in 1..7u64 {
                        if r.next(2) == 0 {
                            ps.push(TickPlacement {
                                node_id: n,
                                disk_id: 0,
                                dc: node_dc[n as usize].into(),
                                tier: node_tier[n as usize],
                                live: node_live[n as usize],
                            });
                        }
                    }
                    let vol = ["a", "b", "c", "zz"][r.next(4) as usize];
                    chunks.entry(hash.clone()).or_default().push(tc(&hash, &format!("/{vol}/{h}/{f}"), vol, ps));
                }
            }
            let mut prot = Prot::none();
            for h in 0..12 {
                let n = 1 + r.next(6);
                match r.next(4) {
                    0 => {
                        prot.protected.insert((format!("h{h}"), n, 0));
                    }
                    1 => {
                        prot.protected.insert((format!("h{h}"), n, 0));
                        prot.pinned.insert((format!("h{h}"), n, 0));
                    }
                    _ => {}
                }
            }
            let mut removed = Vec::new();
            let mut planned: Vec<(u64, CapacityEviction)> = Vec::new();
            for n in 1..7u64 {
                let mut cands: Vec<OldestChunk> = (0..14).map(|h| cand(&format!("h{h}"), r.next(100) * 30, 1 + r.next(10))).collect();
                cands.sort_by_key(|c| c.last_read_ms);
                let target = r.next(40);
                let ev = if retention_mode {
                    plan_retention_demotion(n, 0, &cands, &chunks, &vols, &prot.view(), NOW_P, &mut removed)
                } else {
                    plan_capacity_eviction(n, 0, &cands, &chunks, &vols, &prot.view(), target, &mut removed)
                };
                // Oldest-first, cap, and stop-at-target.
                assert!(ev.len() <= MAX_EVICTIONS_PER_ROUND);
                let sizes: Vec<u64> = ev.iter().map(|e| e.size_bytes).collect();
                if let (false, Some((_, before_last))) = (retention_mode, sizes.split_last()) {
                    assert!(before_last.iter().sum::<u64>() < target, "seed {seed}: planned past the target");
                }
                for e in ev {
                    assert!(!prot.pinned.contains(&(e.chunk_id.clone(), n, 0)), "seed {seed}: pin evicted");
                    if retention_mode {
                        assert!(!prot.protected.contains(&(e.chunk_id.clone(), n, 0)), "seed {seed}: protected copy demoted");
                        for row in &e.rows {
                            let vol = row.file_path.split('/').nth(1).unwrap();
                            let ret_ms = vols[vol].cache.retention_secs * 1000;
                            assert!(ret_ms > 0 && NOW_P - e.last_read_ms >= ret_ms, "seed {seed}: retention not elapsed");
                        }
                    }
                    planned.push((n, e));
                }
            }
            // Apply.
            let mut after = chunks.clone();
            for (n, e) in &planned {
                for tcs in after.values_mut() {
                    for t in tcs.iter_mut().filter(|t| t.chunk_id == e.chunk_id) {
                        t.placements.retain(|p| p.node_id != *n);
                    }
                }
            }
            for (hash, tcs) in &chunks {
                for (b, a) in tcs.iter().zip(&after[hash]) {
                    let Some(pol) = vols.get(&b.volume_id) else {
                        assert_eq!(b.placements.len(), a.placements.len(), "seed {seed}: no-policy row removed");
                        continue;
                    };
                    let mand = |t: &TickChunk| t.placements.iter().filter(|p| p.live && pol.mandatory.for_tier(p.tier) > 0).count();
                    assert_eq!(mand(b), mand(a), "seed {seed}: mandatory live replica lost on {}", b.file_path);
                    if let Some(anc) = &pol.anchor {
                        let has = |t: &TickChunk| t.placements.iter().any(|p| p.live && anc.is_satisfied_by(&p.dc, Some(p.tier)));
                        if has(b) {
                            assert!(has(a), "seed {seed}: anchor copy lost on {}", b.file_path);
                        } else {
                            assert_eq!(b.placements.len(), a.placements.len(), "seed {seed}: anchor-pending row removed");
                        }
                    }
                    if !b.placements.iter().any(|p| p.live) {
                        continue;
                    }
                    assert!(a.placements.iter().any(|p| p.live), "seed {seed}: last live replica removed");
                }
            }
        }
    }
}
