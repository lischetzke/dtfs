//! Capacity eviction: when a hot/warm data disk reaches `[tiering]
//! cache_high_watermark_pct`, remove its least recently read cache copies
//! until the disk is back at `cache_low_watermark_pct`. Leader-only, run from
//! the 30 s pre-cache loop. Removes cache replicas only, with the idle
//! demote's safety checks (cache tier, mandatory survivor, anchor); never a
//! `precache.pins` copy.

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
            });
        }
    }
    out
}

/// The eviction of `chunk_id` from (node_id, disk_id) when EVERY row on that
/// disk passes the idle demote's checks; `None` otherwise or for a stale id.
fn evictable(
    node_id: u64,
    disk_id: u32,
    chunk_id: &str,
    chunks: &HashMap<String, Vec<TickChunk>>,
    volumes: &HashMap<String, TickVolumePolicy>,
    removed: &[(String, u64, u64, u32)],
) -> Option<(DiskTier, Vec<EvictRow>)> {
    let mut rows = Vec::new();
    let mut tier = None;
    for tc in chunks.get(chunk_id)? {
        for p in tc.placements.iter().filter(|p| p.node_id == node_id && p.disk_id == disk_id) {
            let policy = volumes.get(&tc.volume_id)?;
            if !is_cache_tier(policy, p.tier) || !mandatory_survives(tc, p, policy) {
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
    let mut out: Vec<CapacityEviction> = Vec::new();
    let mut freed = 0u64;
    for pass in [1u8, 2] {
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
            let Some((tier, rows)) = evictable(node_id, disk_id, &c.chunk_id, chunks, volumes, removed) else {
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
        let disks: Vec<DiskToEvict> = {
            let sm = self.state_data.read().await;
            let mut v = Vec::new();
            for (nid, n) in sm.nodes.iter() {
                for d in &n.disks {
                    // The disk must itself be a usable replica (node not Down/Failed/
                    // Maintenance/Quarantined, disk not Failed/Removed).
                    if matches!(d.tier, DiskTier::Hot | DiskTier::Warm)
                        && crate::scheduler::survivor_replica_tier(Some(n), d.disk_id).is_some()
                        && over_high_watermark(d.total_bytes, d.used_bytes, high)
                    {
                        v.push(DiskToEvict {
                            node_id: *nid,
                            addr: n.grpc_addr.clone(),
                            disk_id: d.disk_id,
                            total: d.total_bytes,
                            used: d.used_bytes,
                        });
                    }
                }
            }
            v
        };
        if disks.is_empty() {
            return;
        }

        let mut lists: Vec<(DiskToEvict, Vec<OldestChunk>)> = Vec::new();
        for d in disks {
            match self.scheduler.list_oldest_chunks_on_node(&d.addr, d.disk_id, LIST_LIMIT).await {
                Ok(c) => {
                    self.evict_skip_logged.lock().retain(|k| (k.0, k.1) != (d.node_id, d.disk_id));
                    lists.push((d, c));
                }
                Err(s) => {
                    let first = self.evict_skip_logged.lock().insert((d.node_id, d.disk_id, s.code() as i32));
                    let expected = matches!(
                        s.code(),
                        tonic::Code::Unimplemented | tonic::Code::FailedPrecondition | tonic::Code::Unavailable
                    );
                    if first && expected {
                        info!(node_id = d.node_id, disk_id = d.disk_id, code = ?s.code(), "capacity eviction: ListOldestChunks unavailable, disk skipped");
                    } else if first {
                        warn!(node_id = d.node_id, disk_id = d.disk_id, code = ?s.code(), error = %s.message(), "capacity eviction: ListOldestChunks failed, disk skipped");
                    } else {
                        debug!(node_id = d.node_id, disk_id = d.disk_id, code = ?s.code(), "capacity eviction: disk skipped");
                    }
                }
            }
        }
        if lists.is_empty() {
            return;
        }

        let hashes: HashSet<String> = lists.iter().flat_map(|(_, c)| c.iter().map(|o| o.chunk_id.clone())).collect();
        let (chunks, volumes, named) = {
            let sm = self.state_data.read().await;
            (
                eviction_chunks(&sm, &hashes),
                tick_volume_policies(&sm),
                dtfs_raft::state_machine::cluster_understands_named_remove(&sm),
            )
        };
        let (protected, pinned, scheduled) = {
            let st = self.precache.lock();
            (st.protected.clone(), st.pinned.clone(), st.scheduled.clone())
        };
        let prot = EvictProtection { protected: &protected, scheduled: &scheduled, pinned: &pinned };
        let mut removed = Vec::new();
        let plans: Vec<(DiskToEvict, Vec<CapacityEviction>)> = lists
            .into_iter()
            .map(|(d, cands)| {
                let target = bytes_to_free(d.total, d.used, low);
                let ev = plan_capacity_eviction(d.node_id, d.disk_id, &cands, &chunks, &volumes, &prot, target, &mut removed);
                (d, ev)
            })
            .collect();

        let active_pairs = self.scheduler.active_replication_pairs().await;
        for (d, evictions) in plans {
            if evictions.is_empty() {
                continue;
            }
            if !self.scheduler.is_leader() {
                return;
            }
            let now_ms = now_millis();
            let (mut count, mut bytes) = (0u64, 0u64);
            for e in &evictions {
                // A copy task targeting this node may be writing the same id.
                if active_pairs.contains(&(e.chunk_id.clone(), d.node_id)) {
                    continue;
                }
                let mut result = "evicted";
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
                            warn!(chunk_id = %e.chunk_id, node_id = d.node_id, disk_id = d.disk_id, "capacity eviction: byte delete failed after Raft remove (orphan reaper reclaims it)");
                            result = "rpc_failed_after_remove";
                        }
                        crate::scheduler::DelocateOutcome::RemoveFailed => {
                            warn!(chunk_id = %e.chunk_id, node_id = d.node_id, disk_id = d.disk_id, file = %r.file_path, "capacity eviction: RemoveChunkLocation refused or failed, copy kept");
                            result = "remove_failed";
                            break;
                        }
                    }
                }
                self.audit.log(
                    "tiering",
                    "cache.evict_capacity",
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
            if count > 0 {
                let used_pct = d.used as u128 * 100 / d.total.max(1) as u128;
                warn!(node_id = d.node_id, disk_id = d.disk_id, used_pct = used_pct as u64, evicted = count, bytes, "capacity eviction: cache copies evicted above high watermark");
            }
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
    /// row and no pinned copy is ever planned.
    #[test]
    fn property_evictions_keep_mandatory_and_anchor() {
        let dcs = ["leli", "toro", "nrb"];
        let tiers = [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold];
        for seed in 1..300u64 {
            let mut r = Rng(seed * 0x9E37_79B9_7F4A_7C15);
            let mut vols = HashMap::new();
            for v in ["a", "b", "c"] {
                let mut p = policy(r.next(2) as u32);
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
                let mut cands: Vec<OldestChunk> = (0..14).map(|h| cand(&format!("h{h}"), r.next(100), 1 + r.next(10))).collect();
                cands.sort_by_key(|c| c.last_read_ms);
                let target = r.next(40);
                let ev = plan_capacity_eviction(n, 0, &cands, &chunks, &vols, &prot.view(), target, &mut removed);
                // Oldest-first, cap, and stop-at-target.
                assert!(ev.len() <= MAX_EVICTIONS_PER_ROUND);
                let sizes: Vec<u64> = ev.iter().map(|e| e.size_bytes).collect();
                if let Some((_, before_last)) = sizes.split_last() {
                    assert!(before_last.iter().sum::<u64>() < target, "seed {seed}: planned past the target");
                }
                for e in ev {
                    assert!(!prot.pinned.contains(&(e.chunk_id.clone(), n, 0)), "seed {seed}: pin evicted");
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
