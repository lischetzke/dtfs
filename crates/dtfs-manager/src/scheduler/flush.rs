//! TaskScheduler: cache-flush planning and execution. Split out of scheduler.rs on 2026-09-13; the
//! `impl TaskScheduler` block here is an inherent impl continued from the
//! parent, and `use super::*` gives every item the parent's imports.
#![allow(unused_imports)]

use super::*;

impl TaskScheduler {
    /// Bug AV — flush opportunistic cache replicas for every file whose path
    /// starts with `path_prefix`. For each chunk:
    ///
    /// 1. Resolve mandatory tiers for the chunk's volume (via
    ///    `VolumeConfig::effective_mandatory_replicas` from AV-1-2).
    /// 2. Identify candidate replicas — those on tiers in `cache_tiers`
    ///    AND not in any mandatory tier with rf > 0 for this volume.
    /// 3. Verify the chunk still has at least one mandatory-tier replica
    ///    after the proposed removals. If not, audit a refusal and skip.
    /// 4. CO-1-3 — Raft-remove-FIRST: issue `RemoveChunkLocation` Raft
    ///    writes (AV-1-3) to drop the SM entries, THEN `DeleteChunkReplica`
    ///    on each candidate node (AV-1-6), then a single `SetChunkCooldown`
    ///    Raft write (AV-1-4) covering every affected chunk_index. A
    ///    byte-delete RPC that fails after a committed remove only leaves
    ///    de-located bytes the CO-3-2 orphan reaper reclaims; the Raft
    ///    remove is never rolled back.
    ///
    /// Returns a [`FlushCacheReport`] mirroring `PathMigrationReport`'s
    /// shape so the REST + CLI handlers can present a uniform summary.
    pub async fn flush_cache_for_path(
        &self,
        path_prefix: &str,
        cache_tiers: &[DiskTier],
        cooldown_secs: u64,
        max_tasks: usize,
    ) -> FlushCacheReport {
        // Snapshot SM under one read lock so all planning decisions are
        // made against a consistent view (same pattern as
        // `schedule_path_migration`).
        let snapshot: FlushSnapshot = {
            let sm = self.state_data.read().await;
            FlushSnapshot::from_sm(&sm, path_prefix)
        };

        let (actions, mut report) =
            plan_flush_cache(&snapshot, cache_tiers, max_tasks);

        // AV-1-11 — audit + alert on refusals BEFORE the dispatch loop
        // so the operator sees the warning even when nothing was
        // actually deletable (e.g. every chunk would have stranded the
        // anchor). One audit entry per refused chunk; one deduped
        // alert per call when refusals > 0.
        if !report.refusals.is_empty() {
            for refusal in &report.refusals {
                self.audit.log(
                    "scheduler",
                    "cache.flush_refused",
                    &format!(
                        "file={} idx={} reason={}",
                        refusal.file_path, refusal.chunk_index, refusal.reason
                    ),
                    "refused",
                );
            }
            if let Some(alerts) = self.alerts.read().clone() {
                let sample: Vec<serde_json::Value> = report
                    .refusals
                    .iter()
                    .take(20)
                    .map(|r| {
                        serde_json::json!({
                            "file_path": r.file_path,
                            "chunk_index": r.chunk_index,
                            "reason": r.reason,
                        })
                    })
                    .collect();
                alerts.fire(
                    crate::alerts::AlertKind::AnchorViolated,
                    "flush_cache refused: would have stranded mandatory replica(s)",
                    serde_json::json!({
                        "subsystem": "cache_flush",
                        "path_prefix": path_prefix,
                        "refusal_count": report.refusals.len(),
                        "files_matched": report.files_matched,
                        "chunks_examined": report.chunks_examined,
                        "sample": sample,
                    }),
                );
            }
        }

        if actions.is_empty() {
            // Still emit the completion summary so audit timelines tell
            // the full story (operator sees they ran a flush; planner
            // produced 0 actions but possibly N refusals).
            self.audit.log(
                "scheduler",
                "cache.flush_complete",
                &format!(
                    "prefix={} replicas_dropped=0 mandatory_refusals={}",
                    path_prefix, report.mandatory_refusals
                ),
                "success",
            );
            return report;
        }

        // Dispatch deletes. We don't currently bound the parallelism per
        // source node — the typical operator-driven flush is at-most-a-few-
        // hundred chunks. If we ever start seeing 10k-chunk flushes, gate
        // this with a per-source Semaphore (max 8 in-flight per node).
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cooldown_until_ms = now_ms.saturating_add(cooldown_secs.saturating_mul(1000));

        // Group affected chunk_indexes per file_path for one
        // SetChunkCooldown write per file at the end.
        let mut cooldowns: HashMap<String, std::collections::BTreeSet<u64>> =
            HashMap::new();
        let named = dtfs_raft::state_machine::cluster_understands_named_remove(
            &*self.state_data.read().await,
        );

        for action in &actions {
            let Some(node_addr) = self.node_grpc_addr(action.node_id).await else {
                report.unreachable_nodes += 1;
                continue;
            };
            // CO-1-3 — Raft-remove-FIRST ordering via `delocate_replica`:
            // drop the SM entry (with the FILE's own volume anchor as
            // `enforce_anchor`, CO-5-7) BEFORE touching bytes, so the
            // flush is atomic at the metadata layer. On a Raft failure no
            // byte delete is issued (replica stays live); on a
            // post-commit RPC failure the leftover bytes are reclaimed by
            // the CO-3-2 orphan reaper.
            let delocate = SchedulerDelocate {
                sched: self,
                remove_req: ClientRequest::RemoveChunkLocation { now_ms: now_millis(),
                    path: action.file_path.clone(),
                    chunk_index: action.chunk_index,
                    node_id: action.node_id.to_string(),
                    // CO-1-1 — drop only this (node, disk) SM entry.
                    disk_id: action.disk_id,
                    enforce_anchor: action.anchor.clone(),
                    chunk_hash: named.then(|| action.chunk_id.clone()),
                },
                node_addr,
                chunk_id: action.chunk_id.clone(),
                disk_id: action.disk_id,
                tier: action.tier.to_string(),
            };
            match delocate_replica(&delocate).await {
                DelocateOutcome::RemoveFailed => {
                    warn!(
                        chunk = %action.chunk_id,
                        "RemoveChunkLocation Raft write failed during flush"
                    );
                    report.raft_failures += 1;
                    continue;
                }
                outcome => {
                    // SM entry is gone -> the replica is de-located. Count
                    // it dropped and schedule the cooldown regardless of
                    // the byte-delete outcome.
                    cooldowns
                        .entry(action.file_path.clone())
                        .or_default()
                        .insert(action.chunk_index);
                    report.replicas_dropped += 1;
                    if outcome == DelocateOutcome::RemovedBytesLeftover {
                        warn!(
                            chunk = %action.chunk_id,
                            node = action.node_id,
                            "DeleteChunkReplica RPC failed after Raft remove during flush (orphan reaper will reclaim leftover bytes)"
                        );
                        report.rpc_failures += 1;
                    }
                    self.audit.log(
                        "scheduler",
                        "cache.flush_replica",
                        &format!(
                            "chunk={} file={} idx={} node={}",
                            action.chunk_id,
                            action.file_path,
                            action.chunk_index,
                            action.node_id
                        ),
                        if outcome == DelocateOutcome::Done { "success" } else { "rpc_failed_after_remove" },
                    );
                }
            }
        }

        // One SetChunkCooldown per file covering every affected
        // chunk_index. Tiering engine refuses re-promotion while these
        // are in the future.
        for (path, idxs) in cooldowns {
            let req = ClientRequest::SetChunkCooldown {
                path,
                chunk_indexes: idxs.into_iter().collect(),
                expires_at_ms: cooldown_until_ms,
            };
            if let Err(e) = self.raft.client_write(req).await {
                warn!(error = %e, "SetChunkCooldown Raft write failed during flush");
                report.raft_failures += 1;
            }
        }

        self.audit.log(
            "scheduler",
            "cache.flush_complete",
            &format!(
                "prefix={} replicas_dropped={} mandatory_refusals={}",
                path_prefix, report.replicas_dropped, report.mandatory_refusals
            ),
            "success",
        );

        report
    }

    // ── Bug BF-7 — auto-unquarantine cooldown sweep ──────────────────────
}

/// Bug AV — snapshot extracted from the state machine for the flush
/// planner. Contains exactly what the planner needs: files matching the
/// prefix, their volumes' effective mandatory replication policy, and a
/// node table mapping `node_id → (datacenter, [disk_tier])`. Built once
/// under a single SM read lock by the async wrapper.
#[derive(Debug, Clone)]
pub(crate) struct FlushSnapshot {
    /// CO-5-7 — per-file `(path, chunks, mandatory, anchor)`. The anchor is
    /// resolved from the file's OWN volume (not the default volume), so a
    /// flush on a non-default volume enforces its own anchor invariant. The
    /// planner copies it onto each `FlushAction` and the dispatcher threads
    /// it into that action's `RemoveChunkLocation` `enforce_anchor`.
    pub files: Vec<(
        String,
        Vec<ChunkLocationEntry>,
        TierReplication,
        Option<dtfs_common::types::AnchorPolicy>,
    )>,
    pub nodes: HashMap<u64, FlushNodeSummary>,
}

#[derive(Debug, Clone)]
pub(crate) struct FlushNodeSummary {
    pub datacenter: String,
    /// CNF-1 — `(stable disk_id, tier, live)` triples; disk_id is a key,
    /// not a Vec position (a reindexed disk list breaks positional
    /// lookups). CR-3-1/MGR-2 — `live` is `live_replica_tier(node,
    /// disk_id).is_some()`, resolved once at snapshot-build time: false
    /// when the node is Down/Failed or the disk is Failed/Removed, so a
    /// dead replica never counts as the surviving mandatory copy.
    pub disk_tiers: Vec<(u32, DiskTier, bool)>,
}

impl FlushSnapshot {
    pub(super) fn from_sm(sm: &StateMachineData, path_prefix: &str) -> Self {
        let nodes: HashMap<u64, FlushNodeSummary> = sm
            .nodes
            .iter()
            .map(|(id, info)| {
                (
                    *id,
                    FlushNodeSummary {
                        datacenter: info.location.datacenter.clone(),
                        disk_tiers: info
                            .disks
                            .iter()
                            .map(|d| {
                                // #114 — a survivor, not just repair-live.
                                let live = survivor_replica_tier(Some(info), d.disk_id).is_some();
                                (d.disk_id, d.tier, live)
                            })
                            .collect(),
                    },
                )
            })
            .collect();
        let mut files = Vec::new();
        for (path, meta) in sm.files.iter() {
            if !path_matches_prefix(path, path_prefix) {
                continue;
            }
            // CO-5-7 — resolve mandatory replicas AND the anchor from the
            // file's OWN volume, so a flush on a non-default volume enforces
            // its own anchor (pre-fix the anchor was the default volume's,
            // which could either over-protect or fail to protect a
            // non-default volume's actual anchor DC/tier).
            let vol = sm.volumes.get(&meta.volume_id);
            let mandatory = vol
                .map(|v| v.effective_mandatory_replicas().clone())
                .unwrap_or_else(TierReplication::default);
            let anchor = vol.and_then(|v| v.effective_anchor());
            files.push((path.clone(), meta.chunks.clone(), mandatory, anchor));
        }
        Self { files, nodes }
    }
}

/// Bug AV — one replica to drop, produced by the flush planner. The
/// dispatcher feeds these one-by-one into `delete_chunk_replica_on_node`
/// + `RemoveChunkLocation`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushAction {
    pub file_path: String,
    pub chunk_index: u64,
    pub chunk_id: String,
    pub node_id: u64,
    /// CO-1-1 — the specific disk this cache replica lives on. The
    /// dispatcher passes it to both the data-node delete (so only this
    /// disk's copy is unlinked) and `RemoveChunkLocation` (so only this
    /// `(node, disk)` SM entry is dropped), protecting a co-located
    /// mandatory anchor on another disk of the same node.
    pub disk_id: u32,
    /// CO-1-1 — the tier of the disk being flushed, carried so the
    /// data-node delete can disambiguate by tier as well as disk_id.
    pub tier: DiskTier,
    /// CO-5-7 — the anchor policy of the FILE's volume, threaded into the
    /// dispatcher's `RemoveChunkLocation` `enforce_anchor` so the apply
    /// path's defence-in-depth anchor check uses this file's own anchor,
    /// not the default volume's.
    pub anchor: Option<dtfs_common::types::AnchorPolicy>,
}

/// Bug AV (AV-1-11) — one entry per chunk the planner declined to
/// flush because doing so would have stranded the mandatory-tier
/// replicas. The dispatcher consumes these to emit audit + alert
/// hooks (one audit per refusal; one deduped alert per call when
/// any refusals occurred).
///
/// `reason` is a stable, machine-readable string so log scrapers
/// (and S15-style scenarios) can grep without parsing free text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlushRefusal {
    pub file_path: String,
    pub chunk_index: u64,
    /// Stable reason code. Currently the only refusal cause is
    /// `"mandatory_strand"`; reserved for future additions
    /// (e.g. `"cooldown_active"` if AV-2 layers another rule on top).
    pub reason: &'static str,
}

/// Bug AV — operator-facing summary of a flush_cache call. Mirrors
/// PathMigrationReport's shape so the REST + CLI presenters share UX.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct FlushCacheReport {
    pub files_matched: usize,
    pub chunks_examined: usize,
    pub replicas_dropped: usize,
    /// Number of chunks where the proposed flush would have stranded the
    /// mandatory-tier replicas. Skipped (NOT counted in
    /// `replicas_dropped`); operator should investigate.
    pub mandatory_refusals: usize,
    pub truncated: usize,
    /// Per-action runtime failure counters. Populated by the dispatcher,
    /// always 0 from the pure planner.
    #[serde(default)] pub unreachable_nodes: usize,
    #[serde(default)] pub rpc_failures: usize,
    #[serde(default)] pub raft_failures: usize,
    /// AV-1-11 — detailed per-refusal records. Kept out of the JSON
    /// wire shape (large for big prefixes; the counter is sufficient
    /// for the operator-facing report). The dispatcher consumes the
    /// list internally to emit audit log entries and an alert; tests
    /// inspect it to verify the planner's refusal classification.
    #[serde(skip)] pub(crate) refusals: Vec<FlushRefusal>,
}

/// Bug AV — pure planner kernel. Walks the snapshot, identifies cache-
/// tier replicas eligible for deletion, refuses anything that would
/// strand a mandatory-tier replica.
///
/// `cache_tiers` is the operator-supplied list of tiers to flush. The
/// planner additionally filters out any tier that's also configured as
/// mandatory for the file's volume (so e.g. a volume with
/// `mandatory_replicas.warm = 1` won't have its warm replicas dropped
/// even if the operator passed `cache_tiers = [warm]`).
pub(crate) fn plan_flush_cache(
    snapshot: &FlushSnapshot,
    cache_tiers: &[DiskTier],
    max_tasks: usize,
) -> (Vec<FlushAction>, FlushCacheReport) {
    let mut actions: Vec<FlushAction> = Vec::new();
    let mut report = FlushCacheReport::default();

    for (file_path, chunks, mandatory, anchor) in &snapshot.files {
        report.files_matched += 1;
        // Group by chunk_index so we evaluate each chunk as a unit.
        let mut by_index: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
            std::collections::BTreeMap::new();
        for c in chunks {
            by_index.entry(c.chunk_index).or_default().push(c);
        }

        for (chunk_index, entries) in by_index {
            report.chunks_examined += 1;

            // Categorise replicas as cache-eligible or mandatory.
            // A tier is mandatory when mandatory_replicas.<tier> > 0.
            let mandatory_tier = |tier: DiskTier| -> bool {
                mandatory.for_tier(tier) > 0
            };
            // Resolve tier of a stored replica via the node table.
            let entry_tier = |e: &ChunkLocationEntry| -> Option<DiskTier> {
                let nid: u64 = e.node_id.parse().ok()?;
                let node = snapshot.nodes.get(&nid)?;
                // CNF-1 — stable disk_id key, never Vec index.
                node.disk_tiers
                    .iter()
                    .find(|(id, _, _)| *id == e.disk_id)
                    .map(|(_, t, _)| *t)
            };
            // CR-3-1/MGR-2 — true only when the replica is on a live
            // (node not Down/Failed, disk not Failed/Removed) placement.
            // A dead replica must never count as the surviving mandatory
            // copy that lets the planner drop the cache replica.
            let entry_live = |e: &ChunkLocationEntry| -> bool {
                let Ok(nid) = e.node_id.parse::<u64>() else {
                    return false;
                };
                let Some(node) = snapshot.nodes.get(&nid) else {
                    return false;
                };
                node.disk_tiers
                    .iter()
                    .any(|(id, _, live)| *id == e.disk_id && *live)
            };

            // Survivors AFTER the proposed flush. We refuse if no
            // mandatory-tier replica would remain.
            let mut would_drop: Vec<&ChunkLocationEntry> = Vec::new();
            let mut would_remain: Vec<&ChunkLocationEntry> = Vec::new();
            for e in &entries {
                let tier = entry_tier(e);
                let is_cache = match tier {
                    Some(t) => cache_tiers.contains(&t) && !mandatory_tier(t),
                    None => false, // unresolvable tier → leave alone
                };
                if is_cache {
                    would_drop.push(e);
                } else {
                    would_remain.push(e);
                }
            }

            if would_drop.is_empty() {
                continue;
            }

            // Refuse if no LIVE mandatory-tier replica survives (CR-3-1/
            // MGR-2: a replica on a Down/Failed node or Failed/Removed
            // disk cannot serve reads, so it must not be treated as the
            // surviving anchor for this refusal check).
            let mandatory_survives = would_remain.iter().any(|e| {
                entry_live(e) && entry_tier(e).map(mandatory_tier).unwrap_or(false)
            });
            if !mandatory_survives {
                report.mandatory_refusals += 1;
                // AV-1-11: record the refusal so the dispatcher can
                // audit + alert. Reason is stable for log scrapers.
                report.refusals.push(FlushRefusal {
                    file_path: file_path.clone(),
                    chunk_index,
                    reason: "mandatory_strand",
                });
                continue;
            }

            // Cap on number of in-flight actions. Once we hit max_tasks
            // we stop enqueueing — operator re-runs the call to pick up
            // remaining work.
            for e in would_drop {
                // CO-5-8 — once capped, count each remaining would-drop
                // entry directly into `truncated` (the count of replicas
                // we declined to flush this call), not the old
                // `chunks_examined - actions.len()` which mixed chunk and
                // replica-action units and double-/under-counted.
                if actions.len() >= max_tasks {
                    report.truncated += 1;
                    continue;
                }
                let Ok(nid) = e.node_id.parse::<u64>() else {
                    continue;
                };
                // CO-1-1 — the tier was already resolved when we
                // classified the entry as cache; an unresolvable tier
                // would never have landed in `would_drop`.
                let Some(tier) = entry_tier(e) else {
                    continue;
                };
                actions.push(FlushAction {
                    file_path: file_path.clone(),
                    chunk_index,
                    chunk_id: e.chunk_hash.clone(),
                    node_id: nid,
                    disk_id: e.disk_id,
                    tier,
                    // CO-5-7 — carry the file's own volume anchor.
                    anchor: anchor.clone(),
                });
            }
        }
    }

    (actions, report)
}
