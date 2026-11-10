//! TaskScheduler: under-replication / misplacement / anchor-lag repair planning and scheduling. Split out of scheduler.rs on 2026-09-13; the
//! `impl TaskScheduler` block here is an inherent impl continued from the
//! parent, and `use super::*` gives every item the parent's imports.
#![allow(unused_imports)]

use super::*;

impl TaskScheduler {
    /// Schedule a replication task for a specific chunk.
    ///
    /// Creates a new task in `Pending` state that will be picked up on the
    /// next scheduler tick. Returns false when refused for the live-task budget.
    pub async fn schedule_replication(
        &self,
        chunk_id: &str,
        source_node: u64,
        target_node: u64,
        tier: DiskTier,
    ) -> bool {
        let out = self
            .schedule_replications(
                &[(chunk_id, source_node, target_node, tier)],
                usize::MAX,
                MAX_LIVE_TASKS,
            )
            .await;
        out[0] != ScheduleOutcome::Refused
    }

    /// Schedule many replication tasks with ONE migration-state load/save.
    /// Only newly created tasks count toward `max_new`; already-scheduled
    /// pairs are skipped for free. Returns one outcome per request.
    /// `max_live` is the non-terminal-task ceiling for THIS caller (tiering
    /// uses a lower one so promotions cannot starve mandatory repair).
    /// When the tick holds the migration-state lock (it can for minutes
    /// during an advance) every request comes back `Deferred` instead of
    /// blocking the caller; the caller re-plans next tick / returns 503.
    pub(crate) async fn schedule_replications(
        &self,
        reqs: &[(&str, u64, u64, DiskTier)],
        max_new: usize,
        max_live: usize,
    ) -> Vec<ScheduleOutcome> {
        // CO-3-5 — serialise with the tick's load-modify-save (and any
        // other concurrent schedule) so neither writer clobbers the
        // other's tasks or regresses `next_task_id`.
        let Ok(_ms_guard) = self.migration_state_lock.try_lock() else {
            warn!(
                requests = reqs.len(),
                "replication scheduling deferred: migration state busy (scheduler tick in progress)"
            );
            return vec![ScheduleOutcome::Deferred; reqs.len()];
        };
        let mut state = self.load_migration_state().await;
        let outcomes = plan_replication_batch(&mut state, reqs, max_new, max_live, now_millis());
        let mut created = 0usize;
        for (r, o) in reqs.iter().zip(&outcomes) {
            match o {
                ScheduleOutcome::New => {
                    created += 1;
                    info!(chunk_id = %r.0, source = r.1, target = r.2, "Scheduled replication task");
                    self.audit.log(
                        "scheduler",
                        "replication.schedule",
                        &format!("chunk={} src={} dst={}", r.0, r.1, r.2),
                        "pending",
                    );
                }
                ScheduleOutcome::Refused => warn!(
                    chunk_id = %r.0,
                    max_live,
                    "replication task refused: live task budget exhausted"
                ),
                _ => {}
            }
        }
        if created > 0 {
            self.save_migration_state(&state).await;
        }
        outcomes
    }

    /// AV-5-5 — `(chunk_id, target_node)` pairs with a replication task
    /// still in flight (any status except Done / Failed). The AV-2-5 demote
    /// dispatcher consults this (loaded once per tick) to avoid racing a
    /// just-completed PutChunk against a stale planner-emitted demote (the
    /// planner's snapshot can predate the UpdateChunkLocations Raft write
    /// that records the new replica).
    pub(crate) async fn active_replication_pairs(
        &self,
    ) -> std::collections::HashSet<(String, u64)> {
        active_pairs_of(&self.load_migration_state().await)
    }

    /// Check the cluster state for under-replicated chunks and schedule
    /// re-replication tasks for them.
    pub(super) async fn check_under_replicated(&self, migration_state: &mut MigrationState) {
        let sm = self.state_data.read().await;

        // REV-3-4 — bound the work this tick can queue, and build the dedup
        // index ONCE. The old dedup was a full scan of the task map for every
        // candidate, i.e. quadratic in exactly the situation that produced the
        // backlog. `created` tracks what this pass adds.
        let mut created: usize = 0;
        // CR-3-5/MGR-5 — `task_blocks_requeue` (not a bespoke `!= Done &&
        // != Failed`) so a Failed task's retention window also blocks an
        // immediate re-queue here: pre-fix, a deterministically-failing
        // chunk (e.g. its only source replica sits on a Failed disk) got
        // a fresh task every 30s tick forever, growing an unbounded Failed
        // backlog for the same chunk.
        let mut live_index: std::collections::HashSet<(String, u64, DiskTier)> = migration_state
            .tasks
            .values()
            .filter(|t| task_blocks_requeue(t.status))
            .map(|t| (t.chunk_id.clone(), t.target_node, t.tier))
            .collect();
        let mut live_tasks = live_task_count(migration_state);

        // Collect healthy DATA nodes and their live tiers. Bug CH — the
        // role filter is explicit (managers/clients register with no
        // disks, but relying on the empty tier list alone is fragile),
        // and a Failed/Removed disk no longer advertises its tier as a
        // placement target.
        // CR-3-5/MGR-5 — carry each live disk's capacity so the target
        // pick below can also require `placement_cfg.has_room`; a
        // healthy node whose disk of the mandated tier is already full
        // is not a valid backfill target either.
        let healthy_nodes: Vec<(u64, Vec<(DiskTier, bool)>)> = sm
            .nodes
            .iter()
            .filter(|(_, info)| {
                info.role == NodeRole::Data && info.health == HealthStatus::Healthy
            })
            .map(|(id, info)| {
                let tiers: Vec<(DiskTier, bool)> = info
                    .disks
                    .iter()
                    .filter(|d| !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed))
                    .map(|d| (d.tier, self.placement_cfg.has_room(d.total_bytes, d.used_bytes)))
                    .collect();
                (*id, tiers)
            })
            .collect();

        // Walk all files; for each LOGICAL chunk check the PER-TIER replica
        // count against the volume's mandatory per-tier requirement.
        //
        // Bug BX-4 — the previous version derived a single `chunk_tier` from
        // one replica row and checked only that tier's count, so a mandated
        // tier with ZERO replicas (e.g. a chunk that landed only on cold under
        // a warm=2,cold=1 volume because warm was full at write) was never
        // noticed and never backfilled. We now group rows by chunk_index and
        // schedule a copy for every tier that is short of its mandatory count.
        //
        // Bug AV (AV-1-8) — under-replication enforces ONLY mandatory tiers;
        // cache tiers are the tiering engine's job.
        // Rotated start: a budget `return` mid-scan must not always cut off
        // the same tail of the namespace.
        for (_path, file_meta) in rotated_from(&sm.files, now_millis()) {
            if file_meta.is_directory || file_meta.symlink_target.is_some() {
                continue;
            }
            let mandatory: TierReplication = sm
                .volumes
                .get(&file_meta.volume_id)
                .map(|v| v.effective_mandatory_replicas().clone())
                .unwrap_or(TierReplication { hot: 3, warm: 0, cold: 0, tape: 0 });

            let mut by_idx: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
                std::collections::BTreeMap::new();
            for c in &file_meta.chunks {
                by_idx.entry(c.chunk_index).or_default().push(c);
            }

            for (_idx, entries) in &by_idx {
                // Bug CH — count a replica only when it is actually
                // serviceable (node not Down/Failed, disk not
                // Failed/Removed). Pre-CH the only cold replica sitting
                // on a dead node still satisfied cold=1, so node loss
                // was never backfilled by this (leader-gated) tick —
                // which is what the runaway monitor evacuation path
                // existed to paper over.
                let tier_of = |e: &ChunkLocationEntry| -> Option<DiskTier> {
                    let nid: u64 = e.node_id.parse().ok()?;
                    live_replica_tier(sm.nodes.get(&nid), e.disk_id)
                };
                let count_on = |t: DiskTier| entries.iter().filter(|e| tier_of(e) == Some(t)).count() as u32;

                let chunk_hash = match entries.first() {
                    Some(e) => e.chunk_hash.clone(),
                    None => continue,
                };
                // CR-3-5/MGR-5 — the source must be LIVE (`live_replica_tier`,
                // node not Down/Failed AND its specific disk not Failed/
                // Removed), not merely "some row parses and the node
                // happens to be healthy": a copy sitting on a Failed disk
                // of an otherwise-healthy node is not actually readable.
                let Some(source_node_id) = pick_live_source(entries, &sm.nodes) else {
                    continue; // no live source to copy from
                };

                // Schedule a copy for every mandated tier that is short.
                for (tier, _deficit) in under_replicated_tiers(count_on, &mandatory) {
                    // Bug BH — exclude a node that already holds this chunk ON
                    // THE TARGET TIER, but a node holding it on a DIFFERENT
                    // tier is still a valid target (single-node multi-tier
                    // topology: data01 warm+cold).
                    let tier_holders: std::collections::HashSet<u64> = entries
                        .iter()
                        .filter(|e| tier_of(e) == Some(tier))
                        .filter_map(|e| e.node_id.parse().ok())
                        .collect();

                    let Some(target) = pick_backfill_target(&healthy_nodes, tier, &tier_holders)
                    else {
                        continue;
                    };

                    // REV-3-4 — O(1) against the index built once per tick.
                    // This used to be a full scan of every task for every
                    // candidate, so a large backlog made the pass quadratic in
                    // exactly the situation that produced the backlog.
                    if live_index.contains(&(chunk_hash.clone(), target, tier)) {
                        continue;
                    }

                    if !task_budget_available(created, live_tasks) {
                        // The scan is idempotent — whatever is left is picked
                        // up next tick. Say so once, loudly, so an operator
                        // watching a node loss knows the backlog is paced and
                        // not stuck (CLAUDE.md: no silent caps).
                        warn!(
                            created,
                            live_tasks,
                            max_new = MAX_NEW_TASKS_PER_TICK,
                            max_live = MAX_LIVE_TASKS,
                            "REV-3-4: re-replication task budget reached for this tick; \
                             remaining deficits resume next tick"
                        );
                        return;
                    }

                    let now_ms = now_millis();
                    let task_id = format!("repl-{}", migration_state.next_task_id);
                    migration_state.next_task_id += 1;
                    let task = ReplicationTask {
                        task_id: task_id.clone(),
                        chunk_id: chunk_hash.clone(),
                        source_node: source_node_id,
                        target_node: target,
                        tier,
                        status: MigrationStatus::Pending,
                        created_at_ms: now_ms,
                        updated_at_ms: now_ms,
                        attempts: 0,
                        last_error: None,
                    };
                    info!(
                        task_id = %task_id,
                        chunk = %chunk_hash,
                        source = source_node_id,
                        target = target,
                        tier = ?tier,
                        "Bug BX-4 auto-scheduled re-replication for a short mandated tier"
                    );
                    migration_state.tasks.insert(task_id, task);
                    live_index.insert((chunk_hash.clone(), target, tier));
                    created += 1;
                    live_tasks += 1;
                }
            }
        }
    }

    /// Bug AV (AV-3-3) — Auto-anchor-migration check.
    ///
    /// For every chunk whose volume has an anchor policy, verifies
    /// that at least one replica satisfies the anchor invariant
    /// (in the anchor DC; if `required_tier` is set, on that tier).
    /// If not, schedules a `ReplicationTask` targeting a healthy
    /// node in the anchor DC with the required tier.
    ///
    /// This is what makes the warm-first write pattern safe: the
    /// FUSE client (AV-3-1/3-2) ACKs on a warm placement, then
    /// this background pass schedules the cold (anchor) copy. The
    /// volume's mandatory-replica scheduler
    /// (`check_under_replicated`) would also notice the missing
    /// cold copy, but that path only fires when the
    /// `mandatory_replicas.<tier>` counter is below threshold —
    /// the anchor invariant is a separate concern (which DC the
    /// replica lives in, not how many of them exist).
    /// Bug BX-11 — relocate-by-removal pass. Drops ONE excess spill replica
    /// per over-replicated chunk (via the pure `plan_excess_spill_removal`,
    /// which guards mandatory tiers + anchor + cache allowance), issuing
    /// `RemoveChunkLocation` with `enforce_anchor` as defence-in-depth.
    /// Bounded per tick; leader-gated by the caller. Combined with the
    /// per-tier backfill (BX-4) + anchor-lag, a capacity-spilled chunk
    /// converges back to its correct placement once capacity returns.
    pub(super) async fn check_misplaced_rebalance(&self) {
        const MAX_REMOVALS_PER_TICK: usize = 50;

        let in_flight: std::collections::HashSet<String> = self
            .load_migration_state()
            .await
            .tasks
            .values()
            .filter(|t| t.status != MigrationStatus::Done && t.status != MigrationStatus::Failed)
            .map(|t| t.chunk_id.clone())
            .collect();

        // Phase 1 — under a read lock, decide the safe removals.
        let removals: Vec<(String, u64, u64, u32, Option<dtfs_common::types::AnchorPolicy>, String)> = {
            let sm = self.state_data.read().await;
            let mut out = Vec::new();
            'outer: for (path, fm) in &sm.files {
                if fm.is_directory || fm.symlink_target.is_some() {
                    continue;
                }
                let Some(vol) = sm.volumes.get(&fm.volume_id) else {
                    continue;
                };
                let mandatory = vol.effective_mandatory_replicas().clone();
                let anchor = vol.effective_anchor();

                let mut by_idx: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
                    std::collections::BTreeMap::new();
                for c in &fm.chunks {
                    by_idx.entry(c.chunk_index).or_default().push(c);
                }
                for (idx, entries) in by_idx {
                    // A chunk with a promotion/replication in flight is mid-way
                    // to its goal; removing now causes remove / re-promote loops.
                    if entries.first().map_or(false, |e| in_flight.contains(e.chunk_hash.as_str())) {
                        continue;
                    }
                    // CO-1-1 — carry disk_id alongside (node, dc, tier) so
                    // the removal targets the exact (node, disk) replica,
                    // never a co-located anchor copy on another disk.
                    let reps: Vec<(u64, u32, String, DiskTier)> = entries
                        .iter()
                        .filter_map(|e| {
                            let nid = e.node_id.parse::<u64>().ok()?;
                            let info = sm.nodes.get(&nid)?;
                            // CNF-1 — stable disk_id key, never Vec index.
                            // REV-3-7 — count only SERVICEABLE replicas, the
                            // same rule `check_under_replicated` uses. Counting
                            // a replica on a dead node as "excess" is how a
                            // freshly-backfilled copy got removed again on the
                            // very next tick, forever. #114 — and never let a
                            // Maintenance/Quarantined copy count as a survivor.
                            let tier = survivor_replica_tier(Some(info), e.disk_id)?;
                            Some((nid, e.disk_id, info.location.datacenter.clone(), tier))
                        })
                        .collect();
                    let cache_allow = crate::placement_state::cache_allowance(
                        &vol.cache_replicas,
                        crate::placement_state::distinct_dcs(reps.iter().map(|r| r.2.as_str())),
                    );
                    if let Some((node, disk_id)) =
                        plan_excess_spill_removal(&reps, &mandatory, anchor.as_ref(), cache_allow)
                    {
                        // The removed row's own hash (named in the remove).
                        let node_str = node.to_string();
                        let chunk_hash = entries
                            .iter()
                            .find(|e| e.node_id == node_str && e.disk_id == disk_id)
                            .map(|e| e.chunk_hash.clone())
                            .unwrap_or_default();
                        out.push((path.clone(), idx, node, disk_id, anchor.clone(), chunk_hash));
                        if out.len() >= MAX_REMOVALS_PER_TICK {
                            break 'outer;
                        }
                    }
                }
            }
            out
        };

        // Phase 2 — issue the Raft removals (lock released).
        let named = dtfs_raft::state_machine::cluster_understands_named_remove(
            &*self.state_data.read().await,
        );
        for (path, idx, node, disk_id, enforce, chunk_hash) in removals {
            let req = ClientRequest::RemoveChunkLocation { now_ms: now_millis(),
                path: path.clone(),
                chunk_index: idx,
                node_id: node.to_string(),
                // CO-1-1 — target the exact (node, disk) spill replica.
                disk_id,
                enforce_anchor: enforce,
                chunk_hash: named.then(|| chunk_hash.clone()),
            };
            match self.raft.client_write(req).await {
                // A committed entry can still apply to ClientResponse::Error
                // (refused removal): not removed, so no success log/audit.
                Ok(r) if client_response_ok(&r.data).is_err() => warn!(
                    path = %path,
                    idx,
                    node,
                    disk_id,
                    error = client_response_ok(&r.data).err().unwrap_or_default(),
                    "Bug BX-11 excess-replica removal refused by apply"
                ),
                Ok(_) => {
                    info!(
                        path = %path,
                        idx,
                        node,
                        disk_id,
                        chunk = %chunk_hash,
                        "Bug BX-11 removed an excess spill replica (rebalance toward goal)"
                    );
                    self.audit.log(
                        "scheduler",
                        "rebalance.remove_excess",
                        &format!("chunk={chunk_hash} path={path} idx={idx} node={node} disk={disk_id}"),
                        "removed",
                    );
                }
                Err(e) => warn!(
                    path = %path,
                    idx,
                    node,
                    disk_id,
                    error = %e,
                    "Bug BX-11 excess-replica removal failed"
                ),
            }
        }
    }

    /// REV-3-10 — drop the stale location rows for chunks the state machine
    /// thinks `node_id` holds but which are not on its disks.
    ///
    /// Returns the number of rows that could NOT be dropped because they were
    /// the last live copy of that chunk — genuine data loss, which the caller
    /// escalates instead of healing.
    pub(super) async fn reconcile_missing_replicas(&self, node_id: u64, missing: &[&String]) -> usize {
        let node_str = node_id.to_string();
        let mut unreconcilable = 0usize;
        let wanted: std::collections::HashSet<&str> =
            missing.iter().map(|h| h.as_str()).collect();

        // Re-derive the rows to drop from the CURRENT state: the caller's
        // snapshot predates this node's ListChunks reply. ONE pass over the
        // namespace (not one per missing hash).
        let targets: Vec<(String, u64, u32, Option<dtfs_common::types::AnchorPolicy>, String)> = {
            let sm = self.state_data.read().await;
            let mut out = Vec::new();
            for (path, fm) in &sm.files {
                let anchor = sm
                    .volumes
                    .get(&fm.volume_id)
                    .and_then(|v| v.effective_anchor());
                for c in fm
                    .chunks
                    .iter()
                    .filter(|c| c.node_id == node_str && wanted.contains(c.chunk_hash.as_str()))
                {
                    // Never drop the last live copy: that would leave the
                    // file's chunk index with no locations at all.
                    let live_elsewhere = fm.chunks.iter().any(|o| {
                        o.chunk_index == c.chunk_index
                            && !(o.node_id == node_str && o.disk_id == c.disk_id)
                            && o.node_id
                                .parse::<u64>()
                                .ok()
                                .map(|nid| {
                                    live_replica_tier(sm.nodes.get(&nid), o.disk_id).is_some()
                                })
                                .unwrap_or(false)
                    });
                    if live_elsewhere {
                        out.push((path.clone(), c.chunk_index, c.disk_id, anchor.clone(), c.chunk_hash.clone()));
                    } else {
                        unreconcilable += 1;
                    }
                }
            }
            out
        };

        let named = dtfs_raft::state_machine::cluster_understands_named_remove(
            &*self.state_data.read().await,
        );
        for (path, chunk_index, disk_id, anchor, chunk_hash) in targets {
            let req = ClientRequest::RemoveChunkLocation { now_ms: now_millis(),
                path: path.clone(),
                chunk_index,
                node_id: node_str.clone(),
                // CO-1-1 — target the exact (node, disk) row we found stale.
                disk_id,
                enforce_anchor: anchor,
                chunk_hash: named.then_some(chunk_hash),
            };
            match self.raft.client_write(req).await {
                // Apply refused (e.g. would strand the anchor): the stale row
                // is still there, so count it unreconcilable.
                Ok(r) if client_response_ok(&r.data).is_err() => {
                    warn!(
                        node = node_id,
                        path = %path,
                        chunk_index,
                        error = client_response_ok(&r.data).err().unwrap_or_default(),
                        "REV-3-10: apply refused dropping a missing replica row"
                    );
                    unreconcilable += 1;
                }
                Ok(_) => debug!(
                    node = node_id,
                    path = %path,
                    chunk_index,
                    "REV-3-10: dropped a location row whose bytes are not on disk"
                ),
                Err(e) => {
                    // Refused (would strand the anchor) or the write failed:
                    // escalate rather than silently leaving a lie in place.
                    warn!(
                        node = node_id,
                        path = %path,
                        chunk_index,
                        error = %e,
                        "REV-3-10: could not reconcile a missing replica row"
                    );
                    unreconcilable += 1;
                }
            }
        }
        unreconcilable
    }

    pub(super) async fn check_anchor_lag(&self, migration_state: &mut MigrationState) {
        let sm = self.state_data.read().await;

        // Build per-node summary for the planner.
        let nodes: Vec<AnchorRepairNode> = sm
            .nodes
            .iter()
            .map(|(id, info)| AnchorRepairNode {
                node_id: *id,
                datacenter: info.location.datacenter.clone(),
                healthy: info.health == HealthStatus::Healthy,
                tiers: anchor_target_tiers(info, &self.placement_cfg),
            })
            .collect();

        // Quick lookup: existing task fingerprints (chunk_id, target_node)
        // for in-progress tasks so we don't double-schedule.
        // Bug BO-2: keep Failed tasks in the fingerprint set so a chunk that
        // just exhausted its attempts is NOT immediately re-queued on the
        // next 30s tick. A Failed task lingers for failed_task_retention_ms
        // (30 min default, Bug AE prune); only after GC does check_anchor_lag
        // schedule a fresh attempt. Previously Failed was excluded here, so
        // the same chunk was re-queued every tick → tight loop + audit spam.
        let mut existing_tasks: std::collections::HashSet<(String, u64)> =
            migration_state
                .tasks
                .values()
                .filter(|t| task_blocks_requeue(t.status))
                .map(|t| (t.chunk_id.clone(), t.target_node))
                .collect();

        // CR-3-5/MGR-6 — this pass must obey the same REV-3-4 per-tick
        // budget as `check_under_replicated`; pre-fix it created one task
        // per un-anchored chunk with NO bound at all.
        let mut created: usize = 0;
        let mut live_tasks: usize = live_task_count(migration_state);
        // #118 — chunks with no anchor-DC target, per (dc, tier); one
        // summary warn after the walk instead of one warn per chunk.
        let mut blocked: HashMap<(String, DiskTier), usize> = HashMap::new();

        // Walk every file (rotated start, see check_under_replicated).
        'files: for (file_path, file_meta) in rotated_from(&sm.files, now_millis()) {
            let Some(volume) = sm.volumes.get(&file_meta.volume_id) else {
                continue;
            };
            let Some(anchor) = volume.effective_anchor() else {
                continue;
            };

            // Group chunks by chunk_index so we evaluate each chunk
            // (not each replica row) exactly once.
            let mut by_idx: std::collections::BTreeMap<
                u64,
                Vec<&dtfs_raft::ChunkLocationEntry>,
            > = std::collections::BTreeMap::new();
            for c in &file_meta.chunks {
                by_idx.entry(c.chunk_index).or_default().push(c);
            }

            for (chunk_index, entries) in by_idx {
                // Resolve placements for this chunk. LIS-41 / Bug CH —
                // only serviceable replicas count toward Gate 1
                // satisfaction; a Down dc3/cold must not suppress
                // anchor.lag_repair (and must not keep metrics green).
                // CNF-1 disk_id keying lives inside live_replica_tier.
                let placements: Vec<AnchorRepairPlacement> = entries
                    .iter()
                    .filter_map(|e| {
                        let nid = e.node_id.parse::<u64>().ok()?;
                        let info = sm.nodes.get(&nid)?;
                        let tier = live_replica_tier(Some(info), e.disk_id)?;
                        Some(AnchorRepairPlacement {
                            node_id: nid,
                            dc: info.location.datacenter.clone(),
                            tier,
                        })
                    })
                    .collect();

                // The chunk_id (BLAKE3 hash) is the same across
                // replicas for a given chunk_index; pick the first.
                let Some(chunk_id) = entries.first().map(|e| e.chunk_hash.clone()) else {
                    continue;
                };

                let planned = match plan_anchor_lag_repair_checked(
                    &anchor,
                    &placements,
                    &nodes,
                    &chunk_id,
                    &existing_tasks,
                ) {
                    Ok(p) => p,
                    Err(tier) => {
                        *blocked.entry((anchor.datacenter.clone(), tier)).or_default() += 1;
                        None
                    }
                };
                if let Some(action) = planned {
                    // CR-3-5/MGR-6 — same per-tick budget as
                    // `check_under_replicated`; the scan is idempotent so
                    // whatever's left resumes next tick.
                    if !task_budget_available(created, live_tasks) {
                        warn!(
                            created,
                            live_tasks,
                            max_new = MAX_NEW_TASKS_PER_TICK,
                            max_live = MAX_LIVE_TASKS,
                            "CR-3-5/MGR-6: anchor-lag task budget reached for this tick; \
                             remaining deficits resume next tick"
                        );
                        break 'files;
                    }
                    // Source picks a healthy existing replica (any
                    // tier); the dispatcher tolerates cross-tier
                    // copies.
                    let now_ms = now_millis();
                    let task_id =
                        format!("anchor-{}", migration_state.next_task_id);
                    migration_state.next_task_id += 1;

                    let task = ReplicationTask {
                        task_id: task_id.clone(),
                        chunk_id: chunk_id.clone(),
                        source_node: action.source_node,
                        target_node: action.target_node,
                        tier: action.target_tier,
                        status: MigrationStatus::Pending,
                        created_at_ms: now_ms,
                        updated_at_ms: now_ms,
                        attempts: 0,
                        last_error: None,
                    };
                    info!(
                        task_id = %task_id,
                        chunk = %chunk_id,
                        file = %file_path,
                        idx = chunk_index,
                        source = action.source_node,
                        target = action.target_node,
                        tier = ?action.target_tier,
                        dc = %anchor.datacenter,
                        "Bug AV anchor.lag_repair: scheduling anchor migration"
                    );
                    self.audit.log(
                        "scheduler",
                        "anchor.lag_repair",
                        &format!(
                            "chunk={} file={} idx={} target={} tier={:?} dc={}",
                            chunk_id, file_path, chunk_index,
                            action.target_node, action.target_tier,
                            anchor.datacenter
                        ),
                        "scheduled",
                    );
                    migration_state.tasks.insert(task_id, task);
                    // CR-3-5/MGR-6 — update the dedup set + budget
                    // counters WITHIN this pass, so two files sharing a
                    // hash (content-addressed re-download/copy) don't
                    // both schedule a task against the same
                    // (chunk, target) before either lands.
                    existing_tasks.insert((chunk_id.clone(), action.target_node));
                    created += 1;
                    live_tasks += 1;
                }
            }
        }
        drop(sm);
        for ((dc, tier), chunks) in blocked {
            warn!(
                chunks,
                dc = %dc,
                tier = ?tier,
                "anchor-lag repair: no healthy anchor-DC node with room can take the anchor copy"
            );
        }
    }
}

/// Ceiling on live tasks for tiering promotions: half the global budget, so
/// cache promotions can never fill the queue mandatory/anchor repair needs.
pub(crate) const TIERING_MAX_LIVE_TASKS: usize = MAX_LIVE_TASKS / 2;

/// Tasks that count toward the live budget: non-terminal only. Failed
/// tasks linger for dedup (`task_blocks_requeue`) but are not work.
pub(crate) fn live_task_count(state: &MigrationState) -> usize {
    state
        .tasks
        .values()
        .filter(|t| !matches!(t.status, MigrationStatus::Done | MigrationStatus::Failed))
        .count()
}

/// `(chunk_id, target_node)` of every in-flight (non-terminal) task.
pub(crate) fn active_pairs_of(
    state: &MigrationState,
) -> std::collections::HashSet<(String, u64)> {
    state
        .tasks
        .values()
        .filter(|t| !matches!(t.status, MigrationStatus::Done | MigrationStatus::Failed))
        .map(|t| (t.chunk_id.clone(), t.target_node))
        .collect()
}

/// Iterate `m` starting at index `seed % len` and wrapping, visiting every
/// entry once. Scans that can stop early on a budget use this so a
/// permanent early failure cannot starve the entries after it.
pub(crate) fn rotated_from<K: Ord, V>(
    m: &std::collections::BTreeMap<K, V>,
    seed: u64,
) -> impl Iterator<Item = (&K, &V)> {
    let start = if m.is_empty() { 0 } else { (seed % m.len() as u64) as usize };
    m.iter().skip(start).chain(m.iter().take(start))
}

/// Number of NEW tasks in a batch (what the operator actually got).
pub(crate) fn new_task_count(outcomes: &[ScheduleOutcome]) -> usize {
    outcomes.iter().filter(|o| **o == ScheduleOutcome::New).count()
}

/// Result of one request in a `schedule_replications` batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScheduleOutcome {
    /// A new task was created.
    New,
    /// A live/lingering task for (chunk, target) already exists; free.
    AlreadyScheduled,
    /// Refused: live-task budget exhausted.
    Refused,
    /// Not processed: `max_new` new tasks already created this batch.
    Deferred,
}

/// Pure batch planner behind `schedule_replications`: dedups per
/// (chunk, target) via `task_blocks_requeue`, enforces the live-task budget,
/// and counts only NEW tasks toward `max_new`.
pub(crate) fn plan_replication_batch(
    state: &mut MigrationState,
    reqs: &[(&str, u64, u64, DiskTier)],
    max_new: usize,
    max_live: usize,
    now_ms: u64,
) -> Vec<ScheduleOutcome> {
    let mut live = live_task_count(state);
    let mut out = Vec::with_capacity(reqs.len());
    let mut new = 0usize;
    for &(chunk_id, source_node, target_node, tier) in reqs {
        let dup = state.tasks.values().any(|t| {
            t.chunk_id == chunk_id && t.target_node == target_node && task_blocks_requeue(t.status)
        });
        if dup {
            out.push(ScheduleOutcome::AlreadyScheduled);
            continue;
        }
        if new >= max_new {
            out.push(ScheduleOutcome::Deferred);
            continue;
        }
        if live >= max_live {
            out.push(ScheduleOutcome::Refused);
            continue;
        }
        let task_id = format!("repl-{}", state.next_task_id);
        state.next_task_id += 1;
        state.tasks.insert(
            task_id.clone(),
            ReplicationTask {
                task_id,
                chunk_id: chunk_id.to_string(),
                source_node,
                target_node,
                tier,
                status: MigrationStatus::Pending,
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
                attempts: 0,
                last_error: None,
            },
        );
        live += 1;
        new += 1;
        out.push(ScheduleOutcome::New);
    }
    out
}

/// Tiers a node can take a NEW anchor copy on: live disks (not
/// Failed/Removed) that still have room.
pub(crate) fn anchor_target_tiers(
    info: &dtfs_raft::NodeInfo,
    cfg: &dtfs_common::config::PlacementConfig,
) -> Vec<DiskTier> {
    info.disks
        .iter()
        .filter(|d| !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed))
        .filter(|d| cfg.has_room(d.total_bytes, d.used_bytes))
        .map(|d| d.tier)
        .collect()
}

/// AV-3-3 — projection of a node's state used by the anchor-lag
/// planner.
#[derive(Debug, Clone)]
pub(crate) struct AnchorRepairNode {
    pub node_id: u64,
    pub datacenter: String,
    pub healthy: bool,
    pub tiers: Vec<DiskTier>,
}

/// AV-3-3 — one of a chunk's existing replicas.
#[derive(Debug, Clone)]
pub(crate) struct AnchorRepairPlacement {
    pub node_id: u64,
    pub dc: String,
    pub tier: DiskTier,
}

/// AV-3-3 — the planner's output: a single anchor-repair action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AnchorRepairAction {
    pub source_node: u64,
    pub target_node: u64,
    pub target_tier: DiskTier,
}

/// AV-3-3 pure kernel — decide whether a chunk needs an anchor
/// repair, and if so, pick `source` + `target` for the replication
/// task.
///
/// Returns `Some(AnchorRepairAction)` when:
/// 1. No existing placement satisfies the anchor invariant
///    (`AnchorPolicy::is_satisfied_by`-compatible check inline).
/// 2. A healthy candidate node exists in the anchor DC with the
///    required tier (or any tier, if `required_tier` is `None`).
/// 3. The action isn't already in flight
///    (`(chunk_id, target_node) ∉ existing_tasks`).
///
/// Returns `None` when the invariant is already met, no
/// candidate exists, or the action is already scheduled.
///
/// Target tier selection: when `required_tier` is set, use it
/// exactly. Otherwise prefer cold (the durable anchor tier) if
/// any node in the anchor DC has a cold disk; else fall back to
/// any tier the candidate offers.
/// Bug BX-4 — per-tier replica shortfall for one logical chunk. Returns
/// `(tier, deficit)` for every mandated tier with fewer replicas than the
/// volume requires. Pure + testable; replaces the old single-tier-from-
/// source derivation that missed a mandated tier with ZERO replicas
/// entirely (e.g. a chunk that landed only on cold under a warm=2,cold=1
/// volume never got its warm copies scheduled).
pub(crate) fn under_replicated_tiers<F: Fn(DiskTier) -> u32>(
    count_on_tier: F,
    mandatory: &TierReplication,
) -> Vec<(DiskTier, u32)> {
    let mut out = Vec::new();
    for tier in [DiskTier::Hot, DiskTier::Warm, DiskTier::Cold, DiskTier::Tape] {
        let want = mandatory.for_tier(tier);
        let have = count_on_tier(tier);
        if want > have {
            out.push((tier, want - have));
        }
    }
    out
}

/// Bug BX-5 — choose ONE safe excess replica to drop from an
/// over-replicated chunk so it converges toward its goal, without dropping
/// below any mandated tier or stranding the anchor, and without touching
/// legitimate cache replicas (within `cache_allowance`). Returns the
/// `node_id` to remove, or None when no safe removal exists.
///
/// Only acts when the chunk is genuinely OVER goal
/// (`total > mandatory_total + cache_allowance`) — the same threshold
/// BX-3 uses for `Overgoal` — so a chunk carrying mandated + legit cache
/// copies is left alone (the tiering engine owns cache demotion). A
/// candidate removal is accepted only if every mandated tier AND the
/// anchor remain satisfied afterwards, so this can never under-replicate
/// or strand the anchor. Pure + deterministic (first safe candidate in
/// replica order).
// CO-1-1 — each replica is `(node_id, disk_id, dc, tier)`. The removal is
// keyed on the exact `(node, disk)` slot so a co-located anchor copy on a
// different disk of the same node is never the one dropped. Returns the
// `(node_id, disk_id)` of the replica to remove.
pub(crate) fn plan_excess_spill_removal(
    replicas: &[(u64, u32, String, DiskTier)],
    mandatory: &TierReplication,
    anchor: Option<&dtfs_common::types::AnchorPolicy>,
    cache_allowance: u32,
) -> Option<(u64, u32)> {
    let total = replicas.len() as u32;
    let mandatory_total = mandatory.hot + mandatory.warm + mandatory.cold + mandatory.tape;
    if total <= mandatory_total + cache_allowance {
        return None; // not over goal — leave cache/tiering alone
    }
    for (idx, (node, disk_id, _dc, _tier)) in replicas.iter().enumerate() {
        let remaining: Vec<&(u64, u32, String, DiskTier)> = replicas
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, r)| r)
            .collect();
        if crate::placement_state::goal_met(
            remaining.iter().map(|(_, _, dc, tt)| (dc.as_str(), *tt)),
            mandatory,
            anchor,
        ) {
            return Some((*node, *disk_id));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// REV wave-1 hardening — pure predicates for the manager background loops.
// ---------------------------------------------------------------------------

pub(crate) fn plan_anchor_lag_repair(
    anchor: &dtfs_common::types::AnchorPolicy,
    placements: &[AnchorRepairPlacement],
    nodes: &[AnchorRepairNode],
    chunk_id: &str,
    existing_tasks: &std::collections::HashSet<(String, u64)>,
) -> Option<AnchorRepairAction> {
    plan_anchor_lag_repair_checked(anchor, placements, nodes, chunk_id, existing_tasks)
        .ok()
        .flatten()
}

/// #118 — `Err(tier)` when the anchor is unmet and no healthy anchor-DC
/// node with room on `tier` can take the copy, so `check_anchor_lag` can
/// count those chunks and log ONE summary per tick instead of one warn
/// per chunk (the anchor node Down makes every chunk hit this).
pub(crate) fn plan_anchor_lag_repair_checked(
    anchor: &dtfs_common::types::AnchorPolicy,
    placements: &[AnchorRepairPlacement],
    nodes: &[AnchorRepairNode],
    chunk_id: &str,
    existing_tasks: &std::collections::HashSet<(String, u64)>,
) -> Result<Option<AnchorRepairAction>, DiskTier> {
    // Gate 1 — is the anchor already satisfied?
    let satisfied = crate::placement_state::anchor_met(
        anchor,
        placements.iter().map(|p| (p.dc.as_str(), p.tier)),
    );
    if satisfied {
        return Ok(None);
    }

    // Gate 2 — pick a target tier. A required_tier is exact; otherwise
    // prefer cold (the durable anchor floor), then fall back to any tier
    // with room in the anchor DC (`nodes[].tiers` is room-filtered).
    let tier_prefs: &[DiskTier] = match &anchor.required_tier {
        Some(t) => std::slice::from_ref(t),
        None => &[DiskTier::Cold, DiskTier::Warm, DiskTier::Hot, DiskTier::Tape],
    };
    let in_anchor_dc = |n: &&AnchorRepairNode| {
        n.healthy && n.datacenter.eq_ignore_ascii_case(&anchor.datacenter)
    };
    let candidate_tier = tier_prefs
        .iter()
        .copied()
        .find(|t| nodes.iter().filter(in_anchor_dc).any(|n| n.tiers.contains(t)))
        .unwrap_or(tier_prefs[0]);

    // Gate 3 — find a healthy node in the anchor DC with the
    // candidate tier. Bug fix (BF-followup): exclude only nodes that
    // already hold the chunk ON THE CANDIDATE TIER. A node that
    // holds the chunk on warm AND also has a cold disk is a valid
    // target for the cold anchor copy — that single-node
    // multi-tier case is the lab topology (data01 has warm+cold)
    // and previously the holders-set excluded data01 entirely,
    // stranding the anchor-lag pass + leaving cold=0 forever.
    let tier_holders: std::collections::HashSet<u64> = placements
        .iter()
        .filter(|p| p.tier == candidate_tier)
        .map(|p| p.node_id)
        .collect();
    let Some(target) = nodes
        .iter()
        .filter(in_anchor_dc)
        .filter(|n| n.tiers.contains(&candidate_tier))
        .filter(|n| !tier_holders.contains(&n.node_id))
        .min_by_key(|n| n.node_id) // deterministic by node_id
    else {
        debug!(
            chunk = %chunk_id,
            dc = %anchor.datacenter,
            tier = ?candidate_tier,
            "anchor-lag repair: no healthy anchor-DC node with room can take the anchor copy"
        );
        return Err(candidate_tier);
    };

    // Gate 4 — already scheduled?
    if existing_tasks.contains(&(chunk_id.to_string(), target.node_id)) {
        return Ok(None);
    }

    // Source = any existing replica (the dispatcher's
    // `ReplicateChunk` RPC handles cross-tier reads).
    let Some(source) = placements.iter().min_by_key(|p| p.node_id).map(|p| p.node_id) else {
        return Ok(None);
    };

    Ok(Some(AnchorRepairAction {
        source_node: source,
        target_node: target.node_id,
        target_tier: candidate_tier,
    }))
}

// ---------------------------------------------------------------------------
// Bug AE — orphan migration task helpers (pure logic, unit-tested below)
// ---------------------------------------------------------------------------

/// Pure predicate for the scheduler's task-prune retain block. Returns true
/// when the task should stay in the migration state, false when it should
/// be dropped.
///
/// Terminal tasks (`Done`, `Failed`) are dropped once their age exceeds the
/// per-status retention window. Non-terminal tasks (`Pending`, `Copying`,
/// `Verifying`, `Completing`) are always kept — the state machine in
/// `advance_tasks` is responsible for transitioning them onward, including
/// the Bug AE orphan check that fast-forwards orphan tasks to `Failed`.
///
/// CNF-2 — the retention windows are parameters (fed from
/// `raft.{done,failed}_task_retention_secs`, defaults 5 min / 30 min) so
/// the release suite can exercise the Bug CN flood-aftermath prune fast.
#[inline]
pub(crate) fn should_retain_task(
    status: MigrationStatus,
    updated_at_ms: u64,
    now_ms: u64,
    done_retention_ms: u64,
    failed_retention_ms: u64,
) -> bool {
    match status {
        MigrationStatus::Done => {
            now_ms.saturating_sub(updated_at_ms) < done_retention_ms
        }
        MigrationStatus::Failed => {
            now_ms.saturating_sub(updated_at_ms) < failed_retention_ms
        }
        _ => true,
    }
}

/// Bug CN — drop terminal (Done/Failed) tasks past their retention window,
/// returning the count pruned. Pure over the task map so it is unit-testable
/// and can run at the START of `advance_tasks` (before the advance round), so a
/// large terminal backlog left by a flood cannot starve the very GC that
/// exists to clear it.
pub(super) fn prune_terminal_tasks(
    state: &mut MigrationState,
    now_ms: u64,
    done_retention_ms: u64,
    failed_retention_ms: u64,
) -> usize {
    let before = state.tasks.len();
    state.tasks.retain(|_, t| {
        should_retain_task(
            t.status,
            t.updated_at_ms,
            now_ms,
            done_retention_ms,
            failed_retention_ms,
        )
    });
    before.saturating_sub(state.tasks.len())
}

/// Bug AF — pure predicate: does this RPC error string indicate the source
/// data node specifically said "Chunk not found"? When true, the scheduler
/// fast-fails the migration task (bypasses MAX_MIGRATION_ATTEMPTS) so we
/// don't burn 6 × 30s of data-node load on a chunk that's gone.
///
/// Matches both shapes seen in the wild:
///   - "ReplicateChunk error: ReplicateChunk returned error: Chunk not found: <hash>"
///   - "Chunk not found"
#[inline]
pub(crate) fn rpc_error_indicates_chunk_missing(error_message: &str) -> bool {
    error_message.contains("Chunk not found")
}

// ---------------------------------------------------------------------------
// Bug CH — node/disk-loss-aware replica accounting + dispatch target guard
// (pure logic, unit-tested below). See PLAN-BUG-CH /
// ISSUE-SCAN-2026-06-06-2335 for the incident these prevent.
// ---------------------------------------------------------------------------

/// Bug CH — resolve the tier of a replica row ONLY when that replica is
/// actually serviceable: node health not Down/Failed AND backing disk not
/// Failed/Removed. `check_under_replicated` counts coverage through this,
/// so losing a node (or a disk) makes its chunks tier-short and the
/// existing BX-4 backfill repairs them — replacing the monitor's deleted
/// evacuation path as the single, leader-gated repair owner.
///
/// Suspect deliberately still counts: a heartbeat ≤30s overdue must not
/// trigger a cluster-wide re-replication wave.
#[inline]
pub(crate) fn live_replica_tier(
    node: Option<&dtfs_raft::NodeInfo>,
    disk_id: u32,
) -> Option<DiskTier> {
    let node = node?;
    if matches!(node.health, HealthStatus::Down | HealthStatus::Failed) {
        return None;
    }
    // CNF-1 — stable disk_id key, never Vec index.
    let disk = node.disk(disk_id)?;
    if matches!(disk.status, DiskStatus::Failed | DiskStatus::Removed) {
        return None;
    }
    Some(disk.tier)
}

/// #114 — tier of a replica that may AUTHORISE removing another copy
/// (cache demote, flush, rebalance). Stricter than `live_replica_tier`:
/// a Maintenance node may be powered off and a Quarantined node's bytes
/// are suspect, and the monitor never marks either Down. Repair coverage
/// keeps using `live_replica_tier` so these states start no repair storm.
#[inline]
pub(crate) fn survivor_replica_tier(
    node: Option<&dtfs_raft::NodeInfo>,
    disk_id: u32,
) -> Option<DiskTier> {
    let n = node?;
    if n.quarantine.is_some()
        || matches!(n.health, HealthStatus::Maintenance | HealthStatus::Quarantined)
    {
        return None;
    }
    live_replica_tier(node, disk_id)
}

/// CR-3-5/MGR-5 — pick a LIVE replica to copy from: the first entry
/// whose `(node, disk)` resolves to a live tier via `live_replica_tier`
/// (node not Down/Failed, disk not Failed/Removed). `None` when every
/// entry is dead — a replica sitting on a Failed disk of an otherwise-
/// healthy node is not actually readable, so it must never be picked as
/// the copy source.
pub(crate) fn pick_live_source(
    entries: &[&ChunkLocationEntry],
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> Option<u64> {
    entries.iter().find_map(|e| {
        let nid: u64 = e.node_id.parse().ok()?;
        live_replica_tier(nodes.get(&nid), e.disk_id)?;
        Some(nid)
    })
}

/// CR-3-5/MGR-5 — pick a backfill target from `healthy_nodes` (built by
/// `check_under_replicated`): the first node exposing a live disk of
/// `tier` with `has_room`, excluding any node in `exclude` (e.g. one
/// that already holds this chunk on the target tier).
pub(crate) fn pick_backfill_target(
    healthy_nodes: &[(u64, Vec<(DiskTier, bool)>)],
    tier: DiskTier,
    exclude: &std::collections::HashSet<u64>,
) -> Option<u64> {
    healthy_nodes
        .iter()
        .find(|(id, tiers)| {
            !exclude.contains(id) && tiers.iter().any(|(t, has_room)| *t == tier && *has_room)
        })
        .map(|(id, _)| *id)
}

/// Bug CH — dispatch-time guard: can this task's target possibly receive
/// the copy? Err for a target that is unregistered, not a Data node,
/// Down/Failed/Quarantined (BC: quarantined nodes are excluded from every
/// allocation path), or without a live disk of the task's tier. Suspect
/// targets are allowed — the normal attempt/retry machinery handles a
/// transiently slow node.
#[inline]
pub(crate) fn validate_task_target(
    node: Option<&dtfs_raft::NodeInfo>,
    tier: DiskTier,
) -> Result<(), &'static str> {
    let Some(node) = node else {
        return Err("target node not registered");
    };
    if node.role != NodeRole::Data {
        return Err("target node is not a data node");
    }
    if matches!(
        node.health,
        HealthStatus::Down | HealthStatus::Failed | HealthStatus::Quarantined
    ) {
        return Err("target node is down/failed/quarantined");
    }
    let has_live_tier = node.disks.iter().any(|d| {
        d.tier == tier && !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed)
    });
    if !has_live_tier {
        return Err("target node has no live disk of the task tier");
    }
    Ok(())
}
