//! TaskScheduler: orphan reaper, de-location, auto-unquarantine and quarantine gates. Split out of scheduler.rs on 2026-09-13; the
//! `impl TaskScheduler` block here is an inherent impl continued from the
//! parent, and `use super::*` gives every item the parent's imports.

use super::*;

impl TaskScheduler {
    /// Run the auto-unquarantine loop forever. Each tick scans every
    /// Quarantined node; if BOTH `(now - q.since_ms) > cooldown_secs`
    /// AND `(now - q.last_fault_at_ms) > cooldown_secs`, raft-write
    /// `UnquarantineNode` with operator_id="auto-cooldown".
    ///
    /// `cooldown_secs = 0` (the default) DISABLES the loop — operator-
    /// only unquarantine matches the data-consistency-first stance.
    pub async fn run_auto_unquarantine_sweep(
        &self,
        tick_secs: u64,
        cooldown_secs: u64,
    ) {
        if cooldown_secs == 0 {
            info!(
                "Bug BF-7: auto-unquarantine disabled \
                 (manager.scrub.auto_unquarantine_secs=0; manual only)"
            );
            return;
        }
        info!(
            tick_secs,
            cooldown_secs,
            "Bug BF-7: auto-unquarantine sweep starting"
        );
        let mut tick = tokio::time::interval(Duration::from_secs(tick_secs.max(30)));
        tick.tick().await; // skip immediate
        loop {
            tick.tick().await;
            // REV-1-1 — leader-only: the sweep commits UpdateNodeStatus.
            if !self.is_leader() {
                debug!("Bug BF-7: auto-unquarantine tick skipped: not leader");
                continue;
            }
            match self.auto_unquarantine_tick(cooldown_secs).await {
                Ok(n) if n > 0 => info!(
                    cleared = n,
                    "Bug BF-7: auto-unquarantine cleared nodes after cooldown"
                ),
                Ok(_) => debug!("Bug BF-7: auto-unquarantine tick clean"),
                Err(e) => warn!(error = %e, "Bug BF-7: auto-unquarantine tick failed"),
            }
        }
    }

    /// One sweep. Returns the number of nodes auto-unquarantined.
    pub async fn auto_unquarantine_tick(&self, cooldown_secs: u64) -> anyhow::Result<usize> {
        use dtfs_common::types::HealthStatus;
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let cooldown_ms = cooldown_secs.saturating_mul(1000);

        // Collect eligible (node_id, since, last_fault) snapshots.
        let eligible: Vec<u64> = {
            let sm = self.state_data.read().await;
            sm.nodes
                .iter()
                .filter_map(|(id, n)| {
                    if !matches!(n.health, HealthStatus::Quarantined) {
                        return None;
                    }
                    let q = n.quarantine.as_ref()?;
                    let age = now_ms.saturating_sub(q.since_ms);
                    let since_last_fault = now_ms.saturating_sub(q.last_fault_at_ms);
                    if age > cooldown_ms && since_last_fault > cooldown_ms {
                        Some(*id)
                    } else {
                        None
                    }
                })
                .collect()
        };

        let mut cleared = 0usize;
        for node_id in eligible {
            let req = ClientRequest::UnquarantineNode { now_ms: crate::metadata::stamp_now_ms(),
                node_id,
                operator_id: "auto-cooldown".to_string(),
                reason_acknowledged: format!(
                    "auto-cooldown after {}s clean (Bug BF-7)",
                    cooldown_secs
                ),
            };
            match self.raft.client_write(req).await {
                Ok(_) => {
                    cleared += 1;
                    self.audit.log(
                        "scheduler",
                        "node.unquarantine.auto",
                        &format!("node={node_id} cooldown_secs={cooldown_secs}"),
                        "success",
                    );
                }
                Err(e) => {
                    warn!(
                        node_id,
                        error = %e,
                        "Bug BF-7: UnquarantineNode raft write failed"
                    );
                }
            }
        }
        Ok(cleared)
    }

    // ── Bug BC-7 — orphan_scrub ─────────────────────────────────────────

    /// Run the orphan_scrub loop forever. Each tick:
    /// 1. Walk every data node, call `ListChunks`, get the on-disk
    ///    `chunk_id -> .dat mtime` map.
    /// 2. Diff against the union of every file's chunk hashes
    ///    (`chunk_delocated_on_node`).
    /// 3. Reap each orphan whose durable mtime is at least `grace_secs`
    ///    old (`orphan_reap_due`): call `DeleteChunkReplica` on the node +
    ///    audit `orphan.reaped`. Bug CB — the grace is measured against
    ///    the chunk's on-disk mtime, NOT a manager-local first-observed
    ///    clock, so it survives restarts / leadership changes and a
    ///    deleted file's chunks reclaim even under rolling-restart churn.
    ///    An unknown mtime (pre-Bug-CB data node) defers the reap.
    pub async fn run_orphan_scrub(&self, interval_secs: u64, grace_secs: u64) {
        info!(
            interval_secs,
            grace_secs,
            "Bug BC-7: orphan_scrub loop starting"
        );
        // Bug CB — short warmup (NOT a full interval) so a freshly
        // (re)started manager lets its raft log replay settle before the
        // first scan, then create the ticker so its first (immediate) tick
        // fires right after the warmup. The pre-CB code skipped a whole
        // `interval_secs` after every restart, which — combined with the
        // old in-memory grace clock — starved reaping under rolling-restart
        // churn. In-flight puts/promotes are protected by the mtime grace
        // (orphan_reap_due), not by deferring the scan.
        tokio::time::sleep(Duration::from_secs(30)).await;
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs.max(60)));
        loop {
            tick.tick().await;
            // REV-1-1 — followers must not reap. The reap is a direct
            // DeleteChunkReplica RPC to the data node; a follower's view of
            // `sm.files` can be arbitrarily stale and every missing file
            // reads as "all its chunks are orphans".
            if !self.is_leader() {
                debug!("Bug BC-7: orphan_scrub tick skipped: not leader");
                continue;
            }
            let Some(guard) = self.try_begin_orphan_run() else {
                info!("orphan_scrub tick skipped: a manual orphan reap is running");
                continue;
            };
            self.run_orphan_reap(guard, "timer", grace_secs, false).await;
        }
    }

    /// Single-flight token for an orphan reap; `None` while one runs.
    pub fn try_begin_orphan_run(&self) -> Option<tokio::sync::OwnedMutexGuard<()>> {
        self.orphan_run.clone().try_lock_owned().ok()
    }

    pub fn orphan_run_active(&self) -> bool {
        self.orphan_run.try_lock().is_err()
    }

    /// Last finished (or the running) orphan reap.
    pub fn last_orphan_run(&self) -> Option<OrphanRunRecord> {
        self.orphan_last_run.lock().clone()
    }

    /// One orphan reap (timer or manual) holding the single-flight `_guard`.
    /// Records the run for `GET /api/v1/scrub/orphans` and logs the outcome
    /// once; a skipped or aborted sweep lands in `error`, never as "0 reaped".
    pub async fn run_orphan_reap(
        &self,
        _guard: tokio::sync::OwnedMutexGuard<()>,
        trigger: &'static str,
        grace_secs: u64,
        dry_run: bool,
    ) -> OrphanRunRecord {
        let mut rec = OrphanRunRecord {
            trigger,
            grace_secs,
            dry_run,
            started_ms: now_millis(),
            ..Default::default()
        };
        *self.orphan_last_run.lock() = Some(rec.clone());
        if let Err(e) = self.orphan_scrub_tick(&mut rec).await {
            rec.error = Some(e.to_string());
        }
        rec.finished_ms = Some(now_millis());
        *self.orphan_last_run.lock() = Some(rec.clone());
        if let Some(e) = &rec.error {
            warn!(trigger, grace_secs, dry_run, reaped = rec.reaped, error = %e, "orphan reap did not complete");
        } else if rec.candidates > 0 || trigger == "manual" {
            info!(trigger, grace_secs, dry_run, candidates = rec.candidates, reaped = rec.reaped, "orphan reap finished");
        } else {
            debug!(trigger, "Bug BC-7: orphan_scrub tick clean");
        }
        rec
    }

    /// One sweep of orphan_scrub with `rec.grace_secs`; with `rec.dry_run`
    /// it only counts the reap candidates (no delete, no reconcile, no
    /// quarantine). Counters accumulate into `rec`.
    pub(crate) async fn orphan_scrub_tick(&self, rec: &mut OrphanRunRecord) -> anyhow::Result<()> {
        let grace_secs = rec.grace_secs;
        let dry_run = rec.dry_run;
        // REV-1-2 — freshness gate, waited for instead of skipped: under
        // steady writes the leader is often a few entries behind its own log
        // at any instant. Wait (bounded) until it has applied everything that
        // was in its log when the sweep started, BEFORE taking the snapshot.
        let caught_up_to = wait_for_sweep_catch_up(
            || {
                let (applied, last_log) = self.applied_and_log_index();
                (self.is_leader(), applied, last_log)
            },
            Duration::from_millis(250),
            120,
        )
        .await
        .map_err(|r| anyhow::anyhow!("sweep skipped: {r}"))?;

        let now_ms = now_millis();

        // 1. Data nodes, quarantined nodes, and the number of chunk rows.
        // Only "is the namespace view empty" is asked of the file table here
        // (a count, not a set of every hash: ~12M chunks would cost GBs).
        // Per-node expected sets are built just before each node's ListChunks.
        let (data_nodes, in_state_rows, already_quarantined) = {
            let sm = self.state_data.read().await;
            let nodes: Vec<(u64, String)> = sm
                .nodes
                .iter()
                .filter(|(_, n)| matches!(n.role, dtfs_common::types::NodeRole::Data))
                .map(|(id, n)| (*id, n.grpc_addr.clone()))
                .collect();
            let rows: usize = sm.files.values().map(|f| f.chunks.len()).sum();
            let quarantined: std::collections::HashSet<u64> = sm
                .nodes
                .iter()
                .filter(|(_, n)| matches!(
                    n.health,
                    dtfs_common::types::HealthStatus::Quarantined
                ))
                .map(|(id, _)| *id)
                .collect();
            (nodes, rows, quarantined)
        };

        // REV-1-2 — the reap deletes anything on disk that this manager
        // cannot see in `sm.files`, so a state machine that is still
        // replaying would classify every file created since as an orphan. An
        // empty in-state chunk set alongside live data nodes is never a
        // legitimate "everything is an orphan" either.
        let (applied, _) = self.applied_and_log_index();
        if let Some(reason) =
            orphan_sweep_skip_reason(self.is_leader(), applied, caught_up_to, in_state_rows)
        {
            anyhow::bail!("sweep skipped: {reason}");
        }

        let mut reaped: usize = 0;
        let mut aborted = false;

        'nodes: for (node_id, grpc_addr) in &data_nodes {
            // CR2-7-3 / MGR-13 — re-check leadership at the top of EVERY node
            // iteration. A sweep over a 50 TB anchor node can span an
            // election; without this a leader that just lost leadership kept
            // issuing direct data-node delete RPCs for the rest of the sweep,
            // which no Raft rejection can undo. Only leadership is re-checked
            // here: the log-caught-up gate ran once at sweep start (the
            // leader's own in-flight writes keep last_applied < last_log).
            if !orphan_delete_allowed(self.is_leader(), in_state_rows) {
                self.audit.log(
                    "scheduler",
                    "orphan.sweep_aborted_not_leader",
                    &format!("aborted before node={node_id}; reaped_so_far={reaped}"),
                    "aborted",
                );
                aborted = true;
                break 'nodes;
            }
            if grpc_addr.is_empty() { continue; }
            let node_stats = rec.nodes.entry(node_id.to_string()).or_default();
            // Bug BF-8 — what the SM expects on this node BEFORE its listing
            // (a row registered during the listing must not read as missing).
            // Skipped if the node is already Quarantined (apply path
            // accumulates fault_count_total; no need to bump on every tick)
            // and on a dry run, which proposes nothing.
            let check_missing = !dry_run && !already_quarantined.contains(node_id);
            let expected = if check_missing {
                let sm = self.state_data.read().await;
                Some(NodeReapView::from_files(&sm.files, *node_id).node_expected)
            } else {
                None
            };
            let on_disk = match self.list_chunks_on_node(grpc_addr).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(node = %node_id, error = %e, "Bug BC-7: ListChunks failed");
                    node_stats.error = Some(format!("ListChunks failed: {e}"));
                    continue;
                }
            };
            node_stats.listed = on_disk.len();
            // Keep only the few expected-but-absent hashes; the full
            // pre-listing set is dropped before the view below is built.
            let missing: Vec<String> = expected
                .map(|e| e.into_iter().filter(|c| !on_disk.contains_key(c)).collect())
                .unwrap_or_default();
            // The expected set was taken before this node's
            // ListChunks (which can take minutes on a big node). Rebuild
            // this node's view from the CURRENT state machine before any
            // reap decision, so a replica registered or removed meanwhile is
            // judged on fresh state.
            let view = {
                let sm = self.state_data.read().await;
                NodeReapView::from_files(&sm.files, *node_id)
            };
            // Bug BF-8 — in-state-missing inverse. Before processing
            // the on-disk-not-in-state loop, check what the state
            // machine THINKS this node holds; quarantine the node if
            // any expected chunk is absent from its ListChunks reply.
            if check_missing {
                {
                    let missing: Vec<&String> = missing.iter().collect();
                    // CO-3-3 — the `expected` set was taken before this
                    // node's ListChunks, whose reply arrives later. A demote /
                    // unlink / rebalance that committed `RemoveChunkLocation`
                    // in between leaves it still naming the node for a chunk
                    // it no longer holds (legitimately) — quarantining a
                    // healthy node. Re-check against the CURRENT SM (`view`,
                    // read after the listing) and keep only chunks the node is
                    // STILL expected to hold; if none remain, the "missing"
                    // set was a demote race, not a fault — skip the quarantine
                    // (but still fall through to the on-disk reap below).
                    let missing: Vec<&String> = if missing.is_empty() {
                        missing
                    } else {
                        let filtered = still_missing_after_recheck(&missing, &view.node_expected);
                        if filtered.is_empty() {
                            debug!(
                                node = %node_id,
                                "CO-3-3: in-state-missing cleared on SM re-check \
                                 (demote/unlink race) — node not quarantined"
                            );
                        }
                        filtered
                    };
                    if !missing.is_empty() {
                        let first = missing[0].clone();

                        // REV-3-10 — RECONCILE before blaming. The state
                        // machine claims this node holds bytes it does not,
                        // and while that stale row stands `live_replica_tier`
                        // still counts it as coverage, so
                        // `check_under_replicated` sees no deficit and never
                        // backfills: the chunk is genuinely down one replica
                        // and nothing heals it. Quarantining the node does not
                        // fix that — it just stops using an otherwise healthy
                        // node forever.
                        //
                        // Drop the stale row (never the last live one, always
                        // anchor-checked on apply) so the backfill can see the
                        // deficit and re-copy from a replica that really
                        // exists. Rows are re-derived from the CURRENT state
                        // inside the helper: the `expected` snapshot predates
                        // this node's ListChunks.
                        //
                        // Only for a SMALL miss set, though: a node-scale miss
                        // quarantines regardless of what reconciliation finds
                        // (`should_quarantine_for_missing` is already true on
                        // volume alone), and issuing one Raft write per chunk
                        // of a wiped 50 TB disk is exactly the flood REV-3-4
                        // exists to prevent.
                        let unreconcilable = if should_quarantine_for_missing(missing.len(), 0) {
                            0
                        } else {
                            self.reconcile_missing_replicas(*node_id, &missing).await
                        };

                        if should_quarantine_for_missing(missing.len(), unreconcilable) {
                            self.quarantine_for_in_state_missing(
                                *node_id,
                                &first,
                                missing.len(),
                                now_ms,
                            )
                            .await;
                            // Continue to reap on-disk-not-in-state below
                            // — even a Quarantined node can have orphans
                            // that the reaper should still clear.
                        } else {
                            info!(
                                node = %node_id,
                                missing = missing.len(),
                                "REV-3-10: reconciled stale location rows; backfill will \
                                 re-replicate. Node not quarantined."
                            );
                            self.audit.log(
                                "scheduler",
                                "orphan.in_state_missing_reconciled",
                                &format!("node={node_id} rows={} first={first}", missing.len()),
                                "reconciled",
                            );
                        }
                    }
                }
            }

            let mut due: Vec<(&String, &u64, &Option<u32>)> = Vec::new();
            for (cid, (mtime_ms, disk_id_opt)) in &on_disk {
                // CO-3-2 / CR2-7-2 (MGR-10) — a chunk on this node's disk
                // for which the SM records NO location entry naming this
                // node, on ANY of its disks, is a reap candidate: either a
                // phantom replica (the rebalance / excess-spill path drops
                // the SM entry via `RemoveChunkLocation` but never deletes
                // the de-located bytes) or a Bug BP-4 orphan (no file
                // references the hash, so no row names any node). A chunk a
                // file still points at on this node stays in the expected
                // set, and the mtime grace protects an in-flight put whose
                // `commit_chunk_locations` Raft write has not yet applied.
                // A copy recorded under ANOTHER disk id of the same node is
                // never deleted (co-located leftovers may leak instead); the
                // delete carries the reported disk_id so only that disk's
                // copy is touched.
                //
                // Bug CB — measure the reap grace against the chunk's
                // DURABLE on-disk mtime, not a manager-local "first
                // observed orphan" clock. The old in-memory tracker reset
                // on every manager restart / leadership change, so under
                // rolling-restart churn a manager never accumulated the
                // 24h grace and orphaned chunks were never reaped (disk
                // usage of deleted files never dropped). The mtime survives
                // restarts; a freshly-written chunk (recent mtime) is still
                // protected against the in-flight put/promote race.
                let candidate = orphan_reap_candidate(&view, cid);
                if !orphan_reap_due(candidate, *mtime_ms, now_ms, grace_secs) {
                    // Surface a known reap candidate we are deferring only
                    // because its age is unknown (mtime 0 = pre-Bug-CB data
                    // node), so a permanently-stalled reap is diagnosable.
                    if candidate && *mtime_ms == 0 {
                        debug!(
                            node = %node_id, chunk = %cid,
                            "Bug CB: orphan deferred — data node reported no \
                             mtime (upgrade the data node to enable reaping)"
                        );
                    }
                    continue;
                }
                node_stats.candidates += 1;
                if !dry_run {
                    due.push((cid, mtime_ms, disk_id_opt));
                }
            }

            // Audit label only ("orphan" = no file references the hash):
            // one pass that looks up just the due hashes.
            let referenced = if due.is_empty() {
                Default::default()
            } else {
                let sm = self.state_data.read().await;
                referenced_hashes(&sm.files, due.iter().map(|d| d.0.as_str()))
            };
            for (cid, mtime_ms, disk_id_opt) in due {
                let cause = if referenced.contains(cid.as_str()) { "delocated" } else { "orphan" };
                let age_secs = now_ms.saturating_sub(*mtime_ms) / 1000;

                // CR2-7-3 / MGR-13 — re-check leadership immediately before
                // EACH delete (an election can land between two candidates
                // of one node's listing). Leadership only; see node top.
                if !orphan_delete_allowed(self.is_leader(), view.rows) {
                    self.audit.log(
                        "scheduler",
                        "orphan.sweep_aborted_not_leader",
                        &format!(
                            "aborted before delete node={node_id} chunk={cid}; reaped_so_far={reaped}"
                        ),
                        "aborted",
                    );
                    aborted = true;
                    break 'nodes;
                }

                // Past grace — reap.
                // CO-1-1 / CO-3-2 / CR2-7-2 (MGR-10) — pass the exact
                // disk_id when the data node reported one, so a de-located
                // leftover on ONE disk of a node is reaped on that disk
                // without touching a still-live replica of the same chunk
                // on another disk of the same node. A pre-CR2-7-2 data
                // node reports no disk_id -> `None` -> legacy node-wide
                // first-found delete, unchanged.
                match self
                    .delete_chunk_replica_on_node_if_older(
                        grpc_addr, cid, *disk_id_opt, "", Some(*mtime_ms),
                    )
                    .await
                {
                    Ok((was_present, freed)) => {
                        let result_msg = if was_present {
                            format!("freed_bytes={freed}")
                        } else {
                            "noop_already_absent".to_string()
                        };
                        self.audit.log(
                            "scheduler",
                            "orphan.reaped",
                            &format!("node={node_id} chunk={cid} cause={cause} age_secs={age_secs}"),
                            &result_msg,
                        );
                        reaped += 1;
                        node_stats.reaped += 1;
                        *node_stats.bytes_freed.get_or_insert(0) += freed;
                    }
                    Err(e) => {
                        warn!(
                            node = %node_id, chunk = %cid, error = %e,
                            "Bug BC-7: orphan reap delete RPC failed"
                        );
                        node_stats.delete_failed += 1;
                    }
                }
            }
        }

        rec.candidates = rec.nodes.values().map(|n| n.candidates).sum();
        rec.reaped = reaped;
        if !dry_run {
            rec.bytes_freed = Some(rec.nodes.values().filter_map(|n| n.bytes_freed).sum());
        }
        if aborted {
            anyhow::bail!("aborted: lost leadership after reaping {reaped}");
        }
        Ok(())
    }

    /// Bug BF-8 / REV-3-10 — quarantine `node_id` for an unreconcilable
    /// in-state-missing finding. Extracted from `orphan_scrub_tick` so the
    /// `QuarantineInfo` struct literal lives at method top level (the deeply
    /// nested form tripped a rustc 1.94 typeck ICE).
    pub(super) async fn quarantine_for_in_state_missing(
        &self,
        node_id: u64,
        first: &str,
        missing: usize,
        now_ms: u64,
    ) {
        let summary =
            format!("orphan_scrub: {missing} chunk(s) in state but not on disk; first={first}");
        let info = dtfs_common::types::QuarantineInfo {
            since_ms: now_ms,
            reason: dtfs_common::types::QuarantineReason::OrphanScrubInStateMissing,
            triggering_chunk_id: Some(first.to_string()),
            fault_count_total: 0,
            last_fault_at_ms: now_ms,
            last_fault_summary: summary,
        };
        let req = ClientRequest::QuarantineNode { now_ms: crate::metadata::stamp_now_ms(), node_id, info };
        if let Err(e) = self.raft.client_write(req).await {
            warn!(
                node = %node_id, error = %e,
                "Bug BF-8: QuarantineNode raft write failed"
            );
            return;
        }
        self.audit.log(
            "scheduler",
            "orphan.in_state_missing",
            &format!("node={node_id} missing={missing} first={first}"),
            "quarantined",
        );
    }

    /// Bug BC-6 — invoke `ListChunks` on a single data node, drain all
    /// pages, return the combined set of chunk_ids.
    pub(super) async fn list_chunks_on_node(
        &self,
        node_addr: &str,
    ) -> anyhow::Result<std::collections::HashMap<String, (u64, Option<u32>)>> {
        use dtfs_proto::dtfs::data::ListChunksRequest;
        let endpoint = normalize_endpoint(node_addr);
        let mut client = self.tls.connect(&endpoint).await
            .map(DataServiceClient::new)
            .map_err(|e| anyhow::anyhow!("connect to data node {}: {}", node_addr, e))?;
        // Bug CB — value.0 is the chunk's `.dat` mtime (Unix ms); a
        // pre-Bug-CB data node sends an empty `chunk_mtimes_ms`, so a
        // missing entry defaults to 0 ("age unknown") and the reaper
        // defers rather than dropping a possibly in-flight chunk.
        //
        // CR2-7-2 / MGR-10 — value.1 is the disk_id the chunk was found
        // on, index-aligned like chunk_mtimes_ms. A pre-CR2-7-2 data node
        // sends an EMPTY `chunk_disk_ids` for the whole page; that page's
        // chunks get `None` here, so the caller falls back to today's
        // node-wide reap semantics exactly.
        let mut out: std::collections::HashMap<String, (u64, Option<u32>)> =
            std::collections::HashMap::new();
        let mut token = String::new();
        loop {
            let mut req = tonic::Request::new(ListChunksRequest {
                page_token: token.clone(),
                limit: 1000,
            });
            {
                use dtfs_common::crypto::{
                    stamp_data_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
                };
                let secret = self.shared_secret.read().clone();
                if let Some((ts, mac)) = stamp_data_rpc_auth(&secret, "ListChunks", "") {
                    if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
                        req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
                        req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
                    }
                }
            }
            let resp = client
                .list_chunks(req)
                .await
                .map_err(|e| anyhow::anyhow!("ListChunks RPC: {}", e))?
                .into_inner();
            let has_disk_ids = !resp.chunk_disk_ids.is_empty();
            for (i, cid) in resp.chunk_ids.into_iter().enumerate() {
                let mtime = resp.chunk_mtimes_ms.get(i).copied().unwrap_or(0);
                let disk_id = if has_disk_ids {
                    resp.chunk_disk_ids.get(i).copied()
                } else {
                    None
                };
                out.insert(cid, (mtime, disk_id));
            }
            if resp.next_page_token.is_empty() {
                break;
            }
            token = resp.next_page_token;
        }
        Ok(out)
    }

    /// Read-only `ListOldestChunks` on one data disk (capacity eviction,
    /// retention demote), oldest first; `older_than_ms` 0 = no age filter. A
    /// connect failure maps to `Unavailable`.
    pub(crate) async fn list_oldest_chunks_on_node(
        &self,
        node_addr: &str,
        disk_id: u32,
        limit: u32,
        older_than_ms: u64,
        after: Option<(u64, String)>,
    ) -> Result<Vec<dtfs_proto::dtfs::data::OldestChunk>, tonic::Status> {
        use dtfs_common::crypto::{stamp_data_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS};
        use dtfs_proto::dtfs::data::ListOldestChunksRequest;
        let endpoint = normalize_endpoint(node_addr);
        let mut client = self
            .tls
            .connect(&endpoint)
            .await
            .map(DataServiceClient::new)
            .map_err(|e| tonic::Status::unavailable(format!("connect to data node {node_addr}: {e}")))?;
        let mut req = tonic::Request::new(ListOldestChunksRequest {
            disk_id,
            limit,
            older_than_ms,
            after_last_read_ms: after.as_ref().map_or(0, |a| a.0),
            after_chunk_id: after.map(|a| a.1).unwrap_or_default(),
        });
        let secret = self.shared_secret.read().clone();
        if let Some((ts, mac)) = stamp_data_rpc_auth(&secret, "ListOldestChunks", "") {
            if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
                req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
                req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
            }
        }
        Ok(client.list_oldest_chunks(req).await?.into_inner().chunks)
    }
}

/// Get current time in milliseconds since UNIX epoch.
pub(super) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Bug W (part 2) — return a tonic-ready endpoint URL given a stored
/// `NodeInfo.grpc_addr`. New deployments store a fully-formed URL
/// (`http://10.20.0.157:9500`); pre-Bug-W deployments may have stored a
/// bare `host:port`. This helper accepts both and never produces the
/// `http://http://...` form that broke every migration.
#[inline]
pub(crate) fn normalize_endpoint(stored_addr: &str) -> String {
    if stored_addr.starts_with("http://") || stored_addr.starts_with("https://") {
        stored_addr.to_string()
    } else {
        format!("http://{}", stored_addr)
    }
}

/// CO-1-3 — the two ordered side effects of de-locating one replica, behind
/// a trait so the ordering can be unit-tested with a recording mock (mirrors
/// the `MigrationOps` seam). The live impls are the `TaskScheduler`
/// (rebalance/excess-spill + cache-flush dispatch) and the tiering engine's
/// demote dispatch.
pub(crate) trait DelocateOps {
    /// Commit the `RemoveChunkLocation` Raft write that drops this replica's
    /// SM entry (with the volume `enforce_anchor`). MUST happen before any
    /// byte delete.
    async fn remove_location(&self) -> anyhow::Result<()>;
    /// Issue the data-plane `DeleteChunkReplica` byte-delete RPC. Only ever
    /// called AFTER `remove_location` succeeded. `if_mtime_le_ms`: the data
    /// node skips the delete when the `.dat` is newer (re-upload race).
    async fn delete_bytes(&self, if_mtime_le_ms: Option<u64>) -> anyhow::Result<()>;
    /// Chunks are content-addressed: after the remove, does any file still
    /// hold a row for this hash on the same (node, disk)? Then the bytes are
    /// shared and must stay.
    async fn bytes_still_referenced(&self) -> bool {
        false
    }
}

/// Does any file still record `chunk_hash` on `(node_id, disk_id)`? The
/// legacy node-wide sentinel disk id matches any disk of the node.
pub(crate) fn chunk_bytes_referenced_on_disk(
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
    chunk_hash: &str,
    node_id: &str,
    disk_id: u32,
) -> bool {
    let any_disk = disk_id == dtfs_raft::legacy_disk_id();
    files.values().flat_map(|fm| fm.chunks.iter()).any(|c| {
        c.chunk_hash == chunk_hash && c.node_id == node_id && (any_disk || c.disk_id == disk_id)
    })
}

/// CO-1-3 — the outcome of `delocate_replica`, distinguishing the failure
/// windows so callers can keep their report counters honest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DelocateOutcome {
    /// Raft remove committed AND the byte delete succeeded.
    Done,
    /// Raft remove committed but the byte delete RPC failed. The SM entry is
    /// gone (replica de-located); the leftover on-disk bytes are now a chunk
    /// "on a node's disk with no location entry for that node", which the
    /// CO-3-2 orphan reaper reclaims after the durable-mtime grace. The Raft
    /// remove is NEVER rolled back.
    RemovedBytesLeftover,
    /// Raft remove failed; NO byte delete was issued, so the replica is still
    /// live in the SM + on disk. Retry next tick.
    RemoveFailed,
}

/// CO-1-3 — de-locate one replica with the LOCKED-IN ordering: Raft
/// `RemoveChunkLocation` (with `enforce_anchor`) FIRST, then the data-plane
/// byte delete. A byte-delete failure after a committed remove is tolerated
/// (the reaper mops the bytes); a Raft-remove failure short-circuits before
/// any byte is touched. This makes the de-location atomic at the metadata
/// layer and crash-safe: no phantom replica is ever advertised as live while
/// its bytes are gone, and no live replica is ever de-advertised while its
/// bytes remain (the reverse-order hazard).
pub(crate) async fn delocate_replica<O: DelocateOps>(ops: &O) -> DelocateOutcome {
    if ops.remove_location().await.is_err() {
        // Raft remove failed -> issue NO byte delete. The replica is
        // untouched (still live in the SM + on disk).
        return DelocateOutcome::RemoveFailed;
    }
    // Taken BEFORE the re-read: a same-hash put committed after it renames a
    // newer .dat, which the data node then refuses to delete.
    let guard_ms = now_millis();
    // Another file (or chunk index) still points at these bytes: keep them;
    // the orphan reaper reclaims them once nothing references the hash.
    if ops.bytes_still_referenced().await {
        return DelocateOutcome::Done;
    }
    // SM entry is gone. A byte-delete failure now only leaves de-located
    // bytes for the CO-3-2 reaper; never roll back the committed remove.
    // A guard-skipped delete returns Ok: Done, the bytes stay for the reaper.
    if ops.delete_bytes(Some(guard_ms)).await.is_err() {
        return DelocateOutcome::RemovedBytesLeftover;
    }
    DelocateOutcome::Done
}

/// CO-1-3 — live `DelocateOps` impl shared by the scheduler's
/// rebalance/excess-spill + cache-flush dispatch and the tiering demote
/// dispatch. Captures the one `RemoveChunkLocation` Raft request and the
/// target (node_addr, chunk_id, disk_id, tier) for the byte delete.
pub(crate) struct SchedulerDelocate<'a> {
    pub sched: &'a TaskScheduler,
    pub remove_req: ClientRequest,
    pub node_addr: String,
    pub chunk_id: String,
    pub disk_id: u32,
    pub tier: String,
}

impl DelocateOps for SchedulerDelocate<'_> {
    async fn remove_location(&self) -> anyhow::Result<()> {
        // Checked: the apply refuses a removal that would strand the anchor
        // or the last live replica with Ok(ClientResponse::Error), which must
        // keep the byte delete from running.
        self.sched
            .raft_write_checked(self.remove_req.clone())
            .await
            .map_err(|e| anyhow::anyhow!("RemoveChunkLocation Raft write: {}", e))
    }

    async fn bytes_still_referenced(&self) -> bool {
        let ClientRequest::RemoveChunkLocation { node_id, .. } = &self.remove_req else {
            return false;
        };
        let sm = self.sched.state_data.read().await;
        let shared = chunk_bytes_referenced_on_disk(&sm.files, &self.chunk_id, node_id, self.disk_id);
        if shared {
            info!(
                chunk_id = %self.chunk_id,
                node_id = %node_id,
                disk_id = self.disk_id,
                "row removed, bytes kept: another file still references this chunk on the disk"
            );
        }
        shared
    }

    async fn delete_bytes(&self, if_mtime_le_ms: Option<u64>) -> anyhow::Result<()> {
        self.sched
            .delete_chunk_replica_on_node_if_older(
                &self.node_addr,
                &self.chunk_id,
                Some(self.disk_id),
                &self.tier,
                if_mtime_le_ms,
            )
            .await
            .map(|_| ())
    }
}

/// CO-3-2 — is an on-disk chunk a reap candidate for THIS node because the
/// state machine no longer records any location entry naming the node for
/// that chunk? This is the "de-located bytes" condition: the rebalance /
/// excess-spill path (`check_misplaced_rebalance`) commits a
/// `RemoveChunkLocation` to drop a replica from the SM but never deletes the
/// on-disk `.dat` on the de-located node, so the bytes linger forever as a
/// phantom replica. After the durable-mtime grace (`orphan_reap_due`), the
/// orphan_scrub reaps them here.
///
/// `node_expected` is the set of chunk hashes the SM believes this node
/// holds (the union across all of the node's disks; built from every file's
/// chunk locations). A chunk on the node's disk that is NOT in that set has
/// no live location entry naming the node, so the bytes are safe to reap
/// (node-wide — a de-located node should hold no copy at all).
///
/// Bug BP-4 — a chunk referenced by NO file is named by no node either, so
/// it is absent from every `node_expected` and is reapable; a chunk a file
/// still points at is never reapable (CLAUDE.md §A.6 #8). The
/// caller still gates the actual delete behind the mtime grace, so an
/// in-flight put/promote whose `commit_chunk_locations` Raft write has not
/// yet applied (recent mtime) is never dropped.
pub(crate) fn chunk_delocated_on_node(
    node_expected: Option<&std::collections::HashSet<String>>,
    chunk_hash: &str,
) -> bool {
    match node_expected {
        Some(expected) => !expected.contains(chunk_hash),
        // The SM records no chunks at all for this node -> nothing here is
        // expected -> every on-disk chunk is de-located.
        None => true,
    }
}

/// Reap decision view of ONE node, built from the current file table right
/// after that node's ListChunks completes. Holds only this node's hashes:
/// a set of every hash in the namespace is not needed (a hash no file
/// references has no row naming any node, so it is de-located everywhere),
/// nor is a per-disk split (the per-disk verdict was always gated on the
/// node-wide one, which implies it).
pub(crate) struct NodeReapView {
    /// Hashes the SM records on this node, any disk.
    pub node_expected: std::collections::HashSet<String>,
    /// Chunk rows in the whole file table (0 = empty namespace view).
    pub rows: usize,
}

impl NodeReapView {
    pub(crate) fn from_files(
        files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
        node_id: u64,
    ) -> Self {
        let mut v = Self { node_expected: Default::default(), rows: 0 };
        for f in files.values() {
            v.rows += f.chunks.len();
            for c in &f.chunks {
                if c.node_id.parse::<u64>().ok() == Some(node_id) {
                    v.node_expected.insert(c.chunk_hash.clone());
                }
            }
        }
        v
    }

    /// Is the on-disk copy `cid` de-located, i.e. does no row name this
    /// node for it on ANY disk? A row naming another disk of the same node
    /// still protects the chunk.
    pub(crate) fn delocated(&self, cid: &str) -> bool {
        chunk_delocated_on_node(Some(&self.node_expected), cid)
    }
}

/// The subset of `wanted` that some file references (one pass, no set of
/// the whole namespace).
pub(crate) fn referenced_hashes<'a>(
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
    wanted: impl IntoIterator<Item = &'a str>,
) -> std::collections::HashSet<&'a str> {
    let wanted: std::collections::HashSet<&'a str> = wanted.into_iter().collect();
    files
        .values()
        .flat_map(|f| f.chunks.iter())
        .filter_map(|c| wanted.get(c.chunk_hash.as_str()).copied())
        .collect()
}

/// Per-delete gate of the orphan sweep: leadership plus a non-empty file
/// view. The "log caught up" check is NOT here: it runs once at sweep start
/// (`orphan_sweep_skip_reason`), since the leader's own in-flight writes keep
/// `last_applied < last_log_index` and would abort every long sweep.
pub(crate) fn orphan_delete_allowed(is_leader: bool, in_state_chunks: usize) -> bool {
    is_leader && in_state_chunks > 0
}

/// CO-3-3 — given a set of chunks the pre-listing snapshot expected on a
/// node but the node's ListChunks reply omitted (`snapshot_missing`), and
/// the set the CURRENT state machine still expects on that node
/// (`current_expected`), return only the chunks that are still genuinely
/// missing (in both sets). A chunk that dropped out of `current_expected`
/// between the snapshot and the re-check was de-located by a concurrent
/// demote / unlink / rebalance — not a fault — so it must NOT trigger a
/// quarantine. When the result is empty the caller skips the quarantine.
pub(crate) fn still_missing_after_recheck<'a>(
    snapshot_missing: &[&'a String],
    current_expected: &std::collections::HashSet<String>,
) -> Vec<&'a String> {
    snapshot_missing
        .iter()
        .copied()
        .filter(|c| current_expected.contains(*c))
        .collect()
}

/// Bug CB — is an on-disk chunk due to be reaped this tick? A chunk is
/// reaped only when it is unreferenced (`is_orphan`) AND its durable
/// on-disk `.dat` mtime is at least `grace_secs` old. Anchoring the grace
/// to the chunk's mtime (reported by ListChunks) instead of a
/// manager-local "first observed orphan" timestamp makes the grace clock
/// survive manager restarts and leadership changes, so a deleted file's
/// chunks reclaim even under rolling-restart churn — the bug where disk
/// usage of deleted files never dropped.
///
/// `chunk_mtime_ms == 0` means "age unknown" — a pre-Bug-CB data node
/// that does not populate `chunk_mtimes_ms`, or a stat failure. In that
/// case we DEFER (return false) rather than reap with no grace, so the
/// in-flight put/promote race can never drop a just-written chunk during
/// a mixed-version rollout. Reaping resumes once the data node is
/// upgraded to report mtimes.
pub(crate) fn orphan_reap_due(
    is_orphan: bool,
    chunk_mtime_ms: u64,
    now_ms: u64,
    grace_secs: u64,
) -> bool {
    if !is_orphan || chunk_mtime_ms == 0 {
        return false;
    }
    let age_secs = now_ms.saturating_sub(chunk_mtime_ms) / 1000;
    age_secs >= grace_secs
}

/// Is the on-disk copy `cid` a reap candidate, age aside? Exactly when no
/// row names this node for it (CO-3-2). A Bug BP-4 orphan (no file
/// references the hash) always qualifies; the "orphan" vs "delocated" audit
/// label is decided afterwards for the due candidates (`referenced_hashes`).
pub(crate) fn orphan_reap_candidate(view: &NodeReapView, cid: &str) -> bool {
    view.delocated(cid)
}

/// Lowest grace a manual orphan reap accepts. Far above any put->commit
/// window (write lease 120 s, async_commit <= 60 s); the configured
/// `scrub.orphan_grace_secs` keeps its own 6 h floor.
pub(crate) const MANUAL_ORPHAN_GRACE_FLOOR_SECS: u64 = 3600;

/// Per-node counters of one orphan reap.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OrphanNodeStats {
    /// Chunks ListChunks reported.
    pub listed: usize,
    /// Reap candidates past grace (what a dry run would delete).
    pub candidates: usize,
    pub reaped: usize,
    pub delete_failed: usize,
    /// Freed bytes as reported by the data node; `None` on a dry run.
    pub bytes_freed: Option<u64>,
    pub error: Option<String>,
}

/// One orphan reap, as shown by `GET /api/v1/scrub/orphans`.
#[derive(Clone, Debug, Default, Serialize)]
pub struct OrphanRunRecord {
    /// `"manual"` or `"timer"`.
    pub trigger: &'static str,
    pub grace_secs: u64,
    pub dry_run: bool,
    pub started_ms: u64,
    /// `None` while the run is in progress.
    pub finished_ms: Option<u64>,
    pub candidates: usize,
    pub reaped: usize,
    /// `None` on a dry run (ListChunks does not report sizes).
    pub bytes_freed: Option<u64>,
    /// Keyed by node id (string, like every node id in the REST API).
    pub nodes: std::collections::BTreeMap<String, OrphanNodeStats>,
    /// Skipped / aborted sweep; never reported as a clean "0 reaped".
    pub error: Option<String>,
}

/// REV-1-2 freshness gate, waited for: probe `(is_leader, last_applied,
/// last_log_index)` until the state machine has applied everything that was
/// in the log at the FIRST probe ("caught up to when I started"), which is
/// reached even under steady writes. Up to `max_steps` sleeps of `step`.
/// Returns the caught-up log index, or the skip reason.
pub(crate) async fn wait_for_sweep_catch_up(
    mut probe: impl FnMut() -> (bool, u64, u64),
    step: Duration,
    max_steps: u32,
) -> Result<u64, &'static str> {
    let (_, _, target) = probe();
    for i in 0..=max_steps {
        let (is_leader, applied, _) = probe();
        if let Some(reason) = orphan_sweep_skip_reason(is_leader, applied, target, 1) {
            if reason == "not leader" || i == max_steps {
                return Err(reason);
            }
            tokio::time::sleep(step).await;
            continue;
        }
        return Ok(target);
    }
    Err("state machine not caught up")
}

/// Bug BO-1 — which node id (if any) `resolve_healthy_source` must exclude
/// when copying a chunk for an anchor-lag task. For a same-node cross-tier
/// copy (`source_node == target_node`) the target is the ONLY holder of the
/// chunk (on its other tier), so excluding it strands the copy with "no
/// healthy source replica". Returns `None` in that case; `Some(target)`
/// otherwise (unchanged behaviour for cross-node copies).
pub(crate) fn anchor_copy_source_exclude(source_node: u64, target_node: u64) -> Option<u64> {
    if source_node == target_node {
        None
    } else {
        Some(target_node)
    }
}

/// Bug BO-2 — should a task with this status keep a chunk out of the
/// anchor-lag re-queue set? Only non-Done tasks block a re-queue; a Failed
/// task lingers (Bug AE retention; `raft.failed_task_retention_secs`,
/// 30 min default) so the same chunk is NOT
/// immediately re-scheduled every 30s tick. (Previously Failed was excluded
/// too → tight re-queue loop + audit spam.)
pub(crate) fn task_blocks_requeue(status: MigrationStatus) -> bool {
    status != MigrationStatus::Done
}

// ---------------------------------------------------------------------------
// Bug AV (AV-3-3) — anchor-lag-repair pure-planner kernel
// ---------------------------------------------------------------------------

/// REV-1-1 — pure leader predicate. Every manager background loop must
/// consult this before dispatching work; only `tick()` used to.
///
/// Followers that keep running these loops issue **direct data-node RPCs**
/// (DeleteChunk / DeleteChunkReplica) that no Raft rejection can undo, so a
/// follower's stale or empty view physically deletes live bytes. Kept as a
/// free function over the two metrics fields so it is unit-testable without a
/// live raft instance.
pub(crate) fn leader_gate_open(current_leader: Option<u64>, self_id: u64) -> bool {
    current_leader == Some(self_id)
}

/// REV-1-2 — pure freshness predicate for the orphan reaper.
///
/// The reaper deletes any on-disk chunk that is absent from *its own* view of
/// `sm.files`. A manager that is elected while still replaying (or that just
/// loaded an old on-disk snapshot) has a view that is missing every file
/// created since, so every one of those chunks looks like an orphan. Require
/// the local state machine to be caught up, and refuse to sweep when the
/// in-state chunk set is empty while data nodes still report chunks — that
/// combination is never legitimately "everything is an orphan".
///
/// `None` = the sweep may run; `Some(reason)` = why it refuses.
pub(crate) fn orphan_sweep_skip_reason(
    is_leader: bool,
    last_applied: u64,
    last_log_index: u64,
    in_state_chunks: usize,
) -> Option<&'static str> {
    if !is_leader {
        Some("not leader")
    } else if last_applied < last_log_index {
        Some("state machine not caught up")
    } else if in_state_chunks == 0 {
        Some("empty namespace view")
    } else {
        None
    }
}

/// REV-3-5 — how many consecutive memory-pressure skips the scheduler
/// tolerates before it runs a tick anyway.
///
/// At the 30 s tick interval this is 10 minutes of paused repair, which is a
/// real ladder rung; beyond that the pause has stopped being backpressure and
/// has become an outage of the only component that maintains mandatory
/// replicas.
pub(crate) const MAX_CONSECUTIVE_PRESSURE_SKIPS: u32 = 20;

/// REV-3-5 — true when the scheduler must run despite memory pressure.
pub(crate) fn force_tick_after_skips(consecutive_skips: u32) -> bool {
    consecutive_skips >= MAX_CONSECUTIVE_PRESSURE_SKIPS
}

/// REV-3-4 — how many NEW replication tasks one scheduler tick may create.
///
/// `check_under_replicated` had no cap at all: losing the ~50 TB LELI anchor
/// node makes every one of its chunks short of its cold tier, so a single tick
/// inserted one task per chunk (hundreds of thousands), and
/// `save_migration_state` then serialised the whole map into ONE Raft entry —
/// a multi-megabyte proposal replicated to every voter and learner, over the
/// 40 Mbps archive link, every tick. The scan is idempotent, so capping simply
/// spreads the same work over subsequent ticks.
pub(crate) const MAX_NEW_TASKS_PER_TICK: usize = 200;

/// REV-3-4 — ceiling on live (non-terminal) tasks. Beyond this the scheduler
/// stops adding work until the backlog drains, so the persisted state stays a
/// bounded Raft payload no matter how much of the cluster is missing.
pub(crate) const MAX_LIVE_TASKS: usize = 2_000;

/// REV-3-4 — may the tick create another task?
pub(crate) fn task_budget_available(created_this_tick: usize, live_tasks: usize) -> bool {
    created_this_tick < MAX_NEW_TASKS_PER_TICK && live_tasks < MAX_LIVE_TASKS
}

/// REV-3-10 — how many unreconcilable missing replicas justify quarantining
/// the node.
///
/// A single missing chunk is almost always a race (the `expected` snapshot is
/// taken before this node's `ListChunks`, so anything legitimately deleted in
/// that window looks missing — the CO-3-3 re-check catches most of those) or a
/// stale row we just healed. Real node-level loss shows up as many at once.
pub(crate) const IN_STATE_MISSING_QUARANTINE_THRESHOLD: usize = 8;

/// REV-3-10 — quarantine only when the situation is not self-healing.
///
/// `unreconcilable` counts missing replicas whose location row could NOT be
/// dropped because it was the last live copy of that chunk — i.e. genuine data
/// loss that a human has to look at. Everything else is a stale row the
/// scheduler has already removed so the backfill can re-copy from a replica
/// that really exists; blaming the node for those left an otherwise healthy
/// node quarantined forever while the chunk stayed under-replicated.
pub(crate) fn should_quarantine_for_missing(missing: usize, unreconcilable: usize) -> bool {
    unreconcilable > 0 || missing >= IN_STATE_MISSING_QUARANTINE_THRESHOLD
}

/// REV-4-6 — every file that references `chunk_hash`, in path order.
///
/// Chunks are content-addressed, so identical content in two files (a
/// re-download, a copy, two episodes sharing a segment) is ONE chunk id
/// referenced from several `FileMetadata.chunks` lists.
#[cfg(test)]
pub(crate) fn files_referencing_chunk(
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
    chunk_hash: &str,
) -> Vec<String> {
    files
        .iter()
        .filter(|(_, fm)| fm.chunks.iter().any(|c| c.chunk_hash == chunk_hash))
        .map(|(path, _)| path.clone())
        .collect()
}

#[cfg(test)]
mod sched_audit_orphan_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn row(idx: u64, hash: &str, node: &str, disk: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: 1,
        }
    }

    fn files(rows: Vec<ChunkLocationEntry>) -> std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata> {
        let fm = dtfs_raft::state_machine::FileMetadata {
            path: "/f".to_string(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            chunks: rows,
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
            ever_committed: None, replica_added_at_ms: 0,
        };
        [("/f".to_string(), fm)].into()
    }

    // ---- task 1 ----
    struct Ops {
        remove: Result<(), String>,
        deleted: AtomicBool,
    }
    impl DelocateOps for Ops {
        async fn remove_location(&self) -> anyhow::Result<()> {
            self.remove.clone().map_err(|e| anyhow::anyhow!(e))
        }
        async fn delete_bytes(&self, _: Option<u64>) -> anyhow::Result<()> {
            self.deleted.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Default)]
    struct GuardOps {
        reread_ms: std::sync::Mutex<Option<u64>>,
        guard: std::sync::Mutex<Option<Option<u64>>>,
    }
    impl DelocateOps for GuardOps {
        async fn remove_location(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn bytes_still_referenced(&self) -> bool {
            *self.reread_ms.lock().unwrap() = Some(now_millis());
            false
        }
        async fn delete_bytes(&self, if_mtime_le_ms: Option<u64>) -> anyhow::Result<()> {
            *self.guard.lock().unwrap() = Some(if_mtime_le_ms);
            Ok(())
        }
    }

    /// The byte delete carries an mtime guard taken no later than the
    /// reference re-read, so a same-hash put landing after it is kept.
    #[tokio::test]
    async fn byte_delete_carries_mtime_guard_not_later_than_reread() {
        let ops = GuardOps::default();
        assert_eq!(delocate_replica(&ops).await, DelocateOutcome::Done);
        let reread = ops.reread_ms.lock().unwrap().expect("re-read ran");
        let guard = ops.guard.lock().unwrap().expect("delete issued");
        let guard = guard.expect("delete must carry if_mtime_le_ms");
        assert!(guard <= reread, "guard {guard} later than re-read {reread}");
        assert!(guard > 0);
    }

    #[test]
    fn refused_apply_is_an_error_not_success() {
        use dtfs_raft::ClientResponse as R;
        assert_eq!(client_response_ok(&R::Success), Ok(()));
        assert_eq!(client_response_ok(&R::LockGranted), Ok(()));
        assert_eq!(
            client_response_ok(&R::Error { message: "last live replica".into() }),
            Err("last live replica".to_string())
        );
        assert!(client_response_ok(&R::WriteLeaseConflict { owner_id: "o".into(), expires_at_ms: 1 }).is_err());
        assert!(client_response_ok(&R::LockConflict { owner_id: "o".into(), start: 0, end: 1, write: true }).is_err());
    }

    #[tokio::test]
    async fn refused_remove_never_deletes_bytes() {
        let refused = client_response_ok(&dtfs_raft::ClientResponse::Error { message: "would strand anchor".into() });
        let ops = Ops { remove: refused, deleted: AtomicBool::new(false) };
        assert_eq!(delocate_replica(&ops).await, DelocateOutcome::RemoveFailed);
        assert!(!ops.deleted.load(Ordering::SeqCst));

        let ok = Ops { remove: client_response_ok(&dtfs_raft::ClientResponse::Success), deleted: AtomicBool::new(false) };
        assert_eq!(delocate_replica(&ok).await, DelocateOutcome::Done);
        assert!(ok.deleted.load(Ordering::SeqCst));
    }

    struct SharedOps {
        referenced: bool,
        deleted: AtomicBool,
    }
    impl DelocateOps for SharedOps {
        async fn remove_location(&self) -> anyhow::Result<()> {
            Ok(())
        }
        async fn delete_bytes(&self, _: Option<u64>) -> anyhow::Result<()> {
            self.deleted.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn bytes_still_referenced(&self) -> bool {
            self.referenced
        }
    }

    #[tokio::test]
    async fn shared_bytes_survive_delocating_one_file() {
        // /f and /b share hash "h" on node 10 disk 0; flushing /f's row must
        // keep the bytes /b still reads.
        let mut fs = files(vec![row(0, "h", "10", 0)]);
        let mut b = fs["/f"].clone();
        b.path = "/b".into();
        fs.insert("/b".into(), b);
        fs.get_mut("/f").unwrap().chunks.clear(); // /f's row removed by Raft
        let referenced = chunk_bytes_referenced_on_disk(&fs, "h", "10", 0);
        assert!(referenced, "/b still holds h on (10, 0)");
        let ops = SharedOps { referenced, deleted: AtomicBool::new(false) };
        assert_eq!(delocate_replica(&ops).await, DelocateOutcome::Done);
        assert!(!ops.deleted.load(Ordering::SeqCst), "shared bytes deleted");
    }

    #[tokio::test]
    async fn unshared_bytes_are_deleted_after_remove() {
        let fs = files(vec![row(0, "h", "10", 1), row(1, "h", "11", 0), row(2, "x", "10", 0)]);
        // Same hash on another disk / another node, other hash on this disk.
        assert!(!chunk_bytes_referenced_on_disk(&fs, "h", "10", 0));
        // Legacy node-wide removal: any disk of the node counts.
        assert!(chunk_bytes_referenced_on_disk(&fs, "h", "10", dtfs_raft::legacy_disk_id()));
        let ops = SharedOps { referenced: false, deleted: AtomicBool::new(false) };
        assert_eq!(delocate_replica(&ops).await, DelocateOutcome::Done);
        assert!(ops.deleted.load(Ordering::SeqCst));
    }

    // ---- task 3a ----
    #[test]
    fn copy_under_another_disk_of_the_same_node_is_never_delocated() {
        // SM records the chunk on node 1 disk 2; bytes found on disk 1.
        let v = NodeReapView::from_files(&files(vec![row(0, "h", "1", 2)]), 1);
        assert!(!v.delocated("h"));
        // Unrecorded chunk is still reapable.
        assert!(v.delocated("x"));
    }

    #[test]
    fn row_naming_another_node_does_not_protect() {
        let fs = files(vec![row(0, "h", "2", 1)]);
        let v = NodeReapView::from_files(&fs, 1);
        assert!(v.delocated("h"));
        // Still referenced by a file, so labelled "delocated", not "orphan".
        assert!(referenced_hashes(&fs, ["h"]).contains("h"));
    }

    // ---- audit finding 6: reduced orphan-sweep memory ----
    #[test]
    fn view_holds_only_this_nodes_hashes_and_every_orphan_is_delocated() {
        let fs = files(vec![row(0, "a", "1", 0), row(0, "a", "2", 0), row(1, "b", "2", 3)]);
        let v = NodeReapView::from_files(&fs, 1);
        // Only node 1's hash is held, plus the namespace row count.
        assert_eq!(v.node_expected, ["a".to_string()].into());
        assert_eq!(v.rows, 3);
        // Why no namespace-wide set is needed: an unreferenced hash has no
        // row, so it is a candidate on every node.
        for node in [1, 2, 3] {
            let v = NodeReapView::from_files(&fs, node);
            assert!(orphan_reap_candidate(&v, "gone"), "node {node}");
        }
        // Empty table: no rows, the per-delete gate refuses.
        let empty = NodeReapView::from_files(&Default::default(), 1);
        assert_eq!(empty.rows, 0);
        assert!(!orphan_delete_allowed(true, empty.rows));
        // Label lookup returns only wanted hashes that are referenced.
        let r = referenced_hashes(&fs, ["b", "gone"]);
        assert_eq!(r, ["b"].into());
    }

    #[test]
    fn missing_recheck_uses_the_post_listing_view() {
        // Expected before the listing: a, b. Listing shows neither. Meanwhile
        // b's row on node 1 was removed (demote race): only a stays missing.
        let a = "a".to_string();
        let b = "b".to_string();
        let after = NodeReapView::from_files(&files(vec![row(0, "a", "1", 0), row(1, "b", "2", 0)]), 1);
        assert_eq!(still_missing_after_recheck(&[&a, &b], &after.node_expected), vec![&a]);
        let none = NodeReapView::from_files(&files(vec![row(1, "b", "2", 0)]), 1);
        assert!(still_missing_after_recheck(&[&a, &b], &none.node_expected).is_empty());
    }

    // ---- task 3b ----
    #[test]
    fn rebuilt_view_reflects_the_current_state() {
        let before = NodeReapView::from_files(&files(vec![row(0, "h", "1", 0)]), 1);
        assert!(!before.delocated("h"));
        // The replica was removed after the sweep-start snapshot.
        let after = NodeReapView::from_files(&files(vec![row(0, "h", "2", 0)]), 1);
        assert!(after.delocated("h"));
        // A replica registered meanwhile protects the bytes.
        let reg = NodeReapView::from_files(&files(vec![row(0, "new", "1", 3)]), 1);
        assert!(!reg.delocated("new"));
    }

    // ---- task 3c ----
    #[test]
    fn per_delete_gate_is_leadership_only_not_log_lag() {
        // Start-of-sweep gate rejects a lagging log ...
        assert!(orphan_sweep_skip_reason(true, 5, 10, 3).is_some());
        // ... the per-delete gate does not (the leader's own writes lag).
        assert!(orphan_delete_allowed(true, 3));
        assert!(!orphan_delete_allowed(false, 3));
        assert!(!orphan_delete_allowed(true, 0));
    }

    // ---- sweep gate: wait for catch-up instead of skipping ----
    fn probe_seq(seq: Vec<(bool, u64, u64)>) -> impl FnMut() -> (bool, u64, u64) {
        let mut it = seq.into_iter();
        let mut last = (false, 0, 0);
        move || {
            if let Some(v) = it.next() {
                last = v;
            }
            last
        }
    }

    #[tokio::test]
    async fn sweep_gate_waits_for_catch_up_under_steady_writes() {
        // Start: applied 5, log 10. The log keeps growing (steady writes),
        // but applied reaching 10 (the start-time tail) is enough.
        let p = probe_seq(vec![(true, 5, 10), (true, 5, 10), (true, 8, 12), (true, 10, 14)]);
        let r = wait_for_sweep_catch_up(p, Duration::from_millis(1), 10).await;
        assert_eq!(r, Ok(10));
    }

    #[tokio::test]
    async fn sweep_gate_refuses_a_non_leader_without_waiting() {
        let p = probe_seq(vec![(false, 10, 10)]);
        assert_eq!(wait_for_sweep_catch_up(p, Duration::from_secs(3600), 10).await, Err("not leader"));
    }

    #[tokio::test]
    async fn sweep_gate_skips_when_never_caught_up_within_the_bound() {
        let p = probe_seq(vec![(true, 5, 10), (true, 5, 10), (true, 9, 11), (true, 9, 11)]);
        let r = wait_for_sweep_catch_up(p, Duration::from_millis(1), 3).await;
        assert_eq!(r, Err("state machine not caught up"));
    }

    #[test]
    fn sweep_skip_reason_names_the_empty_view() {
        assert_eq!(orphan_sweep_skip_reason(true, 10, 10, 0), Some("empty namespace view"));
        assert_eq!(orphan_sweep_skip_reason(false, 10, 10, 0), Some("not leader"));
        assert_eq!(orphan_sweep_skip_reason(true, 9, 10, 5), Some("state machine not caught up"));
        assert_eq!(orphan_sweep_skip_reason(true, 10, 10, 5), None);
    }

    // ---- manual reap: the given grace decides ----
    #[test]
    fn manual_grace_reaps_an_orphan_the_configured_grace_still_protects() {
        let view = NodeReapView::from_files(&files(vec![row(0, "live", "1", 0)]), 1);
        let now = 100 * 86_400_000;
        let two_h_old = now - 2 * 3_600_000;
        let c = orphan_reap_candidate(&view, "gone");
        assert!(c);
        // Manual grace 1 h: due. Configured 24 h: protected.
        assert!(orphan_reap_due(c, two_h_old, now, MANUAL_ORPHAN_GRACE_FLOOR_SECS));
        assert!(!orphan_reap_due(c, two_h_old, now, 86_400));
        // Boundary: exactly the floor old is due, one second younger is not.
        assert!(orphan_reap_due(true, now - 3_600_000, now, 3600));
        assert!(!orphan_reap_due(true, now - 3_599_000, now, 3600));
        // A chunk this node/disk still holds is never a candidate.
        assert!(!orphan_reap_candidate(&view, "live"));
        // Referenced, but no row names this node: de-located.
        let other = NodeReapView::from_files(&files(vec![row(0, "live", "2", 0)]), 1);
        assert!(orphan_reap_candidate(&other, "live"));
    }
}
