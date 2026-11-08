//! TaskScheduler: migration task lifecycle: copy, verify, register, path migration, snapshots. Split out of scheduler.rs on 2026-09-13; the
//! `impl TaskScheduler` block here is an inherent impl continued from the
//! parent, and `use super::*` gives every item the parent's imports.
#![allow(unused_imports)]

use super::*;

impl TaskScheduler {
    /// Advance each in-progress migration task through its state machine:
    /// `Pending -> Copying -> Verifying -> Completing -> Done`.
    ///
    /// NOTE: Source data is NEVER removed until the target has been verified.
    pub(super) async fn advance_tasks(&self, state: &mut MigrationState) {
        let now_ms = now_millis();

        // U3 — drive the task state machine in rounds so a chunk can go
        // Pending -> Copying -> Verifying -> Completing -> Done within ONE
        // tick (pre-U3 each round was the whole 30s tick), and fan the
        // Copying `replicate_chunk` calls out bounded-parallel (globally, and
        // WI-04 per-target). Each copy RPC is wall-clock bounded by
        // `dtfs_common::replication::migration_rpc_timeout`; each verify RPC
        // by the fixed `MIGRATION_RPC_TIMEOUT` (Bug CN) — so a stalled data
        // node can't hang a round (and the loop). The terminal-task prune
        // runs at the START of `tick` (Bug CN), not here.
        advance_migration_tasks(self, state, now_ms).await;
    }


    /// Load the migration state from the dedicated Raft field.
    ///
    /// Uses `migration_state_json` instead of `cluster_config.config_json` so
    /// that scheduler bookkeeping never clobbers user-visible cluster
    /// configuration (fix for issue #3).
    pub(super) async fn load_migration_state(&self) -> MigrationState {
        let sm = self.state_data.read().await;
        if sm.migration_state_json.is_empty() {
            return MigrationState::default();
        }
        serde_json::from_str(&sm.migration_state_json).unwrap_or_default()
    }

    // -----------------------------------------------------------------------
    // Data-node RPC helpers
    // -----------------------------------------------------------------------

    /// Look up the gRPC address of a node from the Raft state machine.
    pub(crate) async fn node_grpc_addr(&self, node_id: u64) -> Option<String> {
        let sm = self.state_data.read().await;
        sm.nodes.get(&node_id).map(|info| info.grpc_addr.clone())
    }

    /// AV-2-5 — expose the Raft handle so the tiering engine (which
    /// owns a reference to the scheduler) can propose
    /// `RemoveChunkLocation` + `SetChunkCooldown` for engine-driven
    /// demotions. This mirrors the established API surface used by
    /// the manager's REST handlers — the alternative (mirror each
    /// Raft request shape as a scheduler method) would just bloat
    /// the surface without adding safety.
    pub(crate) fn raft(&self) -> &std::sync::Arc<DtfsRaft> {
        &self.raft
    }

    /// Resolve a healthy node holding a LIVE copy of `chunk_id` (node
    /// Healthy and the row's disk not Failed/Removed), optionally excluding
    /// one node id. `preferred` (the task's recorded source) is tried first
    /// so a retry does not hop sources for no reason. Bug BO-1: `exclude` is
    /// `None` for a same-node cross-tier anchor copy (`source_node ==
    /// target_node`): the planner legitimately targets a node that already
    /// holds the chunk on another tier.
    pub(super) async fn resolve_healthy_source(
        &self,
        chunk_id: &str,
        exclude: Option<u64>,
        preferred: Option<u64>,
    ) -> Option<(u64, String)> {
        let refs = self.chunk_refs(chunk_id).await;
        let sm = self.state_data.read().await;
        let locs = refs.iter().flat_map(|r| r.locs.iter().copied());
        let nid = pick_healthy_source(locs, preferred, exclude, &sm.nodes)?;
        Some((nid, sm.nodes.get(&nid)?.grpc_addr.clone()))
    }

    /// Ask the source node to replicate `chunk_id` to `target_addr`,
    /// landing the replica on `target_tier` ("hot"/"warm"/"cold"/
    /// "tape"). AV-5-9: empty tier preserves legacy behaviour (target
    /// falls back to "any healthy disk"), non-empty pins the
    /// destination tier so cache promotions actually land on warm.
    pub(super) async fn replicate_chunk(
        &self,
        source_addr: &str,
        chunk_id: &str,
        target_addr: &str,
        target_tier: &str,
        chunk_bytes: u64,
    ) -> anyhow::Result<Option<u32>> {
        // Bug W (part 2) — `source_addr` is `NodeInfo.grpc_addr`, which the
        // RegisterNode path stores already as a full URL ("http://ip:port").
        // The old code prepended "http://" unconditionally and produced
        // `http://http://...:9500` — tonic rejected this with a generic
        // "transport error" that silenced every migration in Plexmedia.
        let endpoint = normalize_endpoint(source_addr);
        // Bug CN / WI-04 — hard timeout the connect + streaming RPC so a
        // stalled source/target node can't hang the advance round (and the
        // whole scheduler loop) indefinitely. The budget scales with
        // `chunk_bytes` and is always strictly greater than the data node's
        // own inner transfer budget for the same copy (see
        // `dtfs_common::replication`), so the callee always gives up first.
        // CF-6 — the connect goes through the resolved [tls] context (mTLS
        // when enabled).
        let budget = dtfs_common::replication::migration_rpc_timeout(chunk_bytes);
        let resp = tokio::time::timeout(budget, async {
            let mut client = self.tls.connect(&endpoint)
                .await
                .map(DataServiceClient::new)
                .map_err(|e| anyhow::anyhow!("connect to source {}: {}", source_addr, e))?;
            let mut req = tonic::Request::new(ReplicateChunkRequest {
                chunk_id: chunk_id.to_string(),
                target_address: target_addr.to_string(),
                target_tier: target_tier.to_string(),
            });
            {
                use dtfs_common::crypto::{
                    data_rpc_replicate_binding, stamp_data_rpc_auth, DATA_RPC_AUTH_MAC,
                    DATA_RPC_AUTH_TS,
                };
                let secret = self.shared_secret.read().clone();
                let binding = data_rpc_replicate_binding(chunk_id, target_addr);
                if let Some((ts, mac)) =
                    stamp_data_rpc_auth(&secret, "ReplicateChunk", &binding)
                {
                    if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
                        req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
                        req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
                    }
                }
            }
            client
                .replicate_chunk(req)
                .await
                .map_err(|e| anyhow::anyhow!("ReplicateChunk RPC: {}", e))
                .map(|r| r.into_inner())
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "ReplicateChunk to {} timed out after {:?}",
                target_addr,
                budget
            )
        })??;

        if resp.success {
            // The disk the target really stored the copy on; None from an
            // old data node.
            Ok(resp.disk_id)
        } else {
            Err(anyhow::anyhow!("ReplicateChunk returned error: {}", resp.error))
        }
    }

    /// Ask the target node to verify the integrity of `chunk_id`.
    /// Returns `true` if the chunk is valid, `false` if the checksum failed.
    pub(super) async fn verify_chunk(&self, target_addr: &str, chunk_id: &str) -> anyhow::Result<bool> {
        // Bug W (part 2) — same fix as `replicate_chunk`: NodeInfo.grpc_addr
        // already carries the scheme. Don't double-prefix.
        let endpoint = normalize_endpoint(target_addr);
        // Bug CN — timeout, same rationale as `replicate_chunk`. CF-6 —
        // TLS-aware connect.
        let resp = tokio::time::timeout(MIGRATION_RPC_TIMEOUT, async {
            let mut client = self.tls.connect(&endpoint)
                .await
                .map(DataServiceClient::new)
                .map_err(|e| anyhow::anyhow!("connect to target {}: {}", target_addr, e))?;
            let mut req = tonic::Request::new(VerifyChunkRequest {
                chunk_id: chunk_id.to_string(),
            });
            {
                use dtfs_common::crypto::{
                    stamp_data_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
                };
                let secret = self.shared_secret.read().clone();
                if let Some((ts, mac)) = stamp_data_rpc_auth(&secret, "VerifyChunk", chunk_id) {
                    if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
                        req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
                        req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
                    }
                }
            }
            client
                .verify_chunk(req)
                .await
                .map_err(|e| anyhow::anyhow!("VerifyChunk RPC: {}", e))
                .map(|r| r.into_inner())
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "VerifyChunk to {} timed out after {:?}",
                target_addr,
                MIGRATION_RPC_TIMEOUT
            )
        })??;

        if resp.valid {
            Ok(true)
        } else {
            warn!(error = %resp.error, "VerifyChunk returned invalid");
            Ok(false)
        }
    }

    /// Bug AV — call `DeleteChunkReplica` on a data node. Mirrors
    /// `replicate_chunk` / `verify_chunk`: normalises the stored
    /// `NodeInfo.grpc_addr` (already-formed URL via Bug W),
    /// translates transport / RPC errors with context, and surfaces
    /// the idempotent response shape directly.
    ///
    /// Returns `(was_present, freed_bytes)` from the data node's
    /// idempotent delete (AV-1-5). A `was_present == false` is a
    /// normal no-op (retry, or chunk never landed on this node) and
    /// must NOT be treated as an error.
    /// CO-1-1 — `disk_id`/`tier` target a specific replica. `disk_id =
    /// Some(id)` unlinks only that disk's copy (protects a co-located
    /// anchor on another disk); `None` keeps the legacy first-found
    /// behaviour for callers without a disk context.
    pub(crate) async fn delete_chunk_replica_on_node(
        &self,
        node_addr: &str,
        chunk_id: &str,
        disk_id: Option<u32>,
        tier: &str,
    ) -> anyhow::Result<(bool, u64)> {
        let endpoint = normalize_endpoint(node_addr);
        let mut client = self.tls.connect(&endpoint).await
            .map(DataServiceClient::new)
            .map_err(|e| anyhow::anyhow!("connect to data node {}: {}", node_addr, e))?;

        let mut req = tonic::Request::new(dtfs_proto::dtfs::data::DeleteChunkRequest {
            chunk_id: chunk_id.to_string(),
            disk_id,
            tier: tier.to_string(),
        });
        {
            use dtfs_common::crypto::{
                stamp_data_rpc_auth, DATA_RPC_AUTH_MAC, DATA_RPC_AUTH_TS,
            };
            let secret = self.shared_secret.read().clone();
            if let Some((ts, mac)) =
                stamp_data_rpc_auth(&secret, "DeleteChunk", chunk_id)
            {
                if let (Ok(ts_v), Ok(mac_v)) = (ts.parse(), mac.parse()) {
                    req.metadata_mut().insert(DATA_RPC_AUTH_TS, ts_v);
                    req.metadata_mut().insert(DATA_RPC_AUTH_MAC, mac_v);
                }
            }
        }
        let resp = client
            .delete_chunk(req)
            .await
            .map_err(|e| anyhow::anyhow!("DeleteChunkReplica RPC: {}", e))?
            .into_inner();

        if !resp.success {
            // The handler treats absent chunks as was_present=false
            // (success=true). A success=false response is a real
            // failure on the data node side.
            return Err(anyhow::anyhow!(
                "DeleteChunkReplica returned success=false on {}",
                node_addr
            ));
        }
        Ok((resp.was_present, resp.freed_bytes))
    }

    /// Rows of every file referencing `chunk_hash`: from the per-round
    /// index when one is active, else one namespace pass for this hash.
    pub(super) async fn chunk_refs(&self, chunk_hash: &str) -> Vec<ChunkRef> {
        if let Some(refs) = self
            .round_index
            .lock()
            .as_ref()
            .and_then(|i| i.get(chunk_hash))
            .map(|r| r.to_vec())
        {
            return refs;
        }
        let wanted: std::collections::HashSet<String> = [chunk_hash.to_string()].into();
        let sm = self.state_data.read().await;
        ChunkIndex::build(&sm.files, &wanted)
            .get(chunk_hash)
            .map(|r| r.to_vec())
            .unwrap_or_default()
    }

    /// Completing-stage replica registration. Registers the copy on
    /// `target_node` at the disk it ACTUALLY landed on (`landed`, from
    /// `ReplicateChunkResponse.disk_id`); for an old data node that reports
    /// none, the disk is only inferred when the target has exactly one live
    /// disk of `tier` (`pick_registration_disk`). Only the NEW row(s) go to
    /// Raft (never the read set of existing rows), once per referencing file
    /// and chunk_index. `Err` keeps the task Completing (retry next tick):
    /// no usable disk, hash no longer resolves to any row, or a refused or
    /// failed `UpdateChunkLocations`.
    pub(super) async fn register_replica_location(
        &self,
        chunk_id: &str,
        target_node: u64,
        tier: DiskTier,
        landed: Option<u32>,
    ) -> Result<(), String> {
        let refs = self.chunk_refs(chunk_id).await;
        if refs.is_empty() {
            warn!(
                chunk = %chunk_id,
                "CR-3-4/MGR-9: chunk hash not found in any file; not registering"
            );
            return Err(format!("chunk {chunk_id} hash not found in any file"));
        }
        let disk = {
            let sm = self.state_data.read().await;
            pick_registration_disk(sm.nodes.get(&target_node), tier, landed)
        };
        let disk = disk.map_err(|e| {
            warn!(chunk = %chunk_id, target_node, ?tier, reason = %e, "not registering replica");
            e
        })?;
        let writes = plan_new_replica_rows(&refs, chunk_id, target_node, disk);
        if writes.len() > 1 {
            debug!(
                chunk = %chunk_id,
                files = writes.len(),
                "REV-4-6: chunk is shared; registering the new replica for every referencing file"
            );
        }
        let mut first_err: Option<String> = None;
        for (path, chunks) in writes {
            let req = ClientRequest::UpdateChunkLocations {
                now_ms: now_millis(),
                path,
                chunks,
                // Bug P — system-driven, so no client lease applies.
                lease_id: String::new(),
            };
            // Checked: a lease/lock conflict or Error response is NOT a
            // registration.
            if let Err(e) = self.raft_write_checked(req).await {
                warn!(
                    chunk = %chunk_id,
                    error = %e,
                    "Bug AB UpdateChunkLocations Raft write failed; new replica not registered yet"
                );
                if first_err.is_none() {
                    first_err = Some(format!("UpdateChunkLocations failed: {e}"));
                }
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Bug AE / WI-04 — quick existence + size lookup for a chunk hash
    /// across every file in the metadata tree. `None` iff no
    /// `FileMetadata` references the chunk (orphan); `Some(bytes)` is the
    /// chunk's recorded size, used to size the replication RPC deadlines.
    /// Used to detect orphan migration tasks: when the underlying file has
    /// been unlinked or the source disk wiped, the task's chunk_id is no
    /// longer reachable from any live file and we should stop retrying it.
    /// Delegates to the pure helper `chunk_size_in_any_file` so the
    /// predicate logic is unit-testable.
    pub(super) async fn chunk_bytes_anywhere(&self, chunk_hash: &str) -> Option<u64> {
        let refs = self.chunk_refs(chunk_hash).await;
        chunk_bytes_from_refs(&refs)
    }

    /// Persist the migration state via the dedicated `UpdateMigrationState`
    /// Raft command so that it never overwrites `cluster_config` (fix #3).
    pub(super) async fn save_migration_state(&self, state: &MigrationState) {
        let json = match serde_json::to_string(state) {
            Ok(j) => j,
            Err(e) => {
                error!(error = %e, "Failed to serialize migration state");
                return;
            }
        };

        let req = ClientRequest::UpdateMigrationState { json };

        if let Err(e) = self.raft.client_write(req).await {
            // Bug AB diagnostic — followers will see this often (expected
            // because they forward to leader and the call returns
            // ForwardToLeader rather than committing locally). But when
            // EVERY save fails for an extended period the cluster is
            // leaderless and migration tasks loop infinitely re-running
            // ReplicateChunk for the same chunk every tick. Promoted from
            // debug to warn so the symptom is visible in standard log
            // levels — operators should investigate when this is the
            // only manager logging "Migration: Copying -> Verifying"
            // for the same task_id every 30s.
            warn!(error = %e, "Bug AB save_migration_state failed (may not be leader)");
        }
    }

    // -----------------------------------------------------------------------
    // Manual / operator-driven path migration
    // -----------------------------------------------------------------------

    /// Plan and enqueue a manual data migration for every chunk whose owning
    /// file path starts with `path_prefix`. For each chunk the engine ensures
    /// at least one replica satisfies `target_tier` (and, when set,
    /// `target_dc`). Existing replicas are left in place — this is additive,
    /// not destructive, so a follow-up scrub or tiering pass can reclaim
    /// space later without risking data loss during the move.
    ///
    /// Semantics:
    /// * **Already satisfied** — a chunk that already has a replica on a node
    ///   whose disk tier matches `target_tier` and whose datacenter matches
    ///   `target_dc` (when set). Counted, not rescheduled.
    /// * **Scheduled** — one new replication task per chunk needing a copy,
    ///   inserted via [`schedule_replication`](Self::schedule_replication).
    /// * **No source** — chunk has no live healthy replica; can't be copied.
    /// * **No target** — no healthy node matches the tier/DC constraint and
    ///   does not already hold the chunk; needs a fresh node or a relaxed
    ///   constraint.
    ///
    /// `max_tasks` caps the number of new replication tasks enqueued in a
    /// single call so an "all files → tape" command can't lock the
    /// scheduler. Anything past the cap is reported under `truncated_chunks`
    /// and the operator can re-run the call to pick up where it left off
    /// (already-scheduled chunks become `already_satisfied` on the next run
    /// after the task completes).
    pub async fn schedule_path_migration(
        &self,
        path_prefix: &str,
        target_tier: DiskTier,
        target_dc: Option<&str>,
        max_tasks: usize,
    ) -> PathMigrationReport {
        // Snapshot file metadata + node table under one read lock so all
        // decisions are made against a consistent view.
        let (matching_files, nodes_snapshot): (
            Vec<(String, Vec<ChunkLocationEntry>)>,
            HashMap<u64, NodeSummary>,
        ) = {
            let sm = self.state_data.read().await;
            let files: Vec<(String, Vec<ChunkLocationEntry>)> = sm
                .files
                .iter()
                .filter(|(path, _)| path_matches_prefix(path, path_prefix))
                .map(|(path, meta)| (path.clone(), meta.chunks.clone()))
                .collect();
            let nodes: HashMap<u64, NodeSummary> = sm
                .nodes
                .iter()
                .map(|(id, info)| {
                    (
                        *id,
                        NodeSummary {
                            grpc_addr: info.grpc_addr.clone(),
                            datacenter: info.location.datacenter.clone(),
                            healthy: info.health == HealthStatus::Healthy,
                            tiers: info.disks.iter().map(|d| d.tier).collect(),
                        },
                    )
                })
                .collect();
            (files, nodes)
        };

        let (mut report, plan) = plan_path_migration(
            &matching_files,
            &nodes_snapshot,
            target_tier,
            target_dc,
            max_tasks,
        );

        // Dispatch the planned tasks in ONE batch. Scheduling deduplicates
        // so a retry of this call is safe; only NEW tasks are reported.
        let reqs: Vec<(&str, u64, u64, DiskTier)> = plan
            .iter()
            .map(|a| (a.chunk_hash.as_str(), a.source_node, a.target_node, a.tier))
            .collect();
        let outcomes = self
            .schedule_replications(&reqs, usize::MAX, MAX_LIVE_TASKS)
            .await;
        // `max_new` is unbounded here, so Deferred means the tick holds the
        // migration state: report busy so the REST layer answers 503.
        report.busy = outcomes.contains(&ScheduleOutcome::Deferred);
        report.tasks_scheduled = new_task_count(&outcomes);

        info!(
            prefix = %path_prefix,
            tier = ?target_tier,
            dc = ?target_dc,
            files = report.files_matched,
            chunks = report.chunks_total,
            scheduled = report.tasks_scheduled,
            satisfied = report.already_satisfied,
            no_source = report.no_source,
            no_target = report.no_target,
            truncated = report.truncated_chunks,
            "schedule_path_migration complete"
        );

        report
    }

    /// Lightweight snapshot of all migration tasks (used by the WebUI
    /// "Migrations" panel and `GET /api/v1/migrations`).
    pub async fn migration_snapshot(&self) -> Vec<serde_json::Value> {
        let state = self.load_migration_state().await;
        let mut out: Vec<_> = state
            .tasks
            .values()
            .map(|t| {
                serde_json::json!({
                    "task_id":    t.task_id,
                    "chunk_id":   t.chunk_id,
                    "source_node": t.source_node,
                    "target_node": t.target_node,
                    "tier":        format!("{:?}", t.tier).to_ascii_lowercase(),
                    "status":      format!("{:?}", t.status).to_ascii_lowercase(),
                    "attempts":    t.attempts,
                    "last_error":  t.last_error,
                    "created_at_ms": t.created_at_ms,
                    "updated_at_ms": t.updated_at_ms,
                })
            })
            .collect();
        // Bug AW — sort by created_at_ms (set-once) DESC, tiebreak by
        // natural-numeric task_id ASC. Bug AM's earlier fix sorted on
        // updated_at_ms, but that field is bumped on every scheduler
        // tick for any task in flight, so running tasks shuffled position
        // on each 5s WebUI refresh. created_at_ms is immutable after
        // insertion → newest task on top, frozen position thereafter.
        // Natural-numeric tiebreak ("repl-2" before "repl-10") replaces
        // the lexicographic compare that put "repl-10" between "repl-1"
        // and "repl-2".
        sort_migration_snapshot(&mut out);
        out
    }

    // -----------------------------------------------------------------------
    // Bug AV — operator-driven cache-tier flush (AV-1-7)
    // -----------------------------------------------------------------------
}

/// U3 — the side-effecting operations the migration state machine needs.
/// One impl is live (`TaskScheduler`); tests supply a mock. All methods are
/// independent per task, which is what makes the Copying RPCs safe to
/// fan out in parallel.
pub(super) trait MigrationOps {
    /// Bug CH dispatch guard: can `target_node` possibly receive a copy on
    /// `tier`? `Err(reason)` fast-fails the task to Failed.
    async fn validate_target(&self, target_node: u64, tier: DiskTier) -> Result<(), String>;
    /// Bug AE orphan guard + WI-04 RPC-deadline sizing: is `chunk_id` still
    /// referenced by some file, and if so how big is it? `None` fast-fails
    /// the task exactly as the old `chunk_exists() -> false` did (same
    /// audit, same reason string); `Some(0)` (unknown/legacy size) falls
    /// back to the floor in `dtfs_common::replication`.
    async fn chunk_bytes(&self, chunk_id: &str) -> Option<u64>;
    /// F-015: pick a healthy node holding `chunk_id` (optionally excluding
    /// one), returning `(node_id, grpc_addr)`.
    async fn resolve_source(&self, chunk_id: &str, exclude: Option<u64>) -> Option<(u64, String)>;
    /// gRPC address of `node_id`, if registered.
    async fn target_addr(&self, node_id: u64) -> Option<String>;
    /// The parallelizable RPC: copy `chunk_id` (`chunk_bytes` long) from
    /// `src_addr` to `tgt_addr`, landing on `tier_str`. Independent per
    /// task.
    async fn replicate(
        &self,
        src_addr: &str,
        chunk_id: &str,
        tgt_addr: &str,
        tier_str: &str,
        chunk_bytes: u64,
    ) -> Result<(), String>;
    /// VerifyChunk on the target. `Ok(false)` = checksum mismatch.
    async fn verify(&self, tgt_addr: &str, chunk_id: &str) -> Result<bool, String>;
    /// Completing: register the new replica location through Raft.
    /// CR-3-4/MGR-4 — `Err` keeps the task Completing (retry next tick)
    /// instead of unconditionally advancing to Done.
    async fn register_replica(
        &self,
        chunk_id: &str,
        target_node: u64,
        tier: DiskTier,
    ) -> Result<(), String>;
    /// Append an audit-log line (scheduler actor).
    fn audit(&self, action: &str, detail: &str, outcome: &str);
    /// Fire the MigrationFailed alert for an exhausted task.
    fn fire_migration_failed(&self, task: &ReplicationTask, reason: &str);

    /// Like `resolve_source`, trying the task's recorded source first.
    async fn resolve_source_preferring(
        &self,
        chunk_id: &str,
        exclude: Option<u64>,
        _preferred: u64,
    ) -> Option<(u64, String)> {
        self.resolve_source(chunk_id, exclude).await
    }
    /// Like `replicate`, also returning the disk the target stored the copy
    /// on (None = not reported).
    async fn replicate_landed(
        &self,
        src_addr: &str,
        chunk_id: &str,
        tgt_addr: &str,
        tier_str: &str,
        chunk_bytes: u64,
    ) -> Result<Option<u32>, String> {
        self.replicate(src_addr, chunk_id, tgt_addr, tier_str, chunk_bytes)
            .await
            .map(|_| None)
    }
    /// Like `register_replica`, with the disk the copy landed on.
    async fn register_replica_on(
        &self,
        chunk_id: &str,
        target_node: u64,
        tier: DiskTier,
        _landed: Option<u32>,
    ) -> Result<(), String> {
        self.register_replica(chunk_id, target_node, tier).await
    }
    /// Build the per-round chunk index for `wanted` (live impl only).
    async fn begin_round(&self, _wanted: &std::collections::HashSet<String>) {}
    /// Drop the per-round chunk index.
    fn end_round(&self) {}
    /// Persist migration state mid-tick (after each round).
    async fn save_state(&self, _state: &MigrationState) {}
}

impl MigrationOps for TaskScheduler {
    async fn validate_target(&self, target_node: u64, tier: DiskTier) -> Result<(), String> {
        let sm = self.state_data.read().await;
        validate_task_target(sm.nodes.get(&target_node), tier).map_err(|s| s.to_string())
    }

    async fn chunk_bytes(&self, chunk_id: &str) -> Option<u64> {
        self.chunk_bytes_anywhere(chunk_id).await
    }

    async fn resolve_source(&self, chunk_id: &str, exclude: Option<u64>) -> Option<(u64, String)> {
        self.resolve_healthy_source(chunk_id, exclude, None).await
    }

    async fn resolve_source_preferring(
        &self,
        chunk_id: &str,
        exclude: Option<u64>,
        preferred: u64,
    ) -> Option<(u64, String)> {
        self.resolve_healthy_source(chunk_id, exclude, Some(preferred)).await
    }

    async fn target_addr(&self, node_id: u64) -> Option<String> {
        self.node_grpc_addr(node_id).await
    }

    async fn replicate(
        &self,
        src_addr: &str,
        chunk_id: &str,
        tgt_addr: &str,
        tier_str: &str,
        chunk_bytes: u64,
    ) -> Result<(), String> {
        self.replicate_landed(src_addr, chunk_id, tgt_addr, tier_str, chunk_bytes)
            .await
            .map(|_| ())
    }

    async fn replicate_landed(
        &self,
        src_addr: &str,
        chunk_id: &str,
        tgt_addr: &str,
        tier_str: &str,
        chunk_bytes: u64,
    ) -> Result<Option<u32>, String> {
        self.replicate_chunk(src_addr, chunk_id, tgt_addr, tier_str, chunk_bytes)
            .await
            .map_err(|e| format!("ReplicateChunk error: {e}"))
    }

    async fn verify(&self, tgt_addr: &str, chunk_id: &str) -> Result<bool, String> {
        self.verify_chunk(tgt_addr, chunk_id)
            .await
            .map_err(|e| e.to_string())
    }

    async fn register_replica(
        &self,
        chunk_id: &str,
        target_node: u64,
        tier: DiskTier,
    ) -> Result<(), String> {
        self.register_replica_location(chunk_id, target_node, tier, None).await
    }

    async fn register_replica_on(
        &self,
        chunk_id: &str,
        target_node: u64,
        tier: DiskTier,
        landed: Option<u32>,
    ) -> Result<(), String> {
        self.register_replica_location(chunk_id, target_node, tier, landed).await
    }

    async fn begin_round(&self, wanted: &std::collections::HashSet<String>) {
        let idx = {
            let sm = self.state_data.read().await;
            ChunkIndex::build(&sm.files, wanted)
        };
        *self.round_index.lock() = Some(idx);
    }

    fn end_round(&self) {
        *self.round_index.lock() = None;
    }

    async fn save_state(&self, state: &MigrationState) {
        self.save_migration_state(state).await;
    }

    fn audit(&self, action: &str, detail: &str, outcome: &str) {
        self.audit.log("scheduler", action, detail, outcome);
    }

    fn fire_migration_failed(&self, task: &ReplicationTask, reason: &str) {
        if let Some(alerts) = self.alerts.read().clone() {
            alerts.fire(
                crate::alerts::AlertKind::MigrationFailed,
                "migration task exhausted retries",
                serde_json::json!({
                    "task_id": task.task_id,
                    "chunk_id": task.chunk_id,
                    "attempts": task.attempts,
                    "reason": reason,
                    "source_node": task.source_node,
                    "target_node": task.target_node,
                }),
            );
        }
    }
}

/// U3 — a copy job planned for one Copying task in the current round. The
/// pre-checks (target/source/orphan) already passed; only the parallelizable
/// `replicate` RPC remains. `resolved_source` may differ from the task's
/// recorded `source_node` (F-015 re-resolution) — applied when results land.
pub(super) struct CopyJob {
    pub(super) task_id: String,
    pub(super) chunk_id: String,
    pub(super) resolved_source: u64,
    pub(super) src_addr: String,
    pub(super) tgt_addr: String,
    pub(super) tier_str: String,
    /// WI-04 — the chunk's byte count, looked up once (alongside the Bug AE
    /// orphan guard) and carried through to `replicate` so the RPC deadline
    /// scales with it.
    pub(super) chunk_bytes: u64,
}

/// U3 — drive every non-terminal task through its state machine, looping in
/// rounds until a round makes no progress (or the `MAX_ADVANCE_ROUNDS` guard
/// trips). Within a round, the Copying `replicate` RPCs are dispatched
/// bounded-parallel (`MIGRATION_COPY_CONCURRENCY`); Pending/Verifying/
/// Completing transitions stay serial (cheap, and Completing writes Raft).
/// Mutates `state` in place; the caller persists once per tick.
pub(super) async fn advance_migration_tasks<O: MigrationOps>(
    ops: &O,
    state: &mut MigrationState,
    now_ms: u64,
) {
    advance_migration_tasks_budgeted(ops, state, now_ms, MIGRATION_TICK_BUDGET).await
}

/// `advance_migration_tasks` with an explicit wall-clock `budget`: no round
/// after the first starts once it is spent, and state is saved after every
/// round that progressed (so a leader change loses at most one round).
pub(super) async fn advance_migration_tasks_budgeted<O: MigrationOps>(
    ops: &O,
    state: &mut MigrationState,
    now_ms: u64,
    budget: Duration,
) {
    let started = std::time::Instant::now();
    // A task gets at most ONE Copying RPC and ONE Verifying RPC per call
    // (i.e. per tick). This preserves the pre-U3 retry cadence — a no-source
    // / transient-error task still burns one attempt per tick over
    // MAX_MIGRATION_ATTEMPTS ticks, and a verify-mismatch flap re-copies on
    // the NEXT tick, not by spinning rounds in this one. The round loop only
    // exists to let a *forward-progressing* chunk climb Pending -> Done in
    // one tick; it never re-dispatches a network RPC to the same task.
    let mut copied: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut verified: std::collections::HashSet<String> = std::collections::HashSet::new();
    for round in 0..MAX_ADVANCE_ROUNDS {
        if round > 0 && started.elapsed() >= budget {
            info!(round, ?budget, "Migration: tick budget spent; remaining work deferred to next tick");
            break;
        }
        let progressed = advance_round(ops, state, now_ms, &mut copied, &mut verified).await;
        if !progressed {
            break;
        }
        ops.save_state(state).await;
    }
}

/// U3 — a single round over all non-terminal tasks. Returns true if any task
/// changed status this round (so the caller loops again). A chunk can thus
/// climb Pending -> Copying -> Verifying -> Completing -> Done across four
/// rounds of ONE tick instead of four ticks. `copied`/`verified` carry the
/// once-per-call dispatch guard across rounds (see `advance_migration_tasks`).
pub(super) async fn advance_round<O: MigrationOps>(
    ops: &O,
    state: &mut MigrationState,
    now_ms: u64,
    copied: &mut std::collections::HashSet<String>,
    verified: &mut std::collections::HashSet<String>,
) -> bool {
    use futures::stream::{self, StreamExt};

    let mut progressed = false;
    let task_ids: Vec<String> = state.tasks.keys().cloned().collect();
    // One namespace pass for the whole round (not one per task).
    let wanted: std::collections::HashSet<String> = state
        .tasks
        .values()
        .filter(|t| !matches!(t.status, MigrationStatus::Done | MigrationStatus::Failed))
        .map(|t| t.chunk_id.clone())
        .collect();
    ops.begin_round(&wanted).await;

    // ---- Phase 1: Pending -> Copying (pure, serial). ----
    for task_id in &task_ids {
        let Some(task) = state.tasks.get_mut(task_id) else { continue };
        if task.status == MigrationStatus::Pending {
            info!(
                task_id = %task.task_id,
                chunk = %task.chunk_id,
                "Migration: Pending -> Copying"
            );
            task.status = MigrationStatus::Copying;
            task.attempts = 0;
            task.last_error = None;
            task.updated_at_ms = now_ms;
            progressed = true;
        }
    }

    // ---- Phase 2: Copying pre-checks (serial state reads), then dispatch
    // the surviving copies bounded-parallel. ----
    let mut jobs: Vec<CopyJob> = Vec::new();
    for task_id in &task_ids {
        // Bounded round: leave the rest Copying for the next round / tick.
        if jobs.len() >= MIGRATION_MAX_JOBS_PER_ROUND {
            break;
        }
        // Snapshot the fields we need; drop the &mut borrow before awaiting.
        let (chunk_id, target_node, source_node, tier) = {
            let Some(task) = state.tasks.get(task_id) else { continue };
            if task.status != MigrationStatus::Copying {
                continue;
            }
            (task.chunk_id.clone(), task.target_node, task.source_node, task.tier)
        };

        // Once-per-call guard: a task gets at most one copy attempt per tick
        // (a verify-mismatch flap back to Copying re-copies NEXT tick, and a
        // no-source/transient failure burns one attempt per tick — pre-U3
        // cadence). Skip any task already attempted this call.
        if !copied.insert(task_id.clone()) {
            continue;
        }

        // Bug CH — fast-fail tasks whose target can never receive the copy.
        if let Err(reason) = ops.validate_target(target_node, tier).await {
            warn!(
                task_id = %task_id,
                chunk = %chunk_id,
                target = target_node,
                "Bug CH: dropping migration task with invalid target — {}",
                reason
            );
            if let Some(task) = state.tasks.get_mut(task_id) {
                task.status = MigrationStatus::Failed;
                task.last_error = Some(reason.clone());
                task.updated_at_ms = now_ms;
            }
            ops.audit(
                "migration.invalid_target",
                &format!("task={task_id} chunk={chunk_id} target={target_node}"),
                &reason,
            );
            progressed = true;
            continue;
        }

        // Bug AE — drop orphan tasks whose chunk no longer exists anywhere.
        // WI-04: this lookup also gives us the chunk's byte count, carried
        // into the CopyJob to size the replication RPC deadline.
        let chunk_bytes = match ops.chunk_bytes(&chunk_id).await {
            Some(b) => b,
            None => {
                let reason = "chunk no longer exists (orphan task from \
                              pre-wipe or unlinked file)"
                    .to_string();
                warn!(
                    task_id = %task_id,
                    chunk = %chunk_id,
                    "Bug AE: dropping orphan migration task — {}",
                    reason
                );
                if let Some(task) = state.tasks.get_mut(task_id) {
                    task.status = MigrationStatus::Failed;
                    task.last_error = Some(reason.clone());
                    task.updated_at_ms = now_ms;
                }
                ops.audit(
                    "migration.orphan_dropped",
                    &format!("task={task_id} chunk={chunk_id}"),
                    &reason,
                );
                progressed = true;
                continue;
            }
        };

        let target_addr = ops.target_addr(target_node).await;
        // Bug BO-1: for a same-node cross-tier anchor copy, don't exclude
        // the target — it IS the only holder (on its other tier).
        let exclude = anchor_copy_source_exclude(source_node, target_node);
        let resolved_source = ops
            .resolve_source_preferring(&chunk_id, exclude, source_node)
            .await;

        match (resolved_source, target_addr) {
            (Some((src_node, src_addr)), Some(tgt_addr)) => {
                jobs.push(CopyJob {
                    task_id: task_id.clone(),
                    chunk_id: chunk_id.clone(),
                    resolved_source: src_node,
                    src_addr,
                    tgt_addr,
                    tier_str: tier.to_string(),
                    chunk_bytes,
                });
            }
            (None, _) => {
                record_copy_failure(ops, state, task_id, "no healthy source replica".to_string(), now_ms);
                progressed = true;
            }
            (_, None) => {
                record_copy_failure(
                    ops,
                    state,
                    task_id,
                    format!("target node {target_node} unresolvable"),
                    now_ms,
                );
                progressed = true;
            }
        }
    }

    // Dispatch the planned copies, at most MIGRATION_COPY_CONCURRENCY in
    // flight globally, and at most MIGRATION_COPIES_PER_TARGET against any
    // one target address (WI-04) — every anchor-repair copy targets the
    // single LELI anchor node, so without the per-target gate the global
    // cap fair-shares one 40 Mbps link N ways and no copy finishes before
    // its deadline. Each future is independent (distinct task/chunk/target).
    if !jobs.is_empty() {
        // CO-3-5 — this map is local to the round, never scheduler state;
        // it does not outlive this call and holds no lock across an await
        // taken elsewhere. Permits are per-round, not per-tick: a fresh
        // Semaphore is built here (not carried across `advance_round`
        // calls), which is fine because a task gets at most one copy
        // attempt per call (the `copied` guard) — there is no cross-round
        // contention to preserve.
        let mut gates: std::collections::HashMap<String, Arc<tokio::sync::Semaphore>> =
            std::collections::HashMap::new();
        for job in &jobs {
            gates
                .entry(job.tgt_addr.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Semaphore::new(MIGRATION_COPIES_PER_TARGET)));
        }

        let results: Vec<(String, u64, Result<Option<u32>, String>)> = stream::iter(jobs)
            .map(|job| {
                // Safe to unwrap: every job's tgt_addr was inserted above.
                let gate = gates.get(&job.tgt_addr).expect("gate for tgt_addr").clone();
                async move {
                    // Acquiring the per-target permit BEFORE dispatching the
                    // RPC is load-bearing: the RPC's own deadline must start
                    // only once the copy actually begins, not while queued
                    // behind other copies to the same target.
                    let _permit = gate
                        .acquire_owned()
                        .await
                        .expect("gate semaphore is never closed");
                    let r = ops
                        .replicate_landed(
                            &job.src_addr,
                            &job.chunk_id,
                            &job.tgt_addr,
                            &job.tier_str,
                            job.chunk_bytes,
                        )
                        .await;
                    (job.task_id, job.resolved_source, r)
                }
            })
            .buffer_unordered(MIGRATION_COPY_CONCURRENCY)
            .collect()
            .await;

        for (task_id, resolved_source, result) in results {
            progressed = true;
            match result {
                Ok(landed) => {
                    match landed {
                        Some(d) => {
                            state.landed_disks.insert(task_id.clone(), d);
                        }
                        None => {
                            state.landed_disks.remove(&task_id);
                        }
                    }
                    if let Some(task) = state.tasks.get_mut(&task_id) {
                        if task.source_node != resolved_source {
                            info!(
                                task_id = %task.task_id,
                                chunk = %task.chunk_id,
                                old_src = task.source_node,
                                new_src = resolved_source,
                                "Re-replication: source moved to a healthier replica",
                            );
                            task.source_node = resolved_source;
                        }
                        info!(
                            task_id = %task.task_id,
                            chunk = %task.chunk_id,
                            "Migration: Copying -> Verifying (ReplicateChunk OK)"
                        );
                        task.status = MigrationStatus::Verifying;
                        // CO-3-4 — do NOT reset `attempts` here. A successful
                        // copy that is a re-copy after a verify mismatch must
                        // keep the accumulated count, or a persistently corrupt
                        // source loops Copying<->Verifying forever (the verify
                        // mismatch bumps `attempts`, this reset wiped it). Fresh
                        // tasks are already zeroed at Pending->Copying (Phase 1).
                        task.updated_at_ms = now_ms;
                    }
                }
                Err(reason) => {
                    // Record the source we actually tried (F-015) before
                    // accounting the failure.
                    if let Some(task) = state.tasks.get_mut(&task_id) {
                        task.source_node = resolved_source;
                    }
                    record_copy_failure(ops, state, &task_id, reason, now_ms);
                }
            }
        }
    }

    // ---- Phase 3: Verifying (serial). ----
    for task_id in &task_ids {
        let (chunk_id, target_node) = {
            let Some(task) = state.tasks.get(task_id) else { continue };
            if task.status != MigrationStatus::Verifying {
                continue;
            }
            (task.chunk_id.clone(), task.target_node)
        };

        // Once-per-call guard (see phase 2): at most one VerifyChunk RPC per
        // task per tick. A transient verify error retries on the NEXT tick.
        if !verified.insert(task_id.clone()) {
            continue;
        }

        match ops.target_addr(target_node).await {
            Some(tgt_addr) => match ops.verify(&tgt_addr, &chunk_id).await {
                Ok(true) => {
                    if let Some(task) = state.tasks.get_mut(task_id) {
                        info!(
                            task_id = %task.task_id,
                            chunk = %chunk_id,
                            "Migration: Verifying -> Completing (checksum OK)"
                        );
                        task.status = MigrationStatus::Completing;
                        task.updated_at_ms = now_ms;
                    }
                    progressed = true;
                }
                Ok(false) => {
                    warn!(
                        task_id = %task_id,
                        chunk = %chunk_id,
                        "VerifyChunk failed (checksum mismatch); retrying copy"
                    );
                    // CO-3-4 — a mismatch is a failed attempt: charge it (and
                    // fail+alert at the cap) so a persistently corrupt source
                    // can't loop Copying<->Verifying forever. The copy-success
                    // path no longer resets `attempts`, so this count survives
                    // the next tick's re-copy and climbs to the cap.
                    record_copy_failure(
                        ops,
                        state,
                        task_id,
                        "VerifyChunk checksum mismatch".to_string(),
                        now_ms,
                    );
                    // If not yet exhausted, flap back to Copying to re-copy
                    // next tick. A flap is not forward progress; do NOT set
                    // `progressed` (else MAX_ADVANCE_ROUNDS is the only brake
                    // on a persistently-corrupt copy looping in one tick — the
                    // round loop should fall through to the next tick instead).
                    if let Some(task) = state.tasks.get_mut(task_id) {
                        if task.status != MigrationStatus::Failed {
                            task.status = MigrationStatus::Copying;
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        task_id = %task_id,
                        chunk = %chunk_id,
                        "VerifyChunk RPC error, will retry: {}",
                        e
                    );
                    // CO-3-4 — a verify RPC error is also a failed attempt
                    // (don't leave `attempts` untouched). On exhaustion this
                    // fails+alerts; otherwise the task stays Verifying and
                    // retries the verify on the next tick.
                    record_copy_failure(
                        ops,
                        state,
                        task_id,
                        format!("VerifyChunk RPC error: {e}"),
                        now_ms,
                    );
                }
            },
            None => {
                if let Some(task) = state.tasks.get_mut(task_id) {
                    warn!(
                        task_id = %task.task_id,
                        "Cannot resolve target node {} address for verify; failing task",
                        task.target_node
                    );
                    task.status = MigrationStatus::Failed;
                    task.updated_at_ms = now_ms;
                }
                progressed = true;
            }
        }
    }

    // ---- Phase 4: Completing -> Done (serial; writes Raft). ----
    // Phase 2 may have run for a long time; re-index just the chunks being
    // registered so the planned rows come from a fresh view.
    let completing: std::collections::HashSet<String> = state
        .tasks
        .values()
        .filter(|t| t.status == MigrationStatus::Completing)
        .map(|t| t.chunk_id.clone())
        .collect();
    if !completing.is_empty() {
        ops.begin_round(&completing).await;
    }
    for task_id in &task_ids {
        let (chunk_id, target_node, tier, source_node) = {
            let Some(task) = state.tasks.get(task_id) else { continue };
            if task.status != MigrationStatus::Completing {
                continue;
            }
            (task.chunk_id.clone(), task.target_node, task.tier, task.source_node)
        };

        // Once-per-call registration guard (shares the `verified` set under a
        // "reg:" key): a failing register must burn one attempt per tick,
        // not one per round.
        if !verified.insert(format!("reg:{task_id}")) {
            continue;
        }
        let landed = state.landed_disks.get(task_id).copied();
        match ops.register_replica_on(&chunk_id, target_node, tier, landed).await {
            Ok(()) => {
                if let Some(task) = state.tasks.get_mut(task_id) {
                    info!(
                        task_id = %task.task_id,
                        chunk = %chunk_id,
                        target_node,
                        tier = ?tier,
                        "Migration: Completing -> Done (new replica registered)"
                    );
                    task.status = MigrationStatus::Done;
                    task.updated_at_ms = now_ms;
                }
                ops.audit(
                    "replication.complete",
                    &format!("chunk={chunk_id} src={source_node} dst={target_node}"),
                    "success",
                );
            }
            Err(reason) => {
                // CR-3-4/MGR-4 — a failed registration (Raft write error,
                // leader change, unresolvable hash, or no live disk of
                // the tier) must NOT advance to Done: the bytes may be on
                // the target but no row exists, so leaving the task
                // Completing lets it retry next tick instead of silently
                // losing coverage. `record_copy_failure` bumps `attempts`
                // and only fails+alerts at the existing cap.
                record_copy_failure(
                    ops,
                    state,
                    task_id,
                    format!("register_replica: {reason}"),
                    now_ms,
                );
            }
        }
        progressed = true;
    }

    ops.end_round();
    state.landed_disks.retain(|id, _| {
        state
            .tasks
            .get(id)
            .is_some_and(|t| !matches!(t.status, MigrationStatus::Done | MigrationStatus::Failed))
    });
    progressed
}

/// U3 — account a Copying-stage failure: bump attempts, promote a
/// "Chunk not found" to instant-Failed (Bug AF), and on exhaustion mark the
/// task Failed + audit + alert. Mirrors the pre-U3 inline accounting.
pub(super) fn record_copy_failure<O: MigrationOps>(
    ops: &O,
    state: &mut MigrationState,
    task_id: &str,
    reason: String,
    now_ms: u64,
) {
    let Some(task) = state.tasks.get_mut(task_id) else { return };
    task.attempts = task.attempts.saturating_add(1);
    task.last_error = Some(reason.clone());
    task.updated_at_ms = now_ms;

    // Bug AF — a source-side "Chunk not found" can never resolve by retry
    // (the bytes are gone from that node); promote to instant-Failed.
    if rpc_error_indicates_chunk_missing(&reason) {
        task.attempts = MAX_MIGRATION_ATTEMPTS;
    }

    if task.attempts >= MAX_MIGRATION_ATTEMPTS {
        warn!(
            task_id = %task.task_id,
            chunk = %task.chunk_id,
            attempts = task.attempts,
            reason = %reason,
            "Migration: Copying -> Failed (max attempts exhausted)"
        );
        task.status = MigrationStatus::Failed;
        let snapshot = task.clone();
        ops.audit(
            "migration.failed",
            &format!(
                "task={} chunk={} attempts={}",
                snapshot.task_id, snapshot.chunk_id, snapshot.attempts
            ),
            &reason,
        );
        ops.fire_migration_failed(&snapshot, &reason);
    } else {
        warn!(
            task_id = %task.task_id,
            chunk = %task.chunk_id,
            attempt = task.attempts,
            max = MAX_MIGRATION_ATTEMPTS,
            reason = %reason,
            "Copying retry queued",
        );
    }
}

/// One (file, chunk_index) that references a chunk hash, with every replica
/// row recorded for it.
#[derive(Debug, Clone)]
pub(crate) struct ChunkRef {
    pub path: String,
    pub chunk_index: u64,
    pub size_bytes: u64,
    /// `(node_id, disk_id)` of each replica row.
    pub locs: Vec<(u64, u32)>,
}

/// `chunk_hash -> refs` for a bounded set of hashes, built in ONE pass over
/// the file table (instead of one pass per migration task).
#[derive(Debug, Default)]
pub(crate) struct ChunkIndex {
    map: HashMap<String, Vec<ChunkRef>>,
}

impl ChunkIndex {
    pub(crate) fn build(
        files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
        wanted: &std::collections::HashSet<String>,
    ) -> Self {
        let mut map: HashMap<String, Vec<ChunkRef>> =
            wanted.iter().map(|h| (h.clone(), Vec::new())).collect();
        for (path, fm) in files {
            for c in &fm.chunks {
                let Some(refs) = map.get_mut(&c.chunk_hash) else { continue };
                let loc = c.node_id.parse::<u64>().ok().map(|n| (n, c.disk_id));
                match refs
                    .iter_mut()
                    .find(|r| r.chunk_index == c.chunk_index && r.path == *path)
                {
                    Some(r) => {
                        r.size_bytes = r.size_bytes.max(c.size_bytes);
                        r.locs.extend(loc);
                    }
                    None => refs.push(ChunkRef {
                        path: path.clone(),
                        chunk_index: c.chunk_index,
                        size_bytes: c.size_bytes,
                        locs: loc.into_iter().collect(),
                    }),
                }
            }
        }
        Self { map }
    }

    /// `None` = hash was not indexed; `Some(empty)` = indexed, unreferenced.
    pub(crate) fn get(&self, hash: &str) -> Option<&Vec<ChunkRef>> {
        self.map.get(hash)
    }
}

/// Chunk size for the RPC deadline: largest recorded row size, `None` iff no
/// file references the chunk (orphan).
pub(crate) fn chunk_bytes_from_refs(refs: &[ChunkRef]) -> Option<u64> {
    refs.iter().map(|r| r.size_bytes).max()
}

/// Pick the source replica: the node must be Healthy and the row's disk live
/// (`live_replica_tier`); `preferred` first, else first eligible in row
/// order; `exclude` never.
pub(crate) fn pick_healthy_source(
    locs: impl Iterator<Item = (u64, u32)>,
    preferred: Option<u64>,
    exclude: Option<u64>,
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
) -> Option<u64> {
    let eligible: Vec<u64> = locs
        .filter(|(n, d)| {
            Some(*n) != exclude
                && nodes.get(n).is_some_and(|i| {
                    i.health == HealthStatus::Healthy && live_replica_tier(Some(i), *d).is_some()
                })
        })
        .map(|(n, _)| n)
        .collect();
    preferred
        .filter(|p| eligible.contains(p))
        .or_else(|| eligible.first().copied())
}

/// Disk to record for a freshly copied replica. `landed` (reported by the
/// data node) wins. Without it the disk is only inferred when the target has
/// exactly ONE live disk of `tier`: the data node picks by free space, so
/// guessing among several would register the wrong disk and the orphan
/// reaper would later delete the real bytes.
pub(crate) fn pick_registration_disk(
    node: Option<&dtfs_raft::NodeInfo>,
    tier: DiskTier,
    landed: Option<u32>,
) -> Result<u32, String> {
    let Some(node) = node else {
        return Err("target node not registered".to_string());
    };
    if let Some(d) = landed {
        return Ok(d);
    }
    let live: Vec<u32> = node
        .disks
        .iter()
        .filter(|d| d.tier == tier && !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed))
        .map(|d| d.disk_id)
        .collect();
    match live.as_slice() {
        [one] => Ok(*one),
        [] => Err(format!("no live disk of tier {tier:?} on target node")),
        many => Err(format!(
            "data node did not report the landing disk and {} live {tier:?} disks exist; refusing to guess",
            many.len()
        )),
    }
}

/// Rows to ADD for a new replica `(target_node, disk)`: one per
/// (file, chunk_index) that does not already carry it, grouped per file.
/// Existing rows are never re-sent.
pub(crate) fn plan_new_replica_rows(
    refs: &[ChunkRef],
    chunk_hash: &str,
    target_node: u64,
    disk: u32,
) -> Vec<(String, Vec<ChunkLocationEntry>)> {
    let mut out: Vec<(String, Vec<ChunkLocationEntry>)> = Vec::new();
    for r in refs.iter().filter(|r| !r.locs.contains(&(target_node, disk))) {
        let row = ChunkLocationEntry {
            chunk_index: r.chunk_index,
            chunk_hash: chunk_hash.to_string(),
            node_id: target_node.to_string(),
            disk_id: disk,
            size_bytes: r.size_bytes,
        };
        match out.iter_mut().find(|(p, _)| *p == r.path) {
            Some((_, rows)) => rows.push(row),
            None => out.push((r.path.clone(), vec![row])),
        }
    }
    out
}

/// Does `path` fall under the directory prefix `prefix`? Component-wise:
/// "/media/tv" matches "/media/tv" and "/media/tv/x" but not "/media/tv-old".
/// A trailing "/" is ignored and "/" (or "") is the whole tree.
pub(crate) fn path_matches_prefix(path: &str, prefix: &str) -> bool {
    let p = prefix.trim_end_matches('/');
    if p.is_empty() {
        return true;
    }
    path.strip_prefix(p)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

// ---------------------------------------------------------------------------
// Bug AV — flush_cache_for_path pure-planner kernel
// ---------------------------------------------------------------------------

/// A single replication operation produced by [`plan_path_migration`]. The
/// planner is pure — it does not touch Raft, so it's testable. The async
/// `schedule_path_migration` wrapper feeds these into `schedule_replication`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedReplication {
    pub chunk_hash: String,
    pub source_node: u64,
    pub target_node: u64,
    pub tier: DiskTier,
}

/// Pure planning kernel: given the matching files + a node snapshot, decide
/// which replications need to run to satisfy the requested tier/DC. Returns
/// the report counters plus the ordered list of (chunk_hash, src, tgt) tasks
/// to schedule.
///
/// Extracted so it can be unit-tested without touching Raft. See the tests
/// at the bottom of this file.
pub(crate) fn plan_path_migration(
    matching_files: &[(String, Vec<ChunkLocationEntry>)],
    nodes: &HashMap<u64, NodeSummary>,
    target_tier: DiskTier,
    target_dc: Option<&str>,
    max_tasks: usize,
) -> (PathMigrationReport, Vec<PlannedReplication>) {
    let mut report = PathMigrationReport {
        files_matched: matching_files.len(),
        ..Default::default()
    };
    let mut plan: Vec<PlannedReplication> = Vec::new();

    // Track per-chunk targets we've committed to so two chunks with the
    // same hash and chunks within the same file don't pile onto one node.
    let mut already_targeted: std::collections::HashSet<(String, u64)> = Default::default();

    for (path, chunks) in matching_files {
        // Group replicas by chunk_index so each logical chunk is
        // considered exactly once.
        let mut by_index: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
            Default::default();
        for c in chunks {
            by_index.entry(c.chunk_index).or_default().push(c);
        }

        for (chunk_index, replicas) in by_index {
            report.chunks_total += 1;

            // Stop scheduling once the per-call budget is exhausted. We
            // still keep walking so truncated_chunks is accurate.
            if plan.len() >= max_tasks {
                report.truncated_chunks += 1;
                continue;
            }

            let chunk_hash = match replicas.first() {
                Some(r) => r.chunk_hash.clone(),
                None => continue,
            };

            // Already satisfied?
            let satisfied = replicas.iter().any(|r| {
                let nid: u64 = r.node_id.parse().unwrap_or(0);
                nodes
                    .get(&nid)
                    .map(|n| {
                        n.tiers.contains(&target_tier)
                            && target_dc
                                .map(|dc| n.datacenter == dc)
                                .unwrap_or(true)
                    })
                    .unwrap_or(false)
            });
            if satisfied {
                report.already_satisfied += 1;
                continue;
            }

            // Pick a healthy source from the existing replicas.
            let existing_nids: std::collections::HashSet<u64> = replicas
                .iter()
                .filter_map(|r| r.node_id.parse::<u64>().ok())
                .collect();
            let source_node: Option<u64> = replicas
                .iter()
                .filter_map(|r| r.node_id.parse::<u64>().ok())
                .find(|nid| nodes.get(nid).map(|n| n.healthy).unwrap_or(false));
            let Some(src) = source_node else {
                report.no_source += 1;
                report.note_example(path, chunk_index, "no_source");
                continue;
            };

            // Pick a healthy target satisfying tier + (optional) DC, not
            // already holding this chunk + not already targeted earlier.
            // Iterate by node_id ASC so plans are deterministic in tests.
            let mut candidates: Vec<u64> = nodes
                .iter()
                .filter(|(nid, n)| {
                    n.healthy
                        && n.tiers.contains(&target_tier)
                        && target_dc
                            .map(|dc| n.datacenter == dc)
                            .unwrap_or(true)
                        && !existing_nids.contains(nid)
                        && !already_targeted.contains(&(chunk_hash.clone(), **nid))
                })
                .map(|(nid, _)| *nid)
                .collect();
            candidates.sort_unstable();
            let target_node = candidates.into_iter().next();
            let Some(tgt) = target_node else {
                report.no_target += 1;
                report.note_example(path, chunk_index, "no_target");
                continue;
            };

            already_targeted.insert((chunk_hash.clone(), tgt));
            plan.push(PlannedReplication {
                chunk_hash,
                source_node: src,
                target_node: tgt,
                tier: target_tier,
            });
        }
    }

    // tasks_scheduled is set after dispatch; planner just reports the
    // length of the plan it produced.
    report.tasks_scheduled = plan.len();

    (report, plan)
}

/// Per-call summary of a manual `schedule_path_migration` invocation.
///
/// Returned over the REST API so the WebUI can show "what just happened"
/// without polling the migration list immediately.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PathMigrationReport {
    pub files_matched: usize,
    pub chunks_total: usize,
    pub already_satisfied: usize,
    pub tasks_scheduled: usize,
    pub no_source: usize,
    pub no_target: usize,
    /// Chunks past `max_tasks` we walked but did not schedule. The next call
    /// with the same prefix will pick them up.
    pub truncated_chunks: usize,
    /// The scheduler tick held the migration state, nothing was scheduled;
    /// the caller should retry (REST: 503).
    #[serde(default)]
    pub busy: bool,
    /// Up to 5 illustrative entries so the operator can see *which* paths
    /// hit `no_source` / `no_target` without dumping the full list.
    pub examples: Vec<PathMigrationExample>,
}

impl PathMigrationReport {
    pub(super) fn note_example(&mut self, path: &str, chunk_index: u64, reason: &str) {
        if self.examples.len() >= 5 {
            return;
        }
        self.examples.push(PathMigrationExample {
            path: path.to_string(),
            chunk_index,
            reason: reason.to_string(),
        });
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathMigrationExample {
    pub path: String,
    pub chunk_index: u64,
    pub reason: String,
}

/// Per-node summary used by the path-migration planner. Holds just enough
/// info to score a node for tier+DC fit without re-locking the state machine
/// for each candidate.
pub(crate) struct NodeSummary {
    pub grpc_addr: String,
    pub datacenter: String,
    pub healthy: bool,
    pub tiers: Vec<DiskTier>,
}

/// Bug AW — split out the migration-snapshot sort comparator so we can
/// unit-test it without spinning up a scheduler. Sorts in place by
/// `created_at_ms` DESC (newest first, immutable after insertion)
/// then by natural-numeric `task_id` ASC ("repl-2" before "repl-10").
pub(super) fn sort_migration_snapshot(out: &mut Vec<serde_json::Value>) {
    out.sort_by(|a, b| {
        let bts = b
            .get("created_at_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let ats = a
            .get("created_at_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        bts.cmp(&ats).then_with(|| {
            let aid = a.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            let bid = b.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
            natural_task_id_cmp(aid, bid)
        })
    });
}

/// Compare two task IDs like "repl-<N>" by their trailing integer when
/// both parse, otherwise fall back to lexicographic. Keeps a burst of
/// 10+ tasks sorted as repl-1, repl-2, … repl-10 instead of
/// repl-1, repl-10, repl-2, … (the prior lex compare).
pub(super) fn natural_task_id_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (trailing_u64(a), trailing_u64(b)) {
        (Some(an), Some(bn)) => an.cmp(&bn).then_with(|| a.cmp(b)),
        _ => a.cmp(b),
    }
}

pub(super) fn trailing_u64(s: &str) -> Option<u64> {
    let digits: String = s.chars().rev().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return None;
    }
    digits.chars().rev().collect::<String>().parse::<u64>().ok()
}

// ---------------------------------------------------------------------------
// CO-1-3 — Raft-remove-FIRST de-location ordering tests.
//
// `delocate_replica` is the single ordered expression of the LOCKED-IN
// contract used by BOTH the scheduler cache-flush dispatch and the tiering
// demote dispatch. A recording mock asserts: (a) the Raft RemoveChunkLocation
// runs before any byte delete; (b) on a forced Raft-write failure NO byte
// delete is issued (no phantom replica); (c) a post-commit byte-delete
// failure is reported as RemovedBytesLeftover (the SM entry is already gone;
// the CO-3-2 orphan reaper reclaims the leftover bytes) and never rolls the
// Raft remove back.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// U3 — advance_migration_tasks loop + bounded-parallel-copy tests.
//
// advance_tasks made real gRPC calls and needed a live Raft handle, so it
// had no unit coverage. U3 moved the driving logic into the generic
// `advance_migration_tasks` over the `MigrationOps` trait; a mock impl lets
// us assert (a) a chunk climbs Pending -> Done within ONE call (not one
// stage per call), and (b) the Copying RPCs are dispatched with bounded
// concurrency.
// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// CO-3-5 — migration-state load-modify-save mutual exclusion.
//
// `schedule_replication` and `tick` both load the migration state, mutate it
// in memory, then save it back. The live code stores the state in Raft and
// needs a leader handle, so the production methods can't be unit-driven here;
// instead this models the exact load->modify->save critical section against a
// shared in-memory `MigrationState` and proves that serialising it under the
// scheduler's `tokio::Mutex` (the field added by this fix) prevents the
// lost-update race — a concurrent schedule during a tick would otherwise
// clobber a task and regress `next_task_id`.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod sched_audit_tests {
    use super::*;
    use dtfs_common::types::{DiskInfo, NodeRole};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    fn disk(id: u32, tier: DiskTier, status: DiskStatus) -> DiskInfo {
        DiskInfo {
            disk_id: id,
            uuid: String::new(),
            mount_path: String::new(),
            tier,
            total_bytes: 0,
            used_bytes: 0,
            status,
        }
    }

    fn node(health: HealthStatus, disks: Vec<DiskInfo>) -> dtfs_raft::NodeInfo {
        dtfs_raft::NodeInfo {
            hostname: String::new(),
            grpc_addr: "http://n:9500".to_string(),
            role: NodeRole::Data,
            disks,
            location: dtfs_common::topology::Location::new("", "", "", ""),
            health,
            total_capacity: 0,
            used_capacity: 0,
            last_seen_ms: 0,
            total_bytes_read: 0,
            total_bytes_written: 0,
            client_ip: None,
            version: String::new(),
            build_date: String::new(),
            quarantine: None,
        }
    }

    fn row(idx: u64, hash: &str, node: &str, disk: u32) -> ChunkLocationEntry {
        ChunkLocationEntry {
            chunk_index: idx,
            chunk_hash: hash.to_string(),
            node_id: node.to_string(),
            disk_id: disk,
            size_bytes: 1024,
        }
    }

    fn file(chunks: Vec<ChunkLocationEntry>) -> dtfs_raft::state_machine::FileMetadata {
        dtfs_raft::state_machine::FileMetadata {
            path: "/f".to_string(),
            volume_id: "default".to_string(),
            chunk_size: 4 * 1024 * 1024,
            chunks,
            created_at_ms: 0,
            updated_at_ms: 0,
            symlink_target: None,
            logical_size: None,
            is_directory: false,
            cache_cooldown_until_ms: std::collections::BTreeMap::new(),
        }
    }

    // ---- task 7: prefix matching ----
    #[test]
    fn prefix_is_component_wise() {
        assert!(path_matches_prefix("/media/tv", "/media/tv"));
        assert!(path_matches_prefix("/media/tv/s1/e1.mkv", "/media/tv"));
        assert!(path_matches_prefix("/media/tv/s1/e1.mkv", "/media/tv/"));
        assert!(!path_matches_prefix("/media/tv-old/x", "/media/tv"));
        assert!(!path_matches_prefix("/media/tv-old/x", "/media/tv/"));
        assert!(!path_matches_prefix("/media", "/media/tv"));
    }

    #[test]
    fn root_prefix_is_whole_tree() {
        assert!(path_matches_prefix("/a/b", "/"));
        assert!(path_matches_prefix("/a", ""));
    }

    // ---- task 2a: registration disk ----
    #[test]
    fn reported_landing_disk_wins_over_guess() {
        let n = node(
            HealthStatus::Healthy,
            vec![disk(1, DiskTier::Warm, DiskStatus::Healthy), disk(2, DiskTier::Warm, DiskStatus::Healthy)],
        );
        assert_eq!(pick_registration_disk(Some(&n), DiskTier::Warm, Some(2)), Ok(2));
    }

    #[test]
    fn unreported_disk_inferred_only_when_unique() {
        let one = node(
            HealthStatus::Healthy,
            vec![
                disk(1, DiskTier::Warm, DiskStatus::Failed),
                disk(2, DiskTier::Warm, DiskStatus::Healthy),
                disk(3, DiskTier::Cold, DiskStatus::Healthy),
            ],
        );
        assert_eq!(pick_registration_disk(Some(&one), DiskTier::Warm, None), Ok(2));

        let two = node(
            HealthStatus::Healthy,
            vec![disk(1, DiskTier::Warm, DiskStatus::Healthy), disk(2, DiskTier::Warm, DiskStatus::Healthy)],
        );
        let err = pick_registration_disk(Some(&two), DiskTier::Warm, None).unwrap_err();
        assert!(err.contains("refusing to guess"), "{err}");

        let zero = node(HealthStatus::Healthy, vec![disk(3, DiskTier::Cold, DiskStatus::Healthy)]);
        let err = pick_registration_disk(Some(&zero), DiskTier::Warm, None).unwrap_err();
        assert!(err.contains("no live disk"), "{err}");
        assert!(pick_registration_disk(None, DiskTier::Warm, Some(1)).is_err());
    }

    // ---- task 2b: only NEW rows are proposed ----
    #[test]
    fn registration_sends_only_the_new_row() {
        let mut files = std::collections::BTreeMap::new();
        files.insert("/f".to_string(), file(vec![row(0, "h", "1", 0), row(0, "h", "2", 5)]));
        let wanted = ["h".to_string()].into();
        let idx = ChunkIndex::build(&files, &wanted);
        let refs = idx.get("h").unwrap();
        let writes = plan_new_replica_rows(refs, "h", 9, 3);
        assert_eq!(writes.len(), 1);
        let (path, rows) = &writes[0];
        assert_eq!(path, "/f");
        assert_eq!(rows.len(), 1, "existing rows must not be re-proposed");
        assert_eq!((rows[0].node_id.as_str(), rows[0].disk_id, rows[0].chunk_index), ("9", 3, 0));
        assert_eq!(rows[0].size_bytes, 1024);
    }

    #[test]
    fn registration_skips_already_present_and_same_node_other_disk_is_new() {
        let mut files = std::collections::BTreeMap::new();
        files.insert("/f".to_string(), file(vec![row(0, "h", "9", 3)]));
        let wanted = ["h".to_string()].into();
        let idx = ChunkIndex::build(&files, &wanted);
        let refs = idx.get("h").unwrap();
        assert!(plan_new_replica_rows(refs, "h", 9, 3).is_empty());
        assert_eq!(plan_new_replica_rows(refs, "h", 9, 4).len(), 1);
    }

    // ---- task 6: one-pass index ----
    #[test]
    fn index_groups_rows_and_distinguishes_unreferenced_from_unindexed() {
        let mut files = std::collections::BTreeMap::new();
        files.insert("/a".to_string(), file(vec![row(0, "h", "1", 0), row(0, "h", "2", 0), row(1, "other", "1", 0)]));
        files.insert("/b".to_string(), file(vec![row(4, "h", "1", 0)]));
        let wanted: std::collections::HashSet<String> = ["h".to_string(), "gone".to_string()].into();
        let idx = ChunkIndex::build(&files, &wanted);
        let refs = idx.get("h").unwrap();
        assert_eq!(refs.len(), 2);
        assert_eq!(refs.iter().find(|r| r.path == "/a").unwrap().locs, vec![(1, 0), (2, 0)]);
        assert_eq!(chunk_bytes_from_refs(refs), Some(1024));
        assert_eq!(chunk_bytes_from_refs(idx.get("gone").unwrap()), None);
        assert!(idx.get("other").is_none());
    }

    // ---- task 5: source choice ----
    fn nodes_for_source() -> std::collections::BTreeMap<u64, dtfs_raft::NodeInfo> {
        let mut m = std::collections::BTreeMap::new();
        // node 1 Healthy but its disk is Failed.
        m.insert(1, node(HealthStatus::Healthy, vec![disk(0, DiskTier::Cold, DiskStatus::Failed)]));
        m.insert(2, node(HealthStatus::Healthy, vec![disk(0, DiskTier::Cold, DiskStatus::Healthy)]));
        m.insert(3, node(HealthStatus::Healthy, vec![disk(0, DiskTier::Cold, DiskStatus::Healthy)]));
        m.insert(4, node(HealthStatus::Down, vec![disk(0, DiskTier::Cold, DiskStatus::Healthy)]));
        m
    }

    #[test]
    fn source_skips_failed_disk_and_down_node() {
        let nodes = nodes_for_source();
        let locs = [(1u64, 0u32), (4, 0), (2, 0)];
        assert_eq!(pick_healthy_source(locs.into_iter(), None, None, &nodes), Some(2));
    }

    #[test]
    fn source_prefers_recorded_source_only_when_live() {
        let nodes = nodes_for_source();
        let locs = [(2u64, 0u32), (3, 0)];
        assert_eq!(pick_healthy_source(locs.into_iter(), Some(3), None, &nodes), Some(3));
        // Preferred node's only disk is Failed -> falls back to first eligible.
        let locs = [(2u64, 0u32), (1, 0)];
        assert_eq!(pick_healthy_source(locs.into_iter(), Some(1), None, &nodes), Some(2));
    }

    #[test]
    fn source_honours_exclude_and_can_be_none() {
        let nodes = nodes_for_source();
        let locs = [(2u64, 0u32)];
        assert_eq!(pick_healthy_source(locs.into_iter(), None, Some(2), &nodes), None);
        let locs = [(1u64, 0u32), (4, 0)];
        assert_eq!(pick_healthy_source(locs.into_iter(), None, None, &nodes), None);
    }

    // ---- tasks 2a/4: round cap, tick budget, landed disk plumbing ----
    #[derive(Default)]
    struct Ops {
        replicate_calls: AtomicUsize,
        saves: AtomicUsize,
        landed: Option<u32>,
        registered_with: Mutex<Vec<Option<u32>>>,
        preferred_seen: Mutex<Vec<u64>>,
    }

    impl MigrationOps for Ops {
        async fn validate_target(&self, _t: u64, _tier: DiskTier) -> Result<(), String> {
            Ok(())
        }
        async fn chunk_bytes(&self, _c: &str) -> Option<u64> {
            Some(4 << 20)
        }
        async fn resolve_source(&self, _c: &str, _ex: Option<u64>) -> Option<(u64, String)> {
            Some((1, "http://src:9500".to_string()))
        }
        async fn resolve_source_preferring(
            &self,
            c: &str,
            ex: Option<u64>,
            preferred: u64,
        ) -> Option<(u64, String)> {
            self.preferred_seen.lock().unwrap().push(preferred);
            self.resolve_source(c, ex).await
        }
        async fn target_addr(&self, n: u64) -> Option<String> {
            Some(format!("http://tgt{n}:9500"))
        }
        async fn replicate(&self, _s: &str, _c: &str, _t: &str, _tier: &str, _b: u64) -> Result<(), String> {
            Ok(())
        }
        async fn replicate_landed(
            &self,
            _s: &str,
            _c: &str,
            _t: &str,
            _tier: &str,
            _b: u64,
        ) -> Result<Option<u32>, String> {
            self.replicate_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.landed)
        }
        async fn verify(&self, _t: &str, _c: &str) -> Result<bool, String> {
            Ok(true)
        }
        async fn register_replica(&self, _c: &str, _t: u64, _tier: DiskTier) -> Result<(), String> {
            Ok(())
        }
        async fn register_replica_on(
            &self,
            _c: &str,
            _t: u64,
            _tier: DiskTier,
            landed: Option<u32>,
        ) -> Result<(), String> {
            self.registered_with.lock().unwrap().push(landed);
            Ok(())
        }
        async fn save_state(&self, _s: &MigrationState) {
            self.saves.fetch_add(1, Ordering::SeqCst);
        }
        fn audit(&self, _a: &str, _d: &str, _o: &str) {}
        fn fire_migration_failed(&self, _t: &ReplicationTask, _r: &str) {}
    }

    fn state_of(n: usize) -> MigrationState {
        let mut s = MigrationState::default();
        for i in 0..n {
            s.tasks.insert(
                format!("repl-{i}"),
                ReplicationTask {
                    task_id: format!("repl-{i}"),
                    chunk_id: format!("chunk-{i}"),
                    source_node: 1,
                    target_node: 2,
                    tier: DiskTier::Warm,
                    status: MigrationStatus::Pending,
                    created_at_ms: 0,
                    updated_at_ms: 0,
                    attempts: 0,
                    last_error: None,
                },
            );
        }
        s
    }

    #[tokio::test]
    async fn one_round_dispatches_at_most_the_cap() {
        let ops = Ops::default();
        let mut state = state_of(MIGRATION_MAX_JOBS_PER_ROUND + 6);
        let (mut copied, mut verified) = (Default::default(), Default::default());
        advance_round(&ops, &mut state, 1, &mut copied, &mut verified).await;
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), MIGRATION_MAX_JOBS_PER_ROUND);
        let still_copying =
            state.tasks.values().filter(|t| t.status == MigrationStatus::Copying).count();
        assert_eq!(still_copying, 6, "tasks past the cap wait for the next round");
        // The next round picks them up.
        advance_round(&ops, &mut state, 2, &mut copied, &mut verified).await;
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), MIGRATION_MAX_JOBS_PER_ROUND + 6);
    }

    #[tokio::test]
    async fn spent_budget_stops_after_first_round_and_saves_per_round() {
        let ops = Ops::default();
        let mut state = state_of(MIGRATION_MAX_JOBS_PER_ROUND * 2 + 1);
        advance_migration_tasks_budgeted(&ops, &mut state, 1, Duration::ZERO).await;
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), MIGRATION_MAX_JOBS_PER_ROUND);
        assert_eq!(ops.saves.load(Ordering::SeqCst), 1);

        let ops = Ops::default();
        let mut state = state_of(MIGRATION_MAX_JOBS_PER_ROUND * 2 + 1);
        advance_migration_tasks_budgeted(&ops, &mut state, 1, Duration::from_secs(3600)).await;
        assert_eq!(ops.replicate_calls.load(Ordering::SeqCst), MIGRATION_MAX_JOBS_PER_ROUND * 2 + 1);
        assert_eq!(ops.saves.load(Ordering::SeqCst), 3);
        assert!(state.tasks.values().all(|t| t.status == MigrationStatus::Done));
    }

    #[tokio::test]
    async fn landed_disk_reaches_registration_and_is_pruned() {
        let ops = Ops { landed: Some(7), ..Default::default() };
        let mut state = state_of(1);
        advance_migration_tasks(&ops, &mut state, 1).await;
        assert_eq!(*ops.registered_with.lock().unwrap(), vec![Some(7)]);
        assert!(state.landed_disks.is_empty(), "terminal task must not keep a landed disk");
        assert_eq!(*ops.preferred_seen.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn old_data_node_registers_with_no_landed_disk() {
        let ops = Ops::default();
        let mut state = state_of(1);
        advance_migration_tasks(&ops, &mut state, 1).await;
        assert_eq!(*ops.registered_with.lock().unwrap(), vec![None]);
    }
}
