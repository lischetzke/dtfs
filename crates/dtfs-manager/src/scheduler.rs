//! Background task scheduler — Phase 4 (Resilience).
//!
//! Drives periodic and on-demand background tasks such as re-replication of
//! under-replicated chunks, tier migration, and chunk redistribution across
//! the cluster.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dtfs_common::types::{DiskStatus, DiskTier, HealthStatus, MigrationStatus, NodeRole, TierReplication};
use dtfs_raft::state_machine::StateMachineData;
use dtfs_raft::{ChunkLocationEntry, ClientRequest, DtfsRaft};
use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::{ReplicateChunkRequest, VerifyChunkRequest};
// Bug AV — DeleteChunkRequest used by `delete_chunk_replica_on_node`. The
// proto's existing `DeleteChunk` RPC is the dispatcher target; AV-1-5
// extended its response shape with `was_present` + `freed_bytes`.
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::audit::AuditLog;

/// Interval between scheduler ticks.
const SCHEDULER_TICK: Duration = Duration::from_secs(30);

/// Maximum number of consecutive failures (per task per state) before the task
/// is marked Failed. With a 30s tick, 6 attempts ≈ 3 minutes of trying. After
/// exhaustion the task is moved to Failed and an audit entry recorded (P0-4).
const MAX_MIGRATION_ATTEMPTS: u32 = 6;

/// U3 — bound on concurrent `ReplicateChunk` RPCs dispatched per
/// `advance_tasks` round. Pre-U3 the Copying branch awaited each
/// `replicate_chunk` inline and serially, so a large backlog (e.g. 1195
/// chunks @ ~1.6s each) spent a whole ~30 min pass just copying and only
/// verified/completed on the NEXT pass — warm coverage stayed flat then
/// batch-jumped. We now drive a chunk through Pending -> Copying ->
/// Verifying -> Completing -> Done within a SINGLE tick (round loop) and
/// fan the per-task copies out `buffer_unordered`-style up to this cap.
/// Kept deliberately small: each copy is a streaming gRPC that competes
/// with foreground put_chunk traffic on the source data node (Bug AF), and
/// the target archive link is the asymmetric-bandwidth bottleneck (LELI
/// ≤40 Mbps, § A.1/A.7). 6 in-flight copies saturate the slow links
/// without starving foreground reads/writes. Verify/Completing stay serial
/// (cheap, and Completing writes Raft).
const MIGRATION_COPY_CONCURRENCY: usize = 6;

/// WI-04 — max concurrent `ReplicateChunk` RPCs aimed at ONE target node
/// address, on top of the global `MIGRATION_COPY_CONCURRENCY` cap. Anchor
/// repair sends every copy to the single LELI `data-primary01` (A.3.4), so
/// without a per-target cap the global cap of 6 fair-shares one 40 Mbps
/// link six ways and no single copy ever finishes before the (now
/// size-scaled) RPC deadline. 2 keeps each copy at ~2.5 MB/s, a 2.5x margin
/// over the 1 MiB/s budget `dtfs_common::replication` assumes.
const MIGRATION_COPIES_PER_TARGET: usize = 2;

/// U3 — hard cap on `advance_tasks` rounds within one tick. The round loop
/// normally terminates when a pass makes no progress; this cap is a
/// belt-and-braces guard against an unforeseen oscillation (e.g. a chunk
/// flapping Verifying -> Copying) monopolising the tick. A healthy task
/// needs at most 4 transitions (Pending -> Copying -> Verifying ->
/// Completing -> Done), so 8 rounds clears a steady backlog while bounding
/// the worst case.
const MAX_ADVANCE_ROUNDS: usize = 8;

/// Bug CN — hard wall-clock cap on a VerifyChunk RPC from the scheduler to a
/// data node. WITHOUT this the orchestrating RPC could block forever when a
/// target data node stalls (cross-DC flaky link, swap-thrashing node):
/// `buffer_unordered` then never drains, the advance round never finishes,
/// the tick never reaches `save`, and the whole scheduler loop hangs after
/// one leader-onset pass (prod 2026-06-10 — the 2439-task backlog wedged the
/// scheduler this way). On timeout the attempt is treated as failed and
/// retried next tick, so the loop keeps breathing.
///
/// WI-04 / B4 — VerifyChunk only. `ReplicateChunk` moved off this fixed
/// constant onto `dtfs_common::replication::migration_rpc_timeout(chunk_bytes)`
/// = 60s at the 4 MB default chunk, scaling at 1 MiB/s to a hard 300s
/// ceiling — always strictly above the data node's own inner
/// `replicate_transfer_timeout(chunk_bytes)` (30s floor, 270s ceiling) for
/// the same copy, so the callee always gives up first. VerifyChunk
/// re-hashes a file already local to the target — no chunk bytes cross the
/// wire — so it does not scale with the link and stays on this fixed,
/// generous (60s) cap.
const MIGRATION_RPC_TIMEOUT: Duration = Duration::from_secs(60);

/// A replication task tracked by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationTask {
    pub task_id: String,
    pub chunk_id: String,
    pub source_node: u64,
    pub target_node: u64,
    pub tier: DiskTier,
    pub status: MigrationStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    /// Number of consecutive failed attempts in the current state (P0-4).
    /// Reset to 0 on each successful state transition; once it hits
    /// `MAX_MIGRATION_ATTEMPTS` the task is marked `Failed`.
    /// `#[serde(default)]` keeps existing snapshots loadable.
    #[serde(default)]
    pub attempts: u32,
    /// Last error message — kept for audit logging on failure.
    #[serde(default)]
    pub last_error: Option<String>,
}

/// Serializable collection of active migration tasks, stored in Raft state
/// via `ClientRequest::UpdateConfig`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct MigrationState {
    tasks: HashMap<String, ReplicationTask>,
    next_task_id: u64,
}

/// Background task scheduler that drives re-replication and migration.
pub struct TaskScheduler {
    raft: Arc<DtfsRaft>,
    state_data: Arc<RwLock<StateMachineData>>,
    audit: Arc<AuditLog>,
    /// P1-8: optional alert dispatcher. MigrationFailed fires here.
    alerts: parking_lot::RwLock<Option<Arc<crate::alerts::AlertDispatcher>>>,
    /// CF-5: optional memory budget. When set and RSS reaches the Warn
    /// pressure level, the scheduler skips its background ticks
    /// (re-replication / migration / rebalance) so the manager gives RAM
    /// back instead of being OOM/cgroup-killed. None = no cap configured
    /// (the common case) and ticks always run.
    memory: parking_lot::RwLock<Option<Arc<dtfs_common::memory::MemoryBudget>>>,
    /// CNF-2 — terminal-task retention windows for the Bug AE prune,
    /// from `raft.done_task_retention_secs` / `raft.failed_task_retention_secs`.
    done_task_retention_ms: u64,
    failed_task_retention_ms: u64,
    /// CF-6 — resolved `[tls]` context; data-node RPCs speak mTLS when
    /// enabled, plaintext otherwise.
    tls: dtfs_common::tls::TlsContext,
    /// CO-3-5 — serialises every migration-state read-modify-write so a
    /// `schedule_replication` call landing concurrently with a `tick`
    /// cannot clobber a task or regress `next_task_id`. Both load the
    /// state from Raft, mutate it in memory, and save it back; without
    /// mutual exclusion the second save overwrites the first writer's
    /// mutations (lost task / `next_task_id` regression). Held only across
    /// the load..save critical section, never across an unrelated await.
    migration_state_lock: tokio::sync::Mutex<()>,
    /// LIS-19 — cluster shared secret for DeleteChunk auth stamps.
    shared_secret: parking_lot::RwLock<String>,
    /// CR-3-5/MGR-5 — placement capacity policy, so the under-replication
    /// backfill and anchor-lag repair only target a disk with room
    /// (`has_room`), not just one of the right tier. Defaults to
    /// `PlacementConfig::default()`; `set_placement_cfg` (main.rs) wires
    /// the operator's `[placement]` config after construction.
    placement_cfg: dtfs_common::config::PlacementConfig,
}

impl TaskScheduler {
    /// Create a new task scheduler.
    pub fn new(
        raft: Arc<DtfsRaft>,
        state_data: Arc<RwLock<StateMachineData>>,
        audit: Arc<AuditLog>,
        done_task_retention_secs: u64,
        failed_task_retention_secs: u64,
        tls: dtfs_common::tls::TlsContext,
        placement_cfg: dtfs_common::config::PlacementConfig,
    ) -> Self {
        Self {
            raft,
            state_data,
            audit,
            alerts: parking_lot::RwLock::new(None),
            memory: parking_lot::RwLock::new(None),
            // saturating_mul: an absurdly large configured value means
            // "effectively never prune", not a silent wrap to ~0.
            done_task_retention_ms: done_task_retention_secs.saturating_mul(1000),
            failed_task_retention_ms: failed_task_retention_secs.saturating_mul(1000),
            tls,
            migration_state_lock: tokio::sync::Mutex::new(()),
            shared_secret: parking_lot::RwLock::new(String::new()),
            placement_cfg,
        }
    }

    /// Inject the alert dispatcher (called after main.rs has wired one).
    pub fn set_alerts(&self, alerts: Arc<crate::alerts::AlertDispatcher>) {
        *self.alerts.write() = Some(alerts);
    }

    /// LIS-19 — inject the cluster shared secret for DeleteChunk stamps.
    pub fn set_shared_secret(&self, secret: String) {
        *self.shared_secret.write() = secret;
    }

    /// CF-5 — inject the memory budget so the scheduler can skip background
    /// ticks under memory pressure (called after main.rs builds the budget).
    pub fn set_memory_budget(&self, budget: Arc<dtfs_common::memory::MemoryBudget>) {
        *self.memory.write() = Some(budget);
    }

    /// CF-5 — true when a memory budget is configured and RSS has reached the
    /// Warn pressure level. The scheduler's background loop checks this before
    /// each tick and skips the work to give RAM back (the cheapest ladder rung
    /// for the manager: re-replication/migration scans hold the largest
    /// transient state). Returns false when no cap is configured.
    ///
    /// REV-1-1 — true when this manager is the current raft leader.
    ///
    /// Bug AC leader-gated `tick()` only; every other background loop
    /// (orphan scrub, auto-unquarantine, lease GC, and the tiering engine
    /// via `TieringEngine::run`) ran on every manager, including cross-DC
    /// learners. Those loops issue direct data-node delete RPCs, which no
    /// Raft rejection can undo, so a follower's stale view physically
    /// deletes live bytes. All of them now share this gate.
    pub fn is_leader(&self) -> bool {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        leader_gate_open(metrics.current_leader, metrics.id)
    }

    /// REV-1-2 — `(last_applied, last_log_index)` for the freshness gate.
    /// A state machine that has not applied its own log tail is not a safe
    /// basis for deciding that a chunk is unreferenced.
    fn applied_and_log_index(&self) -> (u64, u64) {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        let applied = metrics.last_applied.map(|l| l.index).unwrap_or(0);
        let last_log = metrics.last_log_index.unwrap_or(applied);
        (applied, last_log)
    }

    fn under_memory_pressure(&self) -> bool {
        use dtfs_common::memory::PressureLevel;
        match &*self.memory.read() {
            // REV-3-5 — gate on Shed, not on "anything above Normal".
            // The scheduler is the SOLE owner of mandatory-replica repair
            // (A.3.4), and the memory that keeps a manager at Warn belongs to
            // components this pause does not stop (the state machine, the
            // resident snapshot blob, the tiering maps, the orphan-scrub
            // buffers). Pausing at Warn therefore latched forever on a small
            // manager and durability repair simply never ran again.
            Some(b) => b.pressure_level() == PressureLevel::Shed,
            None => false,
        }
    }

    /// Run the scheduler background loop. Intended to be spawned as a tokio task.
    pub async fn run(&self) {
        info!("Task scheduler started (tick interval: {:?})", SCHEDULER_TICK);

        let mut interval = tokio::time::interval(SCHEDULER_TICK);
        let mut consecutive_pressure_skips: u32 = 0;
        loop {
            interval.tick().await;

            // CF-5 — under memory pressure, skip the tick. Re-replication
            // and migration scans build the largest transient state in the
            // manager; pausing them is the cheapest ladder rung to give RAM
            // back before the systemd cgroup kills us.
            //
            // REV-3-5 — but the pause must be self-limiting. A.5's rule is
            // "slow down, never OOM-kill"; skipping durability repair
            // indefinitely is not slowing down, it is stopping. If RSS never
            // drops (because the memory belongs to something this pause does
            // not release) the old code paused forever and a node loss never
            // healed. Force a tick every MAX_CONSECUTIVE_PRESSURE_SKIPS and
            // make the state visible.
            if self.under_memory_pressure() {
                let skips = consecutive_pressure_skips + 1;
                if force_tick_after_skips(skips) {
                    tracing::error!(
                        skips,
                        "Scheduler running a tick DESPITE memory pressure: \
                         durability repair outranks the soft cap (REV-3-5)"
                    );
                    consecutive_pressure_skips = 0;
                } else {
                    consecutive_pressure_skips = skips;
                    tracing::warn!(
                        skips,
                        "Scheduler tick skipped: under memory pressure (CF-5 ladder)"
                    );
                    continue;
                }
            } else {
                consecutive_pressure_skips = 0;
            }

            if let Err(e) = self.tick().await {
                warn!("Scheduler tick failed: {}", e);
            }
        }
    }

    /// P0-1: lease garbage-collection loop. Sweeps expired write leases
    /// every second by proposing `SweepExpiredLeases` through Raft. Without
    /// this, an orphan lease (FUSE crashed, network partition, kill -9)
    /// blocks all writers to that path until the lease's lease_ms elapses.
    pub async fn run_lease_gc(raft: Arc<DtfsRaft>) {
        info!("Lease GC started (1s tick)");
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            // REV-1-1 — skip on followers instead of proposing a write once a
            // second that can only come back as ForwardToLeader.
            {
                use openraft::async_runtime::watch::WatchReceiver;
                let metrics = raft.metrics().borrow_watched().clone();
                if !leader_gate_open(metrics.current_leader, metrics.id) {
                    continue;
                }
            }
            let now_ms = now_millis();
            if let Err(e) = raft
                .client_write(dtfs_raft::ClientRequest::SweepExpiredLeases { now_ms })
                .await
            {
                // Non-leader managers will get a forward error; that's fine
                // — only the leader needs to drive GC.
                tracing::trace!("SweepExpiredLeases write failed (non-leader?): {e}");
            }
        }
    }

    /// A single scheduler tick: find under-replicated chunks and advance
    /// in-progress migration tasks.
    ///
    /// Bug AC — leader-gated. Followers' Raft writes silently forward to
    /// the leader (or fail entirely when leader is unknown). Running the
    /// scheduler on every manager wastes effort: 3 managers all
    /// independently call `ReplicateChunk` on the source data node for
    /// the same chunk, all observe success, all try to write
    /// UpdateChunkLocations — only the leader's write commits, the
    /// others silently fail. This caused the per-task 3x amplification
    /// during Bug AB diagnosis. Skipping on followers also means a
    /// leader-loss event freezes scheduler work cleanly (no
    /// half-finished state on stale followers) until the next election.
    async fn tick(&self) -> anyhow::Result<()> {
        // Skip on followers — only the current leader drives migrations.
        // REV-1-1: shared with every other background loop via `is_leader()`.
        if !self.is_leader() {
            tracing::trace!("Scheduler tick skipped: not leader");
            return Ok(());
        }

        // CO-3-5 — hold the migration-state lock across the entire
        // load..save lifecycle of this tick so a concurrent
        // `schedule_replication` cannot interleave its own
        // load-modify-save and clobber a task / regress `next_task_id`.
        let _ms_guard = self.migration_state_lock.lock().await;
        let mut migration_state = self.load_migration_state().await;

        // Bug CN — prune terminal tasks FIRST and persist immediately, before
        // the under-replication scan + the (timeout-bounded but possibly slow)
        // advance. A flood that leaves a large terminal backlog (prod
        // 2026-06-10: 2439 Failed tasks) otherwise keeps itself alive: the old
        // prune ran only at the end of `advance_tasks`, so if advance was slow
        // the save never happened and every leader reloaded the same poison.
        // Persisting the prune up front guarantees the backlog drains in one
        // tick even if the rest of the tick stalls.
        let pruned = prune_terminal_tasks(
            &mut migration_state,
            now_millis(),
            self.done_task_retention_ms,
            self.failed_task_retention_ms,
        );
        if pruned > 0 {
            info!(
                pruned,
                remaining = migration_state.tasks.len(),
                "Bug AE/CN: pruned terminal migration tasks (pre-tick)"
            );
            self.save_migration_state(&migration_state).await;
        }

        // 1. Check for under-replicated chunks and schedule new tasks.
        self.check_under_replicated(&mut migration_state).await;

        // 1b. Bug AV (AV-3-3) — check anchor lag and schedule repairs
        // for chunks whose anchor invariant isn't yet satisfied. Used
        // primarily by warm-first writes (AV-3-1/3-2) where the
        // mandatory anchor copy is created asynchronously.
        self.check_anchor_lag(&mut migration_state).await;

        // 1c. Bug BX-11 — rebalance: drop EXCESS spill replicas from
        // over-replicated chunks so a capacity-spilled chunk converges back
        // to goal once the mandated tier has been backfilled (by 1./1b.).
        // Only acts above mandatory_total + cache_allowance and never below a
        // mandated tier / the anchor (pure planner + enforce_anchor on apply),
        // so it can't under-replicate or fight the tiering engine.
        self.check_misplaced_rebalance().await;

        // 2. Advance existing migration tasks through their state machine.
        self.advance_tasks(&mut migration_state).await;

        // 3. Persist the updated migration state back to Raft.
        self.save_migration_state(&migration_state).await;

        Ok(())
    }

    /// Schedule a replication task for a specific chunk.
    ///
    /// Creates a new task in `Pending` state that will be picked up on the
    /// next scheduler tick.
    pub async fn schedule_replication(
        &self,
        chunk_id: &str,
        source_node: u64,
        target_node: u64,
        tier: DiskTier,
    ) {
        // CO-3-5 — serialise with the tick's load-modify-save (and any
        // other concurrent schedule) so neither writer clobbers the
        // other's tasks or regresses `next_task_id`.
        let _ms_guard = self.migration_state_lock.lock().await;
        let mut state = self.load_migration_state().await;

        // Avoid duplicate tasks for the same chunk + target. CR-3-5/MGR-5
        // — `task_blocks_requeue` so a lingering Failed task (within its
        // retention window) also blocks an immediate re-schedule here,
        // matching `check_under_replicated`'s dedup.
        let already_scheduled = state.tasks.values().any(|t| {
            t.chunk_id == chunk_id
                && t.target_node == target_node
                && task_blocks_requeue(t.status)
        });

        if already_scheduled {
            debug!(
                chunk_id,
                target_node, "Replication task already scheduled, skipping"
            );
            return;
        }

        let now_ms = now_millis();
        let task_id = format!("repl-{}", state.next_task_id);
        state.next_task_id += 1;

        let task = ReplicationTask {
            task_id: task_id.clone(),
            chunk_id: chunk_id.to_string(),
            source_node,
            target_node,
            tier,
            status: MigrationStatus::Pending,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            attempts: 0,
            last_error: None,
        };

        info!(
            task_id = %task_id,
            chunk_id = %chunk_id,
            source = source_node,
            target = target_node,
            "Scheduled replication task"
        );

        self.audit.log(
            "scheduler",
            "replication.schedule",
            &format!("chunk={chunk_id} src={source_node} dst={target_node}"),
            "pending",
        );

        state.tasks.insert(task_id, task);
        self.save_migration_state(&state).await;
    }

    /// AV-5-5 — return true when a replication task targeting `chunk_id`
    /// on `target_node` is still in flight (any status except Done /
    /// Failed). The AV-2-5 demote dispatcher consults this to avoid
    /// racing a just-completed PutChunk against a stale planner-emitted
    /// demote (the planner's snapshot can predate the
    /// UpdateChunkLocations Raft write that records the new replica).
    pub(crate) async fn has_active_replication_to(
        &self,
        chunk_id: &str,
        target_node: u64,
    ) -> bool {
        let state = self.load_migration_state().await;
        state.tasks.values().any(|t| {
            t.chunk_id == chunk_id
                && t.target_node == target_node
                && t.status != MigrationStatus::Done
                && t.status != MigrationStatus::Failed
        })
    }

    /// Check the cluster state for under-replicated chunks and schedule
    /// re-replication tasks for them.
    async fn check_under_replicated(&self, migration_state: &mut MigrationState) {
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
        let mut live_tasks = live_index.len();

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
        for (_path, file_meta) in &sm.files {
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
    async fn check_misplaced_rebalance(&self) {
        const MAX_REMOVALS_PER_TICK: usize = 50;

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
                let cache_allow = vol.cache_replicas.hot.max
                    + vol.cache_replicas.warm.max
                    + vol.cache_replicas.cold.max
                    + vol.cache_replicas.tape.max;

                let mut by_idx: std::collections::BTreeMap<u64, Vec<&ChunkLocationEntry>> =
                    std::collections::BTreeMap::new();
                for c in &fm.chunks {
                    by_idx.entry(c.chunk_index).or_default().push(c);
                }
                for (idx, entries) in by_idx {
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
                            // very next tick, forever.
                            let tier = live_replica_tier(Some(info), e.disk_id)?;
                            Some((nid, e.disk_id, info.location.datacenter.clone(), tier))
                        })
                        .collect();
                    if let Some((node, disk_id)) =
                        plan_excess_spill_removal(&reps, &mandatory, anchor.as_ref(), cache_allow)
                    {
                        let chunk_hash =
                            entries.first().map(|e| e.chunk_hash.clone()).unwrap_or_default();
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
        for (path, idx, node, disk_id, enforce, chunk_hash) in removals {
            let req = ClientRequest::RemoveChunkLocation { now_ms: now_millis(),
                path: path.clone(),
                chunk_index: idx,
                node_id: node.to_string(),
                // CO-1-1 — target the exact (node, disk) spill replica.
                disk_id,
                enforce_anchor: enforce,
            };
            match self.raft.client_write(req).await {
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
    async fn reconcile_missing_replicas(&self, node_id: u64, missing: &[&String]) -> usize {
        let node_str = node_id.to_string();
        let mut unreconcilable = 0usize;
        let wanted: std::collections::HashSet<&str> =
            missing.iter().map(|h| h.as_str()).collect();

        // Re-derive the rows to drop from the CURRENT state: the caller's
        // snapshot predates this node's ListChunks reply. ONE pass over the
        // namespace (not one per missing hash).
        let targets: Vec<(String, u64, u32, Option<dtfs_common::types::AnchorPolicy>)> = {
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
                        out.push((path.clone(), c.chunk_index, c.disk_id, anchor.clone()));
                    } else {
                        unreconcilable += 1;
                    }
                }
            }
            out
        };

        for (path, chunk_index, disk_id, anchor) in targets {
            let req = ClientRequest::RemoveChunkLocation { now_ms: now_millis(),
                path: path.clone(),
                chunk_index,
                node_id: node_str.clone(),
                // CO-1-1 — target the exact (node, disk) row we found stale.
                disk_id,
                enforce_anchor: anchor,
            };
            match self.raft.client_write(req).await {
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

    async fn check_anchor_lag(&self, migration_state: &mut MigrationState) {
        let sm = self.state_data.read().await;

        // Build per-node summary for the planner.
        let nodes: Vec<AnchorRepairNode> = sm
            .nodes
            .iter()
            .map(|(id, info)| AnchorRepairNode {
                node_id: *id,
                datacenter: info.location.datacenter.clone(),
                healthy: info.health == HealthStatus::Healthy,
                tiers: info.disks.iter().map(|d| d.tier).collect(),
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
        let mut live_tasks: usize = migration_state
            .tasks
            .values()
            .filter(|t| task_blocks_requeue(t.status))
            .count();

        // Walk every file and run the per-chunk planner.
        for (file_path, file_meta) in &sm.files {
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

                if let Some(action) = plan_anchor_lag_repair(
                    &anchor,
                    &placements,
                    &nodes,
                    &chunk_id,
                    &existing_tasks,
                ) {
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
                        return;
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
    }

    /// Advance each in-progress migration task through its state machine:
    /// `Pending -> Copying -> Verifying -> Completing -> Done`.
    ///
    /// NOTE: Source data is NEVER removed until the target has been verified.
    async fn advance_tasks(&self, state: &mut MigrationState) {
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
    async fn load_migration_state(&self) -> MigrationState {
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

    /// Pick any healthy node that currently has a copy of `chunk_id`,
    /// excluding `exclude` (the migration target). Returns `(node_id,
    /// grpc_addr)` for the chosen source, or `None` if no healthy replica
    /// exists. (Helper for F-015.)
    /// Resolve a healthy node that holds `chunk_id`, optionally excluding
    /// one node id. Bug BO-1: `exclude` is `None` for a same-node
    /// cross-tier anchor copy (`source_node == target_node`) — the planner
    /// (`plan_anchor_lag_repair`, BH) legitimately targets a node that
    /// already holds the chunk on another tier, so excluding the target
    /// would strand the copy with "no healthy source replica". All other
    /// call sites pass `Some(target_node)` (unchanged behaviour).
    async fn resolve_healthy_source(
        &self,
        chunk_id: &str,
        exclude: Option<u64>,
    ) -> Option<(u64, String)> {
        let sm = self.state_data.read().await;
        let mut candidates: Vec<u64> = Vec::new();
        for file in sm.files.values() {
            for c in &file.chunks {
                if c.chunk_hash == chunk_id {
                    if let Ok(nid) = c.node_id.parse::<u64>() {
                        if Some(nid) != exclude && !candidates.contains(&nid) {
                            candidates.push(nid);
                        }
                    }
                }
            }
        }
        for nid in candidates {
            if let Some(info) = sm.nodes.get(&nid) {
                if info.health == HealthStatus::Healthy {
                    return Some((nid, info.grpc_addr.clone()));
                }
            }
        }
        None
    }

    /// Ask the source node to replicate `chunk_id` to `target_addr`,
    /// landing the replica on `target_tier` ("hot"/"warm"/"cold"/
    /// "tape"). AV-5-9: empty tier preserves legacy behaviour (target
    /// falls back to "any healthy disk"), non-empty pins the
    /// destination tier so cache promotions actually land on warm.
    async fn replicate_chunk(
        &self,
        source_addr: &str,
        chunk_id: &str,
        target_addr: &str,
        target_tier: &str,
        chunk_bytes: u64,
    ) -> anyhow::Result<()> {
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
            Ok(())
        } else {
            Err(anyhow::anyhow!("ReplicateChunk returned error: {}", resp.error))
        }
    }

    /// Ask the target node to verify the integrity of `chunk_id`.
    /// Returns `true` if the chunk is valid, `false` if the checksum failed.
    async fn verify_chunk(&self, target_addr: &str, chunk_id: &str) -> anyhow::Result<bool> {
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
            warn!("VerifyChunk returned invalid: {}", resp.error);
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

    /// Find the file path that contains a given chunk hash.
    async fn find_file_for_chunk(&self, chunk_hash: &str) -> Option<String> {
        self.find_files_for_chunk(chunk_hash).await.into_iter().next()
    }

    /// REV-4-6 — EVERY file that references this chunk hash.
    ///
    /// Chunks are content-addressed, so identical content in two files (a
    /// re-download, a copy, two episodes sharing a segment) is ONE chunk id
    /// referenced from several `FileMetadata.chunks` lists. Registering a
    /// completed replica against only the first match left every other
    /// referencing file still short of its mandatory count, so
    /// `check_under_replicated` scheduled the same copy again on the next
    /// tick, forever — the copy succeeded every time and the deficit never
    /// closed.
    async fn find_files_for_chunk(&self, chunk_hash: &str) -> Vec<String> {
        let sm = self.state_data.read().await;
        files_referencing_chunk(&sm.files, chunk_hash)
    }

    /// Completing-stage replica registration (U3 — extracted verbatim from
    /// the pre-U3 Completing branch so the orchestrator's `MigrationOps`
    /// impl can call it). Finds the file owning `chunk_id`, ADDs the new
    /// `target_node`/`tier` replica to the chunk's existing location set
    /// (Bug AB: read existing entries, dedup by (node_id, disk_id) — Bug BH
    /// — pick the disk matching `tier` — AV-5-9), and writes the full set
    /// through Raft. No-op when the file is gone (next round's Bug AE guard
    /// catches the orphan).
    /// CR-3-4/MGR-4/MGR-8/MGR-9 — returns `Err` (instead of silently
    /// warning) when the target has no LIVE disk of `tier`, or when
    /// `chunk_id` no longer resolves to any row in any file (both
    /// resolved under ONE SM read lock via `plan_replica_registration`,
    /// so a lease-holder rewrite or corrupt-replica sweep landing between
    /// two separate lock acquisitions can no longer make a vanished hash
    /// default to chunk_index 0 — MGR-9), or when any `UpdateChunkLocations`
    /// Raft write fails. The Completing dispatcher (below) keeps the task
    /// Completing and retries on `Err` instead of marking it Done.
    async fn register_replica_location(
        &self,
        chunk_id: &str,
        target_node: u64,
        tier: DiskTier,
    ) -> Result<(), String> {
        let writes = {
            let sm = self.state_data.read().await;
            // AV-5-9 / CR-3-4/MGR-8 — pick a LIVE disk_id matching the
            // task's tier on the target; a Failed/Removed disk of the
            // right tier must not be resolved (the fresh replica would
            // record a location `live_replica_tier` can never see).
            let Some(resolved_disk_id) = resolve_disk_id_for_tier(sm.nodes.get(&target_node), tier)
            else {
                warn!(
                    chunk = %chunk_id,
                    target_node,
                    ?tier,
                    "CR-3-4/MGR-8: target has no live disk of this tier; not registering"
                );
                return Err(format!(
                    "no live disk of tier {tier:?} on target node {target_node}"
                ));
            };
            // REV-4-6 / CR-3-4/MGR-9 — register against every referencing
            // file AND every chunk_index within it whose rows carry the
            // hash, under this ONE lock (no second acquisition, no
            // unwrap_or(0) fallback for a hash that isn't there).
            plan_replica_registration(&sm.files, chunk_id, target_node, resolved_disk_id)
        };
        if writes.is_empty() {
            warn!(
                chunk = %chunk_id,
                "CR-3-4/MGR-9: chunk hash not found in any file; not registering"
            );
            return Err(format!("chunk {chunk_id} hash not found in any file"));
        }
        if writes.len() > 1 {
            debug!(
                chunk = %chunk_id,
                files = writes.len(),
                "REV-4-6: chunk is shared; registering the new replica for every referencing file"
            );
        }
        let mut first_err: Option<String> = None;
        for (path, chunks) in writes {
            let req = ClientRequest::UpdateChunkLocations { now_ms: now_millis(),
                path,
                chunks,
                // Bug P — system-driven, so no client lease applies.
                lease_id: String::new(),
            };
            // Bug AB diagnostic — WARN so a stuck-in-loop scheduler is visible.
            if let Err(e) = self.raft.client_write(req).await {
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
    async fn chunk_bytes_anywhere(&self, chunk_hash: &str) -> Option<u64> {
        let sm = self.state_data.read().await;
        chunk_size_in_any_file(chunk_hash, &sm.files)
    }

    /// Persist the migration state via the dedicated `UpdateMigrationState`
    /// Raft command so that it never overwrites `cluster_config` (fix #3).
    async fn save_migration_state(&self, state: &MigrationState) {
        let json = match serde_json::to_string(state) {
            Ok(j) => j,
            Err(e) => {
                error!("Failed to serialize migration state: {}", e);
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
            warn!("Bug AB save_migration_state failed: {} (may not be leader)", e);
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
                .filter(|(path, _)| path.starts_with(path_prefix))
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

        // Dispatch the planned tasks. `schedule_replication` already
        // deduplicates so a retry of this call is safe.
        for action in &plan {
            self.schedule_replication(
                &action.chunk_hash,
                action.source_node,
                action.target_node,
                action.tier,
            )
            .await;
        }
        report.tasks_scheduled = plan.len();

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
        let cooldown_until_ms = now_ms.saturating_add(cooldown_secs * 1000);

        // Group affected chunk_indexes per file_path for one
        // SetChunkCooldown write per file at the end.
        let mut cooldowns: HashMap<String, std::collections::BTreeSet<u64>> =
            HashMap::new();

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
                Err(e) => warn!("Bug BF-7: auto-unquarantine tick failed: {e}"),
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
            let req = ClientRequest::UnquarantineNode {
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
    ///    (`chunk_is_orphan`).
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
            match self.orphan_scrub_tick(grace_secs).await {
                Ok(reaped) => {
                    if reaped > 0 {
                        info!(reaped, "Bug BC-7: orphan_scrub tick reaped chunks");
                    } else {
                        debug!("Bug BC-7: orphan_scrub tick clean");
                    }
                }
                Err(e) => warn!("Bug BC-7: orphan_scrub tick failed: {e}"),
            }
        }
    }

    /// One sweep of orphan_scrub. Returns the number of chunks reaped.
    pub async fn orphan_scrub_tick(&self, grace_secs: u64) -> anyhow::Result<usize> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // 1. Snapshot in-state chunk_ids from state-machine. Bug BF-8
        // also snapshots per-node expected sets so the in-state-missing
        // inverse check can fire alongside the on-disk-not-in-state
        // reap loop without taking the state-machine lock twice.
        let (data_nodes, in_state_chunks, expected_by_node, expected_by_node_disk, already_quarantined) = {
            let sm = self.state_data.read().await;
            let nodes: Vec<(u64, String)> = sm
                .nodes
                .iter()
                .filter(|(_, n)| matches!(n.role, dtfs_common::types::NodeRole::Data))
                .map(|(id, n)| (*id, n.grpc_addr.clone()))
                .collect();
            let mut chunks: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            let mut expected: std::collections::BTreeMap<u64, std::collections::HashSet<String>> =
                std::collections::BTreeMap::new();
            // CR2-7-2 / MGR-10 — same as `expected` but keyed by
            // (node_id, disk_id), so the reap can key the de-located check
            // by the exact disk a chunk was found on, not node-wide. Two
            // replicas of the same chunk can legitimately co-locate on one
            // node on different disks/tiers (Bug BH); a node-wide check
            // alone can't tell a genuinely de-located warm-disk leftover
            // from a still-live cold-disk anchor of the SAME chunk.
            let mut expected_disk: std::collections::BTreeMap<
                (u64, u32),
                std::collections::HashSet<String>,
            > = std::collections::BTreeMap::new();
            for f in sm.files.values() {
                for c in &f.chunks {
                    chunks.insert(c.chunk_hash.clone());
                    if let Ok(nid) = c.node_id.parse::<u64>() {
                        expected.entry(nid).or_default().insert(c.chunk_hash.clone());
                        expected_disk
                            .entry((nid, c.disk_id))
                            .or_default()
                            .insert(c.chunk_hash.clone());
                    }
                }
            }
            let quarantined: std::collections::HashSet<u64> = sm
                .nodes
                .iter()
                .filter(|(_, n)| matches!(
                    n.health,
                    dtfs_common::types::HealthStatus::Quarantined
                ))
                .map(|(id, _)| *id)
                .collect();
            (nodes, chunks, expected, expected_disk, quarantined)
        };

        // REV-1-2 — freshness gate. The reap deletes anything on disk that
        // this manager cannot see in `sm.files`, so a state machine that is
        // still replaying (a manager that was down, or one that just loaded
        // an old on-disk snapshot) would classify every file created since as
        // an orphan. An empty in-state chunk set alongside live data nodes is
        // never a legitimate "everything is an orphan" either.
        let (applied, last_log) = self.applied_and_log_index();
        if !orphan_sweep_allowed(self.is_leader(), applied, last_log, in_state_chunks.len()) {
            debug!(
                applied,
                last_log,
                in_state_chunks = in_state_chunks.len(),
                "REV-1-2: orphan_scrub sweep skipped (not leader / state not caught up / empty view)"
            );
            return Ok(0);
        }

        let mut reaped: usize = 0;

        'nodes: for (node_id, grpc_addr) in &data_nodes {
            // CR2-7-3 / MGR-13 — re-check leadership + freshness at the top
            // of EVERY node iteration, not just once before the loop. A
            // sweep over a 50 TB anchor node can span an election; without
            // this a follower that just lost (or a leader that just lost)
            // leadership kept issuing direct data-node delete RPCs for the
            // rest of the sweep, which no Raft rejection can undo.
            let (applied_now, last_log_now) = self.applied_and_log_index();
            if !orphan_sweep_allowed(self.is_leader(), applied_now, last_log_now, in_state_chunks.len()) {
                self.audit.log(
                    "scheduler",
                    "orphan.sweep_aborted_not_leader",
                    &format!("aborted before node={node_id}; reaped_so_far={reaped}"),
                    "aborted",
                );
                break 'nodes;
            }
            if grpc_addr.is_empty() { continue; }
            let on_disk = match self.list_chunks_on_node(grpc_addr).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(node = %node_id, error = %e, "Bug BC-7: ListChunks failed");
                    continue;
                }
            };
            // Bug BF-8 — in-state-missing inverse. Before processing
            // the on-disk-not-in-state loop, check what the state
            // machine THINKS this node holds; quarantine the node if
            // any expected chunk is absent from its ListChunks reply.
            // Skip if the node is already Quarantined (apply path
            // accumulates fault_count_total; no need to bump on every
            // tick).
            if !already_quarantined.contains(node_id) {
                if let Some(expected) = expected_by_node.get(node_id) {
                    let missing: Vec<&String> =
                        expected.iter().filter(|c| !on_disk.contains_key(*c)).collect();
                    // CO-3-3 — the `expected_by_node` set was snapshotted at
                    // the top of this tick under one read lock, but the
                    // ListChunks reply arrives later. A demote / unlink /
                    // rebalance that committed `RemoveChunkLocation` between
                    // the snapshot and now leaves the stale snapshot still
                    // naming the node for a chunk it no longer holds
                    // (legitimately) — quarantining a healthy node. Re-read
                    // the CURRENT SM and keep only chunks the node is STILL
                    // expected to hold; if none remain, the "missing" set was
                    // a demote race, not a fault — skip the quarantine (but
                    // still fall through to the on-disk reap below).
                    let missing: Vec<&String> = if missing.is_empty() {
                        missing
                    } else {
                        let still_expected =
                            self.current_expected_chunks_for_node(*node_id).await;
                        let filtered = still_missing_after_recheck(&missing, &still_expected);
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

            let node_expected = expected_by_node.get(node_id);
            for (cid, (mtime_ms, disk_id_opt)) in &on_disk {
                // Bug BP-4 — a chunk is an orphan candidate when NO
                // FileMetadata references its hash (`in_state_chunks` is the
                // union of every file's chunk hashes). Never reap a chunk a
                // file still points at, even if its location record looks
                // stale — that was the data-loss vector (BN-1 UUID rotation
                // dropped location bindings; the hash-referenced guard here
                // is the defence-in-depth backstop). CLAUDE.md §A.6 #8.
                let is_orphan = chunk_is_orphan(&in_state_chunks, cid);

                // CO-3-2 / CR2-7-2 (MGR-10) — a chunk on this node's disk
                // for which the SM records NO location entry naming this
                // node (or, when the data node reports which disk, no
                // entry naming this exact (node, disk)) is a phantom
                // replica (the rebalance / excess-spill path drops the SM
                // entry via `RemoveChunkLocation` but never deletes the
                // de-located bytes). This supersets `is_orphan` (a
                // hash-orphan is named by no node), but we keep both
                // signals explicit. Both are safe: a chunk a file still
                // points at on THIS node/disk stays in the expected set,
                // and the mtime grace protects an in-flight put whose
                // `commit_chunk_locations` Raft write has not yet applied.
                //
                // When the data node reports a disk_id (CR2-7-2), key the
                // check by (node, disk) so a de-located leftover on one
                // disk is never missed by (or confused with) a still-live
                // replica of the SAME chunk on another disk of the same
                // node (documented co-located layout: cold anchor + warm
                // cache, Bug BH). A pre-CR2-7-2 data node reports no
                // disk_id — fall back to today's node-wide check exactly.
                let de_located = match disk_id_opt {
                    Some(disk_id) => {
                        let node_disk_expected = expected_by_node_disk.get(&(*node_id, *disk_id));
                        chunk_delocated_on_node(node_disk_expected, cid)
                    }
                    None => chunk_delocated_on_node(node_expected, cid),
                };
                let reap_candidate = is_orphan || de_located;

                // Bug CB — measure the reap grace against the chunk's
                // DURABLE on-disk mtime, not a manager-local "first
                // observed orphan" clock. The old in-memory tracker reset
                // on every manager restart / leadership change, so under
                // rolling-restart churn a manager never accumulated the
                // 24h grace and orphaned chunks were never reaped (disk
                // usage of deleted files never dropped). The mtime survives
                // restarts; a freshly-written chunk (recent mtime) is still
                // protected against the in-flight put/promote race.
                if !orphan_reap_due(reap_candidate, *mtime_ms, now_ms, grace_secs) {
                    // Surface a known reap candidate we are deferring only
                    // because its age is unknown (mtime 0 = pre-Bug-CB data
                    // node), so a permanently-stalled reap is diagnosable.
                    if reap_candidate && *mtime_ms == 0 {
                        debug!(
                            node = %node_id, chunk = %cid,
                            "Bug CB: orphan deferred — data node reported no \
                             mtime (upgrade the data node to enable reaping)"
                        );
                    }
                    continue;
                }
                let age_secs = now_ms.saturating_sub(*mtime_ms) / 1000;

                // CR2-7-3 / MGR-13 — re-check leadership + freshness again
                // immediately before EACH delete batch (not just once per
                // node): a per-node on_disk listing can itself carry many
                // reap candidates, and an election can land between two of
                // them just as easily as between two nodes.
                let (applied_batch, last_log_batch) = self.applied_and_log_index();
                if !orphan_sweep_allowed(
                    self.is_leader(),
                    applied_batch,
                    last_log_batch,
                    in_state_chunks.len(),
                ) {
                    self.audit.log(
                        "scheduler",
                        "orphan.sweep_aborted_not_leader",
                        &format!(
                            "aborted before delete node={node_id} chunk={cid}; reaped_so_far={reaped}"
                        ),
                        "aborted",
                    );
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
                let cause = if is_orphan { "orphan" } else { "delocated" };
                match self
                    .delete_chunk_replica_on_node(grpc_addr, cid, *disk_id_opt, "")
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
                    }
                    Err(e) => {
                        warn!(
                            node = %node_id, chunk = %cid, error = %e,
                            "Bug BC-7: orphan reap delete RPC failed"
                        );
                    }
                }
            }
        }

        Ok(reaped)
    }

    /// Bug BF-8 / REV-3-10 — quarantine `node_id` for an unreconcilable
    /// in-state-missing finding. Extracted from `orphan_scrub_tick` so the
    /// `QuarantineInfo` struct literal lives at method top level (the deeply
    /// nested form tripped a rustc 1.94 typeck ICE).
    async fn quarantine_for_in_state_missing(
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
        let req = ClientRequest::QuarantineNode { node_id, info };
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

    /// CO-3-3 — the set of chunk hashes the CURRENT state machine records a
    /// location entry for on `node_id` (the union across all of the node's
    /// disks). Used by `orphan_scrub_tick` to re-confirm an "in-state but
    /// not on disk" finding against fresh SM state before quarantining: a
    /// demote / unlink that committed `RemoveChunkLocation` after the tick's
    /// opening snapshot drops the chunk from this set, so a stale-snapshot
    /// false positive is filtered out.
    async fn current_expected_chunks_for_node(
        &self,
        node_id: u64,
    ) -> std::collections::HashSet<String> {
        let sm = self.state_data.read().await;
        let mut out = std::collections::HashSet::new();
        for f in sm.files.values() {
            for c in &f.chunks {
                if c.node_id.parse::<u64>().ok() == Some(node_id) {
                    out.insert(c.chunk_hash.clone());
                }
            }
        }
        out
    }

    /// Bug BC-6 — invoke `ListChunks` on a single data node, drain all
    /// pages, return the combined set of chunk_ids.
    async fn list_chunks_on_node(
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
}

// ---------------------------------------------------------------------------
// U3 — migration-task orchestrator (multi-stage-per-tick, bounded-parallel
// copy). The side effects the per-task state machine performs are captured
// behind the `MigrationOps` trait so the loop + concurrency behaviour is
// unit-testable against a mock (advance_round_tests) without a live network
// or a real Raft handle. `TaskScheduler` implements the trait by delegating
// to its existing helpers; the live behaviour is byte-for-byte the pre-U3
// per-task logic, only the *driving loop* changed.
// ---------------------------------------------------------------------------

/// U3 — the side-effecting operations the migration state machine needs.
/// One impl is live (`TaskScheduler`); tests supply a mock. All methods are
/// independent per task, which is what makes the Copying RPCs safe to
/// fan out in parallel.
trait MigrationOps {
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
        self.resolve_healthy_source(chunk_id, exclude).await
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
        self.register_replica_location(chunk_id, target_node, tier).await
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
struct CopyJob {
    task_id: String,
    chunk_id: String,
    resolved_source: u64,
    src_addr: String,
    tgt_addr: String,
    tier_str: String,
    /// WI-04 — the chunk's byte count, looked up once (alongside the Bug AE
    /// orphan guard) and carried through to `replicate` so the RPC deadline
    /// scales with it.
    chunk_bytes: u64,
}

/// U3 — drive every non-terminal task through its state machine, looping in
/// rounds until a round makes no progress (or the `MAX_ADVANCE_ROUNDS` guard
/// trips). Within a round, the Copying `replicate` RPCs are dispatched
/// bounded-parallel (`MIGRATION_COPY_CONCURRENCY`); Pending/Verifying/
/// Completing transitions stay serial (cheap, and Completing writes Raft).
/// Mutates `state` in place; the caller persists once per tick.
async fn advance_migration_tasks<O: MigrationOps>(
    ops: &O,
    state: &mut MigrationState,
    now_ms: u64,
) {
    // A task gets at most ONE Copying RPC and ONE Verifying RPC per call
    // (i.e. per tick). This preserves the pre-U3 retry cadence — a no-source
    // / transient-error task still burns one attempt per tick over
    // MAX_MIGRATION_ATTEMPTS ticks, and a verify-mismatch flap re-copies on
    // the NEXT tick, not by spinning rounds in this one. The round loop only
    // exists to let a *forward-progressing* chunk climb Pending -> Done in
    // one tick; it never re-dispatches a network RPC to the same task.
    let mut copied: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut verified: std::collections::HashSet<String> = std::collections::HashSet::new();
    for _round in 0..MAX_ADVANCE_ROUNDS {
        let progressed = advance_round(ops, state, now_ms, &mut copied, &mut verified).await;
        if !progressed {
            break;
        }
    }
}

/// U3 — a single round over all non-terminal tasks. Returns true if any task
/// changed status this round (so the caller loops again). A chunk can thus
/// climb Pending -> Copying -> Verifying -> Completing -> Done across four
/// rounds of ONE tick instead of four ticks. `copied`/`verified` carry the
/// once-per-call dispatch guard across rounds (see `advance_migration_tasks`).
async fn advance_round<O: MigrationOps>(
    ops: &O,
    state: &mut MigrationState,
    now_ms: u64,
    copied: &mut std::collections::HashSet<String>,
    verified: &mut std::collections::HashSet<String>,
) -> bool {
    use futures::stream::{self, StreamExt};

    let mut progressed = false;
    let task_ids: Vec<String> = state.tasks.keys().cloned().collect();

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
        let resolved_source = ops.resolve_source(&chunk_id, exclude).await;

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

        let results: Vec<(String, u64, Result<(), String>)> = stream::iter(jobs)
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
                        .replicate(
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
                Ok(()) => {
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
    for task_id in &task_ids {
        let (chunk_id, target_node, tier, source_node) = {
            let Some(task) = state.tasks.get(task_id) else { continue };
            if task.status != MigrationStatus::Completing {
                continue;
            }
            (task.chunk_id.clone(), task.target_node, task.tier, task.source_node)
        };

        match ops.register_replica(&chunk_id, target_node, tier).await {
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

    progressed
}

/// U3 — account a Copying-stage failure: bump attempts, promote a
/// "Chunk not found" to instant-Failed (Bug AF), and on exhaustion mark the
/// task Failed + audit + alert. Mirrors the pre-U3 inline accounting.
fn record_copy_failure<O: MigrationOps>(
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

// ---------------------------------------------------------------------------
// Bug AV — flush_cache_for_path pure-planner kernel
// ---------------------------------------------------------------------------

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
    fn from_sm(sm: &StateMachineData, path_prefix: &str) -> Self {
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
                                let live = live_replica_tier(Some(info), d.disk_id).is_some();
                                (d.disk_id, d.tier, live)
                            })
                            .collect(),
                    },
                )
            })
            .collect();
        let mut files = Vec::new();
        for (path, meta) in sm.files.iter() {
            if !path.starts_with(path_prefix) {
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

/// Bug BH — true if `entries` already records a replica of this chunk on
/// the exact `(node_id, disk_id)` slot. The anchor-lag-repair completion
/// path used to dedup on `node_id` alone, which on a no-spare-node
/// multi-tier topology (target already holds the chunk on its warm disk)
/// silently dropped the new *cold* anchor entry — `disk_id` is already
/// tier-resolved (AV-5-9 `resolved_disk_id`), so `(node_id, disk_id)` is
/// the exact `(node, tier)` slot and the correct dedup key.
pub(crate) fn replica_already_present(
    entries: &[ChunkLocationEntry],
    node_id: &str,
    disk_id: u32,
) -> bool {
    entries
        .iter()
        .any(|c| c.node_id == node_id && c.disk_id == disk_id)
}

/// CR-3-4/MGR-9 — pure planner for `register_replica_location`. For
/// EVERY (file, chunk_index) pair whose existing rows carry
/// `chunk_hash` (not just the first match — MGR-8's intra-file
/// duplicate-hash case, e.g. zero-filled padding sharing a chunk),
/// builds the FULL post-registration entry set for that index: existing
/// rows plus the new target replica, deduped by `(node_id, disk_id)`
/// (Bug BH). Returns one `(path, entries)` per referencing file; empty
/// when the hash resolves to no row anywhere (caller treats that as an
/// error rather than defaulting to chunk_index 0 — MGR-9).
pub(crate) fn plan_replica_registration(
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
    chunk_hash: &str,
    target_node: u64,
    resolved_disk_id: u32,
) -> Vec<(String, Vec<ChunkLocationEntry>)> {
    let target_str = target_node.to_string();
    let mut writes = Vec::new();
    for (path, fm) in files.iter() {
        let mut indices: Vec<u64> = fm
            .chunks
            .iter()
            .filter(|c| c.chunk_hash == chunk_hash)
            .map(|c| c.chunk_index)
            .collect();
        indices.sort_unstable();
        indices.dedup();
        if indices.is_empty() {
            continue;
        }
        // Bug AB — carry every EXISTING row of each touched index, then
        // add the new one; merge_chunk_location_entries REPLACES a
        // chunk_index's rows wholesale, so passing only the new entry
        // would wipe prior replicas and loop under-replication forever.
        let mut entries: Vec<ChunkLocationEntry> = fm
            .chunks
            .iter()
            .filter(|c| indices.contains(&c.chunk_index))
            .cloned()
            .collect();
        for idx in &indices {
            // Bug BH — dedup by (node_id, disk_id), NOT node_id alone, so
            // a second-tier mandatory copy on the same node is recorded.
            let already = entries
                .iter()
                .any(|c| c.chunk_index == *idx && c.node_id == target_str && c.disk_id == resolved_disk_id);
            if !already {
                entries.push(ChunkLocationEntry {
                    chunk_index: *idx,
                    chunk_hash: chunk_hash.to_string(),
                    node_id: target_str.clone(),
                    disk_id: resolved_disk_id,
                    size_bytes: 0,
                });
            }
        }
        writes.push((path.clone(), entries));
    }
    writes
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
    /// Up to 5 illustrative entries so the operator can see *which* paths
    /// hit `no_source` / `no_target` without dumping the full list.
    pub examples: Vec<PathMigrationExample>,
}

impl PathMigrationReport {
    fn note_example(&mut self, path: &str, chunk_index: u64, reason: &str) {
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

/// Get current time in milliseconds since UNIX epoch.
fn now_millis() -> u64 {
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
    /// called AFTER `remove_location` succeeded.
    async fn delete_bytes(&self) -> anyhow::Result<()>;
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
    // SM entry is gone. A byte-delete failure now only leaves de-located
    // bytes for the CO-3-2 reaper; never roll back the committed remove.
    if ops.delete_bytes().await.is_err() {
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
        self.sched
            .raft()
            .client_write(self.remove_req.clone())
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("RemoveChunkLocation Raft write: {}", e))
    }

    async fn delete_bytes(&self) -> anyhow::Result<()> {
        self.sched
            .delete_chunk_replica_on_node(
                &self.node_addr,
                &self.chunk_id,
                Some(self.disk_id),
                &self.tier,
            )
            .await
            .map(|_| ())
    }
}

/// Bug BP-4 — GC orphan classification. A physical chunk is an orphan
/// (eligible for reap) ONLY when no FileMetadata references its hash;
/// `in_state_chunks` is the union of every file's chunk hashes. Keying on
/// the hash (not on a per-disk location/UUID match) is the load-bearing
/// safety invariant: a chunk a file still points at must never be deleted,
/// even if its location record looks stale. CLAUDE.md §A.3.3 #4 / §A.6 #8.
pub(crate) fn chunk_is_orphan(
    in_state_chunks: &std::collections::HashSet<String>,
    chunk_hash: &str,
) -> bool {
    !in_state_chunks.contains(chunk_hash)
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
/// This strictly supersets `chunk_is_orphan`: a chunk referenced by NO file
/// is named by no node either, so it is also absent from every
/// `node_expected`. The hash-orphan check is kept as the explicit primary
/// signal; this adds the per-node de-location case (CLAUDE.md §A.6 #8). The
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

/// CO-3-3 — given a set of chunks the tick's OPENING snapshot expected on a
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
        let on_tier = |t: DiskTier| remaining.iter().filter(|(_, _, _, tt)| *tt == t).count() as u32;
        let tiers_ok = on_tier(DiskTier::Hot) >= mandatory.hot
            && on_tier(DiskTier::Warm) >= mandatory.warm
            && on_tier(DiskTier::Cold) >= mandatory.cold
            && on_tier(DiskTier::Tape) >= mandatory.tape;
        let anchor_ok = match anchor {
            None => true,
            Some(a) => remaining.iter().any(|(_, _, dc, tt)| {
                dc == &a.datacenter
                    && match a.required_tier {
                        Some(rt) => *tt == rt,
                        None => true,
                    }
            }),
        };
        if tiers_ok && anchor_ok {
            return Some((*node, *disk_id));
        }
    }
    None
}

// ---------------------------------------------------------------------------
// REV wave-1 hardening — pure predicates for the manager background loops.
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
pub(crate) fn orphan_sweep_allowed(
    is_leader: bool,
    last_applied: u64,
    last_log_index: u64,
    in_state_chunks: usize,
) -> bool {
    if !is_leader {
        return false;
    }
    if last_applied < last_log_index {
        return false;
    }
    in_state_chunks > 0
}

/// CR2-7-3 / MGR-13 — pure model of the abort decision `orphan_scrub_tick`
/// makes at every per-node and per-delete-batch checkpoint: given the
/// sequence of `(is_leader, last_applied, last_log_index, in_state_chunks)`
/// samples observed at each checkpoint (in order), return the index of the
/// FIRST one that fails `orphan_sweep_allowed`, or `None` if every
/// checkpoint stayed clear. The real loop `break`s at exactly that point,
/// so no delete batch after it is ever attempted.
pub(crate) fn first_sweep_abort_index(checks: &[(bool, u64, u64, usize)]) -> Option<usize> {
    checks
        .iter()
        .position(|&(leader, applied, last_log, n)| !orphan_sweep_allowed(leader, applied, last_log, n))
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

pub(crate) fn plan_anchor_lag_repair(
    anchor: &dtfs_common::types::AnchorPolicy,
    placements: &[AnchorRepairPlacement],
    nodes: &[AnchorRepairNode],
    chunk_id: &str,
    existing_tasks: &std::collections::HashSet<(String, u64)>,
) -> Option<AnchorRepairAction> {
    // Gate 1 — is the anchor already satisfied?
    let satisfied = placements.iter().any(|p| {
        p.dc == anchor.datacenter
            && match anchor.required_tier {
                Some(t) => p.tier == t,
                None => true,
            }
    });
    if satisfied {
        return None;
    }

    // Gate 2 — pick a target tier. Prefer the required_tier if set,
    // else cold (the durable anchor floor), else any tier available
    // in the anchor DC.
    let candidate_tier = anchor.required_tier.unwrap_or(DiskTier::Cold);

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
    let target = nodes
        .iter()
        .filter(|n| n.healthy)
        .filter(|n| n.datacenter == anchor.datacenter)
        .filter(|n| n.tiers.contains(&candidate_tier))
        .filter(|n| !tier_holders.contains(&n.node_id))
        .min_by_key(|n| n.node_id)?; // deterministic by node_id

    // Gate 4 — already scheduled?
    if existing_tasks.contains(&(chunk_id.to_string(), target.node_id)) {
        return None;
    }

    // Source = any existing replica (the dispatcher's
    // `ReplicateChunk` RPC handles cross-tier reads).
    let source = placements
        .iter()
        .min_by_key(|p| p.node_id)
        .map(|p| p.node_id)?;

    Some(AnchorRepairAction {
        source_node: source,
        target_node: target.node_id,
        target_tier: candidate_tier,
    })
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
fn prune_terminal_tasks(
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

/// Returns true iff `chunk_hash` appears in any `FileMetadata.chunks` entry
/// across the supplied `files` map. The async caller pulls the
/// state-machine read lock; this function stays sync so it's trivially
/// unit-testable without a tokio runtime. Retained for its test coverage
/// and as the orphan predicate `chunk_size_in_any_file` mirrors (WI-04's
/// `chunk_bytes_anywhere` now uses that instead, since it also needs the
/// chunk's size).
#[inline]
pub(crate) fn chunk_in_any_file(
    chunk_hash: &str,
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
) -> bool {
    files
        .values()
        .any(|f| f.chunks.iter().any(|c| c.chunk_hash == chunk_hash))
}

/// WI-04 — pure helper for `chunk_bytes_anywhere`. Returns the largest
/// `size_bytes` recorded for `chunk_hash` across every `ChunkLocationEntry`
/// row that references it in `files`, or `None` if no file references the
/// chunk at all (the same orphan predicate as `chunk_in_any_file`). `max`
/// is a defensive, never-under-estimate choice — chunks are
/// content-addressed, so distinct rows for the same hash should agree.
#[inline]
pub(crate) fn chunk_size_in_any_file(
    chunk_hash: &str,
    files: &std::collections::BTreeMap<String, dtfs_raft::state_machine::FileMetadata>,
) -> Option<u64> {
    files
        .values()
        .flat_map(|f| f.chunks.iter())
        .filter(|c| c.chunk_hash == chunk_hash)
        .map(|c| c.size_bytes)
        .max()
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

/// CNF-1 — the disk_id recorded into a new `ChunkLocationEntry` (AV-5-9
/// read-back registration) must be the matching disk's `disk_id` FIELD —
/// the key every reader resolves via `NodeInfo::disk` — never its Vec
/// position. CR-3-4/MGR-8 — also filters out Failed/Removed disks: a
/// dead disk of the right tier is not a valid registration target
/// (the fresh replica would record a location `live_replica_tier` can
/// never see, and the repair loop would re-copy it forever). Returns
/// `None` when the target has no LIVE disk of the tier (pre-fix this
/// silently fell back to disk_id 0, mis-registering a copy that may
/// have actually landed on a different, live disk).
#[inline]
pub(crate) fn resolve_disk_id_for_tier(
    node: Option<&dtfs_raft::NodeInfo>,
    tier: DiskTier,
) -> Option<u32> {
    node.and_then(|n| {
        n.disks
            .iter()
            .find(|d| {
                d.tier == tier && !matches!(d.status, DiskStatus::Failed | DiskStatus::Removed)
            })
            .map(|d| d.disk_id)
    })
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

/// Bug AW — split out the migration-snapshot sort comparator so we can
/// unit-test it without spinning up a scheduler. Sorts in place by
/// `created_at_ms` DESC (newest first, immutable after insertion)
/// then by natural-numeric `task_id` ASC ("repl-2" before "repl-10").
fn sort_migration_snapshot(out: &mut Vec<serde_json::Value>) {
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
fn natural_task_id_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    match (trailing_u64(a), trailing_u64(b)) {
        (Some(an), Some(bn)) => an.cmp(&bn).then_with(|| a.cmp(b)),
        _ => a.cmp(b),
    }
}

fn trailing_u64(s: &str) -> Option<u64> {
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
mod tests;
