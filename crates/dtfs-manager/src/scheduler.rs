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

mod flush;
mod migration;
mod orphan;
mod repair;
#[cfg(test)]
pub(crate) use flush::*;
pub(crate) use migration::*;
pub(crate) use orphan::*;
pub(crate) use repair::*;

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

/// Cap on copy jobs dispatched in ONE advance round. The tick holds
/// `migration_state_lock` for the whole advance, so an unbounded round (a
/// backlog of thousands of tasks after a LELI outage) pinned it for hours.
/// Tasks past the cap keep their Copying status and are picked up by the
/// next round / tick.
const MIGRATION_MAX_JOBS_PER_ROUND: usize = 2 * MIGRATION_COPY_CONCURRENCY;

/// Wall-clock budget for the round loop of one tick: no new round starts
/// once it is spent (a started round still finishes; each RPC inside is
/// bounded). Migration state is saved after a round at most once per
/// `SCHEDULER_TICK`, so a leader change loses at most that much progress.
const MIGRATION_TICK_BUDGET: Duration = Duration::from_secs(600);

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

/// Map a Raft apply response to a scheduler-level success/failure. The state
/// machine reports refusals (anchor/last-live-replica guards, lease or lock
/// conflicts) as `Ok(ClientResponse::...)`, not as a Raft error, so callers
/// that only look at `client_write(..).is_err()` treat a refused write as done.
pub(crate) fn client_response_ok(resp: &dtfs_raft::ClientResponse) -> Result<(), String> {
    use dtfs_raft::ClientResponse as R;
    match resp {
        R::Error { message } => Err(message.clone()),
        R::LockConflict { .. } | R::WriteLeaseConflict { .. } => Err(resp.to_string()),
        _ => Ok(()),
    }
}

/// Serializable collection of active migration tasks, stored in Raft state
/// via `ClientRequest::UpdateConfig`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub(crate) struct MigrationState {
    tasks: HashMap<String, ReplicationTask>,
    next_task_id: u64,
    /// task_id -> disk the target data node reported storing the copy on
    /// (`ReplicateChunkResponse.disk_id`), consumed at registration. Absent
    /// for old data nodes. Pruned when the task goes terminal.
    #[serde(default)]
    landed_disks: HashMap<String, u32>,
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
    /// Index of the chunks the in-flight migration tasks refer to, with the
    /// last applied log id it was built at (one namespace pass per advance,
    /// rebuilt only when the state machine applied something since). Only
    /// set between `begin_round` and `end_round`, under `migration_state_lock`.
    round_index: parking_lot::Mutex<Option<(Option<dtfs_raft::LogId>, ChunkIndex)>>,
    /// Single-flight for orphan reaps: the timer and a manual
    /// `POST /api/v1/scrub/orphans` never run at the same time.
    orphan_run: Arc<tokio::sync::Mutex<()>>,
    /// Last (or running) orphan reap, for `GET /api/v1/scrub/orphans`.
    orphan_last_run: parking_lot::Mutex<Option<OrphanRunRecord>>,
    /// Issue #157 — the `default` volume this manager's config describes;
    /// the leader tick creates it when missing (`set_default_volume`).
    default_volume: parking_lot::RwLock<Option<dtfs_common::types::VolumeConfig>>,
}

/// Issue #157 — the create-if-missing proposal for the `default` volume, or
/// `None` once ANY volume exists: `default` is a bootstrap seed, so a later
/// delete of it is not undone (files without any volume, the original #157
/// case, still heal). `expected_version: Some(0)` makes apply refuse it
/// once any `default` was stored (versions start at 1), so a racing operator
/// edit is never overwritten.
pub(crate) fn default_volume_request(
    sm: &StateMachineData,
    cfg: &dtfs_common::types::VolumeConfig,
    now_ms: u64,
) -> Option<ClientRequest> {
    // Bootstrap (no volume yet), or `default` missing while files still use it
    // (repair skips files of a missing volume). A deleted, unused one stays gone.
    let id = cfg.id.0.as_str();
    if sm.volumes.contains_key(id)
        || !(sm.volumes.is_empty() || sm.files.values().any(|f| f.volume_id == id))
    {
        return None;
    }
    Some(ClientRequest::UpsertVolume { config: cfg.clone(), expected_version: Some(0), now_ms })
}

impl TaskScheduler {

    /// Propose `req` and treat a state-machine refusal as an error (see
    /// `client_response_ok`). Use for writes whose success gates a byte
    /// delete or a "registered" claim.
    pub(crate) async fn raft_write_checked(&self, req: ClientRequest) -> anyhow::Result<()> {
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        client_response_ok(&resp.data).map_err(|message| {
            warn!(%message, "scheduler Raft write refused by the state machine");
            anyhow::anyhow!("refused by state machine: {message}")
        })
    }

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
            round_index: parking_lot::Mutex::new(None),
            orphan_run: Arc::new(tokio::sync::Mutex::new(())),
            orphan_last_run: parking_lot::Mutex::new(None),
            default_volume: parking_lot::RwLock::new(None),
        }
    }

    /// Issue #157 — wire the `default` volume from `[defaults]` (main.rs).
    pub fn set_default_volume(&self, cfg: dtfs_common::types::VolumeConfig) {
        *self.default_volume.write() = Some(cfg);
    }

    /// Issue #157 — leader-only, every tick until it lands: create the
    /// `default` volume while the state has no volume at all, from the
    /// leader's own `[defaults]` (cluster-scope keys, identical on every
    /// manager).
    async fn ensure_default_volume(&self) {
        let Some(cfg) = self.default_volume.read().clone() else { return };
        let req = {
            let sm = self.state_data.read().await;
            default_volume_request(&sm, &cfg, now_millis())
        };
        let Some(req) = req else { return };
        match self.raft_write_checked(req).await {
            Ok(()) => info!(anchor = ?cfg.anchor_datacenter, "default volume created"),
            Err(e) => warn!(error = %e, "default volume create failed; retrying next tick"),
        }
    }

    /// Placement thresholds (headroom) the scheduler places against; the
    /// tiering engine uses the same view when picking promotion targets.
    pub(crate) fn placement_cfg(&self) -> &dtfs_common::config::PlacementConfig {
        &self.placement_cfg
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
    pub(super) fn applied_and_log_index(&self) -> (u64, u64) {
        use openraft::async_runtime::watch::WatchReceiver;
        let metrics = self.raft.metrics().borrow_watched().clone();
        let applied = metrics.last_applied.map(|l| l.index).unwrap_or(0);
        let last_log = metrics.last_log_index.unwrap_or(applied);
        (applied, last_log)
    }

    pub(super) fn under_memory_pressure(&self) -> bool {
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
                warn!(error = %e, "Scheduler tick failed");
            }
        }
    }

    /// P0-1: lease garbage-collection loop. Sweeps expired write leases
    /// every second by proposing `SweepExpiredLeases` through Raft. Without
    /// this, an orphan lease (FUSE crashed, network partition, kill -9)
    /// blocks all writers to that path until the lease's lease_ms elapses.
    pub async fn run_lease_gc(
        raft: Arc<DtfsRaft>,
        state_data: Arc<RwLock<StateMachineData>>,
    ) {
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
            // MGR-GC-1 — only propose when the local (leader, hence
            // authoritative) view holds a lease that has actually expired.
            // Pre-fix every second appended a synced no-op `SweepExpiredLeases`
            // entry: ~86k fsynced log writes a day on an idle cluster and a
            // snapshot every ~2.8 h from lease GC alone.
            let any_expired = {
                let sm = state_data.read().await;
                lease_gc_due(&sm, now_ms)
            };
            if !any_expired {
                continue;
            }
            if let Err(e) = raft
                .client_write(dtfs_raft::ClientRequest::SweepExpiredLeases { now_ms, sweep_posix_locks: true })
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
    pub(super) async fn tick(&self) -> anyhow::Result<()> {
        // Skip on followers — only the current leader drives migrations.
        // REV-1-1: shared with every other background loop via `is_leader()`.
        if !self.is_leader() {
            tracing::trace!("Scheduler tick skipped: not leader");
            return Ok(());
        }

        self.ensure_default_volume().await;

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
}

/// MGR-GC-1 — does any lease expiry lie at or before `now_ms`? Mirrors the
/// apply-side predicate (`expires_at_ms <= now_ms`) so a due sweep is never
/// skipped and an idle table never proposes.
pub(crate) fn lease_sweep_due(expiries: impl IntoIterator<Item = u64>, now_ms: u64) -> bool {
    expiries.into_iter().any(|e| e <= now_ms)
}

/// Audit #11 — the sweep also reaps expired POSIX byte-range locks, so
/// an expired lock alone must trigger it.
pub(crate) fn lease_gc_due(sm: &StateMachineData, now_ms: u64) -> bool {
    lease_sweep_due(
        sm.write_leases.values().map(|l| l.expires_at_ms)
            .chain(sm.posix_locks.values().flatten().map(|l| l.expires_at_ms)),
        now_ms,
    )
}

#[cfg(test)]
mod mgr_gc_lease_sweep_due_tests {
    use super::lease_sweep_due;

    #[test]
    fn empty_table_never_sweeps() {
        assert!(!lease_sweep_due(Vec::<u64>::new(), 1_000));
    }

    #[test]
    fn future_leases_do_not_sweep() {
        assert!(!lease_sweep_due([2_000, 3_000], 1_000));
    }

    #[test]
    fn an_expired_lease_sweeps() {
        assert!(lease_sweep_due([5_000, 1_000], 1_000));
    }

    #[test]
    fn an_expired_posix_lock_alone_sweeps() {
        use dtfs_raft::state_machine::{DistributedLock, StateMachineData};
        let mut sm = StateMachineData::default();
        sm.posix_locks.insert("/f".into(), vec![DistributedLock {
            owner_id: "o".into(), start: 0, end: 1, write: true, expires_at_ms: 2_000,
        }]);
        assert!(!super::lease_gc_due(&sm, 1_999));
        assert!(super::lease_gc_due(&sm, 2_000));
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod migration_state_compat_tests {
    use super::*;

    #[test]
    fn old_migration_state_json_loads_without_landed_disks() {
        let s: MigrationState = serde_json::from_str(r#"{"tasks":{},"next_task_id":3}"#).unwrap();
        assert_eq!(s.next_task_id, 3);
        assert!(s.landed_disks.is_empty());
    }
}
