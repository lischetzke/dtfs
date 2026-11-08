//! REST handlers: cluster status, health, stats, metrics and probes. Split out of api.rs on 2026-09-13; every
//! item sees api.rs's imports and private helpers via the glob.
#![allow(unused_imports)]

use super::*;

/// Bug BP-3 — effective (projected) health for a node, downgrading a stored
/// `Healthy` to `Suspect`/`Down` when its `last_seen_ms` is stale. Read-path
/// only: it never mutates state, so it surfaces the truth even when raft
/// writes are stalled (the exact condition that froze `last_seen` while the
/// node still reported `Healthy`, ISSUE-SCAN-2026-06-02 F4). Non-Healthy
/// stored states (Quarantined/Down/…) are authoritative and pass through.
pub(crate) fn effective_health(
    stored: dtfs_common::types::HealthStatus,
    last_seen_ms: u64,
    now_ms: u64,
    window_secs: u64,
) -> dtfs_common::types::HealthStatus {
    use dtfs_common::types::HealthStatus;
    if !matches!(stored, HealthStatus::Healthy) {
        return stored;
    }
    let window_ms = window_secs.saturating_mul(1000);
    // last_seen==0 (never reported) or window==0 (disabled) → leave as stored.
    if last_seen_ms == 0 || window_ms == 0 {
        return stored;
    }
    let age_ms = now_ms.saturating_sub(last_seen_ms);
    if age_ms > window_ms.saturating_mul(4) {
        HealthStatus::Down
    } else if age_ms > window_ms {
        HealthStatus::Suspect
    } else {
        HealthStatus::Healthy
    }
}

/// Closure projecting a node's stored health through `effective_health` using
/// the wall clock now and `[raft].node_liveness_secs`. Every read endpoint
/// must use it so a frozen-heartbeat node is not reported Healthy anywhere.
pub(crate) fn liveness_projector(
    state: &AppState,
) -> impl Fn(&RaftNodeInfo) -> dtfs_common::types::HealthStatus {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let window = state.config.raft.node_liveness_secs;
    move |n| effective_health(n.health, n.last_seen_ms, now_ms, window)
}

/// CF-4c — true iff `node_id` participates in raft membership as a voter or
/// a learner. A Manager-role node that is in neither set is registered/
/// configured but has not actually joined the cluster (the production dc1
/// symptom: roster showed `dtfs-dc1-mgr01 Manager Healthy` while it was an
/// empty term-0 non-member). The nodes projection uses this to flag such a
/// manager rather than render plain "Healthy".
pub(crate) fn is_raft_member(
    node_id: u64,
    voters: &BTreeSet<u64>,
    learners: &BTreeSet<u64>,
) -> bool {
    voters.contains(&node_id) || learners.contains(&node_id)
}

/// Bug BP-4 — count regular files that have no chunk bindings (realized data
/// loss: FileMetadata stranded with zero chunks, e.g. after GC deleted
/// orphaned chunks). Directories + symlinks legitimately have no chunks and
/// are excluded.
pub(crate) fn count_files_missing_chunks(sm: &dtfs_raft::state_machine::StateMachineData) -> usize {
    sm.files
        .values()
        .filter(|f| !f.is_directory && f.symlink_target.is_none() && f.chunks.is_empty())
        .count()
}

/// Bug BB-1 — per-tier `used_bytes` contribution of one node's disks.
/// Returns `(tier, used_bytes)` per disk. When the node ships ANY per-disk
/// usage (`used_bytes > 0` on at least one disk — the post-AU heartbeat),
/// every disk's real `used_bytes` is trusted, including 0 for a genuinely
/// empty disk. Only a legacy node that ships no per-disk usage at all
/// (every disk 0) AND has a node-level `used_capacity` falls back to
/// apportioning that capacity by each disk's share of the node total. This
/// fixes the BA-2 lie where the previous per-disk decision fabricated usage
/// onto an empty tier disk of an otherwise-used node.
pub(crate) fn node_tier_used(
    disks: &[dtfs_common::types::DiskInfo],
    node_used_capacity: u64,
) -> Vec<(DiskTier, u64)> {
    let node_cap: u64 = disks.iter().map(|d| d.total_bytes).sum();
    if node_cap == 0 {
        return Vec::new();
    }
    let reports_per_disk = disks.iter().any(|d| d.used_bytes > 0);
    disks
        .iter()
        .map(|d| {
            let used = if reports_per_disk {
                d.used_bytes
            } else {
                let share = d.total_bytes as f64 / node_cap as f64;
                (node_used_capacity as f64 * share) as u64
            };
            (d.tier, used)
        })
        .collect()
}

/// Bug BV-3 — number of registered data-plane nodes (role==Data only).
/// The shared predicate behind both `cluster_status.data_nodes` and
/// `stats.data_node_count`, so the two endpoints never disagree.
pub(super) fn count_data_nodes(sm: &StateMachineData) -> usize {
    sm.nodes.values().filter(|n| n.role == NodeRole::Data).count()
}

/// Bug BV-1 — files that have chunks but whose mandatory ANCHOR replica
/// isn't placed yet. `write_safety.min_replicas_before_ack` counts ANY
/// replicas toward the ack threshold, so a write can ack on warm replicas
/// before the cold anchor copy lands (convergence is async via the
/// anchor-lag scheduler). `files_missing_chunks:0` then hides this: the
/// chunks exist, just not on the anchor. This read-path projection makes
/// the anchor-pending state honest WITHOUT changing ack timing.
///
/// A file is anchor-pending when its volume has an effective anchor and
/// at least one of its chunks has no *serviceable* replica in the anchor
/// DC on the required tier (mirrors `scheduler::plan_anchor_lag_repair`'s
/// gate-1 after the LIS-41 live_replica_tier filter — Down/Failed nodes
/// do not count). Counted once per file.
/// Bug BV-1 — does any replica satisfy the anchor? A `(datacenter, tier)`
/// list satisfies an `AnchorPolicy` when some replica sits in the anchor
/// DC and (if `required_tier` is set) on that tier. Mirrors gate-1 of
/// `scheduler::plan_anchor_lag_repair`. Pure, so it's unit-tested directly.
pub(super) fn anchor_satisfied_by(
    placements: &[(String, dtfs_common::types::DiskTier)],
    anchor: &dtfs_common::types::AnchorPolicy,
) -> bool {
    placements.iter().any(|(dc, tier)| {
        dc.eq_ignore_ascii_case(&anchor.datacenter)
            && match anchor.required_tier {
                Some(t) => *tier == t,
                None => true,
            }
    })
}

pub(super) fn count_anchor_pending_files(sm: &StateMachineData) -> usize {
    use std::collections::BTreeMap;
    let mut pending = 0usize;
    for file_meta in sm.files.values() {
        let Some(volume) = sm.volumes.get(&file_meta.volume_id) else {
            continue;
        };
        let Some(anchor) = volume.effective_anchor() else {
            continue;
        };
        let mut by_idx: BTreeMap<u64, Vec<&dtfs_raft::ChunkLocationEntry>> = BTreeMap::new();
        for c in &file_meta.chunks {
            by_idx.entry(c.chunk_index).or_default().push(c);
        }
        let any_chunk_unanchored = by_idx.values().any(|entries| {
            // LIS-41 / Bug CH — only serviceable replicas count. A Down
            // node's dc3/cold row must not keep anchor_pending at 0.
            // CNF-1 disk_id keying lives inside live_replica_tier.
            let placements: Vec<(String, dtfs_common::types::DiskTier)> = entries
                .iter()
                .filter_map(|e| {
                    let nid = e.node_id.parse::<u64>().ok()?;
                    let info = sm.nodes.get(&nid)?;
                    let tier = crate::scheduler::live_replica_tier(Some(info), e.disk_id)?;
                    Some((info.location.datacenter.clone(), tier))
                })
                .collect();
            !anchor_satisfied_by(&placements, &anchor)
        });
        if any_chunk_unanchored {
            pending += 1;
        }
    }
    pending
}

/// Bug BX-6 — render the chunk placement-state distribution as JSON for
/// `/api/v1/stats.chunk_placement`. `pending_deletion` is wired through
/// from the classifier (orphan/GC accounting can populate it later).
pub(super) fn chunk_placement_json(sm: &StateMachineData) -> Value {
    let c = crate::placement_state::count_placement_states(sm);
    json!({
        "missing": c.missing,
        "undergoal": c.undergoal,
        "misplaced": c.misplaced,
        "stable": c.stable,
        "overgoal": c.overgoal,
        "pending_deletion": c.pending_deletion,
    })
}

pub(super) async fn cluster_status(State(state): State<Arc<AppState>>) -> Json<Value> {
    let sm = state.sm_data.read().await;
    let file_count = sm.files.len();
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let window = state.config.raft.node_liveness_secs;
    // Bug BV-3 — `data_nodes` must count only role==Data, matching
    // `stats.data_node_count`. Pre-fix it was `sm.nodes.len()`, so a
    // 3-manager + 2-data + 1-client roster reported `data_nodes:6`
    // (and `data_nodes_healthy:6`) — misleading operators/monitoring.
    let node_count = count_data_nodes(&sm);
    // Bug BP-3 — count effective (liveness-projected) health, not the stale
    // stored value, so a frozen-heartbeat node is not counted Healthy.
    // Bug BV-3 — restricted to role==Data (was every node).
    let healthy_count = sm
        .nodes
        .values()
        .filter(|n| n.role == NodeRole::Data)
        .filter(|n| {
            effective_health(n.health, n.last_seen_ms, now_ms, window)
                == dtfs_common::types::HealthStatus::Healthy
        })
        .count();
    // Bug BP-4 — files that lost all chunk bindings (realized data loss).
    let files_missing_chunks = count_files_missing_chunks(&sm);

    let total_capacity: u64 = sm.nodes.values().map(|n| n.total_capacity).sum();
    let used_capacity: u64 = sm.nodes.values().map(|n| n.used_capacity).sum();

    // Bug BP-4 — a bare "ok" masked 184 files at chunk_count=0. Reflect
    // realized data loss + degraded liveness in the top-line status.
    let status = if files_missing_chunks > 0 {
        "degraded"
    } else if healthy_count < node_count {
        "degraded"
    } else {
        "ok"
    };

    Json(json!({
        "status": status,
        "cluster_name": state.config.cluster.name,
        "node": state.config.node.hostname,
        "data_nodes": node_count,
        "data_nodes_healthy": healthy_count,
        "files": file_count,
        "files_missing_chunks": files_missing_chunks,
        // Bug BV-1 — files whose mandatory anchor copy hasn't landed yet
        // (chunks exist on non-anchor replicas; convergence is async). Not
        // data loss, so it does NOT degrade the top-line status — but it is
        // surfaced so `files_missing_chunks:0` no longer hides it.
        "anchor_pending": count_anchor_pending_files(&sm),
        "total_capacity": total_capacity,
        "used_capacity": used_capacity,
        "config_version": sm.cluster_config.version,
    }))
}

pub(super) async fn health_check() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({"status": "healthy"})))
}

// ---------------------------------------------------------------------------
// Join (HMAC handshake — `join::JoinHandler`)
// ---------------------------------------------------------------------------

/// Bug AL — pure helper accepting an explicit `now_ms` so the rate logic
/// is unit-testable without mocking `SystemTime::now()`. See
/// `compute_io_rate` for the production wrapper.
pub(crate) fn compute_io_rate_at(
    tracker: &mut IoRateTracker,
    nodes: &std::collections::BTreeMap<u64, dtfs_raft::NodeInfo>,
    now_ms: u64,
) -> (u64, u64) {
    let window_ms = now_ms.saturating_sub(tracker.last_query_ms);
    // Sub-second back-to-back call: return the cached rate to avoid
    // dividing by tiny numbers (spiky display).
    if tracker.last_query_ms != 0 && window_ms < 1_000 {
        return (tracker.last_read_bps, tracker.last_write_bps);
    }

    let mut delta_read: u64 = 0;
    let mut delta_written: u64 = 0;
    let mut new_snapshot: std::collections::HashMap<u64, IoSnapshot> =
        std::collections::HashMap::with_capacity(nodes.len());

    for (node_id, info) in nodes {
        let snap = IoSnapshot {
            total_bytes_read: info.total_bytes_read,
            total_bytes_written: info.total_bytes_written,
            observed_at_ms: now_ms,
        };
        if let Some(prev) = tracker.previous.get(node_id) {
            // Cumulative counters only grow; if they appear to have
            // gone backwards (process restart, host clock jump), the
            // saturating_sub yields 0 and we just skip the delta.
            delta_read = delta_read
                .saturating_add(info.total_bytes_read.saturating_sub(prev.total_bytes_read));
            delta_written = delta_written
                .saturating_add(info.total_bytes_written.saturating_sub(prev.total_bytes_written));
        }
        new_snapshot.insert(*node_id, snap);
    }

    let read_bps;
    let write_bps;
    if tracker.last_query_ms == 0 {
        // First call ever — no baseline, can't divide.
        read_bps = 0;
        write_bps = 0;
    } else {
        let window_secs = (window_ms / 1_000).max(1);
        read_bps = delta_read / window_secs;
        write_bps = delta_written / window_secs;
    }

    tracker.previous = new_snapshot;
    tracker.last_query_ms = now_ms;
    tracker.last_read_bps = read_bps;
    tracker.last_write_bps = write_bps;
    (read_bps, write_bps)
}

/// Bug BH-8 — count (healthy, total) over cluster *infrastructure*:
/// managers + data nodes. Clients (FUSE mounts) are consumers and must
/// not drag the dashboard health ratio down.
///
/// Managers live in `cluster.initial_voters` (config), not in
/// `sm.nodes` — so they cannot carry a per-node `HealthStatus` here and
/// are counted as healthy voters (a manager fault surfaces via leader
/// election / quorum loss, not this banner). Data-node health is real.
/// Numerator and denominator are built from the same two inputs so they
/// can never diverge over mismatched node sets (the original bug).
pub(crate) fn infra_health_counts<'a>(
    manager_count: usize,
    data_health: impl Iterator<Item = &'a HealthStatus>,
) -> (usize, usize) {
    let mut data_total = 0usize;
    let mut data_healthy = 0usize;
    for h in data_health {
        data_total += 1;
        if *h == HealthStatus::Healthy {
            data_healthy += 1;
        }
    }
    (manager_count + data_healthy, manager_count + data_total)
}

/// REV-4-3 — `write_to_disk` replaces the file the manager parses at
/// startup. CO-4-3 stopped the raw-JSON fallback; these cover what was
/// still missing: proving the *rendered TOML* re-parses through the
/// startup deserialiser, fsyncing before the rename, and never leaving a
/// temp file (or a truncated config) behind on an error path.
pub(super) async fn get_stats(State(state): State<Arc<AppState>>) -> Json<Value> {
    use dtfs_common::types::{DiskTier, HealthStatus, NodeRole};

    let sm = state.sm_data.read().await;

    // ---- Node counts -----------------------------------------------------
    // Managers never register through the data-node heartbeat path so
    // `sm.nodes` only contains data nodes. The authoritative manager set
    // is the initial voters list (operator-supplied at bootstrap; matches
    // the Raft membership after any subsequent add/remove). Using it here
    // makes `manager_count` consistent with the WebUI's expectation even
    // though `sm.nodes.values().filter(role==Manager)` is always 0.
    let manager_count = state.config.cluster.initial_voters.len();

    let data_node_count = sm
        .nodes
        .values()
        .filter(|n| n.role == NodeRole::Data)
        .count();
    let disk_count: usize = sm.nodes.values().map(|n| n.disks.len()).sum();

    // Bug AQ — count unique FUSE clients via the SAME merge logic the
    // /api/v1/nodes endpoint uses for the Nodes table: clients
    // self-registered as `sm.nodes` entries with role=Client (Bug AA)
    // PLUS any lease-derived host that isn't already represented.
    // Pre-fix this counted only write-lease holders, so a read-only
    // Plex playback client never appeared — Home showed "1 FUSE client"
    // while the Nodes page showed 3.
    let client_count: u64 = {
        let mut hosts: std::collections::HashSet<String> = sm
            .nodes
            .values()
            .filter(|n| n.role == NodeRole::Client)
            .map(|n| n.hostname.clone())
            .collect();
        for lease in sm.write_leases.values() {
            if let Some(host) = lease.owner_id.split(':').next() {
                if !host.is_empty() {
                    hosts.insert(host.to_string());
                }
            }
        }
        hosts.len() as u64
    };

    // ---- Capacity aggregates --------------------------------------------
    let raw_total: u64 = sm.nodes.values().map(|n| n.total_capacity).sum();
    let raw_used: u64 = sm.nodes.values().map(|n| n.used_capacity).sum();

    // Per-tier breakdown of capacity + used so the WebUI can show how data
    // is distributed across hot/warm/cold/tape. Used is currently computed
    // per-node (not per-disk), so per-tier used is best-effort: we
    // proportionally split a node's `used_capacity` across its disks by
    // their capacity share within the node.
    let mut tier_total: std::collections::HashMap<DiskTier, u64> = Default::default();
    let mut tier_used: std::collections::HashMap<DiskTier, u64> = Default::default();
    let mut tier_disks: std::collections::HashMap<DiskTier, u32> = Default::default();
    for n in sm.nodes.values() {
        let node_cap: u64 = n.disks.iter().map(|d| d.total_bytes).sum();
        if node_cap == 0 {
            continue;
        }
        for d in &n.disks {
            *tier_total.entry(d.tier).or_default() += d.total_bytes;
            *tier_disks.entry(d.tier).or_default() += 1;
        }
        // Bug BB-1: per-tier used is summed from REAL per-disk `used_bytes`
        // when the node reports any (post-AU heartbeat ships per-disk usage),
        // including 0 for a genuinely-empty disk. Only a legacy node that
        // ships NO per-disk usage at all falls back to apportioning its
        // node-level `used_capacity` by each disk's capacity share. The old
        // code decided this PER-DISK (`used_bytes > 0`), which fabricated
        // usage onto an empty tier disk of an otherwise-used node
        // (BA-2 apportionment lie).
        for (tier, used) in node_tier_used(&n.disks, n.used_capacity) {
            *tier_used.entry(tier).or_default() += used;
        }
    }
    let tier_breakdown: Vec<Value> = [
        DiskTier::Hot,
        DiskTier::Warm,
        DiskTier::Cold,
        DiskTier::Tape,
    ]
    .iter()
    .map(|t| {
        let label = match t {
            DiskTier::Hot => "hot",
            DiskTier::Warm => "warm",
            DiskTier::Cold => "cold",
            DiskTier::Tape => "tape",
        };
        json!({
            "tier":  label,
            "disks": tier_disks.get(t).copied().unwrap_or(0),
            "total_bytes": tier_total.get(t).copied().unwrap_or(0),
            "used_bytes":  tier_used.get(t).copied().unwrap_or(0),
        })
    })
    .collect();

    // Bug AJ — net usable / net used must divide each tier's raw figure by
    // THAT tier's replication factor, then sum. The previous code divided
    // total raw by `hot_replication_factor` only, which broke in two ways:
    //
    //   1. On clusters with no hot tier (Plexmedia: hot=0), `hot_rf.max(1)`
    //      becomes 1 → no division → `net_used == raw_used`. Dashboard then
    //      lied that every byte of raw storage was usable space.
    //   2. Even with hot_rf > 0, using a SINGLE replication factor across
    //      hot+warm+cold+tape misses the per-tier reality (Plexmedia:
    //      warm=2, cold=1) — a warm-rf=2 disk pool reports 2× actual user
    //      data; a cold-rf=1 disk pool reports 1×. Mixing them with one
    //      divisor produces a fiction.
    //
    // Read the default per-tier replication from cluster config; fall back
    // to 1 (best-case net == raw) for any tier that's effectively disabled
    // (rf == 0). The result is a meaningful "how much user data this
    // cluster can hold" number that survives tier mix changes.
    let repl = &state.config.defaults.replication;
    let net_for_tier = |tier: DiskTier, raw_in_tier: u64| -> u64 {
        let rf = match tier {
            DiskTier::Hot => repl.hot,
            DiskTier::Warm => repl.warm,
            DiskTier::Cold => repl.cold,
            DiskTier::Tape => repl.tape,
        };
        let divisor = (rf as u64).max(1);
        raw_in_tier / divisor
    };
    let net_usable: u64 = tier_total
        .iter()
        .map(|(t, raw)| net_for_tier(*t, *raw))
        .sum();
    let net_used: u64 = tier_used
        .iter()
        .map(|(t, raw)| net_for_tier(*t, *raw))
        .sum();

    // Bug AL — true rolling rate (delta cumulative ÷ time-since-last-call)
    // computed in a process-local tracker. See `compute_io_rate` for
    // the full design rationale.
    let (io_read_bps, io_write_bps) = {
        let mut tracker = state.io_rate.write();
        compute_io_rate(&mut tracker, &sm.nodes)
    };

    // Bug BH-8 — infrastructure (managers + data) health for the
    // dashboard banner. Managers come from config voters; data-node
    // health from sm.nodes. Clients excluded so a FUSE mount can't drag
    // the ratio. Same two inputs feed numerator and denominator.
    let eff = liveness_projector(&state);
    let data_health: Vec<HealthStatus> = sm
        .nodes
        .values()
        .filter(|n| n.role == NodeRole::Data)
        .map(|n| eff(n))
        .collect();
    let (infra_healthy, infra_total) = infra_health_counts(manager_count, data_health.iter());

    // Overall cluster health.
    let total_nodes = sm.nodes.len();
    let healthy_nodes = sm
        .nodes
        .values()
        .filter(|n| eff(n) == HealthStatus::Healthy)
        .count();
    let degraded_nodes = sm
        .nodes
        .values()
        .filter(|n| {
            matches!(
                eff(n),
                HealthStatus::Suspect | HealthStatus::Degraded
            )
        })
        .count();
    // Bug BC-11 — count quarantined nodes separately so /api/v1/stats
    // surfaces the integrity-fault state without forcing the operator
    // to scrape Prometheus or enumerate /api/v1/nodes by hand.
    let quarantined_nodes = sm
        .nodes
        .values()
        .filter(|n| matches!(eff(n), HealthStatus::Quarantined))
        .count();
    let health = if total_nodes == 0 {
        "unknown"
    } else if quarantined_nodes > 0 {
        // A Quarantined node = data-integrity event in progress. Even
        // if every other node is Healthy, the cluster is no longer
        // "healthy" — the operator needs to look at the integrity feed.
        // We surface "critical" so dashboards page on this state.
        "critical"
    } else if degraded_nodes > 0 || healthy_nodes < total_nodes {
        if healthy_nodes == 0 {
            "critical"
        } else {
            "degraded"
        }
    } else {
        "healthy"
    };

    // Per (node, level) scrub results (BK — previously keyed per-node only,
    // so a deep scrub overwrote the quick result). Flatten the nested map to
    // one JSON object per (node, level); the WebUI lists every entry.
    let scrub_json = |s: &dtfs_raft::state_machine::ScrubRecord| {
        json!({
            "node_id":         s.node_id,
            "disk_path":       s.disk_path,
            "level":           s.level,
            "completed_at_ms": s.completed_at_ms,
            "chunks_checked":  s.chunks_checked,
            "chunks_healthy":  s.chunks_healthy,
            "chunks_corrupt":  s.chunks_corrupt,
            "chunks_missing":  s.chunks_missing,
            "duration_secs":   s.duration_secs,
        })
    };
    let scrub_results_by_node: Vec<Value> = sm
        .scrub_results_by_level
        .values()
        .flat_map(|by_level| by_level.values())
        .map(scrub_json)
        .collect();

    // Backward-compatible `last_scrub` field: the most recent result across
    // all (node, level) records, or null when no scrubs have run yet.
    let last_scrub = sm
        .scrub_results_by_level
        .values()
        .flat_map(|by_level| by_level.values())
        .max_by_key(|s| s.completed_at_ms)
        .map(scrub_json);

    // Bug AK — logical user-visible bytes across all files. Sum per-file
    // sizes deduped by chunk_index (replicas don't multiply user data).
    // The dashboard contrasts this with `net_used_bytes` (data nodes'
    // actual on-disk usage divided by tier RF). In a healthy cluster the
    // two should match within rounding. When `logical_bytes >>
    // net_used_bytes`, the cluster is carrying GHOST METADATA — files
    // whose chunks were physically lost (data-node wipe, ENOSPC unlink
    // cascade like Bug AD, etc.) but whose FileMetadata still references
    // them. `phantom_bytes_estimate` exposes the delta so operators can
    // see ghost accumulation without diffing two endpoints by hand.
    let logical_bytes: u64 = sm
        .files
        .values()
        .map(|f| unique_chunk_bytes(&f.chunks))
        .sum();
    let phantom_bytes_estimate = logical_bytes.saturating_sub(net_used);

    Json(json!({
        "health":                health,
        "healthy_nodes":         healthy_nodes,
        // Bug BC-11 — non-zero means at least one node has been
        // quarantined after an integrity fault. Routing already
        // excludes them; the counter is what the WebUI badge reads.
        "quarantined_nodes":     quarantined_nodes,
        "total_nodes":           total_nodes,
        // Bug BV-1 — files whose mandatory anchor copy hasn't landed yet
        // (durability-pending, async convergence; not data loss).
        "anchor_pending":        count_anchor_pending_files(&sm),
        // Bug BX-6 — cluster-wide chunk placement-state distribution, one
        // count per logical chunk. The contract the Bug BT WebUI
        // chunk-redundancy panel renders.
        "chunk_placement":       chunk_placement_json(&sm),
        // Bug BH-8 — banner ratio over cluster infrastructure only
        // (managers + data); clients are consumers, not cluster health.
        // Numerator/denominator are taken from one explicit set so they
        // can never diverge.
        "infra_healthy":         infra_healthy,
        "infra_total":           infra_total,
        "manager_count":         manager_count,
        "data_node_count":       data_node_count,
        "disk_count":            disk_count,
        "client_count":          client_count,
        // Legacy field kept so older WebUI builds don't break; matches
        // sm.client_count for forward compat when client heartbeats land.
        "registered_clients":    sm.client_count,
        "file_count":            sm.files.len(),
        "raw_total_bytes":       raw_total,
        "raw_used_bytes":        raw_used,
        "net_usable_bytes":      net_usable,
        "net_used_bytes":        net_used,
        // Bug AK — see above.
        "logical_bytes":         logical_bytes,
        "phantom_bytes_estimate": phantom_bytes_estimate,
        "tier_breakdown":        tier_breakdown,
        "io_read_bytes_per_sec":  io_read_bps,
        "io_write_bytes_per_sec": io_write_bps,
        "last_scrub":            last_scrub,
        "scrub_results_by_node": scrub_results_by_node,
        "config_version":        sm.cluster_config.version,
    }))
}

// ---------------------------------------------------------------------------
// Scrub trigger endpoint
// ---------------------------------------------------------------------------

/// `GET /metrics` — Prometheus text-format scrape.
pub(super) async fn prometheus_metrics(
    State(state): State<Arc<AppState>>,
) -> (axum::http::StatusCode, [(&'static str, &'static str); 1], String) {
    let sm = state.sm_data.read().await;

    let total_files = sm.files.len() as u64;
    let total_volumes = sm.volumes.len() as u64;
    let total_chunks: u64 = sm.files.values().map(|f| f.chunks.len() as u64).sum();
    let active_leases = sm.write_leases.len() as u64;

    let mut healthy = 0u64;
    let mut suspect = 0u64;
    let mut down = 0u64;
    let mut quarantined = 0u64;
    let mut total_capacity: u64 = 0;
    let mut used_capacity: u64 = 0;
    let eff = liveness_projector(&state);
    for info in sm.nodes.values() {
        match eff(info) {
            dtfs_common::types::HealthStatus::Healthy => healthy += 1,
            dtfs_common::types::HealthStatus::Suspect => suspect += 1,
            dtfs_common::types::HealthStatus::Down => down += 1,
            // Bug BC-11 — surface quarantined node count in cluster
            // status so the operator sees a non-zero counter the moment
            // an integrity fault fires, without having to enumerate
            // /api/v1/nodes by hand.
            dtfs_common::types::HealthStatus::Quarantined => quarantined += 1,
            _ => {}
        }
        total_capacity = total_capacity.saturating_add(info.total_capacity);
        used_capacity = used_capacity.saturating_add(info.used_capacity);
    }

    let mut anchor_protected: u64 = 0;
    let mut migration_failed: u64 = 0;
    let mut migration_completed: u64 = 0;
    for entry in &sm.audit_log {
        let s = entry.summary.as_str();
        if s.contains("tier.anchor_protected") {
            anchor_protected += 1;
        }
        if s.contains("migration.failed") {
            migration_failed += 1;
        }
        if s.contains("tier.promote") || s.contains("tier.demote") {
            migration_completed += 1;
        }
    }

    let raft_log_size = sm.audit_log.len() as u64;

    let body = format!(
        "# HELP dtfs_files_total Number of files tracked by the cluster.\n\
         # TYPE dtfs_files_total gauge\n\
         dtfs_files_total {total_files}\n\
         # HELP dtfs_chunks_total Total chunk-location entries.\n\
         # TYPE dtfs_chunks_total gauge\n\
         dtfs_chunks_total {total_chunks}\n\
         # HELP dtfs_volumes_total Number of configured volumes.\n\
         # TYPE dtfs_volumes_total gauge\n\
         dtfs_volumes_total {total_volumes}\n\
         # HELP dtfs_nodes_healthy Number of nodes in Healthy state.\n\
         # TYPE dtfs_nodes_healthy gauge\n\
         dtfs_nodes_healthy {healthy}\n\
         # HELP dtfs_nodes_suspect Number of nodes in Suspect state.\n\
         # TYPE dtfs_nodes_suspect gauge\n\
         dtfs_nodes_suspect {suspect}\n\
         # HELP dtfs_nodes_down Number of nodes in Down state.\n\
         # TYPE dtfs_nodes_down gauge\n\
         dtfs_nodes_down {down}\n\
         # HELP dtfs_nodes_quarantined Bug BC: nodes Quarantined after integrity fault.\n\
         # TYPE dtfs_nodes_quarantined gauge\n\
         dtfs_nodes_quarantined {quarantined}\n\
         # HELP dtfs_cluster_capacity_bytes Total cluster capacity in bytes.\n\
         # TYPE dtfs_cluster_capacity_bytes gauge\n\
         dtfs_cluster_capacity_bytes {total_capacity}\n\
         # HELP dtfs_cluster_used_bytes Used cluster capacity in bytes.\n\
         # TYPE dtfs_cluster_used_bytes gauge\n\
         dtfs_cluster_used_bytes {used_capacity}\n\
         # HELP dtfs_write_leases_active Currently held single-writer leases.\n\
         # TYPE dtfs_write_leases_active gauge\n\
         dtfs_write_leases_active {active_leases}\n\
         # HELP dtfs_anchor_protected_total Tiering decisions refused to protect anchor DC.\n\
         # TYPE dtfs_anchor_protected_total counter\n\
         dtfs_anchor_protected_total {anchor_protected}\n\
         # HELP dtfs_tier_migrations_completed_total Completed tier migrations (promote+demote).\n\
         # TYPE dtfs_tier_migrations_completed_total counter\n\
         dtfs_tier_migrations_completed_total {migration_completed}\n\
         # HELP dtfs_migration_failed_total Migration tasks that exhausted retries.\n\
         # TYPE dtfs_migration_failed_total counter\n\
         dtfs_migration_failed_total {migration_failed}\n\
         # HELP dtfs_audit_log_in_memory Audit log entries held in the FSM (ring-buffered).\n\
         # TYPE dtfs_audit_log_in_memory gauge\n\
         dtfs_audit_log_in_memory {raft_log_size}\n",
    );

    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        body,
    )
}

/// `GET /healthz` — liveness probe. Always 200 if the process responds.
pub(super) async fn healthz_probe() -> (axum::http::StatusCode, &'static str) {
    (axum::http::StatusCode::OK, "ok\n")
}

/// `GET /readyz` — readiness probe. 200 only when at least one data node is
/// healthy AND the state machine has applied at least one log entry (so the
/// manager has finished its Raft warmup); 503 otherwise so K8s/systemd can
/// route around an in-recovery manager.
pub(super) async fn readyz_probe(State(state): State<Arc<AppState>>) -> (axum::http::StatusCode, String) {
    let eff = liveness_projector(&state);
    let sm = state.sm_data.read().await;
    let any_healthy = sm
        .nodes
        .values()
        .any(|n| eff(n) == dtfs_common::types::HealthStatus::Healthy);
    let fsm_initialised = sm.last_applied_log.is_some();
    drop(sm);

    if fsm_initialised && any_healthy {
        (axum::http::StatusCode::OK, "ready\n".to_string())
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            format!(
                "not ready: fsm_initialised={fsm_initialised} any_healthy={any_healthy}\n"
            ),
        )
    }
}

// ---------------------------------------------------------------------------
// Bug AI — pure helper for audit-entry display backfill (unit-tested below)
// ---------------------------------------------------------------------------

/// Produce `(actor, action, target, result)` for the WebUI Audit table.
///
/// When the entry already carries structured fields (i.e. an `AppendAudit`
/// fan-out from `audit.log()` — Bug AH), return them unchanged. When the
/// entry is a legacy `apply_request`-generated one (only `summary` populated),
/// parse `summary` in the form `"VariantName: details"` to synthesise
/// `action` and `target`, set `actor="raft"`, and `result="success"` (the
/// state-machine apply path only pushes audit on the success branch — failed
/// applies return before reaching the `audit()` helper, so any entry
/// reaching us is by construction a success).
///
/// Public-in-crate so the unit tests below can call it without going
/// through the HTTP handler.
pub(crate) fn backfill_audit_fields(
    actor: &str,
    action: &str,
    target: &str,
    result: &str,
    summary: &str,
) -> (String, String, String, String) {
    if !action.is_empty() {
        return (
            actor.to_string(),
            action.to_string(),
            target.to_string(),
            result.to_string(),
        );
    }
    let (action_part, target_part) = match summary.split_once(": ") {
        Some((a, t)) => (a.to_string(), t.to_string()),
        None => (summary.to_string(), String::new()),
    };
    (
        "raft".to_string(),
        action_part,
        target_part,
        "success".to_string(),
    )
}
