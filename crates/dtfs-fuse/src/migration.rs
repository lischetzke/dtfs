// Background migration trigger on remote reads — Phase 2 (FUSE Client).
//
// When a FUSE client reads a chunk from a data node that is topologically
// far away (different rack, datacenter, or region), this module detects
// the remote read and requests that the manager migrate a replica closer.
//
// Requests are rate-limited per chunk to avoid flooding the manager with
// duplicate migration requests.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::{debug, info, instrument, warn};

use dtfs_client::client::DtfsClient;
use dtfs_common::topology::{Location, LocationDistance};

/// Minimum interval between repeated migration requests for the same chunk.
const MIGRATION_COOLDOWN: Duration = Duration::from_secs(5 * 60); // 5 minutes

/// Threshold distance at which a read is considered "remote" and should
/// trigger a migration request. Reads from the same rack or same host
/// are considered local.
const MIGRATION_DISTANCE_THRESHOLD: LocationDistance = LocationDistance::SameDatacenter;

/// WI-09 / H4 — hard bound on the cooldown map. Its entries are only
/// useful for `MIGRATION_COOLDOWN`, and this crate runs no periodic
/// pruner (`prune_expired` had no caller), so without a bound the map
/// grows one entry per remote-read chunk for the process lifetime — an
/// unbounded heap leak on the client whose whole point is a
/// `hard_cap_mb` budget (A.3.2). 4096 entries is ~0.5 MB and covers far
/// more than one `MIGRATION_COOLDOWN` window of cross-DC misses. The
/// cooldown is an advisory rate limiter, not correctness state, so
/// dropping it at the ceiling costs at most one extra advisory RPC per
/// chunk.
const MAX_PENDING_MIGRATIONS: usize = 4096;

/// Triggers background data migration when remote reads are detected.
///
/// The FUSE client calls [`on_remote_read`](Self::on_remote_read) after
/// every chunk read. If the data node serving the chunk is topologically
/// far from the client, the trigger requests the manager to place a replica
/// closer. A per-chunk cooldown prevents spamming the manager.
pub struct MigrationTrigger {
    client: DtfsClient,
    /// The FUSE client's own location in the topology.
    client_location: Location,
    /// Tracks when each chunk was last requested for migration.
    /// Key: chunk_id, Value: timestamp of last request.
    pending_migrations: DashMap<String, Instant>,
}

impl MigrationTrigger {
    /// Create a new migration trigger.
    pub fn new(client: DtfsClient, client_location: Location) -> Self {
        Self {
            client,
            client_location,
            pending_migrations: DashMap::new(),
        }
    }

    /// WI-09 — fire-and-forget wrapper around [`on_remote_read`](Self::on_remote_read).
    ///
    /// This is an advisory hint: the manager RPC it issues
    /// (`allocate_chunks`) is read-only on the manager side and its
    /// response is discarded here, so it triggers no actual data movement
    /// by itself. It must never block the caller's read path — the RPC
    /// runs through `DtfsClient::call_manager`, which sets no per-call
    /// deadline and retries `Unavailable` by walking the manager pool for
    /// up to `MANAGER_WALK_DEADLINE_MS` (15s), against a channel connect
    /// timeout of 5s and a request timeout of 60s. Awaiting it inline
    /// after the chunk bytes are already in hand can stall a completed
    /// read for that long during an election or against a blackholed
    /// manager. Spawn it instead and let it run to completion (or fail) in
    /// the background.
    ///
    /// Takes an owned `Arc<Self>` (rather than `&self`) so it can move a
    /// live handle into the spawned task; callers hold the trigger behind
    /// an `Arc` and pass a cheap `Arc::clone`.
    pub fn spawn_on_remote_read(
        self: Arc<Self>,
        chunk_id: &str,
        node_location: Location,
        file_path: &str,
        volume_id: &str,
    ) {
        let chunk_id = chunk_id.to_string();
        let file_path = file_path.to_string();
        let volume_id = volume_id.to_string();
        tokio::spawn(async move {
            self.on_remote_read(&chunk_id, &node_location, &file_path, &volume_id)
                .await;
        });
    }

    /// Called after a chunk is read from a data node. Determines whether the
    /// read was "remote" and, if so, requests migration closer to the client.
    ///
    /// # Parameters
    /// - `chunk_id`: the chunk that was read
    /// - `node_location`: the topology location of the data node that served
    ///   the read
    /// - `file_path`: the file path (used for the migration request)
    /// - `volume_id`: the volume ID (used for allocation)
    #[instrument(skip(self), fields(chunk_id = %chunk_id, node = %node_location))]
    pub async fn on_remote_read(
        &self,
        chunk_id: &str,
        node_location: &Location,
        file_path: &str,
        volume_id: &str,
    ) {
        let distance = self.client_location.distance(node_location);

        // Only trigger migration if the read crossed a significant boundary.
        if distance < MIGRATION_DISTANCE_THRESHOLD {
            debug!(
                chunk_id,
                ?distance,
                "read is local, no migration needed"
            );
            return;
        }

        // Rate-limit: check cooldown.
        if let Some(last_request) = self.pending_migrations.get(chunk_id) {
            if last_request.elapsed() < MIGRATION_COOLDOWN {
                debug!(
                    chunk_id,
                    elapsed_secs = last_request.elapsed().as_secs(),
                    cooldown_secs = MIGRATION_COOLDOWN.as_secs(),
                    "migration request rate-limited"
                );
                return;
            }
        }

        // H4 — bound the map before growing it further. Try a prune first
        // (cheap, and the common case where the ceiling is mostly expired
        // entries); if it's still at the ceiling with live entries, the
        // cooldown is doing its job under sustained cross-DC traffic and
        // there's nothing more useful to prune, so clear it outright. This
        // is an advisory rate limiter, not correctness state (see the Err
        // arm below), so losing cooldowns at the ceiling is harmless.
        if self.pending_migrations.len() >= MAX_PENDING_MIGRATIONS {
            self.prune_expired();
            if self.pending_migrations.len() >= MAX_PENDING_MIGRATIONS {
                warn!(
                    entries = self.pending_migrations.len(),
                    "migration cooldown map at its ceiling with live entries; clearing (advisory rate limiter only)"
                );
                self.pending_migrations.clear();
            }
        }

        // Record the request timestamp (before the async call to prevent
        // concurrent duplicate requests).
        self.pending_migrations
            .insert(chunk_id.to_string(), Instant::now());

        info!(
            chunk_id,
            ?distance,
            client_location = %self.client_location,
            node_location = %node_location,
            "requesting migration for remote chunk"
        );

        // Request the manager to allocate a new placement closer to us.
        // The manager will handle the actual data movement.
        match self.client.allocate_chunks(file_path, 1, volume_id).await {
            Ok(response) => {
                debug!(
                    chunk_id,
                    placements = response.placements.len(),
                    "migration allocation request accepted"
                );
            }
            Err(e) => {
                warn!(
                    chunk_id,
                    error = %e,
                    "migration allocation request failed"
                );
                // WI-09 — do NOT remove the pending entry here. This is a
                // fire-and-forget advisory hint now (see
                // spawn_on_remote_read); a manager outage means every
                // concurrent remote cache miss for this chunk hits this
                // Err arm, and clearing the cooldown on failure would spawn
                // one RPC per miss instead of one per chunk per
                // MIGRATION_COOLDOWN. The entry was already inserted above
                // regardless of outcome, so leaving it in place here is a
                // no-op for the success path and a rate limiter for the
                // failure path.
            }
        }
    }

    /// Check whether a chunk has a pending (recently requested) migration.
    pub fn is_pending(&self, chunk_id: &str) -> bool {
        self.pending_migrations
            .get(chunk_id)
            .map(|ts| ts.elapsed() < MIGRATION_COOLDOWN)
            .unwrap_or(false)
    }

    /// Number of chunks with active migration cooldowns.
    pub fn pending_count(&self) -> usize {
        self.pending_migrations.len()
    }

    /// Prune expired cooldown entries to free memory. Called from
    /// `on_remote_read` when the map reaches `MAX_PENDING_MIGRATIONS`.
    pub fn prune_expired(&self) {
        let mut to_remove = Vec::new();
        for entry in self.pending_migrations.iter() {
            if entry.value().elapsed() >= MIGRATION_COOLDOWN {
                to_remove.push(entry.key().clone());
            }
        }
        for key in to_remove {
            self.pending_migrations.remove(&key);
        }
    }

    /// Clear all pending migration state.
    pub fn clear(&self) {
        self.pending_migrations.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_location(region: &str, dc: &str) -> Location {
        Location::new(region, dc, "rack-1", "host-1")
    }

    #[test]
    fn test_migration_cooldown_tracking() {
        let client = DtfsClient::new();
        let trigger = MigrationTrigger::new(client, make_location("us", "east"));

        // Not pending initially.
        assert!(!trigger.is_pending("chunk-1"));

        // Simulate adding a pending entry.
        trigger
            .pending_migrations
            .insert("chunk-1".to_string(), Instant::now());
        assert!(trigger.is_pending("chunk-1"));
        assert_eq!(trigger.pending_count(), 1);

        // Clear.
        trigger.clear();
        assert_eq!(trigger.pending_count(), 0);
    }

    #[test]
    fn test_prune_expired() {
        let client = DtfsClient::new();
        let trigger = MigrationTrigger::new(client, make_location("us", "east"));

        // Insert an entry that is already expired.
        trigger.pending_migrations.insert(
            "old-chunk".to_string(),
            Instant::now() - Duration::from_secs(600),
        );

        // Insert a recent entry.
        trigger
            .pending_migrations
            .insert("new-chunk".to_string(), Instant::now());

        assert_eq!(trigger.pending_count(), 2);
        trigger.prune_expired();
        assert_eq!(trigger.pending_count(), 1);
        assert!(!trigger.is_pending("old-chunk"));
        assert!(trigger.is_pending("new-chunk"));
    }

    /// WI-09 / M2 — `spawn_on_remote_read` must run none of
    /// `on_remote_read`'s body on the caller's stack; it must only spawn a
    /// task that runs it in the background.
    ///
    /// This is an ORDERING check, deterministic on the current-thread test
    /// runtime (`#[tokio::test]` default): a `tokio::spawn`ed task is not
    /// polled until this function's next `.await`. So if any of
    /// `on_remote_read` ran inline, its FIRST side effect — the cooldown
    /// insert, which happens before the RPC — would already be visible
    /// immediately when `spawn_on_remote_read` returns, before this test
    /// ever yields. Do NOT add `flavor = "multi_thread"`: the determinism
    /// depends on the current-thread runtime, where nothing else can run
    /// the spawned task ahead of schedule.
    ///
    /// The `known_locations` half of the WI-09 root cause (the caller must
    /// drop its DashMap `Ref` guard before spawning) is structural, not
    /// testable here — `filesystem.rs` binds the location with
    /// `.map(|r| r.clone())` so the guard drops before the spawn (CLAUDE.md
    /// A.8 / CO-3-12) — and is gated live by
    /// `tests/release/cases/99-read-survives-manager-outage.sh`.
    #[tokio::test]
    async fn wi09_spawn_on_remote_read_runs_nothing_inline() {
        // No managers configured: `call_manager` bails instantly with "no
        // managers configured" — no sockets touched.
        let client = DtfsClient::new();
        let trigger = Arc::new(MigrationTrigger::new(client, make_location("us", "east")));
        // A different datacenter (same region) is >= MIGRATION_DISTANCE_THRESHOLD
        // (SameDatacenter), so the RPC path is taken rather than the
        // early-return "local read" path.
        let node_location = make_location("us", "west");

        trigger
            .clone()
            .spawn_on_remote_read("chunk-1", node_location, "/movie.mkv", "vol1");

        assert!(
            !trigger.is_pending("chunk-1"),
            "on_remote_read ran on the caller's stack - spawn_on_remote_read must not execute any of it inline"
        );

        for _ in 0..64 {
            if trigger.is_pending("chunk-1") {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            trigger.is_pending("chunk-1"),
            "the spawned task never ran - the hint must still be issued in the background, not dropped"
        );
    }

    /// WI-09 — a failed RPC (manager unreachable) must NOT clear the
    /// cooldown entry, or a manager outage turns the 5-minute
    /// `MIGRATION_COOLDOWN` into a spawn-per-remote-cache-miss storm.
    #[tokio::test]
    async fn failed_request_keeps_cooldown() {
        // No managers configured: `call_manager` bails instantly with "no
        // managers configured" — no socket touched, no delay.
        let client = DtfsClient::new();
        let trigger = MigrationTrigger::new(client, make_location("us", "east"));
        let node_location = make_location("us", "west");

        trigger
            .on_remote_read("chunk-1", &node_location, "/movie.mkv", "vol1")
            .await;

        assert!(
            trigger.is_pending("chunk-1"),
            "a failed migration RPC must not clear the cooldown entry"
        );
        assert_eq!(trigger.pending_count(), 1);
    }

    /// H4 — `pending_migrations` must never grow past `MAX_PENDING_MIGRATIONS`,
    /// whether the ceiling is reached with mostly-expired entries (the prune
    /// path keeps the map bounded without losing the fresh entry) or with
    /// mostly-live entries (the clear path fires; the cooldown is advisory
    /// only, so losing it is acceptable).
    #[tokio::test]
    async fn h4_pending_migrations_is_bounded() {
        // No managers configured, so on_remote_read's RPC returns fast.
        let client = DtfsClient::new();
        let trigger = MigrationTrigger::new(client, make_location("us", "east"));
        let node_location = make_location("us", "west");

        // Fill the map to the ceiling with expired entries.
        for i in 0..MAX_PENDING_MIGRATIONS {
            trigger.pending_migrations.insert(
                format!("expired-{i}"),
                Instant::now() - Duration::from_secs(600),
            );
        }
        assert_eq!(trigger.pending_count(), MAX_PENDING_MIGRATIONS);

        trigger
            .on_remote_read("fresh", &node_location, "/f", "vol1")
            .await;

        assert!(
            trigger.pending_count() <= MAX_PENDING_MIGRATIONS,
            "prune path should have kept the map at or under the ceiling, got {}",
            trigger.pending_count()
        );
        assert!(
            trigger.is_pending("fresh"),
            "the prune path must keep the newly inserted entry"
        );

        // Refill the map to the ceiling with live (non-expired) entries.
        trigger.clear();
        for i in 0..MAX_PENDING_MIGRATIONS {
            trigger
                .pending_migrations
                .insert(format!("live-{i}"), Instant::now());
        }
        assert_eq!(trigger.pending_count(), MAX_PENDING_MIGRATIONS);

        trigger
            .on_remote_read("fresh2", &node_location, "/f", "vol1")
            .await;

        assert!(
            trigger.pending_count() <= MAX_PENDING_MIGRATIONS,
            "clear path should have kept the map at or under the ceiling, got {}",
            trigger.pending_count()
        );
        assert!(
            trigger.is_pending("fresh2"),
            "the clear path must still leave the newly inserted entry pending"
        );
    }
}
