// Background migration trigger on remote reads — Phase 2 (FUSE Client).
//
// When a FUSE client reads a chunk from a data node that is topologically
// far away (different rack, datacenter, or region), this module detects
// the remote read and requests that the manager migrate a replica closer.
//
// Requests are rate-limited per chunk to avoid flooding the manager with
// duplicate migration requests.

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
                // Remove from pending so it can be retried sooner.
                self.pending_migrations.remove(chunk_id);
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

    /// Prune expired cooldown entries to free memory. Call periodically.
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
}
