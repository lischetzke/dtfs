//! Snapshot serialization helpers.
//!
//! The state machine is serialized to JSON for snapshots. This module provides
//! the data structures that are written into / read from the `Cursor<Vec<u8>>`
//! snapshot blob.

use std::collections::BTreeMap;
use std::io::{self, Cursor, Read};

use serde::{Deserialize, Serialize};

use dtfs_common::types::VolumeConfig;

use crate::state_machine::{
    AuditEntry, ClusterConfig, DistributedLock, FileMetadata, ScrubRecord, WriteLease,
};
use crate::NodeInfo;

/// The complete serializable snapshot of the cluster state machine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateMachineSnapshot {
    /// Last applied log id (term, index).  Stored as `Option<(u64, u64)>` so
    /// that the snapshot format is decoupled from OpenRaft internal types.
    pub last_applied_log: Option<(u64, u64)>,

    /// Last applied membership — serialised separately.
    pub last_membership: Option<serde_json::Value>,

    // ---- domain state ----
    pub files: BTreeMap<String, FileMetadata>,
    pub nodes: BTreeMap<u64, NodeInfo>,
    pub cluster_config: ClusterConfig,
    pub audit_log: Vec<AuditEntry>,

    // ---- observability state ----
    /// Per-node scrub results, keyed by node_id.
    ///
    /// Replaces the old `last_scrub: Option<ScrubRecord>` single-value field
    /// so that every data node's latest scrub result survives snapshot
    /// install/restore (fix for issue #8).
    #[serde(default)]
    pub scrub_results: BTreeMap<u64, ScrubRecord>,

    /// Number of registered FUSE clients (updated by client heartbeats).
    #[serde(default)]
    pub client_count: u64,

    /// Volume configurations.
    #[serde(default)]
    pub volumes: BTreeMap<String, VolumeConfig>,

    /// Migration-scheduler state JSON.  Kept separate from `cluster_config`
    /// so that scheduler bookkeeping never overwrites user config (fix #3).
    #[serde(default)]
    pub migration_state_json: String,

    /// F-039: distributed POSIX byte-range locks held by clients.
    #[serde(default)]
    pub posix_locks: BTreeMap<String, Vec<DistributedLock>>,

    /// P0-1: single-writer leases keyed by path. `#[serde(default)]` keeps
    /// pre-existing snapshots loadable.
    #[serde(default)]
    pub write_leases: BTreeMap<String, WriteLease>,
}

/// Serialize a `StateMachineSnapshot` into a `Cursor<Vec<u8>>`.
pub fn serialize_snapshot(snap: &StateMachineSnapshot) -> io::Result<Cursor<Vec<u8>>> {
    let data = serde_json::to_vec(snap)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Cursor::new(data))
}

/// Deserialize a `StateMachineSnapshot` from a `Cursor<Vec<u8>>`.
pub fn deserialize_snapshot(mut cursor: Cursor<Vec<u8>>) -> io::Result<StateMachineSnapshot> {
    let mut buf = Vec::new();
    cursor.read_to_end(&mut buf)?;
    serde_json::from_slice(&buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Write raw bytes into a fresh `Cursor`.
pub fn cursor_from_bytes(data: Vec<u8>) -> Cursor<Vec<u8>> {
    Cursor::new(data)
}

/// Extract the bytes from a cursor (rewinds first).
pub fn bytes_from_cursor(mut cursor: Cursor<Vec<u8>>) -> io::Result<Vec<u8>> {
    let mut buf = Vec::new();
    cursor.set_position(0);
    cursor.read_to_end(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let snap = StateMachineSnapshot {
            last_applied_log: Some((1, 5)),
            last_membership: None,
            files: BTreeMap::new(),
            nodes: BTreeMap::new(),
            cluster_config: ClusterConfig::default(),
            audit_log: Vec::new(),
            scrub_results: BTreeMap::new(),
            client_count: 0,
            volumes: BTreeMap::new(),
            migration_state_json: String::new(),
            posix_locks: BTreeMap::new(),
            write_leases: BTreeMap::new(),
        };

        let cursor = serialize_snapshot(&snap).unwrap();
        let restored = deserialize_snapshot(cursor).unwrap();
        assert_eq!(snap.last_applied_log, restored.last_applied_log);
        assert_eq!(snap.scrub_results.len(), restored.scrub_results.len());
        assert_eq!(snap.migration_state_json, restored.migration_state_json);
    }
}
