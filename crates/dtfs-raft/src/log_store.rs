//! RocksDB-backed Raft log store.
//!
//! Column families:
//! - `logs`  — Raft log entries keyed by index (big-endian u64), values are
//!   JSON-encoded `Entry` (AV-5-11; older clusters wrote bincode and are
//!   transparently read via the bincode-fallback path).
//! - `meta`  — Small key/value pairs such as the current vote, last purged log
//!   id, etc. (same JSON-first / bincode-fallback codec).
//!
//! AV-5-11 codec note: writes always use `serde_json` because bincode is
//! positional and cannot honour `#[serde(default)]` for trailing
//! struct fields — adding a field to any type embedded in a log entry
//! (e.g. `NodeInfo` inside `ClientRequest::RegisterNode`) breaks reads
//! of every existing log entry against the new binary
//! (see `plans/incidents/RAFT-LOG-EOF-2026-05-23.md`). Reads first try
//! JSON, then fall back to bincode so a manager can roll up to the new
//! binary without a `rm -rf raft-log` step.

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use serde::{de::DeserializeOwned, Serialize};

use openraft::storage::{IOFlushed, LogState, RaftLogReader, RaftLogStorage};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use tokio::sync::RwLock;
use tracing::debug;

use crate::{Entry, LogId, TypeConfig, Vote};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CF_LOGS: &str = "logs";
const CF_META: &str = "meta";

const META_KEY_VOTE: &[u8] = b"vote";
const META_KEY_LAST_PURGED: &[u8] = b"last_purged";

// ---------------------------------------------------------------------------
// LogStore
// ---------------------------------------------------------------------------

/// RocksDB-backed implementation of `RaftLogStorage`.
#[derive(Debug)]
pub struct LogStore {
    db: Arc<DB>,
    /// Cached last purged log id (also persisted in meta CF).
    last_purged: Arc<RwLock<Option<LogId>>>,
}

impl LogStore {
    /// Open (or create) the log store at the given directory.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        let cf_logs = ColumnFamilyDescriptor::new(CF_LOGS, Options::default());
        let cf_meta = ColumnFamilyDescriptor::new(CF_META, Options::default());

        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf_logs, cf_meta])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let db = Arc::new(db);

        // Restore cached `last_purged` from the meta column family.
        let last_purged = {
            let cf = db.cf_handle(CF_META).expect("meta CF must exist");
            match db.get_cf(&cf, META_KEY_LAST_PURGED) {
                Ok(Some(bytes)) => {
                    let lid: Option<LogId> = decode_value(&bytes)?;
                    lid
                }
                _ => None,
            }
        };

        Ok(Self {
            db,
            last_purged: Arc::new(RwLock::new(last_purged)),
        })
    }

    /// Create a `LogReader` that shares the same RocksDB handle.
    fn reader(&self) -> LogReader {
        LogReader {
            db: self.db.clone(),
        }
    }

    /// Bug AC — peek at the persisted vote without acquiring `&mut self`
    /// or going through the full RaftLogStorage trait. Used by the
    /// pre-init poisoned-state detector in main.rs.
    pub fn peek_vote(&self) -> Result<Option<Vote>, io::Error> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        match self.db.get_cf(&cf, META_KEY_VOTE) {
            Ok(Some(bytes)) => {
                let vote: Vote = decode_value(&bytes)?;
                Ok(Some(vote))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    /// Bug AC — peek the last log entry's `LogId` without consuming
    /// any iterator state. Used for poisoned-state detection.
    pub fn peek_last_log_id(&self) -> Result<Option<LogId>, io::Error> {
        let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);
        match iter.next() {
            Some(Ok((_key, value))) => {
                let entry: Entry = decode_value(&value)?;
                Ok(Some(entry.log_id.clone()))
            }
            _ => Ok(None),
        }
    }

    fn persist_last_purged(&self, lid: &Option<LogId>) -> Result<(), io::Error> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        let bytes = encode_value(lid)?;
        self.db
            .put_cf(&cf, META_KEY_LAST_PURGED, bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}

// ---------------------------------------------------------------------------
// Key encoding helpers
// ---------------------------------------------------------------------------

fn index_to_key(index: u64) -> [u8; 8] {
    index.to_be_bytes()
}

fn key_to_index(key: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(key);
    u64::from_be_bytes(buf)
}

/// AV-5-11 — encode a Raft log value (entry / vote / last_purged).
/// Always uses JSON so reads tolerate future field additions on
/// embedded structs.
fn encode_value<T: Serialize>(v: &T) -> Result<Vec<u8>, io::Error> {
    serde_json::to_vec(v).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// AV-5-11 — decode a Raft log value. Tries JSON first (the AV-5-11
/// format); on parse failure falls back to bincode (the pre-AV-5-11
/// format) so a freshly-rolled binary keeps serving an existing
/// cluster without needing a `rm -rf raft-log` recovery step.
fn decode_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, io::Error> {
    match serde_json::from_slice::<T>(bytes) {
        Ok(v) => Ok(v),
        Err(json_err) => bincode::deserialize::<T>(bytes).map_err(|bin_err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("json={json_err}; bincode_fallback={bin_err}"),
            )
        }),
    }
}

// ---------------------------------------------------------------------------
// LogReader
// ---------------------------------------------------------------------------

/// Read-only handle to the log store, used by replication tasks.
#[derive(Debug, Clone)]
pub struct LogReader {
    db: Arc<DB>,
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry>, io::Error> {
        let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");

        let start = match range.start_bound() {
            std::ops::Bound::Included(&v) => v,
            std::ops::Bound::Excluded(&v) => v + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            std::ops::Bound::Included(&v) => v + 1,
            std::ops::Bound::Excluded(&v) => v,
            std::ops::Bound::Unbounded => u64::MAX,
        };

        let mut entries = Vec::new();
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&index_to_key(start), rocksdb::Direction::Forward),
        );
        for item in iter {
            let (key, value) = item.map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let idx = key_to_index(&key);
            if idx >= end {
                break;
            }
            let entry: Entry = decode_value(&value)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        match self.db.get_cf(&cf, META_KEY_VOTE) {
            Ok(Some(bytes)) => {
                let vote: Vote = decode_value(&bytes)?;
                Ok(Some(vote))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }
}

// ---------------------------------------------------------------------------
// RaftLogStorage implementation
// ---------------------------------------------------------------------------

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let last_purged = self.last_purged.read().await.clone();

        let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");

        // Find the last log entry by reverse iteration.
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);
        let last_log_id = match iter.next() {
            Some(Ok((_key, value))) => {
                let entry: Entry = decode_value(&value)?;
                Some(entry.log_id.clone())
            }
            _ => last_purged.clone(),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.reader()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        let bytes = encode_value(vote)?;
        self.db
            .put_cf(&cf, META_KEY_VOTE, bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        // Flush to guarantee durability before returning.
        self.db
            .flush_cf(&cf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");

        for entry in entries {
            let key = index_to_key(entry.log_id.index);
            let value = encode_value(&entry)?;
            self.db
                .put_cf(&cf, key, value)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            debug!("append log: {}", entry.log_id);
        }

        // Flush to disk, then signal completion.
        let flush_result = self
            .db
            .flush_cf(&cf)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        callback.io_completed(flush_result);

        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");

        let start_index = match &last_log_id {
            Some(lid) => lid.index + 1,
            None => 0,
        };

        debug!(
            "truncate_after: keeping up to index {}",
            start_index.saturating_sub(1)
        );

        // Delete entries from start_index onward.
        let keys_to_delete: Vec<Box<[u8]>> = self
            .db
            .iterator_cf(
                &cf,
                rocksdb::IteratorMode::From(
                    &index_to_key(start_index),
                    rocksdb::Direction::Forward,
                ),
            )
            .filter_map(|item| item.ok().map(|(k, _)| k))
            .collect();

        for key in keys_to_delete {
            self.db
                .delete_cf(&cf, &key)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        debug!("purge up to: {}", log_id);

        // Delete entries up to and including `log_id.index`.
        // Scope the CF reference so it doesn't live across any .await.
        {
            let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");
            let end_key = index_to_key(log_id.index + 1);
            self.db
                .delete_range_cf(&cf, index_to_key(0), end_key)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        // Update the cached last_purged and persist it.
        {
            let mut lp = self.last_purged.write().await;
            *lp = Some(log_id.clone());
        }
        self.persist_last_purged(&Some(log_id))?;

        Ok(())
    }
}
