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
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, DB};
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

/// Bug BP-1 — bounds on the RocksDB info-LOG and MANIFEST so the raft log
/// store cannot fill the manager partition (ISSUE-SCAN-2026-06-02 F5: 6.8 GB
/// of `LOG.old.*` + a 207 MB MANIFEST on a 16 GB root). Defaults are sane;
/// `LogStore::new` uses them, `new_with_opts` lets the manager wire config.
#[derive(Debug, Clone, Copy)]
pub struct RocksDbLogOpts {
    /// `keep_log_file_num` — number of RocksDB info `LOG`/`LOG.old.*` files
    /// retained. RocksDB default is 1000; we keep a handful.
    pub keep_log_file_num: u64,
    /// `max_manifest_file_size` — MANIFEST rotates once it exceeds this.
    pub max_manifest_bytes: u64,
    /// CNF-3 — per-CF memtable (write buffer) size in MiB. Bounds the
    /// write-side memory of each column family.
    pub write_buffer_mb: u64,
    /// CNF-3 — LRU block-cache size in MiB. One cache instance is shared
    /// across both column families so this is the total read-cache budget.
    pub block_cache_mb: u64,
}

impl Default for RocksDbLogOpts {
    fn default() -> Self {
        Self {
            keep_log_file_num: 4,
            max_manifest_bytes: 64 * 1024 * 1024,
            write_buffer_mb: 64,
            block_cache_mb: 64,
        }
    }
}

impl LogStore {
    /// Open (or create) the log store at the given directory with default
    /// bounded RocksDB options.
    pub fn new(path: impl AsRef<Path>) -> Result<Self, io::Error> {
        Self::new_with_opts(path, RocksDbLogOpts::default())
    }

    /// Bug BP-1 — open (or create) the log store with explicit RocksDB
    /// info-LOG / MANIFEST bounds.
    pub fn new_with_opts(path: impl AsRef<Path>, opts: RocksDbLogOpts) -> Result<Self, io::Error> {
        let path = path.as_ref();
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);

        // Bug BP-1 — bound the info-LOG count and MANIFEST size so neither
        // grows unbounded and fills the partition. `keep_log_file_num`
        // caps the `LOG.old.*` rotation set; `set_max_manifest_file_size`
        // forces the MANIFEST to roll instead of growing to hundreds of MB.
        db_opts.set_keep_log_file_num(opts.keep_log_file_num.max(1) as usize);
        db_opts.set_max_manifest_file_size(opts.max_manifest_bytes.max(1024 * 1024) as usize);

        // Bug AV-5-2 / BG-6 — tolerate a torn WAL tail left when the
        // previous binary was killed mid-append (e.g. during a rolling
        // upgrade). Without this, RocksDB rejects the open with an
        // opaque "unexpected end of file" and the manager crash-loops.
        // TolerateCorruptedTailRecords drops ONLY an incomplete trailing
        // WAL record, never a committed mid-log entry — safe for a Raft
        // log because any uncommitted tail entry dropped here is
        // re-replicated by the leader, while committed entries were
        // fsync'd before the append was acked.
        db_opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::TolerateCorruptedTailRecords);

        // CNF-3 — bound RocksDB memory: cap each CF's memtable and attach
        // ONE shared LRU block cache to both CFs (a per-CF cache would
        // double the budget). The rocksdb crate clones the cache into the
        // DB's outlive set, so dropping our handle after open is fine.
        let block_cache =
            Cache::new_lru_cache((opts.block_cache_mb.max(1) * 1024 * 1024) as usize);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&block_cache);
        let cf_opts = || {
            let mut o = Options::default();
            o.set_write_buffer_size((opts.write_buffer_mb.max(1) * 1024 * 1024) as usize);
            o.set_block_based_table_factory(&block_opts);
            o
        };
        let cf_logs = ColumnFamilyDescriptor::new(CF_LOGS, cf_opts());
        let cf_meta = ColumnFamilyDescriptor::new(CF_META, cf_opts());

        let db = DB::open_cf_descriptors(&db_opts, path, vec![cf_logs, cf_meta])
            .map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "failed to open Raft log store at {}: {e}. If this \
                         persists after an unclean shutdown the on-disk \
                         RocksDB may be corrupt beyond a torn tail; see the \
                         manager recovery runbook (stop the node, wipe \
                         raft-log + raft-sm, and re-bootstrap from a healthy \
                         peer) rather than letting the service crash-loop.",
                        path.display()
                    ),
                )
            })?;

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

#[cfg(test)]
mod bug_av_5_2_recovery_tests {
    use super::*;

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("dtfs-bg6-{tag}-{}-{nanos}", std::process::id()))
    }

    /// Bug AV-5-2 — a clean log store must reopen without error under the
    /// TolerateCorruptedTailRecords WAL recovery mode (regression guard
    /// that the recovery-mode option does not break the normal open path).
    #[test]
    fn log_store_reopens_clean_db() {
        let dir = scratch_dir("reopen");
        {
            let store = LogStore::new(&dir).expect("first open must succeed");
            // Touch the meta CF so a WAL/SST actually exists on disk.
            store
                .persist_last_purged(&None)
                .expect("persist on fresh store");
        }
        // Reopen the same path — must succeed (mirrors a process restart).
        let reopened = LogStore::new(&dir);
        assert!(
            reopened.is_ok(),
            "reopening a clean log store must succeed, got {:?}",
            reopened.err()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Bug BP-1 — opening with explicit bounded RocksDB opts must succeed
    /// (regression guard that the keep_log_file_num / max_manifest_file_size
    /// wiring does not break the open path). The default bounds are applied
    /// via `LogStore::new`; this exercises the explicit `new_with_opts`.
    #[test]
    fn log_store_opens_with_bounded_opts() {
        let dir = scratch_dir("bp1");
        let opts = RocksDbLogOpts {
            keep_log_file_num: 4,
            max_manifest_bytes: 64 * 1024 * 1024,
            ..RocksDbLogOpts::default()
        };
        {
            let store = LogStore::new_with_opts(&dir, opts).expect("open with bounded opts");
            store.persist_last_purged(&None).expect("persist");
        }
        // Reopen with the same bounds (process restart).
        assert!(LogStore::new_with_opts(&dir, opts).is_ok(), "reopen with bounded opts");
        // Defaults also resolve to bounded values.
        assert_eq!(RocksDbLogOpts::default().keep_log_file_num, 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// CNF-3 — opening with explicit write-buffer / block-cache bounds must
    /// survive a small write workload (append + read back log entries) and a
    /// reopen, proving the bounded memtable + shared block cache do not break
    /// the normal log path.
    #[tokio::test]
    async fn log_store_bounded_memory_write_workload() {
        use openraft::vote::RaftLeaderId;

        fn mk_entry(idx: u64) -> Entry {
            let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
            Entry {
                log_id: openraft::LogId::new(leader, idx),
                payload: openraft::EntryPayload::Blank,
            }
        }

        let dir = scratch_dir("cnf3");
        let opts = RocksDbLogOpts {
            write_buffer_mb: 8,
            block_cache_mb: 8,
            ..RocksDbLogOpts::default()
        };
        {
            let store = LogStore::new_with_opts(&dir, opts).expect("open with memory bounds");
            // Small write workload: append 100 log entries (same put+flush
            // sequence as RaftLogStorage::append) and read them back.
            let cf = store.db.cf_handle(CF_LOGS).expect("logs CF");
            for idx in 0..100u64 {
                let entry = mk_entry(idx);
                let value = encode_value(&entry).expect("encode entry");
                store
                    .db
                    .put_cf(&cf, index_to_key(idx), value)
                    .expect("put entry");
            }
            store.db.flush_cf(&cf).expect("flush logs CF");

            let mut reader = store.reader();
            let entries = reader.try_get_log_entries(0..100).await.expect("read back");
            assert_eq!(entries.len(), 100, "all appended entries read back");
            assert_eq!(entries[99].log_id.index, 99);
        }
        // Reopen with the same bounds (process restart) and re-read.
        {
            let store = LogStore::new_with_opts(&dir, opts).expect("reopen with memory bounds");
            let mut reader = store.reader();
            let entries = reader.try_get_log_entries(0..100).await.expect("read after reopen");
            assert_eq!(entries.len(), 100, "entries survive reopen");
        }
        // Defaults resolve to the conservative 64 MiB bounds.
        assert_eq!(RocksDbLogOpts::default().write_buffer_mb, 64);
        assert_eq!(RocksDbLogOpts::default().block_cache_mb, 64);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
