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
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, WriteBatch, WriteOptions, DB};
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
        // CO-4-12 — a read error here must surface (fail the open) rather
        // than silently resetting last_purged to None, which would make a
        // restarted node re-serve already-purged indices as live log.
        let last_purged = {
            let cf = db.cf_handle(CF_META).expect("meta CF must exist");
            match db.get_cf(&cf, META_KEY_LAST_PURGED) {
                Ok(Some(bytes)) => {
                    let lid: Option<LogId> = decode_value(&bytes)?;
                    lid
                }
                Ok(None) => None,
                Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
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

    /// RAFT-6 / CR2-7-6 — peek the persisted `last_purged` log id
    /// straight from the meta CF, without going through the full
    /// `RaftLogStorage` trait or the cached `Arc<RwLock<..>>`. Used by
    /// `main.rs` startup (via `blank_sm_startup_allowed`) to detect the
    /// corrupt-snapshot-discard-with-purged-log hazard before handing the
    /// log store to openraft.
    pub fn peek_last_purged(&self) -> Result<Option<LogId>, io::Error> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        match self.db.get_cf(&cf, META_KEY_LAST_PURGED) {
            Ok(Some(bytes)) => decode_value(&bytes),
            Ok(None) => Ok(None),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    /// Bug AC — peek the last log entry's `LogId` without consuming
    /// any iterator state. Used for poisoned-state detection.
    pub fn peek_last_log_id(&self) -> Result<Option<LogId>, io::Error> {
        let cf = self.db.cf_handle(CF_LOGS).expect("logs CF");
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);
        // CO-4-12 — a storage error here must propagate, not collapse to
        // an empty-log result.
        last_log_id_from_iter_item(iter.next())
    }

    // RAFT-5 / CR2-7-5 — `purge` now writes `last_purged` inline as part of
    // its single atomic WriteBatch (see the `RaftLogStorage::purge` impl
    // below), so this standalone setter is no longer on the production
    // write path. Kept for test scaffolding (`log_store_reopens_clean_db`,
    // `log_store_opens_with_bounded_opts` use it to touch the meta CF).
    #[allow(dead_code)]
    fn persist_last_purged(&self, lid: &Option<LogId>) -> Result<(), io::Error> {
        let cf = self.db.cf_handle(CF_META).expect("meta CF");
        let bytes = encode_value(lid)?;
        self.db
            .put_cf_opt(&cf, META_KEY_LAST_PURGED, bytes, &synced_write_opts())
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

/// RAFT-5 / CR2-7-5 — an upper-bound key strictly greater than any 8-byte
/// big-endian index key, including `index_to_key(u64::MAX)`. RocksDB's
/// `delete_range_cf` end is exclusive; comparing bytes lexicographically,
/// a 9-byte all-`0xFF` key sorts after every possible 8-byte key. Used to
/// delete "everything from index X onward" in a single call instead of
/// enumerating keys.
fn key_upper_bound() -> [u8; 9] {
    [0xFF; 9]
}

/// RAFT-6 / CR2-7-6 — refuse to start as a voter when the local state
/// machine came up BLANK (its snapshot was corrupt/unreadable and
/// `state_machine.rs::open_at`'s BP-2 discard returned a fresh
/// `StateMachineData::default()`) AND this node's log has already been
/// purged past some point (`last_purged.is_some()`).
///
/// That combination means the discarded snapshot was the ONLY surviving
/// copy of everything before the purge point: if this node then wins an
/// election (openraft elects on log freshness, not on state-machine
/// content), it applies only the log's surviving suffix onto an empty
/// state machine — every file created before the purge point vanishes
/// cluster-wide as soon as this node's next snapshot propagates. A
/// genuinely fresh, never-bootstrapped node has `last_purged == None` and
/// is unaffected; only a snapshot discard "colliding" with a real purge
/// point is refused.
///
/// Pure so `main.rs` can call it right after opening the log store +
/// state machine, before handing either to openraft.
pub fn blank_sm_startup_allowed(sm_is_blank: bool, last_purged: Option<LogId>) -> Result<(), String> {
    match (sm_is_blank, last_purged) {
        (true, Some(lp)) => Err(format!(
            "Raft state machine snapshot was corrupt/unreadable and was \
             discarded (blank state), but this node's log has already \
             been purged up to index {} — the discarded snapshot was the \
             ONLY surviving copy of everything before that point. \
             Refusing to start as a voter: an election win here would \
             apply only the log's surviving suffix onto an empty state \
             machine and lose everything before the purge point, \
             cluster-wide, on this node's next snapshot. See the manager \
             recovery runbook: stop this node, wipe raft-log + raft-sm, \
             and re-bootstrap it as a learner from a healthy peer.",
            lp.index
        )),
        _ => Ok(()),
    }
}

/// Refuse to start when the state machine is STALE relative to the log:
/// its `last_applied` index is below the log's `last_purged` index, so the
/// entries in between exist nowhere on this node (e.g. an old `snapshot.bin`
/// restored next to a newer log store). openraft's own guard is skipped
/// because `read_committed` returns `None`, and a later election win would
/// serve a state machine missing that range. A blank SM (`None`) is covered
/// by `blank_sm_startup_allowed`; `last_applied == last_purged` is fine.
pub fn stale_sm_startup_allowed(
    sm_last_applied: Option<&LogId>,
    last_purged: Option<&LogId>,
) -> Result<(), String> {
    match (sm_last_applied, last_purged) {
        (Some(applied), Some(purged)) if applied.index < purged.index => Err(format!(
            "Raft state machine is stale: it has applied up to index {} but this node's \
             log was already purged up to index {}, so entries {}..={} exist nowhere on \
             this node. Refusing to start. Restore raft-sm from a recent backup, or stop \
             this node, wipe raft-log + raft-sm and re-join it as a learner from a \
             healthy peer.",
            applied.index,
            purged.index,
            applied.index + 1,
            purged.index
        )),
        _ => Ok(()),
    }
}

/// RAFT-5 / CR2-7-5 — `WriteOptions` with `set_sync(true)`. Every durable
/// Raft log/meta write (append, save_vote, truncate, purge) must fsync
/// before returning; a per-write `flush_cf` only flushes RocksDB's
/// in-memory memtable to an SST/WAL buffer, it does not itself fsync, and
/// running it after every single append created one L0 SST per commit
/// (heartbeat usage updates, the 1 s lease sweep, per-tick migration
/// state), driving continuous compaction (Bug BP-1).
fn synced_write_opts() -> WriteOptions {
    let mut wo = WriteOptions::default();
    wo.set_sync(true);
    wo
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

/// CO-4-12 — map the last-entry iterator step to its decoded `LogId`.
///
/// A RocksDB iterator yields `None` only when the column family is
/// genuinely empty; a `Some(Err(_))` means the iterator hit a storage
/// error (a corrupt block, an I/O fault) and the log MUST NOT be treated
/// as empty/purged. Folding that error into "absence" silently drops
/// committed entries (`get_log_state` would report a shorter log than is
/// on disk). So:
/// - `None`            -> `Ok(None)` (truly empty CF)
/// - `Some(Ok(value))` -> decode the entry, return `Ok(Some(log_id))`
/// - `Some(Err(e))`    -> `Err(io::Error)` (propagate, never swallow)
///
/// `item` is the iterator's `Option<Result<(key, value), rocksdb::Error>>`
/// so the same mapping is unit-testable with a forced error in place of a
/// real DB fault.
fn last_log_id_from_iter_item<K, V>(
    item: Option<Result<(K, V), rocksdb::Error>>,
) -> Result<Option<LogId>, io::Error>
where
    V: AsRef<[u8]>,
{
    match item {
        Some(Ok((_key, value))) => {
            let entry: Entry = decode_value(value.as_ref())?;
            Ok(Some(entry.log_id.clone()))
        }
        Some(Err(e)) => Err(io::Error::new(io::ErrorKind::Other, e)),
        None => Ok(None),
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

        // Find the last log entry by reverse iteration. CO-4-12 — an
        // iterator storage error propagates (`?`); only a genuinely empty
        // CF (Ok(None)) falls back to last_purged. Folding a storage error
        // into the empty case fabricated a shorter log and lost committed
        // entries.
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::End);
        let last_log_id = match last_log_id_from_iter_item(iter.next())? {
            Some(lid) => Some(lid),
            None => last_purged.clone(),
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
        // RAFT-5 / CR2-7-5 — a synced write guarantees durability directly;
        // a plain `put_cf` + `flush_cf` only pushes the memtable to an
        // SST/WAL buffer (no fsync) and additionally forces a flush (one
        // L0 SST) on every single vote write.
        self.db
            .put_cf_opt(&cf, META_KEY_VOTE, bytes, &synced_write_opts())
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

        // RAFT-5 / CR2-7-5 — batch every entry in this call into one
        // WriteBatch and commit it with a single synced write instead of
        // N unsynced `put_cf` calls followed by a `flush_cf`. `flush_cf`
        // pushes RocksDB's memtable to a new SST on every call — every
        // Raft commit (heartbeat usage updates, the 1 s lease sweep,
        // per-tick migration state) created one L0 SST, driving
        // continuous compaction (Bug BP-1 fought the SYMPTOM; this fixes
        // the cause). `set_sync(true)` fsyncs the WAL before `write_opt`
        // returns, which is the actual durability guarantee openraft
        // needs before invoking `callback.io_completed`.
        let mut batch = WriteBatch::default();
        for entry in entries {
            let key = index_to_key(entry.log_id.index);
            let value = encode_value(&entry)?;
            batch.put_cf(&cf, key, value);
            debug!("append log: {}", entry.log_id);
        }

        let write_result = self
            .db
            .write_opt(batch, &synced_write_opts())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e));

        callback.io_completed(write_result);

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

        // RAFT-5 / CR2-7-5 — a single `delete_range_cf` instead of
        // enumerate-then-delete-key-by-key. The old loop was not atomic:
        // a crash between two `delete_cf` calls left a HOLE in the middle
        // of the deleted range (some higher indices gone, some lower ones
        // in the range still present), which is a fatal storage
        // inconsistency for openraft (the log is no longer a contiguous
        // prefix). `delete_range_cf` is one RocksDB write; the on-disk
        // result is always either "not yet applied" or "fully applied",
        // never torn.
        self.db
            .delete_range_cf(
                &cf,
                index_to_key(start_index).as_slice(),
                key_upper_bound().as_slice(),
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        debug!("purge up to: {}", log_id);

        // RAFT-5 / CR2-7-5 — the range delete and the `last_purged` meta
        // write commit as ONE WriteBatch with a single synced write,
        // instead of two separate calls (a `delete_range_cf` followed by
        // an unsynced `put_cf`). Pre-fix, a crash between the two left the
        // deleted range durable but `last_purged` still `None` (or stale)
        // for a log that now starts above index 0 — `get_log_state` would
        // report a `last_purged_log_id` that undercounts what was actually
        // purged. Bundling them means either both are durable or neither
        // is.
        let last_purged_bytes = encode_value(&Some(log_id.clone()))?;

        // Scope the CF handles so neither lives across the `.await` below
        // (`ColumnFamily` is not `Send`; openraft requires `purge`'s
        // future to be `Send`).
        {
            let logs_cf = self.db.cf_handle(CF_LOGS).expect("logs CF");
            let meta_cf = self.db.cf_handle(CF_META).expect("meta CF");

            let mut batch = WriteBatch::default();
            batch.delete_range_cf(&logs_cf, index_to_key(0), index_to_key(log_id.index + 1));
            batch.put_cf(&meta_cf, META_KEY_LAST_PURGED, last_purged_bytes);

            self.db
                .write_opt(batch, &synced_write_opts())
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        }

        // Update the cached last_purged to match the now-durable write.
        {
            let mut lp = self.last_purged.write().await;
            *lp = Some(log_id);
        }

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

#[cfg(test)]
mod audit_co_tests {
    use super::*;

    /// Produce a genuine `rocksdb::Error` (its constructor is private, so we
    /// trigger a real one). Opening a DB at a non-existent path with the
    /// default options (`create_if_missing` defaults to false) fails — this
    /// is the same `rocksdb::Error` type an iterator would surface on a
    /// corrupt block / I/O fault, so it exercises the exact `Some(Err(_))`
    /// arm of the production iterator step.
    fn forced_rocksdb_error() -> rocksdb::Error {
        let missing = std::env::temp_dir().join(format!(
            "dtfs-co412-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let opts = Options::default(); // create_if_missing = false
        rocksdb::DB::open(&opts, &missing).expect_err("open of a missing DB must error")
    }

    /// CO-4-12 — a `Some(Err(rocksdb_error))` from the last-entry iterator
    /// MUST propagate as `Err`, never collapse to an empty-log result. The
    /// pre-fix `_ => Ok(None)` catch-all fabricated an empty log here and
    /// lost committed entries.
    #[test]
    fn co_4_12_iter_error_propagates_not_empty() {
        let item: Option<Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> =
            Some(Err(forced_rocksdb_error()));
        let mapped = last_log_id_from_iter_item(item);
        assert!(
            mapped.is_err(),
            "iterator storage error must propagate as Err, got {:?}",
            mapped.ok()
        );
        assert_eq!(mapped.unwrap_err().kind(), io::ErrorKind::Other);
    }

    /// CO-4-12 — a genuinely empty CF (`None` from the iterator) is the only
    /// case that legitimately maps to absence. Must NOT be turned into an
    /// error.
    #[test]
    fn co_4_12_empty_iter_maps_to_none() {
        let item: Option<Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>> = None;
        let mapped = last_log_id_from_iter_item(item).expect("empty CF is not an error");
        assert!(mapped.is_none(), "empty CF must map to None");
    }

    /// RAFT-6 / CR2-7-6 — the dangerous combination: a blank state
    /// machine (snapshot discarded) AND a log already purged past some
    /// point. This is the ONLY case that must refuse startup.
    #[test]
    fn raft_6_blank_sm_with_purged_log_refuses_startup() {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        let purge_id = openraft::LogId::new(leader, 42);

        let result = blank_sm_startup_allowed(true, Some(purge_id));
        assert!(result.is_err(), "blank SM + purged log must refuse startup");
        let msg = result.unwrap_err();
        assert!(msg.contains("42"), "message should name the purge index: {msg}");
        assert!(
            msg.contains("wipe raft-log + raft-sm"),
            "message must point at the recovery runbook: {msg}"
        );
    }

    /// RAFT-6 / CR2-7-6 — a genuinely fresh, never-bootstrapped node has
    /// a blank SM but `last_purged == None` (nothing has ever been
    /// purged). This is normal and must be allowed.
    #[test]
    fn raft_6_blank_sm_with_no_purge_is_a_fresh_node_and_allowed() {
        assert!(
            blank_sm_startup_allowed(true, None).is_ok(),
            "a fresh node (blank SM, never purged) must be allowed to start"
        );
    }

    /// RAFT-6 / CR2-7-6 — a non-blank SM (successfully loaded a real
    /// snapshot, or replayed the log into one over its lifetime) is
    /// always allowed to start, regardless of purge history — it is not
    /// missing anything.
    #[test]
    fn raft_6_non_blank_sm_is_always_allowed() {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        let purge_id = openraft::LogId::new(leader, 42);

        assert!(blank_sm_startup_allowed(false, Some(purge_id)).is_ok());
        assert!(blank_sm_startup_allowed(false, None).is_ok());
    }

    fn lid(i: u64) -> LogId {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        openraft::LogId::new(leader, i)
    }

    #[test]
    fn stale_sm_applied_below_purged_refuses_startup() {
        let (a, p) = (lid(41), lid(42));
        let msg = stale_sm_startup_allowed(Some(&a), Some(&p)).unwrap_err();
        assert!(msg.contains("applied up to index 41") && msg.contains("purged up to index 42"), "{msg}");
        assert!(msg.contains("wipe raft-log + raft-sm"), "{msg}");
    }

    #[test]
    fn stale_sm_boundary_and_other_cases_allowed() {
        let (eq, hi) = (lid(42), lid(43));
        assert!(stale_sm_startup_allowed(Some(&eq), Some(&eq)).is_ok(), "applied == purged");
        assert!(stale_sm_startup_allowed(Some(&hi), Some(&eq)).is_ok(), "applied == purged + 1");
        assert!(stale_sm_startup_allowed(Some(&eq), None).is_ok());
        assert!(stale_sm_startup_allowed(None, None).is_ok());
        assert!(stale_sm_startup_allowed(None, Some(&eq)).is_ok(), "blank SM is the other guard's job");
    }

    /// CO-4-12 — `get_log_state` on a populated log returns the real last
    /// log id (happy path unchanged by the error-propagation fix).
    #[tokio::test]
    async fn co_4_12_get_log_state_reports_last_entry() {
        use openraft::vote::RaftLeaderId;

        fn mk_entry(idx: u64) -> Entry {
            let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
            Entry {
                log_id: openraft::LogId::new(leader, idx),
                payload: openraft::EntryPayload::Blank,
            }
        }

        let dir = std::env::temp_dir().join(format!(
            "dtfs-co412-state-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            let mut store = LogStore::new(&dir).expect("open");
            let cf = store.db.cf_handle(CF_LOGS).expect("logs CF");
            for idx in 0..5u64 {
                let value = encode_value(&mk_entry(idx)).expect("encode");
                store.db.put_cf(&cf, index_to_key(idx), value).expect("put");
            }
            store.db.flush_cf(&cf).expect("flush");

    let state = store.get_log_state().await.expect("get_log_state");
            assert_eq!(
                state.last_log_id.map(|l| l.index),
                Some(4),
                "last_log_id must be the highest committed index"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn mk_entry(idx: u64) -> Entry {
        use openraft::vote::RaftLeaderId;
        let leader = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(0, 0);
        Entry {
            log_id: openraft::LogId::new(leader, idx),
            payload: openraft::EntryPayload::Blank,
        }
    }

    fn scratch(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "dtfs-cr2-7-5-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// RAFT-5 / CR2-7-5 — `truncate_after` must leave the log a
    /// contiguous prefix: every index up to and including the kept
    /// `last_log_id` survives, and nothing beyond it does. Guards the
    /// switch from a key-by-key delete loop to a single `delete_range_cf`.
    #[tokio::test]
    async fn cr2_7_5_truncate_after_leaves_contiguous_prefix() {
        use openraft::storage::RaftLogStorage;

        let dir = scratch("truncate");
        let mut store = LogStore::new(&dir).expect("open");

        // Append indices 0..10 directly through the RaftLogStorage trait
        // (exercises the new WriteBatch + synced-write append path).
        let entries: Vec<Entry> = (0..10).map(mk_entry).collect();
        let callback = IOFlushed::<TypeConfig>::noop();
        RaftLogStorage::append(&mut store, entries, callback)
            .await
            .expect("append");

        // Truncate after index 4 -> keep 0..=4, drop 5..=9.
        let keep = mk_entry(4).log_id;
        RaftLogStorage::truncate_after(&mut store, Some(keep))
            .await
            .expect("truncate_after");

        let mut reader = store.reader();
        let remaining = reader.try_get_log_entries(0..100).await.expect("read back");
        let indices: Vec<u64> = remaining.iter().map(|e| e.log_id.index).collect();
        assert_eq!(
            indices,
            (0..=4).collect::<Vec<u64>>(),
            "truncate_after must leave a contiguous 0..=4 prefix, no hole and nothing beyond it"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// RAFT-5 / CR2-7-5 — `truncate_after(None)` drops the entire log
    /// (start_index = 0) in one call.
    #[tokio::test]
    async fn cr2_7_5_truncate_after_none_drops_everything() {
        use openraft::storage::RaftLogStorage;

        let dir = scratch("truncate-none");
        let mut store = LogStore::new(&dir).expect("open");
        let entries: Vec<Entry> = (0..5).map(mk_entry).collect();
        let callback = IOFlushed::<TypeConfig>::noop();
        RaftLogStorage::append(&mut store, entries, callback)
            .await
            .expect("append");

        RaftLogStorage::truncate_after(&mut store, None)
            .await
            .expect("truncate_after(None)");

        let mut reader = store.reader();
        let remaining = reader.try_get_log_entries(0..100).await.expect("read back");
        assert!(remaining.is_empty(), "truncate_after(None) must drop every entry");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// RAFT-5 / CR2-7-5 — `purge` commits the range delete and the
    /// `last_purged` meta write as one atomic batch: after purging and
    /// reopening the store (simulating a restart), `last_purged` must be
    /// present and the purged range gone.
    #[tokio::test]
    async fn cr2_7_5_purge_persists_last_purged_atomically_across_reopen() {
        use openraft::storage::RaftLogStorage;

        let dir = scratch("purge");
        {
            let mut store = LogStore::new(&dir).expect("open");
            let entries: Vec<Entry> = (0..10).map(mk_entry).collect();
            let callback = IOFlushed::<TypeConfig>::noop();
            RaftLogStorage::append(&mut store, entries, callback)
                .await
                .expect("append");

            let purge_id = mk_entry(4).log_id;
            RaftLogStorage::purge(&mut store, purge_id)
                .await
                .expect("purge");

            let mut reader = store.reader();
            let remaining = reader.try_get_log_entries(0..100).await.expect("read back");
            let indices: Vec<u64> = remaining.iter().map(|e| e.log_id.index).collect();
            assert_eq!(
                indices,
                (5..10).collect::<Vec<u64>>(),
                "purge must remove indices 0..=4"
            );
        }

        // Reopen — the meta write must have survived alongside the range
        // delete (both committed in the same WriteBatch).
        let mut reopened = LogStore::new(&dir).expect("reopen after purge");
        let state = reopened.get_log_state().await.expect("get_log_state");
        assert_eq!(
            state.last_purged_log_id.map(|l| l.index),
            Some(4),
            "last_purged must survive a reopen"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
