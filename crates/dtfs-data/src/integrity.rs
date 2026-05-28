//! Bug BF — data-side integrity-fault collection.
//!
//! When `verify_on_read` (Bug BC-8) detects a hash mismatch, the
//! data node records the event into this collector. The heartbeat
//! loop drains the collector each tick and pushes the faults up to
//! the manager via `HeartbeatRequest.integrity_faults` (proto field
//! 10, Bug BF-1). The manager then issues
//! `ClientRequest::QuarantineNode` + `RemoveCorruptChunkReplicas`
//! per fault (Bug BF-6).
//!
//! Until the first heartbeat carries the fault, `put_chunk`
//! self-rejects on this node via `is_self_quarantined()` (Bug BF-4)
//! so we never accept new writes onto a disk we know is corrupt.

use std::sync::atomic::{AtomicBool, Ordering};
use parking_lot::Mutex;

pub use dtfs_proto::dtfs::manager::IntegrityFault;

/// Bounded queue of pending integrity faults + a sticky self-quarantine
/// flag. Mirrors the `pending_corruption` pattern in
/// `crates/dtfs-data/src/store.rs:85` so failure semantics are uniform.
pub struct IntegrityFaultCollector {
    faults: Mutex<Vec<IntegrityFault>>,
    self_quarantined: AtomicBool,
    cap: usize,
}

impl IntegrityFaultCollector {
    pub fn new(cap: usize) -> Self {
        Self {
            faults: Mutex::new(Vec::new()),
            self_quarantined: AtomicBool::new(false),
            cap: cap.max(1),
        }
    }

    /// Record a fault. Trips `self_quarantined` on first record.
    /// At cap, drops the oldest entry to keep the buffer bounded
    /// (avoids unbounded growth if the mgr is unreachable for a
    /// long time — fresh faults are more informative than stale).
    pub fn record(&self, fault: IntegrityFault) {
        self.self_quarantined.store(true, Ordering::Release);
        let mut v = self.faults.lock();
        if v.len() >= self.cap {
            v.remove(0);
        }
        v.push(fault);
    }

    /// Drain all currently-pending faults. Heartbeat loop calls this
    /// once per tick. Does NOT clear `self_quarantined` — only the
    /// mgr-driven UnquarantineNode raft write clears that (carried
    /// back via ConfigUpdate or a future explicit ack).
    pub fn drain(&self) -> Vec<IntegrityFault> {
        let mut v = self.faults.lock();
        std::mem::take(&mut *v)
    }

    /// Re-enqueue faults the heartbeat RPC failed to deliver. Inserts
    /// at the front so they go first on the next tick. Respects the
    /// cap by truncating the tail (older end of new arrivals) if the
    /// combined set would exceed.
    pub fn re_enqueue(&self, mut faults: Vec<IntegrityFault>) {
        let mut v = self.faults.lock();
        // Old faults from the failed RPC are prepended; newer faults
        // accumulated since stay after.
        faults.append(&mut v);
        if faults.len() > self.cap {
            faults.truncate(self.cap);
        }
        *v = faults;
    }

    pub fn is_self_quarantined(&self) -> bool {
        self.self_quarantined.load(Ordering::Acquire)
    }

    /// Operator-driven clear via mgr-side UnquarantineNode →
    /// HeartbeatResponse.self_quarantine_cleared downlink (Bug BF).
    /// Called from the heartbeat loop in main.rs when the mgr signals
    /// the node is no longer Quarantined.
    pub fn clear_self_quarantined(&self) {
        self.self_quarantined.store(false, Ordering::Release);
    }

    #[cfg(test)]
    pub fn pending_len(&self) -> usize {
        self.faults.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk(chunk_id: &str) -> IntegrityFault {
        IntegrityFault {
            chunk_id: chunk_id.into(),
            observed_at_ms: 0,
            detector: "verify_on_read".into(),
            expected_len: 0,
            actual_len: 0,
            expected_hash: "aa".into(),
            actual_hash: "bb".into(),
        }
    }

    #[test]
    fn bug_bf_2_record_trips_self_quarantine() {
        let c = IntegrityFaultCollector::new(8);
        assert!(!c.is_self_quarantined());
        c.record(mk("c1"));
        assert!(c.is_self_quarantined());
        assert_eq!(c.pending_len(), 1);
    }

    #[test]
    fn bug_bf_2_record_past_cap_drops_oldest() {
        let c = IntegrityFaultCollector::new(2);
        c.record(mk("c1"));
        c.record(mk("c2"));
        c.record(mk("c3"));
        let d = c.drain();
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].chunk_id, "c2");
        assert_eq!(d[1].chunk_id, "c3");
    }

    #[test]
    fn bug_bf_2_drain_empties_but_keeps_quarantine() {
        let c = IntegrityFaultCollector::new(8);
        c.record(mk("c1"));
        assert_eq!(c.drain().len(), 1);
        assert_eq!(c.pending_len(), 0);
        assert!(c.is_self_quarantined(), "drain must NOT clear self-quarantine");
    }

    #[test]
    fn bug_bf_2_re_enqueue_prepends_and_respects_cap() {
        let c = IntegrityFaultCollector::new(3);
        c.record(mk("c-new"));
        c.re_enqueue(vec![mk("c-old1"), mk("c-old2"), mk("c-old3"), mk("c-old4")]);
        let d = c.drain();
        assert_eq!(d.len(), 3);
        // Oldest "c-old1..3" win; "c-old4" + new entries truncated.
        assert_eq!(d[0].chunk_id, "c-old1");
        assert_eq!(d[2].chunk_id, "c-old3");
    }
}
