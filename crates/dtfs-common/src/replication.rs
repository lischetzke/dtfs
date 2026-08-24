//! WI-04 — shared replication deadlines.
//!
//! `dtfs-manager`'s scheduler drives every `ReplicateChunk` RPC to a data
//! node, which in turn streams the chunk to a THIRD node (the target) and
//! waits for that whole transfer inside the RPC handler (see
//! `dtfs-data/src/server.rs::replicate_chunk`). Two independent timeouts
//! bound the SAME unit of work — the manager's outer RPC deadline and the
//! data node's own inner transfer budget — and the manager, as the sole
//! caller, MUST always give up second so the callee's own bookkeeping
//! (partial-transfer cleanup, its own timeout message) has a chance to run.
//! `dtfs-manager` does not depend on `dtfs-data`, so this module is the one
//! shared home that makes "outer > inner" a checkable invariant instead of
//! a comment split across two crates.
//!
//! Mirrors `dtfs-raft/src/network.rs::snapshot_rpc_timeout`, which solved
//! the identical class of bug (CO-3-6) for Raft snapshot sends.
//!
//! ## B4 — the inversion bug and its fix
//!
//! PRE-WI-04 the two deadlines were unrelated fixed constants: the manager's
//! outer `MIGRATION_RPC_TIMEOUT` was 60s and the data node's inner
//! `REPLICATE_TIMEOUT` was 120s. That is backwards — the outer fires BEFORE
//! the inner at every chunk size, so the inner budget was dead code and the
//! manager always abandoned a copy the data node still considered healthy.
//! That inversion is the bug WI-04 was written to fix.
//!
//! WI-04's first cut removed the inversion the wrong way round: it kept the
//! inner floor at 120s and derived the outer from it (floor + margin, so
//! >= 150s for EVERY chunk including the 4 MB default). Outer > inner held,
//! but it walked back Bug CN's invariant that a stalled data node cannot
//! hang an `advance_tasks` round — and with it the 30s scheduler tick — for
//! longer than ~60s.
//!
//! The fix is to LOWER the inner floor, not raise the outer: `REPLICATE_
//! TIMEOUT_FLOOR` drops from 120s to 30s, so with the unchanged 30s
//! `MIGRATION_RPC_MARGIN` the outer deadline lands at exactly 60s for the
//! 4 MB default chunk — Bug CN's bound intact. Size-scaling for larger
//! chunks is kept, but both budgets are now also hard-capped
//! (`REPLICATE_TIMEOUT_CEILING` / `MIGRATION_RPC_CEILING`) so a large-chunk
//! volume (128 MB is the largest chunk size this codebase supports) can
//! never park an advance round — and with it the whole scheduler loop —
//! for minutes, which IS the Bug CN failure. The two ceilings are chosen so
//! the outer stays strictly above the inner even at the cap boundary: that
//! inversion must never come back at any chunk size.
//!
//! Resulting numbers:
//!
//! | chunk size                  | inner (data node) | outer (manager) |
//! |------------------------------|-------------------|------------------|
//! | 4 MB (default)                | 30s                | **60s**          |
//! | 32 MB                          | 32s                | 62s              |
//! | 64 MB                          | 64s                | 94s              |
//! | 128 MB (largest supported)    | 128s               | 158s             |
//! | 256 MB                         | 256s               | 286s             |
//! | >= 270 MB (cap)                | 270s               | **300s**         |

use std::time::Duration;

/// Effective per-copy throughput we budget for. Matches CO-3-6's snapshot
/// floor and the ~2.5 MB/s a copy actually gets when at most
/// `MIGRATION_COPIES_PER_TARGET` (2) copies share the 40 Mbps LELI link
/// (2.5x margin).
pub const REPLICATION_FLOOR_BYTES_PER_SEC: u64 = 1_024 * 1_024;

/// Lower bound on the data node's own transfer budget. B4 — lowered from
/// 120s to 30s: with the unchanged 30s `MIGRATION_RPC_MARGIN` this puts the
/// manager's outer deadline at exactly 60s for the 4 MB default chunk, so
/// Bug CN's bound — a stalled data node cannot hang a `buffer_unordered`
/// copy round or the 30s scheduler tick — survives WI-04 unchanged at the
/// default chunk size. (The pre-B4 value of 120s is what caused the
/// inversion: see the module doc above.)
pub const REPLICATE_TIMEOUT_FLOOR: Duration = Duration::from_secs(30);

/// Extra the manager allows on top of the callee's own budget: connect,
/// handler dispatch, and the source's own error-return trip. Guarantees
/// the callee's timeout always fires first.
pub const MIGRATION_RPC_MARGIN: Duration = Duration::from_secs(30);

/// B4 — hard ceiling on the manager's outer deadline. Without it a
/// 128 MB-chunk volume (or a mis-sized one) could park an advance round —
/// and with it the whole scheduler loop — for many minutes, which is
/// exactly the Bug CN failure. 300s is ~6x the worst real copy (128 MB at
/// the ~2.5 MB/s a copy gets when `MIGRATION_COPIES_PER_TARGET`=2 share the
/// 40 Mbps LELI link) and 5x the default-chunk deadline.
pub const MIGRATION_RPC_CEILING: Duration = Duration::from_secs(300);

/// B4 — inner ceiling, derived so the outer stays at or below
/// `MIGRATION_RPC_CEILING` while remaining STRICTLY greater than the inner
/// budget at EVERY chunk size (the inversion WI-04 introduced must not come
/// back at the cap boundary either): `REPLICATE_TIMEOUT_CEILING +
/// MIGRATION_RPC_MARGIN == MIGRATION_RPC_CEILING`.
pub const REPLICATE_TIMEOUT_CEILING: Duration = Duration::from_secs(270);

/// Data-node side (inner) budget for one outbound replication of
/// `chunk_bytes`. Floored at `REPLICATE_TIMEOUT_FLOOR` (30s), capped at
/// `REPLICATE_TIMEOUT_CEILING` (270s).
pub fn replicate_transfer_timeout(chunk_bytes: u64) -> Duration {
    Duration::from_secs(chunk_bytes / REPLICATION_FLOOR_BYTES_PER_SEC)
        .clamp(REPLICATE_TIMEOUT_FLOOR, REPLICATE_TIMEOUT_CEILING)
}

/// Manager side (outer) budget for one `ReplicateChunk` RPC copying
/// `chunk_bytes`. Always strictly greater than
/// `replicate_transfer_timeout(chunk_bytes)`, and never exceeds
/// `MIGRATION_RPC_CEILING` (300s) because the inner budget is already
/// capped below it by `MIGRATION_RPC_MARGIN`.
pub fn migration_rpc_timeout(chunk_bytes: u64) -> Duration {
    replicate_transfer_timeout(chunk_bytes) + MIGRATION_RPC_MARGIN
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manager_deadline_exceeds_data_node_deadline() {
        for bytes in [
            0u64,
            4 << 20,
            32 << 20,
            64 << 20,
            128 << 20,
            256 << 20,
            269 << 20,
            270 << 20,
            271 << 20,
            512 << 20,
            1 << 30,
            4u64 << 30,
        ] {
            let inner = replicate_transfer_timeout(bytes);
            let outer = migration_rpc_timeout(bytes);
            assert!(
                outer > inner,
                "outer {outer:?} must exceed inner {inner:?} at {bytes} bytes"
            );
            assert!(inner >= REPLICATE_TIMEOUT_FLOOR);
            assert!(inner <= REPLICATE_TIMEOUT_CEILING);
            assert!(outer <= MIGRATION_RPC_CEILING);
        }
    }

    #[test]
    fn transfer_budget_scales_with_chunk_size() {
        assert_eq!(replicate_transfer_timeout(4 << 20), Duration::from_secs(30));
        assert_eq!(replicate_transfer_timeout(128 << 20), Duration::from_secs(128));
        assert_eq!(replicate_transfer_timeout(1 << 30), REPLICATE_TIMEOUT_CEILING);
    }

    // B4 — Bug CN's stated bound: a stalled data node cannot hang an
    // advance round or the 30s scheduler tick for more than ~60s at the
    // default 4 MB chunk size.
    #[test]
    fn b4_default_chunk_keeps_the_bug_cn_sixty_second_bound() {
        assert_eq!(migration_rpc_timeout(4 << 20), Duration::from_secs(60));
    }

    // B4 — guards against a future edit that raises REPLICATE_TIMEOUT_CEILING
    // to meet MIGRATION_RPC_CEILING and silently re-creates the WI-04
    // inversion at large chunk sizes.
    #[test]
    fn b4_inner_deadline_is_strictly_below_outer_at_the_cap_boundary() {
        assert_eq!(
            REPLICATE_TIMEOUT_CEILING + MIGRATION_RPC_MARGIN,
            MIGRATION_RPC_CEILING
        );
        for bytes in [269u64 << 20, 270 << 20, 271 << 20, 1 << 30, u64::MAX / 2] {
            let inner = replicate_transfer_timeout(bytes);
            let outer = migration_rpc_timeout(bytes);
            assert!(
                inner < outer,
                "inner {inner:?} must stay strictly below outer {outer:?} at {bytes} bytes"
            );
            assert!(outer <= MIGRATION_RPC_CEILING);
        }
    }
}
