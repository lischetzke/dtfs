# DTFS Data-Integrity Audit — 2026-05-18

Senior-developer review of write / migrate / delete / scrub paths. 16
findings, 6 HIGH-severity. The HIGH findings cluster around three
concerns:

1. **Crash-recovery is broken on the streaming write path** (#1a, #1b,
   #1c). A power loss during sustained writes can lose chunk bytes *and*
   leave orphan tmp/WAL files that nothing ever reclaims.
2. **Unlink leaks chunks forever** (#3a). Manager removes Raft metadata
   but never tells data nodes to remove the on-disk chunks. The
   SOAK-RECOVERY doc previously listed this as "open known issue: not a
   correctness issue, just visibility" — that classification was wrong.
3. **Silent corruption risks** (#5, #7a). Scrub detects corruption but
   doesn't repair, and `commit_chunk_locations` doesn't verify lease
   ownership so a slow flush can clobber a fresh writer's bytes.

Bug letters K..P assigned below for the HIGH-severity fixes.

| # | Sev | Section | One-liner | Fix bug-id |
|---|-----|---------|-----------|------------|
| 1a | HIGH | 1 / put_chunk | streaming `.dat` never fsynced; only `flush()` | **K** |
| 1b | HIGH | 1 / put_chunk | parent dir of `.dat` and `.meta` never fsynced after rename | **L** |
| 1c | HIGH | 1 / put_chunk | `recover_from_wal` never called at startup; orphans linger | **M** |
| 2  | MED  | 2 / migrate | `VerifyChunk` doesn't re-hash, only checks readability | (queue: Q) |
| 3a | HIGH | 3 / delete | manager never fans out `DeleteChunk` after `unlink` | **N** |
| 3b | MED  | 3 / delete | crash mid-delete cannot be retried (no journal) | follows N |
| 3c | LOW  | 3 / delete | `delete_chunk` skips its own WAL entry | follows M |
| 4  | LOW  | 4 / rename | write_leases not migrated alongside posix_locks | (queue: R) |
| 5  | HIGH | 5 / scrub | scrub WARNs corruption but doesn't trigger repair | **O** |
| 6  | OK   | 6 / read | replica fallback correct (no finding) | — |
| 7a | HIGH | 7 / lease | `commit_chunk_locations` doesn't verify lease_id | **P** |
| 7b | MED  | 7 / lease | mid-write renewal failure swallowed silently | follows P |
| 8  | MED  | 8 / serde | not every StateMachineData field has `#[serde(default)]` | (queue: S) |
| 9  | HIGH | 9 / fsync | snapshot tmp + parent dir not fsynced | merge with L |
| 10a | LOW | 10 / counters | `Ordering::Relaxed` on hot path (eventually consistent — OK) | — |
| 10b | MED | 10 / counters | counters not decremented if delete partial-fails | (queue: T) |

## Severity summary
- HIGH: 6 (fix now — Bugs K..P)
- MED:  6 (Q, R, S, T queued)
- LOW:  4 (acknowledged, not blocking)

## Fix ordering rationale

K, L, M form the "crash safety on streaming writes" cluster — fix
together. N completes the unlink loop. O makes scrub useful. P closes
the concurrent-writer races. P transitively requires a proto change
to thread `lease_id` through `CommitChunkLocations`; we accept that.

The MED items (Q, R, S, T) are queued as follow-ups; each gets a
test-first commit in subsequent passes.
