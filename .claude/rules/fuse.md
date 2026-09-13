---
paths:
  - "crates/dtfs-fuse/**"
---

# dtfs-fuse rules (AGENTS.md § A.3.2)

Narrative and bug history: `docs/src/internals/invariants.md` § A.8
(`dtfs-fuse/*`) and the archived rule text there.

## Locks and awaits

- Never hold a DashMap guard (`open_files`, `known_locations`) across an
  `.await` or a spawn: clone what you need, drop the guard, then await.
- `MigrationTrigger` is fire-and-forget (`spawn_on_remote_read`), never
  awaited inline in `read()`.

## Writes

- `writebuf.rs` batches `write.flush_batch_chunks` (default 4, clamp
  1..=32) complete chunks per `flush()`; `flush_all` drains everything.
- Batch is byte-clamped at `FLUSH_BATCH_MAX_BYTES` (64 MB); total dirty
  bytes per open file are capped at `DIRTY_BYTES_CAP` (2x) and
  force-drained via `flush_complete()` before a hole fill or write would
  exceed it.

## Reads and prefetch

- Foreground miss: first `readahead.join_pending()` on the FIRST candidate
  only, bounded by `PREFETCH_JOIN_TIMEOUT` (2 s), refusing to wait on a
  prefetch that has not yet acquired its slot; else fall through to the
  candidate loop (P0-2 fallover budget stays intact).
- Foreground misses are single-flighted per chunk_id
  (`claim_foreground_fetch` / `complete_foreground_fetch`); a Waiter whose
  Owner failed fetches itself.
- Prefetch fan-out is bounded by the `prefetch_slots` semaphore
  (`readahead.prefetch_max_inflight`, default 2), acquired in ascending
  chunk order. Do NOT re-add a `cache_bytes() >= max_cache_bytes`
  dormancy gate.
- Per (fh, chunk) transition (`should_notify`): `record_cache_hit` at most
  once, bounded by `CACHE_HIT_NOTIFY_MAX_INFLIGHT` (8, skip when full);
  `readahead.on_chunk_read` on every transition, hit or miss.
- Disk cache: viewer-read atime hint comes from `OpenFile.bytes_read_on_fh`
  + `ReadAheadEngine::is_active()` (`is_viewer_read`), not one kernel
  read's length; `evict_to_size` targets `EVICT_TARGET_PCT` (90%).

## Memory ladder

- Warn: halve readahead `cache_mb`. Shed: drop unconsumed prefetch and
  stop spawning (`prefetch_enabled` pinned false; `on_chunk_read`
  early-returns). systemd `MemoryMax` is the backstop.

## Errors

- Every fuse op error goes through `cluster_err_to_errno`: ENOENT only
  for a genuine gRPC `NotFound`, EIO for anything transient. Never a bare
  `Err(_) => ENOENT`.
