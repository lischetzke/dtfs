---
paths:
  - "crates/dtfs-fuse/**"
---

# dtfs-fuse memory ladder + await-guard rules (AGENTS.md § A.3.2, § A.8)

## Never hold a DashMap guard across an `.await`

`filesystem.rs::read()` clones the per-call data (path/chunk_size/
volume_id/locations) out of the `open_files` entry and drops the DashMap
guard BEFORE the per-chunk network loop (same idiom as `write()`/
`flush()`). Holding an `open_files` guard across an `.await` serializes
concurrent opens of the same file. The same rule covers
`known_locations`: the post-fetch migration-hint lookup
(`known_locations.get(&node_addr)`) uses `.map(|r| r.clone())` so the
`Ref` guard drops at the end of that statement, before the migration
trigger runs, never across an `.await`/spawn.

`migration` (`MigrationTrigger`) is fire-and-forget: `read()` calls
`spawn_on_remote_read` (`tokio::spawn`, never `.await`s `on_remote_read`
inline) because the RPC it issues (`DtfsClient::allocate_chunks`) sets no
per-call deadline and retries `Unavailable` for up to the 15s manager
walk deadline — awaiting it inline after the chunk bytes are already in
hand could stall a completed read for that long.

## Write batching (WI-01)

The write buffer (`writebuf.rs`) holds back complete chunks until
`write.flush_batch_chunks` (default 4, clamp `1..=32`) are available
before `flush()` fires, so `flush_segments_to_cluster` amortizes one
`AllocateChunks` + `CommitChunkLocations` round trip over a batch instead
of paying it per chunk. `flush_all` (fsync/close) always drains held-back
chunks regardless of batch state. The effective batch is additionally
byte-clamped (`writebuf::effective_batch_chunks`,
`FLUSH_BATCH_MAX_BYTES` = 64 MB) so batching never holds more than 64 MB
of un-committed data per open file — the memory ladder below sheds
readahead, not write buffers.

R1 (FUSE-1 memory half) — `writebuf::DIRTY_BYTES_CAP` (2x
`FLUSH_BATCH_MAX_BYTES`) governs total dirty bytes per open file: the
buffer is force-drained via `WriteBuffer::flush_complete()` whenever a
sparse-hole zero-fill or an incoming write would meet or exceed the cap,
so a write far past EOF (e.g. a torrent client's first piece landing at
offset 100 MB) can no longer materialise the whole hole into heap before
committing anything.

## Foreground-read prefetch join (WI-05 / B2)

`read()`'s foreground miss tries `readahead.join_pending()` against ONLY
the FIRST candidate (prefetch targets `read_candidates`' head), bounded
by the fixed `readahead::PREFETCH_JOIN_TIMEOUT` (2s, deliberately NOT
`client.chunk_read_timeout_secs`), and refuses to wait on a prefetch that
has not yet acquired its `prefetch_slots` permit (still queued), instead
returning `None` immediately. A chunk genuinely on the wire is JOINED
rather than re-fetched with a second `GetChunk` over the same link;
`None` falls through to the unchanged candidate loop, preserving the
P0-2 fallover budget (`tests/release/cases/69-read-hung-node-fallover.sh`,
`ELAPSED <= TMO*3`).

## Prefetch fan-out bound (WI-05)

`readahead.rs::ReadAheadEngine` bounds prefetch fan-out with a
`prefetch_slots` semaphore sized from `readahead.prefetch_max_inflight`
(default 2) — the prefetch WINDOW stays `prefetch_chunks` deep (default
8), but fetches acquire their slot chained in ascending order
(`spawn_prefetch`'s `prev_acquired` oneshot) so at most
`prefetch_max_inflight` `GetChunk` streams are ever on the wire at once,
nearest chunk first. Before this an uncapped fan-out shared one pooled
HTTP/2 channel per node N ways and starved the nearest chunk instead of
adding bounded heap. Do NOT re-add the old `cache_bytes() >=
max_cache_bytes` dormancy gate — it only ever saw stored bytes, never
in-flight ones, and could go permanently true under the default sizing,
silently killing all further prefetch.

## Memory ladder (CF-5, shared core in `dtfs-common/src/memory.rs`)

FUSE halves the readahead `cache_mb` at Warn, drops unconsumed prefetch
AND stops spawning new prefetch at Shed, via
`ReadAheadEngine.prefetch_enabled: Arc<AtomicBool>` — `on_chunk_read`
early-returns while it is false, so `drop_unconsumed()` is not undone by
the very next read; `ReadAheadCacheController::set_prefetch_enabled()` is
pinned false at Shed and true at Normal/Warn by
`main.rs::run_readahead_pressure_ladder`. `pending` holds
`PendingPrefetch { handle, done, started }`, where `started` is set true
the instant a prefetch task acquires its `prefetch_slots` permit,
letting `join_pending()` above distinguish a fetch genuinely in flight
from one still queued. systemd `MemoryMax` remains the hard backstop.

## FUSE error mapping (CO-4-16)

`fuse_adapter.rs` metadata ops map cluster errors via
`cluster_err_to_errno` — ENOENT ONLY for a genuine gRPC `NotFound`, EIO
for transient/unknown (manager outage, election, timeout). A blip mapped
to ENOENT makes Plex drop the library entry; route every new
error-returning op through the helper, never a bare `Err(_) => ENOENT`.
