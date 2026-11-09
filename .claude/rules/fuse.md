---
paths:
  - "crates/dtfs-fuse/**"
---

# dtfs-fuse rules (CLAUDE.md § A.3.2)

Narrative and bug history: `docs/src/internals/invariants.md` § A.8
(`dtfs-fuse/*`) and the archived rule text there.

## Locks and awaits

- Never hold a DashMap guard (`open_files`, `known_locations`) across an
  `.await` or a spawn: clone what you need, drop the guard, then await.
- A lapsed write lease is re-acquired once under the handle's own
  `lease_owner_id` (`reacquire_write_lease`) before `write()`/`flush()`
  gives up; only a lease held by another writer aborts.
- Background prefetch is bounded by `client.chunk_read_timeout_secs`
  like the foreground fetch (`0` = unbounded).
- `flush()` drops the `lock_owner`'s POSIX locks (kernel close contract).
- POSIX lock ranges from the kernel are INCLUSIVE (`end` = last byte,
  `OFFSET_MAX` = whole file); unlock and re-acquire carve the owner's
  ranges (`carve_owner_range`), never drop a whole overlapping lock.

## Inodes and directories

- `InodeMap` counts kernel lookups (`lookup_ref`: lookup/create/mkdir/
  symlink/link; `add_ref` per readdirplus entry actually pulled by fuse3).
  `forget` drops the mapping (and the path's `mode_overrides` entry) at 0.
  fuse3 0.7 `batch_forget` has no counts: it only drops inodes not looked
  up for `BATCH_FORGET_QUIET` (5 s). Plain `readdir` mints inodes without
  a ref (never forgotten; bounded by distinct paths listed).
- `rename` re-keys every descendant path and drops the replaced
  destination's inode.
  `mode_overrides` are re-keyed the same way (`rekey_mode_overrides`).
- Symlink kind comes from the manager's `is_symlink` (ListDirectory +
  GetFileLocations; old managers send false) via `file_kind`.
- `cluster_err_to_errno`: NotFound ENOENT, FailedPrecondition ENOTEMPTY,
  InvalidArgument EINVAL, else EIO.
- `opendir` snapshots the listing (fh -> `Arc<Vec<DirEntry>>`, dropped at
  `releasedir`); readdir/readdirplus serve pages from it, cookie = position
  (`dir_entry_cookie`). Do not re-list per page.

## Writes

- A bare path `truncate` (no writable fh) to an unaligned shrink opens a
  temporary internal writer (`truncate_needs_temp_writer`) so the tail chunk
  is rewritten via the normal lease + commit path, then released.
- `open`/`create` with `O_TRUNC` on a writable fd truncate to 0 after the
  open succeeds (release the fh and return the errno if that fails);
  `open_err_to_errno` maps `LeaseBusy` to EBUSY for open/create/setattr.
- `fallocate`: mode 0 only grows (`max(current, offset+len)`, else no-op);
  KEEP_SIZE no-op; any other mode bit EOPNOTSUPP (`fallocate_target_size`).
- `write()` seeds every chunk it must preserve: chunks in the write range,
  the partial tail chunk when writing past EOF (`hole_tail_chunk`), and a
  hole chunk below EOF gets a zero seed (`read_seed_bytes`). "No chunk at
  index" = zeros via `seed_hole` (gap: never written); "chunk with no live
  address" keeps refusing at commit.
- After a commit, `mark_committed_and_refresh` merges the committed chunks
  into each handle's snapshot and the metadata cache entry
  (`merge_into_snapshot`: replace the index, grow size). Instead it sets the
  handles' `locations = None` (and drops the cache entry) when a handle has
  no snapshot, the commit was lease-less, the snapshot is positional/
  sizeless, another commit on the path overlapped it (`CommitTracker`;
  commits are not serialized per path, so merge order may differ from apply
  order), or the committing fh is closed/detached (`merge_allowed`). Never
  eager-fetch after a commit (fetch results can land out of order) and never
  per batch (quadratic on big files). `refetch_locations` (read, write and
  the post-truncate refresh) uses a fetch only if no commit on the path
  overlapped it (`FetchToken::may_install`); otherwise it waits for the path
  to go quiet (`wait_quiet`, Notify, no polling) and fetches again, failing
  with EIO after `QUIET_WAIT` (the write-lease length). Never use or install
  an overlapped fetch. SetFileSize (truncate, set_size) holds a commit slot.
  Every other fetch that stores a snapshot (open, create, getattr and link
  cache) takes `begin_fetch` first and stores only if `may_install()`
  (`gated_snapshot`; open/create check after inserting the handle, else
  `locations = None`). read() re-fetches once per snapshot before serving
  zeros for a missing entry below EOF (`gap_needs_refetch`,
  `holes_confirmed`). Writers seed only from a snapshot fetched after the
  lease grant via `quiet_fetch` (`seed_snapshot_usable`): writable open
  re-fetches after the grant; truncate's size/tail fetches use
  `quiet_fetch`. Snapshots are
  `Arc<FileLocations>` (handles and metadata cache): read()/write() clone
  the Arc, never the chunk list; the merge is copy-on-write (`merge_shared`).
- `FileLocations.chunks` is sorted by chunk index (`sort_chunks_by_index` at
  fetch, the merge inserts in order). Per-call lookups use it: `chunk_at`
  binary search, `chunk_count` from the last entry, `chunks_in` for the
  prefetch window. No per-call or per-chunk walk over the whole list.
- Handle `locations = None` means stale; `write()`/`read()` must
  `refetch_locations` before using them.
- `rename` over a path with open writers and `unlink`/`rmdir` of one
  detach those handles (`detach_handles`: path becomes `<path>\0detached-N`,
  unresolvable by the manager) so their commits fail instead of landing in
  the replacement; `reacquire_write_lease` refuses detached handles.
- The lease is renewed BEFORE each commit in `write()` (`renew_if_due`: top,
  before every drain), never after. `set_size`/truncate-refresh failure set
  handle `locations = None`. `create()` seeds from the manager's snapshot
  when it kept a populated file (`committed_any = !chunks.is_empty()`).
- A writable open of a path this process already holds a lease on (knfsd
  reopen, one per NFS COMMIT) takes a writable handle's owner, lease_id,
  deadline and snapshot `Arc` (`shared_writer`): no fetch, no
  acquire_write_lease, no post-lease `quiet_fetch`, not cached into the
  metadata cache; `FetchToken::may_install` (no commit in flight at the
  copy, none started/ended since; a commit in flight may merge into the
  lender before the insert) gates the handle's snapshot (else `None`,
  guarded refetch on first use). The open-time
  O_APPEND tail seed runs only for a sole writer (no reuse, no other
  writable handle of the path) and only if `chunk_needs_seed`: it holds no
  `path_guard`/bg wait, and a stale partial seed of a chunk in an in-flight
  batch truncated it at commit; write()'s RMW seeds the EOF chunk instead.
  That handle's snapshot `None` = fetch path with the same owner
  (`shared_lease_owner`; same owner = manager re-acquire, no EBUSY). Prod:
  two O(file) fetches + a Raft write per COMMIT, 29 -> 20 MiB/s.
  `release()` skips `release_write_lease` while another handle shares the
  owner (`lease_owner_still_open`).
- `release()` drops the buffer when no other writer is open; a refused
  commit is logged at error with chunk indices and bytes lost. flush and
  fsync return the commit error.
- `writebuf.rs` batches `write.flush_batch_chunks` (default 4, clamp
  1..=32) complete chunks per `flush()`; `flush_all` drains everything.
  A write past the logical end zero-fills only the hole's edge chunks
  (`hole_fill_ranges`: old partial tail to its chunk end, the write's chunk
  start to the offset); whole chunks inside the hole get no buffer and no
  upload (missing index = zeros). When it skips a whole chunk, write() first
  `set_size`s the manager to max(logical end, `visible_size`) (never below a
  committed/in-flight end: SetFileSize drops chunks above it), since a file
  with no SetFileSize reports the sum of its chunk sizes; commits then grow
  it. `FileWriteBuffer.extent` is the buffered end (`logical_size` =
  max(extent, chunk ends), kept across partial drains, `None` once fully
  drained, reset by the next write/seed into an empty entry, set by
  `truncate`). A missing-index seed in write() clamps to that end, not the
  snapshot's `total_size` (skipped chunks lie above the cluster EOF).
  Zeros a write filled only because it landed beyond them (hole fill via
  `write_hole`, `ChunkBuf::merge` gap, seed shorter than the buffered start)
  are `ChunkBuf.gaps` until a write covers them; a gappy chunk is complete
  but not `flush()`-eligible (`is_flushable`; knfsd out-of-order WRITEs
  otherwise uploaded each chunk twice). Bytes seeded from an existing
  cluster chunk are content; the zero seed of a missing index
  (`seed_hole`) is gap minus buffered bytes. `flush_all`, the cap drain and
  `restore_segments` (via `ChunkSegment.gaps`) keep/commit them as zeros.
  A chunk drained with gaps leaves them in `WriteBuffer.gap_hints` (per
  path + index; `note_drained`); `seed_chunk` re-applies them minus buffered
  bytes, so the RMW of the committed zeros stays gappy (else x1.6 uploads
  on prod). Hints only steer flush eligibility, never content: dropped on a
  gap-free drain, as writes cover them, by `remove`; re-keyed by `rename`,
  clipped by `truncate`.
  `flush_segments_to_cluster` puts a batch's segments at most
  `write.upload_concurrency` (default 2, clamp 1..=16) at a time
  (`upload_segments`, ordered `buffered`, lowest-index error wins, nothing
  starts after it) and sends ONE `commit_chunk_locations` only after all
  segments reached quorum. Do not unbound it: puts share a fixed 60 s
  deadline and each buffers ~3 MiB per target (put channel depth 2).
- Every drain that goes to `flush_segments_to_cluster` (write flush, cap
  drains, `commit_all`) holds `WriteBuffer::begin_inflight` (`Bytes`
  clones, no copy) until the restore (failure) or
  `mark_committed_and_refresh` (success) is done, so read() (`overlay`,
  in-flight under the buffer) and lookup/read size (`visible_size`) still
  see drained bytes while the upload runs. Without it a reader between
  knfsd's close (flush drains) and the commit saw size 0 / EOF. Write-side
  EOF decisions keep using `logical_size` (async_commit: `write_eof`, below). `WriteBuffer::remove` (unlink,
  rename-over, last writer's release) also drops the in-flight entries of
  the path and its subtree, and `truncate` clips them to the new size, so
  the old file's drained bytes never show after rm+recreate, O_TRUNC or
  rename-over (a later guard drop is a no-op).
- Batch is byte-clamped at `FLUSH_BATCH_MAX_BYTES` (64 MB); total dirty
  bytes per open file are capped at `DIRTY_BYTES_CAP` (2x) and
  force-drained via `WriteBuffer::cap_drain` (gap-free complete chunks,
  then gappy complete ones only until under the cap, never the
  chunks of the pending write: `keep`, they were just seeded) before a hole
  fill or write would exceed it. A failed cap drain restores the drained
  segments and drops the write's own fresh in-range seeds
  (`remove_chunks`, chunks that held no bytes before); the hole tail seed
  stays (it masks a shrink's stale bytes under the hole zeros).
- `write.async_commit` (default off = all of the above unchanged): FUSE
  fsync (`DtfsFilesystem::fsync`) commits nothing and only reports a pending
  background error (`OpenFile.write_error`, once per fh, EIO; any successful
  writable `commit_all` (timer retry, last writer's flush/release) clears it
  first, `record_aged_commit`; release's buffer drop and
  ghost unlink follow its own `commit_all` result only). FUSE flush
  (every close(2), whose result close returns; RELEASE is async and its
  reply ignored) and `release()` run `commit_all` (flush_all) under
  `path_guard`, except when another writable, non-detached fh of the path
  without `close_seen` stays open (`defer_close_commit`): then they commit
  nothing (Ok, no buffer drop, no ghost unlink) and flush only reports the
  pending error. flush sets `close_seen` under the guard, so of two racing
  closes the later commits; the last writer's flush/release commits. A 1 s timer (`commit_aged`, at most
  `AGED_COMMITS_PER_TICK` paths) commits a path whose oldest buffered byte
  (`ChunkBuf.since`, kept across `restore_segments`) is `async_commit_secs`
  old, through `commit_all` with a writable non-detached fh. The cap drain
  then also drains partial chunks outside `keep`. The timer is
  not serialised by the kernel like write/fsync/setattr, so write(), a
  writer's truncate, flush, release and the timer hold `path_guard` (per-path
  tokio mutex, async mode only, one guard at a time; write/flush/release
  take it via `fh_path_guard`, which re-reads the fh's path after acquiring
  and retries if a rename re-keyed it). Only `rename` holds
  several: `from` plus every writable handle at or below `from` or `to`
  (`rename_lock_paths`, taken in sorted order) across `rename_file` and the
  buffer/handle re-key. A writer opened under `from` while rename waits for
  those guards is not covered (the kernel's rename locks make that rare).
- `write.async_commit` background upload: write()'s batch (`flush()`) is
  handed to `spawn_bg_upload` and write() returns (prod: one knfsd thread
  got nothing while each batch uploaded, 9-20 MiB/s). At most ONE batch per
  path (`bg_uploads`: the batch's exact sorted chunk index set + bytes, not a
  lo..=hi range: interleaved NFS streams make batches like {2,19,36,53} and
  a range made most writes wait); while it runs, write() does not
  call `flush()` (complete chunks stay buffered). Not more: overlapping
  commits of a path make the merge fall back to a full re-fetch. The task
  (`bg_upload`) renews the lease (`renew_if_due`), keeps `begin_inflight`
  until restore/merge, on failure restores under the fh's current path and
  arms every writer's `write_error` (`record_aged_commit`, success clears
  it); `BgSlot` clears the entry and wakes `wait_bg_uploads` (Notify) even on
  panic. It holds NO `path_guard` (writes would block on it again); instead
  everything that could touch those chunks waits for it first:
  write() into a chunk of the batch (`range_hits_upload`; else it re-seeds
  from the pre-commit snapshot and the restore clobbers it), a write whose
  dirty + in-flight bytes would meet `DIRTY_BYTES_CAP` (`bg_backpressure`,
  then the cap drain as before), `commit_all` (last writer's flush/release,
  timer), truncate, rename (every lock path) and unlink; NOT async fsync
  (knfsd fsyncs per NFS COMMIT; waiting stalled its one thread). Waiters
  may hold `path_guard` (the task never takes it: no deadlock), and holding
  it means no new batch starts. Write-side EOF (append offset, hole fill,
  tail seed) uses `write_eof` = `visible_size` in async mode (the in-flight
  batch counts, else the next append zero-filled over it). Every EOF use in
  write() (append offset + tail seed, hole fill, size pin) is computed right
  before it by `eof_after`: FIRST the buffer (+ in-flight), THEN the
  handle's CURRENT snapshot (refetched if `None`), max; seeds use that
  snapshot. Never a snapshot cloned before an await: a batch (bg or another
  fd's `commit_all`, neither holds `path_guard` against it) may retire in
  between, and it merges into the handles before dropping its overlay, so
  only this order always counts it (else the pin went below the committed
  end and SetFileSize dropped committed chunks). fallocate's floor
  (`size_floor`) reads `visible_size` before the fetch and the handles.
  Every drain into an overlay goes through `WriteBuffer::drain` (overlay
  registered under the buffer entry write lock): `flush_all_inflight`
  (commit_all), `flush_inflight` (write()'s batch flush; async hands the
  overlay id to `spawn_bg_upload`, adopted by the task), `cap_drain_inflight`
  (both cap drains). `visible_size` and `overlay()` read the overlay under
  the entry's read lock (order buffers -> inflight; no entry: overlay read,
  then re-check), so a drain or restore never hides the bytes from an EOF
  reader or a read(). Never `flush`/`cap_drain` then `begin_inflight`.

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
- A disk-cache hit is also inserted into the readahead memory cache
  (`promote_disk_hit`); the readahead cache evicts oldest-inserted first and
  drains per entry (never `clear()` + `store(0)`). The prefetch candidate
  list is limited to `chunk_idx+1..=chunk_idx+prefetch_chunks`.
- `LocalChunkCache` removals (evict and age prune) go through `evict_entry`.
- A foreground miss writes the disk cache in the background via
  `spawn_bounded` (`LOCAL_CACHE_PUT_MAX_INFLIGHT`, 2); a put without a
  permit is dropped, never queued or awaited on the read path.
- Client `read_chunk` verifies the streamed bytes against the 64-hex
  chunk id (IntegrityFaultError on mismatch).
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

## NFS re-export (knfsd)

- knfsd creates files via FUSE_MKNOD, never FUSE_CREATE: `mknod` is
  regular-file only (else EPERM, never ENOSYS), EEXIST on an existing name
  (lookup first), namespace entry only via `create_entry` (no lease, no fh;
  the follow-up OPEN takes the lease, so the ghost-unlink cannot hit it).
- `lookup(dir, ".")` / `".."` resolve to the dir / its parent (root's ".."
  is root) for NFS handle decoding (`dot_lookup_target`).
- Every entry reply carries `entry_generation(ino)`: one random non-zero
  u32 per process, 0 for root. Known limit: nodeids restart at 2 per
  process, so NFS clients get ESTALE after a dtfs-fuse restart and must
  remount. Other limits: NFSv3 EXCLUSIVE create retransmit gives EEXIST,
  chown is ignored (daemon uid/gid), repeated open/flush/release during
  long NFS writes is untested end to end (a second writable open shares
  the first handle's lease and snapshot, `shared_writer`).
- knfsd opens, fsyncs and closes a separate file for every NFS COMMIT
  while the NFSv4 open-state file stays open (no NFS OPEN/CLOSE). With
  `write.async_commit` that close commits nothing (`defer_close_commit`);
  committing there re-uploaded every partial chunk per COMMIT (x6.9).
