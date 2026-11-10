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
  gives up; only a lease held by another writer aborts. A FRESH grant (new
  lease id: ours was gone) is refused, released and not adopted when the
  manager's file is not the handle's snapshot (`file_changed_under_lease`:
  other `created_at_ms`; a fresh chunk id the snapshot lacks at an index
  no local commit in flight covers (`CommitTracker::in_flight_chunks`, a
  whole-file one skips the chunk compare); with no local commit
  overlapping the check fetch also another `total_size` or an index the
  snapshot has and fresh lacks; no snapshot = not compared) (#134).
  `check_unchanged_since_snapshot` takes `begin_fetch` first, reads the
  in-flight set after the fetch, then the handle's snapshot (a commit
  gone from the set merged or set `None` before it ended), so no own
  commit can cause a refusal.
  A commit failing with an unknown outcome (`commit_outcome_unknown`:
  timeout, transport, Unknown/Unavailable/Internal; not a `rejected`
  response or a definite status) sets the path's handles `locations =
  None` before the restore: it may have applied our chunk ids, which the
  snapshot lacks, and the re-acquire would refuse forever.
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
  InvalidArgument EINVAL, AlreadyExists EEXIST (#120), else EIO.
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
  sizeless, a conflicting commit on the path overlapped it (`CommitTracker`;
  commits are not serialized per path, so merge order may differ from apply
  order), or the committing fh is closed/detached (`merge_allowed`).
  Conflicting = the chunk index sets intersect, or either is a whole-file
  slot (`begin`: SetFileSize/truncate). Overlapping commits of DISJOINT sets
  (`begin_chunks`, background batches of one path) both merge
  (`CommitToken::may_merge`): each replaces only its own indices and the
  size is max(current, committed extent), so the merges commute. Such a
  non-exclusive merge updates the handles but leaves the metadata cache
  entry dropped (two refreshes race on its copy). Never
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
  The metadata cache stores a chunkless copy (`without_chunks`, also after
  a merge): it serves attributes and volume_id only (#135).
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
  owner (`lease_owner_still_open`). The manager keeps ONE lease id per
  owner, so a lender's release by that id also kills a same-owner reopen's
  lease. Three guards: after its insert, a reuse or same-owner fetch open
  whose lender is gone re-acquires (`reacquire_if_lender_gone`; failure
  removes the fh and fails the open); after its release RPC, release()
  re-acquires for a handle of the owner that appeared meanwhile
  (`reacquire_for_reopened_owner`); a commit rejected for a lost lease
  (`is_lease_conflict`) re-acquires once under the fh's own owner (refused
  if another owner holds it) and retries.
- `release()` drops the buffer when no other writer is open; a refused
  commit is logged at error with chunk indices and bytes lost. flush and
  fsync return the commit error.
- `writebuf.rs` batches `write.flush_batch_chunks` (default 4, clamp
  1..=64) complete chunks per `flush()`: it waits for that many and takes,
  lowest indices first, exactly that many when async `max_inflight_batches
  > 1` (more ready ones wait for the next flush), else every ready one up
  to the `batch_limits` bytes (`batch_take_chunks`: capped with one batch
  in flight, ready chunks piled up to the dirty cap and write() cap-drained
  ~110 MB synchronously); `flush_all` drains everything.
  A write past the logical end zero-fills only the hole's edge chunks
  (`hole_fill_ranges`: old partial tail to its chunk end, the write's chunk
  start to the offset); whole chunks inside the hole get no buffer and no
  upload (missing index = zeros). When it skips a whole chunk, write() first
  (before any padding; a failed pin drops the write's fresh seeds like a
  failed cap drain, so the buffer is unchanged)
  `set_size`s the manager to max(logical end, `visible_size`) (never below a
  committed/in-flight end: SetFileSize drops chunks above it), since a file
  with no SetFileSize reports its chunk extent (`FileMetadata::reported_size`;
  the sum of its chunk sizes on managers at v0.1.13 or older); commits then
  grow it. A `set_size` grow (at or above every handle's size, exclusive commit
  slot) updates the handles' snapshot sizes copy-on-write
  (`apply_size_change`); only a shrink or an overlapped one sets them
  `None` (else every sparse write cost an O(file) refetch and sent knfsd
  reopens back to fetch + acquire). `FileWriteBuffer.extent` is the buffered end (`logical_size` =
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
  `write.upload_concurrency` (default 2, clamp 1..=64) at a time
  (`upload_segments`, ordered `buffered`, lowest-index error wins, nothing
  starts after it) and sends ONE `commit_chunk_locations` only after all
  segments reached quorum; that commit carries the committing fh's own
  lease (`commit_lease_id`; path lookup only for a closed fh), and
  `bg_upload` commits under the fh's current path (a detached fh fails,
  never lands in a file re-created at the old path). Do not unbound it: puts share a fixed 60 s
  deadline and each buffers ~3 MiB per target (put channel depth 2).
  A segment's targets go through `put_targets` (#150): quorum
  `put_quorum` = max(ceil(N/2), min(manager `ack_floor`, N); 0 from an
  old manager = 2); an `Unavailable` target is re-put on the put ladder
  within `PUT_RETRY_DEADLINE` (60 s) while short of quorum; once met, a
  target in that retry ladder is dropped (never registered, its late
  copy is an orphan), but every target's FIRST attempt is awaited (the
  anchor target is not identifiable client-side). Only acked targets are
  committed.
- Every drain that goes to `flush_segments_to_cluster` (write flush, cap
  drains, `commit_all`) holds `WriteBuffer::begin_inflight` (`Bytes`
  clones, no copy) until the restore (failure) or
  `mark_committed_and_refresh` (success) is done, so read() (`overlay`,
  in-flight under the buffer) and lookup/read size (`visible_size`) still
  see drained bytes while the upload runs. Without it a reader between
  knfsd's close (flush drains) and the commit saw size 0 / EOF. Write-side
  EOF decisions use `write_eof` = `visible_size` (in-flight counts) in both
  modes. `WriteBuffer::remove` (unlink,
  rename-over, last writer's release) also drops the in-flight entries of
  the path and its subtree, and `truncate` clips them to the new size, so
  the old file's drained bytes never show after rm+recreate, O_TRUNC or
  rename-over (a later guard drop is a no-op).
- Batch is byte-clamped by `writebuf::batch_limits`: `max(64 MB, batch x
  4 MiB)` capped at 256 MB (4 MiB reference chunk, not the volume's: only a
  raised `flush_batch_chunks` grows it, so large-chunk volumes keep the old
  64 MB batch); total dirty bytes per open file are capped
  at `WriteBuffer::dirty_cap` = `max(128 MB, 2 x batch limit)` (defaults
  64 MB / 128 MB = `DIRTY_BYTES_CAP`; 64 x 4 MiB: 256 / 512 MB). The cap is
  per open writer and nothing sheds write buffers: dirty cap x open
  writers plus readahead must fit the client's `[memory]` budget (async
  mode adds a global bound, see `bg_backpressure`). They are
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
  ghost unlink follow its own `commit_all` result only). A new writable fh
  (open, create) inherits the union of its writable siblings'
  `failed_chunks` and then an armed `write_error` (`inherit_bg_failure`):
  knfsd's fresh per-COMMIT fh otherwise fsynced Ok after a failed batch and
  the NFS client dropped its pages. FUSE flush
  (every close(2), whose result close returns; RELEASE is async and its
  reply ignored) and `release()` run `commit_all` (flush_all) under
  `path_guard`, except when another writable, non-detached fh of the path
  without `close_seen` stays open (`defer_close_commit`): then they commit
  nothing (Ok, no buffer drop, no ghost unlink) and flush only reports the
  pending error. flush sets `close_seen` under the guard, so of two racing
  closes the later commits; the last writer's flush/release commits. A 1 s timer (`commit_aged`, at most
  `AGED_COMMITS_PER_TICK` commits; it walks every aged path oldest first
  and skipped ones do not count) commits a path whose oldest buffered byte
  (`ChunkBuf.since`, kept across `restore_segments`) is `async_commit_secs`
  old, through `commit_all` with a writable non-detached fh. It never waits:
  a path whose `path_guard` is held (`try_lock_owned`) or that has batches
  in flight is skipped until a later tick (one busy file stalled every
  other file's age commit). A path still busy at 2 x `async_commit_secs`
  (one knfsd stream always has a batch in flight) gets one task
  (`spawn_stale_commit`, `stale_commits`) that awaits its `path_guard` and
  runs `commit_all`. The cap drain
  then also drains partial chunks outside `keep`. Neither the timer nor
  FUSE_FLUSH (sent without the inode lock: another fd's close commit ran
  beside a write, which then zero-seeded over its in-flight bytes, R3-1)
  is serialised by the kernel with write, so write(), fsync (sync mode), a
  writer's truncate, unlink (across the bg wait, delete and detach: no new
  batch can start meanwhile), flush, release and the timer hold `path_guard` (per-path
  tokio mutex, BOTH modes (only the timer is async-only), one guard at a
  time; write/flush/fsync/release
  take it via `fh_path_guard`, which re-reads the fh's path after acquiring
  and retries if a rename re-keyed it). Only `rename` holds
  several: `from` plus every writable handle at or below `from` or `to`
  (`rename_lock_paths`, taken in sorted order) across `rename_file` and the
  buffer/handle re-key. A writer opened under `from` while rename waits for
  those guards is not covered (the kernel's rename locks make that rare).
- Stop (main.rs, both modes): after the unmount attempt (clean, failed or
  timed out) and BEFORE the lease release (`release_open_leases`: each
  lease an open non-detached handle holds, by id, once per id; #136:
  `release_leases_by_host` spares its pid and drops only expired leases),
  `commit_open_writers_then` commits each path with a writable,
  non-detached open fh once (`path_guard`, `commit_all`, which waits for the
  bg batch), and logs every path not confirmed committed (chunk indices,
  bytes) as lost. A busy mount (knfsd, an open fd) never gets RELEASE, so
  this is the only commit of fsync-acknowledged async bytes. `destroy`
  stays empty. Budget (main.rs): one 40 s deadline from the signal; unmount capped
  at 15 s, the commit gets the rest minus the 5 s lease release
  (`shutdown_commit_budget`), below `OLDEST_TIMEOUT_STOP_SECS` (45 s: hosts
  updated by `dtfs-cli update` keep their old unit file); the unit's 110 s
  is headroom only.
- `write.async_commit` background upload: write()'s batch (`flush()`) is
  handed to `spawn_bg_upload` and write() returns (prod: one knfsd thread
  got nothing while each batch uploaded, 9-20 MiB/s). Up to
  `write.max_inflight_batches` (default 1, clamp 1..=16) batches per path
  (`bg_uploads`: per batch its overlay id, exact sorted chunk index set +
  bytes, not a lo..=hi range: interleaved NFS streams make batches like
  {2,19,36,53} and a range made most writes wait); at the limit write()
  does not call `flush()` (complete chunks stay buffered). Puts in flight
  per path: up to max_inflight_batches x min(upload_concurrency, batch
  chunks); one batch ends with its slowest put, so with 1 the pipeline
  drained at every batch end (prod 59 MiB/s). Batches of a path have
  disjoint chunk sets (in-flight chunks left the buffer; writes into them
  wait), so their overlapping commits still merge (`may_merge`). Uploads
  run in parallel, but a batch's commit_chunk_locations waits until every
  earlier batch of the path ended, committed + merged or restored
  (`wait_commit_turn`: `bg_uploads` order, woken by `BgSlot`, no commit
  token held while waiting; it never waits on a later batch or a
  `path_guard`). A later batch committing first left a hole below it the
  manager reported as the summed size and the next sparse-write pin
  dropped the later chunks (FUSE-1); the manager now also reports the
  chunk extent for a file without explicit size. A failed batch
  (`fail_bg_batch`) restores, then records `(its id, next_inflight_id)` in
  `bg_doom`: every batch of the path drained after it and before its
  restore is doomed by drain order (ids are taken under the buffer entry
  lock the restore also takes), also one write() drained before the
  restore but pushed into `bg_uploads` after it. A doomed batch bails
  before allocate_chunks and before each put (`bg_is_doomed`), and
  `wait_commit_turn` returns false, so it restores and records failed
  chunks instead of uploading/committing above the hole. write() starts no
  new batch while one of the path is doomed (`bg_may_start`; else each new
  batch takes the restored chunks and is doomed in turn); once they ended,
  batches retry them lowest index first, one batch size each, and write(), `commit_all`, unlink and rename drop
  the ranges of a path whose `path_guard` they hold when the path has none in flight. The
  per-file dirty cap is `max(128 MB, (n + 1) x batch limit)` (`dirty_cap_for`;
  n = 1 when async_commit is off). The task
  (`bg_upload`) renews the lease (`renew_if_due`), keeps `begin_inflight`
  until restore/merge, on failure restores under the fh's current path and
  arms every writer's `write_error` (`record_bg_batch`: the batch's chunks
  go to each writer's `failed_chunks`; a batch success removes its chunks
  and clears `write_error` only when none is left, so a later disjoint
  batch never hides an earlier failure; a failed timer/`commit_all` adds
  the path's pending chunks the same way; a cap-drain commit in write()
  removes its chunks like a batch success (`record_drain_commit`) and
  truncate drops failed chunks at or above the new EOF
  (`clip_failed_chunks`, which clears `write_error` on a handle whose
  failed chunks it emptied: an O_TRUNC reopen inherited the EIO for data
  the truncate discarded); a `commit_all` success clears
  both); `BgSlot` removes its batch and wakes `wait_bg` (Notify) even on
  panic. It holds NO `path_guard` (writes would block on it again); instead
  everything that could touch those chunks waits first: write() into a
  chunk of a batch waits until no batch holds the range
  (`range_hits_upload`; else it re-seeds from the pre-commit snapshot and
  the restore clobbers it); a write whose dirty + in-flight (all batches)
  bytes would meet the dirty cap waits until under it or none in flight
  (`bg_backpressure`; the cap drain then never overlaps a batch); ALL
  batches of the path are waited for (`wait_bg_uploads`) by `commit_all`
  (last writer's flush/release, timer, stop), truncate, rename (every lock
  path) and unlink; NOT async fsync
  (knfsd fsyncs per NFS COMMIT; waiting stalled its one thread). Waiters
  may hold `path_guard` (the task never takes it: no deadlock), and holding
  it means no new batch starts. Write-side EOF (append offset, hole fill,
  tail seed) uses `write_eof` = `visible_size` (both modes) (the in-flight
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

- read() runs `read_pass` (snapshot, chunk loop, overlay) and repeats it,
  at most `READ_SNAPSHOT_TRIES` (3) passes, while the handle's snapshot
  `Arc` is no longer the one the pass used: a commit merges before it drops
  its overlay, so a pass it retired under saw neither (#133). A failed
  pass (every candidate failed) re-fetches the locations once per snapshot
  and per call (`holes_confirmed` marks a re-fetched snapshot) and retries
  (#124): a snapshot taken while a replica was down lacks it.

- Foreground miss: first `readahead.join_pending()` on the FIRST candidate
  only, bounded by `PREFETCH_JOIN_TIMEOUT` (2 s), refusing to wait on a
  prefetch that has not yet acquired its slot; else fall through to the
  candidate loop (P0-2 fallover budget stays intact).
- `read_candidates` moves a replica whose fetch failed at node level
  (timeout, connect error, Unavailable, DeadlineExceeded:
  `fetch_error_demotes_node`) in the last `FAILED_REPLICA_DEMOTE` (30 s)
  to the end (`demote_failed`, `failed_replicas`, cleared by its next
  success), so the next chunk and its prefetch skip a dead node (#149).
  A per-chunk failure (NotFound of an evicted cache copy, integrity
  fault) does not demote the node.
- Foreground misses are single-flighted per chunk_id
  (`claim_foreground_fetch` / `complete_foreground_fetch`); a Waiter whose
  Owner failed fetches itself.
- Prefetch fan-out is bounded by the `prefetch_slots` semaphore
  (`readahead.prefetch_max_inflight`, default 2), acquired in ascending
  chunk order. Do NOT re-add a `cache_bytes() >= max_cache_bytes`
  dormancy gate.
- `on_chunk_read` first aborts and drops the queued (not started)
  prefetches of the same file (a seek) and of every file outside the last
  `RECENT_PREFETCH_FILES` (8) distinct files read (`recent_files`,
  `PendingPrefetch.file`), so concurrent streams keep their windows and a
  library scan holds at most 8 windows queued plus the started ones
  (#151). Dropping every file's queue left one of 5-8 Plex streams with a
  window. A task re-checks the cache once it holds its slot, and releases
  `join_pending` waiters before its disk-cache mirror put.
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
- Write buffers (async_commit): `bg_backpressure` also waits while
  `WriteBuffer::total_bytes` (every path, buffered + in flight; summed on
  demand, no drifting counter) plus the incoming write exceeds
  `write_budget_bytes` (`hard_cap_mb - cache_mb - hard_cap_mb / 4`, none
  without a cap), but only while any background batch is in flight
  (`global_write_wait`): batches end without `path_guard`, so this never
  deadlocks and a sole writer always progresses. Sync mode starts no batch
  and never waits on it. The ladder log carries `write_buffer_bytes`.
  A budget below one file's dirty cap (0 included) only warns at startup
  (`write_budget_too_small`, async_commit on), never refuses.

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
