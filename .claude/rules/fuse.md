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
  index" = zeros; "chunk with no live address" keeps refusing at commit.
- After a commit, `mark_committed_and_refresh` merges the committed chunks
  into each handle's snapshot and the metadata cache entry
  (`merge_into_snapshot`: replace the index, grow size). It re-fetches the
  whole file only when a handle has no snapshot, the commit was lease-less,
  or the snapshot is positional/sizeless; never re-fetch per batch
  (quadratic on big files).
- A failed post-commit locations refresh sets the handles' `locations` to
  `None`; `write()`/`read()` must `refetch_locations` before using them.
- `rename` over a path with open writers and `unlink`/`rmdir` of one
  detach those handles (`detach_handles`: path becomes `<path>\0detached-N`,
  unresolvable by the manager) so their commits fail instead of landing in
  the replacement; `reacquire_write_lease` refuses detached handles.
- The lease is renewed BEFORE each commit in `write()` (`renew_if_due`: top,
  before every drain), never after. `set_size`/truncate-refresh failure set
  handle `locations = None`. `create()` seeds from the manager's snapshot
  when it kept a populated file (`committed_any = !chunks.is_empty()`).
- `release()` drops the buffer when no other writer is open; a refused
  commit is logged at error with chunk indices and bytes lost. flush and
  fsync return the commit error.
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
- A disk-cache hit is also inserted into the readahead memory cache
  (`promote_disk_hit`); the readahead cache evicts oldest-inserted first and
  drains per entry (never `clear()` + `store(0)`). The prefetch candidate
  list is limited to `chunk_idx+1..=chunk_idx+prefetch_chunks`.
- `LocalChunkCache` removals (evict and age prune) go through `evict_entry`.
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
  long NFS writes is untested end to end.
