# DTFS POSIX-compliance pass — F-029 / F-030 / F-031

User question: "Fully POSIX compliant? SQLite locking functionality etc must be verified."
**Honest verdict:** not fully — but the three biggest blockers for SQLite-class workloads have been fixed, with end-to-end verification on the lab. Remaining POSIX gaps and the work needed to close them are listed at the bottom.

## What was broken

A minimal probe of `/mnt/dtfs` against `sqlite3 3.46` from Debian 13 produced one row visible after three INSERTs:

```
=== within-session inserts + counts ===
after-1|1
after-2|1         ← second INSERT silently lost
after-3|1         ← third INSERT silently lost
rows|one
```

Drilling in produced three concrete bugs (F-029, F-030, F-031) — listed below in the order they hit.

## F-029 — write-then-read on the same fd returned 0 bytes

`read()` looked only at the cluster-side `total_size` from `FileLocations`; the local `write_buffer` (where every just-pwrite()-ed byte lives until the next chunk-boundary flush) was never consulted. A brand-new file therefore reported `total_size = 0` and the read short-circuited to `Bytes::new()`. SQLite hits this on its very first `CREATE TABLE` page write → page read.

**Fix:** [crates/dtfs-fuse/src/writebuf.rs](../../crates/dtfs-fuse/src/writebuf.rs) — added `WriteBuffer::overlay()`. [crates/dtfs-fuse/src/filesystem.rs](../../crates/dtfs-fuse/src/filesystem.rs) — `read` computes `effective_size = max(locations.total_size, write_buffer.logical_size)`, fetches cluster chunks for that range, then overlays buffered writes on top. `flush` re-fetches locations from the manager and updates every `OpenFile.locations` pointing at the path so the next read sees the new chunks.

**Coherency probe before/after**:

```
                                      before    after
write 16K → read 16K (same fd)        FAIL      PASS
pwrite at offset 0 → pread at 0       FAIL      PASS
pwrite at offset 4096 → pread at 4096 FAIL      PASS
write 16K → close → reopen → read     PASS      PASS
```

## F-030 — chunk re-writes silently kept the previous content

`merge_chunk_location_entries` was keyed by `(chunk_index, chunk_hash, node_id)`. When SQLite rewrote chunk 0 with new content (new hash), the merge **kept** the old `(0, old_hash, node)` rows alongside the new `(0, new_hash, node)` rows. `get_file_locations` then returned whichever hash sorted first in the BTreeMap — for many files, that's the old content. Half the reads were stuck at an earlier version of the database.

**Fix:** [crates/dtfs-raft/src/lib.rs](../../crates/dtfs-raft/src/lib.rs) — for every `chunk_index` present in `incoming`, drop all existing rows at that index, then add the incoming set. The FUSE client always sends the complete replica set per chunk at flush time, so this is safe and the multi-replica add semantics still work for chunk indices that aren't being rewritten.

**SQLite probe before/after**:

```
                              before                          after
single-process INSERT a,b,c    only "a" persists across reopen all three rows
WAL-mode INSERT after open     visible in session, lost on reopen  visible on reopen
200-row INSERT…SELECT batch    1 row reopen                    203 rows reopen
```

## F-031 — fcntl byte-range locks not coordinated across processes

FUSE's default behaviour for `fcntl(F_SETLK)` is to let the kernel grant the lock to each process locally. Two `sqlite3` invocations on the same client both think they hold the writer lock → SQLite's optimistic-commit retry kicks in and many INSERTs roll back.

**Fix (partial):** Enabled the fuse3 `file-lock` feature; implemented `setlk` / `getlk` in [crates/dtfs-fuse/src/fuse_adapter.rs](../../crates/dtfs-fuse/src/fuse_adapter.rs) with an in-memory per-inode lock table. Non-blocking `F_SETLK` works correctly: same-owner upgrades/downgrades succeed, cross-owner overlapping write requests return `EAGAIN`. `F_UNLCK` also trims partial-overhang locks held by the same owner.

**What this fix does NOT do:**

- **Blocking `F_SETLKW`** still returns `EAGAIN` — applications expecting the kernel to wait must add a busy-timeout / retry loop. SQLite has this built in (`PRAGMA busy_timeout` or sqlite3 CLI `.timeout`), but a naïve `sqlite3 -e 'INSERT …'` does not enable it and will fail loudly on contention.
- **Cross-host locks**: two FUSE clients on different machines do not see each other's locks. A future fix would push lock state into the manager.

## Tests added in the repo

| File | Covers |
|---|---|
| [tests/proxmox/sqlite/01-coherency.sh](sqlite/01-coherency.sh) | Bare write-then-read on the same fd (F-029) |
| [tests/proxmox/sqlite/02-single-process.sh](sqlite/02-single-process.sh) | SQLite create/insert/select within one process + reopen (F-029, F-030) |
| [tests/proxmox/sqlite/03-journal-modes.sh](sqlite/03-journal-modes.sh) | Rollback journal vs MEMORY/WAL modes (F-030) |
| [tests/proxmox/sqlite/04-concurrent-noretry.sh](sqlite/04-concurrent-noretry.sh) | Two sqlite3 processes without busy_timeout — characterises F-031 multi-process gap |
| [tests/proxmox/sqlite/05-concurrent-with-timeout.sh](sqlite/05-concurrent-with-timeout.sh) | Two sqlite3 processes with busy_timeout — should converge once `F_SETLKW` blocking is implemented |
| `crates/dtfs-fuse/src/writebuf.rs` — `overlay_returns_buffered_bytes_to_reader` | unit test for the overlay primitive |
| `crates/dtfs-raft/src/lib.rs` — `merge_replaces_all_entries_at_touched_chunk_index` + `merge_leaves_other_chunks_alone` | unit tests for the merge fix |

All unit tests pass on the controller (`cargo test -p dtfs-raft --lib`, `cargo test -p dtfs-fuse --bins`).

## Remaining POSIX-1-2024 gaps

After this pass DTFS is **POSIX-compatible enough for single-process SQLite (and most read-then-edit-then-read patterns) on a single client.** It is not yet a drop-in POSIX filesystem. To get there:

| Gap | Cost | Notes |
|---|---|---|
| `rename` (atomic, same parent) | medium | Manager-side mv + cluster metadata move; required by WAL switchover, log rotation, `mv` |
| `rename` (cross-directory) | high | Same as above plus parent-dir mtime updates |
| Hard links (`link` / `linkat`) | medium | Manager needs a single inode/blob with multiple paths |
| Symlinks (`symlink` / `readlink`) | low | Plain new entry kind + path resolution in lookup |
| `setattr` truncate-shrink | medium | Manager needs to drop chunks past the new size and update size; FUSE write path needs to invalidate buffer |
| `fallocate` / `posix_fallocate` | low | Manager pre-records size + chunk count without writing; FUSE returns size from metadata |
| `xattr` family | medium | Manager needs an xattr map per file; ENOSYS today |
| `F_SETLKW` (blocking lock) | medium | Tokio waiter inside setlk handler; pair with notify-on-unlock |
| Cross-host `fcntl` coordination | high | Push lock state to manager; lease + revoke protocol |
| `mmap` MAP_SHARED writes | high | Page-faulted writes have no FUSE notification — would require kernel changes or accept "best-effort" |
| `O_DIRECT` | low | Currently quietly degrades to buffered I/O |
| `O_DSYNC` / `O_SYNC` | medium | Should imply fsync after each write; today they don't |
| `O_TMPFILE` (anonymous tmpfile) | medium | Used by `mkstemp`; ENOSYS today |
| `flock(2)` (BSD locks) | low | fuse3 has a separate `flock` handler; same in-memory pattern as setlk |
| `inotify` / `fanotify` watchers | high | FUSE notifies kernel via `notify_inval`; requires plumbing in our adapter |
| `statfs` cluster-aware | low | Currently returns synthetic values; could plumb cluster `total/used` capacity |

## How to verify

Whole pass-through verification on the lab:

```bash
# Single-process SQLite
ssh client-01 sudo bash /tmp/sqlite/02-single-process.sh

# Concurrency baseline (multi-process; will show some BUSY without timeout)
ssh client-01 sudo bash /tmp/sqlite/04-concurrent-noretry.sh

# Re-run the abuse suite — must still report 36+ PASS / ≤10 FAIL
bash /tmp/abuse/run-all.sh
```

The expected SQLite probe-3 output after the fix is:

```
=== within-session inserts + counts ===
after-1|1
after-2|2
after-3|3
rows|one,two,three

=== persistence (separate sqlite3 process) ===
3
one
two
three

=== bigger workload: 200 rows then COUNT ===
203

=== reopen count ===
203
```
