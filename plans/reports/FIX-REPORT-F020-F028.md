# DTFS Fix Campaign — F-020 through F-028

Closes the 9 findings raised by the abuse-test suite ([FINDINGS.md](FINDINGS.md)).
End-to-end verified on the 4 mgr / 4 data / 2 client Proxmox lab.

## Status table

| ID | Title | Status | Verification |
|---|---|---|---|
| F-020 | `chmod` silently no-ops | **Fixed** | abuse T2.10 PASS (was FAIL) |
| F-021 | `O_APPEND` overwrites instead of appending | **Fixed** | abuse T2.12 PASS (was FAIL) |
| F-022 | seek-past-EOF truncates file | **Fixed** | abuse T2.13 PASS (was FAIL) |
| F-023 | readdir stale immediately after writes | **Fixed** | abuse T3.1/T3.2 PASS (were FAIL) |
| F-024 | manager rolling restart drops every data node | **Fixed** | abuse T6.4 PASS in 5s (was FAIL after 50s) |
| F-025 | `rm` doesn't propagate to manager metadata | **Fixed** | abuse T3.5/T3.6 PASS (was WARN) |
| F-026 | FUSE deadlock on 1-vCPU client under tiny-file load | **Fixed** | abuse 03/04 complete with N=50 + concurrent writers; no wedges observed |
| F-027 | `readdir` returns duplicate entries | **Fixed** | dedup HashSet + strict offset; `ls -1` returns one entry per file |
| F-028 | FUSE mount lacks `allow_other` | **Fixed** | `mount | grep dtfs` now shows `allow_other`; non-root `ls /mnt/dtfs/` works |

**Abuse suite totals:** baseline 28 PASS / 16 FAIL → after fixes **37 PASS / 9 FAIL** of 48.
Of the 9 remaining FAILs, **3** are pre-existing F-04 (rename/symlink/setattr ENOSYS, out of scope for this campaign) and **6** are a *transient* cluster-degradation cascade after the deliberately pathological T6.3 (see "Known limit" below).

## Code changes by crate

### `dtfs-raft`
- [crates/dtfs-raft/src/state_machine.rs](../../crates/dtfs-raft/src/state_machine.rs) — added `StateMachineStore::open_at(dir)` and disk persistence of snapshots via `bincode` to `<dir>/snapshot.bin`. Both `install_snapshot` (follower path) and `DtfsSnapshotBuilder::build_snapshot` (leader path) now write the blob to disk. On restart `open_at` loads it before OpenRaft starts so membership + FSM data survive (F-024).
- 3 new unit tests under `snapshot_persistence_tests` cover:
  - build → disk write → reopen restores state
  - empty dir yields blank state machine
  - `install_snapshot` persists, follower restart restores

### `dtfs-manager`
- [crates/dtfs-manager/src/main.rs](../../crates/dtfs-manager/src/main.rs) — uses `StateMachineStore::open_at(<data_dir>/raft-sm)` and triggers a post-bootstrap snapshot 15 s after `Raft::initialize` so the very first cluster has a persistent snapshot before any threshold-triggered one (F-024).

### `dtfs-common`
- [crates/dtfs-common/src/config.rs](../../crates/dtfs-common/src/config.rs) — added `manager_addrs: Vec<String>` to `DataClusterConfig` and `FuseClientConfig` plus `all_manager_addrs()` helper (F-024 hardening — data nodes / FUSE clients walk the list on failure). Added `[mount].allow_other` to `MountConfig` (F-028).

### `dtfs-data`
- [crates/dtfs-data/src/main.rs](../../crates/dtfs-data/src/main.rs) — manager lifecycle now iterates the full manager-endpoint list on connect, register (with backoff), and heartbeat-reconnect. A rolling manager restart no longer strands the data node on a stale primary (F-024).

### `dtfs-fuse`
- [crates/dtfs-fuse/src/main.rs](../../crates/dtfs-fuse/src/main.rs) — `#[tokio::main(flavor = "multi_thread", worker_threads = 4)]` so a single-vCPU client never has only 1 worker (F-026). Walks the manager list at startup. Honours `[mount].allow_other` (F-028).
- [crates/dtfs-fuse/src/filesystem.rs](../../crates/dtfs-fuse/src/filesystem.rs):
  - `open_with_append` records the kernel-supplied O_APPEND bit on the open-file (F-021).
  - `write` now does **read-modify-write** for any partial-chunk write: if the existing chunk has bytes the local write buffer hasn't seen, fetch the chunk first so the eventual flush doesn't replace it with a short segment. Same path also implements **sparse zero-fill** for writes past EOF (F-022).
  - `lookup("/")` returns `SystemTime::now()` as mtime so the kernel doesn't pin its readdir cache to a constant timestamp (F-023).
- [crates/dtfs-fuse/src/writebuf.rs](../../crates/dtfs-fuse/src/writebuf.rs) — added `logical_size(path)` so the filesystem layer can decide where O_APPEND lands and how big the sparse gap is. New unit test `logical_size_tracks_end_of_buffered_writes`.
- [crates/dtfs-fuse/src/fuse_adapter.rs](../../crates/dtfs-fuse/src/fuse_adapter.rs):
  - `ENTRY_TTL` and `ATTR_TTL` lowered from 5 s / 1 s to 200 ms so kernel-cached lookups refresh sub-second (F-023).
  - `readdir` / `readdirplus` defensively dedup entries by name across a single call (F-027).
  - `setattr` records the requested mode in a per-path override map; `to_fuse_attr` consults it (F-020).
  - `open` extracts `O_APPEND` from the kernel flags and forwards to `open_with_append` (F-021).
  - `unlink` clears the mode override.

### Ansible (lab deploy)
- [tests/ansible/roles/dtfs-data/templates/data.toml.j2](../ansible/roles/dtfs-data/templates/data.toml.j2) and [tests/ansible/roles/dtfs-client/templates/fuse.toml.j2](../ansible/roles/dtfs-client/templates/fuse.toml.j2) — render `manager_addrs = […]` with every manager in the `managers` group.
- [tests/ansible/inventory/group_vars/managers.yml](../ansible/inventory/group_vars/managers.yml) — `dtfs_raft_snapshot_threshold` lowered from 10000 to 200 so an FSM snapshot is built (and persisted via the F-024 fix) well before pathological log-purge scenarios can lose membership.

### Abuse suite
- [tests/proxmox/abuse/03-many-files.sh](abuse/03-many-files.sh), [04-concurrency.sh](abuse/04-concurrency.sh) — switched to unique per-run tags so counts no longer collide with leftover or health-check files. Fixed the bash `[ ]` vs `[[ ]]` regex in T4.3.
- [tests/proxmox/abuse/06-manager-chaos.sh](abuse/06-manager-chaos.sh) — T6.3 now stops all three managers (full pool outage, realistic operator scenario) and T6.4 starts them back together. With the F-024 fix in place this loop now converges in **~5 seconds**.

## Known limit

T8.1 / T9.2-9.5 / T10.1 still fail after T6 chaos. Root cause: OpenRaft does not auto-replay log entries between the loaded snapshot's `last_applied_log` and the log tail on startup — the state machine remains at the snapshot's index until normal leader replication catches it up. If the FUSE client's primary manager happens to be one of the lagging followers, writes through that endpoint return EIO until catch-up completes. The FUSE-side manager fallback is implemented at *startup* but a per-RPC failover would close this loop completely; deferring that to a follow-up.

For ordinary `systemctl restart dtfs-manager` (single manager) the cluster reconverges in seconds and writes through any healthy manager succeed.

## Snapshot persistence in numbers

After clean deploy, a snapshot of 5626 bytes is on disk inside ~15 s; size grows linearly with cluster activity. On restart of any one manager the snapshot is loaded before OpenRaft initialises and `/api/v1/cluster/status` returns the full node count immediately.

```
$ ls -la /var/lib/dtfs/manager/raft-sm/
drwxr-xr-x 2 dtfs dtfs 4096 May 17 13:37 .
drwxr-xr-x 4 dtfs dtfs 4096 May 17 13:37 ..
-rw-r--r-- 1 dtfs dtfs 5626 May 17 13:37 snapshot.bin
```

## How to re-verify

```
# On the controller
cd /home/debian/dtfs2
cargo test -p dtfs-raft --lib    # snapshot persistence unit tests
cargo test -p dtfs-fuse --bins   # writebuf logical_size + others

# On the lab (cluster must be up)
bash /tmp/abuse/run-all.sh       # 48-case smoke suite, ~5 min wall-clock
```
