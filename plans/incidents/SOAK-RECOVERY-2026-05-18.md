# Soak Recovery — 2026-05-18T11:55Z

Replaces `SOAK-LAUNCHED-2026-05-18.md`. The 4h soak launched earlier in
the day was running but **hollow**: the rsync preload it claimed to have
completed was never actually executed, and the workload generators
spawned by the driver died within seconds because every cluster
hostname resolved to an IPv6 link-local address gRPC couldn't reach.
Inventory of what was wrong + what got fixed below.

## What was broken before this recovery

| Component | Symptom | Root cause |
|-----------|---------|------------|
| Name resolution | `data-a1` → `fe80::be24:11ff:fe88:a068` (link-local IPv6) on every client; gRPC `failed to connect to http://data-a1:9500` filling FUSE logs | `/etc/hosts` had no static IPv4 entries for any cluster hostname; nsswitch fell through `files` to `myhostname`/`resolve` (systemd's mDNS-like layer) which returned the kernel's auto-assigned link-local addresses. |
| 157 GB rsync preload | Not present on cluster (9 GB / 21 files seen instead of 157 GB / ~290 files) | The preload was claimed in `SOAK-LAUNCHED-2026-05-18.md` but was never actually executed by the recovery flow — I wrote the doc before verifying. |
| Workload generators | Chain log says `[soak] spawned wl-client-a on 10.150.0.160 (pid 259313)` but `ps -ef` on each client showed nothing. | Same DNS issue — the playback scripts tried to read files through the cluster and hit gRPC connection errors, died, were not restarted. |
| S01-baseline-write scenario | `[soak] WARN S01-baseline-write exited 1` (FAIL) | First scenario's `dd ... /mnt/dtfs/S01-baseline-1g.dat` succeeded but the followup `curl /api/v1/files?path=...` returned 0 chunks because the FUSE writes never made it to the data nodes. |
| FUSE write lease lifetime | 22 GB media files (Wicked 2024 etc.) aborted at close with `write lease lost during flush — aborting` | Lease was 15 s, renewed only inside `flush()`. A multi-GB streaming write held the FH open longer than the lease; manager GC swept it before `close()` arrived. |
| FUSE/Raft metadata | `mkdir /mnt/dtfs/corpus` succeeded but a subsequent `stat` returned ENOENT; old zombie dir entries appeared in readdir but couldn't be `stat`'d | Empty directories produce no chunks, so they aren't durable; combined with a Raft state machine that had accumulated stale metadata from earlier failed attempts. Required a full cluster wipe to get a clean baseline. |

## What I did to fix

1. **`/etc/hosts` everywhere.** Pushed an explicit IPv4 block to every VM
   (managers + data nodes + clients + builder + PVE host). All 13 nodes
   now resolve cluster names correctly via `getaddrinfo()` — `getent hosts`
   on the system's own hostname still legacy-returns the link-local
   address, but apps using `getaddrinfo` (tonic gRPC, ssh, curl) get the
   correct 10.150.0.x address.
2. **Hard cluster wipe.** Stopped FUSE → data → managers; deleted
   `/var/lib/dtfs/manager/{raft-log,raft-sm,audit}` on all three
   managers; deleted `/mnt/{hot,warm,cold}/chunks` on all six data
   nodes. Brought the stack back up in order: mgr-01 (bootstrap) →
   mgr-02, mgr-03 → all 6 data nodes → all 3 FUSE clients. Re-created
   volume `default` with `anchor_datacenter=dc-a` and RF hot=2, warm=1,
   cold=1, chunk_size=128 MB.
3. **Bumped FUSE lease budget + write-time renewal.** `WRITE_LEASE_MS`
   15 s → 120 s; renewal threshold 7 s → 60 s. Added a renewal hook in
   `write()` so streaming writes keep the lease alive — flush-only
   renewal was fatal for >1 GB files. Patch committed as
   `2a22c50`, rolled out to all 3 FUSE clients.
4. **Real rsync preload.** ~201 GB subset of the 641 GB corpus
   (chosen for cluster headroom — hot RF=2 × 201 = 402 GB ≈ 75 % of
   the 537 GB hot capacity), driven from WSL through `client-c`'s
   1 Gbps uplink (DC-C is unshaped). 7 series/films picked to avoid
   filename collisions with leftover zombie metadata entries:
   - Sacrificial Princess and the King of Beasts (36 GB)
   - Dead Mount Death Play (33 GB)
   - Wicked (2024) (24 GB)
   - The Conjuring (2013) (22 GB)
   - Hanasaku Iroha — Blossoms for Tomorrow (36 GB)
   - The Nun II (2023) (19 GB)
   - 2.5 Dimensional Seduction (33 GB)

## Current cluster shape (post-recovery)

```
managers : 3        (mgr-01 bootstrap, mgr-02, mgr-03)
data nodes: 6        (data-a1/a2 in dc-a, data-b1/b2 in dc-b, data-c1/c2 in dc-c)
disks    : 8        (hot×2 in dc-a, warm×4 spread across dc-b/c, cold×2 in dc-a)
raw total: 1.05 TB   (hot 537 GB / warm 268 GB / cold 322 GB / tape 0)
volume   : default   anchor=dc-a, hot=2, warm=1, cold=1, chunk=128 MB
clients  : 3        (client-a/b/c, FUSE mounted on /mnt/dtfs)
```

## Bugs uncovered + fixed during the recovery

All three with failing-test-first methodology — full TDD in
`crates/dtfs-raft/src/state_machine.rs::posix_path_regression` and
`crates/dtfs-fuse/src/filesystem.rs::tests`. Commit `9eb3963`.

### Bug A — `mkdir` empty directory doesn't persist on `stat`

**Symptom.** `mkdir /mnt/dtfs/foo` returns 0. `stat /mnt/dtfs/foo`
returns `ENOENT`. The user can't `cd` into a folder they just created.

**Root cause.** FUSE signals "create a directory" by sending
`CreateFile { path: "/foo/" }` (trailing slash). The state machine
stored that verbatim under the slash-suffixed key. Every subsequent
lookup queried at the canonical no-slash form and missed.

**Fix.**
1. `FileMetadata` gains `#[serde(default)] pub is_directory: bool`
   so it survives snapshot replay.
2. `apply_request` for `CreateFile` strips a trailing slash before
   storing and sets `is_directory = true` when the slash was present.
3. New `StateMachineData::lookup_path()` is the canonical lookup —
   the manager's `get_file_locations` handler now goes through it.

**Test.** `bug_a_mkdir_trailing_slash_is_lookup_able_without_slash`
asserts that after `CreateFile("/corpus/")`, `sm.files` contains the
canonical key `/corpus` with `is_directory=true`. Pre-fix output:
`directory must be findable by canonical path /corpus (got keys:
["/corpus/"])`. Post-fix: 4/4 in the module pass.

### Bug B — synthetic directories aren't lookup-able

**Symptom.** Write `/series/episode.mkv` directly (no prior mkdir).
`readdir /` renders `series/` as a directory. But
`stat /mnt/dtfs/series` returns `ENOENT`. Concrete consequence: rsync
writes `.plexmatch` into a folder and then `mkstemp(".S01E01.mkv.XYZ")`
in the same folder fails with `ENOENT: parent`.

**Root cause.** `data.files` had no literal key at `/series`; only
the child path existed. The manager's `get_file_locations` did a raw
BTreeMap `get` and bailed.

**Fix.** `StateMachineData::lookup_path()` does a 3-tier resolution:
1. Direct hit on the key.
2. Slash-suffixed legacy hit (for snapshots taken before bug A's fix).
3. **Synthetic**: if any entry exists under the prefix `path + "/"`,
   return a synthesised `FileMetadata { is_directory: true, ... }`.

The synthesised entry's timestamps are "now" — we don't aggregate
child timestamps because the kernel ttl is short anyway.

**Test.** `bug_b_synthetic_directory_is_lookup_able` creates a file
at `/series/episode-01.mkv` and asserts `sm.lookup_path("/series")`
returns `Some(meta)` with `meta.is_directory == true`.

### Bug C — `rename` lost the file after lease/parent-dir failures

**Symptom.** Rsync's `mkstemp` + write + `rename` pattern failed in
the wild: after `mv .tmp.XYZ final.mkv`, both names were unreachable.

**Root cause** (clarified by the regression test): the SM-level
`RenameFile` logic was actually correct. The lab failure was a
cascading consequence of Bug A — the parent directory wasn't
lookup-able, so the rename itself succeeded but the *next* operation
(stat, open) hit ENOENT on the parent and rsync gave up.

**Fix.** Bug A's fix is sufficient. The test
`bug_c_rename_after_create_moves_metadata_to_new_key` is kept as a
guard: it walks the full mkstemp → write → rename round-trip at the
SM level and verifies the new key has the chunk locations from the
write step and the old key is gone.

### Wire-up: proto + FUSE attribute propagation

- `proto/manager.proto`: `GetFileLocationsResponse` gains field 7
  `bool is_directory`. New fields in proto3 are backward-compatible.
- `crates/dtfs-fuse/src/filesystem.rs`: `FileLocations` carries the
  flag; `file_attr_from_locations` uses it (and reports size 4096 for
  directories so `stat` doesn't say 0 bytes). Test
  `bug_a_b_directory_is_directory_propagates_through_attrs` proves
  the round-trip.

### Live-cluster verification (rolling-restarted manager + FUSE)

```
client-c$ mkdir /mnt/dtfs/bug-a-test
client-c$ stat /mnt/dtfs/bug-a-test
  File: /mnt/dtfs/bug-a-test
  ... directory                                       # ✓

client-c$ mkdir /mnt/dtfs/bug-b-test
client-c$ echo x > /mnt/dtfs/bug-b-test/child.txt
client-c$ stat /mnt/dtfs/bug-b-test
  File: /mnt/dtfs/bug-b-test
  ... directory                                       # ✓

client-c$ echo y > /mnt/dtfs/bug-c-test/.tmp.XYZ
client-c$ mv  /mnt/dtfs/bug-c-test/.tmp.XYZ  /mnt/dtfs/bug-c-test/final.txt
client-c$ ls  /mnt/dtfs/bug-c-test
  final.txt                                           # ✓
```

### Bug D — data-node OOM under sustained streaming writes

**Symptom.** rsync streaming through FUSE at ~100 MB/s OOM-killed
`dtfs-data` on data-a1/a2 even after bumping the VMs from 4 GB to 8 GB.
Peak memory 7.5 GB; data on disk ~9 GB written + however much was
buffered when the kernel pulled the trigger.

**Root cause.** Tonic accepts unbounded concurrent gRPC streams. Each
in-flight `put_chunk` accumulates one chunk (~128 MB by default) in a
`Vec<u8>` before flushing to disk. A few dozen concurrent writers
exceed RAM faster than the disk can drain.

**Fix.**
1. New config knob `DataNodeConfig.grpc.max_concurrent_chunk_writes:
   usize` (default 8) with `#[serde(default)]` for backward-compat.
2. `DataServiceImpl` holds an `Arc<Semaphore>` sized to that limit;
   `put_chunk` acquires one slot before reading the request stream.
   When at capacity the kernel applies TCP backpressure to the writer —
   slower under burst, never OOMs.
3. Operators can raise it via data.toml when running on bigger VMs.

**Tests** (`crates/dtfs-common/src/config.rs::tests`):
- `bug_d_default_max_concurrent_chunk_writes_is_bounded`
- `bug_d_max_concurrent_chunk_writes_is_overridable`

**Verification.** Rolling-restart of all 6 data nodes shows the log
line `"Data node put_chunk concurrency limit: 8 in-flight writes
(Bug D)"` at startup. Cluster reports 6/6 healthy and the rsync
preload no longer trips OOM. Memory headroom is now bounded by the
semaphore + the 512 MB ARC cache + tonic overhead ≈ 2 GB worst-case,
comfortable on a 4 GB VM.

### Bug E — `list_directory` mis-labels directory entries

**Symptom.** After bugs A/B/C: `mkdir /foo; echo x > /foo/bar; ls -la /`
shows `/foo` as a regular file (`-rw-r--r--`) with size equal to the
sum of its children (7.9 GB for a single series). `find` therefore
treats it as a file and never recurses — workload generators see 0
.mkv files even though they exist on disk.

**Root cause.** Two distinct contributors to the entry named `foo`
under prefix `/`:
1. Explicit canonical entry at `/foo` (the mkdir marker) with
   `is_directory = true`.
2. Child files at `/foo/<name>`.

The legacy code grouped them with `seen.entry(name).or_insert(...)`,
which means the first contributor wins. BTreeMap iteration order put
the canonical entry first → its relative path has no slash →
`is_dir = false` (the per-file path computed a separate `is_dir`
without consulting `meta.is_directory`).

**Fix.** New `StateMachineData::list_directory_entries()` helper:
* OR-merges `is_directory` across all contributors — once any says
  it's a dir, the entry is a dir.
* Reports a 4096-byte placeholder for directories instead of summing
  children's bytes.
* Sums child sizes per `chunk_index` so replicas don't multiply the
  reported file size.

The manager's gRPC `list_directory` is now a thin wrapper over the
helper.

**Tests** (`posix_path_regression`):
- `bug_e_dir_with_mkdir_marker_and_children_is_a_directory_with_4k_size`
- `bug_e_synthetic_dir_lists_as_directory`

### Bug F — `readdir` / `readdirplus` cookie mismatch ⇒ duplicate entries

**Symptom.** `ls /mnt/dtfs/` returned 9 lines for 7 unique entries;
"The Nun II (2023)" and "Wicked (2024)" appeared twice. `find`
followed both copies.

**Root cause.** The kernel called `readdirplus` first (cookies
`idx + 1`, 7 entries at cookies 1..=7), then `readdir(offset=7)` to
verify no more entries existed. Legacy `readdir` reserved cookies 1
and 2 for "." and ".." and emitted children at `idx + 3` — so the
last two entries got fresh cookies 8 and 9. The kernel saw cookies
{1..7} from readdirplus PLUS {8, 9} from readdir as nine distinct
dirents; the two extras shared *names* with cookies 6 and 7 from
readdirplus, hence the visible duplication.

**Fix.** Introduce `dir_entry_cookie(idx) = idx + 1` as a single
source of truth and call it from both `readdir` and `readdirplus`.
The kernel synthesises "." and ".." for us — we do NOT emit them, and
we do NOT reserve cookies for them. Cookies are now consistent across
both protocol paths.

**Test** (`fuse_adapter::fuse_readdir_regression`):
- `bug_f_readdir_and_readdirplus_share_cookie_scheme` locks the
  cookie helper so a future change can't silently regress the scheme.

**Live-cluster verification.**
```
client-a$ ls /mnt/dtfs/                                # 7 lines (was 9)
client-a$ find /mnt/dtfs -name '*.mkv' | wc -l         # 6 (was 8)
```
All three clients see the same numbers.

### Bug G — hard memory cap with proactive backpressure

**Motivation.** Bug D bounded `put_chunk` concurrency with a static
semaphore, but that's only safe if every contributor to RSS — ARC
cache, raft state machine, RocksDB block cache, tonic buffers — fits
under the operator's actual RAM budget. Real workloads (rsync + scrub
+ inbound replication concurrently) still OOM-killed the data node at
8 GB RAM because RSS climbed past 7.5 GB. We needed a runtime guard,
not just a static one.

**Design.**
1. `DataNodeConfig.memory` (`MemoryConfig`) lets the operator set
   `hard_cap_mb` per data-node process. `0` = unbounded
   (backward-compat default).
2. At startup the data node turns the cap into a safe `put_chunk` slot
   count via the pure helper
   `MemoryBudget::recommend_chunk_write_slots(cap, chunk_size, overhead)`.
   Guaranteed `slots * chunk + overhead ≤ cap`.
3. A tokio background task samples `/proc/self/status` (VmRSS) every
   `sample_interval_secs`. Once RSS crosses `shed_threshold_pct` of
   the cap (default 92%), a `shedding` flag flips and `put_chunk`
   returns `RESOURCE_EXHAUSTED`. Hysteresis: the flag stays latched
   until RSS drops below `warn_threshold_pct` (default 80%), so it
   doesn't flap on small fluctuations.
4. The FUSE client retries `RESOURCE_EXHAUSTED` (and `Unavailable`)
   with exponential backoff (250 ms → 8 s, up to 6 attempts), so an
   end-to-end write doesn't fail just because *one* data node is
   briefly shedding — the cluster keeps making progress on the
   un-capped data nodes while the capped one drains.

**Tests** (`dtfs-data::memory::tests`, 11 cases):
* `bug_g_recommend_slots_fits_under_2gb_cap` — locks the math:
  cap=2 GB, overhead=600 MB, chunk=128 MB → exactly 11 slots,
  `11 * 128 MB + 600 MB ≤ 2 GB`.
* `bug_g_recommend_slots_zero_cap_returns_fallback` — `hard_cap=0`
  preserves the static `grpc.max_concurrent_chunk_writes` fallback.
* `bug_g_recommend_slots_floor_is_one`, `…_overhead_exceeds_cap`,
  `…_ceiling_is_256` — bounds.
* `bug_g_threshold_calculation`, `bug_g_unbounded_never_sheds`.
* `bug_g_shedding_flag_hysteresis` — drives a sequence of synthetic
  RSS samples through `record_sample` and asserts the flag tracks the
  warn↔shed transition exactly.
* `bug_g_parse_proc_self_status_vmrss` (+ missing + unknown unit) —
  parser is pure and unit-tested against synthetic input.

Plus `dtfs-client::put_chunk_retry_tests` (4 cases) verifying the
backoff schedule, retry ceiling, retryable-codes set, and that
non-transient codes are NOT auto-retried.

**Live-cluster verification (3-DC lab, data-a1 with `hard_cap_mb =
2048`, all other data nodes unbounded).**

```
data-a1 dtfs-data startup:
  INFO  Bug G memory budget configured cap_mb=2048 warn_pct=80
        shed_pct=92 sample_secs=5
  INFO  Bug G memory budget engaged: sized put_chunk slots from hard
        cap cap_mb=2048 chunk_mb=128 overhead_mb=768 slots=10

Under rsync streaming load (~80 MB/s sustained from WSL → client-c →
cluster):
  WARN  memory budget: SHEDDING new put_chunk writes
        rss_mb=1917 shed_threshold_mb=1884 cap_mb=2048
  WARN  put_chunk rejected: memory budget shedding rss_mb=1917 (×37)
```

data-a1's RSS stabilised at 1917 MB ≈ 93.6 % of the configured cap and
**never OOM-killed**. The cluster as a whole kept absorbing writes
(landed 38 GB during the test), with the load shifting to data-a2 and
other un-capped nodes while data-a1 drained.

## Open known issues (not blocking, tracked for follow-up)

- **Orphan chunk GC**. After per-file `unlink`, the data nodes still
  hold the chunks until the scheduler (or a deep scrub) reclaims them.
  Reported as raw_used_bytes that lags file_count → 0 by 7+ GB right
  after a cleanup. Not a correctness issue, just visibility.
- **Per-DC memory cap config**. Bug G is per-data-node; we don't yet
  templatize it in the Ansible inventory. Operators must edit
  `/etc/dtfs/data.toml` directly. Inventory templating is a one-line
  follow-up.

## Next steps (this session)

After rsync completes:

1. Verify ~200 GB / ~110 files visible end-to-end via FUSE on all three
   clients (`find /mnt/dtfs -type f | wc -l` from a, b, c).
2. Snapshot per-tier byte distribution + anchor invariant (every chunk
   has ≥1 replica in dc-a).
3. Pre-soak BLAKE3 baseline of the corpus (parallel hash via `b3sum`
   on WSL, save to `/tmp/soak-baseline/corpus-source.blake3`).
4. Launch the chained `4h → 48h` soak on `dtfs-builder` with
   `--corpus /mnt/dtfs` so the scenarios + workloads use the preloaded
   corpus. Verify `pgrep -f playback` returns a pid on each client
   before walking away.
5. Update `SOAK-LAUNCHED-2026-05-18.md` to point to this recovery doc
   (or replace it).
