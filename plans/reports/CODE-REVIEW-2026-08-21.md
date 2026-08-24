# CODE-REVIEW-2026-08-21 — full-workspace review ahead of the next lab test run

Method: 16 independent reviewers (8 per-component, 8 cross-component seam
traces) over the whole workspace, each finding then re-read by an independent
adversarial verifier instructed to refute it. 80 candidate findings, 30 refuted
or downgraded to cosmetic, 50 survived. The top items were additionally re-read
by hand against the source.

**Read this first.** The review itself was run against a stale checkout —
`6eb2b83` (2026-06-11), 40 commits behind `origin/master` (`f841bcd`,
2026-06-15). Every finding below has since been re-checked against
`origin/master`, and each theme is tagged accordingly:

- **OPEN** — still present on current master. These are the ones that matter.
- **FIXED UPSTREAM** — an independent audit series (`CO-*`, `FUSE-HOL`, …)
  already fixed it, sometimes by a better mechanism than the one proposed here.
  Recorded because independent re-discovery is evidence the finding was real,
  and because the upstream fix is worth knowing about.

Scope note: "missing auth/TLS" was excluded by design (known, accepted, CF-6).

Severity: HIGH = data loss / silent corruption / invariant violation / outage /
OOM / non-terminating repair. MEDIUM = slow-healing wrong state, wasted
bandwidth on the 40 Mbps link, operator-misleading observability, resource leak.

---

## T1 — Background loops are not leader-gated, and they delete data — **OPEN** (HIGH)

`TaskScheduler::tick` is leader-gated. Nothing else is. On current master,
`grep -n is_leader crates/dtfs-manager/src/tiering.rs` returns **zero** hits.

Spawned unconditionally on **every** manager, including cross-DC learners:

| Loop | What a follower does |
|---|---|
| `TieringEngine::run` | a follower's read window is always empty (read samples arrive on the heartbeat the leader serves), so it sees every cache replica as idle, plans an `idle_window` demote for all of them, `DeleteChunk`-RPCs each one, and only then fails to record the removal as a non-leader: bytes gone, location entries kept |
| `run_orphan_scrub` | reaps by chunk id against **its own** possibly-stale `sm.files` |
| `run_auto_unquarantine_sweep` | ungated |
| `run_lease_gc` | ungated |
| `POST /api/v1/cache/flush` | an operator flush served by a non-leader deletes the bytes and keeps the entries |

Same failure class as the 2026-06-10 CM incident (monitor not leader-gated),
except these paths issue **direct data-node delete RPCs**, which no Raft
rejection can undo.

Aggravator: `orphan_scrub_tick` has no state-freshness check either. A manager
that was down and is replaying classifies every file created during its outage
as an orphan.

---

## T2 — Replica removal keyed by node, not disk — **FIXED UPSTREAM** (was HIGH)

`RemoveChunkLocation` now carries `disk_id`, and the data-node delete is
disk-scoped. Release cases `62-demote-colocated-anchor-survives` and
`72-idle-demote-anchor-survives` pin it. The demote path's
`enforce_anchor: None` is likewise gone.

---

## T3 — chunk_index map flattened into a positional array — **OPEN** (HIGH)

`get_file_locations` groups by `chunk_index` into a `BTreeMap` and emits
`by_chunk.into_values().collect()` — a **dense** Vec with the index key thrown
away. The FUSE client indexes it **positionally**, `chunks[offset / chunk_size]`.

Any gap in the index space shifts every later chunk. The reader gets a
different chunk's bytes at a valid offset, checksum-verified and all: **silent
wrong data, no error anywhere.** Producers of a gap: `truncate` then a tail
write; a partial multi-chunk commit; a location row removed for a whole index.

This is the most dangerous open finding — it is the only one whose failure mode
is undetectable by the system itself.

---

## T4 — FUSE data path — **PARTLY OPEN** (HIGH)

**T4.1 read length truncated to u32 — FIXED UPSTREAM.** Now clamped in u64 via
`effective_read_len`.

**T4.2 `WriteBuffer::extract_complete_chunks` drops bytes it never emitted —
FIXED UPSTREAM.** Corrected after first publication: the mid-chunk branch now
returns early, emitting and draining nothing and leaving `dirty_offset`
untouched, so the whole contiguous buffer defers to `extract_all_chunks` /
`flush_all`, which already emits the leading partial region. The
`co_1_7_midchunk_flush_does_not_drop_leading_partial_bytes` test **passes** on
current master; only its comment is stale (it still says "today they are
drained without ever being emitted, so this fails"), which is what made this
review call it open. Release case 78 keeps it pinned anyway.

**T4.2b — a SECOND positional-indexing site, found while porting T3 — OPEN.**
The read-modify-write seed in `filesystem.rs::write` resolved its chunk with
`loc.chunks.get(chunk_idx as usize)`, i.e. positionally. On a sparse or gapped
file that seeds the buffer with a *neighbouring* chunk's bytes and then writes
them back **under this chunk's index** — a write-side corruption path, worse
than T3's read-side one because it persists. Fixed with the same index lookup.

**T4.3 sparse-hole fill materialises the whole hole — OPEN, re-check needed.**
The `FILL_CHUNK` cap bounds only the temporary zero buffer; every iteration
appends into one contiguous write-buffer Vec with no flush until the loop ends.
An out-of-order torrent write at offset ~30 GB (the documented NRB landing
workload) grows it to ~30 GB.

---

## T5 — Unbounded growth and O(N^2) sweeps at target scale — **PARTLY OPEN**

| Site | Status |
|---|---|
| data-node `per_chunk_last_access` never pruned + O(M) victim scan | **FIXED UPSTREAM** |
| tiering `read_windows` / `access_stats` never pruned | **FIXED UPSTREAM** |
| `ListChunks` re-walks and re-sorts the whole chunk tree **per 1000-item page** | **OPEN** — at ~12.5M chunks that is ~12.5k full walks per orphan sweep; the scrub never finishes and pins the anchor HDD |
| `check_under_replicated` has no per-tick task cap; one node loss queues one task per chunk into a single Raft entry | **OPEN** |
| snapshot build/send/receive amplifies the state machine several times in heap | **OPEN, untracked** — was CNF-4, but PLAN-CN-FOLLOWUPS was retired upstream (#89) after its other tasks closed; the heap amplification itself was never fixed |
| FUSE `evict_to_size` full stat sweep per evicted chunk, inline on the read path | **OPEN** |

---

## T6 — Repair can be starved, or can fight itself — **OPEN** (HIGH)

**T6.1** The manager pauses scheduler ticks at `Warn`, but the memory that keeps
RSS high belongs to loops the pause does not stop. Once latched the pause is
permanent: a node loss then never heals while the memory-hungry loops keep
running.

**T6.2** `check_under_replicated` counts coverage health-aware via
`live_replica_tier`; `check_misplaced_rebalance`, `check_anchor_lag` and the
`RemoveChunkLocation` anchor-survivor scan count health-blind. A Down anchor
node therefore makes the backfill schedule a copy that the rebalance
immediately removes — forever, over the 40 Mbps link.

**T6.3** A replica whose bytes are gone is never reconciled: the stale row still
counts as coverage, so no backfill happens, and the only response is to
quarantine the (innocent) data node permanently.

---

## T7 — Read locality — **PARTLY OPEN**

**`known_locations` never populated — FIXED UPSTREAM** as CO-3-10
(`ChunkLocationInfo.node_locations`). Before that fix the whole U4 selection
order was inert, which means release cases 42/59 were measuring nothing at the
time they were written.

Still open:

- readahead prefetch always uses `node_addresses[0]`, bypassing every locality
  rule the foreground read applies — and caching under a key the foreground
  read will not look up;
- the P0-11 FUSE local disk cache serves chunks with **no BLAKE3 verification**,
  bypassing `verify_on_read` entirely. A chunk that rots in the client's own
  cache directory is served as good data, and because the cache is consulted
  before the cluster, the healthy replicas are never asked.

---

## T8 — Heartbeat handler storms and lies — **OPEN**

- The integrity-fault loop issues **3 unbatched Raft writes per fault**, one a
  full-namespace sweep. A node with a damaged `.meta` directory ships up to 1024
  faults per heartbeat → ~3072 serial Raft round-trips holding the write lock,
  while the collector refills. Self-sustaining.
- The handler writes `healthy` unconditionally, so operator maintenance mode is
  silently reverted within 5 s.

---

## T9 — Security — **OPEN** (MEDIUM, but the XSS is the worst of the three)

- **Stored XSS.** File paths, node hostnames and audit targets are interpolated
  raw into `innerHTML`, no escaping, no CSP. A filename created over the FUSE
  mount executes script in the operator's browser session — a session that holds
  full unauthenticated control of the REST API.
- `AllocateChunks.num_chunks` (peer-controlled u32) drives an unbounded
  allocation and loop **while holding the state-machine read lock**.
- `POST /api/v1/config` with `write_to_disk` overwrites `/etc/dtfs/manager.toml`
  with non-TOML content; the advertised validation does not exist, so the next
  restart fails to parse and the manager does not come back.

---

## T10 — Other confirmed correctness issues

**OPEN**: completed replicas registered against the *first* file referencing a
(shared, content-addressed) chunk hash, so other referencing files re-copy
forever; the legacy temperature path schedules a migration for essentially every
chunk read with its anchor guard reading a field modern volumes never set;
`scrub_disk` aborts a whole disk pass on the first bad `.meta`; the ARC cache has
no `put` caller so its ladder rung is a no-op while its budget still throttles
`put_chunk`; `first_write_tier = "nearest-warm"` forces allocation RF=1 against
an ack floor of 2, so the write is streamed in full and then rejected.

**FIXED UPSTREAM**: the snapshot RPC inheriting the 5 s AppendEntries deadline
(CO-3-6, now a size-scaled per-call timeout); `dtfs-cli`'s non-ASCII truncate
panic; excess-spill bytes never reclaimed (CO-3-2, via a per-node orphan rule —
a better fix than the one this review proposed).

---

## T11 — Test gaps that matter for next week — **OPEN**

- `13-integrity-corruption` (and its sibling) **prime the FUSE cache before
  flipping the byte**, then drop only the kernel page cache. The read is served
  from the client's own caches, no data node is contacted, and the integrity
  gate passes regardless of what `verify_on_read` does.
- `58-backfill-incremental` counts `done` **and `failed`** as progress, so a
  backfill in which every copy failed reports a fully drained backlog.
- `tests/proxmox/abuse/run-all.sh` globs only `01-10`, orphaning every
  bug-specific regression script from 12 upward — precisely the ones pinning
  previously-shipped data-loss fixes.
- No coverage for: leader-gating of the tiering/orphan/flush paths (T1), a
  sparse or gapped chunk index (T3), non-chunk-aligned write flush (T4.2).

---

## What this means for the test run

The three findings that would most distort next week's results are all **open**:
T1 (a follower silently deleting cache replicas cluster-wide), T3 (silently
serving the wrong chunk's bytes), and T11 (the integrity gate cannot fail).
Fixing T11 first is worth it regardless — an integrity suite that always passes
makes every other result unfalsifiable.

## Process note

This review's own base was stale, which is why half of it duplicates work
already merged. Before the next audit: fetch and re-check the remote, and
diff the finding set against `tests/release/cases/` — the upstream case files
are well commented and name the mechanism they pin, which makes them the
cheapest way to tell an open finding from a closed one.

---

## Addendum — merged with origin/master at ef78945 (2026-08-23)

Twelve further upstream commits landed while this work was in flight, mostly a
security push (HMAC auth on the data-plane RPCs, auth-gated mutating REST and
manager metadata gRPC, a WebUI session secret). Effects on this report:

- **T6.2 was independently re-found upstream as LIS-41** and fixed in
  `check_anchor_lag`. The other two sites this review identified -
  `check_misplaced_rebalance` and the `RemoveChunkLocation` anchor-survivor
  scan - were not covered there and are fixed here; all three now agree.
- **T9's XSS finding stands and is fixed here.** The upstream security push
  added authentication, not output encoding, so the stored-XSS path was
  untouched by it. The two are complementary: auth stops an outsider reaching
  the API, escaping stops a *filename* from executing in the operator's
  browser once inside.
- The release-suite numbering collided (upstream took 76-82 for the security
  cases); this review's cases moved to 83-86.
