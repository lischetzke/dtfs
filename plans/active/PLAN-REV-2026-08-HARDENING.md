# PLAN-REV-2026-08 — review-driven hardening (ported onto current master)

Findings: `plans/reports/CODE-REVIEW-2026-08-21.md`. Target: the lab test
campaign in the week of 2026-08-24.

## History / why this plan was re-based

The review and a first round of fixes were done against local `master` =
`6eb2b83` (2026-06-11), which was **40 commits behind** `origin/master`
(`f841bcd`, 2026-06-15). Roughly half the findings had already been fixed
upstream by the `CO-*` / `FUSE-HOL` audit series, in one case by a better
mechanism than this review proposed (excess-spill reclamation via a per-node
orphan rule, CO-3-2). The first-round work is preserved on branch
`bug/rev-wave1-hardening` and is NOT to be merged; this plan tracks the port of
the still-needed subset onto `bug/rev-hardening-port`, branched from
`origin/master`.

**Already fixed upstream — do not port:** REV-1-3 (disk-keyed
RemoveChunkLocation), REV-1-4 (demote anchor policy), REV-1-5 (u64 read clamp,
`effective_read_len`), REV-2-2 (replica locations on the wire, CO-3-10), REV-3-1
(data-node metrics tables), REV-3-3 (tiering state sweep), REV-3-6 (per-call raft
deadlines, CO-3-6), REV-4-7 (excess-spill bytes, CO-3-2), REV-4-9 (CLI
char-boundary truncate).

## Goals

Close the findings still OPEN on current master, prioritising the ones that
would make next week's results untrustworthy.

## Non-goals / locked-in decisions

- **No release.** The operator cuts releases. Dev builds only.
- No TLS/auth work (CF-6 stays open).
- Do not re-add anything the `CO-*` series already fixed, and do not add a
  second authority over the same bytes (e.g. no delete-at-removal-time path on
  top of the per-node orphan rule).

## Build / verify environment (deviation from § A.4)

The `dtfs-builder` VM (10.20.0.150) is unreachable from the operator host this
session (ICMP + SSH time out; the 10.20.0.0/24 lab is not routed). Substitute:
local **WSL Ubuntu**, which has the full chain (rustup, protoc, clang, fuse3
headers, 24 cores, 47 GB RAM):

```bash
wsl.exe -d Ubuntu -- bash -lc 'cd /mnt/c/Users/<user>/git_github.com/dtfs2 \
  && export CARGO_TARGET_DIR=$HOME/dtfs-port \
  && cargo test -p <crate> --release'
```

Baseline on this base: dtfs-manager 300, dtfs-raft 87, dtfs-data 83,
dtfs-fuse 52, dtfs-common 81, dtfs-client 31, dtfs-cli 12.

Consequence: **unit and crate suites are verifiable; live lab verification is
not.** Release cases are authored and `bash -n` clean but cannot be executed
until the lab is reachable.

Two environment traps worth recording:

- source files check out **CRLF**, so a script matching an `\n`-joined anchor
  silently fails to match. Use the Edit tool or match CRLF explicitly.
- **rustc 1.94 ICEs** during typeck on some struct-literal edits (`query stack
  [typeck] type-checking ...`). The fix is to reformulate - e.g. move the
  literal into a free function (`metadata::new_chunk_location` exists for
  exactly this reason). Same compiler as the crate-level `#![allow(dead_code)]`
  workaround noted in § A.4.

## Task list

Status: ⬜ todo / 🟦 wip / ✅ done / 🟥 blocked / 🟨 review (authored, not live-verified)

### Landed on this branch

- `REV-2-1a` ✅ **`chunk_index` on the wire, manager half.** `proto/manager.proto`
  `ChunkLocationInfo.chunk_index = 6`, populated in `get_file_locations` via the
  new `new_chunk_location` free fn (the inline struct literal ICEs rustc 1.94
  in that function). Old-peer semantics: an older manager sends 0 for every
  entry, which the client must detect and treat as "unlabelled".
- `REV-2-5` ✅ **The integrity gate can fail again.** Case 13 purges the FUSE
  readahead + P0-11 local caches before reading the corrupted chunk (it dropped
  only the kernel page cache, so the read never reached a data node); case 58
  counts `done` only and fails on any `failed` task; the abuse runner globs
  every `NN-*.sh` instead of `01-10`.
- `REV-2-6` 🟨 **New release cases** 76 (sparse chunk index reads as zeros),
  77 (follower loops dispatch nothing), 78 (unaligned writes lose no bytes).
  76 and 78 in `QUICK_CASES`. Authored, not yet executed.

### Ported (all five areas complete)

- `REV-1-1` ✅ leader-gate `TieringEngine::run`, `run_orphan_scrub`,
  `run_auto_unquarantine_sweep`, `run_lease_gc`, `POST /api/v1/files/flush_cache` (the route moved on this base).
- `REV-1-2` ✅ orphan-reaper freshness gate.
- `REV-1-6` ✅ **not ported — already fixed upstream.** The mid-chunk branch
  returns early and defers the buffer to `flush_all`; `co_1_7_*` PASSES on this
  base and only its comment is stale. Release case 78 keeps it pinned.
- `REV-2-1b` ✅ client half: resolve chunks by `chunk_index`, not array position.
- `REV-2-3` ✅ readahead prefetches via the foreground read's candidate order.
- `REV-2-4` ✅ verify the FUSE local cache against the chunk id before serving.
- `REV-3-2` ✅ incremental `ListChunks` paging.
- `REV-3-4` ✅ cap scheduler task creation per tick and overall.
- `REV-3-5` ✅ memory rung gates on Shed and is self-limiting.
- `REV-3-7` ✅ one liveness definition across backfill / rebalance / anchor-lag /
  the anchor-survivor scan.
- `REV-3-8` ✅ batch the heartbeat integrity-fault handler.
- `REV-3-9` ✅ **not ported — already fixed upstream (CO-3-8).** operator maintenance survives a heartbeat.
- `REV-3-10` ✅ reconcile a stale location row instead of quarantining the node.
- `REV-3-11` ✅ **not ported — already fixed upstream (CO-3-5 `migration_state_lock`).** three-way merge for `migration_state_json`.
- `REV-3-12` ✅ single-sweep cache eviction, off the read path.
- `REV-4-1` ✅ WebUI escaping + CSP.
- `REV-4-2` ✅ cap `AllocateChunks.num_chunks` at the boundary.
- `REV-4-3` ✅ config write validates by re-parsing, atomic rename.
- `REV-4-4` ✅ scrub survives one bad file.
- `REV-4-5` ✅ phantom ARC cache no longer throttles `put_chunk`.
- `REV-4-6` ✅ register a completed replica against every referencing file.
- `REV-4-8` ✅ bound the legacy temperature path; fix its anchor guard.
- `REV-4-10` ✅ clamp allocation RF up to the ack floor.

### Follow-ups opened while executing

- `REV-4-11` ✅ release cases for REV-3-2, REV-4-4, REV-4-5 (all cluster-visible
  per § B.5): `87-listchunks-paging-incremental` (ascending ids, exclusive
  cursor, `chunk_mtimes_ms` alignment), `88-scrub-survives-bad-chunk` (torn
  `.meta` + unreadable bucket counted, disk pass still reports a result), and
  `89-write-slots-no-phantom-cache` (`overhead_mb` = 256 with no `[cache]`
  term; `slots` recomputed from the node's own config). 87/89 are in
  `QUICK_CASES`; 88 waits on a scrub pass so it stays out. New shared helper
  `data_rpc_stamp` in `tests/release/lib/common.sh` builds the LIS-19/55/83
  data-plane HMAC as grpcurl `-H` args (empty on a no-secret cluster, which is
  the correct call there).
- `REV-4-12` ✅ **moot on this base.** CO-4-3 already merges the sparse patch
  onto the running config and validates the merged whole, so the "Apply + write
  to disk" button still works; REV-4-3 only refuses payloads whose merged result
  would not render to re-parsable TOML.
- `REV-4-13` ✅ config surface synced (`deploy/examples/data.toml`,
  `docs/src/configuration/data-node.md` mark `[cache]` inert; AGENTS.md A.3.2 /
  A.8 updated; `run_monitor_with_cache` deleted). `cache.rs` itself is kept,
  unwired, with a header explaining why it must not be re-wired for whole
  chunks.
- `REV-4-14` ⬜ `docs/src/access/fuse-mount.md:139` claims a volume without warm
  caching ignores the `first_write_tier` hint. No such check exists. Fix the doc
  or implement the valve.

## Cross-cutting invariants

- Additive proto/Raft fields only, with explicit old-peer semantics.
- AGENTS.md updated in the same commit as any behaviour/config change (§ B.7).
- Config-surface sync (§ B.6) for any new or newly-inert config key.
- No `--no-verify`, no amending pushed commits.

## Verification log

- **REV-4-11** — 2026-08-24. Release cases 87/88/89 added plus the
  `data_rpc_stamp` helper. `bash -n` clean on all four touched scripts; the
  helper was smoke-run locally and emits
  `x-dtfs-rpc-ts` / `x-dtfs-rpc-mac` matching
  `crypto::data_rpc_mac_message` framing byte for byte. **Not yet executed
  against a cluster** — the lab was unreachable for this session, so the live
  half stays 🟨 until `tests/release/run.sh --quick` runs on the campaign
  cluster.

- **case 13 wait_for** — 2026-08-24. The REV-2-5 cache-purge step called
  `wait_for <desc> <secs> <cmd-string>` against a
  `wait_for <secs> <desc> <cmd...>` helper, so the poll never ran an iteration
  and the case failed immediately. Fixed; every other `wait_for` call site in
  `tests/release/` was audited and this was the only misuse.

- **REV-2-1a / REV-2-5 / REV-2-6** — 2026-08-21. `cargo test -p dtfs-manager
  --release`: 300 passed after the proto change. All touched shell scripts
  `bash -n` clean. Live half 🟨 deferred to the campaign.

- **dtfs-fuse port** — 2026-08-21. `cargo test -p dtfs-fuse --release`: 61 passed
  (baseline 52 + 9). Findings that changed the plan:
  - **REV-1-6 was already fixed upstream** and this review was wrong to call it
    open: the mid-chunk branch now returns early and defers the whole buffer to
    `flush_all`, and `co_1_7_midchunk_flush_does_not_drop_leading_partial_bytes`
    PASSES on the unmodified base. Only that test's comment is stale. Nothing
    ported; release case 78 keeps it pinned.
  - **A second positional-indexing site was found**, not in the original review:
    the read-modify-write seed in `filesystem.rs::write` resolved its chunk with
    `chunks.get(chunk_idx)`, so on a gapped file it seeded from a NEIGHBOURING
    chunk and wrote those bytes back under this chunk's index - a persistent,
    write-side version of T3. Fixed with the same `chunk_at` lookup.
  - **The reference patch for REV-2-1 was itself buggy**: it kept
    `result.extend_from_slice(...)`, so skipping a hole slid every later chunk's
    bytes down into it. The port pre-zeroes the read buffer and copies each
    chunk at its own offset instead.
  - REV-2-4 verifies via `read_verified` (8 MiB `read_exact` pieces hashed with
    `IncrementalHasher` straight into the destination buffer - no second
    whole-chunk copy, A.3.2); observed failing first with "corrupt cache entry
    must report a MISS, never serve rotten bytes". The pre-existing cache tests
    used synthetic non-content-addressed ids and had to move to a `cid(data)`
    fixture, since verify-on-read makes id == BLAKE3(content) load-bearing.
  - REV-3-12 discriminates: with the counter moved back into the victim loop it
    reports `left: 3  right: 1` for three victims.

- **Port complete** — 2026-08-21. `cargo test --workspace --release`:
  dtfs-manager 326, dtfs-raft 88, dtfs-data 89, dtfs-fuse 61, dtfs-common 81,
  dtfs-client 31, dtfs-cli 12; 0 failed anywhere. Four more findings turned out
  to be already fixed on this base and were NOT ported - REV-1-6, REV-3-9
  (CO-3-8), REV-3-11 (CO-3-5's `migration_state_lock` serialises both
  load-modify-save paths, so the three-way merge would be dead machinery) and
  REV-4-8's anchor half (CO-4-7). Each was confirmed by reading the code, not
  by grep alone.
  Adaptations worth recording:
  - the tiering state sweep had to move ABOVE the new leader gate: followers
    ingest `report_access` without ever evaluating, so gating the sweep would
    have leaked `access_stats`/`read_windows` on exactly those nodes;
  - REV-3-10's reconcile is itself bounded by `should_quarantine_for_missing`,
    so a wiped 50 TB disk quarantines directly instead of issuing one Raft
    write per chunk - which would have re-created the flood REV-3-4 bounds;
  - REV-4-4 was partly fixed by CO-4-2; the residue was the directory walk
    still using `?` (one unreadable bucket killed the pass) and a chunk removed
    by GC mid-scan being reported as corrupt, making the manager repair a
    deliberately deleted chunk;
  - `list_all_chunks_with_mtime` and `find_file_for_chunk` are now unused in
    production and kept only as test oracles.
  Deviation from § B.7: AGENTS.md A.3.2 / A.5 / A.8 were updated in the FIRST
  commit of the series rather than split across all four, because the four
  commits are one logical change to a single file and git offers no clean way
  to split a paragraph across commits.

- **Merged origin/master @ ef78945** — 2026-08-23. Twelve upstream commits,
  mostly the LIS security push. Two textual conflicts, both resolved in
  upstream's favour where the code was identical:
  - `scheduler.rs` — comment-only. LIS-41 made the SAME `live_replica_tier`
    change in `check_anchor_lag` that REV-3-7 did, so upstream's comment was
    kept and the duplicate dropped. REV-3-7's other two sites
    (`check_misplaced_rebalance`, the state-machine anchor-survivor scan) were
    NOT covered by LIS-41 and remain this branch's contribution; verified all
    three are health-aware exactly once.
  - `tests/release/run.sh` — `QUICK_CASES`, resolved as the union.
  Non-conflict fixups the merge could not see:
  - release cases renumbered 76/77/78/79 -> **83/84/85/86**: upstream took
    76-82 for the security cases and had just cleaned up a numbering collision;
    reintroducing one would have been rude. Docs table updated.
  - case 86 now sends `X-Dtfs-Shared-Secret` (LIS-18 auth-gates mutating REST),
    otherwise its config POST would be refused as unauthenticated and the case
    would never reach the validation path it exists to test. It fails loudly on
    401/403 rather than silently passing.
  - verified the WebUI escaping survived: zero unescaped `${...}` in the
    production half of `webui.rs` (the apparent hits are the assertion list
    inside the test module), including upstream's new session-secret code.
  Merged tree green: `cargo test --workspace --release` — dtfs-manager 345,
  dtfs-raft 88, dtfs-data 98, dtfs-common 98, dtfs-fuse 61, dtfs-client 31,
  dtfs-cli 12; 0 failed.

## Deviations

- 2026-08-21: builder VM unreachable; verification moved to local WSL Ubuntu.
- 2026-08-21: plan re-based onto `origin/master` after discovering the 40-commit
  stale base; the superseded first-round branch is kept for reference only.
