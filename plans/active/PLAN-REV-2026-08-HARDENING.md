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

The `dtfs-builder` VM (10.150.0.150) is unreachable from the operator host this
session (ICMP + SSH time out; the 10.150.0.0/24 lab is not routed). Substitute:
local **WSL Ubuntu**, which has the full chain (rustup, protoc, clang, fuse3
headers, 24 cores, 47 GB RAM):

```bash
wsl.exe -d Ubuntu -- bash -lc 'cd /mnt/c/Users/leduc/git_github.com/dtfs2 \
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

### In flight (parallel ports)

- `REV-1-1` ⬜ leader-gate `TieringEngine::run`, `run_orphan_scrub`,
  `run_auto_unquarantine_sweep`, `run_lease_gc`, `POST /api/v1/cache/flush`.
- `REV-1-2` ⬜ orphan-reaper freshness gate.
- `REV-1-6` ✅ write buffer must not drain bytes it never emitted (upstream
  already carries the failing-by-design test `co_1_7_*`).
- `REV-2-1b` ✅ client half: resolve chunks by `chunk_index`, not array position.
- `REV-2-3` ✅ readahead prefetches via the foreground read's candidate order.
- `REV-2-4` ✅ verify the FUSE local cache against the chunk id before serving.
- `REV-3-2` ⬜ incremental `ListChunks` paging.
- `REV-3-4` ⬜ cap scheduler task creation per tick and overall.
- `REV-3-5` ⬜ memory rung gates on Shed and is self-limiting.
- `REV-3-7` ⬜ one liveness definition across backfill / rebalance / anchor-lag /
  the anchor-survivor scan.
- `REV-3-8` ⬜ batch the heartbeat integrity-fault handler.
- `REV-3-9` ⬜ operator maintenance survives a heartbeat.
- `REV-3-10` ⬜ reconcile a stale location row instead of quarantining the node.
- `REV-3-11` ⬜ three-way merge for `migration_state_json`.
- `REV-3-12` ✅ single-sweep cache eviction, off the read path.
- `REV-4-1` ⬜ WebUI escaping + CSP.
- `REV-4-2` ⬜ cap `AllocateChunks.num_chunks` at the boundary.
- `REV-4-3` ⬜ config write validates by re-parsing, atomic rename.
- `REV-4-4` ⬜ scrub survives one bad file.
- `REV-4-5` ⬜ phantom ARC cache no longer throttles `put_chunk`.
- `REV-4-6` ⬜ register a completed replica against every referencing file.
- `REV-4-8` ⬜ bound the legacy temperature path; fix its anchor guard.
- `REV-4-10` ⬜ clamp allocation RF up to the ack floor.

### Follow-ups opened while executing

- `REV-4-11` ⬜ release cases for REV-3-2, REV-4-4, REV-4-5 (all cluster-visible
  per § B.5).
- `REV-4-12` ⬜ the WebUI "Apply + write to disk" button posts a SPARSE patch, so
  after REV-4-3 it gets a clean 4xx instead of corrupting the file. Making it
  work needs the patch merged onto the running config server-side.
- `REV-4-13` ⬜ if REV-4-5 removes the ARC cache from the live path, sync the
  config surface (`deploy/examples/data.toml`, `docs/src/configuration/`) and
  drop the dead `ArcCache` / `run_monitor_with_cache`.
- `REV-4-14` ⬜ `docs/src/access/fuse-mount.md:139` claims a volume without warm
  caching ignores the `first_write_tier` hint. No such check exists. Fix the doc
  or implement the valve.

## Cross-cutting invariants

- Additive proto/Raft fields only, with explicit old-peer semantics.
- AGENTS.md updated in the same commit as any behaviour/config change (§ B.7).
- Config-surface sync (§ B.6) for any new or newly-inert config key.
- No `--no-verify`, no amending pushed commits.

## Verification log

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

## Deviations

- 2026-08-21: builder VM unreachable; verification moved to local WSL Ubuntu.
- 2026-08-21: plan re-based onto `origin/master` after discovering the 40-commit
  stale base; the superseded first-round branch is kept for reference only.
