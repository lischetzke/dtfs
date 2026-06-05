# PLAN-BUG-CE — FUSE readahead cache must be byte-bounded, not entry-bounded

Status: complete — built + tested on dtfs-builder, deployed and live-verified
on the Minicluster (2026-06-04). All tasks ✅.

## Background

`ReadAheadEngine.cache: DashMap<String, Bytes>` is bounded by ENTRY COUNT
(`max_cache_entries`, hardcoded 256 in `filesystem.rs`). Entries are whole
chunks — up to 128 MB each (see `insert_cached` doc). 256 × chunk_size is
hundreds of MB to multiple GB; on the 1 GB Minicluster client a whole-file
sequential read drove FUSE RSS to ~800 MB and the OOM killer reaped it.
Worse, the prefetch task inserts into the cache (`spawn_prefetch`, the
spawned body) with NO bound check, so concurrent prefetch completions blow
past even the count cap. This violates the memory model (§ A.3.2 / § A.5.2:
FUSE caches must be size-LRU, never unbounded byte accumulation).

## Goals
- The readahead in-memory cache is bounded by a configurable BYTE budget;
  total cached bytes never materially exceed it regardless of chunk size or
  prefetch concurrency.
- Best-effort, never rejects a read (evict to fit; § A.5 — slow down, don't
  fail).

## Non-goals
- True LRU ordering (keep the existing "drop an arbitrary entry" policy;
  DashMap has no order). Byte-bounding is the fix, not recency precision.
- Bounding the disk-backed `local_cache` (already size-capped, P0-11).

## Data-model / config changes
- `crates/dtfs-common/src/config.rs` `ReadAheadConfig` gains
  `#[serde(default = "default_readahead_cache_mb")] pub cache_mb: u64`
  (default 64). Back-compatible (serde default).

## Task list
- **CE-1** ✅ Failing test `bug_ce_cache_is_byte_bounded` in `readahead.rs`:
  4 MB byte cap, `insert_cached` eight 1 MB chunks; assert `cache_bytes() <= cap`
  and not all survive.
- **CE-2** ✅ `ReadAheadEngine`: replaced `max_cache_entries` with
  `max_cache_bytes` + `cache_bytes: Arc<AtomicUsize>`; added `store_in_cache`
  associated helper (accounting + evict-to-fit, skips the just-stored key) and
  a `cache_bytes()` accessor; routed `insert_cached` and the `spawn_prefetch`
  task through it; updated `evict`/`clear` accounting; switched the prefetch
  "cache full" guard to the byte budget. Constructor takes `max_cache_bytes`.
- **CE-3** ✅ `filesystem.rs`: passes `config.readahead.cache_mb * 1024 * 1024`
  instead of the hardcoded `256`.
- **CE-4** ✅ `config.rs`: added `cache_mb` + `default_readahead_cache_mb` (64).
- **CE-5** ✅ changelog `UNRELEASED.md` entry.
- **CE-6** ✅ build + `cargo test -p dtfs-fuse --release` on dtfs-builder;
  observed fail→pass.

## Cross-cutting invariants preserved
- Best-effort memory discipline (§ A.5): the cache sheds entries to stay
  under budget, never rejects a read.
- Disk-cache mirroring of prefetched chunks (P0-11) is unchanged.

## Verification log
- **2026-06-04 — CE-6 fail→pass (dtfs-builder).** Behavioral fail-first: with
  the eviction `while` condition sed-disabled, the new test reported
  `got 8388608 bytes` (8 MB held against a 4 MB budget) → FAILED. With the fix
  restored: `cargo test -p dtfs-fuse --release` 33 passed (was 32;
  +`bug_ce_cache_is_byte_bounded`), `dtfs-common` 47 passed, 0 failed.
- **2026-06-04 — deploy + live-verify (Minicluster).** Built `dtfs-fuse` with
  fresh `SOURCE_DATE_EPOCH` (`20260604-095050`), deployed to mini-client01
  (1 GB RAM). Wrote a 512 MB file through the mount and ran a whole-file
  `md5sum` (the pre-CE OOM trigger) after dropping the page cache: the read
  completed (2 s), the FUSE process kept the SAME pid and stayed `active`
  (not OOM-killed), peak RSS during the read was **147 MB** (vs the ~800 MB
  pre-fix that OOM-killed FUSE), and the checksum was stable across a re-read.
  `[readahead] cache_mb` is absent from the deployed `fuse.toml`, so the
  serde default (64) is in force.

## Deviations from plan
- None. (CE-6's failing observation was made behaviorally by sed-disabling the
  eviction loop, rather than as a bare compile error.)
