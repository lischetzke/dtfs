# FUSE mount serializes ALL operations — one stalled read freezes the whole mount

**Found:** 2026-06-14, while wiring the CO-3-12 live release case (69) on the
minicluster. **Severity: HIGH** (directly hits the prime Plex workload).

## Summary

A single slow/stalled chunk read blocks **every** other FUSE operation on the
mount — not just other reads of the same file, but `open`/`getattr`/`ls` of
**unrelated** files. The DTFS FUSE client processes kernel requests serially
(head-of-line blocking), so any one slow `GetChunk` (e.g. a cold chunk served
from the 40 Mbps LELI archive) stalls all concurrent playback and library
browsing until it completes.

## Evidence (live, minicluster, deployed build 20260614-145804)

Setup: single cold replica of a 4 MiB file on data01; `tc tbf rate 1mbit` on
data01 `eth0` so a cold read stalls ~30 s in the fetch; read issued from the
dc-b client (cold cache).

While that read is stalled:
- 40/40 concurrent metadata-only opens (`: < file`) of the **same** file blocked
  (each hit the 8 s timeout).
- `open` of a **different** file B: TIMEOUT.
- `getattr`/`stat` of a **different** file B: TIMEOUT.

A blocked `getattr` on an unrelated file proves the bottleneck is **global**
request dispatch, not the per-file `open_files` DashMap guard.

## Root cause

- `crates/dtfs-fuse/src/main.rs` mounts with `fuse3::raw::Session::new(MountOptions::default())`
  (fuse3 0.7). Requests are dispatched serially — a handler future that `.await`s
  a slow network fetch holds up the next request.
- The `[io]` concurrency knobs the docs advertise
  (`docs/src/access/fuse-mount.md`: `max_read_concurrency`, `max_write_concurrency`,
  `fuse_threads`) are **not implemented** — no such fields exist in
  `crates/dtfs-common/src/config.rs`. Documentation describes config that does
  nothing.

## Impact

Target workload is multi-stream Plex playback + browsing over an asymmetric WAN
where cold chunks come from a 40 Mbps archive. One cold/slow chunk read freezes
the whole mount → all streams + `ls`/stat stall. This is the workload DTFS
exists to serve, so it is high priority.

## Relation to CO-3-12

CO-3-12 (drop the `open_files` guard before the read chunk loop) is correct and
necessary, but its benefit is entirely masked by this global serialization: even
with the guard dropped, the serial session dispatch blocks concurrent ops. So
CO-3-12 cannot be verified in isolation on a live mount until this is fixed —
release case 69 stays KNOWN_ISSUE pinned here (CO-3-12 remains unit/structural-
covered).

## Repro (scripted)

`tests/release/cases/69-concurrent-read-no-guard-serialize.sh` contains the
throttle + stalled-read + concurrent-open harness. As-is it FAILS on the current
build (40/40 opens blocked) — kept as KNOWN_ISSUE rather than FAIL because it is
exposing this larger defect, not a CO-3-12 regression.

## Fix options (not yet done — needs a decision; likely non-trivial)

1. Dispatch each FUSE request on its own task so a stalled handler does not block
   the session loop (spawn per request in the adapter, or a bounded worker
   pool). Verify fuse3 0.7 actually serializes vs whether a newer fuse3 / a mount
   option enables concurrency.
2. Implement the documented `[io]` config (`fuse_threads`, `max_read_concurrency`,
   `max_write_concurrency`) and wire it to the dispatch + the read/write fan-out,
   OR remove the vaporware config from the docs.
3. Bound the per-chunk fetch with a timeout + replica fallback so a single slow
   node cannot stall a read indefinitely (defence-in-depth, complements 1).

## Recommendation

Treat as a new HIGH bug (suggest next free plan letter). Option 1 is the core
fix; 2 aligns docs with reality; 3 hardens the read path. Re-enable case 69 to a
real PASS once dispatch is concurrent.
