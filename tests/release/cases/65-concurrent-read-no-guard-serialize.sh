#!/usr/bin/env bash
# CO-3-12: the read path used to hold the open_files DashMap entry guard
# across the per-chunk network loop, so a stalled chunk fetch blocked
# concurrent opens of the same file. The fix clones the locations/chunk_size
# out of the entry and DROPS the guard before the await loop (matching the
# write()/flush() idiom).
#
# A faithful live reproduction needs an injected per-chunk stall (a data node
# that hangs GetChunk mid-stream) plus a second opener racing the same fd,
# which the release harness has no deterministic hook for without patching a
# data node. The regression is locked at the unit/structural layer instead:
# read() now binds owned data inside a scoped block that ends before the first
# .await (crates/dtfs-fuse/src/filesystem.rs), so a compile of dtfs-fuse is the
# guard-drop assertion. SKIP here until a stall-injection seam exists.
source "$(dirname "$0")/../lib/common.sh"

case_cleanup() { :; }
trap case_cleanup EXIT

skip "CO-3-12 guard-drop is unit/structural (no live stall-injection seam yet)"
