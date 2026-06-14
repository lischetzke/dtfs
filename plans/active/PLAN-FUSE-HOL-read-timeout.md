# PLAN-FUSE-HOL — bound the FUSE chunk fetch so a hung data node can't stall a read forever

## Corrected finding (supersedes the "whole-mount serialization" claim)

Live root-cause on the minicluster (clean measurements; the earlier "whole mount
freezes" was a `/usr/bin/time`-absent artifact):

- fuse3 0.7.3 dispatches every handler on its own task (concurrent) — NOT the
  bottleneck. During a stalled read, ops on OTHER files are fast (stat/open
  3-5 ms).
- The real bug: `DtfsFilesystem::read` fetches a chunk with
  `self.client.read_chunk(cand, id).await` (filesystem.rs ~715) **with no
  timeout**. P0-2 replica fallback only triggers on a transport `Err`, NOT on a
  hang. A data node that accepts the connection but never responds (dead/hung)
  makes the read await **indefinitely** — confirmed: with data01's :9500
  responses dropped, a cold read hung >35 s (never fell over). The reading app's
  FUSE op never returns.
- Secondary: a concurrent `open` of the SAME file blocks for the stall duration
  (~the fetch). Bounding the fetch also bounds this.

Impact: prime Plex-over-WAN workload — a dead/hung archive node hangs every read
whose first candidate is that node until manual intervention.

## Goals
- A single hung/slow replica cannot stall a read indefinitely: bound each
  per-replica `read_chunk` with a timeout and fail over to the next candidate on
  timeout (extend P0-2 to cover hangs, not just errors).
- Configurable; generous default so legitimately slow (but progressing) reads of
  large chunks over a slow WAN are not cut off.
- Reads stay correct: timeout/fallover is read-only; all-candidates-failed =>
  the existing EIO. No integrity risk.

## Non-goals
- Eliminating the same-file-open serialization entirely (bounded by the timeout;
  deeper per-file-coordination work is out of scope).
- Implementing the full advertised `[io]` block.

## Data-model / config
- New `FuseClientConfig.chunk_read_timeout_secs: u64`, `#[serde(default)]`
  default **60**. `0` = disabled (unbounded, legacy). 60 s is huge vs a 4 MB
  chunk at 40 Mbps (~0.8 s) yet catches hangs.
- Config-surface sync (B.6): `deploy/examples/fuse.toml` + `docs/src/configuration/`.

## Tasks
- FUSE-HOL-1 — config field + default fn (config.rs) + `example_fuse_toml_parses`
  stays green. Done when: config loads, default 60.
- FUSE-HOL-2 — wrap the per-candidate `read_chunk` in
  `tokio::time::timeout(chunk_read_timeout_secs)` in `read()`; on elapsed, warn +
  record last_err + `continue` to the next candidate; `0` keeps the un-timed
  await. Done when: a hung first-candidate falls over to a healthy one; all hung
  => EIO bounded by N x timeout.
- FUSE-HOL-3 — deploy example + docs row.
- FUSE-HOL-4 — release case 69 reworked: drop a data node's :9500 (hung), read a
  file with a healthy + hung replica => served from the healthy one within the
  timeout (not indefinite); a single-replica hung read returns EIO within the
  timeout instead of hanging. Different-file ops stay fast (already true). Keep
  it pinned/auto-promoting.
- FUSE-HOL-5 — correct `plans/reports/ISSUE-FUSE-MOUNT-SERIALIZATION-2026-06-14.md`
  to the accurate finding.

## Verification
Build dtfs-fuse + dtfs-common on the builder, deploy the client, live-verify with
iptables-DROP on a data node's :9500 (hung) at a low test timeout.

## Invariants
- Read-only path; no Raft/metadata mutation. Timeout => try next replica => EIO.
- Anchor/integrity untouched.
