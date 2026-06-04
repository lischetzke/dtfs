# PLAN-BUG-CC — Route per-chunk reads to the leader's tiering engine

Status: complete — built + tested on dtfs-builder, deployed and live-verified
on the Minicluster (2026-06-03). All tasks ✅.

## Background

While testing the caching policy on the Minicluster, read-driven cache
promotion never fired: a cold-only file read repeatedly stayed cold-only and
the tiering tick logged `0 promotions` while demoting the same chunks every
tick. Instrumentation (temporary `BUG2_QUERY_*` / `BUG2_INGEST` logs, since
reverted) showed the read counters DID reach a manager's read-window under the
correct DC, but only on the manager the data node happened to heartbeat to.

Root cause: the tiering engine's per-(chunk,DC) read window (`read_windows`) and
the promote/demote tick are in-memory, per-manager, and the tick is
leader-gated (`crates/dtfs-manager/src/tiering.rs`). The heartbeat handler
(`crates/dtfs-manager/src/metadata.rs`) applied each heartbeat's
`chunk_reads` to the LOCAL manager's engine via `TieringEngine::report_access`.
A data node heartbeats to whatever manager it connected to - often a follower
(after a post-registration leadership change). Reads landing on a follower never
reached the leader's window, so the leader's tick saw zero reads for those
chunks and (a) never promoted them and (b) demoted them as idle. Net effect:
caching only ever evicted, never cached.

(Two non-code config issues found alongside and corrected on the cluster, NOT
part of this code change: the client `[write] first_write_tier` was
`nearest-warm` so writes were born warm with nothing to promote - set to `cold`;
`cache_replicas.warm.min` / `promote_reads` were `0` so the promote target was
`min(0,max)=0` / promotion disabled - set to `min=1,promote_reads>=1`; data02
and the client were misconfigured `datacenter = "dc-b"` vs inventory `dc-a`.)

## Goals
- A per-chunk read counter reaches the LEADER's tiering engine regardless of
  which manager its data node heartbeats to.
- Reuse the existing leader-forward infrastructure; no new gRPC.
- The read report is best-effort and never blocks/fails the heartbeat.

## Non-goals
- Replicating the read window via Raft (it is volatile, not durable state).
- Changing the promote/demote decision logic (it was correct once fed reads).

## Data-model changes
- `crates/dtfs-raft/src/lib.rs` `ClientRequest`: new variant
  `ReportChunkReads { node_id: u64, entries: Vec<(String, HashMap<String,u32>)> }`
  (chunk_id_hex -> reads_by_dc). New enum variant; no `#[serde(default)]` needed
  (tagged enum), no snapshot-compat impact (never persisted - see below).

## Task list
- **CC-1** ✅ Add `ReportChunkReads` to `ClientRequest` + `Display`
  (`dtfs-raft/src/lib.rs`).
- **CC-2** ✅ State-machine apply arm = no-op `ClientResponse::Success`
  (`dtfs-raft/src/state_machine.rs`) - the variant never reaches Raft apply in
  practice (intercepted), the arm exists for match exhaustiveness.
- **CC-3** ✅ `forward_client_write` intercept: if the forwarded request is
  `ReportChunkReads`, call `TieringEngine::report_access` per entry and return
  success BEFORE `raft.client_write` (`dtfs-manager/src/metadata.rs`).
- **CC-4** ✅ Heartbeat handler: replace the local `report_access` loop with
  `proxy_to_leader(ReportChunkReads{..})` (`dtfs-manager/src/metadata.rs`).
  `proxy_to_leader` always routes through `forward_client_write`; on the leader
  it self-forwards (current_leader == self -> own addr). Best-effort
  (`let _ = ...`).
- **CC-5** ✅ changelog `UNRELEASED.md` entry.
- **CC-6** ✅ build + `cargo test -p dtfs-raft -p dtfs-manager --release`
  (57 + 181 passed, 0 failed) on dtfs-builder.
- **CC-7** ✅ live-verify on the Minicluster.

## Cross-cutting invariants preserved
- `ReportChunkReads` is NOT state-machine state; the apply arm is a no-op and it
  is only ever delivered via `forward_client_write` (intercepted), so it never
  mutates the Raft state machine or snapshots.
- Read reporting stays best-effort: a dropped report (mid-election, transient
  transport error) costs one read sample; the next heartbeat re-reports.

## Verification log
- **2026-06-03 — CC-6 build+test (dtfs-builder).** `cargo build --release --bin
  dtfs-manager` RC=0; `cargo test -p dtfs-raft -p dtfs-manager --release` ->
  57 + 181 passed / 0 failed.
- **2026-06-03 — CC-7 live-verify (Minicluster).** With the fix deployed and a
  volume policy of `first_write_tier=cold`, `warm{min=1,max=2}`,
  `promote_reads>=1`: a fresh cold-only file read 2-3x from a cold-cache client
  was promoted to a warm cache replica - `cache.promote` audit rows fired
  (e.g. 24 then 48 for two test files), the tick logged non-zero promotions
  (`8 promotions`), and the warm `.dat` appeared on a dc-a warm disk while the
  cold anchor remained. Confirmed the read path reaches the leader via the
  forward (`forward_client_write` intercept) including the leader self-forward.

## Deviations from plan
- Instrumentation (`BUG2_*` logs) was added to pin the bug, then fully reverted;
  `tiering.rs` carries no net change.
