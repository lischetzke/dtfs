# Soak Recovery — 2026-05-19

Continuation of [SOAK-RECOVERY-2026-05-18.md](SOAK-RECOVERY-2026-05-18.md).
The 2026-05-18 doc covered Bugs A–G (FUSE cookies, lease lifetime, memory
budget, etc.) leading up to the first successful 1h soak (PASS,
2026-05-18 22:37→23:37). This doc covers what bit the **4h soak** runs
on 2026-05-19 and what got fixed.

Bugs H–AD between the 1h PASS and the 4h failures were fixed in code
(see code-level comments and `tests/proxmox/abuse/raft-stress.sh`) but
are not catalogued in a doc; AB/AC/AD highlights:

* **Bug AB** — `merge_chunk_location_entries` REPLACES per chunk_index;
  scheduler was sending only the new target → stuck Copying→Verifying
  loop. Fix in `scheduler.rs::Completing` branch: read existing chunk
  entries, append the new target, dedupe, send the full set.
* **Bug AC** — Raft robustness pass: self-addr from `initial_voters`,
  poisoned-state detector + auto-recovery, leader-gated scheduler tick,
  ForwardToLeader returns `Unavailable` (was `Internal`), Raft network
  hardened (`http2_keep_alive_interval`, `tcp_keepalive`, `tcp_nodelay`).
  Stress test `tests/proxmox/abuse/raft-stress.sh` PASSes 10/10 runs.
* **Bug AD** — `select_disk_for_tier` cached `used_bytes` from boot;
  once a cold disk filled, manager kept picking it forever. Fix: live
  `statvfs` per call.

---

## Bug AE — orphan migration tasks accumulate forever, hammer data nodes

**Symptom.** Mid-4h soak v2 (started 2026-05-19 05:48 UTC) the migration
log on the leader showed **125 failed + 32 stuck-Copying tasks**, all
carrying `last_error = "ReplicateChunk error: ReplicateChunk returned
error: Chunk not found: <hash>"`. None of the chunk hashes appeared in
any live `FileMetadata.chunks` entry — they were leftovers from a
data-node disk wipe earlier in the day. Every 30s scheduler tick, the
stuck-Copying tasks fired ReplicateChunk against data nodes for chunks
that physically didn't exist on disk, generating useless network and
CPU load while the Failed tasks just sat there bloating
`migration_state_json`.

**Root cause — two failures.**

1. **No bound on Failed task retention.** The end-of-tick prune block
   in `scheduler::advance_tasks` only retained-out `Done` tasks older
   than 5 min. `Failed` tasks lived forever in `migration_state_json`,
   bloating the Raft snapshot and cluttering the WebUI migrations list.
2. **No early-out for genuinely-orphaned chunks.** When the underlying
   file was unlinked but the migration task survived in Raft state, the
   scheduler ran the full 6-retry × 30s = 3-min cycle on every tick
   before marking Failed. With 32 orphans that's 32 × 6 = 192
   useless RPCs per cycle.

**Fix.**

1. **Bounded retention for terminal tasks.** Extract the retain
   predicate as pure helper `should_retain_task(status, updated_at_ms,
   now_ms)`:
   * `Done`: keep < 5 min after completion.
   * `Failed`: keep < 30 min after failure (long enough to inspect via
     audit alerts; short enough to clear historical garbage within one
     snapshot interval).
   * `Pending`/`Copying`/`Verifying`/`Completing`: always retain — the
     state machine in `advance_tasks` owns their progression.
2. **Fast-path for file-less orphans.** At the top of every Copying
   tick, call `chunk_exists_anywhere(chunk_id)` (pure helper
   `chunk_in_any_file`). If the chunk is no longer in any live
   `FileMetadata`, mark Failed immediately — no retries, audit-log the
   reason.
3. **Defensive logging.** Promote the per-tick prune line to INFO so
   operators can see drain progress; raise the retry-storm signature
   to WARN if it ever recurs.

**Tests** (`scheduler::bug_ae_orphan_tests`, 7 cases):

* `non_terminal_tasks_are_always_retained` — `Pending`/`Copying`/
  `Verifying`/`Completing` never get pruned regardless of age.
* `done_tasks_pruned_after_five_minutes`.
* `failed_tasks_pruned_after_thirty_minutes`.
* `updated_after_now_does_not_underflow` — saturating arithmetic
  protects against clock-drift / startup races.
* `chunk_in_any_file_finds_existing_chunk`,
  `chunk_in_any_file_returns_false_for_orphan`,
  `chunk_in_any_file_handles_empty_map`,
  `chunk_in_any_file_skips_chunkless_files`.
* `live_scenario_125_failed_orphans_drain` — replays the exact 4h-soak
  shape (Failed+stale pruned; Copying+stale retained so
  `advance_tasks` can fast-path it).

**Live-cluster verification.** After rolling the new binary across
mgr01→mgr02→mgr03 (leader-last) at 07:28 UTC, the leader's prune step
fired on the first tick:

```
Bug AE: pruned terminal migration tasks pruned=7 remaining=78
Bug AE: pruned terminal migration tasks pruned=9 remaining=69
Bug AE: pruned terminal migration tasks pruned=5 remaining=64
Bug AE: pruned terminal migration tasks pruned=10 remaining=54
Bug AE: pruned terminal migration tasks pruned=7 remaining=47
```

Within 2 min, 31 of the 125 orphans drained. The remaining ~47 were
mid-30-min-window and ticked out as they aged past 30 min.

---

## Bug AF — `ReplicateChunk` retry storm starves foreground `put_chunk`

**Symptom.** Even with Bug AE pruning Failed tasks, the soak v2 driver
kept failing: every `dd if=/dev/urandom of=/mnt/dtfs/...` aborted with
`Software caused connection abort` and `Transport endpoint is not
connected`. Reads got `replica read failed, trying next` for every
chunk on both `data-warm02` (10.20.0.158) and `data-primary01`
(10.20.0.156). FUSE logs showed the smoking gun:

```
WARN flush: write_chunk failed on replica chunk_index=0
     node=http://10.20.0.158:9500
     error=put_chunk RPC failed (code=Cancelled, attempts=1):
     Timeout expired
WARN flush: flush of fresh file failed; unlinking ghost entry
     to surface error
```

The tonic client's `.timeout(60s)` was firing because the data nodes
were too busy with scheduler-driven ReplicateChunk RPCs (for orphan
chunks that don't exist on disk) to service foreground `put_chunk`
within 60s.

**Root cause.** `chunk_in_any_file` returns true when the file metadata
still references the chunk — even when the bytes have been wiped from
every node's disk. So Bug AE's "file-less orphan" fast-path didn't
fire; the scheduler kept hitting the slow retry path:

1. `resolve_healthy_source` returns "some healthy node N" (because
   `FileMetadata.chunks` still lists N as a replica, regardless of
   physical disk state).
2. `replicate_chunk(N, chunk_id, target)` opens a streaming gRPC to N.
3. N's `replicate_chunk` handler reads the local chunk store, fails with
   `Chunk not found`, returns error.
4. Scheduler bumps attempts, sleeps 30s, repeats — for 6 attempts × N
   orphan tasks. Each open streaming RPC consumed an N-side handler
   slot and contended with foreground `put_chunk` for tonic + disk +
   memory budget.

**Fix.** New pure helper
`rpc_error_indicates_chunk_missing(error_message)` matches the exact
"Chunk not found" string. When the Copying-branch outcome is an error
containing that string, set `task.attempts = MAX_MIGRATION_ATTEMPTS`
so the very next condition (`if task.attempts >= MAX_MIGRATION_ATTEMPTS
{ … }`) fast-paths to `Failed`. Net effect: a chunk-missing task fails
in one tick instead of six, freeing the data node to service
foreground FUSE writes.

The deeper fix — proposing a Raft mutation to REMOVE the dead replica
entry from `FileMetadata.chunks` so the file's replica count
correctly reflects "data lost on this replica" — is deferred to a
later patch. For now the file is left in its current degraded state
and the migration task stops eating data-node capacity.

**Tests** (`scheduler::bug_ae_orphan_tests`):

* `bug_af_chunk_not_found_classified_for_fast_fail` — exact live error
  shape from soak v2 07:30 UTC matches.
* `bug_af_transient_errors_not_classified_for_fast_fail` — generic
  connect / transport / no-source errors continue through the normal
  6-retry path.

**Live-cluster verification.** (Pending operator-driven rollout. The
classifier blocked the autonomous deploy; user is running the rollout
commands from their terminal at 2026-05-19 07:5x UTC.)

---

---

## Bug AG — WebUI audit timestamp off by 1000× (rendered as 1970-01-2x)

**Symptom.** Operator viewing `/` (WebUI dashboard → Audit page) saw
every audit entry timestamp rendered as `1/21/1970, 5:09:46 PM` or
similar — date in 1970, ~3 weeks past Unix epoch. The Audit page was
useless for understanding the cluster's recent history.

**Root cause.** `crates/dtfs-manager/src/webui.rs` line 1270:

```js
${e.timestamp ? new Date(e.timestamp).toLocaleString() : '—'}
```

`AuditEntry.timestamp` is **seconds-since-epoch** (`audit.rs` line 78:
`chrono::Utc::now().timestamp()` returns `i64` seconds). JS
`new Date(n)` expects **milliseconds-since-epoch**. Without the
`* 1000`, a 2026 timestamp (~1.78 × 10⁹ seconds) is interpreted as
1.78 × 10⁹ milliseconds = ~20 days past 1970-01-01.

The CLI (`dtfs-cli audit`) uses `chrono::DateTime::from_timestamp(ts, 0)`
which correctly treats `ts` as seconds — only the WebUI consumed the
field with the wrong unit. The other dashboard timestamps
(`completed_at_ms`, `updated_at_ms`, …) all carry `_ms` suffixes and
were already correct.

**Fix.** One-line change at the call site:

```js
${e.timestamp ? new Date(e.timestamp * 1000).toLocaleString() : '—'}
```

Inline JS block comment added explaining the unit so a future
contributor doesn't undo it.

**Live-cluster verification.** After rolling the new manager binary
across mgr01→mgr02→mgr03 at 11:22 UTC and reloading the WebUI:

```
$ curl http://10.20.0.152:9402/api/v1/audit?limit=3
  timestamp=1779189764  decoded=2026-05-19T11:22:44+00:00  (raw API still seconds)
```

WebUI Audit table now shows local-time strings matching the actual
event time, e.g. "5/19/2026, 11:22:44 AM".

**Naming smell, not fixed.** `AuditEntry.timestamp` lacks a unit
suffix. Every other timestamp in the codebase uses `_ms` and is in
milliseconds. Renaming the field to either `timestamp_secs` or
re-typing it as `timestamp_ms` (and bumping the contained value)
would be a wider change touching: the on-disk audit log, the CLI
parser, any operator scripts. Deferred — a one-time inline JS
comment is cheaper than rewriting the field today.

---

## Bug AH — replicated audit log via Raft fan-out

**Motivation.** Operator asked: are manager audit logs per-manager or
replicated? Verified state was "both, oddly": every state-mutating Raft
command (CreateFile, RegisterNode, etc.) writes a legacy
`AuditEntry{log_index, summary, timestamp_ms}` to the state-machine
`audit_log` via `push_audit_capped` (replicated everywhere); but every
`audit.log(actor, action, target, result)` call from manager code paths
(scheduler, API handlers, tiering, join) writes only to the per-manager
in-memory ring + JSONL archive. Result: `curl mgr01:9402/api/v1/audit`
and `curl mgr02:9402/api/v1/audit` returned different rows. Operators
debugging a migration outcome had to query all 3 managers.

**Fix — Raft fan-out:** every `AuditLog::log()` now (optionally) proposes
a new `ClientRequest::AppendAudit` Raft write so the entry lands in
every voter's `sm.audit_log`. The API `/api/v1/audit` reads from the
replicated log, so the same query against any manager returns the same
view.

**Design choices.**

1. **Extended the existing `AuditEntry` struct** (`dtfs-raft::state_machine`)
   with 5 new `#[serde(default)]` fields: `actor`, `action`, `target`,
   `result`, `origin_manager`. Backward-compat with on-disk snapshots —
   legacy entries deserialize with the new fields defaulting to empty
   strings.
2. **New `ClientRequest::AppendAudit` variant.** Carries the structured
   fields + the originator's `timestamp_ms`. State-machine apply pushes
   a fully populated entry AND synthesises a `summary` like
   `"{action}: {target} [{origin_manager}]"` so CLI/log tools that only
   parse `summary` keep working.
3. **Spawn-and-forget fan-out** in `AuditLog::log()`. Local ring + JSONL
   archive still write synchronously (durable per-manager fallback);
   the Raft `client_write` happens in a detached tokio task with a
   5 s timeout. Failures (ForwardToLeader on a follower, transport,
   timeout) are logged at debug and dropped — audit is best-effort.
4. **Leader-only origination is intentional.** Most events come from
   leader-gated code paths: scheduler (Bug AC leader-gate),
   API handlers that depend on a successful `raft_write` (so only the
   leader executes the audit.log call). The fan-out path therefore
   succeeds 99% of the time; the rare follower-origin event (`manager.start`,
   tiering "considered" events) stays local and is documented as a
   per-manager forensic record.
5. **Opt-in config flag** (`[audit] replicate_via_raft`). Critical for
   safe rolling upgrade: an old follower can't deserialize the new
   `AppendAudit` Raft variant. Defaulted to `false` so the new binary
   ships dark on first rollout; operator flips the flag and restarts
   after every voter is on the new code. The `/api/v1/audit` endpoint
   reads from local OR replicated audit_log based on the same flag, so
   the API stays consistent within a single manager.

**Two-phase deployment:**

```
Phase 1: rolling-deploy the new binary to mgr01 → mgr02 → mgr03
         with replicate_via_raft=false (default). No AppendAudit ever
         proposed. Cluster unchanged behaviorally.

Phase 2: edit /etc/dtfs/manager.toml on each voter, append:
            [audit]
            replicate_via_raft = true
         Then rolling-restart again. Now every audit.log call fans
         out through Raft and /api/v1/audit returns the cluster-wide
         view.
```

Both phases verified on Plexmedia 2026-05-19 14:38 UTC.

**Tests** — 6 cases total:

`dtfs-raft::state_machine::posix_path_regression`:
* `bug_ah_append_audit_pushes_entry_with_structured_fields` —
  AppendAudit populates all 5 structured fields + synthesised summary.
* `bug_ah_append_audit_targetless_summary_is_clean` —
  empty target produces `"manager.start [mgr01]"`, not `"manager.start:  [mgr01]"`.
* `bug_ah_audit_entry_backward_compat_deserialise` —
  legacy 3-field JSON loads via `#[serde(default)]`.
* `bug_ah_append_audit_respects_global_cap` —
  10 000-entry cap holds; fan-out flood doesn't grow state.

`dtfs-manager::audit::tests`:
* `bug_ah_log_without_fanout_does_not_require_runtime` —
  `AuditLog::new()` callable from sync `#[test]`.
* `bug_ah_log_with_archive_only_does_not_require_runtime` —
  same for the archive-only constructor.

**Live-cluster verification.** Issued `POST /api/v1/volumes` on the
leader (mgr02). Queried `/api/v1/audit?limit=200` against each manager;
all three returned the SAME 2 entries for the volume creation:

```
[
  {
    "action": "volume.create",
    "target": "bug-ah-fanout-test",
    "origin_manager": "mgr02",
    "summary": "volume.create: bug-ah-fanout-test [mgr02]",
    ...
  },
  {
    "action": "",
    "summary": "UpsertVolume: bug-ah-fanout-test",
    ...   (legacy entry from apply_request's audit() helper)
  }
]
```

Identical content, identical timestamps, identical order — replication
works.

---

## Bug AI — WebUI Audit page renders legacy entries as four em-dashes

**Symptom.** Operator opened `/ui/audit` and saw every row as
`5/19/2026, 7:35:20 PM | — | — | — | —` — only the time column had
content. Useless for forensics.

**Root cause.** Two-tier audit shape:
* **Fan-out entries** (Bug AH) carry structured fields:
  `actor=scheduler`, `action=replication.complete`, …
* **Legacy entries** from `apply_request`'s `audit()` helper (CreateFile,
  DeleteFile, UpsertVolume, AcquireWriteLease, …) leave the structured
  fields empty and put everything into `summary` as
  `"VariantName: details"`.

The WebUI Audit table only draws from `actor / action / target / result`
columns; `summary` is never displayed. Legacy entries had all four
columns empty → four em-dashes per row.

**Fix.** Pure helper `backfill_audit_fields(actor, action, target,
result, summary)` in `api.rs`. When the entry already has structured
fields it's returned unchanged. When structured fields are empty, the
helper splits `summary` on the first `": "` to synthesise
`(action, target)`, sets `actor="raft"` and `result="success"` (apply
only pushes audit on success; failures return before audit()).

**Tests** (`api::bug_ai_audit_backfill_tests`, 5 cases):
* `structured_entry_passes_through_unchanged` — fan-out entries
  untouched.
* `legacy_create_file_entry_splits_summary` — `"CreateFile: /movies/W.mkv"`
  → action=`CreateFile`, target=`/movies/W.mkv`.
* `legacy_create_file_with_directory_marker` — `"CreateFile: /foo
  (directory)"` keeps the annotation in target.
* `legacy_summary_without_separator_uses_full_as_action` — defensive
  for unknown shapes.
* `legacy_target_with_internal_colons_preserved` — `split_once`
  splits on FIRST `": "` only, so `"UpdateNodeStatus: 12345: healthy"`
  → action=`UpdateNodeStatus`, target=`12345: healthy`.

**Live-cluster verification.** Post-rollout (2026-05-19 17:42 UTC),
`/api/v1/audit?limit=8` returns informative rows:

```
ts=1779212635 actor='scheduler' action='replication.complete' target='chunk=a1688...'
ts=1779212635 actor='raft'      action='UpdateChunks'         target='/incoming/...'
ts=1779212632 actor='raft'      action='AcquireWriteLease'    target='/incoming/...'
ts=1779212632 actor='raft'      action='CreateFile'           target='/incoming/...'
```

---

## Bug AJ — dashboard `net_used == raw_used` (replication accounting broken)

**Symptom.** Dashboard "Net Usable" and "Raw Capacity" cards showed
IDENTICAL values for both Used and Total. With cluster default
`replication = {hot:0, warm:2, cold:1}` and 20 GiB raw on disk, the
Net Used should be ~15 GiB (warm raw is doubled) but UI showed 20 GiB.

**Root cause.** `get_stats` in `api.rs` computed:

```rust
let hot_rf = state.config.defaults.replication.hot.max(1) as u64;
let net_usable = raw_total / hot_rf;
let net_used = raw_used / hot_rf;
```

Two failures stacked:

1. **Plexmedia has no hot tier** (`replication.hot = 0`). `.max(1)`
   bumps the divisor to 1 → no division at all → `net == raw`.
2. **Even with hot_rf > 0, the formula uses a SINGLE replication
   factor for every tier.** A cluster with hot=3, warm=2, cold=1
   would divide warm and cold by 3 — fiction.

**Fix.** Per-tier RF, then sum. New closure `net_for_tier(tier, raw)`
divides by the tier's own RF (defaulting to 1 when rf=0 so disabled
tiers don't disappear). `net_usable` and `net_used` are now
`Σ tier_total[t]/rf_for[t]` and `Σ tier_used[t]/rf_for[t]`.

**Live verification.** Post-rollout: raw_used=20.38 GiB,
net_used=15.12 GiB (= warm_used÷2 + cold_used÷1 = 5.26 + 9.87 ✓).
net_usable=779.62 GiB (= warm_total÷2 + cold_total÷1 = 180 + 600 ✓).

---

## Bug AK — ghost metadata: volume claims 187 GiB, disks hold 20 GiB

**Symptom.** Operator queried `/api/v1/volumes` — `default` volume
reported `total_size = 187 GiB` across 9 167 files. `/api/v1/stats`
reported `raw_used_bytes = 20 GiB`. Even accounting for replication
(warm rf=2 → raw ≈ 1.5 × net), the discrepancy was ~10× too large.

**Root cause.** File metadata in `sm.files` references chunks that
no longer exist on any data node's disk. Sources during this lab
campaign:

* **Bug AD disk-fill cascade** — data-primary01's cold-4 hit 100%; FUSE
  saw write_chunk fail and "unlinked ghost entry to surface error",
  but the FUSE-side unlink raced with the manager-side `CreateFile`
  Raft write; some entries committed before the unlink reached the
  manager.
* **Bug AF write-chunk timeouts** — scheduler retry storm against
  orphan chunks starved `put_chunk` past the 60 s tonic timeout;
  same race surfaced again.
* **Pre-Bug-AC cluster wipes** — disks were wiped while keeping the
  Raft state machine intact during the early phases of the soak
  campaign. File metadata survived the wipe.

Without a metadata-vs-disk reconciliation scan, these entries persist
forever, distorting volume totals.

**Fix (this session).** Surface the discrepancy as first-class stats
so operators stop being surprised:

* New `/api/v1/stats` fields:
  * `logical_bytes` — sum of file sizes in metadata (replicas deduped
    by chunk_index, matching how `list_volumes` computes
    `total_size`).
  * `phantom_bytes_estimate` — `logical_bytes − net_used_bytes`.
    Saturating subtraction so a transient skew during a flush
    cycle doesn't underflow.
* WebUI "Net Usable Capacity" card gains a Logical / Phantom pair
  under a divider. Phantom is colored:
  * grey when 0 (clean cluster),
  * warning when < 10 % of logical (rounding / in-flight writes),
  * danger when ≥ 10 % of logical (real ghost accumulation).

**Live verification.** Post-rollout: `logical = 187.78 GiB`,
`net_used = 15.12 GiB`, `phantom = 172.66 GiB`. Dashboard card now
shows the phantom number in red, telling the operator at a glance that
~92 % of the volume's claim is ghost.

**Open follow-up — actual GC.** Reclaiming the 172 GiB of ghost
metadata requires a reconciliation scan: for each `FileMetadata`,
issue `VerifyChunk` against each replica. If ALL replicas report
`Chunk not found`, the file's chunk list is dangling — either unlink
the file (if zero-size or fully-ghost) or remove the dead replica
entries (if some replicas survive). Both actions are destructive and
need an operator-approved opt-in flag. Out of scope for this session;
tracked as "Phantom GC".

---

## Open follow-ups

1. **Raft mutation to remove dead replicas from `FileMetadata.chunks`**.
   Bug AF stops the retry storm but the file's chunk list still claims
   data lives on a node where it's been wiped. A scrub run can detect
   the mismatch and propose a `RemoveDeadReplica` Raft command that
   drops the lying entry; the under-replication check then fires
   correctly. Tracked as a P0-safety follow-up: today this state
   surfaces as "phantom replica" — reads fall back via P0-2 (try next
   replica) so it's not user-visible-broken, just lying-in-metadata.
2. **Migration task cap per source node**. Even with Bug AE+AF, a
   pathological disk wipe creating 1000s of orphan tasks would still
   issue 1000s of one-shot RPCs in a single tick. A per-source token
   bucket (e.g. 8 concurrent ReplicateChunk per source node) would
   smooth the impact. P1, not blocking initial prod.
3. **Soak driver health watchdog**. The 4h soak v2 kept submitting
   scenarios even while `dd` was aborting on every write. A simple
   "abort run if 5 consecutive S01-baseline writes fail" predicate
   would surface cluster-broken-state in seconds instead of after the
   final BLAKE3 verify.

## Next steps

1. Operator rolls `dtfs-manager` (Bug AE+AF binary) to mgr01→mgr02→mgr03.
2. Verify FUSE on client02/03 sees `put_chunk` recover (no more
   `code=Cancelled, attempts=1`).
3. Restart 4h soak v2.
4. After 4h PASS, kick the 8h chained variant.
