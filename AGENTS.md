# AGENTS.md — Operating Guide for AI Coding Agents on DTFS

You are working on **DTFS**, a Raft-managed tiered distributed
filesystem written in Rust. Read this file fully before any non-trivial
work. This document is the canonical contract: keep it accurate and
update it whenever you change behaviour, conventions, or the target
workload.

This file is the single source of truth for both **what** to know about
DTFS (Part A — codebase knowledge) and **how** to work on it (Part B —
operating discipline). `CLAUDE.md` is an `@`-import shim that pulls this
file into Claude Code; other tools (Codex, Cursor, …) read AGENTS.md
directly.

---

## Quick rules (apply to every response)

### Approach
- Read existing files before writing. Don't re-read unless changed.
- Thorough in reasoning, concise in output.
- Skip files over 100KB unless required.
- No sycophantic openers or closing fluff.
- No emojis or em-dashes.
- Do not guess APIs, versions, flags, commit SHAs, or package names.
  Verify by reading code or docs before asserting.
- Keep token usage as efficient as reasonable possible. Prefer
  targeted Grep/Read with `offset`/`limit` over full-file reads;
  prefer `grep`/`sed`-style narrow edits over re-emitting whole
  files; reuse already-read context instead of re-reading; batch
  independent tool calls into one message.

### Output
- Return code first. Explanation after, only if non-obvious.
- No inline prose. Use comments sparingly — only where logic is unclear.
- No boilerplate unless explicitly requested.

### Code Rules
- Simplest working solution. No over-engineering.
- No abstractions for single-use operations.
- No speculative features or "you might also want…".
- Read the file before modifying it. Never edit blind.
- No docstrings or type annotations on code not being changed.
- No error handling for scenarios that cannot happen.
- Three similar lines is better than a premature abstraction.

### Review Rules
- State the bug. Show the fix. Stop.
- No suggestions beyond the scope of the review.
- No compliments on the code before or after the review.

### Debugging Rules
- Never speculate about a bug without reading the relevant code first.
- State what you found, where, and the fix. One pass.
- If cause is unclear: say so. Do not guess.

### Simple Formatting
- No em dashes, smart quotes, or decorative Unicode symbols.
- Plain hyphens and straight quotes only.
- Natural language characters (accented letters, CJK, etc.) are fine
  when the content requires them.
- Code output must be copy-paste safe.

---

# Part A — Codebase Knowledge

## A.1 Mission & Target Workload

DTFS exists to serve **a single household-scale Plex media library
across multiple datacenters with asymmetric bandwidth**. Every design
decision is anchored to this scenario.

### A.1.1 Reference deployment — "Plexmedia"

The current real-world target is the `Plexmedia` cluster. Three
datacenters, each with its own role and bandwidth class:

| DC | Link | Role |
|----|------|------|
| LELI | up to 40 Mbps | Primary archive — holds **every** chunk at least once |
| TORO | nominal 400 Mbps | Working set + read-ahead cache for Plex playback |
| NRB | nominal 1 Gbps | Fresh-download landing zone, replicates to LELI |

Per-DC nodes:

| Hostname | DC | Role | Storage | Notes |
|----------|------|--------|---------|-------|
| mgr01, mgr02, mgr03 | LELI | manager | small | Raft quorum sits here for write durability |
| data-primary01 | LELI | data | ~50 TB HDD | Anchor copy of every chunk. *May be N nodes, not just one.* |
| client01 | LELI | client | — | Plex (read) |
| mgr04 | TORO | manager | small | Local metadata reads |
| data-warm01 | TORO | data | ~1 TB SSD | Active set + read-ahead. Empty at bootstrap. |
| client02 | TORO | client | — | Plex (read) |
| mgr05 | NRB | manager | small | Local metadata reads |
| data-warm02 | NRB | data | ~1 TB HDD | Fresh downloads, auto-replicates to LELI |
| client03 | NRB | client | — | Download landing (write) |

A node may host multiple roles (e.g. `toro-plex` runs
`mgr04 + data-warm01 + client02 + Plex` on one box). Memory budgets must
therefore be **per-role** and **respect a host-wide ceiling** so DTFS
never starves a co-resident application.

### A.1.2 Workload characteristics

- **Plex playback**: up to 60 Mbps per stream, plan for 5 concurrent
  streams per Plex instance ⇒ ~300 Mbps egress at TORO/LELI peak.
- **Reads dominate**: most chunks read once or twice ever; a small
  hot set rolls every few weeks as new media arrives.
- **Writes are bursty**: a torrent finishes on NRB and lands a 30 GB
  season in minutes; the cluster must absorb that without OOM-killing
  anything.
- **Anchor invariant**: every chunk MUST have at least one replica
  on LELI's `data-primary01`. Tiering / migration may freely move
  *other* replicas, but the LELI copy is sacred.

### A.1.3 Non-goals (for now)

- Multi-tenant isolation. One cluster, one user, one Plex household.
- POSIX completeness. We target Plex's filesystem-access patterns
  (FUSE read + occasional write); arcane corners (xattrs, mmap,
  open-unlink races) are best-effort.
- Hot-rebalancing across cluster reconfigurations. We support node
  add/remove via drain-then-replace, not online resize.

---

## A.2 Architecture (one-screen orientation)

### A.2.1 Components

- **`dtfs-manager`** — Raft-managed metadata service. Each instance is
  both a Raft peer and a REST/gRPC frontend. Manages: file metadata,
  chunk locations, node registry, volume configs, write leases, scrub
  schedules, alerts, migration tasks.
- **`dtfs-data`** — Data plane. Stores chunks on local disks (per-tier:
  hot / warm / cold / tape). Streams `put_chunk` / `get_chunk` over
  gRPC. Reports per-disk usage + SMART to manager.
- **`dtfs-fuse`** — FUSE client. Mounts the cluster namespace at
  `/mnt/dtfs`. Talks gRPC to managers (metadata) and to data nodes
  (chunk I/O). Local disk cache for read-ahead + viewer-aware atime.
- **`dtfs-client`** — Shared client crate used by both `dtfs-fuse` and
  `dtfs-cli`.
- **`dtfs-raft`** — openraft-based state machine. The Raft log is the
  source of truth; all metadata mutations go through it.
- **`dtfs-common`** — Shared types (DiskTier, VolumeConfig,
  MemoryConfig, …) and helpers (BLAKE3 checksums, hash ring, version
  string from `build.rs`).
- **`dtfs-proto`** — Protobuf / tonic codegen.

### A.2.2 Data flow on a write

```
FUSE write()
   ├── acquire write_lease (manager, Raft)
   ├── allocate_chunks (manager, Raft) → returns target node addrs
   ├── for each chunk:
   │    ├── stream gRPC PutChunkRequest pieces to data node
   │    │     (data node streams to a tmp file on the target disk —
   │    │      see Bug I; no full-chunk Vec in memory)
   │    └── commit_chunk_from_tmp (rename + meta + WAL clear)
   └── commit_chunk_locations (manager, Raft) → durable file entry
```

### A.2.3 Data flow on a read

```
FUSE read()
   ├── lookup metadata (manager, possibly cached for ENTRY_TTL)
   ├── pick nearest healthy replica (location.rs distance heuristic)
   ├── stream gRPC GetChunk → returns bytes
   ├── optionally cache in disk-backed local cache (P0-11)
   └── readahead engine (folder-sequential or file-sequential) fans
       out background prefetches
```

---

## A.3 Conventions — Do Not Break

### A.3.1 Raft state-machine snapshot compatibility

`StateMachineData` is serialised to disk on snapshot. **Every new field
MUST carry `#[serde(default)]`** so old snapshots load cleanly. Add a
focused test under `state_machine::posix_path_regression` or
`snapshot_persistence_tests` whenever you add a field.

### A.3.2 Memory discipline

- **No `Vec<u8>` accumulation of full chunks** anywhere on the hot
  path. Stream to disk, hash incrementally. See Bug I.
- **Best-effort memory caps**, *never* request-rejection by default
  (Bug G's `RESOURCE_EXHAUSTED` rejection is a last-resort safety net,
  off unless the operator explicitly arms it).
- **Per-role config**: `[memory]` lives in each of `manager.toml`,
  `data.toml`, `fuse.toml` with its own `hard_cap_mb`.
- See § A.6 for the canonical memory model.

### A.3.3 Data integrity is the prime directive

Every code path that *touches bytes* must:

1. **Compute or carry a BLAKE3 checksum** end-to-end.
2. **Write to tmp + atomic rename** for any new chunk on disk
   (`store::commit_chunk_from_tmp`).
3. **WAL the operation before performing it**, clear the WAL only after
   fsync. Recovery scans WAL on startup.
4. **Never delete a chunk** until either (a) a replacement replica is
   verified by the receiving data node OR (b) the chunk's file has been
   unlinked at the manager AND policy allows it.
5. **Never silently downgrade replication factor**. If we can't place
   RF=N replicas, queue migration tasks instead of accepting the write
   at RF<N.
6. **Verify on every read AND on scrub** (Bug BC-8). Default
   `verify_on_read = true` makes the data node BLAKE3-hash each
   chunk before serving; mismatch returns `Status::data_loss` so
   the FUSE client falls over to a healthy replica (P0-2). A
   detected fault on this node is also pushed into the WebUI
   `/ui/integrity` feed (BC-12e). Disabling `verify_on_read`
   requires an explicit acknowledgement at the API boundary
   (BC-8 ack guard); the false branch is the only DTFS-supported
   mode where reads can deliver corrupt bytes. Scrub still runs
   (quick + deep + orphan) to catch chunks that haven't been
   read in a long time.

See § A.7 for the senior-dev integrity audit checklist.

### A.3.4 Mandatory vs cache replicas (Bug AV invariant)

Replica creation has two independent classes, and **only the
under-replication scheduler is allowed to maintain the mandatory
class**:

- `mandatory_replicas.<tier>` are protected. The under-replication
  scheduler enforces the configured count and refuses to drop below it
  (`scheduler::check_under_replicated`).
- `cache_replicas.<tier>.{min,max}` are demand-driven. **The tiering
  engine is the *only* component allowed to create cache replicas, and
  only when the configured promotion threshold has fired**
  (`promote_reads` / `promote_window_secs`). The under-replication
  scheduler MUST NOT attempt to backfill cache replicas — they are
  evictable by design.
- The cache-flush planner (`scheduler::plan_flush_cache`) refuses any
  flush that would strand the mandatory copy, emitting a
  `cache.flush_refused` audit row + an `AnchorViolated` alert. The
  async dispatcher path additionally records `cache.flush_replica` per
  successful drop and `cache.flush_complete` per call (unconditional).
- **Every cache promotion and demotion must check
  `would_violate_anchor` against the destination state.** The AV-2-4
  planner already filters demotions where the mandatory copy wouldn't
  survive; the apply path (`RemoveChunkLocation` with `enforce_anchor`)
  re-checks as defence-in-depth. Don't add a new code path that
  schedules replication or removal without consulting one of these
  checks.
- **Engine-driven demote ≠ operator flush.** The asymmetry is
  intentional: idle-decay demote clears the cooldown (next read
  re-promotes immediately); operator flush sets a future cooldown (no
  immediate re-promote). The
  `should_clear_cooldown_on_demote(cooldown_until_ms, now_ms)` helper
  is the boundary — past/absent → clear; future → preserve. Any new
  demote-like path must route through that helper rather than
  unconditionally calling `SetChunkCooldown(0)`.
- **A mandatory replica may co-locate on a node that already holds a
  cache replica of the same chunk (different tier).** On a topology
  with no spare node in the anchor DC, the cold anchor copy *must* land
  on a data node that already holds the warm cache copy. Replica
  presence is therefore keyed by `(node_id, disk_id)` — equivalently
  `(node, tier)` — never by `node_id` alone (Bug BH:
  `scheduler::replica_already_present`). A node-only dedup silently
  drops the second-tier mandatory entry and the anchor-lag repair loops
  forever. The two replicas remain distinct classes (mandatory cold +
  cache warm) on the one node.

When you add a new code path that creates replicas, decide which class
it produces and make the boundary explicit in the function name or doc
comment. Mixing the two classes in one creation site is a regression
risk against the AV invariant.

### A.3.5 The bug catalog (`SOAK-RECOVERY-2026-05-18.md`)

Bugs A–J + the chunk-size default + Bug I (streaming put_chunk) are
documented there with: symptom → root cause → fix → test name → live
verification. **Read this before touching FUSE adapter cookies,
list_directory aggregation, lease lifecycle, or put_chunk buffering** —
those areas have subtle interactions and the doc captures the lessons
learned.

---

## A.4 Build / Test / Deploy mechanics

### A.4.1 Build

The full workspace builds on **Linux** (or **WSL**). The Windows host
cannot build because of `rocksdb` and `fuse3`. The `dtfs-builder` VM in
the lab (`10.20.0.150` historically) hosts the build chain.

```bash
# On dtfs-builder, with cargo on PATH:
cd /home/debian/dtfs2
cargo build --release --workspace
cargo test  --release --workspace
```

Per-crate test:

```bash
cargo test -p dtfs-data --release
```

A `#![allow(dead_code)]` crate-level attribute on the `dtfs-manager`
and `dtfs-data` binaries works around a rustc 1.94 ICE in dead-code
analysis. **Do not remove those allows** without first verifying the
ICE no longer reproduces on the current toolchain.

### A.4.2 Rolling deploy

```bash
# After cargo build --release on the builder:
for h in mgr-01 mgr-02 mgr-03; do
  scp target/release/dtfs-manager debian@$h:/tmp/dtfs-manager.new
  ssh debian@$h 'sudo install -m 755 /tmp/dtfs-manager.new /usr/local/bin/dtfs-manager && sudo systemctl restart dtfs-manager'
done
```

Roll managers leader-last, then data nodes, then FUSE clients.

### A.4.3 Lab access

The Proxmox lab is on `10.20.0.0/24`. PVE host root: ssh
`root@10.20.0.121` (password-only — store via `sshpass` only on the
operator's WSL home, never in a committed file). VM SSH key:
`~/.ssh/dtfs-lab/id_ed25519`. Bandwidth-shaping role lives in
`tests/proxmox/ansible/roles/dtfs-netshape/`.

---

## A.5 Memory Model

DTFS targets **deterministic, bounded memory** because operators run it
on co-resident hosts (toro-plex etc.). The model is best-effort: under
pathological load we slow down, we never OOM-kill.

### A.5.1 Per-process knobs

Every `*.toml` carries an optional `[memory]` block:

```toml
[memory]
hard_cap_mb         = 1500   # 0 = unbounded
warn_threshold_pct  = 75
shed_threshold_pct  = 88     # Last-resort: reject new writes (Bug G)
sample_interval_secs = 3
```

`hard_cap_mb = 0` preserves backward compatibility with older configs.
**Default in new deployments is non-zero** (1.5 GB on 3-GB-RAM data
nodes, scaled accordingly elsewhere).

### A.5.2 Strategy ladder (cheapest first)

When RSS approaches the cap, components apply these strategies in
order — the goal is to STAY under the cap WITHOUT rejecting requests:

1. **Stream to disk, never buffer whole chunks** (Bug I, already
   implemented for `put_chunk`).
2. **Shrink the ARC cache** (data node) / FUSE local cache (client).
   Most caches are pure size-LRU; evict to bring RSS down.
3. **Drop readahead-prefetched chunks** that haven't been consumed.
4. **Pause background tasks**: scrub, tiering, metric collection.
5. **Throttle write acceptance** via TCP backpressure — slow the gRPC
   stream consumption so the kernel applies flow control to the sender.
6. **(Last resort, opt-in)** Return `RESOURCE_EXHAUSTED` to new
   `put_chunk` until RSS drops. Client retries with backoff (Bug G's
   retry policy). This step is disabled by default; arm it only on
   hosts where step 5 is insufficient.

This mirrors Ceph's OSD memory autotuner, which prefers shrinking
caches over rejecting OPs.

### A.5.3 Composite hosts

A host running `mgr04 + data-warm01 + client02 + Plex` (toro-plex) must:

- Set `hard_cap_mb` separately on each role.
- Total ≤ (host RAM − Plex headroom − OS overhead). Operator's
  responsibility, but `tests/proxmox/abuse/14-composite-host-budget.sh`
  validates the sum at deploy time.

---

## A.6 Data Integrity Audit Checklist

Run through this whenever you touch a write / migrate / delete / scrub
path. Mark each item as "Verified by <test name>" in the PR.

1. ☐ Caller computes BLAKE3 via `IncrementalHasher`; final hash is
     stored in `ChunkMeta.checksum`.
2. ☐ On-disk `*.dat` is written to `tmp/<uuid>.dat.tmp` and **atomic
     renamed** to `chunks/<aa>/<bb>/<chunk_id>.dat` only after
     `sync_all()`.
3. ☐ `*.meta` JSON is written with `sync_all()` before the WAL entry
     for the put is cleared.
4. ☐ Counter updates (`bytes_stored`, `chunk_count`) happen **after**
     the meta is durable, so a crash after data-on-disk but before
     meta-on-disk leaves a tmp+wal residue that the rescan path
     reconciles on next boot.
5. ☐ `commit_chunk_from_tmp` removes the tmp on **any** error path so
     a half-written chunk doesn't leak disk.
6. ☐ Migration (`scheduler::ReplicationTask`) only marks a task `Done`
     after `VerifyChunk` on the target succeeds. The source is **never**
     removed by the scheduler — only an explicit `tier-demote` or
     `unlink` deletes a chunk.
7. ☐ Scrub computes BLAKE3 on the **disk file**, compares to
     `chunk.meta.checksum`. Mismatch → mark `chunks_corrupt`, fire
     `ScrubCorruption` alert. Auto-repair fetches from another replica
     via `replicate_chunk`.
8. ☐ `unlink` removes the manager-side `FileMetadata` first, then the
     chunks are deleted async by the GC scheduler. Crash between
     metadata-removal and chunk-deletion leaves orphan chunks, not
     dangling pointers — orphans get reclaimed by the next scrub pass.
9. ☐ `rename` is a single Raft command (atomic from observers'
     perspective). Open handles to the old name continue to read the
     same chunks (the FileMetadata's `path` field is updated but
     chunk_id binding is unchanged).
10. ☐ Reads use replica-fallback (P0-2): if the first replica errors,
     try the next; only fail the read after all candidates fail.

---

## A.7 Bandwidth + Network Shaping

Lab shaping lives in `tests/proxmox/ansible/roles/dtfs-netshape/`. Each
DC class is enforced via `tc qdisc` + IFB on the VM:

| Class | Up | Down | Jitter |
|-------|----|------|--------|
| LELI  | 40 Mbps | 40 Mbps | 25 ms ±5 ms |
| TORO  | 400 Mbps | 400 Mbps | 5 ms ±1 ms |
| NRB   | 1 Gbps | 1 Gbps | 1 ms ±0.5 ms |

Apply pre-soak via `apply-shaping.yml`, revert via `revert-shaping.yml`.
**SSH/22, manager gRPC/9401, manager API/9402, data gRPC/9500, NTP/123
are exempt** from shaping (otherwise Raft heartbeats fail under jitter
and the cluster can't elect a leader).

---

## A.8 File-by-file map (for orientation)

- `crates/dtfs-raft/src/state_machine.rs` — `apply_request` is the
  authoritative state-mutation point. Every metadata change goes
  through here.
- `crates/dtfs-raft/src/lib.rs::ClientRequest` — the application
  command enum. Adding a new mutation = new variant here + new arm in
  `apply_request`.
- `crates/dtfs-manager/src/scheduler.rs` — migration / re-replication
  background loop. `schedule_path_migration` is the public entry point
  for operator-driven moves. Bug AV adds two more:
  `flush_cache_for_path` (operator drops cache replicas under a prefix;
  pure planner `plan_flush_cache` + dispatcher that emits
  `cache.flush_replica` / `cache.flush_refused` / `cache.flush_complete`
  audit + an `AnchorViolated` alert on refusals), and
  `check_under_replicated` (now reads `effective_mandatory_replicas()`
  from the volume — see § A.3.4).
- `crates/dtfs-manager/src/metadata.rs` — gRPC service implementing the
  manager RPCs. Thin wrappers over `raft_write(ClientRequest::*)`.
- `crates/dtfs-manager/src/api.rs` — REST endpoints (consumed by WebUI
  + CLI). `get_stats` is the WebUI home aggregator.
- `crates/dtfs-manager/src/webui.rs` — embedded SPA. Format-string
  HTML; `{{` / `}}` for literal braces.
- `crates/dtfs-data/src/server.rs::put_chunk` — streaming put path.
  Read this before touching the write hot loop.
- `crates/dtfs-data/src/store.rs::commit_chunk_from_tmp` — atomic
  commit. Read § A.6.
- `crates/dtfs-data/src/memory.rs` — Bug G memory budget + sampler.
- `crates/dtfs-fuse/src/filesystem.rs` — FUSE↔cluster bridge. Write
  lease handling, readahead engine wiring, local disk cache.
- `crates/dtfs-fuse/src/fuse_adapter.rs` — fuse3 trait implementation.
  **Cookie scheme** for readdir/readdirplus lives at
  `dir_entry_cookie()` — DO NOT diverge the two paths (Bug F).
- `crates/dtfs-common/build.rs` + `crates/dtfs-common/src/version.rs` —
  Bug AY build-time `VERSION` (currently `v0.0.0-dev`) and
  `BUILD_DATE` (UTC `YYYYMMDD-HHmmss`). Every binary reports these to
  the manager via `RegisterNode`.
- `plans/active/` — in-flight plans. See
  [`plans/README.md`](./plans/README.md).
- `tests/proxmox/abuse/` — shell regression tests (numbered).

---

# Part B — Operating Discipline

The rules below apply to every contributor (human or AI) for any
non-trivial change — a new feature, a multi-bug fix wave, a refactor
that touches more than ~3 files, anything spanning >1 working day. For
trivial work (one-line fixes, single doc edits) the discipline is
overkill; apply judgement and just do the thing.

If a tool's default behaviour conflicts with a rule in this file, this
file wins. If the user issues an instruction in the current session
that conflicts with a rule, the user wins — but flag the conflict so
AGENTS.md can be updated (see § B.9) rather than silently drifting.
AI-generated commits that bypass the fixing procedure or the
plan-driven workflow will be reverted.

DTFS is **primarily authored by Claude Opus**. Any AI agent — and any
human using one — contributing code or documentation must follow the
[Fixing Procedure](#b1-fixing-procedure) for every bug fix, the
[plan-driven workflow](#b2-plan-driven-workflow) for every non-trivial
change, the [commit discipline](#b4-commit-discipline) including the
`Co-Authored-By:` trailer naming the model used, and the
[test discipline](#b5-test-discipline).

---

## B.1 Fixing Procedure

Every bug fix — no matter how small — follows this three-step procedure.
It is the contract every contributor agrees to when working on DTFS,
and it is referenced from [README.md](./README.md#contribution).

1. **Write a test that fails because of the issue.** Put the test in
   the purest layer that demonstrates the bug — see § B.5 for where
   each kind of test lives. If the surface only manifests on a live
   cluster, the failing "test" is a shell regression script under
   `tests/proxmox/abuse/NN-name.sh`. Either way, **run it and observe
   the failure** before writing any fix code.
2. **Implement the fix.** Make the smallest change that turns the
   failing test green; do not bundle unrelated cleanup or scope creep.
3. **Verify the fix works.** Run the new test (must now pass), the
   surrounding crate's test suite (no regressions), and — if the bug is
   operational — live-verify on the lab cluster. Quote the
   failing-then-passing run in the commit body.

Skipping any step makes the fix unverifiable; landing it anyway is a
contract violation.

### B.1.1 When you encounter a bug

1. Reproduce on the lab (or pure unit test if the bug surface allows).
2. Check `plans/active/` for any open plan that already covers the bug.
   If found, add a task to that plan rather than creating a new one.
3. Pick a new bug letter (continue alphabetically from the highest plan
   ID visible in `plans/active/`).
4. Create `plans/active/PLAN-BUG-<letter>-<name>.md` following the
   template in § B.2.
5. Follow the three-step procedure above.

---

## B.2 Plan-driven workflow

Before writing code for a non-trivial change, **write a plan document**.
**All plans live under `plans/`** — this is the canonical location and
is mandatory for new plans:

- `plans/active/PLAN-<id>-<short-name>.md` — in-progress plans.
- `plans/completed/<completion-date>-PLAN-<id>-<short-name>.md` —
  finished plans; git-tracked (committed) but excluded from AI-agent
  context via `.claude/settings.json` deny rules.
- `plans/processed/` — processed reports, issue scans, and
  post-mortems; git-tracked but agent-excluded the same way. Move here
  once findings are captured in active plans. Not read by agents.

See [`plans/README.md`](./plans/README.md) for the directory's naming
and lifecycle rules. **Do not put plans, reports, or post-mortems under
`tests/` again** — that tree is for test infrastructure only (`abuse/`,
`ansible/`, `soak/`, inventories, provisioning scripts).

The plan **must** include:

- **Goals + non-goals** so scope is explicit.
- **Locked-in decisions** from the user (so back-and-forth in chat is
  captured durably).
- **Data-model changes** with concrete shapes — struct fields, proto
  messages, Raft variants, config keys.
- **Task list** with stable IDs (e.g. `<plan-id>-<phase>-<n>`). Each
  task lists:
  - **Files** to touch
  - **Implementation sketch** (not full code — just enough to commit
    to)
  - **Tests** required (unit names + live-verification check)
  - **Done when** — concrete acceptance criteria, not vague
  - **Status** marker (⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked
    · 🟨 needs review)
- **Cross-cutting invariants** that every task must preserve.
- **Verification log** + **Deviations** sections — empty at plan
  authorship, filled in during work.

The plan **stays the source of truth**. When reality diverges from the
plan, update the plan first (Deviations section), then continue the
code. A merge conflict between plan and code is a red flag the plan
was not updated as work progressed — block-and-fix immediately.

---

## B.3 Per-task procedure

For each ⬜ task in order:

1. Mark the task **🟦** in the plan.
2. **Implement** the smallest change that satisfies the listed tests +
   done-when. Do not add scope.
3. Run the relevant `cargo test -p <crate> --release` until green.
4. Sync to the build host (if remote), run the full crate test suite.
   No `--release` regressions allowed.
5. Deploy the affected binary to the verification environment (lab
   cluster, minicluster, integration host).
6. Run the **live verification** described in the task's "Done when"
   criteria.
7. Add a **verification log** entry to the plan: task ID, date,
   commands run, output snippet, anomalies.
8. **Commit** (see § B.4).
9. Mark the task **✅** with the completion date in the plan.
10. Move to next ⬜.

Steps 7-9 land **in the same commit** as the code change. The doc must
never drift from the code — that's the entire reason the plan exists.

---

## B.4 Commit discipline

### Format

```
<plan-id>-<phase>-<n>: <one-line summary, <72 chars>>

<2-3 sentence body explaining what changed, what tests cover it, any
deviation from the plan sketch. Reference the relevant
section/recovery doc.>

Plan: <relative-path-to-plan> (<plan-id>-<phase>-<n> ✅)

Co-Authored-By: <model-name> <noreply@<vendor>.com>
```

The `Plan:` trailer is mandatory for any commit produced under this
discipline so `git log --grep "Plan:"` filters the agent-driven work.
The `Co-Authored-By:` trailer is mandatory per repo convention.

### Granularity

- **One commit per task.** No batching across tasks.
- **Doc-only tasks** (aggregated docs for a phase) get their own
  commit.
- **Co-locate** docs + code: when a code change has a corresponding doc
  update, both go in the **same** commit. The discrete doc tasks
  aggregate cross-phase doc edits; per-task doc snippets ride along
  with the code commit.

### Hygiene

- **Never** `--no-verify`. Pre-commit hooks (when present) must pass.
  If a hook fails, investigate and fix; don't bypass.
- **Never** `--amend` a commit that has been pushed. Create a new
  commit instead.
- **Never** push to `master`/`main` mid-plan. Keep work on local until
  the user reviews each phase boundary. Feature branches at phase
  boundaries (`av-phase-1`, `av-phase-2`) are OK; rebase / squash only
  if explicitly requested.
- **Never** silently drop the `Co-Authored-By` trailer or bypass GPG
  signing if the repo requires it.

---

## B.5 Test discipline

Same rules as the [Fixing Procedure](#b1-fixing-procedure), expanded
for layer choice:

1. **Write the failing test first**, in the most-pure layer that
   demonstrates the bug. Preference order:
   - Pure helper / state-machine apply path (unit test, milliseconds).
   - Manager-side aggregator (unit test against `StateMachineData`).
   - FUSE-side helper (unit test in `dtfs-fuse`).
   - Integration / live cluster (shell script under
     `tests/proxmox/abuse/`).
2. **Run the test, observe it fail** with a meaningful assertion
   message (or, if the API doesn't exist yet, compile error). Quote the
   failure in the commit message.
3. **Implement the fix** as narrowly as possible.
4. **Run the test, observe it pass.** Run the full crate's tests
   (`cargo test -p <crate> --release`) to catch regressions.
5. **Live-verify** on the lab cluster if the fix is operational.

**If a bug doesn't have a Rust-unit-testable surface** (e.g. systemd
unit behaviour, fuse3 kernel cookie protocol), add a shell regression
test in `tests/proxmox/abuse/NN-name.sh` instead — see
`12-fuse-restart-after-crash.sh` for the template.

**No exceptions.** Compile errors as the "failing test" are acceptable.
Skipping the failing-first step makes the fix unverifiable.

### Where each kind of test lives

| Layer | Test home |
|---|---|
| Pure helper / state-machine apply | unit test in the same crate |
| Manager-side aggregator (against `StateMachineData`) | unit test in `dtfs-manager` |
| FUSE-side helper | unit test in `dtfs-fuse` |
| Cross-process / cluster behaviour | shell script under `tests/proxmox/abuse/NN-name.sh` |
| Release-gate end-to-end scenario | `tests/release/cases/NN-name.sh` (see § B.5.1) |
| New durability/anchor invariants | property-style test exercising many random op sequences |

The plan declares the tests up front. Each task's "Done when" requires
those tests green AND the live verification step.

### B.5.1 Release-suite registration is mandatory

Every new test that exercises **cluster-visible behaviour** — a new
API endpoint, a new operator action, a new WebUI flow that hits the
manager, a new failure / recovery path, a new invariant on
placement, integrity, scrub, quarantine, or routing — **MUST also
ship a corresponding case under `tests/release/cases/NN-name.sh`**
in the SAME commit as the unit or abuse test.

The release suite (`tests/release/run.sh`) is the single artefact
the operator runs prior to a release tag. Anything not in it is, by
definition, not gated by release. A regression test that lives only
in `tests/proxmox/abuse/` will get run when someone touches the
adjacent code; it will NOT run on release day. That's the gap this
rule closes.

Rules:

- **One case per scenario.** Numbered, sortable, named for the
  behaviour it verifies (`13-integrity-corruption.sh`, not
  `13-bug-bc-test.sh`).
- **Status protocol mandatory.** Last line of stdout is
  `STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug-id>]`. The harness
  parses this; without it the case shows as FAIL.
- **Cleanup mandatory.** Register `trap case_cleanup EXIT`; cluster
  state must be restorable so re-running the suite works without
  manual reset.
- **No operator interaction.** No prompts, no manual confirmation,
  no fixed wall-clock waits — use `wait_for` from
  `tests/release/lib/common.sh`.
- **KNOWN_ISSUE pins to a Bug ID.** A scenario that depends on
  unshipped functionality exits `KNOWN_ISSUE [BC]` (or similar);
  when BC closes, the case automatically promotes to PASS or FAIL
  on the next run without code changes.
- **Quick-subset opt-in.** If the case is cheap (≤ 2 min) and
  exercises a critical path, list its stem in `QUICK_CASES` inside
  `tests/release/run.sh`. Quick mode (`run.sh --quick`) targets
  ~15 min total.
- **Plan tasks reference the case.** A plan's "Done when" line
  cites the case file (`tests/release/cases/NN-name.sh`); a task
  cannot be marked ✅ until the case is committed AND its row in
  the most recent release-suite run is PASS or KNOWN_ISSUE pinned
  to a tracked follow-up.

If a unit/abuse test exists but no release case does, the PR
description must explain why the scenario does not warrant
release-suite coverage. "Internal-only refactor, no behaviour
change" is acceptable; "out of scope" is not.

See `tests/release/cases/` and `tests/release/lib/common.sh` for the
canonical harness design + case-authoring template.

---

## B.6 Structure Discipline

Where a file lives is part of its meaning. The repository layout
encodes who reads each artefact, when, and against which acceptance
bar. Drift on this is corrosive — it is why work in flight ends up
indistinguishable from a fix landed six months ago. These rules are
mandatory:

| Artefact | Canonical location | Notes |
|---|---|---|
| In-flight work plan | `plans/active/PLAN-<id>-<name>.md` | One per non-trivial change. See § B.2. |
| Finished plan | `plans/completed/<YYYY-MM-DD>-PLAN-<id>-<name>.md` | Move when every task is ✅ verified. Git-tracked; excluded from AI agents via `.claude/settings.json` deny rules. |
| Post-mortem, report, issue scan (processed) | `plans/processed/` | Git-tracked; agent-excluded the same way. Move here once findings are captured in active plans. Not read by agents. |
| Release note (bug fix, new feature, known issue, breaking change, special update requirement) | `changelog/UNRELEASED.md` | One entry per user-visible change, **same commit** as the code. Internal restructures, doc edits, refactors, and test/CI tweaks do **not** go here. See [`changelog/README.md`](./changelog/README.md). |
| Architecture / config / ops reference | `docs/src/...` (mdBook) | Update in the same commit as behaviour changes (§ A.8 + § B.9). |
| Code | `crates/<crate>/src/...` | New crates require a `Cargo.toml` workspace member entry. |
| Protobuf | `proto/<file>.proto` | Rebuild `dtfs-proto` after every edit. |
| Unit tests | inline `#[cfg(test)]` modules | Pure helpers, state-machine apply paths, aggregators. |
| Integration / shell tests | `tests/integration/`, `tests/proxmox/abuse/NN-name.sh` | Cluster-level behaviour, numbered. |
| Release-gate end-to-end suite | `tests/release/cases/NN-name.sh` + `tests/release/lib/` + `tests/release/run.sh` | Single entry point operator runs prior to a release. New tests MUST register here per § B.5.1. |
| Test inventories / provisioning scripts | `tests/proxmox/{ansible,soak,*.json,*.sh,*.ps1}` | Test infrastructure. |
| Operator scripts | `scripts/` | Repeatable operator-facing tooling. |
| Deployment surface | `deploy/` | Docker Compose, sample configs, systemd units. |
| Canonical agent guide (knowledge + discipline) | `AGENTS.md` (this file) | Cross-tool source of truth. |
| Claude Code shim | `CLAUDE.md` | Single `@AGENTS.md` import. Do not add content here — edit AGENTS.md instead. |

What **does not** belong anywhere except these paths:

- A new plan placed under `tests/proxmox/PLAN-*.md` or similar. That
  layout is historical; all new plans go to `plans/active/`.
- A "Project status / PROGRESS" file checked in at the repo root.
  `git log`, the changelog, and the plans are the status sources.
  Historical snapshot reports belong in `plans/processed/` once
  processed.
- Loose `*.md` notes scattered through `crates/` or `tests/`. If the
  note describes work, it belongs in `plans/`; if it describes a
  user-visible change, in `changelog/`; if it documents a subsystem,
  in `docs/`.
- A new top-level directory invented to host an artefact that already
  has a home in the table above.

Edge cases:

- **A doc that is both a plan *and* a post-mortem** (a plan that morphed
  into a recovery write-up): keep it in `plans/active/` while the work
  is in flight, then move it to `plans/processed/` on completion.
- **A code comment that points to a plan**: use the full
  `plans/active/...` path. Update in the same commit that moves the
  file (`git grep` is the tool of last resort, not first).
- **A new artefact type that doesn't fit the table**: update this
  table in the same commit that introduces the artefact. Adding a row
  is cheaper than letting a one-off live somewhere ambiguous.

When in doubt, prefer `plans/processed/` for historical artefacts and
`docs/` for forward-looking reference. Never invent a new top-level
directory without first updating this table.

---

## B.7 Plan-as-living-document

- When the implementation deviates from the plan (different test name,
  additional file touched, missed edge case): **update the plan
  first**, under the `Deviations from plan` section. Then continue.
- When a task's "Done when" criteria need to change mid-implementation
  (a missing dependency surfaces, an environment constraint forces a
  different approach), **update the plan, get user acknowledgement,
  then proceed**. Don't silently drop or expand a task.
- **Plan changes that materially alter the design** (new data field,
  new task, dropped task) require user review **before** the change is
  committed.

A plan that ends in a state matching reality is the artefact future
agents and operators rely on. A plan that diverged silently is worse
than no plan.

---

## B.8 Worked example

For Bug AV (the in-flight feature at time of writing), the discipline
produces:

```
plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md  (the plan)
  ├─ § 0 Background, decisions
  ├─ § 1 Data-model schema
  ├─ § 2 Phase plan + 24 tasks (AV-1-1 .. AV-3-6)
  ├─ § 4 Verification log
  ├─ § 5 Deviations from plan
  └─ Appendix A — 7 worked TOML examples

Commits (mid-implementation, illustrative):
  AV-1-1: CacheTierConfig + AnchorPolicy types
  AV-1-2: VolumeConfig + FileMetadata extension
  ...
  AV-1-12: docs/schema + flush API
  AV-2-1: per-DC rolling read-window store
  ...
```

Every commit's `Plan:` trailer points back to the plan, every plan
task carries the matching commit's status flip + verification-log
entry. `git log --grep "AV-1-"` reconstructs the Phase 1 history in
order.

---

## B.9 Updating this file

This document is canonical. **When you change behaviour that affects
operators, AI agents, or the data plane semantics, update AGENTS.md in
the SAME commit as the code change.** Stale guidance is worse than no
guidance.

Specifically, update:

- § A.1 when the target deployment topology changes.
- § A.3 when conventions change (snapshot compat, memory rules,
  integrity invariants).
- § A.5 when memory strategy changes.
- § A.6 when a new invariant is added to the integrity contract.
- § A.8 when a major file's responsibility changes or a new crate is
  added.
- § B.4 hygiene when commit conventions change (e.g. "always run
  `cargo clippy` before commit").
- § B.6 when a new doc surface gets added or an artefact moves to a
  different canonical location.
- § B.1 / § B.2 / § B.3 when the workflow shape itself changes.

PR descriptions for non-trivial work should explicitly note
"AGENTS.md updated" or "no AGENTS.md change needed because …".

---

## B.10 Cross-reference

- **[README.md](./README.md)** — project overview, deployment warning,
  contribution rules, AI-use policy. Points back here for the
  procedural details.
- **`plans/active/PLAN-<id>-*.md`** — feature-specific plan documents
  currently in flight. One per non-trivial work item.
- **`plans/completed/`** — finished plans; git-tracked, excluded from
  AI agents via `.claude/settings.json` deny rules.
- **`plans/processed/`** — processed reports, issue scans, and
  post-mortems; git-tracked, agent-excluded the same way. Not read by
  agents.
- **`changelog/UNRELEASED.md`** — accumulator for the next release. Add
  a one-line entry per user-visible change in the same commit; see
  [`changelog/README.md`](./changelog/README.md).
- **`CLAUDE.md`** — Claude Code shim. One line: `@AGENTS.md`. Do not
  put content here.

Active plans and the changelog are read together by AI agents starting
work on a non-trivial change. The user is the final arbiter when
AGENTS.md and the plan conflict; the plan is the arbiter when the plan
and the code conflict.
