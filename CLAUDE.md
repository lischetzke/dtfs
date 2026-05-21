# CLAUDE.md — Operating Guide for AI Coding Agents on DTFS

You are working on **DTFS**, a Raft-managed tiered distributed filesystem
written in Rust. Read this file fully before any non-trivial work.
This document is the canonical contract: keep it accurate and update it
whenever you change behaviour, conventions, or the target workload.

---

## 1. Mission & Target Workload

DTFS exists to serve **a single household-scale Plex media library
across multiple datacenters with asymmetric bandwidth**. Every design
decision is anchored to this scenario.

### 1.1 Reference deployment — "Plexmedia"

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

A node may host multiple roles (e.g. `toro-plex` runs `mgr04 + data-warm01 + client02 + Plex` on one box). Memory budgets must therefore
be **per-role** and **respect a host-wide ceiling** so DTFS never
starves a co-resident application.

### 1.2 Workload characteristics

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

### 1.3 Non-goals (for now)

- Multi-tenant isolation. One cluster, one user, one Plex household.
- POSIX completeness. We target Plex's filesystem-access patterns
  (FUSE read + occasional write); arcane corners (xattrs, mmap,
  open-unlink races) are best-effort.
- Hot-rebalancing across cluster reconfigurations. We support node
  add/remove via drain-then-replace, not online resize.

---

## 2. Architecture (one-screen orientation)

### 2.1 Components

- **`dtfs-manager`** — Raft-managed metadata service. Each instance
  is both a Raft peer and a REST/gRPC frontend. Manages: file
  metadata, chunk locations, node registry, volume configs, write
  leases, scrub schedules, alerts, migration tasks.
- **`dtfs-data`** — Data plane. Stores chunks on local disks
  (per-tier: hot / warm / cold / tape). Streams `put_chunk` /
  `get_chunk` over gRPC. Reports per-disk usage + SMART to manager.
- **`dtfs-fuse`** — FUSE client. Mounts the cluster namespace at
  `/mnt/dtfs`. Talks gRPC to managers (metadata) and to data nodes
  (chunk I/O). Local disk cache for read-ahead + viewer-aware atime.
- **`dtfs-client`** — Shared client crate used by both `dtfs-fuse`
  and `dtfs-cli`.
- **`dtfs-raft`** — openraft-based state machine. The Raft log is the
  source of truth; all metadata mutations go through it.
- **`dtfs-common`** — Shared types (DiskTier, VolumeConfig,
  MemoryConfig, …) and helpers (BLAKE3 checksums, hash ring).
- **`dtfs-proto`** — Protobuf / tonic codegen.

### 2.2 Data flow on a write

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

### 2.3 Data flow on a read

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

## 3. Conventions — Do Not Break

### 3.1 Raft state-machine snapshot compatibility

`StateMachineData` is serialised to disk on snapshot. **Every new
field MUST carry `#[serde(default)]`** so old snapshots load cleanly.
Add a focused test under `state_machine::posix_path_regression` or
`snapshot_persistence_tests` whenever you add a field.

### 3.2 Memory discipline

- **No `Vec<u8>` accumulation of full chunks** anywhere on the hot
  path. Stream to disk, hash incrementally. See Bug I.
- **Best-effort memory caps**, *never* request-rejection by default
  (Bug G's `RESOURCE_EXHAUSTED` rejection is a last-resort safety
  net, off unless the operator explicitly arms it).
- **Per-role config**: `[memory]` lives in each of `manager.toml`,
  `data.toml`, `fuse.toml` with its own `hard_cap_mb`.
- See § 6 for the canonical memory model.

### 3.3 Data integrity is the prime directive

Every code path that *touches bytes* must:

1. **Compute or carry a BLAKE3 checksum** end-to-end.
2. **Write to tmp + atomic rename** for any new chunk on disk
   (`store::commit_chunk_from_tmp`).
3. **WAL the operation before performing it**, clear the WAL only
   after fsync. Recovery scans WAL on startup.
4. **Never delete a chunk** until either (a) a replacement replica
   is verified by the receiving data node OR (b) the chunk's file
   has been unlinked at the manager AND policy allows it.
5. **Never silently downgrade replication factor**. If we can't
   place RF=N replicas, queue migration tasks instead of accepting
   the write at RF<N.
6. **Verify on scrub**, not on every read. Reads trust the disk;
   scrub catches bit-rot.

See § 7 for the senior-dev integrity audit checklist.

### 3.4 The bug catalog (`SOAK-RECOVERY-2026-05-18.md`)

Bugs A–J + the chunk-size default + Bug I (streaming put_chunk) are
documented there with: symptom → root cause → fix → test name →
live verification. **Read this before touching FUSE adapter cookies,
list_directory aggregation, lease lifecycle, or put_chunk
buffering** — those areas have subtle interactions and the doc
captures the lessons learned.

---

## 4. Test-First Workflow (NON-NEGOTIABLE)

When you fix or change behaviour, follow this exact order:

1. **Write the failing test first**, in the most-pure layer that
   demonstrates the bug. Preference order:
   - Pure helper / state-machine apply path (unit test, milliseconds).
   - Manager-side aggregator (unit test against `StateMachineData`).
   - FUSE-side helper (unit test in `dtfs-fuse`).
   - Integration / live cluster (shell script under
     `tests/proxmox/abuse/`).
2. **Run the test, observe it fail** with a meaningful assertion
   message (or, if the API doesn't exist yet, compile error). Quote
   the failure in the commit message.
3. **Implement the fix** as narrowly as possible.
4. **Run the test, observe it pass.** Run the full crate's tests
   (`cargo test -p <crate> --release`) to catch regressions.
5. **Live-verify** on the lab cluster if the fix is operational.
6. **Commit** with: the bug ID (A–J style or a new letter), what
   symptom → cause → fix, the test name, and one live-verification
   snippet.

**If a bug doesn't have a Rust-unit-testable surface** (e.g. systemd
unit behaviour, fuse3 kernel cookie protocol), add a shell regression
test in `tests/proxmox/abuse/NN-name.sh` instead — see
`12-fuse-restart-after-crash.sh` for the template.

**No exceptions.** Compile errors as the "failing test" are
acceptable. Skipping the failing-first step makes the fix
unverifiable.

---

## 5. Build / Test / Deploy mechanics

### 5.1 Build

The full workspace builds on **Linux** (or **WSL**). The Windows host
cannot build because of `rocksdb` and `fuse3`. The `dtfs-builder` VM
in the lab (`10.150.0.150` historically) hosts the build chain.

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
and `dtfs-data` binaries works around an rustc 1.94 ICE — see
`ISSUES_TO_FIX.md`. **Do not remove those allows** without first
verifying the ICE no longer reproduces on the current toolchain.

### 5.2 Rolling deploy

```bash
# After cargo build --release on the builder:
for h in mgr-01 mgr-02 mgr-03; do
  scp target/release/dtfs-manager debian@$h:/tmp/dtfs-manager.new
  ssh debian@$h 'sudo install -m 755 /tmp/dtfs-manager.new /usr/local/bin/dtfs-manager && sudo systemctl restart dtfs-manager'
done
```

Roll managers leader-last, then data nodes, then FUSE clients.

### 5.3 Lab access

The Proxmox lab is on `10.150.0.0/24`. PVE host root: ssh
`root@10.150.0.121` (password-only — store via `sshpass` only on the
operator's WSL home, never in a committed file). VM SSH key:
`~/.ssh/dtfs-lab/id_ed25519`. Bandwidth-shaping role lives in
`tests/proxmox/ansible/roles/dtfs-netshape/`.

---

## 6. Memory Model

DTFS targets **deterministic, bounded memory** because operators run
it on co-resident hosts (toro-plex etc.). The model is best-effort:
under pathological load we slow down, we never OOM-kill.

### 6.1 Per-process knobs

Every `*.toml` carries an optional `[memory]` block:

```toml
[memory]
hard_cap_mb         = 1500   # 0 = unbounded
warn_threshold_pct  = 75
shed_threshold_pct  = 88     # Last-resort: reject new writes (Bug G)
sample_interval_secs = 3
```

`hard_cap_mb = 0` preserves backward compatibility with older
configs. **Default in new deployments is non-zero** (1.5 GB on
3-GB-RAM data nodes, scaled accordingly elsewhere).

### 6.2 Strategy ladder (cheapest first)

When RSS approaches the cap, components apply these strategies in
order — the goal is to STAY under the cap WITHOUT rejecting requests:

1. **Stream to disk, never buffer whole chunks** (Bug I, already
   implemented for `put_chunk`).
2. **Shrink the ARC cache** (data node) / FUSE local cache (client).
   Most caches are pure size-LRU; evict to bring RSS down.
3. **Drop readahead-prefetched chunks** that haven't been consumed.
4. **Pause background tasks**: scrub, tiering, metric collection.
5. **Throttle write acceptance** via TCP backpressure — slow the
   gRPC stream consumption so the kernel applies flow control to
   the sender.
6. **(Last resort, opt-in)** Return `RESOURCE_EXHAUSTED` to new
   `put_chunk` until RSS drops. Client retries with backoff
   (Bug G's retry policy). This step is disabled by default; arm
   it only on hosts where step 5 is insufficient.

This mirrors Ceph's OSD memory autotuner, which prefers shrinking
caches over rejecting OPs.

### 6.3 Composite hosts

A host running `mgr04 + data-warm01 + client02 + Plex` (toro-plex)
must:

- Set `hard_cap_mb` separately on each role.
- Total ≤ (host RAM − Plex headroom − OS overhead). Operator's
  responsibility, but `tests/proxmox/abuse/14-composite-host-budget.sh`
  validates the sum at deploy time.

---

## 7. Data Integrity Audit Checklist

Run through this whenever you touch a write / migrate / delete /
scrub path. Mark each item as "Verified by <test name>" in the PR.

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
5. ☐ `commit_chunk_from_tmp` removes the tmp on **any** error path
     so a half-written chunk doesn't leak disk.
6. ☐ Migration (`scheduler::ReplicationTask`) only marks a task
     `Done` after `VerifyChunk` on the target succeeds. The source
     is **never** removed by the scheduler — only an explicit
     `tier-demote` or `unlink` deletes a chunk.
7. ☐ Scrub computes BLAKE3 on the **disk file**, compares to
     `chunk.meta.checksum`. Mismatch → mark `chunks_corrupt`,
     fire `ScrubCorruption` alert. Auto-repair fetches from another
     replica via `replicate_chunk`.
8. ☐ `unlink` removes the manager-side `FileMetadata` first, then
     the chunks are deleted async by the GC scheduler. Crash between
     metadata-removal and chunk-deletion leaves orphan chunks, not
     dangling pointers — orphans get reclaimed by the next scrub
     pass.
9. ☐ `rename` is a single Raft command (atomic from observers'
     perspective). Open handles to the old name continue to read
     the same chunks (the FileMetadata's `path` field is updated
     but chunk_id binding is unchanged).
10. ☐ Reads use replica-fallback (P0-2): if the first replica
     errors, try the next; only fail the read after all candidates
     fail.

---

## 8. When You Encounter a Bug

1. Reproduce on the lab (or pure unit test if the bug surface allows).
2. Open `plans/incidents/SOAK-RECOVERY-YYYY-MM-DD.md` (latest) — if
   the bug looks like one of the catalogued A–J, your fix probably
   has a regression test already; extend it rather than starting
   fresh.
3. Pick a new bug letter (continue alphabetically from the latest
   in the catalog).
4. Follow § 4 test-first procedure.
5. Document in the soak-recovery doc with the same template as
   existing entries.

---

## 9. Bandwidth + Network Shaping

Lab shaping lives in `tests/proxmox/ansible/roles/dtfs-netshape/`.
Each DC class is enforced via `tc qdisc` + IFB on the VM:

| Class | Up | Down | Jitter |
|-------|----|------|--------|
| LELI  | 40 Mbps | 40 Mbps | 25 ms ±5 ms |
| TORO  | 400 Mbps | 400 Mbps | 5 ms ±1 ms |
| NRB   | 1 Gbps | 1 Gbps | 1 ms ±0.5 ms |

Apply pre-soak via `apply-shaping.yml`, revert via
`revert-shaping.yml`. **SSH/22, manager gRPC/9401, manager API/9402,
data gRPC/9500, NTP/123 are exempt** from shaping (otherwise Raft
heartbeats fail under jitter and the cluster can't elect a leader).

---

## 10. File-by-file map (for orientation)

- `crates/dtfs-raft/src/state_machine.rs` — `apply_request` is the
  authoritative state-mutation point. Every metadata change goes
  through here.
- `crates/dtfs-raft/src/lib.rs::ClientRequest` — the application
  command enum. Adding a new mutation = new variant here + new arm
  in `apply_request`.
- `crates/dtfs-manager/src/scheduler.rs` — migration / re-replication
  background loop. `schedule_path_migration` is the public entry
  point for operator-driven moves.
- `crates/dtfs-manager/src/metadata.rs` — gRPC service implementing
  the manager RPCs. Thin wrappers over `raft_write(ClientRequest::*)`.
- `crates/dtfs-manager/src/api.rs` — REST endpoints (consumed by
  WebUI + CLI). `get_stats` is the WebUI home aggregator.
- `crates/dtfs-manager/src/webui.rs` — embedded SPA. Format-string
  HTML; `{{` / `}}` for literal braces.
- `crates/dtfs-data/src/server.rs::put_chunk` — streaming put path.
  Read this before touching the write hot loop.
- `crates/dtfs-data/src/store.rs::commit_chunk_from_tmp` — atomic
  commit. Read § 7.
- `crates/dtfs-data/src/memory.rs` — Bug G memory budget + sampler.
- `crates/dtfs-fuse/src/filesystem.rs` — FUSE↔cluster bridge. Write
  lease handling, readahead engine wiring, local disk cache.
- `crates/dtfs-fuse/src/fuse_adapter.rs` — fuse3 trait
  implementation. **Cookie scheme** for readdir/readdirplus lives at
  `dir_entry_cookie()` — DO NOT diverge the two paths (Bug F).
- `plans/incidents/SOAK-RECOVERY-2026-05-18.md` — current bug
  catalog + verification log.
- `plans/active/` — in-flight plans; `plans/incidents/` and
  `plans/reports/` for archived material. See
  [`plans/README.md`](./plans/README.md).
- `tests/proxmox/abuse/` — shell regression tests (numbered).

---

## 11. Updating this file

This document is canonical. **When you change behaviour that affects
operators, AI agents, or the data plane semantics, update CLAUDE.md
in the SAME commit as the code change.** Stale guidance is worse
than no guidance.

Specifically, update:

- § 1 when the target deployment topology changes.
- § 3 when conventions change (snapshot compat, memory rules,
  integrity invariants).
- § 6 when memory strategy changes.
- § 7 when a new invariant is added to the integrity contract.
- § 10 when a major file's responsibility changes or a new crate is
  added.

PR descriptions for non-trivial work should explicitly note
"CLAUDE.md updated" or "no CLAUDE.md change needed because …".
