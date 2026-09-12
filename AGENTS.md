# AGENTS.md — Operating Guide for AI Agents on DTFS

DTFS is a Raft-managed tiered distributed filesystem in Rust. This file is the canonical contract: keep it accurate and update it in the **same commit** as any behaviour, convention, or workload change. `CLAUDE.md` is just `@AGENTS.md` — edit this file, not that one. Other tools (Codex, Cursor, …) read AGENTS.md directly.

Part A = what to know about DTFS. Part B = how to work on it.

---

## Quick rules (every response)

- Read a file before editing; don't re-read unchanged files; skip >100KB files unless required.
- Thorough reasoning, concise output. Code first, explain only if non-obvious. No sycophancy, no emojis, no em-dashes, straight quotes, copy-paste-safe code.
- Don't guess APIs/versions/flags/SHAs/package names — verify first.
- Token-efficient: targeted Grep/Read with `offset`/`limit`, narrow edits, batch independent tool calls into one message.
- Don't over-engineer: simplest solution, fewest moving parts, solve what's in front of you. No speculative features, no premature abstraction, no error handling for impossible cases.
- Reviews/debugging: state the finding, show the fix, stop. Never speculate without reading the code; say so if the cause is unclear.

---

# Part A — Codebase Knowledge

## A.1 Mission & target workload

DTFS serves **one household-scale Plex media library across multiple datacenters with asymmetric bandwidth**. Every design decision anchors to this.

Reference deployment ("Plexmedia"), three DCs:

| DC | Link | Role | Key nodes |
|----|------|------|-----------|
| LELI | ≤40 Mbps | Primary archive — every chunk anchored here | mgr01-03 (Raft quorum), data-primary01 (~50 TB HDD), client01 |
| TORO | ~400 Mbps | Working set + read-ahead cache | mgr04, data-warm01 (~1 TB SSD), client02 |
| NRB | ~1 Gbps | Fresh-download landing, replicates to LELI | mgr05, data-warm02 (~1 TB HDD), client03 |

A node may host several roles (e.g. `toro-plex` = mgr04+data-warm01+client02+Plex on one box), so memory budgets are **per-role with a host-wide ceiling** — DTFS never starves a co-resident app.

Workload: Plex playback ≤60 Mbps/stream, ~5 streams/instance. Reads dominate (most chunks read once or twice ever); writes are bursty (a finished torrent lands a 30 GB season in minutes — absorb without OOM-killing anything). **Anchor invariant: every chunk MUST have ≥1 replica on LELI's data-primary01.** Tiering/migration may move *other* replicas freely; the LELI copy is sacred.

Non-goals: multi-tenant isolation, POSIX completeness (Plex's FUSE read + occasional write is the target; xattrs/mmap/open-unlink races are best-effort), online hot-rebalance across reconfigs (drain-then-replace instead).

## A.2 Architecture

**dtfs-manager** Raft peer + REST/gRPC frontend, owns metadata (files, chunk locations, node registry, volumes, leases, scrub, migration). **dtfs-data** data plane, stores chunks per-tier, streams put/get, reports usage+SMART. **dtfs-fuse** FUSE client at `/mnt/dtfs`, gRPC to managers+data nodes, local read-ahead cache. **dtfs-client** shared client crate (dtfs-fuse + dtfs-cli). **dtfs-raft** openraft state machine — the log is the source of truth. **dtfs-common** shared types (BLAKE3, hash ring, version). **dtfs-proto** protobuf/tonic codegen.

**Write:** acquire write_lease → allocate_chunks (target node addrs) → per chunk, stream PutChunk pieces to a tmp file on the target disk (no full-chunk `Vec`, Bug I) → commit_chunk_from_tmp (rename + meta + WAL clear) → commit_chunk_locations (durable file entry), all through Raft. CF-3b: commit_chunk_locations rejects the write when any chunk lands fewer distinct `(node_id, disk_id)` replicas than `write_safety.min_replicas_before_ack`.

**Read:** lookup metadata (cached ENTRY_TTL) → pick the read replica (`location.rs::select_read_replica`: configured `read_location_priority` beats physical distance beats a tier tie-break on equal distance only) → stream GetChunk → optionally cache locally (P0-11) → readahead prefetches. Full U4/CO-3-10 detail (how tier + topology reach the FUSE client's `known_locations`): `docs/src/internals/invariants.md` § A.2. CR-6-4 (FUSE-6): `GetFileLocationsResponse` carries the file's `volume_id` on the wire; the FUSE client's `fetch_file_locations` keeps whatever volume_id it already believes when an older manager sends `""`, never silently downgrading to `"default"` — and `allocate_chunks` always resolves placement from the file's OWN registered volume when it already exists, ignoring a stale/wrong request value. CR-6-6 (FUSE-9): a foreground chunk-miss is single-flighted by chunk_id (content-addressed) — the first miss becomes the fetch Owner, a concurrent miss on the same chunk_id joins the Owner's result instead of issuing its own `GetChunk`.

## A.3 Conventions — Do Not Break

**A.3.1 Snapshot compat.** `StateMachineData` serialises to disk on snapshot. Every new field MUST carry `#[serde(default)]` so old snapshots load cleanly. Add a regression test (`state_machine::posix_path_regression` or `snapshot_persistence_tests`) per new field. CR-2-5 — a `ClientRequest` field that lands in replicated state (e.g. a timestamp) MUST also carry `#[serde(default)] now_ms: u64`, stamped by the proposer, with the apply falling back to `log_id.index` when it's `0` (legacy entry) — see `docs/src/internals/invariants.md` § A.8.

**A.3.2 Memory discipline.** No `Vec<u8>` accumulation of full chunks on the hot path, read or write — stream to/off disk, hash incrementally (Bug I, U2). Memory caps are best-effort, never reject by default (§ A.5). `[memory]` (CF-5) runs on all three roles, shared core `dtfs-common/src/memory.rs`, each with its own ladder rung (data sheds put_chunk; manager pauses scheduler ticks at Shed; fuse halves readahead at Warn, stops prefetch at Shed). `MemoryConfig::validate()` (CR-6-1/CMN-1) is called by all three `main.rs` right after config load, rejecting `warn_threshold_pct >= shed_threshold_pct` or either `> 100` at startup instead of flapping the ladder at runtime; the manager also calls the new `PlacementConfig::validate()` (CMN-8) rejecting a `min_free_pct` outside `0.0..=100.0`. systemd `MemoryMax` backstops all roles. FUSE detail: `.claude/rules/fuse.md`. CR-6-5 (FUSE-5): the FUSE client's `metadata_cache` is bounded (`METADATA_CACHE_MAX_ENTRIES = 4096`, FIFO eviction via a DashMap + insertion-order queue) and periodically swept for expired entries on the `metadata_ttl` cadence — before this, one full chunk-location list per path ever stat'ed was retained forever, pinning hundreds of MB during a Plex library scan on a 1 GB client. CR2-8-3/CR2-8-4 (FUSE-11/12): `local_cache.rs`'s disk cache now classifies a viewer read from cumulative per-fh consumption (`OpenFile.bytes_read_on_fh`) or active readahead rather than one 128 KB kernel call's length, and `evict_to_size` targets 90% of `max_bytes` (hysteresis) instead of a full O(N) stat sweep on almost every `put()`.

**A.3.3 Integrity is the prime directive.** Every path that touches bytes must: (1) carry a BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2) write new chunks to tmp + atomic rename after `sync_all()`; (3) WAL the op before performing it, clear only after fsync; (4) never delete a chunk until a replacement is verified or the referencing file is unlinked and no other live file references the id (Bug CI `fanout_delete_allowed`) — a scrub-detected fault quarantines by RENAMING the `.dat` to `.dtfs/quarantine/<id>.dat.bad` (CR-4-1), never `remove_file`, and only after a re-hash of the `.dat` against its own id fails to repair the `.meta` in place; (5) never silently downgrade replication factor; (6) verify on every read and on scrub (Bug BC-8) — a mismatch is `Status::data_loss`, FUSE falls over (P0-2), disabling `verify_on_read` needs an explicit API-boundary ack; (7) LIS-19/55/83 — when `cluster.shared_secret` is set, every mutating data-plane RPC requires an HMAC-SHA256 `x-dtfs-rpc-ts`+`x-dtfs-rpc-mac` pair (empty secret = legacy skip), including `RecordCacheHit` (DATA-11 — previously ungated, letting any caller inflate a chunk's tiering promotion score); (8) LIS-56 — mutating ManagerService RPCs require the same HMAC shape over `dtfs-mgr-rpc`, including CR-5-1's `Heartbeat` and `ReportChunkStatus` (previously ungated: any node id could poison node status or drop replica entries without the cluster secret) — data nodes stamp both from `[cluster].shared_secret`; deploy manager+data together; REST mutations stay on LIS-18. Full checklist + HMAC shape: § A.6 / `.claude/rules/integrity.md`.

**A.3.4 Mandatory vs cache replicas (Bug AV invariant).** Two independent classes, never conflated: `mandatory_replicas.<tier>` (protected, only the under-replication scheduler maintains them) and `cache_replicas.<tier>.{min,max}` (demand-driven, evictable, only the tiering engine creates them). Every promote/demote checks the anchor against the destination first (CO-1-2). Replica presence/removal is keyed by `(node_id, disk_id)`, never `node_id` alone (Bug BH, CO-1-1). CR-3-1 (MGR-2) — every planner that decides whether a "surviving" mandatory/anchor replica justifies dropping another one (`plan_chunk_demotions`'s `mandatory_still_present`, `plan_flush_cache`'s `mandatory_survives`) must resolve that survivor's liveness through `live_replica_tier` first; a replica on a Down/Failed node or Failed/Removed disk can never be the thing that authorises the drop, and the state-machine's `apply_remove_chunk_location` (CR-3-2) independently refuses to remove the last live replica of a chunk regardless of anchor policy. CR-3-7 (CMN-5) — an anchor `datacenter` string and a node's registered `datacenter` are compared with `eq_ignore_ascii_case` (`AnchorPolicy::is_satisfied_by`, the two `hash_ring.rs` anchor-DC compares), matching `SpillPolicy`/`read_location_priority`: a casing mismatch must never make the anchor permanently unsatisfiable. New code that creates/removes replicas: consult `.claude/rules/integrity.md` / `docs/src/internals/invariants.md` § A.3.4.

**A.3.5 Build-date / version stamp.** Every dev build MUST stamp `BUILD_DATE` fresh (the only distinguishing field between two dev builds) — deploy builds MUST go through a wrapper (`scripts/build-linux-amd64.sh`, `scripts/dtfs-build.sh`, `scripts/dtfs-release.sh <tag>`); confirm with `dtfs-<bin> --version`. Release builds additionally stamp real `DTFS_VERSION`. Guards: `scripts/tests/test-{build-date-fresh,release-version-stamp}.sh`. Mechanism (Bug BS, AZ-5): `docs/src/internals/invariants.md` § A.3.5.

## A.4 Build / test / deploy

Linux/WSL only (Windows host can't build rocksdb + fuse3). Build VM: `dtfs-builder` at 10.150.0.150 (`ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150`, `source ~/.cargo/env` first). Not a git checkout — sync via `tar` over SSH (exclude `./target`, `./.git`, `./.claude`, `./dummy_files`), then build over SSH:

```bash
tar czf - --exclude=./target --exclude=./.git --exclude=./.claude \
    --exclude=./dummy_files . \
  | ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150 \
      'mkdir -p ~/dtfs2 && tar xzf - -C ~/dtfs2'
ssh -i ~/.ssh/dtfs-lab/id_ed25519 debian@10.150.0.150 \
  'source ~/.cargo/env && cd ~/dtfs2 && cargo test -p dtfs-cli --release'
```

```bash
cargo build --release --workspace
cargo test  --release --workspace
cargo test -p dtfs-data --release   # per-crate
```

**Builder unreachable → Docker.** The local image `dtfs-buildenv:latest` (rust toolchain at `/usr/local/cargo/bin`, protoc, clang, libfuse3-dev) builds the Windows checkout in place, with the cache in the named volumes `dtfs-target` (`/target`) and `dtfs-registry`; from Git Bash `MSYS_NO_PATHCONV=1` is required and `bash -lc` must not be used inside the image (it drops cargo from `PATH`):

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "${DTFS_REPO_HOST_PATH:-C:/Users/leduc/git_github.com/dtfs2}:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release
```

Unit and crate suites are verifiable this way; live lab verification is not. Release cases authored without a lab are `bash -n` clean and marked unexecuted in the plan.

Stale-artifact trap (Bug CF): the tar sync preserves mtimes, so a just-synced edit can land older than cached build artifacts and cargo silently reuses a stale rlib — `touch` the edited file(s) before a deploy build and confirm the binary carries the change. Full narrative: `docs/src/internals/invariants.md` § A.4.

Deploy builds MUST go through a wrapper so `BUILD_DATE` is fresh (§ A.3.5); a bare `cargo build --release` stamps stale. `scripts/dtfs-release.sh <tag>` stamps `DTFS_VERSION`, builds, writes `target/release-artifacts/<tag>/` + prints the `gh release create` line (AIWF-4-1: `git-cliff` notes when available, `--generate-notes` fallback). A crate-level `#![allow(dead_code)]` on dtfs-manager/dtfs-data works around a rustc 1.94 dead-code ICE — don't remove without re-verifying the ICE first.

Rolling deploy: scp the binary, `install -m 755`, `systemctl restart`. Managers leader-last, then data nodes, then FUSE clients.

Lab: Proxmox on 10.150.0.0/24, PVE host root `ssh root@10.150.0.121` (password-only, sshpass on the operator's WSL home only, never committed). VM key `~/.ssh/dtfs-lab/id_ed25519`. Bandwidth shaping: `tests/proxmox/ansible/roles/dtfs-netshape/`.

## A.5 Memory model

Deterministic, bounded, best-effort: slow down under pathological load, never OOM-kill. All three roles accept an optional `[memory]` block (CF-5, core in `dtfs-common/src/memory.rs`, per-role ladder — § A.3.2; defaults warn 80 / shed 92 / sample 5):

```toml
[memory]
hard_cap_mb          = 1500   # 0 = unbounded (back-compat)
warn_threshold_pct   = 75
shed_threshold_pct   = 88     # last-resort write reject (Bug G), opt-in
sample_interval_secs = 3
```

Ladder (cheapest first, goal: stay under without rejecting): stream to disk not heap → shrink data/FUSE caches → drop unconsumed readahead → pause background tasks → TCP backpressure → (opt-in) RESOURCE_EXHAUSTED.

Composite host (e.g. `toro-plex`): set `hard_cap_mb` per role (else systemd `MemoryMax`); total ≤ host RAM − Plex headroom − OS overhead (`tests/proxmox/abuse/14-composite-host-budget.sh`). Every daemon needs its own unique `[node].hostname`; FUSE registers under `[location].host` (`<data-hostname>_cln`) — see `docs/src/configuration/`.

CR2-8-1 (CMN-3): `[grpc].max_message_size_mb` (manager/data) and the mirrored `[client].max_message_size_mb` (fuse) now actually govern the tonic `max_decoding/encoding_message_size` caps via the shared `dtfs_common::config::max_message_bytes` helper — previously a hardcoded 32 MB literal ignored the parsed config value — and each role's `main.rs` calls the matching `validate()` (reject `0`) alongside `memory.validate()`.

## A.6 Data-integrity audit checklist

Run when you touch a write/migrate/delete/scrub path; mark each "Verified by <test>" in the commit. Full checklist with narrative context lives in `.claude/rules/integrity.md`, loaded automatically for `dtfs-data/**`, `dtfs-manager/src/scheduler.rs`, `dtfs-raft/**`. Summary: (1) BLAKE3 checksum end-to-end into `ChunkMeta.checksum`; (2-3) tmp+atomic-rename+`sync_all()` for both `.dat` and `.meta` — `.meta` is written via a `write_meta_atomic` tmp+rename helper (CR-4-2), never a direct create at the final path, so a crash never leaves a torn sidecar; (4) counters update only after meta is durable, and only for a genuinely new chunk (a re-upload of an already-complete chunk is not double-counted, CR-4-6); (5) tmp removed on any put error path, and any failure AFTER the `.dat` rename also removes the `.dat`/`.meta`/WAL entry it just committed (CR-4-3) — a 256-way striped per-chunk lock plus op-keyed WAL names (`.put.wal.json`/`.delete.wal.json`, legacy `.wal.json` still replayed) keep a same-id put and delete from interleaving into a torn state (CR-4-4); (6) migration verifies before marking Done, de-location is Raft-remove-then-byte-delete (the CO-3-2 reaper mops a failed byte-delete); (7) scrub BLAKE3-checks vs `checksum`, auto-repairs via `replicate_chunk` — but first tries to repair a missing/unparseable `.meta` by re-hashing the `.dat` against its own id (CR-4-1); only a genuine mismatch or unreadable `.dat` quarantines; (8) unlink deletes metadata before bytes, orphan reap keys off durable on-disk mtime (Bug CB), not a manager-local clock — CR2-7-2 (MGR-10): when the data node's `ListChunks` reply carries `chunk_disk_ids`, the reap is keyed by `(node_id, disk_id)` instead of node-wide, so a de-located leftover on one disk of a node is reaped correctly even when another disk of the SAME node still holds a live replica of the same chunk; an older data node reporting no disk ids falls back to the prior node-wide behaviour; (9) rename is one Raft command, chunk_id binding unchanged; (10) reads use replica-fallback (P0-2).

## A.7 Bandwidth + network shaping

`tests/proxmox/ansible/roles/dtfs-netshape/`. Per-class `tc qdisc` + IFB:

| Class | Up/Down | Jitter |
|-------|---------|--------|
| LELI | 40 Mbps | 25±5 ms |
| TORO | 400 Mbps | 5±1 ms |
| NRB | 1 Gbps | 1±0.5 ms |

Apply via apply-shaping.yml, revert via revert-shaping.yml. **Exempt: SSH/22, manager gRPC/9401, manager API/9402, data gRPC/9500, NTP/123** — otherwise Raft heartbeats fail under jitter and the cluster can't elect.

## A.8 File map

Full narrative (bug/task-ID reasoning behind each row) moved to `docs/src/internals/invariants.md` § A.8; this table is the load-bearing summary.

| File | Responsibility |
|---|---|
| `dtfs-raft/src/state_machine.rs` | `apply_request`, the authoritative state-mutation point. CR-2-1..2-6 (RAFT-1/2/3/4/8/9): `RegisterNode` carries forward sticky Quarantined/Maintenance/Draining state instead of resetting to Healthy, `RenameFile` re-keys every child under a renamed directory (not just the exact key), `UpdateChunkLocations` grows `logical_size` to cover chunks committed past a truncate, and the seven `now_ms`-carrying variants apply deterministically — see `docs/src/internals/invariants.md` § A.8. CR2-7-6 (RAFT-6): `open_at`'s BP-2 corrupt-snapshot discard (blank SM) is checked at manager startup against the log store's `last_purged` — a blank SM with an already-purged log refuses to start as a voter (`log_store::blank_sm_startup_allowed`, called from `main.rs`) since the discarded snapshot was the only surviving copy of the pre-purge history |
| `dtfs-raft/src/lib.rs::ClientRequest` | Command enum; new mutation = new variant + arm |
| `dtfs-raft/src/network.rs::full_snapshot` | Snapshot-stream RPC; unreachable-vs-storage error mapping (Bug CL), bounded in-flight sends (CNF-4) |
| `dtfs-raft/src/log_store.rs` | RocksDB-backed Raft log/vote/last-purged store. CR2-7-5 (RAFT-5): `append`/`save_vote`/`persist_last_purged` use a synced `WriteOptions` (`set_sync(true)`) instead of `put_cf` + `flush_cf` (which only flushed the memtable, no fsync, and created one L0 SST per commit); `truncate_after` is one `delete_range_cf` instead of a delete-key-by-key loop (atomic, no torn-log hole on a crash mid-loop); `purge` commits its range delete and the `last_purged` meta write as one synced `WriteBatch` so both are durable together or neither is |
| `dtfs-manager/src/scheduler.rs` | Migration/re-replication loop, SOLE owner of replica repair; leader-gated, bounded, Raft-remove-before-byte-delete. CR-3-1..3-6 (MGR-1/2/4/5/6/7/8/9): `plan_flush_cache`/`plan_chunk_demotions` (tiering.rs) resolve survivor liveness via `live_replica_tier`; `register_replica` (`MigrationOps`) returns `Result` so a failed Completing registration retries instead of advancing to Done; `register_replica_location` collapsed to one SM lock via the pure `plan_replica_registration`, registering every carrying chunk_index in every referencing file; `resolve_disk_id_for_tier` returns `Option<u32>`, filtering Failed/Removed disks; `check_under_replicated`/`schedule_replication` dedup via `task_blocks_requeue`, source/target picks via the pure `pick_live_source`/`pick_backfill_target` (the latter requiring `placement_cfg.has_room`); `check_anchor_lag` now honours the REV-3-4 task budget and updates its dedup set inline; the heartbeat integrity-fault forwards (metadata.rs) surface `accepted: false` on either write's failure — see `docs/src/internals/invariants.md` § A.8. CR2-7-3 (MGR-13): `orphan_scrub_tick` re-checks `is_leader()` + `orphan_sweep_allowed` at the top of every per-node iteration AND immediately before every delete batch (not just once before the loop), auditing `orphan.sweep_aborted_not_leader` and stopping the sweep on either failing. CR2-7-2 (MGR-10): see § A.6 item 8. |
| `dtfs-manager/src/tiering.rs` | AV cache-tick planner + legacy temperature migration. CR2-7-1 (MGR-3): `build_av_cache_snapshot` collects the SM-derived per-chunk inputs under `state_data.read()`, drops that lock, then takes `read_windows.write()` ONCE for the whole batch via the pure `window_counts_for_chunk` — pre-fix it held the SM read lock across a per-chunk × per-DC `read_windows.write()` await, and a fair `tokio::sync::RwLock` queued every gRPC/REST reader behind one snapshot. |
| `dtfs-manager/src/monitor.rs` | Heartbeat health detection only (Bug CH); MUST NOT schedule repair; leader-gated (Bug CM) |
| `dtfs-manager/src/metadata.rs` | ManagerService gRPC over `raft_write`; follower writes forward with HMAC + a `forwardable()` variant allow-list (MGR-11); `write_via_leader` checks `ForwardWriteResponse.success` (MGR-12); LIS-56 auth incl. Heartbeat/ReportChunkStatus. CR-6-2 (CMN-2): `clamp_ack_floor_to_rf` clamps `write_safety.min_replicas_before_ack` down to the effective mandatory RF of the tier chunks actually landed on, warning once per volume + auditing `write_safety.floor_clamped`, so a floor above a tier's own RF (e.g. default 2 above cold's default RF 1) no longer rejects every commit to that tier forever. CR-6-4 (FUSE-6): `get_file_locations` fills `GetFileLocationsResponse.volume_id`; `allocate_chunks` resolves placement via `resolve_placement_volume_id` — the file's OWN registered volume when it exists, never a stale/wrong request value. |
| `dtfs-manager/src/api.rs` | REST endpoints; LIS-18 auth; effective-config merge; decommission refuses while replicas remain |
| `dtfs-manager/src/rest_auth.rs` | LIS-18 mutation-auth check + axum middleware |
| `dtfs-manager/src/membership.rs` | CF-2 online raft membership validation |
| `dtfs-manager/src/join.rs` | Two-phase data-node join (HMAC handshake); node_id derived server-side |
| `dtfs-manager/src/webui.rs` | Embedded SPA; LIS-35 mutating POSTs carry the shared secret |
| `dtfs-data/src/server.rs::put_chunk` | Streaming put path; LIS-19/55/83 auth gates all data-plane RPCs |
| `dtfs-data/src/replication.rs::replicate_to` | Push-replication engine; timeout sized from chunk bytes |
| `dtfs-common/src/replication.rs` | Shared manager/data-node replication-deadline helpers |
| `dtfs-common/src/crypto.rs` | Join HMAC + data-rpc/mgr-rpc stamp/verify |
| `dtfs-data/src/store.rs::commit_chunk_from_tmp` | Atomic commit (§ A.6) |
| `dtfs-common/src/memory.rs` | CF-5 role-agnostic memory budget core |
| `dtfs-data/src/memory.rs` | Data-node rung; `[cache]` is parsed but inert (REV-4-5) |
| `dtfs-fuse/src/filesystem.rs` | FUSE↔cluster bridge; await-guard + write-batching rules in `.claude/rules/fuse.md`. CR-6-5 (FUSE-5): bounded, periodically-swept `metadata_cache`. CR-6-6 (FUSE-8/9/10): per-(fh,chunk)-transition `record_cache_hit` notify (semaphore-bounded), single-flighted foreground chunk-miss fetches (`foreground_inflight`), `on_chunk_read` called on every transition (hit or miss) — full detail in `.claude/rules/fuse.md`. |
| `dtfs-fuse/src/readahead.rs` | `ReadAheadEngine`; prefetch bound + Shed-rung, `.claude/rules/fuse.md` |
| `dtfs-fuse/src/fuse_adapter.rs` | fuse3 trait impl; every op error routes through `cluster_err_to_errno` |
| `dtfs-common/build.rs` + `src/version.rs` | `VERSION`/`BUILD_DATE` stamps (§ A.3.5) |
| `dtfs-cli/src/updater.rs` | `dtfs-cli update` — download, verify, atomic install, restart |
| `scripts/dtfs-release.sh` | Release builder; stamps version, assembles tarball, prints `gh release create` |
| `dtfs-common/src/tls.rs` | CF-6 `TlsContext`; all gRPC routes through it; enable only cluster-wide |
| `plans/active/` | In-flight plans (`plans/README.md`) |
| `tests/proxmox/abuse/` | Numbered shell regression tests |

---

# Part B — Operating Discipline

## B.1 How a change lands

Reproduce/locate → failing test in the purest layer (pure helper > state-machine apply > manager aggregator > FUSE helper > shell case) → smallest fix → `cargo test -p <crate> --release` + full workspace suite → commit to `master`. Branch only for a wire/on-disk format change (`proto/*.proto`, `ClientRequest`, `apply_request`, `#[serde]`-persisted structs), >6 files, or multi-session work; PR only for a release-note line (`bug:`/`feat:` subjects, labeled).

## B.2 Plans

`plans/active/PLAN-<id>-<short-name>.md` (template: `plans/README.md`) for ≥3-task/multi-session/cross-crate work, else the session todo list. Record divergence under Deviations; delete when done.

## B.3 Commits

```
<type or task-id>: <summary ≤72 chars>

What changed, which test covers it, failing->passing output, deviation.

Plan: <path> (<task-id>)        # only when a plan file exists
Co-Authored-By: <model> <noreply@<vendor>.com>
```

`<type>` is `bug`/`feat`/`int`, becomes the release-note line via `git-cliff`/`.github/cliff.toml` (`Breaking:`/`Update-required:` trailers route sections). One commit per task. Never `--no-verify`, never `--amend` a pushed commit, never bypass signing.

## B.4 Tests

Observe fail → implement → observe pass → full crate suite (new durability/anchor invariants: a property-style test over random ops). Cluster-visible behaviour ships a `tests/release/cases/NN-name.sh` in the same commit (`tests/release/README.md`), or a `Release-case:` trailer — `none (<reason>)` for a pure internal change, or `tests/release/cases/NNN-name.sh (<task-id>)` when the case lands in a later commit of the same plan — a commit hook enforces this.

## B.5 Where things live

`plans/{active,reports}/` · `docs/src/` (archaeology: `docs/src/internals/invariants.md`) · `crates/<crate>/src/` · `proto/` · `tests/release/cases/` · `tests/proxmox/abuse/` · `scripts/` · `deploy/examples/*.toml` · `.claude/{agents,rules}/`. No root status files, no loose `.md` under `crates/`/`tests/`, no in-repo changelog. Config-surface sync (`.claude/rules/config-surface.md`): `dtfs-common/src/config.rs` changes update the examples + docs together.

## B.6 Updating this file

A behaviour change visible to an operator, agent, or the data plane updates AGENTS.md in the same commit (say so, or why not); fold in any session instruction that overrode a rule here.
