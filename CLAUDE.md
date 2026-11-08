# CLAUDE.md — Operating Guide for Claude on DTFS

DTFS = Raft-managed tiered distributed filesystem in Rust. This file is canonical contract: keep accurate, update in same commit as any behaviour/convention change.

Environment (2026-09): no build VM, no lab or prod cluster. All verification via crate tests in local Docker image (§ A.4). Cluster-only behaviour reasoned about, not executed.

Agents: `.claude/agents/` (dtfs-explore, dtfs-intake, dtfs-implementer, dtfs-reviewer, dtfs-integrity-auditor). Path-scoped invariants: `.claude/rules/`. Live incidents: `dtfs-diagnose` skill + `dtfs` MCP (`tools/dtfs-mcp/`, `docs/src/operations/diagnostics-mcp.md`); `op_*` dry-run unless confirmed.

---

## Quick rules (every response)

- Read file before editing; don't re-read unchanged files; large files (`scheduler.rs`, `metadata.rs`, `filesystem.rs`, `state_machine.rs`): grep, then Read with `offset`/`limit`.
- Thorough reasoning, concise output. No sycophancy, no emojis, no em-dashes, straight quotes, copy-paste-safe code.
- Don't guess APIs/versions/flags/SHAs/package names. Verify first.
- Simplest solution, fewest moving parts. No speculative features, no premature abstraction, no error handling for impossible cases.
- Reviews/debugging: state finding, show fix, stop. Never speculate without reading code.

---

# Part A — Codebase Knowledge

## A.1 Mission

DTFS serves **one household Plex media library across three DCs with asymmetric bandwidth**:

| DC | Link | Role | Key nodes |
|----|------|------|-----------|
| LELI | ≤40 Mbps | Primary archive, every chunk anchored here | mgr01-03 (Raft quorum), data-primary01 (~50 TB HDD), client01 |
| TORO | ~400 Mbps | Working set + read-ahead cache | mgr04, data-warm01 (~1 TB SSD), client02 |
| NRB | ~1 Gbps | Download landing, replicates to LELI | mgr05, data-warm02 (~1 TB HDD), client03 |

Reads dominate (~5 streams ≤60 Mbps); writes bursty (30 GB in minutes). Hosts are shared with Plex: per-role memory budgets, never starve the co-resident app. **Anchor invariant: every chunk has ≥1 replica on LELI data-primary01.** Other replicas move freely; LELI copy sacred.

Non-goals: multi-tenancy, POSIX completeness (xattrs/mmap/open-unlink best-effort), online rebalance across reconfigs (drain-then-replace).

## A.2 Architecture

Crates: **dtfs-manager** (Raft peer, REST/gRPC, owns metadata), **dtfs-data** (per-tier chunk store, streaming put/get, SMART), **dtfs-fuse** (`/mnt/dtfs`, read-ahead cache), **dtfs-client** (shared by fuse + cli), **dtfs-raft** (openraft SM; log = truth), **dtfs-common** (BLAKE3, hash ring, config, version), **dtfs-proto**.

**Write:** write_lease → allocate_chunks (file's volume) → PutChunk to tmp → commit_chunk_from_tmp → commit_chunk_locations via Raft; rejected below `write_safety.min_replicas_before_ack` distinct `(node_id, disk_id)` replicas.

**Read:** metadata (ENTRY_TTL cache) → `select_read_replica` (priority > distance > tier) → GetChunk, verify → local cache → readahead; misses single-flighted. Detail: `docs/src/architecture/data-flow.md`.

## A.3 Conventions — Do Not Break

**A.3.1 Snapshot compat.** Every new `StateMachineData` field carries `#[serde(default)]` + old-snapshot-loads regression test (`state_machine::posix_path_regression` or `snapshot_persistence_tests`). `ClientRequest` landing a timestamp in replicated state carries `#[serde(default)] now_ms: u64`, stamped by proposer; apply falls back to `log_id.index` when `0`. Guard: `dtfs-raft/src/compat_tests.rs` loads `testdata/{snapshot,client_requests}_v1.json`; fixtures are append-only (add `_v2`, never regenerate `_v1`).

**A.3.2 Memory discipline.** No `Vec<u8>` of full chunks on hot path, read or write: stream to/off disk, hash incrementally. `[memory]` on all roles (core `dtfs-common/src/memory.rs`), validated at startup with `PlacementConfig::validate()`. FUSE caches bounded. FUSE detail: `.claude/rules/fuse.md`.

**A.3.3 Integrity is the prime directive.** Every byte-touching path: (1) BLAKE3 end-to-end into `ChunkMeta.checksum`; (2) `.dat` and `.meta` via tmp + `sync_all()` + atomic rename; (3) WAL before op, clear only after fsync; (4) never delete a chunk until replacement verified or unreferenced; quarantine by RENAME to `.dtfs/quarantine/<id>.dat.bad`; (5) never silently downgrade replication factor; (6) verify on every read (data node AND client) and scrub against the chunk id (content address), mismatch = `Status::data_loss`, FUSE fails over; a `.meta` disagreeing with a `.dat` that hashes to its id is repaired, not quarantined; (7) with `cluster.shared_secret` set, every mutating data-plane and ManagerService RPC requires `x-dtfs-rpc-ts`+`x-dtfs-rpc-mac` HMAC, Raft transport RPCs are HMAC-bound to `from\nto\nblake3(body)` (`require_raft_auth` rollout flag); REST mutations use the LIS-18 header. Checklist: `.claude/rules/integrity.md`.

**A.3.4 Mandatory vs cache replicas.** Never conflated: `mandatory_replicas.<tier>` (only under-replication scheduler maintains) vs `cache_replicas.<tier>.{min,max}` (only tiering engine creates, evictable). Every promote/demote checks anchor first. Replicas keyed by `(node_id, disk_id)`, never `node_id` alone; data disk ids persist in `<disk>/.dtfs-disk-id` (moved `[[disks]]` entry refuses startup) and copies register the disk the target reports. A Raft `ClientResponse::Error` is a failure (`scheduler::client_response_ok`), never followed by a byte delete. A replica justifying removal of another must be LIVE (`live_replica_tier`); `apply_remove_chunk_location` refuses to remove the last live replica. Anchor DC strings compare case-insensitively.

**A.3.5 Build stamp.** Deploy builds only via `scripts/{build-linux-amd64,dtfs-build,dtfs-release}.sh` (fresh `BUILD_DATE`, release stamps `DTFS_VERSION`). Commit subject `release vX.Y.Z` / `deploy` ships via `.gitlab-ci.yml` + `deploy/ansible/update-cluster.yml` (`docs/src/operations/ci-pipeline.md`).

**A.3.6 Raft is the only mutation point.** Replicated state changes only via a `ClientRequest` applied in `apply_request`. Apply is deterministic: no wall clock (use `now_ms`), no RNG, no I/O, no hash-map iteration order leaking into state. Byte deletion only after the Raft removal commits. Lease-less `UpdateChunkLocations` (scheduler) is union-only: never removes rows, never touches an index without rows, never grows size, errors when it skipped anything. Every apply arm that can refuse returns an error and has a negative test.

## A.4 Build / test

Linux toolchain only. Docker image `dtfs-buildenv:latest` (`docker/buildenv.Dockerfile`; rebuild `docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .`). From Git Bash, `MSYS_NO_PATHCONV=1` required; no `bash -lc` inside image:

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "${DTFS_REPO_HOST_PATH:-C:/Users/<user>/git_github.com/dtfs2}:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release [<test_name>]
```

Always `--release`. Iterate with `<test_name>`, finish with full crate. After config/example change run `cargo test -p dtfs-common --release` (`example_*_toml_parses`; rerun once if flaked). Parallel agents: one worktree each (`../dtfs2-wt/<name>`, branch `wt/<name>`), own target volume `dtfs-target-<name>` (copy of `dtfs-target`), mounted at `/build`. Keep crate-level `#![allow(dead_code)]` on dtfs-manager/dtfs-data (rustc 1.94 ICE workaround). Source files: use Edit/Write, never `sed -i`.

## A.5 Memory model

Bounded, best-effort, never OOM-kill, never reject by default. Ladder: stream to disk → shrink caches → drop unconsumed readahead → pause background → backpressure → (opt-in `shed_threshold_pct`) RESOURCE_EXHAUSTED. Composite hosts set `hard_cap_mb` per role. Every daemon has a unique `[node].hostname`; FUSE registers as `[location].host` (`<data-hostname>_cln`). Startup refuses invalid config (memory ladder, zero intervals/timeouts, `max_message_size_mb` 0, unknown TLS mode, `orphan_grace_secs` < 6 h), a moved data disk, and a snapshot older than the purged log. Config: `docs/src/configuration/`.

## A.6 Data-integrity audit checklist

`.claude/rules/integrity.md` (auto-loaded for `dtfs-data/**`, `dtfs-manager/src/scheduler.rs`, `dtfs-raft/**`); history in `docs/src/internals/invariants.md` § A.6.

## A.7 Logging (`tracing`)

- Structured fields: `warn!(chunk_id = %id, node_id, disk_id, "replica rejected")`.
- Every rejection, integrity fault, quarantine, replica add/remove, membership change: once at warn/error (info for planned removals).
- Hot path: debug/trace only. Log an error once where handled.
- Never log chunk bytes, secrets, HMACs, tokens, webhook URLs, or full config structs.

## A.8 File map

Load-bearing ownership only; history per row in `docs/src/internals/invariants.md` § A.8.

| File | Responsibility |
|---|---|
| `dtfs-raft/src/state_machine.rs` | `apply_request`, sole state-mutation point; sticky node states survive `RegisterNode`; dir rename re-keys children |
| `dtfs-raft/src/lib.rs::ClientRequest` | Command enum; new mutation = new variant + arm (+ `now_ms`) |
| `dtfs-manager/src/scheduler.rs` + `scheduler/*.rs` | SOLE owner of replica repair; leader-gated, bounded, Raft-remove-before-byte-delete; disk-keyed orphan reap; leader re-checked before every delete batch |
| `dtfs-manager/src/tiering.rs` | Cache-tick planner; never holds SM read lock across await |
| `dtfs-manager/src/monitor.rs` | Heartbeat health detection only; MUST NOT schedule repair |
| `dtfs-manager/src/metadata.rs` | ManagerService gRPC over `raft_write`; follower writes forward via `forwardable()` allow-list; ack floor clamped to tier RF |
| `dtfs-manager/src/api.rs` + `api/*.rs` | REST router; handlers per resource; decommission refuses while replicas remain |
| `dtfs-data/src/server.rs::put_chunk` | Streaming put; HMAC gates all data-plane RPCs |
| `dtfs-data/src/store.rs::commit_chunk_from_tmp` | Atomic commit (§ A.6) |
| `dtfs-fuse/src/filesystem.rs` | FUSE↔cluster bridge; rules in `.claude/rules/fuse.md` |
| `dtfs-fuse/src/fuse_adapter.rs` | Every op error routes through `cluster_err_to_errno` |
| `dtfs-cli/src/main.rs` + `updater.rs` | REST client, full webui parity, non-zero exit on failure; updater verifies sha256 (+ minisign), token on first hop only |
| `scripts/dtfs-install.sh` | Role installer; rendered configs proven by `dtfs-common/tests/installer_render.rs` |
| `docs/` + `scripts/build-docs.sh` | mdBook (pinned, Docker fallback), `scripts/check-docs-links.py`; CI `pages` job publishes HTML; Ansible spec in `docs/src/deployment/` |

---

# Part B — Operating Discipline

## B.1 How a change lands

Locate → failing tests (positive + negative, § B.4) → smallest fix → targeted run → crate suite → commit to `master`. Workspace suite only for cross-crate or wire changes. No PRs, no plan files; only temporary `wt/*` branches (§ A.4): rebase, workspace suite, fast-forward, push, remove worktree/branch/volume. Wire/on-disk changes also land on master, gated by § A.3.1 tests.

Model split: main session analyses, specs, verifies diff + test output. Code goes to `dtfs-implementer` (sonnet, medium). Exception: main session edits directly for docs, comments, config examples, and code changes of ≤10 lines in one file.

## B.2 Plans

Session todo list only. Deferred work goes in the commit body or final chat report (file:line, fix direction). Written plan only when the user asks.

## B.3 Commits

`<type>: <summary ≤72 chars>`, blank line, what changed and why in a few lines, `Co-Authored-By:` trailer. `<type>` is `bug`/`feat`/`int`/`docs` (release notes via `git-cliff`, `.github/cliff.toml`; `Breaking:`/`Update-required:` trailers route sections). One commit per logical change. **Hard rule: `git push origin master` immediately after every commit**; local master must never lag origin (stale origin = stale agent worktrees). Never `--no-verify`, never `--amend` a pushed commit, never bypass signing.

## B.4 Tests

Every logic change ships a **positive** test (intended outcome) and a **negative** one per new guard, asserting the exact error (`Status::code()`, `DtfsError` variant, errno) and the boundary (min_replicas-1, last live replica, cap hit). Bug fix: test fails before the fix, for the stated reason; network paths untestable here get a pure decision helper + test. Durability/anchor/Raft-apply invariants: property test over random op sequences, invariant asserted after every step. Snapshot fields: old-snapshot-loads test (§ A.3.1). Cluster-only behaviour: "not executed, reasoned" in commit body; `tests/release/` and `tests/proxmox/` are dormant.

## B.5 Where things live

`crates/<crate>/src/` · `proto/` · `docs/src/` · `scripts/` · `tools/` (host Python, outside Docker loop) · `deploy/examples/*.toml` · `.claude/{agents,rules,hooks}/`. No root status files, no loose `.md` under `crates/`/`tests/`, no in-repo changelog. `dtfs-common/src/config.rs` changes update examples + docs together (`.claude/rules/config-surface.md`).

## B.6 Updating this file

Behaviour change visible to operator, agent, or data plane updates CLAUDE.md in same commit. Keep it ≤13 KB; history goes to `invariants.md`, not here.
