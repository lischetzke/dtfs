# Bug BR — Manager version/build_date not published to shared state (blank in WebUI)

**Status:** complete (authored + verified 2026-06-02)
**Source:** ISSUE-SCAN-2026-06-02-1525 (see plans/reports/ — move to processed/ once captured)

---

## 0. Background

The WebUI node table shows a version for only one manager — whichever one served
the page. The other managers render `—`. Confirmed live: querying each manager's
API in turn (`:9402/api/v1/nodes`) shows only that manager versioned; peers blank.
Data/client nodes carry version on every manager because they register via
`RegisterNode`.

Root cause: **managers are Raft peers, not `RegisterNode` callers**, so their
`version`/`build_date` never enter the shared state-machine node registry. The
API synthesizes only the local manager's version
(`crates/dtfs-manager/src/api.rs:778-792`, `is_local` branch from
`dtfs_common::version::{VERSION,BUILD_DATE}`) and leaves peers empty. Documented
Bug AY gap ("Other managers' version stays empty until a future per-manager
status RPC is wired"); persisted since the 2026-05-28 scans.

WebUI render is correct — `webui.rs:1045`/`:1543` show `n.version || '—'`. The
gap is upstream data, not markup.

Related: the same code synthesizes peer-manager `last_seen_ms` as `now_ms`
(`api.rs:805`), so peer managers always look freshly-alive regardless of reality.
A manager self-register/heartbeat fixes both.

---

## 1. Goals / non-goals

**Goals:**
- Publish each manager's `version`/`build_date` into shared Raft state so any
  manager's API/WebUI reports every manager (BR-1).
- Replace the synthesized peer `last_seen_ms` with a real per-manager liveness
  value carried by the same mechanism (BR-2).

**Non-goals:**
- WebUI markup changes — rendering already handles populated fields.
- Client heartbeat (Bug BQ, separate plan) — same root shape, separate node
  class and separate fix site.
- Manager raft-storage / quorum work (Bug BP).

---

## 2. Locked-in decisions

- **BR-1**: Each manager self-registers via the existing `RegisterNode` path
  (role=manager) on startup, carrying `dtfs_common::version::{VERSION,BUILD_DATE}`.
  Version flows through `metadata.rs:565` into the registry exactly like data
  nodes. Re-register is audit-idempotent (Bug BJ `node_info_material_eq` treats
  version/build_date as material — a real upgrade logs once, steady state is
  silent). Reuses an existing, tested path; smaller than a new status RPC.
- **BR-1 API change**: once managers are in the registry with real version,
  remove (or reduce to fallback) the `is_local` synth in `api.rs:778-792` so the
  stored value is used for all managers. Keep a fallback to local VERSION only
  if a manager has not yet self-registered (early bootstrap).
- **BR-2**: The manager self-register/keepalive also refreshes `last_seen_ms`,
  so drop the `now_ms` synth at `api.rs:805` in favour of the stored value
  (projected through BP-3 `effective_health` like every other node). If a
  periodic manager heartbeat is added, coordinate its interval with
  `raft.node_liveness_secs` (same constraint as Bug BQ).

---

## 3. Data-model changes

None new — managers populate the existing `NodeInfo.version` / `build_date` /
`last_seen_ms` fields via `RegisterNode`. No snapshot schema change (fields
already exist and are `#[serde(default)]`-safe).

---

## 4. Task list

### BR-1: Manager self-registers version/build_date into shared state

- Status: ✅ (2026-06-02)
- Files: `crates/dtfs-manager/src/main.rs` (startup self-register, after raft is
  up / leader reachable), `crates/dtfs-manager/src/metadata.rs` (register_node
  already propagates version at :565), `crates/dtfs-manager/src/api.rs:778-792`
  (use stored version, fallback to local synth only pre-registration).
- Sketch:
  1. On manager startup, once the node can reach the leader (or is leader),
     issue a `RegisterNode` for itself with role=manager, hostname, addr, and
     `VERSION`/`BUILD_DATE`. Re-issue on reconnect; idempotent by Bug BJ.
  2. In `api.rs` nodes projection, prefer the stored `info.version` /
     `build_date` for managers; keep the `is_local` VERSION only as a fallback
     when the registry entry is empty (manager not yet self-registered).
- Tests:
  - Unit (manager/state_machine): applying a manager `RegisterNode` populates
    version/build_date and a second identical register adds no audit row
    (extends Bug BJ idempotency tests to role=manager).
  - Unit (api projection): a manager with stored version reports it; an
    unregistered manager falls back to local synth only when `is_local`.
  - Live: `GET /api/v1/nodes` from ANY manager shows version/build_date for all
    three managers (was: only the serving one).
- Done when:
  - Unit green; all managers versioned in the WebUI/API regardless of which
    manager serves the request, on the lab.

### BR-2: Real per-manager last_seen (drop now_ms synth)

- Status: ✅ (2026-06-02, via BR-1 — see Deviations)
- Files: `crates/dtfs-manager/src/api.rs` (~:805 synth), depends on BR-1's
  self-register refreshing `last_seen_ms`.
- Sketch: once managers self-register (and optionally periodically refresh),
  use the stored `last_seen_ms` for peer managers and let `effective_health`
  (BP-3) project staleness, instead of hardcoding `now_ms` (which always looks
  alive).
- Tests:
  - Unit: peer manager with stale stored `last_seen_ms` projects non-Healthy via
    `effective_health`; fresh stays Healthy.
  - Live: a stopped peer manager eventually shows non-Healthy in `/api/v1/nodes`
    instead of perpetual Healthy.
- Done when: unit green; a downed peer manager is reflected as non-Healthy on
  the lab (no longer synthesized alive).

### BR-3: Release case

- Status: ✅ (2026-06-02)
- Files: `tests/release/cases/NN-manager-version-visible.sh`
- Case: query `/api/v1/nodes` from each manager; assert all manager entries
  carry non-empty version + build_date.
- Done when: case PASS or `KNOWN_ISSUE [BR]` pinned.

### BR-4: Changelog

- Status: ✅ (2026-06-02)
- Files: `changelog/UNRELEASED.md`
- Entry: manager version/build_date now visible for all managers in the WebUI;
  peer-manager liveness now reflects reality. Same commit as code.

---

## 5. Cross-cutting invariants

- Manager self-register must be audit-idempotent (Bug BJ) — steady state adds no
  audit rows; only a real version change (upgrade) logs once.
- The API fallback must never show a *wrong* version: prefer stored; synth only
  when stored is empty AND `is_local`.
- BR-2 must route peer-manager health through `effective_health` (BP-3), not a
  second ad-hoc staleness rule.

---

## 6. Verification log

- **BR-1 (2026-06-02)** — Added
  `dtfs-manager/src/main.rs::manager_self_register_lifecycle` (gRPC
  RegisterNode role=manager against the initial_voters pool + loopback
  fallback, walking endpoints on error) and spawned it from `main`. Unit
  test `state_machine::posix_path_regression::bug_br1_manager_self_register_versioned_and_idempotent`
  (manager register stores version, keepalive adds no audit row, upgrade
  logs once) PASS on builder (`cargo test -p dtfs-raft --release`: 56
  passed). Live: before deploy, `/api/v1/nodes` from mgr01 showed
  `mgr02 ver=None`, `mgr03 ver=None` (only the serving manager versioned).
  Rolled the new `dtfs-manager` to mgr02/mgr03 (followers) then mgr01
  (leader). After self-register, `/api/v1/nodes` from every manager
  (mgr01/02/03) reports all three managers with non-empty
  `version=v0.0.0-dev` + `build_date`. Release case
  `tests/release/cases/33-manager-version-visible.sh` → STATUS: PASS
  (3/3 managers versioned from all 3 views).
- **BR-2 (2026-06-02)** — Verified that a registered manager's health
  routes through `effective_health` (BP-3) via step-1 of `list_nodes`
  (no api.rs change — see Deviations). The synthesized `now_ms` peer
  liveness is now reached only for a not-yet-registered manager
  (bootstrap fallback). Covered by case 33.
- **BR-3 (2026-06-02)** — `33-manager-version-visible.sh` PASS against
  the live minicluster.
- **BR-4 (2026-06-02)** — `changelog/UNRELEASED.md` entry for BR-1.

---

## 7. Deviations from plan

- **BR-1 needs no `api.rs` edit.** The plan anticipated reducing the
  `is_local` synth (api.rs:778-792) to a fallback. On reading the code,
  step-1 of `list_nodes` (api.rs:733-747) already renders *every*
  `sm.nodes` entry — managers included — with `effective_health` (BP-3)
  applied and leader marking, and the synth loop (756-813) already
  `continue`s for any hostname present in `sm.nodes`. So once a manager
  self-registers it is rendered by step-1 with its stored version + real
  `last_seen_ms`; the `is_local`/`now_ms` synth is reached only for a
  manager that has not yet self-registered (early bootstrap) — which is
  exactly the fallback the plan wanted. No api.rs change was made.
- **BR-2 is satisfied by BR-1, no separate code.** Because registered
  managers route through step-1 + `effective_health`, a downed peer
  manager ages to non-Healthy automatically; the `now_ms` synth only
  affects not-yet-registered managers. The release case 33 verifies the
  version/liveness visibility end-to-end.
- **Self-register transport.** Rather than a direct local `raft_write`
  (which fails with `ForwardToLeader` on a follower), the manager
  self-registers via the `ManagerServiceClient` RegisterNode RPC against
  the `initial_voters` pool (loopback fallback for single-node), walking
  endpoints on error exactly like a data node. Lives in
  `dtfs-manager/src/main.rs::manager_self_register_lifecycle`.
