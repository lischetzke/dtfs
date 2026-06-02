# Bug BQ — FUSE client has no periodic heartbeat (idle client reported Down)

**Status:** complete (authored + verified 2026-06-02)
**Source:** ISSUE-SCAN-2026-06-02-1512 (see plans/reports/ — move to processed/ once captured)

---

## 0. Background

A healthy, mounted FUSE client is reported `Down` by the manager. Confirmed live
on the minicluster: `mini-client01` (node_id `6841539694661734231`) shows
`health:"Down"`, `last_seen_ms` frozen ~150s, while the daemon is alive
(`dtfs-fuse@mnt-dtfs` active+running, `/mnt/dtfs` mounted, pid 985898, all three
managers reachable on 9401, host idle load 0.00).

Root cause: **dtfs-client / dtfs-fuse has no heartbeat loop.** Client liveness is
a single `RegisterNode` RPC (`crates/dtfs-client/src/client.rs:1028`, role=client).
The only `tokio::time::interval` in `dtfs-fuse` is the local-cache pruner
(`local_cache.rs:349`). Contrast data nodes (`crates/dtfs-data/src/main.rs:117`):
a dedicated heartbeat loop (~5s) keeps `last_seen_ms` current.

The manager side is correct: `effective_health` (`crates/dtfs-manager/src/api.rs:309`,
Bug BP-3, deployed) ages a stale `Healthy` to `Down` once `last_seen_ms` exceeds
`raft.node_liveness_secs`. BP-3 made the long-standing "client last_seen frozen"
symptom visible by projecting `Down`, exposing the missing client heartbeat. Net:
an idle-but-healthy client is indistinguishable from a genuinely dead one.

Secondary (low): on the `dtfs-fuse@mnt-dtfs` restart the unit hit
`/bin/fusermount3: No such file or directory` (`status=203/EXEC`) — the
mount/unmount helper path is wrong on the client image (binary at
`/usr/bin/fusermount3`). The main mount succeeded this cycle; a clean
stop/unmount relying on `fusermount3` at `/bin` will fail.

---

## 1. Goals / non-goals

**Goals:**
- Add a periodic client heartbeat/keepalive so an idle mounted client keeps its
  liveness fresh and is not projected `Down` (BQ-1).
- Fix the `fusermount3` path in the systemd unit / deploy surface (BQ-2).

**Non-goals:**
- Changing the manager-side `effective_health` projection (BP-3 is correct).
- The manager-version / peer-manager liveness gap — that is Bug BR (separate
  plan), even though it shares the "never self-registers" shape.
- Data-loss / raft-storage work (Bug BP).

---

## 2. Locked-in decisions

- **BQ-1**: Spawn a heartbeat task in `dtfs-fuse` (mirroring the data node's
  loop) that periodically refreshes client liveness against the manager. Reuse
  the existing `register_node` (role=client) RPC as the keepalive call unless a
  lighter client-ping is trivial — re-register is already audit-idempotent
  (Bug BJ `node_info_material_eq`), so a periodic re-register does not spam the
  audit log. Interval is a config knob (`[fuse]` / client config), default
  shorter than `raft.node_liveness_secs` (e.g. window/3, and never less than a
  few seconds). The keepalive must tolerate a manager being non-leader / pool
  failover (client already advances active manager on RPC failure — seen in the
  client log).
- **BQ-1 liveness semantics**: the keepalive must update `last_seen_ms`. If that
  is a Raft write it stalls under a degraded raft (Bug BP condition); acceptable
  for now (data nodes have the same property). Do NOT add a new Raft command if
  the existing RegisterNode path already refreshes `last_seen_ms` on apply.
- **BQ-2**: Make the systemd unit locate `fusermount3` via PATH or the correct
  absolute path; ensure the `fuse3` userspace package is installed on the client
  image. Deploy-surface change only.

---

## 3. Data-model changes

None. New config key for the heartbeat interval (client config) with a
backward-safe default. No `StateMachineData` change (reuses RegisterNode).

---

## 4. Task list

### BQ-1: Periodic client heartbeat/keepalive

- Status: ✅ (2026-06-02)
- Files: `crates/dtfs-fuse/src/main.rs` (spawn the loop, like data
  `main.rs:117`), `crates/dtfs-client/src/client.rs` (keepalive call, reuse
  `register_node` at :1028), client/fuse config (`crates/dtfs-common/src/config.rs`
  or the fuse config struct) for the interval.
- Sketch:
  1. Add `heartbeat_interval_secs` to the fuse/client config (default e.g. 10s,
     must be < `node_liveness_secs`).
  2. In `dtfs-fuse` startup, after the initial register + mount, spawn a tokio
     task: `interval(heartbeat_interval)` → call the client keepalive
     (re-`register_node` role=client) → on error, let the client pool advance
     active manager and retry next tick. Log at debug; do not spam.
  3. Confirm the manager `RegisterNode` apply refreshes `last_seen_ms` (it does
     — node is upserted into `sm.nodes`); no manager change needed.
- Tests:
  - Unit (dtfs-fuse/client): a fake manager client records N keepalive calls
    over M intervals; assert the loop fires on cadence and survives an RPC error
    (advances/retries).
  - Live: on the lab, an idle mounted client keeps `health:"Healthy"` in
    `GET /api/v1/nodes` across a period > `node_liveness_secs` (was `Down`).
- Done when:
  - Unit green; idle client stays Healthy past the liveness window on the lab.

### BQ-2: Fix `fusermount3` path in systemd unit

- Status: ✅ (2026-06-02)
- Files: `deploy/` (systemd unit for `dtfs-fuse@`), client-image provisioning
  (`tests/proxmox/ansible/` if the image is built there).
- Sketch: reference `fusermount3` via PATH or `/usr/bin/fusermount3`; ensure
  `fuse3` tools installed on the client image.
- Tests:
  - Abuse/release: restart `dtfs-fuse@mnt-dtfs`, assert no `203/EXEC` /
    `fusermount3: No such file` in the unit journal and the mount comes up.
- Done when: clean restart shows no fusermount3 EXEC error on the lab.

### BQ-3: Release case

- Status: ✅ (2026-06-02)
- Files: `tests/release/cases/NN-client-liveness.sh`
- Case: mount a client, leave it idle past `node_liveness_secs`, assert it is
  still reported Healthy / counted in liveness (not `Down`).
- Done when: case PASS or `KNOWN_ISSUE [BQ]` pinned.

### BQ-4: Changelog

- Status: ✅ (2026-06-02)
- Files: `changelog/UNRELEASED.md`
- Entry: idle FUSE clients now stay reported online via a periodic heartbeat;
  note the fusermount3 unit fix. Same commit as the code.

---

## 5. Cross-cutting invariants

- Keepalive re-register must stay audit-idempotent (Bug BJ): a periodic
  re-register of unchanged node info must NOT add audit rows.
- Heartbeat interval must be < `raft.node_liveness_secs` or the projection still
  flaps the node to `Down` between beats.
- Client must keep working through manager pool failover (non-leader / down
  manager) — keepalive failure on one manager advances to the next, never
  crashes the mount.

---

## 6. Verification log

- **BQ-1 (2026-06-02)** — Added `run_client_heartbeat` generic cadence
  helper + spawn in `dtfs-fuse/src/main.rs`, config knob
  `[client] heartbeat_interval_secs` (default 10). Unit test
  `bq1_heartbeat_tests::keepalive_fires_on_cadence_and_survives_errors`
  (paused-clock) PASS on builder (`cargo test -p dtfs-fuse --release`:
  30 passed). Live: deployed `dtfs-fuse` to mini-client01 (.176); journal
  shows `Bug BQ-1 client keepalive started interval_secs=10`. Left the
  mount idle 45-60 s (> `node_liveness_secs`=30); `/api/v1/nodes` still
  reports `mini-client01 health=Healthy` with `last_seen_age≈1.3 s`
  (pre-fix it aged to Down). Release case
  `tests/release/cases/32-client-liveness.sh` → STATUS: PASS.
- **BQ-2 (2026-06-02)** — Switched `ExecStartPre`/`ExecStop` to
  `/usr/bin/fusermount3` in `deploy/systemd/dtfs-fuse@.service` and the
  ansible template. Live: `systemctl restart dtfs-fuse@mnt-dtfs` on
  mini-client01 came up clean, mount present, no `203/EXEC` /
  `fusermount3: No such file` in the unit journal.
- **BQ-3 (2026-06-02)** — `32-client-liveness.sh` PASS against the live
  minicluster (idle client stays Healthy past 2× the liveness window).
- **BQ-4 (2026-06-02)** — `changelog/UNRELEASED.md` entries for BQ-1/BQ-2.

---

## 7. Deviations from plan

- **BQ-1 keepalive uses a generic helper, not an inline loop.** The
  cadence loop is factored into `run_client_heartbeat<F,Fut,E>` so it is
  unit-testable on a paused clock without a live manager (the plan asked
  for a unit test of the cadence + error survival). The production spawn
  passes a closure that calls `register_self_as_client` (role=client);
  the helper logs at debug and never returns, surviving RPC errors so a
  manager failover does not stop liveness refresh.
- **BQ-2 fix is the absolute path, not PATH lookup.** Settled on
  `/usr/bin/fusermount3` (fuse3's install location) rather than relying on
  systemd PATH resolution, matching the existing absolute-path style of
  the unit. No client-image package change was needed — `fuse3` was
  already installed; only the wrong `/bin` path was at fault.
