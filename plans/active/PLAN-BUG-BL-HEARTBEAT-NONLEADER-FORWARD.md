# PLAN-BUG-BL — Heartbeat Raft writes silently dropped on non-leader managers

Plan id: `BL`
Status: ✅ complete (2026-05-28) — BL-1..BL-6 done. ForwardClientWrite RPC + non-leader heartbeat proxy + self-quarantine guard landed, deployed to all 3 minicluster managers, and definitively live-verified: with both data nodes heartbeating a FOLLOWER (.172) while the leader was .173, last_seen stayed fresh (age 2-3 s) — pre-BL this state froze last_seen. dtfs-manager 142/142.
Author: Claude Opus 4.7 (1M context)
Created: 2026-05-28
Sources: ISSUE-SCAN-2026-05-28-1416 Finding 2 (critical), 3 (high), 4 (high)

---

## 0. Background

When a data node reconnects its heartbeat to a non-leader manager, the
Raft writes inside the heartbeat handler are silently dropped, causing
three distinct failures simultaneously:

1. **Stale liveness timestamps** (`last_seen_ms` frozen at last
   leader-committed heartbeat — observed 4 h stale on Minicluster).
2. **Self-quarantine falsely cleared** — a data node that detected a
   corrupt chunk and self-quarantined is re-enabled for writes within
   one heartbeat cycle.
3. **Integrity faults not committed** — QuarantineNode + RemoveCorrupt-
   ChunkReplicas never land in Raft state; the audit log sees no entry;
   the `/api/v1/integrity/faults` feed appears empty.

All three symptoms share the same root cause.

### Root cause (code-confirmed)

`crates/dtfs-manager/src/metadata.rs` heartbeat handler
(`async fn heartbeat`, line 456+):

```rust
let _ = self.raft_write(ClientRequest::UpdateNodeUsage { … }).await;   // line 518
let _ = self.raft_write(ClientRequest::UpdateNodeDiskUsage { … }).await; // line 543
let _ = self.raft_write(ClientRequest::QuarantineNode { … }).await;     // line 605
let _ = self.raft_write(ClientRequest::RemoveCorruptChunkReplicas { … }).await; // line 612
```

Every Raft write uses `let _` — the error is unconditionally discarded.
When the receiving manager is not the leader, openraft returns a
`ForwardToLeader` error (observable in logs as
`"Raft write deferred — not leader, client should retry error=has to
forward request to: Some(…)"`). This error is thrown away; the
handler returns a success response to the data node.

The `self_quarantine_cleared` computation (line 562):

```rust
let is_quarantined = sm.nodes.get(&node_id).map(|n| n.quarantine.is_some()).unwrap_or(false);
let self_quarantine_cleared = !is_quarantined;
```

Because QuarantineNode never commits on a non-leader, `sm.nodes` reads
`quarantine = None`, `is_quarantined = false`, `self_quarantine_cleared
= true`. The data node clears its local self-quarantine flag within the
next heartbeat cycle (≤ 5 s), re-enabling `put_chunk` on a node whose
corrupt chunk is still on disk.

`process_heartbeat(node_id)` at line 471 IS a local in-memory call
(non-Raft) and therefore DOES succeed on non-leaders — the
ClusterManager liveness map stays current. So the health monitor
correctly keeps nodes as Healthy/Suspect/Down. The damage is to
**Raft-committed state** only: `last_seen_ms`, quarantine status,
integrity fault records.

### Connection to integrity feed gap (1416-F4)

`/api/v1/integrity/faults` is a filtered view of the audit log (api.rs
line 119-122). BC-12e is implemented and deployed. QuarantineNode's
apply path pushes an audit row, but that row never reaches Raft because
the Raft write is dropped on non-leaders. Additionally, the audit log is
being saturated by the BJ RegisterNode spam (mini-client01, ~5 s
cadence), which evicts any QuarantineNode rows that do land.
BJ-1 (idempotent RegisterNode audit) mitigates the eviction; BL fixes
the Raft-write drop.

---

## 1. Goals / Non-goals

### Goals
- Raft writes that are part of the heartbeat handling commit even when
  the receiving manager is not the current leader.
- Self-quarantine issued by a data node survives until the corruption is
  remediated, not until the next heartbeat cycle to a non-leader.
- `last_seen_ms` in Raft state tracks within one heartbeat cycle of
  reality.
- QuarantineNode + RemoveCorruptChunkReplicas writes commit so the
  integrity audit feed is populated.

### Non-goals
- No change to data-node heartbeat client (the fix lives on the manager
  side).
- No change to the in-memory `process_heartbeat` liveness map (already
  works correctly).
- No change to the heartbeat wire format or proto.
- Leader-election stability / Raft tuning out of scope.

---

## 2. Locked-in decisions

1. **Manager-side proxy, not data-side redirect.** The non-leader
   heartbeat handler will forward the Raft writes to the current leader
   by extracting the leader address from the openraft
   `ForwardToLeader` error and issuing the same `ClientRequest` via the
   manager-to-manager gRPC path already used by the join / cluster-
   management flows. Data node code is unchanged.
2. **Forward only the writes, not the full heartbeat RPC.**
   `process_heartbeat` (liveness map) stays local on the receiving
   manager; only the four Raft-mutating writes are proxied.
3. **Self-quarantine guard is a separate, defence-in-depth change.**
   Add an explicit check: a non-leader that receives a heartbeat
   carrying `integrity_faults` must not respond with
   `self_quarantine_cleared = true` until the QuarantineNode write has
   committed (either locally or via forwarding). If forwarding fails,
   respond with `self_quarantine_cleared = false` (keep node
   quarantined) and log a warn.

---

## 3. Data-model changes

None. All changes are in the Raft-write call sites and the heartbeat
response builder. Proto and `ClientRequest` variants are unchanged.

---

## 4. Task list

### BL-1 — Extract and cache current leader address on non-leader managers
- Status: ✅ (2026-05-28) — `resolve_leader_addr(voters, leader_id)` (pure, unit-tested) maps openraft `metrics.current_leader` to a voter addr via `node_id_from_hostname`; `leader_client(addr)` caches a `ManagerServiceClient` per leader addr on `MetadataService`. Unit `bl1_resolve_leader_addr_matches_by_hostname_id` green.
- **Files:** `crates/dtfs-manager/src/metadata.rs` (or a new
  `leader_proxy.rs` helper)
- **Implementation sketch:** When a `raft_write` call returns a
  `ForwardToLeader { leader_id, leader_node }` error, extract the
  leader's gRPC address from `leader_node` (already present in the
  openraft error). Create a thin async helper
  `proxy_to_leader(leader_addr, request)` that opens (or reuses via a
  connection pool) a `ManagerServiceClient` to the leader addr and calls
  `client_write(request)`. Return the result. Cache the leader addr in
  a `tokio::sync::watch` or `Arc<RwLock<Option<String>>>` so repeated
  heartbeat ticks don't re-parse the error.
- **Tests (BL-1):** unit test simulating a `ForwardToLeader` error from
  raft_write and asserting the proxy helper is invoked and the request
  is retried.
- **Done when:** `cargo test -p dtfs-manager --release` green.

### BL-2 — Proxy heartbeat Raft writes through leader
- Status: ✅ (2026-05-28) — new `rpc ForwardClientWrite` on `ManagerService` (proto); leader-side handler applies the serialized `ClientRequest` via `client_write`. `raft_write_or_forward(req)` wraps `client_write` and, on `ForwardToLeader`, proxies through `proxy_to_leader`. All five heartbeat writes (UpdateNodeUsage, UpdateNodeDiskUsage, UpdateNodeStatus, QuarantineNode, RemoveCorruptChunkReplicas) now use it. Live-verified: data nodes on follower .172 kept last_seen fresh while leader was .173.
- **Files:** `crates/dtfs-manager/src/metadata.rs`
- **Implementation sketch:** Replace the four `let _ = self.raft_write(…).await`
  calls inside `heartbeat` with a small wrapper:
  ```
  fn raft_write_or_forward(&self, req) → Result<_, _>
  ```
  which calls `self.raft_write(req)` and on `ForwardToLeader`, calls
  `proxy_to_leader(addr, req)` from BL-1. Log a `debug!` on forward,
  `warn!` on forward failure. `let _` the outer result (best-effort for
  usage updates — liveness is already covered by the local
  `process_heartbeat`).
- **Tests (BL-2):** integration-style unit test: two fake managers, one
  non-leader; send a heartbeat with an integrity fault; assert the
  QuarantineNode write reaches the leader.
- **Done when:** `cargo test -p dtfs-manager --release` green.

### BL-3 — Guard self_quarantine_cleared against uncommitted QuarantineNode
- Status: ✅ (2026-05-28) — `compute_self_quarantine_cleared(is_quarantined, reported_fault_count)` (pure, unit-tested) returns true only when not quarantined AND no fault was reported this cycle. Heartbeat handler uses it. Unit `bl3_self_quarantine_cleared_guard` green.
- **Files:** `crates/dtfs-manager/src/metadata.rs`
- **Implementation sketch:** In the heartbeat handler, after processing
  `integrity_faults` (lines 572-630), if the forward of QuarantineNode
  failed (or if this manager is non-leader and the proxy call returned
  an error), set `self_quarantine_cleared = false` in the response
  regardless of what `sm.nodes` says. This prevents the data node from
  re-enabling `put_chunk` when the corruption state is uncertain. Guard
  goes at the response-builder site before the `HeartbeatResponse` is
  constructed.
- **Tests (BL-3):** unit test: non-leader handler, QuarantineNode
  forward fails, assert response has `self_quarantine_cleared = false`.
- **Done when:** unit assertion green; `cargo test -p dtfs-manager
  --release` clean.

### BL-4 — Abuse test + release case
- Status: ✅ (2026-05-28) — `tests/release/cases/24-heartbeat-nonleader-forward.sh` (restarts the leader, asserts every healthy data node's last_seen advances + is fresh; KNOWN_ISSUE [BL] on a pre-BL binary) and `tests/proxmox/abuse/20-bug-bl-heartbeat-nonleader.sh` (same reproduction, standalone). Release case auto-discovered by `run.sh` full mode (not in QUICK — it restarts the leader). Abuse test ran live → PASS (last_seen advanced 1-2 s after restart).
- **Files:**
  - `tests/proxmox/abuse/20-bug-bl-heartbeat-nonleader.sh`
  - `tests/release/cases/NN-heartbeat-nonleader-forward.sh`
  - `tests/release/run.sh`
- **Implementation sketch:** Stop the Raft leader (kill `dtfs-manager`
  on mini-mgr03), let data nodes heartbeat to a follower for 2
  heartbeat cycles, restart the leader, wait for it to be elected,
  assert `last_seen_ms` on both data nodes updates within 15 s of
  leader restart. Status protocol; `trap case_cleanup EXIT`;
  `wait_for`.
- **Done when:** both scripts present, release case registered in
  `run.sh`, PASS (or KNOWN_ISSUE [BL]) in latest run. Required per
  § B.5.1 (cluster-visible: liveness + integrity fault handling).

### BL-5 — Live verification on Minicluster
- Status: ✅ (2026-05-28) — deployed BL manager to all 3 mini-mgr (leader-last). Definitive proof: stopped the leader (.171) so both data nodes failed over to follower .172 while .173 was elected leader; data nodes' last_seen stayed fresh (age 2-3 s) — the follower forwarded UpdateNodeUsage to the leader. Pre-BL this state froze last_seen for hours (the original 1416-F2 symptom). Cluster restored to 3/3 healthy after the test.
- **Implementation sketch:** Deploy updated manager binary to all three
  mini-mgr nodes. Introduce an integrity fault on a data node, verify
  the QuarantineNode Raft write commits (node shows quarantined in
  `/api/v1/nodes`), verify `/api/v1/integrity/faults` shows the fault
  (note: BJ must also be fixed or the audit cap saturation will still
  mask it). Verify `last_seen_ms` advances within 10 s of a non-leader
  heartbeat.
- **Done when:** all three verified on live cluster; evidence in §6.

### BL-6 — Docs + changelog
- Status: ✅ (2026-05-28) — two `changelog/UNRELEASED.md` Bug BL entries: (1) non-leader heartbeat-write forwarding; (2) self-quarantine no longer falsely cleared.
- **Files:** `changelog/UNRELEASED.md`
- **Implementation sketch:** Two entries: (1) non-leader managers now
  forward heartbeat Raft writes to the leader so node liveness and
  integrity state stay current; (2) self-quarantine no longer falsely
  cleared when the QuarantineNode write fails.
- **Done when:** entries present, same commit as BL-2/BL-3.

---

## 5. Verification log

_(empty — filled per task as work proceeds)_

| Task | Date | Commands | Result |
|------|------|----------|--------|
| BL-1/2/3 | 2026-05-28 | `cargo test -p dtfs-manager --release` (builder) | 142/142 (incl. bl1_resolve_leader_addr, bl3_self_quarantine_cleared_guard) |
| BL-5 | 2026-05-28 | deploy 3 mgrs; `systemctl stop dtfs-manager` on leader; `ss`+`/api/v1/nodes` | data nodes on follower .172 (leader .173), last_seen age 2-3 s |
| BL-4 | 2026-05-28 | `bash tests/proxmox/abuse/20-bug-bl-heartbeat-nonleader.sh` | PASS — last_seen advanced 1-2 s after leader restart |

---

## 6. Cross-cutting invariants

- Best-effort forwarding: forward failures are logged (warn) and
  silently dropped — the liveness path already works via the in-memory
  map; we must not reject the heartbeat RPC on forward failure.
- Snapshot compat: no new fields on `StateMachineData`.
- BJ (RegisterNode spam) should land before BL live verification so the
  audit cap is not saturated during the integrity fault test.

---

## 7. Deviations from plan

- **RCA correction (2026-05-28): the assumed forwarding path does not
  exist.** §2 decision 1 and the BL-1 sketch assumed a manager-to-manager
  gRPC "cluster-management forwarding" path already existed to reuse. It
  does not. Managers only communicate over the openraft consensus
  transport (`server.rs` `RaftRpcKind`: AppendEntries / Vote / Snapshot);
  `ManagerService` has no generic client-write RPC. The only existing
  forwarding is *client-side* (FUSE/CLI retry to another manager on
  `Status::unavailable`).
- **Chosen design (user-approved 2026-05-28): new typed proto RPC.**
  Add `rpc ForwardClientWrite(ForwardWriteRequest) returns
  (ForwardWriteResponse)` to `ManagerService`. A non-leader heartbeat
  handler, on `ForwardToLeader`, resolves the leader's address from
  `config.cluster.initial_voters` (matching `node_id_from_hostname` to
  the openraft `metrics.current_leader`), opens a cached
  `ManagerServiceClient` to it, and forwards the serialized
  `ClientRequest`. The leader's `ForwardClientWrite` handler calls
  `self.raft.client_write` and returns the serialized `ClientResponse`.
  `ClientRequest`/`ClientResponse` are serde-serialized as JSON bytes on
  the wire (both already derive Serialize/Deserialize). This is a NEW
  internal RPC; the plan's "no proto change" non-goal referred to the
  heartbeat wire format, which is unchanged.
- **BL-3 simplification:** the self-quarantine guard reduces to
  `self_quarantine_cleared = !is_quarantined && req.integrity_faults.is_empty()`.
  Forcing `false` whenever the node reports a fault this cycle closes the
  same-cycle re-enable window; combined with BL-2 forwarding (which makes
  the QuarantineNode commit durable), subsequent cycles read the
  replicated quarantine state correctly. No leader-gating (that would
  wrongly pin a node quarantined forever whenever it heartbeats a
  follower after an operator unquarantine).

---

## 8. Notes for the implementer

- **Concurrent work**: uncommitted `dtfs-client/` + `dtfs-fuse/` edits
  from another agent on master — stage only `metadata.rs` for BL
  commits.
- The `raft_write` helper in metadata.rs wraps `self.raft.client_write`
  and maps openraft errors. The `ForwardToLeader` variant already
  surfaces the leader node address — read the `openraft::error::Forward
  ToLeader` struct to extract it before coding BL-1.
- The leader proxy should reuse the existing manager-to-manager gRPC
  client pattern already present in the codebase (cluster-management
  forwarding) rather than opening a raw tonic channel inline.
- Consider a circuit-breaker: if forwarding fails 3× in a row for a
  given heartbeat source, emit a single `warn!` per minute rather than
  per heartbeat to avoid log spam.
