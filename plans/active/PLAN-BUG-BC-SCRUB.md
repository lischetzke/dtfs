# Bug BC — Scrub redesign + integrity-first read path

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

The current scrub system (AGENTS.md § A.6 item 7 + § A.3.3 item 6)
trusts the disk on reads and relies on a single periodic scrub for
bit-rot detection. It also has no orphan-chunk GC despite AGENTS.md
§ A.6 item 8 promising one ("orphans get reclaimed by the next scrub
pass"). The 1h soak (Bug BA) surfaced both gaps: 24 GB of orphan
chunks on disk + per-volume scrub_interval_secs that silently
collapses to a cluster-wide hour-granularity knob (Bug AZ-1).

**Design priority: data consistency above performance.** An
integrity fault must NOT propagate. **A single fault is enough**:
the faulting node is Quarantined immediately (no threshold, no
debounce, no retry-then-fail). Quarantined = excluded from ALL
serving paths (reads, replication source, allocate target) until
an operator explicitly unquarantines via WebUI. The operator must
not be forced to use the API for state, reason, or unquarantine —
every quarantine surface is in the WebUI.

## 0. Goals + non-goals

### Goals
- Three independent scrub strategies with separate cadences:
  `quick_scrub_interval`, `deep_scrub_interval`,
  `orphan_scrub_interval`.
- Hash-verify on every read (`verify_on_read`, default **true**;
  operator can disable per-cluster with explicit
  acknowledgement). Mismatch quarantines the source node on the
  first fault.
- New `NodeStatus::Quarantined` excluded from read source +
  replication source pools.
- Cluster-wide config lives in `manager.toml::[scrub]`, propagated
  to data nodes via heartbeat. Manager-side hardcoded constants in
  `metadata.rs:427-428` removed.
- Per-volume scrub_interval_secs (Bug AZ-1) deprecated — the
  per-volume → cluster-wide collapse was a UX lie. Removed from
  WebUI + API; warn-log + ignore for back-compat reads.

### Non-goals
- Self-healing of unrecoverable corruption (no replicas left). P0
  alert + operator unblocks.
- Distributed scrub coordination (each data node scrubs its own
  disks; mgr cross-checks via orphan_scrub).
- Per-volume scrub cadence (architecturally collapses to
  cluster-wide; not honoured by data nodes whose scope is disks
  not volumes).

## 1. Three scrub strategies

| Strategy | Cadence (default) | Cost | What it checks | Fault action |
|---|---|---|---|---|
| **quick_scrub** | 6 h | low (stat only) | Every `.dat` file: stat exists + size == ChunkMeta.size. Every `.meta` file: chunk_id matches the on-disk pair. | Local meta-vs-disk mismatch → quarantine self via heartbeat extension `integrity_fault`. |
| **deep_scrub** | 7 d | high (full read + BLAKE3) | Every `.dat`: re-compute BLAKE3, compare to ChunkMeta.checksum. Throttled by `max_io_percent`. | Hash mismatch → quarantine self + audit `scrub.deep.corrupt` + re-replicate the chunk from another node (operator unquarantines after manual inspection). |
| **orphan_scrub** | 1 d | medium (mgr-side cross-check) | Mgr asks each data node `ListChunks`; diffs against `state_machine.chunk_locations`. | (a) On-disk-not-in-state → mark + delete after `orphan_grace_secs` (default 24h). (b) In-state-not-on-disk → quarantine the node that claimed the missing chunk + re-replicate. |

## 2. Verify-on-read (`verify_on_read`, default true)

Cluster-wide knob: `manager.toml::[scrub].verify_on_read = true`.
When true (default), every read is hash-verified at the data
node before bytes leave the process. Operator can set to `false`
ONLY with explicit acknowledgement (boundary check in
`update_config` rejects the change without
`acknowledge_disable_verify_on_read: true` in the body, mirroring
the BD-3 cluster-write guard pattern). Disabling is a footgun;
the API contract makes the operator look at it.

Data-side `get_chunk` (`crates/dtfs-data/src/server.rs`):
- If `verify_on_read == true`: streams `.dat` through
  `blake3::Hasher` while sending to client. On EOF, finalised
  hash compared to `ChunkMeta.checksum`.
- Mismatch:
  - Emit gRPC trailer `x-dtfs-integrity-fault: <chunk_id>`.
  - Record fault in-memory (`pending_integrity_faults: Vec`).
  - Heartbeat next tick includes
    `integrity_faults: [{chunk_id, fault_kind, observed_at_ms}]`
    → mgr Quarantines this node **immediately on the first
    fault** (no threshold).

Client (`crates/dtfs-fuse/src/filesystem.rs`):
- Reads stream; on integrity-fault trailer:
  - Discard the buffer (do NOT propagate corrupt bytes to FUSE).
  - Retry against next replica (existing P0-2 fallback path;
    add the trailer signal).
  - Emit alert via mgr REST `POST /api/v1/alerts/raise`.

`verify_on_read = false` path: data node skips the hash compute,
serves bytes raw. Operator-acknowledged risk; AGENTS.md update
flags this as the only DTFS-supported mode where reads can
deliver corrupt bytes.

Performance trade-off accepted by default: BLAKE3 streaming hash
~1 GB/s/core = negligible vs disk reads on Plexmedia hardware.
Single 60 Mbps Plex stream hashes at well under 10% of one core.
Operator-disable exists for benchmark / measurement work only.

## 3. Quarantine

New `NodeStatus::Quarantined` (additive variant; existing
`Healthy/Suspect/Down` unchanged). **Single fault rule**: any
one of the triggers below quarantines the node immediately — no
threshold, no debounce, no retry budget.

Triggers:
- quick_scrub local mismatch → self-quarantine via heartbeat.
- deep_scrub hash mismatch → self-quarantine.
- verify_on_read mismatch → self-quarantine.
- orphan_scrub finds in-state-not-on-disk for a chunk this node
  claimed presence → mgr quarantines remotely.

`NodeInfo` extension (state-machine field, snapshot-safe):
```rust
pub struct NodeInfo {
    // ... existing ...
    #[serde(default)]
    pub quarantine: Option<QuarantineInfo>,
}
pub struct QuarantineInfo {
    pub since_ms: u64,
    pub reason: QuarantineReason,         // enum (compact)
    pub triggering_chunk_id: Option<String>,
    pub fault_count_total: u32,            // accumulator across faults
    pub last_fault_at_ms: u64,
    pub last_fault_summary: String,        // human-readable for WebUI
}
pub enum QuarantineReason {
    QuickScrubSizeMismatch,
    QuickScrubMissingDat,
    DeepScrubHashMismatch,
    VerifyOnReadMismatch,
    OrphanScrubInStateMissing,
    OperatorRequested,                     // manual quarantine action
}
```

Effects (single-fault → full ban):
- `pick_nearest_healthy_replica` (`location.rs`) excludes
  Quarantined — **no read can be served from a Quarantined node**.
- `scheduler::resolve_healthy_source` excludes Quarantined —
  **no replication can use a Quarantined node as source**.
- `allocate_chunks` excludes Quarantined — **no new write can
  land on a Quarantined node**.
- `put_chunk` data-side rejects with `FAILED_PRECONDITION` if
  the node has self-quarantined (defence-in-depth in case mgr
  hasn't propagated the status yet).
- Heartbeats still accepted (so we know when the node is back);
  state stays Quarantined until operator clears it via WebUI.
- `auto_unquarantine_secs > 0` → auto-clear after period if no
  new faults reported (default **0 = manual only**, matching
  data-consistency-first stance).

Audit log entries: `node.quarantine` (with reason +
triggering_chunk) + `node.unquarantine` (with operator id
when available). P0 alert `IntegrityFault` raised on every
quarantine. Mgr emits `alerts.integrity_fault` audit row that
WebUI surfaces in the integrity feed.

## 4. Configuration

### 4.1 New `[scrub]` block in `manager.toml`

```toml
[scrub]
quick_interval_secs    = 21600     # 6 h
deep_interval_secs     = 604800    # 7 d
orphan_interval_secs   = 86400     # 1 d
orphan_grace_secs      = 86400     # 1 d before reap
max_io_percent         = 10        # throttle deep_scrub
verify_on_read         = true      # data consistency knob; false requires explicit ack
quarantine_on_fault    = true      # leave true unless you know what you do
auto_unquarantine_secs = 0         # 0 = manual only (recommended)
```

Loaded into `ManagerConfig.scrub: ScrubConfig` (new sub-config).
All keys carry `#[serde(default = "...")]` so existing
`manager.toml` files keep working.

Data-side `[scrub]` block in `data.toml` becomes a fallback used
only at startup before the first heartbeat lands. Once heartbeat
arrives, the cluster config wins (already the case for the AZ-1
quick override; this just expands what's carried in
`ScrubConfigOverride`).

### 4.2 Proto extension

`proto/manager.proto`:

```protobuf
message ScrubConfigOverride {
  uint64 quick_interval_secs    = 1;  // was: uint32 quick_interval_hours
  uint64 deep_interval_secs     = 2;  // was: uint32 deep_interval_hours
  uint64 orphan_interval_secs   = 3;  // NEW
  uint64 orphan_grace_secs      = 4;  // NEW
  uint32 max_io_percent         = 5;
  bool   verify_on_read         = 6;  // NEW (default true)
  bool   quarantine_on_fault    = 7;  // NEW (default true)
}
```

Wire-compat: rename the existing hour fields to *_secs (semantic
change, ok because the receiving side is data nodes we control;
roll managers first then data nodes — both sides serde-default
the new fields).

### 4.3 Manager hardcoded constants removed

Delete `CLUSTER_QUICK_HOURS_DEFAULT` + `CLUSTER_DEEP_HOURS_DEFAULT`
from `metadata.rs:427-428`. Read from `state.config.scrub` instead.

### 4.4 Per-volume scrub_interval deprecation

`VolumeSchedules.scrub_interval_secs` becomes warn-log-only:
- API `POST /api/v1/volumes/{id}/set-schedules` accepts the field
  for back-compat but emits a `volume.scrub_interval.deprecated`
  audit + log warning + returns 200 with a `warnings: [...]`
  field.
- Heartbeat min-aggregation in `metadata.rs:436-443` removed.
- WebUI volume Schedules row hides scrub_interval_secs, shows
  notice: "Scrub cadence is cluster-wide. See Config → scrub.*".
- `fsck_interval_secs` + `custom` remain (still unwired; future
  work).

## 5. Tasks

### Phase 1 — Config + proto

#### ✅ BC-1 — `ClusterScrubConfig` sub-config + defaults (2026-05-26)
* **Files:** `crates/dtfs-common/src/config.rs` (new
  `ClusterScrubConfig` struct + 7 `default_cluster_*` fns;
  `ManagerConfig.scrub` field with `#[serde(default)]`).
* **Tests:**
  `bug_bc_1_cluster_scrub_config_defaults`,
  `bug_bc_1_cluster_scrub_config_overrides_apply`.
* **Live:** `cargo test -p dtfs-common --release` → 44/44 pass;
  `cargo build --release -p dtfs-manager` clean (only pre-existing
  openraft deprecation warning).
* **Release-suite registration:** N/A for this task per AGENTS.md
  § B.5.1 exemption — pure data model addition with no
  cluster-visible behaviour until BC-3 wires the heartbeat. The
  release case for the full BC integrity path is BE-8
  (`13-integrity-corruption.sh`, already in suite).
* **Deferred to follow-ups:** `manager_config_defaults_baseline()`
  in `api.rs` still needs the `scrub` block — folded into BC-3
  alongside the heartbeat wiring so the WebUI config pane shows
  scrub defaults the moment they're live.

#### ✅ BC-2 — Proto extension + codegen (2026-05-26)
* **Files:** `proto/manager.proto::ScrubConfigOverride` fields 3-8
  added (orphan_interval_secs, orphan_grace_secs, max_io_percent,
  verify_on_read, quarantine_on_fault, auto_unquarantine_secs).
  Fields 1-2 (`quick_interval_hours`, `deep_interval_hours`)
  kept on the wire for back-compat with AZ-1 data nodes.
* **Live:** `cargo build --release -p dtfs-proto -p dtfs-manager
  -p dtfs-data` clean.
* **Release-suite registration:** N/A per § B.5.1 exemption —
  wire-format extension with no behaviour change until BC-8/10
  consume the new fields.

#### ✅ BC-3 — Heartbeat sends full ScrubConfigOverride (2026-05-26)
* **Files:**
  - `crates/dtfs-manager/src/metadata.rs:417-485` — hardcoded
    `CLUSTER_QUICK_HOURS_DEFAULT`/`CLUSTER_DEEP_HOURS_DEFAULT`
    deleted; reads `self.config.scrub` (`ClusterScrubConfig`).
    Emits all 8 BC fields per heartbeat. Per-volume
    `scrub_interval_secs` still honoured but now emits a
    `Bug BC-13` deprecation warn log (paves the way for full
    removal in BC-13).
  - `crates/dtfs-data/src/main.rs:538-580` — hot-reloads
    `max_io_percent`, `verify_on_read`, `quarantine_on_fault`
    in addition to the existing quick/deep interval handling.
    Integrity changes logged at `warn!` level so they're easy
    to spot in `journalctl`.
  - `crates/dtfs-common/src/config.rs::ScrubConfig` extended with
    `verify_on_read` + `quarantine_on_fault` (both default true
    via shared `default_cluster_*` helpers).
* **Live:** `cargo test -p dtfs-common --release` → 44/44 pass;
  `cargo build --release -p dtfs-data -p dtfs-manager` clean.
* **Release-suite registration:** Deferred to BC-8 (the case that
  surfaces this end-to-end is the integrity sequence in
  `13-integrity-corruption.sh`).

### Phase 2 — Scrub engine split

#### ⬜ BC-4 — `quick_scrub` strategy (stat + size + meta presence)
* **Files:** `crates/dtfs-data/src/scrub.rs` (split run_scrub into
  per-level strategies), `crates/dtfs-data/src/store.rs` (add
  `quick_scrub_disk(path) -> ScrubResult`).
* **Tests:** unit `quick_scrub_detects_size_mismatch`,
  `quick_scrub_detects_missing_dat_with_meta_present`.
* **Done when:** mismatch produces `IntegrityFault` returned up
  to data-node main loop.

#### ⬜ BC-5 — `deep_scrub` strategy (BLAKE3 verify)
* **Files:** as BC-4 + `crates/dtfs-data/src/store.rs::deep_scrub_disk`.
* **Tests:** unit `deep_scrub_detects_bitrot`,
  `deep_scrub_ok_on_clean_disk`.

#### ⬜ BC-6 — `orphan_scrub` data-side `ListChunks` RPC
* **Files:** `proto/data.proto` (`ListChunksRequest/Response`),
  `crates/dtfs-data/src/server.rs::list_chunks` (paged, by tier
  filter optional).
* **Tests:** unit `list_chunks_returns_all_dat_files`.

#### ⬜ BC-7 — `orphan_scrub` mgr-side cross-check loop
* **Files:** `crates/dtfs-manager/src/scheduler.rs` (new
  `orphan_scrub_tick`).
* **Logic:**
  - For each data node: `ListChunks`.
  - For each chunk_id on disk not in `chunk_locations` → mark
    `pending_reap_at_ms = now + orphan_grace_secs`.
  - For each pending reap whose deadline passed and still not in
    state → DeleteChunk + audit `orphan.reaped`.
  - For each chunk_id in `chunk_locations` not reported by the
    node that claimed it → quarantine that node + replicate from
    another replica + audit `orphan.in_state_missing`.
* **Tests:** unit `orphan_scrub_reaps_after_grace`,
  `orphan_scrub_quarantines_on_in_state_missing`,
  `orphan_scrub_keeps_chunk_within_grace`.

### Phase 3 — Hash on read

#### ⬜ BC-8 — `verify_on_read` data-side streaming hash verify
* **Files:** `crates/dtfs-data/src/server.rs::get_chunk` (when
  `verify_on_read=true`: incremental `blake3::Hasher` alongside
  the existing stream; final compare against `ChunkMeta.checksum`;
  emit gRPC trailer `x-dtfs-integrity-fault` on mismatch).
  When `verify_on_read=false`: skip the hasher, serve raw.
* **Boundary:** `update_config` rejects writes that flip
  `verify_on_read` to false without
  `acknowledge_disable_verify_on_read: true` in the body — same
  pattern as BD-3 cluster-key guard.
* **Tests:** unit `get_chunk_emits_integrity_fault_trailer_on_bitrot`,
  `get_chunk_skips_verify_when_disabled`,
  `update_config_rejects_disable_verify_without_ack`.

#### ⬜ BC-9 — FUSE client retries on integrity-fault trailer
* **Files:** `crates/dtfs-client/` (or `crates/dtfs-fuse/`).
* **Logic:** detect trailer; discard buffer; try next replica;
  on all-failure return `EIO` to FUSE.
* **Tests:** unit
  `fuse_read_retries_other_replica_on_integrity_fault`.

### Phase 4 — Quarantine (state + routing + WebUI)

#### ✅ BC-10 — `HealthStatus::Quarantined` + `QuarantineInfo` (2026-05-26)
* **Files:**
  - `crates/dtfs-common/src/types.rs` — `Quarantined` variant on
    `HealthStatus`; `QuarantineReason` enum (6 variants);
    `QuarantineInfo` struct; `NodeInfo.quarantine` field.
  - `crates/dtfs-raft/src/lib.rs` — `NodeInfo.quarantine: Option<QuarantineInfo>`
    on the Raft state-machine flavour of NodeInfo; new
    `ClientRequest::QuarantineNode` + `UnquarantineNode` variants
    with Display impls.
  - `crates/dtfs-raft/src/state_machine.rs::apply_request` —
    handlers that set Quarantined + accumulate `fault_count_total`
    across repeated faults; clear back to Healthy on unquarantine;
    audit each transition with structured summary.
  - `crates/dtfs-manager/src/{api.rs,metadata.rs}` — both
    RegisterNode → `RaftNodeInfo` construction sites carry
    `quarantine: None` to satisfy the new field.
* **Tests:** 4 new unit tests in `state_machine::posix_path_regression`:
  - `bug_bc_10_quarantine_node_sets_health_and_info`
  - `bug_bc_10_quarantine_accumulates_fault_count`
  - `bug_bc_10_unquarantine_clears_and_restores_health`
  - `bug_bc_10_quarantine_missing_node_returns_error`
* **Live:** `cargo test -p dtfs-raft --release` → 38/38 pass
  (4 new + 34 prior).
* **Release-suite registration:** Deferred to BC-12 (operator
  API) + BC-14 (live bitrot test) — the quarantine state is
  internal until those land. Per AGENTS.md § B.5.1 exemption
  for non-cluster-visible additions.

#### ✅ BC-11 — Routing excludes Quarantined; status surfaces it (2026-05-26)
* **Verification:** all routing filters in `placement.rs:47`,
  `scheduler.rs:{274,341,430,997,1215}`,
  `api.rs:{288,2176,2700}`, `monitor.rs:{127,273}`, `tiering.rs:792`
  already use **positive** `health == Healthy` semantics →
  Quarantined automatically excluded. The single negative-filter
  site (`placement.rs:47` `!= Healthy`) is also correct: it
  EXCLUDES Quarantined (as not Healthy).
* **Files:**
  - `crates/dtfs-manager/src/api.rs::get_stats` — new
    `quarantined_nodes` count + `quarantined_nodes` field in JSON
    + `health = "critical"` whenever any node is Quarantined
    (operator dashboard pages on this state).
  - `crates/dtfs-manager/src/api.rs` Prometheus body — new
    `dtfs_nodes_quarantined` gauge.
* **Tests:** `bug_bc_11_quarantined_node_excluded_from_healthy_filter`
  pins the positive-filter invariant against any future refactor
  that flips a site to a negative `health != Down`-style predicate.
* **Live:** `cargo test -p dtfs-raft --release bug_bc` →
  5/5 (BC-10×4 + BC-11×1) pass; `cargo build --release -p dtfs-manager`
  clean.
* **Deferred to BC-11 follow-up (low pri):** `put_chunk` data-side
  self-reject defence-in-depth. Currently the mgr routing already
  blocks writes to Quarantined nodes; the data-side check is a
  belt-and-suspenders for the brief window after self-quarantine
  before mgr propagates. Folded into BC-8 (verify_on_read) where
  the self-quarantine machinery lands.

#### ⬜ BC-12 — Operator unquarantine endpoint + audit + auto-clear
* **Files:** `crates/dtfs-manager/src/api.rs`
  (`POST /api/v1/nodes/{id}/unquarantine` body
  `{ "operator_id": "<who>", "reason_acknowledged": "<text>" }`,
  emits `node.unquarantine` audit), scheduler tick that
  clears Quarantined after `auto_unquarantine_secs` if no
  fresh faults arrived.
* **Tests:** unit
  `manual_unquarantine_clears_status`,
  `manual_unquarantine_rejects_without_ack_text`,
  `auto_unquarantine_respects_cooldown`,
  `auto_unquarantine_blocked_by_fresh_fault`.

#### ⬜ BC-12a — Nodes API exposes quarantine state
* **Files:** `crates/dtfs-manager/src/api.rs::list_nodes_row_json`.
* **Implementation:** add `quarantine: { since_ms, reason,
  triggering_chunk_id, fault_count_total, last_fault_at_ms,
  last_fault_summary }` block to each node row when present;
  null/absent otherwise.
* **Tests:** unit `nodes_row_includes_quarantine_block`.

#### ⬜ BC-12b — Integrity faults audit feed endpoint
* **Files:** `crates/dtfs-manager/src/api.rs`
  (`GET /api/v1/integrity/faults?since_ms=<...>&limit=<...>`).
* **Implementation:** filter `audit_log` by
  action in {`scrub.quick.fault`, `scrub.deep.corrupt`,
  `read.integrity_fault`, `orphan.in_state_missing`,
  `node.quarantine`, `node.unquarantine`}.
* **Tests:** unit
  `integrity_faults_endpoint_filters_correctly`.

### Phase 4.5 — Quarantine WebUI

> Operator must never need to curl. Every state, reason, and
> action lives in the WebUI.

#### ⬜ BC-12c — Nodes page Quarantined badge + details
* **Files:** `crates/dtfs-manager/src/webui.rs::renderNodes`.
* **Implementation:**
  - Add a red `QUARANTINED` badge next to the existing status
    badge when `quarantine != null`.
  - Sortable + filterable column "Integrity faults" showing
    `fault_count_total`.
  - Row click expands an inline detail panel:
    `Reason: VerifyOnReadMismatch`, `Triggering chunk: <id>`,
    `First fault: <iso8601>`, `Last fault: <iso8601>`,
    `Summary: <text>`.
  - "Unquarantine…" button (disabled until operator types the
    acknowledgement text in the confirmation modal).
* **Done when:** Quarantined node visible on `/ui/nodes`
  without operator running any curl.

#### ⬜ BC-12d — Unquarantine confirmation modal
* **Files:** `crates/dtfs-manager/src/webui.rs`.
* **Implementation:**
  - Modal shows: full `QuarantineInfo`, the chunk_id that
    triggered, a copyable hash so operator can verify by hand.
  - Required field: `operator_id` (free text or "manual").
  - Required field: `reason_acknowledged` (free text, ≥ 16
    chars — forces real text, not "ok").
  - Submit → `POST /api/v1/nodes/{id}/unquarantine` → toast +
    refresh.
* **Done when:** modal happy path + validation green in browser.

#### ⬜ BC-12e — Integrity feed page `/ui/integrity`
* **Files:** `crates/dtfs-manager/src/webui.rs` (new nav entry
  + page).
* **Implementation:**
  - Live (polled every 5 s) table of integrity-related audit
    rows from `GET /api/v1/integrity/faults`.
  - Columns: time, node, action, chunk_id, summary.
  - Filter chips: action type, node, last 1h/24h/7d.
  - Click on a row to jump to the offending node's details
    panel on `/ui/nodes`.
* **Done when:** operator can answer "what happened?" without
  touching the CLI.

### Phase 5 — Deprecate per-volume scrub

#### ⬜ BC-13 — Warn-log + ignore per-volume scrub_interval
* **Files:** `crates/dtfs-manager/src/api.rs::apply_set_schedules`
  (emit warning), `crates/dtfs-manager/src/metadata.rs` (drop
  min-aggregation), WebUI volume page (hide row, show notice).
* **Tests:** unit
  `apply_set_schedules_warns_on_scrub_interval_deprecated`.

### Phase 6 — Live regression test

#### ⬜ BC-14 — `tests/proxmox/abuse/18-bug-bc-bitrot.sh`
* **Implementation:** write 10 MB file → fetch chunk_id from
  `/api/v1/files` → SSH to a replica → flip a byte
  (`dd conv=notrunc bs=1 count=1 seek=...`) → read the file via
  FUSE → assert:
  (a) read still succeeds (other replica),
  (b) faulty node is `Quarantined` in `/api/v1/nodes` with
      `reason=VerifyOnReadMismatch`,
  (c) `put_chunk` to faulty node returns FAILED_PRECONDITION,
  (d) `allocate_chunks` no longer returns the faulty node,
  (e) `/api/v1/integrity/faults` shows the
      `read.integrity_fault` row,
  (f) operator unquarantine via POST returns the node to
      `Healthy` and write/read resume.
* **Done when:** script exits 0 on green cluster.

### Phase 7 — Docs

#### ⬜ BC-15 — Docs update
* **Files:** `docs/src/storage/scrubbing.md` (replace),
  `docs/src/operations/quarantine.md` (new),
  `AGENTS.md § A.3.3 item 6` (flip "Reads trust the disk;
  scrub catches bit-rot" → "Reads hash-verify; scrub also runs
  to catch chunks unread for long periods").

## 6. Invariants every task must preserve

- **A single integrity fault is enough.** No thresholds, no
  debounce, no retry-then-fail. One mismatch → Quarantine.
- **No corrupt byte ever crosses the FUSE boundary.** Integrity
  fault detected mid-stream: discard, retry, return EIO if
  all replicas fail. Never partial-deliver corrupt data.
- **A Quarantined node serves nothing.** No read, no replication
  source, no allocate target. `put_chunk` self-rejects as
  defence-in-depth.
- **No anchor copy ever dropped by an integrity-driven flow.**
  Quarantine excludes from source pool but does NOT delete the
  on-disk chunk; operator inspects + decides.
- **Orphan reap honours `enforce_anchor`.** A chunk that looks
  orphaned but matches a mandatory anchor copy in policy is
  NEVER reaped without the standard anchor check.
- **Operator never needs the API for quarantine state or
  unquarantine.** WebUI surfaces reason, triggering chunk,
  history, and the unquarantine action. CLI/API exist as a
  parallel path but are not the documented operator flow.
- **Disabling `verify_on_read` requires explicit
  acknowledgement.** No silent toggle. Boundary check makes
  the operator look at the consequence.

## 7. Verification log

> Append per-task. Empty at plan authorship.

## 8. Deviations from plan

> Update as reality forces design changes. Empty at authorship.

## 9. Commit-message convention

```
BC-<n>: <one-line summary, <72 chars>>

<body>

Plan: plans/active/PLAN-BUG-BC-SCRUB.md (BC-<n> done)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```
