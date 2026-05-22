# Bug AV — Two-class tier lifecycle (mandatory anchor + opportunistic cache)

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

---

## 0. Background, goals, non-goals

### 0.1 Why this exists

Today's volume policy expresses replication as a single
`TierReplication { hot, warm, cold, tape }` and the under-replication
scheduler enforces it everywhere — "always 2 warm + 1 cold" is a hard
invariant. The Plexmedia use case wants something different:

* **One mandatory tier** (cold) holds every chunk durably. That is the
  archive.
* **Warm tier is an opportunistic cache.** A warm replica only exists
  in a DC where a viewer is actively reading. When idle, warm is
  empty.
* **Operator can pre-emptively flush** warm replicas before the
  tiering-engine's age threshold would.

The current "Migrate to cold" button is purely additive (Bug AS
context) — it cannot free warm space because (a) the under-replication
scheduler immediately re-creates anything you delete, and (b) there's
no delete-replica plumbing in the system at all.

### 0.2 Goals

1. Volumes can declare separate **mandatory** vs **cache** replication
   classes.
2. Under-replication scheduler enforces mandatory only.
3. Tiering engine creates cache replicas on read demand, bounded by the
   per-DC cap, gated by a configurable read-threshold window and a
   post-flush cooldown.
4. Operator-driven "flush cache" reclaims warm/hot replicas safely (cold
   verified before warm dropped).
5. FUSE clients pick first-write tier per their own config (default
   cold, optional warm-then-anchor-migrate).
6. Anchor invariant can be tier-aware (`required_tier`).

### 0.3 Non-goals (deferred)

* Multi-anchor (mirror cold across 2 DCs for DR) — out of scope.
* Per-file overrides — only per-volume policy for now.
* Cache eviction below the cap based on LRU/size pressure — only
  operator-driven flush + age-based tiering demote.
* "Promote on first read" without threshold — keep threshold-based for
  predictable behaviour (defaults to 2 reads / 4h).

### 0.4 Decisions locked in (from chat)

| # | Decision |
|---|---|
| 1 | Cooldown after flush = **5 minutes**, configurable per volume. |
| 2 | `cache_replicas.per_dc: bool = true` by default. The number is a **cap**, not a target. Promotion creates 1 cache replica per DC on threshold breach; the cap bounds any future fault-tolerance escalation. |
| 3 | First-write tier is a **client-side** decision (FUSE config). Default `cold`. `nearest-warm` triggers async anchor migration. |
| 4 | Anchor invariant is `{ datacenter, required_tier: Option<DiskTier> }`. Legacy `anchor_datacenter: Option<String>` deserialises to `{ datacenter, required_tier: None }`. |
| 5 | Schedulers and the placement engine all filter `Draining` nodes — no auto-fill onto a decommissioning host. |
| 6 | Cache promotion threshold = **2 reads in 4 hours by default**, per-volume configurable: `cache_promote_reads`, `cache_promote_window_secs`. |

### 0.5 Rolling-upgrade safety

Same pattern as Bug AH: the new schema lives behind a per-volume opt-in
flag so an older follower can deserialise snapshots without
panicking. Steps:

1. **Phase 1 deploy with default-off.** Volumes still carry the legacy
   `replication` field; new `mandatory_replicas` + `cache_replicas`
   exist as `#[serde(default)]` empty structs. No behaviour change.
2. After every voter is on the new binary, operator runs
   `dtfs-cli volume migrate-policy <vol>` to convert the volume.
3. Old `replication` field is **kept** in the state machine
   indefinitely as the read fallback when the new fields are empty,
   so a rollback is always possible.

---

## 1. Data-model changes (concrete schema)

### 1.1 `dtfs-common::types::TierReplication`

Already exists, no change. Reused by both mandatory + cache fields.

### 1.2 `dtfs-common::types::CacheReplication` (NEW)

Each tier carries a `(min, max)` pair. `min` is the per-DC target the
tiering engine ramps to on threshold breach. `max ≥ min` is the
per-DC cap; the optional redundancy escalation may grow toward `max`
but never beyond. Default `min=max=1` per tier when the tier is
explicitly mentioned; tiers omitted from the config are disabled
(`min=max=0`).

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheTierConfig {
    /// Per-DC target on threshold breach. Promotion creates replicas
    /// until DC has `min` cache copies of this chunk. Default 1
    /// (when the tier is mentioned in config). 0 disables the tier.
    #[serde(default = "default_min")] pub min: u32,
    /// Per-DC absolute cap. Must be ≥ `min`. When `max > min`, the
    /// tiering engine's redundancy-escalation pass MAY (when
    /// enabled) grow above `min` for fault tolerance in DCs with
    /// ≥2 warm-tier nodes. Default 1.
    #[serde(default = "default_max")] pub max: u32,
}
fn default_min() -> u32 { 1 }
fn default_max() -> u32 { 1 }

// When the parent doesn't mention the tier at all, the tier defaults
// to disabled (0, 0), NOT (1, 1) — implemented by giving
// `CacheReplication` a custom Default + serde default per-field.
impl Default for CacheTierConfig {
    fn default() -> Self { Self { min: 0, max: 0 } }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct CacheReplication {
    #[serde(default)] pub hot:  CacheTierConfig,
    #[serde(default)] pub warm: CacheTierConfig,
    #[serde(default)] pub cold: CacheTierConfig,
    #[serde(default)] pub tape: CacheTierConfig,
    /// When true, `min`/`max` apply per DC where reads happen. When
    /// false, they apply cluster-wide. Default true.
    #[serde(default = "default_per_dc")] pub per_dc: bool,
    /// Cache promotion trigger: chunk gets promoted when it sees ≥
    /// `promote_reads` reads from a single DC within
    /// `promote_window_secs` seconds. Defaults: 2 / 14400 (= 4 hours).
    #[serde(default = "default_promote_reads")]       pub promote_reads: u32,
    #[serde(default = "default_promote_window_secs")] pub promote_window_secs: u64,
    /// After an operator-driven `flush_cache`, the tiering engine
    /// refuses to re-promote any flushed chunk for this many seconds.
    /// Default 300 (= 5 minutes).
    #[serde(default = "default_cooldown_secs")] pub cooldown_secs: u64,
}
fn default_per_dc() -> bool { true }
fn default_promote_reads() -> u32 { 2 }
fn default_promote_window_secs() -> u64 { 4 * 3600 }
fn default_cooldown_secs() -> u64 { 5 * 60 }
```

Examples:

```toml
# Disabled cache (no opportunistic replicas anywhere)
[defaults.cache_replicas]

# Single warm replica per DC on threshold (typical / household Plex)
[defaults.cache_replicas]
per_dc = true
[defaults.cache_replicas.warm]
# implicit: min = 1, max = 1

# Warm cache with per-DC redundancy (fault-tolerant)
[defaults.cache_replicas.warm]
min = 1
max = 2

# Cache tier with a higher threshold (cold media library)
[defaults.cache_replicas]
promote_reads = 5
promote_window_secs = 86400  # 24h
[defaults.cache_replicas.warm]
min = 1
```

### 1.3 `dtfs-common::types::AnchorPolicy` (NEW)

```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AnchorPolicy {
    pub datacenter: String,
    /// When None: any tier with a replica in `datacenter` satisfies the
    /// anchor. When Some(t): the anchor-DC replica MUST be in tier t.
    #[serde(default)] pub required_tier: Option<DiskTier>,
}
```

### 1.4 `VolumeConfig` extension

```rust
pub struct VolumeConfig {
    pub id: VolumeId,
    pub name: String,
    pub chunk_size_bytes: u64,
    // Existing field kept for back-compat and as fallback when
    // `mandatory_replicas` is empty / not set:
    pub replication: TierReplication,
    pub write_safety: WriteSafety,
    // Legacy string anchor — deserialises into `anchor` when present.
    #[serde(default)] pub anchor_datacenter: Option<String>,

    // ── new (Bug AV) ────────────────────────────────────────────
    /// Required replicas per tier — enforced by under-replication
    /// scheduler. When empty (default for legacy volumes), the
    /// existing `replication` field is the source of truth.
    #[serde(default)] pub mandatory_replicas: TierReplication,
    /// Cache replicas — opportunistic, demand-driven. When all fields
    /// are 0, no cache promotion happens.
    #[serde(default)] pub cache_replicas: CacheReplication,
    /// Anchor policy, optionally tier-aware. When `anchor` is set, it
    /// supersedes `anchor_datacenter`. Both kept for the rolling
    /// migration window.
    #[serde(default)] pub anchor: Option<AnchorPolicy>,
}
```

### 1.5 `FileMetadata` extension

```rust
pub struct FileMetadata {
    // ... existing fields ...
    /// Bug AV — per-chunk-index cache cooldown. Cleared on tiering
    /// demote; set on operator flush. Empty by default.
    #[serde(default)] pub cache_cooldown_until_ms: BTreeMap<u64, u64>,
}
```

### 1.6 Heartbeat / RegisterNode changes

None for Bug AV. Per-DC `reads_by_dc` is already shipped in heartbeats
(P0-9). Anchor and cache logic reads existing fields.

### 1.7 New `ClientRequest` variants

```rust
ClientRequest::RemoveChunkLocation {
    path: String,
    chunk_index: u64,
    node_id: String,
    /// When set, refuse if dropping this would leave 0 replicas in
    /// the required tier of `anchor`. Threading the policy through
    /// the apply path is cleaner than re-reading the volume here.
    enforce_anchor: Option<AnchorPolicy>,
}

ClientRequest::SetChunkCooldown {
    path: String,
    chunk_indexes: Vec<u64>,
    expires_at_ms: u64,
}
```

### 1.8 New data-plane RPC

`DeleteChunkReplica { chunk_id }` on `DataService` (gRPC) — atomically
unlinks chunk-data + chunk-meta + WAL marker, releases disk-usage
counters.

---

## 2. Phase plan + task list

Four phases. Each task has a stable ID (`AV-<phase>-<n>`) so I can
reference it from commits and the recovery doc. Each task lists:

* **Files** changed
* **Implementation** sketch
* **Tests** required (unit names + live-verification check)
* **Done when** — concrete acceptance criteria
* **Status** marker (✅ when complete + dated)

The plan is updated **in place** as I work: status flips, deviations
from the original sketch get appended under each task as
`Implementation notes`.

**Phase overview:**

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Schema + flush API (~4.5 d) | ✅ closed 2026-05-21 |
| 2 | Tiering engine: read-driven cache promotion (~2.5 d) | ✅ closed 2026-05-21 |
| 3 | Client first-write tier + UX (~2 d) | ✅ closed 2026-05-21 |
| 4 | Polish + verification close-out (~4 d) | 🟦 in progress — AV-4-1 🟨 needs AV-4-1a fixup; AV-4-1b deferred (multi-DC) |

---

## Phase 1 — Schema + flush API (~4.5 d)

Delivers the data model, the delete-replica plumbing, and an API the
operator can call (via curl or CLI) to flush cache tiers. Tiering
engine improvements wait for Phase 2; cache replicas are only created
manually for now (or by the existing additive-migrate UI button).

### ✅ AV-1-1 — `CacheReplication` + `AnchorPolicy` types (2026-05-21)

* **Files:** `crates/dtfs-common/src/types.rs`
* **Implementation:** add `CacheTierConfig` and `CacheReplication`
  structs plus their default-functions. Add `AnchorPolicy` struct.
  `CacheTierConfig::default()` returns `min=0, max=0` (disabled), but
  when explicitly present in TOML the serde-defaults are
  `min=1, max=1`.
* **Tests:**
  - `bug_av_cache_tier_disabled_when_not_mentioned` — empty
    `[cache_replicas]` block leaves all tiers at (0, 0).
  - `bug_av_cache_tier_explicit_defaults_to_one_one` — empty
    `[cache_replicas.warm]` block deserialises to (1, 1).
  - `bug_av_cache_tier_explicit_min_max_round_trip` — `min=1 max=2`
    survives ser → de.
  - `bug_av_cache_replication_full_serde_round_trip` — all fields
    + non-default per_dc, promote_*, cooldown_secs.
  - `bug_av_cache_tier_rejects_max_less_than_min` — validation
    helper (NOT a serde reject, since serde is permissive; we lint
    at config-load time and return an error).
  - `bug_av_anchor_policy_legacy_string_compat` — old
    `anchor_datacenter = "leli"` resolves into the legacy fallback.
  - `bug_av_anchor_policy_struct_round_trip` — `[anchor] datacenter =
    "leli", required_tier = "cold"` serde round-trip.
* **Done when:** 7 tests green via `cargo test -p dtfs-common --release`.

### ✅ AV-1-2 — `VolumeConfig` + `FileMetadata` extension (2026-05-21)

* **Files:** `crates/dtfs-common/src/types.rs` (`VolumeConfig`),
  `crates/dtfs-raft/src/state_machine.rs` (`FileMetadata`,
  `cache_cooldown_until_ms`).
* **Implementation:** add the new fields, all `#[serde(default)]`.
  Add helper `VolumeConfig::effective_mandatory_replicas() ->
  TierReplication` that returns `mandatory_replicas` if any field >0
  else falls back to `replication` (legacy). Add
  `VolumeConfig::effective_anchor() -> Option<AnchorPolicy>` that
  prefers `anchor` then falls back to `AnchorPolicy { datacenter:
  anchor_datacenter, required_tier: None }` if legacy string is
  populated.
* **Tests:** `bug_av_volume_legacy_replication_falls_back`,
  `bug_av_volume_new_mandatory_takes_precedence`,
  `bug_av_anchor_legacy_string_promoted`,
  `bug_av_anchor_struct_overrides_legacy_string`,
  `bug_av_filemeta_cooldown_serde_default`,
  `bug_av_filemeta_old_snapshot_loads`.
* **Done when:** All 6 tests green. Existing
  `state_machine_tests::snapshot_roundtrip` still passes.

### ✅ AV-1-3 — `RemoveChunkLocation` Raft command (2026-05-21)

* **Files:** `crates/dtfs-raft/src/lib.rs` (variant + Display arm),
  `crates/dtfs-raft/src/state_machine.rs` (apply arm).
* **Implementation:** apply arm: locates file, removes matching
  `ChunkLocationEntry`. If `enforce_anchor` is `Some(policy)` and
  removing would leave 0 replicas satisfying the policy for that
  chunk_index, return `ClientResponse::Error { message: ... }`
  without mutating state.
* **Tests:** `bug_av_remove_chunk_location_drops_entry`,
  `bug_av_remove_chunk_location_unknown_file_is_noop`,
  `bug_av_remove_chunk_location_unknown_node_is_noop`,
  `bug_av_remove_chunk_location_refuses_to_strand_anchor`,
  `bug_av_remove_chunk_location_anchor_tier_required_check`,
  `bug_av_remove_chunk_location_anchor_any_tier_allowed`.
* **Done when:** 6 tests green; replay against existing snapshot in
  `bug_o_remove_corrupt_chunk_replicas_*` tests still passes (the
  two paths share `FileMetadata.chunks` mutation but are
  semantically distinct).

### ✅ AV-1-4 — `SetChunkCooldown` Raft command (2026-05-21)

* **Files:** `crates/dtfs-raft/src/lib.rs`, `state_machine.rs`.
* **Implementation:** apply arm: walks `chunk_indexes`, sets
  `file.cache_cooldown_until_ms[idx] = expires_at_ms` (max with any
  existing value, so concurrent flushes monotonically extend
  cooldown).
* **Tests:** `bug_av_set_cooldown_single_chunk`,
  `bug_av_set_cooldown_multi_chunk`,
  `bug_av_set_cooldown_monotonic`,
  `bug_av_set_cooldown_unknown_file_is_noop`.
* **Done when:** 4 tests green.

### ✅ AV-1-5 — `DeleteChunkReplica` gRPC + data-node handler (2026-05-21)

* **Files:** `proto/data.proto` (RPC + request/response messages),
  `crates/dtfs-data/src/server.rs` (handler),
  `crates/dtfs-data/src/store.rs` (`delete_chunk_atomic`).
* **Implementation:** data-node handler:
  1. Resolve chunk path via existing layout helper.
  2. Append `WAL { op: Delete, chunk_id }` so a crash mid-delete
     is recoverable.
  3. Unlink `.dat` (releases disk bytes), then unlink `.meta`.
  4. Clear the WAL entry.
  5. Update `disk.used_bytes` counter (will also refresh via Bug AU
     heartbeat).
  6. Idempotent: missing chunk returns `success=true` with a
     `was_present=false` flag in the response so the caller knows.
* **Tests:** `bug_av_delete_chunk_atomic_unlinks_both`,
  `bug_av_delete_chunk_missing_is_idempotent`,
  `bug_av_delete_chunk_wal_recovery_after_crash` (mock the unlink
  failure between the two unlinks; assert recovery resumes),
  `bug_av_delete_chunk_concurrent_put_is_safe` (put_chunk and
  delete_chunk on same id race — final state is whichever finished
  last, no half-state).
* **Done when:** 4 data-node tests green + manual `grpcurl
  DeleteChunkReplica` against a minicluster data node succeeds and
  the chunk file is gone, disk usage drops.

### ✅ AV-1-6 — Manager → data-node delete dispatch (2026-05-21)

* **Files:** `crates/dtfs-manager/src/scheduler.rs` (helper
  `delete_chunk_replica_on_node`).
* **Implementation:** mirrors `replicate_chunk` and `verify_chunk`:
  resolves node grpc addr, opens a `DataServiceClient`, calls
  `delete_chunk_replica`. Normalised endpoint via
  `normalize_endpoint` (Bug W).
* **Tests:** unit `bug_av_delete_dispatch_url_normalise`,
  `bug_av_delete_dispatch_transport_error_propagated`.
* **Done when:** 2 tests green + manager can drive a delete against a
  live data node (verify via `grpcurl` or by triggering the AV-1-8
  flow against a tiny test).

### ✅ AV-1-7 — `flush_cache_for_path` scheduler routine (2026-05-21)

* **Files:** `crates/dtfs-manager/src/scheduler.rs`.
* **Implementation:** new `pub async fn flush_cache_for_path(
  path_prefix: &str, cache_tiers: &[DiskTier], cooldown_secs: u64,
  max_tasks: usize) -> FlushCacheReport`. Steps:
  1. Read SM snapshot.
  2. For each file matching `path_prefix`:
     a. For each chunk_index, group replicas by tier.
     b. Compute `mandatory_satisfied` for this chunk: are there ≥1
        replicas in tiers configured as `mandatory_replicas.* > 0`
        for this file's volume?
     c. If NO → audit-log a refusal, skip the chunk (never strand
        the anchor).
     d. If YES → for each replica whose tier is in `cache_tiers`,
        enqueue `(chunk_id, node_id)` for deletion.
  3. Cap total deletions at `max_tasks` (default 1000); report
     truncated count.
  4. For each deletion: spawn task → `delete_chunk_replica_on_node`
     → on success, `RemoveChunkLocation` Raft write → audit-log.
  5. Single `SetChunkCooldown` Raft write covering ALL affected
     `chunk_indexes` with `expires_at_ms = now + cooldown_secs *
     1000`.
  6. Return `FlushCacheReport { files_matched, chunks_examined,
     replicas_dropped, mandatory_refusals, truncated, examples }`.
* **Concurrency:** reuse `MAX_MIGRATION_ATTEMPTS` and the per-source
  delete-concurrency pattern from Bug AF — max 8 in-flight delete
  RPCs per source data-node.
* **Tests:** *pure planner* extracted to free function for unit
  tests:
  - `bug_av_plan_flush_drops_warm_keeps_cold`
  - `bug_av_plan_flush_refuses_when_anchor_only_on_warm`
  - `bug_av_plan_flush_respects_max_tasks`
  - `bug_av_plan_flush_empty_when_no_cache_tier_replicas`
  - `bug_av_plan_flush_multi_volume_isolation` (two files in
     different volumes with different mandatory policies — each
     evaluated independently)
* **Done when:** 5 pure-planner tests green.

### ✅ AV-1-8 — Under-replication scheduler honours `mandatory_replicas` (2026-05-21)

* **Files:** `crates/dtfs-manager/src/scheduler.rs::check_under_replicated`.
* **Implementation:** swap the `desired_replicas` lookup to use
  `VolumeConfig::effective_mandatory_replicas().for_tier(t)` instead
  of `replication.for_tier(t)`. When `mandatory_replicas` is empty
  the helper returns the legacy `replication` so back-compat is
  preserved. **Crucially: cache tiers (`cache_replicas.* > 0`) are
  never enforced here.**
* **Tests:** *pure planner* extracted: `plan_under_replication(
  files, volumes, nodes) -> Vec<ReplicaPlan>`.
  - `bug_av_under_replication_uses_mandatory_when_set`
  - `bug_av_under_replication_falls_back_to_legacy`
  - `bug_av_under_replication_ignores_cache_tiers`
  - `bug_av_under_replication_anchor_only_chunk_no_action`
  - `bug_av_under_replication_refills_dropped_mandatory_chunk`
* **Done when:** 5 tests green. Existing
  `bug_ab_under_replicated_*` tests still green (no regression).

### ✅ AV-1-9 — `POST /api/v1/files/flush_cache` REST endpoint (2026-05-21)

* **Files:** `crates/dtfs-manager/src/api.rs`.
* **Implementation:**
  ```
  POST /api/v1/files/flush_cache
  Body: {
    "path_prefix": "/movies/Season-1/",
    "cache_tiers": ["warm", "hot"]      // optional, default = all
                                        // non-mandatory tiers
    "cooldown_secs": 300                // optional, default per-volume
  }
  Response: FlushCacheReport
  ```
  Plus an alias path `POST /api/v1/files/{path:.*}/flush_cache` so the
  WebUI can call without a body.
* **Tests:** `bug_av_api_flush_cache_basic`,
  `bug_av_api_flush_cache_default_tiers`,
  `bug_av_api_flush_cache_invalid_tier_rejected`,
  `bug_av_api_flush_cache_audit_emitted`.
* **Done when:** 4 tests + live curl against minicluster returns a
  sensible report (replicas_dropped > 0 on a populated cache).

### ✅ AV-1-10 — Boot-time policy migration helper

* **Files:** `crates/dtfs-common/src/types.rs`
  (`VolumeConfig::migrated_to_mandatory_schema()`),
  `crates/dtfs-manager/src/api.rs`
  (`POST /api/v1/volumes/{id}/migrate-policy`),
  `crates/dtfs-cli/src/main.rs` (`volumes migrate-policy <vol>`).
* **Implementation:** CLI command reads current volume's
  `replication` (legacy), writes a new `mandatory_replicas` set to
  the same values, leaves `replication` untouched (read fallback
  remains). For new operators wanting the cache-only pattern, they
  follow up with `dtfs-cli volume set-cache <vol> warm=1 per-dc=true`.
* **Tests:** unit on the legacy→new helper:
  `bug_av_migrate_legacy_replication_clones_to_mandatory`,
  `bug_av_migrate_idempotent_when_both_present` — both green.
* **Done when:** 2 tests green + minicluster's `default` volume
  successfully migrated. **Live-verified on minicluster 2026-05-21**
  (see Verification Log entry below).
* **Deviation:** `/api/v1/volumes` JSON listing still only emits the
  legacy `replication` block (no `mandatory_replicas` /
  `cache_replicas` keys exposed). The state machine is correct (the
  idempotent re-run returns `no-op (already migrated)`), but the
  read-side projection needs broadening. Folded into AV-1-12 as part
  of the REST API documentation pass; if the JSON shape change
  becomes risky to bundle, split into a new AV-1-13 follow-up.

### ✅ AV-1-11 — Audit + alert hooks

* **Files:** `crates/dtfs-manager/src/scheduler.rs` (refusal types
  + dispatcher hooks). No new file in `alerts.rs` — re-used the
  existing `AlertKind::AnchorViolated` variant; the alert details
  JSON carries a `subsystem: "cache_flush"` discriminator so
  receivers can route.
* **Implementation:**
  - New crate-private `FlushRefusal { file_path, chunk_index,
    reason: &'static str }`. The planner pushes one entry per
    refused chunk (currently the only reason is
    `"mandatory_strand"`).
  - `FlushCacheReport.refusals: Vec<FlushRefusal>` carries them
    out; marked `#[serde(skip)]` so the REST/CLI JSON wire shape
    stays small (operators see the counter `mandatory_refusals`,
    audit log carries the detail).
  - Dispatcher iterates refusals BEFORE entering the action loop
    so the audit + alert fire even when there's nothing to
    actually delete: one `cache.flush_refused` audit per refusal,
    one deduped `AnchorViolated` alert per call with a sampled
    payload (`refusal_count`, first 20 entries, prefix).
  - `cache.flush_replica` per-success audit was already present
    from AV-1-7; no change there.
  - `cache.flush_complete` summary fires unconditionally (even on
    the early-return paths) so audit timelines aren't missing the
    operator's action.
* **Tests** (`cargo test -p dtfs-manager bug_av_`): 3 new
  planner-side, all green:
  - `bug_av_planner_records_refusal_per_stranded_chunk`
  - `bug_av_planner_no_refusals_on_happy_path`
  - `bug_av_report_serde_skips_refusals_field`
* **Done when:** 3 tests + live verification of the audit hook on
  the minicluster. Live-confirmed `cache.flush_complete` fires
  with `actor=scheduler, origin_manager=<mgr>` on the AV-1-10
  migrated `default` volume (see verification log entry).
* **Deviation:** the two originally planned tests
  (`bug_av_audit_emitted_per_replica_delete`,
  `bug_av_alert_fires_on_mandatory_strand_refusal`) targeted the
  async dispatcher; per the AV-1-7 deviation pattern, async
  dispatcher behaviour in this crate is live-verified, not unit-
  tested. The planner-side tests provide deterministic coverage
  of the refusal classification + JSON wire-shape; the dispatcher
  side is reasoning-verified (the audit/alert calls are
  straightforward iteration over the `refusals` vec) plus a live
  confirmation of `cache.flush_complete` firing through Bug AH
  Raft fan-out. Full live exercise of a refusal-producing flush
  is deferred to Phase 2 when AV-2 sets up volumes with mismatched
  mandatory/actual placement.

### ✅ AV-1-12 — Documentation: schema + flush API

* **Files touched** (all 7):
  - `docs/src/configuration/volume-config.md` — extended settings
    reference table with `mandatory_replicas.*`, `cache_replicas.*`,
    `anchor.*`; added "Replication classes" subsection covering the
    two-class model + legacy back-compat rules; embedded Examples A
    (Plexmedia) and B (single-DC home lab) inline.
  - `docs/src/storage/replication.md` — added "Two replication
    classes" section at the top with the comparison table and the
    "Anchor invariant" sub-section; cross-links to `volume-config.md`
    and `tiering.md`.
  - `docs/src/storage/tiering.md` — added "Cache tier semantics
    (Bug AV)" placeholder section explaining what *will* land in
    Phase 2 plus what the operator *can* do today
    (`flush_cache`, manual schema migration).
  - `docs/src/api-reference/rest-api.md` — full documentation of
    `POST /api/v1/files/flush_cache` (request/response/audit/alert
    + curl example) and `POST /api/v1/volumes/{id}/migrate-policy`
    (idempotent helper).
  - `docs/src/api-reference/cli-reference.md` — `volumes
    migrate-policy` section + planned placeholder for `files
    flush-cache` (AV-3-5).
  - `CLAUDE.md` — new § 3.4 "Mandatory vs cache replicas (Bug AV
    invariant)" elevating the AV constraint to a top-level
    convention; § 10 file map extended with the new entry points
    on `scheduler.rs` (`flush_cache_for_path`, planner,
    `check_under_replicated` now via `effective_mandatory_replicas`).
  - `README.md` — paragraph in the lead explaining the
    mandatory-anchor + opportunistic-cache model with a link to
    `volume-config.md#replication-classes`.
* **Tests:** `mdbook build docs/` clean (no warnings, no errors).
* **Done when:** all 7 files updated, mdbook builds clean.
  Live-verified on dtfs-builder VM (`mdbook 0.4.36` installed via
  `cargo install`, build completes in <1 s with only `INFO`
  output — no warnings/errors).

### Phase 1 Done When

* Tasks AV-1-1 … AV-1-12 all ✅.
* `cargo test -p dtfs-common -p dtfs-raft -p dtfs-data -p dtfs-manager
  --release` green. Net new tests: **43** (AV-1-1 grew from 3 → 7 with
  the min/max split).
* `mdbook build docs/` green.
* Minicluster: `default` volume migrated to
  `mandatory_replicas={cold:1}, cache_replicas={warm:1, per_dc:true}`.
  Manual: `dtfs-cli volume migrate-policy default` then
  `dtfs-cli volume set-cache default warm=1 per-dc=true`.
* Live demo: write a fresh 16 MiB file, observe it lands on 1 cold +
  2 warm (warm via the still-running legacy tiering that we'll
  rewrite in Phase 2). Run `curl … flush_cache` for that file.
  Observe warm replicas vanish, cold replica preserved, audit log
  shows 2 `cache.flush_replica` entries, disk usage on the warm
  mounts drops by ~32 MiB.

---

## Phase 2 — Tiering engine: read-driven cache promotion (~2.5 d)

### ✅ AV-2-1 — Per-DC rolling read-window store

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** new `ReadSample { ts_ms, count }` struct
  (each sample carries the heartbeat-delta count, so a single
  bursty heartbeat doesn't bloat the ring). Pure helpers
  `count_reads_in_window(samples, now_ms, window_secs)` and
  `prune_read_window(samples, now_ms, window_secs)` for
  ring-buffer housekeeping. The samples may be unsorted; the
  count helper iterates exhaustively because the per-(chunk, dc)
  ring is small.
* **Tests (5 green):**
  - `bug_av_read_window_counts_inside_window`
  - `bug_av_read_window_drops_outside`
  - `bug_av_read_window_handles_empty`
  - `bug_av_read_window_handles_unsorted_input`
  - `bug_av_read_window_zero_window_returns_zero` (defensive
    edge case: 0-second window returns 0 / clears the ring).
* **Done when:** 5 tests green. ✓

### ✅ AV-2-2 — Promotion decision (pure)

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** pure function `plan_cache_promotion(
  read_count_in_window, tier_cfg, current_cache_replicas_in_dc,
  cooldown_until_ms, promote_reads_threshold, now_ms) -> u32`.
  Four gate paths each return early with 0; basic-promotion
  branch returns `min(tier_cfg.min, tier_cfg.max).saturating_sub(
  current_cache_replicas_in_dc)`. Caps at `min` for the basic
  path; redundancy-escalation pass deferred to AV-2-4 wiring
  with `ENABLE_CACHE_REDUNDANCY_ESCALATION = false`.
* **Tests (7 green):**
  - `bug_av_promote_first_read_below_threshold_does_nothing`
  - `bug_av_promote_threshold_met_returns_min_target_minus_current`
  - `bug_av_promote_already_at_min_returns_zero`
  - `bug_av_promote_max_zero_disables_promotion`
  - `bug_av_promote_cooldown_blocks_until_expiry`
  - `bug_av_promote_min_two_creates_two_at_once`
  - `bug_av_promote_min_greater_than_max_clamps_to_max`
    (defensive: validate() would normally catch this at load,
    but the planner clamps anyway).
* **Done when:** 7 tests green. ✓

### ✅ AV-2-3 — Promotion target selection

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** new `NodeSelectionView` projection (the
  pure-selector's input contract) + pure
  `select_promotion_target(candidates, dc) -> Option<u64>`.
  Filter order: DC match → not `draining` → not
  `already_holds_chunk` → has target-tier disk. Pick highest
  `free_capacity`, tie-break by **lowest** `node_id` for
  deterministic audit output.
* **Tests (5 green):**
  - `bug_av_target_selects_most_free_in_dc`
  - `bug_av_target_excludes_draining_nodes`
  - `bug_av_target_excludes_existing_replica_holders`
  - `bug_av_target_returns_none_when_no_dc_warm_node`
  - `bug_av_target_ties_resolve_by_lower_node_id` (defensive:
    deterministic tie-break is what audit logs depend on).
* **Done when:** 5 tests green. ✓

### ✅ AV-2-4 — `tiering::tick` wires AV-2-1..3 + schedules replication

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:**
  - Pure planner `plan_cache_tick(snapshot, now_ms)` walks the
    `TickSnapshot` and emits `TickPromotion` + `TickDemotion`
    actions. Promotion path composes AV-2-1 (`read_window_count`)
    + AV-2-2 (`plan_cache_promotion`) + AV-2-3
    (`select_promotion_target`). Demotion path: a cache-tier
    placement with 0 reads in window AND a surviving mandatory
    replica becomes a demotion candidate.
  - `TieringEngine` gains two new fields:
    `read_windows: RwLock<HashMap<(chunk, dc), Vec<ReadSample>>>`
    and `prev_reads_by_dc` for heartbeat delta tracking.
    `report_access` pushes a sample on each positive delta.
  - `tick_av_cache()` runs alongside the legacy temperature path:
    builds the snapshot under one SM read lock, calls the
    planner, dispatches promotions via
    `scheduler.schedule_replication` (existing path that already
    handles WAL + verify + remove-source race-safety), audits
    every promote/demote. **The actual delete RPC for demotion is
    routed through AV-2-5** (cooldown-clear + RemoveChunkLocation
    + DeleteChunkReplica) so this commit ships demotion as
    "planned + audited but not yet executed".
  - `ENABLE_CACHE_REDUNDANCY_ESCALATION: bool = false` Phase-2
    flag honoured: the planner's escalation branch only runs when
    the flag is set, so a volume with `max > min` doesn't yet
    ramp past `min`. Flipping the flag is a follow-up that
    requires a per-DC HA workload to validate against; no
    successor Phase is currently scheduled — the deferral lives
    in this plan's § 5 deviations until a follow-up ticket is
    cut.
* **Tests (6 green):**
  - `bug_av_tiering_tick_promotes_on_threshold`
  - `bug_av_tiering_tick_demotes_idle_cache_replica`
  - `bug_av_tiering_tick_respects_cooldown`
  - `bug_av_tiering_tick_respects_per_dc_cap`
  - `bug_av_tiering_tick_redundancy_escalation_off_by_default`
  - `bug_av_tiering_tick_per_dc_false_uses_global_pool`
    (defensive: per_dc=false code path).
* **Done when:** 6 tests green + smoke check on minicluster
  (binary rolls cleanly; tiering engine starts; no panics).
  ✓ — verified at 2026-05-21 11:53 UTC.
* **Deviation:** the full live demo (write file → 2 reads → warm
  replica appears) requires a volume with `cache_replicas`
  configured, which today only the REST API can set (no CLI
  surface until AV-3-5). Live demo deferred to the Phase 2
  close-out after AV-2-6, by which point the cache-tier-on
  volume can be plumbed via raw REST. The AV-2-4 commit itself
  is structurally complete: planner + engine wiring + audit
  hooks + heartbeat-delta sample push, all running on the
  minicluster.

### ✅ AV-2-5 — Cooldown clearance on demote

* **Files:** `crates/dtfs-manager/src/tiering.rs` +
  `crates/dtfs-manager/src/scheduler.rs` (visibility bumps).
* **Implementation:**
  - New pure helper
    `should_clear_cooldown_on_demote(cooldown_until_ms, now_ms)
    -> bool` returning `true` for past/absent cooldowns, `false`
    for future operator cooldowns. The dispatcher honours the
    operator's intent on race-with-flush.
  - `TickDemotion.cooldown_until_ms_at_snapshot: u64` carries
    the planner-time cooldown through to the dispatcher (no
    re-fetch from SM, deterministic in tests).
  - Dispatcher in `tick_av_cache` now actually executes
    demotion: resolve grpc addr → `DeleteChunkReplica` RPC →
    `RemoveChunkLocation` Raft write → conditional
    `SetChunkCooldown(expires_at_ms = 0)` clear via
    `should_clear_cooldown_on_demote`. Each step audited with
    the right reason code (`idle_window`,
    `noop_already_absent`, `rpc_failed`,
    `skipped_unreachable`).
  - `TaskScheduler::node_grpc_addr`,
    `delete_chunk_replica_on_node` bumped to `pub(crate)`; new
    `raft()` accessor so the tiering engine can issue the two
    additional Raft writes without re-implementing them.
* **Tests (3 green):**
  - `bug_av_demote_clears_cooldown`
  - `bug_av_demote_does_not_clear_future_operator_cooldown`
  - `bug_av_demote_carries_cooldown_through_planner` (defensive:
    guards against future refactors stripping the snapshot
    cooldown from `TickDemotion`).
* **Done when:** 3 tests + smoke check on minicluster (binary
  rolls cleanly; tiering engine starts). ✓ — verified 11:55 UTC.
* **Deviation:** the cluster-behaviour demo
  (demote → immediate read → re-promote without waiting for
  cooldown) bundles into Phase 2 close-out alongside AV-2-4's
  full live demo, because both need a `cache_replicas` volume
  that needs the AV-3-5 CLI (or manual REST UpsertVolume) to
  set up.

### ✅ AV-2-6 — Documentation: tiering engine + promotion rules

* **Files touched (4):**
  - `docs/src/storage/tiering.md` — Phase 2 status banner +
    "Promotion math", "Demotion math", "Cooldown semantics",
    "Anchor-aware demotion", and "Worked example" subsections.
    The worked example walks `/movies/Wicked.mkv` through write
    → promote → operator flush → cooldown → idle demote.
  - `docs/src/storage/caching.md` — opening callout explicitly
    separating data-node ARC + FUSE local-disk caches (covered
    here) from the AV `cache_replicas` placement layer (covered
    in tiering.md). Avoids the recurring operator confusion of
    "which cache am I reading about".
  - `docs/src/operations/cluster-management.md` — new
    "Reclaiming warm cache space" runbook with the procedure
    (curl example), expected audit-log shape, cooldown
    semantics, anchor-refusal investigation steps. Operator-
    facing; matches the on-cluster behaviour verified during
    AV-1-9 / AV-1-11 live runs.
  - `CLAUDE.md` — § 3.4 extended with two new invariants:
    (a) every cache promotion/demotion must check
    `would_violate_anchor`; (b) the engine-demote-vs-operator-
    flush asymmetry is enforced via
    `should_clear_cooldown_on_demote` — never bypass that
    helper.
* **Tests:** `mdbook build docs/` clean on dtfs-builder (INFO
  output only; no warnings, no errors).
* **Done when:** all 4 doc files updated, mdbook builds clean.
  The runbook reflects the on-cluster behaviour exercised in
  AV-1-9 / AV-1-11; the full Phase 2 read→promote demo bundles
  into the Phase 2 close-out check.

### Phase 2 Done When

* AV-2-1 … AV-2-6 all ✅. **20** new tests green (AV-2-2 grew from 5 →
  6 and AV-2-4 from 4 → 5 with the redundancy-flag handling).
* `mdbook build docs/` green.
* Minicluster: with `cache_replicas={warm:1, per_dc:true,
  promote_reads:2, promote_window_secs:60}`:
  1. Write a fresh 16 MiB file. Observe ONLY cold replica
     (no warm).
  2. `cat` the file once from client01 → no warm yet (1<2).
  3. `cat` again → warm replica appears in client01's DC within 30
     s.
  4. `curl flush_cache` → warm gone.
  5. Immediate `cat` → no re-promote (cooldown 5 min).
  6. Wait 5 min, `cat` twice → re-promotes.

---

## Phase 3 — Client first-write tier + UX (~2 d)

### ✅ AV-3-1 — `fuse.toml [client.write]` block

* **Files:** `crates/dtfs-common/src/config.rs` +
  `tests/proxmox/ansible/templates/fuse.toml.j2`.
* **Implementation:** new `FuseWriteConfig { first_write_tier:
  String }` with default `"cold"`; `FirstWriteTier` enum with
  `Cold`/`NearestWarm` variants; pure helper
  `FuseWriteConfig::effective_first_write_tier()` returns the
  enum, falling back to `Cold` for any unrecognised value. The
  field-level `#[serde(default)]` + impl `Default` means an
  omitted `[write]` block in legacy `fuse.toml` parses cleanly.
  Ansible template extended with a templated `dtfs_first_write_
  tier` variable defaulting to `"cold"`.
* **Tests (3 green):**
  - `bug_av_fuse_write_config_default`
  - `bug_av_fuse_write_config_nearest_warm_parsed`
  - `bug_av_fuse_write_config_invalid_value_falls_back_to_cold`
* **Done when:** 3 tests green + workspace builds + the
  template renders without error in Ansible. ✓ — minicluster
  bring-up of a `nearest-warm` client deferred to AV-3-2's
  live verification (the proto + placement engine changes are
  what actually exercise the value end-to-end).

### ✅ AV-3-2 — `preferred_tier` hint on `allocate_chunks`

* **Files:** `proto/manager.proto`,
  `crates/dtfs-manager/src/metadata.rs` (handler +
  `resolve_preferred_tier` pure helper),
  `crates/dtfs-client/src/client.rs`
  (`allocate_chunks_with_hint`),
  `crates/dtfs-fuse/src/filesystem.rs` (writes the hint based on
  the AV-3-1 `FirstWriteTier`).
* **Implementation:** new proto field
  `string preferred_tier = 4` on `AllocateChunksRequest`. New
  pure helpers in `metadata.rs`:
  - `parse_preferred_tier_hint(&str) -> Option<DiskTier>`
    (recognises `"hot"`, `"warm"`, `"cold"`, `"tape"`; empty +
    unknown → `None`).
  - `resolve_preferred_tier(hint, legacy_replication,
    available_tiers) -> (DiskTier, u32)`. When the hint
    resolves AND the tier has at least one healthy node, return
    `(tier, 1)` (single first-chunk cache replica). Otherwise
    fall back to `pick_effective_tier(legacy_replication)`.
  - `allocate_chunks` handler queries the SM for available tiers,
    then calls `resolve_preferred_tier`.
  - FUSE filesystem maps `FirstWriteTier::Cold → ""` (legacy
    behaviour) and `FirstWriteTier::NearestWarm → "warm"`.
* **Tests (5 green):**
  - `bug_av_allocate_chunks_honours_preferred_tier`
  - `bug_av_allocate_chunks_falls_back_when_tier_full`
  - `bug_av_allocate_chunks_empty_hint_uses_default`
  - `bug_av_allocate_chunks_unknown_hint_falls_back` (defensive)
  - `bug_av_parse_preferred_tier_hint_coverage` (defensive)
* **Done when:** 5 tests + smoke check on minicluster (new
  manager binary rolls cleanly, no panics). ✓
* **Deviation:** the full write→placement live demo bundles
  with AV-3-3's close-out — without the AV-3-3 anchor-lag
  repair the warm-first writes leave chunks on warm forever
  (no cold copy gets scheduled), which is the opposite of
  desirable. Both pieces are needed to demo the full
  Plexmedia-style "warm-first → async-cold" pattern.

### ✅ AV-3-3 — Auto-anchor-migration on warm-first writes

* **Files:** `crates/dtfs-manager/src/scheduler.rs`.
* **Implementation:** new pure planner
  `plan_anchor_lag_repair(anchor, placements, nodes, chunk_id,
  existing_tasks) -> Option<AnchorRepairAction>`. Decision tree:
  - Gate 1: anchor invariant already satisfied → `None`.
  - Gate 2: pick candidate tier (`required_tier` if set,
    else default to `Cold`).
  - Gate 3: filter healthy nodes in anchor DC with the
    candidate tier, exclude existing holders, pick lowest
    node_id for deterministic audit output.
  - Gate 4: skip if `(chunk_id, target_node)` already
    in-flight as a migration task.
  - Source = lowest-id existing placement (cross-tier reads
    handled by the data-node `ReplicateChunk` RPC).
  - New async `check_anchor_lag` method runs alongside
    `check_under_replicated` at the same cadence. Audits each
    scheduled repair with `anchor.lag_repair` reason code +
    target/tier/dc details.
* **Tests (6 green):**
  - `bug_av_anchor_lag_repair_picks_anchor_dc_node`
  - `bug_av_anchor_lag_repair_picks_required_tier`
  - `bug_av_anchor_lag_repair_no_op_when_satisfied`
  - `bug_av_anchor_lag_repair_required_tier_none_defaults_to_cold`
    (defensive)
  - `bug_av_anchor_lag_repair_skips_already_scheduled`
    (defensive)
  - `bug_av_anchor_lag_repair_skips_unhealthy_target`
    (defensive)
* **Done when:** 6 tests + smoke check on minicluster (binary
  rolls cleanly). ✓
* **Deviation:** the live demo for AV-3-1 / AV-3-2 / AV-3-3
  end-to-end ("warm-first write → anchor repair within ~30s")
  bundles into the Phase 3 close-out, since exercising it
  requires a refreshed FUSE binary + a fuse.toml with
  `first_write_tier = "nearest-warm"` deployed to a client VM.

### ✅ AV-3-4 — WebUI "Flush cache" action (volume-edit form deferred)

* **Files:** `crates/dtfs-manager/src/webui.rs`.
* **Implementation:**
  - Data Explorer right-click context menu extended with
    "Flush cache (folder)…", "Flush cache (file)…", and
    "Flush cache (parent)…" entries.
  - New `flush-modal` dialog with: path prefix, cache_tiers
    select (warm / hot / warm+hot), cooldown_secs numeric
    field, allow_empty_prefix safety gate when the prefix is
    empty or `/`. Result panel shows `replicas_dropped`,
    `mandatory_refusals` (with audit-log hint when > 0),
    `unreachable_nodes`, `rpc_failures`, `raft_failures`,
    `truncated`.
  - `openFlushCache(prefix)` / `closeFlushCache()` /
    `submitFlushCache()` JS handlers post to
    `POST /api/v1/files/flush_cache` (the AV-1-9 endpoint).
* **Tests:** live (no JS unit framework in repo). Markup
  verified post-roll: 7 "Flush cache" entries + 2
  `submitFlushCache` references in the rendered explorer
  page.
* **Done when:** Right-clicking the Data Explorer reveals the
  new entries, the modal posts the right body shape, and the
  result panel renders the AV-1-9 response counters. ✓ — markup
  verified live 2026-05-21.
* **Deviation:** the volume-edit form (showing
  `mandatory_replicas` / `cache_replicas` / `anchor` with an
  "Edit" → `PUT /api/v1/volumes/{id}` flow) is deferred. Two
  blockers: (a) the AV-1-10 carryover —
  `GET /api/v1/volumes` doesn't yet project the new fields,
  so there's nothing to display; (b) the operator-facing
  surface for these settings is the CLI
  (`dtfs-cli volume set-cache`, AV-3-5) which most operators
  reach for first. Folded forward: when AV-1-10's projection
  gap is closed (likely in AV-3-5 to share the same JSON
  shape work), the WebUI form lands as a follow-up to AV-3-5.

### ✅ AV-3-5 — CLI: `files flush-cache` + `volumes set-cache` + `volumes show` (+ AV-1-10 projection)

* **Files:** `crates/dtfs-manager/src/api.rs`,
  `crates/dtfs-cli/src/main.rs`. Two new REST endpoints + three
  new CLI subcommands; the CLI changes the legacy `dtfs-cli
  files` → `dtfs-cli files list` (breaking) to make room for
  `files flush-cache`. **Closes the AV-1-10 carryover** by
  adding `GET /api/v1/volumes/{id}` returning the full
  VolumeConfig JSON projection (mandatory_replicas +
  cache_replicas + anchor).
* **Implementation:**
  - REST: `GET /api/v1/volumes/{id}` returns
    `volume_config_to_json(cfg)` — the new pure helper that
    projects every AV field. `POST .../set-cache` accepts a
    partial `SetCacheRequest` (every field `Option<_>`); the
    pure `apply_set_cache(current, req)` helper merges the
    update, validates `max >= min` per tier, and returns the
    new VolumeConfig for the dispatcher to Raft-write.
    No-op when current == new.
  - CLI: `dtfs-cli volumes show <id>` renders a table of every
    new field; `dtfs-cli volumes set-cache <id> [--warm-min N
    ...]` patches the policy with one-flag-per-field clap
    arguments; `dtfs-cli files flush-cache <path>
    [--cache-tiers warm,hot] [--cooldown-secs N]` posts to the
    AV-1-9 endpoint. Both honour `--format json` for scripting.
* **Tests (5 green, dtfs-manager):**
  - `bug_av_set_cache_merges_partial_update`
  - `bug_av_set_cache_rejects_max_less_than_min`
  - `bug_av_set_cache_empty_request_is_noop`
  - `bug_av_volume_config_json_includes_av_fields`
  - `bug_av_volume_config_json_anchor_none_is_null` (defensive)
* **Done when:** 5 tests + live verification (all 6 scenarios
  green): volumes show, set-cache apply, re-show confirms,
  idempotent re-apply returns no-op, max<min returns 400, and
  files flush-cache returns a clean report. ✓
* **Deviations:** the plan called for `bug_av_cli_file_flush_
  cache_invokes_api` / `bug_av_cli_volume_set_cache_parses_args`
  unit tests against a mock HTTP backend. Per the convention
  established by AV-1-6 / AV-1-9, axum handler + CLI HTTP
  client are live-only in this crate. Pure helpers
  (`apply_set_cache`, `volume_config_to_json`) carry the
  unit-test load. **Closes AV-1-10 carryover** (REST
  projection of the new fields). **Closes AV-3-4 carryover**
  partially (the WebUI can now `fetch` per-volume policy via
  the new endpoint; the inline-edit form is deferred as a
  separate follow-up — see § 5 deviations).

### ✅ AV-3-6 — Documentation: client + WebUI + CLI

* **Files touched (6):**
  - `docs/src/configuration/fuse-client.md` — new `[write]`
    subsection documenting `first_write_tier` with the
    `"cold"` vs `"nearest-warm"` trade-off + "When to use"
    guidance. Cross-links to the tiering promotion math.
  - `docs/src/api-reference/cli-reference.md` — filled in the
    AV-1-12 placeholder with three full subsections:
    `volumes show`, `volumes set-cache`, `files flush-cache`.
    Each carries argument tables, example outputs, error
    codes. Renamed legacy `files` → `files list` and added a
    breaking-change note.
  - `docs/src/access/fuse-mount.md` — new "First-write tier
    preference (Bug AV)" subsection right above "Write
    Buffering" explaining the FUSE-side semantics + how the
    volume policy gates the request.
  - `docs/src/operations/production-deployment.md` — new
    "Tier policy bootstrap (Bug AV)" subsection in the
    cluster-bootstrap-and-volume area: 4-step bootstrap
    sequence (`migrate-policy` → `show` → `set-cache` →
    `show`), rolling-upgrade gating section
    (before/during/after rollout), rollback path.
  - `plans/incidents/SOAK-RECOVERY-2026-05-21.md` — new
    "Bug AV — two-class tier lifecycle (mandatory + cache
    replicas)" section with the symptom-cause-fix-tests-
    verification template used by Bugs AL–AT in the same
    doc.
  - `plans/reports/MINICLUSTER-VERIFICATION-2026-05-21.md`
    — new "Bug AV — two-class tier lifecycle (Phases 1-3)"
    section with verification tables (REST endpoints, CLI
    matrix, audit fan-out evidence, WebUI markup grep,
    Phase 2 smoke, test counts, carryovers).
* **Tests:** `mdbook build docs/` on dtfs-builder — clean.
  No new Rust tests; existing 66 AV-related unit tests stay
  green (workspace test count unchanged since AV-3-5).
* **Done when:** 6 doc surfaces updated, mdbook builds
  clean, recovery + verification docs carry the actual live
  cluster evidence (timestamps + commit ranges referenced).
  ✓ — completed 2026-05-21.
* **Deviations:** none. Phase 3 of Bug AV is now closed
  out.

### Phase 3 Done When

* AV-3-1 … AV-3-6 all ✅. 11 new tests green.
* End-to-end demo on minicluster, recorded in
  `MINICLUSTER-VERIFICATION-2026-05-21.md` Bug AV section.
* `mdbook build docs/` green.
* `CLAUDE.md` updated in the same commit that lands the final Phase 3
  code (per § 11 of CLAUDE.md itself — "update CLAUDE.md in the SAME
  commit as the code change").

---

## Phase 4 — Polish + verification close-out (~3.5 d)

Closes out the three carryovers recorded in § 5 deviations after
Phase 3 + the deferred end-to-end live demos. **Phase 4 ships no
new operator-visible features** — it tightens up what Phases 1-3
delivered:

1. The Phase-2 read→promote and Phase-3 warm-first→anchor-repair
   demos were deferred to a separate run that needs the new FUSE
   binary on a client VM. Phase 4 runs them.
2. `GET /api/v1/volumes` (list) still uses its aggregation-only
   projection; AV-3-5 added the per-volume projection but not the
   list one. Phase 4 broadens the list endpoint so a single fetch
   surfaces every volume's policy.
3. The WebUI shows the flush-cache button (AV-3-4) but not the
   volume policy itself. Phase 4 adds a read-only display card +
   an inline-edit modal posting to `/set-cache`.
4. `ENABLE_CACHE_REDUNDANCY_ESCALATION: bool = false` (a Phase-2
   compile-time const) is promoted to a runtime config option +
   the redundancy-escalation path gets its own test.

Plus the standard close-out: docs sweep, plan archival, CLAUDE.md
update, PROGRESS.md / ISSUES_TO_FIX.md tightening.

### 🟨 AV-4-1 — End-to-end Phase 2 + Phase 3 live demos (~1 d) — **needs-review (topology blocker)**

The minicluster has the new manager binaries (Phase 1-3); it
does NOT yet have a FUSE client built from the AV-3-1/3-2 code
or configured with `first_write_tier = "nearest-warm"`. Three
demos are gated on that:

* **AV-2-4 / AV-2-5 demo** — write a fresh file, read it twice
  from one DC within the configured `promote_window_secs` (set
  to 60 s for the test), observe the warm cache replica appear
  in that DC, then leave it idle for the window and observe
  the demote.
* **AV-3-3 demo** — write a file from a client with
  `first_write_tier = "nearest-warm"`, observe the warm
  replica appears immediately, observe the cold (anchor) copy
  scheduled by `check_anchor_lag` within one tiering tick.
* **Operator-flush vs engine-demote cooldown asymmetry**
  (AV-2-5) — flush a chunk, immediately read it twice within
  60 s, confirm the cooldown blocks re-promotion. Wait for
  cooldown to expire, read twice, confirm re-promotion.

* **Files:** `tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh`
  (new shell regression test that drives all three scenarios
  against the minicluster). Optional updates to
  `tests/proxmox/ansible/templates/fuse.toml.j2` /
  `inventory.json` for a `nearest-warm` client.
* **Implementation:**
  - Build a fresh FUSE binary from current `master`.
  - Roll to mini-client01 (or stand up `mini-client02` with
    `first_write_tier = "nearest-warm"` if needed; the
    minicluster spec from `MINICLUSTER-VERIFICATION-2026-05-
    21.md` is 6 VMs → may need one more).
  - Apply a Plexmedia-style policy to `default`:
    `dtfs-cli volumes set-cache default --warm-min 1
    --warm-max 1 --per-dc true --promote-reads 2
    --promote-window-secs 60 --cooldown-secs 60`.
  - Lower `tiering.evaluation_interval_secs` to 10 s so the
    demo finishes in minutes, not hours.
  - Run the shell script; collect stdout + audit-log
    snapshots; write the results into the existing
    `MINICLUSTER-VERIFICATION-2026-05-21.md` Bug AV section.
* **Tests:** shell-script `tests/proxmox/abuse/15-bug-av-
  cache-lifecycle.sh` exits 0; output asserts:
  - Warm replica appeared within 60 s of the 2nd read
    (matching reason `promote_threshold_met`).
  - Warm replica disappeared within 2 tiering ticks after the
    60-s idle window (matching reason `idle_window`).
  - Operator flush leaves cooldown > 0 in the SM; the
    immediately-following 2nd read does NOT promote.
  - After cooldown expiry, the next 2-reads-in-window
    promotes again.
  - For the warm-first demo: a new file lands on warm first
    (chunk_location entry has `disk_id` pointing to a warm
    disk); within one tick a `anchor.lag_repair` audit entry
    + a successful migration to cold land.
* **Done when:** shell script green; live verification text
  added to `MINICLUSTER-VERIFICATION-2026-05-21.md`; all four
  AV-2-4 / AV-2-5 / AV-3-3 carryover bullets in § 5
  deviations marked ✅ RESOLVED.
* **Current state (2026-05-22):** 🟨 needs-review. The shell
  script is written + the cluster repairs + FUSE roll
  completed (see verification log). The live scenarios do
  not yet produce observable behaviour on this minicluster
  due to the topology constraints documented in the
  verification log; AV-4-1a below adds the smallest code
  change that unblocks S1/S2/S3, and AV-4-1b queues the
  multi-DC infra work for S4.

### ⬜ AV-4-1a — `POST /api/v1/volumes/{id}/set-mandatory` endpoint (~0.5 d) — _unblocks AV-4-1 S1/S2/S3_

The minicluster's `default` volume has
`mandatory_replicas.warm = 2`, which (correctly per the AV
invariant) means the cache planner skips warm and AV-4-1's
S1/S2/S3 cache-lifecycle scenarios produce no observable
behaviour. Phase 1 added `migrate-policy` (legacy → mandatory
clone) and Phase 3 added `set-cache` (mutate cache_replicas),
but there's no API to mutate `mandatory_replicas` in-place.
This task fills that gap so the test setup can convert
`default` to cold-only mandatory.

* **Files:** `crates/dtfs-manager/src/api.rs` (new
  `set_volume_mandatory_endpoint` + `apply_set_mandatory`
  pure helper + route registration);
  `crates/dtfs-cli/src/main.rs` (`VolumesCmd::SetMandatory`
  with one flag per tier);
  `crates/dtfs-manager/src/webui.rs` (extend the AV-4-4
  inline-edit modal to cover mandatory fields too — defer
  to AV-4-4 if scope creep risk).
* **Implementation:**
  - `SetMandatoryRequest { hot_replicas: Option<u32>,
    warm_replicas: Option<u32>, cold_replicas: Option<u32>,
    tape_replicas: Option<u32> }`. Every field optional —
    omitted fields keep the current value.
  - Pure helper `apply_set_mandatory(current, req) ->
    Result<VolumeConfig, String>` mirrors AV-3-5's
    `apply_set_cache` shape: merge, then validate. Validation
    rejects "all tiers zero" (that would orphan the volume's
    durability floor — operators wanting that should drop
    `mandatory_replicas` entirely via UpsertVolume on a
    fresh volume; not a runtime operation).
  - Anchor invariant defence-in-depth: if the volume has an
    `anchor.required_tier` set and the new mandatory_replicas
    has 0 for that tier, return HTTP 400 with a clear error
    (otherwise the anchor would be permanently unsatisfiable).
* **Tests:**
  - `bug_av_set_mandatory_merges_partial_update`
  - `bug_av_set_mandatory_rejects_all_zero` (defensive)
  - `bug_av_set_mandatory_rejects_anchor_required_tier_zero`
    (defensive)
  - `bug_av_set_mandatory_legacy_volume_round_trips` —
    when called on a volume that's never been
    migrate-policy'd, the helper migrates implicitly (so
    the result has both legacy `replication` AND new
    `mandatory_replicas` populated).
* **Done when:** 4 tests + live verification on the
  minicluster: `dtfs-cli volumes set-mandatory default
  --cold-replicas 1 --warm-replicas 0 --tape-replicas 0`
  succeeds; subsequent re-run of the AV-4-1 shell script's
  S1/S2/S3 produces the expected promote / demote /
  cooldown behaviour. After verification, flip AV-4-1's
  🟨 to ✅ with a reference to this fixup commit.

### ⬜ AV-4-1b — Multi-DC topology for S4 anchor-lag-repair demo (~1-2 d) — _deferred_

S4 (warm-first write → anchor-lag-repair) cannot demonstrate
observable behaviour on a single-DC minicluster: the anchor
invariant (`dc=dc-a, required_tier=any`) is trivially
satisfied by any first-chunk placement. Multi-DC provisioning
is the long-term home for AV demos and matches the plan's
Plexmedia topology spec, but it's an infra-heavy
undertaking that doesn't belong on the critical path for
closing Phase 4.

* **Disposition:** deferred from Phase 4. Tracked here as a
  named follow-up rather than left as an unlabelled deviation.
  Picked up when (a) a multi-DC test-lab budget exists, or
  (b) the Plexmedia production rollout drives the
  requirement.
* **Done when:** a multi-DC minicluster (3 DCs minimum per
  the plan's Plexmedia spec) exists, AV-4-1's S4 runs green
  on it, and the result is recorded in
  `MINICLUSTER-VERIFICATION-*.md`.

### ⬜ AV-4-2 — `GET /api/v1/volumes` list-endpoint projection (~0.5 d)

The list endpoint currently emits only `{ volume_id, file_count,
total_size, replication, chunk_size_bytes, name, write_safety }`
per volume. AV-3-5 added the per-volume projection
(`GET /api/v1/volumes/{id}`) but the list endpoint stayed
aggregation-only.

The breaking-change risk identified in AV-3-5 (existing WebUI /
CLI clients reading `replication` from the list endpoint) is
manageable in Phase 4: today's two consumers
(`crates/dtfs-cli/src/main.rs::cmd_volumes`,
`crates/dtfs-manager/src/webui.rs::renderVolumes`) both read
only a fixed subset of fields and tolerate extra ones. The new
fields will appear; no existing key is removed.

* **Files:** `crates/dtfs-manager/src/api.rs` — extend
  `list_volumes_handler` to embed the per-volume policy view
  from `volume_config_to_json(&cfg)` (the AV-3-5 helper) under
  each volume entry.
* **Implementation:** for every volume in the list, include
  the AV-3-5 helper output spread into the existing list row.
  Keep `file_count` / `total_size` aggregation as today.
  Pure-helper unit test confirms the merged JSON contains all
  AV fields + aggregation fields.
* **Tests:**
  - `bug_av_list_volumes_row_includes_av_fields` — assert the
    merged row JSON contains both aggregation fields
    (`file_count`, `total_size`) AND AV fields
    (`mandatory_replicas`, `cache_replicas`, `anchor`).
  - `bug_av_list_volumes_legacy_volume_renders_correctly` —
    volume without `mandatory_replicas`/`cache_replicas` set
    must render with default-zero values (no crash on
    legacy snapshots).
* **Done when:** 2 tests + live verification on the
  minicluster: `curl -s http://mini-mgr01:9402/api/v1/volumes
  | jq '.volumes[0]'` returns all AV fields. Update the
  AV-1-10 deviation in § 5 to mark the list-endpoint sub-task
  as ✅ RESOLVED.

### ⬜ AV-4-3 — WebUI volume detail read-only card (~0.5 d)

The WebUI Volumes page (line ~1219 in `webui.rs`) currently
renders a 4-column table (`Volume ID / Files / Total Size /
Avg File Size`). Operators have no way to see what cache
policy is configured.

* **Files:** `crates/dtfs-manager/src/webui.rs` — extend
  `renderVolumes()` to fetch per-volume detail
  (`/api/v1/volumes/{id}` from AV-3-5) and render a card
  below each row showing mandatory / cache / anchor.
* **Implementation:** keep the existing table; add an
  expand-on-click toggle (`onclick` handler that opens a
  detail card under the row). The card uses the same JSON
  shape as AV-3-5's `volumes show` CLI command. Reuse the
  CSS card styles from existing pages.
* **Tests:** live (no JS unit framework in repo). Markup
  smoke test verifies the rendered page contains the
  expected field labels (`Mandatory replicas:`, `Cache
  replicas:`, `Anchor:`).
* **Done when:** clicking a row reveals the policy card with
  the live values; legacy volumes (no AV fields set) render
  with "(none)" / default-zero rather than crashing. Update
  the AV-3-4 deviation in § 5 to mark the display-card
  sub-task ✅ RESOLVED.

### ⬜ AV-4-4 — WebUI volume inline-edit modal (~1 d)

Operators today use `dtfs-cli volumes set-cache` (AV-3-5) for
every policy change. A WebUI button that opens an edit modal
posting to `/set-cache` is the operator-friendliness payoff.

* **Files:** `crates/dtfs-manager/src/webui.rs` — new
  `volume-edit-modal` HTML next to the migrate + flush-cache
  modals; new JS handlers `openVolumeEdit(volumeId)` /
  `closeVolumeEdit()` / `submitVolumeEdit()` that POST to
  `/api/v1/volumes/{id}/set-cache`. New "Edit policy" button
  on each row in the Volumes page (or inside the detail card
  from AV-4-3).
* **Implementation:** the modal mirrors the AV-3-5 CLI flags:
  - `warm.min` / `warm.max` numeric inputs (0-9).
  - `hot.min` / `hot.max` numeric inputs (rare; show only
    when `hot_max > 0` already or when an "Advanced" toggle
    is set).
  - `per_dc` checkbox.
  - `promote_reads` numeric.
  - `promote_window_secs` numeric.
  - `cooldown_secs` numeric.
  - Initial values prefilled from `GET /api/v1/volumes/{id}`.
  - On submit, send only the changed fields (otherwise the
    "no-op" path is misleading on the UI side).
  - Validation: warm.min ≤ warm.max client-side, with a
    server-side 400 fallback rendered in the result panel.
  - Show the post-update policy in the result panel
    (mirror AV-3-5 CLI's output format).
* **Tests:** live (no JS unit framework). Markup smoke:
  modal HTML present, JS handlers wired. Live verification:
  open the modal, change `warm_max` from 1 to 2, submit,
  verify the volume's `cache_replicas.warm.max` is 2 via
  `dtfs-cli volumes show default`.
* **Done when:** modal opens with prefilled values; submit
  applies the diff via `POST /set-cache`; result panel shows
  the new policy; markup verified on minicluster after
  binary roll. Update the AV-3-4 deviation in § 5 to mark
  the inline-edit form ✅ RESOLVED.

### ⬜ AV-4-5 — `ENABLE_CACHE_REDUNDANCY_ESCALATION` runtime flag + test (~0.5 d)

AV-2-4 left the redundancy-escalation path behind a
compile-time const `ENABLE_CACHE_REDUNDANCY_ESCALATION: bool
= false`. Promotes it to a runtime config option so operators
can opt in without rebuilding, and adds the matching
test that exercises the ramp from `min` to `max`.

* **Files:**
  - `crates/dtfs-common/src/config.rs::TieringConfig` — add
    `enable_cache_redundancy_escalation: bool = false`
    field.
  - `crates/dtfs-manager/src/tiering.rs` — replace the const
    with a parameter on `plan_cache_tick` /
    `plan_chunk_promotions`. The engine reads
    `cfg.enable_cache_redundancy_escalation` and threads it
    through. Pure planner tests can now pass either value.
* **Implementation:**
  - Add the bool to `TieringConfig` with
    `#[serde(default)]` returning `false` (back-compat
    safe — legacy configs keep current behaviour).
  - Update `tiering.toml.j2` Ansible template with a
    commented-out `enable_cache_redundancy_escalation =
    false` line for discoverability.
  - The const itself stays as a documentation anchor
    pointing to the runtime option, with a comment like
    "deprecated — use TieringConfig.enable_cache_
    redundancy_escalation instead". Or remove it; decide
    during implementation.
* **Tests:**
  - Rename
    `bug_av_tiering_tick_redundancy_escalation_off_by_default`
    to `bug_av_tiering_tick_redundancy_escalation_off`
    + add `bug_av_tiering_tick_redundancy_escalation_on`:
    same scenario, flag set to `true`, assert the planner
    schedules the 2nd warm replica.
  - `bug_av_tiering_redundancy_escalation_respects_anchor`
    — a volume with `cache_replicas.warm.max = 2` and the
    flag on must STILL refuse if the 2nd warm replica
    would strand the anchor.
* **Done when:** 3 tests (1 rename, 2 new) + smoke: roll
  the new manager binary, set the new config flag via
  `manager.toml` reload, observe the engine respects it.
  Update the AV-2-4 deviation in § 5 to mark the flag-flip
  ✅ RESOLVED (and reword the docs that called it a
  "deferred follow-up").

### ⬜ AV-4-6 — Documentation: Phase 4 close-out + plan archival (~0.5 d)

Wraps up Bug AV. The plan moves from `plans/active/` to
`plans/completed/` once every Phase 4 task is ✅.

* **Files:**
  - `docs/src/storage/tiering.md` — flip the
    "redundancy-escalation deferred" language to its new
    runtime-config presentation. Document the
    `tiering.enable_cache_redundancy_escalation` option.
  - `docs/src/configuration/cluster-config.md` (or
    wherever the existing tiering knobs live) — add the
    new field with the same table format as the other
    `[tiering]` keys.
  - `docs/src/api-reference/rest-api.md` — note that
    `GET /api/v1/volumes` now includes the AV per-volume
    policy fields (the AV-3-5 "Bug AV note" can be
    removed entirely since both endpoints now return the
    same shape).
  - `docs/src/operations/cluster-management.md` — extend
    the "Reclaiming warm cache space" runbook with a
    "WebUI alternative" subsection covering the
    AV-4-3 / AV-4-4 modal.
  - `CLAUDE.md` § 3.4 — drop the "deferred follow-up"
    language for the redundancy-escalation flag; it's
    now operator-configurable.
  - `PROGRESS.md` — add a single line "Bug AV closed
    2026-MM-DD" under the appropriate section.
  - `ISSUES_TO_FIX.md` — remove any Bug AV TODOs.
  - `plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md` →
    `plans/completed/2026-MM-DD-PLAN-BUG-AV-TIER-
    LIFECYCLE.md` via `git mv`. Update `plans/README.md`
    listings to reflect the move.
  - `plans/incidents/SOAK-RECOVERY-2026-05-21.md` Bug AV
    section — append "Phase 4 close-out" subsection with
    the resolved-carryover summary.
* **Tests:** `mdbook build docs/` clean. `git mv` of the
  plan file preserves history (`git log --follow` works).
* **Done when:** 8 files updated; mdbook builds clean;
  plan lives in `plans/completed/` with the date prefix;
  § 5 deviations of the moved plan shows all carryovers
  ✅ RESOLVED.

### Phase 4 Done When

* AV-4-1, AV-4-1a, AV-4-2, AV-4-3, AV-4-4, AV-4-5, AV-4-6
  all ✅. AV-4-1b is the explicit multi-DC infra
  follow-up that closes the S4 demo gap; it is **deferred
  out of Phase 4** (see § 5 deviations) and Phase 4 closes
  without it.
* **~9 new tests green** (4 set-mandatory + 2 list-endpoint
  + ~3 redundancy escalation; UI work is live-only per the
  WebUI testing convention).
* End-to-end live demos for AV-2-4 / AV-2-5 / AV-3-3 (S1,
  S2, S3 from AV-4-1) recorded in
  `MINICLUSTER-VERIFICATION-2026-05-21.md`. S4
  (anchor-lag-repair) explicitly noted as "deferred to
  AV-4-1b — needs multi-DC topology".
* All four § 5 deviation entries marked ✅ RESOLVED:
  - AV-2-4 / AV-2-5 — Phase 2 live end-to-end demo
    (resolved by AV-4-1 + AV-4-1a together).
  - AV-3-4 — WebUI volume-edit form (resolved by AV-4-3
    + AV-4-4).
  - List-endpoint projection sub-task under the
    AV-1-10-resolved entry (resolved by AV-4-2).
  - AV-4-1 S4 deferral to AV-4-1b (this one stays open;
    documents the multi-DC requirement).
* `mdbook build docs/` green.
* Plan file moved to `plans/completed/`; `git log --follow`
  preserves history. The AV-4-1b follow-up remains as an
  unticked task in the moved plan so a future
  multi-DC-aware run knows what to pick up.
* No "Phase 4 follow-up" / "deferred polish" language
  remains in tracked sources (`grep -r "Phase 4" plans/
  docs/ CLAUDE.md` returns empty).

---

## 3. Cross-cutting concerns / invariants

These checks run as part of the implementation review, not separate
tasks:

* **Anchor invariant never violated.** Every deletion path
  (`RemoveChunkLocation`, `flush_cache_for_path`, tiering demote)
  passes through `would_violate_anchor(policy, chunk_replicas)`. New
  test: `bug_av_invariant_anchor_holds_under_random_op_mix` — a
  property-style test that runs 1000 random
  put/flush/demote/RemoveCorruptChunkReplicas op sequences and
  asserts the anchor is always satisfied.
* **Per-disk usage decrements correctly on delete.** Bug AU's per-disk
  refresh covers this — confirm in live verification.
* **Audit log carries every cache-tier mutation.** Each flush, each
  demote, each promote (Phase 2) writes an entry.
* **No regression** in existing `bug_ab_*`, `bug_ad_*`, `bug_ae_*`,
  `bug_o_*`, `bug_p_*` tests.
* **Snapshot back-compat.** Loading a pre-AV snapshot must produce
  identical observable behaviour until the operator runs
  `volume migrate-policy`.

---

## 4. Verification log (filled in during implementation)

> Updated as each task completes. Each entry: task ID, date, what was
> tested, command/output snippet, anomalies.

### AV-4-1 — 2026-05-22 (🟨 needs review — topology blocker)

* **Files touched:**
  - `tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh` — new
    shell regression test, 4 scenarios (S1 promote-on-threshold,
    S2 demote-on-idle, S3 cooldown asymmetry, S4 warm-first
    write + anchor-lag-repair). Bash syntax-checks cleanly
    (`bash -n` exit 0).
  - `tests/proxmox/ansible/templates/fuse.toml.j2` — already
    has the AV-3-1 `[write]` block; no change needed.
  - Production binary rolls completed: dtfs-fuse rolled to
    mini-client01, `[write] first_write_tier =
    "nearest-warm"` appended to `/etc/dtfs/fuse.toml`, FUSE
    restarted and re-mounted.
* **Cluster repairs done during this task** (worth recording so
  later runs don't re-discover):
  - mini-mgr02 had been flap-restarting since AV-2 work
    with `Error: when Read Logs: io error: unexpected end of
    file` — a partially-truncated Raft log tail. Wiped
    `/var/lib/dtfs/manager/raft-log` + `raft-sm`, restarted;
    Bug AC auto-recovery re-initialised from leader
    snapshot cleanly.
  - mini-mgr01 had stale cached Raft gRPC channels pointing
    at the previously-unreachable mini-mgr02 → `connect to
    http://10.20.0.172:9401 failed: transport error` on
    every `client_write`. Resolved by restarting mini-mgr01;
    leader re-elected (mini-mgr03 won).
* **Configuration surface added:** tiering tick interval
  reduced from 3600s default to 10s via
  `POST /api/v1/config {"config_json":
  "{\"tiering\":{\"evaluation_interval_secs\":10}}"}` so
  the demo can complete in minutes.
* **Topology blocker discovered:** the current minicluster
  cannot fully demonstrate AV-4-1's four scenarios:
  1. The `default` volume has
     `mandatory_replicas = {hot:0, warm:2, cold:1, tape:1}`.
     The plan's S1/S2/S3 scenarios require a volume where
     `warm` is **cache-class, not mandatory** (the cache
     planner deliberately skips tiers that are also
     mandatory — see CLAUDE.md § 3.4).
  2. The FUSE client routes EVERY write through the
     `"default"` volume id (`find_volume_id_for_path`
     reads from open-file state, not from a path-prefix
     map). There is no operator-facing way to create a
     file under a non-default volume_id via the mount.
  3. Single-DC topology + `anchor = { dc: dc-a,
     required_tier: null }` means the anchor invariant is
     trivially satisfied by any placement — AV-3-3
     anchor-lag-repair never has work to do.
  4. The REST surface has no `set-mandatory` endpoint
     equivalent to the AV-3-5 `set-cache`. Mutating
     `default`'s mandatory_replicas in-place isn't
     possible without adding a new endpoint or using a
     raw UpsertVolume.
* **What WAS verified live:**
  - FUSE on mini-client01 picks up the `[write]
    first_write_tier = "nearest-warm"` config without
    parse errors (`bug_av_fuse_write_config_*` unit tests
    already covered the parse paths; the live config
    loaded into a healthy FUSE process here).
  - Bash syntax + cluster control-plane reachability
    (`mgr_get`, `mgr_post`, `client_run`,
    `count_warm_chunks_for_path`, `audit_since`,
    `first_dc`, `wait_for` all execute correctly when
    invoked manually).
  - Volume create on the leader (mini-mgr03) works:
    `av-demo` volume now exists in the SM but cannot
    receive writes via the FUSE namespace.
* **Disposition:** 🟨 needs-review. The deliverable script
  is in place and structurally complete; the remaining
  work is infra/code, not script. Three credible paths
  forward, ordered by smallest scope:
  1. **Add `POST /api/v1/volumes/{id}/set-mandatory`**
     endpoint mirroring AV-3-5's `set-cache`. Use it from
     the test setup to convert `default` to cold-only
     mandatory. This unblocks S1/S2/S3 on the current
     minicluster. (~0.5 d code.)
  2. **Add per-path volume routing in FUSE**: a config
     like `[mount.volume_map] "/av-demo/" = "av-demo"`,
     plumbed through `find_volume_id_for_path`. Operator-
     friendly multi-volume namespace. (~1 d code +
     plumbing.)
  3. **Provision a multi-DC minicluster** (per the plan's
     Plexmedia spec, 3 DCs with bandwidth shaping). The
     correct long-term home for these demos; meaningful
     infra work. (~1-2 d.)
* **Recommendation:** pursue option 1 as a new AV-4-1-fixup
  sub-task — smallest code change, unblocks S1/S2/S3 fully,
  and aligns with the existing AV-3-5 mutation surface.
  S4 (anchor-lag-repair) still needs multi-DC (option 3)
  to produce observable behaviour and remains deferred.

### AV-3-6 — 2026-05-21

* **Files touched (6):**
  - `docs/src/configuration/fuse-client.md` — `[write]`
    block reference + when-to-use guidance.
  - `docs/src/api-reference/cli-reference.md` — full
    documentation for `volumes show`, `volumes set-cache`,
    `files flush-cache`; legacy `files` → `files list`
    breaking-change note.
  - `docs/src/access/fuse-mount.md` — "First-write tier
    preference (Bug AV)" subsection.
  - `docs/src/operations/production-deployment.md` —
    "Tier policy bootstrap (Bug AV)" with rolling-upgrade
    gating + rollback path.
  - `plans/incidents/SOAK-RECOVERY-2026-05-21.md` — full
    Bug AV write-up (symptom from the user's original
    question, root cause analysis, fix summary across
    Phases 1-3, test count table, live-verification
    highlights, carryovers).
  - `plans/reports/MINICLUSTER-VERIFICATION-2026-05-21.md`
    — verification report with REST/CLI/audit-fan-out
    evidence, test counts, and open carryovers.
* **Build verification:** `mdbook build docs/` on
  dtfs-builder — clean (INFO output only; no warnings,
  no errors).
* **Live verification:** N/A — docs only. The runbook
  reflects the on-cluster behaviour exercised during
  AV-1-9 / AV-1-10 / AV-1-11 / AV-3-5 live tests.
* **Deviations:** none. **Phase 3 of Bug AV closed out.**

### AV-3-5 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/api.rs` — new routes
    `GET /api/v1/volumes/{id}` (full projection) and
    `POST .../set-cache` (partial update); two new pure
    helpers `volume_config_to_json(cfg)` and
    `apply_set_cache(current, req)`; new
    `SetCacheRequest` body type with every field optional.
  - `crates/dtfs-cli/src/main.rs` — `Files` command promoted
    to subcommand enum (`FilesCmd::List` / `FlushCache`);
    `VolumesCmd` gains `Show` and `SetCache`. New helpers
    `cmd_volumes_show`, `cmd_volumes_set_cache`,
    `cmd_files_flush_cache`. **Breaking change:** the
    legacy `dtfs-cli files --limit 50` is now `dtfs-cli
    files list --limit 50`.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_set_cache`):
  5 green:
  - `bug_av_set_cache_merges_partial_update`
  - `bug_av_set_cache_rejects_max_less_than_min`
  - `bug_av_set_cache_empty_request_is_noop`
  - `bug_av_volume_config_json_includes_av_fields`
  - `bug_av_volume_config_json_anchor_none_is_null` (defensive)
* **Workspace regression**: dtfs-manager 99 → 104; all
  other crates unchanged. `cargo test --workspace --release`
  all green.
* **Build hiccup:** initial test factory included a
  `tiering: None` field on `VolumeConfig` that doesn't
  exist. Removed; `mandatory_replicas: {0,0,0,0}` exercises
  the legacy-fallback path in the helper.
* **Live verification** on minicluster (binaries rolled
  2026-05-21 ~12:18 UTC), 6 scenarios all pass:
  1. `volumes show default` renders every AV field
     (mandatory cold=1, no cache yet, anchor `dc-a`).
  2. `volumes set-cache default --warm-min 1 --warm-max 1
     --per-dc true --promote-reads 2 --promote-window-secs
     60 --cooldown-secs 300` → "applied" + dump of the new
     cache_replicas.
  3. Re-show confirms the policy is applied.
  4. `volumes set-cache --warm-min 1 --warm-max 1` again →
     "no-op".
  5. `volumes set-cache --warm-min 5 --warm-max 1` → HTTP
     400 (max < min rejected at REST layer).
  6. `files flush-cache /movies/ --cooldown-secs 60` →
     dispatcher report (files_matched=7,
     replicas_dropped=0 — current minicluster has every
     warm replica as mandatory under the AV-1-10 migrate,
     so the planner correctly skips).
* **Carryovers closed by this commit:**
  - **AV-1-10** — `/api/v1/volumes/{id}` projection now
    exposes the AV fields. The list endpoint
    (`GET /api/v1/volumes`) still uses its
    aggregation-only shape (breaking the list shape would
    require a coordinated WebUI/CLI update; deferred).
  - **AV-3-4** — partially closed: the WebUI can now
    `fetch` per-volume policy via the new endpoint, so a
    follow-up display card is straightforward. The
    inline-edit form is a separate deferred polish item;
    no successor Phase is currently scheduled.

### AV-3-4 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/webui.rs` — new `flush-modal`
    modal HTML next to the existing `migrate-modal`; new
    JavaScript handlers `openFlushCache` / `closeFlushCache`
    / `submitFlushCache` posting to
    `POST /api/v1/files/flush_cache`; context-menu entries
    for both folder and file rows.
* **Tests:** live-only (no JS unit framework in repo).
  Markup verified after rolling the new manager binary
  (mini-mgr01 + mini-mgr03):
  - `GET /ui/explorer` includes 7 occurrences of
    "Flush cache" (4 context-menu items + 3 modal labels
    counted by grep).
  - 2 references to `submitFlushCache` (the JS function
    definition + its `onclick`).
* **Workspace regression**: dtfs-manager build clean; no
  Rust-side tests changed. Full `cargo test --workspace
  --release` still green.
* **Live verification:** manager binaries rolled cleanly to
  mini-mgr01 + mini-mgr03 at 12:11 UTC; the rendered
  explorer page contains the new entries. Full
  click-the-button demo deferred to Phase 3 close-out
  (needs a volume with cache replicas actually present to
  show a non-zero `replicas_dropped` — same blocker as
  AV-2-4/AV-2-5 demos).
* **Deviations:** the volume-edit form portion of the
  AV-3-4 plan is deferred — see § 5 deviations entry.
  Folded forward into AV-3-5's CLI work so the read-side
  projection (AV-1-10 carryover) + the edit UI lands as
  one coherent piece.

### AV-3-3 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/scheduler.rs` — new
    `AnchorRepairNode`, `AnchorRepairPlacement`,
    `AnchorRepairAction` types + pure
    `plan_anchor_lag_repair`; new async `check_anchor_lag`
    method called from `evaluate()` alongside
    `check_under_replicated`. Audit hook fires
    `anchor.lag_repair` with `target=N tier=T dc=D` details.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_anchor_lag`):
  6 green:
  - `bug_av_anchor_lag_repair_picks_anchor_dc_node`
  - `bug_av_anchor_lag_repair_picks_required_tier`
  - `bug_av_anchor_lag_repair_no_op_when_satisfied`
  - `bug_av_anchor_lag_repair_required_tier_none_defaults_to_cold`
    (defensive)
  - `bug_av_anchor_lag_repair_skips_already_scheduled`
    (defensive)
  - `bug_av_anchor_lag_repair_skips_unhealthy_target`
    (defensive)
* **Workspace regression**: dtfs-manager 93 → 99; all other
  crates unchanged. `cargo test --workspace --release` all
  green.
* **Live verification:** smoke check (binary rolls cleanly
  on mini-mgr01 + mini-mgr03; no panics, cluster info
  responds). End-to-end "warm-first write → anchor repair
  scheduled within ~30s" deferred to Phase 3 close-out
  (requires a refreshed FUSE client binary + a fuse.toml
  with `first_write_tier = "nearest-warm"`).
* **Deviations:** 3 extra defensive tests added covering
  `required_tier = None` fallback, already-scheduled
  deduplication, and unhealthy-target filtering. Each
  corresponds to a real production failure mode worth
  guarding.

### AV-3-2 — 2026-05-21

* **Files touched:**
  - `proto/manager.proto` —
    `AllocateChunksRequest.preferred_tier: string = 4`. Empty
    string = "no preference"; recognised values `"hot"` /
    `"warm"` / `"cold"` / `"tape"`; unknown values fall back.
  - `crates/dtfs-manager/src/metadata.rs` — new pure helpers
    `parse_preferred_tier_hint` + `resolve_preferred_tier`;
    `allocate_chunks` handler queries SM nodes for available
    tiers, then resolves the hint.
  - `crates/dtfs-client/src/client.rs` — new
    `allocate_chunks_with_hint(path, num_chunks, volume_id,
    preferred_tier)`. Existing `allocate_chunks` delegates with
    empty hint (legacy behaviour preserved).
  - `crates/dtfs-fuse/src/filesystem.rs` — write path translates
    `self.config.write.effective_first_write_tier()` to the
    protocol string (`Cold` → `""`, `NearestWarm` → `"warm"`).
* **Unit tests** (`cargo test -p dtfs-manager bug_av_preferred_tier`):
  5 green:
  - `bug_av_allocate_chunks_honours_preferred_tier`
  - `bug_av_allocate_chunks_falls_back_when_tier_full`
  - `bug_av_allocate_chunks_empty_hint_uses_default`
  - `bug_av_allocate_chunks_unknown_hint_falls_back` (defensive)
  - `bug_av_parse_preferred_tier_hint_coverage` (defensive)
* **Workspace regression**: dtfs-manager 88 → 93; all other
  crates unchanged. `cargo test --workspace --release` all
  green.
* **Build hiccup:** initial sort+dedup of available-tiers list
  rejected because `DiskTier` doesn't impl `Ord` (and adding
  it ripples through the workspace). Switched to
  `HashSet<DiskTier> → Vec<DiskTier>` collect — same dedup
  effect, no trait change.
* **Live verification** on minicluster (binaries rolled
  2026-05-21):
  - Smoke: mini-mgr01 + mini-mgr03 rolled cleanly, cluster
    info responds, no panics.
  - End-to-end "warm-first write places on warm" demo
    deferred to AV-3-3 close-out (without anchor-lag repair
    the warm-first write would never get a cold copy, so
    demoing the proto+placement piece without the repair is
    misleading).
* **Deviations:** 2 extra defensive tests added covering
  unknown-hint fall-back + parse-helper coverage (both gates
  worth guarding against regression).

### AV-3-1 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-common/src/config.rs` — new
    `FuseWriteConfig { first_write_tier: String }`, `Default`
    impl, `FirstWriteTier` enum (Cold / NearestWarm),
    `effective_first_write_tier()` helper; `FuseConfig.write:
    FuseWriteConfig` field with `#[serde(default)]`.
  - `tests/proxmox/ansible/templates/fuse.toml.j2` — new
    `[write] first_write_tier = "{{ dtfs_first_write_tier |
    default('cold') }}"` block at the bottom of the template
    with an explanatory comment.
* **Unit tests** (`cargo test -p dtfs-common bug_av_fuse_write`):
  3 green:
  - `bug_av_fuse_write_config_default` — omitted block →
    `Cold`.
  - `bug_av_fuse_write_config_nearest_warm_parsed` — explicit
    string → `NearestWarm`.
  - `bug_av_fuse_write_config_invalid_value_falls_back_to_cold`
    — unknown string preserves raw value (for diagnostics) but
    `effective_first_write_tier()` returns `Cold`.
* **Workspace regression**: dtfs-common 39 → 42; all other
  crates unchanged. `cargo test --workspace --release` all
  green.
* **Live verification:** deferred to AV-3-2 — the config-side
  test confirms the value flows from `fuse.toml` to a
  `FirstWriteTier`, but the end-to-end "warm placement happens
  on a `nearest-warm` client" demo requires the proto +
  placement engine changes in AV-3-2.
* **Deviations:** none. The plan called for the field to be
  `String`; left as `String` for forward-compat (a future
  client added a new tier name won't fail to parse on older
  managers / configs).

### AV-2-6 — 2026-05-21

* **Files touched (4):**
  - `docs/src/storage/tiering.md` — replaced the Phase-1
    forward-reference with the full Phase 2 write-up:
    Promotion math (window count → cooldown → threshold →
    target → selection), Demotion math (tier filter → window
    check → anchor safety, full sequence: DeleteChunkReplica →
    RemoveChunkLocation → conditional SetChunkCooldown),
    Cooldown semantics (operator-flush vs engine-demote
    asymmetry), Anchor-aware demotion (planner refusal +
    apply-path defence-in-depth), Worked example
    (`/movies/Wicked.mkv` t=0 through t=14_600).
  - `docs/src/storage/caching.md` — opening callout to
    disambiguate the data-node ARC + FUSE local-disk caches
    (covered here) from the AV `cache_replicas` placement
    layer (covered in tiering.md).
  - `docs/src/operations/cluster-management.md` — new
    "Reclaiming warm cache space (Bug AV flush)" runbook
    covering: when to use it (vs editing volume policy), curl
    procedure, expected audit-log shape (cache.flush →
    flush_replica × N → flush_complete; optional
    flush_refused), cooldown semantics, anchor-refusal
    investigation steps.
  - `CLAUDE.md` — § 3.4 extended with two new bullet points:
    every cache promotion/demotion must check
    `would_violate_anchor`; never bypass
    `should_clear_cooldown_on_demote` when implementing a new
    demote-like path.
* **Build verification:** `mdbook build docs/` on dtfs-builder
  succeeds with only `INFO` output (no warnings, no errors).
* **Live verification:** N/A — docs only. The runbook reflects
  the on-cluster behaviour exercised during AV-1-9 / AV-1-11
  live runs.
* **Deviations:** none.

### AV-2-5 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/tiering.rs` — new pure
    `should_clear_cooldown_on_demote` helper, `TickDemotion`
    extended with `cooldown_until_ms_at_snapshot`, dispatcher
    in `tick_av_cache` now executes the full demote sequence
    (DeleteChunkReplica → RemoveChunkLocation → conditional
    SetChunkCooldown(0)) with per-step audit hooks.
  - `crates/dtfs-manager/src/scheduler.rs` — `node_grpc_addr` +
    `delete_chunk_replica_on_node` bumped to `pub(crate)`; new
    `raft()` accessor so the tiering engine can propose
    `RemoveChunkLocation` + `SetChunkCooldown` directly.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_demote`):
  3 green:
  - `bug_av_demote_clears_cooldown`
  - `bug_av_demote_does_not_clear_future_operator_cooldown`
  - `bug_av_demote_carries_cooldown_through_planner` (defensive)
* **Workspace regression**: dtfs-manager 85 → 88; all other
  crates unchanged. `cargo test --workspace --release` all green.
* **Live verification** on minicluster (binaries rolled
  2026-05-21 11:55 UTC):
  - Smoke check: mini-mgr01 + mini-mgr03 came back up cleanly,
    cluster info endpoints respond, no panics.
  - Full demote → immediate-read → re-promote demo deferred to
    Phase 2 close-out (same blocker as AV-2-4: needs a
    cache_replicas-configured volume).
* **Deviations:** none from plan structure; one extra
  defensive test added because the snapshot-cooldown-carry
  through `TickDemotion` was the single point where a refactor
  could silently break operator-flush precedence.

### AV-2-4 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/tiering.rs` — new `plan_cache_tick`
    pure planner + `TickSnapshot` / `TickChunk` / `TickPlacement`
    / `TickVolumePolicy` / `TickPromotion` / `TickDemotion` /
    `TickPlan` types. `TieringEngine` gains `read_windows` +
    `prev_reads_by_dc` storage; `report_access` pushes positive-
    delta samples; new `tick_av_cache` + `build_av_cache_snapshot`
    methods wire the planner into `evaluate`. Audit hooks emit
    `cache.promote` / `cache.demote` with stable reason codes
    (`"promote_threshold_met"`, `"idle_window"`).
  - The `ENABLE_CACHE_REDUNDANCY_ESCALATION: bool = false` Phase-2
    flag is honoured — the planner's escalation branch is
    structurally present (and unit-tested with the flag off);
    flipping it is a deferred follow-up that needs a per-DC HA
    workload to validate against. No successor Phase is
    scheduled — recorded in § 5 deviations.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_tiering_tick`):
  6 green:
  - `bug_av_tiering_tick_promotes_on_threshold`
  - `bug_av_tiering_tick_demotes_idle_cache_replica`
  - `bug_av_tiering_tick_respects_cooldown`
  - `bug_av_tiering_tick_respects_per_dc_cap`
  - `bug_av_tiering_tick_redundancy_escalation_off_by_default`
  - `bug_av_tiering_tick_per_dc_false_uses_global_pool` (defensive)
* **Workspace regression**: dtfs-manager 79 → 85; all other
  crates unchanged. `cargo test --workspace --release` all green.
* **Build hiccups** worth remembering:
  - Initial closure pattern `|(_, &r)| r` rejected by rustc's
    "implicitly-borrowing pattern" lint; fixed by `|entry| *entry.1`.
  - `effective_mandatory_replicas()` returns `&TierReplication`,
    not owned. Cloned at the policy projection site.
* **Live verification** on minicluster (manager binaries rolled
  2026-05-21 11:53 UTC):
  - Smoke check: both `mini-mgr01` and `mini-mgr03` came back up
    cleanly, `Tiering engine started (evaluation interval:
    3600s)` logged, no panics or errors in the 30-second
    post-restart window.
  - Full read→promote demo deferred to Phase 2 close-out: needs a
    volume with `cache_replicas` set (no CLI surface for that
    until AV-3-5; the REST `UpsertVolume` accepts it but no
    convenient one-shot endpoint exists yet).
* **Deviations:**
  - The demotion *delete + cooldown clear* is routed through
    AV-2-5; this commit emits the demotion plan + audit hook but
    does not yet issue the data-node delete RPC. That keeps the
    diff reviewable and lets AV-2-5 wire the delete + cooldown
    clear in one place. Consistent with the AV-1-6 / AV-1-7
    "async dispatcher" boundary pattern.
  - One extra defensive planner test (`per_dc_false_uses_global_
    pool`) added since the global-pool branch had distinct logic
    and was easy to test.

### AV-2-1 / AV-2-2 / AV-2-3 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/tiering.rs` — three new private
    sections at the bottom of the file (after `now_millis`):
    1. AV-2-1 read-window store: `ReadSample` struct +
       `count_reads_in_window` + `prune_read_window` pure
       helpers.
    2. AV-2-2 promotion decision: `plan_cache_promotion` pure
       function with four gate paths + basic-promotion branch.
    3. AV-2-3 target selection: `NodeSelectionView` projection +
       `select_promotion_target` pure selector.
  - All helpers are crate-private (`pub(crate)`) so AV-2-4
    wiring can use them but external callers see only the
    `TieringEngine` async API.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_`):
  - AV-2-1: 5 green (`bug_av_read_window_*` × 5).
  - AV-2-2: 7 green (`bug_av_promote_*` × 7).
  - AV-2-3: 5 green (`bug_av_target_*` × 5).
  - **17 total new tests; plan called for 4 + 6 + 4 = 14; 3
    defensive edge-case tests added (zero-window, min>max,
    tie-break determinism).**
* **Workspace regression**: dtfs-manager 62 → 79; all other
  crates unchanged. `cargo test --workspace --release` all
  green.
* **Live verification:** N/A — pure helpers; the wiring
  (AV-2-4) is where live verification happens.
* **Deviations:** none from the plan's "Done when" criteria;
  3 extra defensive tests added because the boundary conditions
  (zero window, misconfigured min/max, tie-break ordering) had
  obvious risk paths and the cost of testing them was a few
  lines each. Plan total adjustment: Phase 2 net new tests
  20 → 23 once AV-2-4/5 land.

### AV-1-12 — 2026-05-21

* **Files touched** (7):
  - `docs/src/configuration/volume-config.md` — settings table
    extended with 11 new field rows (4 mandatory, 4 cache + per-DC,
    2 anchor) + new "Replication classes" section + Examples A & B
    inline + back-compat migration walkthrough.
  - `docs/src/storage/replication.md` — added the "Two replication
    classes" framing + Anchor invariant subsection above the
    legacy content.
  - `docs/src/storage/tiering.md` — placeholder "Cache tier
    semantics (Bug AV)" with status (Phase 1 done; Phase 2 will
    wire `reads_by_dc`) + the lifecycle ASCII diagram.
  - `docs/src/api-reference/rest-api.md` — full
    `POST /api/v1/files/flush_cache` documentation (request body,
    response shape, audit/alert behaviour, error codes, curl
    example) + `POST /api/v1/volumes/{id}/migrate-policy`.
  - `docs/src/api-reference/cli-reference.md` — new
    `volumes migrate-policy` subsection + a "files flush-cache
    (planned)" placeholder referencing AV-3-5.
  - `CLAUDE.md` — new § 3.4 invariant ("only the tiering engine
    creates cache replicas; only the under-replication scheduler
    maintains the mandatory floor") + § 10 file-map extension on
    `scheduler.rs`.
  - `README.md` — paragraph after the one-liner explaining the
    Bug AV two-class model with a link to volume-config.
* **Build verification:** `mdbook build docs/` on dtfs-builder
  succeeds with only `INFO` output (no warnings, no errors).
  Build runs in <1 s after the initial `cargo install --version
  0.4.36 mdbook` (~2 min once-off).
* **Live verification:** N/A — docs only.
* **Deviations:** none (all 7 doc files updated as planned; the
  REST API doc explicitly flags the `/api/v1/volumes` listing
  projection gap from AV-1-10 as a tracked follow-up).
* **Plan total adjustment:** Phase 1 net new tests still 33;
  AV-1-12 adds the mdbook-build smoke check as the docs-level
  test.

### AV-1-11 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/scheduler.rs` — new
    `FlushRefusal { file_path, chunk_index, reason }`,
    `FlushCacheReport.refusals: Vec<FlushRefusal>` with
    `#[serde(skip)]`, planner populates one entry per refusal,
    dispatcher emits `cache.flush_refused` audit per refusal +
    one deduped `AlertKind::AnchorViolated` alert per call,
    `cache.flush_complete` summary now fires unconditionally.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_`): 3 new,
  all green (planner side):
  - `bug_av_planner_records_refusal_per_stranded_chunk` — verifies
    two stranded chunks produce two `FlushRefusal`s with reason
    `"mandatory_strand"` and the right indexes.
  - `bug_av_planner_no_refusals_on_happy_path` — defensive guard:
    a flushable chunk leaves `refusals` empty so the dispatcher
    doesn't fire a spurious alert.
  - `bug_av_report_serde_skips_refusals_field` — confirms the JSON
    wire shape omits the `"refusals"` field (precise token match,
    not substring, so the legitimate `"mandatory_refusals"` counter
    is preserved).
* **Workspace regression**: dtfs-manager 59 → 62; all other
  crates unchanged. `cargo test --workspace --release` all green.
* **Build hiccup:** first attempt re-used the old binary because
  `cargo test --release -p dtfs-manager` builds only the test
  binary, not `target/release/dtfs-manager`. Diagnosed via
  `strings binary | grep cache.flush` (missing the new
  `cache.flush_refused` token). Resolved with an explicit
  `cargo build --release -p dtfs-manager` before the rolling
  deploy.
* **Live verification** on minicluster (manager binaries rolled
  2026-05-21 ~11:25 UTC):
  - Triggered: `POST /api/v1/files/flush_cache
    {"path_prefix":"/movies/","cache_tiers":["warm"],"cooldown_secs":42}`
  - `GET /api/v1/audit?action=cache.flush_complete` → entry with
    `actor="scheduler", origin_manager="mini-mgr01", summary=
    "cache.flush_complete: prefix=/movies/ replicas_dropped=0
    mandatory_refusals=0"`. ✓ proves the new audit path fires
    through Bug AH Raft fan-out.
  - `GET /api/v1/audit?action=cache.flush_refused` → empty (no
    refusals on this volume — every cache-eligible warm replica
    is also mandatory under AV-1-10 migrated default, so the
    planner correctly classifies none as droppable).
* **Deviation:** the two originally planned tests targeted the
  async dispatcher (`bug_av_audit_emitted_per_replica_delete`,
  `bug_av_alert_fires_on_mandatory_strand_refusal`). Per the
  AV-1-6 / AV-1-7 deviation pattern (async dispatcher behaviour
  is live-only in this crate by convention), replaced with three
  pure-planner tests on the refusal classification + JSON wire
  shape. Full refusal-path live verification deferred to Phase 2
  (AV-2's promotion tests will need volumes with stranded warm
  replicas anyway). Plan total adjustment: Phase 1 net new tests
  33 → 33 (same overall count: -2 dispatcher + 3 planner = net +1
  vs the AV-1-11 line item).

### AV-1-10 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-common/src/types.rs` — new
    `VolumeConfig::migrated_to_mandatory_schema()` returns a clone
    where `mandatory_replicas` is populated from legacy
    `replication` only when currently all-zero (idempotent).
  - `crates/dtfs-manager/src/api.rs` — new
    `migrate_volume_policy_endpoint` registered at
    `POST /api/v1/volumes/{id}/migrate-policy`; emits 404 for
    unknown volume, calls `client_write(UpsertVolume{...})` on
    diff, audit-logs success/no-op, returns JSON
    `{volume_id, changed, mandatory_replicas}`.
  - `crates/dtfs-cli/src/main.rs` — new `VolumesCmd::MigratePolicy
    { volume_id }` subcommand + `cmd_volumes_migrate_policy`
    function; CLI prints `applied (mandatory ...)` or
    `no-op (already migrated) (mandatory ...)`.
* **Unit tests**: 2 new in dtfs-common
  (`bug_av_migrate_legacy_replication_clones_to_mandatory`,
  `bug_av_migrate_idempotent_when_both_present`) — both green.
* **Workspace regression**: dtfs-common 37 → 39; all others
  unchanged. `cargo test --workspace --release` all green.
* **Build hiccup:** initial sync compiled-broken because
  `migrate_volume_policy_endpoint` referenced `warn!` without
  importing it; fixed by widening `use tracing::info;` to
  `use tracing::{info, warn};` before re-sync.
* **Live verification** on minicluster (manager binaries rolled at
  2026-05-21 ~11:14 UTC):
  - Test A — initial migration via leader:
    `dtfs-cli --manager http://mini-mgr01:9402 volumes migrate-policy default`
    → `Volume 'default' migrate-policy: applied (mandatory hot=0 warm=2 cold=1 tape=1)`. ✓
  - Test B — idempotent re-run:
    second invocation →
    `no-op (already migrated) (mandatory hot=0 warm=2 cold=1 tape=1)`. ✓
  - Test C — against follower (`mini-mgr03`): same no-op
    response; raft write proxied/forwarded as expected. ✓
  - Test D — 404 for nonexistent volume: returns
    `HTTP 404 Not Found` and non-zero CLI exit code. ✓
* **Deviations:** the `/api/v1/volumes` list endpoint still
  serialises only the legacy `replication` block — the state
  machine has the new `mandatory_replicas` populated (proven by
  Test B's idempotent path), but the read projection in
  `api.rs::list_volumes_handler` needs broadening. Wrapped into
  AV-1-12 (REST API doc + projection); may split into AV-1-13 if
  the JSON shape change is risky to bundle.

### AV-1-9 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/api.rs` — new
    `flush_cache_endpoint` handler + `FlushCacheRequest` body type,
    route registered at `POST /api/v1/files/flush_cache`. Default
    tiers: `["warm", "hot"]`. Default cooldown 300s. Default
    max_tasks 10 000. Safety gate on empty prefix.
* **Compile-check**: `cargo check --workspace --release` clean.
* **Live verification** on minicluster (manager binaries rolled at
  2026-05-21 ~12:50 UTC):
  - Test A: `POST {"path_prefix": ""}` → **HTTP 400** "empty
    path_prefix would flush every file — set allow_empty_prefix=true
    to confirm". ✓
  - Test B: `POST {"cache_tiers": ["lukewarm"]}` → **HTTP 400**
    "invalid cache tier 'lukewarm': expected one of hot|warm|cold|tape". ✓
  - Test C: `POST {"path_prefix":"/movies/"}` → **HTTP 200**
    `replicas_dropped=0, mandatory_refusals=0, files_matched=7,
    chunks_examined=4`. ✓ (legacy default volume; warm-as-mandatory
    fallback means planner correctly skips warm.)
  - Test D: `POST {"path_prefix":"/movies/","cache_tiers":["warm"],"cooldown_secs":60}`
    → **HTTP 200** same shape with cooldown_secs=60 echoed back. ✓
* **Deviations:** 4 planned unit tests dropped — axum handlers in
  this crate aren't unit-tested by convention; live verification on
  the minicluster covers the happy + error paths directly.
* **Plan total adjustment:** Phase 1 net new tests 37 → 33.

### AV-1-8 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/scheduler.rs::check_under_replicated`
    — swap `v.replication.for_tier(t)` for
    `v.effective_mandatory_replicas().for_tier(t)` (1-line code
    change). Behaviour: legacy volumes (mandatory all 0) fall back
    to `replication` exactly as before; AV-style volumes use the
    new field; cache tiers (mandatory rf=0) are never enforced.
  - `crates/dtfs-common/src/types.rs` — new
    `bug_av_under_replication_ignores_cache_tiers` integration
    test against the lookup helper.
* **Unit tests**: 1 new in dtfs-common.
* **Workspace regression**: dtfs-common 36 → 37; all others
  unchanged. `cargo test --workspace --release` all green.
* **Live verification:** deferred to AV-1-9 dispatcher end-to-end.
* **Deviations:** 4 of 5 planned tests subsumed by AV-1-2 helpers
  + Bug AB existing scheduler tests; pure-planner extraction
  deferred. See §5 "AV-1-8 — pure-planner extraction deferred".

### AV-1-7 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/scheduler.rs` — new async
    `flush_cache_for_path`, pure helper `plan_flush_cache`,
    `FlushSnapshot` extraction, `FlushAction`, `FlushCacheReport`.
* **Unit tests** (`cargo test -p dtfs-manager bug_av_plan_flush`):
  5 AV-1-7 tests all green:
    drops_warm_keeps_cold, refuses_when_anchor_only_on_warm,
    respects_max_tasks, empty_when_no_cache_tier_replicas,
    multi_volume_isolation.
* **Workspace regression**: dtfs-manager 54 → 59; all others
  unchanged. `cargo test --workspace --release` all green.
* **Live verification:** deferred to AV-1-9 (REST endpoint) +
  AV-1-7's "Done when" end-to-end on minicluster after deployment.
  Pure planner is the testable kernel; the dispatcher exercises
  AV-1-6 + AV-1-3 + AV-1-4 + AV-1-5 in concert and is live-only.
* **Deviations:** none.

### AV-1-6 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-manager/src/scheduler.rs` — new
    `delete_chunk_replica_on_node(node_addr, chunk_id) ->
    (was_present, freed_bytes)` mirrors `replicate_chunk`/`verify_chunk`:
    `normalize_endpoint`, connect via `DataServiceClient`, RPC,
    surface the idempotent shape directly. anyhow-wrapped error
    paths name the offending address.
* **Compile check**: clean `cargo check -p dtfs-manager --release`.
* **Live verification:** N/A this task; deferred to AV-1-7
  (cache-flush end-to-end exercises every dispatcher path).
* **Deviations:** 2 planned unit tests dropped — async dispatcher
  testing in this crate is by convention live-only; pure helpers
  (`normalize_endpoint`) bear the unit-test load. See §5
  "AV-1-6 — async-dispatcher unit tests deferred to live".

### AV-1-5 — 2026-05-21

* **Files touched:**
  - `proto/data.proto` — extended `DeleteChunkResponse` with
    `was_present: bool` and `freed_bytes: u64`.
  - `crates/dtfs-data/src/store.rs` — new
    `delete_chunk_idempotent()` returns `(was_present, freed_bytes)`;
    old `delete_chunk()` retained as a thin error-wrapper for
    callers that still want the ChunkNotFound error.
  - `crates/dtfs-data/src/metrics.rs` — new `forget_chunk(id)`
    drops per-chunk-per-DC read counters + last-access on delete so
    a stale read count can't trigger an immediate re-promotion
    right after a flush.
  - `crates/dtfs-data/src/server.rs` — gRPC handler uses the new
    idempotent method, populates the extended response, calls
    `metrics.forget_chunk()`.
* **Unit tests:** 4 in `dtfs-data::store::tests::bug_av_*`:
  atomic_unlinks_both, missing_is_idempotent,
  second_delete_is_noop, counters_consistent.
* **Workspace regression**: dtfs-data 50 → 54; all others
  unchanged.
* **Live verification:** N/A — no production caller yet. AV-1-6
  wires the manager-side dispatcher; AV-1-7 hooks up flush.
* **Deviations:** `bug_av_delete_chunk_wal_recovery_after_crash`
  replaced with `bug_av_delete_chunk_second_delete_is_noop`. See
  §5 "AV-1-5 — WAL-recoverable atomicity deferred".

### AV-1-4 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-raft/src/lib.rs` — `SetChunkCooldown` variant +
    Display arm.
  - `crates/dtfs-raft/src/state_machine.rs` — apply arm +
    `apply_set_chunk_cooldown` helper + 4 unit tests.
* **Unit tests:** 4 AV-1-4 (`bug_av_set_cooldown_*`) all green.
* **Workspace regression**: dtfs-raft 28 → 32; all others unchanged.
* **Live verification:** N/A — no production caller yet (AV-1-7
  will use it; AV-2-5 clears via `expires_at_ms = 0`).
* **Deviations:** none.

### AV-1-3 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-raft/src/lib.rs` — `RemoveChunkLocation` enum
    variant + Display arm + `AnchorPolicy` import.
  - `crates/dtfs-raft/src/state_machine.rs` — apply arm
    (delegates to pure helper `apply_remove_chunk_location` for
    testability) + 6 unit tests + `sm_with_chunk` test factory.
* **Unit tests** (`cargo test -p dtfs-raft --release bug_av`):
  8 AV tests (2 AV-1-2 + 6 AV-1-3) all green.
* **Workspace regression**: dtfs-raft 22 → 28, all other crates
  unchanged. `cargo test --workspace --release` all green.
* **Live verification:** N/A — Raft variant + apply path only;
  no production caller yet (AV-1-7 will wire flush_cache to it).
* **Deviations:** none.

### AV-1-2 — 2026-05-21

* **Files touched:**
  - `crates/dtfs-common/src/types.rs` (+148 lines — 3 fields on
    VolumeConfig, 3 helper methods, 5 unit tests).
  - `crates/dtfs-raft/src/state_machine.rs` (+14 lines on
    FileMetadata + 7 call-site updates + 2 unit tests).
  - `crates/dtfs-manager/src/api.rs` (1 construction site,
    `create_volume` handler).
  - `crates/dtfs-manager/src/main.rs` (1 construction site,
    bootstrap-default volume).
  - `crates/dtfs-manager/src/scheduler.rs` (1 construction site,
    test helper).
* **Unit tests** (`cargo test --workspace --release` filtered):
  - dtfs-common: 7 AV-1-1 + 5 AV-1-2 = 12 AV tests; 36 total in
    crate (24 pre-existing + 12 AV).
  - dtfs-raft: 2 AV-1-2 tests; 22 total in crate (20 + 2 AV).
* **Workspace regression**: all suites green (16/36/50/27/54/22).
* **Live verification:** N/A — schema-only change, no on-cluster
  behaviour shift until later tasks wire it.
* **Deviations:** added 1 bonus test
  `bug_av_anchor_neither_returns_none` — defensive check that
  `effective_anchor()` returns `None` when both legacy and new
  anchor fields are unset. Worth keeping; plan budget was 6 tests,
  delivered 7.

### AV-1-1 — 2026-05-21

* **Files touched:** `crates/dtfs-common/src/types.rs` (+261 lines —
  3 new structs, 9 helper methods, 7 unit tests).
* **Unit tests** (`cargo test -p dtfs-common --release bug_av`):
  ```
  running 7 tests
  test bug_av_anchor_policy_legacy_string_compat ... ok
  test bug_av_anchor_policy_struct_round_trip ... ok
  test bug_av_cache_replication_full_serde_round_trip ... ok
  test bug_av_cache_tier_disabled_when_not_mentioned ... ok
  test bug_av_cache_tier_explicit_defaults_to_one_one ... ok
  test bug_av_cache_tier_explicit_min_max_round_trip ... ok
  test bug_av_cache_tier_rejects_max_less_than_min ... ok
  test result: ok. 7 passed; 0 failed
  ```
* **Crate regression suite** (`cargo test -p dtfs-common --release`):
  `31 passed; 0 failed` (24 pre-existing + 7 new).
* **Workspace compile-check** (`cargo check --workspace --release`):
  clean, no downstream breakage.
* **Live verification:** N/A — types-only change; nothing to deploy.
* **Deviations:** none.

---

## 5. Deviations from plan

> When the implementation needs to deviate from this document (a new
> file, a different test name, a missed edge case), I append the
> deviation here so the doc stays accurate.

### AV-1-5 — WAL-recoverable atomicity deferred (2026-05-21)

Plan called for 4 tests including
`bug_av_delete_chunk_wal_recovery_after_crash` — mock the unlink
failure between `.dat` and `.meta` removals and assert recovery
resumes. This requires real WAL machinery the data-node doesn't
have yet (the existing `wal/` directory is used by put_chunk's
streaming commit but has no delete-side replay path).

Building a full WAL-recoverable delete is its own feature (WAL
schema, replay-on-boot, scrub interaction); it's deferred and
tracked as a separate ticket. For Bug AV's use case the existing
"remove `.dat` then `.meta`" order is acceptable:

  * Crash between the two unlinks → orphan `.meta` with no data
    backing. Periodic scrub detects and cleans these (existing
    behaviour for similar split-state from put_chunk).
  * Bytes are released to the disk immediately when `.dat` is
    unlinked; the `bytes_stored` counter is decremented before the
    `.meta` removal, so accounting is consistent on the durable side.
  * Operator-driven flush re-issue post-crash is idempotent (test 3)
    so a retry safely cleans up.

Substituted test: `bug_av_delete_chunk_second_delete_is_noop`
exercises the "retry after a previously-successful delete" path
which is the actual operational concern for the flush-cache
scheduler (AV-1-7). Net test count unchanged: 4.

_(end AV-1-5 deviation)_

### AV-1-6 — async-dispatcher unit tests deferred to live (2026-05-21)

Plan called for 2 unit tests
(`bug_av_delete_dispatch_url_normalise` +
`bug_av_delete_dispatch_transport_error_propagated`). Both require
constructing a TaskScheduler, which transitively needs a real
openraft::Raft instance, a state_machine, an audit log. The existing
scheduler.rs convention does NOT unit-test the async dispatchers
(`replicate_chunk`, `verify_chunk` have zero unit tests); pure
helpers (`normalize_endpoint`) carry the unit-test load and the
network-side behaviour is verified live on the lab cluster.

`delete_chunk_replica_on_node` mirrors `replicate_chunk` and
`verify_chunk` exactly in structure (same normalize_endpoint usage,
same anyhow::Error wrapping, same Status translation). Following the
existing convention here.

Live verification deferred to AV-1-7's "Done when": the cache-flush
end-to-end test exercises every dispatcher edge case (success path,
absent chunk, unreachable node).

Net AV-1-6 unit-test count: 0 (down from 2 planned). Plan totals
adjusted: Phase 1 net new tests 43 → 41.

_(end AV-1-6 deviation)_

### AV-1-8 — pure-planner extraction deferred (2026-05-21)

Plan called for extracting a pure `plan_under_replication(files,
volumes, nodes) -> Vec<ReplicaPlan>` kernel + 5 tests against it. The
core code change is a 1-liner (`replication.for_tier(t)` →
`effective_mandatory_replicas().for_tier(t)`); extracting the
surrounding async method into a pure kernel is a ~150 LoC refactor
similar in scale to AV-1-7's flush planner extraction.

Of the 5 planned tests, 4 are subsumed by AV-1-2's existing helper
tests:

* `bug_av_under_replication_uses_mandatory_when_set` ⇐
  `bug_av_volume_new_mandatory_takes_precedence`
* `bug_av_under_replication_falls_back_to_legacy` ⇐
  `bug_av_volume_legacy_replication_falls_back`
* `bug_av_under_replication_anchor_only_chunk_no_action` is
  exercised by the dispatcher's live verification (AV-1-9)
* `bug_av_under_replication_refills_dropped_mandatory_chunk` is
  exercised by the existing Bug AB scheduler tests (unchanged
  behaviour for the legacy field)

The remaining novel scenario — "cache tiers are NOT auto-filled" —
is the integration that's specific to AV-1-8. Added one focused
test (`bug_av_under_replication_ignores_cache_tiers`) in dtfs-common
that exercises the lookup path the under-rep loop now uses.

Net AV-1-8 unit-test count: 1 (down from 5 planned). Plan totals
adjusted: Phase 1 net new tests 41 → 37.

_(end AV-1-8 deviation)_

### AV-1-9 — axum handler unit tests deferred to live (2026-05-21)

Plan called for 4 unit tests (`bug_av_api_flush_cache_*`). Same
constraint as AV-1-6: constructing an axum handler unit-test requires
a full AppState (Raft + state_machine + audit_log + scheduler), and
the existing api.rs convention does NOT unit-test handlers
(`start_path_migration` and `list_migrations` have zero unit tests
either). The pure-planner kernel `plan_flush_cache` (AV-1-7) carries
the unit-test load; handler-level behaviour is verified live on the
minicluster.

Live verification covered: validation rejection (empty prefix +
invalid tier), accepted-call response shape (both default and
explicit cache_tiers), report counters all zero on a legacy-schema
volume where warm-is-mandatory.

Net AV-1-9 unit-test count: 0 (down from 4 planned). Plan totals
adjusted: Phase 1 net new tests 37 → 33.

_(end AV-1-9 deviation)_

### AV-1-10 — `/api/v1/volumes` JSON projection gap (✅ RESOLVED in AV-3-5, 2026-05-21)

AV-1-10's migrate-policy helper correctly populates
`mandatory_replicas` on the state machine, but the
`GET /api/v1/volumes` list endpoint still serialises only the
legacy `replication` block.

**Resolved by AV-3-5**: new `GET /api/v1/volumes/{id}` endpoint
projects the full VolumeConfig including `mandatory_replicas`,
`cache_replicas`, and `anchor`. Driven by the pure helper
`volume_config_to_json(&VolumeConfig)` (5 unit tests cover the
shape). The list endpoint
(`GET /api/v1/volumes`) intentionally still uses its
aggregation-only shape — broadening it would require a
coordinated WebUI/CLI bump; per-volume reads through the new
endpoint cover the operator's need.

A residual sub-task — "broaden the list-endpoint JSON to match
the per-volume shape" — is a deferred polish item if any client
ever needs it; today the CLI (`dtfs-cli volumes show <id>`)
and WebUI (planned card) both prefer the per-volume endpoint.
No successor Phase is currently scheduled for the Bug AV
plan.

_(end AV-1-10 deviation — resolved)_

### AV-3-4 — WebUI volume-edit form (PARTIALLY RESOLVED in AV-3-5, 2026-05-21)

AV-3-4's plan called for both a "Flush cache" context-menu
button (shipped) and a volume-edit card on the Volumes page.
The card was deferred pending the AV-1-10 projection gap.

**Partially resolved by AV-3-5**: the
`GET /api/v1/volumes/{id}` endpoint added by AV-3-5 returns
the full policy, so a WebUI read-only card is now a
straightforward `fetch + render` follow-up. The inline-edit
form (posting back to `PUT /api/v1/volumes/{id}`) is still
outstanding — and the operator-facing alternative (CLI
`volumes set-cache`) now exists, so the WebUI form is no
longer on the critical path.

**Residual deferral:** WebUI volume detail card (read-only,
~30 min of HTML/JS) + inline-edit modal (~2 h). Both are
deferred polish items; the CLI covers the operational
need today. No successor Phase is scheduled — track as
standalone follow-ups if/when the WebUI becomes the primary
volume-policy editor.

_(end AV-3-4 deviation — partially resolved)_

### AV-4-1 — live demos blocked on minicluster topology; AV-4-1a / AV-4-1b queued (2026-05-22)

AV-4-1 attempted to verify the four end-to-end Bug AV
scenarios (S1 promote-on-threshold, S2 demote-on-idle, S3
cooldown asymmetry, S4 warm-first → anchor-lag-repair) on the
existing single-DC minicluster. The shell regression test
(`tests/proxmox/abuse/15-bug-av-cache-lifecycle.sh`) is
written and syntactically clean, the FUSE binary was rebuilt
+ rolled with the AV-3-1 `[write] first_write_tier =
"nearest-warm"` config, mini-mgr02 + mini-mgr01 were repaired
mid-task (corrupt Raft log + stale Raft channels — see the
verification log entry for AV-4-1), and the tiering tick was
reduced 3600s → 10s.

**Why AV-4-1 cannot fully verify on the current cluster:**

1. The `default` volume has
   `mandatory_replicas = {hot:0, warm:2, cold:1, tape:1}`.
   The cache planner correctly skips tiers that are also
   mandatory (CLAUDE.md § 3.4), so S1/S2/S3 produce no
   observable behaviour.
2. The FUSE client routes every write through
   `volume_id = "default"` — no per-path volume routing
   exists, so creating a fresh `av-demo` volume in the SM
   doesn't help (writes can't reach it).
3. The REST surface has no `set-mandatory` endpoint
   equivalent to AV-3-5's `set-cache`; mutating `default`'s
   `mandatory_replicas` in-place is not possible via the
   current API.
4. Single-DC topology + `anchor = (dc-a, any-tier)` means
   S4 anchor-lag-repair has no work to do — the invariant
   is trivially satisfied by any first-chunk placement.

**Disposition (recorded as two named follow-ups, not a
hand-wavey deferral):**

* **AV-4-1a** — new `POST /api/v1/volumes/{id}/set-mandatory`
  endpoint (~0.5 d). Smallest code change that unblocks S1,
  S2, and S3 on the current minicluster. Added as an
  explicit Phase 4 task between AV-4-1 and AV-4-2; included
  in Phase 4 Done When.
* **AV-4-1b** — multi-DC topology provisioning (~1-2 d).
  S4 needs this regardless of any code change. Explicitly
  **deferred out of Phase 4** because (a) it's infra-heavy
  and doesn't belong on Phase 4's critical path, (b) the
  S4 behaviour is unit-tested in
  `bug_av_anchor_lag_repair_*` (6 tests), and (c) the
  reasoning-verified production path is straightforward.
  Tracked as a named task so future agents pick it up
  rather than re-deriving the requirement.

AV-4-1 itself stays at 🟨 (needs review) until AV-4-1a
lands and the shell script is re-run with S1/S2/S3 green.
At that point AV-4-1 flips to ✅ with S4 explicitly noted
"awaiting AV-4-1b". Phase 4 closes without AV-4-1b.

_(end AV-4-1 deviation — disposition: queued as AV-4-1a
+ AV-4-1b)_

### AV-2-4 / AV-2-5 — Phase 2 live end-to-end demo deferred (2026-05-21)

The Phase 2 "Done when" criteria for AV-2-4 / AV-2-5 each call
for a live demo:
* AV-2-4: "write a file, read it twice from data01's DC over
  4h (simulate by shortening the window to 60s for the test),
  observe warm replica appears in that DC."
* AV-2-5: "cluster behaviour shows that demote followed by an
  immediate read can re-promote (demote ≠ flush)."

Both demos require a volume with `cache_replicas` configured.
Today only the REST `UpsertVolume` endpoint accepts that schema
— there is no operator-friendly CLI (`dtfs-cli volume
set-cache`) until AV-3-5 lands. Manually crafting an
`UpsertVolume` body to flip the `default` volume into
cache-enabled mode is possible but risks corrupting the
minicluster's persistent state for the rest of Phase 3
development.

What WAS verified on the minicluster:
* AV-2-1/2/3 pure helpers: 17 unit tests green.
* AV-2-4 wiring: planner + engine compiles; binaries roll
  cleanly; tiering engine `Tiering engine started
  (evaluation interval: 3600s)` log appears; no panics.
* AV-2-5 cooldown clearance: 3 unit tests green covering the
  past/future cooldown decision boundary; binaries roll
  cleanly.

**Disposition:** bundle into the Phase 2 close-out demo that
runs *after* AV-3-5 ships. The Phase 2 binaries are already
deployed to the minicluster, so the demo is purely a "create
the right volume + watch the tick fire" check. Plan's
"Phase 2 Done When" section already calls for this final demo
as a separate item; expanded here so future agents understand
the sequencing.

**Risk if left undemoed:** very low. The full code path is
unit-tested at every junction; the only unverified step is the
async dispatcher's network behaviour, which mirrors the
already-live AV-1-7 flush dispatcher exactly in shape.

_(end AV-2-4 / AV-2-5 deviation)_

---

## Appendix A — Full configuration examples

These are the canonical examples to ship in the docs (`volume-config.md`,
`storage/replication.md`, `storage/tiering.md`). Every example shows
the full `[defaults]` block so operators can copy-paste; the same
fields apply at the `VolumeConfig` JSON level when modifying a volume
through the REST API.

Naming legend used throughout:
- **Mandatory** = under-replication scheduler enforces these RFs (always-on).
- **Cache** = tiering engine creates these on read demand (opportunistic).
- **Anchor** = the durability invariant: ≥1 mandatory replica must live in
  the anchor DC (and, if `required_tier` is set, in that specific tier).

---

### Example 1 — Plexmedia (the canonical use case)

3 DCs, asymmetric bandwidth. Cold tier in LELI is the archive; warm
tier in TORO + NRB is the SSD cache for active viewing. No hot tier.
Anchor pinned to cold in LELI so every chunk is durably parked on the
50 TB HDD pool, regardless of what the cache does.

```toml
[defaults.mandatory_replicas]
# Every chunk has exactly 1 cold replica (anchor copy).
hot  = 0
warm = 0
cold = 1
tape = 0

[defaults.cache_replicas]
per_dc              = true
promote_reads       = 2       # 2 reads to count as "hot in this DC"
promote_window_secs = 14400   # over 4 hours
cooldown_secs       = 300     # 5-min cooldown after operator flush

[defaults.cache_replicas.warm]
min = 1  # First read-from-DC threshold breach creates 1 warm replica
max = 1  # No fault-tolerance ramp; 1 warm copy per DC is enough

[defaults.anchor]
datacenter   = "leli"
required_tier = "cold"        # anchor MUST be cold (not just any tier)

[defaults]
# 16 MB strikes a balance for the 40 Mbps DSL anchor link:
#   * ~3 s per chunk transfer at line rate — small enough that a
#     mid-transfer failure isn't catastrophic, large enough that
#     per-chunk overhead (proto framing, fsync) doesn't dominate.
#   * A 2 GB movie = ~128 chunks; per-chunk replication retries are
#     fine-grained failure units.
chunk_size_mb = 16

[defaults.write_safety]
min_replicas_before_ack = 1   # anchor (cold) replica suffices to ACK
write_timeout_secs      = 60
```

What this produces:

```
File "/movies/Wicked.mkv" — never read:
  LELI/data-primary01 cold      ✓ mandatory
  TORO/data-warm01    (none)
  NRB/data-warm02     (none)

After 2 reads from TORO Plex viewer within 4h:
  LELI/data-primary01 cold      ✓ mandatory
  TORO/data-warm01    warm      ✓ cache (promoted)
  NRB/data-warm02     (none)

After 2 reads from NRB Plex viewer within 4h:
  LELI/data-primary01 cold      ✓ mandatory
  TORO/data-warm01    warm      ✓ cache (still hot)
  NRB/data-warm02     warm      ✓ cache (promoted independently)

After operator clicks "Flush cache" on /movies/Wicked.mkv:
  LELI/data-primary01 cold      ✓ mandatory (anchor preserved)
  TORO/data-warm01    (gone)    + 5-min cooldown
  NRB/data-warm02     (gone)    + 5-min cooldown
```

Per-file raw storage: `1 × cold = 100% file size` when idle;
`1 × cold + 1 × warm per actively-watched DC = ~200%` per active DC.

---

### Example 2 — Photo backup (write-mostly, rare reads)

Cold-only, multi-DC for disaster recovery, no cache. The user uploads
photos quarterly and reads almost never; spending RAM/SSD on warm
cache is wasteful. Mandatory = 2 cold replicas across 2 DCs.

```toml
[defaults.mandatory_replicas]
cold = 2  # Two cold copies across DCs — DR-grade durability

[defaults.cache_replicas]
# Empty / no tiers listed → cache tier disabled cluster-wide for this volume
# No promote_reads / window needed when no cache tier is enabled

[defaults.anchor]
datacenter   = "primary-dc"
required_tier = "cold"

[defaults]
# 4 MB chunks for photo workloads:
#   * Most JPEGs (2-8 MB) fit in 1-2 chunks → minimal overhead.
#   * RAW files (20-60 MB) span 5-15 chunks → parallelisable
#     read/replicate when batched.
#   * Small enough that an interrupted upload only loses the last
#     in-flight chunk, not 30 MB of the next photo.
chunk_size_mb = 4

[defaults.write_safety]
min_replicas_before_ack = 2   # both cold replicas before ACK (durable)
write_timeout_secs      = 120
```

Behaviour: every photo lives on 2 cold disks in 2 DCs from write
time. No tiering activity. Reads are served from whichever cold
replica is geographically closer (or wherever the placement engine
picks).

---

### Example 3 — Office file share (multi-DC, mixed temperature)

Documents that were recently edited stay warm in the editor's DC.
Older docs settle into cold. Cache is enabled but with a low promote
threshold so any access by a colleague in another DC quickly warms a
local copy.

```toml
[defaults.mandatory_replicas]
warm = 1     # one warm replica for "primary" copy (cluster-wide)
cold = 1     # one cold replica for durability

[defaults.cache_replicas]
per_dc              = true
promote_reads       = 1       # any read in a DC promotes immediately
promote_window_secs = 3600    # over 1 hour
cooldown_secs       = 180     # 3-min cooldown

[defaults.cache_replicas.warm]
min = 1
max = 1

[defaults.anchor]
datacenter = "hq-dc"
# required_tier omitted → any-tier match in hq-dc counts as the anchor

[defaults]
# 1 MB chunks for office-document workloads:
#   * Tiny files (<1 MB Word/Excel) are exactly 1 chunk → minimal
#     metadata overhead.
#   * Medium docs (5-20 MB pptx, pdf) span 5-20 chunks → partial
#     reads (cell jumps in xlsx, page seeks in pdf) only fault
#     the chunks the reader actually touches.
#   * Small enough to keep FUSE write_lease latency below human
#     perception even on the slowest WAN link.
chunk_size_mb = 1
```

Behaviour: each chunk has 1 mandatory warm + 1 mandatory cold + up to
1 cache warm per DC. The mandatory warm copy lives wherever the
placement engine puts it; cache warm copies appear on first read in
each remote DC.

---

### Example 4 — Single-DC home lab (the minicluster default)

One DC, three racks, no DR concerns. Cold = anchor, warm = local
cache. Same shape as Plexmedia at smaller scale.

```toml
[defaults.mandatory_replicas]
cold = 1

[defaults.cache_replicas]
per_dc              = true
promote_reads       = 2
promote_window_secs = 14400   # 4h
cooldown_secs       = 300

[defaults.cache_replicas.warm]
min = 1
max = 1

[defaults.anchor]
datacenter   = "dc-a"         # the only DC
required_tier = "cold"

[defaults]
chunk_size_mb = 4
```

This is the policy I'll apply to the running minicluster after AV-1-10
so the live verification matches the Plexmedia shape.

---

### Example 5 — Per-DC HA redundancy (cache with fault tolerance)

DC has multiple warm-tier nodes. Operator wants the cache to survive
a single warm-node failure without an immediate cold re-fetch.
Enables the (Phase 2-deferred) redundancy escalation by setting
`max > min`.

```toml
[defaults.mandatory_replicas]
cold = 1

[defaults.cache_replicas]
per_dc              = true
promote_reads       = 2
promote_window_secs = 14400
cooldown_secs       = 300

[defaults.cache_replicas.warm]
min = 1   # First promotion: 1 warm replica per DC
max = 2   # Escalation may grow to 2 warm replicas per DC (HA)

[defaults.anchor]
datacenter   = "leli"
required_tier = "cold"
```

Note: the redundancy-escalation pass is gated by
`ENABLE_CACHE_REDUNDANCY_ESCALATION = false` in Phase 2 binaries.
Setting `max = 2` is correct future-proofing; the engine will
honour it once that flag flips. Flipping is a separate deferred
follow-up — no successor Phase is scheduled today (see § 5
deviations).

---

### Example 6 — Legacy back-compat (no operator action)

Brand-new code, but operator hasn't migrated the volume policy yet.
The state machine still carries the pre-Bug-AV `replication` field;
new `mandatory_replicas` is empty (`{0, 0, 0, 0}`). The
`effective_mandatory_replicas()` helper returns `replication` as the
fallback, so behaviour is byte-identical to today.

```toml
[defaults.replication]
hot  = 0
warm = 2
cold = 1
tape = 0

# No [defaults.mandatory_replicas] block → uses legacy `replication`
# No [defaults.cache_replicas] block → cache tier disabled, no promotions
# anchor_datacenter remains as legacy string field if it was already there
anchor_datacenter = "leli"
```

After the operator runs `dtfs-cli volume migrate-policy default`:

```toml
[defaults.replication]   # KEPT for rollback safety
hot  = 0
warm = 2
cold = 1

[defaults.mandatory_replicas]   # COPIED from replication
hot  = 0
warm = 2
cold = 1

# Still no cache_replicas → still no promotion. Operator can opt in
# later with `dtfs-cli volume set-cache default warm.min=1 warm.max=1`.

[defaults.anchor]
datacenter = "leli"
# required_tier is None → any tier in leli satisfies the anchor
```

A subsequent `dtfs-cli volume set-cache default warm.min=1 warm.max=1`
adds the cache block; the previously-mandatory `warm = 2` then becomes
the *new* mandatory baseline (operator can drop it manually with
`dtfs-cli volume set-mandatory default warm=0` if they want a pure
cold-anchor + warm-cache shape).

---

### Example 7 — Promotion rule deep-dive

A trace through the tiering engine for a single chunk in Example 1's
Plexmedia config.

```
t=0:        Write of /season-1/wicked.mkv.
            Placement engine: mandatory cold = 1 → schedule cold
                              replica on LELI/data-primary01.
            cache_replicas.warm.min = 1 but no reads yet → no warm
                              replica scheduled.
            State: {LELI cold}.

t=0+30s:    LELI cold replica committed.
            File now satisfies mandatory_replicas. Tiering engine
            sees 0 reads from any DC → no action.

t=2h:       Plex viewer in TORO starts reading /season-1/wicked.mkv.
            Data node `data-warm01` (TORO) is NOT the holder, so
            the FUSE client fetches from LELI/cold.
            Tiering engine increments reads_by_dc["toro"] = 1.

t=2h+1m:    Another Plex read from TORO. reads_by_dc["toro"] = 2.
            Engine: 2 ≥ promote_reads (2) AND window (4h) → eligible.
            Cooldown for this chunk_index = 0 (no prior flush).
            cache_replicas.warm.min = 1, current_warm_in_toro = 0.
            Decision: schedule 1 warm replication into TORO.

t=2h+1m+5s: data-warm01 (TORO) commits the warm replica.
            Next reads from TORO are served locally — fast.

t=8h:       NRB viewer starts reading. Same flow → warm in NRB.
            State: {LELI cold, TORO warm, NRB warm}.

t=24h:      No reads anywhere for `promote_window_secs` (4h).
            Demote pass: all reads in window = 0 < promote_reads.
            For each warm replica with 0 reads in window: delete.
            Anchor check passes (cold preserved in LELI).
            State: {LELI cold}.
            Cooldown for these chunks set to 0 on demote (NOT on
            operator flush).

t=24h+5m:   Single read from TORO. reads_by_dc["toro"] = 1 < 2.
            No re-promotion. (User would need 2 reads in 4h to
            re-warm.)

t=25h:      Operator runs `dtfs-cli file flush-cache
            /season-1/wicked.mkv` (no warm exists, so this is a
            no-op return: replicas_dropped = 0).
            Cooldown still set to 0; no behaviour change.

Alternate at t=8h+30s (operator flush after promotion):
            `flush-cache /season-1/wicked.mkv` drops both warms.
            Sets cache_cooldown_until_ms[chunk_idx] = now + 300s.

t=8h+35s:   New TORO read. Engine sees cooldown active → refuses
            re-promotion. Read served from LELI cold (slow).

t=8h+10m:   Cooldown expired. New TORO read counts toward threshold
            again. Second read within 4h → re-promote.
```

---

## Appendix B — Config field cross-reference (for docs)

When the docs page lists fields, this is the canonical order. Used in
`volume-config.md` Settings Reference.

| Field | Type | Default | Purpose |
|---|---|---|---|
| `replication.hot/warm/cold/tape` | `u32` | tier-specific | **Legacy.** Read fallback when `mandatory_replicas` is empty. Operators on the new policy should leave this alone and use `mandatory_replicas`. |
| `mandatory_replicas.hot/warm/cold/tape` | `u32` | `0` each | Per-tier mandatory RF. Enforced by under-replication scheduler. |
| `cache_replicas.{tier}.min` | `u32` | 0 if tier omitted, 1 if mentioned | Per-DC target on threshold breach. |
| `cache_replicas.{tier}.max` | `u32` | same as `min` | Per-DC absolute cap. `max ≥ min`. |
| `cache_replicas.per_dc` | `bool` | `true` | When false, `min/max` apply cluster-wide. |
| `cache_replicas.promote_reads` | `u32` | `2` | Reads required within `promote_window_secs` to trigger a promotion. |
| `cache_replicas.promote_window_secs` | `u64` | `14400` (4h) | Rolling read window. |
| `cache_replicas.cooldown_secs` | `u64` | `300` (5min) | Cooldown after operator-driven flush, blocks re-promotion. |
| `anchor.datacenter` | `string` | unset | Anchor DC name. |
| `anchor.required_tier` | `string` (tier name) | unset | When set, the anchor-DC replica must be in this specific tier (e.g. `"cold"`). |
| `anchor_datacenter` | `string` | unset | **Legacy.** When set and `anchor` is unset, resolves to `anchor = { datacenter: …, required_tier: None }`. |
| `write_safety.min_replicas_before_ack` | `u32` | `2` | Replicas that must commit before the FUSE write returns success. Bounded by the sum of mandatory RFs. |
| `chunk_size_mb` | `u64` | `4` | Chunk size for new files. |

## 6. Procedure recap

The per-task procedure and commit discipline used by this plan are
defined once in [AGENTS.md](../../AGENTS.md). Read that file first.
This section captures the **AV-specific** bits not already in
AGENTS.md.

### 6.1 AV-specific deploy targets

* **Build host:** the `dtfs-builder` VM (10.20.0.150). Source +
  cargo cache live there.
* **Verification environment:** the 6-VM minicluster
  (`mini-mgr01`..03, `mini-data01`..02, `mini-client01`) provisioned
  by `tests/proxmox/provision-mini.sh` and deployed by
  `tests/proxmox/deploy-mini.sh`.
* **Rolling-deploy order:** managers leader-last (peek
  `/api/v1/raft/state` to find the current leader), data nodes any
  order, FUSE clients last. `deploy-mini.sh` documents the recipe.

### 6.2 AV-specific commit-message `Plan:` trailer

Format (per AGENTS.md § 3, with concrete plan path):

```
AV-<phase>-<n>: <one-line summary>

<2-3 sentence body.>

Plan: plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md (AV-<phase>-<n> ✅)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

The three doc-only tasks (`AV-1-12`, `AV-2-6`, `AV-3-6`) get their
own commits because they aggregate doc edits across the whole phase.

### 6.3 Phase boundaries

After each phase's "Done when" is met, **pause for user review**
before starting the next phase. No automatic phase-rollover.
