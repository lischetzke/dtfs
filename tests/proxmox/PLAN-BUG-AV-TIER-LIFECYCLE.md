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

Three phases. Each task has a stable ID (`AV-<phase>-<n>`) so I can
reference it from commits and the recovery doc. Each task lists:

* **Files** changed
* **Implementation** sketch
* **Tests** required (unit names + live-verification check)
* **Done when** — concrete acceptance criteria
* **Status** marker (✅ when complete + dated)

The plan is updated **in place** as I work: status flips, deviations
from the original sketch get appended under each task as
`Implementation notes`.

---

## Phase 1 — Schema + flush API (~4.5 d)

Delivers the data model, the delete-replica plumbing, and an API the
operator can call (via curl or CLI) to flush cache tiers. Tiering
engine improvements wait for Phase 2; cache replicas are only created
manually for now (or by the existing additive-migrate UI button).

### ⬜ AV-1-1 — `CacheReplication` + `AnchorPolicy` types

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

### ⬜ AV-1-2 — `VolumeConfig` + `FileMetadata` extension

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

### ⬜ AV-1-3 — `RemoveChunkLocation` Raft command

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

### ⬜ AV-1-4 — `SetChunkCooldown` Raft command

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

### ⬜ AV-1-5 — `DeleteChunkReplica` gRPC + data-node handler

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

### ⬜ AV-1-6 — Manager → data-node delete dispatch

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

### ⬜ AV-1-7 — `flush_cache_for_path` scheduler routine

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

### ⬜ AV-1-8 — Under-replication scheduler honours `mandatory_replicas`

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

### ⬜ AV-1-9 — `POST /api/v1/files/flush_cache` REST endpoint

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

### ⬜ AV-1-10 — Boot-time policy migration helper

* **Files:** `crates/dtfs-manager/src/main.rs` (one-shot helper),
  `crates/dtfs-cli/src/main.rs` (`volume migrate-policy <vol>`).
* **Implementation:** CLI command reads current volume's
  `replication` (legacy), writes a new `mandatory_replicas` set to
  the same values, leaves `replication` untouched (read fallback
  remains). For new operators wanting the cache-only pattern, they
  follow up with `dtfs-cli volume set-cache <vol> warm=1 per-dc=true`.
* **Tests:** unit on the legacy→new helper:
  `bug_av_migrate_legacy_replication_clones_to_mandatory`,
  `bug_av_migrate_idempotent_when_both_present`.
* **Done when:** 2 tests green + minicluster's `default` volume
  successfully migrated.

### ⬜ AV-1-11 — Audit + alert hooks

* **Files:** `crates/dtfs-manager/src/scheduler.rs`,
  `crates/dtfs-manager/src/alerts.rs`.
* **Implementation:**
  - On every replica deletion via flush: audit
    `"cache.flush_replica chunk=X node=Y reason=operator"`.
  - On mandatory-tier strand refusal: audit + alert
    `AlertKind::AnchorViolated`-equivalent (or new
    `CacheFlushRefused`).
* **Tests:** `bug_av_audit_emitted_per_replica_delete`,
  `bug_av_alert_fires_on_mandatory_strand_refusal`.
* **Done when:** 2 tests + live verification (`grep cache.flush`
  audit log after a real flush returns N matching entries where N =
  replicas_dropped).

### ⬜ AV-1-12 — Documentation: schema + flush API

* **Files:**
  - `docs/src/configuration/volume-config.md` — extend the settings
    reference table with the new fields; add a "Replication classes"
    subsection explaining mandatory vs cache and the back-compat
    rules. Include 2 of the 5 examples from this plan's Appendix A
    inline.
  - `docs/src/storage/replication.md` — add the same "mandatory vs
    cache" framing at the top; cross-link to tiering.md for the
    promotion engine.
  - `docs/src/storage/tiering.md` — add a "Cache tier semantics
    (Bug AV)" section *placeholder*; full text lands with AV-2-6.
  - `docs/src/api-reference/rest-api.md` — document the new
    `POST /api/v1/files/flush_cache` endpoint with request/response
    schema + one example curl invocation.
  - `docs/src/api-reference/cli-reference.md` — placeholder for the
    CLI commands (filled in by AV-3-5).
  - `CLAUDE.md` — § 3 (Conventions): add invariant "cache replicas
    are NEVER auto-created by the under-replication scheduler — only
    the tiering engine creates them, and only on threshold." § 10
    (file map): note the new `flush_cache_for_path` entry point in
    scheduler.rs.
  - `README.md` — one-paragraph mention in the feature list of the
    new "mandatory anchor + opportunistic cache" model with a link
    to the volume-config docs.
* **Tests:** doc smoke test — `mdbook build` from `docs/` succeeds
  (no broken cross-links).
* **Done when:** all 5 doc files updated, mdbook builds clean.

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

### ⬜ AV-2-1 — Per-DC rolling read-window store

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** existing `report_access` already takes
  `reads_by_dc: HashMap<String, u32>`. Today's engine sums across DCs
  and uses fixed thresholds. New: a per-`(chunk_id, dc)` ring-buffer
  of timestamps trimmed to `promote_window_secs`. Pure-function
  helper `count_reads_in_window(samples, now_ms, window_secs) ->
  u32`.
* **Tests:**
  - `bug_av_read_window_counts_inside_window`
  - `bug_av_read_window_drops_outside`
  - `bug_av_read_window_handles_empty`
  - `bug_av_read_window_handles_unsorted_input`
* **Done when:** 4 tests green.

### ⬜ AV-2-2 — Promotion decision (pure)

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** pure function `plan_cache_promotion(
  read_count_in_window, tier_cfg: &CacheTierConfig,
  current_cache_replicas_in_dc: u32, cooldown_until_ms: u64,
  promote_reads_threshold: u32, now_ms: u64) -> u32`.
  Returns the **number of new replicas to schedule** in this DC for
  this tier. Logic:
  - If `tier_cfg.max == 0` → cache tier disabled, return 0.
  - If `now_ms < cooldown_until_ms` → return 0.
  - If `read_count_in_window < promote_reads_threshold` → return 0.
  - Otherwise: `min(tier_cfg.min, tier_cfg.max).saturating_sub(
    current_cache_replicas_in_dc)`. (Caps at `min` for the basic
    promotion path; the optional redundancy escalation pass below
    can ramp further toward `max`.)
* **Tests:**
  - `bug_av_promote_first_read_below_threshold_does_nothing`
  - `bug_av_promote_threshold_met_returns_min_target_minus_current`
  - `bug_av_promote_already_at_min_returns_zero`
  - `bug_av_promote_max_zero_disables_promotion`
  - `bug_av_promote_cooldown_blocks_until_expiry`
  - `bug_av_promote_min_two_creates_two_at_once`
* **Done when:** 6 tests green.

### ⬜ AV-2-3 — Promotion target selection

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** pick a warm-tier node in the requesting DC with
  the most free space, excluding `Draining` nodes and nodes that
  already hold this chunk. Returns `None` if no candidate.
* **Tests:**
  - `bug_av_target_selects_most_free_in_dc`
  - `bug_av_target_excludes_draining_nodes`
  - `bug_av_target_excludes_existing_replica_holders`
  - `bug_av_target_returns_none_when_no_dc_warm_node`
* **Done when:** 4 tests green.

### ⬜ AV-2-4 — `tiering::tick` wires AV-2-1..3 + schedules replication

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** replace the old hot/cold-threshold logic with
  the new cache-promotion + cache-demotion. Demotion = "cache
  replica with no reads in `promote_window_secs` is eligible for
  flush". Demotion runs at the same low priority as today's
  scheduler; doesn't fight the cooldown.

  **Redundancy escalation** (when `max > min`) is implemented behind
  a constant `ENABLE_CACHE_REDUNDANCY_ESCALATION: bool = false` in
  Phase 2 — the code is there and unit-tested but turned off until
  we have a real per-DC HA use case. Flip to true in a follow-up
  with the matching test extension.
* **Tests:** *engine-level integration with a mock SM*:
  - `bug_av_tiering_tick_promotes_on_threshold`
  - `bug_av_tiering_tick_demotes_idle_cache_replica`
  - `bug_av_tiering_tick_respects_cooldown`
  - `bug_av_tiering_tick_respects_per_dc_cap`
  - `bug_av_tiering_tick_redundancy_escalation_off_by_default`
* **Done when:** 5 tests green + live demo on minicluster: write a
  file, read it twice from data01's DC over 4h (simulate by
  shortening the window to 60s for the test), observe warm replica
  appears in that DC.

### ⬜ AV-2-5 — Cooldown clearance on demote

* **Files:** `crates/dtfs-manager/src/tiering.rs`.
* **Implementation:** when the engine demotes a chunk (cache replica
  removed because idle), also clear that chunk's
  `cache_cooldown_until_ms[idx]` via `SetChunkCooldown { ...,
  expires_at_ms: 0 }`. Pre-existing operator flushes' cooldowns
  remain because they were set with future timestamps.
* **Tests:** `bug_av_demote_clears_cooldown`,
  `bug_av_demote_does_not_clear_future_operator_cooldown`.
* **Done when:** 2 tests + cluster behaviour shows that demote
  followed by an immediate read can re-promote (demote ≠ flush).

### ⬜ AV-2-6 — Documentation: tiering engine + promotion rules

* **Files:**
  - `docs/src/storage/tiering.md` — replace the placeholder from
    AV-1-12 with the full Bug AV write-up:
    * Mandatory vs cache invariants.
    * Promotion math (`promote_reads`, `promote_window_secs`, per-DC
      counting, `min`/`max` semantics).
    * Cooldown semantics (5-min default, why operator flushes set it,
      why idle demote clears it).
    * Anchor-aware demotion (the engine never strands the anchor).
    * Worked example: a chunk's life cycle across promotion, idle
      demote, operator flush.
  - `docs/src/storage/caching.md` — cross-reference cache_replicas
    here; note this is the volume-policy view of caching (the
    page also covers data-node ARC cache + FUSE local cache, which
    are different things).
  - `docs/src/operations/cluster-management.md` — operator runbook
    for "I want to free warm space across all of /movies/Season-1/":
    explain the `dtfs-cli file flush-cache` and webui equivalent,
    note the audit log entries to expect, note that cooldown protects
    against accidental immediate re-promotion.
  - `CLAUDE.md` — § 3 (Conventions): add invariant "every cache
    promotion / demotion must check `would_violate_anchor` against
    the destination state; never strand the anchor".
* **Tests:** doc smoke test (`mdbook build`); manual eyeball pass.
* **Done when:** files updated; mdbook builds clean; the runbook
  exactly matches the on-cluster behaviour from the Phase 2 live
  verification.

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

### ⬜ AV-3-1 — `fuse.toml [client.write]` block

* **Files:** `crates/dtfs-common/src/config.rs::FuseConfig`,
  `tests/proxmox/ansible/templates/fuse.toml.j2`.
* **Implementation:**
  ```rust
  pub struct FuseWriteConfig {
      #[serde(default = "default_first_write_tier")]
      pub first_write_tier: String, // "cold" | "nearest-warm"
  }
  fn default_first_write_tier() -> String { "cold".into() }
  ```
* **Tests:** `bug_av_fuse_write_config_default`,
  `bug_av_fuse_write_config_nearest_warm_parsed`,
  `bug_av_fuse_write_config_invalid_value_falls_back_to_cold`.
* **Done when:** 3 tests + a minicluster client with
  `first_write_tier="nearest-warm"` boots cleanly.

### ⬜ AV-3-2 — `preferred_tier` hint on `allocate_chunks`

* **Files:** `proto/manager.proto`,
  `crates/dtfs-manager/src/metadata.rs` (`allocate_chunks` handler),
  `crates/dtfs-manager/src/placement.rs` (PlacementEngine),
  `crates/dtfs-fuse/src/filesystem.rs` (write path).
* **Implementation:** new proto field `string preferred_tier = N`
  (`#[serde(default)]` empty = "no preference, use volume default").
  Placement engine honours when set, ignored otherwise.
* **Tests:**
  - `bug_av_allocate_chunks_honours_preferred_tier`
  - `bug_av_allocate_chunks_falls_back_when_tier_full`
  - `bug_av_allocate_chunks_empty_hint_uses_default`
* **Done when:** 3 tests + live verification: FUSE with
  `nearest-warm` writes a 4 MiB file, observe chunk lands on a warm
  disk first (data01-warm or data02-warm), then anchor migration
  schedules a cold copy within 30 s.

### ⬜ AV-3-3 — Auto-anchor-migration on warm-first writes

* **Files:** `crates/dtfs-manager/src/scheduler.rs`.
* **Implementation:** new pass in `check_under_replicated` (or
  separate `check_anchor_lag` running at the same cadence): for
  every chunk with no replica in the anchor's required tier, enqueue
  a normal `ReplicationTask` targeting a cold node in the anchor DC.
  Audit: `"anchor.lag_repair chunk=X target=Y"`.
* **Tests:**
  - `bug_av_anchor_lag_repair_picks_anchor_dc_node`
  - `bug_av_anchor_lag_repair_picks_required_tier`
  - `bug_av_anchor_lag_repair_no_op_when_satisfied`
* **Done when:** 3 tests + live: a warm-first write reaches anchor
  tier within ~scheduler tick + replication time (~30-60 s).

### ⬜ AV-3-4 — WebUI "Flush cache" action + volume-edit form

* **Files:** `crates/dtfs-manager/src/webui.rs`.
* **Implementation:**
  - Add "Flush cache" to the right-click context menu on Data
    Explorer (folder and file actions).
  - New modal with "Cooldown override (secs)" optional override.
  - Volume page: add a card showing
    `mandatory_replicas` / `cache_replicas` / `anchor` + "Edit"
    button → form posts to `PUT /api/v1/volumes/{id}`.
* **Tests:** live (no JS unit framework in repo).
* **Done when:** Right-clicking `/movies/Season-1/` and choosing
  "Flush cache" runs the API + reports back; volume page renders
  the new fields; edit form round-trips.

### ⬜ AV-3-5 — CLI: `dtfs-cli file flush-cache <path>` + `dtfs-cli volume set-cache <id>`

* **Files:** `crates/dtfs-cli/src/main.rs`.
* **Implementation:** thin wrappers over the new API endpoints.
* **Tests:** `bug_av_cli_file_flush_cache_invokes_api` (mock-http
  unit), `bug_av_cli_volume_set_cache_parses_args`.
* **Done when:** 2 tests + minicluster: `dtfs-cli volume set-cache
  default warm=1 per-dc=true promote-reads=2 promote-window-secs=60
  cooldown-secs=300` succeeds; `dtfs-cli file flush-cache
  /movies/Season-1/` succeeds.

### ⬜ AV-3-6 — Documentation: client + WebUI + CLI

* **Files:**
  - `docs/src/configuration/fuse-client.md` — document
    `[client.write].first_write_tier` ("cold" vs "nearest-warm"),
    including the latency / durability trade-off and where the
    async anchor-migration kicks in.
  - `docs/src/api-reference/cli-reference.md` — fill in the placeholder
    from AV-1-12: `dtfs-cli file flush-cache <path>`,
    `dtfs-cli volume set-cache <id> warm.min=N warm.max=M
    per-dc=true|false promote-reads=R promote-window-secs=W
    cooldown-secs=C`,
    `dtfs-cli volume migrate-policy <id>`.
  - `docs/src/access/fuse-mount.md` — note how `first_write_tier`
    interacts with the FUSE write path (warm-first writes ACK
    sooner; anchor migration is async).
  - `docs/src/operations/production-deployment.md` — add a "Tier
    policy bootstrap" subsection: ordered steps for migrating an
    existing cluster from legacy `replication` → `mandatory_replicas`
    + `cache_replicas`, including the rolling-upgrade gating and the
    rollback path.
  - **`tests/proxmox/SOAK-RECOVERY-2026-05-21.md` Bug AV section** —
    full write-up: symptom (the user's question that started this),
    root cause (additive-only migrate, no delete path, no per-file
    tier preference), fix (everything in this plan), tests,
    live-verification log.
  - **`tests/proxmox/MINICLUSTER-VERIFICATION-2026-05-21.md` Bug AV
    section** — verification table + actual cluster output captured
    during AV-3-4 + AV-3-5 live demos.
* **Tests:** doc smoke test; the runbook walkthrough is performed
  during the live demo and recorded verbatim.
* **Done when:** All 6 doc surfaces updated; mdbook builds clean;
  recovery + verification docs carry the actual live cluster
  evidence.

### Phase 3 Done When

* AV-3-1 … AV-3-6 all ✅. 11 new tests green.
* End-to-end demo on minicluster, recorded in
  `MINICLUSTER-VERIFICATION-2026-05-21.md` Bug AV section.
* `mdbook build docs/` green.
* `CLAUDE.md` updated in the same commit that lands the final Phase 3
  code (per § 11 of CLAUDE.md itself — "update CLAUDE.md in the SAME
  commit as the code change").

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

_(empty)_

---

## 5. Deviations from plan

> When the implementation needs to deviate from this document (a new
> file, a different test name, a missed edge case), I append the
> deviation here so the doc stays accurate.

_(empty)_

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
chunk_size_mb = 128

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
chunk_size_mb = 32            # smaller chunks suit JPEG / RAW files

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
chunk_size_mb = 4             # docs are small; small chunks lower latency
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
honour it once that flag flips (Phase 4 follow-up).

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

> Per user's spec (2026-05-21 chat). Repeats here so it stays close to
> the plan.

For each ⬜ task in order:

1. Mark task 🟦 in this doc.
2. Implement (smallest change that satisfies the listed tests + done-when).
3. Run `cargo test -p <relevant-crate> --release` until green.
4. Sync to builder, run full crate test suite.
5. Deploy affected binary to minicluster (rolling restart for
   manager; restart-in-place for data + FUSE).
6. Run the live verification described in "Done when".
7. Add a `verification log` entry.
8. **Commit.** Stage the task's files + the plan update (status flip
   + verification-log entry) and commit with the message format:

   ```
   Bug AV-<phase>-<n>: <one-line summary>

   <2-3 sentence body: what changed, what tests cover it, any
   deviation from plan>

   Plan: tests/proxmox/PLAN-BUG-AV-TIER-LIFECYCLE.md (AV-<phase>-<n> ✅)
   ```

   One commit per task. Doc-only tasks (AV-1-12, AV-2-6, AV-3-6) get
   their own commit. The plan-status update is part of the same
   commit so the doc never drifts from the code.

9. Mark task ✅ with date in the plan.
10. Move to next ⬜.

Plan stays the source of truth — if reality diverges, the plan is
updated first (Deviations section), then the work continues.

### 6.1 Commit hygiene

* **Never `--no-verify`**. Pre-commit hooks (when added) must pass.
* **Never `--amend`** a pushed commit; create a new commit instead.
* **Never push to master** during AV work — keep on local until the
  user reviews each phase boundary. If/when a feature branch is
  warranted, it gets created at the start of a phase and merged
  after the phase's "Done when" is met.
* **Trailers required** on every commit (per CLAUDE.md / repo
  convention):

  ```
  Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
  ```

* **Co-locate** docs with code: when a code change has a
  corresponding doc update, both go in the same commit. The 3 doc
  tasks (AV-1-12 / AV-2-6 / AV-3-6) are their own commits because
  they aggregate doc edits across the phase.
