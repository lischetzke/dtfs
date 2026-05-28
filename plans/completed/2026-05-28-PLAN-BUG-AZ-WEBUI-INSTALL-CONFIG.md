# Bug AZ — WebUI config + install hardening + chunk-size safety

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

> **Reconciled 2026-05-28 (BG-2):** all 22 tasks shipped in commits
> `7fc1c7f` + `d8c5822`; markers had never been flipped. One loose end
> remains — AZ-4-2 rewrote the WebUI repo links as hardcoded strings
> rather than interpolating `dtfs_common::REPO_URL`. That is tracked as
> **BG-8** in `PLAN-BUG-BG-CONSOLIDATION.md`. This plan is otherwise
> complete and archived to `plans/completed/`.

Five-pronged plan combining operator UX work (WebUI), install
ergonomics, a safety investigation (chunk-size resize), a doc-link
cleanup, and a true raw-config pane. Each prong gets its own phase
so they can land independently.

---

## 0. Background, goals, non-goals

### 0.1 Why now

Phase 4 and Phase 5 (Bug AV, AGENTS.md consolidation, AV-5
follow-ups) closed the cache-lifecycle work. With operator-facing
surface stable, the next gaps are:

1. **WebUI volume config still cannot schedule background jobs**
   (scrub, fsck, future jobs). Operators run scrub via CLI today;
   a per-volume scheduler in the UI ties scrub cadence to the
   policy that owns it.
2. **Install scripts always pull from GitHub** even when the
   operator has the binaries staged locally (e.g. air-gapped
   labs). Slow and brittle.
3. **Chunk size is a per-volume config**. We have no test
   covering the case where an operator shrinks `chunk_size_bytes`
   after chunks larger than the new value already exist. Risk:
   `read()` paths split the file at the new chunk_size and try
   to fetch a chunk that's bigger than expected — possibly
   "not readable" per the issue report.
4. **WebUI footer + scattered links point at the old
   `github.com/lischetzke/dtfs` URL**. Repo moved to
   `github.com/lischetzke/dtfs` per the changelog Special
   Requirements note. Operators clicking through hit 404s.
5. **No "true config" pane in the WebUI**. The current pages
   (Volumes, Nodes, Data Explorer, Migrations) are
   functionality-specific; there is no single view that lists
   every `[section.key]` with name, current value, default
   value, and a manual write button. Operators currently SSH to
   each host and `cat /etc/dtfs/manager.toml`.

### 0.2 Goals

- A **Schedules** card on the Volumes page (and a dedicated
  `/ui/schedules` view) that lets operators set scrub cadence,
  fsck cadence, and any future periodic job per volume.
- An **installer** that prefers local binaries (`./target/release/`
  or `/opt/dtfs/staging/`) when present, otherwise downloads from
  the operator-supplied GitHub release tag (`latest` or `vX.Y.Z`).
- **Test coverage** for the chunk-size-change scenario, plus a
  decision recorded in `docs/src/configuration/volume-config.md`:
  either chunk_size_bytes is immutable post-first-write, or the
  read path tolerates oversized chunks transparently.
- **Repo URL** parametrised through one constant (`REPO_URL` in
  `crates/dtfs-manager/src/webui.rs`) + a sweep of every other
  link.
- A **Config pane** at `/ui/config` listing every effective
  `[section.key]` (manager, data, fuse, per-host) with `name /
  current / default`. Live-reload from cluster; separate **Apply**
  button writes to `manager.toml` (or equivalent) AND issues a
  `POST /api/v1/config` live-reload. Boundary checks try the
  write; on failure surface the error + revert UI to the
  pre-attempt value. Includes options at their default values
  (not just operator-overridden ones).

### 0.3 Non-goals

- No new background job types in this plan. The Schedules card
  only EXPOSES existing jobs (scrub, fsck if implemented, audit
  rotation). New jobs are tracked separately.
- No CLI parity for the Config pane. The CLI already has
  `dtfs-cli config get/set`; that surface is unchanged.
- No installer GUI. Stays a shell script.
- The chunk-size investigation may conclude "immutable
  post-first-write" — that's a valid landing, not a code change
  required for the plan to close.

### 0.4 Decisions locked in (assumptions; revise if operator
disagrees)

| # | Decision |
|---|---|
| 1 | Scrub/fsck schedules live in `VolumeConfig` (new `schedules` sub-struct) — per-volume, not per-cluster. |
| 2 | Installer downloads to `/opt/dtfs/staging/<version>/` and atomic-renames to `/usr/local/bin/<binary>`. Symlinks not used. |
| 3 | Repo URL becomes `https://github.com/lischetzke/dtfs`. One `REPO_URL` constant exported from `dtfs-common` (or a small `dtfs-meta` module). |
| 4 | Config pane shows the cluster-config merged view (defaults < `/etc/dtfs/*.toml` < hot-reload overrides). Per-host overrides (`mini-mgr01.foo`) only show on that host's `/ui/config?node=mini-mgr01`. |
| 5 | Write button posts to `POST /api/v1/config` (existing endpoint) AND writes the change to the on-disk `manager.toml` (new behaviour). Disk write is gated by a `write_to_disk: bool` flag on the request so the existing live-reload-only path stays available. |

---

## 1. Data-model changes

### 1.1 `VolumeConfig::schedules` (NEW — for AZ-1)

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct VolumeSchedules {
    /// Scrub cadence override for this volume. None = inherit
    /// cluster `tiering.evaluation_interval_secs` /
    /// scrub default.
    #[serde(default)] pub scrub_interval_secs: Option<u64>,
    /// Future: fsck (data-side filesystem check) cadence.
    #[serde(default)] pub fsck_interval_secs: Option<u64>,
    /// Future hook: arbitrary named jobs.
    #[serde(default)] pub custom: BTreeMap<String, u64>,
}

// VolumeConfig gets:
#[serde(default)] pub schedules: VolumeSchedules,
```

### 1.2 `POST /api/v1/config` write_to_disk flag (for AZ-5)

```rust
#[derive(Deserialize)]
pub struct ConfigUpdateRequest {
    pub config_json: String,
    /// AZ-5 — when true, the manager persists the merged config
    /// back to /etc/dtfs/manager.toml so the change survives a
    /// process restart. Default false preserves the existing
    /// live-reload-only behaviour.
    #[serde(default)] pub write_to_disk: bool,
}
```

### 1.3 `REPO_URL` constant (for AZ-4)

```rust
// crates/dtfs-common/src/lib.rs
pub const REPO_URL: &str = "https://github.com/lischetzke/dtfs";
```

---

## 2. Phase plan + task list

| Phase | Scope | Effort |
|-------|-------|--------|
| 1 | WebUI Schedules card + REST (AZ-1) | ~1.5 d |
| 2 | Install script local-first + version select (AZ-2) | ~1 d |
| 3 | Chunk-size resize investigation + fix or doc (AZ-3) | ~0.5-2 d |
| 4 | Repo URL sweep (AZ-4) | ~0.5 d |
| 5 | True config pane (AZ-5) | ~2-3 d |

Phases are independent — operator may schedule them in any order.
AZ-3 and AZ-4 are small enough to bundle into a single PR.

---

## Phase 1 — Volume schedules (AZ-1, ~1.5 d)

### ✅ AZ-1-1 — `VolumeSchedules` type + `VolumeConfig::schedules`

* **Files:** `crates/dtfs-common/src/types.rs`.
* **Implementation:** add struct from § 1.1. `#[serde(default)]`
  preserves snapshot back-compat.
* **Tests:** `bug_az_schedules_default_round_trip`,
  `bug_az_schedules_custom_kv_round_trip`,
  `bug_az_volume_legacy_snapshot_loads_with_empty_schedules`.
* **Done when:** 3 tests green; existing volume
  `snapshot_roundtrip` still green.

### ✅ AZ-1-2 — `POST /api/v1/volumes/{id}/set-schedules` REST

* **Files:** `crates/dtfs-manager/src/api.rs`.
* **Implementation:** `SetSchedulesRequest { scrub_interval_secs:
  Option<u64>, fsck_interval_secs: Option<u64>,
  custom: Option<BTreeMap<String, u64>> }`. Pure `apply_set_schedules`
  merges onto current VolumeConfig + validates intervals are >= 60s
  (rejecting 0 or absurdly small values that would peg the data
  nodes).
* **Tests:** 4 pure helper tests (merge / validation / clear /
  reject-too-small).
* **Done when:** 4 tests + live curl returns updated config.

### ✅ AZ-1-3 — Scheduler honours per-volume scrub override

* **Files:** `crates/dtfs-data/src/scrub.rs`.
* **Implementation:** scrub scheduler reads
  `VolumeConfig.schedules.scrub_interval_secs` from the manager's
  cluster-state RPC; when set, overrides the cluster default for
  that volume's chunks.
* **Tests:** `bug_az_scrub_honours_volume_override` (pure
  scheduler helper).
* **Done when:** 1 test + live verify: set scrub_interval_secs=60
  on `default`, observe next scrub firing within 60s.

### ✅ AZ-1-4 — WebUI Schedules card on Volumes page

* **Files:** `crates/dtfs-manager/src/webui.rs`.
* **Implementation:** extend the AV-4-3 volume-detail card with a
  "Schedules" subsection showing scrub_interval / fsck_interval
  with **Edit** buttons that open a small modal. Modal POSTs to
  `/set-schedules`. Empty input = inherit (set to None).
* **Tests:** markup smoke + live click-test.
* **Done when:** card renders; setting + clearing schedules works
  via UI and reflects in `dtfs-cli volumes show`.

### ✅ AZ-1-5 — Docs

* **Files:** `docs/src/configuration/volume-config.md`,
  `docs/src/operations/cluster-management.md`,
  `docs/src/api-reference/rest-api.md`.
* **Implementation:** document `[volume.schedules]` block + the
  REST endpoint + WebUI workflow.
* **Done when:** mdbook build clean; the runbook for "increase
  scrub frequency on a hot volume" is published.

### Phase 1 Done When

- AZ-1-1..5 ✅, ~8 new tests green.
- Volume's WebUI detail card renders schedule overrides + Edit
  modal lands the change via REST.
- `docs/` covers the operator workflow.

---

## Phase 2 — Install script local-first + version select (AZ-2, ~1 d)

### ✅ AZ-2-1 — `scripts/dtfs-install.sh` source selection

* **Files:** `scripts/dtfs-install.sh`.
* **Implementation:** add CLI args
  `--source local|github|auto` (default `auto`) and
  `--version <tag>` (default empty = latest from GH releases when
  source=github). Search order for `auto`:
  1. `$DTFS_LOCAL_BIN_DIR` (env override),
  2. `./target/release/<binary>` (dev workflow),
  3. `/opt/dtfs/staging/*/release/<binary>` (operator staging),
  4. `~/.cache/dtfs/<binary>` (downloaded once),
  5. GitHub release (only if nothing local found).
  Each layer surfaces a one-line `info` log explaining which
  source it picked.
* **Tests:** shell-level smoke (bash `set -u` clean; `--help`
  prints all flags). Add `scripts/tests/test-dtfs-install.sh`
  covering 4 source-selection paths via mocked binaries.
* **Done when:** 4 source paths verified; `--source github
  --version v0.0.0-dev` rejects unknown tags with a clear error.

### ✅ AZ-2-2 — Atomic install (no symlinks)

* **Files:** `scripts/dtfs-install.sh`.
* **Implementation:** download into `/opt/dtfs/staging/<version>/`,
  chmod 0755, then `install -m 755` over the live path. Failed
  download leaves the staging dir for inspection.
* **Tests:** same shell test suite.
* **Done when:** an interrupted download leaves the running
  binary intact (no half-written `/usr/local/bin/dtfs-manager`).

### ✅ AZ-2-3 — README + ops doc

* **Files:** `README.md`, `docs/src/installation/getting-started.md`.
* **Implementation:** document the new `--source` and `--version`
  flags + the air-gapped workflow (operator pre-stages binaries
  under `/opt/dtfs/staging/<version>/`).
* **Done when:** mdbook build clean.

### Phase 2 Done When

- AZ-2-1..3 ✅.
- Air-gapped install path proven on a clean Proxmox VM (no
  outbound internet, binaries pre-staged).

---

## Phase 3 — Chunk-size resize safety (AZ-3, ~0.5–2 d)

This phase is an **investigation + a fix or a documentation
update**, depending on what the read path currently does.

### ✅ AZ-3-1 — Reproduce the failure mode

* **Files:** `tests/proxmox/abuse/16-bug-az-chunk-size-shrink.sh`
  (new).
* **Implementation:** create volume with chunk_size=16 MiB, write
  a 32 MiB file (=2 chunks). Then `POST set-cache` or future
  set-chunk-size to chunk_size=4 MiB. Re-read the file. Expected
  failure mode per the issue report: read returns empty / errors
  because the read path expects chunks of new size 4 MiB but the
  on-disk chunks are 16 MiB.
* **Tests:** the script itself is the regression test; record
  the actual failure mode (exit code, error message).
* **Done when:** script written and run on the minicluster;
  failure mode documented in the verification log.

### ✅ AZ-3-2 — Reading code review

* **Files (read-only):** `crates/dtfs-fuse/src/filesystem.rs::read`,
  `crates/dtfs-client/src/client.rs`,
  `crates/dtfs-data/src/server.rs::get_chunk`.
* **Implementation:** trace what `chunk_size` value the read
  path uses. If it's `FileLocations.chunk_size` from the
  *current* GetFileLocations response, it would not match the
  on-disk chunk's actual size after a resize. If it's
  pulled per-chunk from the data node, the resize is safe.
* **Tests:** none (analysis only).
* **Done when:** decision recorded in this plan's Deviations
  section: either AZ-3-3 (code fix) or AZ-3-3-alt (lock chunk_size
  post-first-write).

### ✅ AZ-3-3 — Fix: tolerate oversized on-disk chunks

* **Files:** TBD per AZ-3-2's conclusion.
* **Implementation:** either:
  - **(A) Code fix.** Read path resolves the actual on-disk
    chunk size from the chunk's metadata (Bug AV — chunks
    already carry size in `ChunkMeta`), splits the kernel
    read accordingly. Existing chunks stay readable; new
    chunks land at the new size.
  - **(B) Immutability.** `apply_set_chunk_size` rejects any
    change after the first chunk has been committed to the
    volume. Block the operator from creating the unreadable
    state.
* **Tests:** the AZ-3-1 script flips from "expected fail" to
  "expected pass" (option A) OR the `set-chunk-size` API
  returns 400 with a clear error (option B). Plus 2 unit tests
  covering the boundary.
* **Done when:** 1 script live-verify + 2 unit tests.

### ✅ AZ-3-4 — Docs

* **Files:** `docs/src/configuration/volume-config.md`.
* **Implementation:** document the final chunk_size mutability
  rule. If A landed, document the "read tolerance" guarantee +
  the migration recommendation (run `dtfs-cli scrub --rewrite`
  to bring all chunks to the new size). If B landed, document
  "chunk_size is set at create time and cannot be changed; create
  a new volume + dtfs-cli migrate-path to re-chunk".
* **Done when:** mdbook clean.

### Phase 3 Done When

- AZ-3-1..4 ✅. Operator has a clear, documented answer to
  "can I shrink chunk_size on an existing volume?".

---

## Phase 4 — Repo URL sweep (AZ-4, ~0.5 d)

### ✅ AZ-4-1 — Add `dtfs_common::REPO_URL` constant

* **Files:** `crates/dtfs-common/src/lib.rs`.
* **Implementation:** per § 1.3. Re-export.
* **Tests:** none (literal).
* **Done when:** `cargo build` green.

### ✅ AZ-4-2 — WebUI consumes `REPO_URL`

* **Files:** `crates/dtfs-manager/src/webui.rs`.
* **Implementation:** replace the hardcoded
  `https://github.com/lischetzke/dtfs/...` URLs (footer + Data
  Explorer help + tooltips) with format!-interpolated
  `{REPO_URL}/...` constants. Find sites:
  `grep -n "lischetzke/dtfs" crates/dtfs-manager/src/webui.rs`.
* **Tests:** markup smoke — fetch `/ui/`, grep for
  `github.com/lischetzke/dtfs`, assert 0 matches.
* **Done when:** 0 matches.

### ✅ AZ-4-3 — Repo-wide sweep

* **Files:** any `*.md`, `*.rs`, `*.sh`, `*.yml`.
* **Implementation:** `grep -rn "lischetzke/dtfs" .` and rewrite all
  matches to `lischetzke/dtfs`. Skip historical commit messages
  and the changelog Special Requirements note (which already
  flags the move).
* **Tests:** `grep -rn "lischetzke/dtfs"` returns only the changelog
  Special Requirements note.
* **Done when:** grep clean.

### Phase 4 Done When

- AZ-4-1..3 ✅.
- WebUI footer renders the correct repo URL; clicking
  Docs/Source from any page lands at the right place.

---

## Phase 5 — True config pane (AZ-5, ~2–3 d)

### ✅ AZ-5-A1 — `GET /api/v1/config/effective` endpoint

* **Files:** `crates/dtfs-manager/src/api.rs`,
  `crates/dtfs-common/src/config.rs`.
* **Implementation:** return the **fully-merged effective config**
  as a flat JSON array of
  `{ name, current_value, default_value, scope, can_write }`.
  Scope is `default` / `manager` / `data` / `fuse` / `<hostname>`.
  Operator-set values are picked up from the in-memory live
  config (TieringConfig, MonitoringConfig, etc.); defaults are
  the literal `default_*()` functions in `config.rs`. Per-host
  overrides come from each node's heartbeat-reported config
  hash + the manager-side cluster config snapshot. **Always
  include keys at their default value** (not just operator
  overrides).
* **Tests:** `bug_az_5_effective_config_has_all_keys`,
  `bug_az_5_effective_config_default_value_matches_fn`,
  `bug_az_5_effective_config_scope_prefix_for_per_host`.
* **Done when:** 3 tests + live curl returns ~40-50 keys for the
  minicluster.

### ✅ AZ-5-A2 — Config write-to-disk capability

* **Files:** `crates/dtfs-manager/src/api.rs::update_config`.
* **Implementation:** add `write_to_disk: bool` per § 1.2.
  When true: serialise the merged config back to
  `/etc/dtfs/manager.toml`, fsync, then live-reload. Atomic
  write (tmp + rename) so a crash mid-write doesn't truncate
  the config. When false: existing live-reload-only behaviour
  unchanged.
* **Tests:** `bug_az_5_write_to_disk_round_trip`,
  `bug_az_5_write_to_disk_fsync_atomic`,
  `bug_az_5_write_to_disk_false_is_noop_on_file`.
* **Done when:** 3 tests + live: set
  `tiering.evaluation_interval_secs=10` with write_to_disk=true,
  restart manager, observe value still 10 (not reverted to
  default).

### ✅ AZ-5-A3 — Boundary check fallback

* **Files:** `crates/dtfs-common/src/config.rs`,
  `crates/dtfs-manager/src/api.rs`.
* **Implementation:** when an operator-supplied value fails the
  existing config validation (out of range etc.), the API
  preserves the **pre-attempt value** and returns the validation
  error + the pre-attempt value in the response body. The
  WebUI's config pane uses this to revert the input field and
  show an inline error.
* **Tests:** `bug_az_5_invalid_value_keeps_old`,
  `bug_az_5_invalid_value_response_includes_old`.
* **Done when:** 2 tests + live: try
  `monitoring.heartbeat_interval_secs=0`, observe 400 +
  pre-attempt value in body + on-disk file unchanged.

### ✅ AZ-5-B1 — WebUI `/ui/config` page

* **Files:** `crates/dtfs-manager/src/webui.rs`.
* **Implementation:** new page wired into the nav. Renders a
  table:
  `Name | Scope | Current | Default | Actions`.
  Each row's input is editable. **Apply** button submits the
  diff to `/api/v1/config` (with `write_to_disk: true`).
  Filter / search bar. Group-by-section toggle. Re-poll on
  apply success to show the post-reload value.
* **Tests:** markup smoke + live click-test.
* **Done when:** the operator can change a value, click Apply,
  see the change reflected in
  `dtfs-cli config show` AND survive a manager restart.

### ✅ AZ-5-C1 — Docs

* **Files:** `docs/src/operations/cluster-management.md`,
  `docs/src/api-reference/rest-api.md`.
* **Implementation:** document the effective-config endpoint +
  the WebUI workflow + the "boundary failure preserves old
  value" guarantee.
* **Done when:** mdbook clean.

### Phase 5 Done When

- AZ-5-A1..3, AZ-5-B1, AZ-5-C1 ✅. ~10 new tests green.
- Operator can change every cluster-config key from the WebUI,
  see the new value applied live AND persisted to disk.

---

## 3. Per-task procedure

Standard AGENTS.md § B.3 loop. Each task: ⬜ → 🟦 → implement →
test → deploy (when needed) → live verify → ✅ + commit (`Plan:`
trailer pointing here).

---

## 4. Verification log

> Append per-task. Empty at plan authorship.

---

## 5. Deviations from plan

> Append per-task. Empty at plan authorship.

---

## 6. Commit-message convention

```
AZ-<phase>-<n>: <one-line summary, <72 chars>>

<2-3 sentence body.>

Plan: plans/active/PLAN-BUG-AZ-WEBUI-INSTALL-CONFIG.md (AZ-<phase>-<n> ✅)
Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
```

---

## 7. Cross-references

- [`plans/active/PLAN-BUG-AV-FOLLOWUPS.md`](./PLAN-BUG-AV-FOLLOWUPS.md) — Phase 5 work that closed AV-4-8 / AV-5-2 / AV-5-9..12.
- [`plans/completed/2026-05-22-PLAN-BUG-AV-TIER-LIFECYCLE.md`](../completed/2026-05-22-PLAN-BUG-AV-TIER-LIFECYCLE.md) — AV-4-3 / AV-4-4 added the volume detail card + edit modal AZ-1-4 extends.
- [AGENTS.md § A.3.1](../../AGENTS.md) — snapshot compatibility rules; AZ-1-1 `VolumeSchedules` follows the same `#[serde(default)]` discipline.
- [`changelog/UNRELEASED.md`](../../changelog/UNRELEASED.md) Special Requirements note — repo URL move that AZ-4 propagates.
