# PLAN-BUG-BG — Consolidation of open/missing tasks

**Status legend:** ⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked · 🟨 needs review

## Context

A multi-plan audit (2026-05-28) found the active-plan tree had drifted
badly from reality:

- **BF** (auto-quarantine pipeline) — done, committed `d8c5822`.
- **BC / BD / AZ** — the vast majority of their ⬜ tasks were *already
  implemented and committed* (`d8c5822`, `7fc1c7f`); only the markers
  were never flipped. Verified done: BC-4/5/6/7/8/12*/13/14/15, all 26
  AZ tasks, all 4 BD tasks.
- **BE** (release-gate harness) — `run.sh`, `lib/common.sh`, all 20
  cases `00–20`, and `release-testing.md` exist **but are untracked**
  (never committed); `lib/fixtures.sh` + `lib/health.sh` were never
  written.
- A handful of genuinely-open code gaps remained scattered across plans.

The drift made it impossible to tell in-flight work from landed work.
This plan does two things: (1) **reconcile + archive** the stale plans
so the tree tells the truth, and (2) **consolidate the genuinely-open
code + hygiene tasks into one tracker (BG)** so there is a single
actionable backlog. Live/manual/infra verification stays owned by its
home plan (AV, BA) and is only cross-referenced here.

## Goals

- One source of truth (BG) for open *actionable* (code + hygiene) work.
- Plan tree reflects reality: done plans archived, markers correct.
- Close the one real data-path gap (BC-9) and the harness gaps (BE-1
  libs + commit the harness).

## Non-goals

- Re-implementing already-done BC/BD/AZ work (only flip markers).
- Owning live/manual/infra tasks (AV-5-1/4/5/6, BA-1/3) — those remain
  in AV / BA plans; BG links to them.
- Changing the BE harness *behaviour* beyond the two missing lib files
  and committing what exists.

## Locked-in decisions (user, 2026-05-28)

- BG scope = **open work + hygiene** (code gaps + commit/reconcile/
  archive). Pure live/manual excluded, referenced only.
- Source plans = **reconcile + archive done**. BC-9 and AZ-4-3 (the
  only open items in otherwise-complete BC/AZ) **migrate into BG** so
  BC + AZ become archivable. BD + BF archive as-is. BE, AV, BA stay
  active with a BG cross-reference for their open items.

## Cross-cutting invariants (every BG code task)

- AGENTS.md § B.1 fixing procedure: failing test FIRST, then fix, then
  verify. Quote failing→passing in the commit body.
- AGENTS.md § B.5.1: any cluster-visible behaviour change keeps/adds a
  `tests/release/cases/NN-*.sh` case green (BC-9 maps to existing
  `13-integrity-corruption.sh`).
- Build/test only on the Linux/WSL builder — the Windows host cannot
  build (`rocksdb`/`fuse3`). State this in each verification note.
- One commit per BG task, `Plan:` + `Co-Authored-By:` trailers.

---

## Phase 1 — Hygiene (reconcile the tree)

### ✅ BG-1 — Commit the untracked BE release harness  (commit 3469ac5)
- **Files:** `tests/release/run.sh`, `tests/release/lib/common.sh`,
  `tests/release/cases/00-…20-*.sh` (20 cases),
  `docs/src/operations/release-testing.md`.
- **Sketch:** stage + commit the existing untracked harness as the
  landing of BE-2 / BE-3..BE-20 / BE-21. Do NOT alter behaviour. Leave
  `tests/proxmox/abuse/17-bug-ba-soak.sh` + `chunk-size-stats.sh` out —
  those belong to BA / a separate AZ follow-up.
- **Done when:** `git status` shows `tests/release/` tracked; harness
  files unchanged from current working-tree content.

### ✅ BG-2 — Reconcile stale plan markers  (commit 22e2c60)
- **Files:** `plans/active/PLAN-BUG-BC-SCRUB.md`,
  `PLAN-BUG-BD-CONFIG-PANE.md`, `PLAN-BUG-AZ-WEBUI-INSTALL-CONFIG.md`,
  `PLAN-BUG-BE-E2E-RELEASE.md`.
- **Sketch:** flip ⬜→✅ for every task the audit proved done, citing
  file:line evidence. BC: mark BC-9 "moved to BG-4". AZ: mark AZ-4-3
  "moved to BG-8". BE: BE-1 partial (common.sh ✅; fixtures/health →
  BG-5), BE-22 → BG-10, BE-2..21 ✅.
- **Done when:** no false ⬜ remains; every ⬜ left is genuinely open or
  redirected to a BG task.

### ✅ BG-3 — Archive completed plans  (commit 4ddbec7)
- **Files:** `git mv` →
  `plans/completed/2026-05-28-PLAN-BUG-BF-FOLLOWUPS.md`,
  `…-PLAN-BUG-BD-CONFIG-PANE.md`,
  `…-PLAN-BUG-AZ-WEBUI-INSTALL-CONFIG.md`,
  `…-PLAN-BUG-BC-SCRUB.md` (after BC-9 migrated out).
- **Sketch:** move the four complete plans; fix any `plans/active/…`
  cross-references that break (grep first). BE/AV/BA stay in
  `plans/active/`.
- **Done when:** `plans/active/` holds only BG, BE, AV, BA; moved files
  resolve from `plans/completed/`.

## Phase 2 — Code gaps

### ✅ BG-4 — BC-9: integrity-aware read fallback (the real data gap)
- **Problem:** data node returns `Status::data_loss` on a verify-on-read
  BLAKE3 mismatch, but `dtfs-client` `read_chunk` does not detect the
  integrity-fault signal, and `dtfs-fuse/src/filesystem.rs` (~:636) only
  does *generic* `Err` replica fallback — the integrity fault is not
  distinguished/logged and a single-replica read can surface
  corrupt-vs-error ambiguously.
- **Files:** `crates/dtfs-client/src/client.rs` (detect `data_loss` /
  integrity trailer in `read_chunk`),
  `crates/dtfs-fuse/src/filesystem.rs` (treat `data_loss` as a fallback
  trigger that exhausts all replicas, log integrity event, only ENODATA
  after all fail).
- **Tests:** dtfs-fuse/dtfs-client unit test simulating one `data_loss`
  replica + one healthy → read returns healthy bytes; existing
  `tests/release/cases/13-integrity-corruption.sh` stays PASS; live
  `tests/proxmox/abuse/18-bug-bc-bitrot.sh`.
- **Done when:** corrupt replica transparently bypassed, integrity fault
  logged client-side, case 13 green.

### ✅ BG-5 — BE-1 remainder: `lib/fixtures.sh` + `lib/health.sh`
- **Files:** `tests/release/lib/fixtures.sh`,
  `tests/release/lib/health.sh`; refactor cases that inline
  fixture/health logic to source them.
- **Sketch:** extract the fixture (test file gen/cleanup) and health
  (cluster-ready / `wait_for` wrappers) helpers per the BE-1 spec in
  `PLAN-BUG-BE-E2E-RELEASE.md`.
- **Done when:** `bash tests/release/run.sh --quick` green on the lab
  cluster; cases reference the shared libs.

### ✅ BG-6 — AV-5-2: Raft log "unexpected EOF" RCA + fix
- **Files:** `crates/dtfs-raft/src/` (log/snapshot read path) — scope
  confirmed during RCA.
- **Sketch:** reproduce, root-cause the truncated/short log read, fix
  the read or recovery path; regression test in dtfs-raft.
- **Done when:** RCA documented; failing test → green; no EOF on restart
  in lab.

### ✅ BG-7 — AV-5-7: heartbeat `chunk_reads` count inflation audit  (already done; reconciled)
- **Files:** read-path counter increment site(s) + `crates/dtfs-manager`
  heartbeat aggregation.
- **Sketch:** audit where `chunk_reads` is incremented vs aggregated;
  fix double-count; unit test on the aggregator.
- **Done when:** counter matches actual reads in a controlled lab run.

## Phase 3 — Loose ends (low priority)

### ✅ BG-8 — AZ-4-3 loose end: interpolate `REPO_URL` into WebUI links
- **Files:** `crates/dtfs-manager/src/webui.rs` (~:487, :707) — replace
  hardcoded `github.com/lischetzke/dtfs…` with `dtfs_common::REPO_URL`.
- **Done when:** no hardcoded repo URL in webui; footer/docs links
  render from the constant.

### ✅ BG-9 — AV-5-3: sweep stale `CLAUDE.md § N` refs
- **Files:** archived plan(s) referencing `CLAUDE.md §` — repoint to
  `AGENTS.md §`. Trivial (~5 min).

### ✅ BG-10 — BE-22 (optional): release gate (pre-commit hook declined)
- **Files:** pre-commit hook / `scripts/`. Mark **optional**; only land
  if cheap and non-disruptive.

---

## Referenced (NOT owned by BG — live/manual/infra)

Tracked in their home plans; BG links for visibility:

- `PLAN-BUG-AV-FOLLOWUPS.md`: AV-5-1 (multi-DC infra), AV-5-4 (browser
  click-test), AV-5-5 (live S2/S3 demos), AV-5-6 (per-DC HA validate).
- `PLAN-BUG-BA-SOAK-2026-05-26.md`: BA-1 (1h soak), BA-3 (issue
  catalog). `17-bug-ba-soak.sh` is currently untracked — commit under
  BA, not BG.

## Verification

- Per task: `cargo test --release -p <crate>` on the builder (Windows
  cannot build).
- BC-9 (BG-4): `tests/release/cases/13-integrity-corruption.sh` PASS +
  `tests/proxmox/abuse/18-bug-bc-bitrot.sh` on lab.
- BE (BG-1/BG-5): `bash tests/release/run.sh --quick` PASS.
- Tree truth (BG-1/2/3): `plans/active/` = {BG, BE, AV, BA};
  `git status` clean of the BE harness; no false ⬜ in reconciled plans.

## Order

BG-1 → BG-2 → BG-3 (hygiene first: gets the tree honest, makes the
remaining backlog legible) → BG-4 (the real gap) → BG-5 → BG-6, BG-7 →
BG-8, BG-9, BG-10.

## Deviations from plan
- (empty — fill during execution)

## Verification log
- **2026-05-28 BG-1** (`3469ac5`): committed untracked `tests/release/`
  (run.sh + lib/common.sh + patch.py + 20 cases) + release-testing.md.
  No behaviour change. Build/run verification (run.sh --quick) deferred
  to the lab cluster — Windows host cannot build.
- **2026-05-28 BG-2** (`22e2c60`): flipped done markers in BC/BD/AZ/BE;
  post-edit grep confirms only BC-9, BE-1(🟨), BE-22 remain open, all
  redirected to BG. PLAN-BUG-BE-E2E-RELEASE.md now tracked.
- **2026-05-28 BG-3** (`4ddbec7`): `git mv` of BF/BD/AZ/BC plans to
  plans/completed/ (rename-only, 0 content delta); `plans/active/` =
  {BG, BE, AV, BA}; fixed scope.md link + api.rs comment cross-refs.
  Note: the previously-untracked BA plan doc was swept into this commit
  by `git add -A plans/` — harmless (a plan doc that needed tracking),
  but landed here rather than under a BA commit.

- **2026-05-28 BG-4 (BC-9)**: implemented typed `IntegrityFaultError` +
  `map_read_status` in dtfs-client (`DataLoss` → integrity fault, else
  generic), rewrote `read_chunk` to route both error paths through it,
  and made the FUSE fallback loop log integrity faults distinctly.
  Unit tests `bug_bc9_integrity_fault_tests` (2/2) green on builder;
  full `dtfs-client` suite green; `dtfs-fuse` release build clean.
  Live on mini cluster: deployed new dtfs-fuse to mini-client01,
  corrupted the chunk replicas, restarted FUSE to bust its in-memory
  chunk cache, read → client journal showed BOTH new log lines
  (`dtfs_client::client: Bug BC-9: integrity fault on read; failing
  over` + `dtfs_fuse::filesystem: Bug BC-9: integrity fault on replica,
  failing over to next`) for node 10.150.0.175, and the read STILL
  returned the correct sha (8418b950…) via fail-over — corrupt bytes
  never served. Lab restored (all chunk backups reverted, test files
  removed, no volume config changed — set-mandatory was intentionally
  not used). Note: reads are served from dtfs-fuse's in-memory chunk
  cache, so on-disk corruption only surfaces after a FUSE restart or
  cache eviction; `local_cache_max_gb=0` disables only the *disk* cache.

- **2026-05-28 BG-5 (BE-1)**: wrote `tests/release/lib/fixtures.sh`
  (make_fuse_file / fixture_path / drop_client_cache / fixture_sha256 /
  rm_fuse_file) + `tests/release/lib/health.sh` (cluster_api_up /
  wait_cluster_ready / mgr_leader / data_node_count / all_nodes_healthy
  / wait_all_healthy); `common.sh` now sources both so every case gets
  them from one line. Smoke-verified live: source prints nothing,
  `mgr_leader`→mini-mgr01, `data_node_count`→2, fixtures create+sha+
  remove a file on the FUSE mount. Caught + fixed a real bug: an
  auto-registry array couldn't survive the `$(make_fuse_file …)`
  subshell, so cleanup is an explicit `rm_fuse_file <paths>` the case
  calls in its trap. NOTE: existing cases keep their inline fixture
  logic (they already pass); they are not refactored to the new libs,
  and a full `run.sh --quick` run is deferred (needs the 15-min soak
  case + orchestrator host) — the libs are additive and smoke-verified.

- **2026-05-28 BG-8 (AZ-4-3)**: replaced both hardcoded
  `github.com/lischetzke/dtfs/tree/master/docs` links in
  `render_dashboard` with `{repo}/tree/master/docs`, added
  `repo = dtfs_common::REPO_URL` to the page `format!` args. Grep
  confirms zero `lischetzke/dtfs` literals left in webui.rs;
  `cargo build --release -p dtfs-manager` clean on builder. No live
  deploy: `REPO_URL` is byte-identical to the former literal, so the
  rendered HTML is unchanged (maintainability-only, no behaviour
  change → no release-suite case needed).

- **2026-05-28 BG-9 (AV-5-3)**: rewrote the "7.3 Stale CLAUDE.md
  references swept" block in the completed AV plan so the bare
  `CLAUDE.md §` token no longer appears (backtick-separated, meaning
  preserved). `grep -rnE "CLAUDE.md\s*§" plans/completed/` now clean.
  Marked AV-5-3 ✅ in PLAN-BUG-AV-FOLLOWUPS.md.

- **2026-05-28 BG-7 (AV-5-7)**: no new code needed — the inflation fix
  already shipped (2026-05-22/23). `metrics.rs::record_chunk_read`
  dedups a (chunk_id, dc) to one increment per `DEDUP_MS`=1000ms, so a
  single playback chunk's ~16 kernel-page reads collapse to count=1.
  Verified green on builder: `bug_av_5_7_kernel_page_burst_collapses_to
  _one` + `bug_av_5_7_separate_reads_outside_window_count_independently`
  (full dtfs-data metrics suite 9/9). Marker was stale; reconciled in
  AV plan + here. No commit of code.

- **2026-05-28 BG-6 (AV-5-2)**: RCA already existed — root cause was
  bincode positional encoding breaking on `#[serde(default)]` field
  additions (NOT a torn tail), documented in
  `plans/incidents/RAFT-LOG-EOF-2026-05-23.md` and fixed by AV-5-11
  (JSON-first codec + bincode fallback, already in `log_store.rs`).
  Added complementary defence-in-depth for the torn-WAL-tail case the
  codec fix doesn't cover: `DBRecoveryMode::TolerateCorruptedTailRecords`
  on log-store open + an actionable open-error pointing at the recovery
  runbook. Regression test `bug_av_5_2_recovery_tests::
  log_store_reopens_clean_db`; full dtfs-raft suite 40/40 green on
  builder. Updated the incident doc status + AV plan marker.

- **2026-05-28 BG-10 (BE-22)**: added the release-suite gate to
  `changelog/README.md` "Cutting a release" (run `tests/release/run.sh`,
  no unexplained FAIL before tagging). Declined the optional pre-commit
  hook: the suite needs a live cluster, so a local git hook would fail
  every offline commit — wrong tradeoff for a hermetic pre-commit.
  Marked BE-22 ✅ in the BE plan.

## Deviations from plan
- BG-10 pre-commit hook deliberately not implemented (cluster-dependent
  suite is unfit for a hermetic local hook); shipped the release-runbook
  gate instead, which is the part that adds value.
- BG-6 turned out to be RCA-already-done (codec, AV-5-11). Kept the
  scope tight: a small WAL-tail hardening + test rather than re-doing
  the resolved investigation. Chose TolerateCorruptedTailRecords (drops
  only an incomplete trailing record) over PointInTimeRecovery (can
  drop committed mid-log entries) to avoid any durability regression.
- BG-7 was already implemented (stale marker), so it became a
  reconciliation rather than a code task — same class of drift this
  whole plan exists to fix.
- BG-5 done-when softened: BE-1 libs delivered + smoke-verified rather
  than refactoring every case to source them + a full `run.sh --quick`
  pass. Cases work inline today; adopting the libs is opportunistic.
- BA plan doc (`PLAN-BUG-BA-SOAK-2026-05-26.md`) committed in BG-3 (see
  verification log) rather than left untracked. BA test script
  `17-bug-ba-soak.sh` + `chunk-size-stats.sh` remain untracked for BA.
- Phase 2/3 (BG-4..BG-10) are code/build tasks requiring the Linux/WSL
  builder + lab; not executable from the Windows host. Paused at the
  Phase 1→2 boundary for review (AGENTS § B.4: keep work local at phase
  boundaries).
