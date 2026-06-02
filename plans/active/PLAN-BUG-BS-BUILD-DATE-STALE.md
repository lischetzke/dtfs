# Bug BS — Dev BUILD_DATE frozen (build.rs rerun gating defeats auto-stamp)

**Status:** complete (build-host verified 2026-06-02; live re-stamp confirmed at deploy)
**Source:** user report 2026-06-02 ("version string still the same"); AGENTS.md §A.3.6 hard requirement.

---

## 0. Background

Every node in the minicluster reports `version:"v0.0.0-dev"`,
`build_date:"20260522-131629"` — identical across all nodes and unchanged
across days of rebuilds. `BUILD_DATE` is the only field distinguishing two dev
builds (`VERSION` is pinned `v0.0.0-dev`), so a frozen stamp makes "is the new
binary actually deployed?" unanswerable. This is the exact symptom the new
AGENTS.md §A.3.6 hard requirement forbids.

Root cause — `crates/dtfs-common/build.rs`:
```rust
let build_date = dt.format("%Y%m%d-%H%M%S").to_string();   // from SystemTime::now()
println!("cargo:rustc-env=DTFS_BUILD_DATE={build_date}");
println!("cargo:rerun-if-changed=build.rs");               // <-- the bug
println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
```
`cargo:rerun-if-changed=build.rs` tells cargo to re-run the build script ONLY
when `build.rs` itself changes. Cargo caches `DTFS_BUILD_DATE` and reuses it on
every subsequent `cargo build`, so the stamp is frozen at the first run
(`20260522-131629`, the day build.rs last changed). `version.rs` consumes it via
`env!("DTFS_BUILD_DATE")`.

The gating was deliberate (comment: "otherwise cargo would invalidate the cache
on every invocation and force rebuilds across the workspace") — it traded
freshness for cache stability and broke the feature.

---

## 1. Goals / non-goals

**Goals:**
- `BUILD_DATE` reflects the instant of THIS build: two builds a minute apart
  produce two different stamps (BS-1).
- Do it without force-rebuilding the whole workspace on every `cargo build`
  (the constraint that motivated the broken gating).
- Verify it (BS-2) so it cannot silently regress.

**Non-goals:**
- A real semver release process / bumping `VERSION` (still `v0.0.0-dev`).
- Changing what reports the version (Bug BR covers manager self-publish).
- Reproducible-build behaviour when `SOURCE_DATE_EPOCH` is set — that pin is the
  sanctioned exception (AGENTS.md §A.3.6) and must keep working.

---

## 2. Locked-in decisions

- **Mechanism (preferred)**: isolate the timestamp so only a trivial unit
  re-stamps, OR drive the stamp from `SOURCE_DATE_EPOCH` exported by the build
  wrapper. Decision after a quick spike on rebuild cost; default to the approach
  that refreshes every build with the least rebuild fan-out:
  1. **Build-wrapper `SOURCE_DATE_EPOCH`** — the deploy/build script
     (`scripts/` or the dtfs-builder build step) exports
     `SOURCE_DATE_EPOCH=$(date +%s)` before `cargo build`. `build.rs` already
     honours it AND already has `cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH`,
     so the stamp refreshes whenever the wrapper runs, with no source change.
     Smallest code change; keeps plain `cargo build` cache-stable for local devs
     who do not export it (they get a stable-but-old date, acceptable since the
     authoritative builds go through the wrapper). **Lead candidate.**
  2. **Sentinel-file rerun** — `cargo:rerun-if-changed=<sentinel>` where the
     wrapper `touch`es the sentinel each build. Equivalent effect, more moving
     parts.
  3. **Unconditional rerun** — drop `rerun-if-changed=build.rs`. Re-stamps every
     build but reruns build.rs (and rebuilds dtfs-common + downstream) every
     time. Only if profiling shows the cost is negligible.
- **Keep** `SOURCE_DATE_EPOCH` honouring exactly as-is for reproducible builds.
- The fix MUST be observable: a documented two-build check + an automated
  assertion (BS-2).

---

## 3. Data-model changes

None. `VERSION` / `BUILD_DATE` constants and the `DTFS_BUILD_DATE` env contract
are unchanged; only WHEN the stamp is recomputed changes.

---

## 4. Task list

### BS-1: Refresh BUILD_DATE on every authoritative build

- Status: ✅ (2026-06-02)
- Files: `crates/dtfs-common/build.rs`, the build wrapper in `scripts/` and/or
  the dtfs-builder build step in `tests/proxmox/` / `deploy/` (wherever the
  release build is driven), AGENTS.md §A.4.1 (document the wrapper requirement).
- Sketch (lead candidate — option 1):
  1. Add `SOURCE_DATE_EPOCH=$(date -u +%s)` export to the canonical build
     wrapper before `cargo build --release`. `build.rs` already formats it and
     already declares `rerun-if-env-changed=SOURCE_DATE_EPOCH`, so the stamp
     updates each wrapper invocation without touching cached crates that did not
     otherwise change.
  2. Document in AGENTS.md §A.4.1 that release builds go through the wrapper
     (cross-reference §A.3.6).
  3. If a spike shows plain `cargo build` (no wrapper) must also refresh, fall
     back to option 2 (sentinel) — record the decision in Deviations.
- Tests:
  - Build twice with the wrapper a minute apart; assert the two binaries report
    different `BUILD_DATE` (`dtfs-manager --version` / `version_string()` or the
    `/api/v1/nodes` field after deploy).
- Done when:
  - Two wrapper builds a minute apart yield two distinct `BUILD_DATE` values;
    `SOURCE_DATE_EPOCH`-pinned builds still reproduce a fixed stamp.

### BS-2: Automated guard against regression

- Status: ✅ (2026-06-02)
- Files: `tests/release/cases/NN-build-date-fresh.sh` (or a build-wrapper
  self-check).
- Sketch: in the release suite (or the build step), run two stamped builds (or
  invoke `build.rs`'s logic twice with `SOURCE_DATE_EPOCH` t and t+60) and assert
  the emitted `DTFS_BUILD_DATE` differs; assert a fixed `SOURCE_DATE_EPOCH`
  reproduces a fixed value. Status protocol per §B.5.1.
- Done when: case PASS (distinct stamps) / pinned `KNOWN_ISSUE [BS]`.

### BS-3: Changelog

- Status: ✅ (2026-06-02)
- Files: `changelog/UNRELEASED.md`
- Entry: dev `BUILD_DATE` now reflects each build (was frozen at first build);
  note `SOURCE_DATE_EPOCH` reproducible-build behaviour preserved. Same commit
  as BS-1.

---

## 5. Cross-cutting invariants

- AGENTS.md §A.3.6 hard requirement: two builds at different times → two
  different `BUILD_DATE`.
- `SOURCE_DATE_EPOCH` reproducible-build pin MUST keep producing a fixed stamp.
- Do not regress workspace build times: the chosen mechanism must not force a
  full rebuild on every incremental `cargo build` for local dev.
- `BUILD_DATE` stays UTC, format `YYYYMMDD-HHmmss`, no zone suffix.

---

## 6. Verification log

- **BS-1 (2026-06-02)** — Implemented option 1 (build-wrapper
  `SOURCE_DATE_EPOCH`). `build-linux-amd64.sh` + `dtfs-build.sh` now
  `export SOURCE_DATE_EPOCH="${SOURCE_DATE_EPOCH:-$(date -u +%s)}"` before
  `cargo build`; `build.rs` comment updated; logic unchanged (already
  honoured the env + `rerun-if-env-changed`). Added a compile-time
  `VERSION_STRING` const and wired clap `version = VERSION_STRING` on all
  four bins so `dtfs-* --version` prints `v0.0.0-dev (built ...)`.
- **BS-2 (2026-06-02)** — `scripts/tests/test-build-date-fresh.sh` run on
  the builder: `EPOCH_A=1000000000 -> 20010909-014640`,
  `EPOCH_B=1000000060 -> 20010909-014740` (distinct), re-build at EPOCH_A
  -> `20010909-014640` (reproducible). Output: `PASS: BUILD_DATE is fresh
  per-instant and reproducible under a pinned epoch`. This also confirms
  the `VERSION_STRING`/clap wiring compiles and `--version` emits the
  stamp.
- **BS-3 (2026-06-02)** — `changelog/UNRELEASED.md` entry added.
- **Live (at deploy)** — the wrapper build that produces the BU/BV/BT
  binaries stamps today's instant; deployed nodes report it via
  `dtfs-* --version` and `/api/v1/nodes.build_date` (no longer the frozen
  `20260522-131629`).

---

## 7. Deviations from plan

- **Option 1 chosen, no spike needed.** `build.rs` already honoured
  `SOURCE_DATE_EPOCH` + `rerun-if-env-changed`, so the wrapper export was
  the smallest change and keeps plain `cargo build` cache-stable. Options
  2/3 (leaf crate / unconditional rerun) not pursued.
- **BS-2 is a build-host self-check, not a live release case.** Placed at
  `scripts/tests/test-build-date-fresh.sh` rather than
  `tests/release/cases/` because `BUILD_DATE` is a build-time property —
  it cannot be observed against a running cluster without rebuilding, and
  the release suite runs against live nodes. Referenced from AGENTS.md
  §A.3.6.
- **Added `--version` reporting (scope bump, in spirit).** Wired
  `VERSION_STRING` through clap on all four binaries so an operator can
  read a deployed binary's stamp directly — directly serves the §A.3.6
  goal ("is the new binary actually deployed?").
