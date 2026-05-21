# AGENTS.md — Operating Discipline for AI Agents on DTFS

This file complements [CLAUDE.md](./CLAUDE.md). CLAUDE.md tells an
agent **what** to know about this codebase (target workload,
architecture, file map, integrity invariants, memory model, bug
catalog). AGENTS.md tells an agent **how** to work on it — the
discipline for planning, implementing, testing, committing,
documenting.

For trivial work (one-line fixes, single doc edits) this discipline
is overkill — apply judgement and just do the thing. For any
non-trivial change — a new feature, a multi-bug fix wave, a refactor
that touches more than ~3 files, anything spanning >1 working day —
AGENTS.md is mandatory.

---

## Fixing Procedure

Every bug fix — no matter how small — follows this three-step
procedure. It is the contract every contributor (human or AI) agrees
to when working on DTFS, and it is referenced from [README.md](./README.md#contribution).

1. **Write a test that fails because of the issue.** Put the test in
   the purest layer that demonstrates the bug — see
   [§ 4](#4-test-discipline) for where each kind of test lives. If
   the surface only manifests on a live cluster, the failing
   "test" is a shell regression script under
   `tests/proxmox/abuse/NN-name.sh`. Either way, **run it and
   observe the failure** before writing any fix code.
2. **Implement the fix.** Make the smallest change that turns the
   failing test green; do not bundle unrelated cleanup or scope
   creep.
3. **Verify the fix works.** Run the new test (must now pass), the
   surrounding crate's test suite (no regressions), and — if the
   bug is operational — live-verify on the lab cluster per
   [CLAUDE.md § 4](./CLAUDE.md#4-test-first-workflow-non-negotiable).
   Quote the failing-then-passing run in the commit body.

Skipping any step makes the fix unverifiable; landing it anyway is
a contract violation. This procedure applies to all contributors,
AI or human — see [Use of AI](#use-of-ai) below.

---

## Use of AI

AI is allowed and encouraged on this project. DTFS is **primarily
authored by Claude Opus**. Any AI agent — and any human using one —
contributing code or documentation **must follow every rule defined
in this file**, including:

- The [Fixing Procedure](#fixing-procedure) above for every bug fix.
- The [plan-driven workflow](#1-plan-driven-workflow) (§ 1) for
  every non-trivial change.
- The [commit discipline](#3-commit-discipline) (§ 3), including
  the `Co-Authored-By:` trailer naming the model used.
- The [test discipline](#4-test-discipline) (§ 4).

If a tool's default behaviour conflicts with a rule in this file,
this file wins. If the user issues an instruction in the current
session that conflicts with a rule, the user wins — but flag the
conflict so AGENTS.md can be updated (see [§ 8](#8-updating-agentsmd))
rather than silently drifting. AI-generated commits that bypass the
fixing procedure or the plan-driven workflow will be reverted.

---

## Structure Discipline

Where a file lives is part of its meaning. The repository layout
encodes who reads each artefact, when, and against which acceptance
bar. Drift on this is corrosive — it is why work in flight ends up
indistinguishable from a fix landed six months ago. These rules are
mandatory:

| Artefact | Canonical location | Notes |
|---|---|---|
| In-flight work plan | `plans/active/PLAN-<id>-<name>.md` | One per non-trivial change. See [§ 1](#1-plan-driven-workflow). |
| Finished plan | `plans/completed/<YYYY-MM-DD>-PLAN-<id>-<name>.md` | `git mv` when every task is ✅ verified. |
| Post-mortem (SOAK-RECOVERY, INTEGRITY-AUDIT, …) | `plans/incidents/` | Write-once; documents what we did and why. |
| Lab campaign / fix-report artefact | `plans/reports/` | Historical record only — no live state. |
| User-visible release note | `changelog/UNRELEASED.md` | One line per change, **same commit** as the code. See [`changelog/README.md`](./changelog/README.md). |
| Architecture / config / ops reference | `docs/src/...` (mdBook) | Update in the same commit as behaviour changes (CLAUDE.md § 11). |
| Code | `crates/<crate>/src/...` | New crates require a `Cargo.toml` workspace member entry. |
| Protobuf | `proto/<file>.proto` | Rebuild `dtfs-proto` after every edit. |
| Unit tests | inline `#[cfg(test)]` modules | Pure helpers, state-machine apply paths, aggregators. |
| Integration / shell tests | `tests/integration/`, `tests/proxmox/abuse/NN-name.sh` | Cluster-level behaviour, numbered. |
| Test inventories / provisioning scripts | `tests/proxmox/{ansible,soak,*.json,*.sh,*.ps1}` | Test infrastructure. |
| Operator scripts | `scripts/` | Repeatable operator-facing tooling. |
| Deployment surface | `deploy/` | Docker Compose, sample configs, systemd units. |
| Open-issue tracker | `ISSUES_TO_FIX.md` (repo root) | Status of known gaps. Linked from CLAUDE.md. |
| Agent operating discipline | `AGENTS.md` (this file) | Updated alongside discipline changes (§ 8). |
| Codebase knowledge for agents | `CLAUDE.md` | Architecture, invariants, file map, bug catalog. |

What **does not** belong anywhere except these paths:

- A new plan placed under `tests/proxmox/PLAN-*.md` or similar.
  That layout is historical; all new plans go to `plans/active/`.
- A "Project status / PROGRESS" file checked in at the repo root.
  `git log`, the changelog, and the plans are the status sources.
  Historical snapshot reports live under `plans/reports/`.
- Loose `*.md` notes scattered through `crates/` or `tests/`. If
  the note describes work, it belongs in `plans/`; if it describes
  a user-visible change, in `changelog/`; if it documents a
  subsystem, in `docs/`.
- A new top-level directory invented to host an artefact that
  already has a home in the table above.

Edge cases:

- **A doc that is both a plan *and* a post-mortem** (a plan that
  morphed into a recovery write-up): keep it in `plans/active/`
  while the work is in flight, then move it to `plans/incidents/`
  on completion. Leave a one-line breadcrumb in `plans/completed/`
  if downstream readers expect to find it there.
- **A code comment that points to a plan or incident**: use the
  full `plans/active/...` or `plans/incidents/...` path. Update
  these in the same commit that moves the file (`git grep` is
  the tool of last resort, not first).
- **A new artefact type that doesn't fit the table**: update this
  table in the same commit that introduces the artefact. Adding a
  row is cheaper than letting a one-off live somewhere ambiguous.

When in doubt, prefer `plans/reports/` for historical artefacts and
`docs/` for forward-looking reference. Never invent a new top-level
directory without first updating this table.

---

## 1. Plan-driven workflow

Before writing code for a non-trivial change, **write a plan
document**. **All plans live under `plans/`** — this is the canonical
location and is mandatory for new plans:

- `plans/active/PLAN-<id>-<short-name>.md` — in-progress plans.
- `plans/completed/<completion-date>-PLAN-<id>-<short-name>.md` —
  finished plans, moved here once every task is ✅ verified.
- `plans/incidents/` — post-mortem write-ups (SOAK-RECOVERY,
  INTEGRITY-AUDIT style).
- `plans/reports/` — historical lab / fix-campaign reports.

See [`plans/README.md`](./plans/README.md) for the directory's
naming and lifecycle rules. Legacy plans previously under
`tests/proxmox/PLAN-*.md`, `tests/proxmox/SOAK-RECOVERY-*.md`,
`tests/proxmox/INTEGRITY-AUDIT-*.md`, `tests/proxmox/FIX-REPORT-*.md`,
etc. have been migrated into `plans/`; **do not put plans, reports,
or post-mortems under `tests/` again** — that tree is for test
infrastructure only (`abuse/`, `ansible/`, `soak/`, inventories,
provisioning scripts).

The plan **must** include:

* **Goals + non-goals** so scope is explicit.
* **Locked-in decisions** from the user (so back-and-forth in chat is
  captured durably).
* **Data-model changes** with concrete shapes — struct fields, proto
  messages, Raft variants, config keys.
* **Task list** with stable IDs (e.g. `<plan-id>-<phase>-<n>`). Each
  task lists:
  - **Files** to touch
  - **Implementation sketch** (not full code — just enough to commit
    to)
  - **Tests** required (unit names + live-verification check)
  - **Done when** — concrete acceptance criteria, not vague
  - **Status** marker (⬜ todo · 🟦 in progress · ✅ done · 🟥 blocked
    · 🟨 needs review)
* **Cross-cutting invariants** that every task must preserve.
* **Verification log** + **Deviations** sections — empty at plan
  authorship, filled in during work.

The plan **stays the source of truth**. When reality diverges from
the plan, update the plan first (Deviations section), then continue
the code. A merge conflict between plan and code is a red flag the
plan was not updated as work progressed — block-and-fix immediately.

---

## 2. Per-task procedure

For each ⬜ task in order:

1. Mark the task **🟦** in the plan.
2. **Implement** the smallest change that satisfies the listed tests
   + done-when. Do not add scope.
3. Run the relevant `cargo test -p <crate> --release` until green.
4. Sync to the build host (if remote), run the full crate test
   suite. No `--release` regressions allowed.
5. Deploy the affected binary to the verification environment
   (lab cluster, minicluster, integration host).
6. Run the **live verification** described in the task's "Done
   when" criteria.
7. Add a **verification log** entry to the plan: task ID, date,
   commands run, output snippet, anomalies.
8. **Commit** (see § 3).
9. Mark the task **✅** with the completion date in the plan.
10. Move to next ⬜.

Steps 7-9 land **in the same commit** as the code change. The doc
must never drift from the code — that's the entire reason the plan
exists.

---

## 3. Commit discipline

### Format

```
<plan-id>-<phase>-<n>: <one-line summary, <72 chars>>

<2-3 sentence body explaining what changed, what tests cover it, any
deviation from the plan sketch. Reference the relevant
section/recovery doc.>

Plan: <relative-path-to-plan> (<plan-id>-<phase>-<n> ✅)

Co-Authored-By: <model-name> <noreply@<vendor>.com>
```

The `Plan:` trailer is mandatory for any commit produced under this
discipline so `git log --grep "Plan:"` filters the agent-driven work.
The `Co-Authored-By:` trailer is mandatory per repo convention.

### Granularity

* **One commit per task.** No batching across tasks.
* **Doc-only tasks** (aggregated docs for a phase) get their own
  commit.
* **Co-locate** docs + code: when a code change has a corresponding
  doc update, both go in the **same** commit. The discrete doc
  tasks aggregate cross-phase doc edits; per-task doc snippets ride
  along with the code commit.

### Hygiene

* **Never** `--no-verify`. Pre-commit hooks (when present) must
  pass. If a hook fails, investigate and fix; don't bypass.
* **Never** `--amend` a commit that has been pushed. Create a new
  commit instead.
* **Never** push to `master`/`main` mid-plan. Keep work on local
  until the user reviews each phase boundary. Feature branches at
  phase boundaries (`av-phase-1`, `av-phase-2`) are OK; rebase /
  squash only if explicitly requested.
* **Never** silently drop the `Co-Authored-By` trailer or bypass
  GPG signing if the repo requires it.

---

## 4. Test discipline

Cross-references CLAUDE.md § 4 (Test-First Workflow). Same rules,
restated here for completeness:

1. **Write the failing test first**, in the purest layer that
   demonstrates the gap (unit > integration > shell).
2. **Run, observe failure**, quote the failure in the commit body.
3. **Implement the fix** as narrowly as possible.
4. **Run, observe pass.** Run the full crate's tests to catch
   regressions.
5. **Live-verify** on the deployed environment if the change is
   operational.

The plan **declares the tests up front**. Each task's "Done when"
requires those tests green AND the live verification step.

### What to test where

| Layer | Test home |
|---|---|
| Pure helper / state-machine apply | unit test in the same crate |
| Manager-side aggregator (against `StateMachineData`) | unit test in `dtfs-manager` |
| FUSE-side helper | unit test in `dtfs-fuse` |
| Cross-process / cluster behaviour | shell script under `tests/proxmox/abuse/NN-name.sh` |
| New durability/anchor invariants | property-style test exercising many random op sequences |

---

## 5. Plan-as-living-document

* When the implementation deviates from the plan (different test
  name, additional file touched, missed edge case): **update the
  plan first**, under the `Deviations from plan` section. Then
  continue.
* When a task's "Done when" criteria need to change mid-implementation
  (a missing dependency surfaces, an environment constraint forces a
  different approach), **update the plan, get user
  acknowledgement, then proceed**. Don't silently drop or expand a
  task.
* **Plan changes that materially alter the design** (new data field,
  new task, dropped task) require user review **before** the change
  is committed.

A plan that ends in a state matching reality is the artefact future
agents and operators rely on. A plan that diverged silently is
worse than no plan.

---

## 6. Worked example

For Bug AV (the in-flight feature at time of writing), the
discipline produces:

```
plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md  (the plan)
  ├─ § 0 Background, decisions
  ├─ § 1 Data-model schema
  ├─ § 2 Phase plan + 24 tasks (AV-1-1 .. AV-3-6)
  ├─ § 4 Verification log
  ├─ § 5 Deviations from plan
  └─ Appendix A — 7 worked TOML examples

Commits (mid-implementation, illustrative):
  AV-1-1: CacheTierConfig + AnchorPolicy types
  AV-1-2: VolumeConfig + FileMetadata extension
  ...
  AV-1-12: docs/schema + flush API
  AV-2-1: per-DC rolling read-window store
  ...
```

Every commit's `Plan:` trailer points back to the plan, every plan
task carries the matching commit's status flip + verification-log
entry. `git log --grep "AV-1-"` reconstructs the Phase 1 history in
order.

---

## 7. Cross-reference

* **[README.md](./README.md)** — project overview, deployment
  warning, contribution rules, AI-use policy. Points back here for
  the procedural details.
* **[CLAUDE.md](./CLAUDE.md)** — codebase-specific knowledge: target
  workload, architecture, file map, integrity invariants, memory
  model, bug catalog.
* **`plans/active/PLAN-<id>-*.md`** — feature-specific plan
  documents currently in flight. One per non-trivial work item.
* **`plans/completed/`** — finished plans, archived once every task
  is ✅ verified.
* **`plans/incidents/SOAK-RECOVERY-*.md`**, **`plans/incidents/INTEGRITY-AUDIT-*.md`** —
  incident-style write-ups after the fact (symptom → root cause →
  fix → verification). The plan covers *what we're about to do*;
  the incident doc covers *what we just did and why*.
* **`plans/reports/`** — historical lab-campaign and fix-report
  artefacts.
* **`changelog/UNRELEASED.md`** — accumulator for the next release.
  Add a one-line entry per user-visible change in the same commit;
  see [`changelog/README.md`](./changelog/README.md).
* **AGENTS.md** (this file) — the operating discipline that ties the
  above together.

All of these are read together by any AI agent starting work on a
non-trivial change. The user is the final arbiter when AGENTS.md
and the plan conflict; the plan is the arbiter when the plan and
the code conflict.

---

## 8. Updating AGENTS.md

This file evolves slowly. Update it when:

* A new discipline emerges (e.g. "always run `cargo clippy` before
  commit" — that's a § 3 hygiene change).
* A new doc surface gets added (e.g. an `INCIDENT-*.md` convention)
  — § 7 cross-reference grows.
* The plan template changes — § 1 / § 2 mirror the new shape.

Update AGENTS.md **in the same commit** as the change that motivated
it. PR description (or commit body) names the discipline change so
future agents can `git log AGENTS.md` to trace the rule's origin.
