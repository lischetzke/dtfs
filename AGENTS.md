# AGENTS.md — Operating Discipline for AI Agents on DTFS2

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

## 1. Plan-driven workflow

Before writing code for a non-trivial change, **write a plan
document**. Default location: `tests/proxmox/PLAN-<id>-<short-name>.md`
(matches the existing `Bug A`-prefixed-ID convention; can be relocated
when the bug catalog moves).

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
tests/proxmox/PLAN-BUG-AV-TIER-LIFECYCLE.md  (the plan)
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

* **[CLAUDE.md](./CLAUDE.md)** — codebase-specific knowledge: target
  workload, architecture, file map, integrity invariants, memory
  model, bug catalog.
* **`tests/proxmox/PLAN-<id>-*.md`** — feature-specific plan
  documents. One per non-trivial work item.
* **`tests/proxmox/SOAK-RECOVERY-*.md`** — incident-style write-ups
  after the fact (symptom → root cause → fix → verification). The
  plan covers *what we're about to do*; the recovery doc covers
  *what we just did and why*.
* **AGENTS.md** (this file) — the operating discipline that ties the
  above together.

All four are read together by any AI agent starting work on a
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
