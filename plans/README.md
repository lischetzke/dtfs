# `plans/` — Work Plans, Incidents, Reports

This directory is the **canonical home for all multi-step work
planning, post-mortems, and historical reports** in DTFS. It is
referenced from [AGENTS.md § 1](../AGENTS.md#1-plan-driven-workflow)
and is mandatory for any non-trivial change (see AGENTS.md for what
counts as non-trivial).

## Layout

```
plans/
├── README.md            # this file
├── active/              # in-progress plans — one per non-trivial work item
├── completed/           # finished plans, archived once every task is ✅
├── incidents/           # post-mortems (symptom → root cause → fix → verify)
└── reports/             # historical lab-campaign + fix-report artefacts
```

## Naming

- **Active plans**: `plans/active/PLAN-<id>-<short-name>.md`
  - `<id>` matches the bug-letter or feature-letter convention
    (e.g. `BUG-AV`, `FEAT-TIER-LIFECYCLE`).
  - `<short-name>` is kebab-case, ≤ 4 words.
  - Example: `plans/active/PLAN-BUG-AV-TIER-LIFECYCLE.md`.
- **Completed plans**: `plans/completed/<YYYY-MM-DD>-PLAN-<id>-<short-name>.md`
  - Date prefix is the day every task in the plan reached ✅ +
    verified.
  - Example: `plans/completed/2026-06-01-PLAN-BUG-AV-TIER-LIFECYCLE.md`.
- **Incidents**: `plans/incidents/SOAK-RECOVERY-<YYYY-MM-DD>.md`,
  `plans/incidents/INTEGRITY-AUDIT-<YYYY-MM-DD>.md`, and similar.
- **Reports**: free-form, date- or topic-prefixed
  (`FIX-REPORT-<id>.md`, `LAB-REPORT-<YYYY-MM-DD>.md`,
  `MINICLUSTER-VERIFICATION-<YYYY-MM-DD>.md`).

## Lifecycle

1. **Author** a plan under `active/` before writing code. The plan
   must contain everything [AGENTS.md § 1](../AGENTS.md#1-plan-driven-workflow)
   requires (goals, non-goals, tasks with stable IDs, tests,
   acceptance criteria, verification log, deviations section).
2. **Iterate** on the plan as work progresses. The plan is the
   source of truth; if reality diverges, update the plan
   (Deviations section) before continuing the code.
3. **Archive** when every task is ✅ verified: `git mv` the file
   from `active/` to `completed/<completion-date>-<original-name>`.
   The verification log + deviations stay with the plan.
4. **Write up** any incident or campaign that happened during the
   plan in `incidents/` (post-mortem) or `reports/` (campaign
   artefact). These are write-once references for future agents.

## What does **not** belong here

- **Per-task TODO lists / scratch notes** — keep those in the
  session, not in version control.
- **Code or scripts** — those go in `crates/`, `tests/`, `scripts/`.
- **User-visible release notes** — those go in
  [`changelog/`](../changelog/README.md). A plan documents *how* a
  change was made; the changelog documents *what changed* from a
  user's perspective.

## Migration of legacy plans

Plans, post-mortems, and historical reports used to live under
`tests/proxmox/`. They have been migrated here:

- `tests/proxmox/PLAN-BUG-AV-TIER-LIFECYCLE.md` →
  [`active/PLAN-BUG-AV-TIER-LIFECYCLE.md`](active/PLAN-BUG-AV-TIER-LIFECYCLE.md)
- `tests/proxmox/SOAK-RECOVERY-*.md`,
  `tests/proxmox/INTEGRITY-AUDIT-*.md` → `incidents/`
- `tests/proxmox/FIX-REPORT-*.md`, `LAB-REPORT-*.md`,
  `MINICLUSTER-VERIFICATION-*.md`, `REPORT-*.md`,
  `SOAK-LAUNCHED-*.md`, `FINDINGS.md` → `reports/`
- `PROGRESS.md` (repo root, March snapshot) →
  [`reports/PROGRESS-2026-03.md`](reports/PROGRESS-2026-03.md)

Do **not** create new plans under `tests/proxmox/`. The `tests/`
tree is for test infrastructure only — see
[`AGENTS.md` § Structure Discipline](../AGENTS.md#structure-discipline).
