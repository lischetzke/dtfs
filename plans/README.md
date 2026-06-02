# `plans/` — Work Plans

This directory is the **canonical home for all multi-step work planning** in DTFS.
Referenced from [AGENTS.md § B.2](../AGENTS.md#b2-plan-driven-workflow) and
mandatory for any non-trivial change.

## Layout

```
plans/
├── README.md            # this file
├── active/              # in-progress plans — one per non-trivial work item
├── completed/           # finished plans — git-tracked, agent-excluded
└── processed/           # processed reports, incidents, issue scans — git-tracked, agent-excluded
```

`completed/` and `processed/` are git-tracked (committed) so the history is shared,
but excluded from AI-agent context via deny rules in `.claude/settings.json`
(`Read(plans/completed/**)` / `Read(plans/processed/**)`). Agents do not read them;
humans and git history do.

## Naming

- **Active plans**: `plans/active/PLAN-<id>-<short-name>.md`
  - `<id>` follows the bug-letter convention (`BUG-AV`, `BUG-BL`, etc.).
  - `<short-name>` is kebab-case, 4 words or fewer.
  - Example: `plans/active/PLAN-BUG-BL-HEARTBEAT-NONLEADER-FORWARD.md`.
- **Completed plans**: `plans/completed/<YYYY-MM-DD>-PLAN-<id>-<short-name>.md`
  - Date prefix is the day every task reached ✅ and was verified.
  - Example: `plans/completed/2026-05-28-PLAN-BUG-BJ-REGISTER-AUDIT-SPAM.md`.
- **Processed material**: free-form; move with a date prefix if helpful.

## Lifecycle

1. **Author** a plan under `active/` before writing code. The plan must contain
   everything [AGENTS.md § B.2](../AGENTS.md#b2-plan-driven-workflow) requires:
   goals, non-goals, tasks with stable IDs, implementation sketches, tests,
   acceptance criteria, verification log, deviations section.
2. **Iterate** as work progresses. The plan is the source of truth; if reality
   diverges, update the Deviations section before continuing the code.
3. **Archive** when every task is ✅ verified: move the file from `active/` to
   `completed/<completion-date>-<original-name>`. The verification log and
   deviations stay with the plan.
4. **Process** reports and incidents: once all findings from a scan or post-mortem
   have been captured in active plans, move the source document to `processed/`.
   It is no longer needed by agents.

## Picking the next bug letter

Check `plans/active/` for the highest letter currently in use (e.g. `BN`).
The next bug is `BO`, then `BP`, and so on.

## What does **not** belong here

- **Per-task TODO lists / scratch notes** — keep those in the session.
- **Code or scripts** — those go in `crates/`, `tests/`, `scripts/`.
- **User-visible release notes** — those go in
  [`changelog/`](../changelog/README.md).
