# `plans/` — Work Plans

Canonical home for multi-step work planning in DTFS. See
[AGENTS.md § B.2](../AGENTS.md#b2-plans). Write a plan file when the work
has **≥ 3 tasks, spans sessions, or crosses crates**; a single fix uses
the session's todo list instead — the file costs more tokens than it
saves for one task.

## Layout

```
plans/
├── README.md   # this file
└── active/     # in-progress plans, one per non-trivial work item
```

## Naming

`plans/active/PLAN-<id>-<short-name>.md`. `<id>` follows the bug-letter
convention (`BUG-AV`, `BUG-BL`, …); `<short-name>` is kebab-case, 4 words
or fewer. Example: `plans/active/PLAN-BUG-BL-HEARTBEAT-NONLEADER-FORWARD.md`.

## Lifecycle

1. **Author** under `active/` before writing code: goals, non-goals,
   data-model shapes, tasks with stable IDs (`<plan-id>-<phase>-<n>`,
   each with files/sketch/tests/done-when/status), cross-cutting
   invariants, and empty Verification log + Deviations sections.
2. **Iterate** as work proceeds. The plan is the source of truth; if
   reality diverges, update the Deviations section before continuing.
   Verification log entries are one line per task (command + result),
   not a paragraph — the failing→passing test output belongs in the
   commit body instead (see AGENTS.md § B.3/B.4).
3. **Delete** the plan file once every task is ✅ and merged. Git history
   keeps it — nothing is archived.

## Picking the next bug letter

The highest letter currently in `plans/active/`, plus one.

## What does not belong here

Scratch notes → keep in the session. Code/scripts → `crates/`, `tests/`,
`scripts/`. User-visible release notes → the PR title/description
(GitHub auto-generates release notes from merged PRs).
