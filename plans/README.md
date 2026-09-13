# `plans/` — Work Plans

Optional. Default planning tool is the session todo list. Write a plan
file only for work that must survive across sessions (see
[AGENTS.md § B.2](../AGENTS.md#b2-plans)).

## Layout

```
plans/
├── README.md   # this file
└── active/     # in-progress plans, one per multi-session work item
```

## Shape

`plans/active/PLAN-<short-name>.md` (kebab-case, 4 words or fewer):

```markdown
# <title>
Goal: one paragraph.
Non-goals: bullets.
Decisions already made: bullets.
Tasks:
- [ ] task
- [x] done task
Open questions: bullets.
```

No task IDs, no status columns, no verification log, no deviations
section. Tick tasks as they land; delete the file when everything is
ticked. Git history is the archive — nothing is copied to a reports
directory.
