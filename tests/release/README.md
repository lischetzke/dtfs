# `tests/release/` — release-gate contract

`tests/release/run.sh` is the single artefact the operator runs before a
release tag. Anything not registered here isn't release-gated. See
[AGENTS.md § B.4](../../AGENTS.md#b4-tests).

## When a case is required

Only when a lab cluster exists to run it. As of 2026-09 there is none
(AGENTS.md § A.4), so no commit is required to ship a case and no hook
enforces it; the existing cases stay as the release gate for the day a
cluster comes back. When authoring one anyway, follow the rules below.

## Writing a case

- One case per scenario; number + name for the behaviour
  (`NN-short-name.sh`). Pick the next free number — check
  `ls tests/release/cases/` for the highest in use (duplicates warn
  loudly in `run.sh` but still run; don't create one).
- `source tests/release/lib/common.sh` for the shared helpers
  (`mgr_get_any`, `mgr_post_leader`, `wait_for`, `wait_for_soft`,
  `assert_eq`/`assert_match`/`assert_ge`, chunk/host helpers, HMAC
  stamping for LIS-19/55/56/83).
- `trap case_cleanup EXIT` and define `case_cleanup`; the suite must
  continue to the next case even if this one fails.
- No operator interaction. Poll with `wait_for`/`wait_for_soft`, never a
  fixed sleep-and-hope.
- Last line of stdout MUST be exactly one of:
  `STATUS: PASS` / `STATUS: FAIL` / `STATUS: SKIP` /
  `STATUS: KNOWN_ISSUE [<bug-id>]` — use the `pass`/`fail`/`skip`/
  `known_issue` helpers in `lib/common.sh`, don't hand-roll it.
- `KNOWN_ISSUE` pins to a bug id; the case auto-promotes to a real
  PASS/FAIL once that bug closes and the case is re-checked.
- Cheap critical-path cases get added to `QUICK_CASES` in `run.sh` (only
  when asked — most cases stay full-suite-only).

## Running

```bash
bash tests/release/run.sh            # full suite (~60-90 min)
bash tests/release/run.sh --quick     # QUICK_CASES subset (~15 min)
bash tests/release/run.sh --chaos     # CHAOS_CASES subset (3dc lab)
```

Reports land in `target/release-reports/RELEASE-<run-id>/` (untracked) (per-case log +
`summary.json`).
