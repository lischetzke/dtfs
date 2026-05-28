# Release testing (Bug BE)

The release-gate suite at `tests/release/` is the single artefact
the operator runs prior to a release tag. Anything not in it is
not gated by release.

## Run

```
bash tests/release/run.sh            # full suite (~60-90 min)
bash tests/release/run.sh --quick    # ~15 min subset
```

Required env (defaults match the lab minicluster):

| Var | Default | Notes |
|---|---|---|
| `MGRS` | `mini-mgr01,mini-mgr02,mini-mgr03` | csv |
| `DATA` | `mini-data01,mini-data02` | csv |
| `CLIENT` | `mini-client01` | host |
| `MOUNT` | `/mnt/dtfs` | FUSE mount on CLIENT |
| `VOL` | `default` | volume id |
| `MGR_PORT` | `9402` | manager API |
| `SSH_KEY` | `~/.ssh/dtfs-lab/id_ed25519` | private key |

## Output

Per run, `plans/reports/RELEASE-<id>/`:

- `<case>.log` — per-case stdout
- `summary.json` — counts + per-case status (PASS / FAIL / SKIP /
  KNOWN_ISSUE[bug-id])

Exit code is 0 iff zero FAILs. KNOWN_ISSUE rows do not fail the
suite — they pin to a Bug ID and flip green automatically once
that bug closes.

## Adding a case

1. Add `tests/release/cases/NN-name.sh`.
2. Source `tests/release/lib/common.sh` at the top.
3. Register `trap case_cleanup EXIT` and define `case_cleanup`.
4. Last line of stdout MUST be `STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug>]`.
5. Use `pass / fail / skip / known_issue <bug>` helpers from
   `lib/common.sh`.
6. If the case is cheap (≤ 2 min) and a critical path, append its
   stem to `QUICK_CASES` in `run.sh`.

Required per AGENTS.md § B.5.1: every test that exercises
cluster-visible behaviour SHIPS a release-suite case in the same
commit as the unit/abuse test. KNOWN_ISSUE is fine when the
scenario depends on un-shipped functionality.

## Current case list

| # | Case | What it verifies | Quick |
|---|---|---|---|
| 00 | prereqs | ssh + mgr API + 6 nodes + FUSE mount | yes |
| 01 | basic-write-read | 4 MiB sha256 roundtrip | yes |
| 02 | replica-placement | mandatory replica count converges | yes |
| 03 | scrub-quick | manual quick scrub trigger | — |
| 04 | scrub-deep | manual deep scrub trigger | — |
| 05 | migration | operator migrate path + sha holds | yes |
| 06 | flush-cache | mandatory preserved + audit emitted | yes |
| 07 | set-schedules | endpoint shape + deprecated-scrub-warn | — |
| 08 | set-anchor | AnchorPolicy applies | — |
| 09 | set-mandatory | RF change | — |
| 10 | leader-rotation | new leader elected ≤30s | — |
| 11 | data-node-restart | reads fall over to surviving replica | — |
| 12 | fuse-restart | mount returns + reads resume | — |
| 13 | integrity-corruption | corrupt → fallback → quarantine flow | yes |
| 14 | orphan-reap | unlink + orphan_scrub reaps after grace | — |
| 15 | soak-15min | condensed mixed workload | yes |
| 16 | version-report | every node reports version + build_date | — |
| 17 | stats-correctness | logical ≤ net ≤ raw; tier sums | yes |
| 18 | api-shape | endpoint schema keys present | yes |
| 19 | config-effective | scope_kind on every row; scrub keys present | — |
| 20 | quarantine-flow | unquarantine endpoint validation | — |
