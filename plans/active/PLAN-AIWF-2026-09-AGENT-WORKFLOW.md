# PLAN-AIWF-2026-09 — agent workflow + token efficiency

Single-developer project. Part B of AGENTS.md was written for a team-shaped
process (branch → PR → plan file → verification log → release case) and is now
the main per-session token cost and the main friction. This plan replaces it
with a leaner contract, moves the removed prose into project-local agents,
path-scoped rules, and deterministic hooks, and defines the agent roster.

## Goals

- Cut always-loaded context. AGENTS.md is **991 lines (~14k tokens) loaded at
  every session start**, ~150 of them Part B. Target: ≤ 250 lines total,
  Part B ≤ 55 lines (≈37% of current Part B).
- Drop branch/PR ceremony for work that does not need it; keep a precise,
  testable threshold for when it is still required.
- Ship `.claude/agents/*.md` for the task kinds this project actually has, so
  file-heavy stages burn a subagent's context instead of the main session's.
- Replace prose-enforced rules with hooks where a script can decide.

## Non-goals

- **No change to Part A.** Every technical invariant (anchor, integrity,
  memory ladder, snapshot compat) stays exactly as written; only its *storage
  location* may change (AGENTS.md vs path-scoped rule file vs docs/).
- No change to what the operator runs for a release (`tests/release/run.sh`,
  `scripts/dtfs-release.sh`).
- No new runtime dependency in the shipped binaries.

---

## 1. Research summary

| # | Source | URL | Takeaway | Adopted |
|---|---|---|---|---|
| 1 | Best practices for Claude Code (Anthropic) | https://code.claude.com/docs/en/best-practices | "Bloated CLAUDE.md files cause Claude to ignore your actual instructions." Per line: "Would removing this cause Claude to make mistakes?" Explore→plan→code→commit; skip the plan when "you could describe the diff in one sentence". Subagents for investigation + adversarial review. "If Claude already does something correctly without the instruction, delete it or convert it to a hook." | Yes — drives §3, §4, §6, §7 |
| 2 | Create custom subagents | https://code.claude.com/docs/en/sub-agents | Frontmatter `name`+`description` required; optional `tools`, `disallowedTools`, `model`, `permissionMode`, `maxTurns`, `skills`, `isolation`, `effort`. Each subagent "runs in its own context window… fresh, isolated"; `description` drives auto-delegation; project agents beat user beat plugin; omitted `tools` inherits everything. | Yes — §4 |
| 3 | Memory / CLAUDE.md | https://code.claude.com/docs/en/memory | Target < 200 lines per memory file; `@path` imports expand inline (they do **not** save context) with a 4-hop depth cap; `.claude/rules/*.md` exist and support `paths:` frontmatter globs so a rule loads only for matching files. | Yes — §4, §7 |
| 4 | Hooks reference | https://code.claude.com/docs/en/hooks | Events incl. `PreToolUse`, `PostToolUse`, `Stop`, `SubagentStop`, `SessionStart`, `PreCompact`. Shape `{"hooks":{"<Event>":[{"matcher":"…","hooks":[{"type":"command","command":"…"}]}]}}`. PreToolUse blocks via **exit code 2** (stderr = reason) or JSON `hookSpecificOutput.permissionDecision: "deny"`. | Yes — §6 |
| 5 | Model configuration | https://code.claude.com/docs/en/model-config | Subagent `model` accepts `sonnet`/`opus`/`haiku`/`fable`/`best`, a full model id, or `inherit`; resolution = invocation override → agent field → `CLAUDE_CODE_SUBAGENT_MODEL` → session model. | Yes — model tiering in §5 |
| 6 | Skills | https://code.claude.com/docs/en/skills | Skills = reusable procedures, lazily loaded; subagents = isolated context; built-in commands = fixed logic. "If you're creating automation and not writing a command that's built-in, use a skill." | Yes — B.2 plan template becomes agent prompt, not AGENTS.md prose |
| 7 | Effective context engineering for AI agents | https://www.anthropic.com/engineering/effective-context-engineering-for-ai-agents | Context is a finite budget ("context rot"); the three levers are compaction, structured note-taking, and sub-agent architectures. | Yes — §7 |
| 8 | How we built our multi-agent research system | https://www.anthropic.com/engineering/multi-agent-research-system | Verified quotes: "agents typically use about 4× more tokens than chat interactions, and multi-agent systems use about 15× more tokens than chats"; multi-agent Opus-lead + Sonnet-workers "outperformed single-agent Claude Opus 4 by 90.2% on our internal research eval". Orchestrator-worker. | Yes — §5 orchestration rules; the 15× is why the roster is sequential, not fan-out |
| 9 | Building effective agents | https://www.anthropic.com/engineering/building-effective-agents | Five patterns (prompt chaining, routing, parallelization, orchestrator-workers, evaluator-optimizer); "build the *right* system", reduce abstraction layers. | Yes — roster is prompt-chaining + one evaluator-optimizer loop (reviewer→implementer) |
| 10 | Writing effective tools for agents | https://www.anthropic.com/engineering/writing-tools-for-agents | Tool/response design drives agent token cost; prototype → evaluate → optimize. | Partial — informs agent output caps (§5) |
| 11 | Code execution with MCP | https://www.anthropic.com/engineering/code-execution-with-mcp | Presenting bulk data through code instead of raw tool output cut one agent's usage 150k → 2k tokens. | Partial — justifies graphify/ripgrep-summary over raw file dumps (§7) |
| 12 | Prompt caching | https://platform.claude.com/docs/en/build-with-claude/prompt-caching | Stable system-prompt prefixes are cacheable; editing the always-loaded file invalidates the prefix for that session. | Partial — a *stable* AGENTS.md is cached, so size matters most on cache-miss/first turn |

**Verification status.** Sources 1, 2, 3, 4, 5, 6, 8, 9 were fetched and quoted
directly. Sources 7, 10, 11, 12 are URL-confirmed but summarised from search
results / a subagent's partial fetch — treat their figures as indicative.
Confirm the `.claude/rules/` `paths:` frontmatter (source 3) against a live
`/context` before task AIWF-1-2 relies on it; it is the one load-bearing
mechanism claim not re-verified by hand.

---

## 2. Locked-in decisions (recommendations — user to confirm)

**D1 — Default landing is a direct commit to `master`.** Rationale: one
developer, no review queue; a branch only buys value when the change can break
a running cluster mid-merge or needs a release-note line.

**D2 — Branch required only above this threshold** (any one of):
- touches `proto/*.proto`, or `dtfs-raft/src/lib.rs::ClientRequest`, or
  `state_machine.rs::apply_request`, or any `#[serde]`-persisted struct
  (`StateMachineData`, `ChunkMeta`, WAL records) — i.e. a wire/on-disk format;
- touches > 6 files or spans more than one session;
- a multi-task plan exists for it (D4).
Rationale: those are exactly the changes where a half-landed `master` is
deployable-but-wrong. Everything else is cheaper to revert than to branch.

**D3 — PRs only when the change must appear in release notes.** `.github/
release.yml` groups by PR label, so no-PR = no note. Replacement, recommended:
make the **commit subject** the note source — `bug:` / `feat:` / `int:` prefix
on every subject (already the de-facto convention: see `bug: bound BR-1 …`,
`int: give every release case a unique number`) and generate notes from the
commit range with `git-cliff` (`.github/cliff.toml` reproducing the existing
sections; `scripts/dtfs-release.sh` writes `notes.md` and passes
`gh release create --notes-file`). Evaluated and rejected: `gh release create
--notes-from-tag` (requires hand-writing the annotated tag body — same work,
no grouping); keeping `--generate-notes` only (silently drops every
direct-to-master change from the notes, which is the whole point of D1).
Interim: until `git-cliff` lands, keep a PR for user-visible `bug:`/`feat:`
work only.

**D4 — Plan files for multi-task work only.** Write
`plans/active/PLAN-<id>-<name>.md` when the work has ≥ 3 tasks *or* spans
sessions *or* crosses crates. A single fix uses the in-session todo list and no
file. Rationale: the plan's real value is cross-session state; for a one-task
fix the file costs more tokens than it saves.

**D5 — The verification-log ritual is dropped.** What replaces it: the
failing→passing test output is quoted in the **commit body** (already the
practice) and, for plan-driven work, one line per task in the plan's
Verification log — not a paragraph. Rationale: git is the log; the plan should
carry only what git cannot (deviation reasoning).

**D6 — Release-case registration stays a rule, enforced by a hook, not prose.**
A `PreToolUse` hook on `git commit` blocks when the staged diff touches
cluster-visible code and adds no `tests/release/cases/NN-*.sh`; the escape
hatch is an explicit `Release-case: none (<reason>)` commit trailer. Rationale:
source 1 — convert instructions Claude already follows into hooks and delete
the prose.

**D7 — Trailers: keep `Co-Authored-By:`, drop `Plan:`.** The `Plan:` trailer
only exists to make `git log --grep "Plan:"` filter agent work; with D1 that
filter matches almost everything and stops being informative. Keep a
`Plan: <path> (<task-id>)` line **only when a plan file exists**, which is the
case the grep actually wants.

**D8 — Agents are project-local and hand-rolled, not a wrapper around the
`leduc:*` pipeline.** The `leduc:*` agents (feature-lead → intake → triage →
plan → design → implement → review → security-review) are not on disk in
`~/.claude`; they arrive from a managed plugin, so this repo cannot version or
edit their prompts, and they know nothing about A.3/A.6, the anchor invariant,
the Docker buildenv, or the release-case contract. Use `leduc:feature` ad hoc
for generic GitHub-issue work; the DTFS-specific stages get their own files.
Same for `superpowers` (its `writing-plans` / `test-driven-development` /
`subagent-driven-development` skills overlap B.1-B.3 but are generic — do not
duplicate them in the agent prompts; reference them only if invoked).

---

## 3. AGENTS.md Part B — replacement text

Replaces B.1-B.7 verbatim (≈150 lines → 53 lines).

```markdown
# Part B — Operating Discipline

## B.1 How a change lands

1. Reproduce / locate. 2. Write the failing test in the purest layer that
shows it (pure helper > state-machine apply > manager aggregator > FUSE
helper > shell case). 3. Smallest fix that turns it green, no bundled
cleanup. 4. `cargo test -p <crate> --release` in the build env, plus the
full workspace suite before anything is deployed. 5. Commit.

Default: commit straight to `master`. Use a branch only if the change
touches a wire or on-disk format (`proto/*.proto`, `ClientRequest`,
`apply_request`, any `#[serde]`-persisted struct), touches > 6 files, or
spans more than one session. Open a PR only when the change needs a
release-note line (`bug:` / `feat:` subjects; label it).

## B.2 Plans

Write `plans/active/PLAN-<id>-<short-name>.md` when the work has ≥ 3
tasks, spans sessions, or crosses crates — otherwise use the session todo
list. Template and rules: `plans/README.md`. The plan is the source of
truth; when reality diverges, record it under Deviations before
continuing. Delete the file when every task is done — git keeps it.

## B.3 Commits

```
<type or task-id>: <summary ≤72 chars>

What changed, which test covers it, the failing→passing output, any
deviation from the plan.

Plan: <path> (<task-id>)        # only when a plan file exists
Co-Authored-By: <model> <noreply@<vendor>.com>
```

`<type>` is `bug` / `feat` / `int` and becomes the release-note line.
One commit per task; doc updates ride with the code. Never `--no-verify`,
never `--amend` a pushed commit, never bypass signing.

## B.4 Tests

Observe fail → implement → observe pass → full crate suite. New
durability/anchor invariants get a property-style test over random op
sequences. Every change with cluster-visible behaviour also ships a case
under `tests/release/cases/NN-name.sh` in the same commit (rules:
`tests/release/README.md`); a pure internal refactor may skip it with a
`Release-case: none (<reason>)` trailer. A commit hook enforces this.

## B.5 Where things live

`plans/active/` in-flight plans · `plans/reports/` scans and reviews ·
`docs/src/` operator + architecture reference · `crates/<crate>/src/`
code · `proto/` wire · `tests/release/cases/` release gate ·
`tests/proxmox/abuse/` lab tests · `scripts/` operator scripts ·
`deploy/examples/*.toml` canonical configs · `.claude/agents/` agent
roster · `.claude/rules/` path-scoped invariants. No root status files,
no loose `.md` under `crates/` or `tests/`, no in-repo changelog.

Config-surface sync: any change to `dtfs-common/src/config.rs` updates
`deploy/examples/*.toml` and `docs/src/configuration/*` in the same
commit (`example_*_toml_parses` gates the first).

## B.6 Updating this file

Behaviour change visible to an operator, an agent, or the data plane →
update AGENTS.md in the same commit, and say "AGENTS.md updated" or "no
change needed because …" in the body. If a session instruction overrode a
rule here, fold the resolution in.
```

**What moves out of the always-loaded file:**

| Removed prose | New home |
|---|---|
| B.2 plan template (goals/decisions/data-model/tasks/…) | `plans/README.md` + `.claude/agents/dtfs-planner.md` |
| B.3 per-task loop | `.claude/agents/dtfs-implementer.md` |
| B.5 release-case rules (STATUS line, `trap case_cleanup`, QUICK_CASES) | new `tests/release/README.md` + `.claude/agents/dtfs-release-case-author.md` + the D6 hook |
| B.6 artefact table long form + PR label taxonomy | condensed above; label taxonomy → `.github/release.yml` comments |
| A.6 integrity checklist (Part A, 10 items) | `.claude/rules/integrity.md`, `paths:` scoped to `crates/dtfs-data/**`, `crates/dtfs-manager/src/scheduler.rs`, `crates/dtfs-raft/**` |
| A.2/A.8 bug-archaeology detail (CO-/REV-/WI- narratives) | `docs/src/internals/invariants.md`, one pointer line left in A.8 |

**Placement rule.** `AGENTS.md` = what a non-Claude tool (Codex, Cursor) also
needs, kept ≤ 250 lines. `.claude/rules/*.md` = Claude-only, path-scoped
invariants that must not cost tokens on an unrelated session. `.claude/
CLAUDE.md` = pointers only (graphify, agent roster index); never content —
`@`-imports expand inline and save nothing (source 3).

---

## 4. Agent roster (`.claude/agents/`)

Shared rules for every file below: `tools` is an explicit allow-list;
`description` is the routing trigger and names the artefact it consumes and
produces; every prompt ends with "output ≤ N lines, no preamble, no praise";
no agent reads a whole large file (`scheduler.rs` ~4k lines, `metadata.rs`,
`filesystem.rs`) — they read cited regions via `offset`/`limit` or grep.

| File | model | tools | isolation |
|---|---|---|---|
| `dtfs-explore.md` | haiku | Read, Grep, Glob | no |
| `dtfs-intake.md` | sonnet | Read, Grep, Glob, Bash(git log/show) | no |
| `dtfs-planner.md` | opus | Read, Grep, Glob, Write, Agent(dtfs-explore) | no |
| `dtfs-implementer.md` | sonnet (opus for wire/Raft work) | Read, Edit, Write, Grep, Glob, Bash | no (worktree only if the main tree is dirty) |
| `dtfs-reviewer.md` | opus | Read, Grep, Glob, Bash(git diff/show) | no |
| `dtfs-integrity-auditor.md` | opus | Read, Grep, Bash(git diff) | no |
| `dtfs-release-case-author.md` | sonnet | Read, Write, Grep, Glob, Bash(bash -n) | no |

**dtfs-explore** — `description`: "Read-only locator for DTFS. Finds where a
symbol, config key, Raft variant, or invariant lives and returns a file:line
table. Use before any planning or implementation instead of grepping from the
main session. Never proposes fixes." Loads nothing from AGENTS.md. Output: ≤ 25
lines, `path:line — one-clause note`. Budget: reads excerpts only; the whole
point is that its 30 file reads never enter the main context.

**dtfs-intake** — `description`: "Turns a raw DTFS request, bug report, or
cluster symptom into a structured spec: observed behaviour, expected behaviour,
suspected subsystem, blast radius, and the open questions that must be answered
before planning. Pure extraction — never proposes a fix." Loads A.1 (workload)
+ A.2 (architecture) only. Output: fixed sections `## Symptom / ## Expected /
## Subsystem / ## Blast radius / ## Open questions (numbered) / ## Size
(1-task | multi-task)`; ≤ 60 lines. The Size field is what routes D4.

**dtfs-planner** — `description`: "Writes a DTFS plan file for multi-task work:
goals, non-goals, data-model shapes, tasks with stable IDs, tests, done-when.
Use when intake reports multi-task, when work crosses crates, or when the user
asks for a plan." Loads A.3 (conventions), A.6, plans/README.md, and one
existing plan as a shape reference. Must call `dtfs-explore` rather than
reading source itself. Output: the plan file + a ≤ 10-line summary. Must NOT
write code. Prompt outline: (1) read the intake spec; (2) dispatch explore for
every unknown file location; (3) decide branch-vs-master against the D2
threshold and state it; (4) for each task give files, sketch, required test in
the purest layer, done-when, status ⬜; (5) flag every task that touches a wire
or persisted format with the A.3.1 `#[serde(default)]` + regression-test
requirement; (6) list cross-cutting invariants from A.3.4/A.6 that the tasks
must not break; (7) stop — do not implement.

**dtfs-implementer** — `description`: "Implements one DTFS plan task or one
small fix end to end: failing test first, smallest fix, crate suite green in
the Docker buildenv, commit. Use after a plan exists or for a scoped one-task
fix." Loads A.3, the build block below, and its single task. Build command it
must use verbatim (from PLAN-CR; `dtfs-builder` at 10.150.0.150 is currently
unreachable):
```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "C:/Users/leduc/git_github.com/dtfs2:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release 2>&1 | tail -30
```
Hard rules in the prompt: no `bash -lc` inside the image; `--release` always;
source files are CRLF — use Edit, never `sed -i`; rustc 1.94 ICEs on some
struct-literal edits in dtfs-manager/dtfs-data (reformulate, never remove
`#![allow(dead_code)]`); run `cargo test -p dtfs-common --release` after any
config/example change. Output: ≤ 15 lines — files changed, test names added,
before→after pass counts, commit SHA. Must NOT: touch a second task, refactor
beyond the fix, or write the plan's Deviations section without saying why.

**dtfs-reviewer** — `description`: "Reviews a DTFS diff or crate against the
Part A invariants and reports numbered findings with file:line evidence and a
severity. Use before merging wire/Raft/scheduler work, or on request." Loads
A.3 + A.6 + the `.claude/rules/integrity.md` rule. Output format copied from
`plans/reports/CORE-REVIEW-2026-09-12.md`: `### <CRATE>-<n> — <one line>
(HIGH|MEDIUM|LOW)` then evidence line, impact line, fix line. Severity ladder
is that report's: HIGH = data loss / corruption / invariant violation / outage
/ OOM / non-terminating repair / auth bypass. Must re-read every claimed line
before reporting it (the CORE-REVIEW method) and must report "no finding"
rather than manufacture one — source 1's over-engineering warning.

**dtfs-integrity-auditor** — `description`: "Runs the AGENTS.md A.6
data-integrity checklist over a diff that touches a write, migrate, delete, or
scrub path, and states Verified / Violated / N-A per item with evidence. Use
whenever a diff touches dtfs-data store, the scheduler's delocate paths, or the
orphan reaper." Loads A.3.3, A.3.4, A.6 only. Output: a 10-row table (one row
per A.6 item) + a findings list; ≤ 40 lines. Narrower than dtfs-reviewer on
purpose: it is a checklist, so it is cheap and can run on every such diff.

**dtfs-release-case-author** — `description`: "Writes a
`tests/release/cases/NN-name.sh` for a cluster-visible behaviour change, in the
house style, `bash -n` clean. Use when a commit adds cluster-visible behaviour
and no case exists yet." Loads `tests/release/README.md` + two neighbouring
cases. Rules: next free number (currently 100 cases, `95`-`99` used; continue
from `100-`), `trap case_cleanup EXIT`, `wait_for` from `lib/common.sh`, last
stdout line `STATUS: PASS|FAIL|SKIP|KNOWN_ISSUE [<bug-id>]`, no operator
interaction, do not add to `QUICK_CASES` unless asked. Output: the file + one
line naming it.

**Deferred (add only when the trigger returns):** `dtfs-lab-verifier` (SSH to
the Proxmox lab / dtfs-lm, run the case, report) — the lab and the builder are
both unreachable today, so the agent would rot; the existing
`dtfs-proactive-issue-finder` skill already covers read-only cluster
inspection. `dtfs-config-surface-checker` — a hook does this deterministically
and cheaper (§5).

**Orchestration.** The main session is the orchestrator and does only: read the
user's request, pick the chain, hand off, apply the user's decisions. Chain for
a bug: `dtfs-intake` → (multi-task? `dtfs-planner` → user approval) →
`dtfs-implementer` per task → `dtfs-reviewer` (+ `dtfs-integrity-auditor` if
the diff touches A.6 paths) → implementer fix round (max 2) →
`dtfs-release-case-author` if the hook demands one. Sequential, not parallel:
source 8 measures multi-agent at ~15× chat token cost, which only pays when
subagents explore *independent* branches — these stages are dependent. The
orchestrator must **not** read source files itself (that is `dtfs-explore`'s
job), must not run builds itself (the implementer's), and must not re-derive a
subagent's findings. `caveman` / `cavecrew` stay opt-in: use `/caveman` for
long interactive debugging sessions where the main thread is the bottleneck;
do **not** bake caveman compression into the agent prompts above — their
outputs are already capped and a compressed integrity finding is harder to
audit than a plain one.

---

## 5. Hooks (`.claude/settings.json`)

Only the three with a clear payoff. `.claude/settings.json` does not exist yet
(only `settings.local.json`), so this creates it, checked in.

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Bash",
        "hooks": [
          { "type": "command", "command": "bash .claude/hooks/guard-git.sh" },
          { "type": "command", "command": "bash .claude/hooks/check-release-case.sh" }
        ]
      }
    ],
    "PostToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          { "type": "command", "command": "bash .claude/hooks/rustfmt-check.sh" }
        ]
      }
    ]
  }
}
```

- `guard-git.sh` — reads the tool input from stdin; if the command matches
  `--no-verify`, `push --force` (without `--force-with-lease`), `commit
  --amend` on a pushed commit, or `git checkout --` on a dirty tracked file,
  print the reason to stderr and `exit 2` (source 4: exit 2 blocks). Encodes
  the three "never" rules deleted from B.3.
- `check-release-case.sh` — only for `git commit`; if `git diff --cached
  --name-only` touches `crates/dtfs-manager/src/{api,metadata,scheduler}.rs`,
  `crates/dtfs-data/src/`, `crates/dtfs-fuse/src/`, or `proto/`, and no
  `tests/release/cases/` file is staged, and the message carries no
  `Release-case: none` trailer → `exit 2` with the reason. Implements D6.
- `rustfmt-check.sh` — for an edited `crates/**/*.rs`, run `rustfmt --check
  --edition 2021` on that one file inside `dtfs-buildenv`; non-zero → print the
  diff, exit 0 (advisory, not blocking: a formatting block mid-task is more
  disruptive than the nit it catches). Skip entirely if `docker` is absent.

Rejected: a `Stop` hook that reminds about the AGENTS.md-update rule (fires on
every turn, becomes noise); a `SessionStart` hook that prints cluster state
(the `dtfs-proactive-issue-finder` skill does it on demand).

---

## 6. Token-efficiency measures

1. **AGENTS.md ≤ 250 lines** (from 991). Part A keeps: A.1 topology table,
   A.3 conventions as one-line rules with pointers, A.4 build commands, A.5
   the `[memory]` block, A.8 as a file→responsibility table with ≤ 1 line per
   entry. The CO-/REV-/WI-/Bug-letter narratives move to
   `docs/src/internals/invariants.md` (a reviewer or auditor loads it; a
   typo-fix session never does).
2. **`.claude/CLAUDE.md` stays ≤ 10 lines**: graphify pointer + "agent roster
   in `.claude/agents/`, invariants in `.claude/rules/`". No `@`-imports beyond
   the existing `@AGENTS.md` — imports expand inline and save nothing.
3. **Path-scoped rules** carry the heavy invariants: `integrity.md` (A.6) for
   data/scheduler/raft paths, `fuse.md` (A.3.2 memory ladder, guard-across-
   await) for `crates/dtfs-fuse/**`, `config-surface.md` (B.5 sync rule) for
   `crates/dtfs-common/src/config.rs`.
4. **Agent output caps** as specified in §4 (explore 25 lines, intake 60,
   implementer 15, auditor 40). Uncapped subagent reports are the second
   biggest main-context leak after file reads.
5. **Read the region, not the file.** `scheduler.rs`, `metadata.rs`,
   `filesystem.rs`, `state_machine.rs` are each thousands of lines; every
   prompt says: grep for the symbol, then Read with `offset`/`limit`.
6. **graphify before grep** for "where does X live / what calls Y / what does
   this subsystem touch" — `graphify-out/` already exists and answers those
   without a fan-out of file reads. Plain grep stays right for an exact string.
7. **`/clear` between unrelated tasks**, and start implementation in a fresh
   session from the plan file (source 1: a clean session with a written spec
   beats a long session with corrections).
8. **Keep AGENTS.md stable within a session.** Editing it invalidates the
   cached prompt prefix; batch AGENTS.md edits to the end of a task.

---

## 7. Tasks

| ID | Task | Files | Done when | Status |
|---|---|---|---|---|
| AIWF-1-1 | Rewrite Part B from §3 | `AGENTS.md` | Part B ≤ 55 lines; every deleted rule has a new home or is deliberately dropped in this plan | ✅ |
| AIWF-1-2 | Move A.6 + FUSE/memory + config-surface invariants into path-scoped rules | `.claude/rules/{integrity,fuse,config-surface}.md`, `AGENTS.md` | `/context` shows the rules load only for matching paths; AGENTS.md keeps a one-line pointer each | ✅ |
| AIWF-1-3 | Condense Part A narratives into docs | `docs/src/internals/invariants.md`, `AGENTS.md`, `docs/src/SUMMARY.md` | AGENTS.md ≤ 250 lines; no invariant text lost (diff-reviewed) | ✅ |
| AIWF-1-4 | Split the release-case contract out of Part B | `tests/release/README.md` | Contains STATUS line, `trap case_cleanup`, `wait_for`, numbering, QUICK_CASES rules | ✅ |
| AIWF-1-5 | Update `plans/README.md` for D4/D5 | `plans/README.md` | States the ≥3-task trigger and the one-line verification-log form | ✅ |
| AIWF-2-1 | Author `dtfs-explore`, `dtfs-intake` | `.claude/agents/*.md` | Each routes on `description` alone in a trial prompt | ✅ |
| AIWF-2-2 | Author `dtfs-planner`, `dtfs-implementer` | `.claude/agents/*.md` | Implementer's build block matches the Docker command verbatim | ⬜ |
| AIWF-2-3 | Author `dtfs-reviewer`, `dtfs-integrity-auditor` | `.claude/agents/*.md` | Output matches the CORE-REVIEW-2026-09-12 finding shape | ⬜ |
| AIWF-2-4 | Author `dtfs-release-case-author` | `.claude/agents/*.md` | Produces a `bash -n`-clean case for a known past change | ⬜ |
| AIWF-3-1 | Add `.claude/settings.json` + `guard-git.sh` | `.claude/{settings.json,hooks/guard-git.sh}` | A `git commit --no-verify` attempt is blocked with a readable reason | ⬜ |
| AIWF-3-2 | Add `check-release-case.sh` | `.claude/hooks/check-release-case.sh` | Blocks a staged `dtfs-data/src` change with no case; passes with the trailer | ⬜ |
| AIWF-3-3 | Add `rustfmt-check.sh` (advisory) | `.claude/hooks/rustfmt-check.sh` | Prints a diff on a deliberately mis-formatted edit; never blocks; no-ops without docker | ⬜ |
| AIWF-4-1 | Release-note mechanism decision (D3) | `.github/cliff.toml`, `scripts/dtfs-release.sh` | `git-cliff` reproduces the current section grouping from commit subjects on the last 30 commits — or the task is closed as "keep PRs" | ⬜ |
| AIWF-5-1 | Dry run: one real small fix end-to-end through the roster | any | A deferred LOW finding from CORE-REVIEW (e.g. CMN-4 `hex_decode` ASCII guard) lands via intake → implementer → auditor; record total tokens and compare against a same-fix baseline done inline | ⬜ |

Ordering rationale: the AGENTS.md rewrite lands first because every agent
prompt cites its sections; hooks land after the agents so the agents are the
thing being guarded; the dry run is last and is the only evidence that the
roster is cheaper, not just tidier.

## Verification log

- AIWF-1-1 (2026-09-12): `git log --oneline -1` -> `int: rewrite AGENTS.md
  Part B for a single-developer workflow`; Part B 65 lines added / 156
  removed net, section itself 51 lines.
- AIWF-1-2 (2026-09-12): confirmed `.claude/rules/*.md` `paths:` frontmatter
  against https://code.claude.com/docs/en/memory ("Path-specific rules")
  before moving content; added `.claude/rules/{integrity,fuse,config-surface}.md`
  and un-ignored them in `.gitignore`.
- AIWF-1-3 (2026-09-12): `wc -l AGENTS.md` -> 188 (was 1041 at session start);
  Part B span 34 lines; bug-archaeology narrative moved to
  `docs/src/internals/invariants.md` + `docs/src/SUMMARY.md` entry.
- AIWF-1-4 (2026-09-12): created `tests/release/README.md` with the STATUS
  line / `trap case_cleanup` / `wait_for` / numbering / QUICK_CASES rules
  extracted from the old B.5 prose.
- AIWF-1-5 (2026-09-12): `plans/README.md` now states the >=3-task/
  multi-session/cross-crate trigger and the one-line verification-log form.
- AIWF-2-1 (2026-09-12): added `.claude/agents/dtfs-explore.md` (haiku,
  Read/Grep/Glob) and `.claude/agents/dtfs-intake.md` (sonnet, +
  Bash(git log/show)); frontmatter checked against
  https://code.claude.com/docs/en/sub-agents.

## 8. Risks / open questions

**Decided 2026-09-12 (operator accepted the lead's recommendations):**
1. Codex/Cursor losing `.claude/rules` content: **accepted** (rarely used here).
2. `paths:` scoping: **verify first** — AIWF-1-2 stays gated on that check.
3. Release notes: **`git-cliff`** (AIWF-4-1 implements; PRs no longer required
   for `bug:`/`feat:` once it lands).
4. Branch threshold: **as written** — wire/on-disk format OR > 6 files OR
   multi-session.
5. Models: **Opus for `dtfs-reviewer` + `dtfs-integrity-auditor`; Sonnet for
   `dtfs-planner`** (and intake/implementer/release-case-author); Haiku for
   `dtfs-explore`.
6. Docker bind mount: **parameterise** via `DTFS_REPO_HOST_PATH` env var
   (default the main checkout); implementer refuses to run from a worktree if
   the var is unset.
7. `dtfs-implementer` **may commit** (guarded by AIWF-3-1 git-guard hook).
8. `.claude/CLAUDE.md` **states** claude-mem / context-mode / superpowers are
   opt-in, not part of the DTFS contract.

Sequencing with PLAN-CR-2026-09: AIWF-1-* (AGENTS.md rewrite) lands right
after CR Group 1 merges and before CR Group 2 starts, so Groups 2-6 follow
the new Part B (they still use branches: every group crosses the D2
wire/on-disk threshold).

Original questions, kept for the record:

1. `AGENTS.md` is also read by Codex/Cursor, which do **not** support
   `.claude/rules/`. Moving A.6 there makes those tools weaker. Accept, or
   keep a compressed A.6 in AGENTS.md and the long form in the rule file?
2. The `.claude/rules/` `paths:` mechanism is confirmed from one doc fetch
   only — AIWF-1-2 must verify with `/context` before the content moves.
3. Dropping PRs loses GitHub's label-based grouping. Is adding `git-cliff` to
   the release host acceptable, or keep PRs for `bug:`/`feat:` only?
4. D2's "> 6 files" threshold is a guess. Better signal may be "the change
   cannot be deployed to the lab in one rolling restart".
5. Opus for planner/reviewer/auditor is the expensive choice; at ~15× chat
   cost for multi-agent chains (source 8) this could raise, not lower, spend.
   The AIWF-5-1 dry run is the decision point — should any of the three drop
   to Sonnet with `effort: high` instead?
6. The Docker buildenv command is hard-coded to
   `C:/Users/leduc/git_github.com/dtfs2`. If a worktree is ever used, the
   implementer's bind mount is wrong. Parameterise, or forbid worktrees?
7. Should `dtfs-implementer` be allowed to commit at all, or only stage and
   report? Committing is what makes it autonomous; it is also what makes a bad
   run expensive to unwind.
8. `claude-mem`, `context-mode`, and `superpowers` are all installed and all
   claim context savings. None is wired into this plan. Worth an explicit
   "these are opt-in, none is part of the DTFS contract" line in
   `.claude/CLAUDE.md`?
