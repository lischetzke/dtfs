# `changelog/` — Release Notes

This directory holds the **user-visible release notes** for DTFS.
Each released version has its own file; `UNRELEASED.md` is the
accumulator for the next release. Contents feed
`gh release create --notes-file`, so what you write here becomes
the GitHub release page verbatim.

## Layout

```
changelog/
├── README.md         # this file
├── UNRELEASED.md     # accumulator for the next release
└── v<X.Y.Z>.md       # one file per released version (created at release time)
```

## Scope — what goes here

Only changes a user, operator, or downstream redistributor needs
to know about. Five categories, nothing else:

- **Bug Fixes** — user-observable bugs that are now fixed.
- **New Features** — new capabilities, configuration knobs, or
  RPCs the user can call.
- **Known Issues** — open gaps the user should be aware of at
  release time (with a workaround if available).
- **Breaking Changes** — anything that changes the contract for
  config files, CLI flags, gRPC/REST shapes, on-disk format,
  default behaviour, or licence terms.
- **Special Requirements for Updates** — manual steps an operator
  must take when moving to this version (data migration, config
  rewrite, dependency bump, remote URL change, etc.).

## What does **not** belong here

- Internal repository restructures (folder moves, file renames
  that don't affect users).
- Documentation cleanups, README rewrites, `AGENTS.md` /
  `CLAUDE.md` edits.
- New plans, post-mortems, or reports — those live in
  [`plans/`](../plans/README.md).
- Refactors with no behaviour change.
- Test additions, CI config changes, build-system tweaks.
- Per-PR developer notes — those belong in the PR description or
  commit body.

If in doubt, ask "would a user redeploying to this version care?"
If the answer is no, it doesn't belong here.

## How to write entries

Add the entry in the **same commit** as the user-visible change.
Use only the categories that actually have content:

```markdown
# Unreleased

## Breaking Changes
- ...

## Special Requirements for Updates
- ...

## Known Issues
- ...

## New Features
- ...

## Bug Fixes
- ...
```

## Cutting a release

**Release gate (Bug BE):** before tagging, run the end-to-end release
suite against a representative cluster and confirm every case is `PASS`
or a `KNOWN_ISSUE` pinned to a tracked follow-up:

```bash
bash tests/release/run.sh            # full suite
bash tests/release/run.sh --quick    # ~15-min critical-path subset
```

Do not tag a release with an unexplained `FAIL`. Then:

1. Add the version header at the top of `UNRELEASED.md`:

   ```markdown
   # v0.2.0 — 2026-06-15
   ```

2. `git mv changelog/UNRELEASED.md changelog/v0.2.0.md` and create
   a fresh `UNRELEASED.md` with the empty category skeleton above.
3. Tag the release: `git tag -a v0.2.0 -m "v0.2.0"`.
4. Push the tag: `git push origin v0.2.0`.
5. Create the GitHub release:

   ```bash
   gh release create v0.2.0 \
     --title "v0.2.0" \
     --notes-file changelog/v0.2.0.md
   ```

   `gh` publishes the release page with the changelog file as the
   body. Attach build artifacts via additional positional args if a
   build job hasn't already uploaded them.
