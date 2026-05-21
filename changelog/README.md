# `changelog/` — Per-release Changelog Entries

This directory holds **user-visible** release notes for DTFS. It is
the source `gh release create` reads from when cutting a release,
which lets us produce consistent release pages straight from the
repository.

## Layout

```
changelog/
├── README.md         # this file
├── UNRELEASED.md     # accumulator for the next release
└── v<X.Y.Z>.md       # one file per released version (created at release time)
```

## How to write entries

Add a one-line entry to `UNRELEASED.md` in the **same commit** as
any user-visible change. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/):

```markdown
## Added
- Per-DC rolling read-window cache for warm-tier nodes.

## Changed
- Project renamed from DTFS2 to DTFS.
- License switched to AGPL-3.0.

## Fixed
- Bug F: list_directory aggregator skipped entries past 4 KiB.

## Removed
- Deprecated `--legacy-cookies` flag from `dtfs-fuse`.

## Security
- TLS: minimum protocol bumped to TLSv1.3 for cluster join.
```

Only the sections relevant to the entry need to be present.
Internal refactors, doc-only edits, test-only changes, and CI tweaks
**do not** belong in the changelog — those are visible from
`git log`, not from a release page.

## Cutting a release

1. Review `UNRELEASED.md` for completeness; add the release date as
   a header at the top of its contents:

   ```markdown
   # v0.2.0 — 2026-06-15
   ```

2. `git mv changelog/UNRELEASED.md changelog/v0.2.0.md` and create
   a fresh empty `UNRELEASED.md` skeleton (the section headings
   shown above, all empty).
3. Tag the release: `git tag -a v0.2.0 -m "v0.2.0"`.
4. Push the tag: `git push origin v0.2.0`.
5. Create the GitHub release:

   ```bash
   gh release create v0.2.0 \
     --title "v0.2.0" \
     --notes-file changelog/v0.2.0.md
   ```

   `gh` will publish the release page with the changelog file as
   the body. Attach build artifacts via `--target` / additional
   positional args if a build job hasn't already uploaded them.

## What does **not** belong here

- **How a change was made** — that's a plan (see
  [`plans/`](../plans/README.md)).
- **Incident write-ups / post-mortems** — those are
  [`plans/incidents/`](../plans/README.md).
- **Per-PR developer notes** — those belong in the PR description
  or commit body.
