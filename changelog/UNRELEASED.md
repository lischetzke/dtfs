# Unreleased

## Added
- `plans/` directory as the canonical home for all work plans,
  incidents, and reports. See [plans/README.md](../plans/README.md).
- `changelog/` directory (this folder) for per-release notes
  consumed by `gh release create`.
- Explicit **Fixing Procedure** in [AGENTS.md](../AGENTS.md#fixing-procedure):
  failing test → fix → verify.
- **Use of AI** section in [README.md](../README.md#use-of-ai) and
  [AGENTS.md](../AGENTS.md#use-of-ai) — AI is permitted and must
  follow AGENTS.md.
- **Structure Discipline** section in [AGENTS.md](../AGENTS.md#structure-discipline)
  codifying where every kind of artefact lives.

## Changed
- Project renamed from **DTFS2** to **DTFS** (folder name unchanged
  for now).
- License switched from **MIT** to **AGPL-3.0**.
- Repository moved to <https://github.com/lischetzke/dtfs>.
- README restructured as a hub linking to quick start, installation,
  configuration, troubleshooting, and contribution docs. Added a
  deployment warning disclaiming liability and stating DTFS is not
  for mission-critical data.
- Plans, post-mortems, and historical reports migrated from
  `tests/proxmox/` into `plans/` (`active/`, `incidents/`,
  `reports/`). `PROGRESS.md` moved from repo root to
  `plans/reports/PROGRESS-2026-03.md`. Source-code doc comments
  that referenced the old paths were updated in the same change.

## Fixed
- _(none yet)_

## Removed
- _(none yet)_

## Security
- _(none yet)_
