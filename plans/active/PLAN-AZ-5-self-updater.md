# PLAN-AZ-5 — Self-updater + release builder

## Goals

- `dtfs-cli update <manager|data|fuse|cli|all>` — pull the latest GitHub
  release, verify it (sha256 vs in-tarball MANIFEST), atomically install
  the target binary, restart the matching systemd service on the local
  host. `--check` reports installed vs latest; no changes.
- Release source URL is a config key `[updater].repo_url` with a default
  of `dtfs_common::REPO_URL`.
- `scripts/dtfs-release.sh <version>` — stamp + build + assemble every
  GitHub-release file (fixed-name `dtfs-linux-amd64.tar.gz`, `SHA256SUMS`,
  release notes) and print (or `--publish`) the `gh release create`
  command.
- Stamp the version into the binaries via `DTFS_VERSION` (default
  `v0.0.0-dev`, so dev/incremental builds are unchanged).

## Non-goals

- Cluster-wide rolling (stays in `scripts/dtfs-update.sh`).
- Background/auto-update; semver/downgrade gating.
- Daemon self-update subcommands; rollback; non-amd64 assets.

## Locked-in decisions

1. Local node only.
2. Single `dtfs-cli update <target>` command (daemons untouched).
3. On-demand only.
4. Release script stages files + prints `gh` command; `--publish` opt-in.
5. `DTFS_VERSION` stamped into binaries (default `v0.0.0-dev`).

## Data-model / interface changes

- `dtfs_common::config::UpdaterConfig { repo_url: String, install_dir:
  String }` with `#[serde(default)]` fields + `Default`. Not added to the
  role config structs (serde ignores `[updater]` for daemons; no
  `deny_unknown_fields` in tree).
- New clap subcommand `Commands::Update { target, check, tag, repo_url,
  install_dir, config, no_restart, yes }`; `target` is a `ValueEnum`
  `{manager,data,fuse,client(alias->fuse),cli,all}`.
- `build.rs` emits `DTFS_VERSION` (rustc-env + rerun-if-env-changed);
  `version.rs` `VERSION`/`VERSION_STRING` read `env!("DTFS_VERSION")`.
- Release asset contract (unchanged, now produced by us):
  `<repo>/releases/{latest/download|download/<tag>}/dtfs-linux-amd64.tar.gz`,
  layout `dtfs/bin/*` + `dtfs/systemd/*` + `dtfs/MANIFEST`
  (`<sha256>  <basename>` per binary).

## Tasks

- **AZ-5-0** ✅ This plan file.
- **AZ-5-6** ✅ Version stamping: `build.rs` + `version.rs` +
  `scripts/tests/test-release-version-stamp.sh`.
- **AZ-5-1** ✅ `UpdaterConfig` in `dtfs-common::config` + unit tests
  (`updater_config_defaults`, `updater_config_override`).
- **AZ-5-2** ✅ `dtfs-cli/src/updater.rs` pure helpers + unit tests
  (`parse_owner_repo`, `tarball_url`, `api_latest_url`, `manifest_sha`,
  `target_binaries`/`target_service`, `resolve_repo_url`).
- **AZ-5-3** ✅ `updater.rs` runtime (download→verify→install→restart,
  `--check`) + wire `mod updater`/`Commands::Update`/dispatch in
  `main.rs` + `sha2` dep in `Cargo.toml`.
- **AZ-5-7** ✅ `scripts/dtfs-release.sh`.
- **AZ-5-4** ✅ `tests/release/cases/NN-updater.sh` + register in
  `tests/release/run.sh`.
- **AZ-5-5** ✅ Sample `[updater]` blocks, `changelog/UNRELEASED.md`,
  `docs/src` (update cmd + `[updater]` + cutting-a-release),
  `AGENTS.md` (§ A.8, § A.4, § A.3.5).

## Cross-cutting invariants

- Integrity: never install a binary whose sha256 != MANIFEST; one
  mismatch aborts the whole update (§ A.3.3, A.6).
- Bounded memory: `curl -o` streams the download; sha256 reads in chunks;
  no whole-file `Vec` (§ A.3.2).
- Atomic install via `install -m 0755` only.
- Confirm before restart (manager restart → re-election) unless `--yes`.

## Verification log

All on `dtfs-builder` (10.20.0.150), 2026-06-04, after a `tar`-over-SSH
sync + `*.sh` CRLF→LF normalize.

- **AZ-5-6** `bash scripts/tests/test-release-version-stamp.sh` →
  `PASS: DTFS_VERSION stamps the binary; unset stays v0.0.0-dev`
  (`dtfs-cli --version` = `v0.0.0-dev …` unset, `v9.9.9-guard …` stamped).
- **AZ-5-1** `cargo test --release -p dtfs-common` → 49 passed
  (incl. `config::tests::updater_config_defaults` / `…_override`).
- **AZ-5-2/3** `cargo test --release -p dtfs-cli` → 7 passed (all
  `updater::tests::*`). `dtfs-cli update --help` lists targets
  (`manager|data|fuse|cli|all`, `client` alias) + flags; bad target
  rejected.
- **AZ-5-4** `tests/release/cases/42-updater-local-install.sh` →
  `STATUS: PASS` (offline file:// install + sha256 gate; corrupt binary
  refused, nothing installed). Re-ran with the release cli: PASS.
- **AZ-5-7** `bash scripts/dtfs-release.sh v9.9.9-test` → full workspace
  build (dtfs-fuse 33 tests pass), staged
  `target/release-artifacts/v9.9.9-test/{dtfs-linux-amd64.tar.gz,SHA256SUMS,RELEASE_NOTES.md}`,
  stamp check `dtfs-cli v9.9.9-test (built …)`, printed `gh release create`.
- **Round-trip** updater installed from the real artifact via file:// →
  installed `dtfs-cli --version` = `v9.9.9-test` (stamp survives build →
  release → download → verify → install).
- **AZ-5-5** `mdbook build docs` clean; release tests stayed green.

## Deviations

- 2026-06-04: initial edits accidentally landed in the non-worktree
  checkout (`~/git_github.com/dtfs2` on `bug-cb-orphan-reap`); reverted
  there and re-applied in the worktree. No effect on delivered code.
- Builder lacked clang and the operator host has no rsync — installed
  `clang`/`libclang-dev` on the builder (full workspace now builds) and
  used `tar`-over-SSH with a `*.sh` CRLF→LF normalize step. Builder
  access + sync documented in AGENTS.md § A.4.
- `UpdaterConfig` is defined in dtfs-common (tested there) but NOT added
  as a field on the role config structs — the CLI reads `[updater]` via a
  local `Option`-typed extractor through `load_config`, so precedence
  (flag > config > env > default) is exact and the daemons ignore the
  block (no `deny_unknown_fields` in tree).

## Notes

- AGENTS.md § A.4 updated with `dtfs-builder` access (10.20.0.150,
  user `debian`, key `~/.ssh/dtfs-lab/id_ed25519`, `tar`-over-SSH sync).
