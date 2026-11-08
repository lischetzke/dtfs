# CI pipeline: test, release, docs

GitHub Actions in `.github/workflows/` test every change, cut releases and
publish this documentation. CI never deploys: the operator rolls a release out
with `deploy/ansible/update-cluster.yml` (below). The repository is
`https://github.com/lischetzke/dtfs` (public, default branch `master`) and the
only source. Every release build goes through the version-stamping wrappers
(`CLAUDE.md` § A.3.5).

| Workflow | Runs on | Does |
|---|---|---|
| `ci.yml` | push to `master`, pull requests | `cargo test --workspace --release`, script guards, docs link check, wiki conversion check, Ansible syntax check |
| `release.yml` | push to `master` whose head commit subject starts with `release vX.Y.Z`; manual run | tests, version-stamped build, checksums, optional signature, git tag + GitHub release |
| `docs.yml` | push to `master` touching `docs/**`; manual run | mdBook to GitHub Pages, and the GitHub Wiki |

All workflows default to `permissions: contents: read`; the `publish` job of
`release.yml` and the wiki job of `docs.yml` raise it to `contents: write`, the
Pages deploy job adds `pages: write` and `id-token: write`.

## CI

`ci.yml` has two jobs.

- `rust` runs in the `rust:1.94.1-bookworm` container with the native packages
  of `docker/buildenv.Dockerfile` (clang, libclang, cmake, protobuf-compiler,
  pkg-config, libfuse3-dev, fuse3, libssl-dev, nodejs; node is what the dashboard
  `node --check` test needs). `Swatinem/rust-cache` keeps `target/` between
  runs. After the workspace tests it runs the build-stamp guards
  (`test-release-version-stamp.sh`, `test-build-date-fresh.sh`).
- `scripts` runs `bash -n` over every script, `test-ci-version.sh`,
  `test-dtfs-install.sh`, `check-webui-js.sh`, the wiki sync unit tests,
  `scripts/check-docs-links.py`, a dry run of `scripts/sync-wiki.py` (fails when a
  docs link would not resolve to a wiki page) and `ansible-playbook
  --syntax-check` of the deploy playbook (`ansible-core` installed with `pipx`).

## Releasing

The first word of the head commit subject decides, on a push to `master`
(`scripts/ci/version.sh`):

| Subject starts with | Result |
|---|---|
| `release vX.Y.Z` (the `v` is optional) | release `vX.Y.Z` |
| anything else | nothing is built or released |

```
release v0.2.0: heartbeat and lease fixes
bug: fix the heartbeat forward      <- tests only (ci.yml), nothing ships
```

The keyword must be the first word, so a commit that merely mentions "release"
never ships. A release can also be started without a commit: Actions >
`release` > Run workflow on `master`, with `version` set to `vX.Y.Z`.

`release.yml` runs three jobs:

1. `prepare` resolves the version and fails when the tag already exists
   (bump the version).
2. `build` runs the tests and `scripts/dtfs-release.sh vX.Y.Z` in
   `rust:1.94.1-bookworm` (the glibc must match the Debian bookworm hosts): the
   four binaries stamped with `DTFS_VERSION` and a fresh `BUILD_DATE`,
   `dtfs-linux-amd64.tar.gz` (binaries, systemd units, `dtfs/MANIFEST`) and
   `SHA256SUMS`. The stamp is verified before the files are uploaded as the
   `dist` artifact.
3. `publish` refuses an existing tag or release, signs `SHA256SUMS` when
   signing is configured (below), generates the notes with `git-cliff` from
   `.github/cliff.toml` (commit subjects and `Breaking:` / `Update-required:` /
   `Known-issue:` trailers) and runs `gh release create vX.Y.Z` with the tarball,
   `SHA256SUMS` and, when signed, `SHA256SUMS.minisig`. The release creates the tag
   at the built commit. The same command is what
   `scripts/dtfs-release.sh vX.Y.Z --publish` runs by hand.

Naming, URLs and how to verify a download are in
[Release Artifacts](../deployment/artifacts.md).

## Release signing (optional)

With the repository secret `MINISIGN_SECRET_KEY` set, the `publish` job runs
`scripts/ci/sign-release.sh` (installing `minisign` first) and attaches
`SHA256SUMS.minisig`. Unset, releases stay unsigned.

Generate the key pair once, on a trusted machine:

```bash
minisign -G -p dtfs-release.pub -s dtfs-release.key   # prompts for a password
```

Store the contents of `dtfs-release.key` as the secret `MINISIGN_SECRET_KEY`
and its password as `MINISIGN_PASSWORD` (Settings > Secrets and variables >
Actions; `-W` makes a password-less key, then leave the password unset). Put the
public key (the second line of `dtfs-release.pub`) into the consumers'
`[updater] minisign_public_key` and into `dtfs_minisign_pubkey` for the
playbook. Verify by hand with `minisign -V -p dtfs-release.pub -m SHA256SUMS`.

## Deploying a release

Deploys are done by the operator from a control host with SSH access as `root`
to every cluster host (key named in the inventory by
`ansible_ssh_private_key_file`), `ansible-core`, `curl`, `jq`, and `minisign`
when signatures are required. Keep the production inventory outside the
repository, in the shape of `deploy/ansible/inventory.example.yml`; host names
must equal each node's `[node].hostname` (or set `dtfs_hostname`); clients carry
`dtfs_mount_point`.

```bash
ansible-playbook -i /path/to/inventory.yml deploy/ansible/update-cluster.yml \
    -e dtfs_version=v0.2.0 -e dtfs_minisign_pubkey=<base64> \
    -e dtfs_restart_delay_secs=30
```

The playbook downloads `dtfs-linux-amd64.tar.gz`, `SHA256SUMS` and (with
`dtfs_minisign_pubkey`) `SHA256SUMS.minisig` from
`https://github.com/lischetzke/dtfs/releases/download/<dtfs_version>/`
(`dtfs_repo` overrides the repository; `-e dtfs_tarball=<path>` uses a local
file, for example one built with `scripts/dtfs-release.sh`). It verifies the
checksum, the signature and the packaged `MANIFEST` before touching a node, then
updates one node at a time (managers learners-first and leader last, then data
nodes, then clients), each with a health gate and a settle delay, and finally runs
`scripts/dtfs-cluster-healthcheck.sh`. A node already on the target version with
an active service is skipped, so a failed run can be re-run. The previous binary
is kept as `<bin>.prev`. The exact steps, rollback and the config flips that must
follow an update (`require_raft_auth` and others) are in
[Rolling Update and Rollback](../deployment/rolling-update.md); the variables are
in [Role Variables and Idempotency](../deployment/role-variables.md).

### Recovering from a failed rollout

1. Stuck or unhealthy node: fix or inspect it, then re-run the same command;
   completed nodes are skipped.
2. Roll a node back from its `<bin>.prev` copies, in reverse update order
   ([Rollback](../deployment/rolling-update.md#rollback)).
3. Withdraw the release so `latest` (the installer, `dtfs-cli update`) no longer
   resolves to it: `gh release delete vX.Y.Z` (needs write access). The tag stays
   and the version number is burnt, so ship the fix as the next version. To keep
   the release visible but not `latest`, use `gh release edit vX.Y.Z
   --prerelease` or `--latest=false` instead.

## Documentation

`docs.yml` publishes the book twice from `docs/src` on `master`:

- GitHub Pages: `scripts/build-docs.sh --clean` runs the Markdown link check
  (broken relative links and pages missing from `SUMMARY.md` fail the job),
  downloads the pinned, sha256-verified mdBook release and builds `docs/book/`.
  `MDBOOK_OUTPUT__HTML__SITE_URL=/dtfs/` makes the 404 page resolve under the
  project sub-path (`https://lischetzke.github.io/dtfs/`). The result is deployed
  with `actions/upload-pages-artifact` + `actions/deploy-pages`.
- GitHub Wiki: `scripts/sync-wiki.py docs/src <dir>` converts the pages listed in
  `SUMMARY.md` into wiki pages (names from the titles, `Home` from the
  introduction, `_Sidebar` mirroring `SUMMARY.md`, `_Footer` pointing back at
  `docs/src`; links to other docs pages become wiki links, links to repository
  files become `blob/master` URLs) and pushes them to
  `https://github.com/lischetzke/dtfs.wiki.git` with the workflow token. The wiki
  is fully generated: edits made there are overwritten, edit `docs/src`.

One-time repository setup: Settings > Pages > Source "GitHub Actions"; enable the
wiki and create its first page by hand (GitHub creates the wiki repository only
then). Build locally with `./scripts/build-docs.sh` (see the script header for the
Docker fallback on non-Linux hosts).

## Health check

`scripts/dtfs-cluster-healthcheck.sh --managers <list> [--expect-version V]
[--min-data-nodes N] [--timeout S]` is the post-deploy gate used by the playbook's
last task; what it asserts is listed in
[Rolling Update](../deployment/rolling-update.md#per-node-procedure). Exit 0
healthy, 1 unhealthy after the timeout, 3 bad arguments. Read-only; needs no
shared secret, only `curl` and `jq`.

## Version stamps

Release versions are `vX.Y.Z`; a build without `DTFS_VERSION` reports
`v0.0.0-dev`. `dtfs-cli update` and `dtfs-install.sh --source remote` resolve
`latest` to the newest release. Naming, URLs and verification are specified in
[Release Artifacts](../deployment/artifacts.md).
