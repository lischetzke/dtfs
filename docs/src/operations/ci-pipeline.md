# CI pipeline: test, release, deploy

The GitLab pipeline in `.gitlab-ci.yml` is the only sanctioned way to
ship a build to the cluster. It runs on the builder host (a GitLab runner
with the shell executor and the tag `dtfs-builder`), so every build goes
through the version-stamping wrappers (`CLAUDE.md` § A.3.5).

## What a commit triggers

The first word of the commit subject decides:

| Subject starts with | Version | Pipeline |
|---|---|---|
| `release vX.Y.Z` (the `v` is optional) | `vX.Y.Z` | test, build, GitLab release + git tag, deploy, verify |
| `deploy` | `v<base>_<YYYYMMDDHHmm>` (UTC), `<base>` = last `vX.Y.Z` tag | same, the release is marked as an alpha build |
| anything else | none | test only |

Examples:

```
release v0.2.0: heartbeat and lease fixes
deploy the monitor threshold change
bug: fix the heartbeat forward      <- tests only, nothing ships
```

The keyword must be the first word, so a commit whose text merely
mentions "deploy" never ships. Keyword commits on a branch other than
the default branch build and test but neither release nor deploy. A
`release` whose tag already exists fails in `prepare`. A pipeline started
by hand (CI/CD > Pipelines > Run pipeline) can set
`DTFS_DEPLOY_MODE=deploy` or `DTFS_DEPLOY_MODE=release` plus
`DTFS_RELEASE_VERSION=vX.Y.Z` to ship the current commit without a
keyword.

## Stages

1. `prepare` computes `MODE` and `VERSION` with `scripts/ci/version.sh`
   and hands them to every later job (dotenv artifact).
2. `test` runs `cargo test --workspace --release`, the shell-script
   guards (`bash -n` over every script, `test-ci-version.sh`,
   `test-dtfs-install.sh`, `test-build-date-fresh.sh`,
   `test-release-version-stamp.sh`) and an Ansible syntax check of the deploy playbook. Every
   commit runs this.
3. `build` runs `scripts/dtfs-release.sh $VERSION`: version-stamped
   binaries, `dtfs-linux-amd64.tar.gz`, `SHA256SUMS`, git-cliff notes.
   The result is the `dist/` job artifact.
4. `release` signs `SHA256SUMS` when signing is configured (below), uploads `dist/*` to the project's Generic Package Registry
   (package `dtfs`, version `$VERSION`) and creates the GitLab release,
   which also creates the git tag. Alpha builds carry an "Alpha build"
   banner in the description. Tag pipelines are disabled so this push
   does not start a second pipeline.
5. `deploy` runs `deploy/ansible/update-cluster.yml` against the
   production inventory (see below). One deploy at a time
   (`resource_group: cluster`).
6. `verify` runs `scripts/dtfs-cluster-healthcheck.sh` from the runner
   with `--expect-version $VERSION`.
7. `yank:release` is a manual job (never blocks the pipeline), see
   "Recovering from a failed rollout".

The build honours `CARGO_TARGET_DIR` (exported from `DTFS_CI_TARGET_DIR`);
`dtfs-build.sh` and `dtfs-release.sh` write their output under it, default
`target/`.

## Rolling update

`deploy` runs `deploy/ansible/update-cluster.yml`: preflight on the control host,
then one node at a time (managers learners-first and leader last, then data nodes,
then clients), each with a health gate and a settle delay, then
`scripts/dtfs-cluster-healthcheck.sh`. A node already on the target version with an
active service is skipped, so a failed run can be re-run. The previous binary is
kept as `<bin>.prev`. The exact steps, health conditions, rollback, the extra
checks an Ansible role should add, and the config flips that must follow an update
(`require_raft_auth` and others) are in
[Rolling Update and Rollback](../deployment/rolling-update.md); the playbook's
variables are in [Role Variables and Idempotency](../deployment/role-variables.md).

Run it by hand:

```bash
ansible-playbook -i /etc/dtfs-ci/inventory.yml deploy/ansible/update-cluster.yml     -e dtfs_tarball=target/release-artifacts/v0.2.0/dtfs-linux-amd64.tar.gz     -e dtfs_version=v0.2.0 -e dtfs_restart_delay_secs=30
```

### Recovering from a failed rollout

1. Stuck or unhealthy node: fix or inspect it, then re-run the same command;
   completed nodes are skipped.
2. Roll a node back from its `<bin>.prev` copies, in reverse update order
   ([Rollback](../deployment/rolling-update.md#rollback)).
3. Withdraw the release: the `release` job runs before the rollout, so a failed
   deploy leaves it as `latest`. Run the manual job `yank:release` in the pipeline
   (or `scripts/ci/gitlab-yank.sh <version>` in a job). It deletes the GitLab
   release object only; the tag and packages stay and the version number is
   burnt, so ship the fix as the next version. Needs `DTFS_RELEASE_API_TOKEN`
   (project access token, scope `api`, Maintainer) unless the instance lets job
   tokens delete releases.

## Documentation job

Job `pages` (stage `build`, independent of the test and release chain) builds this
book with `scripts/build-docs.sh --clean` on every push to the default branch
that touches `docs/**`, `scripts/build-docs.sh` or `scripts/check-docs-links.py`,
and on a manually started pipeline. The script runs a Markdown link check (broken
relative links, pages missing from `SUMMARY.md` fail the job), downloads the pinned
mdBook release binary once into `~/.cache/dtfs/mdbook` (sha256-verified), builds
`docs/book/` and the job copies it to `public/`, which GitLab publishes as Pages
and keeps as the `dtfs-docs` artifact for four weeks. `site-url` is derived from
`CI_PAGES_URL` so the 404 page resolves under the project sub-path. Build locally
with `./scripts/build-docs.sh` (see the script header for the Docker fallback on
non-Linux hosts). GitLab Pages must be enabled for the project.

## Runner prerequisites

On the builder host:

- Rust toolchain in `~/.cargo/bin` for the runner user, plus `protoc`,
  `clang`/`libclang-dev`, `libfuse3-dev`, `pkg-config`, `build-essential`
  (the same set as `docker/buildenv.Dockerfile`).
- `ansible-core` (the playbook uses only `ansible.builtin` modules),
  `curl`, `jq`, `git-cliff` (optional; without it the release notes fall
  back to a one-line description), and for the `pages` job `python3`, `tar`
  and `sha256sum`, plus outbound HTTPS to github.com once to fetch mdBook.
- SSH access as `root` to every cluster host with the key named in the
  inventory (`ansible_ssh_private_key_file`). Ansible runs remote tasks
  as root directly; there is no sudo/become. Only the preflight and the
  final health check run locally on the runner.
- The production inventory outside the repository, in the shape of
  `deploy/ansible/inventory.example.yml`. Host names must equal each
  node's `[node].hostname` (or set `dtfs_hostname`); clients carry
  `dtfs_mount_point`.

Project CI/CD variables:

| Variable | Required | Meaning |
|---|---|---|
| `DTFS_ANSIBLE_INVENTORY` | yes | Path on the runner to the production inventory. |
| `DTFS_MANAGER_API_URLS` | yes | Comma list of manager REST endpoints for `verify`, e.g. `mgr01:9402,mgr02:9402,mgr03:9402`. List every voter. |
| `DTFS_RESTART_DELAY_SECS` | no | Settle time after each node (default 30). |
| `DTFS_HEALTH_TIMEOUT_SECS` | no | Health-wait budget per node and for the final check (default 300). |
| `MINISIGN_SECRET_KEY` | no | Protected (not masked: multi-line). Minisign secret key (contents, or a File variable). Enables signing. |
| `MINISIGN_PASSWORD` | no | Protected, masked. Password of that key; empty for a key without one. |
| `DTFS_RELEASE_API_TOKEN` | no | Protected. Project access token (`api`) for `yank:release`. |
| `DTFS_CI_TARGET_DIR` | no | Persistent `CARGO_TARGET_DIR` on the builder so builds stay incremental. |

## Release signing (optional)

With `MINISIGN_SECRET_KEY` set, the `release` job runs
`scripts/ci/sign-release.sh` and uploads `SHA256SUMS.minisig` next to
`SHA256SUMS`. Install `minisign` on the builder host (the runner uses the
shell executor); the job fails if the key is set but `minisign` is missing.
Unset, releases stay unsigned.

Generate the key pair once, on a trusted machine:

```bash
minisign -G -p dtfs-release.pub -s dtfs-release.key   # prompts for a password
```

Store the contents of `dtfs-release.key` as the protected CI
variable (File type works too) `MINISIGN_SECRET_KEY` and the password as `MINISIGN_PASSWORD`
(`-W` makes a password-less key; then leave the password unset). Put the
public key (the second line of `dtfs-release.pub`) into the consumers'
`[updater] minisign_public_key`. Verify by hand with
`minisign -V -p dtfs-release.pub -m SHA256SUMS`.

## Health check

`scripts/dtfs-cluster-healthcheck.sh --managers <list> [--expect-version V]
[--min-data-nodes N] [--timeout S]` is the post-deploy gate used by the playbook's
last task and the `verify` stage; what it asserts is listed in
[Rolling Update](../deployment/rolling-update.md#per-node-procedure). Exit 0
healthy, 1 unhealthy after the timeout, 3 bad arguments. Read-only; needs no
shared secret, only `curl` and `jq`.

## Version stamps

Alpha versions look like `v0.1.9_202609131530`. `dtfs-cli update` and
`dtfs-install.sh --source remote` resolve `latest` to the newest full `vX.Y.Z`
release and skip alphas; an alpha installs only when named with `--tag` or
`--version`. Naming, package URLs, tokens and verification are specified in
[Release Artifacts](../deployment/artifacts.md).
