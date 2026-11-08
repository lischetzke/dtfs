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

## Rolling update order

`deploy/ansible/update-cluster.yml` reads `/api/v1/raft/state` from every
manager first and refuses to start if a manager is unreachable or the
cluster has no single leader. It then updates one node at a time, each
followed by a health wait and a settle delay:

1. managers: learners, then follower voters, then the leader last. Wait:
   REST port up, `running_state == ok`, a leader known.
2. data nodes. Wait: the leader's `/api/v1/nodes` lists the node as
   `Healthy` with the new version.
3. FUSE clients (`dtfs-fuse@<mount>.service`). Wait: the mount point is
   back.

After each node the installed binary must report the new version stamp.
Binaries are installed to `/usr/local/bin`; the previous file is kept as
`<bin>.prev` (one copy, overwritten by the next update, never piling up).
A node already running `dtfs_version` with an active service is skipped, so
a failed run can simply be re-run. A failed wait stops the run at that node;
the nodes before it stay on the new version, the ones after it on the old
one. The final task runs the same health check the `verify` stage repeats.

Run it by hand:

```bash
ansible-playbook -i /etc/dtfs-ci/inventory.yml deploy/ansible/update-cluster.yml \
    -e dtfs_tarball=target/release-artifacts/v0.2.0/dtfs-linux-amd64.tar.gz \
    -e dtfs_version=v0.2.0 -e dtfs_restart_delay_secs=30
```

### Playbook inputs

The playbook has no DTFS-host assumptions beyond the three roles, so it can
be included from a larger Ansible repository. Override with `-e`:

| Variable | Default | Meaning |
|---|---|---|
| `dtfs_version` | required | Expected `dtfs-* --version` stamp, e.g. `v0.2.0`. |
| `dtfs_tarball` | | Path on the control host to `dtfs-linux-amd64.tar.gz`. |
| `dtfs_tarball_url` / `dtfs_repo_token` | | Alternative to `dtfs_tarball`: download URL (Generic Package Registry) and optional GitLab token (`gldt-` deploy token or access token). |
| `dtfs_group_managers` / `dtfs_group_data_nodes` / `dtfs_group_clients` | `managers` / `data_nodes` / `clients` | Inventory group per role; data and client groups may be absent. |
| `dtfs_api_port` | `9402` | Manager REST port (per-host `dtfs_api_port` wins). |
| `dtfs_bin_dir` | `/usr/local/bin` | Binary install dir (per-host `dtfs_bin_dir` wins). |
| `dtfs_restart_delay_secs` | `30` | Settle pause after each updated node. |
| `dtfs_health_timeout_secs` | `300` | Per-node and final health-wait budget. |
| `dtfs_force_update` | `false` | Reinstall and restart even if already on `dtfs_version`. |
| `dtfs_run_healthcheck` | `true` | Run `scripts/dtfs-cluster-healthcheck.sh` at the end (needs the repo's `scripts/` next to the playbook). |

Per-host inventory variables: `ansible_host` (required), `dtfs_hostname`
(identity registered with the managers, default the inventory name),
`dtfs_mount_point` (clients). The per-node tasks live in
`deploy/ansible/tasks/update-node.yml` and are included with `node` and
`role` (`manager`, `data`, `client`).

### Recovering from a failed rollout

1. Stuck or unhealthy node: fix or inspect it, then re-run the same
   command; completed nodes are skipped.
2. Roll a node back: on the node, `cp -p /usr/local/bin/<bin>.prev
   /usr/local/bin/<bin>` for each binary of its role (`dtfs-manager`,
   `dtfs-data` or `dtfs-fuse`, and `dtfs-cli`) and `systemctl restart` the
   unit. Roll back in the reverse order (clients, data nodes, followers,
   leader).
3. Withdraw the release: the `release` job runs before the rollout, so a
   failed deploy leaves it as `latest`. Run the manual job `yank:release`
   in the pipeline (or `scripts/ci/gitlab-yank.sh <version>` in a job).
   It deletes the GitLab release object only; the tag and packages stay and
   the version number is burnt, so ship the fix as the next version.
   Needs `DTFS_RELEASE_API_TOKEN` (project access token, scope `api`,
   Maintainer) unless the instance lets job tokens delete releases.

## Runner prerequisites

On the builder host:

- Rust toolchain in `~/.cargo/bin` for the runner user, plus `protoc`,
  `clang`/`libclang-dev`, `libfuse3-dev`, `pkg-config`, `build-essential`
  (the same set as `docker/buildenv.Dockerfile`).
- `ansible-core` (the playbook uses only `ansible.builtin` modules),
  `curl`, `jq`, `git-cliff` (optional; without it the release notes fall
  back to a one-line description).
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
[--min-data-nodes N] [--timeout S]` polls until every listed manager
reports `running_state == ok` and the same leader, the leader's
`/api/v1/cluster/status` is `ok` with every data node healthy and no
file missing chunks, and (with `--expect-version`) every node that
reports a version reports `V`. Exit 0 healthy, 1 unhealthy after the
timeout, 3 bad arguments. Read-only; needs no shared secret.

## Version stamps

Alpha versions look like `v0.1.9_202609131530`. `dtfs-cli update` and
`dtfs-install.sh --source remote` resolve `latest` to the newest full
`vX.Y.Z` release and skip alphas; an alpha installs only when named with
`--tag`/`--version`. Both read the tarball from the Generic Package
Registry the `release` stage uploads to
(`<repo_url>/api/v4/projects/<group>%2F<project>/packages/generic/dtfs/<tag>/`),
authenticated with `[updater].token` / `DTFS_REPO_TOKEN` on a private
project (deploy token, scope `read_package_registry`).
