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
   guards (`scripts/tests/test-ci-version.sh`, `bash -n` over every
   script) and an Ansible syntax check of the deploy playbook. Every
   commit runs this.
3. `build` runs `scripts/dtfs-release.sh $VERSION`: version-stamped
   binaries, `dtfs-linux-amd64.tar.gz`, `SHA256SUMS`, git-cliff notes.
   The result is the `dist/` job artifact.
4. `release` uploads `dist/*` to the project's Generic Package Registry
   (package `dtfs`, version `$VERSION`) and creates the GitLab release,
   which also creates the git tag. Alpha builds carry an "Alpha build"
   banner in the description. Tag pipelines are disabled so this push
   does not start a second pipeline.
5. `deploy` runs `deploy/ansible/update-cluster.yml` against the
   production inventory (see below). One deploy at a time
   (`resource_group: cluster`).
6. `verify` runs `scripts/dtfs-cluster-healthcheck.sh` from the runner
   with `--expect-version $VERSION`.

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
Binaries are installed atomically to `/usr/local/bin` with a timestamped
backup of the previous file (`copy: backup: true`) for a manual rollback.
A failed wait stops the run at that node; the nodes before it stay on
the new version, the ones after it on the old one. The final task runs
the same health check the `verify` stage repeats.

Run it by hand:

```bash
ansible-playbook -i /etc/dtfs-ci/inventory.yml deploy/ansible/update-cluster.yml \
    -e dtfs_tarball=target/release-artifacts/v0.2.0/dtfs-linux-amd64.tar.gz \
    -e dtfs_version=v0.2.0 -e dtfs_restart_delay_secs=30
```

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
| `DTFS_CI_TARGET_DIR` | no | Persistent `CARGO_TARGET_DIR` on the builder so builds stay incremental. |

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
