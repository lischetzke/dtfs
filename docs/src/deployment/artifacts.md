# Release Artifacts

Sources: `scripts/dtfs-release.sh`, `scripts/dtfs-build.sh`,
`scripts/ci/{version,gitlab-release,sign-release,gitlab-yank}.sh`,
`crates/dtfs-cli/src/updater.rs`.

## What a release consists of

Files published per version, all under one package version:

| File | Always | Content |
|---|---|---|
| `dtfs-linux-amd64.tar.gz` | yes | The only tarball name consumers use. Linux x86_64 only. |
| `SHA256SUMS` | yes | One line: `<sha256>  dtfs-linux-amd64.tar.gz` (output of `sha256sum`, so `sha256sum -c SHA256SUMS` works in the download directory). |
| `SHA256SUMS.minisig` | only if the CI variable `MINISIGN_SECRET_KEY` was set | minisign signature over `SHA256SUMS`. |

`notes.md` (git-cliff release notes) is built but not uploaded as a file; it is
the description of the GitLab release.

Inside the tarball (single top-level directory `dtfs/`):

```text
dtfs/bin/dtfs-manager
dtfs/bin/dtfs-data
dtfs/bin/dtfs-fuse
dtfs/bin/dtfs-cli
dtfs/systemd/dtfs-manager.service
dtfs/systemd/dtfs-data.service
dtfs/systemd/dtfs-fuse@.service
dtfs/configs/*.toml            # sample configs from deploy/configs/
dtfs/MANIFEST                  # build info, then "<sha256>  <binary>" per file in bin/
```

`MANIFEST` is the per-binary checksum list that `dtfs-cli update` verifies
before it installs anything. The `systemd/` units are the same files as
`deploy/systemd/` in the repository at that revision.

## Where they are published

Primary: the GitLab Generic Package Registry of the project, package name
`dtfs`, package version = release tag:

```text
<repo_url>/api/v4/projects/<group>%2F<project>/packages/generic/dtfs/<tag>/<file>
# default repo_url: https://github.com/lischetzke/dtfs
# example: https://gitlab.example.invalid/api/v4/projects/example%2Fdtfs/packages/generic/dtfs/v0.2.0/dtfs-linux-amd64.tar.gz
```

Authentication for a private project, sent as an HTTP header (the installer and
`dtfs-cli update` choose by prefix):

| Token | Header |
|---|---|
| Deploy token `gldt-...`, scope `read_package_registry` | `DEPLOY-TOKEN: <token>` |
| Project or personal access token `glpat-...`, scope `read_api` | `PRIVATE-TOKEN: <token>` |

Redirects: the registry may redirect to object storage. Do not forward the token
header to the redirect target (`dtfs-cli` follows redirects hop by hop and drops
it after the first hop). With `curl`, do the first request with the header and
`--max-redirs 0`, then fetch the `Location` URL without it; Ansible
`get_url` with `headers:` as `update-cluster.yml` does works when the instance
serves the file directly.

Secondary: `scripts/dtfs-release.sh <tag> --publish` creates a GitHub release
(`https://github.com/lischetzke/dtfs`) with `dtfs-linux-amd64.tar.gz` and
`SHA256SUMS`. A `github.com` repo URL makes the installer and updater use
`releases/latest/download/` and `releases/download/<tag>/`. CI does not create
GitHub releases and no `.minisig` is attached there; treat GitLab as the source
of record.

## Version naming

| Kind | Tag / version string | Created by | Resolves as `latest` |
|---|---|---|---|
| Release | `vX.Y.Z` (three numbers) | commit subject starting `release vX.Y.Z` (the `v` is optional in the subject; the tag always has it) | yes |
| Alpha | `v<base>_<YYYYMMDDHHmm>` (UTC), `<base>` = newest `vX.Y.Z` tag, for example `v0.1.9_202609131530` | commit subject starting `deploy`, or a manual pipeline with `DTFS_DEPLOY_MODE=deploy` | no, only when named explicitly; the GitLab release is marked as an alpha build |
| Dev build | `v0.0.0-dev` | any build without `DTFS_VERSION` | never published |

`latest` is resolved by listing `GET <api>/releases?per_page=50` and taking the
first `tag_name` that matches `^v[0-9]+\.[0-9]+\.[0-9]+$` (needs `jq`). Pin an
explicit tag in automation; resolve `latest` only for interactive use.

The version is stamped into every binary. The check an automation should use:

```bash
/usr/local/bin/dtfs-cli --version
# dtfs-cli v0.2.0 (built 20260913-153012)
```

The format is `<binary-name> <version> (built <UTC YYYYMMDD-HHmmss>)`; every
daemon prints its own binary name first (`dtfs-manager --version`, and so on).
The playbook compares with the regex `^<binary> <version> ` (note the
trailing space, so `v0.2.0` does not match `v0.2.0_2026...`). Managers and data
nodes also report the same version string to the cluster:
`GET /api/v1/nodes` returns `version` and `build_date` per node.

## How to verify a download

Do all of this on the control host, before anything is copied to a node.

1. Download `dtfs-linux-amd64.tar.gz`, `SHA256SUMS` and, if you require
   signatures, `SHA256SUMS.minisig` into one empty directory.
2. Signature (when a public key is configured; the key is the second line of
   `dtfs-release.pub`, a base64 string starting `RW`):

   ```bash
   minisign -V -P '<public-key-base64>' -m SHA256SUMS      # reads SHA256SUMS.minisig
   # or with a key file: minisign -V -p dtfs-release.pub -m SHA256SUMS
   ```

   A missing `.minisig` while a key is configured is a hard failure, not a
   warning (`dtfs-cli update` behaves the same: it fetches `.minisig` and aborts
   on any verification error; without a key it only prints a warning).
3. Checksum of the tarball against the signed list:
   `sha256sum -c SHA256SUMS` (the file name inside must be
   `dtfs-linux-amd64.tar.gz`).
4. Unpack and check the stamp:

   ```bash
   tar -xzf dtfs-linux-amd64.tar.gz
   ./dtfs/bin/dtfs-cli --version      # must start with: dtfs-cli <expected tag> 
   (cd dtfs/bin && awk 'NF==2 && length($1)==64 {print $1"  "$2}' ../MANIFEST | sha256sum -c -)
   ```

   Running the packaged `dtfs-cli` on the control host needs Linux x86_64; on
   another OS compare the version through the nodes after install instead.
5. Distribute the already verified files (the playbook copies
   `dtfs/bin/<binary>` to each node; no download happens on the nodes).

Where the public key lives for consumers: `[updater] minisign_public_key` in
any role config (read only by `dtfs-cli update`, ignored by the daemons) or the
environment variable `DTFS_MINISIGN_PUBKEY`, which wins. Generating and
protecting the signing key: [CI Pipeline](../operations/ci-pipeline.md#release-signing-optional).

## Yanked releases

A failed rollout is handled by deleting the GitLab release object
(`yank:release` job, `scripts/ci/gitlab-yank.sh <tag>`); the tag and the package
files stay downloadable by exact URL but `latest` no longer resolves to them.
A role that pins an explicit tag must therefore not assume a published tag is
still the current one: check `GET <api>/releases/<tag>` returns 200 before a
fleet-wide run, and never reuse a yanked version number (the fix ships as the
next version).
