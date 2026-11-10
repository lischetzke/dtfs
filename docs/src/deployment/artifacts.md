# Release Artifacts

Sources: `scripts/dtfs-release.sh`, `scripts/dtfs-build.sh`,
`scripts/ci/{version,sign-release}.sh`, `.github/workflows/release.yml`,
`crates/dtfs-cli/src/updater.rs`.

## What a release consists of

Files attached to the GitHub release of each version:

| File | Always | Content |
|---|---|---|
| `dtfs-linux-amd64.tar.gz` | yes | The only tarball name consumers use. Linux x86_64 only. |
| `SHA256SUMS` | yes | One line: `<sha256>  dtfs-linux-amd64.tar.gz` (output of `sha256sum`, so `sha256sum -c SHA256SUMS` works in the download directory). |
| `SHA256SUMS.minisig` | only if the repository secret `MINISIGN_SECRET_KEY` was set | minisign signature over `SHA256SUMS`. |

`notes.md` (git-cliff release notes) is built but not uploaded as a file; it is
the body of the GitHub release.

Inside the tarball (single top-level directory `dtfs/`):

```text
dtfs/bin/dtfs-manager
dtfs/bin/dtfs-data
dtfs/bin/dtfs-fuse
dtfs/bin/dtfs-cli
dtfs/systemd/dtfs-manager.service
dtfs/systemd/dtfs-data.service
dtfs/systemd/dtfs-fuse@.service
dtfs/MANIFEST                  # build info, then "<sha256>  <binary>" per file in bin/
```

`MANIFEST` is the per-binary checksum list that `dtfs-cli update` verifies
before it installs anything. The `systemd/` units are the same files as
`deploy/systemd/` in the repository at that revision.

## Where they are published

GitHub releases of `https://github.com/lischetzke/dtfs`, created by
`.github/workflows/release.yml` (or by hand with
`scripts/dtfs-release.sh <tag> --publish`, which runs the same
`gh release create`). The tag is created with the release:

```text
https://github.com/lischetzke/dtfs/releases/download/<tag>/<file>
https://github.com/lischetzke/dtfs/releases/latest/download/<file>
```

The repository is public: no token is needed to download. GitHub redirects
asset downloads to object storage; `curl -L`, `dtfs-cli`, the installer and
Ansible `get_url` all follow it.

## Version naming

| Kind | Tag / version string | Created by | Resolves as `latest` |
|---|---|---|---|
| Release | `vX.Y.Z` (three numbers) | commit subject starting `release vX.Y.Z` on master (the `v` is optional in the subject; the tag always has it), or the `release` workflow started by hand with a `version` | yes |
| Dev build | `v0.0.0-dev` | any build without `DTFS_VERSION` | never published |

There are no alpha or per-commit builds and CI never deploys. `latest` is the
release GitHub marks as latest (`releases/latest`). Pin an explicit tag in
automation; resolve `latest` only for interactive use.

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
any role config (read only by `dtfs-cli update`, ignored by the daemons), the
environment variable `DTFS_MINISIGN_PUBKEY`, or `dtfs-cli update --minisign-key`;
precedence is flag, then environment, then config. `scripts/dtfs-install.sh
--minisign-key <base64-or-.pub-path>` (or the same environment variable) does
the same check at install time: it needs the `minisign` binary and
`SHA256SUMS` + `SHA256SUMS.minisig` beside the tarball (or at the release URL
for a remote download), requires the tarball sha256 to be listed, and refuses
to install on any failure. Without a key the installer only warns.

`SHA256SUMS` carries no version, so a signature alone does not tell one
release from another. Both consumers therefore read the staged binaries'
`--version` after the `MANIFEST` check (the binaries are covered by the
signature through `MANIFEST` and the tarball sha256): `dtfs-cli update --tag T`
and `dtfs-install.sh --version T` (or the tag a remote install resolved
`latest` to) refuse a release whose binaries report another version, and
`dtfs-cli update` without `--tag` refuses a release older than the installed
binary. Generating and
protecting the signing key: [CI Pipeline](../operations/ci-pipeline.md#release-signing-optional).

## Yanked releases

A failed rollout is handled by deleting the GitHub release object
(`gh release delete <tag>`, see [CI Pipeline](../operations/ci-pipeline.md#recovering-from-a-failed-rollout));
the tag stays (`gh release delete` without `--cleanup-tag`) but `latest` no longer
resolves to the release and its assets are gone.
A role that pins an explicit tag must therefore not assume a published tag is
still the current one: check `gh release view <tag>` succeeds before a
fleet-wide run, and never reuse a yanked version number (the fix ships as the
next version).
