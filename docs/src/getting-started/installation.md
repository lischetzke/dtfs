# Installation

Two ways to get DTFS onto a host: a release tarball (normal) or a build from
source (development). Installing a whole cluster by automation is specified in
[Deployment automation](../deployment/overview.md); this page covers one host.

## From a release (recommended)

Releases are published to the project's GitLab package registry as
`dtfs-linux-amd64.tar.gz` (see [Release Artifacts](../deployment/artifacts.md)
for layout, checksums and signatures). Linux x86_64 with glibc 2.36 or newer
(Debian 12, Ubuntu 24.04) is required; other prerequisites per role are in
[Host Prerequisites](../deployment/host-prerequisites.md).

The installer unpacks a tarball, installs the binaries and systemd units,
renders a minimal config and starts the service:

```bash
sudo ./scripts/dtfs-install.sh --source remote --version v0.2.0       # download + install
sudo ./scripts/dtfs-install.sh /path/to/dtfs-linux-amd64.tar.gz       # a tarball you verified yourself
sudo ./scripts/dtfs-install.sh --role data --noninteractive --managers m1:9401,m2:9401 --disks /mnt/disk-a,/mnt/disk-b
```

It does not verify checksums or signatures; for anything beyond a lab, verify
the download first. The config it renders is minimal, so a real cluster should
template its own ([Configuration Templating](../deployment/config-templating.md)).

| Flag | Default | Behaviour |
|------|---------|-----------|
| `--source local\|remote\|auto` | `auto` | `local` searches `$DTFS_LOCAL_BIN_DIR`, `./target/release`, `/opt/dtfs/staging/*/` and `~/.cache/dtfs` for `dtfs-<version>-linux-amd64.tar.gz` (a differently named file must be passed as the positional argument). `remote` downloads from `$DTFS_REPO_URL` (default `https://github.com/lischetzke/dtfs`, package registry; a github.com URL uses GitHub releases; `jq` is needed to resolve `latest` on GitLab). `auto` tries local, then remote. |
| `--version <tag>` | `latest` | Release tag; `latest` is the newest full `vX.Y.Z`, alphas are skipped. |
| `--role manager\|data\|fuse-client` | prompt | Host role. |
| `--noninteractive` | off | No prompts; needs `--role` and `--managers`. |
| `--managers h:9401,...` `--cluster-name N` `--hostname H` `--secret S` | prompts | Config values. The secret also comes from `DTFS_SHARED_SECRET`; prefer that, `--secret` is visible in `ps`. |
| `--bootstrap true\|false` | `false` non-interactively | Manager only; true on exactly one manager, first start of a new cluster. |
| `--disks /a,/b` | prompt | Data role; the first disk is `hot`, the rest `cold`. The order becomes the permanent disk ids. |
| `--mountpoint P` | `/mnt/dtfs` | FUSE role. |
| `--reconfigure` | off | Re-render an existing `/etc/dtfs/<role>.toml`; otherwise it and its secret are kept. |
| `--uninstall` | | Remove units and binaries; config and data stay. `--purge --yes` also deletes `/etc/dtfs` and `/var/lib/dtfs` (never data disks). |

Environment: `DTFS_REPO_URL`, `DTFS_REPO_TOKEN` (GitLab deploy token `gldt-...`
or access token for a private project), `DTFS_LOCAL_BIN_DIR`,
`DTFS_SHARED_SECRET`. Downloads are cached in `~/.cache/dtfs` and staged in
`/opt/dtfs/staging/<version>/` (kept after a failed run). Air-gapped: place the
tarball there and use `--source local`. The units run with
`ProtectSystem=strict`; the installer creates `/var/lib/dtfs/<role>` and writes
the data node's `disks.conf` drop-in (disk mount points must exist before the
data node starts). Layout and modes: [Filesystem Layout](../deployment/filesystem-layout.md).
Regression test: `bash scripts/tests/test-dtfs-install.sh`.

## From source

Build in the Docker image `dtfs-buildenv:latest` (Debian 12, Rust 1.94.1, protoc,
clang, libfuse3-dev; `docker/buildenv.Dockerfile`) or on a Linux host with the
same packages. Steps, crate layout and test commands are in
[Building from Source](../development/building.md) and
[Testing](../development/testing.md). Deploy builds must go through the wrapper
scripts so the version and build date are stamped (`scripts/dtfs-build.sh`,
`scripts/dtfs-release.sh <tag>`); a bare `cargo build --release` stamps stale
metadata. The manager and data crates need RocksDB and fuse3 and do not build on
Windows or macOS; use Docker or WSL 2.

The result is four binaries (`dtfs-manager`, `dtfs-data`, `dtfs-fuse`,
`dtfs-cli`) in `target/release/`, or the release tarball layout under
`target/dist/`.

## Next steps

- [Quick Start](./quickstart.md): one node by hand in a few minutes
- [First Cluster](./first-cluster.md): a multi-node cluster walkthrough
- [Deployment automation](../deployment/overview.md): the spec for Ansible-style automation
- [Manager Configuration](../configuration/manager.md): every key
