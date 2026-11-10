# Installation

Two ways to get DTFS onto a host: a release tarball (normal) or a build from
source (development). Installing a whole cluster by automation is specified in
[Deployment automation](../deployment/overview.md); this page covers one host.

## From a release (recommended)

Releases are published as GitHub releases of `lischetzke/dtfs` with the asset
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

Before installing, the installer checks every binary against the tarball's
`dtfs/MANIFEST` sha256 list and refuses to install on a mismatch or a missing
`MANIFEST`. That detects corruption, not forgery: for authenticity pass
`--minisign-key` (below), otherwise it prints a warning that the release is not
verified. The config it renders is minimal, so a real cluster should
template its own ([Configuration Templating](../deployment/config-templating.md)).

| Flag | Default | Behaviour |
|------|---------|-----------|
| `--source local\|remote\|auto` | `auto` | `local` searches `$DTFS_LOCAL_BIN_DIR`, `/opt/dtfs/staging/*/` and `~/.cache/dtfs` for `dtfs-<version>-linux-amd64.tar.gz` (a differently named file must be passed as the positional argument); without `--version` it takes the newest by version sort. `remote` downloads from `$DTFS_REPO_URL` (default `https://github.com/lischetzke/dtfs`, GitHub releases; other hosts are refused). `auto` without `--version` resolves the latest tag from the remote first, reuses a local tarball of exactly that tag or downloads it, and falls back to the newest local tarball (with a warning) only when the remote cannot be reached. A local tarball that no tag pinned is refused when it is older than the installed `dtfs-cli`. |
| `--version <tag>` | `latest` | Release tag; `latest` is the newest full `vX.Y.Z`, alphas are skipped. The staged `dtfs-cli --version` must report this tag (or the tag `latest` resolved to), else the install is refused. |
| `--role manager\|data\|fuse-client` | prompt | Host role. |
| `--noninteractive` | off | No prompts; needs `--role` and `--managers`. |
| `--managers hostname=addr:9401,...` `--cluster-name N` `--hostname H` `--secret S` | prompts | Config values. The secret also comes from `DTFS_SHARED_SECRET`; prefer that, `--secret` is visible in `ps`. For the manager role each entry must be `hostname=addr:port`, where `hostname` equals that manager's `[node].hostname`; a bare address is refused, except one self entry with `--bootstrap true`. Data and FUSE roles accept the same form and ignore the name. |
| `--minisign-key K` | `$DTFS_MINISIGN_PUBKEY` | minisign public key (base64 line or a `.pub` file path). Needs the `minisign` binary and `SHA256SUMS` + `SHA256SUMS.minisig` beside the tarball (or at the release URL for `--source remote`); verifies the signature, then that the tarball sha256 is listed. Any failure refuses the install. |
| `--chown-disks` | off | Data role: `chown dtfs:dtfs` each disk path. Without it the installer only prints the `chown` command when the `dtfs` user cannot write a disk. |
| `--bootstrap true\|false` | `false` non-interactively | Manager only; true on exactly one manager, first start of a new cluster. |
| `--disks /a,/b` | prompt | Data role; the first disk is `hot`, the rest `cold`. The order becomes the permanent disk ids. |
| `--mountpoint P` | `/mnt/dtfs` | FUSE role. |
| `--reconfigure` | off | Re-render an existing `/etc/dtfs/<role>.toml`; otherwise it and its secret are kept. |
| `--uninstall` | | Remove units and binaries; config and data stay. `--purge --yes` also deletes `/etc/dtfs` and `/var/lib/dtfs` (never data disks). |

Environment: `DTFS_REPO_URL`, `DTFS_REPO_TOKEN` (optional GitHub token for rate
limits or a private fork), `DTFS_LOCAL_BIN_DIR`,
`DTFS_SHARED_SECRET`. Downloads are cached in `~/.cache/dtfs`. A tarball that passed the `MANIFEST`
and version checks is kept as
`/opt/dtfs/staging/<version>/dtfs-<version>-linux-amd64.tar.gz`, `<version>`
being what its binaries report. Air-gapped: place the
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
