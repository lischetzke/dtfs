# DTFS

DTFS (Distributed Tiered Filesystem) is a Raft-managed, chunked, tiered
distributed filesystem written in Rust. It is built for one household media
library (Plex) served from several datacenters with very different bandwidth:
one site keeps a durable copy of everything on large slow disks, the others
serve the working set from nearer, faster ones. Clients see an ordinary Linux
mount.

> [!WARNING]
> DTFS is not intended for mission-critical data. Validate your use case in a
> test lab against the exact version you plan to deploy before putting any
> data you care about on it. No guarantee is given, and the author is not
> responsible for data loss.

## How it works

| Component | Binary | Role | Default port |
|---|---|---|---|
| Manager | `dtfs-manager` | Raft peer (3 or 5 per cluster). Owns all metadata: files, chunk locations, nodes, volumes, leases. Serves the REST API and the web dashboard | 9401 gRPC, 9402 REST + web UI |
| Data node | `dtfs-data` | Stores chunk bytes on its disks, one tier per disk (hot, warm, cold, tape). Verifies and scrubs them | 9500 gRPC |
| FUSE client | `dtfs-fuse` | Mounts the filesystem (Linux) and streams chunks directly to and from data nodes | - |
| CLI | `dtfs-cli` | Operator tool over the REST API, plus the self-updater | - |

- **Files are split into chunks.** A chunk's id is the BLAKE3 hash of its
  bytes. The hash is checked on write, on every read (by the data node and
  again by the client) and on every scrub. A bad copy is quarantined, never
  silently deleted.
- **Metadata goes through Raft, bytes do not.** Every metadata change is a
  Raft command; chunk bytes flow only between clients and data nodes.
- **Two kinds of replicas.** *Mandatory* replicas are the durability floor
  and always include the **anchor** copy in the volume's anchor datacenter;
  the repair scheduler maintains them. *Cache* replicas appear near readers
  on demand, are evictable and never count as durability. See
  [Replication classes](docs/src/configuration/volume-config.md#replication-classes).
- **Bounded memory.** Every role has a memory budget, so DTFS can share a
  host with Plex without starving it.

The full picture is in the [Introduction](docs/src/introduction.md) and the
[Architecture overview](docs/src/architecture/overview.md).

## Getting started

| Goal | Read |
|---|---|
| Try it out | [Quick start](docs/src/getting-started/quickstart.md), then [First cluster](docs/src/getting-started/first-cluster.md) |
| Size the hosts | [System requirements](docs/src/getting-started/system-requirements.md) |
| Install by hand (`scripts/dtfs-install.sh`) | [Installation guide](docs/src/getting-started/installation.md) |
| Automate install and updates (Ansible) | [Deployment automation](docs/src/deployment/overview.md) |
| Build and test from source | [Building](docs/src/development/building.md), [Testing](docs/src/development/testing.md) |

The supported toolchain is Linux. The reproducible way to build and test is
the Docker build image:

```bash
docker build -t dtfs-buildenv:latest -f docker/buildenv.Dockerfile .
docker run --rm -v "$PWD:/build" -v dtfs-target:/target \
  -v dtfs-registry:/usr/local/cargo/registry -w /build \
  dtfs-buildenv:latest cargo test --workspace --release
```

On Windows Git Bash, prefix the `docker run` with `MSYS_NO_PATHCONV=1`.

## Operating a cluster

| Task | Read |
|---|---|
| Configure managers, data nodes, FUSE clients and volumes | [Configuration](docs/src/configuration/manager.md) |
| Day-to-day work: nodes, disks, volumes, migrations | [Cluster management](docs/src/operations/cluster-management.md), [Disk management](docs/src/operations/disk-management.md) |
| Watch the cluster | [Monitoring](docs/src/operations/monitoring.md), [Alerting](docs/src/operations/alerting.md), the web dashboard at `http://<manager>:9402/` |
| Something is wrong | [Troubleshooting](docs/src/operations/troubleshooting.md), [Failure scenarios](docs/src/operations/failure-scenarios.md), [Diagnostics MCP server](docs/src/operations/diagnostics-mcp.md) |
| Ship a build to the cluster | [CI pipeline](docs/src/operations/ci-pipeline.md) |
| Script against it | [REST API](docs/src/api-reference/rest-api.md), [CLI reference](docs/src/api-reference/cli-reference.md) |

## Documentation

The docs are an mdBook in `docs/src/` (table of contents:
[`SUMMARY.md`](docs/src/SUMMARY.md)). Build the HTML site with
`scripts/build-docs.sh` (output in `docs/book/`). It checks internal links
first and uses a local mdBook, or downloads the pinned release. CI publishes
the same build to GitHub Pages and the pages to the GitHub Wiki
(`.github/workflows/docs.yml`).

## Repository layout

| Path | Contents |
|---|---|
| `crates/` | Rust workspace: `dtfs-manager`, `dtfs-data`, `dtfs-fuse`, `dtfs-client`, `dtfs-raft`, `dtfs-common`, `dtfs-proto`, `dtfs-cli` |
| `proto/` | Protobuf / gRPC definitions |
| `deploy/` | Example configs, systemd units, the Ansible rolling-update playbook |
| `scripts/` | Build, release, install, healthcheck and docs scripts |
| `docker/` | The build and test image |
| `tools/dtfs-mcp/` | MCP server for diagnosing a live cluster |
| `tests/` | Integration, chaos, performance and lab harnesses |
| `docs/` | Documentation source |

## Contributing

Contributions are welcome. Every fix comes with a test that fails without the
fix and passes with it; every new guard also gets a negative test.

[`CLAUDE.md`](CLAUDE.md) is the binding contract for humans and AI agents
alike: Part A covers the architecture, the integrity invariants and the file
map; Part B covers how a change lands (tests, commit format, where files
live). Read it before any non-trivial change. The
[Contribution guide](docs/src/development/contributing.md) explains the
workflow in more detail. Start with [`CONTRIBUTING.md`](.github/CONTRIBUTING.md)
and the [Code of Conduct](.github/CODE_OF_CONDUCT.md); pull requests get a
checklist from the template.

## Use of AI

AI is allowed and encouraged. DTFS is primarily written by Claude. Every
contributor, human or AI, follows the rules in [`CLAUDE.md`](CLAUDE.md).

## License

[GNU Affero General Public License v3.0](LICENSE).
