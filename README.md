# DTFS

Distributed tiered file storage: **Raft** managers (metadata), **chunk** data nodes, **gRPC** data plane, optional **FUSE** client (Linux), **REST + WebUI** on managers.

DTFS separates replication into a **mandatory anchor** copy (the durable
floor; under-replication scheduler enforces it) and **opportunistic
cache** copies created on read demand by the tiering engine (freely
evictable; operator can pre-emptively drop them via
`POST /api/v1/files/flush_cache` to reclaim warm storage). See
[Volume Configuration](docs/src/configuration/volume-config.md#replication-classes)
for the schema and worked examples.

> [!WARNING]
> **Deployment notice — please read.**
>
> Always validate your specific use case in a test lab against the exact version you intend to deploy, **before** putting DTFS in front of any data you care about. The author is not responsible for any data loss. **DTFS is not intended for mission-critical data and must not be used for that purpose.** No guarantee is given that everything will work without issues.

## Documentation

| What you need | Where to look |
|---|---|
| Get something running fast | [Quick Start](docs/src/getting-started/quickstart.md) |
| Full build and install steps | [Installation Guide](docs/src/getting-started/installation.md) |
| Tune managers, data nodes, FUSE, volumes | [Advanced Configuration](docs/src/configuration/) |
| Diagnose problems on a live cluster | [Debugging / Troubleshooting](docs/src/operations/troubleshooting.md) |
| Contribute code or docs | [Contribution Guide](docs/src/development/contributing.md) |

Full table of contents: [`docs/src/SUMMARY.md`](docs/src/SUMMARY.md).

## Repository Layout

- `crates/` — Rust workspace (`dtfs-manager`, `dtfs-data`, `dtfs-client`, `dtfs-fuse`, `dtfs-raft`, …)
- `proto/` — Protobuf / gRPC definitions
- `deploy/` — Docker Compose, sample configs, systemd units
- `docs/` — mdBook documentation source
- `plans/` — active plans, completed plans, incidents, reports (see [plans/README.md](plans/README.md))
- `changelog/` — per-release changelog entries (see [changelog/README.md](changelog/README.md))
- `tests/` — integration, chaos, performance, and lab harness
- [`CLAUDE.md`](CLAUDE.md) — codebase-specific knowledge for agents (architecture, integrity invariants, memory model, bug catalog)
- [`AGENTS.md`](AGENTS.md) — operating discipline for contributors and AI agents

The authoritative table of *where each kind of artefact lives* is in [AGENTS.md § Structure Discipline](AGENTS.md#structure-discipline). Read that before adding a file at a new path.

## Contribution

Contributions are welcome. **Every fix must come with a working test case that demonstrates the bug and proves the fix is correct.** The procedure is defined in [AGENTS.md § Fixing Procedure](AGENTS.md#fixing-procedure):

1. Write a test that fails because of the issue.
2. Implement the fix.
3. Verify the test now passes (and that existing tests still pass).

Before any non-trivial change, read [AGENTS.md](AGENTS.md) (contributor and AI-agent operating discipline) and [CLAUDE.md](CLAUDE.md) (architecture, invariants, file map). For style, PR process, and where things live, see the [Contribution Guide](docs/src/development/contributing.md).

## Use of AI

AI is allowed and encouraged on this project. DTFS is **primarily authored by Claude Opus**. Any AI agent — and any human using one — contributing code or docs **must follow every rule defined in [AGENTS.md](AGENTS.md)**, including the fixing procedure above, the plan-driven workflow, and the commit / test discipline.

## License

DTFS is licensed under the [GNU Affero General Public License v3.0](LICENSE).
