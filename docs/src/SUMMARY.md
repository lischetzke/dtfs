# Summary

[Introduction](./introduction.md)

# Getting Started

- [System Requirements](./getting-started/system-requirements.md)
- [Installation](./getting-started/installation.md)
- [Quick Start](./getting-started/quickstart.md)
- [First Cluster](./getting-started/first-cluster.md)

# Deployment automation

- [Overview](./deployment/overview.md)
- [Release Artifacts](./deployment/artifacts.md)
- [Host Prerequisites](./deployment/host-prerequisites.md)
- [Filesystem Layout and Units](./deployment/filesystem-layout.md)
- [Configuration Templating](./deployment/config-templating.md)
- [Fresh Install Order](./deployment/fresh-install.md)
- [Rolling Update and Rollback](./deployment/rolling-update.md)
- [Decommission and Replace](./deployment/decommission-replace.md)
- [Role Variables and Idempotency](./deployment/role-variables.md)

# Architecture

- [Overview](./architecture/overview.md)
- [Data Flow](./architecture/data-flow.md)
- [Consensus (Raft)](./architecture/consensus.md)
- [Data Distribution](./architecture/data-distribution.md)

# Configuration

- [Manager Configuration](./configuration/manager.md)
- [Data Node Configuration](./configuration/data-node.md)
- [FUSE Client Configuration](./configuration/fuse-client.md)
- [Cluster-wide Configuration](./configuration/cluster-config.md)
- [Cluster vs Node-scoped Keys](./configuration/scope.md)
- [Volume Configuration](./configuration/volume-config.md)
- [Live Reload](./configuration/live-reload.md)

# Operations

- [Cluster Management](./operations/cluster-management.md)
- [CI Pipeline: Test, Release, Deploy](./operations/ci-pipeline.md)
- [Disk Management](./operations/disk-management.md)
- [Orphan Chunk Cleanup](./operations/orphan-cleanup-runbook.md)
- [Maintenance Mode](./operations/maintenance-mode.md)
- [Monitoring](./operations/monitoring.md)
- [Log Shipping (Filebeat → Elastic)](./operations/log-shipping.md)
- [Alerting](./operations/alerting.md)
- [Backup & Recovery](./operations/backup-recovery.md)
- [Time Synchronisation](./operations/time-sync.md)
- [Failure Scenarios](./operations/failure-scenarios.md)
- [Troubleshooting](./operations/troubleshooting.md)
- [Live Diagnostics (MCP)](./operations/diagnostics-mcp.md)

# Storage

- [Tiering](./storage/tiering.md)
- [Replication](./storage/replication.md)
- [Scrubbing](./storage/scrubbing.md)
- [Caching](./storage/caching.md)
- [Migration](./storage/migration.md)
- [Tape Storage (Frozen Tier)](./storage/tape.md)

# Access

- [FUSE Mount](./access/fuse-mount.md)
- [Read-Ahead Modes](./access/read-ahead.md)
- [Partition Behavior](./access/partition-behavior.md)

# Security

- [TLS](./security/tls.md)
- [Cluster Join Protocol](./security/cluster-join.md)
- [Authentication](./security/authentication.md)
- [Audit Log](./security/audit-log.md)

# API Reference

- [REST API](./api-reference/rest-api.md)
- [gRPC API](./api-reference/grpc-api.md)
- [CLI Reference](./api-reference/cli-reference.md)

# Lab (dormant — no cluster since 2026-09)

- [Production Deployment Runbook](./lab/production-deployment.md)
- [Release Testing](./lab/release-testing.md)
- [Chaos / Cross-DC / Balance Testing](./lab/chaos-testing.md)

# Development

- [Building from Source](./development/building.md)
- [Testing](./development/testing.md)
- [Contributing](./development/contributing.md)

# Internals

- [Invariants & Bug Archaeology](./internals/invariants.md)
  - [Archive: removed contract text (2026-09)](./internals/archive/agents-md-2026-09.md)
