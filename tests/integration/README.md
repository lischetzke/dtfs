# Integration Tests

Integration tests for DTFS use a multi-node cluster spun up via docker-compose.

## Prerequisites

- Docker and Docker Compose
- Built DTFS binaries (via `cargo build --release`)

## Running

There is **no** Cargo integration test binary named `integration` in this workspace. Tests are the **shell scripts** under this directory (see numbered `*.sh` files and `run_all.sh` where present).

```bash
# Small lab (1 manager + 2 data) — preferred for local Docker verification
cd deploy && docker compose -f docker-compose.lab.yml up -d --build

# Full multi-DC compose (3 managers + 4 data); needs cross-network join config
# to form a real Raft quorum — see docker-compose.yml
docker compose up -d --build

# Run shell integration scripts (example — see scripts for exact usage)
cd ../tests/integration && ./run_all.sh

# Tear down
cd ../../deploy && docker compose -f docker-compose.lab.yml down -v
```

Builder image is `rust:1.88-bookworm` (Cargo.lock needs ≥1.88). Data services run as root in compose so named volumes can accept disk UUID writes.

## Test Categories

- **cluster_formation**: Verify 3-manager Raft cluster forms and elects leader
- **basic_io**: Write/read files via gRPC, verify data integrity
- **fuse_mount**: Mount DTFS via FUSE, perform POSIX operations
- **replication**: Verify per-tier replication counts
- **failover**: Kill nodes, verify auto re-replication
- **tiering**: Write data, verify temperature classification and tier migration
- **cross_dc**: Read from remote DC, verify transparent fetch + background migration
