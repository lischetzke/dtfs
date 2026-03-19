# Integration Tests

Integration tests for DTFS use a multi-node cluster spun up via docker-compose.

## Prerequisites

- Docker and Docker Compose
- Built DTFS binaries (via `cargo build --release`)

## Running

```bash
# Build the docker images
cd deploy && docker compose build

# Start the test cluster
docker compose up -d

# Run integration tests
cargo test --test integration

# Tear down
docker compose down -v
```

## Test Categories

- **cluster_formation**: Verify 3-manager Raft cluster forms and elects leader
- **basic_io**: Write/read files via gRPC, verify data integrity
- **fuse_mount**: Mount DTFS via FUSE, perform POSIX operations
- **replication**: Verify per-tier replication counts
- **failover**: Kill nodes, verify auto re-replication
- **tiering**: Write data, verify temperature classification and tier migration
- **cross_dc**: Read from remote DC, verify transparent fetch + background migration
