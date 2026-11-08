# Integration Tests

Integration tests for DTFS use a multi-node cluster spun up via `scripts/dev-cluster.sh up` (3 managers + 3 data nodes). The numbered scripts predate it and are dormant; they may need their endpoints adjusted (REST on 127.0.0.1:29402/29412/29422).

## Prerequisites

- Docker and Docker Compose
- Built DTFS binaries (via `cargo build --release`)

## Running

There is **no** Cargo integration test binary named `integration` in this workspace. Tests are the **shell scripts** under this directory (see numbered `*.sh` files and `run_all.sh` where present).

```bash
scripts/dev-cluster.sh up
cd tests/integration && ./run_all.sh
cd ../.. && scripts/dev-cluster.sh down
```

## Test Categories

- **cluster_formation**: Verify 3-manager Raft cluster forms and elects leader
- **basic_io**: Write/read files via gRPC, verify data integrity
- **fuse_mount**: Mount DTFS via FUSE, perform POSIX operations
- **replication**: Verify per-tier replication counts
- **failover**: Kill nodes, verify auto re-replication
- **tiering**: Write data, verify temperature classification and tier migration
- **cross_dc**: Read from remote DC, verify transparent fetch + background migration
