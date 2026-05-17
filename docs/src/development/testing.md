# Testing

DTFS uses a multi-layered testing strategy: unit tests, integration tests, chaos tests, and benchmarks. All tests are run through the standard Cargo test infrastructure.

## Unit Tests

Unit tests live alongside the source code in each crate using Rust's built-in `#[cfg(test)]` modules.

### Running All Unit Tests

```bash
cargo test
```

### Running Tests for a Specific Crate

```bash
cargo test -p dtfs-common
cargo test -p dtfs-manager
cargo test -p dtfs-data
cargo test -p dtfs-raft
cargo test -p dtfs-client
```

### Running a Specific Test

```bash
cargo test -p dtfs-common test_hmac_roundtrip
```

### Key Unit Test Areas

| Crate | Test Area | What Is Tested |
|-------|-----------|----------------|
| `dtfs-common` | `crypto` | HMAC-SHA256 roundtrip, CA generation, node certificate signing, nonce uniqueness |
| `dtfs-common` | `hash_ring` | Consistent hashing ring distribution, node addition/removal |
| `dtfs-common` | `checksum` | BLAKE3 checksum computation and verification |
| `dtfs-common` | `config` | TOML parsing, default values, validation |
| `dtfs-manager` | `placement` | Rack-aware placement, failure domain spreading |
| `dtfs-manager` | `audit` | Ring buffer behavior, filtering, capacity limits |
| `dtfs-manager` | `tiering` | Temperature classification, migration decisions |
| `dtfs-data` | `store` | WAL writes, atomic renames, crash recovery |
| `dtfs-data` | `cache` | ARC cache eviction, hit/miss tracking |
| `dtfs-data` | `scrub` | Quick and deep scrub correctness |
| `dtfs-raft` | `state_machine` | Raft state machine apply, snapshot, restore |
| `dtfs-raft` | `lib` (`merge_tests`) | Chunk location merge semantics for `UpdateChunkLocations` |

## Integration Tests

End-to-end checks use **Docker Compose** (see `deploy/`) plus **shell scripts** under `tests/integration/` (and related `tests/performance/`, `tests/chaos/`). There is **no** workspace-root `tests/*.rs` harness yet.

### Running Shell / Docker Integration Flow

```bash
cd deploy && docker compose build && docker compose up -d
# Then run scripts from tests/integration/ (e.g. ./run_all.sh when available)
docker compose down -v
```

### Key scenarios (targets for scripts / manual QA)

- **Node join flow** — gRPC `RegisterNode` with `shared_secret`, or REST `POST /api/v1/join/*` HMAC flow.
- **File write/read roundtrip** — create file, allocate chunks, write to data nodes, `CommitChunkLocations`, read back.
- **Replication** — multiple placements / scheduler-driven copies.
- **Scrub detection** — corrupt chunk, deep scrub, re-replication.
- **Configuration propagation** — REST config updates and heartbeat-driven data-node config.

## Chaos Tests

Chaos tests simulate failure conditions to verify that DTFS recovers correctly. These tests are typically longer-running and are gated behind a feature flag or test filter.

### Running Chaos Tests

```bash
cargo test chaos -- --ignored
```

Chaos tests are marked with `#[ignore]` by default so they do not run in the standard `cargo test` cycle.

### Chaos Test Scenarios

| Test | Description |
|------|-------------|
| Node crash during write | Kill a data node mid-write and verify WAL recovery on restart |
| Leader failover | Stop the Raft leader and verify a new leader is elected and operations resume |
| Network partition | Simulate a partition between data nodes and verify that reads fall back to available replicas |
| Disk failure | Mark a disk as failed and verify chunks are re-replicated to healthy disks |
| Concurrent joins | Multiple nodes attempt to join simultaneously; all should succeed without deadlock |

## Benchmarks

Performance benchmarks use Rust's built-in benchmark framework or `criterion` for more detailed statistical analysis.

### Running Benchmarks

```bash
cargo bench
```

### Running Benchmarks for a Specific Crate

```bash
cargo bench -p dtfs-data
cargo bench -p dtfs-common
```

### Key Benchmark Areas

| Crate | Benchmark | What Is Measured |
|-------|-----------|------------------|
| `dtfs-common` | `checksum` | BLAKE3 hashing throughput at various chunk sizes |
| `dtfs-common` | `hash_ring` | Consistent hash ring lookup latency |
| `dtfs-data` | `store` | Sequential and random chunk write/read throughput |
| `dtfs-data` | `cache` | ARC cache lookup latency under contention |
| `dtfs-raft` | `state_machine` | State machine apply throughput |

## Test Configuration

Some tests require specific environment variables or configuration:

| Variable | Purpose |
|----------|---------|
| `DTFS_TEST_DATA_DIR` | Directory for temporary test data (defaults to a `tempfile`-managed directory) |
| `RUST_LOG` | Log level for test output (e.g., `RUST_LOG=dtfs_manager=debug cargo test`) |

## Continuous Integration

The recommended CI pipeline runs the following stages in order:

1. **Format check** -- `cargo fmt --check`
2. **Lint** -- `cargo clippy -- -D warnings`
3. **Unit tests** -- `cargo test`
4. **Integration tests** -- `cargo test --test '*'`
5. **Chaos tests** (nightly/weekly) -- `cargo test chaos -- --ignored`
6. **Benchmarks** (optional, for regression tracking) -- `cargo bench`
