# Chaos Tests

Chaos tests verify DTFS resilience under adverse conditions.

## Test Scenarios

- **node_crash**: Kill data nodes with SIGKILL, verify data integrity after recovery
- **network_partition**: Isolate DCs via iptables, verify partition safety lockdown
- **disk_failure**: Simulate disk failures, verify automatic re-replication to healthy disks
- **leader_failover**: Kill Raft leader, verify new election and service continuity
- **concurrent_writes**: Hammer cluster with concurrent writes, verify write safety guarantees
- **migration_crash**: Crash nodes during tier migration, verify no data loss
- **clock_skew**: Introduce clock drift between nodes, verify Raft stability

## Running

```bash
# Dormant harness; a local cluster comes from scripts/dev-cluster.sh up
cargo test --test chaos -- --nocapture
```

These tests are destructive and should only be run against ephemeral test clusters.
