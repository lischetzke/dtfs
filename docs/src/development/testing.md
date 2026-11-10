# Testing

All verification runs as unit and crate tests inside the Docker build image ([Building from Source](./building.md)). There is no build VM and no lab cluster; behavior that only exists on a real cluster is reasoned about and noted in the commit body, not executed.

## Running tests

```bash
MSYS_NO_PATHCONV=1 docker run --rm -v "<path-to-checkout>:/build" \
  -v dtfs-target:/target -v dtfs-registry:/usr/local/cargo/registry \
  -w /build dtfs-buildenv:latest cargo test -p <crate> --release
```

- Always `--release`. Run the crate you changed; run the whole workspace (`cargo test --workspace --release`) for cross-crate changes.
- After any config struct or example change, run `cargo test -p dtfs-common --release`: the `example_*_toml_parses` tests check every shipped example. If one flakes while another container holds the shared build lock, rerun it once.
- A single test: append the test name after `--` (`cargo test -p dtfs-raft --release posix_path_regression`).

## Where tests live

Unit tests sit beside the code in `#[cfg(test)]` modules or `tests.rs` files (`dtfs-raft/src/state_machine/tests.rs`, `dtfs-manager/src/scheduler/tests.rs`, `dtfs-manager/src/api/tests.rs`). `crates/dtfs-data/tests/` holds loopback integration tests such as mutual TLS.

## Conventions

1. **Test the purest layer.** Prefer, in order: a pure helper, then a state-machine `apply` test, then a manager aggregator, then a FUSE helper. Planners (`plan_cache_tick`, `plan_cache_promotion`) and decision functions (`demote_preserves_anchor`) are pure on purpose.
2. **Red, green, suite.** Write the failing test, watch it fail for the right reason, make the smallest fix, watch it pass, then run the crate suite.
3. **Snapshot compatibility.** Every new field on `StateMachineData` carries `#[serde(default)]` and gets a regression test (`state_machine::posix_path_regression` or `snapshot_persistence_tests`). A new replicated timestamp carries `#[serde(default)] now_ms` and falls back to the log index when `0`.
4. **Durability and anchor invariants** get a property-style test over random operation sequences, not just examples.
5. **Cluster-only behavior** (failover timing, WAN behavior) cannot run here. Say so in the commit body instead of adding a test that cannot execute.
6. **No wall clock in `apply`.** A test that would need `SystemTime` inside apply is a sign the field should come from the proposer.

## Dormant harnesses

`tests/release/` (the lab release gate, chaos cases included; needs a lab inventory, see [Lab: Chaos Testing](../lab/chaos-testing.md) and [Release Testing](../lab/release-testing.md)) and `tests/integration/` (shell scripts that target the local dev cluster below) are the only cluster harnesses left. They are not part of the verification loop and need no change to ship a fix. CI (`.github/workflows/ci.yml`) starts no cluster: runtime behaviour is checked by hand on `scripts/dev-cluster.sh` (`empty-files`, `big-file`, `nfs-check`). The unreferenced Ansible and performance harnesses and the stale chaos README were removed (#137).

The host-side Python tooling under `tools/dtfs-mcp` is tested separately (`uv run --with pytest --with "mcp<2" --with httpx pytest tools/dtfs-mcp`), outside the cargo loop.

## Local dev cluster

`scripts/dev-cluster.sh` (needs only docker and bash; works from Git Bash and Linux) runs a real 3-manager, 3-data-node cluster in Docker for runtime checks that crate tests cannot give. Binaries are built in the build image and copied into the `dtfs-dev-bin` volume, so later builds never swap binaries under running daemons. Everything is named `dtfs-dev-*`; the network is `dtfs-dev-net` (172.31.250.0/24).

| Command | What it does |
|---|---|
| `up [--no-build]` | Build, write configs, start `mgr01-03` (leli/toro/nrb) and `data01` (leli cold), `data02` (toro warm), `data03` (nrb warm); wait for 3 healthy data nodes |
| `deploy` | Rebuild, copy binaries, restart all daemons (state kept), wait healthy |
| `status` | Container states plus the `/api/v1/cluster/status` summary |
| `fuse [cmd...]` | Start or reuse the FUSE client container; run `cmd` inside it (mount at `/mnt/dtfs`) |
| `nfs-check` | Re-export the FUSE mount over NFSv3 in a privileged container: small file, 10 MB `cmp` via NFS and FUSE, rename, remove, `cmp` again after dropping caches; PASS/FAIL per step, non-zero exit on failure |
| `empty-files` | Regression check for v0.1.8 (empty and sparse files must not make `GET /api/v1/cluster/status` report "degraded"). Asserts a clean baseline on mgr01/02/03 (status `ok`, `files_missing_chunks` 0), then in a unique `/mnt/dtfs/empty-check-<ts>` creates empty files via `touch`, `: >`, write-then-truncate, `truncate -s 10M` (sparse) and `mknod` (the knfsd create path), plus `touch`/`: >` over an NFS re-export. Re-asserts status on every manager, checks the mtime of `a` and `e` is not 1970, then cleans up (`exportfs -u`, `umount`, `fusermount3 -u`). PASS/FAIL per step, non-zero exit on failure |
| `big-file <GiB>` | Opt-in: stream random data through FUSE while hashing, read back, copy inside DTFS, compare; prints MB/s per phase and peak RSS of `dtfs-fuse`. Needs free disk of about size x replicas x 2 |
| `precache-check` | Predictive pre-caching end to end. Needs a cluster built with `DTFS_DEV_VERSION=v0.1.16` (the policy is gated; older stamps get 409). Creates volume `media` (mandatory cold 1, anchor leli cold, warm cache 1/1 per-DC, promote window 600 s, Plex pre-cache 1 GiB / 1 series), starts a toro FUSE client (`dtfs-dev-fuse-toro`, `volume_id = "media"`), writes `/tv/Show` (E01-E05, 300 MiB, `.plexmatch`), `/tv/Other` (2 x 100 MiB) and `/movies/{A,B,C}`, restarts the client (empty cache) and plays E01 from the middle. PASS criteria: E02 and E03 fully on `data02`, E01 not in the series file list, series root `/tv/Show`; a one-chunk read of `/movies/A.mkv` creates no cursor; playing `/tv/Other` replaces `/tv/Show` under `max-series 1`; every file keeps a `data01` anchor row on every chunk. Takes about 10 minutes. The dev cluster sets `[tiering] precache_max_mbps = 200`. Cannot show real 40 Mbps link behaviour or Plex scanner reads |
| `down` | Remove all containers, the network and volumes (state is lost) |

Manager REST is published on host ports 29402 (mgr01), 29412 (mgr02), 29422 (mgr03). TLS is disabled and the shared secret is a fixed dev value. `DTFS_DEV_TARGET_VOLUME` selects the cargo target volume (default `dtfs-target`).

What it proves: the release binaries start, form a Raft quorum, register data nodes, and serve real writes, reads and replication through FUSE and an NFS re-export. What it cannot: there is no systemd, journald or sshd, so the MCP `host_*` tools and Ansible do not apply; all nodes share one host, so WAN behaviour and failover timing are not exercised.

## Environment

`RUST_LOG=dtfs_manager=debug` raises log output for a failing test. `DTFS_TEST_DATA_DIR` overrides the temp directory some data-node tests use.
