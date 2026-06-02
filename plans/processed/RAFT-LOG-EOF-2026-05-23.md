# Raft log "unexpected end of file" — RCA (AV-5-2)

**Status:** Root cause confirmed and code-fixed.
**Resolved by:** AV-5-11 (JSON-first codec + bincode fallback in
`log_store.rs`) for the primary cause; the wipe-and-rebootstrap runbook
below remains the recovery path for unrelated corruption.
**Code fix:** **AV-5-11** landed (log format change). BG-6 (2026-05-28)
added complementary defence-in-depth for the *other* EOF source — a
torn WAL tail from an unclean shutdown — via
`DBRecoveryMode::TolerateCorruptedTailRecords` on log-store open plus an
actionable open-error message, so a torn tail self-heals instead of
crash-looping. Regression test: `bug_av_5_2_recovery_tests` in
`log_store.rs`.

## Symptom

On 2026-05-22 during the Phase 4 close-out deploy, every manager
failed with:

```
Error: when Read Logs: std::io::error::Error: io error: unexpected end of file

Caused by:
    std::io::error::Error: io error: unexpected end of file
```

The error surfaced immediately after `systemctl restart dtfs-manager`
on each node. The new binary could not read the Raft log written by
the previous binary. Cargo.lock was unchanged across the two builds,
and all schema additions during the work cycle carried
`#[serde(default)]`.

The cluster recovered only after manually:

```bash
sudo systemctl stop dtfs-manager
sudo rm -rf /var/lib/dtfs/manager/raft-log /var/lib/dtfs/manager/raft-sm
sudo systemctl start dtfs-manager
```

…on all three managers in sequence. Voters re-bootstrapped fresh,
the leader's snapshot transferred to the followers, and the cluster
served traffic again. On-disk chunks were not affected.

## Root cause

`dtfs-raft/src/log_store.rs` uses `bincode::serialize` /
`bincode::deserialize` to (de)serialise every Raft log entry
(`Entry<TypeConfig>`). The `Entry` payload is `ClientRequest`, which
includes:

```rust
pub enum ClientRequest {
    ...
    RegisterNode {
        node_id: u64,
        info: NodeInfo,   // <-- struct embedded in the log entry
    },
    ...
}
```

`NodeInfo` (`dtfs-raft/src/lib.rs:133`) is the manager's view of a
registered node. The work cycle that preceded the deploy added three
new fields:

```rust
pub struct NodeInfo {
    ...
    #[serde(default)] pub client_ip: Option<String>,   // Bug AX
    #[serde(default)] pub version: String,             // Bug AY
    #[serde(default)] pub build_date: String,          // Bug AY
}
```

The `#[serde(default)]` annotation works for **self-describing**
formats (JSON, MessagePack-as-map, CBOR) but **does NOT work for
bincode**. bincode v1's default format is **positional**: it writes
each field in declaration order with NO field names or length prefix.
On deserialize it reads bytes positionally and assumes the writer
emitted every field. When the new binary tries to read an old
`RegisterNode` entry, the deserializer:

1. Reads the existing fields successfully.
2. Reaches the position where it expects `client_ip` bytes.
3. The input buffer is exhausted (the old writer didn't emit those
   bytes).
4. Returns `bincode::Error::Io(unexpected end of file)`.

openraft wraps the underlying error with `"when Read Logs:"` context.

This is a **classic bincode schema-evolution trap**. Adding fields to
a serde-bincode-serialised struct is a breaking change against any
on-disk data even if the new fields are `Option<T>` /
`#[serde(default)]`. The only safe schema evolution for bincode
is **adding new top-level enum variants** (variants are
length-prefixed via the discriminant), never **adding fields to
existing variants** that carry structs.

## Why Cargo.lock didn't shift

The same Cargo.lock that produced the old binary produced the new
binary. Cargo.lock pins `bincode 1.x`. The format didn't change —
the **payload shape** did. bincode's behaviour is "as advertised";
there is no library bug here.

## What did NOT trigger the issue

- Bug AV's `CacheReplication` / `AnchorPolicy` additions on
  `VolumeConfig` — those are inside `UpsertVolume` and would have
  failed similarly, but the existing log didn't contain any
  `UpsertVolume` entries with the new fields yet (they had not
  been written by the old binary).
- The Raft snapshot — snapshots are written/read by the state machine
  (`dtfs-raft/src/state_machine.rs`) which uses `serde_json::to_vec` /
  `serde_json::from_slice` (self-describing). `#[serde(default)]`
  works there; snapshot evolution survived the deploy.

So **the snapshot side was forward-compatible; the log side was
not.** That's why a wipe-and-rebootstrap worked: the new binary
loaded the JSON snapshot fine, then re-ran an empty log into a
fresh state.

## Why the wipe is the only safe recovery

Without a code fix, every existing log entry containing a
`RegisterNode` (or any other variant that embeds a struct that gained
fields) trips the EOF on read. Selective surgery on the log file is
not feasible: the entries are bincode-packed back-to-back with no
length prefixes per entry that you can use to skip the broken ones.

## Recovery runbook (current state of the art)

When the manager fails to start with `when Read Logs: ... unexpected
end of file`:

1. **Confirm the symptom on every manager.**
   ```bash
   for h in mgr-01 mgr-02 mgr-03; do
       ssh debian@$h "sudo journalctl -u dtfs-manager -n 20 | grep 'Read Logs'"
   done
   ```
2. **Stop ALL managers** (quorum is already lost; ordering doesn't
   matter beyond "stop everyone first").
   ```bash
   for h in mgr-01 mgr-02 mgr-03; do
       ssh debian@$h "sudo systemctl stop dtfs-manager"
   done
   ```
3. **Wipe Raft log + state-machine snapshot** on every manager.
   ```bash
   for h in mgr-01 mgr-02 mgr-03; do
       ssh debian@$h "sudo rm -rf /var/lib/dtfs/manager/raft-log \
                                  /var/lib/dtfs/manager/raft-sm"
   done
   ```
4. **Start managers** in order. The first to come up will bootstrap
   the cluster from `cluster.initial_voters` in `manager.toml`; the
   others join as followers and receive the leader's snapshot.
   ```bash
   for h in mgr-01 mgr-02 mgr-03; do
       ssh debian@$h "sudo systemctl start dtfs-manager"; sleep 8
   done
   ```
5. **Re-register data nodes** — they will reconnect on the next
   heartbeat (≈5 s) and re-register automatically via `RegisterNode`.
6. **Re-apply operator configuration** (Bug AV
   `set-mandatory` / `set-cache`, anchor policy, etc.) — these are
   per-volume Raft entries and were destroyed with the log.

On-disk chunks survive untouched.

## Permanent fix — AV-5-11 (deferred)

The log format must become forward-compatible. Options, lowest →
highest effort:

1. **Switch log entries from bincode to a self-describing format**
   (`serde_json::to_vec` or `rmp_serde` for MessagePack as map).
   Self-describing formats honour `#[serde(default)]` and let new
   binaries read entries written by old binaries. Cost: marginal
   per-entry CPU + slightly larger log entries. Forward path: ship a
   migration tool that reads bincode, re-writes as JSON, switches
   `log_store.rs` to JSON. **Recommended.**

2. **Bincode + manual version field on every embedded struct.**
   `#[derive(...)] pub struct NodeInfo { version: u8, ... }`.
   Custom `Deserialize` impl matches on the version byte and pads
   missing fields with defaults. Doable but every struct that may
   appear in a log entry needs the discipline; one missed update
   recurs this RCA.

3. **Length-prefix every log entry payload AND every nested struct.**
   Switch to bincode with `with_varint_encoding` + custom struct
   header. Most engineering, least gain over option 1.

AV-5-11 picks option 1 unless an operator can demonstrate the
per-entry CPU cost is material on Plexmedia-class workloads.

## Audit / cross-references

- [`plans/active/PLAN-BUG-AV-FOLLOWUPS.md`](../active/PLAN-BUG-AV-FOLLOWUPS.md) — AV-5-2 task entry.
- [`plans/completed/2026-05-22-PLAN-BUG-AV-TIER-LIFECYCLE.md`](../completed/2026-05-22-PLAN-BUG-AV-TIER-LIFECYCLE.md) — § 7.4 records the original on-the-day symptom.
- [`AGENTS.md` § A.3.1](../../AGENTS.md) — Raft state-machine snapshot compatibility rules (must extend to log entries too; AV-5-11 will update).
- [`crates/dtfs-raft/src/log_store.rs`](../../crates/dtfs-raft/src/log_store.rs) — current bincode-based log adapter.
- [`crates/dtfs-raft/src/lib.rs`](../../crates/dtfs-raft/src/lib.rs) — `NodeInfo`, `ClientRequest` definitions.
