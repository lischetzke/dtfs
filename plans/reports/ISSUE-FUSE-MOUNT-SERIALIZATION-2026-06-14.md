# FUSE read can hang indefinitely on a dead/hung data node (FUSE-HOL)

**Found:** 2026-06-14 wiring the CO-3-12 live case. **Corrected 2026-06-15**
after clean re-measurement. **Severity: HIGH.** Fix: PLAN-FUSE-HOL-read-timeout.

## Correction to the original write-up

The original version claimed the FUSE mount serializes **all** operations
(one stalled read freezes the whole mount). That was a **measurement artifact**:
the latency probes used `/usr/bin/time -p ...`, which is **absent** on the
client VMs, so every probe exited non-zero and was misread as a timeout. Clean
re-measurement (`date +%s%N` deltas) shows the mount is NOT globally serialized.

## Accurate findings (clean measurements, deployed build)

- fuse3 0.7.3 dispatches every handler on its own task (`spawn` per request);
  the runtime is multi-thread (4 workers). During a stalled read, operations on
  OTHER files are fast: `stat`/`open` of a different file = 3-5 ms; `stat` of the
  same file = 4 ms. **No mount-wide freeze.**
- The real bug: `DtfsFilesystem::read` fetches each chunk with
  `self.client.read_chunk(cand, id).await` (filesystem.rs ~715) with **no
  timeout**. The P0-2 replica fallback only triggers on a transport `Err`, NOT
  on a hang. A data node that is reachable but never returns the chunk (hung, or
  egress black-holed) makes the read await **indefinitely** — the application's
  FUSE read syscall never returns. Confirmed: with data01's chunk-port responses
  dropped on an established channel, a cold read hung >35 s with no fallover.
- Secondary: a concurrent `open` of the SAME file blocks for the in-flight
  read's fetch duration (~the stall). Bounding the fetch also bounds this.

## Impact

Prime Plex-over-WAN workload: a hung/dead archive data node makes every read
whose chosen replica is that node hang forever (app freezes on that file) until
the node recovers or is evicted. P0-2 does not help because a hang produces no
error.

## Fix (PLAN-FUSE-HOL-read-timeout)

Bound each per-replica `read_chunk` with a configurable timeout
(`client.chunk_read_timeout_secs`, default 60 s, `0` = disabled) and fail over
to the next candidate on timeout — extending P0-2 to cover hangs, not just
errors. Read-only path, so timeout/fallover is safe; all-candidates-failed =>
EIO (existing). Verified: the connect-error fallover path works live (read
recovered in ~5 s with correct bytes when the first candidate's node was
unreachable). The hung-after-connect timeout path is code-correct + builds; its
live netem test is pending (the test's `netem delay 25s` made the target node's
SSH unmanageable — recover by rebooting the VM).

## Relation to CO-3-12

CO-3-12 (drop the `open_files` guard before the read loop) is correct and
unit-covered. It is not the cause of the hang. Release case 69 stays
KNOWN_ISSUE [FUSE-HOL] until the timeout fix lands + is live-verified, then it
asserts hung-node fallover.
