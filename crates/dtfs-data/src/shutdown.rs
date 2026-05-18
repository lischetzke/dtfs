//! Bug U — graceful shutdown support for the data node.
//!
//! Why this module exists: a filesystem that loses data on `systemctl stop`
//! is a filesystem nobody should run in production. When the data node
//! receives SIGTERM (systemd stop) or SIGINT (Ctrl+C), it MUST:
//!
//! 1. Stop accepting *new* `put_chunk` requests (tonic server shuts down).
//! 2. Let *in-flight* writes finish — they hold a `write_slots` permit, so
//!    we wait for all permits to come back.
//! 3. Drain the `pending_corruption` queue (so the manager learns about
//!    quarantines we discovered just before shutdown).
//! 4. Send one last heartbeat marking the node as draining.
//!
//! Step 2 is the data-integrity critical one. A SIGTERM that aborts mid-write
//! leaves a tmp file behind (which Bug M's WAL replay reclaims on restart)
//! but the FUSE client sees the gRPC stream torn down and surfaces an EIO
//! to userspace. That's recoverable but operationally awful — Plex playback
//! would error out mid-stream, and any uncommitted write fragment is lost.
//! By waiting for the existing slot-bounded writes to drain, we get
//! "best-effort no-data-loss" at shutdown without holding systemd hostage
//! indefinitely.
//!
//! The hard timeout (`max_wait`) is a safety valve: if some buggy state
//! permanently held a slot, we'd otherwise hang `systemctl stop` until the
//! TimeoutStopSec kicks in and SIGKILLs us, which is strictly worse than
//! exiting cleanly after the timeout.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

/// Outcome of waiting for in-flight writes to drain.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// All permits returned before the deadline.
    Drained,
    /// Deadline hit while at least one permit was still held. We log this
    /// at WARN level — the kernel will SIGKILL us soon anyway, so we exit
    /// cleanly with whatever progress we made.
    Timeout { permits_outstanding: usize },
}

/// Wait until every concurrency slot owned by `write_slots` is free, OR
/// `max_wait` elapses — whichever happens first.
///
/// `total_slots` is the value the semaphore was created with (Semaphore
/// does not expose this directly). We assume slots are issued via
/// `acquire_owned` and dropped on the response path, which matches Bug D's
/// design in `put_chunk` ([server.rs:146-151]).
///
/// This helper is a pure function over Tokio primitives so it can be
/// unit-tested without the rest of the data-node wiring.
pub async fn wait_for_drain(
    write_slots: Arc<Semaphore>,
    total_slots: usize,
    max_wait: Duration,
) -> DrainOutcome {
    let deadline = tokio::time::Instant::now() + max_wait;
    loop {
        let available = write_slots.available_permits();
        if available >= total_slots {
            return DrainOutcome::Drained;
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return DrainOutcome::Timeout {
                permits_outstanding: total_slots.saturating_sub(available),
            };
        }
        // Poll every 50ms — coarse enough that we're not burning CPU,
        // fine enough that a shutdown that completes in ~100ms feels
        // snappy. We can't use `acquire_many(total_slots)` because that
        // would block forever even if total_slots-1 returned promptly.
        let tick = Duration::from_millis(50).min(remaining);
        tokio::time::sleep(tick).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Bug U regression — when no slot is held, drain returns immediately
    /// with `Drained`.
    #[tokio::test]
    async fn bug_u_drain_returns_immediately_when_no_slots_held() {
        let sem = Arc::new(Semaphore::new(8));
        let result =
            wait_for_drain(sem.clone(), 8, Duration::from_secs(30)).await;
        assert_eq!(result, DrainOutcome::Drained);
    }

    /// Bug U regression — when a slot is briefly held and then released,
    /// drain returns `Drained` (not `Timeout`).
    #[tokio::test]
    async fn bug_u_drain_returns_after_slot_released() {
        let sem = Arc::new(Semaphore::new(4));
        // Hold one permit briefly, then drop it on a background task.
        let permit = sem.clone().acquire_owned().await.unwrap();
        let release_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(150)).await;
            drop(permit);
        });

        let result = wait_for_drain(sem.clone(), 4, Duration::from_secs(2)).await;
        release_handle.await.unwrap();
        assert_eq!(result, DrainOutcome::Drained);
    }

    /// Bug U regression — when a slot is never released, drain returns
    /// `Timeout` with the correct outstanding count rather than hanging
    /// forever. Critical because the alternative is systemd SIGKILL.
    #[tokio::test]
    async fn bug_u_drain_times_out_when_slot_never_released() {
        let sem = Arc::new(Semaphore::new(8));
        // Hold 3 permits and never release them.
        let _p1 = sem.clone().acquire_owned().await.unwrap();
        let _p2 = sem.clone().acquire_owned().await.unwrap();
        let _p3 = sem.clone().acquire_owned().await.unwrap();

        let result =
            wait_for_drain(sem.clone(), 8, Duration::from_millis(250)).await;
        match result {
            DrainOutcome::Timeout {
                permits_outstanding,
            } => {
                assert_eq!(permits_outstanding, 3, "should report 3 stuck slots");
            }
            other => panic!("expected Timeout, got {other:?}"),
        }
    }

    /// Bug U regression — total_slots == 0 means there's nothing to drain;
    /// returns `Drained` instantly. Defensive: if memory budget recommends
    /// 0 slots we'd otherwise wait forever.
    #[tokio::test]
    async fn bug_u_drain_zero_total_returns_drained() {
        let sem = Arc::new(Semaphore::new(0));
        let result =
            wait_for_drain(sem.clone(), 0, Duration::from_secs(30)).await;
        assert_eq!(result, DrainOutcome::Drained);
    }
}
