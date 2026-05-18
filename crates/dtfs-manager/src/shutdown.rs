//! Bug U — graceful shutdown coordination for the manager.
//!
//! On SIGTERM/SIGINT the manager MUST:
//!   1. Stop accepting new gRPC + REST requests (servers stop binding new conns).
//!   2. Trigger a final Raft snapshot so the on-disk state reflects every
//!      committed entry at exit time (otherwise the next start replays
//!      potentially-millions of log entries on top of an older snapshot).
//!   3. Flush the audit log archive so the "manager.stop" event is durable.
//!   4. Fire one final webhook alert so operators know shutdown was clean.
//!
//! Step 2 is the data-safety one. Without a snapshot at exit, a long-running
//! cluster's log grows unboundedly; if the disk fills before the next
//! `SnapshotPolicy::LogsSinceLast` threshold is hit, the manager can't even
//! start. Snapshotting at shutdown is cheap (state machine is in-memory),
//! takes <1s, and bounds recovery time.

use std::sync::Arc;

use tokio::sync::Notify;

/// Broadcast-style shutdown signal. Any number of tasks can call
/// `wait_for_shutdown().await` and they all unblock together when
/// `trigger()` fires.
#[derive(Default)]
pub struct ShutdownSignal {
    notify: Notify,
    fired: parking_lot::Mutex<bool>,
}

impl ShutdownSignal {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            notify: Notify::new(),
            fired: parking_lot::Mutex::new(false),
        })
    }

    /// Fire the shutdown signal. Idempotent — calling twice is a no-op.
    pub fn trigger(&self) {
        let mut fired = self.fired.lock();
        if *fired {
            return;
        }
        *fired = true;
        // notify_waiters wakes everyone currently waiting; future waits
        // see the `fired` flag and return immediately.
        self.notify.notify_waiters();
    }

    /// Has the signal already fired?
    pub fn is_fired(&self) -> bool {
        *self.fired.lock()
    }

    /// Wait until the signal fires (or return immediately if it already has).
    pub async fn wait_for_shutdown(self: Arc<Self>) {
        if self.is_fired() {
            return;
        }
        // notify.notified() is cancel-safe and edge-triggered.
        self.notify.notified().await;
    }
}

/// Block until SIGTERM (systemd stop) or SIGINT (Ctrl+C). Logs which signal fired.
pub async fn wait_for_terminate_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGTERM handler");
                return;
            }
        };
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!(error = %e, "failed to install SIGINT handler");
                return;
            }
        };
        tokio::select! {
            _ = sigterm.recv() => tracing::info!("SIGTERM received"),
            _ = sigint.recv() => tracing::info!("SIGINT received"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("Ctrl+C received");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    /// Bug U regression — multiple waiters all unblock on a single trigger.
    #[tokio::test]
    async fn bug_u_shutdown_signal_broadcasts_to_all_waiters() {
        let signal = ShutdownSignal::new();
        let s1 = signal.clone();
        let s2 = signal.clone();
        let s3 = signal.clone();

        let h1 = tokio::spawn(async move { s1.wait_for_shutdown().await });
        let h2 = tokio::spawn(async move { s2.wait_for_shutdown().await });
        let h3 = tokio::spawn(async move { s3.wait_for_shutdown().await });

        // Give the tasks a moment to install themselves as waiters.
        tokio::time::sleep(Duration::from_millis(50)).await;
        signal.trigger();

        tokio::time::timeout(Duration::from_secs(1), async {
            h1.await.unwrap();
            h2.await.unwrap();
            h3.await.unwrap();
        })
        .await
        .expect("all 3 waiters should unblock within 1s");
    }

    /// Bug U regression — a `wait` after `trigger` returns immediately.
    /// Without the `fired` flag check we'd block forever on a fresh waiter
    /// that arrived after notify_waiters consumed all permits.
    #[tokio::test]
    async fn bug_u_late_waiter_returns_immediately() {
        let signal = ShutdownSignal::new();
        signal.trigger();
        // No race: this waiter installs AFTER notify_waiters already fired.
        tokio::time::timeout(Duration::from_millis(200), signal.wait_for_shutdown())
            .await
            .expect("late waiter should not hang");
    }

    /// Bug U regression — `trigger()` is idempotent. Calling twice does
    /// nothing the second time (we don't want spurious "shutting down"
    /// log spam if the signal handler fires twice during a restart loop).
    #[tokio::test]
    async fn bug_u_trigger_is_idempotent() {
        let signal = ShutdownSignal::new();
        signal.trigger();
        assert!(signal.is_fired());
        signal.trigger(); // no panic, no second notification
        assert!(signal.is_fired());
    }
}
