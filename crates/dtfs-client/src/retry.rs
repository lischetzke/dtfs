// Retry + failover logic — Phase 2 (FUSE Client).
// Handles connection failures and retries with exponential backoff.

use std::future::Future;
use std::time::Duration;

use rand::Rng;
use tracing::{debug, warn};

/// Hard cap so `max_retries` cannot lock the runtime forever.
const MAX_RETRIES_CAP: u32 = 4096;

/// Configuration for retry with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (not counting the initial attempt).
    pub max_retries: u32,
    /// Base delay between retries. Doubled on each subsequent attempt.
    pub base_delay: Duration,
    /// Maximum delay cap. The backoff will never exceed this value.
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryConfig {
    pub fn new(max_retries: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_retries: max_retries.min(MAX_RETRIES_CAP),
            base_delay,
            max_delay,
        }
    }

    /// Compute the delay for a given attempt (0-indexed), with additive jitter.
    ///
    /// The previous implementation returned a random value in `[0, capped]`,
    /// which could produce a delay of 0 ms on any attempt — defeating the
    /// purpose of exponential backoff entirely (fix for issue #9).
    ///
    /// The corrected approach: wait the full exponential delay, then add a
    /// random jitter of up to 25 % of that delay to spread retries and avoid
    /// thundering-herd effects.
    fn delay_for_attempt(&self, attempt: u32) -> Duration {
        // Exponential: base_delay * 2^attempt, capped at max_delay.
        let exp_delay = self
            .base_delay
            .saturating_mul(1u32.wrapping_shl(attempt));
        let capped = exp_delay.min(self.max_delay);

        // Jitter: up to 25 % of the capped delay (minimum 1 ms when non-zero).
        let jitter_range = ((capped.as_millis() / 4) as u64).max(if capped.is_zero() { 0 } else { 1 });
        let jitter_ms = if jitter_range > 0 {
            rand::thread_rng().gen_range(0..=jitter_range)
        } else {
            0
        };

        capped + Duration::from_millis(jitter_ms)
    }
}

/// Retry an async operation with exponential backoff and jitter.
///
/// The `operation` closure is called repeatedly until it succeeds or the
/// maximum number of retries is exhausted. On failure, the function sleeps
/// for an exponentially increasing duration (with jitter) before the next
/// attempt.
///
/// Returns the result of the first successful attempt, or the error from
/// the final failed attempt.
pub async fn retry_with_backoff<F, Fut, T, E>(
    config: &RetryConfig,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut last_error: Option<E> = None;

    for attempt in 0..=config.max_retries {
        match operation().await {
            Ok(value) => {
                if attempt > 0 {
                    debug!(attempt, "operation succeeded after retry");
                }
                return Ok(value);
            }
            Err(err) => {
                if attempt < config.max_retries {
                    let delay = config.delay_for_attempt(attempt);
                    warn!(
                        attempt,
                        max_retries = config.max_retries,
                        delay_ms = delay.as_millis() as u64,
                        error = %err,
                        "operation failed, retrying after backoff"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    warn!(
                        attempt,
                        error = %err,
                        "operation failed, no more retries"
                    );
                }
                last_error = Some(err);
            }
        }
    }

    Err(last_error.expect("at least one attempt must have been made"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_succeeds_first_try() {
        let config = RetryConfig::new(
            3,
            Duration::from_millis(1),
            Duration::from_millis(10),
        );
        let result: Result<u32, String> =
            retry_with_backoff(&config, || async { Ok(42) }).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let config = RetryConfig::new(
            3,
            Duration::from_millis(1),
            Duration::from_millis(10),
        );
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<u32, String> = retry_with_backoff(&config, move || {
            let c = counter_clone.clone();
            async move {
                let attempt = c.fetch_add(1, Ordering::SeqCst);
                if attempt < 2 {
                    Err(format!("fail {attempt}"))
                } else {
                    Ok(42)
                }
            }
        })
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausted() {
        let config = RetryConfig::new(
            2,
            Duration::from_millis(1),
            Duration::from_millis(10),
        );
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result: Result<u32, String> = retry_with_backoff(&config, move || {
            let c = counter_clone.clone();
            async move {
                c.fetch_add(1, Ordering::SeqCst);
                Err("always fails".to_string())
            }
        })
        .await;

        assert!(result.is_err());
        // initial attempt + 2 retries = 3 total
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}

#[cfg(test)]
mod audit_co_tests {
    use super::*;

    // CO-5-19: backoff multiplier `1u32.wrapping_shl(attempt)` wraps the shift
    // amount mod 32, so attempt 32 multiplies by `1 << 0 == 1` (collapsing to
    // base_delay) while attempt 31 multiplies by `1 << 31` (saturates → capped
    // at max_delay). Backoff must be monotonic non-decreasing across attempts;
    // delay_for_attempt(32) must be >= delay_for_attempt(31).
    #[test]
    fn co_5_19_backoff_does_not_collapse_at_attempt_32() {
        // base*2^31 vastly exceeds max_delay, so attempt 31 caps at ~1000s.
        // With the wrap bug, attempt 32 falls back to ~base_delay (100ms).
        let config = RetryConfig::new(
            5000, // above the 4096 cap, but only used to allow high attempts conceptually
            Duration::from_millis(100),
            Duration::from_secs(1000),
        );

        let d31 = config.delay_for_attempt(31);
        let d32 = config.delay_for_attempt(32);

        // Jitter adds at most 25% on top of the capped base, so comparing the
        // capped portions is robust: d31 ~ 1000s, d32 (correct) should also be
        // capped at 1000s; the bug makes d32 ~ 100ms.
        assert!(
            d32 >= d31,
            "backoff must not collapse at attempt 32: d31={:?} d32={:?}",
            d31,
            d32
        );
    }
}
