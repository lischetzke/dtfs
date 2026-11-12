//! Link budget (`[[tiering.links]]`): one token bucket per target DC, debited
//! when a replication task is created for a copy into that DC from another
//! DC. Leader-local memory, cleared when this manager is not the leader.

use std::collections::HashMap;

use dtfs_common::config::TieringLink;
use serde::Serialize;
use tracing::info;

use crate::precache::refill_tokens;

/// Token-bucket depth in seconds of a link's `max_mbps` (same as pre-cache).
pub(crate) const LINK_BURST_SECS: u64 = 120;
/// At most one info line per DC per this window about skipped copies.
const SKIP_LOG_EVERY_MS: u64 = 60_000;

#[derive(Debug, Default)]
pub(crate) struct LinkBudget {
    links: Vec<TieringLink>,
    /// Keyed by the lowercased `to_dc`.
    buckets: HashMap<String, Bucket>,
}

#[derive(Debug, Default)]
struct Bucket {
    tokens: u64,
    at_ms: u64,
    skipped: u64,
    skipped_bytes: u64,
    window_start_ms: u64,
    window_skipped: u64,
    window_bytes: u64,
}

/// Per-link state for `GET /api/v1/tiering/precache` (`link_budget_backlog`).
/// Skip counters cover this manager's current leader tenure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub(crate) struct LinkBacklog {
    pub to_dc: String,
    pub max_mbps: u64,
    pub tokens_bytes: u64,
    pub skipped_requests: u64,
    pub skipped_bytes: u64,
}

impl LinkBudget {
    pub(crate) fn new(links: Vec<TieringLink>) -> Self {
        Self { links, buckets: HashMap::new() }
    }

    /// The link a copy from `source_dc` into `target_dc` is metered on, if any.
    /// Same-DC copies never are.
    fn link_for(&self, source_dc: &str, target_dc: &str) -> Option<&TieringLink> {
        if source_dc.eq_ignore_ascii_case(target_dc) {
            return None;
        }
        self.links.iter().find(|l| l.to_dc.eq_ignore_ascii_case(target_dc))
    }

    pub(crate) fn is_metered(&self, source_dc: &str, target_dc: &str) -> bool {
        self.link_for(source_dc, target_dc).is_some()
    }

    /// Debit `bytes` when the copy fits the target link's balance; `false` =
    /// skip it this round (the caller re-plans next tick). Unmetered = `true`.
    pub(crate) fn admit(&mut self, source_dc: &str, target_dc: &str, bytes: u64, now_ms: u64) -> bool {
        let Some(link) = self.link_for(source_dc, target_dc) else {
            return true;
        };
        let (dc, mbps) = (link.to_dc.clone(), link.max_mbps);
        // A new bucket starts full (refill caps it at the burst).
        let b = self
            .buckets
            .entry(dc.to_ascii_lowercase())
            .or_insert_with(|| Bucket { tokens: u64::MAX, at_ms: now_ms, ..Default::default() });
        // A chunk larger than the burst must still fit eventually (min_cap).
        b.tokens = refill_tokens(b.tokens, b.at_ms, now_ms, mbps, LINK_BURST_SECS, bytes);
        b.at_ms = now_ms;
        if bytes <= b.tokens {
            b.tokens -= bytes;
            return true;
        }
        b.skipped += 1;
        b.skipped_bytes += bytes;
        b.window_skipped += 1;
        b.window_bytes += bytes;
        if now_ms.saturating_sub(b.window_start_ms) >= SKIP_LOG_EVERY_MS {
            info!(
                to_dc = %dc,
                max_mbps = mbps,
                skipped = b.window_skipped,
                skipped_bytes = b.window_bytes,
                "link budget exhausted: copies into this DC wait for the next round"
            );
            b.window_start_ms = now_ms;
            b.window_skipped = 0;
            b.window_bytes = 0;
        }
        false
    }

    /// Leadership lost: the next tenure starts with full buckets.
    pub(crate) fn reset(&mut self) {
        self.buckets.clear();
    }

    pub(crate) fn status(&self) -> Vec<LinkBacklog> {
        self.links
            .iter()
            .map(|l| {
                let b = self.buckets.get(&l.to_dc.to_ascii_lowercase());
                LinkBacklog {
                    to_dc: l.to_dc.clone(),
                    max_mbps: l.max_mbps,
                    // Untouched bucket: full burst.
                    tokens_bytes: b.map_or(l.max_mbps * 125_000 * LINK_BURST_SECS, |b| b.tokens),
                    skipped_requests: b.map_or(0, |b| b.skipped),
                    skipped_bytes: b.map_or(0, |b| b.skipped_bytes),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BURST_1MBPS: u64 = 125_000 * LINK_BURST_SECS; // 15 MB

    fn budget(links: &[(&str, u64)]) -> LinkBudget {
        LinkBudget::new(links.iter().map(|(d, m)| TieringLink { to_dc: (*d).into(), max_mbps: *m }).collect())
    }

    #[test]
    fn boundary_budget_minus_one_then_refused() {
        let mut lb = budget(&[("leli", 1)]);
        let t = 1_000_000;
        assert!(lb.admit("toro", "leli", BURST_1MBPS - 1, t));
        assert!(!lb.admit("toro", "leli", 2, t), "1 byte left, 2 requested");
        assert!(lb.admit("toro", "leli", 1, t));
        assert!(!lb.admit("toro", "leli", 1, t), "bucket empty");
        let s = &lb.status()[0];
        assert_eq!((s.tokens_bytes, s.skipped_requests, s.skipped_bytes), (0, 2, 3));
    }

    #[test]
    fn refill_by_elapsed_time_and_burst_cap() {
        let mut lb = budget(&[("leli", 1)]);
        let t = 1_000_000;
        assert!(lb.admit("toro", "leli", BURST_1MBPS, t));
        // 1 s at 1 Mbps = 125 000 bytes.
        assert!(lb.admit("toro", "leli", 125_000, t + 1_000));
        assert!(!lb.admit("toro", "leli", 1, t + 1_000));
        // Ten hours later the balance is capped at the 120 s burst.
        let later = t + 36_000_000;
        assert!(lb.admit("toro", "leli", BURST_1MBPS, later));
        assert!(!lb.admit("toro", "leli", 1, later));
    }

    #[test]
    fn chunk_larger_than_burst_still_fits_eventually() {
        let mut lb = budget(&[("leli", 1)]);
        assert!(lb.admit("toro", "leli", BURST_1MBPS * 2, 1_000_000));
    }

    #[test]
    fn same_dc_never_metered_and_compare_is_case_insensitive() {
        let mut lb = budget(&[("LELI", 1)]);
        for _ in 0..10 {
            assert!(lb.admit("leli", "Leli", BURST_1MBPS, 1_000));
        }
        assert!(lb.is_metered("toro", "leli"));
        assert!(!lb.is_metered("LeLi", "leli"));
        assert!(lb.admit("toro", "leli", BURST_1MBPS, 1_000));
        assert!(!lb.admit("TORO", "lElI", 1, 1_000), "case-insensitive target shares the bucket");
        assert!(lb.admit("leli", "toro", u64::MAX, 1_000), "no link into toro");
    }

    #[test]
    fn no_links_is_unlimited() {
        let mut lb = budget(&[]);
        for _ in 0..10 {
            assert!(lb.admit("toro", "leli", u64::MAX, 1_000));
        }
        assert!(lb.status().is_empty());
    }

    #[test]
    fn reset_refills_to_full_burst() {
        let mut lb = budget(&[("leli", 1)]);
        assert!(lb.admit("toro", "leli", BURST_1MBPS, 1_000_000));
        lb.reset();
        assert_eq!(lb.status()[0].tokens_bytes, BURST_1MBPS);
        assert!(lb.admit("toro", "leli", BURST_1MBPS, 1_000_000));
    }
}
