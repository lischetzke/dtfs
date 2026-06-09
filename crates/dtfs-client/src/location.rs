// Location-aware data node selection — Phase 2 (FUSE Client).
// Selects nearest data node based on Location hierarchy.

use std::collections::HashMap;

use dtfs_common::topology::{Location, LocationDistance};
use tracing::debug;

/// Tie-break rank for a replica's storage tier (U4 read-locality).
///
/// Lower = preferred when two replicas are at the EXACT same physical
/// distance. We prefer faster media (warm SSD/HDD) over the cold anchor HDD
/// so reads offload the slow LELI archive disk. `hot` (rare for the media
/// workload) ranks fastest; `tape` slowest; an empty/unknown string (e.g. an
/// older manager that doesn't populate `node_tiers`) ranks last so it never
/// wins a tie over a known tier but also never penalises distance.
fn tier_tiebreak_rank(tier: &str) -> u8 {
    match tier {
        "hot" => 0,
        "warm" => 1,
        "cold" => 2,
        "tape" => 3,
        _ => u8::MAX,
    }
}

/// True iff `loc` matches a `read_location_priority` entry. An entry matches
/// when it equals (case-insensitively) the replica's datacenter or host, so
/// an operator can list either a whole DC ("dc1") or a single host.
fn location_matches_priority(loc: &Location, entry: &str) -> bool {
    loc.datacenter.eq_ignore_ascii_case(entry) || loc.host.eq_ignore_ascii_case(entry)
}

/// Select the read replica for a chunk using the U4 precedence:
///
/// 1. **Configured `read_location_priority`** — the first replica whose
///    location (datacenter or host) matches the *earliest-listed* priority
///    entry wins. This is the operator's explicit intent and takes precedence
///    over raw distance (lets a TORO client try DC1, then DC3).
/// 2. **Physical distance** — `Location::distance` (SameHost < ... <
///    CrossRegion). A strictly-nearer replica ALWAYS wins over a farther one.
/// 3. **Tier tie-break** — only when distance is EQUAL, prefer faster media
///    (warm over cold) to offload the cold anchor HDD. NEVER overrides a
///    strictly-nearer replica.
///
/// `replicas` is `(addr, tier)` where `tier` is the lowercase tier string from
/// the manager (`""` = unknown, older manager). Each replica's location is
/// looked up in `known_locations` by addr; an unknown location is treated as
/// `CrossRegion` (worst distance), matching `select_nearest_node`.
///
/// Falls back to the first replica when there is no client location, and to
/// `None` when `replicas` is empty.
pub fn select_read_replica(
    client_location: Option<&Location>,
    replicas: &[(String, String)],
    known_locations: &HashMap<String, Location>,
    read_location_priority: &[String],
) -> Option<String> {
    if replicas.is_empty() {
        return None;
    }

    let client_loc = match client_location {
        Some(loc) => loc,
        None => {
            debug!("no client location available, selecting first replica");
            return Some(replicas[0].0.clone());
        }
    };

    // (1) Priority list: scan entries in operator-listed order; the first
    // entry that any replica's location matches wins, preferring the
    // earliest-listed replica among matches (stable over `replicas` order).
    for entry in read_location_priority {
        if let Some((addr, _)) = replicas.iter().find(|(addr, _)| {
            known_locations
                .get(addr)
                .map(|loc| location_matches_priority(loc, entry))
                .unwrap_or(false)
        }) {
            debug!(selected = %addr, %entry, "selected replica by read_location_priority");
            return Some(addr.clone());
        }
    }

    // (2) distance, then (3) tier tie-break on EQUAL distance. Iterate in
    // order so ties keep the first-listed replica (stable).
    let mut best: Option<(&str, LocationDistance, u8)> = None;
    for (addr, tier) in replicas {
        let distance = match known_locations.get(addr) {
            Some(node_loc) => client_loc.distance(node_loc),
            None => LocationDistance::CrossRegion,
        };
        let rank = tier_tiebreak_rank(tier);
        let better = match best {
            None => true,
            Some((_, best_dist, best_rank)) => {
                distance < best_dist || (distance == best_dist && rank < best_rank)
            }
        };
        if better {
            best = Some((addr, distance, rank));
        }
    }

    let (best_addr, best_distance, best_rank) = best.expect("replicas non-empty");
    debug!(
        selected = %best_addr,
        distance = ?best_distance,
        tier_rank = best_rank,
        candidates = replicas.len(),
        "selected read replica (distance + tier tie-break)"
    );
    Some(best_addr.to_string())
}

/// Select the nearest data node from a list of candidate addresses.
///
/// Uses the topology `Location` hierarchy to compute distances between the
/// client location and each candidate. If a candidate's location is not
/// present in `known_locations`, it is treated as `CrossRegion` (maximum
/// distance).
///
/// Falls back to the first address in `node_addresses` if no location
/// information is available.
///
/// Returns `None` if `node_addresses` is empty.
pub fn select_nearest_node(
    client_location: Option<&Location>,
    node_addresses: &[String],
    known_locations: &HashMap<String, Location>,
) -> Option<String> {
    if node_addresses.is_empty() {
        return None;
    }

    // If we have no client location, just return the first node.
    let client_loc = match client_location {
        Some(loc) => loc,
        None => {
            debug!("no client location available, selecting first node");
            return Some(node_addresses[0].clone());
        }
    };

    let mut best_addr = &node_addresses[0];
    let mut best_distance = LocationDistance::CrossRegion;

    for addr in node_addresses {
        let distance = match known_locations.get(addr) {
            Some(node_loc) => client_loc.distance(node_loc),
            None => {
                // Unknown location — treat as worst case.
                LocationDistance::CrossRegion
            }
        };

        if distance < best_distance {
            best_distance = distance;
            best_addr = addr;
        }
    }

    debug!(
        selected = %best_addr,
        distance = ?best_distance,
        candidates = node_addresses.len(),
        "selected nearest data node"
    );

    Some(best_addr.clone())
}

/// Sort node addresses by distance from `client_location`, nearest first.
///
/// Used for replica fallback on read failures: if the nearest node fails,
/// the caller iterates through the remaining candidates in order.
/// Nodes whose location is unknown sort last (CrossRegion). Stable order
/// preserves the caller's original ordering for ties.
pub fn sort_nodes_by_distance(
    client_location: Option<&Location>,
    node_addresses: &[String],
    known_locations: &HashMap<String, Location>,
) -> Vec<String> {
    if node_addresses.is_empty() {
        return Vec::new();
    }

    let client_loc = match client_location {
        Some(loc) => loc,
        None => return node_addresses.to_vec(),
    };

    let mut indexed: Vec<(usize, LocationDistance, String)> = node_addresses
        .iter()
        .enumerate()
        .map(|(idx, addr)| {
            let dist = match known_locations.get(addr) {
                Some(node_loc) => client_loc.distance(node_loc),
                None => LocationDistance::CrossRegion,
            };
            (idx, dist, addr.clone())
        })
        .collect();

    // Sort by distance (ascending = nearest first), then by original index for stability.
    indexed.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));

    indexed.into_iter().map(|(_, _, addr)| addr).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(region: &str, dc: &str, rack: &str, host: &str) -> Location {
        Location::new(region, dc, rack, host)
    }

    #[test]
    fn test_empty_addresses() {
        let result = select_nearest_node(None, &[], &HashMap::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_no_client_location_returns_first() {
        let addrs = vec!["node-a:9000".to_string(), "node-b:9000".to_string()];
        let result = select_nearest_node(None, &addrs, &HashMap::new());
        assert_eq!(result.unwrap(), "node-a:9000");
    }

    #[test]
    fn test_selects_same_rack() {
        let client = loc("eu", "fra", "rack-1", "client-01");
        let addrs = vec![
            "node-a:9000".to_string(),
            "node-b:9000".to_string(),
            "node-c:9000".to_string(),
        ];
        let mut locations = HashMap::new();
        // node-a is cross-region
        locations.insert("node-a:9000".to_string(), loc("us", "east", "rack-1", "n1"));
        // node-b is same rack
        locations.insert(
            "node-b:9000".to_string(),
            loc("eu", "fra", "rack-1", "n2"),
        );
        // node-c is same datacenter but different rack
        locations.insert(
            "node-c:9000".to_string(),
            loc("eu", "fra", "rack-2", "n3"),
        );

        let result = select_nearest_node(Some(&client), &addrs, &locations);
        assert_eq!(result.unwrap(), "node-b:9000");
    }

    #[test]
    fn test_unknown_locations_fallback_to_first() {
        let client = loc("eu", "fra", "rack-1", "client-01");
        let addrs = vec!["node-a:9000".to_string(), "node-b:9000".to_string()];
        // No known locations — all CrossRegion, so first wins.
        let result = select_nearest_node(Some(&client), &addrs, &HashMap::new());
        assert_eq!(result.unwrap(), "node-a:9000");
    }

    #[test]
    fn test_prefers_same_host() {
        let client = loc("eu", "fra", "rack-1", "host-01");
        let addrs = vec!["node-a:9000".to_string(), "node-b:9000".to_string()];
        let mut locations = HashMap::new();
        locations.insert(
            "node-a:9000".to_string(),
            loc("eu", "fra", "rack-1", "host-02"),
        );
        locations.insert(
            "node-b:9000".to_string(),
            loc("eu", "fra", "rack-1", "host-01"),
        );

        let result = select_nearest_node(Some(&client), &addrs, &locations);
        assert_eq!(result.unwrap(), "node-b:9000");
    }

    #[test]
    fn test_sort_orders_by_distance() {
        let client = loc("eu", "fra", "rack-1", "host-01");
        let addrs = vec![
            "far:9000".to_string(),
            "same-host:9000".to_string(),
            "same-rack:9000".to_string(),
            "same-dc:9000".to_string(),
        ];
        let mut locations = HashMap::new();
        locations.insert("far:9000".into(), loc("us", "east", "rack-1", "n1"));
        locations.insert("same-host:9000".into(), loc("eu", "fra", "rack-1", "host-01"));
        locations.insert("same-rack:9000".into(), loc("eu", "fra", "rack-1", "other"));
        locations.insert("same-dc:9000".into(), loc("eu", "fra", "rack-2", "n3"));

        let sorted = sort_nodes_by_distance(Some(&client), &addrs, &locations);
        assert_eq!(
            sorted,
            vec![
                "same-host:9000".to_string(),
                "same-rack:9000".to_string(),
                "same-dc:9000".to_string(),
                "far:9000".to_string(),
            ]
        );
    }

    #[test]
    fn test_sort_empty() {
        let sorted = sort_nodes_by_distance(None, &[], &HashMap::new());
        assert!(sorted.is_empty());
    }

    #[test]
    fn test_sort_no_client_loc_preserves_order() {
        let addrs = vec!["b:9000".to_string(), "a:9000".to_string()];
        let sorted = sort_nodes_by_distance(None, &addrs, &HashMap::new());
        assert_eq!(sorted, addrs);
    }

    #[test]
    fn test_sort_unknown_locations_last_stable() {
        let client = loc("eu", "fra", "rack-1", "host-01");
        let addrs = vec![
            "unknown-1:9000".to_string(),
            "known-far:9000".to_string(),
            "unknown-2:9000".to_string(),
            "known-near:9000".to_string(),
        ];
        let mut locations = HashMap::new();
        locations.insert("known-far:9000".into(), loc("us", "east", "rack-1", "n1"));
        locations.insert(
            "known-near:9000".into(),
            loc("eu", "fra", "rack-1", "host-01"),
        );

        let sorted = sort_nodes_by_distance(Some(&client), &addrs, &locations);
        // known-near first, then unknown-1/known-far/unknown-2 all CrossRegion → stable original order.
        assert_eq!(sorted[0], "known-near:9000");
    }

    // --- U4: tier-aware + read_location_priority selection ---

    fn rep(addr: &str, tier: &str) -> (String, String) {
        (addr.to_string(), tier.to_string())
    }

    #[test]
    fn test_priority_list_picks_listed_location_first() {
        // TORO client; replicas in DC3 (nearer-ish) and DC1.
        // Priority lists DC1 first, so it must win over distance.
        let client = loc("eu", "toro", "rack-1", "client-02");
        let replicas = vec![rep("dc3:9500", "warm"), rep("dc1:9500", "cold")];
        let mut locations = HashMap::new();
        locations.insert("dc3:9500".into(), loc("eu", "dc3", "rack-9", "n1"));
        locations.insert(
            "dc1:9500".into(),
            loc("eu", "dc1", "rack-9", "n2"),
        );

        let priority = vec!["dc1".to_string(), "dc3".to_string()];
        let got = select_read_replica(Some(&client), &replicas, &locations, &priority);
        assert_eq!(got.unwrap(), "dc1:9500");

        // Reversing the priority order flips the winner.
        let priority2 = vec!["dc3".to_string(), "dc1".to_string()];
        let got2 = select_read_replica(Some(&client), &replicas, &locations, &priority2);
        assert_eq!(got2.unwrap(), "dc3:9500");
    }

    #[test]
    fn test_equal_distance_warm_beats_cold() {
        // Two replicas at the SAME distance (both same-datacenter, diff rack).
        // No priority list → tier tie-break prefers warm over cold.
        let client = loc("eu", "fra", "rack-1", "client-01");
        let replicas = vec![rep("cold-node:9500", "cold"), rep("warm-node:9500", "warm")];
        let mut locations = HashMap::new();
        locations.insert("cold-node:9500".into(), loc("eu", "fra", "rack-2", "n1"));
        locations.insert("warm-node:9500".into(), loc("eu", "fra", "rack-3", "n2"));

        let got = select_read_replica(Some(&client), &replicas, &locations, &[]);
        assert_eq!(got.unwrap(), "warm-node:9500");
    }

    #[test]
    fn test_nearer_cold_beats_farther_warm() {
        // LELI reader: a LOCAL cold replica must beat a remote warm one.
        // Tier is only a tie-break and must NOT override strictly-nearer.
        let client = loc("eu", "leli", "rack-1", "client-01");
        let replicas = vec![
            rep("remote-warm:9500", "warm"),
            rep("local-cold:9500", "cold"),
        ];
        let mut locations = HashMap::new();
        // remote warm = cross-region (farthest)
        locations.insert("remote-warm:9500".into(), loc("us", "east", "rack-1", "n1"));
        // local cold = same host (nearest)
        locations.insert(
            "local-cold:9500".into(),
            loc("eu", "leli", "rack-1", "client-01"),
        );

        let got = select_read_replica(Some(&client), &replicas, &locations, &[]);
        assert_eq!(got.unwrap(), "local-cold:9500");
    }

    #[test]
    fn test_no_priority_no_tier_behaves_like_distance_only() {
        // Empty tier strings (older manager) + no priority list ⇒ pure
        // distance, identical to select_nearest_node's choice.
        let client = loc("eu", "fra", "rack-1", "client-01");
        let replicas = vec![
            rep("far:9500", ""),
            rep("same-rack:9500", ""),
            rep("same-dc:9500", ""),
        ];
        let mut locations = HashMap::new();
        locations.insert("far:9500".into(), loc("us", "east", "rack-1", "n1"));
        locations.insert("same-rack:9500".into(), loc("eu", "fra", "rack-1", "other"));
        locations.insert("same-dc:9500".into(), loc("eu", "fra", "rack-2", "n3"));

        let addrs: Vec<String> = replicas.iter().map(|(a, _)| a.clone()).collect();
        let via_old = select_nearest_node(Some(&client), &addrs, &locations);
        let via_new = select_read_replica(Some(&client), &replicas, &locations, &[]);
        assert_eq!(via_new, via_old);
        assert_eq!(via_new.unwrap(), "same-rack:9500");
    }

    #[test]
    fn test_priority_falls_through_to_distance_when_no_match() {
        // Priority entry matches no replica → fall through to distance.
        let client = loc("eu", "fra", "rack-1", "client-01");
        let replicas = vec![rep("far:9500", "cold"), rep("near:9500", "cold")];
        let mut locations = HashMap::new();
        locations.insert("far:9500".into(), loc("us", "east", "rack-1", "n1"));
        locations.insert("near:9500".into(), loc("eu", "fra", "rack-1", "n2"));

        let priority = vec!["nonexistent-dc".to_string()];
        let got = select_read_replica(Some(&client), &replicas, &locations, &priority);
        assert_eq!(got.unwrap(), "near:9500");
    }

    #[test]
    fn test_select_read_replica_empty() {
        assert!(select_read_replica(None, &[], &HashMap::new(), &[]).is_none());
    }

    #[test]
    fn test_select_read_replica_no_client_loc_returns_first() {
        let replicas = vec![rep("a:9500", "cold"), rep("b:9500", "warm")];
        let got = select_read_replica(None, &replicas, &HashMap::new(), &[]);
        assert_eq!(got.unwrap(), "a:9500");
    }
}
