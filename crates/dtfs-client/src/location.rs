// Location-aware data node selection — Phase 2 (FUSE Client).
// Selects nearest data node based on Location hierarchy.

use std::collections::HashMap;

use dtfs_common::topology::{Location, LocationDistance};
use tracing::debug;

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
}
