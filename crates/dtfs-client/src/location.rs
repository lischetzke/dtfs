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
}
