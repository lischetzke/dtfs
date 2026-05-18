use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::topology::Location;
use crate::types::{DiskTier, FailureDomain, NodeId};

/// A virtual node on the consistent hash ring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualNode {
    pub position: u32,
    pub physical_node: NodeId,
    pub disk_id: u32,
    pub tier: DiskTier,
    pub location: Location,
}

/// Placement group — maps chunks to a set of data nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementGroup {
    pub id: u32,
    pub primary: NodeId,
    pub replicas: Vec<NodeId>,
    pub tier: DiskTier,
}

/// Placement rule for determining where to put data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementRule {
    pub tier: DiskTier,
    pub replica_count: u32,
    pub failure_domain: FailureDomain,
    pub prefer_client_locations: bool,
    /// P0-10: when set, at least one chosen replica MUST live in this DC.
    /// Used to keep a durable copy of every chunk in the "anchor" DC (e.g.
    /// DC-A in a 3-DC topology), so tiering never strands the last anchor
    /// copy by migrating it elsewhere.
    ///
    /// If no node in the required DC has matching tier capacity, the rule
    /// falls back to the relaxed pass (which may produce a placement that
    /// doesn't satisfy the anchor); callers should check the result against
    /// the anchor DC and emit a `placement_anchor_unsatisfiable` metric.
    #[serde(default)]
    pub required_datacenter: Option<String>,
}

/// Consistent hash ring with location-aware, tier-aware placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsistentHashRing {
    ring: BTreeMap<u32, VirtualNode>,
    num_pgs: u32,
}

impl ConsistentHashRing {
    pub fn new(num_pgs: u32) -> Self {
        Self {
            ring: BTreeMap::new(),
            num_pgs,
        }
    }

    /// Add virtual nodes for a data node to the ring.
    /// `weight` is proportional to disk capacity (1.0 = standard).
    pub fn add_node(
        &mut self,
        node_id: &NodeId,
        disk_id: u32,
        tier: DiskTier,
        location: &Location,
        weight: f64,
    ) {
        let num_vnodes = (weight * 150.0) as u32;
        for i in 0..num_vnodes {
            let key = format!("{}:{}:{}", node_id.0, disk_id, i);
            let position = hash_position(&key);
            self.ring.insert(
                position,
                VirtualNode {
                    position,
                    physical_node: node_id.clone(),
                    disk_id,
                    tier,
                    location: location.clone(),
                },
            );
        }
    }

    /// Remove all virtual nodes for a given physical node.
    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.ring
            .retain(|_, vn| vn.physical_node != *node_id);
    }

    /// Map a chunk to its placement group ID.
    pub fn chunk_to_pg(&self, chunk_id: &[u8; 32]) -> u32 {
        let hash = xxhash_rust::xxh3::xxh3_64(chunk_id);
        (hash % self.num_pgs as u64) as u32
    }

    /// Find N distinct physical nodes for a placement group, respecting tier and failure domain.
    pub fn find_nodes_for_pg(
        &self,
        pg_id: u32,
        rule: &PlacementRule,
        client_locations: &[Location],
    ) -> Vec<NodeId> {
        let start_position = hash_position(&format!("pg:{}", pg_id));
        let mut selected: Vec<NodeId> = Vec::new();
        let mut selected_hosts: Vec<String> = Vec::new();
        let mut selected_racks: Vec<String> = Vec::new();
        let mut selected_dcs: Vec<String> = Vec::new();

        // Walk the ring clockwise from start_position
        let iter = self
            .ring
            .range(start_position..)
            .chain(self.ring.range(..start_position));

        for (_, vn) in iter {
            if selected.len() >= rule.replica_count as usize {
                break;
            }

            // Must match the required tier
            if vn.tier != rule.tier {
                continue;
            }

            // Must be on a different host (always enforced)
            if selected_hosts.contains(&vn.location.host) {
                continue;
            }

            // Failure domain checks
            match rule.failure_domain {
                FailureDomain::Datacenter => {
                    if selected_dcs.contains(&vn.location.datacenter)
                        && selected.len() < rule.replica_count as usize
                    {
                        // Try to spread across DCs, but don't block if we can't
                        let all_dcs_covered = selected_dcs.len()
                            >= self.unique_dc_count_for_tier(rule.tier);
                        if !all_dcs_covered {
                            continue;
                        }
                    }
                }
                FailureDomain::Rack => {
                    if selected_racks.contains(&vn.location.rack)
                        && selected.len() < rule.replica_count as usize
                    {
                        let all_racks_covered = selected_racks.len()
                            >= self.unique_rack_count_for_tier(rule.tier);
                        if !all_racks_covered {
                            continue;
                        }
                    }
                }
                FailureDomain::Host => {
                    // Already handled above
                }
            }

            // Prefer client locations for hot data
            if rule.prefer_client_locations && !client_locations.is_empty() && selected.is_empty() {
                // For the first replica, prefer a node in a client's DC
                let in_client_dc = client_locations
                    .iter()
                    .any(|cl| cl.datacenter == vn.location.datacenter);
                if !in_client_dc {
                    // Skip for now, but we'll fall through if we can't find any
                    continue;
                }
            }

            selected.push(vn.physical_node.clone());
            selected_hosts.push(vn.location.host.clone());
            selected_racks.push(vn.location.rack.clone());
            selected_dcs.push(vn.location.datacenter.clone());
        }

        // If we couldn't fill all replicas with strict rules, do a relaxed pass
        if selected.len() < rule.replica_count as usize {
            let iter = self
                .ring
                .range(start_position..)
                .chain(self.ring.range(..start_position));

            for (_, vn) in iter {
                if selected.len() >= rule.replica_count as usize {
                    break;
                }
                if vn.tier != rule.tier {
                    continue;
                }
                if selected.contains(&vn.physical_node) {
                    continue;
                }
                if selected_hosts.contains(&vn.location.host) {
                    continue;
                }
                selected.push(vn.physical_node.clone());
                selected_hosts.push(vn.location.host.clone());
            }
        }

        // P0-10: enforce the anchor-DC constraint. If a `required_datacenter`
        // is set and none of the selected replicas live there, attempt to
        // swap in a candidate from the required DC. We replace the LAST
        // (lowest-priority) selected node so the primary remains stable.
        if let Some(anchor) = &rule.required_datacenter {
            let already_in_anchor = selected
                .iter()
                .zip(selected_dcs.iter())
                .any(|(_, dc)| dc == anchor);
            if !already_in_anchor {
                // Find a candidate in the anchor DC. Bug Z — relax the tier
                // constraint for the anchor swap: in Plexmedia the warm tier
                // (rf=2) has no node in LELI, but the cold-tier
                // `data-primary01` lives there. Without this relaxation
                // the anchor invariant is "unsatisfiable" for every chunk
                // because no warm node exists in LELI. The anchor copy
                // landing on a different tier is acceptable — its role is
                // durability, not access latency.
                let iter = self
                    .ring
                    .range(start_position..)
                    .chain(self.ring.range(..start_position));
                let mut anchor_candidate: Option<NodeId> = None;
                let mut anchor_host: Option<String> = None;
                for (_, vn) in iter {
                    if vn.location.datacenter != *anchor {
                        continue;
                    }
                    // Anchor candidate must not collide with any *retained*
                    // selected host. We're going to drop the last slot, so
                    // ignore the last selected_hosts entry for this check.
                    let retained_hosts: &[String] = if !selected_hosts.is_empty() {
                        &selected_hosts[..selected_hosts.len() - 1]
                    } else {
                        &selected_hosts
                    };
                    if retained_hosts.contains(&vn.location.host) {
                        continue;
                    }
                    anchor_candidate = Some(vn.physical_node.clone());
                    anchor_host = Some(vn.location.host.clone());
                    break;
                }
                if let (Some(node), Some(host)) = (anchor_candidate, anchor_host) {
                    if selected.len() == rule.replica_count as usize {
                        // Replace the last slot with the anchor candidate.
                        if let Some(last) = selected.last_mut() {
                            *last = node;
                        }
                        if let Some(last) = selected_hosts.last_mut() {
                            *last = host;
                        }
                        if let Some(last) = selected_dcs.last_mut() {
                            *last = anchor.clone();
                        }
                    } else if selected.len() < rule.replica_count as usize {
                        selected.push(node);
                        selected_hosts.push(host);
                        selected_dcs.push(anchor.clone());
                    }
                }
                // If still no anchor candidate exists, the placement is
                // "anchor-unsatisfiable". Caller should detect via:
                //   !selected_dcs.iter().any(|d| d == anchor)
                // and emit a metric / audit entry.
            }
        }

        selected
    }

    /// Count unique datacenters that have nodes of a given tier.
    fn unique_dc_count_for_tier(&self, tier: DiskTier) -> usize {
        let mut dcs: Vec<&str> = self
            .ring
            .values()
            .filter(|vn| vn.tier == tier)
            .map(|vn| vn.location.datacenter.as_str())
            .collect();
        dcs.sort();
        dcs.dedup();
        dcs.len()
    }

    /// Count unique racks that have nodes of a given tier.
    fn unique_rack_count_for_tier(&self, tier: DiskTier) -> usize {
        let mut racks: Vec<String> = self
            .ring
            .values()
            .filter(|vn| vn.tier == tier)
            .map(|vn| format!("{}:{}", vn.location.datacenter, vn.location.rack))
            .collect();
        racks.sort();
        racks.dedup();
        racks.len()
    }

    /// Returns true if the ring has any virtual nodes.
    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    /// Number of virtual nodes on the ring.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Number of placement groups.
    pub fn num_pgs(&self) -> u32 {
        self.num_pgs
    }
}

/// Hash a string to a u32 ring position.
fn hash_position(key: &str) -> u32 {
    let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
    (hash & 0xFFFFFFFF) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::Location;

    fn make_location(dc: &str, rack: &str, host: &str) -> Location {
        Location::new("eu", dc, rack, host)
    }

    #[test]
    fn test_add_and_remove_node() {
        let mut ring = ConsistentHashRing::new(256);
        let node = NodeId::from_string("node-1".to_string());
        let loc = make_location("dc-a", "rack-1", "host-1");

        ring.add_node(&node, 0, DiskTier::Hot, &loc, 1.0);
        assert_eq!(ring.len(), 150);

        ring.remove_node(&node);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_chunk_to_pg() {
        let ring = ConsistentHashRing::new(256);
        let chunk_id = [42u8; 32];
        let pg = ring.chunk_to_pg(&chunk_id);
        assert!(pg < 256);
    }

    #[test]
    fn test_find_nodes_different_hosts() {
        let mut ring = ConsistentHashRing::new(256);

        for i in 0..5 {
            let node = NodeId::from_string(format!("node-{}", i));
            let loc = make_location("dc-a", &format!("rack-{}", i), &format!("host-{}", i));
            ring.add_node(&node, 0, DiskTier::Hot, &loc, 1.0);
        }

        let rule = PlacementRule {
            tier: DiskTier::Hot,
            replica_count: 3,
            failure_domain: FailureDomain::Host,
            prefer_client_locations: false,
            required_datacenter: None,
        };

        let nodes = ring.find_nodes_for_pg(0, &rule, &[]);
        assert_eq!(nodes.len(), 3);

        // All nodes should be different
        let unique: std::collections::HashSet<_> = nodes.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn test_tier_filtering() {
        let mut ring = ConsistentHashRing::new(256);

        let hot_node = NodeId::from_string("hot-node".to_string());
        let cold_node = NodeId::from_string("cold-node".to_string());
        let loc1 = make_location("dc-a", "rack-1", "host-1");
        let loc2 = make_location("dc-a", "rack-2", "host-2");

        ring.add_node(&hot_node, 0, DiskTier::Hot, &loc1, 1.0);
        ring.add_node(&cold_node, 0, DiskTier::Cold, &loc2, 1.0);

        let rule = PlacementRule {
            tier: DiskTier::Cold,
            replica_count: 1,
            failure_domain: FailureDomain::Host,
            prefer_client_locations: false,
            required_datacenter: None,
        };

        let nodes = ring.find_nodes_for_pg(0, &rule, &[]);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0], cold_node);
    }
}
