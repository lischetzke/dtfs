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

/// CO-3-11 — the parallel placement vectors produced by
/// [`ConsistentHashRing::find_placement_for_pg`]. `nodes[i]` lives in
/// `datacenters[i]` on rack `racks[i]` (the rack is stored canonically as
/// `"{datacenter}:{rack}"` so equally-named racks in different DCs never
/// alias). The three vectors are always the same length; the anchor (P0-10)
/// and failure-domain checks read them positionally, so they must stay in
/// lock-step on every push path (strict pass, relaxed fill, anchor swap).
#[derive(Debug, Clone, Default)]
pub struct Placement {
    pub nodes: Vec<NodeId>,
    pub datacenters: Vec<String>,
    pub racks: Vec<String>,
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

    /// Find N distinct physical nodes for a placement group, respecting tier
    /// and failure domain. Thin wrapper over [`Self::find_placement_for_pg`]
    /// for callers that only need the node ids.
    pub fn find_nodes_for_pg(
        &self,
        pg_id: u32,
        rule: &PlacementRule,
        client_locations: &[Location],
    ) -> Vec<NodeId> {
        self.find_placement_for_pg(pg_id, rule, client_locations).nodes
    }

    /// Find N distinct physical nodes for a placement group and return the
    /// parallel datacenter/rack vectors alongside the node ids (CO-3-11 seam).
    /// Exposing the parallel vectors lets callers and tests verify the P0-10
    /// anchor and failure-domain invariants directly instead of inferring
    /// them from the opaque node list.
    pub fn find_placement_for_pg(
        &self,
        pg_id: u32,
        rule: &PlacementRule,
        client_locations: &[Location],
    ) -> Placement {
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

        // CMN-7 — the ring is immutable across this walk, so the unique
        // DC/rack counts for this tier are the same on every visited
        // vnode. Hoist both out of the strict-pass loop instead of
        // re-scanning + sorting + deduping the whole ring (~1500 vnodes/
        // disk at weight 10) on every skipped vnode.
        let dc_count_for_tier = self.unique_dc_count_for_tier(rule.tier);
        let rack_count_for_tier = self.unique_rack_count_for_tier(rule.tier);

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
                        let all_dcs_covered = selected_dcs.len() >= dc_count_for_tier;
                        if !all_dcs_covered {
                            continue;
                        }
                    }
                }
                FailureDomain::Rack => {
                    // Compare the canonical "{dc}:{rack}" key (matching
                    // `unique_rack_count_for_tier`), so same-named racks in
                    // different DCs count as distinct failure domains.
                    let rack_key = rack_key(&vn.location);
                    if selected_racks.contains(&rack_key)
                        && selected.len() < rule.replica_count as usize
                    {
                        let all_racks_covered = selected_racks.len() >= rack_count_for_tier;
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
            selected_racks.push(rack_key(&vn.location));
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
                // CO-3-11: keep the parallel dc/rack vectors in lock-step
                // with `selected`, or the P0-10 anchor `zip` below goes blind
                // and the anchor swap can drop the wrong (or a duplicate) slot.
                selected.push(vn.physical_node.clone());
                selected_hosts.push(vn.location.host.clone());
                selected_racks.push(rack_key(&vn.location));
                selected_dcs.push(vn.location.datacenter.clone());
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
                .any(|(_, dc)| dc.eq_ignore_ascii_case(anchor));
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
                // When `selected` is already full we drop the last slot for
                // the anchor, so the last entry is NOT "retained" and may be
                // reused; when it isn't full we keep every entry.
                let full = selected.len() == rule.replica_count as usize;
                let retained = if full && !selected.is_empty() {
                    selected.len() - 1
                } else {
                    selected.len()
                };
                let mut anchor_candidate: Option<NodeId> = None;
                let mut anchor_host: Option<String> = None;
                let mut anchor_rack: Option<String> = None;
                for (_, vn) in iter {
                    if !vn.location.datacenter.eq_ignore_ascii_case(anchor) {
                        continue;
                    }
                    // The anchor candidate must not collide with any *retained*
                    // node or host, and must not duplicate a retained NodeId
                    // (a node can span tiers/disks, so dedup on the id too).
                    if selected[..retained].contains(&vn.physical_node) {
                        continue;
                    }
                    if selected_hosts[..retained].contains(&vn.location.host) {
                        continue;
                    }
                    anchor_candidate = Some(vn.physical_node.clone());
                    anchor_host = Some(vn.location.host.clone());
                    anchor_rack = Some(rack_key(&vn.location));
                    break;
                }
                if let (Some(node), Some(host), Some(rack)) =
                    (anchor_candidate, anchor_host, anchor_rack)
                {
                    if full {
                        // Replace the last slot with the anchor candidate.
                        if let Some(last) = selected.last_mut() {
                            *last = node;
                        }
                        if let Some(last) = selected_hosts.last_mut() {
                            *last = host;
                        }
                        if let Some(last) = selected_racks.last_mut() {
                            *last = rack;
                        }
                        if let Some(last) = selected_dcs.last_mut() {
                            *last = anchor.clone();
                        }
                    } else {
                        selected.push(node);
                        selected_hosts.push(host);
                        selected_racks.push(rack);
                        selected_dcs.push(anchor.clone());
                    }
                }
                // If still no anchor candidate exists, the placement is
                // "anchor-unsatisfiable". Caller should detect via:
                //   !selected_dcs.iter().any(|d| d == anchor)
                // and emit a metric / audit entry.
            }
        }

        Placement {
            nodes: selected,
            datacenters: selected_dcs,
            racks: selected_racks,
        }
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

/// CO-3-11 — canonical rack key. Racks are only globally unique when scoped
/// by datacenter, so `"{datacenter}:{rack}"` is the comparison/dedup key
/// everywhere (matches `unique_rack_count_for_tier`).
fn rack_key(loc: &Location) -> String {
    format!("{}:{}", loc.datacenter, loc.rack)
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

    // CO-3-11 ---------------------------------------------------------------

    /// The relaxed fill pass must keep `datacenters`/`racks` in lock-step with
    /// `nodes` and must never return a duplicate NodeId. Pre-fix the relaxed
    /// branch pushed only `nodes`+hosts, so the parallel vectors were shorter
    /// than `nodes` and the anchor swap operated on stale indices.
    #[test]
    fn co_3_11_relaxed_pass_keeps_vectors_in_lockstep_no_dup() {
        let mut ring = ConsistentHashRing::new(256);
        // Two DCs, one host each, all warm.
        ring.add_node(
            &NodeId::from_string("n-a".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-a", "rack-1", "host-a"),
            1.0,
        );
        ring.add_node(
            &NodeId::from_string("n-b".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-b", "rack-1", "host-b"),
            1.0,
        );

        // `prefer_client_locations` with a client in a DC that hosts no node
        // makes the strict pass skip every candidate for the (still-empty)
        // first slot, so it selects nothing and the relaxed pass does all the
        // work. `required_datacenter` then exercises the anchor `zip`.
        let rule = PlacementRule {
            tier: DiskTier::Warm,
            replica_count: 2,
            failure_domain: FailureDomain::Datacenter,
            prefer_client_locations: true,
            required_datacenter: Some("dc-a".to_string()),
        };
        let client = make_location("dc-z", "rack-9", "client-1");

        let placement = ring.find_placement_for_pg(0, &rule, &[client]);
        assert_eq!(placement.nodes.len(), 2, "relaxed pass should fill both");
        // Lock-step: the three parallel vectors are the same length.
        assert_eq!(placement.nodes.len(), placement.datacenters.len());
        assert_eq!(placement.nodes.len(), placement.racks.len());
        // No duplicate NodeId.
        let unique: std::collections::HashSet<_> = placement.nodes.iter().collect();
        assert_eq!(unique.len(), placement.nodes.len(), "no duplicate NodeId");
        // The anchor DC is actually represented (the zip can see it now).
        assert!(placement.datacenters.iter().any(|d| d == "dc-a"));
    }

    /// CR-3-7 / CMN-5 — the anchor-DC swap compares case-insensitively: a
    /// `required_datacenter` of "DC-A" is satisfied by a node registered
    /// under "dc-a" (mixed casing between config and registration must not
    /// make the anchor permanently unsatisfiable).
    #[test]
    fn cr_3_7_anchor_dc_swap_is_case_insensitive() {
        let mut ring = ConsistentHashRing::new(256);
        ring.add_node(
            &NodeId::from_string("n-a".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-a", "rack-1", "host-a"),
            1.0,
        );
        ring.add_node(
            &NodeId::from_string("n-b".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-b", "rack-1", "host-b"),
            1.0,
        );

        let rule = PlacementRule {
            tier: DiskTier::Warm,
            replica_count: 2,
            failure_domain: FailureDomain::Datacenter,
            prefer_client_locations: true,
            required_datacenter: Some("DC-A".to_string()),
        };
        let client = make_location("dc-z", "rack-9", "client-1");

        let placement = ring.find_placement_for_pg(0, &rule, &[client]);
        assert_eq!(placement.nodes.len(), 2);
        // The anchor DC ("dc-a") must be represented even though the rule
        // spelled it "DC-A".
        assert!(placement
            .datacenters
            .iter()
            .any(|d| d.eq_ignore_ascii_case("dc-a")));
    }

    /// FailureDomain::Rack must scope rack names by datacenter: two DCs each
    /// with a rack literally named "rack-1" are distinct failure domains, so a
    /// 2-replica rule selects both nodes. Pre-fix the bare-name compare treated
    /// them as the same rack and dropped the second node.
    #[test]
    fn co_3_11_rack_failure_domain_is_dc_scoped() {
        let mut ring = ConsistentHashRing::new(256);
        ring.add_node(
            &NodeId::from_string("n-a".to_string()),
            0,
            DiskTier::Hot,
            &make_location("dc-a", "rack-1", "host-a"),
            1.0,
        );
        ring.add_node(
            &NodeId::from_string("n-b".to_string()),
            0,
            DiskTier::Hot,
            &make_location("dc-b", "rack-1", "host-b"),
            1.0,
        );

        let rule = PlacementRule {
            tier: DiskTier::Hot,
            replica_count: 2,
            failure_domain: FailureDomain::Rack,
            prefer_client_locations: false,
            required_datacenter: None,
        };

        let placement = ring.find_placement_for_pg(0, &rule, &[]);
        assert_eq!(placement.nodes.len(), 2, "same-named racks in different DCs are distinct");
        let unique: std::collections::HashSet<_> = placement.nodes.iter().collect();
        assert_eq!(unique.len(), 2);
        // Racks are stored canonically as "{dc}:{rack}".
        assert!(placement.racks.contains(&"dc-a:rack-1".to_string()));
        assert!(placement.racks.contains(&"dc-b:rack-1".to_string()));
    }

    /// CMN-7 — hoisting `unique_dc_count_for_tier`/`unique_rack_count_for_tier`
    /// out of the strict-pass loop must not change the placement result: the
    /// ring is immutable during the walk, so both counts are constant across
    /// every visited vnode for a fixed tier. Multi-DC, multi-rack ring with a
    /// DC-scoped rule exercises both hoisted counts.
    #[test]
    fn cmn_7_hoisted_dc_rack_counts_dont_change_placement() {
        let mut ring = ConsistentHashRing::new(256);
        ring.add_node(
            &NodeId::from_string("n-a".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-a", "rack-1", "host-a"),
            1.0,
        );
        ring.add_node(
            &NodeId::from_string("n-b".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-b", "rack-1", "host-b"),
            1.0,
        );
        ring.add_node(
            &NodeId::from_string("n-c".to_string()),
            0,
            DiskTier::Warm,
            &make_location("dc-c", "rack-2", "host-c"),
            1.0,
        );

        let dc_rule = PlacementRule {
            tier: DiskTier::Warm,
            replica_count: 3,
            failure_domain: FailureDomain::Datacenter,
            prefer_client_locations: false,
            required_datacenter: None,
        };
        let placement = ring.find_placement_for_pg(7, &dc_rule, &[]);
        assert_eq!(placement.nodes.len(), 3, "one replica per DC");
        let unique_dcs: std::collections::HashSet<_> = placement.datacenters.iter().collect();
        assert_eq!(unique_dcs.len(), 3, "all three DCs represented");

        let rack_rule = PlacementRule {
            tier: DiskTier::Warm,
            replica_count: 3,
            failure_domain: FailureDomain::Rack,
            prefer_client_locations: false,
            required_datacenter: None,
        };
        let placement2 = ring.find_placement_for_pg(7, &rack_rule, &[]);
        assert_eq!(placement2.nodes.len(), 3, "one replica per rack");
        let unique_racks: std::collections::HashSet<_> = placement2.racks.iter().collect();
        assert_eq!(unique_racks.len(), 3, "all three dc-scoped racks represented");
    }
}
