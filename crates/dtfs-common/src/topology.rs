use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

use crate::types::{DiskInfo, NodeId, NodeRole};

/// 4-level location hierarchy: Region > Datacenter > Rack > Host.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Location {
    pub region: String,
    pub datacenter: String,
    pub rack: String,
    pub host: String,
}

impl Location {
    pub fn new(region: &str, datacenter: &str, rack: &str, host: &str) -> Self {
        Self {
            region: region.to_string(),
            datacenter: datacenter.to_string(),
            rack: rack.to_string(),
            host: host.to_string(),
        }
    }

    /// Compute distance between two locations.
    /// Lower values mean closer proximity.
    pub fn distance(&self, other: &Location) -> LocationDistance {
        if self.host == other.host
            && self.rack == other.rack
            && self.datacenter == other.datacenter
            && self.region == other.region
        {
            LocationDistance::SameHost
        } else if self.rack == other.rack
            && self.datacenter == other.datacenter
            && self.region == other.region
        {
            LocationDistance::SameRack
        } else if self.datacenter == other.datacenter && self.region == other.region {
            LocationDistance::SameDatacenter
        } else if self.region == other.region {
            LocationDistance::SameRegion
        } else {
            LocationDistance::CrossRegion
        }
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.region, self.datacenter, self.rack, self.host
        )
    }
}

/// Distance between two locations (lower ordinal = closer).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum LocationDistance {
    SameHost = 0,
    SameRack = 1,
    SameDatacenter = 2,
    SameRegion = 3,
    CrossRegion = 4,
}

/// A node registered in the topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNode {
    pub id: NodeId,
    pub role: NodeRole,
    pub hostname: String,
    pub grpc_addr: String,
    pub location: Location,
    pub disks: Vec<DiskInfo>,
}

/// The cluster topology map, maintained in Raft state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologyMap {
    pub nodes: BTreeMap<NodeId, TopologyNode>,
}

impl TopologyMap {
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
        }
    }

    pub fn add_node(&mut self, node: TopologyNode) {
        self.nodes.insert(node.id.clone(), node);
    }

    pub fn remove_node(&mut self, id: &NodeId) -> Option<TopologyNode> {
        self.nodes.remove(id)
    }

    pub fn get_node(&self, id: &NodeId) -> Option<&TopologyNode> {
        self.nodes.get(id)
    }

    /// Get all nodes in a specific datacenter.
    pub fn nodes_in_dc(&self, datacenter: &str) -> Vec<&TopologyNode> {
        self.nodes
            .values()
            .filter(|n| n.location.datacenter == datacenter)
            .collect()
    }

    /// Get all data nodes.
    pub fn data_nodes(&self) -> Vec<&TopologyNode> {
        self.nodes
            .values()
            .filter(|n| n.role == NodeRole::Data)
            .collect()
    }

    /// Get all manager nodes.
    pub fn manager_nodes(&self) -> Vec<&TopologyNode> {
        self.nodes
            .values()
            .filter(|n| n.role == NodeRole::Manager)
            .collect()
    }

    /// Get all unique datacenter names.
    pub fn datacenters(&self) -> Vec<String> {
        let mut dcs: Vec<String> = self
            .nodes
            .values()
            .map(|n| n.location.datacenter.clone())
            .collect();
        dcs.sort();
        dcs.dedup();
        dcs
    }

    /// Find the nearest data node to a given location that has a specific node role.
    pub fn nearest_nodes(&self, location: &Location, role: NodeRole) -> Vec<&TopologyNode> {
        let mut nodes: Vec<&TopologyNode> = self
            .nodes
            .values()
            .filter(|n| n.role == role)
            .collect();
        nodes.sort_by_key(|n| n.location.distance(location));
        nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_location_distance() {
        let a = Location::new("eu", "frankfurt", "rack-a1", "node-01");
        let b = Location::new("eu", "frankfurt", "rack-a1", "node-01");
        assert_eq!(a.distance(&b), LocationDistance::SameHost);

        let c = Location::new("eu", "frankfurt", "rack-a1", "node-02");
        assert_eq!(a.distance(&c), LocationDistance::SameRack);

        let d = Location::new("eu", "frankfurt", "rack-b1", "node-03");
        assert_eq!(a.distance(&d), LocationDistance::SameDatacenter);

        let e = Location::new("eu", "amsterdam", "rack-a1", "node-01");
        assert_eq!(a.distance(&e), LocationDistance::SameRegion);

        let f = Location::new("us", "east", "rack-a1", "node-01");
        assert_eq!(a.distance(&f), LocationDistance::CrossRegion);
    }

    #[test]
    fn test_location_distance_ordering() {
        assert!(LocationDistance::SameHost < LocationDistance::SameRack);
        assert!(LocationDistance::SameRack < LocationDistance::SameDatacenter);
        assert!(LocationDistance::SameDatacenter < LocationDistance::SameRegion);
        assert!(LocationDistance::SameRegion < LocationDistance::CrossRegion);
    }

    #[test]
    fn test_location_display() {
        let loc = Location::new("eu", "frankfurt", "rack-a1", "node-01");
        assert_eq!(loc.to_string(), "eu/frankfurt/rack-a1/node-01");
    }
}
