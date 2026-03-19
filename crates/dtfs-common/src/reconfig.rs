use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Trait for modules that support live configuration reload.
///
/// Every module (cache, scrub, replication, tiering, placement, etc.) implements
/// this trait. When cluster config changes via Raft, the manager pushes the new
/// config to affected nodes, which call `apply_config()` on the relevant module.
///
/// Implementations must be non-blocking and safe to call while the module is
/// actively serving requests.
#[async_trait]
pub trait Reconfigurable: Send + Sync {
    /// The specific configuration type this module uses.
    type Config: Send + Sync;

    /// Apply new configuration without restart.
    async fn apply_config(&self, config: &Self::Config) -> Result<()>;

    /// Get the current configuration.
    async fn current_config(&self) -> Self::Config;
}

/// Monotonically increasing version number for cluster configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ConfigVersion(pub u64);

impl ConfigVersion {
    pub fn initial() -> Self {
        Self(0)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}

impl Default for ConfigVersion {
    fn default() -> Self {
        Self::initial()
    }
}

/// Versioned cluster configuration that is replicated via Raft.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedConfig<T> {
    pub version: ConfigVersion,
    pub config: T,
}

impl<T> VersionedConfig<T> {
    pub fn new(config: T) -> Self {
        Self {
            version: ConfigVersion::initial(),
            config,
        }
    }

    pub fn update(&mut self, config: T) {
        self.version = self.version.next();
        self.config = config;
    }
}

/// Tracks which config version each node has applied.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConfigVersionTracker {
    pub node_versions: std::collections::BTreeMap<crate::types::NodeId, ConfigVersion>,
}

impl ConfigVersionTracker {
    pub fn new() -> Self {
        Self {
            node_versions: std::collections::BTreeMap::new(),
        }
    }

    /// Record that a node has applied a specific config version.
    pub fn update_node(&mut self, node_id: crate::types::NodeId, version: ConfigVersion) {
        self.node_versions.insert(node_id, version);
    }

    /// Check if a node is behind the current config version.
    pub fn is_behind(&self, node_id: &crate::types::NodeId, current: ConfigVersion) -> bool {
        match self.node_versions.get(node_id) {
            Some(v) => *v < current,
            None => true,
        }
    }

    /// Get all nodes that are behind the current config version.
    pub fn nodes_behind(&self, current: ConfigVersion) -> Vec<&crate::types::NodeId> {
        self.node_versions
            .iter()
            .filter(|(_, v)| **v < current)
            .map(|(id, _)| id)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::NodeId;

    #[test]
    fn test_config_version_ordering() {
        let v0 = ConfigVersion::initial();
        let v1 = v0.next();
        let v2 = v1.next();
        assert!(v0 < v1);
        assert!(v1 < v2);
    }

    #[test]
    fn test_versioned_config_update() {
        let mut vc = VersionedConfig::new("initial".to_string());
        assert_eq!(vc.version, ConfigVersion(0));

        vc.update("updated".to_string());
        assert_eq!(vc.version, ConfigVersion(1));
        assert_eq!(vc.config, "updated");
    }

    #[test]
    fn test_config_version_tracker() {
        let mut tracker = ConfigVersionTracker::new();
        let node1 = NodeId::from_string("node-1".to_string());
        let node2 = NodeId::from_string("node-2".to_string());

        let current = ConfigVersion(5);
        assert!(tracker.is_behind(&node1, current));

        tracker.update_node(node1.clone(), ConfigVersion(5));
        assert!(!tracker.is_behind(&node1, current));
        assert!(tracker.is_behind(&node2, current));

        let behind = tracker.nodes_behind(current);
        assert!(behind.is_empty() || !behind.contains(&&node1));
    }
}
