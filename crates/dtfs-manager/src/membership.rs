//! CF-2 — online raft membership change (add-learner / promote / remove).
//!
//! Pure validation helpers for the `/api/v1/raft/{add-learner,promote,remove}`
//! endpoints in `api.rs`. Everything testable without a live Raft lives here;
//! the endpoints themselves only do auth + leader gating + the openraft call.

use dtfs_common::types::NodeRole;

/// Validate a membership-change target and derive its raft node id.
///
/// `addr` is required for add-learner (the leader stores it in the membership
/// config and every peer dials it for replication) and `None` for
/// promote/remove (the node is already in the membership, its addr is known).
/// A wildcard addr is rejected outright — Bug AC: peers dial the advertised
/// string verbatim, and `0.0.0.0:9401` routes each peer back to itself, so
/// replication never reaches the new node.
///
/// `registered_role` is the node-registry role already stored under the
/// derived node id, if any. `node_id = hash(hostname)` (composite-host
/// identity rule, CLAUDE.md § A.5), so a hostname that already belongs to a
/// Data/Client daemon means the operator passed the wrong name (e.g. the data
/// daemon's hostname on a composite host) — joining it would fuse two
/// daemons' identities. Refuse.
pub fn validate_membership_target(
    hostname: &str,
    addr: Option<&str>,
    registered_role: Option<NodeRole>,
) -> Result<u64, String> {
    let hostname = hostname.trim();
    if hostname.is_empty() {
        return Err("hostname required".to_string());
    }
    if let Some(addr) = addr {
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| format!("addr {addr:?} must be host:port"))?;
        if host.is_empty() || port.parse::<u16>().is_err() {
            return Err(format!("addr {addr:?} must be host:port"));
        }
        if host == "0.0.0.0" || host == "[::]" || host == "0" {
            return Err(format!(
                "addr {addr:?} is a wildcard — raft peers dial the advertised \
                 string verbatim (Bug AC); use the routable IP:port"
            ));
        }
    }
    let node_id = dtfs_common::node_id_from_hostname(hostname);
    match registered_role {
        Some(NodeRole::Manager) | None => Ok(node_id),
        Some(role) => Err(format!(
            "hostname {hostname:?} is already registered as a {role:?} node \
             (node_id {node_id}); node_id is hash(hostname), so the manager \
             needs its own unique hostname (composite-host identity rule)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cf2_empty_hostname_rejected() {
        assert!(validate_membership_target("", Some("10.0.0.1:9401"), None).is_err());
        assert!(validate_membership_target("   ", None, None).is_err());
    }

    #[test]
    fn cf2_addr_must_be_host_port() {
        for bad in ["10.0.0.1", "10.0.0.1:", ":9401", "10.0.0.1:notaport", ""] {
            assert!(
                validate_membership_target("mgr-x", Some(bad), None).is_err(),
                "addr {bad:?} should be rejected"
            );
        }
    }

    #[test]
    fn cf2_wildcard_addr_rejected() {
        // Bug AC — peers dial the advertised addr verbatim; a wildcard
        // self-routes and replication never arrives.
        for bad in ["0.0.0.0:9401", "[::]:9401", "0:9401"] {
            let err = validate_membership_target("mgr-x", Some(bad), None).unwrap_err();
            assert!(err.contains("wildcard"), "error should name the wildcard: {err}");
        }
    }

    #[test]
    fn cf2_non_manager_hostname_collision_rejected() {
        let err = validate_membership_target("dtfs-dc1-data01", None, Some(NodeRole::Data))
            .unwrap_err();
        assert!(err.contains("Data"), "error should name the colliding role: {err}");
        let err = validate_membership_target("data01_cln", None, Some(NodeRole::Client))
            .unwrap_err();
        assert!(err.contains("Client"), "error should name the colliding role: {err}");
    }

    #[test]
    fn cf2_valid_targets_return_hostname_hash_id() {
        let expect = dtfs_common::node_id_from_hostname("dtfs-dc1-mgr01");
        // add-learner shape: addr present, unregistered hostname.
        assert_eq!(
            validate_membership_target("dtfs-dc1-mgr01", Some("100.64.0.10:9401"), None),
            Ok(expect)
        );
        // re-add of an already-registered manager is fine (idempotent).
        assert_eq!(
            validate_membership_target(
                "dtfs-dc1-mgr01",
                Some("100.64.0.10:9401"),
                Some(NodeRole::Manager)
            ),
            Ok(expect)
        );
        // promote/remove shape: no addr needed.
        assert_eq!(
            validate_membership_target("dtfs-dc1-mgr01", None, Some(NodeRole::Manager)),
            Ok(expect)
        );
    }
}
