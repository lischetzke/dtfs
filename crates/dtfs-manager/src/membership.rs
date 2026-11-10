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

/// Refuse a membership target older than an apply gate the cluster has
/// opened (`cluster_on_v0_1_15` / `cluster_understands_named_remove`): it
/// would replay the log with the legacy apply rules and diverge.
/// `registered` is the target's node-registry `(role, version)`, if any.
pub fn ensure_target_meets_apply_gates(
    hostname: &str,
    registered: Option<(NodeRole, &str)>,
    v0_1_16_open: bool,
    v0_1_15_open: bool,
    named_remove_open: bool,
) -> Result<(), String> {
    let min = if v0_1_16_open {
        dtfs_raft::state_machine::V0_1_16_MIN_VERSION
    } else if v0_1_15_open {
        dtfs_raft::state_machine::V0_1_15_MIN_VERSION
    } else if named_remove_open {
        dtfs_raft::state_machine::NAMED_REMOVE_MIN_VERSION
    } else {
        return Ok(());
    };
    if let Some((NodeRole::Manager, v)) = registered {
        if dtfs_common::version::release_at_least(v, min) {
            return Ok(());
        }
    }
    let seen = match registered {
        Some((role, v)) => format!("{role:?} {v:?}"),
        None => "no".to_string(),
    };
    let min = format!("v{}.{}.{}", min.0, min.1, min.2);
    tracing::warn!(hostname, registered = %seen, required = %min, "membership change refused: target below an open apply gate");
    Err(format!(
        "{hostname:?} must be a registered manager on {min} or later: the cluster's {min} apply \
         rules are active and an older manager would diverge on log replay (registered: {seen}); \
         start it on the current release first"
    ))
}

/// Refuse a voter-set change that would leave the cluster unable to commit.
///
/// Openraft commits the joint configuration with a majority of BOTH the old
/// and the new voter set, so each set must have a majority of members that
/// are reachable and caught up. `reachable_caught_up` must include the
/// leader itself (build it from the raft metrics replication map: members
/// whose matched log index is at or near the leader's last log index).
/// Pure so the endpoints in `api/raft.rs` only gather the sets.
pub fn ensure_voter_change_has_quorum(
    current_voters: &std::collections::BTreeSet<u64>,
    target_voters: &std::collections::BTreeSet<u64>,
    reachable_caught_up: &std::collections::BTreeSet<u64>,
) -> Result<(), String> {
    if target_voters.is_empty() {
        return Err("target voter set is empty".to_string());
    }
    for (label, set) in [("current", current_voters), ("new", target_voters)] {
        let have = set.intersection(reachable_caught_up).count();
        let need = set.len() / 2 + 1;
        if have < need {
            tracing::warn!(
                set = label,
                reachable_caught_up = have,
                needed = need,
                "membership change refused: no majority of reachable, caught-up voters"
            );
            return Err(format!(
                "refusing membership change: only {have} of {} {label} voters are                  reachable and caught up (need {need})",
                set.len()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_gates_refuse_v0_1_14_target_when_v0_1_15_gate_open() {
        let err = ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.1.14")), false, true, true)
            .unwrap_err();
        assert!(err.contains("on v0.1.15 or later"), "{err}");
        // Only the named-remove gate open: v0.1.14 is enough, v0.1.13 is not.
        assert!(ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.1.14")), false, false, true).is_ok());
        let err = ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.1.13")), false, false, true)
            .unwrap_err();
        assert!(err.contains("on v0.1.14 or later"), "{err}");
    }

    #[test]
    fn apply_gates_v0_1_16_gate_wins_when_open() {
        let err = ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.1.15")), true, true, true)
            .unwrap_err();
        assert!(err.contains("v0.1.16"), "{err}");
        assert!(ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.1.16")), true, true, true).is_ok());
    }

    #[test]
    fn apply_gates_refuse_unregistered_or_non_manager_target() {
        let err = ensure_target_meets_apply_gates("mgr06", None, false, false, true).unwrap_err();
        assert!(err.contains("(registered: no)"), "{err}");
        let err = ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Data, "v0.1.15")), false, true, true)
            .unwrap_err();
        assert!(err.contains("(registered: Data \"v0.1.15\")"), "{err}");
        // A dev build never satisfies an open gate.
        assert!(ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.0.0-dev")), false, true, true).is_err());
    }

    #[test]
    fn apply_gates_allow_any_target_when_closed_and_current_release_when_open() {
        assert!(ensure_target_meets_apply_gates("mgr06", None, false, false, false).is_ok());
        assert!(ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.0.0-dev")), false, false, false).is_ok());
        assert!(ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.1.15")), false, true, true).is_ok());
        assert!(ensure_target_meets_apply_gates("mgr06", Some((NodeRole::Manager, "v0.2.0")), false, true, true).is_ok());
    }

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

    fn set(ids: &[u64]) -> std::collections::BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    #[test]
    fn quorum_ok_when_majority_of_both_sets_reachable() {
        // 3 voters -> remove 3, leaving {1,2}; all reachable.
        assert_eq!(
            ensure_voter_change_has_quorum(&set(&[1, 2, 3]), &set(&[1, 2]), &set(&[1, 2, 3])),
            Ok(())
        );
        // Boundary: exactly a majority (2 of 3) of each set.
        assert_eq!(
            ensure_voter_change_has_quorum(&set(&[1, 2, 3]), &set(&[1, 2, 3]), &set(&[1, 2])),
            Ok(())
        );
    }

    #[test]
    fn quorum_refused_when_new_set_lacks_majority() {
        // Promote learner 4 into {1,2,3,4}: 4 is not caught up, only 1,2,3
        // reachable -> 3 of 4 is fine, but with only {1,2} it is not.
        let err = ensure_voter_change_has_quorum(
            &set(&[1, 2, 3]),
            &set(&[1, 2, 3, 4]),
            &set(&[1, 2]),
        )
        .unwrap_err();
        assert!(err.contains("2 of 4 new voters") && err.contains("need 3"), "{err}");
    }

    #[test]
    fn quorum_refused_when_old_set_lacks_majority() {
        // New set {1} is fully reachable but the old set {1,2,3} is not.
        let err = ensure_voter_change_has_quorum(&set(&[1, 2, 3]), &set(&[1]), &set(&[1]))
            .unwrap_err();
        assert!(err.contains("1 of 3 current voters"), "{err}");
    }

    #[test]
    fn quorum_refused_for_empty_target() {
        assert!(ensure_voter_change_has_quorum(&set(&[1]), &set(&[]), &set(&[1])).is_err());
    }
}
