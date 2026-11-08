//! Node join protocol with secret validation — Phase 3 (Clustering).
//!
//! Handles the handshake when a new data-node requests to join the cluster,
//! including shared-secret verification, optional CA cert generation, and
//! initial capability exchange.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dtfs_common::crypto::{compute_join_hmac, generate_nonce, verify_join_hmac};
use dtfs_raft::{ClientRequest, ClientResponse, DtfsRaft, NodeInfo};
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::audit::AuditLog;

/// CO-2-3 — phase-1 challenges expire after this window. Short-lived: a
/// legitimate node completes the handshake within a few RPC round-trips; a
/// stale challenge MUST NOT remain forgeable indefinitely.
const CHALLENGE_TTL_MS: u64 = 60_000;

/// Handles the two-phase cluster join handshake.
pub struct JoinHandler {
    shared_secret: String,
    cluster_name: String,
    raft: Arc<DtfsRaft>,
    audit: Arc<AuditLog>,
    /// CO-2-3 — outstanding phase-1 challenges keyed by the server-issued
    /// challenge string, valued by their expiry instant (ms since epoch).
    /// Single-use: phase-2 removes the entry before verifying, so a replayed
    /// `complete` finds nothing.
    challenges: RwLock<HashMap<String, u64>>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The result returned to a joining node after the first handshake step.
pub struct JoinResult {
    pub accepted: bool,
    pub challenge: String,
    pub ca_cert_pem: Vec<u8>,
    pub rejection_reason: String,
}

/// Outcome of the second (completion) step.
pub struct JoinCompletion {
    pub success: bool,
    pub config_version: u64,
    pub config_json: String,
}

impl JoinHandler {
    pub fn new(
        shared_secret: String,
        cluster_name: String,
        raft: Arc<DtfsRaft>,
        audit: Arc<AuditLog>,
    ) -> Self {
        Self {
            shared_secret,
            cluster_name,
            raft,
            audit,
            challenges: RwLock::new(HashMap::new()),
        }
    }

    /// Phase 1 — validate the HMAC from the requesting node, optionally
    /// generate a CA certificate, and return cluster information.
    pub fn handle_join_request(
        &self,
        nonce: &str,
        hmac_bytes: &[u8],
        hostname: &str,
    ) -> JoinResult {
        // Verify HMAC(shared_secret, nonce).
        if !verify_join_hmac(&self.shared_secret, nonce, hmac_bytes) {
            warn!(hostname = %hostname, "Join request rejected: invalid HMAC");
            self.audit.log(
                hostname,
                "node.join.request",
                hostname,
                "rejected: invalid HMAC",
            );
            return JoinResult {
                accepted: false,
                challenge: String::new(),
                ca_cert_pem: Vec::new(),
                rejection_reason: "invalid shared secret".to_string(),
            };
        }

        // Generate a challenge nonce for the completion step and persist it
        // with a TTL so phase-2 can verify the caller actually holds the
        // shared secret (CO-2-3). Prune expired challenges opportunistically
        // so the map cannot grow without bound under join churn.
        let challenge = generate_nonce();
        {
            let now = now_ms();
            let mut map = self.challenges.write();
            map.retain(|_, &mut expiry| expiry > now);
            map.insert(challenge.clone(), now + CHALLENGE_TTL_MS);
        }

        info!("Join request accepted for {}", hostname);
        self.audit
            .log(hostname, "node.join.request", hostname, "accepted");

        JoinResult {
            accepted: true,
            challenge,
            // No join-time CA: TLS material comes from the [tls] config.
            ca_cert_pem: Vec::new(),
            rejection_reason: String::new(),
        }
    }

    /// CO-2-3 — pure proof-of-possession check for phase-2 (no Raft, no
    /// audit). Consumes the challenge single-use, then verifies
    /// `HMAC(shared_secret, challenge)` in constant time. Returns `Ok(())`
    /// when the caller is authorised; `Err(reason)` otherwise. An empty
    /// `shared_secret` is the documented legacy downgrade and always passes.
    fn verify_completion(&self, challenge: &str, completion_hmac: &[u8]) -> Result<(), &'static str> {
        verify_completion_inner(
            &self.shared_secret,
            &self.challenges,
            now_ms(),
            challenge,
            completion_hmac,
        )
    }

    /// Phase 2 — finalise the join by registering the node in Raft state.
    ///
    /// CO-2-3 (security) — proves the caller holds the cluster secret before
    /// any Raft write:
    /// * `challenge` MUST be a still-valid, not-yet-consumed challenge issued
    ///   by a prior phase-1 `handle_join_request` (single-use, TTL-bounded).
    /// * `completion_hmac` MUST equal `HMAC(shared_secret, challenge)`,
    ///   verified in constant time.
    /// * `node_id` is derived SERVER-SIDE from `info.hostname`; no client may
    ///   register under an id it does not own.
    ///
    /// Back-compat: when `shared_secret` is empty (legacy plaintext cluster)
    /// the challenge/HMAC checks are skipped — the join is unauthenticated,
    /// exactly as before. This is a documented downgrade; deploy a non-empty
    /// `cluster.shared_secret` to require proof of possession.
    pub async fn complete_join(
        &self,
        challenge: &str,
        completion_hmac: &[u8],
        info: NodeInfo,
    ) -> anyhow::Result<JoinCompletion> {
        // Derive the node id from the (server-trusted) hostname; never trust a
        // client-supplied id.
        let node_id = dtfs_common::node_id_from_hostname(&info.hostname);

        if let Err(reason) = self.verify_completion(challenge, completion_hmac) {
            warn!(
                "Join completion rejected for {}: {}",
                info.hostname, reason
            );
            self.audit.log(
                &info.hostname,
                "node.join.complete",
                &node_id.to_string(),
                &format!("rejected: {reason}"),
            );
            return Ok(JoinCompletion {
                success: false,
                config_version: 0,
                config_json: reason.to_string(),
            });
        }

        let req = ClientRequest::RegisterNode { node_id, info: info.clone() };

        match self.raft.client_write(req).await {
            Ok(resp) => {
                let data = resp.data;
                match data {
                    ClientResponse::Success | ClientResponse::FileCreated { .. } => {
                        info!("Node {} ({}) joined the cluster", node_id, info.hostname);
                        self.audit.log(
                            &info.hostname,
                            "node.join.complete",
                            &node_id.to_string(),
                            "success",
                        );
                        Ok(JoinCompletion {
                            success: true,
                            config_version: 0,
                            config_json: String::new(),
                        })
                    }
                    ClientResponse::Error { message } => {
                        warn!(node_id = %node_id, error = %message, "Join completion failed for node");
                        self.audit.log(
                            &info.hostname,
                            "node.join.complete",
                            &node_id.to_string(),
                            &format!("error: {}", message),
                        );
                        Ok(JoinCompletion {
                            success: false,
                            config_version: 0,
                            config_json: message,
                        })
                    }
                    // F-039 + P0-1 — RegisterNode never produces these, but exhaustive match required.
                    ClientResponse::LockGranted
                    | ClientResponse::LockConflict { .. }
                    | ClientResponse::WriteLeaseGranted { .. }
                    | ClientResponse::WriteLeaseConflict { .. }
                    | ClientResponse::WriteLeasesReleased { .. } => {
                        Ok(JoinCompletion {
                            success: false,
                            config_version: 0,
                            config_json: "unexpected non-membership response from RegisterNode".into(),
                        })
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Raft write failed during join");
                Err(anyhow::anyhow!("Raft write failed: {}", e))
            }
        }
    }

    /// Convenience: compute an HMAC for testing / bootstrap. Used for both the
    /// phase-1 request (over the client nonce) and the phase-2 completion (over
    /// the server-issued challenge) — same key, same construction.
    pub fn compute_hmac(&self, nonce: &str) -> Vec<u8> {
        compute_join_hmac(&self.shared_secret, nonce)
    }
}

/// CO-2-3 — pure phase-2 proof-of-possession check, factored out of
/// `JoinHandler` so it is testable without a live Raft node. Consumes the
/// challenge single-use, then constant-time-verifies `HMAC(secret, challenge)`.
/// An empty `secret` is the legacy unauthenticated downgrade and always passes.
fn verify_completion_inner(
    secret: &str,
    challenges: &RwLock<HashMap<String, u64>>,
    now: u64,
    challenge: &str,
    completion_hmac: &[u8],
) -> Result<(), &'static str> {
    if secret.is_empty() {
        return Ok(());
    }
    // Single-use: consume the challenge regardless of outcome so a replay
    // cannot reuse it.
    let known = {
        let mut map = challenges.write();
        match map.remove(challenge) {
            Some(expiry) => expiry > now,
            None => false,
        }
    };
    if !known {
        return Err("unknown or expired challenge");
    }
    if !verify_join_hmac(secret, challenge, completion_hmac) {
        return Err("invalid shared secret");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "cluster-secret";

    fn store(entries: &[(&str, u64)]) -> RwLock<HashMap<String, u64>> {
        RwLock::new(entries.iter().map(|(c, e)| (c.to_string(), *e)).collect())
    }

    #[test]
    fn co_2_3_legit_completion_succeeds_and_is_single_use() {
        let challenges = store(&[("chal-1", 10_000)]);
        let hmac = compute_join_hmac(SECRET, "chal-1");
        // First completion with a valid challenge + correct HMAC succeeds.
        assert!(verify_completion_inner(SECRET, &challenges, 1_000, "chal-1", &hmac).is_ok());
        // Replay of the same challenge is rejected (single-use consumed it).
        assert_eq!(
            verify_completion_inner(SECRET, &challenges, 1_000, "chal-1", &hmac),
            Err("unknown or expired challenge"),
        );
    }

    #[test]
    fn co_2_3_no_prior_request_rejected() {
        let challenges = store(&[]);
        let hmac = compute_join_hmac(SECRET, "never-issued");
        assert_eq!(
            verify_completion_inner(SECRET, &challenges, 1_000, "never-issued", &hmac),
            Err("unknown or expired challenge"),
        );
    }

    #[test]
    fn co_2_3_expired_challenge_rejected() {
        let challenges = store(&[("chal-x", 500)]);
        let hmac = compute_join_hmac(SECRET, "chal-x");
        // now (1_000) is past the challenge expiry (500).
        assert_eq!(
            verify_completion_inner(SECRET, &challenges, 1_000, "chal-x", &hmac),
            Err("unknown or expired challenge"),
        );
    }

    #[test]
    fn co_2_3_bad_hmac_rejected() {
        let challenges = store(&[("chal-2", 10_000)]);
        let forged = compute_join_hmac("wrong-secret", "chal-2");
        assert_eq!(
            verify_completion_inner(SECRET, &challenges, 1_000, "chal-2", &forged),
            Err("invalid shared secret"),
        );
    }

    #[test]
    fn co_2_3_empty_secret_is_legacy_downgrade() {
        // No shared secret configured: unauthenticated join preserved (no
        // challenge required, any/empty HMAC accepted).
        let challenges = store(&[]);
        assert!(verify_completion_inner("", &challenges, 1_000, "anything", &[]).is_ok());
    }

    #[test]
    fn co_2_3_node_id_is_derived_from_hostname() {
        // The server never trusts a client-supplied id; the id is a pure
        // function of the hostname.
        assert_eq!(
            dtfs_common::node_id_from_hostname("dtfs-data01"),
            dtfs_common::node_id_from_hostname("dtfs-data01"),
        );
        assert_ne!(
            dtfs_common::node_id_from_hostname("dtfs-data01"),
            dtfs_common::node_id_from_hostname("dtfs-data02"),
        );
    }
}
