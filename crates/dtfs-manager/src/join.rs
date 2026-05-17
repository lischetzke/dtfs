//! Node join protocol with secret validation — Phase 3 (Clustering).
//!
//! Handles the handshake when a new data-node requests to join the cluster,
//! including shared-secret verification, optional CA cert generation, and
//! initial capability exchange.

use std::sync::Arc;

use dtfs_common::crypto::{
    compute_join_hmac, generate_cluster_ca, generate_node_cert, generate_nonce, verify_join_hmac,
};
use dtfs_raft::{ClientRequest, ClientResponse, DtfsRaft, NodeInfo};
use parking_lot::RwLock;
use tracing::{info, warn};

use crate::audit::AuditLog;

/// Cached cluster CA material (lazily generated on first join if auto-TLS).
struct CaState {
    ca_cert_pem: Vec<u8>,
    ca_key_pem: Vec<u8>,
}

/// Handles the two-phase cluster join handshake.
pub struct JoinHandler {
    shared_secret: String,
    cluster_name: String,
    raft: Arc<DtfsRaft>,
    audit: Arc<AuditLog>,
    /// Lazily generated CA certificate for auto-TLS.
    ca: RwLock<Option<CaState>>,
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
            ca: RwLock::new(None),
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
            warn!("Join request rejected: invalid HMAC from {}", hostname);
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

        // Ensure a cluster CA exists (lazy init for auto-TLS).
        let ca_cert_pem = {
            let mut ca = self.ca.write();
            if ca.is_none() {
                match generate_cluster_ca(&self.cluster_name) {
                    Ok((cert, key)) => {
                        *ca = Some(CaState {
                            ca_cert_pem: cert,
                            ca_key_pem: key,
                        });
                    }
                    Err(e) => {
                        warn!("Failed to generate cluster CA: {}", e);
                    }
                }
            }
            ca.as_ref().map(|c| c.ca_cert_pem.clone()).unwrap_or_default()
        };

        // Generate a challenge nonce for the completion step.
        let challenge = generate_nonce();

        info!("Join request accepted for {}", hostname);
        self.audit
            .log(hostname, "node.join.request", hostname, "accepted");

        JoinResult {
            accepted: true,
            challenge,
            ca_cert_pem,
            rejection_reason: String::new(),
        }
    }

    /// Generate a node certificate signed by the cluster CA.
    pub fn generate_node_certificate(
        &self,
        hostname: &str,
    ) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        let ca = self.ca.read();
        let ca = ca
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Cluster CA not initialized"))?;
        generate_node_cert(&ca.ca_cert_pem, &ca.ca_key_pem, hostname)
    }

    /// Phase 2 — finalise the join by registering the node in Raft state.
    pub async fn complete_join(
        &self,
        node_id: u64,
        info: NodeInfo,
    ) -> anyhow::Result<JoinCompletion> {
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
                        warn!("Join completion failed for node {}: {}", node_id, message);
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
                warn!("Raft write failed during join: {}", e);
                Err(anyhow::anyhow!("Raft write failed: {}", e))
            }
        }
    }

    /// Convenience: compute an HMAC for testing / bootstrap.
    pub fn compute_hmac(&self, nonce: &str) -> Vec<u8> {
        compute_join_hmac(&self.shared_secret, nonce)
    }
}
