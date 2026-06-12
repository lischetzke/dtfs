//! CF-6 — in-process loopback test for gRPC mTLS.
//!
//! Generates a cluster CA + node certs via `dtfs_common::crypto`, writes
//! them to a tempdir, starts a real tonic server with the resolved
//! `TlsContext`, and asserts:
//!   * a client holding the right CA + a CA-signed client cert round-trips
//!     an RPC (mutual TLS works end-to-end),
//!   * a client trusting/presenting a DIFFERENT CA is rejected,
//!   * a client with the right CA but NO client certificate is rejected
//!     (client auth is mandatory),
//!   * a plaintext client cannot talk to a TLS server,
//!   * the default config (mode = "auto", no cert files) still serves
//!     plaintext — back-compat for every existing deployment.

use std::path::PathBuf;

use dtfs_common::config::TlsConfig;
use dtfs_common::crypto::{generate_cluster_ca, generate_node_cert};
use dtfs_common::tls::TlsContext;
use dtfs_proto::dtfs::data::data_service_client::DataServiceClient;
use dtfs_proto::dtfs::data::data_service_server::{DataService, DataServiceServer};
use dtfs_proto::dtfs::data::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

// ---------------------------------------------------------------------------
// Minimal DataService stub — only verify_chunk answers.
// ---------------------------------------------------------------------------

struct StubDataService;

#[tonic::async_trait]
impl DataService for StubDataService {
    type GetChunkStream =
        tokio_stream::wrappers::ReceiverStream<Result<GetChunkResponse, Status>>;

    async fn put_chunk(
        &self,
        _req: Request<tonic::Streaming<PutChunkRequest>>,
    ) -> Result<Response<PutChunkResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn get_chunk(
        &self,
        _req: Request<GetChunkRequest>,
    ) -> Result<Response<Self::GetChunkStream>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn delete_chunk(
        &self,
        _req: Request<DeleteChunkRequest>,
    ) -> Result<Response<DeleteChunkResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn replicate_chunk(
        &self,
        _req: Request<ReplicateChunkRequest>,
    ) -> Result<Response<ReplicateChunkResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn verify_chunk(
        &self,
        _req: Request<VerifyChunkRequest>,
    ) -> Result<Response<VerifyChunkResponse>, Status> {
        Ok(Response::new(VerifyChunkResponse::default()))
    }

    async fn get_disk_status(
        &self,
        _req: Request<DiskStatusRequest>,
    ) -> Result<Response<DiskStatusResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn report_stats(
        &self,
        _req: Request<StatsRequest>,
    ) -> Result<Response<StatsResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn record_cache_hit(
        &self,
        _req: Request<RecordCacheHitRequest>,
    ) -> Result<Response<RecordCacheHitResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }

    async fn list_chunks(
        &self,
        _req: Request<ListChunksRequest>,
    ) -> Result<Response<ListChunksResponse>, Status> {
        Err(Status::unimplemented("stub"))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Write a PEM blob to `dir/name` and return the path.
fn write_pem(dir: &std::path::Path, name: &str, pem: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, pem).unwrap();
    p
}

/// Generate a CA + one node cert for 127.0.0.1, write all PEMs under
/// `dir` with the given prefix, and return the resolved (enabled)
/// TlsContext for that material.
fn issue_identity(dir: &std::path::Path, prefix: &str, ca: &(Vec<u8>, Vec<u8>)) -> TlsContext {
    let (cert, key) = generate_node_cert(&ca.0, &ca.1, "127.0.0.1").unwrap();
    let cert_file = write_pem(dir, &format!("{prefix}.crt"), &cert);
    let key_file = write_pem(dir, &format!("{prefix}.key"), &key);
    let ca_file = write_pem(dir, &format!("{prefix}-ca.crt"), &ca.0);
    let cfg = TlsConfig {
        mode: "auto".to_string(),
        cert_file: Some(cert_file),
        key_file: Some(key_file),
        ca_file: Some(ca_file),
    };
    let ctx = TlsContext::from_config(&cfg).unwrap();
    assert!(ctx.is_enabled(), "context with on-disk certs must enable TLS");
    ctx
}

/// Start the stub server with the given context on 127.0.0.1:0; returns
/// the bound `ip:port`.
async fn start_server(ctx: &TlsContext) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut builder = tonic::transport::Server::builder();
    if let Some(server_tls) = ctx.server_tls() {
        builder = builder.tls_config(server_tls).unwrap();
    }
    tokio::spawn(
        builder
            .add_service(DataServiceServer::new(StubDataService))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    format!("127.0.0.1:{}", addr.port())
}

async fn rpc_roundtrip(ctx: &TlsContext, addr: &str) -> anyhow::Result<()> {
    let channel = ctx.connect(addr).await?;
    let mut client = DataServiceClient::new(channel);
    client
        .verify_chunk(VerifyChunkRequest::default())
        .await
        .map_err(|e| anyhow::anyhow!("verify_chunk: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn mtls_roundtrip_with_cluster_ca() {
    let dir = tempfile::tempdir().unwrap();
    let ca = generate_cluster_ca("loopback").unwrap();
    let server_ctx = issue_identity(dir.path(), "server", &ca);
    let client_ctx = issue_identity(dir.path(), "client", &ca);

    let addr = start_server(&server_ctx).await;
    rpc_roundtrip(&client_ctx, &addr)
        .await
        .expect("mTLS client with the cluster CA must round-trip an RPC");
}

#[tokio::test]
async fn wrong_ca_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let ca = generate_cluster_ca("loopback").unwrap();
    let rogue_ca = generate_cluster_ca("rogue").unwrap();
    let server_ctx = issue_identity(dir.path(), "server", &ca);
    // Client trusts + presents the ROGUE CA's material.
    let client_ctx = issue_identity(dir.path(), "rogue-client", &rogue_ca);

    let addr = start_server(&server_ctx).await;
    let res = rpc_roundtrip(&client_ctx, &addr).await;
    assert!(res.is_err(), "client with the wrong CA must be rejected, got Ok");
}

#[tokio::test]
async fn missing_client_cert_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let ca = generate_cluster_ca("loopback").unwrap();
    let server_ctx = issue_identity(dir.path(), "server", &ca);
    let addr = start_server(&server_ctx).await;

    // Hand-rolled client TLS: trusts the cluster CA but presents NO
    // identity. The server requires client certs, so this must fail.
    let tls = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(&ca.0));
    let res = async {
        let channel = tonic::transport::Endpoint::from_shared(format!("https://{addr}"))?
            .tls_config(tls)?
            .connect()
            .await?;
        DataServiceClient::new(channel)
            .verify_chunk(VerifyChunkRequest::default())
            .await
            .map_err(|e| anyhow::anyhow!("rpc: {e}"))?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    assert!(res.is_err(), "client without a client cert must be rejected");
}

#[tokio::test]
async fn plaintext_client_cannot_reach_tls_server() {
    let dir = tempfile::tempdir().unwrap();
    let ca = generate_cluster_ca("loopback").unwrap();
    let server_ctx = issue_identity(dir.path(), "server", &ca);
    let addr = start_server(&server_ctx).await;

    let res = rpc_roundtrip(&TlsContext::plaintext(), &addr).await;
    assert!(res.is_err(), "plaintext client against a TLS server must fail");
}

#[tokio::test]
async fn default_config_still_serves_plaintext() {
    // Back-compat: mode = "auto" with no certificates configured resolves
    // to plaintext, the server listens plaintext, plaintext clients work.
    let ctx = TlsContext::from_config(&TlsConfig::default()).unwrap();
    assert!(!ctx.is_enabled(), "default config must stay plaintext");

    let addr = start_server(&ctx).await;
    rpc_roundtrip(&TlsContext::plaintext(), &addr)
        .await
        .expect("plaintext server must keep serving plaintext clients");
}
