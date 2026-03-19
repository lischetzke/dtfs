use thiserror::Error;

#[derive(Error, Debug)]
pub enum DtfsError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Chunk not found: {0}")]
    ChunkNotFound(String),

    #[error("Chunk corrupt: {id}, expected checksum {expected}, got {actual}")]
    ChunkCorrupt {
        id: String,
        expected: String,
        actual: String,
    },

    #[error("Disk not found: {0}")]
    DiskNotFound(String),

    #[error("Disk full: {0}")]
    DiskFull(String),

    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Volume not found: {0}")]
    VolumeNotFound(String),

    #[error("No quorum available")]
    NoQuorum,

    #[error("Write safety not met: needed {needed} replicas, got {got}")]
    WriteSafetyNotMet { needed: u32, got: u32 },

    #[error("Write timeout after {0} seconds")]
    WriteTimeout(u32),

    #[error("Partition lockdown: {0}")]
    PartitionLockdown(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Authentication error: {0}")]
    Auth(String),

    #[error("Cluster join rejected: {0}")]
    JoinRejected(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("gRPC error: {0}")]
    Grpc(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type DtfsResult<T> = Result<T, DtfsError>;
