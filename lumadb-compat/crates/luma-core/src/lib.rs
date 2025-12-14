pub mod types;
pub mod transactions;

use async_trait::async_trait;
pub use types::Value;
pub use transactions::{Transaction, TransactionManager, IsolationLevel, TransactionOptions};
use std::net::SocketAddr;
use thiserror::Error;

/// Core error type for protocol operations
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("IO Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protocol Error: {0}")]
    Protocol(String),
    #[error("Authentication Failed: {0}")]
    Auth(String),
    #[error("Internal Error: {0}")]
    Internal(String),
    #[error("Type Conversion Error: {0}")]
    TypeConversion(String),
}


pub mod ir;
pub mod indexing;
pub mod compression;
pub type Result<T> = std::result::Result<T, ProtocolError>;
pub use ir::*;
pub mod parsing;
pub mod storage;
pub mod query;
pub mod vector;
pub mod security;
pub mod ai;
pub mod processor;
pub use processor::{QueryProcessor, MockQueryProcessor, QueryRequest, QueryResult};
pub mod remote;
pub mod ingestion; // Phase 15: Observability
pub mod stream; // Stream Processing Engine (Phase 10)
pub mod distributed; // Phase: Raft Consensus
pub mod timescale; // TimescaleDB extensions
pub mod luma {
    pub mod v3 {
        tonic::include_proto!("luma.v3");
    }
}
pub use remote::RemoteQueryProcessor;

/// Trait for a protocol adapter (e.g., Postgres, MySQL)
#[async_trait]
pub trait ProtocolAdapter: Send + Sync {
    /// The port this protocol listens on by default
    fn default_port(&self) -> u16;

    /// Handle a new connection
    async fn handle_connection(
        &self,
        socket: tokio::net::TcpStream,
        addr: SocketAddr,
        processor: Box<dyn QueryProcessor>,
    ) -> Result<()>;
}
