//! gRPC API implementation (stub)

use std::sync::Arc;

use tracing::info;

use lumadb_common::config::GrpcApiConfig;
use lumadb_common::error::Result;
use lumadb_query::QueryEngine;
use lumadb_streaming::StreamingEngine;
use lumadb_security::SecurityManager;

/// gRPC server
#[derive(Clone)]
pub struct GrpcServer {
    config: GrpcApiConfig,
}

impl GrpcServer {
    /// Create a new gRPC server
    pub async fn new(
        config: &GrpcApiConfig,
        _query: Arc<QueryEngine>,
        _streaming: Arc<StreamingEngine>,
        _security: Arc<SecurityManager>,
    ) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
        })
    }

    /// Run the gRPC server
    pub async fn run(&self) -> Result<()> {
        info!("gRPC API server listening on port {}", self.config.port);
        // gRPC implementation would go here using Tonic
        Ok(())
    }

    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down gRPC API server");
        Ok(())
    }
}
