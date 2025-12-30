//! Unified Single-Port Multi-Protocol Server
//!
//! All protocols accessible via path prefixes on a single port.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    http::{Method, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::signal;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

use crate::core::StorageEngine;
use crate::d1::{D1Config, D1Server};
use crate::dynamodb::{DynamoDBConfig, DynamoDBServer};
use crate::storage::LumaStorage;
use crate::turso::{TursoConfig, TursoServer};

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub bind_address: String,
    pub port: u16,
    pub enable_cors: bool,
    pub enable_tracing: bool,
    pub enable_dynamodb: bool,
    pub enable_d1: bool,
    pub enable_turso: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 8000,
            enable_cors: true,
            enable_tracing: true,
            enable_dynamodb: true,
            enable_d1: true,
            enable_turso: true,
        }
    }
}

/// Unified multi-protocol server
pub struct MultiProtocolServer {
    config: ServerConfig,
    storage: Arc<dyn StorageEngine>,
    start_time: Instant,
}

impl MultiProtocolServer {
    /// Create with default storage
    pub fn new(config: ServerConfig) -> Self {
        Self {
            config,
            storage: Arc::new(LumaStorage::new()),
            start_time: Instant::now(),
        }
    }

    /// Create with custom storage
    pub fn with_storage(config: ServerConfig, storage: Arc<dyn StorageEngine>) -> Self {
        Self { 
            config, 
            storage,
            start_time: Instant::now(),
        }
    }

    /// Build the unified router
    pub fn router(&self) -> Router {
        let storage = self.storage.clone();
        let config = self.config.clone();
        let start_time = self.start_time;

        // Health endpoints (stateless)
        let health_router = Router::new()
            .route("/health", get(|| async {
                Json(json!({
                    "status": "healthy",
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }))
            }))
            .route("/ready", get(|| async {
                Json(json!({ "status": "ready" }))
            }))
            .route("/metrics", get(move || {
                let uptime = start_time.elapsed().as_secs();
                async move {
                    Json(json!({
                        "uptime_seconds": uptime,
                        "protocols": ["dynamodb", "d1", "turso"]
                    }))
                }
            }))
            .route("/info", get(|| async {
                Json(json!({
                    "name": "LumaDB Multi-Protocol Server",
                    "version": env!("CARGO_PKG_VERSION"),
                    "paths": {
                        "dynamodb": "/dynamodb/*",
                        "d1": "/d1/*",
                        "turso": "/turso/*"
                    }
                }))
            }));

        let mut router = health_router;

        // Nest protocol routers
        if config.enable_dynamodb {
            let dynamodb = DynamoDBServer::new(storage.clone(), DynamoDBConfig::default());
            router = router.nest("/dynamodb", dynamodb.router());
        }

        if config.enable_d1 {
            let d1 = D1Server::new(storage.clone(), D1Config::default());
            router = router.nest("/d1", d1.router());
        }

        if config.enable_turso {
            let turso = TursoServer::new(storage.clone(), TursoConfig::default());
            router = router.nest("/turso", turso.router());
        }

        // Add CORS
        if self.config.enable_cors {
            let cors = CorsLayer::new()
                .allow_methods([Method::GET, Method::POST, Method::PUT, Method::DELETE, Method::OPTIONS])
                .allow_headers(Any)
                .allow_origin(Any);
            router = router.layer(cors);
        }

        // Add tracing
        if self.config.enable_tracing {
            router = router.layer(TraceLayer::new_for_http());
        }

        router
    }

    /// Run the server
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = format!("{}:{}", self.config.bind_address, self.config.port).parse()?;
        
        info!("LumaDB Multi-Protocol Server starting on {}", addr);
        info!("  /dynamodb/* - DynamoDB API");
        info!("  /d1/*       - Cloudflare D1 API");
        info!("  /turso/*    - Turso/LibSQL API");
        info!("  /health     - Health check");

        let router = self.router();
        let listener = tokio::net::TcpListener::bind(addr).await?;
        
        axum::serve(listener, router)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        Ok(())
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("Received Ctrl+C, shutting down..."),
        _ = terminate => info!("Received terminate signal, shutting down..."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ServerConfig::default();
        assert_eq!(config.port, 8000);
    }
}
