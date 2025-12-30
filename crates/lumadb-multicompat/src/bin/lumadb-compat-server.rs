//! LumaDB Multi-Protocol Server Binary
//!
//! Single-port server with path-based routing for all protocols.

use std::env;

use lumadb_multicompat::{MultiProtocolServer, ServerConfig};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "lumadb_multicompat=info,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load configuration from environment
    let config = ServerConfig {
        bind_address: env::var("BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0".to_string()),
        port: env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8000),
        enable_cors: env::var("ENABLE_CORS").map(|v| v != "false" && v != "0").unwrap_or(true),
        enable_tracing: env::var("ENABLE_TRACING").map(|v| v != "false" && v != "0").unwrap_or(true),
        enable_dynamodb: env::var("ENABLE_DYNAMODB").map(|v| v != "false" && v != "0").unwrap_or(true),
        enable_d1: env::var("ENABLE_D1").map(|v| v != "false" && v != "0").unwrap_or(true),
        enable_turso: env::var("ENABLE_TURSO").map(|v| v != "false" && v != "0").unwrap_or(true),
    };

    println!("╔═══════════════════════════════════════════════════════════╗");
    println!("║       LumaDB Multi-Protocol Compatibility Server          ║");
    println!("╠═══════════════════════════════════════════════════════════╣");
    println!("║  Port: {}                                               ║", config.port);
    println!("║                                                           ║");
    println!("║  Endpoints:                                               ║");
    println!("║    /dynamodb/*  - AWS DynamoDB API                        ║");
    println!("║    /d1/*        - Cloudflare D1 API                       ║");
    println!("║    /turso/*     - Turso/LibSQL API                        ║");
    println!("║    /health      - Health Check                            ║");
    println!("╚═══════════════════════════════════════════════════════════╝");

    let server = MultiProtocolServer::new(config);
    server.run().await
}
