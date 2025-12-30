//! LumaDB Supabase Compatibility Server
//!
//! Binary entry point for running Supabase-compatible services.

use clap::Parser;
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use supabase_common::config::SupabaseConfig;
use supabase_compat::SupabaseServer;

#[derive(Parser, Debug)]
#[command(name = "lumadb-supabase")]
#[command(about = "LumaDB Supabase Compatibility Layer", long_about = None)]
struct Args {
    /// Path to configuration file
    #[arg(short, long, default_value = "supabase.toml")]
    config: PathBuf,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // Initialize logging
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level)))
        .init();

    info!(
        "LumaDB Supabase Compatibility Layer v{}",
        env!("CARGO_PKG_VERSION")
    );

    // Load configuration
    let config = if args.config.exists() {
        let content = std::fs::read_to_string(&args.config)?;
        toml::from_str(&content)?
    } else {
        info!("Using default configuration");
        SupabaseConfig::default()
    };

    // Create and run server
    let server = SupabaseServer::new(config).await?;

    // Handle shutdown signals
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install CTRL+C handler");
        info!("Received shutdown signal");
    };

    tokio::select! {
        result = server.run() => {
            if let Err(e) = result {
                tracing::error!("Server error: {}", e);
            }
        }
        _ = shutdown => {
            server.shutdown().await?;
        }
    }

    Ok(())
}
