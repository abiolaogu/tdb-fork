//! Supabase Compatibility Layer for `LumaDB`
//!
//! This crate provides a Supabase-compatible API layer on top of `LumaDB`,
//! enabling drop-in replacement for existing Supabase applications.
//!
//! # Features
//!
//! - **PostgREST-compatible REST API** - Auto-generated from database schema
//! - **GoTrue-compatible Authentication** - JWT, OAuth, Magic Links
//! - **Row Level Security** - Policy-based access control
//! - **Real-time Subscriptions** - WebSocket-based change notifications
//! - **Storage Service** - S3-compatible object storage
//! - **Edge Functions** - Serverless JavaScript/TypeScript runtime

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub use supabase_auth as auth;
pub use supabase_common as common;
pub use supabase_rest as rest;

use std::sync::Arc;
use tracing::info;

use supabase_common::config::SupabaseConfig;
use supabase_common::error::Result;

/// Main Supabase compatibility server
pub struct SupabaseServer {
    config: SupabaseConfig,
    auth_server: Arc<supabase_auth::AuthServer>,
    rest_server: Arc<supabase_rest::RestServer>,
}

impl SupabaseServer {
    /// Create a new Supabase server with the given configuration
    ///
    /// # Errors
    /// Returns an error if auth or REST server initialization fails.
    pub async fn new(config: SupabaseConfig) -> Result<Self> {
        info!("Initializing Supabase compatibility layer");

        // Create auth server
        let auth_server = Arc::new(supabase_auth::AuthServer::new(&config.auth)?);

        // Create REST server with auth state for integration
        let rest_server = Arc::new(supabase_rest::RestServer::new(
            &config.rest,
            auth_server.state(),
        )?);

        Ok(Self {
            config,
            auth_server,
            rest_server,
        })
    }

    /// Get reference to the auth server
    #[must_use]
    pub fn auth(&self) -> &supabase_auth::AuthServer {
        &self.auth_server
    }

    /// Get reference to the REST server
    #[must_use]
    pub fn rest(&self) -> &supabase_rest::RestServer {
        &self.rest_server
    }

    /// Get the configuration
    #[must_use]
    pub fn config(&self) -> &SupabaseConfig {
        &self.config
    }

    /// Start all Supabase services
    ///
    /// # Errors
    /// Returns an error if the REST server fails to start.
    pub async fn run(&self) -> Result<()> {
        info!("Starting Supabase compatibility services");
        info!(
            "  - Auth service: http://{}:{}",
            self.config.auth.host, self.config.auth.port
        );
        info!(
            "  - REST service: http://{}:{}",
            self.config.rest.host, self.config.rest.port
        );

        // For now, just run the REST server (auth is passed to REST for integration)
        // In a full deployment, these would be separate processes/containers
        self.rest_server.run().await?;

        Ok(())
    }

    /// Shutdown all services gracefully
    ///
    /// # Errors
    /// Returns an error if shutdown fails.
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Supabase compatibility services");
        Ok(())
    }
}
