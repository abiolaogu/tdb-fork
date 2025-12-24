//! Pinecone-compatible REST API server

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer, middleware, HttpResponse};
use tracing::info;

use lumadb_storage::StorageEngine;
use lumadb_common::error::Result;

use super::handlers::{self, PineconeState};

/// Pinecone-compatible REST API server
pub struct PineconeServer {
    storage: Arc<StorageEngine>,
    host: String,
    port: u16,
    index_name: String,
    dimension: usize,
}

impl PineconeServer {
    /// Create a new Pinecone-compatible server
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            host: "0.0.0.0".to_string(),
            port: 8081,
            index_name: "default".to_string(),
            dimension: 1536, // OpenAI embedding dimension
        }
    }

    /// Set the bind address
    pub fn bind(mut self, addr: &str) -> Self {
        if let Some((host, port)) = addr.split_once(':') {
            self.host = host.to_string();
            self.port = port.parse().unwrap_or(8081);
        }
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the index name
    pub fn index_name(mut self, name: &str) -> Self {
        self.index_name = name.to_string();
        self
    }

    /// Set the vector dimension
    pub fn dimension(mut self, dim: usize) -> Self {
        self.dimension = dim;
        self
    }

    /// Run the Pinecone-compatible server
    pub async fn run(self) -> Result<()> {
        let state = web::Data::new(PineconeState {
            storage: self.storage.clone(),
            index_name: self.index_name.clone(),
            dimension: self.dimension,
        });

        info!("Starting Pinecone-compatible API on {}:{}", self.host, self.port);
        info!("Index: {} (dimension: {})", self.index_name, self.dimension);
        info!("Pinecone clients can connect to http://{}:{}", self.host, self.port);

        HttpServer::new(move || {
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .max_age(3600);

            App::new()
                .app_data(state.clone())
                .wrap(cors)
                .wrap(middleware::Logger::default())
                .wrap(middleware::Compress::default())
                // Health endpoints
                .route("/", web::get().to(root))
                .route("/health", web::get().to(health))
                // Vector operations
                .route("/vectors/upsert", web::post().to(handlers::upsert))
                .route("/query", web::post().to(handlers::query))
                .route("/vectors/fetch", web::get().to(handlers::fetch))
                .route("/vectors/delete", web::post().to(handlers::delete))
                .route("/vectors/update", web::post().to(handlers::update))
                .route("/vectors/list", web::get().to(handlers::list))
                // Index operations
                .route("/describe_index_stats", web::get().to(handlers::describe_index_stats))
                .route("/describe_index_stats", web::post().to(handlers::describe_index_stats))
        })
        .bind(format!("{}:{}", self.host, self.port))?
        .run()
        .await
        .map_err(|e| lumadb_common::error::Error::Internal(e.to_string()))
    }
}

/// Root endpoint
async fn root() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "message": "LumaDB Pinecone Compatible API",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// Health check
async fn health() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": 1
    }))
}
