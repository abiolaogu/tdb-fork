//! Qdrant-compatible REST API server

use std::sync::Arc;

use actix_cors::Cors;
use actix_web::{web, App, HttpServer, middleware};
use tracing::info;

use lumadb_storage::StorageEngine;
use lumadb_common::error::Result;

use super::handlers::{self, QdrantState};

/// Qdrant-compatible REST API server
pub struct QdrantServer {
    storage: Arc<StorageEngine>,
    host: String,
    port: u16,
}

impl QdrantServer {
    /// Create a new Qdrant-compatible server
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            host: "0.0.0.0".to_string(),
            port: 6333,
        }
    }

    /// Set the bind address
    pub fn bind(mut self, addr: &str) -> Self {
        if let Some((host, port)) = addr.split_once(':') {
            self.host = host.to_string();
            self.port = port.parse().unwrap_or(6333);
        }
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Run the Qdrant-compatible server
    pub async fn run(self) -> Result<()> {
        let state = web::Data::new(QdrantState {
            storage: self.storage.clone(),
        });

        info!("Starting Qdrant-compatible API on {}:{}", self.host, self.port);
        info!("Qdrant clients can connect to http://{}:{}", self.host, self.port);

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
                // Root endpoints
                .route("/", web::get().to(handlers::root))
                .route("/healthz", web::get().to(handlers::healthz))
                .route("/readyz", web::get().to(handlers::readyz))
                .route("/livez", web::get().to(handlers::livez))
                .route("/telemetry", web::get().to(handlers::telemetry))
                .route("/cluster", web::get().to(handlers::cluster_info))
                // Collection endpoints
                .route("/collections", web::get().to(handlers::list_collections))
                .route("/collections/{name}", web::get().to(handlers::get_collection))
                .route("/collections/{name}", web::put().to(handlers::create_collection))
                .route("/collections/{name}", web::delete().to(handlers::delete_collection))
                // Point endpoints
                .route("/collections/{name}/points", web::put().to(handlers::upsert_points))
                .route("/collections/{name}/points", web::post().to(handlers::get_points))
                .route("/collections/{name}/points/delete", web::post().to(handlers::delete_points))
                .route("/collections/{name}/points/search", web::post().to(handlers::search_points))
                .route("/collections/{name}/points/scroll", web::post().to(handlers::scroll_points))
                .route("/collections/{name}/points/count", web::post().to(handlers::count_points))
        })
        .bind(format!("{}:{}", self.host, self.port))?
        .run()
        .await
        .map_err(|e| lumadb_common::error::Error::Internal(e.to_string()))
    }
}
