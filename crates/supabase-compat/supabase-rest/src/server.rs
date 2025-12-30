//! REST API server implementation

use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer, HttpRequest, HttpResponse, dev::ServiceRequest};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

use supabase_common::config::RestConfig;
use supabase_common::error::Result;

use crate::handlers::{self, DataStore, RestState};
use crate::ratelimit::{RateLimitConfig, RateLimiter};
use crate::schema::SchemaCache;
use supabase_auth::handlers::AuthState;

/// PostgREST-compatible REST API server
pub struct RestServer {
    config: RestConfig,
    state: Arc<RestState>,
    data_store: Arc<DataStore>,
    auth_state: Option<Arc<AuthState>>,
    rate_limiter: Arc<RateLimiter>,
}

impl RestServer {
    /// Create a new REST server
    pub fn new(config: &RestConfig, auth_state: Arc<AuthState>) -> Result<Self> {
        let schema_cache = Arc::new(SchemaCache::with_mock_schema());
        let data_store = Arc::new(DataStore::with_sample_data());
        let rate_limiter = Arc::new(RateLimiter::new(RateLimitConfig {
            max_requests: 100,
            window: Duration::from_secs(60),
            by_ip: true,
            by_user: true,
        }));

        let state = Arc::new(RestState {
            schema_cache,
            max_rows: config.max_rows,
        });

        Ok(Self {
            config: config.clone(),
            state,
            data_store,
            auth_state: Some(auth_state),
            rate_limiter,
        })
    }

    /// Create without auth (for standalone use)
    pub fn new_standalone(config: &RestConfig) -> Result<Self> {
        let schema_cache = Arc::new(SchemaCache::with_mock_schema());
        let data_store = Arc::new(DataStore::with_sample_data());
        let rate_limiter = Arc::new(RateLimiter::new(RateLimitConfig::default()));

        let state = Arc::new(RestState {
            schema_cache,
            max_rows: config.max_rows,
        });

        Ok(Self {
            config: config.clone(),
            state,
            data_store,
            auth_state: None,
            rate_limiter,
        })
    }

    /// Get the rate limiter
    #[must_use]
    pub fn rate_limiter(&self) -> Arc<RateLimiter> {
        self.rate_limiter.clone()
    }

    /// Get the auth state if present
    #[must_use]
    pub fn auth_state(&self) -> Option<Arc<AuthState>> {
        self.auth_state.clone()
    }

    /// Run the REST server
    pub async fn run(&self) -> Result<()> {
        let state = self.state.clone();
        let data_store = self.data_store.clone();
        let rate_limiter = self.rate_limiter.clone();
        let host = self.config.host.clone();
        let port = self.config.port;

        info!("Starting REST API server on {}:{}", host, port);
        info!("  Rate limiting: 100 req/min per IP");

        HttpServer::new(move || {
            let cors = Cors::permissive();
            let limiter = rate_limiter.clone();

            App::new()
                .app_data(web::Data::new(state.clone()))
                .app_data(web::Data::new(data_store.clone()))
                .app_data(web::Data::new(limiter))
                .wrap(cors)
                .wrap(middleware::Compress::default())
                .wrap(middleware::Logger::default())
                // Health check (no rate limit)
                .route("/health", web::get().to(handlers::health_handler))
                // OpenAPI schema
                .route("/", web::get().to(handlers::openapi_handler))
                // RPC endpoints
                .route("/rpc/{function}", web::post().to(handlers::rpc_handler))
                // Table CRUD endpoints with rate limiting
                .route("/{table}", web::get().to(rate_limited_select))
                .route("/{table}", web::post().to(rate_limited_insert))
                .route("/{table}", web::patch().to(rate_limited_update))
                .route("/{table}", web::delete().to(rate_limited_delete))
        })
        .workers(4)
        .bind(format!("{}:{}", host, port))?
        .run()
        .await?;

        Ok(())
    }
}

/// Rate-limited select handler
async fn rate_limited_select(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    req: HttpRequest,
    limiter: web::Data<Arc<RateLimiter>>,
) -> HttpResponse {
    // Check rate limit by IP
    if let Some(ip) = req.connection_info().peer_addr() {
        if let Ok(ip_addr) = ip.parse() {
            if !limiter.check_ip(ip_addr).is_allowed() {
                return HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", "60"))
                    .json(serde_json::json!({"error": "Rate limit exceeded"}));
            }
        }
    }
    handlers::select_handler(state, data_store, path, req).await
}

/// Rate-limited insert handler
async fn rate_limited_insert(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
    req: HttpRequest,
    limiter: web::Data<Arc<RateLimiter>>,
) -> HttpResponse {
    if let Some(ip) = req.connection_info().peer_addr() {
        if let Ok(ip_addr) = ip.parse() {
            if !limiter.check_ip(ip_addr).is_allowed() {
                return HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", "60"))
                    .json(serde_json::json!({"error": "Rate limit exceeded"}));
            }
        }
    }
    handlers::insert_handler(state, data_store, path, body, req).await
}

/// Rate-limited update handler
async fn rate_limited_update(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
    req: HttpRequest,
    limiter: web::Data<Arc<RateLimiter>>,
) -> HttpResponse {
    if let Some(ip) = req.connection_info().peer_addr() {
        if let Ok(ip_addr) = ip.parse() {
            if !limiter.check_ip(ip_addr).is_allowed() {
                return HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", "60"))
                    .json(serde_json::json!({"error": "Rate limit exceeded"}));
            }
        }
    }
    handlers::update_handler(state, data_store, path, body, req).await
}

/// Rate-limited delete handler
async fn rate_limited_delete(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    req: HttpRequest,
    limiter: web::Data<Arc<RateLimiter>>,
) -> HttpResponse {
    if let Some(ip) = req.connection_info().peer_addr() {
        if let Ok(ip_addr) = ip.parse() {
            if !limiter.check_ip(ip_addr).is_allowed() {
                return HttpResponse::TooManyRequests()
                    .insert_header(("Retry-After", "60"))
                    .json(serde_json::json!({"error": "Rate limit exceeded"}));
            }
        }
    }
    handlers::delete_handler(state, data_store, path, req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rest_server_creation() {
        let config = RestConfig::default();
        let server = RestServer::new_standalone(&config);
        assert!(server.is_ok());
    }

    #[test]
    fn test_rest_server_has_rate_limiter() {
        let config = RestConfig::default();
        let server = RestServer::new_standalone(&config).unwrap();
        let limiter = server.rate_limiter();
        assert!(limiter.check("test").is_allowed());
    }
}

