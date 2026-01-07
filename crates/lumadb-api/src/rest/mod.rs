//! REST API implementation

use std::sync::Arc;

use actix_web::{web, App, HttpServer, HttpResponse, middleware};
use actix_cors::Cors;
use tracing::info;

use lumadb_common::config::RestApiConfig;
use lumadb_common::error::Result;
use lumadb_query::QueryEngine;
use lumadb_streaming::StreamingEngine;
use lumadb_security::SecurityManager;

/// REST API server
#[derive(Clone)]
pub struct RestServer {
    config: RestApiConfig,
    query: Arc<QueryEngine>,
    streaming: Arc<StreamingEngine>,
    security: Arc<SecurityManager>,
}

impl RestServer {
    /// Create a new REST server
    pub async fn new(
        config: &RestApiConfig,
        query: Arc<QueryEngine>,
        streaming: Arc<StreamingEngine>,
        security: Arc<SecurityManager>,
    ) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            query,
            streaming,
            security,
        })
    }

    /// Build CORS middleware based on configuration
    fn build_cors(origins: &[String]) -> Cors {
        // If "*" is in the list or list is empty, use permissive mode (development only)
        if origins.is_empty() || origins.iter().any(|o| o == "*") {
            tracing::warn!("CORS is configured with wildcard origin - not recommended for production");
            return Cors::permissive();
        }

        // Configure CORS with specific origins for production security
        let mut cors = Cors::default()
            .allowed_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"])
            .allowed_headers(vec![
                actix_web::http::header::AUTHORIZATION,
                actix_web::http::header::ACCEPT,
                actix_web::http::header::CONTENT_TYPE,
            ])
            .max_age(3600);

        for origin in origins {
            cors = cors.allowed_origin(origin);
        }

        cors
    }

    /// Run the REST server
    pub async fn run(&self) -> Result<()> {
        let query = self.query.clone();
        let streaming = self.streaming.clone();
        let security = self.security.clone();
        let cors_origins = self.config.cors_origins.clone();

        info!("Starting REST API server on {}:{}", self.config.host, self.config.port);

        HttpServer::new(move || {
            let cors = Self::build_cors(&cors_origins);

            App::new()
                .app_data(web::Data::new(query.clone()))
                .app_data(web::Data::new(streaming.clone()))
                .app_data(web::Data::new(security.clone()))
                .wrap(cors)
                .wrap(middleware::Compress::default())
                .wrap(middleware::Logger::default())
                .route("/health", web::get().to(health_check))
                .route("/health/live", web::get().to(liveness))
                .route("/health/ready", web::get().to(readiness))
                .route("/metrics", web::get().to(metrics))
                .service(
                    web::scope("/api/v1")
                        .route("/query", web::post().to(execute_query))
                        .route("/collections", web::get().to(list_collections))
                        .route("/collections", web::post().to(create_collection))
                        .route("/topics", web::get().to(list_topics))
                        .route("/topics", web::post().to(create_topic))
                        .route("/topics/{name}/produce", web::post().to(produce))
                        .route("/topics/{name}/consume", web::get().to(consume))
                )
        })
        .workers(self.config.workers)
        .bind(format!("{}:{}", self.config.host, self.config.port))?
        .run()
        .await?;

        Ok(())
    }

    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down REST API server");
        Ok(())
    }
}

// ============================================================================
// Handlers
// ============================================================================

async fn health_check() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn liveness() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({"status": "alive"}))
}

async fn readiness(query: web::Data<Arc<QueryEngine>>) -> HttpResponse {
    if query.is_ready().await {
        HttpResponse::Ok().json(serde_json::json!({"status": "ready"}))
    } else {
        HttpResponse::ServiceUnavailable().json(serde_json::json!({"status": "not ready"}))
    }
}

async fn metrics() -> HttpResponse {
    let metrics = lumadb_common::metrics::export_prometheus();
    HttpResponse::Ok()
        .content_type("text/plain; charset=utf-8")
        .body(metrics)
}

#[derive(serde::Deserialize)]
struct QueryRequest {
    query: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

async fn execute_query(
    query_engine: web::Data<Arc<QueryEngine>>,
    request: web::Json<QueryRequest>,
) -> HttpResponse {
    match query_engine.execute(&request.query, &request.params).await {
        Ok(result) => HttpResponse::Ok().json(result),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

async fn list_collections(query_engine: web::Data<Arc<QueryEngine>>) -> HttpResponse {
    match query_engine.list_collections().await {
        Ok(collections) => HttpResponse::Ok().json(collections),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

#[derive(serde::Deserialize)]
struct CreateCollectionRequest {
    name: String,
}

async fn create_collection(
    query_engine: web::Data<Arc<QueryEngine>>,
    request: web::Json<CreateCollectionRequest>,
) -> HttpResponse {
    match query_engine.create_collection(&request.name, None, None).await {
        Ok(collection) => HttpResponse::Created().json(collection),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

async fn list_topics(streaming: web::Data<Arc<StreamingEngine>>) -> HttpResponse {
    match streaming.list_topics().await {
        Ok(topics) => HttpResponse::Ok().json(topics),
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

#[derive(serde::Deserialize)]
struct CreateTopicRequest {
    name: String,
    #[serde(default = "default_partitions")]
    partitions: u32,
    #[serde(default = "default_replication")]
    replication_factor: u32,
}

fn default_partitions() -> u32 { 3 }
fn default_replication() -> u32 { 1 }

async fn create_topic(
    streaming: web::Data<Arc<StreamingEngine>>,
    request: web::Json<CreateTopicRequest>,
) -> HttpResponse {
    let config = lumadb_common::types::TopicConfig::new(
        &request.name,
        request.partitions,
        request.replication_factor,
    );

    match streaming.create_topic(config).await {
        Ok(()) => HttpResponse::Created().json(serde_json::json!({
            "name": request.name,
            "partitions": request.partitions,
        })),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

#[derive(serde::Deserialize)]
struct ProduceRequest {
    records: Vec<lumadb_streaming::ProduceRecord>,
    #[serde(default)]
    acks: i8,
}

async fn produce(
    streaming: web::Data<Arc<StreamingEngine>>,
    path: web::Path<String>,
    request: web::Json<ProduceRequest>,
) -> HttpResponse {
    let topic = path.into_inner();

    match streaming.produce(&topic, request.records.as_slice(), request.acks).await {
        Ok(result) => HttpResponse::Ok().json(result),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}

#[derive(serde::Deserialize)]
struct ConsumeParams {
    group_id: Option<String>,
    offset: Option<String>,
    max_records: Option<usize>,
}

async fn consume(
    streaming: web::Data<Arc<StreamingEngine>>,
    path: web::Path<String>,
    params: web::Query<ConsumeParams>,
) -> HttpResponse {
    let topic = path.into_inner();

    match streaming.consume(
        &topic,
        params.group_id.as_deref(),
        params.offset.as_deref(),
        params.max_records.unwrap_or(100),
        5000,
    ).await {
        Ok(records) => HttpResponse::Ok().json(records),
        Err(e) => HttpResponse::BadRequest().json(serde_json::json!({
            "error": e.to_string(),
        })),
    }
}
