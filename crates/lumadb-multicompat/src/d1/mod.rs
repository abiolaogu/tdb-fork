//! Cloudflare D1 HTTP API Server
//!
//! Full D1 API compatible server using Axum.
//! Implements Cloudflare's API wrapper format.

use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};
use tracing::{debug, info, instrument};

use crate::core::{AdapterError, Column, Row, StorageEngine, UnifiedResult, Value};

/// D1 server configuration
#[derive(Debug, Clone)]
pub struct D1Config {
    pub account_id: String,
    pub port: u16,
    pub enable_auth: bool,
}

impl Default for D1Config {
    fn default() -> Self {
        Self {
            account_id: "default".to_string(),
            port: 8787,
            enable_auth: false,
        }
    }
}

/// D1 HTTP API server
pub struct D1Server {
    storage: Arc<dyn StorageEngine>,
    config: D1Config,
}

impl D1Server {
    /// Create a new D1 server
    pub fn new(storage: Arc<dyn StorageEngine>, config: D1Config) -> Self {
        Self { storage, config }
    }

    /// Create Axum router with D1 API routes
    pub fn router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            // Main query endpoint
            .route(
                "/client/v4/accounts/:account_id/d1/database/:database_id/query",
                post(handle_query),
            )
            // Raw endpoint (alternative query path)
            .route(
                "/client/v4/accounts/:account_id/d1/database/:database_id/raw",
                post(handle_raw_query),
            )
            // Database info endpoint
            .route(
                "/client/v4/accounts/:account_id/d1/database/:database_id",
                get(handle_info),
            )
            // List databases
            .route(
                "/client/v4/accounts/:account_id/d1/database",
                get(handle_list_databases),
            )
            // Simple query endpoint (Workers binding style)
            .route("/query", post(handle_simple_query))
            .route("/batch", post(handle_batch))
            .with_state(state)
    }
}

// ===== Request Types =====

#[derive(Debug, Deserialize)]
struct D1QueryRequest {
    sql: String,
    #[serde(default)]
    params: Option<Vec<JsonValue>>,
}

#[derive(Debug, Deserialize)]
struct D1BatchRequest {
    statements: Vec<D1Statement>,
}

#[derive(Debug, Deserialize)]
struct D1Statement {
    sql: String,
    #[serde(default)]
    params: Option<Vec<JsonValue>>,
}

// ===== Response Types =====

/// Cloudflare API wrapper response
#[derive(Debug, Serialize)]
struct CloudflareResponse<T> {
    success: bool,
    result: T,
    errors: Vec<CloudflareError>,
    messages: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CloudflareError {
    code: i32,
    message: String,
}

#[derive(Debug, Serialize)]
struct D1QueryResult {
    results: Vec<JsonValue>,
    success: bool,
    meta: D1Meta,
}

#[derive(Debug, Serialize)]
struct D1Meta {
    duration: f64,
    rows_read: u64,
    rows_written: u64,
    last_row_id: Option<i64>,
    changed_db: bool,
    changes: u64,
    size_after: Option<u64>,
}

#[derive(Debug, Serialize)]
struct D1DatabaseInfo {
    uuid: String,
    name: String,
    version: String,
    num_tables: u64,
    file_size: u64,
    created_at: String,
}

// ===== Handlers =====

/// Handle query with Cloudflare API wrapper
#[instrument(skip(state))]
async fn handle_query(
    State(state): State<Arc<D1Server>>,
    Path((_account_id, database_id)): Path<(String, String)>,
    headers: HeaderMap,
    Json(request): Json<D1QueryRequest>,
) -> Result<Response, D1Error> {
    // Validate auth if enabled
    if state.config.enable_auth {
        validate_auth(&headers)?;
    }

    debug!("D1 query on {}: {}", database_id, request.sql);
    
    let result = execute_query(&state.storage, &request.sql, request.params).await?;

    let response = CloudflareResponse {
        success: true,
        result: vec![result],
        errors: vec![],
        messages: vec![],
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Handle raw query endpoint
async fn handle_raw_query(
    State(state): State<Arc<D1Server>>,
    Path((_account_id, database_id)): Path<(String, String)>,
    Json(request): Json<D1QueryRequest>,
) -> Result<Response, D1Error> {
    let result = execute_query(&state.storage, &request.sql, request.params).await?;
    Ok((StatusCode::OK, Json(result)).into_response())
}

/// Handle simple query endpoint (Workers binding style)
async fn handle_simple_query(
    State(state): State<Arc<D1Server>>,
    Json(request): Json<D1QueryRequest>,
) -> Result<Response, D1Error> {
    let result = execute_query(&state.storage, &request.sql, request.params).await?;
    Ok((StatusCode::OK, Json(result)).into_response())
}

/// Handle batch query execution
#[instrument(skip(state))]
async fn handle_batch(
    State(state): State<Arc<D1Server>>,
    Json(request): Json<D1BatchRequest>,
) -> Result<Response, D1Error> {
    let mut results = Vec::with_capacity(request.statements.len());

    for stmt in request.statements {
        match execute_query(&state.storage, &stmt.sql, stmt.params).await {
            Ok(result) => results.push(result),
            Err(e) => {
                // Return error in Cloudflare format
                let response = CloudflareResponse::<Vec<D1QueryResult>> {
                    success: false,
                    result: results,
                    errors: vec![CloudflareError {
                        code: 1000,
                        message: e.to_string(),
                    }],
                    messages: vec![],
                };
                return Ok((StatusCode::OK, Json(response)).into_response());
            }
        }
    }

    let response = CloudflareResponse {
        success: true,
        result: results,
        errors: vec![],
        messages: vec![],
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Handle database info endpoint
async fn handle_info(
    State(state): State<Arc<D1Server>>,
    Path((_account_id, database_id)): Path<(String, String)>,
) -> Result<Response, D1Error> {
    let tables = state.storage.list_tables().await
        .map_err(D1Error::from)?;

    let info = D1DatabaseInfo {
        uuid: database_id.clone(),
        name: database_id,
        version: "v1".to_string(),
        num_tables: tables.len() as u64,
        file_size: 0,
        created_at: chrono::Utc::now().to_rfc3339(),
    };

    let response = CloudflareResponse {
        success: true,
        result: info,
        errors: vec![],
        messages: vec![],
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Handle list databases endpoint
async fn handle_list_databases(
    State(_state): State<Arc<D1Server>>,
    Path(_account_id): Path<String>,
) -> Result<Response, D1Error> {
    // Return a single default database
    let databases = vec![D1DatabaseInfo {
        uuid: "default".to_string(),
        name: "default".to_string(),
        version: "v1".to_string(),
        num_tables: 0,
        file_size: 0,
        created_at: chrono::Utc::now().to_rfc3339(),
    }];

    let response = CloudflareResponse {
        success: true,
        result: databases,
        errors: vec![],
        messages: vec![],
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

// ===== Core Query Execution =====

async fn execute_query(
    storage: &Arc<dyn StorageEngine>,
    sql: &str,
    params: Option<Vec<JsonValue>>,
) -> Result<D1QueryResult, D1Error> {
    let start = Instant::now();

    // Convert params to internal format
    let internal_params: Vec<Value> = params
        .unwrap_or_default()
        .into_iter()
        .map(json_to_value)
        .collect();

    // Execute the query
    let result = storage.execute_sql(sql, internal_params).await
        .map_err(D1Error::from)?;

    let duration = start.elapsed().as_secs_f64();

    // Convert rows to JSON objects
    let results: Vec<JsonValue> = result.rows.iter()
        .map(row_to_json_object)
        .collect();

    // Determine if this was a write operation
    let is_write = sql.trim().to_uppercase().starts_with("INSERT")
        || sql.trim().to_uppercase().starts_with("UPDATE")
        || sql.trim().to_uppercase().starts_with("DELETE")
        || sql.trim().to_uppercase().starts_with("CREATE")
        || sql.trim().to_uppercase().starts_with("DROP")
        || sql.trim().to_uppercase().starts_with("ALTER");

    Ok(D1QueryResult {
        results,
        success: true,
        meta: D1Meta {
            duration,
            rows_read: result.metadata.rows_read,
            rows_written: result.metadata.rows_written,
            last_row_id: result.last_insert_id,
            changed_db: is_write,
            changes: result.affected_rows,
            size_after: None,
        },
    })
}

// ===== Conversion Helpers =====

fn row_to_json_object(row: &Row) -> JsonValue {
    let mut obj = Map::new();
    for col in &row.columns {
        obj.insert(col.name.clone(), value_to_json(&col.value));
    }
    JsonValue::Object(obj)
}

fn value_to_json(value: &Value) -> JsonValue {
    match value {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => JsonValue::Bool(*b),
        Value::Integer(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::String(s) => JsonValue::String(s.clone()),
        Value::Bytes(b) => JsonValue::String(base64::encode(b)),
        Value::Array(arr) => {
            JsonValue::Array(arr.iter().map(value_to_json).collect())
        }
        Value::Object(obj) => {
            let map: Map<String, JsonValue> = obj
                .iter()
                .map(|(k, v)| (k.clone(), value_to_json(v)))
                .collect();
            JsonValue::Object(map)
        }
        Value::StringSet(ss) => json!(ss),
        Value::NumberSet(ns) => json!(ns),
        Value::BinarySet(bs) => {
            json!(bs.iter().map(|b| base64::encode(b)).collect::<Vec<_>>())
        }
    }
}

fn json_to_value(json: JsonValue) -> Value {
    match json {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(b) => Value::Bool(b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else {
                Value::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        JsonValue::String(s) => Value::String(s),
        JsonValue::Array(arr) => {
            Value::Array(arr.into_iter().map(json_to_value).collect())
        }
        JsonValue::Object(obj) => {
            let map = obj
                .into_iter()
                .map(|(k, v)| (k, json_to_value(v)))
                .collect();
            Value::Object(map)
        }
    }
}

fn validate_auth(headers: &HeaderMap) -> Result<(), D1Error> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| D1Error::Unauthorized("Missing Authorization header".into()))?;

    if !auth.starts_with("Bearer ") {
        return Err(D1Error::Unauthorized("Invalid Authorization format".into()));
    }

    // In production, validate the token
    // For now, accept any Bearer token
    Ok(())
}

// ===== Error Types =====

#[derive(Debug, thiserror::Error)]
pub enum D1Error {
    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<AdapterError> for D1Error {
    fn from(e: AdapterError) -> Self {
        match e {
            AdapterError::NotFound(msg) => D1Error::NotFound(msg),
            AdapterError::AuthenticationError(msg) => D1Error::Unauthorized(msg),
            AdapterError::QueryError(msg) => D1Error::QueryError(msg),
            _ => D1Error::InternalError(e.to_string()),
        }
    }
}

impl IntoResponse for D1Error {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            D1Error::QueryError(_) => (StatusCode::BAD_REQUEST, 1001),
            D1Error::Unauthorized(_) => (StatusCode::UNAUTHORIZED, 10000),
            D1Error::NotFound(_) => (StatusCode::NOT_FOUND, 10001),
            D1Error::InternalError(_) => (StatusCode::INTERNAL_SERVER_ERROR, 1002),
        };

        let response = CloudflareResponse::<()> {
            success: false,
            result: (),
            errors: vec![CloudflareError {
                code,
                message: self.to_string(),
            }],
            messages: vec![],
        };

        (status, Json(response)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_to_value_primitives() {
        assert_eq!(json_to_value(JsonValue::Null), Value::Null);
        assert_eq!(json_to_value(json!(true)), Value::Bool(true));
        assert_eq!(json_to_value(json!(42)), Value::Integer(42));
        assert_eq!(json_to_value(json!("hello")), Value::String("hello".into()));
    }

    #[test]
    fn test_value_to_json_primitives() {
        assert_eq!(value_to_json(&Value::Null), JsonValue::Null);
        assert_eq!(value_to_json(&Value::Bool(true)), json!(true));
        assert_eq!(value_to_json(&Value::Integer(42)), json!(42));
        assert_eq!(value_to_json(&Value::String("hi".into())), json!("hi"));
    }

    #[test]
    fn test_row_to_json() {
        let mut row = Row::new();
        row.push("id", Value::Integer(1));
        row.push("name", Value::String("test".into()));

        let json = row_to_json_object(&row);
        assert_eq!(json.get("id"), Some(&json!(1)));
        assert_eq!(json.get("name"), Some(&json!("test")));
    }
}
