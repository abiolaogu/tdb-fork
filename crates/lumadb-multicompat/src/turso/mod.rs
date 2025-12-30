//! Turso (LibSQL) HTTP API Server
//!
//! Full LibSQL HTTP API compatible server using Axum.
//! Supports v1 (execute, batch) and v2 (pipeline) APIs.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use tracing::{debug, info, instrument};

use crate::core::{AdapterError, Column, Row, StorageEngine, UnifiedResult, Value};

/// Turso server configuration
#[derive(Debug, Clone)]
pub struct TursoConfig {
    pub org_name: String,
    pub port: u16,
    pub enable_auth: bool,
}

impl Default for TursoConfig {
    fn default() -> Self {
        Self {
            org_name: "default".to_string(),
            port: 8080,
            enable_auth: false,
        }
    }
}

/// Turso HTTP API server
pub struct TursoServer {
    storage: Arc<dyn StorageEngine>,
    config: TursoConfig,
}

impl TursoServer {
    /// Create a new Turso server
    pub fn new(storage: Arc<dyn StorageEngine>, config: TursoConfig) -> Self {
        Self { storage, config }
    }

    /// Create Axum router with Turso API routes
    pub fn router(self) -> Router {
        let state = Arc::new(self);
        Router::new()
            // V1 API
            .route("/v1/execute", post(handle_execute))
            .route("/v1/batch", post(handle_batch))
            // V2 API (pipeline/transactions)
            .route("/v2/pipeline", post(handle_pipeline))
            // Alternative paths
            .route("/", post(handle_execute))
            .route("/execute", post(handle_execute))
            .route("/batch", post(handle_batch))
            .route("/pipeline", post(handle_pipeline))
            .with_state(state)
    }
}

// ===== Request Types =====

#[derive(Debug, Deserialize)]
struct ExecuteRequest {
    #[serde(rename = "stmt")]
    statement: Statement,
}

#[derive(Debug, Deserialize)]
struct BatchRequest {
    #[serde(rename = "stmts")]
    statements: Vec<Statement>,
}

#[derive(Debug, Deserialize)]
struct PipelineRequest {
    requests: Vec<PipelineStep>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum PipelineStep {
    Execute { stmt: Statement },
    Close,
}

#[derive(Debug, Deserialize, Clone)]
struct Statement {
    sql: String,
    #[serde(default)]
    args: Option<Vec<TursoValue>>,
    #[serde(default)]
    named_args: Option<Vec<NamedArg>>,
}

#[derive(Debug, Deserialize, Clone)]
struct NamedArg {
    name: String,
    value: TursoValue,
}

// ===== Value Types =====

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "lowercase")]
pub enum TursoValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(String), // base64 encoded
}

// Alternative untagged format for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum TursoValueCompat {
    Typed(TursoValue),
    RawNull,
    RawInt(i64),
    RawFloat(f64),
    RawText(String),
}

// ===== Response Types =====

#[derive(Debug, Serialize)]
struct ExecuteResponse {
    results: Vec<StmtResult>,
}

#[derive(Debug, Serialize)]
struct StmtResult {
    cols: Vec<Column_>,
    rows: Vec<Vec<ResponseValue>>,
    affected_row_count: u64,
    last_insert_rowid: Option<String>,
    replication_index: Option<u64>,
}

#[derive(Debug, Serialize)]
struct Column_ {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    decltype: Option<String>,
}

#[derive(Debug, Serialize)]
struct ResponseValue {
    #[serde(rename = "type")]
    type_: String,
    value: JsonValue,
}

#[derive(Debug, Serialize)]
struct PipelineResponse {
    results: Vec<PipelineStepResult>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum PipelineStepResult {
    Ok { response: StepResponse },
    Error { error: StepError },
}

#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum StepResponse {
    Execute { result: StmtResult },
    Close,
}

#[derive(Debug, Serialize)]
struct StepError {
    message: String,
    code: String,
}

// ===== Handlers =====

/// Handle single statement execution (v1)
#[instrument(skip(state))]
async fn handle_execute(
    State(state): State<Arc<TursoServer>>,
    headers: HeaderMap,
    Json(request): Json<ExecuteRequest>,
) -> Result<Response, TursoError> {
    if state.config.enable_auth {
        validate_auth(&headers)?;
    }

    debug!("Turso execute: {}", request.statement.sql);
    
    let result = execute_statement(&state.storage, &request.statement).await?;

    let response = ExecuteResponse {
        results: vec![result],
    };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Handle batch execution (v1)
#[instrument(skip(state))]
async fn handle_batch(
    State(state): State<Arc<TursoServer>>,
    headers: HeaderMap,
    Json(request): Json<BatchRequest>,
) -> Result<Response, TursoError> {
    if state.config.enable_auth {
        validate_auth(&headers)?;
    }

    let mut results = Vec::with_capacity(request.statements.len());

    for stmt in &request.statements {
        let result = execute_statement(&state.storage, stmt).await?;
        results.push(result);
    }

    let response = ExecuteResponse { results };

    Ok((StatusCode::OK, Json(response)).into_response())
}

/// Handle pipeline execution (v2 - transactions)
#[instrument(skip(state))]
async fn handle_pipeline(
    State(state): State<Arc<TursoServer>>,
    headers: HeaderMap,
    Json(request): Json<PipelineRequest>,
) -> Result<Response, TursoError> {
    if state.config.enable_auth {
        validate_auth(&headers)?;
    }

    let mut results = Vec::with_capacity(request.requests.len());

    for step in &request.requests {
        let step_result = match step {
            PipelineStep::Execute { stmt } => {
                match execute_statement(&state.storage, stmt).await {
                    Ok(result) => PipelineStepResult::Ok {
                        response: StepResponse::Execute { result },
                    },
                    Err(e) => PipelineStepResult::Error {
                        error: StepError {
                            message: e.to_string(),
                            code: "SQLITE_ERROR".to_string(),
                        },
                    },
                }
            }
            PipelineStep::Close => PipelineStepResult::Ok {
                response: StepResponse::Close,
            },
        };
        results.push(step_result);
    }

    let response = PipelineResponse { results };

    Ok((StatusCode::OK, Json(response)).into_response())
}

// ===== Core Execution =====

async fn execute_statement(
    storage: &Arc<dyn StorageEngine>,
    stmt: &Statement,
) -> Result<StmtResult, TursoError> {
    let start = Instant::now();

    // Convert args to internal format
    let params = convert_args(stmt)?;

    // Execute query
    let result = storage.execute_sql(&stmt.sql, params).await
        .map_err(TursoError::from)?;

    // Build column metadata
    let cols: Vec<Column_> = if let Some(first_row) = result.rows.first() {
        first_row.columns.iter()
            .map(|c| Column_ {
                name: c.name.clone(),
                decltype: Some(infer_type(&c.value)),
            })
            .collect()
    } else {
        vec![]
    };

    // Build row data
    let rows: Vec<Vec<ResponseValue>> = result.rows.iter()
        .map(|row| {
            row.columns.iter()
                .map(|col| value_to_response(&col.value))
                .collect()
        })
        .collect();

    Ok(StmtResult {
        cols,
        rows,
        affected_row_count: result.affected_rows,
        last_insert_rowid: result.last_insert_id.map(|id| id.to_string()),
        replication_index: None,
    })
}

// ===== Conversion Helpers =====

fn convert_args(stmt: &Statement) -> Result<Vec<Value>, TursoError> {
    let mut params = Vec::new();

    // Handle positional args
    if let Some(args) = &stmt.args {
        for arg in args {
            params.push(turso_value_to_internal(arg)?);
        }
    }

    // Handle named args (substitute into SQL or append)
    if let Some(named_args) = &stmt.named_args {
        for na in named_args {
            params.push(turso_value_to_internal(&na.value)?);
        }
    }

    Ok(params)
}

fn turso_value_to_internal(tv: &TursoValue) -> Result<Value, TursoError> {
    match tv {
        TursoValue::Null => Ok(Value::Null),
        TursoValue::Integer(i) => Ok(Value::Integer(*i)),
        TursoValue::Float(f) => Ok(Value::Float(*f)),
        TursoValue::Text(s) => Ok(Value::String(s.clone())),
        TursoValue::Blob(b64) => {
            let bytes = base64::decode(b64)
                .map_err(|e| TursoError::InvalidRequest(format!("Invalid base64: {}", e)))?;
            Ok(Value::Bytes(bytes))
        }
    }
}

fn value_to_response(value: &Value) -> ResponseValue {
    match value {
        Value::Null => ResponseValue {
            type_: "null".to_string(),
            value: JsonValue::Null,
        },
        Value::Bool(b) => ResponseValue {
            type_: "integer".to_string(),
            value: json!(if *b { 1 } else { 0 }),
        },
        Value::Integer(i) => ResponseValue {
            type_: "integer".to_string(),
            value: json!(i.to_string()),
        },
        Value::Float(f) => ResponseValue {
            type_: "float".to_string(),
            value: json!(f),
        },
        Value::String(s) => ResponseValue {
            type_: "text".to_string(),
            value: json!(s),
        },
        Value::Bytes(b) => ResponseValue {
            type_: "blob".to_string(),
            value: json!(base64::encode(b)),
        },
        Value::Array(arr) => ResponseValue {
            type_: "text".to_string(),
            value: json!(format!("{:?}", arr)),
        },
        Value::Object(obj) => ResponseValue {
            type_: "text".to_string(),
            value: json!(serde_json::to_string(obj).unwrap_or_default()),
        },
        _ => ResponseValue {
            type_: "text".to_string(),
            value: json!(format!("{:?}", value)),
        },
    }
}

fn infer_type(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(_) => "INTEGER".to_string(),
        Value::Integer(_) => "INTEGER".to_string(),
        Value::Float(_) => "REAL".to_string(),
        Value::String(_) => "TEXT".to_string(),
        Value::Bytes(_) => "BLOB".to_string(),
        _ => "TEXT".to_string(),
    }
}

fn validate_auth(headers: &HeaderMap) -> Result<(), TursoError> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| TursoError::Unauthorized("Missing Authorization header".into()))?;

    if !auth.starts_with("Bearer ") {
        return Err(TursoError::Unauthorized("Invalid Authorization format".into()));
    }

    Ok(())
}

// ===== Error Types =====

#[derive(Debug, thiserror::Error)]
pub enum TursoError {
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Internal error: {0}")]
    InternalError(String),
}

impl From<AdapterError> for TursoError {
    fn from(e: AdapterError) -> Self {
        match e {
            AdapterError::AuthenticationError(msg) => TursoError::Unauthorized(msg),
            AdapterError::QueryError(msg) => TursoError::QueryError(msg),
            AdapterError::InvalidRequest(msg) => TursoError::InvalidRequest(msg),
            _ => TursoError::InternalError(e.to_string()),
        }
    }
}

impl IntoResponse for TursoError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            TursoError::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "SQLITE_ERROR"),
            TursoError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "AUTH_ERROR"),
            TursoError::QueryError(_) => (StatusCode::BAD_REQUEST, "SQLITE_ERROR"),
            TursoError::InternalError(_) => (StatusCode::INTERNAL_SERVER_ERROR, "INTERNAL_ERROR"),
        };

        let body = json!({
            "error": {
                "message": self.to_string(),
                "code": code
            }
        });

        (status, Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_turso_value_conversion() {
        let tv = TursoValue::Integer(42);
        let v = turso_value_to_internal(&tv).unwrap();
        assert_eq!(v, Value::Integer(42));

        let tv = TursoValue::Text("hello".into());
        let v = turso_value_to_internal(&tv).unwrap();
        assert_eq!(v, Value::String("hello".into()));
    }

    #[test]
    fn test_value_to_response() {
        let r = value_to_response(&Value::Integer(42));
        assert_eq!(r.type_, "integer");

        let r = value_to_response(&Value::String("test".into()));
        assert_eq!(r.type_, "text");

        let r = value_to_response(&Value::Null);
        assert_eq!(r.type_, "null");
    }

    #[test]
    fn test_blob_conversion() {
        let b64 = base64::encode(b"hello");
        let tv = TursoValue::Blob(b64);
        let v = turso_value_to_internal(&tv).unwrap();
        assert_eq!(v, Value::Bytes(b"hello".to_vec()));
    }
}
