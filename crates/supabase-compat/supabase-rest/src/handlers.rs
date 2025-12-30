//! HTTP handlers for REST API endpoints

use actix_web::{web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

use supabase_common::error::Error;
use supabase_common::types::ApiError;

use crate::query::ParsedQuery;
use crate::schema::SchemaCache;

/// Shared REST API state
pub struct RestState {
    pub schema_cache: Arc<SchemaCache>,
    pub max_rows: usize,
}

// ============================================================================
// Mock Data Store (for development)
// ============================================================================

use parking_lot::RwLock;
use uuid::Uuid;

/// Simple in-memory data store for development
pub struct DataStore {
    tables: Arc<RwLock<HashMap<String, Vec<serde_json::Value>>>>,
}

impl DataStore {
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn with_sample_data() -> Self {
        let store = Self::new();

        // Add sample users
        let users = vec![
            serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "email": "alice@example.com",
                "name": "Alice",
                "created_at": chrono::Utc::now().to_rfc3339(),
            }),
            serde_json::json!({
                "id": Uuid::new_v4().to_string(),
                "email": "bob@example.com",
                "name": "Bob",
                "created_at": chrono::Utc::now().to_rfc3339(),
            }),
        ];

        store.tables.write().insert("users".to_string(), users);
        store
    }

    pub fn select(&self, table: &str, query: &ParsedQuery) -> Vec<serde_json::Value> {
        let tables = self.tables.read();
        let rows = tables.get(table).cloned().unwrap_or_default();

        // Apply filters (simplified)
        let filtered: Vec<_> = rows
            .into_iter()
            .filter(|row| {
                query.filters.iter().all(|f| {
                    if let Some(value) = row.get(&f.column) {
                        match &f.value {
                            crate::query::FilterValue::Single(v) => match &f.operator {
                                crate::query::FilterOperator::Eq => {
                                    value.as_str().map(|s| s == v).unwrap_or(false)
                                }
                                _ => true,
                            },
                            _ => true,
                        }
                    } else {
                        true
                    }
                })
            })
            .collect();

        // Apply limit/offset
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(100);

        filtered.into_iter().skip(offset).take(limit).collect()
    }

    pub fn insert(&self, table: &str, row: serde_json::Value) -> serde_json::Value {
        let mut tables = self.tables.write();
        let rows = tables.entry(table.to_string()).or_insert_with(Vec::new);

        // Add default fields if not present
        let mut row = row;
        if row.get("id").is_none() {
            row["id"] = serde_json::Value::String(Uuid::new_v4().to_string());
        }
        if row.get("created_at").is_none() {
            row["created_at"] = serde_json::Value::String(chrono::Utc::now().to_rfc3339());
        }

        rows.push(row.clone());
        row
    }

    pub fn update(
        &self,
        table: &str,
        query: &ParsedQuery,
        updates: serde_json::Value,
    ) -> Vec<serde_json::Value> {
        let mut tables = self.tables.write();
        let rows = tables.get_mut(table);

        if let Some(rows) = rows {
            let mut updated = Vec::new();

            for row in rows.iter_mut() {
                // Check if row matches filters
                let matches = query.filters.iter().all(|f| {
                    if let Some(value) = row.get(&f.column) {
                        match &f.value {
                            crate::query::FilterValue::Single(v) => {
                                value.as_str().map(|s| s == v).unwrap_or(false)
                            }
                            _ => true,
                        }
                    } else {
                        false
                    }
                });

                if matches {
                    // Merge updates
                    if let (Some(row_obj), Some(updates_obj)) =
                        (row.as_object_mut(), updates.as_object())
                    {
                        for (key, value) in updates_obj {
                            row_obj.insert(key.clone(), value.clone());
                        }
                    }
                    updated.push(row.clone());
                }
            }

            updated
        } else {
            vec![]
        }
    }

    pub fn delete(&self, table: &str, query: &ParsedQuery) -> usize {
        let mut tables = self.tables.write();
        let rows = tables.get_mut(table);

        if let Some(rows) = rows {
            let original_len = rows.len();

            rows.retain(|row| {
                !query.filters.iter().all(|f| {
                    if let Some(value) = row.get(&f.column) {
                        match &f.value {
                            crate::query::FilterValue::Single(v) => {
                                value.as_str().map(|s| s == v).unwrap_or(false)
                            }
                            _ => true,
                        }
                    } else {
                        false
                    }
                })
            });

            original_len - rows.len()
        } else {
            0
        }
    }
}

impl Default for DataStore {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// HTTP Handlers
// ============================================================================

/// GET /{table} - Read rows from table
pub async fn select_handler(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();

    // Check if table exists
    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    // Parse query string
    let query_string = req.query_string();
    let query = match ParsedQuery::parse(query_string) {
        Ok(q) => q,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiError::new("invalid_query", e.to_string()));
        }
    };

    // Execute query
    let rows = data_store.select(&table, &query);

    // Return with headers
    let count = rows.len();
    HttpResponse::Ok()
        .insert_header((
            "Content-Range",
            format!("0-{}/{}", count.saturating_sub(1), count),
        ))
        .json(rows)
}

/// POST /{table} - Insert rows into table
pub async fn insert_handler(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();

    // Check if table exists
    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    let prefer = req
        .headers()
        .get("Prefer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Handle array or single object
    let rows: Vec<serde_json::Value> = if body.is_array() {
        body.as_array().cloned().unwrap_or_default()
    } else {
        vec![body.into_inner()]
    };

    let mut inserted = Vec::new();
    for row in rows {
        let result = data_store.insert(&table, row);
        inserted.push(result);
    }

    // Return based on Prefer header
    if prefer.contains("return=representation") {
        if inserted.len() == 1 {
            HttpResponse::Created().json(&inserted[0])
        } else {
            HttpResponse::Created().json(inserted)
        }
    } else {
        HttpResponse::Created().finish()
    }
}

/// PATCH /{table} - Update rows in table
pub async fn update_handler(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();

    // Check if table exists
    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    // Parse query string for filters
    let query_string = req.query_string();
    let query = match ParsedQuery::parse(query_string) {
        Ok(q) => q,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiError::new("invalid_query", e.to_string()));
        }
    };

    let prefer = req
        .headers()
        .get("Prefer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Execute update
    let updated = data_store.update(&table, &query, body.into_inner());

    if prefer.contains("return=representation") {
        HttpResponse::Ok().json(updated)
    } else {
        HttpResponse::NoContent().finish()
    }
}

/// DELETE /{table} - Delete rows from table
pub async fn delete_handler(
    state: web::Data<Arc<RestState>>,
    data_store: web::Data<Arc<DataStore>>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();

    // Check if table exists
    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    // Parse query string for filters
    let query_string = req.query_string();
    let query = match ParsedQuery::parse(query_string) {
        Ok(q) => q,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiError::new("invalid_query", e.to_string()));
        }
    };

    // Execute delete
    let deleted_count = data_store.delete(&table, &query);

    HttpResponse::NoContent()
        .insert_header(("X-Deleted-Count", deleted_count.to_string()))
        .finish()
}

/// POST /rpc/{function} - Call RPC function
pub async fn rpc_handler(
    state: web::Data<Arc<RestState>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
) -> HttpResponse {
    let function_name = path.into_inner();

    // Check if function exists
    if state.schema_cache.get_function(&function_name).is_none() {
        // For now, just echo the input as a placeholder
        return HttpResponse::Ok().json(serde_json::json!({
            "function": function_name,
            "params": body.into_inner(),
            "result": null,
            "message": "RPC not yet implemented"
        }));
    }

    // TODO: Execute actual RPC call
    HttpResponse::Ok().json(serde_json::json!({}))
}

/// GET / - Get OpenAPI schema
pub async fn openapi_handler(state: web::Data<Arc<RestState>>) -> HttpResponse {
    let schema = state.schema_cache.generate_openapi();
    HttpResponse::Ok().json(schema)
}

/// Health check
pub async fn health_handler() -> HttpResponse {
    HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "rest",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}
