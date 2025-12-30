//! LumaDB Query Engine Integration
//!
//! Bridges the Supabase REST API with LumaDB's native query engine,
//! providing a unified interface for executing queries with RLS enforcement.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use supabase_common::error::{Error, Result};
use supabase_common::types::TableInfo;

/// Query execution result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result rows as JSON objects
    pub rows: Vec<JsonValue>,
    /// Number of rows affected (for mutations)
    pub rows_affected: u64,
    /// Column names
    pub columns: Vec<String>,
    /// Whether result was from cache
    pub cached: bool,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryResult {
    /// Create empty result
    pub fn empty() -> Self {
        Self {
            rows: vec![],
            rows_affected: 0,
            columns: vec![],
            cached: false,
            execution_time_ms: 0,
        }
    }

    /// Create from rows
    pub fn from_rows(rows: Vec<JsonValue>) -> Self {
        let columns = rows
            .first()
            .and_then(|r| r.as_object())
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();

        Self {
            rows_affected: rows.len() as u64,
            rows,
            columns,
            cached: false,
            execution_time_ms: 0,
        }
    }
}

/// Query context containing security information
#[derive(Debug, Clone)]
pub struct QueryContext {
    /// User ID from JWT
    pub user_id: Option<String>,
    /// User role
    pub role: String,
    /// Schema to query
    pub schema: String,
    /// Whether to bypass RLS (for service role)
    pub bypass_rls: bool,
}

impl Default for QueryContext {
    fn default() -> Self {
        Self {
            user_id: None,
            role: "anon".to_string(),
            schema: "public".to_string(),
            bypass_rls: false,
        }
    }
}

/// Query operation type
#[derive(Debug, Clone, Copy)]
pub enum QueryOperation {
    Select,
    Insert,
    Update,
    Delete,
    Rpc,
}

/// Unified query interface for LumaDB integration
///
/// This trait defines the contract that any database backend must implement
/// to work with the Supabase compatibility layer.
#[async_trait::async_trait]
pub trait QueryBackend: Send + Sync {
    /// Execute a SELECT query
    async fn select(
        &self,
        table: &str,
        columns: &str,
        filters: &str,
        order: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
        context: &QueryContext,
    ) -> Result<QueryResult>;

    /// Execute an INSERT query
    async fn insert(
        &self,
        table: &str,
        rows: Vec<JsonValue>,
        returning: bool,
        context: &QueryContext,
    ) -> Result<QueryResult>;

    /// Execute an UPDATE query
    async fn update(
        &self,
        table: &str,
        values: JsonValue,
        filters: &str,
        returning: bool,
        context: &QueryContext,
    ) -> Result<QueryResult>;

    /// Execute a DELETE query
    async fn delete(
        &self,
        table: &str,
        filters: &str,
        returning: bool,
        context: &QueryContext,
    ) -> Result<QueryResult>;

    /// Call an RPC function
    async fn rpc(
        &self,
        function: &str,
        args: JsonValue,
        context: &QueryContext,
    ) -> Result<QueryResult>;

    /// Get table schema information
    async fn get_table_info(&self, schema: &str, table: &str) -> Result<TableInfo>;

    /// List all tables in a schema
    async fn list_tables(&self, schema: &str) -> Result<Vec<TableInfo>>;
}

/// In-memory query backend for development/testing
pub struct InMemoryBackend {
    tables: parking_lot::RwLock<HashMap<String, Vec<JsonValue>>>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            tables: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Add sample data
    pub fn with_sample_data(self) -> Self {
        let mut tables = self.tables.write();

        // Sample users
        tables.insert(
            "public.users".to_string(),
            vec![
                serde_json::json!({
                    "id": "11111111-1111-1111-1111-111111111111",
                    "email": "alice@example.com",
                    "name": "Alice",
                    "role": "admin",
                    "created_at": "2024-01-01T00:00:00Z"
                }),
                serde_json::json!({
                    "id": "22222222-2222-2222-2222-222222222222",
                    "email": "bob@example.com",
                    "name": "Bob",
                    "role": "user",
                    "created_at": "2024-01-02T00:00:00Z"
                }),
            ],
        );

        // Sample posts
        tables.insert(
            "public.posts".to_string(),
            vec![
                serde_json::json!({
                    "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "title": "Hello World",
                    "content": "My first post",
                    "author_id": "11111111-1111-1111-1111-111111111111",
                    "published": true,
                    "created_at": "2024-01-01T12:00:00Z"
                }),
                serde_json::json!({
                    "id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    "title": "Private Draft",
                    "content": "Work in progress",
                    "author_id": "22222222-2222-2222-2222-222222222222",
                    "published": false,
                    "created_at": "2024-01-03T12:00:00Z"
                }),
            ],
        );

        drop(tables);
        self
    }

    fn get_table_key(schema: &str, table: &str) -> String {
        format!("{}.{}", schema, table)
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl QueryBackend for InMemoryBackend {
    async fn select(
        &self,
        table: &str,
        columns: &str,
        filters: &str,
        order: Option<&str>,
        limit: Option<usize>,
        offset: Option<usize>,
        context: &QueryContext,
    ) -> Result<QueryResult> {
        let key = Self::get_table_key(&context.schema, table);
        let tables = self.tables.read();

        let rows = tables.get(&key).cloned().unwrap_or_default();

        // Apply simple filtering (production would use proper SQL parsing)
        let filtered: Vec<JsonValue> = rows
            .into_iter()
            .skip(offset.unwrap_or(0))
            .take(limit.unwrap_or(1000))
            .collect();

        Ok(QueryResult::from_rows(filtered))
    }

    async fn insert(
        &self,
        table: &str,
        rows: Vec<JsonValue>,
        returning: bool,
        context: &QueryContext,
    ) -> Result<QueryResult> {
        let key = Self::get_table_key(&context.schema, table);
        let mut tables = self.tables.write();

        let table_rows = tables.entry(key).or_insert_with(Vec::new);

        let mut inserted = Vec::new();
        for mut row in rows {
            // Add default ID if not present
            if row.get("id").is_none() {
                row["id"] = JsonValue::String(uuid::Uuid::new_v4().to_string());
            }
            if row.get("created_at").is_none() {
                row["created_at"] = JsonValue::String(chrono::Utc::now().to_rfc3339());
            }
            table_rows.push(row.clone());
            inserted.push(row);
        }

        if returning {
            Ok(QueryResult::from_rows(inserted))
        } else {
            let mut result = QueryResult::empty();
            result.rows_affected = inserted.len() as u64;
            Ok(result)
        }
    }

    async fn update(
        &self,
        table: &str,
        values: JsonValue,
        filters: &str,
        returning: bool,
        context: &QueryContext,
    ) -> Result<QueryResult> {
        let key = Self::get_table_key(&context.schema, table);
        let mut tables = self.tables.write();

        let mut updated = Vec::new();

        if let Some(table_rows) = tables.get_mut(&key) {
            for row in table_rows.iter_mut() {
                // Simple update - in production would use proper filter matching
                if let (Some(row_obj), Some(values_obj)) = (row.as_object_mut(), values.as_object())
                {
                    for (k, v) in values_obj {
                        row_obj.insert(k.clone(), v.clone());
                    }
                    updated.push(row.clone());
                }
            }
        }

        if returning {
            Ok(QueryResult::from_rows(updated))
        } else {
            let mut result = QueryResult::empty();
            result.rows_affected = updated.len() as u64;
            Ok(result)
        }
    }

    async fn delete(
        &self,
        table: &str,
        filters: &str,
        returning: bool,
        context: &QueryContext,
    ) -> Result<QueryResult> {
        let key = Self::get_table_key(&context.schema, table);
        let mut tables = self.tables.write();

        let deleted_count;
        let table_rows = tables.get_mut(&key);

        if let Some(rows) = table_rows {
            let original_len = rows.len();
            // For demo, delete all matching (would use proper filter parsing)
            rows.clear();
            deleted_count = original_len;
        } else {
            deleted_count = 0;
        }

        let mut result = QueryResult::empty();
        result.rows_affected = deleted_count as u64;
        Ok(result)
    }

    async fn rpc(
        &self,
        function: &str,
        args: JsonValue,
        context: &QueryContext,
    ) -> Result<QueryResult> {
        // Simple echo function for testing
        Ok(QueryResult::from_rows(vec![serde_json::json!({
            "function": function,
            "args": args,
            "result": "RPC executed"
        })]))
    }

    async fn get_table_info(&self, schema: &str, table: &str) -> Result<TableInfo> {
        use supabase_common::types::ColumnInfo;

        // Return mock table info
        Ok(TableInfo {
            schema: schema.to_string(),
            name: table.to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "uuid".to_string(),
                is_nullable: false,
                has_default: true,
                is_identity: false,
                is_generated: false,
                max_length: None,
                numeric_precision: None,
                description: Some("Primary key".to_string()),
            }],
            primary_key: Some(vec!["id".to_string()]),
            foreign_keys: vec![],
            is_view: false,
            is_insertable: true,
            is_updatable: true,
            is_deletable: true,
        })
    }

    async fn list_tables(&self, schema: &str) -> Result<Vec<TableInfo>> {
        // Collect table names first, then drop the lock before any async calls
        let table_names: Vec<String> = {
            let tables = self.tables.read();
            let prefix = format!("{}.", schema);
            tables
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .map(|k| k.strip_prefix(&prefix).unwrap_or(k).to_string())
                .collect()
        }; // Lock is dropped here

        let mut infos = Vec::new();
        for name in table_names {
            if let Ok(info) = self.get_table_info(schema, &name).await {
                infos.push(info);
            }
        }

        Ok(infos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_in_memory_backend() {
        let backend = InMemoryBackend::new().with_sample_data();
        let context = QueryContext::default();

        let result = backend
            .select("users", "*", "", None, None, None, &context)
            .await
            .unwrap();

        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_insert() {
        let backend = InMemoryBackend::new();
        let context = QueryContext::default();

        let rows = vec![serde_json::json!({
            "email": "test@example.com",
            "name": "Test User"
        })];

        let result = backend.insert("users", rows, true, &context).await.unwrap();
        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0].get("id").is_some());
    }
}
