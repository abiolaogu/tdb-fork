//! Database schema introspection

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use supabase_common::error::{Error, Result};
use supabase_common::types::{ColumnInfo, ForeignKeyInfo, FunctionInfo, TableInfo};

/// Schema cache for introspected database metadata
pub struct SchemaCache {
    tables: Arc<RwLock<HashMap<String, TableInfo>>>,
    functions: Arc<RwLock<HashMap<String, FunctionInfo>>>,
    schema: String,
    last_refresh: Arc<RwLock<Option<chrono::DateTime<chrono::Utc>>>>,
}

impl SchemaCache {
    /// Create a new schema cache
    pub fn new(schema: &str) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            functions: Arc::new(RwLock::new(HashMap::new())),
            schema: schema.to_string(),
            last_refresh: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize with mock schema for development
    pub fn with_mock_schema() -> Self {
        let cache = Self::new("public");

        // Add sample tables
        let users_table = TableInfo {
            schema: "public".to_string(),
            name: "users".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    is_nullable: false,
                    has_default: true,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: Some("Primary key".to_string()),
                },
                ColumnInfo {
                    name: "email".to_string(),
                    data_type: "text".to_string(),
                    is_nullable: false,
                    has_default: false,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: Some("User email".to_string()),
                },
                ColumnInfo {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                    is_nullable: true,
                    has_default: false,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
                ColumnInfo {
                    name: "created_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    is_nullable: false,
                    has_default: true,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
            ],
            primary_key: Some(vec!["id".to_string()]),
            foreign_keys: vec![],
            is_view: false,
            is_insertable: true,
            is_updatable: true,
            is_deletable: true,
        };

        let posts_table = TableInfo {
            schema: "public".to_string(),
            name: "posts".to_string(),
            columns: vec![
                ColumnInfo {
                    name: "id".to_string(),
                    data_type: "uuid".to_string(),
                    is_nullable: false,
                    has_default: true,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: Some("Primary key".to_string()),
                },
                ColumnInfo {
                    name: "title".to_string(),
                    data_type: "text".to_string(),
                    is_nullable: false,
                    has_default: false,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
                ColumnInfo {
                    name: "content".to_string(),
                    data_type: "text".to_string(),
                    is_nullable: true,
                    has_default: false,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
                ColumnInfo {
                    name: "author_id".to_string(),
                    data_type: "uuid".to_string(),
                    is_nullable: false,
                    has_default: false,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
                ColumnInfo {
                    name: "published".to_string(),
                    data_type: "boolean".to_string(),
                    is_nullable: false,
                    has_default: true,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
                ColumnInfo {
                    name: "created_at".to_string(),
                    data_type: "timestamptz".to_string(),
                    is_nullable: false,
                    has_default: true,
                    is_identity: false,
                    is_generated: false,
                    max_length: None,
                    numeric_precision: None,
                    description: None,
                },
            ],
            primary_key: Some(vec!["id".to_string()]),
            foreign_keys: vec![ForeignKeyInfo {
                name: "posts_author_id_fkey".to_string(),
                columns: vec!["author_id".to_string()],
                referenced_schema: "public".to_string(),
                referenced_table: "users".to_string(),
                referenced_columns: vec!["id".to_string()],
            }],
            is_view: false,
            is_insertable: true,
            is_updatable: true,
            is_deletable: true,
        };

        cache
            .tables
            .write()
            .insert("users".to_string(), users_table);
        cache
            .tables
            .write()
            .insert("posts".to_string(), posts_table);
        *cache.last_refresh.write() = Some(chrono::Utc::now());

        cache
    }

    /// Get table info by name
    pub fn get_table(&self, name: &str) -> Option<TableInfo> {
        self.tables.read().get(name).cloned()
    }

    /// Get all tables
    pub fn get_all_tables(&self) -> Vec<TableInfo> {
        self.tables.read().values().cloned().collect()
    }

    /// Check if table exists
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.read().contains_key(name)
    }

    /// Get function info by name
    pub fn get_function(&self, name: &str) -> Option<FunctionInfo> {
        self.functions.read().get(name).cloned()
    }

    /// Get all functions
    pub fn get_all_functions(&self) -> Vec<FunctionInfo> {
        self.functions.read().values().cloned().collect()
    }

    /// Get table columns
    pub fn get_columns(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        self.get_table(table)
            .map(|t| t.columns)
            .ok_or_else(|| Error::TableNotFound(table.to_string()))
    }

    /// Check if column exists
    pub fn has_column(&self, table: &str, column: &str) -> bool {
        self.get_table(table)
            .map(|t| t.columns.iter().any(|c| c.name == column))
            .unwrap_or(false)
    }

    /// Get foreign key relationships for a table
    pub fn get_foreign_keys(&self, table: &str) -> Vec<ForeignKeyInfo> {
        self.get_table(table)
            .map(|t| t.foreign_keys)
            .unwrap_or_default()
    }

    /// Get tables that reference this table
    pub fn get_referencing_tables(&self, table: &str) -> Vec<(String, ForeignKeyInfo)> {
        let tables = self.tables.read();
        let mut results = Vec::new();

        for (name, info) in tables.iter() {
            for fk in &info.foreign_keys {
                if fk.referenced_table == table {
                    results.push((name.clone(), fk.clone()));
                }
            }
        }

        results
    }

    /// Get schema name
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Refresh schema from database (placeholder)
    pub async fn refresh(&self) -> Result<()> {
        // TODO: Implement actual schema introspection from LumaDB
        // For now, this is a no-op with mock data
        *self.last_refresh.write() = Some(chrono::Utc::now());
        Ok(())
    }
}

/// OpenAPI schema generator
#[derive(Debug, Clone, Serialize)]
pub struct OpenApiSchema {
    pub openapi: String,
    pub info: OpenApiInfo,
    pub paths: HashMap<String, OpenApiPath>,
    pub components: OpenApiComponents,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenApiInfo {
    pub title: String,
    pub version: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenApiPath {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<OpenApiOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post: Option<OpenApiOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<OpenApiOperation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<OpenApiOperation>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenApiOperation {
    pub summary: String,
    pub tags: Vec<String>,
    pub parameters: Vec<OpenApiParameter>,
    pub responses: HashMap<String, OpenApiResponse>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenApiParameter {
    pub name: String,
    #[serde(rename = "in")]
    pub location: String,
    pub required: bool,
    pub schema: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenApiResponse {
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenApiComponents {
    pub schemas: HashMap<String, serde_json::Value>,
}

impl SchemaCache {
    /// Generate OpenAPI schema from database schema
    pub fn generate_openapi(&self) -> OpenApiSchema {
        let mut paths = HashMap::new();
        let mut schemas = HashMap::new();

        for table in self.get_all_tables() {
            let path_key = format!("/{}", table.name);

            // Generate schema for table
            let table_schema = self.table_to_json_schema(&table);
            schemas.insert(table.name.clone(), table_schema);

            // Generate path operations
            paths.insert(
                path_key,
                OpenApiPath {
                    get: Some(OpenApiOperation {
                        summary: format!("Read rows from {}", table.name),
                        tags: vec![table.name.clone()],
                        parameters: vec![
                            OpenApiParameter {
                                name: "select".to_string(),
                                location: "query".to_string(),
                                required: false,
                                schema: serde_json::json!({"type": "string"}),
                            },
                            OpenApiParameter {
                                name: "order".to_string(),
                                location: "query".to_string(),
                                required: false,
                                schema: serde_json::json!({"type": "string"}),
                            },
                            OpenApiParameter {
                                name: "limit".to_string(),
                                location: "query".to_string(),
                                required: false,
                                schema: serde_json::json!({"type": "integer"}),
                            },
                            OpenApiParameter {
                                name: "offset".to_string(),
                                location: "query".to_string(),
                                required: false,
                                schema: serde_json::json!({"type": "integer"}),
                            },
                        ],
                        responses: HashMap::from([(
                            "200".to_string(),
                            OpenApiResponse {
                                description: "OK".to_string(),
                            },
                        )]),
                    }),
                    post: if table.is_insertable {
                        Some(OpenApiOperation {
                            summary: format!("Create row in {}", table.name),
                            tags: vec![table.name.clone()],
                            parameters: vec![],
                            responses: HashMap::from([(
                                "201".to_string(),
                                OpenApiResponse {
                                    description: "Created".to_string(),
                                },
                            )]),
                        })
                    } else {
                        None
                    },
                    patch: if table.is_updatable {
                        Some(OpenApiOperation {
                            summary: format!("Update rows in {}", table.name),
                            tags: vec![table.name.clone()],
                            parameters: vec![],
                            responses: HashMap::from([(
                                "200".to_string(),
                                OpenApiResponse {
                                    description: "OK".to_string(),
                                },
                            )]),
                        })
                    } else {
                        None
                    },
                    delete: if table.is_deletable {
                        Some(OpenApiOperation {
                            summary: format!("Delete rows from {}", table.name),
                            tags: vec![table.name.clone()],
                            parameters: vec![],
                            responses: HashMap::from([(
                                "204".to_string(),
                                OpenApiResponse {
                                    description: "No Content".to_string(),
                                },
                            )]),
                        })
                    } else {
                        None
                    },
                },
            );
        }

        OpenApiSchema {
            openapi: "3.0.3".to_string(),
            info: OpenApiInfo {
                title: "LumaDB Supabase REST API".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                description: Some("Auto-generated REST API from database schema".to_string()),
            },
            paths,
            components: OpenApiComponents { schemas },
        }
    }

    /// Convert table to JSON schema
    fn table_to_json_schema(&self, table: &TableInfo) -> serde_json::Value {
        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();

        for column in &table.columns {
            let col_schema = self.column_to_json_schema(column);
            properties.insert(column.name.clone(), col_schema);

            if !column.is_nullable && !column.has_default {
                required.push(serde_json::Value::String(column.name.clone()));
            }
        }

        serde_json::json!({
            "type": "object",
            "properties": properties,
            "required": required,
        })
    }

    /// Convert column to JSON schema
    fn column_to_json_schema(&self, column: &ColumnInfo) -> serde_json::Value {
        let json_type = match column.data_type.as_str() {
            "integer" | "bigint" | "smallint" | "int4" | "int8" | "int2" => "integer",
            "real" | "double precision" | "float4" | "float8" | "numeric" | "decimal" => "number",
            "boolean" | "bool" => "boolean",
            "json" | "jsonb" => "object",
            "uuid" | "text" | "varchar" | "char" | "timestamptz" | "timestamp" | "date"
            | "time" => "string",
            _ => "string",
        };

        let mut schema = serde_json::json!({
            "type": json_type,
        });

        if let Some(desc) = &column.description {
            schema["description"] = serde_json::Value::String(desc.clone());
        }

        if column.data_type == "uuid" {
            schema["format"] = serde_json::Value::String("uuid".to_string());
        } else if column.data_type.contains("timestamp") {
            schema["format"] = serde_json::Value::String("date-time".to_string());
        } else if column.data_type == "date" {
            schema["format"] = serde_json::Value::String("date".to_string());
        }

        schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_schema() {
        let cache = SchemaCache::with_mock_schema();
        assert!(cache.has_table("users"));
        assert!(cache.has_table("posts"));
        assert!(!cache.has_table("nonexistent"));
    }

    #[test]
    fn test_get_columns() {
        let cache = SchemaCache::with_mock_schema();
        let columns = cache.get_columns("users").unwrap();
        assert!(columns.iter().any(|c| c.name == "email"));
    }

    #[test]
    fn test_foreign_keys() {
        let cache = SchemaCache::with_mock_schema();
        let fks = cache.get_foreign_keys("posts");
        assert_eq!(fks.len(), 1);
        assert_eq!(fks[0].referenced_table, "users");
    }

    #[test]
    fn test_openapi_generation() {
        let cache = SchemaCache::with_mock_schema();
        let openapi = cache.generate_openapi();
        assert!(openapi.paths.contains_key("/users"));
        assert!(openapi.paths.contains_key("/posts"));
    }
}
