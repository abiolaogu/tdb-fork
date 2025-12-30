//! GraphQL API Service for Supabase Compatibility
//!
//! Provides auto-generated GraphQL API from database schema:
//! - Schema introspection
//! - Query and mutation generation
//! - Subscription support via Realtime

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// GraphQL schema representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLSchema {
    /// Types in the schema
    pub types: Vec<GraphQLType>,
    /// Query type fields
    pub queries: Vec<GraphQLField>,
    /// Mutation type fields
    pub mutations: Vec<GraphQLField>,
    /// Subscription type fields
    pub subscriptions: Vec<GraphQLField>,
}

/// A GraphQL type definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLType {
    pub name: String,
    pub kind: TypeKind,
    pub fields: Vec<GraphQLField>,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeKind {
    Object,
    Input,
    Enum,
    Scalar,
    Interface,
    Union,
}

/// A GraphQL field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLField {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub is_list: bool,
    pub args: Vec<GraphQLArg>,
    pub description: Option<String>,
}

/// A GraphQL argument
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLArg {
    pub name: String,
    pub type_name: String,
    pub nullable: bool,
    pub default_value: Option<String>,
}

/// GraphQL request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLRequest {
    pub query: String,
    #[serde(default)]
    pub operation_name: Option<String>,
    #[serde(default)]
    pub variables: Option<serde_json::Value>,
}

/// GraphQL response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub errors: Vec<GraphQLError>,
}

/// GraphQL error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub locations: Option<Vec<ErrorLocation>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorLocation {
    pub line: u32,
    pub column: u32,
}

/// GraphQL executor
pub struct GraphQLExecutor {
    schema: Arc<RwLock<GraphQLSchema>>,
}

impl GraphQLExecutor {
    /// Create a new executor with empty schema
    pub fn new() -> Self {
        Self {
            schema: Arc::new(RwLock::new(GraphQLSchema {
                types: vec![],
                queries: vec![],
                mutations: vec![],
                subscriptions: vec![],
            })),
        }
    }

    /// Generate schema from database tables
    pub fn generate_schema_from_tables(&self, tables: &[supabase_common::types::TableInfo]) {
        let mut schema = self.schema.write();

        for table in tables {
            // Generate object type for table
            let object_type = GraphQLType {
                name: to_pascal_case(&table.name),
                kind: TypeKind::Object,
                fields: table
                    .columns
                    .iter()
                    .map(|col| GraphQLField {
                        name: col.name.clone(),
                        type_name: sql_to_graphql_type(&col.data_type),
                        nullable: col.is_nullable,
                        is_list: false,
                        args: vec![],
                        description: col.description.clone(),
                    })
                    .collect(),
                description: Some(format!("Generated from table {}", table.name)),
            };

            schema.types.push(object_type);

            // Generate query for table
            schema.queries.push(GraphQLField {
                name: to_camel_case(&table.name),
                type_name: format!("[{}!]!", to_pascal_case(&table.name)),
                nullable: false,
                is_list: true,
                args: vec![
                    GraphQLArg {
                        name: "filter".to_string(),
                        type_name: format!("{}Filter", to_pascal_case(&table.name)),
                        nullable: true,
                        default_value: None,
                    },
                    GraphQLArg {
                        name: "orderBy".to_string(),
                        type_name: format!("{}OrderBy", to_pascal_case(&table.name)),
                        nullable: true,
                        default_value: None,
                    },
                    GraphQLArg {
                        name: "limit".to_string(),
                        type_name: "Int".to_string(),
                        nullable: true,
                        default_value: None,
                    },
                    GraphQLArg {
                        name: "offset".to_string(),
                        type_name: "Int".to_string(),
                        nullable: true,
                        default_value: None,
                    },
                ],
                description: Some(format!("Query {} records", table.name)),
            });

            // Generate mutations
            schema.mutations.push(GraphQLField {
                name: format!("insert{}", to_pascal_case(&table.name)),
                type_name: format!("{}!", to_pascal_case(&table.name)),
                nullable: false,
                is_list: false,
                args: vec![GraphQLArg {
                    name: "object".to_string(),
                    type_name: format!("{}InsertInput!", to_pascal_case(&table.name)),
                    nullable: false,
                    default_value: None,
                }],
                description: Some(format!("Insert a {} record", table.name)),
            });
        }
    }

    /// Execute a GraphQL request
    pub async fn execute(&self, request: GraphQLRequest) -> GraphQLResponse {
        // Simplified execution - in production would use a full GraphQL parser
        if request.query.contains("__schema") || request.query.contains("__type") {
            return self.introspection_response();
        }

        // Placeholder response
        GraphQLResponse {
            data: Some(serde_json::json!({"message": "GraphQL executor placeholder"})),
            errors: vec![],
        }
    }

    fn introspection_response(&self) -> GraphQLResponse {
        let schema = self.schema.read();
        GraphQLResponse {
            data: Some(serde_json::json!({
                "__schema": {
                    "types": schema.types.iter().map(|t| {
                        serde_json::json!({
                            "name": t.name,
                            "kind": format!("{:?}", t.kind).to_uppercase()
                        })
                    }).collect::<Vec<_>>(),
                    "queryType": {"name": "Query"},
                    "mutationType": {"name": "Mutation"}
                }
            })),
            errors: vec![],
        }
    }
}

impl Default for GraphQLExecutor {
    fn default() -> Self {
        Self::new()
    }
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect()
}

fn to_camel_case(s: &str) -> String {
    let pascal = to_pascal_case(s);
    let mut chars = pascal.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_lowercase().collect::<String>() + chars.as_str(),
    }
}

fn sql_to_graphql_type(sql_type: &str) -> String {
    match sql_type.to_lowercase().as_str() {
        "int" | "integer" | "int4" | "int8" | "bigint" | "smallint" => "Int".to_string(),
        "float" | "float4" | "float8" | "double" | "real" | "numeric" | "decimal" => {
            "Float".to_string()
        }
        "bool" | "boolean" => "Boolean".to_string(),
        "uuid" => "ID".to_string(),
        "json" | "jsonb" => "JSON".to_string(),
        "timestamp" | "timestamptz" | "date" | "time" => "DateTime".to_string(),
        _ => "String".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_graphql_executor() {
        let executor = GraphQLExecutor::new();
        assert_eq!(executor.schema.read().types.len(), 0);
    }

    #[test]
    fn test_case_conversion() {
        assert_eq!(to_pascal_case("user_profiles"), "UserProfiles");
        assert_eq!(to_camel_case("user_profiles"), "userProfiles");
    }
}
