//! Comprehensive test suite for multi-protocol compatibility layer

mod dynamodb_tests;
mod d1_tests;
mod turso_tests;
mod storage_tests;
mod integration_tests;

use std::sync::Arc;
use lumadb_multicompat::{
    LumaStorage, Value, Row, Column, QueryFilter, BatchOperation,
    DynamoDBServer, DynamoDBConfig,
    D1Server, D1Config,
    TursoServer, TursoConfig,
    StorageEngine,
};

/// Shared test storage
fn create_test_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

/// Helper to create test rows
fn test_row(id: &str, name: &str) -> Row {
    let mut row = Row::new();
    row.push("id", Value::String(id.to_string()));
    row.push("name", Value::String(name.to_string()));
    row
}
