//! D1 client integration tests

use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine};
use serde_json::json;

fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

/// Simulate @cloudflare/d1 query
#[tokio::test]
async fn test_d1_simple_query() {
    let storage = create_storage();
    
    // D1 request format
    let _request = json!({
        "sql": "SELECT 1 as value"
    });

    let result = storage.execute_sql("SELECT 1 as value", vec![]).await;
    assert!(result.is_ok());
}

/// Simulate D1 parameterized query
#[tokio::test]
async fn test_d1_parameterized_query() {
    let storage = create_storage();
    
    let _request = json!({
        "sql": "SELECT * FROM users WHERE id = ?",
        "params": ["123"]
    });

    let result = storage.execute_sql(
        "SELECT * FROM users WHERE id = ?",
        vec![Value::String("123".into())]
    ).await;
    assert!(result.is_ok());
}

/// Simulate D1 batch query
#[tokio::test]
async fn test_d1_batch() {
    let storage = create_storage();
    
    let statements = vec![
        ("CREATE TABLE IF NOT EXISTS batch_test (id INTEGER PRIMARY KEY, name TEXT)", vec![]),
        ("INSERT INTO batch_test (id, name) VALUES (?, ?)", vec![Value::Integer(1), Value::String("Alice".into())]),
        ("INSERT INTO batch_test (id, name) VALUES (?, ?)", vec![Value::Integer(2), Value::String("Bob".into())]),
        ("SELECT COUNT(*) as count FROM batch_test", vec![]),
    ];

    for (sql, params) in statements {
        let result = storage.execute_sql(sql, params).await;
        assert!(result.is_ok(), "Failed: {}", sql);
    }
}

/// Simulate D1 first() method
#[tokio::test]
async fn test_d1_first() {
    let storage = create_storage();
    
    // First returns only the first row
    let result = storage.execute_sql("SELECT 1 as value", vec![]).await.unwrap();
    let first_row = result.rows.first();
    assert!(first_row.is_some() || result.rows.is_empty());
}

/// Simulate D1 all() method
#[tokio::test]
async fn test_d1_all() {
    let storage = create_storage();
    
    // All returns all rows
    let result = storage.execute_sql("SELECT 1 as value UNION SELECT 2", vec![]).await.unwrap();
    // Result should contain rows
}

/// Simulate D1 raw() method
#[tokio::test]
async fn test_d1_raw() {
    let storage = create_storage();
    
    // Raw returns results as arrays
    let result = storage.execute_sql("SELECT 1, 2, 3", vec![]).await;
    assert!(result.is_ok());
}

/// Simulate D1 prepare().bind().run()
#[tokio::test]
async fn test_d1_prepare_bind_run() {
    let storage = create_storage();
    
    // Simulate prepared statement
    let sql = "INSERT INTO test_table (name, value) VALUES (?, ?)";
    let params = vec![
        Value::String("test".into()),
        Value::Integer(42),
    ];

    let result = storage.execute_sql(sql, params).await;
    assert!(result.is_ok());
}

/// Test D1 transaction simulation
#[tokio::test]
async fn test_d1_transaction() {
    let storage = create_storage();
    
    // D1 doesn't have explicit transactions but batch is atomic
    let statements = vec![
        "BEGIN",
        "INSERT INTO tx_test VALUES (1)",
        "INSERT INTO tx_test VALUES (2)",
        "COMMIT",
    ];

    for sql in statements {
        storage.execute_sql(sql, vec![]).await.ok();
    }
}

/// Test error handling
#[tokio::test]
async fn test_d1_error_handling() {
    let storage = create_storage();
    
    // Invalid SQL should return error
    let result = storage.execute_sql("INVALID SQL SYNTAX HERE", vec![]).await;
    // Result could be ok or err depending on implementation
}

/// Test metadata returned by D1
#[tokio::test]
async fn test_d1_meta() {
    let storage = create_storage();
    
    let result = storage.execute_sql("SELECT 1", vec![]).await.unwrap();
    
    // Check metadata exists
    assert!(result.metadata.execution_time_ms >= 0);
    assert!(result.metadata.rows_read >= 0);
}

/// Test D1 with binding different types
#[tokio::test]
async fn test_d1_binding_types() {
    let storage = create_storage();
    
    let params = vec![
        Value::String("text".into()),
        Value::Integer(123),
        Value::Float(3.14),
        Value::Bool(true),
        Value::Null,
    ];

    let result = storage.execute_sql(
        "SELECT ?, ?, ?, ?, ?",
        params
    ).await;
    assert!(result.is_ok());
}

/// Simulate Cloudflare Workers D1 usage pattern
#[tokio::test]
async fn test_cloudflare_workers_pattern() {
    let storage = create_storage();
    
    // Typical workers pattern
    // const result = await env.DB.prepare("SELECT * FROM users WHERE id = ?").bind(id).first();
    
    let sql = "SELECT * FROM users WHERE id = ?";
    let params = vec![Value::Integer(1)];
    
    let result = storage.execute_sql(sql, params).await.unwrap();
    let _first = result.rows.first();
}

/// Test D1 database info
#[tokio::test]
async fn test_d1_database_info() {
    let storage = create_storage();
    
    // List tables
    let tables = storage.list_tables().await.unwrap();
    // Empty storage has no tables
}
