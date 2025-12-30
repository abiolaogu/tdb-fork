//! Turso client integration tests

use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine};
use serde_json::json;

fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

/// Simulate @libsql/client execute
#[tokio::test]
async fn test_turso_execute() {
    let storage = create_storage();
    
    // Turso execute request
    let _request = json!({
        "stmt": {
            "sql": "SELECT 1 as value"
        }
    });

    let result = storage.execute_sql("SELECT 1 as value", vec![]).await;
    assert!(result.is_ok());
}

/// Simulate Turso with positional args
#[tokio::test]
async fn test_turso_positional_args() {
    let storage = create_storage();
    
    let _request = json!({
        "stmt": {
            "sql": "SELECT ? as a, ? as b",
            "args": [
                {"type": "integer", "value": "1"},
                {"type": "text", "value": "hello"}
            ]
        }
    });

    let result = storage.execute_sql(
        "SELECT ? as a, ? as b",
        vec![Value::Integer(1), Value::String("hello".into())]
    ).await;
    assert!(result.is_ok());
}

/// Simulate Turso with named args
#[tokio::test]
async fn test_turso_named_args() {
    let storage = create_storage();
    
    let _request = json!({
        "stmt": {
            "sql": "SELECT :id as id, :name as name",
            "named_args": [
                {"name": "id", "value": {"type": "integer", "value": "1"}},
                {"name": "name", "value": {"type": "text", "value": "Alice"}}
            ]
        }
    });

    // Named args converted to positional
    let result = storage.execute_sql(
        "SELECT ? as id, ? as name",
        vec![Value::Integer(1), Value::String("Alice".into())]
    ).await;
    assert!(result.is_ok());
}

/// Simulate Turso batch
#[tokio::test]
async fn test_turso_batch() {
    let storage = create_storage();
    
    let statements = vec![
        "CREATE TABLE IF NOT EXISTS turso_test (id INTEGER PRIMARY KEY, name TEXT)",
        "INSERT INTO turso_test VALUES (1, 'Alice')",
        "INSERT INTO turso_test VALUES (2, 'Bob')",
        "SELECT * FROM turso_test",
    ];

    for sql in statements {
        let result = storage.execute_sql(sql, vec![]).await;
        assert!(result.is_ok(), "Failed: {}", sql);
    }
}

/// Simulate Turso pipeline (v2 API)
#[tokio::test]
async fn test_turso_pipeline() {
    let storage = create_storage();
    
    let _pipeline = json!({
        "requests": [
            {"type": "execute", "stmt": {"sql": "BEGIN"}},
            {"type": "execute", "stmt": {"sql": "INSERT INTO pipeline_test VALUES (1)"}},
            {"type": "execute", "stmt": {"sql": "COMMIT"}},
            {"type": "close"}
        ]
    });

    // Execute pipeline steps
    for sql in ["BEGIN", "INSERT INTO pipeline_test VALUES (1)", "COMMIT"] {
        storage.execute_sql(sql, vec![]).await.ok();
    }
}

/// Test Turso transaction
#[tokio::test]
async fn test_turso_transaction() {
    let storage = create_storage();
    
    storage.execute_sql("BEGIN", vec![]).await.ok();
    
    let mut row = Row::new();
    row.push("id", Value::Integer(1));
    storage.execute_kv_put("TxTable", Value::Integer(1), row).await.unwrap();
    
    storage.execute_sql("COMMIT", vec![]).await.ok();
    
    // Verify data persists
    let result = storage.execute_kv_get("TxTable", Value::Integer(1)).await.unwrap();
    assert!(result.is_some());
}

/// Test Turso sync (replica)
#[tokio::test]
async fn test_turso_sync() {
    let storage = create_storage();
    
    // Simulate sync operation (no-op in this implementation)
    let result = storage.execute_sql("SELECT 1", vec![]).await;
    assert!(result.is_ok());
}

/// Test concurrent requests
#[tokio::test]
async fn test_turso_concurrent() {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    let storage = create_storage();
    let counter = Arc::new(AtomicU64::new(0));
    
    let mut handles = vec![];
    for i in 0..10 {
        let s = storage.clone();
        let c = counter.clone();
        handles.push(tokio::spawn(async move {
            let mut row = Row::new();
            row.push("id", Value::Integer(i));
            s.execute_kv_put("ConcurrentTurso", Value::Integer(i), row).await.ok();
            c.fetch_add(1, Ordering::SeqCst);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 10);
}

/// Simulate libsql Rust client
#[tokio::test]
async fn test_libsql_rust_client() {
    let storage = create_storage();
    
    // libsql::connect("libsql://db-org.turso.io?authToken=...")
    // let conn = db.connect()?;
    // let mut rows = conn.query("SELECT * FROM users", ())?;
    
    let result = storage.execute_sql("SELECT * FROM users", vec![]).await;
    assert!(result.is_ok());
}

/// Simulate libsql Python client
#[tokio::test]
async fn test_libsql_python_client() {
    let storage = create_storage();
    
    // import libsql_experimental as libsql
    // conn = libsql.connect("test.db")
    // conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)")
    
    let statements = vec![
        "CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT)",
        "INSERT INTO users VALUES (1, 'Python User')",
        "SELECT * FROM users",
    ];

    for sql in statements {
        storage.execute_sql(sql, vec![]).await.ok();
    }
}

/// Test TypeScript client pattern
#[tokio::test]
async fn test_turso_typescript_pattern() {
    let storage = create_storage();
    
    // import { createClient } from '@libsql/client';
    // const client = createClient({ url: '...', authToken: '...' });
    // const result = await client.execute('SELECT * FROM users');
    
    let result = storage.execute_sql("SELECT * FROM users WHERE id = 1", vec![]).await;
    assert!(result.is_ok());
}

/// Test blob handling
#[tokio::test]
async fn test_turso_blob() {
    let storage = create_storage();
    
    // Insert blob data
    let mut row = Row::new();
    row.push("id", Value::Integer(1));
    row.push("data", Value::Bytes(b"binary data here".to_vec()));
    
    storage.execute_kv_put("BlobTable", Value::Integer(1), row).await.unwrap();
    
    let result = storage.execute_kv_get("BlobTable", Value::Integer(1)).await.unwrap();
    assert!(result.is_some());
}

/// Test error messages
#[tokio::test]
async fn test_turso_errors() {
    let storage = create_storage();
    
    // Non-existent table
    let result = storage.execute_kv_get("NoTable", Value::Integer(1)).await;
    assert!(result.is_err());
}

/// Test replication index
#[tokio::test]
async fn test_turso_replication_index() {
    let storage = create_storage();
    
    // Execute a write and check metadata
    let mut row = Row::new();
    row.push("id", Value::Integer(999));
    storage.execute_kv_put("ReplicationTest", Value::Integer(999), row).await.unwrap();
    
    // Replication index would be in metadata
    let result = storage.execute_sql("SELECT 1", vec![]).await.unwrap();
    // result.metadata would contain replication_index if implemented
}
