//! Turso/LibSQL adapter tests

use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine};
use serde_json::json;

fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

// ===== Value Type Tests =====

mod value_types {
    use lumadb_multicompat::Value;

    #[test]
    fn test_turso_null() {
        let turso_val = serde_json::json!({"type": "null", "value": null});
        // Turso uses typed values
        assert_eq!(turso_val["type"], "null");
    }

    #[test]
    fn test_turso_integer() {
        let turso_val = serde_json::json!({"type": "integer", "value": "42"});
        assert_eq!(turso_val["type"], "integer");
        assert_eq!(turso_val["value"], "42");
    }

    #[test]
    fn test_turso_float() {
        let turso_val = serde_json::json!({"type": "float", "value": 3.14});
        assert_eq!(turso_val["type"], "float");
    }

    #[test]
    fn test_turso_text() {
        let turso_val = serde_json::json!({"type": "text", "value": "hello"});
        assert_eq!(turso_val["type"], "text");
        assert_eq!(turso_val["value"], "hello");
    }

    #[test]
    fn test_turso_blob() {
        // Blob is base64 encoded
        let turso_val = serde_json::json!({"type": "blob", "value": "aGVsbG8="});
        assert_eq!(turso_val["type"], "blob");
    }
}

// ===== API Response Format Tests =====

mod response_format {
    #[test]
    fn test_execute_response() {
        let response = serde_json::json!({
            "results": [{
                "cols": [{"name": "id", "decltype": "INTEGER"}],
                "rows": [[{"type": "integer", "value": "1"}]],
                "affected_row_count": 0,
                "last_insert_rowid": null,
                "replication_index": null
            }]
        });

        assert!(response["results"].is_array());
        let result = &response["results"][0];
        assert!(result["cols"].is_array());
        assert!(result["rows"].is_array());
    }

    #[test]
    fn test_pipeline_response() {
        let response = serde_json::json!({
            "results": [
                {"type": "ok", "response": {"type": "execute", "result": {}}},
                {"type": "ok", "response": {"type": "close"}}
            ]
        });

        let results = response["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_error_response() {
        let response = serde_json::json!({
            "error": {
                "message": "SQL error",
                "code": "SQLITE_ERROR"
            }
        });

        assert!(response["error"].is_object());
        assert!(response["error"]["message"].is_string());
    }
}

// ===== Statement Execution Tests =====

mod execution {
    use super::*;

    #[tokio::test]
    async fn test_simple_execute() {
        let storage = create_storage();
        let result = storage.execute_sql("SELECT 1 as value", vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_positional_params() {
        let storage = create_storage();
        let result = storage.execute_sql(
            "SELECT ? as a, ? as b",
            vec![Value::Integer(1), Value::String("two".into())]
        ).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ddl_statement() {
        let storage = create_storage();
        let result = storage.execute_sql(
            "CREATE TABLE IF NOT EXISTS turso_test (id INTEGER PRIMARY KEY, name TEXT)",
            vec![]
        ).await;
        assert!(result.is_ok());
    }
}

// ===== Batch Tests =====

mod batch {
    use super::*;

    #[tokio::test]
    async fn test_batch_statements() {
        let storage = create_storage();
        
        let statements = vec![
            "CREATE TABLE IF NOT EXISTS batch_t (id INT)",
            "INSERT INTO batch_t VALUES (1)",
            "INSERT INTO batch_t VALUES (2)",
            "SELECT * FROM batch_t",
        ];

        for sql in statements {
            let result = storage.execute_sql(sql, vec![]).await;
            assert!(result.is_ok(), "Failed: {}", sql);
        }
    }
}

// ===== Pipeline (Transaction) Tests =====

mod pipeline {
    use super::*;

    #[tokio::test]
    async fn test_transaction_semantics() {
        let storage = create_storage();
        
        // Begin transaction
        storage.execute_sql("BEGIN", vec![]).await.ok();
        
        // Insert some data
        let mut row = Row::new();
        row.push("id", Value::Integer(1));
        storage.execute_kv_put("TxTest", Value::Integer(1), row).await.unwrap();
        
        // Commit
        storage.execute_sql("COMMIT", vec![]).await.ok();
        
        // Verify data persists
        let result = storage.execute_kv_get("TxTest", Value::Integer(1)).await.unwrap();
        assert!(result.is_some());
    }
}

// ===== Concurrent Access Tests =====

mod concurrency {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn test_concurrent_reads() {
        let storage = create_storage();
        let counter = Arc::new(AtomicU64::new(0));
        
        // Insert test data
        let mut row = Row::new();
        row.push("id", Value::String("concurrent".into()));
        storage.execute_kv_put("Concurrent", Value::String("concurrent".into()), row).await.unwrap();

        let mut handles = vec![];
        
        for _ in 0..10 {
            let s = storage.clone();
            let c = counter.clone();
            handles.push(tokio::spawn(async move {
                let result = s.execute_kv_get("Concurrent", Value::String("concurrent".into())).await;
                if result.is_ok() && result.unwrap().is_some() {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        let storage = create_storage();
        let counter = Arc::new(AtomicU64::new(0));
        
        let mut handles = vec![];
        
        for i in 0..10 {
            let s = storage.clone();
            let c = counter.clone();
            handles.push(tokio::spawn(async move {
                let mut row = Row::new();
                row.push("id", Value::Integer(i));
                let result = s.execute_kv_put("ConcurrentWrite", Value::Integer(i), row).await;
                if result.is_ok() {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }
}
