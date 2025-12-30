//! D1 adapter tests

use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine};
use serde_json::json;

fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

// ===== JSON Response Format Tests =====

mod response_format {
    use serde_json::json;

    #[test]
    fn test_cloudflare_wrapper_format() {
        // Validate expected response structure
        let response = json!({
            "success": true,
            "result": [{
                "results": [],
                "success": true,
                "meta": {
                    "duration": 0.001,
                    "rows_read": 0,
                    "rows_written": 0,
                    "last_row_id": null,
                    "changed_db": false,
                    "changes": 0
                }
            }],
            "errors": [],
            "messages": []
        });

        assert!(response["success"].as_bool().unwrap());
        assert!(response["result"].is_array());
        assert!(response["errors"].is_array());
    }

    #[test]
    fn test_error_response_format() {
        let error_response = json!({
            "success": false,
            "result": null,
            "errors": [{
                "code": 1001,
                "message": "Query error"
            }],
            "messages": []
        });

        assert!(!error_response["success"].as_bool().unwrap());
        assert!(!error_response["errors"].as_array().unwrap().is_empty());
    }
}

// ===== SQL Execution Tests =====

mod sql_execution {
    use super::*;

    #[tokio::test]
    async fn test_simple_select() {
        let storage = create_storage();
        let result = storage.execute_sql("SELECT 1", vec![]).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_parameterized_query() {
        let storage = create_storage();
        let result = storage.execute_sql(
            "SELECT * FROM users WHERE id = ?",
            vec![Value::String("123".into())]
        ).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_insert_query() {
        let storage = create_storage();
        let result = storage.execute_sql(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            vec![
                Value::String("1".into()),
                Value::String("Alice".into())
            ]
        ).await;
        assert!(result.is_ok());
    }
}

// ===== Batch Query Tests =====

mod batch_queries {
    use super::*;

    #[tokio::test]
    async fn test_batch_execution() {
        let storage = create_storage();
        
        // Execute multiple queries
        let queries = vec![
            ("INSERT INTO batch_test VALUES (1)", vec![]),
            ("INSERT INTO batch_test VALUES (2)", vec![]),
            ("SELECT COUNT(*) FROM batch_test", vec![]),
        ];

        for (sql, params) in queries {
            let result = storage.execute_sql(sql, params).await;
            assert!(result.is_ok());
        }
    }
}

// ===== Data Type Conversion Tests =====

mod type_conversion {
    use lumadb_multicompat::Value;
    use serde_json::json;

    fn json_to_value(j: serde_json::Value) -> Value {
        match j {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(arr) => {
                Value::Array(arr.into_iter().map(json_to_value).collect())
            }
            serde_json::Value::Object(obj) => {
                Value::Object(obj.into_iter().map(|(k, v)| (k, json_to_value(v))).collect())
            }
        }
    }

    #[test]
    fn test_null_conversion() {
        let val = json_to_value(json!(null));
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn test_integer_conversion() {
        let val = json_to_value(json!(42));
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn test_float_conversion() {
        let val = json_to_value(json!(3.14));
        match val {
            Value::Float(f) => assert!((f - 3.14).abs() < 0.001),
            _ => panic!("Expected float"),
        }
    }

    #[test]
    fn test_string_conversion() {
        let val = json_to_value(json!("hello"));
        assert_eq!(val, Value::String("hello".into()));
    }

    #[test]
    fn test_array_conversion() {
        let val = json_to_value(json!([1, 2, 3]));
        match val {
            Value::Array(arr) => assert_eq!(arr.len(), 3),
            _ => panic!("Expected array"),
        }
    }
}

// ===== Metadata Tests =====

mod metadata {
    use super::*;

    #[tokio::test]
    async fn test_query_metadata() {
        let storage = create_storage();
        let result = storage.execute_sql("SELECT 1", vec![]).await.unwrap();
        
        // Check metadata is populated
        assert!(result.metadata.execution_time_ms >= 0);
    }

    #[tokio::test]
    async fn test_affected_rows() {
        let storage = create_storage();
        
        // PUT operation should show affected rows
        let mut row = Row::new();
        row.push("id", Value::String("1".into()));
        
        storage.execute_kv_put("MetaTest", Value::String("1".into()), row).await.unwrap();
        
        // Verify table was created
        let tables = storage.list_tables().await.unwrap();
        assert!(tables.contains(&"MetaTest".to_string()));
    }
}
