//! DynamoDB adapter tests

use std::sync::Arc;
use lumadb_multicompat::{
    LumaStorage, Value, Row, StorageEngine,
    DynamoDBServer, DynamoDBConfig,
};
use serde_json::json;

fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

// ===== Data Type Conversion Tests =====

mod type_conversion {
    use lumadb_multicompat::dynamodb::translator::*;
    use lumadb_multicompat::Value;
    use serde_json::json;

    #[test]
    fn test_string_type() {
        let ddb = json!({"S": "hello"});
        let val = parse_dynamodb_value(&ddb).unwrap();
        assert_eq!(val, Value::String("hello".into()));
        
        let back = value_to_dynamodb(&val);
        assert_eq!(back["S"], "hello");
    }

    #[test]
    fn test_number_integer() {
        let ddb = json!({"N": "42"});
        let val = parse_dynamodb_value(&ddb).unwrap();
        assert_eq!(val, Value::Integer(42));
    }

    #[test]
    fn test_number_float() {
        let ddb = json!({"N": "3.14159"});
        let val = parse_dynamodb_value(&ddb).unwrap();
        match val {
            Value::Float(f) => assert!((f - 3.14159).abs() < 0.0001),
            _ => panic!("Expected float"),
        }
    }

    #[test]
    fn test_boolean() {
        let ddb = json!({"BOOL": true});
        let val = parse_dynamodb_value(&ddb).unwrap();
        assert_eq!(val, Value::Bool(true));
    }

    #[test]
    fn test_null() {
        let ddb = json!({"NULL": true});
        let val = parse_dynamodb_value(&ddb).unwrap();
        assert_eq!(val, Value::Null);
    }

    #[test]
    fn test_list() {
        let ddb = json!({"L": [{"S": "a"}, {"N": "1"}, {"BOOL": false}]});
        let val = parse_dynamodb_value(&ddb).unwrap();
        match val {
            Value::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], Value::String("a".into()));
                assert_eq!(arr[1], Value::Integer(1));
                assert_eq!(arr[2], Value::Bool(false));
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_map() {
        let ddb = json!({"M": {"name": {"S": "Alice"}, "age": {"N": "30"}}});
        let val = parse_dynamodb_value(&ddb).unwrap();
        match val {
            Value::Object(map) => {
                assert_eq!(map.get("name"), Some(&Value::String("Alice".into())));
                assert_eq!(map.get("age"), Some(&Value::Integer(30)));
            }
            _ => panic!("Expected object"),
        }
    }

    #[test]
    fn test_string_set() {
        let ddb = json!({"SS": ["a", "b", "c"]});
        let val = parse_dynamodb_value(&ddb).unwrap();
        match val {
            Value::StringSet(ss) => {
                assert!(ss.contains(&"a".to_string()));
                assert!(ss.contains(&"b".to_string()));
                assert!(ss.contains(&"c".to_string()));
            }
            _ => panic!("Expected string set"),
        }
    }

    #[test]
    fn test_number_set() {
        let ddb = json!({"NS": ["1", "2", "3.5"]});
        let val = parse_dynamodb_value(&ddb).unwrap();
        match val {
            Value::NumberSet(ns) => {
                assert_eq!(ns.len(), 3);
            }
            _ => panic!("Expected number set"),
        }
    }

    #[test]
    fn test_binary() {
        let ddb = json!({"B": "aGVsbG8="});  // base64 for "hello"
        let val = parse_dynamodb_value(&ddb).unwrap();
        match val {
            Value::Bytes(b) => assert_eq!(b, b"hello"),
            _ => panic!("Expected bytes"),
        }
    }

    #[test]
    fn test_roundtrip_conversion() {
        use std::collections::HashMap;
        
        let original = Value::Object(HashMap::from([
            ("string".into(), Value::String("test".into())),
            ("number".into(), Value::Integer(42)),
            ("bool".into(), Value::Bool(true)),
            ("null".into(), Value::Null),
        ]));

        let dynamo = value_to_dynamodb(&original);
        let back = parse_dynamodb_value(&dynamo).unwrap();
        assert_eq!(original, back);
    }
}

// ===== Key Condition Expression Tests =====

mod key_condition_parsing {
    use lumadb_multicompat::dynamodb::translator::*;
    use lumadb_multicompat::{Value, KeyCondition, SortKeyCondition};
    use serde_json::json;

    #[test]
    fn test_simple_equality() {
        let expr = "pk = :pk";
        let values = json!({":pk": {"S": "user123"}});
        
        let cond = parse_key_condition(expr, None, values.as_object()).unwrap();
        assert!(cond.is_some());
        
        let cond = cond.unwrap();
        assert_eq!(cond.partition_key.0, "pk");
        assert_eq!(cond.partition_key.1, Value::String("user123".into()));
    }

    #[test]
    fn test_with_sort_key_gt() {
        let expr = "pk = :pk AND sk > :sk";
        let values = json!({
            ":pk": {"S": "user123"},
            ":sk": {"N": "100"}
        });
        
        let cond = parse_key_condition(expr, None, values.as_object()).unwrap().unwrap();
        
        assert!(cond.sort_key.is_some());
        let (sk_name, sk_cond) = cond.sort_key.unwrap();
        assert_eq!(sk_name, "sk");
        match sk_cond {
            SortKeyCondition::GreaterThan(v) => assert_eq!(v, Value::Integer(100)),
            _ => panic!("Expected GreaterThan"),
        }
    }

    #[test]
    fn test_begins_with() {
        let expr = "pk = :pk AND begins_with(sk, :prefix)";
        let values = json!({
            ":pk": {"S": "user123"},
            ":prefix": {"S": "ORDER#"}
        });
        
        let cond = parse_key_condition(expr, None, values.as_object()).unwrap().unwrap();
        
        let (sk_name, sk_cond) = cond.sort_key.unwrap();
        assert_eq!(sk_name, "sk");
        match sk_cond {
            SortKeyCondition::BeginsWith(prefix) => assert_eq!(prefix, "ORDER#"),
            _ => panic!("Expected BeginsWith"),
        }
    }

    #[test]
    #[ignore = "BETWEEN parsing not yet implemented"]
    fn test_between() {
        let expr = "pk = :pk AND sk BETWEEN :low AND :high";
        let values = json!({
            ":pk": {"S": "user123"},
            ":low": {"N": "1"},
            ":high": {"N": "100"}
        });
        
        let cond = parse_key_condition(expr, None, values.as_object()).unwrap().unwrap();
        
        let (_, sk_cond) = cond.sort_key.unwrap();
        match sk_cond {
            SortKeyCondition::Between(low, high) => {
                assert_eq!(low, Value::Integer(1));
                assert_eq!(high, Value::Integer(100));
            }
            _ => panic!("Expected Between"),
        }
    }

    #[test]
    fn test_with_attribute_names() {
        let expr = "#pk = :pk";
        let names = json!({"#pk": "partition_key"});
        let values = json!({":pk": {"S": "test"}});
        
        let cond = parse_key_condition(
            expr,
            names.as_object(),
            values.as_object()
        ).unwrap().unwrap();
        
        assert_eq!(cond.partition_key.0, "partition_key");
    }
}

// ===== CRUD Operation Tests =====

mod crud_operations {
    use super::*;
    
    #[tokio::test]
    async fn test_put_and_get() {
        let storage = create_storage();
        
        let mut row = Row::new();
        row.push("pk", Value::String("user1".into()));
        row.push("sk", Value::String("profile".into()));
        row.push("name", Value::String("Alice".into()));
        
        let key = Value::Object([
            ("pk".into(), Value::String("user1".into())),
            ("sk".into(), Value::String("profile".into())),
        ].into());
        
        storage.execute_kv_put("Users", key.clone(), row.clone()).await.unwrap();
        
        let result = storage.execute_kv_get("Users", key).await.unwrap();
        assert!(result.is_some());
        
        let retrieved = result.unwrap();
        assert_eq!(retrieved.get("name"), Some(&Value::String("Alice".into())));
    }

    #[tokio::test]
    async fn test_delete() {
        let storage = create_storage();
        
        let key = Value::String("item1".into());
        let mut row = Row::new();
        row.push("id", Value::String("item1".into()));
        
        storage.execute_kv_put("Items", key.clone(), row).await.unwrap();
        storage.execute_kv_delete("Items", key.clone()).await.unwrap();
        
        let result = storage.execute_kv_get("Items", key).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_batch_write() {
        use lumadb_multicompat::BatchOperation;
        
        let storage = create_storage();
        
        let ops = vec![
            BatchOperation::Put {
                table: "BatchTest".into(),
                key: Value::String("1".into()),
                value: {
                    let mut r = Row::new();
                    r.push("id", Value::String("1".into()));
                    r
                },
            },
            BatchOperation::Put {
                table: "BatchTest".into(),
                key: Value::String("2".into()),
                value: {
                    let mut r = Row::new();
                    r.push("id", Value::String("2".into()));
                    r
                },
            },
        ];

        storage.batch_write(ops).await.unwrap();

        let r1 = storage.execute_kv_get("BatchTest", Value::String("1".into())).await.unwrap();
        let r2 = storage.execute_kv_get("BatchTest", Value::String("2".into())).await.unwrap();
        
        assert!(r1.is_some());
        assert!(r2.is_some());
    }
}

// ===== Query Tests =====

mod query_tests {
    use super::*;
    use lumadb_multicompat::{QueryFilter, KeyCondition, SortKeyCondition};
    
    #[tokio::test]
    async fn test_query_with_partition_key() {
        let storage = create_storage();
        
        // Insert test data
        for i in 1..=5 {
            let key = Value::Object([
                ("pk".into(), Value::String("user1".into())),
                ("sk".into(), Value::String(format!("order{}", i))),
            ].into());
            
            let mut row = Row::new();
            row.push("pk", Value::String("user1".into()));
            row.push("sk", Value::String(format!("order{}", i)));
            row.push("amount", Value::Integer(i * 100));
            
            storage.execute_kv_put("Orders", key, row).await.unwrap();
        }

        // Query all orders for user1
        let filter = QueryFilter {
            key_condition: Some(KeyCondition {
                partition_key: ("pk".into(), Value::String("user1".into())),
                sort_key: None,
            }),
            ..Default::default()
        };

        let results = storage.execute_kv_query("Orders", filter).await.unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn test_query_with_limit() {
        let storage = create_storage();
        
        for i in 1..=10 {
            let key = Value::String(format!("{}", i));
            let mut row = Row::new();
            row.push("id", Value::String(format!("{}", i)));
            storage.execute_kv_put("LimitTest", key, row).await.unwrap();
        }

        let filter = QueryFilter {
            limit: Some(3),
            ..Default::default()
        };

        let results = storage.execute_kv_query("LimitTest", filter).await.unwrap();
        assert_eq!(results.len(), 3);
    }
}

// ===== Error Handling Tests =====

mod error_handling {
    use super::*;
    
    #[tokio::test]
    async fn test_get_nonexistent_table() {
        let storage = create_storage();
        let result = storage.execute_kv_get("NonexistentTable", Value::String("key".into())).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_describe_nonexistent_table() {
        let storage = create_storage();
        let result = storage.describe_table("NonexistentTable").await;
        assert!(result.is_err());
    }
}
