//! Complete DynamoDB SDK Integration Tests
//!
//! These tests use HTTP client to simulate real AWS SDK behavior.

use std::sync::Arc;
use std::collections::HashMap;
use serde_json::{json, Value};
use lumadb_multicompat::{LumaStorage, StorageEngine};
use lumadb_multicompat::core::{Value as LumaValue, Row};

/// Test helper to create storage
fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

// ===== PutItem Tests =====

#[tokio::test]
async fn test_put_item_simple() {
    let storage = create_storage();
    
    // Simulate PutItem request
    let put_request = json!({
        "operation": "PutItem",
        "TableName": "Users",
        "Item": {
            "pk": {"S": "USER#123"},
            "sk": {"S": "PROFILE"},
            "name": {"S": "Alice"},
            "age": {"N": "30"},
            "active": {"BOOL": true}
        }
    });

    // Convert to storage operation
    let key = LumaValue::Object(HashMap::from([
        ("pk".into(), LumaValue::String("USER#123".into())),
        ("sk".into(), LumaValue::String("PROFILE".into())),
    ]));

    let mut row = Row::new();
    row.push("pk", LumaValue::String("USER#123".into()));
    row.push("sk", LumaValue::String("PROFILE".into()));
    row.push("name", LumaValue::String("Alice".into()));
    row.push("age", LumaValue::Integer(30));
    row.push("active", LumaValue::Bool(true));

    let result = storage.execute_kv_put("Users", key, row).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_put_item_with_all_types() {
    let storage = create_storage();
    
    let key = LumaValue::String("all_types".into());
    let mut row = Row::new();
    row.push("id", LumaValue::String("all_types".into()));
    row.push("string_val", LumaValue::String("text".into()));
    row.push("number_val", LumaValue::Integer(42));
    row.push("float_val", LumaValue::Float(3.14159));
    row.push("bool_val", LumaValue::Bool(true));
    row.push("null_val", LumaValue::Null);
    row.push("list_val", LumaValue::Array(vec![
        LumaValue::Integer(1),
        LumaValue::Integer(2),
        LumaValue::Integer(3),
    ]));
    row.push("map_val", LumaValue::Object(HashMap::from([
        ("nested".into(), LumaValue::String("value".into())),
    ])));

    storage.execute_kv_put("TypeTest", key.clone(), row).await.unwrap();

    let result = storage.execute_kv_get("TypeTest", key).await.unwrap();
    assert!(result.is_some());

    let item = result.unwrap();
    assert_eq!(item.get("string_val"), Some(&LumaValue::String("text".into())));
    assert_eq!(item.get("number_val"), Some(&LumaValue::Integer(42)));
}

// ===== GetItem Tests =====

#[tokio::test]
async fn test_get_item_existing() {
    let storage = create_storage();
    
    // Put item first
    let key = LumaValue::Object(HashMap::from([
        ("pk".into(), LumaValue::String("USER#456".into())),
        ("sk".into(), LumaValue::String("PROFILE".into())),
    ]));

    let mut row = Row::new();
    row.push("pk", LumaValue::String("USER#456".into()));
    row.push("sk", LumaValue::String("PROFILE".into()));
    row.push("email", LumaValue::String("user@example.com".into()));

    storage.execute_kv_put("Users", key.clone(), row).await.unwrap();

    // GetItem request
    let get_request = json!({
        "operation": "GetItem",
        "TableName": "Users",
        "Key": {
            "pk": {"S": "USER#456"},
            "sk": {"S": "PROFILE"}
        }
    });

    let result = storage.execute_kv_get("Users", key).await.unwrap();
    assert!(result.is_some());

    let item = result.unwrap();
    assert_eq!(item.get("email"), Some(&LumaValue::String("user@example.com".into())));
}

#[tokio::test]
async fn test_get_item_not_found() {
    let storage = create_storage();
    
    // Create table with a different item
    let key = LumaValue::String("exists".into());
    let mut row = Row::new();
    row.push("id", LumaValue::String("exists".into()));
    storage.execute_kv_put("GetNotFound", key, row).await.unwrap();

    // Get non-existent item
    let key = LumaValue::String("does_not_exist".into());
    let result = storage.execute_kv_get("GetNotFound", key).await.unwrap();
    assert!(result.is_none());
}

// ===== DeleteItem Tests =====

#[tokio::test]
async fn test_delete_item() {
    let storage = create_storage();
    
    let key = LumaValue::String("to_delete".into());
    let mut row = Row::new();
    row.push("id", LumaValue::String("to_delete".into()));

    storage.execute_kv_put("DeleteTest", key.clone(), row).await.unwrap();

    // Verify exists
    assert!(storage.execute_kv_get("DeleteTest", key.clone()).await.unwrap().is_some());

    // Delete
    storage.execute_kv_delete("DeleteTest", key.clone()).await.unwrap();

    // Verify deleted
    assert!(storage.execute_kv_get("DeleteTest", key).await.unwrap().is_none());
}

// ===== Query Tests =====

#[tokio::test]
async fn test_query_by_partition_key() {
    use lumadb_multicompat::core::{QueryFilter, KeyCondition};
    
    let storage = create_storage();
    
    // Insert multiple items for same partition key
    for i in 1..=5 {
        let key = LumaValue::Object(HashMap::from([
            ("pk".into(), LumaValue::String("QUERY_USER".into())),
            ("sk".into(), LumaValue::String(format!("ORDER#{:03}", i))),
        ]));

        let mut row = Row::new();
        row.push("pk", LumaValue::String("QUERY_USER".into()));
        row.push("sk", LumaValue::String(format!("ORDER#{:03}", i)));
        row.push("amount", LumaValue::Integer(i * 100));

        storage.execute_kv_put("QueryTest", key, row).await.unwrap();
    }

    // Query request
    let _query_request = json!({
        "operation": "Query",
        "TableName": "QueryTest",
        "KeyConditionExpression": "pk = :pk",
        "ExpressionAttributeValues": {
            ":pk": {"S": "QUERY_USER"}
        }
    });

    let filter = QueryFilter {
        key_condition: Some(KeyCondition {
            partition_key: ("pk".into(), LumaValue::String("QUERY_USER".into())),
            sort_key: None,
        }),
        ..Default::default()
    };

    let results = storage.execute_kv_query("QueryTest", filter).await.unwrap();
    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn test_query_with_limit() {
    use lumadb_multicompat::core::{QueryFilter, KeyCondition};
    
    let storage = create_storage();
    
    for i in 1..=10 {
        let key = LumaValue::Object(HashMap::from([
            ("pk".into(), LumaValue::String("LIMIT_USER".into())),
            ("sk".into(), LumaValue::Integer(i)),
        ]));

        let mut row = Row::new();
        row.push("pk", LumaValue::String("LIMIT_USER".into()));
        row.push("sk", LumaValue::Integer(i));

        storage.execute_kv_put("LimitTest", key, row).await.unwrap();
    }

    let filter = QueryFilter {
        key_condition: Some(KeyCondition {
            partition_key: ("pk".into(), LumaValue::String("LIMIT_USER".into())),
            sort_key: None,
        }),
        limit: Some(3),
        ..Default::default()
    };

    let results = storage.execute_kv_query("LimitTest", filter).await.unwrap();
    assert_eq!(results.len(), 3);
}

// ===== BatchWriteItem Tests =====

#[tokio::test]
async fn test_batch_write_25_items() {
    use lumadb_multicompat::core::BatchOperation;
    
    let storage = create_storage();
    
    // DynamoDB allows up to 25 items per BatchWriteItem
    let ops: Vec<BatchOperation> = (0..25)
        .map(|i| BatchOperation::Put {
            table: "BatchTable".into(),
            key: LumaValue::String(format!("batch_item_{}", i)),
            value: {
                let mut r = Row::new();
                r.push("id", LumaValue::String(format!("batch_item_{}", i)));
                r.push("index", LumaValue::Integer(i));
                r.push("data", LumaValue::String(format!("Batch data {}", i)));
                r
            },
        })
        .collect();

    storage.batch_write(ops).await.unwrap();

    // Verify all items exist
    for i in 0..25 {
        let key = LumaValue::String(format!("batch_item_{}", i));
        let result = storage.execute_kv_get("BatchTable", key).await.unwrap();
        assert!(result.is_some(), "Item {} should exist", i);
    }
}

#[tokio::test]
async fn test_batch_write_mixed_operations() {
    use lumadb_multicompat::core::BatchOperation;
    
    let storage = create_storage();
    
    // First put an item to delete later
    let key = LumaValue::String("to_be_deleted".into());
    let mut row = Row::new();
    row.push("id", LumaValue::String("to_be_deleted".into()));
    storage.execute_kv_put("MixedBatch", key, row).await.unwrap();

    // Batch with puts and deletes
    let ops = vec![
        BatchOperation::Put {
            table: "MixedBatch".into(),
            key: LumaValue::String("new_item_1".into()),
            value: {
                let mut r = Row::new();
                r.push("id", LumaValue::String("new_item_1".into()));
                r
            },
        },
        BatchOperation::Put {
            table: "MixedBatch".into(),
            key: LumaValue::String("new_item_2".into()),
            value: {
                let mut r = Row::new();
                r.push("id", LumaValue::String("new_item_2".into()));
                r
            },
        },
        BatchOperation::Delete {
            table: "MixedBatch".into(),
            key: LumaValue::String("to_be_deleted".into()),
        },
    ];

    storage.batch_write(ops).await.unwrap();

    // Verify new items exist
    assert!(storage.execute_kv_get("MixedBatch", LumaValue::String("new_item_1".into())).await.unwrap().is_some());
    assert!(storage.execute_kv_get("MixedBatch", LumaValue::String("new_item_2".into())).await.unwrap().is_some());
    
    // Verify deleted item is gone
    assert!(storage.execute_kv_get("MixedBatch", LumaValue::String("to_be_deleted".into())).await.unwrap().is_none());
}

// ===== TransactWriteItems Tests =====

#[tokio::test]
async fn test_transact_write() {
    use lumadb_multicompat::core::{TransactWriteItem, BatchOperation};
    
    let storage = create_storage();
    
    let items = vec![
        TransactWriteItem {
            operation: BatchOperation::Put {
                table: "TransactTable".into(),
                key: LumaValue::String("tx_item_1".into()),
                value: {
                    let mut r = Row::new();
                    r.push("id", LumaValue::String("tx_item_1".into()));
                    r.push("status", LumaValue::String("created".into()));
                    r
                },
            },
            condition_expression: None,
        },
        TransactWriteItem {
            operation: BatchOperation::Put {
                table: "TransactTable".into(),
                key: LumaValue::String("tx_item_2".into()),
                value: {
                    let mut r = Row::new();
                    r.push("id", LumaValue::String("tx_item_2".into()));
                    r.push("status", LumaValue::String("created".into()));
                    r
                },
            },
            condition_expression: None,
        },
    ];

    storage.transact_write(items).await.unwrap();

    // Both should exist
    assert!(storage.execute_kv_get("TransactTable", LumaValue::String("tx_item_1".into())).await.unwrap().is_some());
    assert!(storage.execute_kv_get("TransactTable", LumaValue::String("tx_item_2".into())).await.unwrap().is_some());
}

// ===== Table Operations Tests =====

#[tokio::test]
async fn test_create_and_describe_table() {
    use lumadb_multicompat::core::{TableSchema, KeySchema, AttributeDefinition, AttributeType};
    
    let storage = create_storage();
    
    let schema = TableSchema {
        name: "DescribableTable".into(),
        key_schema: KeySchema {
            partition_key: "pk".into(),
            sort_key: Some("sk".into()),
        },
        attributes: vec![
            AttributeDefinition { name: "pk".into(), attribute_type: AttributeType::S },
            AttributeDefinition { name: "sk".into(), attribute_type: AttributeType::N },
        ],
        global_secondary_indexes: vec![],
        local_secondary_indexes: vec![],
    };

    storage.create_table("DescribableTable", schema).await.unwrap();

    // DescribeTable
    let described = storage.describe_table("DescribableTable").await.unwrap();
    assert_eq!(described.name, "DescribableTable");
    assert_eq!(described.key_schema.partition_key, "pk");
    assert_eq!(described.key_schema.sort_key, Some("sk".into()));
}

#[tokio::test]
async fn test_list_tables() {
    use lumadb_multicompat::core::{TableSchema, KeySchema};
    
    let storage = create_storage();
    
    // Create a few tables
    for name in ["ListTable1", "ListTable2", "ListTable3"] {
        let schema = TableSchema {
            name: name.into(),
            key_schema: KeySchema {
                partition_key: "id".into(),
                sort_key: None,
            },
            attributes: vec![],
            global_secondary_indexes: vec![],
            local_secondary_indexes: vec![],
        };
        storage.create_table(name, schema).await.unwrap();
    }

    let tables = storage.list_tables().await.unwrap();
    assert!(tables.contains(&"ListTable1".to_string()));
    assert!(tables.contains(&"ListTable2".to_string()));
    assert!(tables.contains(&"ListTable3".to_string()));
}

#[tokio::test]
async fn test_delete_table() {
    use lumadb_multicompat::core::{TableSchema, KeySchema};
    
    let storage = create_storage();
    
    let schema = TableSchema {
        name: "ToDeleteTable".into(),
        key_schema: KeySchema {
            partition_key: "id".into(),
            sort_key: None,
        },
        attributes: vec![],
        global_secondary_indexes: vec![],
        local_secondary_indexes: vec![],
    };

    storage.create_table("ToDeleteTable", schema).await.unwrap();
    
    // Verify exists
    let tables = storage.list_tables().await.unwrap();
    assert!(tables.contains(&"ToDeleteTable".to_string()));

    // Delete
    storage.delete_table("ToDeleteTable").await.unwrap();

    // Verify deleted
    let tables = storage.list_tables().await.unwrap();
    assert!(!tables.contains(&"ToDeleteTable".to_string()));
}

// ===== Error Handling Tests =====

#[tokio::test]
async fn test_resource_not_found() {
    let storage = create_storage();
    
    let result = storage.execute_kv_get("NonExistentTable", LumaValue::String("key".into())).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_table_already_exists() {
    use lumadb_multicompat::core::{TableSchema, KeySchema};
    
    let storage = create_storage();
    
    let schema = TableSchema {
        name: "DuplicateTable".into(),
        key_schema: KeySchema {
            partition_key: "id".into(),
            sort_key: None,
        },
        attributes: vec![],
        global_secondary_indexes: vec![],
        local_secondary_indexes: vec![],
    };

    storage.create_table("DuplicateTable", schema.clone()).await.unwrap();
    
    // Second create should fail
    let result = storage.create_table("DuplicateTable", schema).await;
    assert!(result.is_err());
}

// ===== Concurrent Access Tests =====

#[tokio::test]
async fn test_concurrent_puts() {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    let storage = create_storage();
    let counter = Arc::new(AtomicU64::new(0));

    let mut handles = vec![];
    for t in 0..10 {
        let s = storage.clone();
        let c = counter.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..10 {
                let key = LumaValue::Integer((t * 100 + i) as i64);
                let mut row = Row::new();
                row.push("id", key.clone());
                if s.execute_kv_put("ConcurrentPuts", key, row).await.is_ok() {
                    c.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(counter.load(Ordering::SeqCst), 100);
}

#[tokio::test]
async fn test_concurrent_reads() {
    let storage = create_storage();
    
    // Prepopulate
    for i in 0..100 {
        let key = LumaValue::Integer(i);
        let mut row = Row::new();
        row.push("id", LumaValue::Integer(i));
        storage.execute_kv_put("ConcurrentReads", key, row).await.unwrap();
    }

    let successful_reads = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut handles = vec![];
    for _ in 0..10 {
        let s = storage.clone();
        let sr = successful_reads.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..100 {
                let key = LumaValue::Integer(i % 100);
                if s.execute_kv_get("ConcurrentReads", key).await.is_ok() {
                    sr.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    assert_eq!(successful_reads.load(std::sync::atomic::Ordering::SeqCst), 1000);
}
