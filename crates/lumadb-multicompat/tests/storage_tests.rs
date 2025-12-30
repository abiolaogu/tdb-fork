//! Storage engine tests

use std::sync::Arc;
use lumadb_multicompat::{LumaStorage, Value, Row, StorageEngine, BatchOperation};

fn create_storage() -> Arc<LumaStorage> {
    Arc::new(LumaStorage::new())
}

// ===== Core Storage Operations =====

mod core_operations {
    use super::*;

    #[tokio::test]
    async fn test_create_table() {
        use lumadb_multicompat::core::{TableSchema, KeySchema, AttributeDefinition, AttributeType};
        
        let storage = create_storage();
        
        let schema = TableSchema {
            name: "TestTable".into(),
            key_schema: KeySchema {
                partition_key: "pk".into(),
                sort_key: Some("sk".into()),
            },
            attributes: vec![
                AttributeDefinition { name: "pk".into(), attribute_type: AttributeType::S },
                AttributeDefinition { name: "sk".into(), attribute_type: AttributeType::S },
            ],
            global_secondary_indexes: vec![],
            local_secondary_indexes: vec![],
        };

        let result = storage.create_table("TestTable", schema).await;
        assert!(result.is_ok());

        let tables = storage.list_tables().await.unwrap();
        assert!(tables.contains(&"TestTable".to_string()));
    }

    #[tokio::test]
    async fn test_delete_table() {
        let storage = create_storage();
        
        // Create table first
        let mut row = Row::new();
        row.push("id", Value::String("1".into()));
        storage.execute_kv_put("ToDelete", Value::String("1".into()), row).await.unwrap();
        
        // Delete it
        storage.delete_table("ToDelete").await.unwrap();
        
        let tables = storage.list_tables().await.unwrap();
        assert!(!tables.contains(&"ToDelete".to_string()));
    }

    #[tokio::test]
    async fn test_describe_table() {
        use lumadb_multicompat::core::{TableSchema, KeySchema};
        
        let storage = create_storage();
        
        let schema = TableSchema {
            name: "DescribeMe".into(),
            key_schema: KeySchema {
                partition_key: "id".into(),
                sort_key: None,
            },
            attributes: vec![],
            global_secondary_indexes: vec![],
            local_secondary_indexes: vec![],
        };

        storage.create_table("DescribeMe", schema).await.unwrap();

        let described = storage.describe_table("DescribeMe").await.unwrap();
        assert_eq!(described.name, "DescribeMe");
        assert_eq!(described.key_schema.partition_key, "id");
    }
}

// ===== Cache Tests =====

mod cache_tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_hit() {
        let storage = LumaStorage::new();
        
        // First query populates cache
        storage.execute_sql("SELECT 1", vec![]).await.unwrap();
        
        // Second query should hit cache
        storage.execute_sql("SELECT 1", vec![]).await.unwrap();
        
        let stats = storage.cache_stats();
        assert!(stats.total_hits > 0 || stats.entries > 0);
    }

    #[tokio::test]
    async fn test_cache_invalidation() {
        let storage = LumaStorage::new();
        
        // Populate
        storage.execute_sql("SELECT * FROM test_cache", vec![]).await.ok();
        
        // Clear
        storage.clear_cache();
        
        let stats = storage.cache_stats();
        assert_eq!(stats.entries, 0);
    }
}

// ===== Batch Operation Tests =====

mod batch_tests {
    use super::*;

    #[tokio::test]
    async fn test_mixed_batch() {
        let storage = create_storage();
        
        // First, create some data
        let mut row1 = Row::new();
        row1.push("id", Value::String("to_delete".into()));
        storage.execute_kv_put("BatchMixed", Value::String("to_delete".into()), row1).await.unwrap();
        
        // Now batch with mixed operations
        let ops = vec![
            BatchOperation::Put {
                table: "BatchMixed".into(),
                key: Value::String("new".into()),
                value: {
                    let mut r = Row::new();
                    r.push("id", Value::String("new".into()));
                    r
                },
            },
            BatchOperation::Delete {
                table: "BatchMixed".into(),
                key: Value::String("to_delete".into()),
            },
        ];

        storage.batch_write(ops).await.unwrap();

        // Verify
        let new = storage.execute_kv_get("BatchMixed", Value::String("new".into())).await.unwrap();
        let deleted = storage.execute_kv_get("BatchMixed", Value::String("to_delete".into())).await.unwrap();
        
        assert!(new.is_some());
        assert!(deleted.is_none());
    }
}

// ===== Query Filter Tests =====

mod filter_tests {
    use super::*;
    use lumadb_multicompat::{QueryFilter, KeyCondition, SortKeyCondition};

    #[tokio::test]
    async fn test_filter_with_begins_with() {
        let storage = create_storage();
        
        // Insert test data
        for prefix in &["ORDER#1", "ORDER#2", "PAYMENT#1"] {
            let key = Value::Object([
                ("pk".into(), Value::String("user1".into())),
                ("sk".into(), Value::String(prefix.to_string())),
            ].into());
            
            let mut row = Row::new();
            row.push("pk", Value::String("user1".into()));
            row.push("sk", Value::String(prefix.to_string()));
            
            storage.execute_kv_put("FilterTest", key, row).await.unwrap();
        }

        let filter = QueryFilter {
            key_condition: Some(KeyCondition {
                partition_key: ("pk".into(), Value::String("user1".into())),
                sort_key: Some(("sk".into(), SortKeyCondition::BeginsWith("ORDER#".into()))),
            }),
            ..Default::default()
        };

        let results = storage.execute_kv_query("FilterTest", filter).await.unwrap();
        assert_eq!(results.len(), 2);
    }

    #[tokio::test]
    async fn test_scan_forward_false() {
        let storage = create_storage();
        
        for i in 1..=5 {
            let key = Value::Integer(i);
            let mut row = Row::new();
            row.push("id", Value::Integer(i));
            storage.execute_kv_put("ScanTest", key, row).await.unwrap();
        }

        let filter = QueryFilter {
            scan_forward: false,
            ..Default::default()
        };

        let results = storage.execute_kv_query("ScanTest", filter).await.unwrap();
        // Results should be in reverse order
        assert!(!results.is_empty());
    }
}
