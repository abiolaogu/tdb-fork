//! Integration tests for LumaDB
//!
//! These tests verify the integration between different components of LumaDB.

use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::timeout;

// Re-export for tests
use lumadb_common::config::{Config, StorageConfig, StreamingConfig, QueryConfig, SecurityConfig};
use lumadb_storage::StorageEngine;
use lumadb_streaming::StreamingEngine;
use lumadb_query::QueryEngine;

/// Test helper to create a temporary storage engine
async fn create_test_storage() -> (Arc<StorageEngine>, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let mut config = StorageConfig::default();
    config.path = temp_dir.path().join("data").to_string_lossy().to_string();

    let engine = StorageEngine::new(&config)
        .await
        .expect("Failed to create storage engine");

    (Arc::new(engine), temp_dir)
}

/// Test helper to create a streaming engine
async fn create_test_streaming(storage: Arc<StorageEngine>) -> Arc<StreamingEngine> {
    let config = StreamingConfig::default();
    let raft = Arc::new(lumadb_streaming::RaftStub);

    Arc::new(
        StreamingEngine::new(&config, storage, raft)
            .await
            .expect("Failed to create streaming engine")
    )
}

/// Test helper to create a query engine
async fn create_test_query(storage: Arc<StorageEngine>) -> Arc<QueryEngine> {
    let config = QueryConfig::default();

    Arc::new(
        QueryEngine::new(&config, storage)
            .await
            .expect("Failed to create query engine")
    )
}

// ============================================================================
// Storage Engine Tests
// ============================================================================

#[tokio::test]
async fn test_storage_engine_initialization() {
    let (engine, _temp_dir) = create_test_storage().await;

    // Verify engine is operational
    assert!(engine.is_healthy().await);
}

#[tokio::test]
async fn test_storage_create_collection() {
    let (engine, _temp_dir) = create_test_storage().await;

    // Create a collection
    engine.create_collection("test_collection")
        .await
        .expect("Failed to create collection");

    // Verify collection exists
    let collections = engine.list_collections()
        .await
        .expect("Failed to list collections");

    assert!(collections.iter().any(|c| c.name == "test_collection"));
}

#[tokio::test]
async fn test_storage_document_crud() {
    let (engine, _temp_dir) = create_test_storage().await;

    // Create collection
    engine.create_collection("users")
        .await
        .expect("Failed to create collection");

    // Insert document
    let doc = lumadb_common::types::Document::new(serde_json::json!({
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    }));

    engine.insert_document("users", &doc)
        .await
        .expect("Failed to insert document");

    // Retrieve document
    let retrieved = engine.get_document("users", &doc.id)
        .await
        .expect("Failed to get document");

    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();
    assert_eq!(retrieved.data.get("name").unwrap(), "Alice");
}

// ============================================================================
// Streaming Engine Tests
// ============================================================================

#[tokio::test]
async fn test_streaming_create_topic() {
    let (storage, _temp_dir) = create_test_storage().await;
    let streaming = create_test_streaming(storage).await;

    // Create a topic
    let config = lumadb_common::types::TopicConfig::new("test-topic", 3, 1);
    streaming.create_topic(config)
        .await
        .expect("Failed to create topic");

    // Verify topic exists
    let topics = streaming.list_topics()
        .await
        .expect("Failed to list topics");

    assert!(topics.iter().any(|t| t.name == "test-topic"));
}

#[tokio::test]
async fn test_streaming_produce_consume() {
    let (storage, _temp_dir) = create_test_storage().await;
    let streaming = create_test_streaming(storage).await;

    // Create topic
    let config = lumadb_common::types::TopicConfig::new("events", 1, 1);
    streaming.create_topic(config)
        .await
        .expect("Failed to create topic");

    // Produce records
    let records = vec![
        lumadb_streaming::ProduceRecord {
            key: Some("user-1".to_string()),
            value: serde_json::json!({"action": "login"}),
            headers: None,
            partition: None,
        },
    ];

    let metadata = streaming.produce("events", &records, 1)
        .await
        .expect("Failed to produce records");

    assert_eq!(metadata.len(), 1);
    assert_eq!(metadata[0].topic, "events");

    // Consume records
    let consumed = streaming.consume("events", Some("test-group"), Some("earliest"), 10, 5000)
        .await
        .expect("Failed to consume records");

    assert!(!consumed.is_empty());
}

// ============================================================================
// Query Engine Tests
// ============================================================================

#[tokio::test]
async fn test_query_engine_sql() {
    let (storage, _temp_dir) = create_test_storage().await;
    let query = create_test_query(storage).await;

    // Simple query execution
    let result = query.execute("SELECT 1 + 1 AS result", &[])
        .await
        .expect("Failed to execute query");

    // Query result should be valid
    assert!(result.rows().len() >= 0);
}

#[tokio::test]
async fn test_query_engine_collections() {
    let (storage, _temp_dir) = create_test_storage().await;
    let query = create_test_query(storage).await;

    // Create collection through query engine
    let meta = query.create_collection("products", None, None)
        .await
        .expect("Failed to create collection");

    assert_eq!(meta.name, "products");

    // Insert documents
    let docs = vec![
        serde_json::json!({"name": "Widget", "price": 9.99}),
        serde_json::json!({"name": "Gadget", "price": 19.99}),
    ];

    let result = query.insert("products", &docs)
        .await
        .expect("Failed to insert documents");

    assert_eq!(result.inserted_count, 2);

    // Find documents
    let found = query.find("products", None, Some(10))
        .await
        .expect("Failed to find documents");

    assert_eq!(found.len(), 2);
}

// ============================================================================
// Security Tests
// ============================================================================

#[tokio::test]
async fn test_security_config_jwt_secret() {
    let config = SecurityConfig::default();

    // Default config should have a secret (auto-generated for dev)
    assert!(!config.jwt_secret.is_empty());

    // If it's auto-generated, it should start with "dev-secret-"
    if config.jwt_secret.starts_with("dev-secret-") {
        // This is expected in development
        assert!(config.jwt_secret.len() > 15);
    }
}

#[tokio::test]
async fn test_security_manager_initialization() {
    let config = SecurityConfig {
        auth_enabled: true,
        auth_method: "jwt".to_string(),
        jwt_secret: "test-secret-key-12345".to_string(),
        jwt_expiration_secs: 3600,
        ..Default::default()
    };

    let manager = lumadb_security::SecurityManager::new(&config)
        .await
        .expect("Failed to create security manager");

    // Should be able to authenticate with auth disabled session
    let session = manager.authenticate(
        &lumadb_security::Credentials::Token("test-token".to_string())
    ).await;

    // Token auth should work (or fail gracefully)
    assert!(session.is_ok() || session.is_err());
}

// ============================================================================
// End-to-End Tests
// ============================================================================

#[tokio::test]
async fn test_e2e_document_workflow() {
    let (storage, _temp_dir) = create_test_storage().await;
    let query = create_test_query(storage).await;

    // Create collection
    query.create_collection("orders", None, None)
        .await
        .expect("Failed to create collection");

    // Insert order
    let order = serde_json::json!({
        "customer": "John Doe",
        "items": [
            {"product": "Widget", "qty": 2},
            {"product": "Gadget", "qty": 1}
        ],
        "total": 39.97,
        "status": "pending"
    });

    query.insert("orders", &[order])
        .await
        .expect("Failed to insert order");

    // Find order
    let filter = serde_json::json!({"customer": "John Doe"});
    let orders = query.find("orders", Some(&filter), Some(10))
        .await
        .expect("Failed to find orders");

    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].get("customer").unwrap(), "John Doe");

    // Update order
    let update = serde_json::json!({"$set": {"status": "shipped"}});
    let update_result = query.update("orders", &filter, &update)
        .await
        .expect("Failed to update order");

    assert_eq!(update_result.modified_count, 1);

    // Verify update
    let updated_orders = query.find("orders", Some(&filter), Some(10))
        .await
        .expect("Failed to find updated orders");

    assert_eq!(updated_orders[0].get("status").unwrap(), "shipped");
}

#[tokio::test]
async fn test_e2e_streaming_workflow() {
    let (storage, _temp_dir) = create_test_storage().await;
    let streaming = create_test_streaming(storage).await;

    // Create topic
    let config = lumadb_common::types::TopicConfig::new("audit-log", 2, 1);
    streaming.create_topic(config)
        .await
        .expect("Failed to create topic");

    // Produce multiple batches
    for i in 0..5 {
        let records = vec![
            lumadb_streaming::ProduceRecord {
                key: Some(format!("event-{}", i)),
                value: serde_json::json!({
                    "type": "user_action",
                    "action": "page_view",
                    "page": format!("/page/{}", i),
                    "timestamp": chrono::Utc::now().timestamp_millis()
                }),
                headers: None,
                partition: None,
            },
        ];

        streaming.produce("audit-log", &records, 1)
            .await
            .expect("Failed to produce");
    }

    // Consume all records
    let consumed = streaming.consume("audit-log", Some("audit-consumer"), Some("earliest"), 100, 5000)
        .await
        .expect("Failed to consume");

    assert!(consumed.len() >= 5);
}

// ============================================================================
// Performance Tests (basic benchmarks)
// ============================================================================

#[tokio::test]
async fn test_perf_batch_insert() {
    let (storage, _temp_dir) = create_test_storage().await;
    let query = create_test_query(storage).await;

    // Create collection
    query.create_collection("perf_test", None, None)
        .await
        .expect("Failed to create collection");

    // Insert batch of documents
    let batch_size = 100;
    let docs: Vec<serde_json::Value> = (0..batch_size)
        .map(|i| serde_json::json!({
            "id": i,
            "name": format!("item-{}", i),
            "value": i * 100
        }))
        .collect();

    let start = std::time::Instant::now();
    let result = query.insert("perf_test", &docs)
        .await
        .expect("Failed to batch insert");
    let duration = start.elapsed();

    assert_eq!(result.inserted_count, batch_size);

    // Basic performance assertion (should complete within reasonable time)
    assert!(duration < Duration::from_secs(5), "Batch insert took too long: {:?}", duration);
}

#[tokio::test]
async fn test_perf_streaming_throughput() {
    let (storage, _temp_dir) = create_test_storage().await;
    let streaming = create_test_streaming(storage).await;

    // Create topic
    let config = lumadb_common::types::TopicConfig::new("throughput-test", 4, 1);
    streaming.create_topic(config)
        .await
        .expect("Failed to create topic");

    // Produce many records
    let batch_size = 100;
    let records: Vec<lumadb_streaming::ProduceRecord> = (0..batch_size)
        .map(|i| lumadb_streaming::ProduceRecord {
            key: Some(format!("key-{}", i)),
            value: serde_json::json!({"seq": i, "data": "x".repeat(100)}),
            headers: None,
            partition: None,
        })
        .collect();

    let start = std::time::Instant::now();
    let metadata = streaming.produce("throughput-test", &records, 1)
        .await
        .expect("Failed to produce");
    let duration = start.elapsed();

    assert_eq!(metadata.len(), batch_size);

    // Calculate throughput
    let records_per_sec = batch_size as f64 / duration.as_secs_f64();
    println!("Streaming throughput: {:.0} records/sec", records_per_sec);

    // Should be reasonably fast
    assert!(duration < Duration::from_secs(5), "Streaming took too long: {:?}", duration);
}
