use luma_core::{Database, Config, Document};
use std::collections::HashMap;
use tokio::runtime::Runtime;

#[test]
fn test_snapshot_restore() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Setup Dir 1
        let dir1 = tempfile::tempdir().unwrap();
        let mut config1 = Config::default();
        config1.data_dir = dir1.path().to_path_buf();
        let db1 = Database::new(config1).await.unwrap();
        let collection = "snapshot_test";

        // Insert Data
        let mut data = HashMap::new();
        data.insert("key".to_string(), "value".into());
        let doc = Document::with_id("doc1", data);
        db1.insert(collection, doc).await.unwrap();

        // Backup
        let snapshot_path = dir1.path().join("snapshot.bin");
        db1.backup(snapshot_path.to_str().unwrap()).await.unwrap();
        
        // Close DB1 (optional, but good practice)
        db1.close().await.unwrap();

        // Setup Dir 2 (New Node)
        let dir2 = tempfile::tempdir().unwrap();
        let mut config2 = Config::default();
        config2.data_dir = dir2.path().to_path_buf();
        let db2 = Database::new(config2).await.unwrap();

        // Restore
        db2.restore(snapshot_path.to_str().unwrap()).await.unwrap();

        // Verify Data
        let doc_restored = db2.get(collection, &"doc1".to_string()).await.unwrap();
        assert!(doc_restored.is_some());
        assert_eq!(doc_restored.unwrap().id, "doc1");
        
        db2.close().await.unwrap();
    });
}
