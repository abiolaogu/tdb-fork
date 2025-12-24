//! Main migration tool implementation

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use lumadb_storage::StorageEngine;
use lumadb_common::types::Document;

use super::sources::{MigrationSource, SourceConfig};

/// Migration tool for importing data from other vector databases
pub struct MigrationTool {
    storage: Arc<StorageEngine>,
    config: MigrationConfig,
}

/// Migration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationConfig {
    /// Batch size for imports
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Number of concurrent workers
    #[serde(default = "default_workers")]
    pub workers: usize,

    /// Whether to create collections if they don't exist
    #[serde(default = "default_true")]
    pub create_collections: bool,

    /// Whether to overwrite existing documents
    #[serde(default)]
    pub overwrite: bool,

    /// Vector field name in source documents
    #[serde(default = "default_vector_field")]
    pub vector_field: String,

    /// Target collection name (optional, uses source name if not specified)
    pub target_collection: Option<String>,
}

fn default_batch_size() -> usize { 1000 }
fn default_workers() -> usize { 4 }
fn default_true() -> bool { true }
fn default_vector_field() -> String { "vector".to_string() }

impl Default for MigrationConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            workers: default_workers(),
            create_collections: true,
            overwrite: false,
            vector_field: default_vector_field(),
            target_collection: None,
        }
    }
}

/// Migration progress tracking (not Clone due to AtomicU64)
#[derive(Debug)]
pub struct MigrationProgress {
    pub total_documents: AtomicU64,
    pub imported_documents: AtomicU64,
    pub failed_documents: AtomicU64,
}

impl MigrationProgress {
    pub fn new() -> Self {
        Self {
            total_documents: AtomicU64::new(0),
            imported_documents: AtomicU64::new(0),
            failed_documents: AtomicU64::new(0),
        }
    }

    pub fn get_stats(&self) -> MigrationStats {
        MigrationStats {
            total_documents: self.total_documents.load(Ordering::Relaxed),
            imported_documents: self.imported_documents.load(Ordering::Relaxed),
            failed_documents: self.failed_documents.load(Ordering::Relaxed),
        }
    }
}

impl Default for MigrationProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationStats {
    pub total_documents: u64,
    pub imported_documents: u64,
    pub failed_documents: u64,
}

/// Document batch for migration
#[derive(Debug, Clone)]
pub struct DocumentBatch {
    pub collection: String,
    pub documents: Vec<Document>,
    pub vectors: Vec<(String, Vec<f32>)>, // (doc_id, vector)
}

impl MigrationTool {
    /// Create a new migration tool
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            config: MigrationConfig::default(),
        }
    }

    /// Set migration configuration
    pub fn with_config(mut self, config: MigrationConfig) -> Self {
        self.config = config;
        self
    }

    /// Import from a JSON/JSONL file
    pub async fn import_json(&self, path: &str, collection: &str) -> crate::Result<MigrationStats> {
        let source = MigrationSource::json(path);
        self.import_from_source(source, collection).await
    }

    /// Import from Qdrant
    pub async fn import_from_qdrant(
        &self,
        url: &str,
        collection: &str,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = MigrationSource::qdrant(url, collection);
        let target_collection = target.unwrap_or(collection);
        self.import_from_source(source, target_collection).await
    }

    /// Import from Pinecone
    pub async fn import_from_pinecone(
        &self,
        api_key: &str,
        environment: &str,
        index: &str,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = MigrationSource::pinecone(api_key, environment, index);
        let target_collection = target.unwrap_or(index);
        self.import_from_source(source, target_collection).await
    }

    /// Import from MongoDB
    pub async fn import_from_mongodb(
        &self,
        connection_string: &str,
        database: &str,
        collection: &str,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = MigrationSource::mongodb(connection_string, database, collection);
        let target_collection = target.unwrap_or(collection);
        self.import_from_source(source, target_collection).await
    }

    /// Import from Weaviate
    pub async fn import_from_weaviate(
        &self,
        url: &str,
        class_name: &str,
        api_key: Option<&str>,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = if let Some(key) = api_key {
            MigrationSource::weaviate_with_key(url, class_name, key)
        } else {
            MigrationSource::weaviate(url, class_name)
        };
        let target_collection = target.unwrap_or(class_name);
        self.import_from_source(source, target_collection).await
    }

    /// Import from Milvus
    pub async fn import_from_milvus(
        &self,
        host: &str,
        port: u16,
        collection: &str,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = MigrationSource::milvus(host, port, collection);
        let target_collection = target.unwrap_or(collection);
        self.import_from_source(source, target_collection).await
    }

    /// Import from Milvus with authentication
    pub async fn import_from_milvus_with_auth(
        &self,
        host: &str,
        port: u16,
        collection: &str,
        username: &str,
        password: &str,
        use_tls: bool,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = MigrationSource::milvus_with_auth(host, port, collection, username, password, use_tls);
        let target_collection = target.unwrap_or(collection);
        self.import_from_source(source, target_collection).await
    }

    /// Import from Zilliz Cloud
    pub async fn import_from_zilliz(
        &self,
        endpoint: &str,
        collection: &str,
        api_key: &str,
        target: Option<&str>,
    ) -> crate::Result<MigrationStats> {
        let source = MigrationSource::zilliz(endpoint, collection, api_key);
        let target_collection = target.unwrap_or(collection);
        self.import_from_source(source, target_collection).await
    }

    /// Generic import from any source
    pub async fn import_from_source(
        &self,
        source: MigrationSource,
        target_collection: &str,
    ) -> crate::Result<MigrationStats> {
        info!(
            "Starting migration from {:?} to collection '{}'",
            source.source_type(),
            target_collection
        );

        let progress = Arc::new(MigrationProgress::new());

        // Create collection if needed
        if self.config.create_collections {
            let _ = self.storage.create_collection(target_collection).await;
        }

        // Stream documents from source
        let (tx, mut rx) = mpsc::channel::<DocumentBatch>(self.config.workers * 2);

        // Spawn source reader
        let source_clone = source.clone();
        let batch_size = self.config.batch_size;
        let vector_field = self.config.vector_field.clone();
        let target = target_collection.to_string();

        let reader_handle = tokio::spawn(async move {
            if let Err(e) = source_clone.stream_documents(tx, batch_size, &vector_field, &target).await {
                error!("Source reader error: {}", e);
            }
        });

        // Process batches
        let storage = self.storage.clone();
        let progress_clone = progress.clone();

        while let Some(batch) = rx.recv().await {
            let doc_count = batch.documents.len();
            progress_clone.total_documents.fetch_add(doc_count as u64, Ordering::Relaxed);

            // Insert documents (vectors are included in document data)
            for doc in &batch.documents {
                match storage.insert_document(&batch.collection, doc).await {
                    Ok(_) => {
                        progress_clone.imported_documents.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        warn!("Failed to insert document {}: {}", doc.id, e);
                        progress_clone.failed_documents.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        // Wait for reader to complete
        let _ = reader_handle.await;

        let stats = progress.get_stats();
        info!(
            "Migration complete: {} imported, {} failed out of {} total",
            stats.imported_documents,
            stats.failed_documents,
            stats.total_documents
        );

        Ok(stats)
    }

    /// Bulk import documents (vectors should be included in document data)
    pub async fn bulk_import(
        &self,
        collection: &str,
        documents: Vec<Document>,
        _vectors: Vec<(String, Vec<f32>)>, // Deprecated: vectors now stored in document data
    ) -> crate::Result<MigrationStats> {
        let mut imported = 0u64;
        let mut failed = 0u64;
        let total = documents.len() as u64;

        // Create collection if needed
        if self.config.create_collections {
            let _ = self.storage.create_collection(collection).await;
        }

        // Insert documents in batches
        for chunk in documents.chunks(self.config.batch_size) {
            for doc in chunk {
                match self.storage.insert_document(collection, doc).await {
                    Ok(_) => imported += 1,
                    Err(e) => {
                        warn!("Failed to insert document {}: {}", doc.id, e);
                        failed += 1;
                    }
                }
            }
        }

        Ok(MigrationStats {
            total_documents: total,
            imported_documents: imported,
            failed_documents: failed,
        })
    }
}
