//! Main storage engine implementation

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use parking_lot::RwLock;
use sled::Db;
use tracing::{info, warn};

use serde::{Deserialize, Serialize};

use lumadb_common::config::StorageConfig;
use lumadb_common::error::{Result, StorageError};
use lumadb_common::types::{CollectionInfo, CollectionMetadata, Document};

use crate::cache::BufferPool;
use crate::fulltext::FullTextIndex;
use crate::lsm::LsmTree;
use crate::vector::VectorIndex;
use crate::wal::WriteAheadLog;

/// Vector search result from storage engine
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    /// Document ID
    pub doc_id: String,
    /// Similarity score
    pub score: f32,
    /// Document payload
    pub payload: serde_json::Value,
}

/// Main storage engine orchestrating all storage components
pub struct StorageEngine {
    /// Configuration
    config: StorageConfig,
    /// Embedded database for metadata
    db: Db,
    /// LSM trees for key-value storage
    lsm_trees: DashMap<String, Arc<LsmTree>>,
    /// Vector indexes
    vector_indexes: DashMap<String, Arc<VectorIndex>>,
    /// Full-text indexes
    fulltext_indexes: DashMap<String, Arc<FullTextIndex>>,
    /// Buffer pool for caching
    buffer_pool: Arc<BufferPool>,
    /// Write-ahead log
    wal: Option<Arc<WriteAheadLog>>,
    /// Running state
    running: Arc<RwLock<bool>>,
}

impl StorageEngine {
    /// Create a new storage engine
    pub async fn new(config: &StorageConfig) -> Result<Self> {
        info!("Initializing storage engine at {}", config.path);

        // Create storage directory if it doesn't exist
        tokio::fs::create_dir_all(&config.path).await?;

        // Open embedded database for metadata
        let db_path = Path::new(&config.path).join("metadata");
        let db = sled::open(&db_path).map_err(|e| {
            lumadb_common::error::Error::Storage(StorageError::WriteFailed(e.to_string()))
        })?;

        // Initialize buffer pool
        let buffer_pool = Arc::new(BufferPool::new(config.max_memory_bytes));

        // Initialize WAL if enabled
        let wal = if config.wal_enabled {
            let wal_path = Path::new(&config.path).join("wal");
            Some(Arc::new(WriteAheadLog::new(&wal_path).await?))
        } else {
            None
        };

        info!("Storage engine initialized successfully");

        Ok(Self {
            config: config.clone(),
            db,
            lsm_trees: DashMap::new(),
            vector_indexes: DashMap::new(),
            fulltext_indexes: DashMap::new(),
            buffer_pool,
            wal,
            running: Arc::new(RwLock::new(true)),
        })
    }

    /// Check if storage engine is ready
    pub async fn is_ready(&self) -> bool {
        *self.running.read()
    }

    /// Check if storage engine is healthy (performs basic health checks)
    pub async fn is_healthy(&self) -> bool {
        // Check if engine is running
        if !*self.running.read() {
            return false;
        }

        // Verify database is accessible by performing a simple read operation
        self.db.contains_key("__health_check__").is_ok()
    }

    /// Get or create an LSM tree for a collection
    pub fn get_or_create_lsm(&self, name: &str) -> Arc<LsmTree> {
        self.lsm_trees
            .entry(name.to_string())
            .or_insert_with(|| {
                let path = Path::new(&self.config.path)
                    .join("collections")
                    .join(name);
                Arc::new(LsmTree::new(&path, self.buffer_pool.clone()))
            })
            .clone()
    }

    /// Get or create a vector index for a collection
    pub fn get_or_create_vector_index(
        &self,
        name: &str,
        dimensions: usize,
    ) -> Arc<VectorIndex> {
        self.vector_indexes
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(VectorIndex::new(dimensions))
            })
            .clone()
    }

    /// Get or create a full-text index for a collection
    pub fn get_or_create_fulltext_index(&self, name: &str) -> Result<Arc<FullTextIndex>> {
        if let Some(idx) = self.fulltext_indexes.get(name) {
            return Ok(idx.clone());
        }

        let path = Path::new(&self.config.path)
            .join("fulltext")
            .join(name);
        let index = Arc::new(FullTextIndex::new(&path)?);
        self.fulltext_indexes.insert(name.to_string(), index.clone());
        Ok(index)
    }

    // ========================================================================
    // Collection Operations
    // ========================================================================

    /// Create a new collection
    pub async fn create_collection(&self, name: &str) -> Result<()> {
        info!("Creating collection: {}", name);

        // Check if collection already exists
        let key = format!("collection:{}", name);
        if self.db.contains_key(&key).map_err(|e| {
            lumadb_common::error::Error::Storage(StorageError::ReadFailed(e.to_string()))
        })? {
            return Err(lumadb_common::error::Error::Storage(
                StorageError::WriteFailed(format!("Collection {} already exists", name)),
            ));
        }

        // Create collection metadata
        let metadata = CollectionMetadata {
            name: name.to_string(),
            count: 0,
            size_bytes: 0,
            indexes: vec![],
            schema: None,
        };

        let value = serde_json::to_vec(&metadata)?;
        self.db.insert(&key, value).map_err(|e| {
            lumadb_common::error::Error::Storage(StorageError::WriteFailed(e.to_string()))
        })?;

        // Create LSM tree for the collection
        self.get_or_create_lsm(name);

        // Write to WAL if enabled
        if let Some(ref wal) = self.wal {
            wal.append(&format!("CREATE_COLLECTION:{}", name).into_bytes())
                .await?;
        }

        Ok(())
    }

    /// List all collections
    pub async fn list_collections(&self) -> Result<Vec<CollectionMetadata>> {
        let mut collections = Vec::new();

        for result in self.db.scan_prefix("collection:") {
            let (_, value) = result.map_err(|e| {
                lumadb_common::error::Error::Storage(StorageError::ReadFailed(e.to_string()))
            })?;
            let metadata: CollectionMetadata = serde_json::from_slice(&value)?;
            collections.push(metadata);
        }

        Ok(collections)
    }

    /// Get collection metadata
    pub async fn get_collection(&self, name: &str) -> Result<Option<CollectionMetadata>> {
        let key = format!("collection:{}", name);
        match self.db.get(&key) {
            Ok(Some(value)) => {
                let metadata: CollectionMetadata = serde_json::from_slice(&value)?;
                Ok(Some(metadata))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(lumadb_common::error::Error::Storage(
                StorageError::ReadFailed(e.to_string()),
            )),
        }
    }

    /// Delete a collection
    pub async fn delete_collection(&self, name: &str) -> Result<()> {
        info!("Deleting collection: {}", name);

        let key = format!("collection:{}", name);
        self.db.remove(&key).map_err(|e| {
            lumadb_common::error::Error::Storage(StorageError::WriteFailed(e.to_string()))
        })?;

        // Remove LSM tree
        self.lsm_trees.remove(name);

        // Remove vector index if exists
        self.vector_indexes.remove(name);

        // Remove full-text index if exists
        self.fulltext_indexes.remove(name);

        // Write to WAL
        if let Some(ref wal) = self.wal {
            wal.append(&format!("DELETE_COLLECTION:{}", name).into_bytes())
                .await?;
        }

        Ok(())
    }

    // ========================================================================
    // Document Operations
    // ========================================================================

    /// Insert a document
    pub async fn insert_document(&self, collection: &str, doc: &Document) -> Result<()> {
        let lsm = self.get_or_create_lsm(collection);

        let key = doc.id.as_bytes().to_vec();
        let value = serde_json::to_vec(doc)?;

        // Write to WAL first
        if let Some(ref wal) = self.wal {
            let entry = format!("INSERT:{}:{}", collection, doc.id);
            wal.append(&entry.into_bytes()).await?;
            wal.append(&value).await?;
        }

        // Write to LSM tree
        lsm.put(key, value)?;

        // Update collection metadata
        self.increment_collection_count(collection).await?;

        Ok(())
    }

    /// Get a document by ID
    pub async fn get_document(&self, collection: &str, id: &str) -> Result<Option<Document>> {
        let lsm = self.get_or_create_lsm(collection);

        match lsm.get(id.as_bytes())? {
            Some(value) => {
                let doc: Document = serde_json::from_slice(&value)?;
                Ok(Some(doc))
            }
            None => Ok(None),
        }
    }

    /// Delete a document
    pub async fn delete_document(&self, collection: &str, id: &str) -> Result<bool> {
        let lsm = self.get_or_create_lsm(collection);

        // Write to WAL first
        if let Some(ref wal) = self.wal {
            let entry = format!("DELETE:{}:{}", collection, id);
            wal.append(&entry.into_bytes()).await?;
        }

        let deleted = lsm.delete(id.as_bytes())?;

        if deleted {
            self.decrement_collection_count(collection).await?;
        }

        Ok(deleted)
    }

    /// Scan documents in a collection
    pub async fn scan_documents(
        &self,
        collection: &str,
        prefix: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<Document>> {
        let lsm = self.get_or_create_lsm(collection);

        let mut docs = Vec::new();
        let limit = limit.unwrap_or(1000);

        for entry in lsm.scan(prefix.unwrap_or(&[]))? {
            let (_, value) = entry?;
            let doc: Document = serde_json::from_slice(&value)?;
            docs.push(doc);

            if docs.len() >= limit {
                break;
            }
        }

        Ok(docs)
    }

    // ========================================================================
    // Vector Operations (for compatibility layer)
    // ========================================================================

    /// Get collection info with vector dimensions
    pub async fn get_collection_info(&self, name: &str) -> Result<Option<CollectionInfo>> {
        match self.get_collection(name).await? {
            Some(metadata) => {
                // Get vector index dimensions if exists
                let vector_dimensions = self.vector_indexes.get(name).map(|idx| idx.dimensions());
                Ok(Some(CollectionInfo {
                    name: metadata.name,
                    count: metadata.count,
                    size_bytes: metadata.size_bytes,
                    vector_dimensions,
                }))
            }
            None => Ok(None),
        }
    }

    /// Create a vector-enabled collection
    pub async fn create_vector_collection(&self, name: &str, dimensions: usize) -> Result<()> {
        // Create regular collection
        let _ = self.create_collection(name).await;
        // Initialize vector index
        self.get_or_create_vector_index(name, dimensions);
        Ok(())
    }

    /// Insert a document with vector embedding
    pub async fn insert_vector_document(
        &self,
        collection: &str,
        doc: &Document,
        vector: &[f32],
    ) -> Result<()> {
        // Insert document
        self.insert_document(collection, doc).await?;

        // Index vector
        if let Some(index) = self.vector_indexes.get(collection) {
            index.insert(doc.id.clone(), vector.to_vec()).map_err(|e| {
                lumadb_common::error::Error::Storage(StorageError::WriteFailed(e))
            })?;
        }
        Ok(())
    }

    /// Vector similarity search
    pub async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<VectorSearchResult>> {
        let index = self.vector_indexes.get(collection).ok_or_else(|| {
            lumadb_common::error::Error::Storage(StorageError::ReadFailed(format!(
                "No vector index for collection: {}",
                collection
            )))
        })?;

        let results = index.search(query, k);
        let mut search_results = Vec::with_capacity(results.len());

        for (doc_id, score) in results {
            let payload = self.get_document(collection, &doc_id).await?
                .map(|d| d.data)
                .unwrap_or_default();
            search_results.push(VectorSearchResult {
                doc_id,
                score,
                payload,
            });
        }

        Ok(search_results)
    }

    /// Count documents in a collection
    pub async fn count_documents(&self, collection: &str) -> Result<usize> {
        match self.get_collection(collection).await? {
            Some(metadata) => Ok(metadata.count),
            None => Ok(0),
        }
    }

    // ========================================================================
    // Internal Helpers
    // ========================================================================

    async fn increment_collection_count(&self, name: &str) -> Result<()> {
        let key = format!("collection:{}", name);
        if let Ok(Some(value)) = self.db.get(&key) {
            let mut metadata: CollectionMetadata = serde_json::from_slice(&value)?;
            metadata.count += 1;
            let new_value = serde_json::to_vec(&metadata)?;
            self.db.insert(&key, new_value).map_err(|e| {
                lumadb_common::error::Error::Storage(StorageError::WriteFailed(e.to_string()))
            })?;
        }
        Ok(())
    }

    async fn decrement_collection_count(&self, name: &str) -> Result<()> {
        let key = format!("collection:{}", name);
        if let Ok(Some(value)) = self.db.get(&key) {
            let mut metadata: CollectionMetadata = serde_json::from_slice(&value)?;
            metadata.count = metadata.count.saturating_sub(1);
            let new_value = serde_json::to_vec(&metadata)?;
            self.db.insert(&key, new_value).map_err(|e| {
                lumadb_common::error::Error::Storage(StorageError::WriteFailed(e.to_string()))
            })?;
        }
        Ok(())
    }

    /// Shutdown the storage engine
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down storage engine...");
        *self.running.write() = false;

        // Flush WAL
        if let Some(ref wal) = self.wal {
            wal.sync().await?;
        }

        // Flush database
        self.db.flush().map_err(|e| {
            lumadb_common::error::Error::Storage(StorageError::WriteFailed(e.to_string()))
        })?;

        info!("Storage engine shutdown complete");
        Ok(())
    }
}
