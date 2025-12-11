//! Collection - Document collection management

use std::sync::Arc;
use parking_lot::RwLock;
use dashmap::DashMap;

use crate::types::{Document, DocumentId, Value};
use crate::error::{Result, LumaError};
use crate::shard::ShardManager;
use crate::wal::WriteAheadLog;

/// A collection of documents
pub struct Collection {
    name: String,
    shards: Arc<ShardManager>,
    wal: Arc<WriteAheadLog>,
    /// Secondary indexes
    indexes: DashMap<String, SecondaryIndex>,
    /// Document count
    count: std::sync::atomic::AtomicU64,
}

struct SecondaryIndex {
    field: String,
    entries: DashMap<Vec<u8>, Vec<DocumentId>>,
}

impl Collection {
    pub fn new(name: String, shards: Arc<ShardManager>, wal: Arc<WriteAheadLog>) -> Self {
        Self {
            name,
            shards,
            wal,
            indexes: DashMap::new(),
            count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Insert a document
    pub async fn insert(&self, doc: Document) -> Result<DocumentId> {
        let id = doc.id.clone();
        let key = self.doc_key(&id);

        // Check for vector field "_vector"
        let mut vector: Option<Vec<f32>> = None;
        if let Some(val) = doc.data.get("_vector") {
            if let Value::Array(arr) = val {
                let vec_data: Option<Vec<f32>> = arr.iter().map(|v| {
                    match v {
                        Value::Float(f) => Some(*f as f32),
                        Value::Int(i) => Some(*i as f32), // Handle ints too just in case
                        _ => None
                    }
                }).collect();
                vector = vec_data;
            }
        }

        let value = bincode::serialize(&doc)?;

        // Get shard for this key
        let shard = self.shards.get_shard(&key);
        shard.put(key, value).await?;

        // Index vector if present
        if let Some(vec) = vector {
            // Note: DocumentId in VectorIndex is currently String.
            shard.index_vector(id.clone(), vec)?;
        }

        // Update secondary indexes
        self.update_indexes(&id, &doc);

        self.count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        Ok(id)
    }

    /// Get a document by ID
    pub async fn get(&self, id: &DocumentId) -> Result<Option<Document>> {
        let key = self.doc_key(id);
        let shard = self.shards.get_shard(&key);

        match shard.get(&key).await? {
            Some(value) => {
                let doc: Document = bincode::deserialize(&value)?;
                if doc.is_expired() {
                    // Remove expired document
                    self.delete(id).await?;
                    Ok(None)
                } else {
                    Ok(Some(doc))
                }
            }
            None => Ok(None),
        }
    }

    /// Update a document
    pub async fn update(&self, id: &DocumentId, mut doc: Document) -> Result<bool> {
        let key = self.doc_key(id);
        let shard = self.shards.get_shard(&key);

        // Check if exists
        if shard.get(&key).await?.is_none() {
            return Ok(false);
        }

        doc.increment_revision();
        let value = bincode::serialize(&doc)?;
        shard.put(key, value).await?;

        // Update indexes
        self.update_indexes(id, &doc);

        Ok(true)
    }

    /// Delete a document
    pub async fn delete(&self, id: &DocumentId) -> Result<bool> {
        let key = self.doc_key(id);
        let shard = self.shards.get_shard(&key);

        if shard.get(&key).await?.is_some() {
            shard.delete(&key).await?;
            self.count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            // TODO: Remove from secondary indexes
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Batch insert for high throughput
    pub async fn batch_insert(&self, docs: Vec<Document>) -> Result<Vec<DocumentId>> {
        let mut ids = Vec::with_capacity(docs.len());

        for doc in docs {
            ids.push(self.insert(doc).await?);
        }

        Ok(ids)
    }

    /// Scan with predicate
    pub async fn scan<F>(&self, predicate: F) -> Result<Vec<Document>>
    where
        F: Fn(&Document) -> bool + Send + Sync,
    {
        let mut results = Vec::new();

        // Scan all shards
        for shard in self.shards.all_shards() {
            let entries = shard.scan_prefix(self.name.as_bytes()).await?;
            for (_, value) in entries {
                let doc: Document = bincode::deserialize(&value)?;
                if !doc.is_expired() && predicate(&doc) {
                    results.push(doc);
                }
            }
        }

        Ok(results)
    }

    /// Replay insert during recovery
    pub async fn replay_insert(&self, doc: Document) -> Result<()> {
        let key = self.doc_key(&doc.id);
        let value = bincode::serialize(&doc)?;
        let shard = self.shards.get_shard(&key);
        shard.put(key, value).await?;
        Ok(())
    }

    /// Replay update during recovery
    pub async fn replay_update(&self, id: &DocumentId, doc: Document) -> Result<()> {
        self.replay_insert(doc).await
    }

    /// Replay delete during recovery
    pub async fn replay_delete(&self, id: &DocumentId) -> Result<()> {
        let key = self.doc_key(id);
        let shard = self.shards.get_shard(&key);
        shard.delete(&key).await?;
        Ok(())
    }

    /// Flush collection to disk
    pub async fn flush(&self) -> Result<()> {
        for shard in self.shards.all_shards() {
            shard.flush().await?;
        }
        Ok(())
    }

    /// Compact collection
    pub async fn compact(&self) -> Result<()> {
        for shard in self.shards.all_shards() {
            shard.compact().await?;
        }
        Ok(())
    }

    /// Get document count
    pub fn count(&self) -> u64 {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Create a secondary index
    pub fn create_index(&self, name: &str, field: &str) -> Result<()> {
        self.indexes.insert(
            name.to_string(),
            SecondaryIndex {
                field: field.to_string(),
                entries: DashMap::new(),
            },
        );
        Ok(())
    }

    // ============================================================================
    // Internal
    // ============================================================================

    fn doc_key(&self, id: &DocumentId) -> Vec<u8> {
        format!("{}:{}", self.name, id).into_bytes()
    }

    fn update_indexes(&self, id: &DocumentId, doc: &Document) {
        for entry in self.indexes.iter() {
            let index = entry.value();
            if let Some(value) = doc.data.get(&index.field) {
                let key = self.value_to_bytes(value);
                index.entries
                    .entry(key)
                    .or_insert_with(Vec::new)
                    .push(id.clone());
            }
        }
    }

    fn value_to_bytes(&self, value: &Value) -> Vec<u8> {
        match value {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Int(i) => i.to_be_bytes().to_vec(),
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Bool(b) => vec![if *b { 1 } else { 0 }],
            _ => bincode::serialize(value).unwrap_or_default(),
        }
    }
}
