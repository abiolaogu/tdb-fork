//! Query engine implementation

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{info, debug};

use lumadb_common::config::QueryConfig;
use lumadb_common::error::{Result, Error, QueryError};
use lumadb_common::types::{QueryPlan, Document, CollectionMetadata, VectorSearchResult};
use lumadb_storage::StorageEngine;

use crate::parser::Parser;
use crate::analyzer::Analyzer;
use crate::optimizer::Optimizer;
use crate::executor::Executor;
use crate::QueryResult;

/// Main query engine
pub struct QueryEngine {
    /// Configuration
    config: QueryConfig,
    /// Storage engine
    storage: Arc<StorageEngine>,
    /// Query cache
    cache: DashMap<u64, CachedResult>,
    /// Parser
    parser: Parser,
    /// Analyzer
    analyzer: Analyzer,
    /// Optimizer
    optimizer: Optimizer,
    /// Executor
    executor: Executor,
    /// Ready state
    ready: Arc<RwLock<bool>>,
}

struct CachedResult {
    result: QueryResult,
    timestamp: i64,
}

impl QueryEngine {
    /// Create a new query engine
    pub async fn new(config: &QueryConfig, storage: Arc<StorageEngine>) -> Result<Self> {
        info!("Initializing query engine");

        Ok(Self {
            config: config.clone(),
            storage: storage.clone(),
            cache: DashMap::new(),
            parser: Parser::new(),
            analyzer: Analyzer::new(storage.clone()),
            optimizer: Optimizer::new(),
            executor: Executor::new(storage),
            ready: Arc::new(RwLock::new(true)),
        })
    }

    /// Check if engine is ready
    pub async fn is_ready(&self) -> bool {
        *self.ready.read()
    }

    /// Execute a query
    pub async fn execute(
        &self,
        query: &str,
        params: &[serde_json::Value],
    ) -> Result<QueryResult> {
        debug!("Executing query: {}", query);

        // Check cache
        let cache_key = self.cache_key(query, params);
        if self.config.cache_enabled {
            if let Some(cached) = self.cache.get(&cache_key) {
                let age = chrono::Utc::now().timestamp_millis() - cached.timestamp;
                if age < 60000 {
                    // 1 minute TTL
                    let mut result = cached.result.clone();
                    result.cached = true;
                    lumadb_common::metrics::record_cache_access(true);
                    return Ok(result);
                }
            }
            lumadb_common::metrics::record_cache_access(false);
        }

        // Parse
        let ast = self.parser.parse(query)?;

        // Analyze
        let analyzed = self.analyzer.analyze(&ast)?;

        // Optimize
        let plan = self.optimizer.optimize(&analyzed)?;

        // Execute
        let result = self.executor.execute(&plan, params).await?;

        // Cache result
        if self.config.cache_enabled {
            self.cache.insert(
                cache_key,
                CachedResult {
                    result: result.clone(),
                    timestamp: chrono::Utc::now().timestamp_millis(),
                },
            );

            // Evict old entries if cache is too large
            if self.cache.len() > self.config.cache_size {
                self.evict_cache();
            }
        }

        // Record metrics
        lumadb_common::metrics::record_query("execute", true);

        Ok(result)
    }

    /// Explain a query plan
    pub async fn explain(&self, query: &str) -> Result<QueryPlan> {
        let ast = self.parser.parse(query)?;
        let analyzed = self.analyzer.analyze(&ast)?;
        let plan = self.optimizer.optimize(&analyzed)?;

        Ok(plan.to_query_plan())
    }

    /// List collections
    pub async fn list_collections(&self) -> Result<Vec<CollectionMetadata>> {
        self.storage.list_collections().await
    }

    /// Create a collection
    pub async fn create_collection(
        &self,
        name: &str,
        schema: Option<&serde_json::Value>,
        options: Option<&crate::CollectionOptions>,
    ) -> Result<CollectionMetadata> {
        self.storage.create_collection(name).await?;

        Ok(CollectionMetadata {
            name: name.to_string(),
            count: 0,
            size_bytes: 0,
            indexes: vec![],
            schema: schema.cloned(),
        })
    }

    /// Insert documents
    pub async fn insert(
        &self,
        collection: &str,
        docs: &[serde_json::Value],
    ) -> Result<InsertResult> {
        let mut ids = Vec::new();

        for doc_value in docs {
            let doc = Document::new(doc_value.clone());
            ids.push(doc.id.clone());
            self.storage.insert_document(collection, &doc).await?;
        }

        Ok(InsertResult {
            inserted_count: ids.len() as u64,
            ids,
        })
    }

    /// Find documents
    pub async fn find(
        &self,
        collection: &str,
        filter: Option<&serde_json::Value>,
        limit: Option<usize>,
    ) -> Result<Vec<serde_json::Value>> {
        let docs = self.storage.scan_documents(collection, None, limit).await?;

        // Apply filter if specified
        let filtered: Vec<_> = if let Some(filter) = filter {
            docs.into_iter()
                .filter(|doc| self.matches_filter(doc, filter))
                .map(|doc| doc.data)
                .collect()
        } else {
            docs.into_iter().map(|doc| doc.data).collect()
        };

        Ok(filtered)
    }

    /// Find one document
    pub async fn find_one(
        &self,
        collection: &str,
        filter: Option<&serde_json::Value>,
    ) -> Result<Option<serde_json::Value>> {
        let results = self.find(collection, filter, Some(1)).await?;
        Ok(results.into_iter().next())
    }

    /// Update documents
    pub async fn update(
        &self,
        collection: &str,
        filter: &serde_json::Value,
        update: &serde_json::Value,
    ) -> Result<UpdateResult> {
        let docs = self.find(collection, Some(filter), None).await?;
        let mut modified_count = 0;

        for doc_value in docs {
            // Apply update
            let updated = self.apply_update(&doc_value, update);
            let id = doc_value
                .get("_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            let doc = Document::with_id(id, updated);
            self.storage.insert_document(collection, &doc).await?;
            modified_count += 1;
        }

        Ok(UpdateResult {
            matched_count: modified_count,
            modified_count,
        })
    }

    /// Delete documents
    pub async fn delete(
        &self,
        collection: &str,
        filter: &serde_json::Value,
    ) -> Result<DeleteResult> {
        let docs = self.find(collection, Some(filter), None).await?;
        let mut deleted_count = 0;

        for doc_value in docs {
            if let Some(id) = doc_value.get("_id").and_then(|v| v.as_str()) {
                if self.storage.delete_document(collection, id).await? {
                    deleted_count += 1;
                }
            }
        }

        Ok(DeleteResult { deleted_count })
    }

    /// Vector similarity search
    pub async fn vector_search(
        &self,
        collection: &str,
        vector: &[f32],
        k: usize,
        _filter: Option<&str>,
    ) -> Result<Vec<VectorSearchResult>> {
        let index = self.storage.get_or_create_vector_index(collection, vector.len());
        let results = index.search(vector, k);

        // Convert (id, score) tuples to VectorSearchResult
        let search_results = results
            .into_iter()
            .map(|(id, score)| VectorSearchResult {
                id,
                score,
                document: None,
                vector: None,
            })
            .collect();

        // TODO: Apply filter expression if specified

        Ok(search_results)
    }

    // ========================================================================
    // Helper Methods
    // ========================================================================

    fn cache_key(&self, query: &str, params: &[serde_json::Value]) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        query.hash(&mut hasher);
        for param in params {
            param.to_string().hash(&mut hasher);
        }
        hasher.finish()
    }

    fn evict_cache(&self) {
        // Remove oldest entries
        let now = chrono::Utc::now().timestamp_millis();
        self.cache.retain(|_, v| now - v.timestamp < 300000); // 5 minutes
    }

    fn matches_filter(&self, doc: &Document, filter: &serde_json::Value) -> bool {
        if let serde_json::Value::Object(filter_map) = filter {
            for (key, expected) in filter_map {
                if let Some(actual) = doc.get(key) {
                    if actual != expected {
                        return false;
                    }
                } else {
                    return false;
                }
            }
            true
        } else {
            true
        }
    }

    fn apply_update(
        &self,
        doc: &serde_json::Value,
        update: &serde_json::Value,
    ) -> serde_json::Value {
        if let (serde_json::Value::Object(mut doc_map), serde_json::Value::Object(update_map)) =
            (doc.clone(), update.clone())
        {
            // Handle $set operator
            if let Some(serde_json::Value::Object(set_map)) = update_map.get("$set") {
                for (key, value) in set_map {
                    doc_map.insert(key.clone(), value.clone());
                }
            }

            // Handle $unset operator
            if let Some(serde_json::Value::Object(unset_map)) = update_map.get("$unset") {
                for key in unset_map.keys() {
                    doc_map.remove(key);
                }
            }

            // Handle direct field updates
            for (key, value) in update_map {
                if !key.starts_with('$') {
                    doc_map.insert(key, value);
                }
            }

            serde_json::Value::Object(doc_map)
        } else {
            doc.clone()
        }
    }
}

/// Options for creating a collection
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CollectionOptions {
    pub vector_dimensions: Option<usize>,
    pub enable_full_text: Option<bool>,
    pub time_series: Option<bool>,
}

/// Result of insert operation
#[derive(Debug, Clone, serde::Serialize)]
pub struct InsertResult {
    pub inserted_count: u64,
    pub ids: Vec<String>,
}

/// Result of update operation
#[derive(Debug, Clone, serde::Serialize)]
pub struct UpdateResult {
    pub matched_count: u64,
    pub modified_count: u64,
}

/// Result of delete operation
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeleteResult {
    pub deleted_count: u64,
}
