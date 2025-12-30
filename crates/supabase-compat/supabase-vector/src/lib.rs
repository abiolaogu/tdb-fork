//! Vector Search Service for Supabase Compatibility
//!
//! Provides pgvector-compatible vector operations:
//! - Vector embedding storage
//! - Similarity search (cosine, L2, inner product)
//! - Indexing support (IVFFlat, HNSW)

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Vector dimension
pub type Vector = Vec<f32>;

/// Distance metric for similarity search
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DistanceMetric {
    Cosine,
    L2,
    InnerProduct,
}

/// A stored vector with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredVector {
    pub id: String,
    pub embedding: Vector,
    pub metadata: serde_json::Value,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl StoredVector {
    pub fn new(embedding: Vector, metadata: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            embedding,
            metadata,
            created_at: chrono::Utc::now(),
        }
    }
}

/// Search result with distance
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub id: String,
    pub distance: f32,
    pub metadata: serde_json::Value,
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexConfig {
    pub name: String,
    pub metric: DistanceMetric,
    pub dimensions: usize,
    pub index_type: IndexType,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum IndexType {
    Flat,
    IVFFlat { lists: usize },
    HNSW { m: usize, ef_construction: usize },
}

/// Vector store for embeddings
pub struct VectorStore {
    vectors: Arc<RwLock<HashMap<String, Vec<StoredVector>>>>,
    indexes: Arc<RwLock<HashMap<String, IndexConfig>>>,
}

impl VectorStore {
    pub fn new() -> Self {
        Self {
            vectors: Arc::new(RwLock::new(HashMap::new())),
            indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a vector index
    pub fn create_index(&self, config: IndexConfig) {
        self.indexes.write().insert(config.name.clone(), config);
    }

    /// Insert a vector
    pub fn insert(&self, table: &str, vector: StoredVector) {
        self.vectors
            .write()
            .entry(table.to_string())
            .or_insert_with(Vec::new)
            .push(vector);
    }

    /// Similarity search
    pub fn search(
        &self,
        table: &str,
        query: &Vector,
        metric: DistanceMetric,
        limit: usize,
    ) -> Vec<SearchResult> {
        let vectors = self.vectors.read();
        let table_vectors = match vectors.get(table) {
            Some(v) => v,
            None => return vec![],
        };

        let mut results: Vec<SearchResult> = table_vectors
            .iter()
            .map(|v| SearchResult {
                id: v.id.clone(),
                distance: compute_distance(&v.embedding, query, metric),
                metadata: v.metadata.clone(),
            })
            .collect();

        // Sort by distance (ascending for L2/cosine, descending for inner product)
        match metric {
            DistanceMetric::InnerProduct => {
                results.sort_by(|a, b| {
                    b.distance
                        .partial_cmp(&a.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
            _ => {
                results.sort_by(|a, b| {
                    a.distance
                        .partial_cmp(&b.distance)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
            }
        }

        results.truncate(limit);
        results
    }

    /// Delete a vector
    pub fn delete(&self, table: &str, id: &str) -> bool {
        let mut vectors = self.vectors.write();
        if let Some(table_vectors) = vectors.get_mut(table) {
            let initial_len = table_vectors.len();
            table_vectors.retain(|v| v.id != id);
            return table_vectors.len() < initial_len;
        }
        false
    }
}

impl Default for VectorStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Compute distance between two vectors
fn compute_distance(a: &[f32], b: &[f32], metric: DistanceMetric) -> f32 {
    match metric {
        DistanceMetric::Cosine => {
            let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
            let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
            let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
            1.0 - (dot / (norm_a * norm_b))
        }
        DistanceMetric::L2 => a
            .iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt(),
        DistanceMetric::InnerProduct => a.iter().zip(b.iter()).map(|(x, y)| x * y).sum(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_store() {
        let store = VectorStore::new();

        let v1 = StoredVector::new(vec![1.0, 0.0, 0.0], serde_json::json!({"label": "a"}));
        let v2 = StoredVector::new(vec![0.0, 1.0, 0.0], serde_json::json!({"label": "b"}));

        store.insert("embeddings", v1);
        store.insert("embeddings", v2);

        let results = store.search(
            "embeddings",
            &vec![1.0, 0.0, 0.0],
            DistanceMetric::Cosine,
            10,
        );
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].distance, 0.0); // Exact match
    }

    #[test]
    fn test_cosine_distance() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![1.0, 0.0, 0.0];
        assert!((compute_distance(&a, &b, DistanceMetric::Cosine) - 0.0).abs() < 0.001);

        let c = vec![0.0, 1.0, 0.0];
        assert!((compute_distance(&a, &c, DistanceMetric::Cosine) - 1.0).abs() < 0.001);
    }
}
