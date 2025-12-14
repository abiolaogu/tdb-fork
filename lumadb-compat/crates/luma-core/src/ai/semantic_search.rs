//! AI-Powered Semantic Search for Elasticsearch
//! Provides vector embeddings and semantic similarity search

use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{info, debug};

/// Vector embedding
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Embedding {
    pub vector: Vec<f32>,
    pub model: String,
    pub dimensions: usize,
}

impl Embedding {
    pub fn new(vector: Vec<f32>, model: &str) -> Self {
        let dimensions = vector.len();
        Self {
            vector,
            model: model.to_string(),
            dimensions,
        }
    }

    /// Cosine similarity
    pub fn cosine_similarity(&self, other: &Embedding) -> f32 {
        if self.vector.len() != other.vector.len() {
            return 0.0;
        }

        let dot: f32 = self.vector.iter().zip(&other.vector).map(|(a, b)| a * b).sum();
        let norm_a: f32 = self.vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = other.vector.iter().map(|x| x * x).sum::<f32>().sqrt();

        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }

        dot / (norm_a * norm_b)
    }

    /// Euclidean distance
    pub fn euclidean_distance(&self, other: &Embedding) -> f32 {
        if self.vector.len() != other.vector.len() {
            return f32::MAX;
        }

        self.vector.iter()
            .zip(&other.vector)
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    /// Dot product
    pub fn dot_product(&self, other: &Embedding) -> f32 {
        self.vector.iter().zip(&other.vector).map(|(a, b)| a * b).sum()
    }
}

/// Semantic search engine
pub struct SemanticSearch {
    embeddings_cache: Arc<RwLock<std::collections::HashMap<String, Embedding>>>,
    model: String,
}

impl SemanticSearch {
    pub fn new(model: &str) -> Self {
        Self {
            embeddings_cache: Arc::new(RwLock::new(std::collections::HashMap::new())),
            model: model.to_string(),
        }
    }

    /// Generate embedding for text (mock implementation - replace with real LLM)
    pub async fn embed(&self, text: &str) -> Result<Embedding, String> {
        // Check cache first
        let cache = self.embeddings_cache.read().await;
        if let Some(cached) = cache.get(text) {
            return Ok(cached.clone());
        }
        drop(cache);

        // Generate embedding (mock - using simple hash-based approach)
        let vector = self.generate_mock_embedding(text);
        let embedding = Embedding::new(vector, &self.model);

        // Cache it
        let mut cache = self.embeddings_cache.write().await;
        cache.insert(text.to_string(), embedding.clone());

        Ok(embedding)
    }

    /// Generate mock embedding from text
    fn generate_mock_embedding(&self, text: &str) -> Vec<f32> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let dimensions = 384; // Common model dimension
        let mut vector = vec![0.0f32; dimensions];

        // Generate pseudo-random vector based on text
        let words: Vec<&str> = text.split_whitespace().collect();
        for (i, word) in words.iter().enumerate() {
            let mut hasher = DefaultHasher::new();
            word.to_lowercase().hash(&mut hasher);
            let hash = hasher.finish();

            for j in 0..dimensions {
                let idx = (hash as usize + i * 17 + j) % dimensions;
                vector[idx] += ((hash >> (j % 64)) & 1) as f32 * 0.1;
            }
        }

        // Normalize
        let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut vector {
                *v /= norm;
            }
        }

        vector
    }

    /// Semantic search
    pub async fn search(
        &self,
        query: &str,
        documents: &[(String, String)], // (id, text)
        top_k: usize,
    ) -> Result<Vec<SemanticSearchResult>, String> {
        let query_embedding = self.embed(query).await?;

        let mut results: Vec<SemanticSearchResult> = Vec::new();

        for (id, text) in documents {
            let doc_embedding = self.embed(text).await?;
            let score = query_embedding.cosine_similarity(&doc_embedding);

            results.push(SemanticSearchResult {
                id: id.clone(),
                score,
                text: text.clone(),
            });
        }

        // Sort by score descending
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(top_k);

        Ok(results)
    }

    /// Hybrid search (combine BM25 + semantic)
    pub async fn hybrid_search(
        &self,
        query: &str,
        documents: &[(String, String)],
        bm25_scores: &std::collections::HashMap<String, f32>,
        alpha: f32, // Weight for semantic (1-alpha for BM25)
        top_k: usize,
    ) -> Result<Vec<SemanticSearchResult>, String> {
        let semantic_results = self.search(query, documents, documents.len()).await?;

        let mut hybrid_results: Vec<SemanticSearchResult> = semantic_results.iter()
            .map(|r| {
                let bm25_score = bm25_scores.get(&r.id).copied().unwrap_or(0.0);
                SemanticSearchResult {
                    id: r.id.clone(),
                    score: alpha * r.score + (1.0 - alpha) * bm25_score,
                    text: r.text.clone(),
                }
            })
            .collect();

        hybrid_results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        hybrid_results.truncate(top_k);

        Ok(hybrid_results)
    }

    /// kNN search
    pub async fn knn_search(
        &self,
        query_vector: &[f32],
        indexed_vectors: &[(String, Vec<f32>)],
        k: usize,
    ) -> Vec<(String, f32)> {
        let query_embedding = Embedding::new(query_vector.to_vec(), "custom");

        let mut scores: Vec<(String, f32)> = indexed_vectors.iter()
            .map(|(id, vec)| {
                let doc_embedding = Embedding::new(vec.clone(), "custom");
                let score = query_embedding.cosine_similarity(&doc_embedding);
                (id.clone(), score)
            })
            .collect();

        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(k);

        scores
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticSearchResult {
    pub id: String,
    pub score: f32,
    pub text: String,
}

/// AI Query Understanding
pub struct QueryUnderstanding {
    semantic_search: SemanticSearch,
}

impl QueryUnderstanding {
    pub fn new() -> Self {
        Self {
            semantic_search: SemanticSearch::new("lumadb-embed-v1"),
        }
    }

    /// Expand query with synonyms/related terms
    pub fn expand_query(&self, query: &str) -> Vec<String> {
        let mut expansions = vec![query.to_string()];
        
        // Simple synonym expansion
        let synonyms: std::collections::HashMap<&str, Vec<&str>> = [
            ("error", vec!["failure", "exception", "fault"]),
            ("slow", vec!["latency", "delay", "performance"]),
            ("memory", vec!["ram", "heap", "buffer"]),
            ("cpu", vec!["processor", "core", "compute"]),
            ("disk", vec!["storage", "io", "write"]),
        ].into_iter().collect();

        for word in query.split_whitespace() {
            if let Some(syns) = synonyms.get(word.to_lowercase().as_str()) {
                for syn in syns {
                    expansions.push(query.replace(word, syn));
                }
            }
        }

        expansions
    }

    /// Extract entities from query
    pub fn extract_entities(&self, query: &str) -> Vec<Entity> {
        let mut entities = Vec::new();

        // Simple pattern matching without regex
        for word in query.split_whitespace() {
            // IP Address detection (simple check)
            if word.split('.').count() == 4 && word.split('.').all(|p| p.parse::<u8>().is_ok()) {
                entities.push(Entity {
                    text: word.to_string(),
                    entity_type: "IP_ADDRESS".to_string(),
                    start: 0,
                    end: word.len(),
                });
            }
            // Email detection
            else if word.contains('@') && word.contains('.') {
                entities.push(Entity {
                    text: word.to_string(),
                    entity_type: "EMAIL".to_string(),
                    start: 0,
                    end: word.len(),
                });
            }
            // UUID detection (36 char with dashes)
            else if word.len() == 36 && word.chars().filter(|c| *c == '-').count() == 4 {
                entities.push(Entity {
                    text: word.to_string(),
                    entity_type: "UUID".to_string(),
                    start: 0,
                    end: word.len(),
                });
            }
            // Date detection (YYYY-MM-DD)
            else if word.len() == 10 && word.chars().nth(4) == Some('-') && word.chars().nth(7) == Some('-') {
                entities.push(Entity {
                    text: word.to_string(),
                    entity_type: "DATE".to_string(),
                    start: 0,
                    end: word.len(),
                });
            }
        }

        entities
    }

    /// Suggest query corrections
    pub fn suggest_corrections(&self, query: &str) -> Vec<String> {
        // Simple typo correction using common misspellings
        let corrections: std::collections::HashMap<&str, &str> = [
            ("erorr", "error"),
            ("erro", "error"),
            ("excpetion", "exception"),
            ("latnecy", "latency"),
            ("memeory", "memory"),
        ].into_iter().collect();

        let mut suggestions = Vec::new();
        for word in query.split_whitespace() {
            if let Some(correction) = corrections.get(word.to_lowercase().as_str()) {
                suggestions.push(query.replace(word, correction));
            }
        }

        suggestions
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Entity {
    pub text: String,
    pub entity_type: String,
    pub start: usize,
    pub end: usize,
}

impl Default for QueryUnderstanding {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cosine_similarity() {
        let a = Embedding::new(vec![1.0, 0.0, 0.0], "test");
        let b = Embedding::new(vec![1.0, 0.0, 0.0], "test");
        assert!((a.cosine_similarity(&b) - 1.0).abs() < 0.001);

        let c = Embedding::new(vec![0.0, 1.0, 0.0], "test");
        assert!((a.cosine_similarity(&c) - 0.0).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_semantic_search() {
        let ss = SemanticSearch::new("test");
        let docs = vec![
            ("1".to_string(), "cpu usage high error".to_string()),
            ("2".to_string(), "memory leak detected".to_string()),
            ("3".to_string(), "network timeout".to_string()),
        ];

        let results = ss.search("cpu problem", &docs, 2).await.unwrap();
        assert_eq!(results.len(), 2);
    }
}
