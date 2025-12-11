use crate::error::{LumaError, Result};
use crate::types::DocumentId;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// Trait for Vector Index implementations (FAISS, HNSW, BruteForce)
pub trait VectorIndex: Send + Sync {
    /// Add a vector to the index
    fn add(&mut self, id: DocumentId, vector: Vec<f32>);
    
    /// Search for k nearest neighbors
    fn search(&self, query: &[f32], k: usize) -> Vec<(DocumentId, f32)>;
    
    /// Save index to disk
    fn save(&self, path: &Path) -> Result<()>;
    
    /// Load index from disk
    fn load(&mut self, path: &Path) -> Result<()>;
    
    /// Count of vectors
    fn len(&self) -> usize;
}

/// Simple Brute-Force Vector Index (for prototype)
/// Uses Cosine Similarity
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SimpleVectorIndex {
    vectors: Vec<(DocumentId, Vec<f32>)>,
    dimension: usize,
}

impl SimpleVectorIndex {
    pub fn new(dimension: usize) -> Self {
        Self {
            vectors: Vec::new(),
            dimension,
        }
    }

    fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
        if a.len() != b.len() {
            return 0.0;
        }
        let dot_product: f32 = a.iter().zip(b).map(|(x, y)| x * y).sum();
        let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
        let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            0.0
        } else {
            dot_product / (norm_a * norm_b)
        }
    }
}

impl VectorIndex for SimpleVectorIndex {
    fn add(&mut self, id: DocumentId, vector: Vec<f32>) {
        if self.dimension == 0 && !vector.is_empty() {
             self.dimension = vector.len();
        }
        // Basic dimensionality check could go here
        self.vectors.push((id, vector));
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<(DocumentId, f32)> {
        if self.vectors.is_empty() {
            return vec![];
        }

        let mut scores: Vec<(DocumentId, f32)> = self.vectors
            .iter()
            .map(|(id, vec)| (id.clone(), Self::cosine_similarity(query, vec)))
            .collect();

        // Sort by score descending
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.truncate(k);
        scores
    }

    fn save(&self, path: &Path) -> Result<()> {
        let file = File::create(path).map_err(|e| LumaError::Io(e))?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, self).map_err(|e| LumaError::Internal(format!("Serialization error: {}", e)))?;
        Ok(())
    }

    fn load(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path).map_err(|e| LumaError::Io(e))?;
        let reader = BufReader::new(file);
        let loaded: SimpleVectorIndex = bincode::deserialize_from(reader).map_err(|e| LumaError::Internal(format!("Deserialization error: {}", e)))?;
        self.vectors = loaded.vectors;
        self.dimension = loaded.dimension;
        Ok(())
    }
    
    fn len(&self) -> usize {
        self.vectors.len()
    }
}
