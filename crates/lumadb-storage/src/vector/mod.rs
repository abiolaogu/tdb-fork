//! Vector similarity search using HNSW

use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;
use rand::Rng;

use lumadb_common::types::{DistanceMetric, VectorSearchResult};

/// HNSW-based vector index
pub struct VectorIndex {
    /// Number of dimensions
    dimensions: usize,
    /// Distance metric
    metric: DistanceMetric,
    /// Vectors stored by ID
    vectors: DashMap<String, Vec<f32>>,
    /// HNSW graph layers
    layers: RwLock<Vec<HnswLayer>>,
    /// Entry point
    entry_point: RwLock<Option<String>>,
    /// Maximum layer
    max_layer: AtomicUsize,
    /// HNSW M parameter (max connections per layer)
    m: usize,
    /// HNSW M0 parameter (max connections at layer 0)
    m0: usize,
    /// ef_construction parameter
    ef_construction: usize,
}

struct HnswLayer {
    /// Neighbors for each node
    neighbors: DashMap<String, Vec<String>>,
}

impl HnswLayer {
    fn new() -> Self {
        Self {
            neighbors: DashMap::new(),
        }
    }
}

impl VectorIndex {
    /// Create a new vector index
    pub fn new(dimensions: usize) -> Self {
        Self::with_params(dimensions, DistanceMetric::Cosine, 16, 32, 200)
    }

    /// Create with custom parameters
    pub fn with_params(
        dimensions: usize,
        metric: DistanceMetric,
        m: usize,
        m0: usize,
        ef_construction: usize,
    ) -> Self {
        Self {
            dimensions,
            metric,
            vectors: DashMap::new(),
            layers: RwLock::new(vec![HnswLayer::new()]),
            entry_point: RwLock::new(None),
            max_layer: AtomicUsize::new(0),
            m,
            m0,
            ef_construction,
        }
    }

    /// Insert a vector
    pub fn insert(&self, id: String, vector: Vec<f32>) -> Result<(), String> {
        if vector.len() != self.dimensions {
            return Err(format!(
                "Vector dimension mismatch: expected {}, got {}",
                self.dimensions,
                vector.len()
            ));
        }

        // Normalize if using cosine similarity
        let vector = if self.metric == DistanceMetric::Cosine {
            self.normalize(&vector)
        } else {
            vector
        };

        // Store vector
        self.vectors.insert(id.clone(), vector.clone());

        // Determine random level for this node
        let level = self.random_level();

        // Ensure we have enough layers
        {
            let mut layers = self.layers.write();
            while layers.len() <= level {
                layers.push(HnswLayer::new());
            }
        }

        // Update max_layer if needed
        let current_max = self.max_layer.load(Ordering::SeqCst);
        if level > current_max {
            self.max_layer.store(level, Ordering::SeqCst);
        }

        // Insert into HNSW structure
        let entry_point = self.entry_point.read().clone();

        if entry_point.is_none() {
            // First node
            *self.entry_point.write() = Some(id.clone());
            return Ok(());
        }

        let entry = entry_point.unwrap();

        // Find neighbors at each layer
        let layers = self.layers.read();
        for l in (0..=level.min(layers.len() - 1)).rev() {
            let m_max = if l == 0 { self.m0 } else { self.m };

            // Find ef_construction nearest neighbors at layer l
            let neighbors = self.search_layer(&entry, &vector, self.ef_construction, l);

            // Select M best neighbors
            let selected: Vec<_> = neighbors.into_iter().take(m_max).collect();

            // Add bidirectional connections
            let layer = &layers[l];
            layer.neighbors.insert(id.clone(), selected.clone());

            for neighbor_id in &selected {
                layer
                    .neighbors
                    .entry(neighbor_id.clone())
                    .or_insert_with(Vec::new)
                    .push(id.clone());
            }
        }

        // Update entry point if this node is at a higher level
        if level > current_max {
            *self.entry_point.write() = Some(id);
        }

        Ok(())
    }

    /// Search for nearest neighbors
    pub fn search(&self, query: &[f32], k: usize, ef: usize) -> Vec<VectorSearchResult> {
        if query.len() != self.dimensions {
            return vec![];
        }

        // Normalize if using cosine
        let query = if self.metric == DistanceMetric::Cosine {
            self.normalize(query)
        } else {
            query.to_vec()
        };

        let entry_point = self.entry_point.read().clone();
        if entry_point.is_none() {
            return vec![];
        }

        let entry = entry_point.unwrap();
        let max_layer = self.max_layer.load(Ordering::SeqCst);

        // Greedy search from top layer to layer 1
        let mut current_nearest = entry;
        for l in (1..=max_layer).rev() {
            let neighbors = self.search_layer(&current_nearest, &query, 1, l);
            if let Some(nearest) = neighbors.first() {
                current_nearest = nearest.clone();
            }
        }

        // Search layer 0 with ef
        let candidates = self.search_layer(&current_nearest, &query, ef.max(k), 0);

        // Return top k results
        candidates
            .into_iter()
            .take(k)
            .map(|id| {
                let vector = self.vectors.get(&id).map(|v| v.clone());
                let score = vector
                    .as_ref()
                    .map(|v| self.distance(&query, v))
                    .unwrap_or(f32::MAX);

                VectorSearchResult {
                    id,
                    score,
                    document: None,
                    vector,
                }
            })
            .collect()
    }

    /// Search within a layer
    fn search_layer(&self, entry: &str, query: &[f32], ef: usize, layer: usize) -> Vec<String> {
        let mut visited = std::collections::HashSet::new();
        let mut candidates = BinaryHeap::new();
        let mut results = BinaryHeap::new();

        // Get entry vector
        if let Some(entry_vec) = self.vectors.get(entry) {
            let dist = self.distance(query, &entry_vec);
            candidates.push(std::cmp::Reverse(OrderedFloat(dist, entry.to_string())));
            results.push(OrderedFloat(dist, entry.to_string()));
            visited.insert(entry.to_string());
        }

        let layers = self.layers.read();
        if layer >= layers.len() {
            return vec![];
        }

        while let Some(std::cmp::Reverse(OrderedFloat(dist, id))) = candidates.pop() {
            let worst = results.peek().map(|of| of.0).unwrap_or(f32::MAX);
            if dist > worst && results.len() >= ef {
                break;
            }

            // Get neighbors
            if let Some(neighbors) = layers[layer].neighbors.get(&id) {
                for neighbor in neighbors.iter() {
                    if visited.contains(neighbor) {
                        continue;
                    }
                    visited.insert(neighbor.clone());

                    if let Some(vec) = self.vectors.get(neighbor) {
                        let d = self.distance(query, &vec);

                        if results.len() < ef || d < worst {
                            candidates.push(std::cmp::Reverse(OrderedFloat(d, neighbor.clone())));
                            results.push(OrderedFloat(d, neighbor.clone()));

                            if results.len() > ef {
                                results.pop();
                            }
                        }
                    }
                }
            }
        }

        results
            .into_sorted_vec()
            .into_iter()
            .map(|of| of.1)
            .collect()
    }

    /// Compute distance between two vectors
    fn distance(&self, a: &[f32], b: &[f32]) -> f32 {
        match self.metric {
            DistanceMetric::Euclidean => {
                a.iter()
                    .zip(b.iter())
                    .map(|(x, y)| (x - y).powi(2))
                    .sum::<f32>()
                    .sqrt()
            }
            DistanceMetric::Cosine => {
                // For normalized vectors, cosine distance = 1 - dot product
                1.0 - a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
            }
            DistanceMetric::DotProduct => {
                -a.iter().zip(b.iter()).map(|(x, y)| x * y).sum::<f32>()
            }
            DistanceMetric::Manhattan => a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum(),
        }
    }

    /// Normalize a vector
    fn normalize(&self, v: &[f32]) -> Vec<f32> {
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            v.iter().map(|x| x / norm).collect()
        } else {
            v.to_vec()
        }
    }

    /// Generate random level for HNSW
    fn random_level(&self) -> usize {
        let mut rng = rand::thread_rng();
        let mut level = 0;
        let ml = 1.0 / (self.m as f64).ln();

        while rng.gen::<f64>() < ml.exp().recip() && level < 16 {
            level += 1;
        }

        level
    }

    /// Get number of vectors
    pub fn len(&self) -> usize {
        self.vectors.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.vectors.is_empty()
    }
}

/// Wrapper for ordered float comparison
#[derive(Clone)]
struct OrderedFloat(f32, String);

impl PartialEq for OrderedFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for OrderedFloat {}

impl PartialOrd for OrderedFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
    }
}
