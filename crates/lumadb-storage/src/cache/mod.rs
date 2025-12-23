//! Buffer pool and caching layer

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;

/// LRU-based buffer pool for caching data
pub struct BufferPool {
    /// Cache storage
    cache: DashMap<u64, CacheEntry>,
    /// Maximum size in bytes
    max_size: usize,
    /// Current size in bytes
    current_size: AtomicUsize,
    /// Hit count
    hits: AtomicUsize,
    /// Miss count
    misses: AtomicUsize,
}

struct CacheEntry {
    value: Vec<u8>,
    access_count: AtomicUsize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: DashMap::new(),
            max_size,
            current_size: AtomicUsize::new(0),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    /// Get a value from the cache
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let hash = self.hash_key(key);

        if let Some(entry) = self.cache.get(&hash) {
            entry.access_count.fetch_add(1, Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.value.clone());
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Put a value into the cache
    pub fn put(&self, key: &[u8], value: &[u8]) {
        let hash = self.hash_key(key);
        let size = value.len();

        // Check if we need to evict
        while self.current_size.load(Ordering::Relaxed) + size > self.max_size {
            self.evict_one();
        }

        let entry = CacheEntry {
            value: value.to_vec(),
            access_count: AtomicUsize::new(1),
        };

        if let Some(old) = self.cache.insert(hash, entry) {
            self.current_size.fetch_sub(old.value.len(), Ordering::Relaxed);
        }

        self.current_size.fetch_add(size, Ordering::Relaxed);
    }

    /// Remove a value from the cache
    pub fn remove(&self, key: &[u8]) {
        let hash = self.hash_key(key);

        if let Some((_, entry)) = self.cache.remove(&hash) {
            self.current_size.fetch_sub(entry.value.len(), Ordering::Relaxed);
        }
    }

    /// Evict one entry (LFU-like)
    fn evict_one(&self) {
        let mut min_access = usize::MAX;
        let mut min_key = None;

        // Find least frequently used
        for entry in self.cache.iter() {
            let access = entry.value().access_count.load(Ordering::Relaxed);
            if access < min_access {
                min_access = access;
                min_key = Some(*entry.key());
            }
        }

        if let Some(key) = min_key {
            if let Some((_, entry)) = self.cache.remove(&key) {
                self.current_size.fetch_sub(entry.value.len(), Ordering::Relaxed);
            }
        }
    }

    /// Hash a key to u64
    fn hash_key(&self, key: &[u8]) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;

        CacheStats {
            size: self.current_size.load(Ordering::Relaxed),
            max_size: self.max_size,
            entries: self.cache.len(),
            hits,
            misses,
            hit_rate: if total > 0 {
                hits as f64 / total as f64
            } else {
                0.0
            },
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.clear();
        self.current_size.store(0, Ordering::Relaxed);
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub max_size: usize,
    pub entries: usize,
    pub hits: usize,
    pub misses: usize,
    pub hit_rate: f64,
}
