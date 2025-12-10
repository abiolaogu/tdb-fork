//! BlockCache - Sharded LRU cache for read performance
//!
//! Inspired by DragonflyDB's sharded design for lock contention reduction
//! and Aerospike's hybrid memory architecture.

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// High-performance sharded LRU cache
pub struct BlockCache {
    /// Sharded storage for reduced lock contention
    shards: Vec<CacheShard>,
    /// Number of shards (power of 2 for fast modulo)
    num_shards: usize,
    /// Maximum total size in bytes
    max_size: usize,
    /// Current size in bytes
    current_size: AtomicUsize,
    /// Statistics
    stats: CacheStats,
}

struct CacheShard {
    /// Data storage
    data: DashMap<Vec<u8>, CacheEntry>,
    /// LRU order tracking
    lru: Mutex<VecDeque<Vec<u8>>>,
    /// Shard size
    size: AtomicUsize,
}

#[derive(Clone)]
struct CacheEntry {
    value: Arc<Vec<u8>>,
    size: usize,
    access_count: u32,
    compressed: bool,
}

/// Cache statistics
#[derive(Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub inserts: AtomicU64,
    pub evictions: AtomicU64,
}

impl BlockCache {
    /// Create a new cache with the specified maximum size
    pub fn new(max_size: usize) -> Self {
        // Use power of 2 shards based on CPU count
        let num_shards = (std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4) * 2)
            .next_power_of_two();

        let shards = (0..num_shards)
            .map(|_| CacheShard {
                data: DashMap::new(),
                lru: Mutex::new(VecDeque::new()),
                size: AtomicUsize::new(0),
            })
            .collect();

        Self {
            shards,
            num_shards,
            max_size,
            current_size: AtomicUsize::new(0),
            stats: CacheStats::default(),
        }
    }

    /// Get the shard for a key
    fn get_shard(&self, key: &[u8]) -> &CacheShard {
        let hash = xxhash_rust::xxh3::xxh3_64(key) as usize;
        &self.shards[hash & (self.num_shards - 1)]
    }

    /// Insert a value into the cache
    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        self.insert_with_options(key, value, false)
    }

    /// Insert with compression option
    pub fn insert_with_options(&self, key: Vec<u8>, value: Vec<u8>, compressed: bool) {
        let size = key.len() + value.len();

        // Evict if necessary
        while self.current_size.load(Ordering::Relaxed) + size > self.max_size {
            if !self.evict_one() {
                break;
            }
        }

        let shard = self.get_shard(&key);
        let entry = CacheEntry {
            value: Arc::new(value),
            size,
            access_count: 1,
            compressed,
        };

        // Insert into shard
        if let Some(old) = shard.data.insert(key.clone(), entry) {
            // Update size if replacing
            let delta = size as isize - old.size as isize;
            if delta > 0 {
                self.current_size.fetch_add(delta as usize, Ordering::Relaxed);
                shard.size.fetch_add(delta as usize, Ordering::Relaxed);
            } else {
                self.current_size.fetch_sub((-delta) as usize, Ordering::Relaxed);
                shard.size.fetch_sub((-delta) as usize, Ordering::Relaxed);
            }
        } else {
            self.current_size.fetch_add(size, Ordering::Relaxed);
            shard.size.fetch_add(size, Ordering::Relaxed);

            // Add to LRU
            shard.lru.lock().push_back(key);
        }

        self.stats.inserts.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a value from the cache
    pub fn get(&self, key: &[u8]) -> Option<Arc<Vec<u8>>> {
        let shard = self.get_shard(key);

        if let Some(mut entry) = shard.data.get_mut(key) {
            entry.access_count = entry.access_count.saturating_add(1);
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Some(entry.value.clone());
        }

        self.stats.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Remove a value from the cache
    pub fn remove(&self, key: &[u8]) -> bool {
        let shard = self.get_shard(key);

        if let Some((_, entry)) = shard.data.remove(key) {
            self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
            shard.size.fetch_sub(entry.size, Ordering::Relaxed);
            return true;
        }
        false
    }

    /// Evict one entry using LRU policy
    fn evict_one(&self) -> bool {
        // Find shard with entries
        for shard in &self.shards {
            let key = {
                let mut lru = shard.lru.lock();
                lru.pop_front()
            };

            if let Some(key) = key {
                if let Some((_, entry)) = shard.data.remove(&key) {
                    self.current_size.fetch_sub(entry.size, Ordering::Relaxed);
                    shard.size.fetch_sub(entry.size, Ordering::Relaxed);
                    self.stats.evictions.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
            }
        }
        false
    }

    /// Clear the cache
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.data.clear();
            shard.lru.lock().clear();
            shard.size.store(0, Ordering::Relaxed);
        }
        self.current_size.store(0, Ordering::Relaxed);
    }

    /// Get current memory usage
    pub fn memory_usage(&self) -> usize {
        self.current_size.load(Ordering::Relaxed)
    }

    /// Get cache statistics
    pub fn stats(&self) -> (u64, u64, u64, u64) {
        (
            self.stats.hits.load(Ordering::Relaxed),
            self.stats.misses.load(Ordering::Relaxed),
            self.stats.inserts.load(Ordering::Relaxed),
            self.stats.evictions.load(Ordering::Relaxed),
        )
    }

    /// Get hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.stats.hits.load(Ordering::Relaxed);
        let misses = self.stats.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Check if cache contains key
    pub fn contains(&self, key: &[u8]) -> bool {
        let shard = self.get_shard(key);
        shard.data.contains_key(key)
    }

    /// Get number of entries
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.data.len()).sum()
    }

    /// Check if cache is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_insert_get() {
        let cache = BlockCache::new(1024 * 1024);

        cache.insert(b"key1".to_vec(), b"value1".to_vec());

        let result = cache.get(b"key1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_slice(), b"value1");
    }

    #[test]
    fn test_cache_eviction() {
        let cache = BlockCache::new(100);

        // Insert entries until eviction
        for i in 0..10 {
            let key = format!("key{}", i).into_bytes();
            let value = vec![0u8; 20];
            cache.insert(key, value);
        }

        // Should have evicted some entries
        assert!(cache.memory_usage() <= 100);
    }

    #[test]
    fn test_cache_stats() {
        let cache = BlockCache::new(1024);

        cache.insert(b"key1".to_vec(), b"value1".to_vec());
        cache.get(b"key1");
        cache.get(b"nonexistent");

        let (hits, misses, _, _) = cache.stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
    }
}
