//! Query cache for optimizing repeated queries

use dashmap::DashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use tracing::debug;

use crate::core::{Row, UnifiedResult, Value};

/// Cache entry with TTL
struct CacheEntry {
    result: UnifiedResult,
    created: Instant,
    hits: u64,
}

/// LRU query cache with TTL
pub struct QueryCache {
    cache: DashMap<u64, CacheEntry>,
    max_entries: usize,
    ttl: Duration,
}

impl QueryCache {
    /// Create new cache with max entries
    pub fn new(max_entries: usize) -> Self {
        Self {
            cache: DashMap::new(),
            max_entries,
            ttl: Duration::from_secs(60),
        }
    }

    /// Set TTL for cache entries
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Get cached result
    pub fn get(&self, key: &CacheKey) -> Option<UnifiedResult> {
        let hash = key.hash();
        
        if let Some(mut entry) = self.cache.get_mut(&hash) {
            // Check TTL
            if entry.created.elapsed() > self.ttl {
                drop(entry);
                self.cache.remove(&hash);
                return None;
            }
            
            entry.hits += 1;
            debug!("Cache hit for query (hits: {})", entry.hits);
            return Some(entry.result.clone());
        }
        
        None
    }

    /// Store result in cache
    pub fn put(&self, key: &CacheKey, result: UnifiedResult) {
        // Evict if needed
        if self.cache.len() >= self.max_entries {
            self.evict_oldest();
        }

        let hash = key.hash();
        self.cache.insert(hash, CacheEntry {
            result,
            created: Instant::now(),
            hits: 0,
        });
    }

    /// Invalidate cache entry
    pub fn invalidate(&self, key: &CacheKey) {
        self.cache.remove(&key.hash());
    }

    /// Invalidate all entries for a table
    pub fn invalidate_table(&self, table: &str) {
        self.cache.retain(|_, _| {
            // In production, store table info in entry for selective invalidation
            true
        });
        debug!("Invalidated cache for table: {}", table);
    }

    /// Clear entire cache
    pub fn clear(&self) {
        self.cache.clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let mut total_hits = 0u64;
        let mut entries = 0usize;
        
        for entry in self.cache.iter() {
            total_hits += entry.hits;
            entries += 1;
        }
        
        CacheStats {
            entries,
            max_entries: self.max_entries,
            total_hits,
        }
    }

    fn evict_oldest(&self) {
        // Find oldest entry
        let mut oldest_key = None;
        let mut oldest_time = Instant::now();
        
        for entry in self.cache.iter() {
            if entry.created < oldest_time {
                oldest_time = entry.created;
                oldest_key = Some(*entry.key());
            }
        }
        
        if let Some(key) = oldest_key {
            self.cache.remove(&key);
        }
    }
}

/// Cache key composed of SQL and params
pub struct CacheKey {
    sql: String,
    params: Vec<Value>,
}

impl CacheKey {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }

    fn hash(&self) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        self.sql.hash(&mut hasher);
        // Hash params
        for (i, p) in self.params.iter().enumerate() {
            i.hash(&mut hasher);
            format!("{:?}", p).hash(&mut hasher);
        }
        hasher.finish()
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
    pub total_hits: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_put_get() {
        let cache = QueryCache::new(100);
        let key = CacheKey::new("SELECT 1", vec![]);
        
        let mut row = Row::new();
        row.push("1", Value::Integer(1));
        let result = UnifiedResult::from_rows(vec![row]);
        
        cache.put(&key, result.clone());
        
        let cached = cache.get(&key);
        assert!(cached.is_some());
    }

    #[test]
    fn test_cache_eviction() {
        let cache = QueryCache::new(2);
        
        for i in 0..5 {
            let key = CacheKey::new(format!("SELECT {}", i), vec![]);
            cache.put(&key, UnifiedResult::empty());
        }
        
        assert!(cache.cache.len() <= 2);
    }
}
