//! Query Plan Cache
//! LRU-based caching for compiled query plans

use std::collections::HashMap;
use std::sync::Arc;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use parking_lot::RwLock;
use tracing::{debug, info};

/// Cached query plan (simplified representation)
#[derive(Clone, Debug)]
pub struct CachedPlan {
    pub query_hash: u64,
    pub plan: QueryPlan,
    pub created_at: Instant,
    pub hit_count: u64,
    pub compile_time_us: u64,
}

/// Query plan representation
#[derive(Clone, Debug)]
pub struct QueryPlan {
    pub operations: Vec<PlanOperation>,
    pub estimated_rows: usize,
    pub estimated_cost: f64,
}

#[derive(Clone, Debug)]
pub enum PlanOperation {
    Scan { table: String, columns: Vec<String>, predicate: Option<String> },
    Filter { condition: String },
    Project { columns: Vec<String> },
    Aggregate { group_by: Vec<String>, aggregates: Vec<String> },
    Sort { columns: Vec<String>, desc: bool },
    Limit { count: usize, offset: usize },
    Join { left: Box<PlanOperation>, right: Box<PlanOperation>, condition: String },
}

/// LRU Query Plan Cache
pub struct QueryPlanCache {
    cache: Arc<RwLock<LruCache>>,
    max_size: usize,
    ttl: Duration,
    stats: Arc<RwLock<CacheStats>>,
}

struct LruCache {
    entries: HashMap<u64, CachedPlan>,
    order: Vec<u64>,
}

#[derive(Default, Debug)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub total_compile_time_saved_us: u64,
}

impl QueryPlanCache {
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(RwLock::new(LruCache {
                entries: HashMap::with_capacity(max_size),
                order: Vec::with_capacity(max_size),
            })),
            max_size,
            ttl,
            stats: Arc::new(RwLock::new(CacheStats::default())),
        }
    }
    
    /// Get cached plan if available
    pub fn get(&self, query: &str) -> Option<CachedPlan> {
        let hash = self.hash_query(query);
        
        let mut cache = self.cache.write();
        
        if let Some(plan) = cache.entries.get_mut(&hash) {
            // Check TTL
            if plan.created_at.elapsed() > self.ttl {
                cache.entries.remove(&hash);
                cache.order.retain(|h| *h != hash);
                
                let mut stats = self.stats.write();
                stats.misses += 1;
                return None;
            }
            
            // Update LRU order
            cache.order.retain(|h| *h != hash);
            cache.order.push(hash);
            
            plan.hit_count += 1;
            
            let mut stats = self.stats.write();
            stats.hits += 1;
            stats.total_compile_time_saved_us += plan.compile_time_us;
            
            debug!("Cache hit for query (hash: {})", hash);
            return Some(plan.clone());
        }
        
        let mut stats = self.stats.write();
        stats.misses += 1;
        
        None
    }
    
    /// Insert plan into cache
    pub fn insert(&self, query: &str, plan: QueryPlan, compile_time_us: u64) {
        let hash = self.hash_query(query);
        
        let mut cache = self.cache.write();
        
        // Evict if at capacity
        while cache.entries.len() >= self.max_size {
            if let Some(oldest) = cache.order.first().copied() {
                cache.entries.remove(&oldest);
                cache.order.remove(0);
                
                let mut stats = self.stats.write();
                stats.evictions += 1;
            }
        }
        
        let cached = CachedPlan {
            query_hash: hash,
            plan,
            created_at: Instant::now(),
            hit_count: 0,
            compile_time_us,
        };
        
        cache.entries.insert(hash, cached);
        cache.order.push(hash);
        
        debug!("Cached plan (hash: {}, compile_time: {}us)", hash, compile_time_us);
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let stats = self.stats.read();
        CacheStats {
            hits: stats.hits,
            misses: stats.misses,
            evictions: stats.evictions,
            total_compile_time_saved_us: stats.total_compile_time_saved_us,
        }
    }
    
    /// Get cache hit ratio
    pub fn hit_ratio(&self) -> f64 {
        let stats = self.stats.read();
        let total = stats.hits + stats.misses;
        if total == 0 {
            0.0
        } else {
            stats.hits as f64 / total as f64
        }
    }
    
    /// Clear the cache
    pub fn clear(&self) {
        let mut cache = self.cache.write();
        cache.entries.clear();
        cache.order.clear();
        info!("Query plan cache cleared");
    }
    
    /// Hash query string for cache lookup
    fn hash_query(&self, query: &str) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        let normalized = query.trim().to_lowercase();
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for QueryPlanCache {
    fn default() -> Self {
        Self::new(1000, Duration::from_secs(3600))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cache_insert_and_get() {
        let cache = QueryPlanCache::new(10, Duration::from_secs(60));
        
        let plan = QueryPlan {
            operations: vec![],
            estimated_rows: 100,
            estimated_cost: 1.0,
        };
        
        cache.insert("SELECT * FROM test", plan.clone(), 500);
        
        let cached = cache.get("SELECT * FROM test").unwrap();
        assert_eq!(cached.hit_count, 1);
        assert_eq!(cached.compile_time_us, 500);
    }
    
    #[test]
    fn test_cache_eviction() {
        let cache = QueryPlanCache::new(2, Duration::from_secs(60));
        
        let plan = QueryPlan {
            operations: vec![],
            estimated_rows: 100,
            estimated_cost: 1.0,
        };
        
        cache.insert("query1", plan.clone(), 100);
        cache.insert("query2", plan.clone(), 100);
        cache.insert("query3", plan.clone(), 100);
        
        // query1 should be evicted
        assert!(cache.get("query1").is_none());
        assert!(cache.get("query2").is_some());
        assert!(cache.get("query3").is_some());
    }
}
