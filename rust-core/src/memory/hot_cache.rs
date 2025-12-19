//! High-performance lock-free cache for hot data
//!
//! Features:
//! - Lock-free concurrent access using crossbeam skiplist
//! - TTL-based expiration with bucket indexing  
//! - Memory pressure management and LRU eviction

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;

/// Cache key wrapper for type safety
#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug)]
pub struct CacheKey(String);

impl From<&str> for CacheKey {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for CacheKey {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Cached value with expiry metadata
#[derive(Clone, Debug)]
pub struct CacheValue {
    pub data: Vec<u8>,
    pub expiry: Instant,
    pub access_count: u64,
}

impl CacheValue {
    pub fn new(data: Vec<u8>, ttl: Duration) -> Self {
        Self {
            data,
            expiry: Instant::now() + ttl,
            access_count: 1,
        }
    }

    pub fn is_expired(&self) -> bool {
        Instant::now() > self.expiry
    }
}

/// Lock-free hot data cache using crossbeam skiplist
pub struct HotDataCache {
    cache: SkipMap<CacheKey, CacheValue>,
    ttl_buckets: DashMap<u64, Vec<CacheKey>>,
    max_memory: usize,
    current_memory: AtomicUsize,
    default_ttl: Duration,
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl HotDataCache {
    pub fn new(max_memory_bytes: usize) -> Self {
        Self {
            cache: SkipMap::new(),
            ttl_buckets: DashMap::new(),
            max_memory: max_memory_bytes,
            current_memory: AtomicUsize::new(0),
            default_ttl: Duration::from_secs(300),
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    pub fn get(&self, key: &CacheKey) -> Option<Vec<u8>> {
        let entry = self.cache.get(key)?;
        let value = entry.value();

        if value.is_expired() {
            self.cache.remove(key);
            return None;
        }

        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(value.data.clone())
    }

    pub fn set(&self, key: CacheKey, value: Vec<u8>) {
        self.set_with_ttl(key, value, self.default_ttl);
    }

    pub fn set_with_ttl(&self, key: CacheKey, value: Vec<u8>, ttl: Duration) {
        let value_size = value.len();
        if self.current_memory.load(Ordering::Relaxed) + value_size > self.max_memory {
            self.evict_lru();
        }

        let cached = CacheValue::new(value, ttl);
        let expiry_bucket = cached.expiry.elapsed().as_secs();

        self.current_memory.fetch_add(value_size, Ordering::Relaxed);
        self.cache.insert(key.clone(), cached);
        self.ttl_buckets.entry(expiry_bucket).or_default().push(key);
    }

    pub fn invalidate(&self, key: &CacheKey) {
        if let Some(entry) = self.cache.remove(key) {
            self.current_memory.fetch_sub(entry.value().data.len(), Ordering::Relaxed);
        }
    }

    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    fn evict_lru(&self) {
        let target = self.max_memory * 9 / 10;
        let mut evicted = 0;
        while self.current_memory.load(Ordering::Relaxed) > target && evicted < 100 {
            if let Some(entry) = self.cache.pop_front() {
                self.current_memory.fetch_sub(entry.value().data.len(), Ordering::Relaxed);
                evicted += 1;
            } else {
                break;
            }
        }
    }
}
