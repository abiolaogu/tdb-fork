//! Memory management for TDB+
//!
//! Implements high-performance in-memory data structures:
//! - MemTable: Lock-free skip list for writes
//! - BlockCache: LRU cache with sharding for reads
//!
//! Inspired by DragonflyDB's shared-nothing architecture

mod memtable;
mod cache;
mod arena;
pub mod hot_cache;

pub use memtable::MemTable;
pub use cache::BlockCache;
pub use arena::Arena;
pub use hot_cache::HotDataCache;

use crate::types::KeyValue;
use crate::error::Result;

/// Memory manager for coordinating memtables and caches
pub struct MemoryManager {
    /// Active memtable for writes
    active: MemTable,
    /// Immutable memtables pending flush
    immutable: Vec<MemTable>,
    /// Block cache for reads
    cache: BlockCache,
    /// Maximum memtable size
    max_memtable_size: usize,
    /// Maximum number of immutable memtables
    max_immutable: usize,
}

impl MemoryManager {
    pub fn new(memtable_size: usize, cache_size: usize, max_immutable: usize) -> Self {
        Self {
            active: MemTable::new(memtable_size),
            immutable: Vec::new(),
            cache: BlockCache::new(cache_size),
            max_memtable_size: memtable_size,
            max_immutable,
        }
    }

    /// Put a key-value pair
    pub fn put(&mut self, kv: KeyValue) -> Result<bool> {
        let rotated = self.active.put(kv)?;
        if rotated {
            self.rotate_memtable();
        }
        Ok(rotated)
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Option<KeyValue> {
        // Check active memtable first
        if let Some(kv) = self.active.get(key) {
            return Some(kv);
        }

        // Check immutable memtables (newest first)
        for memtable in self.immutable.iter().rev() {
            if let Some(kv) = memtable.get(key) {
                return Some(kv);
            }
        }

        None
    }

    /// Rotate active memtable to immutable
    fn rotate_memtable(&mut self) {
        let old = std::mem::replace(
            &mut self.active,
            MemTable::new(self.max_memtable_size),
        );
        self.immutable.push(old);
    }

    /// Check if flush is needed
    pub fn needs_flush(&self) -> bool {
        self.immutable.len() >= self.max_immutable
    }

    /// Get immutable memtables for flushing
    pub fn get_immutable_for_flush(&mut self) -> Option<MemTable> {
        if self.immutable.is_empty() {
            None
        } else {
            Some(self.immutable.remove(0))
        }
    }

    /// Get cache reference
    pub fn cache(&self) -> &BlockCache {
        &self.cache
    }

    /// Get mutable cache reference
    pub fn cache_mut(&mut self) -> &mut BlockCache {
        &mut self.cache
    }

    /// Get approximate memory usage
    pub fn memory_usage(&self) -> usize {
        let mut total = self.active.size();
        for memtable in &self.immutable {
            total += memtable.size();
        }
        total += self.cache.memory_usage();
        total
    }
}
