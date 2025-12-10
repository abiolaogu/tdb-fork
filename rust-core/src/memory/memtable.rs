//! MemTable - In-memory write buffer using lock-free skip list
//!
//! Inspired by ScyllaDB's per-shard memtables and DragonflyDB's
//! lock-free data structures.

use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::types::{KeyValue, SequenceNumber};
use crate::error::Result;

/// Lock-free memtable using skip list
pub struct MemTable {
    /// Skip list for ordered key storage
    data: SkipMap<Vec<u8>, KeyValue>,
    /// Current size in bytes
    size: AtomicUsize,
    /// Maximum size before rotation
    max_size: usize,
    /// Sequence number counter
    sequence: AtomicU64,
    /// Creation timestamp
    created_at: i64,
    /// Number of entries
    count: AtomicUsize,
    /// Bloom filter for fast negative lookups
    bloom: RwLock<BloomFilter>,
}

impl MemTable {
    /// Create a new memtable with specified max size
    pub fn new(max_size: usize) -> Self {
        Self {
            data: SkipMap::new(),
            size: AtomicUsize::new(0),
            max_size,
            sequence: AtomicU64::new(0),
            created_at: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            count: AtomicUsize::new(0),
            bloom: RwLock::new(BloomFilter::new(max_size / 100)), // ~1% of entries
        }
    }

    /// Put a key-value pair. Returns true if memtable should be rotated.
    pub fn put(&self, mut kv: KeyValue) -> Result<bool> {
        let key = kv.key.clone();
        let entry_size = key.len() + kv.value.len() + 64; // Overhead estimate

        // Assign sequence number
        kv.sequence = self.sequence.fetch_add(1, Ordering::SeqCst);

        // Update bloom filter
        self.bloom.write().insert(&key);

        // Insert into skip list
        self.data.insert(key, kv);

        // Update size
        let new_size = self.size.fetch_add(entry_size, Ordering::SeqCst) + entry_size;
        self.count.fetch_add(1, Ordering::SeqCst);

        // Check if rotation needed
        Ok(new_size >= self.max_size)
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Option<KeyValue> {
        // Fast path: check bloom filter first
        if !self.bloom.read().may_contain(key) {
            return None;
        }

        // Look up in skip list
        self.data.get(key).map(|entry| entry.value().clone())
    }

    /// Delete a key (insert tombstone)
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let tombstone = KeyValue::tombstone(
            key.to_vec(),
            self.sequence.fetch_add(1, Ordering::SeqCst),
        );
        self.put(tombstone)
    }

    /// Get current size in bytes
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    /// Get entry count
    pub fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }

    /// Check if memtable is full
    pub fn is_full(&self) -> bool {
        self.size() >= self.max_size
    }

    /// Get the highest sequence number
    pub fn max_sequence(&self) -> SequenceNumber {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Iterate over all entries in order
    pub fn iter(&self) -> impl Iterator<Item = (Vec<u8>, KeyValue)> + '_ {
        self.data.iter().map(|entry| (entry.key().clone(), entry.value().clone()))
    }

    /// Get creation timestamp
    pub fn created_at(&self) -> i64 {
        self.created_at
    }

    /// Scan a key range
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Vec<KeyValue> {
        self.data
            .range(start.to_vec()..end.to_vec())
            .map(|entry| entry.value().clone())
            .collect()
    }
}

/// Simple bloom filter for fast negative lookups
struct BloomFilter {
    bits: Vec<u64>,
    num_bits: usize,
    num_hashes: usize,
}

impl BloomFilter {
    fn new(expected_entries: usize) -> Self {
        // ~1% false positive rate
        let num_bits = (expected_entries * 10).max(64);
        let num_hashes = 7;
        let num_words = (num_bits + 63) / 64;

        Self {
            bits: vec![0u64; num_words],
            num_bits,
            num_hashes,
        }
    }

    fn insert(&mut self, key: &[u8]) {
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        for i in 0..self.num_hashes {
            let bit_pos = self.get_bit_position(hash, i);
            let word = bit_pos / 64;
            let bit = bit_pos % 64;
            if word < self.bits.len() {
                self.bits[word] |= 1 << bit;
            }
        }
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        for i in 0..self.num_hashes {
            let bit_pos = self.get_bit_position(hash, i);
            let word = bit_pos / 64;
            let bit = bit_pos % 64;
            if word >= self.bits.len() || (self.bits[word] & (1 << bit)) == 0 {
                return false;
            }
        }
        true
    }

    fn get_bit_position(&self, hash: u64, i: usize) -> usize {
        // Double hashing
        let h1 = hash as usize;
        let h2 = (hash >> 32) as usize;
        (h1.wrapping_add(i.wrapping_mul(h2))) % self.num_bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memtable_put_get() {
        let memtable = MemTable::new(1024 * 1024);

        let kv = KeyValue::new(b"key1".to_vec(), b"value1".to_vec(), 0);
        memtable.put(kv).unwrap();

        let result = memtable.get(b"key1");
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, b"value1");
    }

    #[test]
    fn test_memtable_rotation() {
        let memtable = MemTable::new(100); // Small size to trigger rotation

        let kv = KeyValue::new(vec![0; 50], vec![0; 50], 0);
        let should_rotate = memtable.put(kv).unwrap();
        assert!(should_rotate);
    }

    #[test]
    fn test_bloom_filter() {
        let mut bloom = BloomFilter::new(1000);
        bloom.insert(b"test_key");

        assert!(bloom.may_contain(b"test_key"));
        // May have false positives but should work most of the time
    }
}
