//! Primary Index - Always in RAM
//!
//! The primary index is ALWAYS kept in RAM for O(1) lookup latency.
//! This is a key design principle from Aerospike.
//!
//! Features:
//! - Lock-free concurrent access
//! - Compact memory layout
//! - Bloom filter for negative lookups

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::hash::{Hash, Hasher};

use dashmap::DashMap;

use super::RecordLocation;

/// Compact index entry (fits in cache line)
#[repr(C, align(64))] // Cache line aligned
pub struct CompactIndexEntry {
    /// Key fingerprint for fast comparison
    pub fingerprint: u64,

    /// Storage tier (2 bits) + flags (6 bits)
    pub flags: u8,

    /// Record size (max 16MB with 256-byte granularity)
    pub size_units: u16,

    /// Generation for MVCC
    pub generation: u32,

    /// Offset in storage (48 bits = 256 TB addressable)
    pub offset_low: u32,
    pub offset_high: u16,

    /// Reserved for future use
    _reserved: [u8; 2],
}

impl CompactIndexEntry {
    const TIER_MASK: u8 = 0b0000_0011;
    const DELETED_FLAG: u8 = 0b0000_0100;
    const HOT_FLAG: u8 = 0b0000_1000;

    pub fn new(fingerprint: u64, tier: u8, offset: u64, size: u32) -> Self {
        Self {
            fingerprint,
            flags: tier & Self::TIER_MASK,
            size_units: (size / 256) as u16,
            generation: 1,
            offset_low: offset as u32,
            offset_high: (offset >> 32) as u16,
            _reserved: [0; 2],
        }
    }

    pub fn offset(&self) -> u64 {
        self.offset_low as u64 | ((self.offset_high as u64) << 32)
    }

    pub fn set_offset(&mut self, offset: u64) {
        self.offset_low = offset as u32;
        self.offset_high = (offset >> 32) as u16;
    }

    pub fn size(&self) -> u32 {
        self.size_units as u32 * 256
    }

    pub fn tier(&self) -> u8 {
        self.flags & Self::TIER_MASK
    }

    pub fn set_tier(&mut self, tier: u8) {
        self.flags = (self.flags & !Self::TIER_MASK) | (tier & Self::TIER_MASK);
    }

    pub fn is_deleted(&self) -> bool {
        self.flags & Self::DELETED_FLAG != 0
    }

    pub fn set_deleted(&mut self, deleted: bool) {
        if deleted {
            self.flags |= Self::DELETED_FLAG;
        } else {
            self.flags &= !Self::DELETED_FLAG;
        }
    }

    pub fn is_hot(&self) -> bool {
        self.flags & Self::HOT_FLAG != 0
    }

    pub fn set_hot(&mut self, hot: bool) {
        if hot {
            self.flags |= Self::HOT_FLAG;
        } else {
            self.flags &= !Self::HOT_FLAG;
        }
    }
}

/// Bloom filter for fast negative lookups
pub struct BloomFilter {
    bits: Vec<AtomicU64>,
    num_hashes: usize,
    size_bits: usize,
}

impl BloomFilter {
    /// Create bloom filter with given capacity and false positive rate
    pub fn new(expected_items: usize, fp_rate: f64) -> Self {
        // Calculate optimal size and hash functions
        let size_bits = Self::optimal_size(expected_items, fp_rate);
        let num_hashes = Self::optimal_hashes(size_bits, expected_items);

        let num_words = (size_bits + 63) / 64;
        let bits = (0..num_words).map(|_| AtomicU64::new(0)).collect();

        Self {
            bits,
            num_hashes,
            size_bits,
        }
    }

    pub fn insert(&self, key: &[u8]) {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i);
            let bit_idx = hash % self.size_bits;
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;

            self.bits[word_idx].fetch_or(1 << bit_offset, Ordering::Relaxed);
        }
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        for i in 0..self.num_hashes {
            let hash = self.hash(key, i);
            let bit_idx = hash % self.size_bits;
            let word_idx = bit_idx / 64;
            let bit_offset = bit_idx % 64;

            if self.bits[word_idx].load(Ordering::Relaxed) & (1 << bit_offset) == 0 {
                return false;
            }
        }
        true
    }

    fn optimal_size(n: usize, p: f64) -> usize {
        let ln2_squared = std::f64::consts::LN_2 * std::f64::consts::LN_2;
        (-(n as f64) * p.ln() / ln2_squared).ceil() as usize
    }

    fn optimal_hashes(m: usize, n: usize) -> usize {
        ((m as f64 / n as f64) * std::f64::consts::LN_2).ceil() as usize
    }

    fn hash(&self, key: &[u8], seed: usize) -> usize {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        seed.hash(&mut hasher);
        hasher.finish() as usize
    }
}

/// Statistics for the primary index
#[derive(Debug, Default)]
pub struct IndexStats {
    pub entries: AtomicU64,
    pub deleted: AtomicU64,
    pub memory_bytes: AtomicU64,
    pub bloom_false_positives: AtomicU64,
}
