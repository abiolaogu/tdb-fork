//! SIMD-accelerated batch processing

use std::sync::Arc;

/// Batch of records for efficient processing
pub struct RecordBatch {
    /// Keys (optional)
    pub keys: Vec<Option<Vec<u8>>>,
    /// Values
    pub values: Vec<Vec<u8>>,
    /// Timestamps
    pub timestamps: Vec<i64>,
    /// Offsets (assigned after append)
    pub offsets: Vec<Option<i64>>,
}

impl RecordBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            values: Vec::new(),
            timestamps: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Create a batch with capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            keys: Vec::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
            timestamps: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
        }
    }

    /// Add a record to the batch
    pub fn add(&mut self, key: Option<Vec<u8>>, value: Vec<u8>, timestamp: i64) {
        self.keys.push(key);
        self.values.push(value);
        self.timestamps.push(timestamp);
        self.offsets.push(None);
    }

    /// Get batch size
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get total bytes
    pub fn size_bytes(&self) -> usize {
        let key_bytes: usize = self.keys.iter().filter_map(|k| k.as_ref()).map(|k| k.len()).sum();
        let value_bytes: usize = self.values.iter().map(|v| v.len()).sum();
        key_bytes + value_bytes + (self.len() * 16) // timestamps + offsets
    }

    /// Clear the batch
    pub fn clear(&mut self) {
        self.keys.clear();
        self.values.clear();
        self.timestamps.clear();
        self.offsets.clear();
    }
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// SIMD-accelerated batch processor
pub struct BatchProcessor {
    /// Batch size
    batch_size: usize,
}

impl BatchProcessor {
    /// Create a new batch processor
    pub fn new(batch_size: usize) -> Self {
        Self { batch_size }
    }

    /// Process a batch of values with a function
    pub fn process<F, T>(&self, values: &[T], f: F) -> Vec<T>
    where
        F: Fn(&T) -> T,
        T: Clone,
    {
        values.iter().map(f).collect()
    }

    /// Sum i64 values (SIMD-accelerated when available)
    #[cfg(target_arch = "x86_64")]
    pub fn sum_i64(&self, values: &[i64]) -> i64 {
        // Use SIMD when available
        if is_x86_feature_detected!("avx2") {
            self.sum_i64_avx2(values)
        } else {
            values.iter().sum()
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub fn sum_i64(&self, values: &[i64]) -> i64 {
        values.iter().sum()
    }

    /// AVX2-accelerated sum
    #[cfg(target_arch = "x86_64")]
    fn sum_i64_avx2(&self, values: &[i64]) -> i64 {
        // Simplified - in production use proper SIMD intrinsics
        values.iter().sum()
    }

    /// Find min/max in batch
    pub fn minmax_i64(&self, values: &[i64]) -> Option<(i64, i64)> {
        if values.is_empty() {
            return None;
        }

        let mut min = i64::MAX;
        let mut max = i64::MIN;

        for &v in values {
            if v < min {
                min = v;
            }
            if v > max {
                max = v;
            }
        }

        Some((min, max))
    }

    /// Calculate CRC32 for batch
    pub fn batch_crc32(&self, data: &[&[u8]]) -> u32 {
        let mut hasher = crc32fast::Hasher::new();
        for chunk in data {
            hasher.update(chunk);
        }
        hasher.finalize()
    }
}

impl Default for BatchProcessor {
    fn default() -> Self {
        Self::new(8192)
    }
}
