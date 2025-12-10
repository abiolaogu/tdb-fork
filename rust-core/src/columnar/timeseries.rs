//! Time-Series Optimizations
//!
//! Specialized optimizations for time-series data:
//! - Timestamp indexing with O(1) range queries
//! - Downsampling and aggregation
//! - Delta-of-delta compression
//! - Sliding window operations

use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use parking_lot::RwLock;

use super::simd;
use crate::error::{TdbError, TdbResult};

/// Time granularity for bucketing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeGranularity {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

impl TimeGranularity {
    /// Duration in nanoseconds
    pub fn nanos(&self) -> i64 {
        match self {
            TimeGranularity::Nanosecond => 1,
            TimeGranularity::Microsecond => 1_000,
            TimeGranularity::Millisecond => 1_000_000,
            TimeGranularity::Second => 1_000_000_000,
            TimeGranularity::Minute => 60 * 1_000_000_000,
            TimeGranularity::Hour => 3600 * 1_000_000_000,
            TimeGranularity::Day => 86400 * 1_000_000_000i64,
            TimeGranularity::Week => 7 * 86400 * 1_000_000_000i64,
            TimeGranularity::Month => 30 * 86400 * 1_000_000_000i64, // Approximate
            TimeGranularity::Year => 365 * 86400 * 1_000_000_000i64, // Approximate
        }
    }

    /// Truncate timestamp to granularity
    pub fn truncate(&self, ts: i64) -> i64 {
        (ts / self.nanos()) * self.nanos()
    }
}

/// Aggregation function for downsampling
#[derive(Debug, Clone, Copy)]
pub enum AggregateFunc {
    First,
    Last,
    Min,
    Max,
    Sum,
    Avg,
    Count,
    StdDev,
    Variance,
}

/// Time-series index for fast range queries
pub struct TimeIndex {
    /// Sorted timestamps (nanoseconds since epoch)
    timestamps: Vec<i64>,

    /// B-tree index for range queries: timestamp -> position
    btree: BTreeMap<i64, usize>,

    /// Block index for large datasets: block_id -> (start_ts, end_ts, start_pos)
    blocks: Vec<BlockInfo>,

    /// Block size for indexing
    block_size: usize,
}

#[derive(Debug, Clone)]
struct BlockInfo {
    start_ts: i64,
    end_ts: i64,
    start_pos: usize,
    count: usize,
}

impl TimeIndex {
    /// Create a new time index
    pub fn new() -> Self {
        Self {
            timestamps: Vec::new(),
            btree: BTreeMap::new(),
            blocks: Vec::new(),
            block_size: 4096,
        }
    }

    /// Create with specified block size
    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            timestamps: Vec::new(),
            btree: BTreeMap::new(),
            blocks: Vec::new(),
            block_size,
        }
    }

    /// Append timestamp (must be >= last timestamp for sorted series)
    pub fn append(&mut self, ts: i64) -> usize {
        let pos = self.timestamps.len();
        self.timestamps.push(ts);

        // Update B-tree index
        self.btree.insert(ts, pos);

        // Update block index
        if pos % self.block_size == 0 {
            // Start new block
            self.blocks.push(BlockInfo {
                start_ts: ts,
                end_ts: ts,
                start_pos: pos,
                count: 1,
            });
        } else if let Some(block) = self.blocks.last_mut() {
            block.end_ts = ts;
            block.count += 1;
        }

        pos
    }

    /// Bulk append (timestamps must be sorted)
    pub fn append_bulk(&mut self, timestamps: &[i64]) {
        for &ts in timestamps {
            self.append(ts);
        }
    }

    /// Find range of positions for time range
    pub fn range(&self, start: i64, end: i64) -> (usize, usize) {
        // Use block index for initial narrowing
        let start_block = self.find_block_for_timestamp(start);
        let end_block = self.find_block_for_timestamp(end);

        let search_start = start_block.map(|b| self.blocks[b].start_pos).unwrap_or(0);
        let search_end = end_block
            .map(|b| self.blocks[b].start_pos + self.blocks[b].count)
            .unwrap_or(self.timestamps.len());

        // Binary search within narrowed range
        let start_pos = self.timestamps[search_start..search_end]
            .binary_search(&start)
            .unwrap_or_else(|x| x) + search_start;

        let end_pos = self.timestamps[search_start..search_end]
            .binary_search(&end)
            .map(|x| x + 1)
            .unwrap_or_else(|x| x) + search_start;

        (start_pos, end_pos)
    }

    /// Get positions for exact timestamp matches (O(1) average)
    pub fn get(&self, ts: i64) -> Option<usize> {
        self.btree.get(&ts).copied()
    }

    fn find_block_for_timestamp(&self, ts: i64) -> Option<usize> {
        if self.blocks.is_empty() {
            return None;
        }

        // Binary search on blocks
        let mut left = 0;
        let mut right = self.blocks.len();

        while left < right {
            let mid = left + (right - left) / 2;
            if self.blocks[mid].end_ts < ts {
                left = mid + 1;
            } else if self.blocks[mid].start_ts > ts {
                right = mid;
            } else {
                return Some(mid);
            }
        }

        if left < self.blocks.len() {
            Some(left)
        } else {
            Some(self.blocks.len() - 1)
        }
    }
}

impl Default for TimeIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// Time-series column with optimized operations
pub struct TimeSeriesColumn {
    /// Timestamps (nanoseconds since epoch)
    timestamps: Vec<i64>,

    /// Values
    values: Vec<f64>,

    /// Time index for fast range queries
    index: TimeIndex,

    /// Is data sorted by time?
    sorted: bool,
}

impl TimeSeriesColumn {
    pub fn new() -> Self {
        Self {
            timestamps: Vec::new(),
            values: Vec::new(),
            index: TimeIndex::new(),
            sorted: true,
        }
    }

    /// Append a data point
    pub fn append(&mut self, ts: i64, value: f64) {
        // Check sort order
        if !self.timestamps.is_empty() && ts < *self.timestamps.last().unwrap() {
            self.sorted = false;
        }

        self.timestamps.push(ts);
        self.values.push(value);

        if self.sorted {
            self.index.append(ts);
        }
    }

    /// Bulk append (optimized)
    pub fn append_bulk(&mut self, timestamps: &[i64], values: &[f64]) {
        debug_assert_eq!(timestamps.len(), values.len());

        self.timestamps.extend_from_slice(timestamps);
        self.values.extend_from_slice(values);

        if self.sorted {
            self.index.append_bulk(timestamps);
        }
    }

    /// Range query
    pub fn range(&self, start_ts: i64, end_ts: i64) -> (&[i64], &[f64]) {
        let (start_pos, end_pos) = self.index.range(start_ts, end_ts);
        (
            &self.timestamps[start_pos..end_pos],
            &self.values[start_pos..end_pos],
        )
    }

    /// Downsample with aggregation (SIMD accelerated)
    pub fn downsample(
        &self,
        granularity: TimeGranularity,
        func: AggregateFunc,
    ) -> (Vec<i64>, Vec<f64>) {
        if self.timestamps.is_empty() {
            return (Vec::new(), Vec::new());
        }

        let bucket_size = granularity.nanos();
        let mut result_ts = Vec::new();
        let mut result_values = Vec::new();

        let mut bucket_start = granularity.truncate(self.timestamps[0]);
        let mut bucket_values: Vec<f64> = Vec::new();

        for (&ts, &value) in self.timestamps.iter().zip(self.values.iter()) {
            let ts_bucket = granularity.truncate(ts);

            if ts_bucket != bucket_start {
                // Emit bucket
                if !bucket_values.is_empty() {
                    result_ts.push(bucket_start);
                    result_values.push(self.aggregate(&bucket_values, func));
                }
                bucket_start = ts_bucket;
                bucket_values.clear();
            }

            bucket_values.push(value);
        }

        // Emit last bucket
        if !bucket_values.is_empty() {
            result_ts.push(bucket_start);
            result_values.push(self.aggregate(&bucket_values, func));
        }

        (result_ts, result_values)
    }

    /// Sliding window operation
    pub fn sliding_window(
        &self,
        window_size: usize,
        func: AggregateFunc,
    ) -> Vec<f64> {
        if self.values.len() < window_size {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.values.len() - window_size + 1);

        for i in 0..=(self.values.len() - window_size) {
            let window = &self.values[i..i + window_size];
            result.push(self.aggregate(window, func));
        }

        result
    }

    /// Moving average (optimized)
    pub fn moving_average(&self, window_size: usize) -> Vec<f64> {
        if self.values.len() < window_size {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.values.len() - window_size + 1);

        // Initial sum
        let mut sum: f64 = self.values[..window_size].iter().sum();
        result.push(sum / window_size as f64);

        // Sliding window
        for i in window_size..self.values.len() {
            sum += self.values[i];
            sum -= self.values[i - window_size];
            result.push(sum / window_size as f64);
        }

        result
    }

    /// Exponential moving average
    pub fn ema(&self, span: usize) -> Vec<f64> {
        if self.values.is_empty() {
            return Vec::new();
        }

        let alpha = 2.0 / (span as f64 + 1.0);
        let mut result = Vec::with_capacity(self.values.len());

        result.push(self.values[0]);
        for i in 1..self.values.len() {
            let ema = alpha * self.values[i] + (1.0 - alpha) * result[i - 1];
            result.push(ema);
        }

        result
    }

    /// Rate of change (diff)
    pub fn diff(&self, periods: usize) -> Vec<f64> {
        if self.values.len() <= periods {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.values.len() - periods);
        for i in periods..self.values.len() {
            result.push(self.values[i] - self.values[i - periods]);
        }
        result
    }

    /// Percentage change
    pub fn pct_change(&self, periods: usize) -> Vec<f64> {
        if self.values.len() <= periods {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.values.len() - periods);
        for i in periods..self.values.len() {
            let prev = self.values[i - periods];
            if prev != 0.0 {
                result.push((self.values[i] - prev) / prev);
            } else {
                result.push(f64::NAN);
            }
        }
        result
    }

    fn aggregate(&self, values: &[f64], func: AggregateFunc) -> f64 {
        match func {
            AggregateFunc::First => values.first().copied().unwrap_or(f64::NAN),
            AggregateFunc::Last => values.last().copied().unwrap_or(f64::NAN),
            AggregateFunc::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
            AggregateFunc::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
            AggregateFunc::Sum => simd::sum_f64(values),
            AggregateFunc::Avg => simd::avg_f64(values),
            AggregateFunc::Count => values.len() as f64,
            AggregateFunc::StdDev => {
                let avg = simd::avg_f64(values);
                let variance: f64 = values.iter()
                    .map(|x| (x - avg).powi(2))
                    .sum::<f64>() / values.len() as f64;
                variance.sqrt()
            }
            AggregateFunc::Variance => {
                let avg = simd::avg_f64(values);
                values.iter()
                    .map(|x| (x - avg).powi(2))
                    .sum::<f64>() / values.len() as f64
            }
        }
    }

    /// Get length
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }
}

impl Default for TimeSeriesColumn {
    fn default() -> Self {
        Self::new()
    }
}

/// Delta-of-delta encoding for timestamps
pub struct DeltaDeltaEncoder {
    prev: i64,
    prev_delta: i64,
}

impl DeltaDeltaEncoder {
    pub fn new(first: i64) -> Self {
        Self {
            prev: first,
            prev_delta: 0,
        }
    }

    pub fn encode(&mut self, value: i64) -> i64 {
        let delta = value - self.prev;
        let delta_delta = delta - self.prev_delta;

        self.prev = value;
        self.prev_delta = delta;

        delta_delta
    }
}

pub struct DeltaDeltaDecoder {
    prev: i64,
    prev_delta: i64,
}

impl DeltaDeltaDecoder {
    pub fn new(first: i64) -> Self {
        Self {
            prev: first,
            prev_delta: 0,
        }
    }

    pub fn decode(&mut self, delta_delta: i64) -> i64 {
        let delta = self.prev_delta + delta_delta;
        let value = self.prev + delta;

        self.prev = value;
        self.prev_delta = delta;

        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_index_range() {
        let mut index = TimeIndex::new();
        for i in 0..1000 {
            index.append(i * 1000); // Timestamps: 0, 1000, 2000, ...
        }

        let (start, end) = index.range(5000, 10000);
        assert_eq!(start, 5);
        assert_eq!(end, 11);
    }

    #[test]
    fn test_downsample() {
        let mut col = TimeSeriesColumn::new();
        for i in 0..100 {
            col.append(i * 1_000_000_000, i as f64); // One point per second
        }

        let (ts, values) = col.downsample(TimeGranularity::Minute, AggregateFunc::Avg);
        assert_eq!(ts.len(), 2); // Two minute buckets
    }

    #[test]
    fn test_moving_average() {
        let mut col = TimeSeriesColumn::new();
        for i in 0..10 {
            col.append(i, i as f64);
        }

        let ma = col.moving_average(3);
        assert_eq!(ma.len(), 8);
        assert!((ma[0] - 1.0).abs() < 0.001); // (0 + 1 + 2) / 3
    }
}
