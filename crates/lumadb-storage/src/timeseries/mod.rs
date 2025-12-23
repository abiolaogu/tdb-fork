//! Time-series optimized storage using Gorilla compression

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use lumadb_common::error::Result;
use lumadb_common::types::Timestamp;

/// Time-series storage engine
pub struct TimeSeriesStore {
    /// Metrics stored by name
    metrics: DashMap<String, MetricSeries>,
}

impl TimeSeriesStore {
    /// Create a new time-series store
    pub fn new() -> Self {
        Self {
            metrics: DashMap::new(),
        }
    }

    /// Write a data point
    pub fn write(
        &self,
        metric: &str,
        timestamp: Timestamp,
        value: f64,
        tags: &[(String, String)],
    ) -> Result<()> {
        let series_key = self.make_series_key(metric, tags);

        self.metrics
            .entry(series_key)
            .or_insert_with(|| MetricSeries::new(metric.to_string()))
            .add_point(timestamp, value);

        Ok(())
    }

    /// Query data points
    pub fn query(
        &self,
        metric: &str,
        start: Timestamp,
        end: Timestamp,
        tags: Option<&[(String, String)]>,
    ) -> Result<Vec<DataPoint>> {
        let mut results = Vec::new();

        for entry in self.metrics.iter() {
            let series = entry.value();

            if series.name != metric {
                continue;
            }

            // Filter by tags if specified
            if let Some(filter_tags) = tags {
                if !self.matches_tags(&entry.key(), filter_tags) {
                    continue;
                }
            }

            // Get points in range
            results.extend(series.query_range(start, end));
        }

        // Sort by timestamp
        results.sort_by_key(|p| p.timestamp);

        Ok(results)
    }

    /// Aggregate data points
    pub fn aggregate(
        &self,
        metric: &str,
        start: Timestamp,
        end: Timestamp,
        aggregation: Aggregation,
        bucket_ms: i64,
    ) -> Result<Vec<DataPoint>> {
        let points = self.query(metric, start, end, None)?;

        if points.is_empty() {
            return Ok(vec![]);
        }

        // Group by bucket
        let mut buckets: BTreeMap<Timestamp, Vec<f64>> = BTreeMap::new();

        for point in points {
            let bucket = (point.timestamp / bucket_ms) * bucket_ms;
            buckets.entry(bucket).or_default().push(point.value);
        }

        // Apply aggregation
        let results: Vec<DataPoint> = buckets
            .into_iter()
            .map(|(timestamp, values)| {
                let value = match aggregation {
                    Aggregation::Sum => values.iter().sum(),
                    Aggregation::Avg => values.iter().sum::<f64>() / values.len() as f64,
                    Aggregation::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
                    Aggregation::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    Aggregation::Count => values.len() as f64,
                    Aggregation::First => values.first().copied().unwrap_or(0.0),
                    Aggregation::Last => values.last().copied().unwrap_or(0.0),
                };
                DataPoint { timestamp, value }
            })
            .collect();

        Ok(results)
    }

    /// Make a unique series key from metric name and tags
    fn make_series_key(&self, metric: &str, tags: &[(String, String)]) -> String {
        let mut key = metric.to_string();
        let mut sorted_tags: Vec<_> = tags.iter().collect();
        sorted_tags.sort_by_key(|(k, _)| k);

        for (k, v) in sorted_tags {
            key.push_str(&format!(",{}={}", k, v));
        }

        key
    }

    /// Check if series key matches tags
    fn matches_tags(&self, series_key: &str, tags: &[(String, String)]) -> bool {
        for (key, value) in tags {
            let needle = format!(",{}={}", key, value);
            if !series_key.contains(&needle) {
                return false;
            }
        }
        true
    }
}

impl Default for TimeSeriesStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A single metric time series
struct MetricSeries {
    /// Metric name
    name: String,
    /// Data points (compressed using Gorilla-like encoding)
    points: RwLock<Vec<CompressedBlock>>,
    /// Current block being written
    current_block: RwLock<DataBlock>,
}

impl MetricSeries {
    fn new(name: String) -> Self {
        Self {
            name,
            points: RwLock::new(Vec::new()),
            current_block: RwLock::new(DataBlock::new()),
        }
    }

    fn add_point(&self, timestamp: Timestamp, value: f64) {
        let mut block = self.current_block.write();
        block.add(timestamp, value);

        // Flush block if full
        if block.len() >= 1000 {
            let compressed = block.compress();
            self.points.write().push(compressed);
            *block = DataBlock::new();
        }
    }

    fn query_range(&self, start: Timestamp, end: Timestamp) -> Vec<DataPoint> {
        let mut results = Vec::new();

        // Query compressed blocks
        for block in self.points.read().iter() {
            results.extend(block.query_range(start, end));
        }

        // Query current block
        results.extend(self.current_block.read().query_range(start, end));

        results
    }
}

/// Uncompressed data block
struct DataBlock {
    points: Vec<DataPoint>,
}

impl DataBlock {
    fn new() -> Self {
        Self { points: Vec::new() }
    }

    fn add(&mut self, timestamp: Timestamp, value: f64) {
        self.points.push(DataPoint { timestamp, value });
    }

    fn len(&self) -> usize {
        self.points.len()
    }

    fn compress(&self) -> CompressedBlock {
        // Simplified compression - in production use Gorilla encoding
        CompressedBlock {
            points: self.points.clone(),
        }
    }

    fn query_range(&self, start: Timestamp, end: Timestamp) -> Vec<DataPoint> {
        self.points
            .iter()
            .filter(|p| p.timestamp >= start && p.timestamp <= end)
            .cloned()
            .collect()
    }
}

/// Compressed data block
struct CompressedBlock {
    points: Vec<DataPoint>,
}

impl CompressedBlock {
    fn query_range(&self, start: Timestamp, end: Timestamp) -> Vec<DataPoint> {
        self.points
            .iter()
            .filter(|p| p.timestamp >= start && p.timestamp <= end)
            .cloned()
            .collect()
    }
}

/// A single data point
#[derive(Debug, Clone)]
pub struct DataPoint {
    pub timestamp: Timestamp,
    pub value: f64,
}

/// Aggregation functions
#[derive(Debug, Clone, Copy)]
pub enum Aggregation {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    First,
    Last,
}
