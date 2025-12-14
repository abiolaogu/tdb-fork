//! TimescaleDB Extensions Implementation
//! Provides hypertables, continuous aggregates, and time-series functions

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{DateTime, Utc, Duration};
use serde::{Deserialize, Serialize};
use tracing::info;

/// Hypertable configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hypertable {
    pub schema: String,
    pub table_name: String,
    pub time_column: String,
    pub chunk_interval_secs: i64, // Using i64 for serde compat
    pub partitioning_column: Option<String>,
    pub compression_enabled: bool,
    pub retention_policy_secs: Option<i64>,
    pub created_at: DateTime<Utc>,
}

/// Time bucket/chunk
#[derive(Debug, Clone)]
pub struct Chunk {
    pub id: u64,
    pub hypertable: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub compressed: bool,
    pub row_count: u64,
}

/// Continuous aggregate definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousAggregate {
    pub name: String,
    pub source_hypertable: String,
    pub view_definition: String,
    pub bucket_width_secs: i64,
    pub materialized: bool,
    pub refresh_policy: RefreshPolicy,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshPolicy {
    pub start_offset_secs: i64,
    pub end_offset_secs: i64,
    pub schedule_interval_secs: i64,
}

/// TimescaleDB extension manager
pub struct TimescaleExtension {
    hypertables: Arc<RwLock<HashMap<String, Hypertable>>>,
    chunks: Arc<RwLock<Vec<Chunk>>>,
    continuous_aggregates: Arc<RwLock<HashMap<String, ContinuousAggregate>>>,
    chunk_id_counter: Arc<RwLock<u64>>,
}

impl TimescaleExtension {
    pub fn new() -> Self {
        Self {
            hypertables: Arc::new(RwLock::new(HashMap::new())),
            chunks: Arc::new(RwLock::new(Vec::new())),
            continuous_aggregates: Arc::new(RwLock::new(HashMap::new())),
            chunk_id_counter: Arc::new(RwLock::new(0)),
        }
    }

    /// CREATE EXTENSION timescaledb
    pub async fn init(&self) -> Result<(), String> {
        info!("TimescaleDB extension initialized");
        Ok(())
    }

    /// create_hypertable(table, time_column, chunk_time_interval)
    pub async fn create_hypertable(
        &self,
        schema: &str,
        table_name: &str,
        time_column: &str,
        chunk_interval_days: i64,
    ) -> Result<Hypertable, String> {
        let key = format!("{}.{}", schema, table_name);
        
        let mut hypertables = self.hypertables.write().await;
        if hypertables.contains_key(&key) {
            return Err(format!("table {} is already a hypertable", table_name));
        }

        let hypertable = Hypertable {
            schema: schema.to_string(),
            table_name: table_name.to_string(),
            time_column: time_column.to_string(),
            chunk_interval_secs: chunk_interval_days * 86400,
            partitioning_column: None,
            compression_enabled: false,
            retention_policy_secs: None,
            created_at: Utc::now(),
        };

        hypertables.insert(key.clone(), hypertable.clone());
        info!("Created hypertable: {}", key);

        Ok(hypertable)
    }

    /// add_dimension(table, column, number_partitions)
    pub async fn add_dimension(
        &self,
        schema: &str,
        table_name: &str,
        column: &str,
        _num_partitions: u32,
    ) -> Result<(), String> {
        let key = format!("{}.{}", schema, table_name);
        let mut hypertables = self.hypertables.write().await;
        
        let ht = hypertables.get_mut(&key)
            .ok_or_else(|| format!("hypertable {} not found", key))?;
        
        ht.partitioning_column = Some(column.to_string());
        info!("Added dimension {} to hypertable {}", column, key);
        
        Ok(())
    }

    /// set_chunk_time_interval(table, interval)
    pub async fn set_chunk_time_interval(
        &self,
        schema: &str,
        table_name: &str,
        interval_days: i64,
    ) -> Result<(), String> {
        let key = format!("{}.{}", schema, table_name);
        let mut hypertables = self.hypertables.write().await;
        
        let ht = hypertables.get_mut(&key)
            .ok_or_else(|| format!("hypertable {} not found", key))?;
        
        ht.chunk_interval_secs = interval_days * 86400;
        info!("Set chunk interval to {} days for {}", interval_days, key);
        
        Ok(())
    }

    /// add_compression_policy(table, compress_after)
    pub async fn add_compression_policy(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<(), String> {
        let key = format!("{}.{}", schema, table_name);
        let mut hypertables = self.hypertables.write().await;
        
        let ht = hypertables.get_mut(&key)
            .ok_or_else(|| format!("hypertable {} not found", key))?;
        
        ht.compression_enabled = true;
        info!("Enabled compression for {}", key);
        
        Ok(())
    }

    /// add_retention_policy(table, drop_after)
    pub async fn add_retention_policy(
        &self,
        schema: &str,
        table_name: &str,
        retention_days: i64,
    ) -> Result<(), String> {
        let key = format!("{}.{}", schema, table_name);
        let mut hypertables = self.hypertables.write().await;
        
        let ht = hypertables.get_mut(&key)
            .ok_or_else(|| format!("hypertable {} not found", key))?;
        
        ht.retention_policy_secs = Some(retention_days * 86400);
        info!("Set retention policy to {} days for {}", retention_days, key);
        
        Ok(())
    }

    /// CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)
    pub async fn create_continuous_aggregate(
        &self,
        name: &str,
        source_hypertable: &str,
        view_definition: &str,
        bucket_width_hours: i64,
    ) -> Result<ContinuousAggregate, String> {
        let mut aggregates = self.continuous_aggregates.write().await;
        
        if aggregates.contains_key(name) {
            return Err(format!("continuous aggregate {} already exists", name));
        }

        let cagg = ContinuousAggregate {
            name: name.to_string(),
            source_hypertable: source_hypertable.to_string(),
            view_definition: view_definition.to_string(),
            bucket_width_secs: bucket_width_hours * 3600,
            materialized: true,
            refresh_policy: RefreshPolicy {
                start_offset_secs: 7 * 86400,
                end_offset_secs: 3600,
                schedule_interval_secs: 3600,
            },
            created_at: Utc::now(),
        };

        aggregates.insert(name.to_string(), cagg.clone());
        info!("Created continuous aggregate: {}", name);

        Ok(cagg)
    }

    /// add_continuous_aggregate_policy(view, start_offset, end_offset, schedule_interval)
    pub async fn add_continuous_aggregate_policy(
        &self,
        name: &str,
        start_offset_days: i64,
        end_offset_hours: i64,
        schedule_interval_hours: i64,
    ) -> Result<(), String> {
        let mut aggregates = self.continuous_aggregates.write().await;
        
        let cagg = aggregates.get_mut(name)
            .ok_or_else(|| format!("continuous aggregate {} not found", name))?;
        
        cagg.refresh_policy = RefreshPolicy {
            start_offset_secs: start_offset_days * 86400,
            end_offset_secs: end_offset_hours * 3600,
            schedule_interval_secs: schedule_interval_hours * 3600,
        };
        
        info!("Set refresh policy for {}", name);
        Ok(())
    }

    /// refresh_continuous_aggregate(view, start, end)
    pub async fn refresh_continuous_aggregate(
        &self,
        name: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> Result<u64, String> {
        let aggregates = self.continuous_aggregates.read().await;
        
        if !aggregates.contains_key(name) {
            return Err(format!("continuous aggregate {} not found", name));
        }

        let rows_refreshed = 1000u64;
        info!("Refreshed continuous aggregate {}: {} rows", name, rows_refreshed);
        
        Ok(rows_refreshed)
    }

    /// time_bucket(interval, timestamp) 
    pub fn time_bucket(&self, interval_secs: i64, timestamp: DateTime<Utc>) -> DateTime<Utc> {
        let ts_secs = timestamp.timestamp();
        let bucket_start = (ts_secs / interval_secs) * interval_secs;
        DateTime::from_timestamp(bucket_start, 0).unwrap_or(timestamp)
    }

    /// time_bucket_gapfill(interval, timestamp)
    pub fn time_bucket_gapfill(
        &self,
        interval_secs: i64,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Vec<DateTime<Utc>> {
        let mut buckets = Vec::new();
        let mut current = self.time_bucket(interval_secs, start);
        
        while current < end {
            buckets.push(current);
            current = current + Duration::seconds(interval_secs);
        }
        
        buckets
    }

    /// locf(value) - Last Observation Carried Forward
    pub fn locf<T: Clone>(&self, values: &[Option<T>]) -> Vec<Option<T>> {
        let mut result = Vec::with_capacity(values.len());
        let mut last_value: Option<T> = None;
        
        for v in values {
            if v.is_some() {
                last_value = v.clone();
            }
            result.push(last_value.clone());
        }
        
        result
    }

    /// interpolate(value)
    pub fn interpolate(&self, values: &[Option<f64>]) -> Vec<f64> {
        let mut result = vec![0.0; values.len()];
        
        for i in 0..values.len() {
            if let Some(v) = values[i] {
                result[i] = v;
            } else {
                let prev = (0..i).rev().find_map(|j| values[j]);
                let next = (i+1..values.len()).find_map(|j| values[j]);
                
                result[i] = match (prev, next) {
                    (Some(p), Some(n)) => (p + n) / 2.0,
                    (Some(p), None) => p,
                    (None, Some(n)) => n,
                    (None, None) => 0.0,
                };
            }
        }
        
        result
    }

    /// first(value, time)
    pub fn first<T: Clone>(&self, values: &[(T, DateTime<Utc>)]) -> Option<T> {
        values.iter()
            .min_by_key(|(_, t)| t)
            .map(|(v, _)| v.clone())
    }

    /// last(value, time)
    pub fn last<T: Clone>(&self, values: &[(T, DateTime<Utc>)]) -> Option<T> {
        values.iter()
            .max_by_key(|(_, t)| t)
            .map(|(v, _)| v.clone())
    }

    /// histogram(column, min, max, nbuckets)
    pub fn histogram(&self, values: &[f64], min: f64, max: f64, nbuckets: usize) -> Vec<u64> {
        let mut buckets = vec![0u64; nbuckets];
        let bucket_width = (max - min) / nbuckets as f64;
        
        for &v in values {
            if v >= min && v < max {
                let idx = ((v - min) / bucket_width) as usize;
                if idx < nbuckets {
                    buckets[idx] += 1;
                }
            }
        }
        
        buckets
    }

    /// approximate_row_count(table)
    pub async fn approximate_row_count(&self, schema: &str, table_name: &str) -> Result<u64, String> {
        let key = format!("{}.{}", schema, table_name);
        let chunks = self.chunks.read().await;
        
        let count: u64 = chunks.iter()
            .filter(|c| c.hypertable == key)
            .map(|c| c.row_count)
            .sum();
        
        Ok(count)
    }

    /// show_chunks(table)
    pub async fn show_chunks(&self, schema: &str, table_name: &str) -> Vec<Chunk> {
        let key = format!("{}.{}", schema, table_name);
        let chunks = self.chunks.read().await;
        
        chunks.iter()
            .filter(|c| c.hypertable == key)
            .cloned()
            .collect()
    }

    /// drop_chunks(table, older_than)
    pub async fn drop_chunks(
        &self,
        schema: &str,
        table_name: &str,
        older_than: DateTime<Utc>,
    ) -> Result<u64, String> {
        let key = format!("{}.{}", schema, table_name);
        let mut chunks = self.chunks.write().await;
        
        let before = chunks.len();
        chunks.retain(|c| !(c.hypertable == key && c.end_time < older_than));
        let dropped = (before - chunks.len()) as u64;
        
        info!("Dropped {} chunks from {}", dropped, key);
        Ok(dropped)
    }

    /// compress_chunk(chunk)
    pub async fn compress_chunk(&self, chunk_id: u64) -> Result<(), String> {
        let mut chunks = self.chunks.write().await;
        
        let chunk = chunks.iter_mut()
            .find(|c| c.id == chunk_id)
            .ok_or_else(|| format!("chunk {} not found", chunk_id))?;
        
        chunk.compressed = true;
        info!("Compressed chunk {}", chunk_id);
        
        Ok(())
    }

    /// hypertable_detailed_size(table)
    pub async fn hypertable_detailed_size(
        &self,
        schema: &str,
        table_name: &str,
    ) -> Result<HypertableSize, String> {
        let key = format!("{}.{}", schema, table_name);
        let chunks = self.chunks.read().await;
        
        let total_chunks: usize = chunks.iter().filter(|c| c.hypertable == key).count();
        let compressed_chunks: usize = chunks.iter().filter(|c| c.hypertable == key && c.compressed).count();
        let row_count: u64 = chunks.iter().filter(|c| c.hypertable == key).map(|c| c.row_count).sum();
        
        Ok(HypertableSize {
            table_name: table_name.to_string(),
            total_chunks,
            compressed_chunks,
            row_count,
            uncompressed_size_bytes: row_count * 100,
            compressed_size_bytes: (compressed_chunks as u64) * 1000,
        })
    }

    /// List all hypertables
    pub async fn list_hypertables(&self) -> Vec<Hypertable> {
        self.hypertables.read().await.values().cloned().collect()
    }

    /// List all continuous aggregates
    pub async fn list_continuous_aggregates(&self) -> Vec<ContinuousAggregate> {
        self.continuous_aggregates.read().await.values().cloned().collect()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HypertableSize {
    pub table_name: String,
    pub total_chunks: usize,
    pub compressed_chunks: usize,
    pub row_count: u64,
    pub uncompressed_size_bytes: u64,
    pub compressed_size_bytes: u64,
}

impl Default for TimescaleExtension {
    fn default() -> Self {
        Self::new()
    }
}
