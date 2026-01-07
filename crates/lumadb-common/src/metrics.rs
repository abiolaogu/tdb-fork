//! Metrics and observability for LumaDB
#![allow(clippy::non_std_lazy_statics)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::missing_panics_doc)]
#![allow(clippy::inefficient_to_string)]
#![allow(clippy::must_use_candidate)]
#![allow(clippy::doc_markdown)]

use metrics::{counter, gauge, histogram};
use parking_lot::RwLock;
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;
use std::time::Instant;

/// Global metrics registry
static METRICS: once_cell::sync::Lazy<Arc<MetricsRegistry>> =
    once_cell::sync::Lazy::new(|| Arc::new(MetricsRegistry::new()));

/// Metrics registry for LumaDB
pub struct MetricsRegistry {
    start_time: Instant,
    custom_metrics: RwLock<Vec<CustomMetric>>,
}

struct CustomMetric {
    name: String,
    value: f64,
    labels: Vec<(String, String)>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            custom_metrics: RwLock::new(Vec::new()),
        }
    }

    /// Get the global metrics registry
    pub fn global() -> Arc<MetricsRegistry> {
        Arc::clone(&METRICS)
    }

    /// Record a custom metric
    pub fn record(&self, name: &str, value: f64, labels: &[(&str, &str)]) {
        let mut metrics = self.custom_metrics.write();
        metrics.push(CustomMetric {
            name: name.to_string(),
            value,
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), (*v).to_string()))
                .collect(),
        });
    }

    /// Get uptime in seconds
    pub fn uptime_secs(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Storage Metrics
// ============================================================================

/// Record bytes written to storage
pub fn record_storage_write(bytes: u64) {
    counter!("lumadb_storage_bytes_written_total").increment(bytes);
}

/// Record bytes read from storage
pub fn record_storage_read(bytes: u64) {
    counter!("lumadb_storage_bytes_read_total").increment(bytes);
}

/// Record storage operation latency
pub fn record_storage_latency(operation: &str, latency_us: f64) {
    histogram!("lumadb_storage_operation_duration_us", "operation" => operation.to_string())
        .record(latency_us);
}

/// Update storage size gauge
pub fn set_storage_size(bytes: f64) {
    gauge!("lumadb_storage_size_bytes").set(bytes);
}

/// Record compaction event
pub fn record_compaction(bytes_before: u64, bytes_after: u64, duration_ms: u64) {
    counter!("lumadb_compaction_total").increment(1);
    counter!("lumadb_compaction_bytes_before_total").increment(bytes_before);
    counter!("lumadb_compaction_bytes_after_total").increment(bytes_after);
    histogram!("lumadb_compaction_duration_ms").record(duration_ms as f64);
}

// ============================================================================
// Streaming Metrics
// ============================================================================

/// Record messages produced
pub fn record_messages_produced(topic: &str, count: u64, bytes: u64) {
    counter!("lumadb_messages_produced_total", "topic" => topic.to_string()).increment(count);
    counter!("lumadb_bytes_produced_total", "topic" => topic.to_string()).increment(bytes);
}

/// Record messages consumed
pub fn record_messages_consumed(topic: &str, group: &str, count: u64, bytes: u64) {
    counter!("lumadb_messages_consumed_total", "topic" => topic.to_string(), "group" => group.to_string())
        .increment(count);
    counter!("lumadb_bytes_consumed_total", "topic" => topic.to_string(), "group" => group.to_string())
        .increment(bytes);
}

/// Record consumer lag
pub fn set_consumer_lag(topic: &str, partition: i32, group: &str, lag: i64) {
    gauge!("lumadb_consumer_lag",
        "topic" => topic.to_string(),
        "partition" => partition.to_string(),
        "group" => group.to_string()
    ).set(lag as f64);
}

/// Record produce latency
pub fn record_produce_latency(latency_us: f64) {
    histogram!("lumadb_produce_latency_us").record(latency_us);
}

/// Record fetch latency
pub fn record_fetch_latency(latency_us: f64) {
    histogram!("lumadb_fetch_latency_us").record(latency_us);
}

// ============================================================================
// Query Metrics
// ============================================================================

/// Record query executed
pub fn record_query(query_type: &str, success: bool) {
    let status = if success { "success" } else { "error" };
    counter!("lumadb_queries_total",
        "type" => query_type.to_string(),
        "status" => status.to_string()
    ).increment(1);
}

/// Record query latency
pub fn record_query_latency(query_type: &str, latency_ms: f64) {
    histogram!("lumadb_query_duration_ms", "type" => query_type.to_string()).record(latency_ms);
}

/// Record rows scanned
pub fn record_rows_scanned(count: u64) {
    counter!("lumadb_rows_scanned_total").increment(count);
}

/// Record cache hit/miss
pub fn record_cache_access(hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    counter!("lumadb_cache_accesses_total", "result" => result.to_string()).increment(1);
}

// ============================================================================
// Connection Metrics
// ============================================================================

/// Update active connections gauge
pub fn set_active_connections(protocol: &str, count: i64) {
    gauge!("lumadb_active_connections", "protocol" => protocol.to_string()).set(count as f64);
}

/// Record new connection
pub fn record_connection(protocol: &str) {
    counter!("lumadb_connections_total", "protocol" => protocol.to_string()).increment(1);
}

/// Record connection error
pub fn record_connection_error(protocol: &str, error_type: &str) {
    counter!("lumadb_connection_errors_total",
        "protocol" => protocol.to_string(),
        "error" => error_type.to_string()
    ).increment(1);
}

// ============================================================================
// Cluster Metrics
// ============================================================================

/// Update node status
pub fn set_node_status(node_id: u64, is_leader: bool) {
    gauge!("lumadb_node_is_leader", "node_id" => node_id.to_string())
        .set(if is_leader { 1.0 } else { 0.0 });
}

/// Record Raft event
pub fn record_raft_event(event_type: &str) {
    counter!("lumadb_raft_events_total", "type" => event_type.to_string()).increment(1);
}

/// Record replication lag
pub fn set_replication_lag(follower_id: u64, lag_entries: u64) {
    gauge!("lumadb_replication_lag_entries", "follower" => follower_id.to_string())
        .set(lag_entries as f64);
}

// ============================================================================
// Export Functions
// ============================================================================

/// Export metrics in Prometheus format
#[must_use]
pub fn export_prometheus() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        tracing::warn!("Failed to encode Prometheus metrics: {}", e);
        return String::new();
    }

    // Add custom metrics from our registry
    let registry = MetricsRegistry::global();
    let uptime = format!(
        "# HELP lumadb_uptime_seconds Server uptime in seconds\n\
         # TYPE lumadb_uptime_seconds gauge\n\
         lumadb_uptime_seconds {}\n",
        registry.uptime_secs()
    );

    let metrics_str = String::from_utf8(buffer).unwrap_or_default();
    format!("{uptime}{metrics_str}")
}

/// Timer guard for automatic latency recording
pub struct LatencyTimer {
    start: Instant,
    metric_name: String,
    labels: Vec<(String, String)>,
}

impl LatencyTimer {
    #[must_use]
    pub fn new(metric_name: &str, labels: &[(&str, &str)]) -> Self {
        Self {
            start: Instant::now(),
            metric_name: metric_name.to_string(),
            labels: labels
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        }
    }

    #[must_use]
    pub fn elapsed_us(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1_000_000.0
    }

    #[must_use]
    pub fn elapsed_ms(&self) -> f64 {
        self.start.elapsed().as_secs_f64() * 1000.0
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let latency = self.elapsed_us();
        // Record the metric when the timer is dropped
        // Note: The metric name needs to be a static string for the histogram macro
        // For dynamic metrics, use the registry directly
        MetricsRegistry::global().record(&self.metric_name, latency, &[]);
    }
}
