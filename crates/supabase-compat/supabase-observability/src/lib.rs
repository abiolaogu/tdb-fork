//! Observability Stack for Supabase Compatibility
//!
//! Provides monitoring and metrics:
//! - Request metrics
//! - Database query stats
//! - Prometheus-compatible exports

#![warn(clippy::all, clippy::pedantic)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Counter metric
pub struct Counter {
    name: String,
    labels: HashMap<String, String>,
    value: AtomicU64,
}

impl Counter {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            labels: HashMap::new(),
            value: AtomicU64::new(0),
        }
    }

    pub fn with_label(mut self, key: &str, value: &str) -> Self {
        self.labels.insert(key.to_string(), value.to_string());
        self
    }

    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_by(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// Histogram metric
#[derive(Debug)]
pub struct Histogram {
    name: String,
    buckets: Vec<f64>,
    counts: Vec<AtomicU64>,
    sum: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    pub fn new(name: &str, buckets: Vec<f64>) -> Self {
        let counts = buckets.iter().map(|_| AtomicU64::new(0)).collect();
        Self {
            name: name.to_string(),
            buckets,
            counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, value: f64) {
        self.sum
            .fetch_add((value * 1000.0) as u64, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);

        for (i, bucket) in self.buckets.iter().enumerate() {
            if value <= *bucket {
                self.counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Service health status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub service: String,
    pub status: ServiceStatus,
    pub version: String,
    pub uptime_seconds: u64,
    pub checked_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// Metrics collector
pub struct MetricsCollector {
    counters: RwLock<HashMap<String, Arc<Counter>>>,
    histograms: RwLock<HashMap<String, Arc<Histogram>>>,
    start_time: DateTime<Utc>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            start_time: Utc::now(),
        }
    }

    /// Register a counter
    pub fn register_counter(&self, name: &str) -> Arc<Counter> {
        let counter = Arc::new(Counter::new(name));
        self.counters
            .write()
            .insert(name.to_string(), counter.clone());
        counter
    }

    /// Register a histogram
    pub fn register_histogram(&self, name: &str, buckets: Vec<f64>) -> Arc<Histogram> {
        let hist = Arc::new(Histogram::new(name, buckets));
        self.histograms
            .write()
            .insert(name.to_string(), hist.clone());
        hist
    }

    /// Get a counter
    pub fn counter(&self, name: &str) -> Option<Arc<Counter>> {
        self.counters.read().get(name).cloned()
    }

    /// Get uptime in seconds
    pub fn uptime_seconds(&self) -> u64 {
        (Utc::now() - self.start_time).num_seconds() as u64
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        for (name, counter) in self.counters.read().iter() {
            output.push_str(&format!("{} {}\n", name, counter.get()));
        }

        for (name, hist) in self.histograms.read().iter() {
            for (i, bucket) in hist.buckets.iter().enumerate() {
                output.push_str(&format!(
                    "{}_bucket{{le=\"{}\"}} {}\n",
                    name,
                    bucket,
                    hist.counts[i].load(Ordering::Relaxed)
                ));
            }
            output.push_str(&format!(
                "{}_sum {}\n",
                name,
                hist.sum.load(Ordering::Relaxed) as f64 / 1000.0
            ));
            output.push_str(&format!(
                "{}_count {}\n",
                name,
                hist.count.load(Ordering::Relaxed)
            ));
        }

        output
    }

    /// Get health status
    pub fn health(&self, service: &str, version: &str) -> HealthStatus {
        HealthStatus {
            service: service.to_string(),
            status: ServiceStatus::Healthy,
            version: version.to_string(),
            uptime_seconds: self.uptime_seconds(),
            checked_at: Utc::now(),
        }
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new("requests_total");
        counter.inc();
        counter.inc();
        assert_eq!(counter.get(), 2);
    }

    #[test]
    fn test_histogram() {
        let hist = Histogram::new("request_duration", vec![0.1, 0.5, 1.0, 5.0]);
        hist.observe(0.3);
        hist.observe(2.0);
        assert_eq!(hist.count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_prometheus_export() {
        let collector = MetricsCollector::new();
        let counter = collector.register_counter("http_requests_total");
        counter.inc_by(100);

        let output = collector.export_prometheus();
        assert!(output.contains("http_requests_total 100"));
    }
}
