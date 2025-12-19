//! OTLP (OpenTelemetry Protocol) receiver implementation
//!
//! Implements gRPC services for receiving traces, metrics, and logs
//! compatible with OpenTelemetry collectors and agents.

use std::sync::Arc;

/// Trait for pluggable storage backends that can ingest observability data.
/// Implement this trait to store telemetry data in your preferred backend.
pub trait DataIngestor: Send + Sync {
    /// Ingest trace spans
    fn ingest_traces(&self, spans: Vec<TraceSpan>) -> Result<(), String>;
    /// Ingest metric data points
    fn ingest_metrics(&self, metrics: Vec<MetricData>) -> Result<(), String>;
    /// Ingest log records
    fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<(), String>;
}

/// Represents a single trace span
#[derive(Debug, Clone)]
pub struct TraceSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub kind: i32,
    pub start_time_unix_nano: u64,
    pub end_time_unix_nano: u64,
    pub attributes: String, // JSON string
    pub status_code: i32,
    pub status_message: Option<String>,
    pub resource_attributes: Option<String>,
}

/// Represents metric data
#[derive(Debug, Clone)]
pub struct MetricData {
    pub name: String,
    pub timestamp_unix_nano: u64,
    pub value: f64,
    pub labels: Option<String>, // JSON string
    pub resource_attributes: Option<String>,
}

/// Represents a log record
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub timestamp_unix_nano: u64,
    pub severity_text: Option<String>,
    pub severity_number: Option<i32>,
    pub body: Option<String>,
    pub attributes: Option<String>,
    pub resource_attributes: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
}

/// OTLP gRPC receiver that handles trace, metric, and log exports
pub struct OtlpReceiver {
    ingestor: Arc<dyn DataIngestor>,
}

impl Clone for OtlpReceiver {
    fn clone(&self) -> Self {
        Self {
            ingestor: self.ingestor.clone(),
        }
    }
}

impl OtlpReceiver {
    /// Create a new OTLP receiver with the given data ingestor
    pub fn new(ingestor: Arc<dyn DataIngestor>) -> Self {
        Self { ingestor }
    }

    /// Handle trace export request
    pub fn export_traces(&self, spans: Vec<TraceSpan>) -> Result<(), String> {
        self.ingestor.ingest_traces(spans)
    }

    /// Handle metrics export request
    pub fn export_metrics(&self, metrics: Vec<MetricData>) -> Result<(), String> {
        self.ingestor.ingest_metrics(metrics)
    }

    /// Handle logs export request
    pub fn export_logs(&self, logs: Vec<LogRecord>) -> Result<(), String> {
        self.ingestor.ingest_logs(logs)
    }
}

/// Configuration for the OTLP server
#[derive(Debug, Clone)]
pub struct OtlpConfig {
    /// gRPC listen address (e.g., "0.0.0.0:4317")
    pub grpc_addr: String,
    /// HTTP listen address for OTLP/HTTP (e.g., "0.0.0.0:4318")
    pub http_addr: Option<String>,
    /// Maximum message size in bytes
    pub max_message_size: usize,
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            grpc_addr: "0.0.0.0:4317".to_string(),
            http_addr: Some("0.0.0.0:4318".to_string()),
            max_message_size: 4 * 1024 * 1024, // 4MB
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockIngestor;

    impl DataIngestor for MockIngestor {
        fn ingest_traces(&self, spans: Vec<TraceSpan>) -> Result<(), String> {
            println!("Ingested {} spans", spans.len());
            Ok(())
        }

        fn ingest_metrics(&self, metrics: Vec<MetricData>) -> Result<(), String> {
            println!("Ingested {} metrics", metrics.len());
            Ok(())
        }

        fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<(), String> {
            println!("Ingested {} logs", logs.len());
            Ok(())
        }
    }

    #[test]
    fn test_otlp_receiver() {
        let ingestor = Arc::new(MockIngestor);
        let receiver = OtlpReceiver::new(ingestor);

        let span = TraceSpan {
            trace_id: "abc123".to_string(),
            span_id: "def456".to_string(),
            parent_span_id: None,
            name: "test-span".to_string(),
            kind: 1,
            start_time_unix_nano: 1000000000,
            end_time_unix_nano: 2000000000,
            attributes: "{}".to_string(),
            status_code: 0,
            status_message: None,
            resource_attributes: None,
        };

        assert!(receiver.export_traces(vec![span]).is_ok());
    }
}
