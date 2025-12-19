//! Storage layer for observability data
//!
//! Provides columnar storage optimized for traces, metrics, and logs
//! with efficient time-range queries and aggregations.

use std::sync::Arc;
use std::collections::HashMap;
use parking_lot::RwLock;

use super::otlp::{DataIngestor, LogRecord, MetricData, TraceSpan};

/// Schema definition for observability tables
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub fields: Vec<FieldDefinition>,
    pub partition_by: Vec<String>,
    pub order_by: Vec<String>,
}

/// Field definition within a schema
#[derive(Debug, Clone)]
pub struct FieldDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Supported data types
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Utf8,
    Int32,
    Int64,
    UInt64,
    Float64,
    Boolean,
}

/// Simple columnar table for observability data
#[derive(Debug)]
pub struct ColumnTable {
    pub schema: TableSchema,
    pub data: RwLock<TableData>,
}

/// In-memory columnar data storage
#[derive(Debug, Default)]
pub struct TableData {
    /// Columns keyed by field name
    pub columns: HashMap<String, ColumnData>,
    /// Row count
    pub row_count: usize,
}

/// Column data as typed vectors
#[derive(Debug, Clone)]
pub enum ColumnData {
    Utf8(Vec<Option<String>>),
    Int32(Vec<Option<i32>>),
    Int64(Vec<Option<i64>>),
    UInt64(Vec<Option<u64>>),
    Float64(Vec<Option<f64>>),
    Boolean(Vec<Option<bool>>),
}

impl ColumnTable {
    /// Create a new table with the given schema
    pub fn new(schema: TableSchema) -> Self {
        let mut columns = HashMap::new();
        for field in &schema.fields {
            let column = match field.data_type {
                DataType::Utf8 => ColumnData::Utf8(Vec::new()),
                DataType::Int32 => ColumnData::Int32(Vec::new()),
                DataType::Int64 => ColumnData::Int64(Vec::new()),
                DataType::UInt64 => ColumnData::UInt64(Vec::new()),
                DataType::Float64 => ColumnData::Float64(Vec::new()),
                DataType::Boolean => ColumnData::Boolean(Vec::new()),
            };
            columns.insert(field.name.clone(), column);
        }

        Self {
            schema,
            data: RwLock::new(TableData {
                columns,
                row_count: 0,
            }),
        }
    }

    /// Get the row count
    pub fn row_count(&self) -> usize {
        self.data.read().row_count
    }
}

/// Central storage for all observability data
pub struct ObservabilityStore {
    pub traces: Arc<ColumnTable>,
    pub metrics: Arc<ColumnTable>,
    pub logs: Arc<ColumnTable>,
}

impl ObservabilityStore {
    /// Create a new observability store with default schemas
    pub fn new() -> Self {
        Self {
            traces: Arc::new(create_trace_table()),
            metrics: Arc::new(create_metric_table()),
            logs: Arc::new(create_log_table()),
        }
    }

    /// Insert trace spans
    pub fn insert_traces(&self, spans: &[TraceSpan]) -> Result<(), String> {
        let mut data = self.traces.data.write();
        
        for span in spans {
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("trace_id") {
                col.push(Some(span.trace_id.clone()));
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("span_id") {
                col.push(Some(span.span_id.clone()));
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("parent_span_id") {
                col.push(span.parent_span_id.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("name") {
                col.push(Some(span.name.clone()));
            }
            if let Some(ColumnData::Int32(col)) = data.columns.get_mut("kind") {
                col.push(Some(span.kind));
            }
            if let Some(ColumnData::UInt64(col)) = data.columns.get_mut("start_time_unix_nano") {
                col.push(Some(span.start_time_unix_nano));
            }
            if let Some(ColumnData::UInt64(col)) = data.columns.get_mut("end_time_unix_nano") {
                col.push(Some(span.end_time_unix_nano));
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("attributes") {
                col.push(Some(span.attributes.clone()));
            }
            if let Some(ColumnData::Int32(col)) = data.columns.get_mut("status_code") {
                col.push(Some(span.status_code));
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("status_message") {
                col.push(span.status_message.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("resource_attributes") {
                col.push(span.resource_attributes.clone());
            }
            data.row_count += 1;
        }
        
        Ok(())
    }

    /// Insert metrics
    pub fn insert_metrics(&self, metrics: &[MetricData]) -> Result<(), String> {
        let mut data = self.metrics.data.write();
        
        for metric in metrics {
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("name") {
                col.push(Some(metric.name.clone()));
            }
            if let Some(ColumnData::UInt64(col)) = data.columns.get_mut("timestamp_unix_nano") {
                col.push(Some(metric.timestamp_unix_nano));
            }
            if let Some(ColumnData::Float64(col)) = data.columns.get_mut("value") {
                col.push(Some(metric.value));
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("labels") {
                col.push(metric.labels.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("resource_attributes") {
                col.push(metric.resource_attributes.clone());
            }
            data.row_count += 1;
        }
        
        Ok(())
    }

    /// Insert logs
    pub fn insert_logs(&self, logs: &[LogRecord]) -> Result<(), String> {
        let mut data = self.logs.data.write();
        
        for log in logs {
            if let Some(ColumnData::UInt64(col)) = data.columns.get_mut("timestamp_unix_nano") {
                col.push(Some(log.timestamp_unix_nano));
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("severity_text") {
                col.push(log.severity_text.clone());
            }
            if let Some(ColumnData::Int32(col)) = data.columns.get_mut("severity_number") {
                col.push(log.severity_number);
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("body") {
                col.push(log.body.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("attributes") {
                col.push(log.attributes.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("resource_attributes") {
                col.push(log.resource_attributes.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("trace_id") {
                col.push(log.trace_id.clone());
            }
            if let Some(ColumnData::Utf8(col)) = data.columns.get_mut("span_id") {
                col.push(log.span_id.clone());
            }
            data.row_count += 1;
        }
        
        Ok(())
    }
}

impl Default for ObservabilityStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DataIngestor for ObservabilityStore {
    fn ingest_traces(&self, spans: Vec<TraceSpan>) -> Result<(), String> {
        self.insert_traces(&spans)
    }

    fn ingest_metrics(&self, metrics: Vec<MetricData>) -> Result<(), String> {
        self.insert_metrics(&metrics)
    }

    fn ingest_logs(&self, logs: Vec<LogRecord>) -> Result<(), String> {
        self.insert_logs(&logs)
    }
}

/// Create the trace table schema
fn create_trace_table() -> ColumnTable {
    let schema = TableSchema {
        name: "traces".to_string(),
        fields: vec![
            FieldDefinition { name: "trace_id".into(), data_type: DataType::Utf8, nullable: false },
            FieldDefinition { name: "span_id".into(), data_type: DataType::Utf8, nullable: false },
            FieldDefinition { name: "parent_span_id".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "name".into(), data_type: DataType::Utf8, nullable: false },
            FieldDefinition { name: "kind".into(), data_type: DataType::Int32, nullable: false },
            FieldDefinition { name: "start_time_unix_nano".into(), data_type: DataType::UInt64, nullable: false },
            FieldDefinition { name: "end_time_unix_nano".into(), data_type: DataType::UInt64, nullable: false },
            FieldDefinition { name: "attributes".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "status_code".into(), data_type: DataType::Int32, nullable: false },
            FieldDefinition { name: "status_message".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "resource_attributes".into(), data_type: DataType::Utf8, nullable: true },
        ],
        partition_by: vec!["start_time_unix_nano".to_string()],
        order_by: vec!["trace_id".to_string(), "start_time_unix_nano".to_string()],
    };
    ColumnTable::new(schema)
}

/// Create the metric table schema
fn create_metric_table() -> ColumnTable {
    let schema = TableSchema {
        name: "metrics".to_string(),
        fields: vec![
            FieldDefinition { name: "name".into(), data_type: DataType::Utf8, nullable: false },
            FieldDefinition { name: "timestamp_unix_nano".into(), data_type: DataType::UInt64, nullable: false },
            FieldDefinition { name: "value".into(), data_type: DataType::Float64, nullable: false },
            FieldDefinition { name: "labels".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "resource_attributes".into(), data_type: DataType::Utf8, nullable: true },
        ],
        partition_by: vec!["name".to_string()],
        order_by: vec!["timestamp_unix_nano".to_string()],
    };
    ColumnTable::new(schema)
}

/// Create the log table schema
fn create_log_table() -> ColumnTable {
    let schema = TableSchema {
        name: "logs".to_string(),
        fields: vec![
            FieldDefinition { name: "timestamp_unix_nano".into(), data_type: DataType::UInt64, nullable: false },
            FieldDefinition { name: "severity_text".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "severity_number".into(), data_type: DataType::Int32, nullable: true },
            FieldDefinition { name: "body".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "attributes".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "resource_attributes".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "trace_id".into(), data_type: DataType::Utf8, nullable: true },
            FieldDefinition { name: "span_id".into(), data_type: DataType::Utf8, nullable: true },
        ],
        partition_by: vec!["timestamp_unix_nano".to_string()],
        order_by: vec!["timestamp_unix_nano".to_string()],
    };
    ColumnTable::new(schema)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_observability_store_creation() {
        let store = ObservabilityStore::new();
        assert_eq!(store.traces.row_count(), 0);
        assert_eq!(store.metrics.row_count(), 0);
        assert_eq!(store.logs.row_count(), 0);
    }

    #[test]
    fn test_insert_traces() {
        let store = ObservabilityStore::new();
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

        store.insert_traces(&[span]).unwrap();
        assert_eq!(store.traces.row_count(), 1);
    }

    #[test]
    fn test_insert_metrics() {
        let store = ObservabilityStore::new();
        let metric = MetricData {
            name: "http_requests_total".to_string(),
            timestamp_unix_nano: 1000000000,
            value: 42.0,
            labels: Some(r#"{"method":"GET"}"#.to_string()),
            resource_attributes: None,
        };

        store.insert_metrics(&[metric]).unwrap();
        assert_eq!(store.metrics.row_count(), 1);
    }

    #[test]
    fn test_insert_logs() {
        let store = ObservabilityStore::new();
        let log = LogRecord {
            timestamp_unix_nano: 1000000000,
            severity_text: Some("INFO".to_string()),
            severity_number: Some(9),
            body: Some("Application started".to_string()),
            attributes: None,
            resource_attributes: None,
            trace_id: None,
            span_id: None,
        };

        store.insert_logs(&[log]).unwrap();
        assert_eq!(store.logs.row_count(), 1);
    }
}
