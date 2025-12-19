//! Observability module for OpenTelemetry-compatible telemetry data
//! 
//! Provides:
//! - OTLP gRPC receiver for traces, metrics, and logs
//! - Query API compatible with Grafana data source format
//! - Columnar storage optimized for time-series observability data

pub mod otlp;
pub mod query;
pub mod storage;

pub use otlp::{DataIngestor, OtlpReceiver};
pub use query::{QueryRequest, QueryResponse};
pub use storage::ObservabilityStore;
