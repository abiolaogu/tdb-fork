//! Protocol implementations for LumaDB
//! Provides wire-level compatibility with multiple databases

pub mod postgres;
pub mod prometheus;
pub mod otlp;
pub mod redis;
pub mod clickhouse;
pub mod druid;
pub mod elasticsearch;

// Re-export common types
pub use crate::protocols::postgres::AuthConfig;

use luma_protocol_core::Value;
use std::sync::Arc;
use async_trait::async_trait;

/// Query request for protocol handlers
#[derive(Debug, Clone)]
pub struct QueryRequest {
    pub query: String,
    pub params: Vec<Vec<u8>>,
}

/// Query result for protocol handlers
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub rows: Vec<Vec<Value>>,
}

/// Query processor trait for protocol handlers
#[async_trait]
pub trait QueryProcessor: Send + Sync {
    async fn process(&self, request: QueryRequest) -> Result<QueryResult, Box<dyn std::error::Error + Send + Sync>>;
}
