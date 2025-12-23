//! LumaDB Storage Engine
//!
//! High-performance storage engine providing:
//! - LSM-Tree based key-value storage
//! - Columnar storage using Apache Arrow
//! - Vector indexing with HNSW
//! - Time-series optimized storage
//! - Full-text search with Tantivy

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod cache;
pub mod columnar;
pub mod fulltext;
pub mod lsm;
pub mod timeseries;
pub mod vector;

mod engine;
mod wal;

pub use engine::StorageEngine;
pub use wal::WriteAheadLog;

use lumadb_common::config::StorageConfig;
use lumadb_common::error::Result;

/// Storage engine options
#[derive(Debug, Clone)]
pub struct StorageOptions {
    /// Path to storage directory
    pub path: String,
    /// Maximum memory for buffer pool
    pub max_memory: usize,
    /// Enable WAL
    pub wal_enabled: bool,
    /// Enable compression
    pub compression_enabled: bool,
}

impl From<StorageConfig> for StorageOptions {
    fn from(config: StorageConfig) -> Self {
        Self {
            path: config.path,
            max_memory: config.max_memory_bytes,
            wal_enabled: config.wal_enabled,
            compression_enabled: config.compression_enabled,
        }
    }
}
