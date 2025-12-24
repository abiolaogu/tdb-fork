//! LumaDB Query Engine
//!
//! Provides:
//! - LQL (LumaDB Query Language) parser
//! - SQL parser (via sqlparser-rs)
//! - Cost-based query optimizer
//! - Vectorized query execution
//! - User-defined functions (UDFs)

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod analyzer;
pub mod executor;
pub mod functions;
pub mod optimizer;
pub mod parser;

mod engine;

pub use engine::QueryEngine;

use serde::{Deserialize, Serialize};

/// Options for creating a collection
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CollectionOptions {
    /// Enable schema validation
    pub schema_validation: bool,
    /// Collection storage engine
    pub engine: Option<String>,
    /// Additional options
    pub extra: std::collections::HashMap<String, serde_json::Value>,
}

/// Query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    /// Result rows
    rows: Vec<std::collections::HashMap<String, serde_json::Value>>,
    /// Number of rows affected
    rows_affected: u64,
    /// Whether result was from cache
    pub cached: bool,
    /// Column names
    columns: Vec<String>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl QueryResult {
    /// Create a new query result
    pub fn new(rows: Vec<std::collections::HashMap<String, serde_json::Value>>) -> Self {
        let columns = if let Some(first) = rows.first() {
            first.keys().cloned().collect()
        } else {
            Vec::new()
        };

        Self {
            rows_affected: rows.len() as u64,
            rows,
            cached: false,
            columns,
            execution_time_ms: 0,
        }
    }

    /// Get rows
    pub fn rows(&self) -> &[std::collections::HashMap<String, serde_json::Value>] {
        &self.rows
    }

    /// Get columns
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Get rows affected
    pub fn rows_affected(&self) -> u64 {
        self.rows_affected
    }

    /// Check if result was cached
    pub fn was_cached(&self) -> bool {
        self.cached
    }

    /// Convert to JSON
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(&self.rows).unwrap_or_default()
    }

    /// Convert to table string
    pub fn to_table(&self) -> String {
        if self.rows.is_empty() {
            return "(empty result set)".to_string();
        }

        let mut output = String::new();

        // Header
        output.push_str(&self.columns.join(" | "));
        output.push('\n');
        output.push_str(&"-".repeat(self.columns.len() * 15));
        output.push('\n');

        // Rows
        for row in &self.rows {
            let values: Vec<String> = self
                .columns
                .iter()
                .map(|c| {
                    row.get(c)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            output.push_str(&values.join(" | "));
            output.push('\n');
        }

        output
    }
}
