//! Columnar Storage Engine
//!
//! kdb+ style columnar storage with:
//! - SIMD vectorized operations
//! - Cache-friendly memory layout
//! - Efficient compression
//! - Time-series optimizations
//!
//! This is what makes TDB+ competitive with kdb+ for analytics.

pub mod vector;
pub mod simd;
pub mod compression;
pub mod timeseries;

use std::collections::HashMap;


use parking_lot::RwLock;

use crate::error::{LumaError, Result as LumaResult};

/// Column data types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Timestamp,     // Nanosecond precision
    Date,          // Days since epoch
    Time,          // Nanoseconds since midnight
    Symbol,        // Interned string
    String,        // Variable length
    Binary,        // Variable length bytes
    Vector(Box<ColumnType>), // Nested vector
}

impl ColumnType {
    /// Size in bytes (for fixed-width types)
    pub fn size(&self) -> Option<usize> {
        match self {
            ColumnType::Bool => Some(1),
            ColumnType::Int8 | ColumnType::UInt8 => Some(1),
            ColumnType::Int16 | ColumnType::UInt16 => Some(2),
            ColumnType::Int32 | ColumnType::UInt32 | ColumnType::Date => Some(4),
            ColumnType::Int64 | ColumnType::UInt64 | ColumnType::Timestamp | ColumnType::Time => Some(8),
            ColumnType::Float32 => Some(4),
            ColumnType::Float64 => Some(8),
            ColumnType::Symbol => Some(4), // Index into symbol table
            ColumnType::String | ColumnType::Binary => None,
            ColumnType::Vector(_) => None,
        }
    }

    /// Is this a numeric type suitable for SIMD?
    pub fn is_simd_compatible(&self) -> bool {
        matches!(self,
            ColumnType::Int8 | ColumnType::Int16 | ColumnType::Int32 | ColumnType::Int64 |
            ColumnType::UInt8 | ColumnType::UInt16 | ColumnType::UInt32 | ColumnType::UInt64 |
            ColumnType::Float32 | ColumnType::Float64
        )
    }
}

/// Column statistics for query optimization
#[derive(Debug, Clone, Default)]
pub struct ColumnStats {
    pub count: u64,
    pub null_count: u64,
    pub min: Option<ColumnValue>,
    pub max: Option<ColumnValue>,
    pub sum: Option<f64>,
    pub distinct_count: Option<u64>,
}

/// Column value (for statistics and comparisons)
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

/// A column of data
pub struct Column {
    /// Column name
    pub name: String,

    /// Data type
    pub dtype: ColumnType,

    /// Raw data buffer
    data: Vec<u8>,

    /// Null bitmap (bit per row)
    nulls: Option<Vec<u64>>,

    /// Offsets for variable-length types
    offsets: Option<Vec<u32>>,

    /// Statistics
    stats: ColumnStats,

    /// Compression codec
    compression: CompressionCodec,

    /// Is data sorted?
    sorted: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum CompressionCodec {
    #[default]
    None,
    LZ4,
    Zstd,
    Delta,      // For sorted integers
    DeltaDelta, // For timestamps
    RunLength,  // For low cardinality
    Dictionary, // For symbols
}

impl Column {
    /// Create a new column
    pub fn new(name: impl Into<String>, dtype: ColumnType) -> Self {
        Self {
            name: name.into(),
            dtype,
            data: Vec::new(),
            nulls: None,
            offsets: None,
            stats: ColumnStats::default(),
            compression: CompressionCodec::None,
            sorted: false,
        }
    }

    /// Create column with pre-allocated capacity
    pub fn with_capacity(name: impl Into<String>, dtype: ColumnType, capacity: usize) -> Self {
        let element_size = dtype.size().unwrap_or(0);
        Self {
            name: name.into(),
            dtype,
            data: Vec::with_capacity(capacity * element_size),
            nulls: None,
            offsets: if element_size == 0 {
                Some(Vec::with_capacity(capacity + 1))
            } else {
                None
            },
            stats: ColumnStats::default(),
            compression: CompressionCodec::None,
            sorted: false,
        }
    }

    /// Number of rows
    pub fn len(&self) -> usize {
        if let Some(size) = self.dtype.size() {
            self.data.len() / size
        } else if let Some(ref offsets) = self.offsets {
            offsets.len().saturating_sub(1)
        } else {
            0
        }
    }

    /// Is column empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get raw data slice
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Get typed slice for numeric columns (zero-copy)
    pub fn as_i64_slice(&self) -> Option<&[i64]> {
        if self.dtype != ColumnType::Int64 && self.dtype != ColumnType::Timestamp {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const i64,
                self.data.len() / 8,
            )
        })
    }

    pub fn as_f64_slice(&self) -> Option<&[f64]> {
        if self.dtype != ColumnType::Float64 {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const f64,
                self.data.len() / 8,
            )
        })
    }

    pub fn as_i32_slice(&self) -> Option<&[i32]> {
        if self.dtype != ColumnType::Int32 && self.dtype != ColumnType::Date {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const i32,
                self.data.len() / 4,
            )
        })
    }

    pub fn as_f32_slice(&self) -> Option<&[f32]> {
        if self.dtype != ColumnType::Float32 {
            return None;
        }
        Some(unsafe {
            std::slice::from_raw_parts(
                self.data.as_ptr() as *const f32,
                self.data.len() / 4,
            )
        })
    }

    /// Append i64 values (bulk)
    pub fn append_i64(&mut self, values: &[i64]) {
        debug_assert!(self.dtype == ColumnType::Int64 || self.dtype == ColumnType::Timestamp);

        let bytes = unsafe {
            std::slice::from_raw_parts(
                values.as_ptr() as *const u8,
                values.len() * 8,
            )
        };
        self.data.extend_from_slice(bytes);
        self.stats.count += values.len() as u64;
    }

    /// Append f64 values (bulk)
    pub fn append_f64(&mut self, values: &[f64]) {
        debug_assert!(self.dtype == ColumnType::Float64);

        let bytes = unsafe {
            std::slice::from_raw_parts(
                values.as_ptr() as *const u8,
                values.len() * 8,
            )
        };
        self.data.extend_from_slice(bytes);
        self.stats.count += values.len() as u64;
    }

    /// Get statistics
    pub fn stats(&self) -> &ColumnStats {
        &self.stats
    }

    /// Compute statistics
    pub fn compute_stats(&mut self) {
        match self.dtype {
            ColumnType::Int64 | ColumnType::Timestamp => {
                if let Some(slice) = self.as_i64_slice() {
                    let (min, max, sum) = simd::stats_i64(slice);
                    self.stats.min = Some(ColumnValue::Int64(min));
                    self.stats.max = Some(ColumnValue::Int64(max));
                    self.stats.sum = Some(sum as f64);
                }
            }
            ColumnType::Float64 => {
                if let Some(slice) = self.as_f64_slice() {
                    let (min, max, sum) = simd::stats_f64(slice);
                    self.stats.min = Some(ColumnValue::Float64(min));
                    self.stats.max = Some(ColumnValue::Float64(max));
                    self.stats.sum = Some(sum);
                }
            }
            _ => {}
        }
    }
}

/// A columnar table (collection of columns)
pub struct ColumnarTable {
    /// Table name
    name: String,

    /// Columns by name
    columns: HashMap<String, Column>,

    /// Column order
    column_order: Vec<String>,

    /// Row count
    row_count: usize,

    /// Primary key column(s)
    primary_key: Vec<String>,

    /// Sort key for time-series
    sort_key: Option<String>,
}

impl ColumnarTable {
    /// Create a new columnar table
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: HashMap::new(),
            column_order: Vec::new(),
            row_count: 0,
            primary_key: Vec::new(),
            sort_key: None,
        }
    }

    /// Add a column
    pub fn add_column(&mut self, column: Column) {
        let name = column.name.clone();
        self.column_order.push(name.clone());
        self.columns.insert(name, column);
    }

    /// Get column by name
    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }

    /// Get mutable column
    pub fn column_mut(&mut self, name: &str) -> Option<&mut Column> {
        self.columns.get_mut(name)
    }

    /// Execute vectorized operation across columns
    pub fn apply_simd<F>(&self, col_name: &str, f: F) -> LumaResult<Vec<f64>>
    where
        F: Fn(&[f64]) -> Vec<f64>,
    {
        let col = self.column(col_name)
            .ok_or(LumaError::NotFound(format!("Column {}", col_name)))?;

        let slice = col.as_f64_slice()
            .ok_or(LumaError::InvalidArgument("Column is not f64".into()))?;

        Ok(f(slice))
    }

    /// SIMD sum
    pub fn sum(&self, col_name: &str) -> LumaResult<f64> {
        let col = self.column(col_name)
            .ok_or(LumaError::NotFound(format!("Column {}", col_name)))?;

        match col.dtype {
            ColumnType::Float64 => {
                let slice = col.as_f64_slice().unwrap();
                Ok(simd::sum_f64(slice))
            }
            ColumnType::Int64 | ColumnType::Timestamp => {
                let slice = col.as_i64_slice().unwrap();
                Ok(simd::sum_i64(slice) as f64)
            }
            _ => Err(LumaError::InvalidArgument("Column is not numeric".into())),
        }
    }

    /// SIMD average
    pub fn avg(&self, col_name: &str) -> LumaResult<f64> {
        let col = self.column(col_name)
            .ok_or(LumaError::NotFound(format!("Column {}", col_name)))?;

        match col.dtype {
            ColumnType::Float64 => {
                let slice = col.as_f64_slice().unwrap();
                Ok(simd::avg_f64(slice))
            }
            ColumnType::Int64 | ColumnType::Timestamp => {
                let slice = col.as_i64_slice().unwrap();
                Ok(simd::sum_i64(slice) as f64 / slice.len() as f64)
            }
            _ => Err(LumaError::InvalidArgument("Column is not numeric".into())),
        }
    }

    /// SIMD filter
    pub fn filter_gt(&self, col_name: &str, threshold: f64) -> LumaResult<Vec<usize>> {
        let col = self.column(col_name)
            .ok_or(LumaError::NotFound(format!("Column {}", col_name)))?;

        let slice = col.as_f64_slice()
            .ok_or(LumaError::InvalidArgument("Column is not f64".into()))?;

        Ok(simd::filter_gt_f64(slice, threshold))
    }

    /// Row count
    pub fn len(&self) -> usize {
        self.row_count
    }

    /// Is table empty?
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

/// Schema definition
pub struct Schema {
    pub columns: Vec<(String, ColumnType)>,
    pub primary_key: Vec<String>,
    pub sort_key: Option<String>,
}
