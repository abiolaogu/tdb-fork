//! Columnar storage using Apache Arrow

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Float64Array, Int64Array, StringArray, BooleanArray,
    ArrayBuilder, Float64Builder, Int64Builder, StringBuilder, BooleanBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dashmap::DashMap;
use parking_lot::RwLock;

use lumadb_common::error::{Result, Error};

/// Columnar storage for analytical queries
pub struct ColumnarStore {
    /// Tables stored in columnar format
    tables: DashMap<String, ColumnarTable>,
}

impl ColumnarStore {
    /// Create a new columnar store
    pub fn new() -> Self {
        Self {
            tables: DashMap::new(),
        }
    }

    /// Create a new table
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<()> {
        if self.tables.contains_key(name) {
            return Err(Error::Internal(format!("Table {} already exists", name)));
        }

        let table = ColumnarTable::new(schema);
        self.tables.insert(name.to_string(), table);
        Ok(())
    }

    /// Get a table
    pub fn get_table(&self, name: &str) -> Option<dashmap::mapref::one::Ref<String, ColumnarTable>> {
        self.tables.get(name)
    }

    /// Insert a record batch into a table
    pub fn insert(&self, table: &str, batch: RecordBatch) -> Result<()> {
        let table = self.tables.get(table)
            .ok_or_else(|| Error::Internal(format!("Table {} not found", table)))?;
        table.insert(batch)
    }

    /// Scan a table
    pub fn scan(&self, table: &str) -> Result<Vec<RecordBatch>> {
        let table = self.tables.get(table)
            .ok_or_else(|| Error::Internal(format!("Table {} not found", table)))?;
        Ok(table.scan())
    }

    /// Get table schema
    pub fn schema(&self, table: &str) -> Option<Schema> {
        self.tables.get(table).map(|t| t.schema())
    }
}

impl Default for ColumnarStore {
    fn default() -> Self {
        Self::new()
    }
}

/// A single columnar table
pub struct ColumnarTable {
    /// Table schema
    schema: Arc<Schema>,
    /// Record batches
    batches: RwLock<Vec<RecordBatch>>,
    /// Row count
    row_count: std::sync::atomic::AtomicUsize,
}

impl ColumnarTable {
    /// Create a new columnar table
    pub fn new(schema: Schema) -> Self {
        Self {
            schema: Arc::new(schema),
            batches: RwLock::new(Vec::new()),
            row_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Get schema
    pub fn schema(&self) -> Schema {
        (*self.schema).clone()
    }

    /// Insert a record batch
    pub fn insert(&self, batch: RecordBatch) -> Result<()> {
        // Validate schema matches
        if batch.schema() != self.schema {
            return Err(Error::Internal("Schema mismatch".to_string()));
        }

        let rows = batch.num_rows();
        self.batches.write().push(batch);
        self.row_count.fetch_add(rows, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Scan all batches
    pub fn scan(&self) -> Vec<RecordBatch> {
        self.batches.read().clone()
    }

    /// Get row count
    pub fn row_count(&self) -> usize {
        self.row_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Filter batches by predicate
    pub fn filter<F>(&self, predicate: F) -> Result<Vec<RecordBatch>>
    where
        F: Fn(&RecordBatch, usize) -> bool,
    {
        let batches = self.batches.read();
        let mut results = Vec::new();

        for batch in batches.iter() {
            let mut indices = Vec::new();
            for i in 0..batch.num_rows() {
                if predicate(batch, i) {
                    indices.push(i);
                }
            }

            if !indices.is_empty() {
                // Create filtered batch
                let columns: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        let mut builder = make_builder(col.data_type(), indices.len());
                        for &idx in &indices {
                            append_value(&mut builder, col, idx);
                        }
                        finish_builder(builder)
                    })
                    .collect();

                let filtered = RecordBatch::try_new(batch.schema(), columns)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                results.push(filtered);
            }
        }

        Ok(results)
    }
}

/// Create a builder for a data type
fn make_builder(data_type: &DataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        DataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        DataType::Utf8 => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
        DataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        _ => Box::new(Int64Builder::with_capacity(capacity)), // Fallback
    }
}

/// Append a value from an array to a builder
fn append_value(builder: &mut Box<dyn ArrayBuilder>, array: &ArrayRef, index: usize) {
    match array.data_type() {
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            if array.is_null(index) {
                b.append_null();
            } else {
                b.append_value(arr.value(index));
            }
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
            let b = builder.as_any_mut().downcast_mut::<Float64Builder>().unwrap();
            if array.is_null(index) {
                b.append_null();
            } else {
                b.append_value(arr.value(index));
            }
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
            let b = builder.as_any_mut().downcast_mut::<StringBuilder>().unwrap();
            if array.is_null(index) {
                b.append_null();
            } else {
                b.append_value(arr.value(index));
            }
        }
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            let b = builder.as_any_mut().downcast_mut::<BooleanBuilder>().unwrap();
            if array.is_null(index) {
                b.append_null();
            } else {
                b.append_value(arr.value(index));
            }
        }
        _ => {}
    }
}

/// Finish building an array
fn finish_builder(mut builder: Box<dyn ArrayBuilder>) -> ArrayRef {
    builder.finish()
}
