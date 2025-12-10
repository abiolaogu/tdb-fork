//! Core types for TDB+

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};

/// Unique document identifier
pub type DocumentId = String;

/// Collection name
pub type CollectionName = String;

/// Shard identifier
pub type ShardId = u32;

/// Sequence number for ordering
pub type SequenceNumber = u64;

/// A document stored in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Document ID
    #[serde(rename = "_id")]
    pub id: DocumentId,

    /// Revision number for optimistic concurrency
    #[serde(rename = "_rev")]
    pub revision: u64,

    /// Creation timestamp
    #[serde(rename = "_created")]
    pub created_at: DateTime<Utc>,

    /// Last update timestamp
    #[serde(rename = "_updated")]
    pub updated_at: DateTime<Utc>,

    /// Time-to-live in seconds (0 = no expiry)
    #[serde(rename = "_ttl", default)]
    pub ttl: u64,

    /// Document data
    #[serde(flatten)]
    pub data: HashMap<String, Value>,
}

impl Document {
    /// Create a new document with auto-generated ID
    pub fn new(data: HashMap<String, Value>) -> Self {
        let now = Utc::now();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            revision: 1,
            created_at: now,
            updated_at: now,
            ttl: 0,
            data,
        }
    }

    /// Create a document with a specific ID
    pub fn with_id(id: impl Into<String>, data: HashMap<String, Value>) -> Self {
        let now = Utc::now();
        Self {
            id: id.into(),
            revision: 1,
            created_at: now,
            updated_at: now,
            ttl: 0,
            data,
        }
    }

    /// Get a value by key
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.data.get(key)
    }

    /// Set a value
    pub fn set(&mut self, key: impl Into<String>, value: Value) {
        self.data.insert(key.into(), value);
        self.updated_at = Utc::now();
    }

    /// Calculate the size in bytes
    pub fn size_bytes(&self) -> usize {
        // Estimate: serialize to get accurate size
        bincode::serialized_size(self).unwrap_or(0) as usize
    }

    /// Check if document has expired
    pub fn is_expired(&self) -> bool {
        if self.ttl == 0 {
            return false;
        }
        let expiry = self.created_at + chrono::Duration::seconds(self.ttl as i64);
        Utc::now() > expiry
    }

    /// Increment revision for update
    pub fn increment_revision(&mut self) {
        self.revision += 1;
        self.updated_at = Utc::now();
    }
}

/// Value type supporting multiple data types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<Value>),
    Object(HashMap<String, Value>),
    /// Vector embedding for AI operations
    Vector(Vec<f32>),
}

impl Value {
    /// Check if value is null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to get as string
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to get as f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as vector (for AI embeddings)
    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            Value::Vector(v) => Some(v),
            _ => None,
        }
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Int(i)
    }
}

impl From<i32> for Value {
    fn from(i: i32) -> Self {
        Value::Int(i as i64)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<f32>> for Value {
    fn from(v: Vec<f32>) -> Self {
        Value::Vector(v)
    }
}

/// Key-value pair for internal storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp: i64,
    pub sequence: SequenceNumber,
    pub deleted: bool,
}

impl KeyValue {
    pub fn new(key: Vec<u8>, value: Vec<u8>, sequence: SequenceNumber) -> Self {
        Self {
            key,
            value,
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            sequence,
            deleted: false,
        }
    }

    pub fn tombstone(key: Vec<u8>, sequence: SequenceNumber) -> Self {
        Self {
            key,
            value: Vec::new(),
            timestamp: Utc::now().timestamp_nanos_opt().unwrap_or(0),
            sequence,
            deleted: true,
        }
    }
}

/// Range for scanning
#[derive(Debug, Clone)]
pub struct KeyRange {
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub start_inclusive: bool,
    pub end_inclusive: bool,
}

impl KeyRange {
    pub fn all() -> Self {
        Self {
            start: None,
            end: None,
            start_inclusive: true,
            end_inclusive: true,
        }
    }

    pub fn prefix(prefix: Vec<u8>) -> Self {
        let mut end = prefix.clone();
        // Increment last byte for exclusive end
        if let Some(last) = end.last_mut() {
            *last = last.saturating_add(1);
        }
        Self {
            start: Some(prefix),
            end: Some(end),
            start_inclusive: true,
            end_inclusive: false,
        }
    }
}

/// Compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

impl Default for Compression {
    fn default() -> Self {
        Compression::Lz4
    }
}

/// Write options
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// Sync to disk immediately
    pub sync: bool,
    /// Skip WAL (dangerous, but fast)
    pub skip_wal: bool,
    /// Time-to-live in seconds
    pub ttl: Option<u64>,
}

/// Read options
#[derive(Debug, Clone, Default)]
pub struct ReadOptions {
    /// Read from specific snapshot
    pub snapshot: Option<SequenceNumber>,
    /// Fill cache on read
    pub fill_cache: bool,
    /// Verify checksums
    pub verify_checksums: bool,
}

/// Batch operation for atomic writes
#[derive(Debug, Clone)]
pub struct WriteBatch {
    pub operations: Vec<BatchOperation>,
}

#[derive(Debug, Clone)]
pub enum BatchOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl WriteBatch {
    pub fn new() -> Self {
        Self { operations: Vec::new() }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOperation::Put { key, value });
    }

    pub fn delete(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOperation::Delete { key });
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}
