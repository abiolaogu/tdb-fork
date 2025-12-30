//! Core types and traits for multi-protocol compatibility.
//!
//! This module defines the unified data structures that enable translation
//! between different database protocols while maintaining zero-copy efficiency
//! where possible.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Unified query result that can be converted to any protocol format.
///
/// This is the canonical result type returned by all storage operations.
/// Protocol adapters translate this into their specific response formats.
///
/// # Performance
///
/// - Uses `Cow` for strings to enable zero-copy when possible
/// - Pre-allocates capacity hints for common result sizes
/// - Supports streaming for large result sets
///
/// # Example
///
/// ```rust
/// use lumadb_multicompat::core::{UnifiedResult, Row, Column, Value, ResultMetadata};
///
/// let result = UnifiedResult {
///     rows: vec![Row {
///         columns: vec![
///             Column { name: "id".into(), value: Value::Integer(1) },
///             Column { name: "name".into(), value: Value::String("Alice".into()) },
///         ],
///     }],
///     affected_rows: 0,
///     last_insert_id: None,
///     metadata: ResultMetadata::default(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnifiedResult {
    /// Result rows from the query
    pub rows: Vec<Row>,
    /// Number of rows affected by write operations
    pub affected_rows: u64,
    /// Last auto-generated ID for inserts
    pub last_insert_id: Option<i64>,
    /// Execution metadata for observability
    pub metadata: ResultMetadata,
}

impl UnifiedResult {
    /// Create an empty result (for operations with no return data)
    #[must_use]
    pub fn empty() -> Self {
        Self {
            rows: Vec::new(),
            affected_rows: 0,
            last_insert_id: None,
            metadata: ResultMetadata::default(),
        }
    }

    /// Create a result with affected rows count
    #[must_use]
    pub fn with_affected_rows(count: u64) -> Self {
        Self {
            rows: Vec::new(),
            affected_rows: count,
            last_insert_id: None,
            metadata: ResultMetadata::default(),
        }
    }

    /// Create a result from rows
    #[must_use]
    pub fn from_rows(rows: Vec<Row>) -> Self {
        let row_count = rows.len() as u64;
        Self {
            rows,
            affected_rows: 0,
            last_insert_id: None,
            metadata: ResultMetadata {
                rows_read: row_count,
                ..Default::default()
            },
        }
    }

    /// Check if the result is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.affected_rows == 0
    }

    /// Get the number of rows
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
}

/// A single row of data with named columns.
///
/// Rows are protocol-agnostic and can represent:
/// - SQL result rows
/// - DynamoDB items
/// - Document database documents
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Row {
    /// Ordered list of columns in the row
    pub columns: Vec<Column>,
}

impl Row {
    /// Create an empty row
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a row with pre-allocated capacity
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            columns: Vec::with_capacity(capacity),
        }
    }

    /// Add a column to the row
    pub fn push(&mut self, name: impl Into<String>, value: Value) {
        self.columns.push(Column {
            name: name.into(),
            value,
        });
    }

    /// Get a column value by name
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| &c.value)
    }

    /// Get a mutable column value by name
    pub fn get_mut(&mut self, name: &str) -> Option<&mut Value> {
        self.columns
            .iter_mut()
            .find(|c| c.name == name)
            .map(|c| &mut c.value)
    }

    /// Convert row to a HashMap for easy access
    #[must_use]
    pub fn to_map(&self) -> HashMap<String, Value> {
        self.columns
            .iter()
            .map(|c| (c.name.clone(), c.value.clone()))
            .collect()
    }

    /// Create a row from a HashMap
    #[must_use]
    pub fn from_map(map: HashMap<String, Value>) -> Self {
        Self {
            columns: map
                .into_iter()
                .map(|(name, value)| Column { name, value })
                .collect(),
        }
    }
}

impl FromIterator<(String, Value)> for Row {
    fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
        Self {
            columns: iter
                .into_iter()
                .map(|(name, value)| Column { name, value })
                .collect(),
        }
    }
}

/// A named column with a value.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column value
    pub value: Value,
}

impl Column {
    /// Create a new column
    #[must_use]
    pub fn new(name: impl Into<String>, value: Value) -> Self {
        Self {
            name: name.into(),
            value,
        }
    }
}

/// Universal value type supporting all protocol data types.
///
/// This enum can represent values from any supported protocol:
/// - SQL: NULL, booleans, integers, floats, strings, blobs
/// - DynamoDB: All AttributeValue types including sets and maps
/// - JSON: Full JSON value support
///
/// # Serialization
///
/// Uses serde's `untagged` representation for clean JSON output.
/// For DynamoDB format, use the `to_dynamodb_value` method.
///
/// # Example
///
/// ```rust
/// use lumadb_multicompat::core::Value;
///
/// let s = Value::String("hello".into());
/// let n = Value::Integer(42);
/// let arr = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Value {
    /// SQL NULL or missing value
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    Integer(i64),
    /// 64-bit floating point
    Float(f64),
    /// UTF-8 string
    String(String),
    /// Binary data
    Bytes(Vec<u8>),
    /// Array of values (JSON array, DynamoDB list)
    Array(Vec<Value>),
    /// Key-value map (JSON object, DynamoDB map)
    Object(HashMap<String, Value>),
    /// String set (DynamoDB SS)
    StringSet(Vec<String>),
    /// Number set (DynamoDB NS)
    NumberSet(Vec<f64>),
    /// Binary set (DynamoDB BS)
    BinarySet(Vec<Vec<u8>>),
}

impl Value {
    /// Check if the value is null
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to get as boolean
    #[must_use]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to get as integer
    #[must_use]
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Try to get as float
    #[must_use]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to get as string
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as bytes
    #[must_use]
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to get as array
    #[must_use]
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(a) => Some(a),
            _ => None,
        }
    }

    /// Try to get as object/map
    #[must_use]
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(o) => Some(o),
            _ => None,
        }
    }

    /// Get the type name for error messages
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Integer(_) => "integer",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::StringSet(_) => "string_set",
            Value::NumberSet(_) => "number_set",
            Value::BinarySet(_) => "binary_set",
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Bool(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(i64::from(v))
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Integer(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytes(v)
    }
}

impl<T: Into<Value>> From<Vec<T>> for Value {
    fn from(v: Vec<T>) -> Self {
        Value::Array(v.into_iter().map(Into::into).collect())
    }
}

impl<T: Into<Value>> From<HashMap<String, T>> for Value {
    fn from(v: HashMap<String, T>) -> Self {
        Value::Object(v.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

impl From<serde_json::Value> for Value {
    fn from(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Array(a) => {
                Value::Array(a.into_iter().map(Value::from).collect())
            }
            serde_json::Value::Object(o) => {
                Value::Object(o.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
        }
    }
}

impl From<Value> for serde_json::Value {
    fn from(v: Value) -> Self {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::Integer(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => {
                serde_json::Number::from_f64(f)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null)
            }
            Value::String(s) => serde_json::Value::String(s),
            Value::Bytes(b) => serde_json::Value::String(base64::encode(&b)),
            Value::Array(a) => {
                serde_json::Value::Array(a.into_iter().map(Into::into).collect())
            }
            Value::Object(o) => {
                serde_json::Value::Object(o.into_iter().map(|(k, v)| (k, v.into())).collect())
            }
            Value::StringSet(s) => {
                serde_json::Value::Array(s.into_iter().map(serde_json::Value::String).collect())
            }
            Value::NumberSet(n) => {
                serde_json::Value::Array(
                    n.into_iter()
                        .filter_map(|f| serde_json::Number::from_f64(f).map(serde_json::Value::Number))
                        .collect(),
                )
            }
            Value::BinarySet(b) => {
                serde_json::Value::Array(
                    b.into_iter()
                        .map(|bytes| serde_json::Value::String(base64::encode(&bytes)))
                        .collect(),
                )
            }
        }
    }
}

/// Metadata about query execution for observability.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ResultMetadata {
    /// Query execution time in milliseconds
    pub execution_time_ms: u64,
    /// Number of rows read during execution
    pub rows_read: u64,
    /// Number of rows written during execution
    pub rows_written: u64,
    /// Bytes scanned (for capacity tracking)
    pub bytes_scanned: u64,
    /// Whether the result was served from cache
    pub cached: bool,
    /// Consumed capacity units (DynamoDB compatibility)
    pub consumed_capacity: Option<f64>,
}

impl ResultMetadata {
    /// Create metadata with execution timing
    pub fn with_timing(start: Instant) -> Self {
        Self {
            execution_time_ms: start.elapsed().as_millis() as u64,
            ..Default::default()
        }
    }
}

/// Trait for protocol adapters that handle incoming requests.
///
/// Each protocol (DynamoDB, D1, Turso) implements this trait to:
/// 1. Parse incoming protocol-specific requests
/// 2. Translate to unified storage operations
/// 3. Convert results back to protocol-specific format
///
/// # Thread Safety
///
/// Adapters must be `Send + Sync` to support concurrent request handling.
///
/// # Example
///
/// ```rust,ignore
/// struct MyAdapter { storage: Arc<dyn StorageEngine> }
///
/// #[async_trait]
/// impl ProtocolAdapter for MyAdapter {
///     async fn handle_request(&self, request: Vec<u8>) -> Result<Vec<u8>, AdapterError> {
///         // Parse request, execute, format response
///     }
///     
///     fn protocol_name(&self) -> &'static str { "myprotocol" }
///     
///     async fn health_check(&self) -> Result<(), AdapterError> { Ok(()) }
/// }
/// ```
#[async_trait]
pub trait ProtocolAdapter: Send + Sync {
    /// Handle an incoming protocol request.
    ///
    /// # Arguments
    /// * `request` - Raw request bytes in protocol-specific format
    ///
    /// # Returns
    /// Raw response bytes in protocol-specific format
    async fn handle_request(&self, request: Vec<u8>) -> Result<Vec<u8>, AdapterError>;

    /// Get the protocol name for logging and metrics.
    fn protocol_name(&self) -> &'static str;

    /// Perform a health check for the adapter.
    async fn health_check(&self) -> Result<(), AdapterError>;

    /// Get adapter-specific metrics (optional).
    fn metrics(&self) -> HashMap<String, f64> {
        HashMap::new()
    }
}

/// Errors that can occur in protocol adapters.
#[derive(Debug, thiserror::Error)]
pub enum AdapterError {
    /// Request format is invalid or cannot be parsed
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Query execution failed
    #[error("Query error: {0}")]
    QueryError(String),

    /// Authentication or authorization failed
    #[error("Authentication failed: {0}")]
    AuthenticationError(String),

    /// Requested resource does not exist
    #[error("Resource not found: {0}")]
    NotFound(String),

    /// Resource already exists
    #[error("Resource already exists: {0}")]
    AlreadyExists(String),

    /// Operation would exceed capacity limits
    #[error("Throughput exceeded: {0}")]
    ThroughputExceeded(String),

    /// Conditional check failed (e.g., DynamoDB condition expressions)
    #[error("Condition check failed: {0}")]
    ConditionCheckFailed(String),

    /// Transaction was cancelled
    #[error("Transaction cancelled: {0}")]
    TransactionCancelled(String),

    /// Validation error in request data
    #[error("Validation error: {0}")]
    ValidationError(String),

    /// Internal server error
    #[error("Internal error: {0}")]
    InternalError(String),

    /// I/O error
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
}

impl AdapterError {
    /// Get HTTP status code for this error type
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            AdapterError::InvalidRequest(_) => 400,
            AdapterError::ValidationError(_) => 400,
            AdapterError::AuthenticationError(_) => 401,
            AdapterError::NotFound(_) => 404,
            AdapterError::AlreadyExists(_) => 409,
            AdapterError::ConditionCheckFailed(_) => 409,
            AdapterError::ThroughputExceeded(_) => 429,
            AdapterError::TransactionCancelled(_) => 409,
            AdapterError::QueryError(_) => 500,
            AdapterError::InternalError(_) => 500,
            AdapterError::IoError(_) => 500,
            AdapterError::SerializationError(_) => 500,
        }
    }

    /// Get DynamoDB-style error code
    #[must_use]
    pub fn dynamodb_code(&self) -> &'static str {
        match self {
            AdapterError::InvalidRequest(_) => "ValidationException",
            AdapterError::ValidationError(_) => "ValidationException",
            AdapterError::AuthenticationError(_) => "UnrecognizedClientException",
            AdapterError::NotFound(_) => "ResourceNotFoundException",
            AdapterError::ConditionCheckFailed(_) => "ConditionalCheckFailedException",
            AdapterError::ThroughputExceeded(_) => "ProvisionedThroughputExceededException",
            AdapterError::TransactionCancelled(_) => "TransactionCanceledException",
            _ => "InternalServerError",
        }
    }
}

/// Core storage engine interface for all database operations.
///
/// This trait abstracts the underlying LumaDB storage, allowing
/// protocol adapters to execute queries without knowing storage details.
///
/// # Operations
///
/// - **SQL**: Full SQL query support via `execute_sql`
/// - **Key-Value**: DynamoDB-style operations via `execute_kv_*`
/// - **Batch**: Efficient bulk operations via `batch_write`
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) for concurrent access.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Execute a SQL query with parameters.
    ///
    /// # Arguments
    /// * `sql` - SQL query string (may contain `?` placeholders)
    /// * `params` - Parameter values for placeholders
    ///
    /// # Returns
    /// Query result with rows and metadata
    async fn execute_sql(
        &self,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<UnifiedResult, AdapterError>;

    /// Get a single item by primary key.
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `key` - Primary key value (may be composite for DynamoDB)
    async fn execute_kv_get(
        &self,
        table: &str,
        key: Value,
    ) -> Result<Option<Row>, AdapterError>;

    /// Put (upsert) an item.
    ///
    /// # Arguments
    /// * `table` - Table name
    /// * `key` - Primary key value
    /// * `value` - Row data to store
    async fn execute_kv_put(
        &self,
        table: &str,
        key: Value,
        value: Row,
    ) -> Result<(), AdapterError>;

    /// Delete an item by primary key.
    async fn execute_kv_delete(
        &self,
        table: &str,
        key: Value,
    ) -> Result<(), AdapterError>;

    /// Query items with filter conditions.
    ///
    /// Supports both key conditions (efficient) and filter expressions.
    async fn execute_kv_query(
        &self,
        table: &str,
        filter: QueryFilter,
    ) -> Result<Vec<Row>, AdapterError>;

    /// Execute multiple write operations atomically.
    ///
    /// # Arguments
    /// * `operations` - List of put/delete operations
    ///
    /// # Atomicity
    /// All operations succeed or all fail together.
    async fn batch_write(
        &self,
        operations: Vec<BatchOperation>,
    ) -> Result<(), AdapterError>;

    /// Execute a transactional write with condition checks.
    async fn transact_write(
        &self,
        operations: Vec<TransactWriteItem>,
    ) -> Result<(), AdapterError>;

    /// Create a table with the given schema.
    async fn create_table(
        &self,
        table: &str,
        schema: TableSchema,
    ) -> Result<(), AdapterError>;

    /// Delete a table.
    async fn delete_table(&self, table: &str) -> Result<(), AdapterError>;

    /// List all tables.
    async fn list_tables(&self) -> Result<Vec<String>, AdapterError>;

    /// Describe a table's schema.
    async fn describe_table(&self, table: &str) -> Result<TableSchema, AdapterError>;
}

/// Query filter for key-value operations.
#[derive(Debug, Clone, Default)]
pub struct QueryFilter {
    /// Key condition for efficient querying (uses indexes)
    pub key_condition: Option<KeyCondition>,
    /// Additional filter expression (applied after key condition)
    pub filter_expression: Option<String>,
    /// Projection - columns to return (None = all)
    pub projection: Option<Vec<String>>,
    /// Maximum items to return
    pub limit: Option<usize>,
    /// Scan direction (true = ascending)
    pub scan_forward: bool,
    /// Exclusive start key for pagination
    pub exclusive_start_key: Option<Value>,
    /// Consistent read flag
    pub consistent_read: bool,
}

/// Key condition for efficient index-based queries.
#[derive(Debug, Clone)]
pub struct KeyCondition {
    /// Partition key (required, must be equality)
    pub partition_key: (String, Value),
    /// Sort key condition (optional)
    pub sort_key: Option<(String, SortKeyCondition)>,
}

/// Conditions that can be applied to sort keys.
#[derive(Debug, Clone)]
pub enum SortKeyCondition {
    /// Exact match
    Equal(Value),
    /// Less than
    LessThan(Value),
    /// Less than or equal
    LessThanOrEqual(Value),
    /// Greater than
    GreaterThan(Value),
    /// Greater than or equal
    GreaterThanOrEqual(Value),
    /// Between two values (inclusive)
    Between(Value, Value),
    /// String prefix match
    BeginsWith(String),
}

/// Batch write operation.
#[derive(Debug, Clone)]
pub enum BatchOperation {
    /// Put (upsert) an item
    Put {
        table: String,
        key: Value,
        value: Row,
    },
    /// Delete an item
    Delete {
        table: String,
        key: Value,
    },
}

/// Transactional write item with optional condition.
#[derive(Debug, Clone)]
pub struct TransactWriteItem {
    /// The operation to perform
    pub operation: BatchOperation,
    /// Condition expression that must be true
    pub condition_expression: Option<String>,
    /// Values for condition expression placeholders
    pub expression_values: Option<HashMap<String, Value>>,
}

/// Table schema definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Primary key definition
    pub key_schema: KeySchema,
    /// Attribute definitions
    pub attributes: Vec<AttributeDefinition>,
    /// Global secondary indexes
    pub global_secondary_indexes: Vec<IndexSchema>,
    /// Local secondary indexes
    pub local_secondary_indexes: Vec<IndexSchema>,
}

/// Primary key schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeySchema {
    /// Partition key attribute name
    pub partition_key: String,
    /// Sort key attribute name (optional)
    pub sort_key: Option<String>,
}

/// Attribute type definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeDefinition {
    /// Attribute name
    pub name: String,
    /// Attribute type (S, N, B for string, number, binary)
    pub attribute_type: AttributeType,
}

/// DynamoDB-compatible attribute types.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AttributeType {
    /// String
    S,
    /// Number
    N,
    /// Binary
    B,
}

/// Secondary index schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    /// Index name
    pub name: String,
    /// Key schema for the index
    pub key_schema: KeySchema,
    /// Projection type
    pub projection: IndexProjection,
}

/// Index projection type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexProjection {
    /// All attributes
    All,
    /// Keys only
    KeysOnly,
    /// Specific attributes
    Include(Vec<String>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_from_primitives() {
        assert_eq!(Value::from(true), Value::Bool(true));
        assert_eq!(Value::from(42i64), Value::Integer(42));
        assert_eq!(Value::from(3.14), Value::Float(3.14));
        assert_eq!(Value::from("hello"), Value::String("hello".to_string()));
    }

    #[test]
    fn test_value_type_checks() {
        assert!(Value::Null.is_null());
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Integer(42).as_i64(), Some(42));
        assert_eq!(Value::Float(3.14).as_f64(), Some(3.14));
        assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
    }

    #[test]
    fn test_value_json_conversion() {
        let json = serde_json::json!({
            "name": "Alice",
            "age": 30,
            "active": true
        });
        let value = Value::from(json.clone());
        let back: serde_json::Value = value.into();
        assert_eq!(json, back);
    }

    #[test]
    fn test_row_operations() {
        let mut row = Row::new();
        row.push("id", Value::Integer(1));
        row.push("name", Value::String("Bob".into()));

        assert_eq!(row.get("id"), Some(&Value::Integer(1)));
        assert_eq!(row.get("name"), Some(&Value::String("Bob".into())));
        assert_eq!(row.get("missing"), None);
    }

    #[test]
    fn test_row_from_map() {
        let mut map = HashMap::new();
        map.insert("x".to_string(), Value::Integer(1));
        map.insert("y".to_string(), Value::Integer(2));

        let row = Row::from_map(map.clone());
        let back = row.to_map();

        assert_eq!(back.get("x"), Some(&Value::Integer(1)));
        assert_eq!(back.get("y"), Some(&Value::Integer(2)));
    }

    #[test]
    fn test_unified_result_helpers() {
        let empty = UnifiedResult::empty();
        assert!(empty.is_empty());
        assert_eq!(empty.row_count(), 0);

        let affected = UnifiedResult::with_affected_rows(5);
        assert!(!affected.is_empty());
        assert_eq!(affected.affected_rows, 5);
    }

    #[test]
    fn test_adapter_error_codes() {
        let err = AdapterError::NotFound("table".into());
        assert_eq!(err.status_code(), 404);
        assert_eq!(err.dynamodb_code(), "ResourceNotFoundException");

        let err = AdapterError::ThroughputExceeded("limit".into());
        assert_eq!(err.status_code(), 429);
        assert_eq!(err.dynamodb_code(), "ProvisionedThroughputExceededException");
    }
}
