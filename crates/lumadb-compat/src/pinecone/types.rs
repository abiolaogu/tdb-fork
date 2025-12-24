//! Pinecone API types - exact compatibility with Pinecone REST API

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Vector Types
// ============================================================================

/// Vector with ID, values, and optional metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vector {
    /// Unique ID for the vector
    pub id: String,
    /// The vector values (embeddings)
    pub values: Vec<f32>,
    /// Optional sparse vector values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_values: Option<SparseValues>,
    /// Optional metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

/// Sparse vector representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SparseValues {
    pub indices: Vec<u32>,
    pub values: Vec<f32>,
}

/// Metadata type (key-value pairs)
pub type Metadata = HashMap<String, serde_json::Value>;

// ============================================================================
// Upsert Types
// ============================================================================

/// Request to upsert vectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpsertRequest {
    /// List of vectors to upsert
    pub vectors: Vec<Vector>,
    /// Optional namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Response from upsert operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpsertResponse {
    /// Number of vectors upserted
    pub upserted_count: u64,
}

// ============================================================================
// Query Types
// ============================================================================

/// Request to query vectors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRequest {
    /// Query vector (required unless using ID)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<Vec<f32>>,
    /// Query by vector ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Number of results to return
    pub top_k: usize,
    /// Optional namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Optional filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<MetadataFilter>,
    /// Include values in response
    #[serde(default)]
    pub include_values: bool,
    /// Include metadata in response
    #[serde(default)]
    pub include_metadata: bool,
    /// Sparse vector for hybrid search
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_vector: Option<SparseValues>,
}

/// Metadata filter for query
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataFilter {
    /// Simple key-value match
    Simple(HashMap<String, serde_json::Value>),
    /// Complex filter with operators
    Complex(FilterExpression),
}

/// Complex filter expression
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterExpression {
    #[serde(rename = "$and", skip_serializing_if = "Option::is_none")]
    pub and: Option<Vec<FilterExpression>>,
    #[serde(rename = "$or", skip_serializing_if = "Option::is_none")]
    pub or: Option<Vec<FilterExpression>>,
    #[serde(flatten)]
    pub conditions: HashMap<String, FilterCondition>,
}

/// Filter condition operators
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FilterCondition {
    /// Exact match
    Eq(serde_json::Value),
    /// Complex operators
    Operators(FilterOperators),
}

/// Filter operators
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterOperators {
    #[serde(rename = "$eq", skip_serializing_if = "Option::is_none")]
    pub eq: Option<serde_json::Value>,
    #[serde(rename = "$ne", skip_serializing_if = "Option::is_none")]
    pub ne: Option<serde_json::Value>,
    #[serde(rename = "$gt", skip_serializing_if = "Option::is_none")]
    pub gt: Option<f64>,
    #[serde(rename = "$gte", skip_serializing_if = "Option::is_none")]
    pub gte: Option<f64>,
    #[serde(rename = "$lt", skip_serializing_if = "Option::is_none")]
    pub lt: Option<f64>,
    #[serde(rename = "$lte", skip_serializing_if = "Option::is_none")]
    pub lte: Option<f64>,
    #[serde(rename = "$in", skip_serializing_if = "Option::is_none")]
    pub in_: Option<Vec<serde_json::Value>>,
    #[serde(rename = "$nin", skip_serializing_if = "Option::is_none")]
    pub nin: Option<Vec<serde_json::Value>>,
    #[serde(rename = "$exists", skip_serializing_if = "Option::is_none")]
    pub exists: Option<bool>,
}

/// Response from query operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// Matched vectors with scores
    pub matches: Vec<ScoredVector>,
    /// Namespace of results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Usage information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// Scored vector match
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredVector {
    /// Vector ID
    pub id: String,
    /// Similarity score
    pub score: f32,
    /// Vector values (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<f32>>,
    /// Sparse values (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_values: Option<SparseValues>,
    /// Metadata (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

/// Usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    pub read_units: u64,
}

// ============================================================================
// Fetch Types
// ============================================================================

/// Request to fetch vectors by ID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRequest {
    /// Vector IDs to fetch
    pub ids: Vec<String>,
    /// Optional namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Response from fetch operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchResponse {
    /// Fetched vectors by ID
    pub vectors: HashMap<String, FetchedVector>,
    /// Namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Usage information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// Fetched vector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchedVector {
    /// Vector ID
    pub id: String,
    /// Vector values
    pub values: Vec<f32>,
    /// Sparse values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_values: Option<SparseValues>,
    /// Metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Metadata>,
}

// ============================================================================
// Delete Types
// ============================================================================

/// Request to delete vectors
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteRequest {
    /// Vector IDs to delete
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<String>>,
    /// Delete all vectors
    #[serde(default)]
    pub delete_all: bool,
    /// Optional namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Filter to select vectors to delete
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<MetadataFilter>,
}

/// Response from delete operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResponse {}

// ============================================================================
// Update Types
// ============================================================================

/// Request to update a vector
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateRequest {
    /// Vector ID to update
    pub id: String,
    /// New vector values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<f32>>,
    /// New sparse values
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse_values: Option<SparseValues>,
    /// Metadata to set (replaces existing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set_metadata: Option<Metadata>,
    /// Optional namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Response from update operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResponse {}

// ============================================================================
// Index Stats Types
// ============================================================================

/// Response from describe_index_stats
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DescribeIndexStatsResponse {
    /// Namespaces in the index
    pub namespaces: HashMap<String, NamespaceStats>,
    /// Vector dimension
    pub dimension: usize,
    /// Index fullness (0.0 to 1.0)
    pub index_fullness: f32,
    /// Total vector count
    pub total_vector_count: u64,
}

/// Stats for a namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceStats {
    pub vector_count: u64,
}

// ============================================================================
// List Types
// ============================================================================

/// Request to list vector IDs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListRequest {
    /// Optional prefix filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Maximum number of IDs to return
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    /// Pagination token
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination_token: Option<String>,
    /// Optional namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Response from list operation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListResponse {
    /// Vector IDs
    pub vectors: Vec<VectorId>,
    /// Pagination info
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pagination: Option<Pagination>,
    /// Namespace
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Usage information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// Vector ID wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorId {
    pub id: String,
}

/// Pagination info
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pagination {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

// ============================================================================
// Error Types
// ============================================================================

/// Pinecone error response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PineconeError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Vec<serde_json::Value>>,
}

impl PineconeError {
    pub fn not_found(message: &str) -> Self {
        Self {
            code: 5, // NOT_FOUND
            message: message.to_string(),
            details: None,
        }
    }

    pub fn invalid_argument(message: &str) -> Self {
        Self {
            code: 3, // INVALID_ARGUMENT
            message: message.to_string(),
            details: None,
        }
    }

    pub fn internal(message: &str) -> Self {
        Self {
            code: 13, // INTERNAL
            message: message.to_string(),
            details: None,
        }
    }
}
