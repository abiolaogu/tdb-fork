//! Common type definitions for LumaDB

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unique identifier type
pub type Id = u64;

/// Timestamp in milliseconds since Unix epoch
pub type Timestamp = i64;

/// Offset type for log positions
pub type Offset = i64;

/// Partition identifier
pub type PartitionId = i32;

/// Node identifier
pub type NodeId = u64;

/// Term number for Raft consensus
pub type Term = u64;

/// Log index for Raft consensus
pub type LogIndex = u64;

// ============================================================================
// Record Types
// ============================================================================

/// A record in a topic/partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Optional key for partitioning
    pub key: Option<Bytes>,
    /// Record value
    pub value: Bytes,
    /// Record headers
    pub headers: HashMap<String, String>,
    /// Timestamp
    pub timestamp: Timestamp,
    /// Partition (assigned after produce)
    pub partition: Option<PartitionId>,
    /// Offset (assigned after produce)
    pub offset: Option<Offset>,
}

impl Record {
    pub fn new(value: impl Into<Bytes>) -> Self {
        Self {
            key: None,
            value: value.into(),
            headers: HashMap::new(),
            timestamp: chrono::Utc::now().timestamp_millis(),
            partition: None,
            offset: None,
        }
    }

    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = timestamp;
        self
    }
}

/// Metadata about a produced record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordMetadata {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: PartitionId,
    /// Offset in the partition
    pub offset: Offset,
    /// Timestamp
    pub timestamp: Timestamp,
}

// ============================================================================
// Topic Types
// ============================================================================

/// Topic configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub partitions: u32,
    /// Replication factor
    pub replication_factor: u32,
    /// Retention time in milliseconds
    pub retention_ms: Option<u64>,
    /// Retention size in bytes
    pub retention_bytes: Option<u64>,
    /// Segment size in bytes
    pub segment_bytes: Option<u64>,
    /// Additional configuration
    pub config: HashMap<String, String>,
}

impl TopicConfig {
    pub fn new(name: impl Into<String>, partitions: u32, replication_factor: u32) -> Self {
        Self {
            name: name.into(),
            partitions,
            replication_factor,
            retention_ms: None,
            retention_bytes: None,
            segment_bytes: None,
            config: HashMap::new(),
        }
    }
}

/// Topic metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Topic name
    pub name: String,
    /// Partitions
    pub partitions: Vec<PartitionMetadata>,
    /// Is internal topic
    pub is_internal: bool,
}

/// Partition metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMetadata {
    /// Partition ID
    pub id: PartitionId,
    /// Leader node
    pub leader: Option<NodeId>,
    /// Replica nodes
    pub replicas: Vec<NodeId>,
    /// In-sync replica nodes
    pub isr: Vec<NodeId>,
    /// Low watermark (earliest offset)
    pub low_watermark: Offset,
    /// High watermark (latest offset)
    pub high_watermark: Offset,
}

// ============================================================================
// Document Types
// ============================================================================

/// A document stored in a collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Document ID
    #[serde(rename = "_id")]
    pub id: String,
    /// Document data
    #[serde(flatten)]
    pub data: serde_json::Value,
}

impl Document {
    pub fn new(data: serde_json::Value) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        Self { id, data }
    }

    pub fn with_id(id: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            id: id.into(),
            data,
        }
    }

    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.data.get(key)
    }
}

/// Collection metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionMetadata {
    /// Collection name
    pub name: String,
    /// Document count
    pub count: usize,
    /// Size in bytes
    pub size_bytes: u64,
    /// Indexes
    pub indexes: Vec<IndexMetadata>,
    /// Schema (optional)
    pub schema: Option<serde_json::Value>,
}

/// Collection info with vector dimensions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    /// Collection name
    pub name: String,
    /// Document count
    pub count: usize,
    /// Size in bytes
    pub size_bytes: u64,
    /// Vector dimensions (if vector index exists)
    pub vector_dimensions: Option<usize>,
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index name
    pub name: String,
    /// Index type
    pub index_type: IndexType,
    /// Indexed fields
    pub fields: Vec<String>,
    /// Is unique
    pub unique: bool,
}

/// Index types
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    /// B-tree index
    BTree,
    /// Hash index
    Hash,
    /// Full-text search index
    FullText,
    /// Vector similarity index
    Vector,
    /// Geospatial index
    Geo,
}

// ============================================================================
// Vector Types
// ============================================================================

/// Vector search result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorSearchResult {
    /// Document ID
    pub id: String,
    /// Similarity score
    pub score: f32,
    /// Document data
    pub document: Option<Document>,
    /// Vector (if requested)
    pub vector: Option<Vec<f32>>,
}

/// Vector index configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VectorIndexConfig {
    /// Number of dimensions
    pub dimensions: usize,
    /// Distance metric
    pub metric: DistanceMetric,
    /// HNSW M parameter
    pub hnsw_m: usize,
    /// HNSW ef_construction parameter
    pub hnsw_ef_construction: usize,
}

/// Distance metrics for vector similarity
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Euclidean distance (L2)
    Euclidean,
    /// Cosine similarity
    Cosine,
    /// Dot product
    DotProduct,
    /// Manhattan distance (L1)
    Manhattan,
}

// ============================================================================
// Query Types
// ============================================================================

/// Query execution plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryPlan {
    /// Plan nodes
    pub nodes: Vec<PlanNode>,
    /// Estimated cost
    pub estimated_cost: f64,
    /// Estimated rows
    pub estimated_rows: u64,
}

/// Plan node in query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanNode {
    /// Node type
    pub node_type: String,
    /// Description
    pub description: String,
    /// Children nodes
    pub children: Vec<PlanNode>,
    /// Estimated cost
    pub cost: f64,
    /// Estimated rows
    pub rows: u64,
}

// ============================================================================
// Cluster Types
// ============================================================================

/// Node information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID
    pub id: NodeId,
    /// Node address
    pub address: String,
    /// Node status
    pub status: NodeStatus,
    /// Is leader
    pub is_leader: bool,
    /// Last heartbeat timestamp
    pub last_heartbeat: Timestamp,
    /// Node metadata
    pub metadata: HashMap<String, String>,
}

/// Node status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeStatus {
    /// Node is online and healthy
    Online,
    /// Node is offline
    Offline,
    /// Node is suspected to be down
    Suspect,
    /// Node is being drained
    Draining,
    /// Node is joining the cluster
    Joining,
    /// Node is leaving the cluster
    Leaving,
}

/// Cluster status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterStatus {
    /// Cluster name
    pub name: String,
    /// Current leader
    pub leader: Option<NodeId>,
    /// All nodes
    pub nodes: Vec<NodeInfo>,
    /// Number of healthy nodes
    pub healthy_nodes: usize,
    /// Total partitions
    pub total_partitions: u32,
    /// Under-replicated partitions
    pub under_replicated_partitions: u32,
}
