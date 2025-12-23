//! Main streaming engine implementation

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{info, warn, debug};

use lumadb_common::config::StreamingConfig;
use lumadb_common::error::{Result, Error};
use lumadb_common::types::{
    Offset, PartitionId, Record, RecordMetadata, TopicConfig, TopicMetadata, PartitionMetadata,
};

use crate::consumer::ConsumerGroup;
use crate::log::{PartitionLog, Segment};

/// High-performance streaming engine
pub struct StreamingEngine {
    /// Configuration
    config: StreamingConfig,
    /// Topics
    topics: DashMap<String, Topic>,
    /// Consumer groups
    consumer_groups: DashMap<String, Arc<ConsumerGroup>>,
    /// Running state
    running: Arc<RwLock<bool>>,
}

/// A topic with multiple partitions
struct Topic {
    /// Topic configuration
    config: TopicConfig,
    /// Partitions
    partitions: Vec<Arc<Partition>>,
}

/// A single partition
struct Partition {
    /// Partition ID
    id: PartitionId,
    /// Append-only log
    log: PartitionLog,
}

impl StreamingEngine {
    /// Create a new streaming engine
    pub async fn new(
        config: &StreamingConfig,
        _storage: Arc<lumadb_storage::StorageEngine>,
        _raft: Arc<crate::RaftStub>,
    ) -> Result<Self> {
        info!("Initializing streaming engine with thread-per-core architecture");

        Ok(Self {
            config: config.clone(),
            topics: DashMap::new(),
            consumer_groups: DashMap::new(),
            running: Arc::new(RwLock::new(true)),
        })
    }

    /// Run the streaming engine
    pub async fn run(&self) -> Result<()> {
        info!("Starting streaming engine...");

        while *self.running.read() {
            // Main event loop - in production this would use io_uring or epoll
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        Ok(())
    }

    /// Shutdown the streaming engine
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down streaming engine...");
        *self.running.write() = false;
        Ok(())
    }

    // ========================================================================
    // Topic Operations
    // ========================================================================

    /// Create a new topic
    pub async fn create_topic(&self, config: TopicConfig) -> Result<()> {
        info!("Creating topic: {} with {} partitions", config.name, config.partitions);

        if self.topics.contains_key(&config.name) {
            return Err(Error::Internal(format!("Topic {} already exists", config.name)));
        }

        let mut partitions = Vec::new();
        for id in 0..config.partitions as i32 {
            partitions.push(Arc::new(Partition {
                id,
                log: PartitionLog::new(id, self.config.segment_size_bytes),
            }));
        }

        let topic = Topic {
            config: config.clone(),
            partitions,
        };

        self.topics.insert(config.name.clone(), topic);

        Ok(())
    }

    /// List all topics
    pub async fn list_topics(&self) -> Result<Vec<TopicMetadata>> {
        let mut topics = Vec::new();

        for entry in self.topics.iter() {
            let topic = entry.value();
            let partitions: Vec<PartitionMetadata> = topic
                .partitions
                .iter()
                .map(|p| PartitionMetadata {
                    id: p.id,
                    leader: Some(1), // Current node is leader
                    replicas: vec![1],
                    isr: vec![1],
                    low_watermark: p.log.low_watermark(),
                    high_watermark: p.log.high_watermark(),
                })
                .collect();

            topics.push(TopicMetadata {
                name: topic.config.name.clone(),
                partitions,
                is_internal: false,
            });
        }

        Ok(topics)
    }

    /// Get topic metadata
    pub async fn get_topic(&self, name: &str) -> Result<Option<TopicMetadata>> {
        if let Some(entry) = self.topics.get(name) {
            let topic = entry.value();
            let partitions: Vec<PartitionMetadata> = topic
                .partitions
                .iter()
                .map(|p| PartitionMetadata {
                    id: p.id,
                    leader: Some(1),
                    replicas: vec![1],
                    isr: vec![1],
                    low_watermark: p.log.low_watermark(),
                    high_watermark: p.log.high_watermark(),
                })
                .collect();

            Ok(Some(TopicMetadata {
                name: topic.config.name.clone(),
                partitions,
                is_internal: false,
            }))
        } else {
            Ok(None)
        }
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        info!("Deleting topic: {}", name);

        self.topics
            .remove(name)
            .ok_or_else(|| Error::Internal(format!("Topic {} not found", name)))?;

        Ok(())
    }

    // ========================================================================
    // Produce Operations
    // ========================================================================

    /// Produce records to a topic
    pub async fn produce(
        &self,
        topic: &str,
        records: &[ProduceRecord],
        acks: i8,
    ) -> Result<Vec<RecordMetadata>> {
        let topic_entry = self.topics.get(topic)
            .ok_or_else(|| Error::Internal(format!("Topic {} not found", topic)))?;

        let mut results = Vec::new();

        for record in records {
            // Determine partition
            let partition = if let Some(p) = record.partition {
                p
            } else if let Some(ref key) = record.key {
                // Hash key to partition
                let hash = xxhash_rust::xxh3::xxh3_64(key.as_bytes());
                (hash % topic_entry.partitions.len() as u64) as i32
            } else {
                // Round-robin
                rand::random::<i32>().abs() % topic_entry.partitions.len() as i32
            };

            // Get partition
            let partition_ref = topic_entry
                .partitions
                .get(partition as usize)
                .ok_or_else(|| Error::Internal(format!("Partition {} not found", partition)))?;

            // Create record bytes
            let value_bytes = serde_json::to_vec(&record.value)?;
            let record_bytes = Record {
                key: record.key.as_ref().map(|k| bytes::Bytes::from(k.clone())),
                value: bytes::Bytes::from(value_bytes),
                headers: record.headers.clone().unwrap_or_default(),
                timestamp: chrono::Utc::now().timestamp_millis(),
                partition: Some(partition),
                offset: None,
            };

            // Append to log
            let offset = partition_ref.log.append(&record_bytes)?;

            results.push(RecordMetadata {
                topic: topic.to_string(),
                partition,
                offset,
                timestamp: record_bytes.timestamp,
            });

            // Record metrics
            lumadb_common::metrics::record_messages_produced(topic, 1, record_bytes.value.len() as u64);
        }

        // Handle acks (simplified)
        if acks == -1 {
            // Wait for all replicas - not implemented in single-node mode
        }

        Ok(results)
    }

    // ========================================================================
    // Consume Operations
    // ========================================================================

    /// Consume records from a topic
    pub async fn consume(
        &self,
        topic: &str,
        group_id: Option<&str>,
        offset: Option<&str>,
        max_records: usize,
        timeout_ms: u64,
    ) -> Result<Vec<ConsumeRecord>> {
        let topic_entry = self.topics.get(topic)
            .ok_or_else(|| Error::Internal(format!("Topic {} not found", topic)))?;

        let mut results = Vec::new();
        let start_offset = Self::parse_offset(offset)?;

        // Consume from each partition
        for partition in &topic_entry.partitions {
            let partition_offset = if let Some(gid) = group_id {
                // Get committed offset for consumer group
                self.get_committed_offset(gid, topic, partition.id)
                    .await?
                    .unwrap_or(start_offset)
            } else {
                start_offset
            };

            let records = partition.log.fetch(partition_offset, max_records)?;

            for (offset, record) in records {
                results.push(ConsumeRecord {
                    topic: topic.to_string(),
                    partition: partition.id,
                    offset,
                    timestamp: record.timestamp,
                    key: record.key.map(|b| String::from_utf8_lossy(&b).to_string()),
                    value: serde_json::from_slice(&record.value)?,
                    headers: record.headers,
                });
            }

            if results.len() >= max_records {
                break;
            }
        }

        // Record metrics
        if let Some(gid) = group_id {
            lumadb_common::metrics::record_messages_consumed(
                topic,
                gid,
                results.len() as u64,
                results.iter().map(|r| serde_json::to_vec(&r.value).unwrap_or_default().len() as u64).sum(),
            );
        }

        Ok(results)
    }

    /// Parse offset string
    fn parse_offset(offset: Option<&str>) -> Result<Offset> {
        match offset {
            None | Some("latest") => Ok(-1), // Latest
            Some("earliest") => Ok(0),
            Some(s) => s.parse().map_err(|_| Error::Internal("Invalid offset".to_string())),
        }
    }

    /// Get committed offset for a consumer group
    async fn get_committed_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Option<Offset>> {
        if let Some(group) = self.consumer_groups.get(group_id) {
            Ok(group.get_offset(topic, partition))
        } else {
            Ok(None)
        }
    }

    /// Commit offsets for a consumer group
    pub async fn commit_offsets(
        &self,
        group_id: &str,
        offsets: &[(String, PartitionId, Offset)],
    ) -> Result<()> {
        let group = self
            .consumer_groups
            .entry(group_id.to_string())
            .or_insert_with(|| Arc::new(ConsumerGroup::new(group_id)));

        for (topic, partition, offset) in offsets {
            group.commit_offset(topic, *partition, *offset);
        }

        Ok(())
    }
}

/// Record to produce (from REST API)
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ProduceRecord {
    pub key: Option<String>,
    pub value: serde_json::Value,
    pub headers: Option<HashMap<String, String>>,
    pub partition: Option<i32>,
}

/// Record from consume (for REST API)
#[derive(Debug, Clone, serde::Serialize)]
pub struct ConsumeRecord {
    pub topic: String,
    pub partition: PartitionId,
    pub offset: Offset,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: serde_json::Value,
    pub headers: HashMap<String, String>,
}

/// Stub for Raft engine (will be replaced with real implementation)
pub struct RaftStub;
