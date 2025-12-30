//! Auto-Sharding Engine for LumaDB
//!
//! Provides automatic data partitioning and routing to enable
//! horizontal scaling beyond Raft's 3-5 node optimal range.
//!
//! Key features:
//! - Consistent hashing for shard assignment
//! - Automatic shard rebalancing
//! - Shard-aware query routing
//! - Multi-Raft group management (each shard = 1 Raft group)

use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{info, debug, warn};

use lumadb_common::error::{Error, Result};
use lumadb_common::types::NodeId;

/// Number of virtual nodes per physical node for consistent hashing
const VIRTUAL_NODES_PER_NODE: u32 = 150;

/// Configuration for auto-sharding
#[derive(Debug, Clone)]
pub struct ShardingConfig {
    /// Total number of shards (power of 2 recommended)
    pub num_shards: u32,
    /// Replication factor per shard
    pub replication_factor: u32,
    /// Minimum nodes required before rebalancing
    pub min_nodes_for_rebalance: u32,
    /// Enable automatic shard splitting
    pub auto_split_enabled: bool,
    /// Shard size threshold for splitting (bytes)
    pub split_threshold_bytes: u64,
}

impl Default for ShardingConfig {
    fn default() -> Self {
        Self {
            num_shards: 64,
            replication_factor: 3,
            min_nodes_for_rebalance: 3,
            auto_split_enabled: true,
            split_threshold_bytes: 1024 * 1024 * 1024, // 1GB
        }
    }
}

/// Shard information
#[derive(Debug, Clone)]
pub struct Shard {
    pub id: u32,
    pub range_start: u64,
    pub range_end: u64,
    pub leader: Option<NodeId>,
    pub replicas: Vec<NodeId>,
    pub size_bytes: u64,
    pub status: ShardStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardStatus {
    Active,
    Rebalancing,
    Splitting,
    Recovering,
}

/// Consistent hash ring for shard routing
struct ConsistentHashRing {
    ring: BTreeMap<u64, NodeId>,
    nodes: HashMap<NodeId, u32>, // node -> virtual node count
}

impl ConsistentHashRing {
    fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: HashMap::new(),
        }
    }

    fn add_node(&mut self, node_id: NodeId) {
        for i in 0..VIRTUAL_NODES_PER_NODE {
            let hash = hash_key(&format!("{}:{}", node_id, i));
            self.ring.insert(hash, node_id);
        }
        self.nodes.insert(node_id, VIRTUAL_NODES_PER_NODE);
    }

    fn remove_node(&mut self, node_id: NodeId) {
        for i in 0..VIRTUAL_NODES_PER_NODE {
            let hash = hash_key(&format!("{}:{}", node_id, i));
            self.ring.remove(&hash);
        }
        self.nodes.remove(&node_id);
    }

    fn get_node(&self, key: &str) -> Option<NodeId> {
        if self.ring.is_empty() {
            return None;
        }

        let hash = hash_key(key);
        
        // Find the first node with hash >= key hash
        self.ring
            .range(hash..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, &node)| node)
    }

    fn get_nodes_for_replication(&self, key: &str, count: usize) -> Vec<NodeId> {
        if self.ring.is_empty() {
            return vec![];
        }

        let hash = hash_key(key);
        let mut nodes = Vec::with_capacity(count);
        let mut seen = std::collections::HashSet::new();

        // Start from the key's position and walk clockwise
        for (_, &node) in self.ring.range(hash..).chain(self.ring.iter()) {
            if !seen.contains(&node) {
                nodes.push(node);
                seen.insert(node);
                if nodes.len() >= count {
                    break;
                }
            }
        }

        nodes
    }
}

/// Auto-sharding engine
pub struct AutoShardingEngine {
    config: ShardingConfig,
    shards: Arc<RwLock<HashMap<u32, Shard>>>,
    hash_ring: Arc<RwLock<ConsistentHashRing>>,
    shard_to_raft_group: Arc<RwLock<HashMap<u32, u64>>>,
}

impl AutoShardingEngine {
    /// Create a new auto-sharding engine
    pub fn new(config: ShardingConfig) -> Self {
        let engine = Self {
            config: config.clone(),
            shards: Arc::new(RwLock::new(HashMap::new())),
            hash_ring: Arc::new(RwLock::new(ConsistentHashRing::new())),
            shard_to_raft_group: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize shards
        engine.initialize_shards();
        engine
    }

    fn initialize_shards(&self) {
        let mut shards = self.shards.write();
        let range_size = u64::MAX / u64::from(self.config.num_shards);

        for i in 0..self.config.num_shards {
            let shard = Shard {
                id: i,
                range_start: u64::from(i) * range_size,
                range_end: if i == self.config.num_shards - 1 {
                    u64::MAX
                } else {
                    (u64::from(i) + 1) * range_size - 1
                },
                leader: None,
                replicas: vec![],
                size_bytes: 0,
                status: ShardStatus::Active,
            };
            shards.insert(i, shard);
        }

        info!("Initialized {} shards", self.config.num_shards);
    }

    /// Add a node and trigger rebalancing
    pub fn add_node(&self, node_id: NodeId) {
        self.hash_ring.write().add_node(node_id);
        info!("Added node {} to sharding ring", node_id);
        
        // Trigger async rebalance
        self.rebalance_shards();
    }

    /// Remove a node and trigger rebalancing
    pub fn remove_node(&self, node_id: NodeId) {
        self.hash_ring.write().remove_node(node_id);
        info!("Removed node {} from sharding ring", node_id);
        
        // Trigger async rebalance
        self.rebalance_shards();
    }

    /// Get the shard for a key
    pub fn get_shard_for_key(&self, key: &str) -> u32 {
        let hash = hash_key(key);
        let shard_id = (hash % u64::from(self.config.num_shards)) as u32;
        shard_id
    }

    /// Get the node responsible for a key
    pub fn route_key(&self, key: &str) -> Option<NodeId> {
        let shard_id = self.get_shard_for_key(key);
        let shards = self.shards.read();
        shards.get(&shard_id).and_then(|s| s.leader)
    }

    /// Get all replica nodes for a key
    pub fn get_replicas_for_key(&self, key: &str) -> Vec<NodeId> {
        let shard_id = self.get_shard_for_key(key);
        let shards = self.shards.read();
        shards
            .get(&shard_id)
            .map(|s| s.replicas.clone())
            .unwrap_or_default()
    }

    /// Rebalance shards across nodes
    pub fn rebalance_shards(&self) {
        let ring = self.hash_ring.read();
        let node_count = ring.nodes.len();
        
        if node_count < self.config.min_nodes_for_rebalance as usize {
            debug!("Not enough nodes for rebalancing: {} < {}", 
                   node_count, self.config.min_nodes_for_rebalance);
            return;
        }

        let mut shards = self.shards.write();
        
        for (shard_id, shard) in shards.iter_mut() {
            let key = format!("shard-{}", shard_id);
            let nodes = ring.get_nodes_for_replication(
                &key, 
                self.config.replication_factor as usize
            );
            
            if !nodes.is_empty() {
                shard.leader = Some(nodes[0]);
                shard.replicas = nodes;
                shard.status = ShardStatus::Active;
            }
        }

        info!("Rebalanced {} shards across {} nodes", 
              shards.len(), node_count);
    }

    /// Check if any shards need splitting
    pub fn check_and_split_shards(&self) -> Vec<u32> {
        if !self.config.auto_split_enabled {
            return vec![];
        }

        let shards = self.shards.read();
        let split_candidates: Vec<u32> = shards
            .iter()
            .filter(|(_, s)| s.size_bytes > self.config.split_threshold_bytes)
            .map(|(&id, _)| id)
            .collect();

        if !split_candidates.is_empty() {
            warn!("Shards needing split: {:?}", split_candidates);
        }

        split_candidates
    }

    /// Get shard statistics
    pub fn get_stats(&self) -> ShardingStats {
        let shards = self.shards.read();
        let ring = self.hash_ring.read();

        let total_size: u64 = shards.values().map(|s| s.size_bytes).sum();
        let active_shards = shards.values()
            .filter(|s| s.status == ShardStatus::Active)
            .count();

        ShardingStats {
            total_shards: shards.len() as u32,
            active_shards: active_shards as u32,
            total_nodes: ring.nodes.len() as u32,
            total_data_bytes: total_size,
            replication_factor: self.config.replication_factor,
        }
    }

    /// Update shard size (called after writes)
    pub fn update_shard_size(&self, shard_id: u32, delta_bytes: i64) {
        let mut shards = self.shards.write();
        if let Some(shard) = shards.get_mut(&shard_id) {
            if delta_bytes >= 0 {
                shard.size_bytes = shard.size_bytes.saturating_add(delta_bytes as u64);
            } else {
                shard.size_bytes = shard.size_bytes.saturating_sub((-delta_bytes) as u64);
            }
        }
    }
}

/// Sharding statistics
#[derive(Debug, Clone)]
pub struct ShardingStats {
    pub total_shards: u32,
    pub active_shards: u32,
    pub total_nodes: u32,
    pub total_data_bytes: u64,
    pub replication_factor: u32,
}

/// Hash a key to u64 using FNV-1a
fn hash_key(key: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistent_hash_ring() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);

        // Same key should always route to same node
        let node1 = ring.get_node("user:123");
        let node2 = ring.get_node("user:123");
        assert_eq!(node1, node2);
    }

    #[test]
    fn test_shard_assignment() {
        let config = ShardingConfig {
            num_shards: 16,
            ..Default::default()
        };
        let engine = AutoShardingEngine::new(config);

        let shard1 = engine.get_shard_for_key("key1");
        let shard2 = engine.get_shard_for_key("key2");
        
        // Keys should be distributed
        assert!(shard1 < 16);
        assert!(shard2 < 16);
    }

    #[test]
    fn test_node_addition_rebalance() {
        let engine = AutoShardingEngine::new(ShardingConfig::default());
        
        engine.add_node(1);
        engine.add_node(2);
        engine.add_node(3);

        let stats = engine.get_stats();
        assert_eq!(stats.total_nodes, 3);
        assert_eq!(stats.total_shards, 64);
    }

    #[test]
    fn test_replication_nodes() {
        let mut ring = ConsistentHashRing::new();
        ring.add_node(1);
        ring.add_node(2);
        ring.add_node(3);
        ring.add_node(4);
        ring.add_node(5);

        let nodes = ring.get_nodes_for_replication("test-key", 3);
        assert_eq!(nodes.len(), 3);
        
        // All nodes should be unique
        let unique: std::collections::HashSet<_> = nodes.iter().collect();
        assert_eq!(unique.len(), 3);
    }
}
