//! Cluster management
//!
//! Provides:
//! - Placement driver
//! - Partition scheduling
//! - Membership management

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::sync::Arc;
use std::collections::HashMap;

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{info, debug};

use lumadb_common::config::ClusterConfig;
use lumadb_common::error::Result;
use lumadb_common::types::{NodeId, NodeInfo, NodeStatus, ClusterStatus};
use lumadb_raft::RaftEngine;

/// Cluster manager
pub struct ClusterManager {
    config: ClusterConfig,
    raft: Arc<RaftEngine>,
    nodes: DashMap<NodeId, NodeInfo>,
    partitions: DashMap<String, PartitionAssignment>,
    running: Arc<RwLock<bool>>,
}

/// Partition assignment
#[derive(Debug, Clone)]
struct PartitionAssignment {
    partition_id: String,
    leader: Option<NodeId>,
    replicas: Vec<NodeId>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub async fn new(config: &ClusterConfig, raft: Arc<RaftEngine>) -> Result<Self> {
        info!("Initializing cluster manager: {}", config.name);

        let manager = Self {
            config: config.clone(),
            raft,
            nodes: DashMap::new(),
            partitions: DashMap::new(),
            running: Arc::new(RwLock::new(true)),
        };

        // Register self as first node
        let self_node = NodeInfo {
            id: 1,
            address: "localhost:8080".to_string(),
            status: NodeStatus::Online,
            is_leader: true,
            last_heartbeat: chrono::Utc::now().timestamp_millis(),
            metadata: HashMap::new(),
        };
        manager.nodes.insert(1, self_node);

        Ok(manager)
    }

    /// Get cluster status
    pub fn status(&self) -> ClusterStatus {
        let nodes: Vec<NodeInfo> = self.nodes.iter().map(|e| e.value().clone()).collect();
        let healthy = nodes.iter().filter(|n| n.status == NodeStatus::Online).count();

        ClusterStatus {
            name: self.config.name.clone(),
            leader: nodes.iter().find(|n| n.is_leader).map(|n| n.id),
            nodes,
            healthy_nodes: healthy,
            total_partitions: self.partitions.len() as u32,
            under_replicated_partitions: 0,
        }
    }

    /// Add a node to the cluster
    pub async fn add_node(&self, address: &str) -> Result<NodeId> {
        let node_id = self.nodes.len() as NodeId + 1;

        let node = NodeInfo {
            id: node_id,
            address: address.to_string(),
            status: NodeStatus::Joining,
            is_leader: false,
            last_heartbeat: chrono::Utc::now().timestamp_millis(),
            metadata: HashMap::new(),
        };

        self.nodes.insert(node_id, node);
        info!("Added node {} at {}", node_id, address);

        Ok(node_id)
    }

    /// Remove a node from the cluster
    pub async fn remove_node(&self, node_id: NodeId) -> Result<()> {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            node.status = NodeStatus::Leaving;
        }

        // Reassign partitions
        self.rebalance().await?;

        self.nodes.remove(&node_id);
        info!("Removed node {}", node_id);

        Ok(())
    }

    /// Rebalance partitions across nodes
    pub async fn rebalance(&self) -> Result<()> {
        info!("Rebalancing partitions");

        let nodes: Vec<NodeId> = self
            .nodes
            .iter()
            .filter(|e| e.status == NodeStatus::Online)
            .map(|e| e.id)
            .collect();

        if nodes.is_empty() {
            return Ok(());
        }

        // Redistribute partitions evenly
        for (i, mut entry) in self.partitions.iter_mut().enumerate() {
            let leader_idx = i % nodes.len();
            entry.leader = Some(nodes[leader_idx]);

            // Assign replicas
            entry.replicas.clear();
            for j in 0..3.min(nodes.len()) {
                let replica_idx = (leader_idx + j) % nodes.len();
                entry.replicas.push(nodes[replica_idx]);
            }
        }

        Ok(())
    }

    /// Get nodes list
    pub fn nodes(&self) -> Vec<NodeInfo> {
        self.nodes.iter().map(|e| e.value().clone()).collect()
    }

    /// Update node heartbeat
    pub fn heartbeat(&self, node_id: NodeId) {
        if let Some(mut node) = self.nodes.get_mut(&node_id) {
            node.last_heartbeat = chrono::Utc::now().timestamp_millis();
            node.status = NodeStatus::Online;
        }
    }
}
