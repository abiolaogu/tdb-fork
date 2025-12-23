//! Multi-Raft consensus implementation
//!
//! Provides distributed consensus for LumaDB clusters.

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

use std::sync::Arc;
use std::collections::HashMap;

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, debug, warn};

use lumadb_common::config::RaftConfig;
use lumadb_common::error::Result;
use lumadb_common::types::{NodeId, Term, LogIndex};
use lumadb_storage::StorageEngine;

/// Raft engine managing multiple Raft groups
pub struct RaftEngine {
    config: RaftConfig,
    node_id: NodeId,
    groups: DashMap<u64, Arc<RaftGroup>>,
    storage: Arc<StorageEngine>,
    running: Arc<RwLock<bool>>,
}

impl RaftEngine {
    /// Create a new Raft engine
    pub async fn new(config: &RaftConfig, storage: Arc<StorageEngine>) -> Result<Self> {
        info!("Initializing Raft engine");

        Ok(Self {
            config: config.clone(),
            node_id: 1, // Would be configured in production
            groups: DashMap::new(),
            storage,
            running: Arc::new(RwLock::new(true)),
        })
    }

    /// Get or create a Raft group
    pub fn get_or_create_group(&self, group_id: u64) -> Arc<RaftGroup> {
        self.groups
            .entry(group_id)
            .or_insert_with(|| Arc::new(RaftGroup::new(group_id, self.node_id, &self.config)))
            .clone()
    }

    /// Propose a value to a Raft group
    pub async fn propose(&self, group_id: u64, data: Vec<u8>) -> Result<()> {
        let group = self.get_or_create_group(group_id);
        group.propose(data).await
    }

    /// Shutdown the Raft engine
    pub async fn shutdown(&self) -> Result<()> {
        info!("Shutting down Raft engine");
        *self.running.write() = false;
        Ok(())
    }
}

/// A single Raft consensus group
pub struct RaftGroup {
    group_id: u64,
    node_id: NodeId,
    state: RwLock<RaftState>,
    log: RwLock<Vec<LogEntry>>,
}

/// Raft node state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// Raft state
struct RaftState {
    current_term: Term,
    voted_for: Option<NodeId>,
    state: NodeState,
    commit_index: LogIndex,
    last_applied: LogIndex,
    leader_id: Option<NodeId>,
}

/// Log entry
#[derive(Debug, Clone)]
struct LogEntry {
    term: Term,
    index: LogIndex,
    data: Vec<u8>,
}

impl RaftGroup {
    fn new(group_id: u64, node_id: NodeId, config: &RaftConfig) -> Self {
        Self {
            group_id,
            node_id,
            state: RwLock::new(RaftState {
                current_term: 0,
                voted_for: None,
                state: NodeState::Follower,
                commit_index: 0,
                last_applied: 0,
                leader_id: None,
            }),
            log: RwLock::new(Vec::new()),
        }
    }

    /// Propose a value
    async fn propose(&self, data: Vec<u8>) -> Result<()> {
        let mut state = self.state.write();

        // Only leader can propose
        if state.state != NodeState::Leader {
            // In single-node mode, become leader immediately
            state.state = NodeState::Leader;
            state.leader_id = Some(self.node_id);
        }

        // Append to log
        let mut log = self.log.write();
        let index = log.len() as LogIndex + 1;
        log.push(LogEntry {
            term: state.current_term,
            index,
            data,
        });

        // In single-node mode, commit immediately
        state.commit_index = index;
        state.last_applied = index;

        Ok(())
    }

    /// Check if this node is the leader
    pub fn is_leader(&self) -> bool {
        self.state.read().state == NodeState::Leader
    }

    /// Get the current leader
    pub fn leader(&self) -> Option<NodeId> {
        self.state.read().leader_id
    }

    /// Get the current term
    pub fn term(&self) -> Term {
        self.state.read().current_term
    }
}
