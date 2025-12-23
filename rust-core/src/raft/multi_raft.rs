use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use raft::{
    prelude::*, Config as RaftConfig, RawNode, storage::MemStorage,
    eraftpb::{Entry, Message, EntryType, ConfChange, ConfChangeV2},
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum RaftError {
    #[error("Raft internal error: {0}")]
    Raft(#[from] raft::Error),
    #[error("Group {0} not found")]
    GroupNotFound(u64),
    #[error("Proposal dropped")]
    ProposalDropped,
}

pub struct Proposal {
    pub group_id: u64,
    pub data: Vec<u8>,
    pub callback: tokio::sync::oneshot::Sender<Result<ProposeResult, RaftError>>,
}

pub struct ProposeResult {
    pub index: u64,
    pub term: u64,
}

/// Multi-Raft manager - coordinates multiple Raft groups
pub struct MultiRaft {
    node_id: u64,
    groups: Arc<RwLock<HashMap<u64, RaftGroup>>>,
    // router: Arc<RaftRouter>, // To be implemented
    // transport: Arc<RaftTransport>, // To be implemented
    // storage: Arc<dyn RaftStorage>, // To be implemented
    
    // Channels
    proposal_tx: mpsc::Sender<Proposal>,
    // apply_tx: mpsc::Sender<ApplyBatch>,
}

pub struct RaftGroup {
    pub group_id: u64,
    pub raw_node: RawNode<MemStorage>,
    pub peers: Vec<u64>,
}

impl MultiRaft {
    pub fn new(node_id: u64) -> Self {
        let (proposal_tx, _proposal_rx) = mpsc::channel(10000); // Worker loop placeholder
        
        Self {
            node_id,
            groups: Arc::new(RwLock::new(HashMap::new())),
            proposal_tx,
        }
    }

    pub async fn create_group(&self, group_id: u64, peers: Vec<u64>) -> Result<(), RaftError> {
        let config = RaftConfig {
            id: self.node_id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };
        config.validate().unwrap();

        let storage = MemStorage::new();
        // Initialize peers in storage for bootstrapping
        // In real impl, we'd use apply_conf_change or initial_state
        
        let raw_node = RawNode::new(&config, storage, &raft::default_logger())?;

        let group = RaftGroup {
            group_id,
            raw_node,
            peers,
        };

        self.groups.write().await.insert(group_id, group);
        Ok(())
    }

    pub async fn step(&self, group_id: u64, msg: Message) -> Result<(), RaftError> {
        let mut groups = self.groups.write().await;
        if let Some(group) = groups.get_mut(&group_id) {
            group.raw_node.step(msg)?;
            Ok(())
        } else {
            Err(RaftError::GroupNotFound(group_id))
        }
    }

    pub async fn tick(&self) {
        let mut groups = self.groups.write().await;
        for group in groups.values_mut() {
            group.raw_node.tick();
        }
    }
}
