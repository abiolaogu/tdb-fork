//! Raft Consensus Implementation
//! Provides distributed strong consistency

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock, Mutex};
use tokio::time::interval;
use tracing::{info, debug, warn, error};
use serde::{Serialize, Deserialize};

/// Raft node state
#[derive(Debug, Clone, PartialEq)]
pub enum NodeState {
    Follower,
    Candidate,
    Leader,
}

/// Log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: RaftCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftCommand {
    Write { key: String, value: Vec<u8> },
    Delete { key: String },
    Noop,
}

/// Raft node configuration
#[derive(Debug, Clone)]
pub struct RaftConfig {
    pub node_id: u64,
    pub peers: Vec<String>,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
    pub heartbeat_interval_ms: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: vec![],
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
        }
    }
}

/// Raft RPC messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftMessage {
    RequestVote {
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
}

/// Raft node
pub struct RaftNode {
    config: RaftConfig,
    state: Arc<RwLock<RaftNodeState>>,
    log: Arc<RwLock<Vec<LogEntry>>>,
    state_machine: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    tx: mpsc::Sender<RaftMessage>,
    rx: Arc<Mutex<mpsc::Receiver<RaftMessage>>>,
}

struct RaftNodeState {
    node_state: NodeState,
    current_term: u64,
    voted_for: Option<u64>,
    commit_index: u64,
    last_applied: u64,
    leader_id: Option<u64>,
    last_heartbeat: Instant,
    
    // Leader state
    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,
    
    // Candidate state
    votes_received: u64,
}

impl RaftNode {
    pub fn new(config: RaftConfig) -> Self {
        let (tx, rx) = mpsc::channel(1000);
        
        Self {
            config,
            state: Arc::new(RwLock::new(RaftNodeState {
                node_state: NodeState::Follower,
                current_term: 0,
                voted_for: None,
                commit_index: 0,
                last_applied: 0,
                leader_id: None,
                last_heartbeat: Instant::now(),
                next_index: HashMap::new(),
                match_index: HashMap::new(),
                votes_received: 0,
            })),
            log: Arc::new(RwLock::new(vec![])),
            state_machine: Arc::new(RwLock::new(HashMap::new())),
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }
    
    /// Start the Raft node
    pub async fn start(&self) {
        info!("Starting Raft node {}", self.config.node_id);
        
        let state = self.state.clone();
        let config = self.config.clone();
        let log = self.log.clone();
        
        // Election timer
        tokio::spawn(async move {
            let mut election_interval = interval(Duration::from_millis(config.election_timeout_min_ms));
            
            loop {
                election_interval.tick().await;
                
                let mut state_guard = state.write().await;
                
                if state_guard.node_state == NodeState::Leader {
                    continue;
                }
                
                let elapsed = state_guard.last_heartbeat.elapsed();
                let timeout = Duration::from_millis(
                    config.election_timeout_min_ms + 
                    rand::random::<u64>() % (config.election_timeout_max_ms - config.election_timeout_min_ms)
                );
                
                if elapsed > timeout {
                    // Start election
                    state_guard.node_state = NodeState::Candidate;
                    state_guard.current_term += 1;
                    state_guard.voted_for = Some(config.node_id);
                    state_guard.votes_received = 1;
                    
                    info!("Node {} starting election for term {}", config.node_id, state_guard.current_term);
                    
                    // In a real implementation, send RequestVote RPCs to peers
                }
            }
        });
    }
    
    /// Handle incoming Raft message
    pub async fn handle_message(&self, msg: RaftMessage) -> Option<RaftMessage> {
        match msg {
            RaftMessage::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                let mut state = self.state.write().await;
                let log = self.log.read().await;
                
                let mut vote_granted = false;
                
                if term > state.current_term {
                    state.current_term = term;
                    state.node_state = NodeState::Follower;
                    state.voted_for = None;
                }
                
                if term >= state.current_term {
                    let can_vote = state.voted_for.is_none() || state.voted_for == Some(candidate_id);
                    
                    let our_last_term = log.last().map(|e| e.term).unwrap_or(0);
                    let our_last_index = log.len() as u64;
                    
                    let log_ok = last_log_term > our_last_term || 
                        (last_log_term == our_last_term && last_log_index >= our_last_index);
                    
                    if can_vote && log_ok {
                        state.voted_for = Some(candidate_id);
                        state.last_heartbeat = Instant::now();
                        vote_granted = true;
                        debug!("Node {} voting for {} in term {}", self.config.node_id, candidate_id, term);
                    }
                }
                
                Some(RaftMessage::RequestVoteResponse {
                    term: state.current_term,
                    vote_granted,
                })
            }
            
            RaftMessage::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                let mut state = self.state.write().await;
                let mut log = self.log.write().await;
                
                if term < state.current_term {
                    return Some(RaftMessage::AppendEntriesResponse {
                        term: state.current_term,
                        success: false,
                        match_index: 0,
                    });
                }
                
                state.current_term = term;
                state.node_state = NodeState::Follower;
                state.leader_id = Some(leader_id);
                state.last_heartbeat = Instant::now();
                
                // Check log consistency
                if prev_log_index > 0 {
                    if (prev_log_index as usize) > log.len() {
                        return Some(RaftMessage::AppendEntriesResponse {
                            term: state.current_term,
                            success: false,
                            match_index: log.len() as u64,
                        });
                    }
                    
                    let entry = &log[(prev_log_index - 1) as usize];
                    if entry.term != prev_log_term {
                        log.truncate((prev_log_index - 1) as usize);
                        return Some(RaftMessage::AppendEntriesResponse {
                            term: state.current_term,
                            success: false,
                            match_index: log.len() as u64,
                        });
                    }
                }
                
                // Append new entries
                for entry in entries {
                    if (entry.index as usize) <= log.len() {
                        // Entry exists, check for conflict
                        if log[(entry.index - 1) as usize].term != entry.term {
                            log.truncate((entry.index - 1) as usize);
                            log.push(entry);
                        }
                    } else {
                        log.push(entry);
                    }
                }
                
                // Update commit index
                if leader_commit > state.commit_index {
                    state.commit_index = std::cmp::min(leader_commit, log.len() as u64);
                }
                
                Some(RaftMessage::AppendEntriesResponse {
                    term: state.current_term,
                    success: true,
                    match_index: log.len() as u64,
                })
            }
            
            _ => None,
        }
    }
    
    /// Propose a write (leader only)
    pub async fn propose(&self, command: RaftCommand) -> Result<u64, String> {
        let state = self.state.read().await;
        
        if state.node_state != NodeState::Leader {
            return Err("Not the leader".to_string());
        }
        
        let term = state.current_term;
        drop(state);
        
        let mut log = self.log.write().await;
        let index = log.len() as u64 + 1;
        
        log.push(LogEntry {
            term,
            index,
            command,
        });
        
        Ok(index)
    }
    
    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        self.state.read().await.node_state == NodeState::Leader
    }
    
    /// Get current leader ID
    pub async fn leader_id(&self) -> Option<u64> {
        let state = self.state.read().await;
        if state.node_state == NodeState::Leader {
            Some(self.config.node_id)
        } else {
            state.leader_id
        }
    }
}
