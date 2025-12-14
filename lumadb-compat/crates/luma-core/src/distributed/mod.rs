//! Distributed module
//! Raft consensus and cluster management

pub mod raft;

pub use raft::{RaftNode, RaftConfig, RaftCommand, RaftMessage, NodeState};
