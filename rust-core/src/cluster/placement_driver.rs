use std::collections::{HashMap, BTreeMap};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

/// Placement Driver - cluster brain
pub struct PlacementDriver {
    // cluster_id: u64,
    nodes: Arc<RwLock<HashMap<u64, NodeInfo>>>,
    regions: Arc<RwLock<BTreeMap<Vec<u8>, Region>>>,
    // tso: Arc<TimestampOracle>, // From txn module
    // scheduler: Arc<Scheduler>,
}

#[derive(Clone)]
pub struct Region {
    pub id: u64,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub peers: Vec<Peer>,
    pub leader: Option<u64>,
}

#[derive(Clone)]
pub struct Peer {
    pub id: u64,
    pub store_id: u64,
    // pub role: PeerRole,
}

#[derive(Clone)]
pub struct NodeInfo {
    pub id: u64,
    pub address: String,
    pub capacity: u64,
    pub available: u64,
    pub last_heartbeat: Instant,
}

impl PlacementDriver {
    pub fn new() -> Self {
        Self {
            nodes: Arc::new(RwLock::new(HashMap::new())),
            regions: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn register_node(&self, node: NodeInfo) {
        self.nodes.write().await.insert(node.id, node);
    }

    pub async fn get_region(&self, key: &[u8]) -> Option<Region> {
        let regions = self.regions.read().await;
        regions.range(..=key.to_vec())
            .last()
            .filter(|(_, r)| key < r.end_key.as_slice() || r.end_key.is_empty())
            .map(|(_, r)| r.clone())
    }
}
