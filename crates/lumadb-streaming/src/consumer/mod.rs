//! Consumer group management

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;

use lumadb_common::types::{Offset, PartitionId};

/// Consumer group for coordinating multiple consumers
pub struct ConsumerGroup {
    /// Group ID
    group_id: String,
    /// Committed offsets: topic -> partition -> offset
    offsets: DashMap<String, DashMap<PartitionId, AtomicI64>>,
    /// Group members
    members: RwLock<Vec<Member>>,
    /// Current generation
    generation: AtomicI64,
    /// Group state
    state: RwLock<GroupState>,
}

/// Group member
#[derive(Debug, Clone)]
struct Member {
    id: String,
    client_id: String,
    assigned_partitions: Vec<(String, PartitionId)>,
    last_heartbeat: i64,
}

/// Group state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GroupState {
    Empty,
    Stable,
    PreparingRebalance,
    CompletingRebalance,
    Dead,
}

impl ConsumerGroup {
    /// Create a new consumer group
    pub fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            offsets: DashMap::new(),
            members: RwLock::new(Vec::new()),
            generation: AtomicI64::new(0),
            state: RwLock::new(GroupState::Empty),
        }
    }

    /// Get committed offset for a topic/partition
    pub fn get_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.offsets
            .get(topic)
            .and_then(|partitions| {
                partitions
                    .get(&partition)
                    .map(|o| o.load(Ordering::SeqCst))
            })
    }

    /// Commit an offset
    pub fn commit_offset(&self, topic: &str, partition: PartitionId, offset: Offset) {
        let partitions = self
            .offsets
            .entry(topic.to_string())
            .or_insert_with(DashMap::new);

        partitions
            .entry(partition)
            .or_insert_with(|| AtomicI64::new(-1))
            .store(offset, Ordering::SeqCst);
    }

    /// Join the group
    pub fn join(&self, member_id: &str, client_id: &str) -> JoinResult {
        let mut members = self.members.write();
        let mut state = self.state.write();

        // Check if member already exists
        if let Some(member) = members.iter_mut().find(|m| m.id == member_id) {
            member.last_heartbeat = chrono::Utc::now().timestamp_millis();
            return JoinResult {
                generation: self.generation.load(Ordering::SeqCst),
                leader_id: members.first().map(|m| m.id.clone()),
                member_id: member_id.to_string(),
            };
        }

        // Add new member
        members.push(Member {
            id: member_id.to_string(),
            client_id: client_id.to_string(),
            assigned_partitions: Vec::new(),
            last_heartbeat: chrono::Utc::now().timestamp_millis(),
        });

        // Trigger rebalance
        *state = GroupState::PreparingRebalance;
        let new_gen = self.generation.fetch_add(1, Ordering::SeqCst) + 1;

        JoinResult {
            generation: new_gen,
            leader_id: members.first().map(|m| m.id.clone()),
            member_id: member_id.to_string(),
        }
    }

    /// Leave the group
    pub fn leave(&self, member_id: &str) {
        let mut members = self.members.write();
        members.retain(|m| m.id != member_id);

        if members.is_empty() {
            *self.state.write() = GroupState::Empty;
        } else {
            *self.state.write() = GroupState::PreparingRebalance;
            self.generation.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// Heartbeat from a member
    pub fn heartbeat(&self, member_id: &str) -> HeartbeatResult {
        let mut members = self.members.write();

        if let Some(member) = members.iter_mut().find(|m| m.id == member_id) {
            member.last_heartbeat = chrono::Utc::now().timestamp_millis();
            HeartbeatResult::Ok
        } else {
            HeartbeatResult::UnknownMember
        }
    }

    /// Sync group after rebalance
    pub fn sync(
        &self,
        member_id: &str,
        generation: i64,
        assignments: HashMap<String, Vec<(String, PartitionId)>>,
    ) -> SyncResult {
        let current_gen = self.generation.load(Ordering::SeqCst);

        if generation != current_gen {
            return SyncResult::StaleGeneration;
        }

        let mut members = self.members.write();
        let mut state = self.state.write();

        // Apply assignments
        for member in members.iter_mut() {
            if let Some(partitions) = assignments.get(&member.id) {
                member.assigned_partitions = partitions.clone();
            }
        }

        // Find this member's assignment
        let assignment = members
            .iter()
            .find(|m| m.id == member_id)
            .map(|m| m.assigned_partitions.clone())
            .unwrap_or_default();

        *state = GroupState::Stable;

        SyncResult::Assignment(assignment)
    }

    /// Get group ID
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get current generation
    pub fn generation(&self) -> i64 {
        self.generation.load(Ordering::SeqCst)
    }
}

/// Result of joining a group
#[derive(Debug)]
pub struct JoinResult {
    pub generation: i64,
    pub leader_id: Option<String>,
    pub member_id: String,
}

/// Result of heartbeat
#[derive(Debug)]
pub enum HeartbeatResult {
    Ok,
    UnknownMember,
    RebalanceInProgress,
}

/// Result of sync
#[derive(Debug)]
pub enum SyncResult {
    Assignment(Vec<(String, PartitionId)>),
    StaleGeneration,
    NotLeader,
}
