//! WebSocket Channel Management
//!
//! Provides Phoenix-compatible channel semantics for real-time subscriptions.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::subscription::{Subscription, SubscriptionFilter};

/// A real-time channel for pub/sub messaging
#[derive(Debug, Clone)]
pub struct Channel {
    /// Unique channel ID
    pub id: Uuid,
    /// Channel topic (e.g., "room:lobby", "realtime:public:posts")
    pub topic: String,
    /// Active subscriptions
    subscriptions: Arc<RwLock<HashMap<Uuid, Subscription>>>,
    /// Channel state
    pub state: ChannelState,
}

/// Channel connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelState {
    Closed,
    Joining,
    Joined,
    Leaving,
    Errored,
}

impl Channel {
    /// Create a new channel
    pub fn new(topic: &str) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic: topic.to_string(),
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            state: ChannelState::Closed,
        }
    }

    /// Subscribe to database changes
    pub fn on_postgres_changes(
        &self,
        event: &str,
        schema: &str,
        table: &str,
        filter: Option<&str>,
    ) -> Subscription {
        let sub = Subscription {
            id: Uuid::new_v4(),
            channel_id: self.id,
            event: event.to_string(),
            filter: SubscriptionFilter::PostgresChanges {
                schema: schema.to_string(),
                table: table.to_string(),
                filter: filter.map(|s| s.to_string()),
                event: event.to_string(),
            },
        };

        self.subscriptions.write().insert(sub.id, sub.clone());
        sub
    }

    /// Subscribe to presence events
    pub fn on_presence(&self, event: &str) -> Subscription {
        let sub = Subscription {
            id: Uuid::new_v4(),
            channel_id: self.id,
            event: event.to_string(),
            filter: SubscriptionFilter::Presence,
        };

        self.subscriptions.write().insert(sub.id, sub.clone());
        sub
    }

    /// Subscribe to broadcast events
    pub fn on_broadcast(&self, event: &str) -> Subscription {
        let sub = Subscription {
            id: Uuid::new_v4(),
            channel_id: self.id,
            event: event.to_string(),
            filter: SubscriptionFilter::Broadcast {
                event: event.to_string(),
            },
        };

        self.subscriptions.write().insert(sub.id, sub.clone());
        sub
    }

    /// Get all subscriptions
    pub fn subscriptions(&self) -> Vec<Subscription> {
        self.subscriptions.read().values().cloned().collect()
    }

    /// Remove a subscription
    pub fn unsubscribe(&self, subscription_id: Uuid) {
        self.subscriptions.write().remove(&subscription_id);
    }
}

/// Manages all active channels
pub struct ChannelManager {
    channels: RwLock<HashMap<String, Channel>>,
}

impl ChannelManager {
    /// Create a new channel manager
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a channel by topic
    pub fn channel(&self, topic: &str) -> Channel {
        let mut channels = self.channels.write();

        if let Some(channel) = channels.get(topic) {
            channel.clone()
        } else {
            let channel = Channel::new(topic);
            channels.insert(topic.to_string(), channel.clone());
            channel
        }
    }

    /// Remove a channel
    pub fn remove(&self, topic: &str) {
        self.channels.write().remove(topic);
    }

    /// List all active channel topics
    pub fn topics(&self) -> Vec<String> {
        self.channels.read().keys().cloned().collect()
    }

    /// Get the number of active channels
    pub fn count(&self) -> usize {
        self.channels.read().len()
    }
}

impl Default for ChannelManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_channel_creation() {
        let channel = Channel::new("room:lobby");
        assert_eq!(channel.topic, "room:lobby");
        assert_eq!(channel.state, ChannelState::Closed);
    }

    #[test]
    fn test_channel_manager() {
        let manager = ChannelManager::new();
        let channel = manager.channel("room:test");
        assert_eq!(channel.topic, "room:test");
        assert_eq!(manager.count(), 1);
    }
}
