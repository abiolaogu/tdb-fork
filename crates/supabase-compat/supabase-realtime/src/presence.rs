//! Presence tracking for real-time user status
//!
//! Tracks user presence across channels for features like:
//! - Who's online
//! - Cursor positions
//! - User status indicators

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Presence state for a user in a channel
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceState {
    /// User identifier
    pub user_id: String,
    /// Presence key (unique per device/tab)
    pub presence_ref: String,
    /// User's presence metadata
    pub state: serde_json::Value,
    /// When they joined
    pub joined_at: DateTime<Utc>,
    /// Last seen timestamp
    pub last_seen: DateTime<Utc>,
}

impl PresenceState {
    /// Create new presence state
    pub fn new(user_id: &str, state: serde_json::Value) -> Self {
        let now = Utc::now();
        Self {
            user_id: user_id.to_string(),
            presence_ref: Uuid::new_v4().to_string(),
            state,
            joined_at: now,
            last_seen: now,
        }
    }

    /// Update last seen
    pub fn touch(&mut self) {
        self.last_seen = Utc::now();
    }

    /// Update state
    pub fn update_state(&mut self, state: serde_json::Value) {
        self.state = state;
        self.touch();
    }
}

/// Presence event types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PresenceEvent {
    /// User joined
    Join {
        key: String,
        new_presences: Vec<PresenceState>,
    },
    /// User left
    Leave {
        key: String,
        left_presences: Vec<PresenceState>,
    },
    /// Full sync
    Sync {
        presences: HashMap<String, Vec<PresenceState>>,
    },
}

/// Manages presence for a channel
pub struct Presence {
    /// Channel topic
    topic: String,
    /// User presences by user_id
    presences: Arc<RwLock<HashMap<String, Vec<PresenceState>>>>,
    /// Callbacks for presence events
    callbacks: RwLock<Vec<Arc<dyn Fn(PresenceEvent) + Send + Sync>>>,
}

impl Presence {
    /// Create new presence tracker for a channel
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            presences: Arc::new(RwLock::new(HashMap::new())),
            callbacks: RwLock::new(Vec::new()),
        }
    }

    /// Track a user's presence
    pub fn track(&self, user_id: &str, state: serde_json::Value) -> PresenceState {
        let presence = PresenceState::new(user_id, state);

        {
            let mut presences = self.presences.write();
            presences
                .entry(user_id.to_string())
                .or_insert_with(Vec::new)
                .push(presence.clone());
        }

        // Emit join event
        self.emit(PresenceEvent::Join {
            key: user_id.to_string(),
            new_presences: vec![presence.clone()],
        });

        presence
    }

    /// Untrack a user's presence
    pub fn untrack(&self, user_id: &str, presence_ref: &str) {
        let removed = {
            let mut presences = self.presences.write();
            if let Some(user_presences) = presences.get_mut(user_id) {
                let idx = user_presences
                    .iter()
                    .position(|p| p.presence_ref == presence_ref);
                if let Some(idx) = idx {
                    Some(user_presences.remove(idx))
                } else {
                    None
                }
            } else {
                None
            }
        };

        if let Some(presence) = removed {
            self.emit(PresenceEvent::Leave {
                key: user_id.to_string(),
                left_presences: vec![presence],
            });
        }
    }

    /// Update a user's presence state
    pub fn update(&self, user_id: &str, presence_ref: &str, state: serde_json::Value) {
        let mut presences = self.presences.write();
        if let Some(user_presences) = presences.get_mut(user_id) {
            if let Some(presence) = user_presences
                .iter_mut()
                .find(|p| p.presence_ref == presence_ref)
            {
                presence.update_state(state);
            }
        }
    }

    /// Get all presences
    pub fn list(&self) -> HashMap<String, Vec<PresenceState>> {
        self.presences.read().clone()
    }

    /// Get presence for a specific user
    pub fn get(&self, user_id: &str) -> Option<Vec<PresenceState>> {
        self.presences.read().get(user_id).cloned()
    }

    /// Register a callback for presence events
    pub fn on_sync<F>(&self, callback: F)
    where
        F: Fn(PresenceEvent) + Send + Sync + 'static,
    {
        self.callbacks.write().push(Arc::new(callback));
    }

    /// Emit a presence event
    fn emit(&self, event: PresenceEvent) {
        for callback in self.callbacks.read().iter() {
            callback(event.clone());
        }
    }

    /// Sync all presences
    pub fn sync(&self) {
        let presences = self.presences.read().clone();
        self.emit(PresenceEvent::Sync { presences });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_presence_track() {
        let presence = Presence::new("room:lobby");
        let state = presence.track("user1", serde_json::json!({"status": "online"}));

        assert_eq!(state.user_id, "user1");

        let all = presence.list();
        assert_eq!(all.len(), 1);
        assert!(all.contains_key("user1"));
    }

    #[test]
    fn test_presence_untrack() {
        let presence = Presence::new("room:lobby");
        let state = presence.track("user1", serde_json::json!({"status": "online"}));

        presence.untrack("user1", &state.presence_ref);

        let all = presence.list();
        assert!(all.get("user1").map(|v| v.is_empty()).unwrap_or(true));
    }
}
