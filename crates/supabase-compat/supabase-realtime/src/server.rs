//! WebSocket server for real-time connections
//!
//! Phoenix-compatible WebSocket server supporting:
//! - Channel joins and leaves
//! - Heartbeat/keepalive
//! - Message routing

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use supabase_common::config::RealtimeConfig;
use supabase_common::error::Result;

use crate::cdc::CdcListener;
use crate::channel::ChannelManager;

/// Phoenix protocol message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhoenixMessage {
    /// Topic (channel name)
    pub topic: String,
    /// Event type
    pub event: String,
    /// Payload
    pub payload: serde_json::Value,
    /// Reference for request/response matching
    #[serde(rename = "ref")]
    pub reference: Option<String>,
    /// Join reference
    pub join_ref: Option<String>,
}

impl PhoenixMessage {
    /// Create a new message
    pub fn new(topic: &str, event: &str, payload: serde_json::Value) -> Self {
        Self {
            topic: topic.to_string(),
            event: event.to_string(),
            payload,
            reference: Some(Uuid::new_v4().to_string()),
            join_ref: None,
        }
    }

    /// Create a reply message
    pub fn reply(original: &PhoenixMessage, status: &str, response: serde_json::Value) -> Self {
        Self {
            topic: original.topic.clone(),
            event: "phx_reply".to_string(),
            payload: serde_json::json!({
                "status": status,
                "response": response
            }),
            reference: original.reference.clone(),
            join_ref: original.join_ref.clone(),
        }
    }

    /// Create a heartbeat reply
    pub fn heartbeat_reply(original: &PhoenixMessage) -> Self {
        Self::reply(original, "ok", serde_json::json!({}))
    }
}

/// Connected client state
#[derive(Debug)]
pub struct ClientState {
    /// Client ID
    pub id: Uuid,
    /// Subscribed topics
    pub topics: Vec<String>,
    /// User ID (if authenticated)
    pub user_id: Option<String>,
    /// Last heartbeat
    pub last_heartbeat: std::time::Instant,
}

impl ClientState {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            topics: Vec::new(),
            user_id: None,
            last_heartbeat: std::time::Instant::now(),
        }
    }
}

impl Default for ClientState {
    fn default() -> Self {
        Self::new()
    }
}

/// Real-time WebSocket server
pub struct RealtimeServer {
    config: RealtimeConfig,
    channel_manager: Arc<ChannelManager>,
    cdc_listener: Arc<CdcListener>,
    clients: RwLock<HashMap<Uuid, ClientState>>,
}

impl RealtimeServer {
    /// Create a new realtime server
    pub fn new(config: &RealtimeConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            channel_manager: Arc::new(ChannelManager::new()),
            cdc_listener: Arc::new(CdcListener::new()),
            clients: RwLock::new(HashMap::new()),
        })
    }

    /// Get the channel manager
    pub fn channels(&self) -> Arc<ChannelManager> {
        self.channel_manager.clone()
    }

    /// Get the CDC listener
    pub fn cdc(&self) -> Arc<CdcListener> {
        self.cdc_listener.clone()
    }

    /// Register a new client
    pub fn register_client(&self) -> Uuid {
        let state = ClientState::new();
        let id = state.id;
        self.clients.write().insert(id, state);
        id
    }

    /// Remove a client
    pub fn unregister_client(&self, client_id: Uuid) {
        self.clients.write().remove(&client_id);
    }

    /// Handle an incoming message
    pub fn handle_message(&self, client_id: Uuid, msg: PhoenixMessage) -> Option<PhoenixMessage> {
        match msg.event.as_str() {
            "phx_join" => self.handle_join(client_id, &msg),
            "phx_leave" => self.handle_leave(client_id, &msg),
            "heartbeat" => Some(PhoenixMessage::heartbeat_reply(&msg)),
            "access_token" => self.handle_access_token(client_id, &msg),
            _ => self.handle_custom_event(client_id, &msg),
        }
    }

    fn handle_join(&self, client_id: Uuid, msg: &PhoenixMessage) -> Option<PhoenixMessage> {
        let mut clients = self.clients.write();
        if let Some(client) = clients.get_mut(&client_id) {
            if !client.topics.contains(&msg.topic) {
                client.topics.push(msg.topic.clone());
            }
        }

        // Create channel if needed
        self.channel_manager.channel(&msg.topic);

        Some(PhoenixMessage::reply(
            msg,
            "ok",
            serde_json::json!({
                "postgres_changes": []
            }),
        ))
    }

    fn handle_leave(&self, client_id: Uuid, msg: &PhoenixMessage) -> Option<PhoenixMessage> {
        let mut clients = self.clients.write();
        if let Some(client) = clients.get_mut(&client_id) {
            client.topics.retain(|t| t != &msg.topic);
        }

        Some(PhoenixMessage::reply(msg, "ok", serde_json::json!({})))
    }

    fn handle_access_token(&self, client_id: Uuid, msg: &PhoenixMessage) -> Option<PhoenixMessage> {
        // Extract and validate JWT from payload
        if let Some(token) = msg.payload.get("access_token").and_then(|v| v.as_str()) {
            let mut clients = self.clients.write();
            if let Some(client) = clients.get_mut(&client_id) {
                // In production, would validate JWT and extract user_id
                client.user_id = Some(format!("user_{}", client_id));
            }
        }

        Some(PhoenixMessage::reply(msg, "ok", serde_json::json!({})))
    }

    fn handle_custom_event(
        &self,
        _client_id: Uuid,
        msg: &PhoenixMessage,
    ) -> Option<PhoenixMessage> {
        // Handle broadcast and other custom events
        Some(PhoenixMessage::reply(msg, "ok", serde_json::json!({})))
    }

    /// Get count of connected clients
    pub fn client_count(&self) -> usize {
        self.clients.read().len()
    }

    /// Get all subscribed topics across all clients
    pub fn all_topics(&self) -> Vec<String> {
        self.channel_manager.topics()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RealtimeConfig {
        RealtimeConfig::default()
    }

    #[test]
    fn test_client_registration() {
        let server = RealtimeServer::new(&test_config()).unwrap();
        let client_id = server.register_client();
        assert_eq!(server.client_count(), 1);

        server.unregister_client(client_id);
        assert_eq!(server.client_count(), 0);
    }

    #[test]
    fn test_channel_join() {
        let server = RealtimeServer::new(&test_config()).unwrap();
        let client_id = server.register_client();

        let msg = PhoenixMessage::new("room:lobby", "phx_join", serde_json::json!({}));
        let reply = server.handle_message(client_id, msg);

        assert!(reply.is_some());
        assert!(server.all_topics().contains(&"room:lobby".to_string()));
    }
}
