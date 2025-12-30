//! Subscription management for real-time events

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A subscription to real-time events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    /// Unique subscription ID
    pub id: Uuid,
    /// Channel this subscription belongs to
    pub channel_id: Uuid,
    /// Event type to listen for
    pub event: String,
    /// Filter for this subscription
    pub filter: SubscriptionFilter,
}

/// Filter types for subscriptions
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SubscriptionFilter {
    /// PostgreSQL change events
    PostgresChanges {
        schema: String,
        table: String,
        filter: Option<String>,
        event: String,
    },
    /// Presence events
    Presence,
    /// Broadcast events
    Broadcast { event: String },
}

impl Subscription {
    /// Check if this subscription matches an event
    pub fn matches(&self, schema: &str, table: &str, event_type: &str) -> bool {
        match &self.filter {
            SubscriptionFilter::PostgresChanges {
                schema: sub_schema,
                table: sub_table,
                event: sub_event,
                ..
            } => {
                (sub_schema == "*" || sub_schema == schema)
                    && (sub_table == "*" || sub_table == table)
                    && (sub_event == "*" || sub_event.eq_ignore_ascii_case(event_type))
            }
            SubscriptionFilter::Presence => event_type.starts_with("presence"),
            SubscriptionFilter::Broadcast { event } => event == "*" || event == event_type,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_matches() {
        let sub = Subscription {
            id: Uuid::new_v4(),
            channel_id: Uuid::new_v4(),
            event: "INSERT".to_string(),
            filter: SubscriptionFilter::PostgresChanges {
                schema: "public".to_string(),
                table: "users".to_string(),
                filter: None,
                event: "INSERT".to_string(),
            },
        };

        assert!(sub.matches("public", "users", "INSERT"));
        assert!(!sub.matches("public", "users", "DELETE"));
        assert!(!sub.matches("public", "posts", "INSERT"));
    }

    #[test]
    fn test_wildcard_subscription() {
        let sub = Subscription {
            id: Uuid::new_v4(),
            channel_id: Uuid::new_v4(),
            event: "*".to_string(),
            filter: SubscriptionFilter::PostgresChanges {
                schema: "public".to_string(),
                table: "*".to_string(),
                filter: None,
                event: "*".to_string(),
            },
        };

        assert!(sub.matches("public", "users", "INSERT"));
        assert!(sub.matches("public", "posts", "DELETE"));
    }
}
