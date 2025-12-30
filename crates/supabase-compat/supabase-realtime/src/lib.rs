//! Supabase Realtime Engine
//!
//! Provides Phoenix-compatible real-time functionality:
//! - WebSocket connections with channel subscriptions
//! - PostgreSQL-compatible Change Data Capture (CDC)
//! - Presence tracking for user status
//! - Broadcast for pub/sub messaging

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod cdc;
pub mod channel;
pub mod presence;
pub mod server;
pub mod subscription;

pub use cdc::{CdcListener, ChangeEvent};
pub use channel::{Channel, ChannelManager};
pub use presence::{Presence, PresenceState};
pub use server::RealtimeServer;
pub use subscription::{Subscription, SubscriptionFilter};
