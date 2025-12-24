//! Qdrant REST API Compatibility Layer
//!
//! Provides 100% drop-in replacement for Qdrant REST API.
//! Existing Qdrant clients can connect without modification.
//!
//! Supported endpoints:
//! - Collections: create, get, list, delete, update
//! - Points: upsert, get, delete, search, scroll
//! - Snapshots, cluster info, etc.

mod types;
mod handlers;
mod server;

pub use server::QdrantServer;
pub use types::*;
