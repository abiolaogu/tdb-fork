//! Pinecone REST API Compatibility Layer
//!
//! Provides 100% drop-in replacement for Pinecone REST API.
//! Existing Pinecone clients can connect without modification.
//!
//! Supported endpoints:
//! - /vectors/upsert
//! - /vectors/query
//! - /vectors/fetch
//! - /vectors/delete
//! - /vectors/update
//! - /describe_index_stats

mod types;
mod handlers;
mod server;

pub use server::PineconeServer;
pub use types::*;
