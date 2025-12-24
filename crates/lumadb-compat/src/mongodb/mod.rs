//! MongoDB Wire Protocol Compatibility Layer with Atlas Vector Search
//!
//! Provides drop-in replacement for MongoDB clients with full $vectorSearch support.
//! Existing MongoDB drivers can connect without modification.
//!
//! Supported features:
//! - MongoDB wire protocol (OP_MSG)
//! - CRUD operations
//! - $vectorSearch aggregation stage
//! - Indexes and collections

mod types;
mod protocol;
mod handlers;
mod server;

pub use server::MongoDBServer;
pub use types::*;
