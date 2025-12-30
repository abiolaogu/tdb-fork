//! # LumaDB Multi-Protocol Compatibility Layer
//!
//! Drop-in replacement for:
//! - **AWS DynamoDB** (NoSQL) - Full HTTP API with Axum
//! - **Cloudflare D1** (Edge SQL)
//! - **Turso (libSQL)** (Distributed SQLite)
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Protocol Servers                          │
//! │   DynamoDB HTTP │  D1 API  │  Turso/libSQL  │  Future...    │
//! ├─────────────────────────────────────────────────────────────┤
//! │                 Unified Query Interface                      │
//! │         UnifiedResult │ Row │ Column │ Value                │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Storage Engine                             │
//! │              LumaDB Native Storage Layer                     │
//! └─────────────────────────────────────────────────────────────┘
//! ```

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod core;
pub mod dynamodb;
pub mod d1;
pub mod turso;
pub mod storage;
pub mod server;

// Re-export core types
pub use core::{
    AdapterError, BatchOperation, Column, KeyCondition, ProtocolAdapter,
    QueryFilter, ResultMetadata, Row, SortKeyCondition, StorageEngine,
    UnifiedResult, Value,
};

// Re-export adapters
pub use d1::{D1Server, D1Config};
pub use turso::{TursoServer, TursoConfig};
pub use storage::LumaStorage;

// Re-export DynamoDB server
pub use dynamodb::{DynamoDBServer, DynamoDBConfig};

// Re-export unified server
pub use server::{MultiProtocolServer, ServerConfig};
