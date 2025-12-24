//! LumaDB Vector Database Compatibility Layer
//!
//! This crate provides drop-in replacement compatibility for popular vector databases:
//! - **Qdrant**: Full REST API compatibility
//! - **Pinecone**: Full REST API compatibility
//! - **MongoDB Atlas Vector Search**: Wire protocol with $vectorSearch support
//!
//! # Migration Support
//!
//! Migrate data from other vector databases to LumaDB:
//! - **Qdrant**: REST API-based migration
//! - **Pinecone**: Full index export with namespaces
//! - **MongoDB**: Atlas Vector Search collections
//! - **Weaviate**: GraphQL API-based class migration
//! - **Milvus**: REST API with schema auto-detection
//! - **Zilliz Cloud**: Managed Milvus with API key auth
//!
//! # Compatibility Server Usage
//!
//! ```rust,ignore
//! use lumadb_compat::{QdrantServer, PineconeServer, MongoDBServer};
//!
//! // Start Qdrant-compatible server on port 6333
//! let qdrant = QdrantServer::new(storage.clone()).bind("0.0.0.0:6333");
//!
//! // Start Pinecone-compatible server on port 8081
//! let pinecone = PineconeServer::new(storage.clone()).bind("0.0.0.0:8081");
//!
//! // Start MongoDB-compatible server on port 27017
//! let mongodb = MongoDBServer::new(storage.clone()).bind("0.0.0.0:27017");
//! ```
//!
//! # Migration Tool Usage
//!
//! ```rust,ignore
//! use lumadb_compat::{MigrationTool, MigrationSource};
//!
//! let tool = MigrationTool::new(storage.clone());
//!
//! // Migrate from Weaviate
//! tool.import_from_weaviate("http://localhost:8080", "Articles", None, Some("articles")).await?;
//!
//! // Migrate from Milvus
//! tool.import_from_milvus("localhost", 19530, "my_collection", None).await?;
//!
//! // Migrate from Zilliz Cloud
//! tool.import_from_zilliz("https://your-instance.zillizcloud.com", "vectors", "api-key", None).await?;
//!
//! // Or use generic source for more control
//! let source = MigrationSource::weaviate_with_key("http://localhost:8080", "Products", "api-key");
//! tool.import_from_source(source, "products").await?;
//! ```

#![warn(clippy::all)]
#![allow(clippy::module_name_repetitions)]

pub mod qdrant;
pub mod pinecone;
pub mod mongodb;
pub mod migration;

pub use qdrant::QdrantServer;
pub use pinecone::PineconeServer;
pub use mongodb::MongoDBServer;
pub use migration::{MigrationTool, MigrationSource};

use thiserror::Error;

/// Compatibility layer errors
#[derive(Error, Debug)]
pub enum CompatError {
    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Vector dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Network error: {0}")]
    Network(String),
}

impl From<lumadb_common::error::Error> for CompatError {
    fn from(e: lumadb_common::error::Error) -> Self {
        CompatError::Storage(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, CompatError>;
