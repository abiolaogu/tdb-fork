//! SDK Compatibility Layers
//!
//! Provides Qdrant-like and Pinecone-like interfaces for LumaDB.
//! Allows gradual migration from other vector databases.
//!
//! # Example - Qdrant-style
//!
//! ```rust,ignore
//! use lumadb_sdk::compat::qdrant::{QdrantClient, Point};
//!
//! let client = QdrantClient::connect("http://localhost:6333").await?;
//!
//! // Upsert points (Qdrant style)
//! let points = vec![
//!     Point::new(1)
//!         .with_vector(vec![0.1, 0.2, 0.3])
//!         .with_payload(json!({"name": "test"})),
//! ];
//! client.upsert("my_collection", &points).await?;
//!
//! // Search (Qdrant style)
//! let results = client.search("my_collection", vec![0.1, 0.2, 0.3], 10).await?;
//! ```
//!
//! # Example - Pinecone-style
//!
//! ```rust,ignore
//! use lumadb_sdk::compat::pinecone::{PineconeClient, Vector};
//!
//! let client = PineconeClient::connect(
//!     "http://localhost:8081",
//!     "api-key",
//!     "my-index"
//! ).await?;
//!
//! // Upsert vectors (Pinecone style)
//! let vectors = vec![
//!     Vector::new("vec1", vec![0.1, 0.2, 0.3])
//!         .with_metadata(json!({"category": "test"})),
//! ];
//! client.upsert(&vectors, None).await?;
//!
//! // Query (Pinecone style)
//! let results = client.query(vec![0.1, 0.2, 0.3], 10, None).await?;
//! ```

pub mod qdrant;
pub mod pinecone;

pub use qdrant::QdrantClient;
pub use pinecone::PineconeClient;
