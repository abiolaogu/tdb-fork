//! AI Module for LumaDB
//! Provides AI-powered features: PromptQL, semantic search, embeddings

pub mod promptql;
pub mod semantic_search;

pub use promptql::{PromptQLEngine, PromptQLQuery, PromptQLResult, LLMProvider};
pub use semantic_search::{SemanticSearch, Embedding, QueryUnderstanding};
