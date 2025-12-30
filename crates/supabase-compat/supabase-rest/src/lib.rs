//! Supabase REST API Service (PostgREST-compatible)
//!
//! Provides a PostgREST-compatible REST API that auto-generates endpoints
//! from database schema, supporting:
//! - CRUD operations on tables
//! - Complex filtering (eq, neq, gt, gte, lt, lte, like, ilike, in, is, etc.)
//! - Ordering and pagination
//! - Nested resource expansion
//! - RPC calls to stored functions
//! - OpenAPI documentation

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod backend;
pub mod handlers;
pub mod lumadb_backend;
pub mod query;
pub mod ratelimit;
pub mod rls_handlers;
pub mod schema;
pub mod server;

pub use backend::{InMemoryBackend, QueryBackend, QueryContext, QueryResult};
pub use lumadb_backend::LumaDbBackend;
pub use ratelimit::{RateLimitConfig, RateLimitResult, RateLimiter};
pub use rls_handlers::RlsRestState;
pub use server::RestServer;

use supabase_common::config::RestConfig;
use supabase_common::error::Result;
