//! Row Level Security (RLS) Engine for Supabase Compatibility
//!
//! Provides PostgreSQL-compatible RLS implementation:
//! - Policy definition and storage
//! - USING and WITH CHECK clause evaluation
//! - JWT claims injection (auth.uid(), auth.role())
//! - Policy stacking and combination
//! - Query rewriting with security predicates

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod context;
pub mod evaluator;
pub mod policy;
pub mod rewriter;

pub use context::RlsContext;
pub use evaluator::PolicyEvaluator;
pub use policy::{PolicyCommand, RlsPolicy};
