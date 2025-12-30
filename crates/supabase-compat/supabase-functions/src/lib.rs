//! Supabase Edge Functions Runtime
//!
//! Provides serverless function execution:
//! - HTTP-triggered functions
//! - Function deployment and versioning
//! - Environment variable management
//! - Execution isolation

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod function;
pub mod runtime;
pub mod server;

pub use function::{EdgeFunction, FunctionConfig, FunctionStatus};
pub use runtime::FunctionRuntime;
pub use server::FunctionsServer;
