//! LumaDB Common - Shared utilities and types
//!
//! This crate provides common functionality used across all LumaDB components:
//! - Error types and handling
//! - Configuration management
//! - Metrics and observability
//! - Common type definitions

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod config;
pub mod error;
pub mod metrics;
pub mod types;

pub use config::Config;
pub use error::{Error, Result};
pub use types::*;
