//! Supabase Common Types and Utilities
//!
//! Shared types, configuration, and error handling for the Supabase compatibility layer.

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod config;
pub mod error;
pub mod types;

pub use config::SupabaseConfig;
pub use error::{Error, Result};
