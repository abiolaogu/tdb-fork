//! Supabase Authentication Service (GoTrue-compatible)
//!
//! Provides Supabase-compatible authentication including:
//! - Email/Password authentication with Argon2 hashing
//! - Magic link (passwordless) authentication
//! - OAuth 2.0 provider integration
//! - JWT token generation and validation
//! - Session management with refresh tokens
//! - User management CRUD operations

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod handlers;
pub mod jwt;
pub mod middleware;
pub mod providers;
pub mod server;
pub mod session;
pub mod user;

pub use server::AuthServer;

use supabase_common::config::AuthConfig;
use supabase_common::error::Result;
