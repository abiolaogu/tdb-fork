//! LumaDB Security Layer
//!
//! Provides:
//! - Authentication (SASL, JWT, mTLS)
//! - Authorization (RBAC, ACLs)
//! - Encryption (TLS, at-rest encryption)
//! - Audit logging

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod auth;
pub mod authz;
pub mod crypto;

mod manager;

pub use manager::SecurityManager;

use lumadb_common::config::SecurityConfig;
use lumadb_common::error::Result;
