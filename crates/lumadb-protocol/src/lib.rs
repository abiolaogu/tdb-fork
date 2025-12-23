//! Wire protocol implementations
//!
//! Provides compatibility with:
//! - Apache Kafka protocol
//! - PostgreSQL wire protocol
//! - MongoDB wire protocol
//! - Redis protocol (RESP)

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod kafka;
pub mod mongodb;
pub mod postgres;
pub mod redis;
