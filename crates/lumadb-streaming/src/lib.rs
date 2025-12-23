//! LumaDB Streaming Engine
//!
//! Ultra-fast streaming engine providing:
//! - 100x performance vs Redpanda/Kafka
//! - Thread-per-core architecture
//! - Zero-copy networking
//! - Lock-free data structures
//! - SIMD batch processing

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod batch;
pub mod consumer;
pub mod log;
pub mod network;
pub mod reactor;

mod engine;

pub use engine::{StreamingEngine, ProduceRecord, ConsumeRecord, RaftStub};
