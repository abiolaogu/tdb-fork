//! Storage Engine - LSM-tree based persistent storage
//!
//! Implements a high-performance LSM-tree storage engine inspired by:
//! - RocksDB/LevelDB for leveled compaction
//! - ScyllaDB for shard-per-core design
//! - Aerospike for hybrid memory architecture

mod engine;
mod sstable;
mod collection;
mod manifest;

pub use engine::StorageEngine;
pub use sstable::{SSTable, SSTableBuilder, SSTableReader};
pub use collection::Collection;
pub use manifest::Manifest;
pub use policy::PolicyEngine;

pub mod policy;
