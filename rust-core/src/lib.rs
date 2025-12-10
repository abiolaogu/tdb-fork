//! TDB+ Core Storage Engine
//!
//! A high-performance, distributed storage engine written in Rust.
//! Designed to outperform Aerospike, ScyllaDB, DragonflyDB, YugabyteDB, and kdb+.
//!
//! # Key Features
//!
//! - **Hybrid Memory Architecture**: Data runs on both RAM and SSD, with automatic
//!   hot/cold tiering (Aerospike-inspired)
//! - **Columnar Storage**: kdb+-style vectorized operations with SIMD acceleration
//! - **Shard-Per-Core**: ScyllaDB-inspired lock-free architecture
//! - **io_uring**: Maximum I/O throughput on Linux
//! - **Predictable Latency**: Sub-millisecond SLAs with admission control
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    TDB+ Storage Engine v2.0                          │
//! ├─────────────────────────────────────────────────────────────────────┤
//! │                                                                      │
//! │  ┌─────────────────────────────────────────────────────────────┐    │
//! │  │                  Hybrid Memory Layer                         │    │
//! │  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │    │
//! │  │  │   RAM    │  │   SSD    │  │   HDD    │  │  Cache   │    │    │
//! │  │  │  (Hot)   │◄─┤  (Warm)  │◄─┤  (Cold)  │  │  (LRU)   │    │    │
//! │  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘    │    │
//! │  └─────────────────────────────────────────────────────────────┘    │
//! │                                                                      │
//! │  ┌──────────────────────┐  ┌────────────────────────────────────┐  │
//! │  │  Columnar Engine     │  │      Row-Based Engine              │  │
//! │  │  ┌────────────────┐  │  │  ┌─────────────┐  ┌─────────────┐ │  │
//! │  │  │ SIMD Vectorized│  │  │  │   MemTable  │  │ BlockCache  │ │  │
//! │  │  │  Operations    │  │  │  │  (SkipList) │  │   (LRU)     │ │  │
//! │  │  └────────────────┘  │  │  └─────────────┘  └─────────────┘ │  │
//! │  │  ┌────────────────┐  │  │  ┌─────────────────────────────┐  │  │
//! │  │  │  Time-Series   │  │  │  │     LSM-Tree Storage        │  │  │
//! │  │  │  Optimizations │  │  │  │  (Leveled/Universal)        │  │  │
//! │  │  └────────────────┘  │  │  └─────────────────────────────┘  │  │
//! │  └──────────────────────┘  └────────────────────────────────────┘  │
//! │                                                                      │
//! │  ┌───────────────────────────────────────────────────────────────┐  │
//! │  │                   I/O Subsystem                                │  │
//! │  │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐ │  │
//! │  │  │   io_uring   │  │  Direct I/O  │  │   WAL (Group Commit)│ │  │
//! │  │  │   (Linux)    │  │   Bypass     │  │                     │ │  │
//! │  │  └──────────────┘  └──────────────┘  └─────────────────────┘ │  │
//! │  └───────────────────────────────────────────────────────────────┘  │
//! │                                                                      │
//! │  ┌───────────────────────────────────────────────────────────────┐  │
//! │  │                   SLA & Latency Control                        │  │
//! │  │  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐ │  │
//! │  │  │  Admission   │  │   Latency    │  │   Backpressure      │ │  │
//! │  │  │  Control     │  │  Histograms  │  │   Management        │ │  │
//! │  │  └──────────────┘  └──────────────┘  └─────────────────────┘ │  │
//! │  └───────────────────────────────────────────────────────────────┘  │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```

// Core modules
pub mod storage;
pub mod index;
pub mod memory;
pub mod wal;
pub mod error;
pub mod config;
pub mod types;
pub mod shard;
pub mod compaction;

// High-performance modules
pub mod hybrid;      // Hybrid RAM/SSD storage (Aerospike-style)
pub mod columnar;    // Columnar storage with SIMD (kdb+-style)
pub mod io;          // io_uring and direct I/O
pub mod latency;     // Predictable latency SLAs
pub mod ffi;         // FFI bindings for Go/Python
pub mod node_bindings; // Node.js bindings using napi-rs
pub mod scripting;     // Stored Procedures & Triggers

// Re-exports - Core
pub use storage::{StorageEngine, Collection};
pub use types::Document;
pub use index::{Index, IndexType, BTreeIndex, HashIndex};
pub use memory::{MemTable, BlockCache};
pub use wal::WriteAheadLog;
pub use error::{TdbError, Result};
pub use config::Config;
pub use types::*;
pub use shard::ShardManager;

// Re-exports - High Performance
pub use hybrid::{HybridStorage, HybridConfig, StorageTier};
pub use columnar::{Column, ColumnType, ColumnarTable, Schema};
pub use io::{IoUring, UringConfig, BatchedIo};
pub use latency::{SlaMonitor, SlaTier, LatencyHistogram};

use std::sync::Arc;
use parking_lot::RwLock;
use dashmap::DashMap;

/// TDB+ Database instance
pub struct Database {
    config: Config,
    shards: Arc<ShardManager>,
    collections: DashMap<String, Arc<Collection>>,
    wal: Arc<WriteAheadLog>,
    stats: Arc<RwLock<DatabaseStats>>,
    
    // Scripting
    pub scripting: Arc<scripting::ScriptingEngine>,
    pub procedures: Arc<scripting::procedures::Procedures>,
    pub triggers: Arc<scripting::triggers::Triggers>,
}

impl Database {
    /// Create a new database instance
    pub fn new(config: Config) -> Result<Self> {
        let wal = Arc::new(WriteAheadLog::new(&config)?);
        let shards = Arc::new(ShardManager::new(&config)?);

        let scripting = Arc::new(scripting::ScriptingEngine::new());
        let procedures = Arc::new(scripting::procedures::Procedures::new(scripting.clone()));
        let triggers = Arc::new(scripting::triggers::Triggers::new(scripting.clone()));

        Ok(Self {
            config,
            shards,
            collections: DashMap::new(),
            wal,
            stats: Arc::new(RwLock::new(DatabaseStats::default())),
            scripting,
            procedures,
            triggers,
        })
    }

    /// Open an existing database or create new
    pub async fn open(config: Config) -> Result<Self> {
        let db = Self::new(config)?;
        db.recover_from_wal().await?;
        Ok(db)
    }

    /// Get or create a collection
    pub fn collection(&self, name: &str) -> Arc<Collection> {
        self.collections
            .entry(name.to_string())
            .or_insert_with(|| {
                Arc::new(Collection::new(
                    name.to_string(),
                    self.shards.clone(),
                    self.wal.clone(),
                ))
            })
            .clone()
    }

    /// Insert a document
    pub async fn insert(&self, collection: &str, mut doc: Document) -> Result<DocumentId> {
        use scripting::triggers::TriggerEvent;
        
        // Before Insert Trigger
        self.triggers.on_event(collection, TriggerEvent::BeforeInsert, &mut doc)?;
        
        let coll = self.collection(collection);
        let id = coll.insert(doc.clone()).await?;
        self.stats.write().inserts += 1;
        
        // After Insert Trigger
        self.triggers.on_event(collection, TriggerEvent::AfterInsert, &mut doc)?;
        
        Ok(id)
    }

    /// Get a document by ID
    pub async fn get(&self, collection: &str, id: &DocumentId) -> Result<Option<Document>> {
        let coll = self.collection(collection);
        let result = coll.get(id).await?;
        self.stats.write().reads += 1;
        Ok(result)
    }

    /// Update a document
    pub async fn update(&self, collection: &str, id: &DocumentId, mut doc: Document) -> Result<bool> {
        use scripting::triggers::TriggerEvent;
        
        self.triggers.on_event(collection, TriggerEvent::BeforeUpdate, &mut doc)?;
        
        let coll = self.collection(collection);
        let result = coll.update(id, doc.clone()).await?;
        self.stats.write().updates += 1;
        
        self.triggers.on_event(collection, TriggerEvent::AfterUpdate, &mut doc)?;
        Ok(result)
    }

    /// Delete a document
    pub async fn delete(&self, collection: &str, id: &DocumentId) -> Result<bool> {
        use scripting::triggers::TriggerEvent;
        // Note: Delete triggers don't have access to the document content in this simple implementation
        // A real impl would fetch it first.
        let mut empty_doc = Document::new(std::collections::HashMap::new());
        
        self.triggers.on_event(collection, TriggerEvent::BeforeDelete, &mut empty_doc)?;
        
        let coll = self.collection(collection);
        let result = coll.delete(id).await?;
        self.stats.write().deletes += 1;
        
        self.triggers.on_event(collection, TriggerEvent::AfterDelete, &mut empty_doc)?;
        Ok(result)
    }

    /// Batch insert for high throughput
    pub async fn batch_insert(&self, collection: &str, docs: Vec<Document>) -> Result<Vec<DocumentId>> {
        let coll = self.collection(collection);
        let ids = coll.batch_insert(docs).await?;
        self.stats.write().inserts += ids.len() as u64;
        Ok(ids)
    }

    /// Scan with predicate
    pub async fn scan<F>(&self, collection: &str, predicate: F) -> Result<Vec<Document>>
    where
        F: Fn(&Document) -> bool + Send + Sync,
    {
        let coll = self.collection(collection);
        coll.scan(predicate).await
    }

    /// Recover from WAL after crash
    async fn recover_from_wal(&self) -> Result<()> {
        // Recovery is handled by individual StorageEngine instances
        Ok(())
    }

    /// Flush all data to disk
    pub async fn flush(&self) -> Result<()> {
        for entry in self.collections.iter() {
            entry.value().flush().await?;
        }
        self.wal.sync().await?;
        Ok(())
    }

    /// Close the database
    pub async fn close(&self) -> Result<()> {
        // Flush all collections
        for entry in self.collections.iter() {
            entry.value().flush().await?;
        }
        self.wal.sync().await?;
        Ok(())
    }

    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats {
        self.stats.read().clone()
    }

    /// Compact all collections
    pub async fn compact(&self) -> Result<()> {
        for entry in self.collections.iter() {
            entry.value().compact().await?;
        }
        Ok(())
    }
}

/// Database statistics
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct DatabaseStats {
    pub inserts: u64,
    pub reads: u64,
    pub updates: u64,
    pub deletes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub compactions: u64,
    pub bytes_written: u64,
    pub bytes_read: u64,
}

/// WAL operation types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WalOperation {
    Insert { collection: String, doc: Document },
    Update { collection: String, id: DocumentId, doc: Document },
    Delete { collection: String, id: DocumentId },
}

/// WAL entry
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WalEntry {
    pub sequence: u64,
    pub timestamp: i64,
    pub op: WalOperation,
}

// FFI exports for Go and Python
#[cfg(feature = "python")]
mod python_bindings;

#[no_mangle]
pub extern "C" fn tdb_version() -> *const std::ffi::c_char {
    static VERSION: &str = "2.0.0\0";
    VERSION.as_ptr() as *const std::ffi::c_char
}
