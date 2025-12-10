//! Hybrid Memory Architecture
//!
//! Aerospike-inspired hybrid storage that seamlessly operates across:
//! - RAM (fastest, volatile)
//! - NVMe/SSD (fast, persistent)
//! - HDD (slower, high capacity)
//!
//! Key innovations:
//! - Primary index ALWAYS in RAM for predictable latency
//! - Automatic hot/cold data tiering
//! - Intelligent prefetching and caching
//! - Zero-copy data paths where possible

pub mod tier;
pub mod index;
pub mod prefetch;
pub mod migration;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::{RwLock, Mutex};
use tokio::sync::mpsc;

use crate::error::{TdbError, Result as TdbResult};
use crate::types::{KeyValue, Value};

/// Storage tier types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum StorageTier {
    /// In-memory storage (fastest, volatile)
    Memory,
    /// NVMe/SSD storage (fast, persistent)
    SSD,
    /// HDD storage (slower, high capacity)
    HDD,
    /// Hybrid mode - hot data in RAM, cold on SSD
    Hybrid,
}

/// Configuration for hybrid storage
#[derive(Debug)]
pub struct HybridConfig {
    /// Storage mode
    pub mode: StorageTier,

    /// RAM budget for data (bytes)
    pub memory_budget: usize,

    /// RAM budget for primary index (always in RAM)
    pub index_memory_budget: usize,

    /// SSD data directory
    pub ssd_path: Option<PathBuf>,

    /// HDD data directory (for cold data)
    pub hdd_path: Option<PathBuf>,

    /// Hot/cold threshold (access count in last window)
    pub hot_threshold: u32,

    /// Time window for access tracking (seconds)
    pub access_window_secs: u64,

    /// Enable automatic data migration
    pub auto_migrate: bool,

    /// Migration batch size
    pub migration_batch_size: usize,

    /// Prefetch enabled
    pub prefetch_enabled: bool,

    /// Prefetch lookahead (number of records)
    pub prefetch_lookahead: usize,

    /// Direct I/O (bypass OS page cache)
    pub direct_io: bool,

    /// Use huge pages for memory allocation
    pub huge_pages: bool,

    /// NUMA node affinity (-1 for auto)
    pub numa_node: i32,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            mode: StorageTier::Hybrid,
            memory_budget: 8 * 1024 * 1024 * 1024, // 8 GB
            index_memory_budget: 2 * 1024 * 1024 * 1024, // 2 GB
            ssd_path: Some(PathBuf::from("./data/ssd")),
            hdd_path: None,
            hot_threshold: 10,
            access_window_secs: 3600, // 1 hour
            auto_migrate: true,
            migration_batch_size: 1000,
            prefetch_enabled: true,
            prefetch_lookahead: 100,
            direct_io: true,
            huge_pages: true,
            numa_node: -1,
        }
    }
}

impl HybridConfig {
    /// Configuration optimized for maximum speed (all in RAM)
    pub fn all_ram(memory_budget: usize) -> Self {
        Self {
            mode: StorageTier::Memory,
            memory_budget,
            index_memory_budget: memory_budget / 4,
            ssd_path: None,
            hdd_path: None,
            auto_migrate: false,
            ..Default::default()
        }
    }

    /// Configuration for persistent SSD storage
    pub fn ssd_persistent(ssd_path: PathBuf, cache_size: usize) -> Self {
        Self {
            mode: StorageTier::SSD,
            memory_budget: cache_size,
            ssd_path: Some(ssd_path),
            ..Default::default()
        }
    }

    /// Configuration for hybrid RAM + SSD (Aerospike-style)
    pub fn hybrid(memory_budget: usize, ssd_path: PathBuf) -> Self {
        Self {
            mode: StorageTier::Hybrid,
            memory_budget,
            ssd_path: Some(ssd_path),
            auto_migrate: true,
            ..Default::default()
        }
    }

    /// Configuration for tiered storage (RAM -> SSD -> HDD)
    pub fn tiered(memory_budget: usize, ssd_path: PathBuf, hdd_path: PathBuf) -> Self {
        Self {
            mode: StorageTier::Hybrid,
            memory_budget,
            ssd_path: Some(ssd_path),
            hdd_path: Some(hdd_path),
            auto_migrate: true,
            ..Default::default()
        }
    }
}

/// Location metadata for a record
#[derive(Debug)]
pub struct RecordLocation {
    /// Current storage tier
    pub tier: StorageTier,

    /// Offset within the tier's storage
    pub offset: u64,

    /// Record size in bytes
    pub size: u32,

    /// Access count in current window
    pub access_count: AtomicU64,

    /// Last access timestamp
    pub last_access: AtomicU64,

    /// Creation timestamp
    pub created_at: u64,
}

impl RecordLocation {
    pub fn new(tier: StorageTier, offset: u64, size: u32) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            tier,
            offset,
            size,
            access_count: AtomicU64::new(1),
            last_access: AtomicU64::new(now),
            created_at: now,
        }
    }

    pub fn record_access(&self) {
        self.access_count.fetch_add(1, Ordering::Relaxed);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_access.store(now, Ordering::Relaxed);
    }

    pub fn is_hot(&self, threshold: u32, window_secs: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let last = self.last_access.load(Ordering::Relaxed);
        if now - last > window_secs {
            return false;
        }

        self.access_count.load(Ordering::Relaxed) >= threshold as u64
    }
}

/// Primary index entry (always in RAM for O(1) lookup)
#[derive(Debug)]
pub struct IndexEntry {
    /// Key hash for fast comparison
    pub key_hash: u64,

    /// Record location
    pub location: RecordLocation,

    /// Generation for MVCC
    pub generation: u32,

    /// Tombstone flag
    pub deleted: bool,
}

/// Statistics for hybrid storage
#[derive(Debug, Default)]
pub struct HybridStats {
    /// Records in RAM
    pub ram_records: AtomicUsize,
    /// Records on SSD
    pub ssd_records: AtomicUsize,
    /// Records on HDD
    pub hdd_records: AtomicUsize,

    /// RAM bytes used
    pub ram_bytes: AtomicUsize,
    /// SSD bytes used
    pub ssd_bytes: AtomicUsize,
    /// HDD bytes used
    pub hdd_bytes: AtomicUsize,

    /// Cache hits
    pub cache_hits: AtomicU64,
    /// Cache misses
    pub cache_misses: AtomicU64,

    /// Records migrated to SSD
    pub migrations_to_ssd: AtomicU64,
    /// Records migrated to RAM
    pub migrations_to_ram: AtomicU64,

    /// Prefetch hits
    pub prefetch_hits: AtomicU64,
}

impl HybridStats {
    pub fn hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        hits as f64 / total as f64
    }
}

/// Hybrid storage manager
pub struct HybridStorage {
    config: HybridConfig,

    /// Primary index (always in RAM)
    /// Maps key -> IndexEntry
    primary_index: DashMap<Vec<u8>, IndexEntry>,

    /// RAM data storage
    ram_store: tier::RamStore,

    /// SSD data storage
    ssd_store: Option<tier::SsdStore>,

    /// HDD data storage
    hdd_store: Option<tier::HddStore>,

    /// Read cache for SSD/HDD data
    read_cache: tier::ReadCache,

    /// Prefetch manager
    prefetcher: Option<prefetch::Prefetcher>,

    /// Migration manager
    migrator: Option<migration::Migrator>,

    /// Statistics
    stats: Arc<HybridStats>,

    /// Shutdown signal
    shutdown: tokio::sync::broadcast::Sender<()>,
}

impl HybridStorage {
    /// Create new hybrid storage
    pub async fn new(config: HybridConfig) -> TdbResult<Self> {
        let stats = Arc::new(HybridStats::default());
        let (shutdown, _) = tokio::sync::broadcast::channel(1);

        // Initialize RAM store
        let ram_store = tier::RamStore::new(
            config.memory_budget,
            config.huge_pages,
            config.numa_node,
        )?;

        // Initialize SSD store if configured
        let ssd_store = if let Some(ref path) = config.ssd_path {
            Some(tier::SsdStore::new(path.clone(), config.direct_io).await?)
        } else {
            None
        };

        // Initialize HDD store if configured
        let hdd_store = if let Some(ref path) = config.hdd_path {
            Some(tier::HddStore::new(path.clone()).await?)
        } else {
            None
        };

        // Initialize read cache
        let cache_size = config.memory_budget / 4; // 25% of memory for cache
        let read_cache = tier::ReadCache::new(cache_size);

        // Initialize prefetcher
        let prefetcher = if config.prefetch_enabled {
            Some(prefetch::Prefetcher::new(
                config.prefetch_lookahead,
                stats.clone(),
            ))
        } else {
            None
        };

        // Initialize migrator
        let migrator = if config.auto_migrate && config.mode == StorageTier::Hybrid {
            Some(migration::Migrator::new(
                config.hot_threshold,
                config.access_window_secs,
                config.migration_batch_size,
                stats.clone(),
            ))
        } else {
            None
        };

        Ok(Self {
            config,
            primary_index: DashMap::new(),
            ram_store,
            ssd_store,
            hdd_store,
            read_cache,
            prefetcher,
            migrator,
            stats,
            shutdown,
        })
    }

    /// Put a record (automatically placed in appropriate tier)
    pub async fn put(&self, key: &[u8], value: &[u8]) -> TdbResult<()> {
        let key_hash = self.hash_key(key);

        // Determine initial tier based on mode and memory availability
        let tier = self.select_tier_for_write(value.len());

        // Write to selected tier
        let (offset, size) = match tier {
            StorageTier::Memory => {
                let offset = self.ram_store.write(value)?;
                self.stats.ram_records.fetch_add(1, Ordering::Relaxed);
                self.stats.ram_bytes.fetch_add(value.len(), Ordering::Relaxed);
                (offset, value.len() as u32)
            }
            StorageTier::SSD => {
                let ssd = self.ssd_store.as_ref()
                    .ok_or(TdbError::Config("SSD storage not configured".into()))?;
                let offset = ssd.write(value).await?;
                self.stats.ssd_records.fetch_add(1, Ordering::Relaxed);
                self.stats.ssd_bytes.fetch_add(value.len(), Ordering::Relaxed);
                (offset, value.len() as u32)
            }
            StorageTier::HDD => {
                let hdd = self.hdd_store.as_ref()
                    .ok_or(TdbError::Config("HDD storage not configured".into()))?;
                let offset = hdd.write(value).await?;
                self.stats.hdd_records.fetch_add(1, Ordering::Relaxed);
                self.stats.hdd_bytes.fetch_add(value.len(), Ordering::Relaxed);
                (offset, value.len() as u32)
            }
            StorageTier::Hybrid => {
                // In hybrid mode, new writes go to RAM first
                if self.ram_store.has_space(value.len()) {
                    let offset = self.ram_store.write(value)?;
                    self.stats.ram_records.fetch_add(1, Ordering::Relaxed);
                    self.stats.ram_bytes.fetch_add(value.len(), Ordering::Relaxed);
                    (offset, value.len() as u32)
                } else {
                    // Spill to SSD
                    let ssd = self.ssd_store.as_ref()
                        .ok_or(TdbError::Config("SSD storage not configured".into()))?;
                    let offset = ssd.write(value).await?;
                    self.stats.ssd_records.fetch_add(1, Ordering::Relaxed);
                    self.stats.ssd_bytes.fetch_add(value.len(), Ordering::Relaxed);
                    (offset, value.len() as u32)
                }
            }
        };

        // Update primary index (always in RAM)
        let location = RecordLocation::new(tier, offset, size);
        let entry = IndexEntry {
            key_hash,
            location,
            generation: 1,
            deleted: false,
        };

        self.primary_index.insert(key.to_vec(), entry);

        Ok(())
    }

    /// Get a record
    pub async fn get(&self, key: &[u8]) -> TdbResult<Option<Vec<u8>>> {
        // Lookup in primary index (always O(1) from RAM)
        let entry = match self.primary_index.get(key) {
            Some(e) => e,
            None => return Ok(None),
        };

        if entry.deleted {
            return Ok(None);
        }

        // Record access for hot/cold tracking
        entry.location.record_access();

        // Read from appropriate tier
        let data = match entry.location.tier {
            StorageTier::Memory | StorageTier::Hybrid => {
                // Try RAM first
                if let Some(data) = self.ram_store.read(entry.location.offset, entry.location.size) {
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                    data
                } else {
                    // Fall through to SSD
                    self.read_from_ssd_or_hdd(&entry.location).await?
                }
            }
            StorageTier::SSD => {
                self.read_from_ssd_or_hdd(&entry.location).await?
            }
            StorageTier::HDD => {
                self.read_from_ssd_or_hdd(&entry.location).await?
            }
        };

        // Trigger prefetch for sequential access patterns
        if let Some(ref prefetcher) = self.prefetcher {
            prefetcher.on_access(key);
        }

        Ok(Some(data))
    }

    /// Delete a record
    pub async fn delete(&self, key: &[u8]) -> TdbResult<bool> {
        if let Some(mut entry) = self.primary_index.get_mut(key) {
            entry.deleted = true;
            entry.generation += 1;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get statistics
    pub fn stats(&self) -> &HybridStats {
        &self.stats
    }

    /// Force migration of cold data to lower tier
    pub async fn compact(&self) -> TdbResult<usize> {
        if let Some(ref migrator) = self.migrator {
            let migrated = migrator.migrate_cold_data(
                &self.primary_index,
                &self.ram_store,
                self.ssd_store.as_ref(),
                self.hdd_store.as_ref(),
                &self.config,
            ).await?;
            Ok(migrated)
        } else {
            Ok(0)
        }
    }

    // Internal helpers

    fn hash_key(&self, key: &[u8]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    fn select_tier_for_write(&self, size: usize) -> StorageTier {
        match self.config.mode {
            StorageTier::Memory => StorageTier::Memory,
            StorageTier::SSD => StorageTier::SSD,
            StorageTier::HDD => StorageTier::HDD,
            StorageTier::Hybrid => {
                if self.ram_store.has_space(size) {
                    StorageTier::Memory
                } else if self.ssd_store.is_some() {
                    StorageTier::SSD
                } else {
                    StorageTier::HDD
                }
            }
        }
    }

    async fn read_from_ssd_or_hdd(&self, location: &RecordLocation) -> TdbResult<Vec<u8>> {
        // Check read cache first
        let cache_key = (location.tier, location.offset);
        if let Some(data) = self.read_cache.get(&cache_key) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(data);
        }

        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);

        // Read from storage
        let data = match location.tier {
            StorageTier::SSD => {
                let ssd = self.ssd_store.as_ref()
                    .ok_or(TdbError::Internal("SSD store not available".into()))?;
                ssd.read(location.offset, location.size).await?
            }
            StorageTier::HDD => {
                let hdd = self.hdd_store.as_ref()
                    .ok_or(TdbError::Internal("HDD store not available".into()))?;
                hdd.read(location.offset, location.size).await?
            }
            _ => return Err(TdbError::Internal("Invalid tier for SSD/HDD read".into())),
        };

        // Add to cache
        self.read_cache.insert(cache_key, data.clone());

        Ok(data)
    }
}
