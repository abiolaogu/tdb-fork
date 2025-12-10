//! Configuration for TDB+
//!
//! Inspired by best practices from Aerospike, ScyllaDB, and DragonflyDB

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use crate::types::Compression;

/// Main database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Data directory
    pub data_dir: PathBuf,

    /// Memory configuration
    pub memory: MemoryConfig,

    /// Storage configuration
    pub storage: StorageConfig,

    /// WAL configuration
    pub wal: WalConfig,

    /// Compaction configuration
    pub compaction: CompactionConfig,

    /// Shard configuration (ScyllaDB-inspired)
    pub sharding: ShardConfig,

    /// Cache configuration
    pub cache: CacheConfig,

    /// Performance tuning
    pub performance: PerformanceConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./tdb_data"),
            memory: MemoryConfig::default(),
            storage: StorageConfig::default(),
            wal: WalConfig::default(),
            compaction: CompactionConfig::default(),
            sharding: ShardConfig::default(),
            cache: CacheConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

/// Memory configuration (Aerospike-inspired hybrid memory)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum memory for memtables (bytes)
    pub memtable_size: usize,

    /// Number of memtables before flush
    pub max_memtables: usize,

    /// Block size for storage
    pub block_size: usize,

    /// Enable memory-mapped files
    pub use_mmap: bool,

    /// Memory limit before rejecting writes
    pub memory_limit: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            memtable_size: 64 * 1024 * 1024,      // 64 MB
            max_memtables: 4,
            block_size: 4 * 1024,                  // 4 KB
            use_mmap: true,
            memory_limit: 1024 * 1024 * 1024,     // 1 GB
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Compression algorithm
    pub compression: Compression,

    /// Compression level (1-22 for zstd)
    pub compression_level: i32,

    /// Maximum file size before rotation
    pub max_file_size: usize,

    /// Enable direct I/O (bypass page cache)
    pub direct_io: bool,

    /// Sync writes to disk
    pub sync_writes: bool,

    /// Enable checksums for data integrity
    pub checksums: bool,

    /// Bloom filter bits per key
    pub bloom_bits_per_key: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            compression: Compression::Lz4,
            compression_level: 3,
            max_file_size: 256 * 1024 * 1024,     // 256 MB
            direct_io: false,
            sync_writes: false,
            checksums: true,
            bloom_bits_per_key: 10,
        }
    }
}

/// Write-Ahead Log configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// Enable WAL
    pub enabled: bool,

    /// WAL directory (defaults to data_dir/wal)
    pub dir: Option<PathBuf>,

    /// Maximum WAL file size
    pub max_file_size: usize,

    /// Sync mode
    pub sync_mode: WalSyncMode,

    /// Batch size for group commit
    pub batch_size: usize,

    /// Batch timeout in microseconds
    pub batch_timeout_us: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WalSyncMode {
    /// Sync after every write (safest, slowest)
    Always,
    /// Sync periodically
    Periodic,
    /// Never sync (fastest, risky)
    Never,
    /// Group commit (DragonflyDB-inspired)
    GroupCommit,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dir: None,
            max_file_size: 64 * 1024 * 1024,      // 64 MB
            sync_mode: WalSyncMode::GroupCommit,
            batch_size: 1000,
            batch_timeout_us: 1000,                // 1ms
        }
    }
}

/// Compaction configuration (YugabyteDB/RocksDB-inspired)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    /// Compaction style
    pub style: CompactionStyle,

    /// Number of levels for leveled compaction
    pub num_levels: usize,

    /// Level0 file number trigger
    pub level0_file_num_trigger: usize,

    /// Level0 slowdown trigger
    pub level0_slowdown_trigger: usize,

    /// Level0 stop trigger
    pub level0_stop_trigger: usize,

    /// Target file size base
    pub target_file_size_base: usize,

    /// Max bytes for level base
    pub max_bytes_for_level_base: usize,

    /// Background compaction threads
    pub max_background_compactions: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompactionStyle {
    /// Leveled compaction (good for reads)
    Leveled,
    /// Universal/tiered compaction (good for writes)
    Universal,
    /// FIFO compaction (for time-series)
    Fifo,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            style: CompactionStyle::Leveled,
            num_levels: 7,
            level0_file_num_trigger: 4,
            level0_slowdown_trigger: 8,
            level0_stop_trigger: 12,
            target_file_size_base: 64 * 1024 * 1024,  // 64 MB
            max_bytes_for_level_base: 256 * 1024 * 1024,  // 256 MB
            max_background_compactions: 4,
        }
    }
}

/// Shard configuration (ScyllaDB shard-per-core inspired)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardConfig {
    /// Number of shards (0 = auto-detect based on CPU cores)
    pub num_shards: usize,

    /// Shard-per-core architecture
    pub shard_per_core: bool,

    /// Replication factor
    pub replication_factor: usize,

    /// Hash function for sharding
    pub hash_function: HashFunction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum HashFunction {
    Xxh3,
    Murmur3,
    Crc32,
}

impl Default for ShardConfig {
    fn default() -> Self {
        Self {
            num_shards: 0,  // Auto-detect
            shard_per_core: true,
            replication_factor: 1,
            hash_function: HashFunction::Xxh3,
        }
    }
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Block cache size
    pub block_cache_size: usize,

    /// Row cache size
    pub row_cache_size: usize,

    /// Enable compressed block cache
    pub compressed_cache: bool,

    /// Cache index and filter blocks
    pub cache_index_and_filter: bool,

    /// High priority pool ratio
    pub high_priority_ratio: f64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            block_cache_size: 128 * 1024 * 1024,  // 128 MB
            row_cache_size: 64 * 1024 * 1024,     // 64 MB
            compressed_cache: true,
            cache_index_and_filter: true,
            high_priority_ratio: 0.5,
        }
    }
}

/// Performance tuning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Number of read threads
    pub read_threads: usize,

    /// Number of write threads
    pub write_threads: usize,

    /// Enable parallel reads
    pub parallel_reads: bool,

    /// Enable pipelined writes
    pub pipelined_writes: bool,

    /// IO priority (0 = highest)
    pub io_priority: u8,

    /// Rate limiter for writes (bytes/sec, 0 = unlimited)
    pub write_rate_limit: usize,

    /// Maximum concurrent file opens
    pub max_open_files: usize,

    /// Enable statistics collection
    pub enable_stats: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        let num_cpus = num_cpus::get();
        Self {
            read_threads: num_cpus,
            write_threads: num_cpus / 2,
            parallel_reads: true,
            pipelined_writes: true,
            io_priority: 0,
            write_rate_limit: 0,
            max_open_files: 10000,
            enable_stats: true,
        }
    }
}

// Helper function to get CPU count
mod num_cpus {
    pub fn get() -> usize {
        std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(4)
    }
}

impl Config {
    /// Create a config optimized for speed (less durability)
    pub fn fast() -> Self {
        let mut config = Self::default();
        config.wal.sync_mode = WalSyncMode::Never;
        config.storage.sync_writes = false;
        config.storage.checksums = false;
        config
    }

    /// Create a config optimized for durability
    pub fn durable() -> Self {
        let mut config = Self::default();
        config.wal.sync_mode = WalSyncMode::Always;
        config.storage.sync_writes = true;
        config
    }

    /// Create a config optimized for memory usage
    pub fn low_memory() -> Self {
        let mut config = Self::default();
        config.memory.memtable_size = 16 * 1024 * 1024;  // 16 MB
        config.memory.max_memtables = 2;
        config.cache.block_cache_size = 32 * 1024 * 1024;  // 32 MB
        config.cache.row_cache_size = 16 * 1024 * 1024;    // 16 MB
        config
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.memory.memtable_size == 0 {
            return Err("memtable_size must be > 0".to_string());
        }
        if self.memory.block_size == 0 {
            return Err("block_size must be > 0".to_string());
        }
        if self.sharding.replication_factor == 0 {
            return Err("replication_factor must be > 0".to_string());
        }
        Ok(())
    }
}
