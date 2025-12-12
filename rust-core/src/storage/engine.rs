//! StorageEngine - Core LSM-tree storage implementation
//!
//! Features:
//! - Memory-mapped file access for zero-copy reads
//! - Write-ahead logging for durability
//! - Background compaction
//! - Bloom filters for fast negative lookups

use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::config::Config;
use crate::memory::{MemTable, BlockCache};
use crate::types::{KeyValue, KeyRange, SequenceNumber, Compression, Query, Document, DocumentId};
use crate::error::{Result, LumaError};
use crate::wal::WriteAheadLog;

use super::sstable::{SSTable, SSTableBuilder};
use super::manifest::Manifest;

/// LSM-tree storage engine
pub struct StorageEngine {
    /// Configuration
    config: Arc<Config>,

    /// Active memtable for writes
    active_memtable: Arc<RwLock<MemTable>>,

    /// Immutable memtables pending flush
    immutable_memtables: Arc<RwLock<Vec<Arc<MemTable>>>>,

    /// Block cache for reads
    block_cache: Arc<BlockCache>,

    /// SSTable files organized by level
    levels: Arc<RwLock<Vec<Vec<Arc<SSTable>>>>>,

    /// Write-ahead log
    wal: Arc<WriteAheadLog>,

    /// Manifest for tracking files
    manifest: Arc<RwLock<Manifest>>,

    /// Sequence number counter
    sequence: Arc<std::sync::atomic::AtomicU64>,

    /// Background task sender
    bg_sender: mpsc::UnboundedSender<BackgroundTask>,

    /// Data directory
    data_dir: PathBuf,
}

enum BackgroundTask {
    Flush(Arc<MemTable>),
    Compact { level: usize },
    Shutdown,
}

impl StorageEngine {
    /// Create a new storage engine
    pub async fn open(config: Config) -> Result<Self> {
        let data_dir = config.data_dir.clone();

        // Ensure directories exist
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(data_dir.join("sstables"))?;

        // Initialize WAL
        let wal = Arc::new(WriteAheadLog::new(&config)?);

        // Initialize manifest
        let manifest = Arc::new(RwLock::new(Manifest::open(&data_dir)?));

        // Initialize cache
        let block_cache = Arc::new(BlockCache::new(config.cache.block_cache_size));

        // Load existing SSTables
        let levels = Self::load_sstables(&data_dir, &manifest.read())?;

        // Create background task channel
        let (bg_sender, bg_receiver) = mpsc::unbounded_channel();

        let engine = Self {
            config: Arc::new(config.clone()),
            active_memtable: Arc::new(RwLock::new(MemTable::new(config.memory.memtable_size))),
            immutable_memtables: Arc::new(RwLock::new(Vec::new())),
            block_cache,
            levels: Arc::new(RwLock::new(levels)),
            wal,
            manifest,
            sequence: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            bg_sender,
            data_dir,
        };

        // Start background workers
        engine.start_background_workers(bg_receiver);

        // Recover from WAL
        engine.recover().await?;

        Ok(engine)
    }

    /// Put a key-value pair
    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let seq = self.next_sequence();
        let kv = KeyValue::new(key.clone(), value.clone(), seq);

        // Write to WAL first
        self.wal.append(&kv).await?;

        // Write to memtable
        let should_flush = {
            let memtable = self.active_memtable.read();
            memtable.put(kv)?
        };

        // Trigger flush if needed
        if should_flush {
            self.maybe_schedule_flush();
        }

        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check memtable first (newest data)
        if let Some(kv) = self.active_memtable.read().get(key) {
            if kv.deleted {
                return Ok(None);
            }
            return Ok(Some(kv.value.clone()));
        }

        // Check immutable memtables
        for memtable in self.immutable_memtables.read().iter().rev() {
            if let Some(kv) = memtable.get(key) {
                if kv.deleted {
                    return Ok(None);
                }
                return Ok(Some(kv.value.clone()));
            }
        }

        // Check block cache
        if let Some(value) = self.block_cache.get(key) {
            return Ok(Some(value.to_vec()));
        }

        // Check SSTables (level by level)
        let levels = self.levels.read();
        for level in levels.iter() {
            for sstable in level.iter().rev() {
                if let Some(value) = sstable.get(key)? {
                    // Add to cache
                    self.block_cache.insert(key.to_vec(), value.clone());
                    return Ok(Some(value));
                }
            }
        }

        Ok(None)
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let seq = self.next_sequence();
        let tombstone = KeyValue::tombstone(key.to_vec(), seq);

        // Write to WAL
        self.wal.append(&tombstone).await?;

        // Write tombstone to memtable
        self.active_memtable.read().put(tombstone)?;

        Ok(())
    }

    /// Scan a key range
    pub async fn scan(&self, range: KeyRange) -> Result<Vec<KeyValue>> {
        let mut results = Vec::new();

        // Collect from memtables
        let start = range.start.as_deref().unwrap_or(&[]);
        let end = range.end.as_deref().unwrap_or(&[0xff; 256]);

        // Active memtable
        results.extend(self.active_memtable.read().scan(start, end));

        // Immutable memtables
        for memtable in self.immutable_memtables.read().iter() {
            results.extend(memtable.scan(start, end));
        }

        // SSTables
        let levels = self.levels.read();
        for level in levels.iter() {
            for sstable in level.iter() {
                results.extend(sstable.scan(start, end)?);
            }
        }

        // Sort by key and deduplicate (keep newest)
        results.sort_by(|a, b| (&a.key, std::cmp::Reverse(a.sequence))
            .cmp(&(&b.key, std::cmp::Reverse(b.sequence))));
        results.dedup_by(|a, b| a.key == b.key);

        // Filter out tombstones
        results.retain(|kv| !kv.deleted);

        Ok(results)
    }

    /// Flush memtable to disk
    pub async fn flush(&self) -> Result<()> {
        let memtable = {
            let mut active = self.active_memtable.write();
            let old = std::mem::replace(
                &mut *active,
                MemTable::new(self.config.memory.memtable_size),
            );
            Arc::new(old)
        };

        self.flush_memtable(memtable).await
    }

    /// Force compaction
    pub async fn compact(&self) -> Result<()> {
        for level in 0..self.config.compaction.num_levels {
            self.compact_level(level).await?;
        }
        Ok(())
    }

    /// Close the storage engine
    pub async fn close(&self) -> Result<()> {
        // Flush remaining data
        self.flush().await?;

        // Sync WAL
        self.wal.sync().await?;

        // Signal background workers to stop
        let _ = self.bg_sender.send(BackgroundTask::Shutdown);

        Ok(())
    }

    // ============================================================================
    // Internal Methods
    // ============================================================================

    fn next_sequence(&self) -> SequenceNumber {
        self.sequence.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn maybe_schedule_flush(&self) {
        let memtable = self.active_memtable.read();
        if memtable.is_full() {
            // Move to immutable
            drop(memtable);

            let memtable = {
                let mut active = self.active_memtable.write();
                let old = std::mem::replace(
                    &mut *active,
                    MemTable::new(self.config.memory.memtable_size),
                );
                Arc::new(old)
            };

            self.immutable_memtables.write().push(memtable.clone());
            let _ = self.bg_sender.send(BackgroundTask::Flush(memtable));
        }
    }

    async fn flush_memtable(&self, memtable: Arc<MemTable>) -> Result<()> {
        // Build SSTable
        let sstable_path = self.data_dir
            .join("sstables")
            .join(format!("{}_{}.sst", 0, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)));

        let mut builder = SSTableBuilder::new(
            &sstable_path,
            self.config.storage.compression,
            self.config.storage.bloom_bits_per_key,
        )?;

        for (key, kv) in memtable.iter() {
            builder.add(&key, &kv)?;
        }

        let sstable = builder.finish()?;

        // Add to level 0
        self.levels.write()[0].push(Arc::new(sstable));

        // Update manifest
        self.manifest.write().add_sstable(0, &sstable_path)?;

        // Remove from immutable list
        self.immutable_memtables.write().retain(|m| !Arc::ptr_eq(m, &memtable));

        // Check if compaction needed
        if self.levels.read()[0].len() >= self.config.compaction.level0_file_num_trigger {
            let _ = self.bg_sender.send(BackgroundTask::Compact { level: 0 });
        }

        Ok(())
    }

    async fn compact_level(&self, level: usize) -> Result<()> {
        let max_level = self.config.compaction.num_levels - 1;
        
        // 1. Identify input files from current level (Level L)
        // For L0, we take all files to resolve overlaps.
        // For L > 0, taking all files guarantees we form a comprehensive range.
        let files_l = {
            let levels = self.levels.read();
            if level >= levels.len() || levels[level].is_empty() {
                return Ok(());
            }
            levels[level].clone()
        };

        if files_l.is_empty() {
            return Ok(());
        }

        // 2. Identify overlapping files from next level (Level L+1)
        let target_level = (level + 1).min(max_level);
        let mut min_key_l = files_l[0].min_key().to_vec();
        let mut max_key_l = files_l[0].max_key().to_vec();

        for sst in &files_l {
            if sst.min_key() < min_key_l.as_slice() {
                min_key_l = sst.min_key().to_vec();
            }
            if sst.max_key() > max_key_l.as_slice() {
                max_key_l = sst.max_key().to_vec();
            }
        }

        let files_l_plus_1 = {
            let levels = self.levels.read();
            if target_level >= levels.len() {
                Vec::new()
            } else {
                levels[target_level].iter()
                    .filter(|sst| {
                        // Check for overlap: !(sst.max < range.min || sst.min > range.max)
                        // sst.max < min_key_l || sst.min > max_key_l
                        let sst_min = sst.min_key();
                        let sst_max = sst.max_key();
                        !(sst_max < min_key_l.as_slice() || sst_min > max_key_l.as_slice())
                    })
                    .cloned()
                    .collect()
            }
        };

        let mut files_to_compact = files_l.clone();
        files_to_compact.extend(files_l_plus_1.clone());

        // 3. Merge all entries
        let mut entries: Vec<KeyValue> = Vec::new();
        for sstable in &files_to_compact {
            // Optimization: use iterator instead of loading full vector
            entries.extend(sstable.scan(&[], &[0xff; 256])?);
        }

        // 4. Sort and deduplicate
        entries.sort_by(|a, b| (&a.key, std::cmp::Reverse(a.sequence))
            .cmp(&(&b.key, std::cmp::Reverse(b.sequence))));
        entries.dedup_by(|a, b| a.key == b.key);

        // Remove tombstones at max level
        if target_level == max_level {
            entries.retain(|kv| !kv.deleted);
        }

        if entries.is_empty() {
             // Remove inputs if result is empty
             let mut levels = self.levels.write();
             if level < levels.len() {
                 levels[level].clear();
             }
             if target_level < levels.len() {
                 levels[target_level].retain(|sst| !files_l_plus_1.iter().any(|f| Arc::ptr_eq(f, sst)));
             }
              // Update manifest
             let mut manifest = self.manifest.write();
             for sstable in &files_to_compact {
                 manifest.remove_sstable(level, sstable.path()).ok(); // Ignore errors (might be in L or L+1)
                 manifest.remove_sstable(target_level, sstable.path()).ok();
             }
             return Ok(());
        }

        // 5. Split and Write Output Files
        let mut new_sstables = Vec::new();
        let target_file_size = self.config.compaction.target_file_size_base;
        
        let mut current_entries = Vec::new();
        let mut current_size = 0;

        for kv in entries {
            // Approximate size
            let kv_size = kv.key.len() + kv.value.len() + 16; 
            current_entries.push(kv);
            current_size += kv_size;

            if current_size >= target_file_size {
                 let sstable = self.write_sstable(&current_entries, target_level).await?;
                 new_sstables.push(sstable);
                 current_entries.clear();
                 current_size = 0;
            }
        }

        if !current_entries.is_empty() {
            let sstable = self.write_sstable(&current_entries, target_level).await?;
            new_sstables.push(sstable);
        }

        // 6. Update Levels atomically (as much as possible)
        {
            let mut levels = self.levels.write();
            
            // Ensure target level exists
            while levels.len() <= target_level {
                levels.push(Vec::new());
            }

            // Remove old files from Level L (we took ALL of them)
            if level < levels.len() {
                levels[level].clear();
            }

            // Remove overlapping files from Level L+1
            levels[target_level].retain(|sst| !files_l_plus_1.iter().any(|f| Arc::ptr_eq(f, sst)));

            // Add new files to Level L+1
            for sst in &new_sstables {
                levels[target_level].push(sst.clone());
            }
            // Sort level L+1 by min_key to maintain sorted invariant for next runs
            levels[target_level].sort_by(|a, b| a.min_key().cmp(b.min_key()));
        }

        // 7. Update Manifest
        let mut manifest = self.manifest.write();
        // Remove old
        for sstable in &files_l {
            manifest.remove_sstable(level, sstable.path()).ok();
        }
        for sstable in &files_l_plus_1 {
            manifest.remove_sstable(target_level, sstable.path()).ok();
        }
        // Add new
        for sstable in &new_sstables {
            manifest.add_sstable(target_level, sstable.path())?;
        }

        Ok(())
    }

    async fn write_sstable(&self, entries: &[KeyValue], level: usize) -> Result<Arc<SSTable>> {
         let sstable_path = self.data_dir
            .join("sstables")
            .join(format!("{}_{}.sst", level, chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)));

        let mut builder = SSTableBuilder::new(
            &sstable_path,
            self.config.storage.compression,
            self.config.storage.bloom_bits_per_key,
        )?;

        for kv in entries {
            builder.add(&kv.key, kv)?;
        }

        let sstable = builder.finish()?;
        Ok(Arc::new(sstable))
    }

    async fn recover(&self) -> Result<()> {
        // Recover from WAL
        let entries = self.wal.recover().await?;

        for entry in entries {
            self.active_memtable.read().put(entry)?;
        }

        Ok(())
    }

    fn load_sstables(data_dir: &PathBuf, manifest: &Manifest) -> Result<Vec<Vec<Arc<SSTable>>>> {
        let mut levels = Vec::new();

        for (level, paths) in manifest.sstables() {
            while levels.len() <= level {
                levels.push(Vec::new());
            }

            for path in paths {
                let sstable = SSTable::open(path)?;
                levels[level].push(Arc::new(sstable));
            }
        }

        // Ensure at least some levels exist
        while levels.len() < 7 {
            levels.push(Vec::new());
        }

        Ok(levels)
    }

    fn start_background_workers(&self, mut receiver: mpsc::UnboundedReceiver<BackgroundTask>) {
        let engine = self.clone_for_background();

        tokio::spawn(async move {
            while let Some(task) = receiver.recv().await {
                match task {
                    BackgroundTask::Flush(memtable) => {
                        if let Err(e) = engine.flush_memtable(memtable).await {
                            eprintln!("Flush error: {}", e);
                        }
                    }
                    BackgroundTask::Compact { level } => {
                        if let Err(e) = engine.compact_level(level).await {
                            eprintln!("Compaction error: {}", e);
                        }
                    }
                    BackgroundTask::Shutdown => {
                        break;
                    }
                }
            }
        });
    }

    fn clone_for_background(&self) -> Self {
        Self {
            config: self.config.clone(),
            active_memtable: self.active_memtable.clone(),
            immutable_memtables: self.immutable_memtables.clone(),
            block_cache: self.block_cache.clone(),
            levels: self.levels.clone(),
            wal: self.wal.clone(),
            manifest: self.manifest.clone(),
            sequence: self.sequence.clone(),
            bg_sender: self.bg_sender.clone(),
            data_dir: self.data_dir.clone(),
        }
    }
}
