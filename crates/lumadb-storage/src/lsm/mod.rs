//! LSM-Tree based key-value storage

use std::path::Path;
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use parking_lot::RwLock;
use tracing::debug;

use lumadb_common::error::Result;

use crate::cache::BufferPool;

/// LSM-Tree implementation
pub struct LsmTree {
    /// Active memtable (skip list for fast writes)
    memtable: RwLock<Arc<SkipMap<Vec<u8>, Vec<u8>>>>,
    /// Immutable memtables waiting to be flushed
    immutable_memtables: RwLock<Vec<Arc<SkipMap<Vec<u8>, Vec<u8>>>>>,
    /// Buffer pool for caching
    buffer_pool: Arc<BufferPool>,
    /// Storage path
    #[allow(dead_code)]
    path: std::path::PathBuf,
    /// Maximum memtable size before flush
    max_memtable_size: usize,
    /// Current memtable size
    memtable_size: std::sync::atomic::AtomicUsize,
}

impl LsmTree {
    /// Create a new LSM tree
    pub fn new(path: &Path, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            memtable: RwLock::new(Arc::new(SkipMap::new())),
            immutable_memtables: RwLock::new(Vec::new()),
            buffer_pool,
            path: path.to_path_buf(),
            max_memtable_size: 64 * 1024 * 1024, // 64MB
            memtable_size: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Put a key-value pair
    pub fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let size = key.len() + value.len();

        // Insert into memtable
        {
            let memtable = self.memtable.read();
            memtable.insert(key.clone(), value.clone());
        }

        // Update size
        let current_size = self
            .memtable_size
            .fetch_add(size, std::sync::atomic::Ordering::SeqCst);

        // Check if we need to flush
        if current_size + size > self.max_memtable_size {
            self.maybe_flush()?;
        }

        // Update cache
        self.buffer_pool.put(&key, &value);

        Ok(())
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check cache first
        if let Some(value) = self.buffer_pool.get(key) {
            return Ok(Some(value));
        }

        // Check active memtable
        let memtable = self.memtable.read();
        if let Some(entry) = memtable.get(key) {
            let value = entry.value().clone();
            drop(entry); // Explicitly drop the entry before returning
            self.buffer_pool.put(key, &value);
            return Ok(Some(value));
        }
        drop(memtable);

        // Check immutable memtables
        let immutables = self.immutable_memtables.read();
        for imm in immutables.iter().rev() {
            if let Some(entry) = imm.get(key) {
                let value = entry.value().clone();
                drop(entry);
                self.buffer_pool.put(key, &value);
                return Ok(Some(value));
            }
        }

        // TODO: Check SSTable files on disk

        Ok(None)
    }

    /// Delete a key
    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        // Check if key exists
        let existed = self.get(key)?.is_some();

        if existed {
            // Remove from memtable
            let memtable = self.memtable.read();
            memtable.remove(key);
            self.buffer_pool.remove(key);
        }

        Ok(existed)
    }

    /// Scan keys with a prefix
    pub fn scan(&self, prefix: &[u8]) -> Result<ScanIterator> {
        let memtable = self.memtable.read().clone();
        Ok(ScanIterator {
            memtable,
            prefix: prefix.to_vec(),
            started: false,
        })
    }

    /// Maybe flush memtable to disk
    fn maybe_flush(&self) -> Result<()> {
        let current_size = self
            .memtable_size
            .load(std::sync::atomic::Ordering::SeqCst);

        if current_size > self.max_memtable_size {
            debug!("Flushing memtable, size: {}", current_size);

            // Swap memtable with a new one
            let old_memtable = {
                let mut memtable_lock = self.memtable.write();
                std::mem::replace(&mut *memtable_lock, Arc::new(SkipMap::new()))
            };

            // Add old memtable to immutable list
            self.immutable_memtables.write().push(old_memtable);

            // Reset size
            self.memtable_size
                .store(0, std::sync::atomic::Ordering::SeqCst);

            // TODO: Schedule background flush to SSTable
        }

        Ok(())
    }

    /// Force flush all memtables
    pub fn flush(&self) -> Result<()> {
        self.maybe_flush()
    }

    /// Get approximate size
    pub fn approximate_size(&self) -> usize {
        self.memtable_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Iterator for scanning keys
pub struct ScanIterator {
    memtable: Arc<SkipMap<Vec<u8>, Vec<u8>>>,
    prefix: Vec<u8>,
    started: bool,
}

impl Iterator for ScanIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
        }

        // Find next entry with prefix
        for entry in self.memtable.iter() {
            let key = entry.key();
            if key.starts_with(&self.prefix) {
                return Some(Ok((key.clone(), entry.value().clone())));
            }
            if key > &self.prefix {
                break;
            }
        }

        None
    }
}
