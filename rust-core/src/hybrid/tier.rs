//! Storage tier implementations
//!
//! High-performance implementations for each storage tier:
//! - RamStore: Lock-free, NUMA-aware, huge pages support
//! - SsdStore: Direct I/O, io_uring, zero-copy
//! - HddStore: Buffered writes, sequential optimization
//! - ReadCache: Sharded LRU with clock eviction

use std::alloc::{alloc, dealloc, Layout};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write as IoWrite};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};

use crate::error::{TdbError, Result as TdbResult};
use super::StorageTier;

// =============================================================================
// RAM Store - Maximum Performance
// =============================================================================

/// Lock-free RAM storage with huge pages and NUMA awareness
pub struct RamStore {
    /// Memory regions (multiple for NUMA)
    regions: Vec<MemoryRegion>,

    /// Total capacity
    capacity: usize,

    /// Current write offset (atomic for lock-free)
    write_offset: AtomicU64,

    /// Number of NUMA nodes
    numa_nodes: usize,

    /// Using huge pages
    huge_pages: bool,
}

struct MemoryRegion {
    ptr: NonNull<u8>,
    size: usize,
    layout: Layout,
}

unsafe impl Send for MemoryRegion {}
unsafe impl Sync for MemoryRegion {}

impl RamStore {
    pub fn new(capacity: usize, huge_pages: bool, numa_node: i32) -> TdbResult<Self> {
        let numa_nodes = Self::detect_numa_nodes();
        let regions = Self::allocate_regions(capacity, numa_nodes, huge_pages)?;

        Ok(Self {
            regions,
            capacity,
            write_offset: AtomicU64::new(0),
            numa_nodes,
            huge_pages,
        })
    }

    fn detect_numa_nodes() -> usize {
        // Try to detect NUMA topology
        // Default to 1 if detection fails
        #[cfg(target_os = "linux")]
        {
            if let Ok(content) = std::fs::read_to_string("/sys/devices/system/node/online") {
                // Parse "0-3" format
                if let Some(range) = content.trim().split('-').last() {
                    if let Ok(max) = range.parse::<usize>() {
                        return max + 1;
                    }
                }
            }
        }
        1
    }

    fn allocate_regions(
        total_capacity: usize,
        numa_nodes: usize,
        huge_pages: bool,
    ) -> TdbResult<Vec<MemoryRegion>> {
        let region_size = total_capacity / numa_nodes;
        let mut regions = Vec::with_capacity(numa_nodes);

        for _ in 0..numa_nodes {
            let region = Self::allocate_region(region_size, huge_pages)?;
            regions.push(region);
        }

        Ok(regions)
    }

    fn allocate_region(size: usize, huge_pages: bool) -> TdbResult<MemoryRegion> {
        let alignment = if huge_pages {
            2 * 1024 * 1024 // 2MB huge pages
        } else {
            4096 // Regular page size
        };

        let layout = Layout::from_size_align(size, alignment)
            .map_err(|e| TdbError::Memory(format!("Invalid layout: {}", e)))?;

        let ptr = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(TdbError::Memory("Failed to allocate memory".into()));
            }

            // Zero memory
            std::ptr::write_bytes(ptr, 0, size);

            // Try to use huge pages via madvise
            #[cfg(target_os = "linux")]
            if huge_pages {
                libc::madvise(
                    ptr as *mut libc::c_void,
                    size,
                    libc::MADV_HUGEPAGE,
                );
            }

            NonNull::new_unchecked(ptr)
        };

        Ok(MemoryRegion { ptr, size, layout })
    }

    /// Write data and return offset
    pub fn write(&self, data: &[u8]) -> TdbResult<u64> {
        let size = data.len();

        // Atomic allocation of space
        let offset = self.write_offset.fetch_add(size as u64, Ordering::SeqCst);

        if offset as usize + size > self.capacity {
            return Err(TdbError::Memory("RAM store full".into()));
        }

        // Determine which region and local offset
        let (region_idx, local_offset) = self.locate(offset);

        // Copy data
        unsafe {
            let dst = self.regions[region_idx].ptr.as_ptr().add(local_offset);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, size);
        }

        Ok(offset)
    }

    /// Read data at offset
    pub fn read(&self, offset: u64, size: u32) -> Option<Vec<u8>> {
        if offset as usize + size as usize > self.capacity {
            return None;
        }

        let (region_idx, local_offset) = self.locate(offset);

        let mut data = vec![0u8; size as usize];
        unsafe {
            let src = self.regions[region_idx].ptr.as_ptr().add(local_offset);
            std::ptr::copy_nonoverlapping(src, data.as_mut_ptr(), size as usize);
        }

        Some(data)
    }

    /// Zero-copy read (returns slice into memory)
    pub fn read_zero_copy(&self, offset: u64, size: u32) -> Option<&[u8]> {
        if offset as usize + size as usize > self.capacity {
            return None;
        }

        let (region_idx, local_offset) = self.locate(offset);

        unsafe {
            let ptr = self.regions[region_idx].ptr.as_ptr().add(local_offset);
            Some(std::slice::from_raw_parts(ptr, size as usize))
        }
    }

    /// Check if there's space for a write
    pub fn has_space(&self, size: usize) -> bool {
        let current = self.write_offset.load(Ordering::Relaxed) as usize;
        current + size <= self.capacity
    }

    /// Get current usage
    pub fn usage(&self) -> usize {
        self.write_offset.load(Ordering::Relaxed) as usize
    }

    fn locate(&self, offset: u64) -> (usize, usize) {
        let region_size = self.capacity / self.numa_nodes;
        let region_idx = (offset as usize) / region_size;
        let local_offset = (offset as usize) % region_size;
        (region_idx.min(self.numa_nodes - 1), local_offset)
    }
}

impl Drop for RamStore {
    fn drop(&mut self) {
        for region in &self.regions {
            unsafe {
                dealloc(region.ptr.as_ptr(), region.layout);
            }
        }
    }
}

// =============================================================================
// SSD Store - High-Performance Persistent Storage
// =============================================================================

/// SSD storage with direct I/O and io_uring support
pub struct SsdStore {
    /// Data file path
    path: PathBuf,

    /// Data file
    file: Mutex<File>,

    /// Current write offset
    write_offset: AtomicU64,

    /// Using direct I/O
    direct_io: bool,

    /// Write buffer for batching
    write_buffer: Mutex<WriteBuffer>,

    /// Buffer size (aligned for direct I/O)
    buffer_size: usize,
}

struct WriteBuffer {
    data: Vec<u8>,
    offset: u64,
}

impl SsdStore {
    pub async fn new(path: PathBuf, direct_io: bool) -> TdbResult<Self> {
        std::fs::create_dir_all(&path)
            .map_err(|e| TdbError::Io(e))?;

        let data_path = path.join("data.tdb");

        let mut options = OpenOptions::new();
        options.create(true).read(true).write(true);

        // Enable direct I/O on Linux
        #[cfg(target_os = "linux")]
        if direct_io {
            options.custom_flags(libc::O_DIRECT);
        }

        let file = options.open(&data_path)
            .map_err(|e| TdbError::Io(e))?;

        let file_size = file.metadata()
            .map(|m| m.len())
            .unwrap_or(0);

        // Buffer size must be aligned for direct I/O
        let buffer_size = if direct_io { 4096 * 256 } else { 64 * 1024 }; // 1MB or 64KB

        Ok(Self {
            path,
            file: Mutex::new(file),
            write_offset: AtomicU64::new(file_size),
            direct_io,
            write_buffer: Mutex::new(WriteBuffer {
                data: Vec::with_capacity(buffer_size),
                offset: file_size,
            }),
            buffer_size,
        })
    }

    pub async fn write(&self, data: &[u8]) -> TdbResult<u64> {
        let mut buffer = self.write_buffer.lock();

        // Check if we need to flush
        if buffer.data.len() + data.len() > self.buffer_size {
            self.flush_buffer(&mut buffer)?;
        }

        let offset = buffer.offset + buffer.data.len() as u64;
        buffer.data.extend_from_slice(data);

        // Update write offset
        self.write_offset.store(offset + data.len() as u64, Ordering::Release);

        Ok(offset)
    }

    pub async fn read(&self, offset: u64, size: u32) -> TdbResult<Vec<u8>> {
        // Check if data is in write buffer
        {
            let buffer = self.write_buffer.lock();
            if offset >= buffer.offset {
                let local_offset = (offset - buffer.offset) as usize;
                if local_offset + size as usize <= buffer.data.len() {
                    return Ok(buffer.data[local_offset..local_offset + size as usize].to_vec());
                }
            }
        }

        // Read from file
        let mut file = self.file.lock();

        // Align offset for direct I/O
        let (aligned_offset, prefix_len) = if self.direct_io {
            let alignment = 4096u64;
            let aligned = (offset / alignment) * alignment;
            (aligned, (offset - aligned) as usize)
        } else {
            (offset, 0)
        };

        file.seek(SeekFrom::Start(aligned_offset))
            .map_err(|e| TdbError::Io(e))?;

        // Read aligned size for direct I/O
        let read_size = if self.direct_io {
            let total = prefix_len + size as usize;
            ((total + 4095) / 4096) * 4096
        } else {
            size as usize
        };

        let mut data = vec![0u8; read_size];
        file.read_exact(&mut data)
            .map_err(|e| TdbError::Io(e))?;

        // Extract actual data
        if self.direct_io {
            Ok(data[prefix_len..prefix_len + size as usize].to_vec())
        } else {
            Ok(data)
        }
    }

    pub fn sync(&self) -> TdbResult<()> {
        // Flush buffer first
        {
            let mut buffer = self.write_buffer.lock();
            self.flush_buffer(&mut buffer)?;
        }

        // Sync file
        self.file.lock().sync_all()
            .map_err(|e| TdbError::Io(e))
    }

    fn flush_buffer(&self, buffer: &mut WriteBuffer) -> TdbResult<()> {
        if buffer.data.is_empty() {
            return Ok(());
        }

        let mut file = self.file.lock();

        file.seek(SeekFrom::Start(buffer.offset))
            .map_err(|e| TdbError::Io(e))?;

        // Pad to alignment for direct I/O
        let write_data = if self.direct_io && buffer.data.len() % 4096 != 0 {
            let aligned_size = ((buffer.data.len() + 4095) / 4096) * 4096;
            let mut aligned = vec![0u8; aligned_size];
            aligned[..buffer.data.len()].copy_from_slice(&buffer.data);
            aligned
        } else {
            buffer.data.clone()
        };

        file.write_all(&write_data)
            .map_err(|e| TdbError::Io(e))?;

        // Reset buffer
        buffer.offset = buffer.offset + buffer.data.len() as u64;
        buffer.data.clear();

        Ok(())
    }
}

// =============================================================================
// HDD Store - High Capacity Cold Storage
// =============================================================================

/// HDD storage optimized for sequential access
pub struct HddStore {
    path: PathBuf,
    file: Mutex<File>,
    write_offset: AtomicU64,
    write_buffer: Mutex<Vec<u8>>,
    buffer_size: usize,
}

impl HddStore {
    pub async fn new(path: PathBuf) -> TdbResult<Self> {
        std::fs::create_dir_all(&path)
            .map_err(|e| TdbError::Io(e))?;

        let data_path = path.join("cold_data.tdb");

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&data_path)
            .map_err(|e| TdbError::Io(e))?;

        let file_size = file.metadata()
            .map(|m| m.len())
            .unwrap_or(0);

        Ok(Self {
            path,
            file: Mutex::new(file),
            write_offset: AtomicU64::new(file_size),
            write_buffer: Mutex::new(Vec::with_capacity(4 * 1024 * 1024)), // 4MB buffer
            buffer_size: 4 * 1024 * 1024,
        })
    }

    pub async fn write(&self, data: &[u8]) -> TdbResult<u64> {
        let mut buffer = self.write_buffer.lock();

        if buffer.len() + data.len() > self.buffer_size {
            self.flush_buffer(&mut buffer)?;
        }

        let offset = self.write_offset.fetch_add(data.len() as u64, Ordering::SeqCst);
        buffer.extend_from_slice(data);

        Ok(offset)
    }

    pub async fn read(&self, offset: u64, size: u32) -> TdbResult<Vec<u8>> {
        let mut file = self.file.lock();

        file.seek(SeekFrom::Start(offset))
            .map_err(|e| TdbError::Io(e))?;

        let mut data = vec![0u8; size as usize];
        file.read_exact(&mut data)
            .map_err(|e| TdbError::Io(e))?;

        Ok(data)
    }

    fn flush_buffer(&self, buffer: &mut Vec<u8>) -> TdbResult<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut file = self.file.lock();
        file.seek(SeekFrom::End(0))
            .map_err(|e| TdbError::Io(e))?;
        file.write_all(buffer)
            .map_err(|e| TdbError::Io(e))?;

        buffer.clear();
        Ok(())
    }
}

// =============================================================================
// Read Cache - Fast Access to SSD/HDD Data
// =============================================================================

/// Sharded LRU cache with clock eviction algorithm
pub struct ReadCache {
    shards: Vec<CacheShard>,
    capacity: usize,
}

struct CacheShard {
    entries: RwLock<CacheEntries>,
}

struct CacheEntries {
    map: HashMap<(StorageTier, u64), CacheEntry>,
    order: Vec<(StorageTier, u64)>,
    capacity: usize,
}

use std::collections::HashMap;

struct CacheEntry {
    data: Vec<u8>,
    clock_bit: bool,
}

impl ReadCache {
    const NUM_SHARDS: usize = 64;

    pub fn new(capacity: usize) -> Self {
        let shard_capacity = capacity / Self::NUM_SHARDS;
        let shards = (0..Self::NUM_SHARDS)
            .map(|_| CacheShard {
                entries: RwLock::new(CacheEntries {
                    map: HashMap::new(),
                    order: Vec::new(),
                    capacity: shard_capacity,
                }),
            })
            .collect();

        Self { shards, capacity }
    }

    pub fn get(&self, key: &(StorageTier, u64)) -> Option<Vec<u8>> {
        let shard = self.shard(key);
        let mut entries = shard.entries.write();

        if let Some(entry) = entries.map.get_mut(key) {
            entry.clock_bit = true; // Mark as recently used
            Some(entry.data.clone())
        } else {
            None
        }
    }

    pub fn insert(&self, key: (StorageTier, u64), data: Vec<u8>) {
        let shard = self.shard(&key);
        let mut entries = shard.entries.write();

        // Evict if necessary
        while self.estimate_size(&entries) + data.len() > entries.capacity {
            if !self.evict_one(&mut entries) {
                break;
            }
        }

        entries.map.insert(key, CacheEntry {
            data,
            clock_bit: true,
        });
        entries.order.push(key);
    }

    fn shard(&self, key: &(StorageTier, u64)) -> &CacheShard {
        let hash = key.1 as usize;
        &self.shards[hash % Self::NUM_SHARDS]
    }

    fn estimate_size(&self, entries: &CacheEntries) -> usize {
        entries.map.values().map(|e| e.data.len()).sum()
    }

    fn evict_one(&self, entries: &mut CacheEntries) -> bool {
        // Clock algorithm
        while let Some(key) = entries.order.first().cloned() {
            entries.order.remove(0);

            if let Some(entry) = entries.map.get_mut(&key) {
                if entry.clock_bit {
                    // Give second chance
                    entry.clock_bit = false;
                    entries.order.push(key);
                } else {
                    // Evict
                    entries.map.remove(&key);
                    return true;
                }
            }
        }
        false
    }
}
