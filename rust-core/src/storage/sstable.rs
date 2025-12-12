//! SSTable - Sorted String Table for persistent storage

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use memmap2::Mmap;

use crate::types::{KeyValue, Compression};
use crate::error::{Result, LumaError};

/// SSTable file format:
/// [Header] [Data Blocks] [Index Block] [Bloom Filter] [Footer]
pub struct SSTable {
    path: PathBuf,
    mmap: Option<Mmap>,
    index: Vec<IndexEntry>,
    bloom: BloomFilter,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    entry_count: usize,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexEntry {
    key: Vec<u8>,
    offset: u64,
    size: u32,
}

struct BloomFilter {
    bits: Vec<u64>,
    num_hashes: usize,
}

impl SSTable {
    /// Open an existing SSTable
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };

        // Parse footer to get offsets
        let footer = Self::read_footer(&mmap)?;

        // Read index
        let index = Self::read_index(&mmap, footer.index_offset, footer.index_size)?;

        // Read bloom filter
        let bloom = Self::read_bloom(&mmap, footer.bloom_offset, footer.bloom_size)?;

        let (min_key, max_key) = if index.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            (index.first().unwrap().key.clone(), index.last().unwrap().key.clone())
        };

        Ok(Self {
            path: path.to_path_buf(),
            mmap: Some(mmap),
            index,
            bloom,
            min_key,
            max_key,
            entry_count: footer.entry_count as usize,
        })
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check bloom filter first
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }

        // Binary search in index
        let idx = self.index.binary_search_by(|e| e.key.as_slice().cmp(key));

        match idx {
            Ok(i) => {
                let entry = &self.index[i];
                let data = self.read_block(entry.offset, entry.size)?;
                let kv: KeyValue = bincode::deserialize(&data)?;
                if kv.deleted {
                    Ok(None)
                } else {
                    Ok(Some(kv.value))
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Scan a key range
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<KeyValue>> {
        let mut results = Vec::new();

        // Find start position
        let start_idx = self.index.partition_point(|e| e.key.as_slice() < start);

        for i in start_idx..self.index.len() {
            let entry = &self.index[i];
            if entry.key.as_slice() >= end {
                break;
            }

            let data = self.read_block(entry.offset, entry.size)?;
            let kv: KeyValue = bincode::deserialize(&data)?;
            results.push(kv);
        }

        Ok(results)
    }

    /// Get path
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get file size in bytes
    pub fn size(&self) -> u64 {
        self.mmap.as_ref().map(|m| m.len() as u64).unwrap_or(0)
    }

    /// Get min key
    pub fn min_key(&self) -> &[u8] {
        &self.min_key
    }

    /// Get max key
    pub fn max_key(&self) -> &[u8] {
        &self.max_key
    }

    fn read_block(&self, offset: u64, size: u32) -> Result<Vec<u8>> {
        let mmap = self.mmap.as_ref().ok_or(LumaError::FileNotFound("mmap".into()))?;
        let start = offset as usize;
        let end = start + size as usize;

        if end > mmap.len() {
            return Err(LumaError::Corruption("block extends past file".into()));
        }

        Ok(mmap[start..end].to_vec())
    }

    fn read_footer(mmap: &Mmap) -> Result<Footer> {
        if mmap.len() < 48 {
            return Err(LumaError::Corruption("file too small".into()));
        }

        let footer_start = mmap.len() - 48;
        let footer_data = &mmap[footer_start..];

        Ok(Footer {
            index_offset: u64::from_le_bytes(footer_data[0..8].try_into().unwrap()),
            index_size: u32::from_le_bytes(footer_data[8..12].try_into().unwrap()),
            bloom_offset: u64::from_le_bytes(footer_data[12..20].try_into().unwrap()),
            bloom_size: u32::from_le_bytes(footer_data[20..24].try_into().unwrap()),
            entry_count: u64::from_le_bytes(footer_data[24..32].try_into().unwrap()),
            // Checksum and magic number
        })
    }

    fn read_index(mmap: &Mmap, offset: u64, size: u32) -> Result<Vec<IndexEntry>> {
        let start = offset as usize;
        let end = start + size as usize;

        if end > mmap.len() {
            return Err(LumaError::Corruption("index extends past file".into()));
        }

        let data = &mmap[start..end];
        let entries: Vec<IndexEntry> = bincode::deserialize(data)?;
        Ok(entries)
    }

    fn read_bloom(mmap: &Mmap, offset: u64, size: u32) -> Result<BloomFilter> {
        let start = offset as usize;
        let end = start + size as usize;

        if end > mmap.len() {
            return Ok(BloomFilter::empty());
        }

        let data = &mmap[start..end];
        // Simple bloom filter deserialization
        let bits: Vec<u64> = bincode::deserialize(data).unwrap_or_default();
        Ok(BloomFilter { bits, num_hashes: 7 })
    }
}

struct Footer {
    index_offset: u64,
    index_size: u32,
    bloom_offset: u64,
    bloom_size: u32,
    entry_count: u64,
}

impl BloomFilter {
    fn empty() -> Self {
        Self { bits: Vec::new(), num_hashes: 7 }
    }

    fn may_contain(&self, key: &[u8]) -> bool {
        if self.bits.is_empty() {
            return true;
        }

        let hash = xxhash_rust::xxh3::xxh3_64(key);
        let num_bits = self.bits.len() * 64;

        for i in 0..self.num_hashes {
            let h1 = hash as usize;
            let h2 = (hash >> 32) as usize;
            let bit_pos = (h1.wrapping_add(i.wrapping_mul(h2))) % num_bits;
            let word = bit_pos / 64;
            let bit = bit_pos % 64;

            if word >= self.bits.len() || (self.bits[word] & (1 << bit)) == 0 {
                return false;
            }
        }
        true
    }
}

/// SSTable builder
pub struct SSTableBuilder {
    path: PathBuf,
    writer: BufWriter<File>,
    index: Vec<IndexEntry>,
    bloom_bits: Vec<u64>,
    compression: Compression,
    current_offset: u64,
    entry_count: u64,
}

impl SSTableBuilder {
    pub fn new(path: &Path, compression: Compression, bloom_bits_per_key: usize) -> Result<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path: path.to_path_buf(),
            writer,
            index: Vec::new(),
            bloom_bits: vec![0u64; 1024], // Initial size
            compression,
            current_offset: 0,
            entry_count: 0,
        })
    }

    pub fn add(&mut self, key: &[u8], kv: &KeyValue) -> Result<()> {
        let data = bincode::serialize(kv)?;

        // Compress if needed
        let data = match self.compression {
            Compression::Lz4 => lz4_flex::compress_prepend_size(&data),
            Compression::Zstd => zstd::encode_all(&data[..], 3)?,
            Compression::None => data,
        };

        // Write data
        let size = data.len() as u32;
        self.writer.write_all(&data)?;

        // Add to index
        self.index.push(IndexEntry {
            key: key.to_vec(),
            offset: self.current_offset,
            size,
        });

        // Update bloom filter
        self.add_to_bloom(key);

        self.current_offset += size as u64;
        self.entry_count += 1;

        Ok(())
    }

    pub fn finish(mut self) -> Result<SSTable> {
        // Write index
        let index_offset = self.current_offset;
        let index_data = bincode::serialize(&self.index)?;
        self.writer.write_all(&index_data)?;
        let index_size = index_data.len() as u32;
        self.current_offset += index_size as u64;

        // Write bloom filter
        let bloom_offset = self.current_offset;
        let bloom_data = bincode::serialize(&self.bloom_bits)?;
        self.writer.write_all(&bloom_data)?;
        let bloom_size = bloom_data.len() as u32;
        self.current_offset += bloom_size as u64;

        // Write footer
        self.writer.write_all(&index_offset.to_le_bytes())?;
        self.writer.write_all(&index_size.to_le_bytes())?;
        self.writer.write_all(&bloom_offset.to_le_bytes())?;
        self.writer.write_all(&bloom_size.to_le_bytes())?;
        self.writer.write_all(&self.entry_count.to_le_bytes())?;
        self.writer.write_all(&[0u8; 16])?; // Padding for checksum/magic

        self.writer.flush()?;

        SSTable::open(&self.path)
    }

    fn add_to_bloom(&mut self, key: &[u8]) {
        let hash = xxhash_rust::xxh3::xxh3_64(key);
        let num_bits = self.bloom_bits.len() * 64;

        for i in 0usize..7 {
            let h1 = hash as usize;
            let h2 = (hash >> 32) as usize;
            let bit_pos = (h1.wrapping_add(i.wrapping_mul(h2))) % num_bits;
            let word = bit_pos / 64;
            let bit = bit_pos % 64;

            if word < self.bloom_bits.len() {
                self.bloom_bits[word] |= 1 << bit;
            }
        }
    }
}

pub struct SSTableReader;
