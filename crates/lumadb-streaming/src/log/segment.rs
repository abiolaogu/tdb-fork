//! Log segment implementation

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::RwLock;

use lumadb_common::error::Result;
use lumadb_common::types::Offset;

/// A segment of the partition log
pub struct Segment {
    /// Base offset of this segment
    base_offset: Offset,
    /// In-memory index: offset -> position in data
    index: RwLock<BTreeMap<Offset, usize>>,
    /// Data storage (in production this would be memory-mapped files)
    data: RwLock<Vec<u8>>,
    /// Current size
    size: AtomicUsize,
}

impl Segment {
    /// Create a new segment
    pub fn new(base_offset: Offset) -> Self {
        Self {
            base_offset,
            index: RwLock::new(BTreeMap::new()),
            data: RwLock::new(Vec::new()),
            size: AtomicUsize::new(0),
        }
    }

    /// Append data at offset
    pub fn append(&self, offset: Offset, data: &[u8]) -> Result<()> {
        let mut storage = self.data.write();
        let position = storage.len();

        // Write length prefix + data
        let len = data.len() as u32;
        storage.extend_from_slice(&len.to_le_bytes());
        storage.extend_from_slice(data);

        // Update index
        self.index.write().insert(offset, position);

        // Update size
        self.size.fetch_add(4 + data.len(), Ordering::SeqCst);

        Ok(())
    }

    /// Read entries starting from offset
    pub fn read_from(&self, start_offset: Offset) -> Result<Vec<(Offset, Vec<u8>)>> {
        let mut results = Vec::new();
        let index = self.index.read();
        let data = self.data.read();

        for (&offset, &position) in index.range(start_offset..) {
            if position + 4 > data.len() {
                break;
            }

            // Read length prefix
            let len = u32::from_le_bytes([
                data[position],
                data[position + 1],
                data[position + 2],
                data[position + 3],
            ]) as usize;

            if position + 4 + len > data.len() {
                break;
            }

            // Read data
            let entry_data = data[position + 4..position + 4 + len].to_vec();
            results.push((offset, entry_data));
        }

        Ok(results)
    }

    /// Get segment size
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    /// Get base offset
    pub fn base_offset(&self) -> Offset {
        self.base_offset
    }

    /// Get highest offset in segment
    pub fn highest_offset(&self) -> Option<Offset> {
        self.index.read().keys().last().copied()
    }
}
