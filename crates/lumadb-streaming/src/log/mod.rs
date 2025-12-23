//! Append-only log storage for streaming

use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use crossbeam::queue::SegQueue;
use parking_lot::RwLock;

use lumadb_common::error::{Result, Error};
use lumadb_common::types::{Offset, PartitionId, Record};

mod segment;

pub use segment::Segment;

/// Partition log - append-only storage for a single partition
pub struct PartitionLog {
    /// Partition ID
    partition_id: PartitionId,
    /// Active segment
    active_segment: RwLock<Segment>,
    /// Immutable segments
    segments: RwLock<Vec<Segment>>,
    /// High watermark (latest committed offset)
    high_watermark: AtomicI64,
    /// Low watermark (earliest available offset)
    low_watermark: AtomicI64,
    /// Maximum segment size
    max_segment_size: usize,
    /// Next offset to assign
    next_offset: AtomicI64,
}

impl PartitionLog {
    /// Create a new partition log
    pub fn new(partition_id: PartitionId, max_segment_size: usize) -> Self {
        Self {
            partition_id,
            active_segment: RwLock::new(Segment::new(0)),
            segments: RwLock::new(Vec::new()),
            high_watermark: AtomicI64::new(-1),
            low_watermark: AtomicI64::new(0),
            max_segment_size,
            next_offset: AtomicI64::new(0),
        }
    }

    /// Append a record to the log
    pub fn append(&self, record: &Record) -> Result<Offset> {
        let offset = self.next_offset.fetch_add(1, Ordering::SeqCst);

        // Create record with offset
        let mut record = record.clone();
        record.offset = Some(offset);

        // Serialize record
        let data = bincode::serialize(&record)
            .map_err(|e| Error::Internal(format!("Serialization failed: {}", e)))?;

        // Check if we need to roll the segment
        {
            let segment = self.active_segment.read();
            if segment.size() + data.len() > self.max_segment_size {
                drop(segment);
                self.roll_segment()?;
            }
        }

        // Append to active segment
        self.active_segment.write().append(offset, &data)?;

        // Update high watermark
        self.high_watermark.store(offset, Ordering::SeqCst);

        Ok(offset)
    }

    /// Fetch records starting from an offset
    pub fn fetch(&self, start_offset: Offset, max_records: usize) -> Result<Vec<(Offset, Record)>> {
        let mut results = Vec::new();
        let start = if start_offset < 0 {
            // -1 means latest
            self.high_watermark.load(Ordering::SeqCst)
        } else {
            start_offset
        };

        // Check segments
        for segment in self.segments.read().iter() {
            if results.len() >= max_records {
                break;
            }

            for (offset, data) in segment.read_from(start)? {
                if results.len() >= max_records {
                    break;
                }

                let record: Record = bincode::deserialize(&data)
                    .map_err(|e| Error::Internal(format!("Deserialization failed: {}", e)))?;
                results.push((offset, record));
            }
        }

        // Check active segment
        if results.len() < max_records {
            for (offset, data) in self.active_segment.read().read_from(start)? {
                if results.len() >= max_records {
                    break;
                }

                let record: Record = bincode::deserialize(&data)
                    .map_err(|e| Error::Internal(format!("Deserialization failed: {}", e)))?;
                results.push((offset, record));
            }
        }

        Ok(results)
    }

    /// Roll to a new segment
    fn roll_segment(&self) -> Result<()> {
        let mut active = self.active_segment.write();
        let old_segment = std::mem::replace(
            &mut *active,
            Segment::new(self.next_offset.load(Ordering::SeqCst)),
        );

        // Make old segment immutable and add to list
        self.segments.write().push(old_segment);

        Ok(())
    }

    /// Get high watermark
    pub fn high_watermark(&self) -> Offset {
        self.high_watermark.load(Ordering::SeqCst)
    }

    /// Get low watermark
    pub fn low_watermark(&self) -> Offset {
        self.low_watermark.load(Ordering::SeqCst)
    }

    /// Get log size in bytes
    pub fn size_bytes(&self) -> usize {
        let mut size = self.active_segment.read().size();
        for segment in self.segments.read().iter() {
            size += segment.size();
        }
        size
    }
}

/// Log entry with offset
#[derive(Debug, Clone)]
pub struct LogEntry {
    pub offset: Offset,
    pub data: Bytes,
    pub timestamp: i64,
}
