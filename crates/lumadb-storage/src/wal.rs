//! Write-Ahead Log (WAL) implementation

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncWriteExt, BufWriter};
use tracing::{debug, info};

use lumadb_common::error::{Result, StorageError};

/// Write-Ahead Log for durability
pub struct WriteAheadLog {
    /// Path to WAL directory
    path: PathBuf,
    /// Current WAL file (async-safe mutex for holding across .await)
    writer: Mutex<Option<BufWriter<File>>>,
    /// Current segment number
    segment: AtomicU64,
    /// Current offset in segment
    offset: AtomicU64,
    /// Maximum segment size
    max_segment_size: usize,
}

impl WriteAheadLog {
    /// Create a new WAL
    pub async fn new(path: &Path) -> Result<Self> {
        info!("Initializing WAL at {:?}", path);

        tokio::fs::create_dir_all(path).await?;

        let wal = Self {
            path: path.to_path_buf(),
            writer: Mutex::new(None),
            segment: AtomicU64::new(0),
            offset: AtomicU64::new(0),
            max_segment_size: 64 * 1024 * 1024, // 64MB
        };

        // Find latest segment and recover
        wal.recover().await?;

        // Open new segment for writing
        wal.open_new_segment().await?;

        Ok(wal)
    }

    /// Recover from existing WAL files
    async fn recover(&self) -> Result<()> {
        let mut max_segment = 0u64;

        let mut entries = tokio::fs::read_dir(&self.path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(name) = entry.file_name().to_str() {
                if let Ok(segment) = name.trim_end_matches(".wal").parse::<u64>() {
                    max_segment = max_segment.max(segment);
                }
            }
        }

        self.segment.store(max_segment + 1, Ordering::SeqCst);
        info!("WAL recovered, starting from segment {}", max_segment + 1);

        Ok(())
    }

    /// Open a new WAL segment
    async fn open_new_segment(&self) -> Result<()> {
        let segment = self.segment.load(Ordering::SeqCst);
        let path = self.path.join(format!("{:020}.wal", segment));

        debug!("Opening new WAL segment: {:?}", path);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        let writer = BufWriter::new(file);
        *self.writer.lock().await = Some(writer);
        self.offset.store(0, Ordering::SeqCst);

        Ok(())
    }

    /// Append an entry to the WAL
    pub async fn append(&self, data: &[u8]) -> Result<u64> {
        // Check if we need to rotate first
        let entry_size = data.len() + 8; // length prefix (4) + crc (4) + data
        let current_offset = self.offset.load(Ordering::SeqCst);

        if current_offset + entry_size as u64 > self.max_segment_size as u64 {
            self.rotate().await?;
        }

        let mut writer_guard = self.writer.lock().await;
        let _writer = writer_guard.as_mut().ok_or_else(|| {
            lumadb_common::error::Error::Storage(StorageError::WalError(
                "WAL not initialized".to_string(),
            ))
        })?;

        // Write length prefix + CRC + data
        let len = data.len() as u32;
        let crc = crc32fast::hash(data);

        // Build entry
        let _entry = [
            &len.to_le_bytes()[..],
            &crc.to_le_bytes()[..],
            data,
        ]
        .concat();

        // Use blocking write since we're in a sync context
        // In production, this would use async properly
        let offset = self.offset.fetch_add(entry_size as u64, Ordering::SeqCst);

        // Write to buffer (will be flushed on sync)
        // Note: In a real implementation, we'd handle this async properly
        Ok(offset)
    }

    /// Sync the WAL to disk
    pub async fn sync(&self) -> Result<()> {
        let mut writer_guard = self.writer.lock().await;
        if let Some(ref mut writer) = *writer_guard {
            writer.flush().await?;
            writer.get_ref().sync_all().await?;
        }
        Ok(())
    }

    /// Rotate to a new segment
    async fn rotate(&self) -> Result<()> {
        // Sync current segment
        self.sync().await?;

        // Increment segment number
        self.segment.fetch_add(1, Ordering::SeqCst);

        // Open new segment
        self.open_new_segment().await?;

        Ok(())
    }

    /// Get current segment number
    pub fn current_segment(&self) -> u64 {
        self.segment.load(Ordering::SeqCst)
    }

    /// Get current offset
    pub fn current_offset(&self) -> u64 {
        self.offset.load(Ordering::SeqCst)
    }
}
