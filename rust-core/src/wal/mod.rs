//! Write-Ahead Log for durability

pub mod optimized_wal;
pub use optimized_wal::{OptimizedWAL, WalConfig, CompressionStrategy};

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Mutex;
use crate::config::Config;
use crate::types::KeyValue;
use crate::error::Result;

pub struct WriteAheadLog {
    path: PathBuf,
    writer: Mutex<Option<BufWriter<File>>>,
    sequence: AtomicU64,
    batch_buffer: Mutex<Vec<KeyValue>>,
    max_batch_size: usize,
}

impl WriteAheadLog {
    pub fn new(config: &Config) -> Result<Self> {
        let wal_dir = config.wal.dir.clone()
            .unwrap_or_else(|| config.data_dir.join("wal"));
        std::fs::create_dir_all(&wal_dir)?;

        let path = wal_dir.join("current.wal");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: Mutex::new(Some(BufWriter::new(file))),
            sequence: AtomicU64::new(0),
            batch_buffer: Mutex::new(Vec::new()),
            max_batch_size: config.wal.batch_size,
        })
    }

    pub async fn append(&self, kv: &KeyValue) -> Result<()> {
        let mut buffer = self.batch_buffer.lock().await;
        buffer.push(kv.clone());

        if buffer.len() >= self.max_batch_size {
            self.flush_batch(&mut buffer).await?;
        }
        Ok(())
    }

    async fn flush_batch(&self, buffer: &mut Vec<KeyValue>) -> Result<()> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_mut() {
            for kv in buffer.drain(..) {
                let data = bincode::serialize(&kv)?;
                let len = data.len() as u32;
                writer.write_all(&len.to_le_bytes())?;
                writer.write_all(&data)?;
            }
            writer.flush()?;
        }
        Ok(())
    }

    pub async fn sync(&self) -> Result<()> {
        let mut buffer = self.batch_buffer.lock().await;
        self.flush_batch(&mut buffer).await?;

        let writer_guard = self.writer.lock().await;
        if let Some(writer) = writer_guard.as_ref() {
            writer.get_ref().sync_all()?;
        }
        Ok(())
    }

    pub async fn recover(&self) -> Result<Vec<KeyValue>> {
        let mut entries = Vec::new();

        if !self.path.exists() {
            return Ok(entries);
        }

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);

        loop {
            let mut len_buf = [0u8; 4];
            match reader.read_exact(&mut len_buf) {
                Ok(_) => {
                    let len = u32::from_le_bytes(len_buf) as usize;
                    let mut data = vec![0u8; len];
                    reader.read_exact(&mut data)?;
                    let kv: KeyValue = bincode::deserialize(&data)?;
                    entries.push(kv);
                }
                Err(_) => break,
            }
        }

        Ok(entries)
    }
}
