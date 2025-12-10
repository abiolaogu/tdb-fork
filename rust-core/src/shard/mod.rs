//! Shard Manager - ScyllaDB-inspired shard-per-core architecture

use std::sync::Arc;
use dashmap::DashMap;
use crate::config::Config;
use crate::memory::MemTable;
use crate::error::Result;

pub struct ShardManager {
    shards: Vec<Arc<Shard>>,
    num_shards: usize,
}

pub struct Shard {
    id: u32,
    memtable: MemTable,
    data: DashMap<Vec<u8>, Vec<u8>>,
}

impl ShardManager {
    pub fn new(config: &Config) -> Result<Self> {
        let num_shards = if config.sharding.num_shards == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        } else {
            config.sharding.num_shards
        };

        let shards = (0..num_shards)
            .map(|i| Arc::new(Shard::new(i as u32, config.memory.memtable_size)))
            .collect();

        Ok(Self { shards, num_shards })
    }

    pub fn get_shard(&self, key: &[u8]) -> &Arc<Shard> {
        let hash = xxhash_rust::xxh3::xxh3_64(key) as usize;
        &self.shards[hash % self.num_shards]
    }

    pub fn all_shards(&self) -> impl Iterator<Item = &Arc<Shard>> {
        self.shards.iter()
    }
}

impl Shard {
    pub fn new(id: u32, memtable_size: usize) -> Self {
        Self {
            id,
            memtable: MemTable::new(memtable_size),
            data: DashMap::new(),
        }
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.data.insert(key, value);
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.data.get(key).map(|v| v.clone()))
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.data.remove(key);
        Ok(())
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.data
            .iter()
            .filter(|entry| entry.key().starts_with(prefix))
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect())
    }

    pub async fn flush(&self) -> Result<()> {
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        Ok(())
    }
}
