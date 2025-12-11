use std::sync::Arc;
use crate::config::Config;
use crate::error::Result;
use crate::hybrid::{HybridStorage, HybridConfig, StorageTier};

pub struct ShardManager {
    shards: Vec<Arc<Shard>>,
    num_shards: usize,
}

pub struct Shard {
    id: u32,
    storage: HybridStorage,
}

impl ShardManager {
    pub async fn new(config: &Config) -> Result<Self> {
        let num_shards = if config.sharding.num_shards == 0 {
            std::thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(4)
        } else {
            config.sharding.num_shards
        };

        // Construct HybridConfig from main Config
        let hybrid_config = HybridConfig {
            mode: StorageTier::Hybrid, // Default to Hybrid
            memory_budget: config.memory.memtable_size, // Use memtable size as budget per shard? Or total?
            // Usually explicit config handles this. Let's assume per-shard budget or global?
            // config.memory.memtable_size usually per shard in LSM, but here...?
            // Let's assume it's per-shard allocation for now.
            index_memory_budget: config.memory.memtable_size / 4,
            ssd_path: Some(config.data_dir.join("ssd")),
            hdd_path: Some(config.data_dir.join("hdd")),
            hot_threshold: 10,
            access_window_secs: 3600,
            auto_migrate: true,
            migration_batch_size: 1000,
            prefetch_enabled: true,
            prefetch_lookahead: 8,
            direct_io: true,
            huge_pages: true,
            numa_node: -1,
            tiering: config.tiering.clone(), // Pass the tiering config!
        };

        // Create shards
        let mut shards = Vec::with_capacity(num_shards);
        for i in 0..num_shards {
            // Each shard gets a clone of config, but maybe we need unique paths per shard?
            // "Shard-Per-Core" usually means they share the same directory but key range separation, 
            // or have distinct files.
            // HybridStorage `new` might assume exclusive access to files?
            // If they share `ssd_path`, they might overwrite.
            // A simple way is to append shard ID to path.
            let mut shard_config = hybrid_config.clone();
            if let Some(p) = shard_config.ssd_path.as_mut() {
                *p = p.join(format!("shard_{}", i));
            }
            if let Some(p) = shard_config.hdd_path.as_mut() {
                *p = p.join(format!("shard_{}", i));
            }

            shards.push(Arc::new(Shard::new(i as u32, shard_config).await?));
        }

        Ok(Self { shards, num_shards })
    }

    pub fn get_shard(&self, key: &[u8]) -> &Arc<Shard> {
        let hash = xxhash_rust::xxh3::xxh3_64(key) as usize;
        &self.shards[hash % self.num_shards]
    }

    pub fn all_shards(&self) -> impl Iterator<Item = &Arc<Shard>> {
        self.shards.iter()
    }

    pub async fn delete_prefix(&self, prefix: &[u8]) -> Result<()> {
        for shard in &self.shards {
            shard.delete_prefix(prefix).await?;
        }
        Ok(())
    }

    pub fn search_vector(&self, query: &[f32], k: usize) -> Vec<(Vec<u8>, f32)> {
        let mut all_results = Vec::new();
        // Scatter
        for shard in &self.shards {
             let shard_results = shard.search_vector(query, k);
             all_results.extend(shard_results);
        }
        // Sort & Gather
        all_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        if all_results.len() > k {
            all_results.truncate(k);
        }
        all_results
    }
}

impl Shard {
    pub async fn new(id: u32, config: HybridConfig) -> Result<Self> {
        Ok(Self {
            id,
            storage: HybridStorage::new(config).await?,
        })
    }

    pub async fn put(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.storage.put(&key, &value).await.map_err(|e| e.into())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.storage.get(key).await.map_err(|e| e.into())
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.storage.delete(key).await?;
        Ok(())
    }

    pub async fn delete_prefix(&self, prefix: &[u8]) -> Result<()> {
        self.storage.delete_prefix(prefix).await.map_err(|e| e.into())
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.storage.scan(prefix).await.map_err(|e| e.into())
    }

    pub async fn flush(&self) -> Result<()> {
        // HybridStorage likely flushes implicitly or via method?
        // It doesn't have explicit flush in previous view.
        Ok(())
    }

    pub async fn compact(&self) -> Result<()> {
        Ok(())
    }

    pub fn index_vector(&self, id: String, vector: Vec<f32>) -> Result<()> {
        self.storage.index_vector(id, vector).map_err(|e| e.into())
    }

    pub fn search_vector(&self, query: &[f32], k: usize) -> Vec<(Vec<u8>, f32)> {
        self.storage.search_vector(query, k)
    }
}

