//! Data Migration Between Tiers
//!
//! Automatically moves data between tiers based on access patterns:
//! - Hot data -> RAM (frequently accessed)
//! - Warm data -> SSD
//! - Cold data -> HDD

use std::sync::atomic::Ordering;
use std::sync::Arc;

use dashmap::DashMap;

use crate::error::Result as LumaResult;
use super::{
    HybridConfig, HybridStats, IndexEntry, RecordLocation, StorageTier,
    tier::{RamStore, SsdStore, HddStore},
};

/// Data migration manager
pub struct Migrator {
    /// Access count threshold for "hot" data
    hot_threshold: u32,

    /// Time window for access tracking
    window_secs: u64,

    /// Batch size for migration
    batch_size: usize,

    /// Statistics
    stats: Arc<HybridStats>,
}

impl Migrator {
    pub fn new(
        hot_threshold: u32,
        window_secs: u64,
        batch_size: usize,
        stats: Arc<HybridStats>,
    ) -> Self {
        Self {
            hot_threshold,
            window_secs,
            batch_size,
            stats,
        }
    }

    /// Migrate cold data from RAM to SSD
    pub async fn migrate_cold_data(
        &self,
        index: &DashMap<Vec<u8>, IndexEntry>,
        ram_store: &RamStore,
        ssd_store: Option<&SsdStore>,
        hdd_store: Option<&HddStore>,
        config: &HybridConfig,
    ) -> LumaResult<usize> {
        let ssd = match ssd_store {
            Some(s) => s,
            None => return Ok(0),
        };

        let mut migrated = 0;
        let mut candidates: Vec<(Vec<u8>, IndexEntry)> = Vec::new();

        // Find cold data candidates
        for entry in index.iter() {
            let loc = &entry.value().location;

            // Only consider RAM data
            if loc.tier != StorageTier::Memory && loc.tier != StorageTier::Hybrid {
                continue;
            }

            // Check if cold
            if !loc.is_hot(self.hot_threshold, self.window_secs) {
                candidates.push((entry.key().clone(), IndexEntry {
                    key_hash: entry.value().key_hash,
                    location: RecordLocation::new(
                        loc.tier,
                        loc.offset,
                        loc.size,
                    ),
                    generation: entry.value().generation,
                    deleted: entry.value().deleted,
                }));

                if candidates.len() >= self.batch_size {
                    break;
                }
            }
        }

        // Migrate candidates
        for (key, entry) in candidates {
            // Read from RAM
            if let Some(data) = ram_store.read(entry.location.offset, entry.location.size) {
                // Write to SSD
                let new_offset = ssd.write(&data).await?;

                // Update index
                if let Some(mut idx_entry) = index.get_mut(&key) {
                    idx_entry.location = RecordLocation::new(
                        StorageTier::SSD,
                        new_offset,
                        entry.location.size,
                    );
                    idx_entry.generation += 1;
                }

                migrated += 1;
                self.stats.migrations_to_ssd.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(migrated)
    }

    /// Promote hot data from SSD to RAM
    pub async fn promote_hot_data(
        &self,
        index: &DashMap<Vec<u8>, IndexEntry>,
        ram_store: &RamStore,
        ssd_store: Option<&SsdStore>,
    ) -> LumaResult<usize> {
        let ssd = match ssd_store {
            Some(s) => s,
            None => return Ok(0),
        };

        let mut promoted = 0;
        let mut candidates: Vec<(Vec<u8>, IndexEntry)> = Vec::new();

        // Find hot data on SSD
        for entry in index.iter() {
            let loc = &entry.value().location;

            if loc.tier != StorageTier::SSD {
                continue;
            }

            // Check if hot and RAM has space
            if loc.is_hot(self.hot_threshold, self.window_secs)
                && ram_store.has_space(loc.size as usize)
            {
                candidates.push((entry.key().clone(), IndexEntry {
                    key_hash: entry.value().key_hash,
                    location: RecordLocation::new(
                        loc.tier,
                        loc.offset,
                        loc.size,
                    ),
                    generation: entry.value().generation,
                    deleted: entry.value().deleted,
                }));

                if candidates.len() >= self.batch_size {
                    break;
                }
            }
        }

        // Promote candidates
        for (key, entry) in candidates {
            // Read from SSD
            let data = ssd.read(entry.location.offset, entry.location.size).await?;

            // Write to RAM
            let new_offset = ram_store.write(&data)?;

            // Update index
            if let Some(mut idx_entry) = index.get_mut(&key) {
                idx_entry.location = RecordLocation::new(
                    StorageTier::Memory,
                    new_offset,
                    entry.location.size,
                );
                idx_entry.generation += 1;
            }

            promoted += 1;
            self.stats.migrations_to_ram.fetch_add(1, Ordering::Relaxed);
        }

        Ok(promoted)
    }
}
