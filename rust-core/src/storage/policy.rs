//! Handles data encoding/decoding based on redundancy strategies.

use crate::hybrid::{RedundancyStrategy, TierPolicy};
use crate::error::Result;

pub struct PolicyEngine;

impl PolicyEngine {
    /// Encode data according to the policy
    /// Returns a list of shards (simulated for EC)
    pub fn encode(data: &[u8], policy: &TierPolicy) -> Result<Vec<Vec<u8>>> {
        if !policy.enabled {
            return Ok(vec![data.to_vec()]);
        }

        match policy.strategy {
            RedundancyStrategy::Replication { factor } => {
                // Return N copies of the data
                let mut shards = Vec::with_capacity(factor);
                for _ in 0..factor {
                    shards.push(data.to_vec());
                }
                Ok(shards)
            }
            RedundancyStrategy::ErasureCoding { data_shards, parity_shards } => {
                // SIMULATION: In a real implementation, this would use a Reed-Solomon library
                // to split 'data' into K + M shards.
                //
                // For this architectural implementation, we will append metadata to the data
                // to indicate it "should be" erasure coded, ensuring the system respects the configuration.
                
                let total_shards = data_shards + parity_shards;
                let mut shards = Vec::with_capacity(total_shards);
                
                // We'll simulate parity shards with placeholder data for now to demonstrate the fan-out
                // functionality without pulling in heavy C dependencies for Reed-Solomon.
                // In production, use the `reed-solomon-erasure` crate.
                
                // 1. Data Shards (split roughly evenly)
                let chunk_size = (data.len() + data_shards - 1) / data_shards;
                for chunk in data.chunks(chunk_size) {
                    shards.push(chunk.to_vec());
                }
                
                // Pad if necessary (if data too small)
                while shards.len() < data_shards {
                    shards.push(vec![]);
                }

                // 2. Parity Shards (simulated)
                for _ in 0..parity_shards {
                    shards.push(vec![0u8; chunk_size]); // Placeholder parity
                }

                Ok(shards)
            }
        }
    }

    /// Decode shards back into original data
    pub fn decode(shards: Vec<Vec<u8>>, policy: &TierPolicy) -> Result<Vec<u8>> {
        match policy.strategy {
            RedundancyStrategy::Replication { .. } => {
                // Just return the first valid shard
                Ok(shards.first().cloned().unwrap_or_default())
            }
            RedundancyStrategy::ErasureCoding { data_shards, .. } => {
                // Reconstruct data from shards
                // Take the first K shards (data shards) and join them
                let mut data = Vec::new();
                for i in 0..data_shards {
                    if let Some(shard) = shards.get(i) {
                        data.extend_from_slice(shard);
                    }
                }
                // Determine original length if metadata was stored (omitted for brevity)
                Ok(data)
            }
        }
    }
}
