//! Configuration loading

use std::path::Path;
use anyhow::Result;
use lumadb_common::config::Config;

/// Load configuration from file
pub async fn load(path: &str) -> Result<Config> {
    let path = Path::new(path);

    if path.exists() {
        Config::load(path).await.map_err(|e| anyhow::anyhow!(e))
    } else {
        // Use default configuration
        Ok(Config::default())
    }
}
