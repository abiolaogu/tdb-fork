//! MongoDB wire protocol implementation (stub)

use lumadb_common::error::Result;

/// MongoDB protocol server (stub)
pub struct MongoDBServer {
    port: u16,
}

impl MongoDBServer {
    pub fn new(port: u16) -> Self {
        Self { port }
    }

    pub async fn run(&self) -> Result<()> {
        // MongoDB protocol implementation would go here
        Ok(())
    }
}
