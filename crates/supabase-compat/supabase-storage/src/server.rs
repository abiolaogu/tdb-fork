//! Storage server for HTTP API

use std::sync::Arc;

use supabase_common::config::StorageConfig;
use supabase_common::error::Result;

use crate::bucket::BucketManager;
use crate::object::ObjectStore;

/// Storage server managing buckets and objects
pub struct StorageServer {
    config: StorageConfig,
    bucket_manager: Arc<BucketManager>,
    object_store: Arc<ObjectStore>,
}

impl StorageServer {
    /// Create a new storage server
    pub fn new(config: &StorageConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            bucket_manager: Arc::new(BucketManager::new()),
            object_store: Arc::new(ObjectStore::new()),
        })
    }

    /// Get the bucket manager
    pub fn buckets(&self) -> Arc<BucketManager> {
        self.bucket_manager.clone()
    }

    /// Get the object store
    pub fn objects(&self) -> Arc<ObjectStore> {
        self.object_store.clone()
    }

    /// Generate a public URL for an object
    pub fn get_public_url(&self, bucket: &str, path: &str) -> String {
        format!(
            "{}/storage/v1/object/public/{}/{}",
            self.config
                .public_url
                .as_deref()
                .unwrap_or("http://localhost:3000"),
            bucket,
            path
        )
    }

    /// Generate an authenticated URL for an object
    pub fn get_authenticated_url(&self, bucket: &str, path: &str) -> String {
        format!(
            "{}/storage/v1/object/authenticated/{}/{}",
            self.config
                .public_url
                .as_deref()
                .unwrap_or("http://localhost:3000"),
            bucket,
            path
        )
    }

    /// Generate a signed URL with expiry
    pub fn create_signed_url(&self, bucket: &str, path: &str, expires_in: u64) -> String {
        let expiry = chrono::Utc::now().timestamp() as u64 + expires_in;
        // In production, this would include a cryptographic signature
        format!(
            "{}/storage/v1/object/sign/{}/{}?token=placeholder&expires={}",
            self.config
                .public_url
                .as_deref()
                .unwrap_or("http://localhost:3000"),
            bucket,
            path,
            expiry
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> StorageConfig {
        StorageConfig::default()
    }

    #[test]
    fn test_storage_server() {
        let server = StorageServer::new(&test_config()).unwrap();
        assert!(server.buckets().list().is_empty());
    }

    #[test]
    fn test_url_generation() {
        let server = StorageServer::new(&test_config()).unwrap();
        let url = server.get_public_url("images", "photo.jpg");
        assert!(url.contains("/storage/v1/object/public/images/photo.jpg"));
    }
}
