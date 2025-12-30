//! Bucket management for storage service

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Storage bucket configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    /// Unique bucket ID
    pub id: String,
    /// Bucket name (unique identifier)
    pub name: String,
    /// Owner user ID
    pub owner: Option<String>,
    /// Whether bucket is public
    pub public: bool,
    /// Optional file size limit in bytes
    pub file_size_limit: Option<usize>,
    /// Allowed MIME types (None = all allowed)
    pub allowed_mime_types: Option<Vec<String>>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
}

impl Bucket {
    /// Create a new bucket
    pub fn new(name: &str) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            owner: None,
            public: false,
            file_size_limit: None,
            allowed_mime_types: None,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a public bucket
    pub fn public(name: &str) -> Self {
        let mut bucket = Self::new(name);
        bucket.public = true;
        bucket
    }

    /// Set file size limit
    pub fn with_size_limit(mut self, limit: usize) -> Self {
        self.file_size_limit = Some(limit);
        self
    }

    /// Set allowed MIME types
    pub fn with_mime_types(mut self, types: Vec<String>) -> Self {
        self.allowed_mime_types = Some(types);
        self
    }

    /// Check if a file size is allowed
    pub fn is_size_allowed(&self, size: usize) -> bool {
        self.file_size_limit
            .map(|limit| size <= limit)
            .unwrap_or(true)
    }

    /// Check if a MIME type is allowed
    pub fn is_mime_type_allowed(&self, mime_type: &str) -> bool {
        self.allowed_mime_types
            .as_ref()
            .map(|types| {
                types
                    .iter()
                    .any(|t| t == "*" || t == mime_type || mime_type.starts_with(t))
            })
            .unwrap_or(true)
    }
}

/// Bucket creation options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CreateBucketOptions {
    pub public: Option<bool>,
    pub file_size_limit: Option<usize>,
    pub allowed_mime_types: Option<Vec<String>>,
}

/// Manages storage buckets
pub struct BucketManager {
    buckets: Arc<RwLock<HashMap<String, Bucket>>>,
}

impl BucketManager {
    /// Create a new bucket manager
    pub fn new() -> Self {
        Self {
            buckets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a new bucket
    pub fn create(&self, name: &str, options: CreateBucketOptions) -> Result<Bucket, String> {
        let mut buckets = self.buckets.write();

        if buckets.contains_key(name) {
            return Err(format!("Bucket '{}' already exists", name));
        }

        let mut bucket = Bucket::new(name);
        if let Some(public) = options.public {
            bucket.public = public;
        }
        bucket.file_size_limit = options.file_size_limit;
        bucket.allowed_mime_types = options.allowed_mime_types;

        buckets.insert(name.to_string(), bucket.clone());
        Ok(bucket)
    }

    /// Get a bucket by name
    pub fn get(&self, name: &str) -> Option<Bucket> {
        self.buckets.read().get(name).cloned()
    }

    /// List all buckets
    pub fn list(&self) -> Vec<Bucket> {
        self.buckets.read().values().cloned().collect()
    }

    /// Delete a bucket
    pub fn delete(&self, name: &str) -> Result<(), String> {
        let mut buckets = self.buckets.write();
        if buckets.remove(name).is_none() {
            return Err(format!("Bucket '{}' not found", name));
        }
        Ok(())
    }

    /// Update bucket settings
    pub fn update(&self, name: &str, options: CreateBucketOptions) -> Result<Bucket, String> {
        let mut buckets = self.buckets.write();
        let bucket = buckets
            .get_mut(name)
            .ok_or_else(|| format!("Bucket '{}' not found", name))?;

        if let Some(public) = options.public {
            bucket.public = public;
        }
        if options.file_size_limit.is_some() {
            bucket.file_size_limit = options.file_size_limit;
        }
        if options.allowed_mime_types.is_some() {
            bucket.allowed_mime_types = options.allowed_mime_types;
        }
        bucket.updated_at = Utc::now();

        Ok(bucket.clone())
    }

    /// Empty a bucket (delete all objects)
    pub fn empty(&self, name: &str) -> Result<(), String> {
        let buckets = self.buckets.read();
        if !buckets.contains_key(name) {
            return Err(format!("Bucket '{}' not found", name));
        }
        // Object deletion would happen in the ObjectStore
        Ok(())
    }
}

impl Default for BucketManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_creation() {
        let bucket = Bucket::new("my-bucket");
        assert_eq!(bucket.name, "my-bucket");
        assert!(!bucket.public);
    }

    #[test]
    fn test_bucket_manager() {
        let manager = BucketManager::new();
        let bucket = manager
            .create("test", CreateBucketOptions::default())
            .unwrap();
        assert_eq!(bucket.name, "test");

        let retrieved = manager.get("test");
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_mime_type_check() {
        let bucket = Bucket::new("images").with_mime_types(vec!["image/".to_string()]);

        assert!(bucket.is_mime_type_allowed("image/png"));
        assert!(bucket.is_mime_type_allowed("image/jpeg"));
        assert!(!bucket.is_mime_type_allowed("application/pdf"));
    }
}
