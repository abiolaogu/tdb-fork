//! Storage object management

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Metadata for a stored object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Content type (MIME)
    pub content_type: String,
    /// Content length in bytes
    pub content_length: usize,
    /// ETag (content hash)
    pub etag: Option<String>,
    /// Last modified time
    pub last_modified: DateTime<Utc>,
    /// Custom metadata
    pub metadata: HashMap<String, String>,
    /// Cache control header
    pub cache_control: Option<String>,
}

impl ObjectMetadata {
    pub fn new(content_type: &str, content_length: usize) -> Self {
        Self {
            content_type: content_type.to_string(),
            content_length,
            etag: None,
            last_modified: Utc::now(),
            metadata: HashMap::new(),
            cache_control: None,
        }
    }
}

/// A stored object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageObject {
    /// Unique object ID
    pub id: String,
    /// Bucket this object belongs to
    pub bucket_id: String,
    /// Object path/name
    pub name: String,
    /// Owner user ID
    pub owner: Option<String>,
    /// Object metadata
    pub metadata: ObjectMetadata,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
}

impl StorageObject {
    pub fn new(bucket_id: &str, name: &str, metadata: ObjectMetadata) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            bucket_id: bucket_id.to_string(),
            name: name.to_string(),
            owner: None,
            metadata,
            created_at: now,
            updated_at: now,
        }
    }
}

/// Object listing options
#[derive(Debug, Clone, Default)]
pub struct ListOptions {
    pub prefix: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub sort_by: Option<SortColumn>,
    pub order: Option<SortOrder>,
}

#[derive(Debug, Clone, Copy)]
pub enum SortColumn {
    Name,
    CreatedAt,
    UpdatedAt,
    Size,
}

#[derive(Debug, Clone, Copy)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Manages storage objects (in-memory implementation)
pub struct ObjectStore {
    /// Objects by bucket_id -> path -> object
    objects: Arc<RwLock<HashMap<String, HashMap<String, StorageObject>>>>,
    /// Object data by object_id -> bytes
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl ObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Upload an object
    pub fn upload(
        &self,
        bucket_id: &str,
        path: &str,
        data: Vec<u8>,
        content_type: &str,
    ) -> StorageObject {
        let metadata = ObjectMetadata::new(content_type, data.len());
        let object = StorageObject::new(bucket_id, path, metadata);

        {
            let mut objects = self.objects.write();
            let bucket_objects = objects
                .entry(bucket_id.to_string())
                .or_insert_with(HashMap::new);
            bucket_objects.insert(path.to_string(), object.clone());
        }

        {
            self.data.write().insert(object.id.clone(), data);
        }

        object
    }

    /// Download an object
    pub fn download(&self, bucket_id: &str, path: &str) -> Option<(StorageObject, Vec<u8>)> {
        let object = {
            let objects = self.objects.read();
            objects.get(bucket_id)?.get(path).cloned()
        }?;

        let data = self.data.read().get(&object.id).cloned()?;
        Some((object, data))
    }

    /// Get object metadata without data
    pub fn get_metadata(&self, bucket_id: &str, path: &str) -> Option<StorageObject> {
        self.objects.read().get(bucket_id)?.get(path).cloned()
    }

    /// List objects in a bucket
    pub fn list(&self, bucket_id: &str, options: ListOptions) -> Vec<StorageObject> {
        let objects = self.objects.read();
        let bucket_objects = match objects.get(bucket_id) {
            Some(objs) => objs,
            None => return vec![],
        };

        let mut result: Vec<StorageObject> = bucket_objects
            .values()
            .filter(|obj| {
                options
                    .prefix
                    .as_ref()
                    .map(|p| obj.name.starts_with(p))
                    .unwrap_or(true)
            })
            .cloned()
            .collect();

        // Sort
        if let Some(column) = options.sort_by {
            let desc = matches!(options.order, Some(SortOrder::Desc));
            result.sort_by(|a, b| {
                let cmp = match column {
                    SortColumn::Name => a.name.cmp(&b.name),
                    SortColumn::CreatedAt => a.created_at.cmp(&b.created_at),
                    SortColumn::UpdatedAt => a.updated_at.cmp(&b.updated_at),
                    SortColumn::Size => a.metadata.content_length.cmp(&b.metadata.content_length),
                };
                if desc {
                    cmp.reverse()
                } else {
                    cmp
                }
            });
        }

        // Pagination
        let offset = options.offset.unwrap_or(0);
        let limit = options.limit.unwrap_or(100);
        result.into_iter().skip(offset).take(limit).collect()
    }

    /// Delete an object
    pub fn delete(&self, bucket_id: &str, path: &str) -> Option<StorageObject> {
        let object = {
            let mut objects = self.objects.write();
            objects.get_mut(bucket_id)?.remove(path)
        }?;

        self.data.write().remove(&object.id);
        Some(object)
    }

    /// Delete multiple objects
    pub fn delete_many(&self, bucket_id: &str, paths: &[String]) -> Vec<StorageObject> {
        paths
            .iter()
            .filter_map(|p| self.delete(bucket_id, p))
            .collect()
    }

    /// Move/rename an object
    pub fn move_object(
        &self,
        from_bucket: &str,
        from_path: &str,
        to_bucket: &str,
        to_path: &str,
    ) -> Option<StorageObject> {
        let (object, data) = self.download(from_bucket, from_path)?;
        self.delete(from_bucket, from_path);

        let new_object = self.upload(to_bucket, to_path, data, &object.metadata.content_type);
        Some(new_object)
    }

    /// Copy an object
    pub fn copy_object(
        &self,
        from_bucket: &str,
        from_path: &str,
        to_bucket: &str,
        to_path: &str,
    ) -> Option<StorageObject> {
        let (object, data) = self.download(from_bucket, from_path)?;
        let new_object = self.upload(to_bucket, to_path, data, &object.metadata.content_type);
        Some(new_object)
    }
}

impl Default for ObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upload_download() {
        let store = ObjectStore::new();

        let data = b"Hello, World!".to_vec();
        let obj = store.upload("bucket1", "test.txt", data.clone(), "text/plain");

        let (retrieved, retrieved_data) = store.download("bucket1", "test.txt").unwrap();
        assert_eq!(retrieved.name, "test.txt");
        assert_eq!(retrieved_data, data);
    }

    #[test]
    fn test_list_with_prefix() {
        let store = ObjectStore::new();

        store.upload("bucket1", "images/a.jpg", vec![], "image/jpeg");
        store.upload("bucket1", "images/b.jpg", vec![], "image/jpeg");
        store.upload("bucket1", "docs/c.pdf", vec![], "application/pdf");

        let images = store.list(
            "bucket1",
            ListOptions {
                prefix: Some("images/".to_string()),
                ..Default::default()
            },
        );

        assert_eq!(images.len(), 2);
    }
}
