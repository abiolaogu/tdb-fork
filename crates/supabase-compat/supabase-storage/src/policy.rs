//! Storage access policies

use serde::{Deserialize, Serialize};

/// Storage policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoragePolicy {
    /// Policy name
    pub name: String,
    /// Bucket this policy applies to  
    pub bucket_id: String,
    /// Operation this policy controls
    pub operation: StorageOperation,
    /// Role this policy applies to
    pub role: Option<String>,
    /// SQL expression for policy check
    pub expression: String,
}

/// Storage operations that can be controlled by policies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum StorageOperation {
    Select,
    Insert,
    Update,
    Delete,
}

impl StoragePolicy {
    /// Create a public SELECT policy
    pub fn public_read(name: &str, bucket_id: &str) -> Self {
        Self {
            name: name.to_string(),
            bucket_id: bucket_id.to_string(),
            operation: StorageOperation::Select,
            role: None,
            expression: "true".to_string(),
        }
    }

    /// Create an authenticated-only policy
    pub fn authenticated(name: &str, bucket_id: &str, operation: StorageOperation) -> Self {
        Self {
            name: name.to_string(),
            bucket_id: bucket_id.to_string(),
            operation,
            role: Some("authenticated".to_string()),
            expression: "auth.role() = 'authenticated'".to_string(),
        }
    }

    /// Create owner-only policy
    pub fn owner_only(name: &str, bucket_id: &str, operation: StorageOperation) -> Self {
        Self {
            name: name.to_string(),
            bucket_id: bucket_id.to_string(),
            operation,
            role: Some("authenticated".to_string()),
            expression: "auth.uid() = owner".to_string(),
        }
    }
}
