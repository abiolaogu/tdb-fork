//! Supabase Storage Service (S3-compatible)
//!
//! Provides object storage with:
//! - Bucket management
//! - File upload/download
//! - Access policies
//! - Image transformations

#![warn(clippy::all, clippy::pedantic)]
#![allow(clippy::module_name_repetitions)]

pub mod bucket;
pub mod object;
pub mod policy;
pub mod server;
pub mod transform;

pub use bucket::{Bucket, BucketManager};
pub use object::{ObjectMetadata, StorageObject};
pub use policy::StoragePolicy;
pub use server::StorageServer;
