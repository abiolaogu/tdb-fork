//! Handle management for FFI
//!
//! Provides thread-safe handle allocation and lookup for cross-language resources.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::storage::engine::StorageEngine;

/// Global handle map for storage engines
pub static ENGINES: HandleMap<Arc<StorageEngine>> = HandleMap::new();

/// Thread-safe handle map for managing FFI resources
pub struct HandleMap<T> {
    next_id: AtomicU64,
    handles: RwLock<HashMap<u64, T>>,
}

impl<T> HandleMap<T> {
    pub const fn new() -> Self {
        HandleMap {
            next_id: AtomicU64::new(1),
            handles: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a value and return its handle
    pub fn insert(&self, value: T) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut handles = self.handles.write().unwrap();
        handles.insert(id, value);
        id
    }

    /// Get a value by handle
    pub fn get(&self, handle: u64) -> Option<T>
    where
        T: Clone,
    {
        let handles = self.handles.read().unwrap();
        handles.get(&handle).cloned()
    }

    /// Remove a value by handle
    pub fn remove(&self, handle: u64) -> Option<T> {
        let mut handles = self.handles.write().unwrap();
        handles.remove(&handle)
    }
}
