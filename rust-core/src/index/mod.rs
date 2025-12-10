//! Index implementations - B-tree and Hash indexes

use std::collections::{BTreeMap, HashMap};
use parking_lot::RwLock;
use crate::types::DocumentId;
use crate::error::Result;

pub trait Index: Send + Sync {
    fn insert(&self, key: &[u8], doc_id: &DocumentId) -> Result<()>;
    fn remove(&self, key: &[u8], doc_id: &DocumentId) -> Result<()>;
    fn lookup(&self, key: &[u8]) -> Result<Vec<DocumentId>>;
    fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<DocumentId>>;
}

#[derive(Clone, Copy, Debug)]
pub enum IndexType {
    BTree,
    Hash,
    FullText,
}

pub struct BTreeIndex {
    data: RwLock<BTreeMap<Vec<u8>, Vec<DocumentId>>>,
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self { data: RwLock::new(BTreeMap::new()) }
    }
}

impl Index for BTreeIndex {
    fn insert(&self, key: &[u8], doc_id: &DocumentId) -> Result<()> {
        self.data.write()
            .entry(key.to_vec())
            .or_insert_with(Vec::new)
            .push(doc_id.clone());
        Ok(())
    }

    fn remove(&self, key: &[u8], doc_id: &DocumentId) -> Result<()> {
        if let Some(ids) = self.data.write().get_mut(key) {
            ids.retain(|id| id != doc_id);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<DocumentId>> {
        Ok(self.data.read().get(key).cloned().unwrap_or_default())
    }

    fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<DocumentId>> {
        let data = self.data.read();
        let mut results = Vec::new();
        for (_, ids) in data.range(start.to_vec()..end.to_vec()) {
            results.extend(ids.clone());
        }
        Ok(results)
    }
}

pub struct HashIndex {
    data: RwLock<HashMap<Vec<u8>, Vec<DocumentId>>>,
}

impl HashIndex {
    pub fn new() -> Self {
        Self { data: RwLock::new(HashMap::new()) }
    }
}

impl Index for HashIndex {
    fn insert(&self, key: &[u8], doc_id: &DocumentId) -> Result<()> {
        self.data.write()
            .entry(key.to_vec())
            .or_insert_with(Vec::new)
            .push(doc_id.clone());
        Ok(())
    }

    fn remove(&self, key: &[u8], doc_id: &DocumentId) -> Result<()> {
        if let Some(ids) = self.data.write().get_mut(key) {
            ids.retain(|id| id != doc_id);
        }
        Ok(())
    }

    fn lookup(&self, key: &[u8]) -> Result<Vec<DocumentId>> {
        Ok(self.data.read().get(key).cloned().unwrap_or_default())
    }

    fn range(&self, _start: &[u8], _end: &[u8]) -> Result<Vec<DocumentId>> {
        // Hash index doesn't support range queries efficiently
        Ok(Vec::new())
    }
}

impl Default for BTreeIndex {
    fn default() -> Self { Self::new() }
}

impl Default for HashIndex {
    fn default() -> Self { Self::new() }
}
