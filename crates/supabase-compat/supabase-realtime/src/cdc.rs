//! Change Data Capture (CDC) for PostgreSQL-compatible change tracking
//!
//! Listens to database changes and emits events for real-time subscribers.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Type of database change event
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ChangeType {
    Insert,
    Update,
    Delete,
}

/// A database change event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeEvent {
    /// Unique event ID
    pub id: Uuid,
    /// Type of change
    #[serde(rename = "type")]
    pub change_type: ChangeType,
    /// Schema name
    pub schema: String,
    /// Table name
    pub table: String,
    /// Timestamp of the change
    pub commit_timestamp: DateTime<Utc>,
    /// New record (for INSERT and UPDATE)
    pub new: Option<serde_json::Value>,
    /// Old record (for UPDATE and DELETE)
    pub old: Option<serde_json::Value>,
    /// Primary key columns
    pub columns: Vec<ColumnInfo>,
    /// Any errors
    pub errors: Option<Vec<String>>,
}

/// Column metadata for CDC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub type_name: String,
}

impl ChangeEvent {
    /// Create an INSERT event
    pub fn insert(schema: &str, table: &str, new_record: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            change_type: ChangeType::Insert,
            schema: schema.to_string(),
            table: table.to_string(),
            commit_timestamp: Utc::now(),
            new: Some(new_record),
            old: None,
            columns: vec![],
            errors: None,
        }
    }

    /// Create an UPDATE event
    pub fn update(
        schema: &str,
        table: &str,
        old_record: serde_json::Value,
        new_record: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            change_type: ChangeType::Update,
            schema: schema.to_string(),
            table: table.to_string(),
            commit_timestamp: Utc::now(),
            new: Some(new_record),
            old: Some(old_record),
            columns: vec![],
            errors: None,
        }
    }

    /// Create a DELETE event
    pub fn delete(schema: &str, table: &str, old_record: serde_json::Value) -> Self {
        Self {
            id: Uuid::new_v4(),
            change_type: ChangeType::Delete,
            schema: schema.to_string(),
            table: table.to_string(),
            commit_timestamp: Utc::now(),
            new: None,
            old: Some(old_record),
            columns: vec![],
            errors: None,
        }
    }
}

/// Callback type for CDC listeners
pub type CdcCallback = Box<dyn Fn(ChangeEvent) + Send + Sync>;

/// CDC listener that tracks database changes
pub struct CdcListener {
    /// Registered callbacks by schema.table
    callbacks: RwLock<HashMap<String, Vec<Arc<CdcCallback>>>>,
    /// Global callbacks (all tables)
    global_callbacks: RwLock<Vec<Arc<CdcCallback>>>,
}

impl CdcListener {
    /// Create a new CDC listener
    pub fn new() -> Self {
        Self {
            callbacks: RwLock::new(HashMap::new()),
            global_callbacks: RwLock::new(Vec::new()),
        }
    }

    /// Register a callback for a specific table
    pub fn on_table<F>(&self, schema: &str, table: &str, callback: F)
    where
        F: Fn(ChangeEvent) + Send + Sync + 'static,
    {
        let key = format!("{}.{}", schema, table);
        let mut callbacks = self.callbacks.write();
        callbacks
            .entry(key)
            .or_insert_with(Vec::new)
            .push(Arc::new(Box::new(callback)));
    }

    /// Register a global callback for all tables
    pub fn on_all<F>(&self, callback: F)
    where
        F: Fn(ChangeEvent) + Send + Sync + 'static,
    {
        self.global_callbacks
            .write()
            .push(Arc::new(Box::new(callback)));
    }

    /// Emit a change event
    pub fn emit(&self, event: ChangeEvent) {
        let key = format!("{}.{}", event.schema, event.table);

        // Call table-specific callbacks
        if let Some(callbacks) = self.callbacks.read().get(&key) {
            for callback in callbacks {
                callback(event.clone());
            }
        }

        // Call global callbacks
        for callback in self.global_callbacks.read().iter() {
            callback(event.clone());
        }
    }

    /// Emit an INSERT event
    pub fn emit_insert(&self, schema: &str, table: &str, record: serde_json::Value) {
        self.emit(ChangeEvent::insert(schema, table, record));
    }

    /// Emit an UPDATE event
    pub fn emit_update(
        &self,
        schema: &str,
        table: &str,
        old_record: serde_json::Value,
        new_record: serde_json::Value,
    ) {
        self.emit(ChangeEvent::update(schema, table, old_record, new_record));
    }

    /// Emit a DELETE event
    pub fn emit_delete(&self, schema: &str, table: &str, record: serde_json::Value) {
        self.emit(ChangeEvent::delete(schema, table, record));
    }
}

impl Default for CdcListener {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_change_event_creation() {
        let event = ChangeEvent::insert(
            "public",
            "users",
            serde_json::json!({"id": 1, "name": "Alice"}),
        );
        assert_eq!(event.change_type, ChangeType::Insert);
        assert_eq!(event.schema, "public");
        assert_eq!(event.table, "users");
    }

    #[test]
    fn test_cdc_listener() {
        let listener = CdcListener::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        listener.on_table("public", "users", move |_| {
            counter_clone.fetch_add(1, Ordering::SeqCst);
        });

        listener.emit_insert("public", "users", serde_json::json!({"id": 1}));
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
