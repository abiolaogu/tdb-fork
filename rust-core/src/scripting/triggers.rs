use std::sync::Arc;
use dashmap::DashMap;
use crate::error::Result;
use super::ScriptingEngine;
use crate::types::Document;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum TriggerEvent {
    BeforeInsert,
    AfterInsert,
    BeforeUpdate,
    AfterUpdate,
    BeforeDelete,
    AfterDelete,
}

pub struct Triggers {
    engine: Arc<ScriptingEngine>,
    // Map: Collection -> Event -> List of Script Names
    hooks: DashMap<String, DashMap<TriggerEvent, Vec<String>>>,
}

impl Triggers {
    pub fn new(engine: Arc<ScriptingEngine>) -> Self {
        Self {
            engine,
            hooks: DashMap::new(),
        }
    }

    pub fn register(&self, collection: &str, event: TriggerEvent, script_name: &str) {
        let entry = self.hooks.entry(collection.to_string()).or_insert_with(DashMap::new);
        let mut list = entry.entry(event).or_insert_with(Vec::new);
        if !list.contains(&script_name.to_string()) {
            list.push(script_name.to_string());
        }
    }

    pub fn on_event(&self, collection: &str, event: TriggerEvent, doc: &mut Document) -> Result<()> {
        if let Some(col_hooks) = self.hooks.get(collection) {
             if let Some(scripts) = col_hooks.get(&event) {
                 for script_name in scripts.iter() {
                     // In a real implementation, we would pass the document as an argument
                     // and potentially allow the script to modify it (for Before* events).
                     // For now, we just execute the script/function.
                     // The document binding would need complex Rhai type registration.
                     
                     // Placeholder: Just log or run simple function
                     // self.engine.call_fn(..., script_name, (doc.clone()))?;
                 }
             }
        }
        Ok(())
    }
}
