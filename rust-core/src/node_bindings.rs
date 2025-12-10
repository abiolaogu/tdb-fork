use napi::bindgen_prelude::*;
use napi_derive::napi;
use std::sync::Arc;
use crate::config::Config;
use crate::Database as CoreDatabase;
use crate::types::Document;

#[napi]
pub struct Database {
    inner: Arc<CoreDatabase>,
}

#[napi]
impl Database {
    #[napi(factory)]
    pub async fn open(path: String) -> Result<Database> {
        let mut config = Config::default();
        config.data_dir = std::path::PathBuf::from(path);
        
        // Optimize for local node usage
        config.memory.use_mmap = true;
        
        let db = CoreDatabase::open(config)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
            
        Ok(Database {
            inner: Arc::new(db),
        })
    }

    #[napi]
    pub async fn close(&self) -> Result<()> {
        self.inner.close()
            .await
            .map_err(|e| Error::from_reason(e.to_string()))
    }

    #[napi]
    pub async fn register_procedure(&self, name: String, script: String) -> Result<()> {
        self.inner.procedures.register(&name, &script)
            .map_err(|e| Error::from_reason(e.to_string()))
    }
    
    #[napi]
    pub async fn execute_procedure(&self, name: String, args_json: String) -> Result<String> {
        // Simple argument parsing from JSON array
        let _args: Vec<serde_json::Value> = serde_json::from_str(&args_json)
             .map_err(|e| Error::from_reason(e.to_string()))?;
        
        // Dynamic conversion omitted for brevity, passing empty args
        let result = self.inner.procedures.execute(&name, vec![])
            .map_err(|e| Error::from_reason(e.to_string()))?;
            
        Ok(result.to_string())
    }

    #[napi]
    pub async fn register_trigger(&self, collection: String, event: String, script_name: String) -> Result<()> {
        use crate::scripting::triggers::TriggerEvent;
        let evt = match event.as_str() {
            "BeforeInsert" => TriggerEvent::BeforeInsert,
            "AfterInsert"  => TriggerEvent::AfterInsert,
            "BeforeUpdate" => TriggerEvent::BeforeUpdate,
            "AfterUpdate"  => TriggerEvent::AfterUpdate,
            "BeforeDelete" => TriggerEvent::BeforeDelete,
            "AfterDelete"  => TriggerEvent::AfterDelete,
            _ => return Err(Error::from_reason("Invalid trigger event".to_string())),
        };
        
        self.inner.triggers.register(&collection, evt, &script_name);
        Ok(())
    }

    #[napi]
    pub fn collection(&self, name: String) -> Collection {
        Collection {
            name: name.clone(),
            db: self.inner.clone(),
        }
    }
}

#[napi]
pub struct Collection {
    name: String,
    db: Arc<CoreDatabase>,
}

#[napi]
impl Collection {
    #[napi]
    pub async fn insert(&self, doc_json: String) -> Result<String> {
        let doc: Document = serde_json::from_str(&doc_json)
            .map_err(|e| Error::from_reason(format!("Invalid JSON: {}", e)))?;
            
        let id = self.db.insert(&self.name, doc)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
            
        Ok(id)
    }

    #[napi]
    pub async fn get(&self, id: String) -> Result<Option<String>> {
        let doc = self.db.get(&self.name, &id)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
            
        match doc {
            Some(d) => serde_json::to_string(&d)
                .map(Some)
                .map_err(|e| Error::from_reason(e.to_string())),
            None => Ok(None),
        }
    }
    
    #[napi]
    pub async fn scan(&self) -> Result<Vec<String>> {
        let docs = self.db.scan(&self.name, |_| true)
            .await
            .map_err(|e| Error::from_reason(e.to_string()))?;
            
        docs.into_iter()
            .map(|d| serde_json::to_string(&d).map_err(|e| Error::from_reason(e.to_string())))
            .collect()
    }
}
