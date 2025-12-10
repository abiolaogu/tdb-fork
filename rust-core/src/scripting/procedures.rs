use std::sync::Arc;
use dashmap::DashMap;
use rhai::AST;
use crate::error::Result;
use super::ScriptingEngine;

pub struct Procedures {
    engine: Arc<ScriptingEngine>,
    scripts: DashMap<String, AST>,
}

impl Procedures {
    pub fn new(engine: Arc<ScriptingEngine>) -> Self {
        Self {
            engine,
            scripts: DashMap::new(),
        }
    }

    pub fn register(&self, name: &str, script: &str) -> Result<()> {
        let ast = self.engine.compile(script)?;
        self.scripts.insert(name.to_string(), ast);
        Ok(())
    }

    pub fn execute(&self, name: &str, args: Vec<rhai::Dynamic>) -> Result<rhai::Dynamic> {
        let ast = self.scripts.get(name)
            .ok_or_else(|| crate::error::TdbError::NotFound(format!("Procedure '{}' not found", name)))?;
            
        // Convert Vec<Dynamic> to FuncArgs is tricky dynamically in Rhai without a macro
        // For simplicity in this iteration, we treat it as a single array argument or void
        // Real implementation would inspect AST or usage.
        
        // Hack: execute the main function if it exists, roughly
        self.engine.call_fn(&ast, name, args)
    }
}
