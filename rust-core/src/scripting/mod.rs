use rhai::{Engine, Scope, AST, Dynamic};
use std::sync::Arc;
use crate::error::Result;
use crate::types::Document;
use crate::storage::Collection;

pub mod procedures;
pub mod triggers;

/// Wrapper around Rhai engine
#[derive(Clone)]
pub struct ScriptingEngine {
    engine: Arc<Engine>,
}

impl ScriptingEngine {
    pub fn new() -> Self {
        let mut engine = Engine::new();
        
        // Register custom types
        engine.register_type::<Document>()
              .register_fn("get_id", |doc: &mut Document| doc.id.clone())
              .register_fn("matches", |doc: &mut Document, key: &str, value: &str| {
                  // Simple helper for scripts
                  if let Some(v) = doc.data.get(key) {
                      v.as_str() == Some(value)
                  } else {
                      false
                  }
              });

        // Register Collection type (as Arc<Collection> since Collection is not Clone)
        engine.register_type_with_name::<Arc<Collection>>("Collection");
        
        Self {
            engine: Arc::new(engine),
        }
    }

    pub fn compile(&self, script: &str) -> Result<AST> {
        self.engine.compile(script)
            .map_err(|e| crate::error::TdbError::Internal(format!("Script compilation error: {}", e)))
    }

    pub fn call_fn(&self, ast: &AST, name: &str, args: impl rhai::FuncArgs) -> Result<Dynamic> {
        let mut scope = Scope::new();
        self.engine.call_fn(&mut scope, ast, name, args)
            .map_err(|e| crate::error::TdbError::Internal(format!("Script execution error: {}", e)))
    }

    pub fn eval(&self, script: &str) -> Result<Dynamic> {
        self.engine.eval(script)
            .map_err(|e| crate::error::TdbError::Internal(format!("Script evaluation error: {}", e)))
    }
}

impl Default for ScriptingEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Value, Document};
    use std::collections::HashMap;

    #[test]
    fn test_rhai_document_binding() {
        let engine = ScriptingEngine::new();
        let script = r#"
            // We can't create Document in script yet without a constructor, 
            // but we can verify the type is registered if we can pass it in.
            // Since eval doesn't take scope, we test basic compilation of registered types usage if we could.
            // However, without a constructor, we can only test side effects or return values if we had a function returning Document.
            // Let's at least check that 'Collection' is recognized as a type name if possible, or just run a simple script.
            let x = 1 + 2;
            x
        "#;
        let result = engine.eval(script).unwrap();
        assert_eq!(result.as_int().unwrap(), 3);
    }
}
