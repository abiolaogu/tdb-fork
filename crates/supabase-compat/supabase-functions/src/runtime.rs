//! Function execution runtime

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;

use crate::function::{EdgeFunction, FunctionStatus, InvocationRequest, InvocationResponse};

/// Runtime for executing edge functions
pub struct FunctionRuntime {
    /// Deployed functions by slug
    functions: Arc<RwLock<HashMap<String, EdgeFunction>>>,
    /// Global environment variables
    global_env: HashMap<String, String>,
}

impl FunctionRuntime {
    /// Create a new function runtime
    pub fn new() -> Self {
        Self {
            functions: Arc::new(RwLock::new(HashMap::new())),
            global_env: HashMap::new(),
        }
    }

    /// Set global environment variables
    pub fn with_global_env(mut self, env: HashMap<String, String>) -> Self {
        self.global_env = env;
        self
    }

    /// Deploy a function
    pub fn deploy(&self, mut function: EdgeFunction) -> Result<EdgeFunction, String> {
        // In production, this would compile/validate the function code
        function.activate();

        let slug = function.slug.clone();
        self.functions.write().insert(slug, function.clone());

        Ok(function)
    }

    /// Get a deployed function
    pub fn get(&self, slug: &str) -> Option<EdgeFunction> {
        self.functions.read().get(slug).cloned()
    }

    /// List all functions
    pub fn list(&self) -> Vec<EdgeFunction> {
        self.functions.read().values().cloned().collect()
    }

    /// Delete a function
    pub fn delete(&self, slug: &str) -> Option<EdgeFunction> {
        self.functions.write().remove(slug)
    }

    /// Invoke a function
    pub async fn invoke(
        &self,
        slug: &str,
        request: InvocationRequest,
    ) -> Result<InvocationResponse, String> {
        let start = Instant::now();

        let function = self
            .functions
            .read()
            .get(slug)
            .cloned()
            .ok_or_else(|| format!("Function '{}' not found", slug))?;

        if !function.is_invokable() {
            return Err(format!("Function '{}' is not active", slug));
        }

        // Check allowed methods
        if !function.config.allowed_methods.contains(&request.method) {
            return Ok(InvocationResponse::error(405, "Method not allowed"));
        }

        // Simulate function execution
        // In production, this would run the actual function code in an isolated runtime
        let mut response = self.execute_function(&function, &request).await;
        response.execution_time_ms = start.elapsed().as_millis() as u64;

        Ok(response)
    }

    /// Execute function code (simulated)
    async fn execute_function(
        &self,
        function: &EdgeFunction,
        request: &InvocationRequest,
    ) -> InvocationResponse {
        // Build environment for function
        let mut env = self.global_env.clone();
        env.extend(function.env_vars.clone());

        // Simulate function execution
        // In production, this would use a JS/TS runtime like Deno or V8
        InvocationResponse::ok(serde_json::json!({
            "function": function.name,
            "method": request.method,
            "path": request.path,
            "message": "Function executed successfully"
        }))
    }

    /// Check function health
    pub fn health_check(&self, slug: &str) -> bool {
        self.functions
            .read()
            .get(slug)
            .map(|f| f.status == FunctionStatus::Active)
            .unwrap_or(false)
    }
}

impl Default for FunctionRuntime {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_deploy_and_invoke() {
        let runtime = FunctionRuntime::new();
        let func = EdgeFunction::new("hello");

        runtime.deploy(func).unwrap();

        let request = InvocationRequest {
            method: "GET".to_string(),
            path: "/".to_string(),
            headers: HashMap::new(),
            query: HashMap::new(),
            body: None,
        };

        let response = runtime.invoke("hello", request).await.unwrap();
        assert_eq!(response.status, 200);
    }
}
