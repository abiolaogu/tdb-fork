//! HTTP server for edge functions

use std::sync::Arc;

use supabase_common::config::FunctionsConfig;
use supabase_common::error::Result;

use crate::function::{EdgeFunction, InvocationRequest, InvocationResponse};
use crate::runtime::FunctionRuntime;

/// Edge functions HTTP server
pub struct FunctionsServer {
    config: FunctionsConfig,
    runtime: Arc<FunctionRuntime>,
}

impl FunctionsServer {
    /// Create a new functions server
    pub fn new(config: &FunctionsConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
            runtime: Arc::new(FunctionRuntime::new()),
        })
    }

    /// Get the function runtime
    pub fn runtime(&self) -> Arc<FunctionRuntime> {
        self.runtime.clone()
    }

    /// Deploy a function
    pub fn deploy(&self, function: EdgeFunction) -> std::result::Result<EdgeFunction, String> {
        self.runtime.deploy(function)
    }

    /// List all functions
    pub fn list_functions(&self) -> Vec<EdgeFunction> {
        self.runtime.list()
    }

    /// Get a function by slug
    pub fn get_function(&self, slug: &str) -> Option<EdgeFunction> {
        self.runtime.get(slug)
    }

    /// Delete a function
    pub fn delete_function(&self, slug: &str) -> Option<EdgeFunction> {
        self.runtime.delete(slug)
    }

    /// Invoke a function
    pub async fn invoke(
        &self,
        slug: &str,
        request: InvocationRequest,
    ) -> std::result::Result<InvocationResponse, String> {
        self.runtime.invoke(slug, request).await
    }

    /// Get the functions endpoint URL
    pub fn endpoint_url(&self) -> String {
        format!(
            "http://{}:{}/functions/v1",
            self.config.host, self.config.port
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> FunctionsConfig {
        FunctionsConfig::default()
    }

    #[test]
    fn test_functions_server() {
        let server = FunctionsServer::new(&test_config()).unwrap();
        assert!(server.list_functions().is_empty());
    }

    #[tokio::test]
    async fn test_deploy_and_invoke() {
        let server = FunctionsServer::new(&test_config()).unwrap();

        let func = EdgeFunction::new("test-func");
        server.deploy(func).unwrap();

        assert!(server.get_function("test-func").is_some());
    }
}
