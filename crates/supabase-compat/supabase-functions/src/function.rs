//! Edge function definition and management

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Status of an edge function
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FunctionStatus {
    /// Function is active and can be invoked
    Active,
    /// Function is being deployed
    Deploying,
    /// Function deployment failed
    Failed,
    /// Function is disabled
    Disabled,
}

/// Configuration for an edge function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionConfig {
    /// Whether the function requires authentication
    pub verify_jwt: bool,
    /// Memory limit in MB
    pub memory_limit_mb: u32,
    /// Timeout in seconds
    pub timeout_seconds: u32,
    /// Allowed HTTP methods
    pub allowed_methods: Vec<String>,
    /// CORS configuration
    pub cors: CorsConfig,
}

impl Default for FunctionConfig {
    fn default() -> Self {
        Self {
            verify_jwt: true,
            memory_limit_mb: 256,
            timeout_seconds: 30,
            allowed_methods: vec!["GET".to_string(), "POST".to_string(), "OPTIONS".to_string()],
            cors: CorsConfig::default(),
        }
    }
}

/// CORS configuration for functions
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CorsConfig {
    pub allowed_origins: Vec<String>,
    pub allowed_headers: Vec<String>,
    pub max_age: Option<u32>,
}

/// An edge function definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeFunction {
    /// Unique function ID
    pub id: String,
    /// Function name (used in URL path)
    pub name: String,
    /// Function slug (URL-safe name)
    pub slug: String,
    /// Function status
    pub status: FunctionStatus,
    /// Version string
    pub version: String,
    /// Entry point file
    pub entrypoint: String,
    /// Function configuration
    pub config: FunctionConfig,
    /// Environment variables
    pub env_vars: HashMap<String, String>,
    /// Created timestamp
    pub created_at: DateTime<Utc>,
    /// Updated timestamp
    pub updated_at: DateTime<Utc>,
}

impl EdgeFunction {
    /// Create a new edge function
    pub fn new(name: &str) -> Self {
        let now = Utc::now();
        let slug = name.to_lowercase().replace(' ', "-");

        Self {
            id: Uuid::new_v4().to_string(),
            name: name.to_string(),
            slug,
            status: FunctionStatus::Deploying,
            version: "1".to_string(),
            entrypoint: "index.ts".to_string(),
            config: FunctionConfig::default(),
            env_vars: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Set function configuration
    pub fn with_config(mut self, config: FunctionConfig) -> Self {
        self.config = config;
        self
    }

    /// Add an environment variable
    pub fn with_env(mut self, key: &str, value: &str) -> Self {
        self.env_vars.insert(key.to_string(), value.to_string());
        self
    }

    /// Mark function as active
    pub fn activate(&mut self) {
        self.status = FunctionStatus::Active;
        self.updated_at = Utc::now();
    }

    /// Mark function as failed
    pub fn mark_failed(&mut self) {
        self.status = FunctionStatus::Failed;
        self.updated_at = Utc::now();
    }

    /// Disable function
    pub fn disable(&mut self) {
        self.status = FunctionStatus::Disabled;
        self.updated_at = Utc::now();
    }

    /// Check if function can be invoked
    pub fn is_invokable(&self) -> bool {
        self.status == FunctionStatus::Active
    }
}

/// Invocation request for an edge function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRequest {
    /// HTTP method
    pub method: String,
    /// Request path
    pub path: String,
    /// Request headers
    pub headers: HashMap<String, String>,
    /// Query parameters
    pub query: HashMap<String, String>,
    /// Request body
    pub body: Option<serde_json::Value>,
}

/// Invocation response from an edge function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationResponse {
    /// HTTP status code
    pub status: u16,
    /// Response headers
    pub headers: HashMap<String, String>,
    /// Response body
    pub body: serde_json::Value,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl InvocationResponse {
    /// Create a success response
    pub fn ok(body: serde_json::Value) -> Self {
        Self {
            status: 200,
            headers: HashMap::new(),
            body,
            execution_time_ms: 0,
        }
    }

    /// Create an error response
    pub fn error(status: u16, message: &str) -> Self {
        Self {
            status,
            headers: HashMap::new(),
            body: serde_json::json!({"error": message}),
            execution_time_ms: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_function_creation() {
        let func = EdgeFunction::new("hello-world");
        assert_eq!(func.name, "hello-world");
        assert_eq!(func.slug, "hello-world");
        assert_eq!(func.status, FunctionStatus::Deploying);
    }

    #[test]
    fn test_function_activation() {
        let mut func = EdgeFunction::new("test");
        assert!(!func.is_invokable());

        func.activate();
        assert!(func.is_invokable());
    }
}
