//! Configuration types for Supabase compatibility layer

use serde::{Deserialize, Serialize};

/// Main configuration for all Supabase services
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SupabaseConfig {
    /// REST API configuration
    pub rest: RestConfig,
    /// Authentication service configuration
    pub auth: AuthConfig,
    /// Real-time service configuration
    pub realtime: RealtimeConfig,
    /// Storage service configuration
    pub storage: StorageConfig,
    /// Edge functions configuration
    pub functions: FunctionsConfig,
    /// Database connection configuration
    pub database: DatabaseConfig,
}

impl Default for SupabaseConfig {
    fn default() -> Self {
        Self {
            rest: RestConfig::default(),
            auth: AuthConfig::default(),
            realtime: RealtimeConfig::default(),
            storage: StorageConfig::default(),
            functions: FunctionsConfig::default(),
            database: DatabaseConfig::default(),
        }
    }
}

/// REST API (PostgREST-compatible) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RestConfig {
    /// Host to bind to
    pub host: String,
    /// Port for REST API (default: 3000)
    pub port: u16,
    /// Database schema to expose (default: "public")
    pub schema: String,
    /// Maximum rows per request
    pub max_rows: usize,
    /// Enable OpenAPI documentation
    pub openapi_enabled: bool,
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
}

impl Default for RestConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 3000,
            schema: "public".to_string(),
            max_rows: 1000,
            openapi_enabled: true,
            cors_origins: vec!["*".to_string()],
        }
    }
}

/// Authentication service (GoTrue-compatible) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Host to bind to
    pub host: String,
    /// Port for auth service (default: 9999)
    pub port: u16,
    /// JWT secret for HS256 (auto-generated if not set)
    pub jwt_secret: Option<String>,
    /// JWT expiration in seconds (default: 3600)
    pub jwt_expiry: u64,
    /// Refresh token expiration in seconds (default: 604800 = 7 days)
    pub refresh_token_expiry: u64,
    /// Site URL for redirects
    pub site_url: String,
    /// Enable email confirmations
    pub email_confirm_required: bool,
    /// Enable phone confirmations
    pub phone_confirm_required: bool,
    /// OAuth providers configuration
    pub oauth_providers: OAuthProvidersConfig,
    /// Password requirements
    pub password_requirements: PasswordRequirements,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 9999,
            jwt_secret: None,
            jwt_expiry: 3600,
            refresh_token_expiry: 604800,
            site_url: "http://localhost:3000".to_string(),
            email_confirm_required: false,
            phone_confirm_required: false,
            oauth_providers: OAuthProvidersConfig::default(),
            password_requirements: PasswordRequirements::default(),
        }
    }
}

/// OAuth providers configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct OAuthProvidersConfig {
    pub google: Option<OAuthProviderConfig>,
    pub github: Option<OAuthProviderConfig>,
    pub gitlab: Option<OAuthProviderConfig>,
    pub discord: Option<OAuthProviderConfig>,
    pub apple: Option<OAuthProviderConfig>,
    pub azure: Option<OAuthProviderConfig>,
    pub facebook: Option<OAuthProviderConfig>,
    pub twitter: Option<OAuthProviderConfig>,
    pub linkedin: Option<OAuthProviderConfig>,
    pub slack: Option<OAuthProviderConfig>,
    pub spotify: Option<OAuthProviderConfig>,
    pub twitch: Option<OAuthProviderConfig>,
    pub notion: Option<OAuthProviderConfig>,
    pub bitbucket: Option<OAuthProviderConfig>,
}

/// Single OAuth provider configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthProviderConfig {
    pub client_id: String,
    pub client_secret: String,
    pub redirect_uri: Option<String>,
    pub scopes: Vec<String>,
}

/// Password requirements configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PasswordRequirements {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_numbers: bool,
    pub require_special: bool,
}

impl Default for PasswordRequirements {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: false,
            require_lowercase: false,
            require_numbers: false,
            require_special: false,
        }
    }
}

/// Real-time service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RealtimeConfig {
    /// Host to bind to
    pub host: String,
    /// Port for realtime WebSocket (default: 4000)
    pub port: u16,
    /// Maximum connections per node
    pub max_connections: usize,
    /// Heartbeat interval in seconds
    pub heartbeat_interval: u64,
    /// Channel message buffer size
    pub message_buffer_size: usize,
}

impl Default for RealtimeConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 4000,
            max_connections: 100_000,
            heartbeat_interval: 30,
            message_buffer_size: 1000,
        }
    }
}

/// Storage service configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Host to bind to
    pub host: String,
    /// Port for storage service (default: 5000)
    pub port: u16,
    /// Storage backend path
    pub storage_path: String,
    /// Maximum file size in bytes (default: 50MB)
    pub max_file_size: usize,
    /// Allowed MIME types (empty = all allowed)
    pub allowed_mime_types: Vec<String>,
    /// Enable image transformations
    pub enable_image_transforms: bool,
    /// Public URL for object access
    pub public_url: Option<String>,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 5000,
            storage_path: "./storage".to_string(),
            max_file_size: 50 * 1024 * 1024,
            allowed_mime_types: vec![],
            enable_image_transforms: true,
            public_url: None,
        }
    }
}

/// Edge functions configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FunctionsConfig {
    /// Host to bind to
    pub host: String,
    /// Port for functions service (default: 5001)
    pub port: u16,
    /// Default timeout in seconds
    pub timeout_seconds: u32,
    /// Default memory limit in MB
    pub memory_limit_mb: u32,
    /// Maximum concurrent invocations
    pub max_concurrent: usize,
}

impl Default for FunctionsConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 5001,
            timeout_seconds: 30,
            memory_limit_mb: 256,
            max_concurrent: 100,
        }
    }
}

/// Database connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DatabaseConfig {
    /// Database host
    pub host: String,
    /// Database port
    pub port: u16,
    /// Database name
    pub database: String,
    /// Database user
    pub user: String,
    /// Database password
    pub password: String,
    /// Connection pool size
    pub pool_size: u32,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: "postgres".to_string(),
            user: "postgres".to_string(),
            password: "postgres".to_string(),
            pool_size: 10,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SupabaseConfig::default();
        assert_eq!(config.rest.port, 3000);
        assert_eq!(config.auth.port, 9999);
        assert_eq!(config.realtime.port, 4000);
        assert_eq!(config.storage.port, 5000);
    }

    #[test]
    fn test_config_serialization() {
        let config = SupabaseConfig::default();
        let json = serde_json::to_string(&config).unwrap();
        let parsed: SupabaseConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(config.rest.port, parsed.rest.port);
    }
}
