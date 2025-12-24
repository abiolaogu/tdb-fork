//! Configuration management for LumaDB

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::{Error, Result};

/// Main configuration structure for LumaDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Storage configuration
    #[serde(default)]
    pub storage: StorageConfig,

    /// Streaming engine configuration
    #[serde(default)]
    pub streaming: StreamingConfig,

    /// Query engine configuration
    #[serde(default)]
    pub query: QueryConfig,

    /// API configuration
    #[serde(default)]
    pub api: ApiConfig,

    /// Kafka protocol configuration
    #[serde(default)]
    pub kafka: KafkaConfig,

    /// Raft consensus configuration
    #[serde(default)]
    pub raft: RaftConfig,

    /// Cluster configuration
    #[serde(default)]
    pub cluster: ClusterConfig,

    /// Security configuration
    #[serde(default)]
    pub security: SecurityConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
            streaming: StreamingConfig::default(),
            query: QueryConfig::default(),
            api: ApiConfig::default(),
            kafka: KafkaConfig::default(),
            raft: RaftConfig::default(),
            cluster: ClusterConfig::default(),
            security: SecurityConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from a YAML/TOML file
    pub async fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = tokio::fs::read_to_string(path.as_ref())
            .await
            .map_err(|e| Error::Config(format!("Failed to read config file: {}", e)))?;

        let config: Config = if path.as_ref().extension().map_or(false, |ext| ext == "toml") {
            toml::from_str(&content)
                .map_err(|e| Error::Config(format!("Failed to parse TOML config: {}", e)))?
        } else {
            serde_json::from_str(&content)
                .map_err(|e| Error::Config(format!("Failed to parse JSON config: {}", e)))?
        };

        Ok(config)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Node ID
    pub node_id: u64,
    /// Bind address
    pub bind_address: String,
    /// Data directory
    pub data_dir: String,
    /// Number of worker threads (0 = auto)
    pub workers: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            bind_address: "0.0.0.0".to_string(),
            data_dir: "/var/lib/lumadb".to_string(),
            workers: 0, // auto-detect
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Path to storage directory
    pub path: String,
    /// Maximum memory usage in bytes
    pub max_memory_bytes: usize,
    /// Enable write-ahead logging
    pub wal_enabled: bool,
    /// WAL sync mode
    pub wal_sync_mode: String,
    /// Compaction interval in seconds
    pub compaction_interval_secs: u64,
    /// Enable compression
    pub compression_enabled: bool,
    /// Compression algorithm
    pub compression_algorithm: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: "/var/lib/lumadb/data".to_string(),
            max_memory_bytes: 4 * 1024 * 1024 * 1024, // 4GB
            wal_enabled: true,
            wal_sync_mode: "fsync".to_string(),
            compaction_interval_secs: 3600,
            compression_enabled: true,
            compression_algorithm: "lz4".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingConfig {
    /// Number of partitions for default topics
    pub default_partitions: u32,
    /// Default replication factor
    pub default_replication_factor: u32,
    /// Segment size in bytes
    pub segment_size_bytes: usize,
    /// Retention time in milliseconds
    pub retention_ms: u64,
    /// Enable thread-per-core architecture
    pub thread_per_core: bool,
    /// Batch size for I/O operations
    pub batch_size: usize,
}

impl Default for StreamingConfig {
    fn default() -> Self {
        Self {
            default_partitions: 3,
            default_replication_factor: 1,
            segment_size_bytes: 1024 * 1024 * 1024, // 1GB
            retention_ms: 7 * 24 * 60 * 60 * 1000,  // 7 days
            thread_per_core: true,
            batch_size: 16384,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryConfig {
    /// Maximum query execution time in milliseconds
    pub max_execution_time_ms: u64,
    /// Enable query caching
    pub cache_enabled: bool,
    /// Query cache size in entries
    pub cache_size: usize,
    /// Enable vectorized execution
    pub vectorized_execution: bool,
    /// Default batch size for vectorized execution
    pub batch_size: usize,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            max_execution_time_ms: 30000,
            cache_enabled: true,
            cache_size: 10000,
            vectorized_execution: true,
            batch_size: 8192,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    /// REST API configuration
    #[serde(default)]
    pub rest: RestApiConfig,
    /// GraphQL API configuration
    #[serde(default)]
    pub graphql: GraphQLApiConfig,
    /// gRPC API configuration
    #[serde(default)]
    pub grpc: GrpcApiConfig,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            rest: RestApiConfig::default(),
            graphql: GraphQLApiConfig::default(),
            grpc: GrpcApiConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApiConfig {
    /// Enable REST API
    pub enabled: bool,
    /// Port number
    pub port: u16,
    /// Host address
    pub host: String,
    /// Number of worker threads
    pub workers: usize,
    /// Maximum connections
    pub max_connections: usize,
    /// Request timeout in milliseconds
    pub request_timeout_ms: u64,
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
}

impl Default for RestApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            host: "0.0.0.0".to_string(),
            workers: 4,
            max_connections: 10000,
            request_timeout_ms: 30000,
            cors_origins: vec!["*".to_string()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLApiConfig {
    /// Enable GraphQL API
    pub enabled: bool,
    /// Port number
    pub port: u16,
    /// Enable GraphQL Playground
    pub playground_enabled: bool,
    /// Maximum query depth
    pub max_depth: usize,
    /// Maximum query complexity
    pub max_complexity: usize,
}

impl Default for GraphQLApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8081,
            playground_enabled: true,
            max_depth: 10,
            max_complexity: 1000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcApiConfig {
    /// Enable gRPC API
    pub enabled: bool,
    /// Port number
    pub port: u16,
    /// Enable TLS
    pub tls_enabled: bool,
    /// Maximum message size in bytes
    pub max_message_size: usize,
}

impl Default for GrpcApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8082,
            tls_enabled: false,
            max_message_size: 16 * 1024 * 1024, // 16MB
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Enable Kafka protocol
    pub enabled: bool,
    /// Port number
    pub port: u16,
    /// Advertised listeners
    pub advertised_listeners: Vec<String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9092,
            advertised_listeners: vec!["localhost:9092".to_string()],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftConfig {
    /// Raft election timeout minimum in milliseconds
    pub election_timeout_min_ms: u64,
    /// Raft election timeout maximum in milliseconds
    pub election_timeout_max_ms: u64,
    /// Heartbeat interval in milliseconds
    pub heartbeat_interval_ms: u64,
    /// Snapshot interval in number of log entries
    pub snapshot_interval: u64,
    /// Maximum log entries before snapshot
    pub max_log_entries: u64,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            election_timeout_min_ms: 150,
            election_timeout_max_ms: 300,
            heartbeat_interval_ms: 50,
            snapshot_interval: 10000,
            max_log_entries: 100000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Cluster name
    pub name: String,
    /// Initial cluster members
    pub initial_members: Vec<String>,
    /// Enable auto-discovery
    pub auto_discovery: bool,
    /// Discovery method
    pub discovery_method: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: "lumadb-cluster".to_string(),
            initial_members: vec![],
            auto_discovery: false,
            discovery_method: "static".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Enable authentication
    pub auth_enabled: bool,
    /// Authentication method
    pub auth_method: String,
    /// JWT secret key (required when auth_method is "jwt")
    #[serde(default = "default_jwt_secret")]
    pub jwt_secret: String,
    /// JWT token expiration in seconds
    #[serde(default = "default_jwt_expiration")]
    pub jwt_expiration_secs: u64,
    /// Enable TLS
    pub tls_enabled: bool,
    /// TLS certificate path
    pub tls_cert_path: Option<String>,
    /// TLS key path
    pub tls_key_path: Option<String>,
    /// Enable audit logging
    pub audit_enabled: bool,
}

fn default_jwt_secret() -> String {
    std::env::var("LUMADB_JWT_SECRET").unwrap_or_else(|_| {
        // Generate a random secret if not provided (development only)
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let s = RandomState::new();
        let mut hasher = s.build_hasher();
        hasher.write_u64(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64);
        format!("dev-secret-{:x}", hasher.finish())
    })
}

fn default_jwt_expiration() -> u64 {
    86400 // 24 hours
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            auth_enabled: false,
            auth_method: "none".to_string(),
            jwt_secret: default_jwt_secret(),
            jwt_expiration_secs: default_jwt_expiration(),
            tls_enabled: false,
            tls_cert_path: None,
            tls_key_path: None,
            audit_enabled: false,
        }
    }
}
