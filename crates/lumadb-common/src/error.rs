//! Error types for LumaDB
//!
//! Provides a unified error type hierarchy for the entire system.

use thiserror::Error;

/// Result type alias using LumaDB's Error type
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for LumaDB
#[derive(Error, Debug)]
pub enum Error {
    // Storage Errors
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    // Query Errors
    #[error("Query error: {0}")]
    Query(#[from] QueryError),

    // Network Errors
    #[error("Network error: {0}")]
    Network(#[from] NetworkError),

    // Protocol Errors
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    // Authentication/Authorization Errors
    #[error("Auth error: {0}")]
    Auth(#[from] AuthError),

    // Cluster Errors
    #[error("Cluster error: {0}")]
    Cluster(#[from] ClusterError),

    // Transaction Errors
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),

    // Configuration Errors
    #[error("Configuration error: {0}")]
    Config(String),

    // IO Errors
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    // Serialization Errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    // Internal Errors
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Storage-related errors
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Key not found: {0}")]
    KeyNotFound(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Write failed: {0}")]
    WriteFailed(String),

    #[error("Read failed: {0}")]
    ReadFailed(String),

    #[error("Compaction failed: {0}")]
    CompactionFailed(String),

    #[error("Corrupt data: {0}")]
    CorruptData(String),

    #[error("Capacity exceeded: {0}")]
    CapacityExceeded(String),

    #[error("WAL error: {0}")]
    WalError(String),
}

/// Query-related errors
#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Semantic error: {0}")]
    SemanticError(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Unknown function: {0}")]
    UnknownFunction(String),

    #[error("Unknown column: {0}")]
    UnknownColumn(String),

    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Query timeout after {0}ms")]
    Timeout(u64),

    #[error("Query cancelled")]
    Cancelled,
}

/// Network-related errors
#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("TLS error: {0}")]
    TlsError(String),

    #[error("Address resolution failed: {0}")]
    AddressResolution(String),
}

/// Protocol-related errors
#[derive(Error, Debug)]
pub enum ProtocolError {
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),

    #[error("Unsupported version: {0}")]
    UnsupportedVersion(String),

    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),

    #[error("CRC mismatch")]
    CrcMismatch,

    #[error("Incomplete message")]
    IncompleteMessage,
}

/// Authentication/Authorization errors
#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("Token expired")]
    TokenExpired,

    #[error("Invalid token: {0}")]
    InvalidToken(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("User not found: {0}")]
    UserNotFound(String),

    #[error("Role not found: {0}")]
    RoleNotFound(String),
}

/// Cluster-related errors
#[derive(Error, Debug)]
pub enum ClusterError {
    #[error("Node not found: {0}")]
    NodeNotFound(String),

    #[error("Leader not elected")]
    NoLeader,

    #[error("Not leader, leader is: {0}")]
    NotLeader(String),

    #[error("Consensus failed: {0}")]
    ConsensusFailed(String),

    #[error("Replication failed: {0}")]
    ReplicationFailed(String),

    #[error("Partition unavailable: {0}")]
    PartitionUnavailable(String),

    #[error("Quorum not reached")]
    QuorumNotReached,
}

/// Transaction-related errors
#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Transaction aborted: {0}")]
    Aborted(String),

    #[error("Conflict detected: {0}")]
    Conflict(String),

    #[error("Deadlock detected")]
    Deadlock,

    #[error("Lock timeout")]
    LockTimeout,

    #[error("Serialization failure: {0}")]
    SerializationFailure(String),

    #[error("Transaction already committed")]
    AlreadyCommitted,

    #[error("Transaction already rolled back")]
    AlreadyRolledBack,
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Serialization(e.to_string())
    }
}
