//! Error types for TDB+

use thiserror::Error;

/// Result type for TDB+ operations
pub type Result<T> = std::result::Result<T, LumaError>;

/// TDB+ error types
#[derive(Error, Debug)]
pub enum LumaError {
    // Storage errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage corruption detected: {0}")]
    Corruption(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("File not found: {0}")]
    FileNotFound(String),

    #[error("Storage error: {0}")]
    Storage(String),

    // Document errors
    #[error("Document not found: {0}")]
    DocumentNotFound(String),

    #[error("Document already exists: {0}")]
    DocumentExists(String),

    #[error("Collection not found: {0}")]
    CollectionNotFound(String),

    #[error("Revision conflict: expected {expected}, got {actual}")]
    RevisionConflict { expected: u64, actual: u64 },

    // Serialization errors
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    // Transaction errors
    #[error("Transaction aborted: {0}")]
    TransactionAborted(String),

    #[error("Transaction timeout")]
    TransactionTimeout,

    #[error("Deadlock detected")]
    Deadlock,

    // Capacity errors
    #[error("Memory limit exceeded: {used} / {limit} bytes")]
    MemoryLimitExceeded { used: usize, limit: usize },

    #[error("Write buffer full")]
    WriteBufferFull,

    #[error("Too many open files")]
    TooManyOpenFiles,

    // Configuration errors
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    // WAL errors
    #[error("WAL write failed: {0}")]
    WalWriteFailed(String),

    #[error("WAL recovery failed: {0}")]
    WalRecoveryFailed(String),

    // Compaction errors
    #[error("Compaction failed: {0}")]
    CompactionFailed(String),

    // Index errors
    #[error("Index error: {0}")]
    IndexError(String),

    #[error("Index not found: {0}")]
    IndexNotFound(String),

    // Shard errors
    #[error("Shard not found: {0}")]
    ShardNotFound(u32),

    #[error("Shard unavailable: {0}")]
    ShardUnavailable(u32),

    // General errors
    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Operation cancelled")]
    Cancelled,

    // Legacy/Generic mappings
    #[error("Configuration error: {0}")]
    Config(String),
    #[error("Memory error: {0}")]
    Memory(String),
    #[error("Not found: {0}")]
    NotFound(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

impl From<bincode::Error> for LumaError {
    fn from(e: bincode::Error) -> Self {
        LumaError::Serialization(e.to_string())
    }
}

impl From<serde_json::Error> for LumaError {
    fn from(e: serde_json::Error) -> Self {
        LumaError::Serialization(e.to_string())
    }
}

impl LumaError {
    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            LumaError::Io(_)
                | LumaError::TransactionTimeout
                | LumaError::WriteBufferFull
                | LumaError::ShardUnavailable(_)
        )
    }

    /// Check if error indicates data corruption
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            LumaError::Corruption(_) | LumaError::ChecksumMismatch { .. }
        )
    }

    /// Get error code for FFI
    pub fn code(&self) -> i32 {
        match self {
            LumaError::Io(_) => 1,
            LumaError::Corruption(_) => 2,
            LumaError::ChecksumMismatch { .. } => 3,
            LumaError::FileNotFound(_) => 4,
            LumaError::Storage(_) => 204, // New code
            LumaError::DocumentNotFound(_) => 5,
            LumaError::DocumentExists(_) => 6,
            LumaError::CollectionNotFound(_) => 7,
            LumaError::RevisionConflict { .. } => 8,
            LumaError::Serialization(_) => 9,
            LumaError::Deserialization(_) => 10,
            LumaError::TransactionAborted(_) => 11,
            LumaError::TransactionTimeout => 12,
            LumaError::Deadlock => 13,
            LumaError::MemoryLimitExceeded { .. } => 14,
            LumaError::WriteBufferFull => 15,
            LumaError::TooManyOpenFiles => 16,
            LumaError::InvalidConfig(_) => 17,
            LumaError::WalWriteFailed(_) => 18,
            LumaError::WalRecoveryFailed(_) => 19,
            LumaError::CompactionFailed(_) => 20,
            LumaError::IndexError(_) => 21,
            LumaError::IndexNotFound(_) => 22,
            LumaError::ShardNotFound(_) => 23,
            LumaError::ShardUnavailable(_) => 24,
            LumaError::Internal(_) => 99,
            LumaError::NotImplemented(_) => 100,
            LumaError::Cancelled => 101,
            LumaError::Config(_) => 200,
            LumaError::Memory(_) => 201,
            LumaError::NotFound(_) => 202,
            LumaError::InvalidArgument(_) => 203,
        }
    }
}
