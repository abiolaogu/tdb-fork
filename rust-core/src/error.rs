//! Error types for TDB+

use thiserror::Error;

/// Result type for TDB+ operations
pub type Result<T> = std::result::Result<T, TdbError>;

/// TDB+ error types
#[derive(Error, Debug)]
pub enum TdbError {
    // Storage errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Storage corruption detected: {0}")]
    Corruption(String),

    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("File not found: {0}")]
    FileNotFound(String),

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
}

impl From<bincode::Error> for TdbError {
    fn from(e: bincode::Error) -> Self {
        TdbError::Serialization(e.to_string())
    }
}

impl From<serde_json::Error> for TdbError {
    fn from(e: serde_json::Error) -> Self {
        TdbError::Serialization(e.to_string())
    }
}

impl TdbError {
    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            TdbError::Io(_)
                | TdbError::TransactionTimeout
                | TdbError::WriteBufferFull
                | TdbError::ShardUnavailable(_)
        )
    }

    /// Check if error indicates data corruption
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            TdbError::Corruption(_) | TdbError::ChecksumMismatch { .. }
        )
    }

    /// Get error code for FFI
    pub fn code(&self) -> i32 {
        match self {
            TdbError::Io(_) => 1,
            TdbError::Corruption(_) => 2,
            TdbError::ChecksumMismatch { .. } => 3,
            TdbError::FileNotFound(_) => 4,
            TdbError::DocumentNotFound(_) => 5,
            TdbError::DocumentExists(_) => 6,
            TdbError::CollectionNotFound(_) => 7,
            TdbError::RevisionConflict { .. } => 8,
            TdbError::Serialization(_) => 9,
            TdbError::Deserialization(_) => 10,
            TdbError::TransactionAborted(_) => 11,
            TdbError::TransactionTimeout => 12,
            TdbError::Deadlock => 13,
            TdbError::MemoryLimitExceeded { .. } => 14,
            TdbError::WriteBufferFull => 15,
            TdbError::TooManyOpenFiles => 16,
            TdbError::InvalidConfig(_) => 17,
            TdbError::WalWriteFailed(_) => 18,
            TdbError::WalRecoveryFailed(_) => 19,
            TdbError::CompactionFailed(_) => 20,
            TdbError::IndexError(_) => 21,
            TdbError::IndexNotFound(_) => 22,
            TdbError::ShardNotFound(_) => 23,
            TdbError::ShardUnavailable(_) => 24,
            TdbError::Internal(_) => 99,
            TdbError::NotImplemented(_) => 100,
            TdbError::Cancelled => 101,
        }
    }
}
