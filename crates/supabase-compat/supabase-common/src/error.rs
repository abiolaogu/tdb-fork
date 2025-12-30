//! Error types for Supabase compatibility layer

use thiserror::Error;

/// Supabase-specific error types
#[derive(Error, Debug)]
pub enum Error {
    // Authentication Errors
    #[error("Invalid credentials")]
    InvalidCredentials,

    #[error("User not found")]
    UserNotFound,

    #[error("User already exists")]
    UserAlreadyExists,

    #[error("Invalid token")]
    InvalidToken,

    #[error("Token expired")]
    TokenExpired,

    #[error("Refresh token invalid or expired")]
    RefreshTokenInvalid,

    #[error("Email not confirmed")]
    EmailNotConfirmed,

    #[error("Phone not confirmed")]
    PhoneNotConfirmed,

    #[error("Password does not meet requirements: {0}")]
    WeakPassword(String),

    #[error("MFA required")]
    MfaRequired,

    #[error("Invalid MFA code")]
    InvalidMfaCode,

    #[error("OAuth error: {0}")]
    OAuthError(String),

    // Authorization Errors
    #[error("Unauthorized")]
    Unauthorized,

    #[error("Forbidden: insufficient permissions")]
    Forbidden,

    #[error("RLS policy violation")]
    RlsPolicyViolation,

    // Database Errors
    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Foreign key violation: {0}")]
    ForeignKeyViolation(String),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    // REST API Errors
    #[error("Invalid query parameter: {0}")]
    InvalidQueryParam(String),

    #[error("Invalid filter: {0}")]
    InvalidFilter(String),

    #[error("Resource not found")]
    ResourceNotFound,

    #[error("Method not allowed")]
    MethodNotAllowed,

    #[error("Content type not supported")]
    UnsupportedContentType,

    // Storage Errors
    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("File too large: max {max_size} bytes")]
    FileTooLarge { max_size: usize },

    #[error("Invalid file type: {0}")]
    InvalidFileType(String),

    #[error("Storage quota exceeded")]
    StorageQuotaExceeded,

    // Realtime Errors
    #[error("Channel not found: {0}")]
    ChannelNotFound(String),

    #[error("Subscription error: {0}")]
    SubscriptionError(String),

    // General Errors
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Service unavailable")]
    ServiceUnavailable,

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

/// Result type alias for Supabase operations
pub type Result<T> = std::result::Result<T, Error>;

/// HTTP status code for each error type
impl Error {
    pub fn status_code(&self) -> u16 {
        match self {
            // 400 Bad Request
            Self::InvalidQueryParam(_)
            | Self::InvalidFilter(_)
            | Self::ValidationError(_)
            | Self::WeakPassword(_) => 400,

            // 401 Unauthorized
            Self::InvalidCredentials
            | Self::InvalidToken
            | Self::TokenExpired
            | Self::RefreshTokenInvalid
            | Self::Unauthorized => 401,

            // 403 Forbidden
            Self::Forbidden
            | Self::RlsPolicyViolation
            | Self::EmailNotConfirmed
            | Self::PhoneNotConfirmed => 403,

            // 404 Not Found
            Self::UserNotFound
            | Self::TableNotFound(_)
            | Self::ColumnNotFound(_)
            | Self::ResourceNotFound
            | Self::BucketNotFound(_)
            | Self::ObjectNotFound(_)
            | Self::ChannelNotFound(_) => 404,

            // 405 Method Not Allowed
            Self::MethodNotAllowed => 405,

            // 409 Conflict
            Self::UserAlreadyExists | Self::DuplicateKey(_) => 409,

            // 413 Payload Too Large
            Self::FileTooLarge { .. } => 413,

            // 415 Unsupported Media Type
            Self::UnsupportedContentType | Self::InvalidFileType(_) => 415,

            // 422 Unprocessable Entity
            Self::ForeignKeyViolation(_) | Self::MfaRequired | Self::InvalidMfaCode => 422,

            // 429 Too Many Requests
            Self::RateLimitExceeded | Self::StorageQuotaExceeded => 429,

            // 500 Internal Server Error
            Self::DatabaseError(_)
            | Self::QueryError(_)
            | Self::InternalError(_)
            | Self::ConfigError(_)
            | Self::IoError(_)
            | Self::JsonError(_)
            | Self::OAuthError(_)
            | Self::SubscriptionError(_) => 500,

            // 503 Service Unavailable
            Self::ServiceUnavailable => 503,
        }
    }

    /// Error code for API responses (matches Supabase error codes)
    pub fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidCredentials => "invalid_credentials",
            Self::UserNotFound => "user_not_found",
            Self::UserAlreadyExists => "user_already_exists",
            Self::InvalidToken => "invalid_token",
            Self::TokenExpired => "token_expired",
            Self::RefreshTokenInvalid => "refresh_token_invalid",
            Self::EmailNotConfirmed => "email_not_confirmed",
            Self::PhoneNotConfirmed => "phone_not_confirmed",
            Self::WeakPassword(_) => "weak_password",
            Self::MfaRequired => "mfa_required",
            Self::InvalidMfaCode => "invalid_mfa_code",
            Self::OAuthError(_) => "oauth_error",
            Self::Unauthorized => "unauthorized",
            Self::Forbidden => "forbidden",
            Self::RlsPolicyViolation => "rls_violation",
            Self::TableNotFound(_) => "table_not_found",
            Self::ColumnNotFound(_) => "column_not_found",
            Self::DuplicateKey(_) => "duplicate_key",
            Self::ForeignKeyViolation(_) => "fk_violation",
            Self::DatabaseError(_) => "database_error",
            Self::QueryError(_) => "query_error",
            Self::InvalidQueryParam(_) => "invalid_param",
            Self::InvalidFilter(_) => "invalid_filter",
            Self::ResourceNotFound => "not_found",
            Self::MethodNotAllowed => "method_not_allowed",
            Self::UnsupportedContentType => "unsupported_content_type",
            Self::BucketNotFound(_) => "bucket_not_found",
            Self::ObjectNotFound(_) => "object_not_found",
            Self::FileTooLarge { .. } => "file_too_large",
            Self::InvalidFileType(_) => "invalid_file_type",
            Self::StorageQuotaExceeded => "quota_exceeded",
            Self::ChannelNotFound(_) => "channel_not_found",
            Self::SubscriptionError(_) => "subscription_error",
            Self::ConfigError(_) => "config_error",
            Self::InternalError(_) => "internal_error",
            Self::RateLimitExceeded => "rate_limit_exceeded",
            Self::ServiceUnavailable => "service_unavailable",
            Self::ValidationError(_) => "validation_error",
            Self::IoError(_) => "io_error",
            Self::JsonError(_) => "json_error",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_status_codes() {
        assert_eq!(Error::InvalidCredentials.status_code(), 401);
        assert_eq!(Error::UserNotFound.status_code(), 404);
        assert_eq!(Error::UserAlreadyExists.status_code(), 409);
        assert_eq!(Error::FileTooLarge { max_size: 1000 }.status_code(), 413);
    }

    #[test]
    fn test_error_codes() {
        assert_eq!(
            Error::InvalidCredentials.error_code(),
            "invalid_credentials"
        );
        assert_eq!(Error::RlsPolicyViolation.error_code(), "rls_violation");
    }
}
