//! Common types for Supabase compatibility layer

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ============================================================================
// User Types
// ============================================================================

/// User representation matching Supabase's user model
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: Uuid,
    pub aud: String,
    pub role: String,
    pub email: Option<String>,
    pub email_confirmed_at: Option<DateTime<Utc>>,
    pub phone: Option<String>,
    pub phone_confirmed_at: Option<DateTime<Utc>>,
    pub confirmation_sent_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub recovery_sent_at: Option<DateTime<Utc>>,
    pub last_sign_in_at: Option<DateTime<Utc>>,
    pub app_metadata: serde_json::Value,
    pub user_metadata: serde_json::Value,
    pub identities: Vec<Identity>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl User {
    pub fn new(email: Option<String>, phone: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            aud: "authenticated".to_string(),
            role: "authenticated".to_string(),
            email,
            email_confirmed_at: None,
            phone,
            phone_confirmed_at: None,
            confirmation_sent_at: None,
            confirmed_at: None,
            recovery_sent_at: None,
            last_sign_in_at: None,
            app_metadata: serde_json::json!({}),
            user_metadata: serde_json::json!({}),
            identities: vec![],
            created_at: now,
            updated_at: now,
        }
    }
}

/// OAuth identity linked to a user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Identity {
    pub id: String,
    pub user_id: Uuid,
    pub identity_data: serde_json::Value,
    pub provider: String,
    pub last_sign_in_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ============================================================================
// Session Types
// ============================================================================

/// Session containing access and refresh tokens
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub expires_at: u64,
    pub refresh_token: String,
    pub user: User,
}

/// Token claims for JWT
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClaims {
    /// Audience
    pub aud: String,
    /// Expiration time (as UTC timestamp)
    pub exp: i64,
    /// Issued at (as UTC timestamp)
    pub iat: i64,
    /// Issuer
    pub iss: String,
    /// Subject (user ID)
    pub sub: String,
    /// Email
    pub email: Option<String>,
    /// Phone
    pub phone: Option<String>,
    /// App metadata
    pub app_metadata: serde_json::Value,
    /// User metadata
    pub user_metadata: serde_json::Value,
    /// Role
    pub role: String,
    /// Authentication method used
    pub amr: Option<Vec<AuthMethod>>,
    /// Session ID
    pub session_id: Option<String>,
}

/// Authentication method reference
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthMethod {
    pub method: String,
    pub timestamp: i64,
}

// ============================================================================
// Database Types
// ============================================================================

/// Table metadata from schema introspection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub schema: String,
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKeyInfo>,
    pub is_view: bool,
    pub is_insertable: bool,
    pub is_updatable: bool,
    pub is_deletable: bool,
}

/// Column metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub has_default: bool,
    pub is_identity: bool,
    pub is_generated: bool,
    pub max_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub description: Option<String>,
}

/// Foreign key relationship
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeignKeyInfo {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_schema: String,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

/// Stored function/procedure metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionInfo {
    pub schema: String,
    pub name: String,
    pub return_type: String,
    pub is_set_returning: bool,
    pub parameters: Vec<FunctionParam>,
    pub volatility: String,
    pub description: Option<String>,
}

/// Function parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionParam {
    pub name: String,
    pub data_type: String,
    pub has_default: bool,
    pub mode: String, // IN, OUT, INOUT, VARIADIC
}

// ============================================================================
// RLS Types
// ============================================================================

/// Row Level Security policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RlsPolicy {
    pub name: String,
    pub schema: String,
    pub table: String,
    pub command: RlsCommand,
    pub roles: Vec<String>,
    pub using_expression: Option<String>,
    pub check_expression: Option<String>,
}

/// RLS command type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum RlsCommand {
    All,
    Select,
    Insert,
    Update,
    Delete,
}

// ============================================================================
// Storage Types
// ============================================================================

/// Storage bucket
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: String,
    pub name: String,
    pub owner: Option<Uuid>,
    pub public: bool,
    pub file_size_limit: Option<usize>,
    pub allowed_mime_types: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Storage object metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageObject {
    pub id: Uuid,
    pub bucket_id: String,
    pub name: String,
    pub owner: Option<Uuid>,
    pub metadata: serde_json::Value,
    pub path_tokens: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_accessed_at: Option<DateTime<Utc>>,
}

// ============================================================================
// Response Types
// ============================================================================

/// Standard API error response
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl ApiError {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: None,
            hint: None,
        }
    }

    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_creation() {
        let user = User::new(Some("test@example.com".to_string()), None);
        assert!(user.email.is_some());
        assert_eq!(user.role, "authenticated");
    }

    #[test]
    fn test_api_error() {
        let error = ApiError::new("invalid_request", "Missing required field")
            .with_hint("Include 'email' field");
        assert_eq!(error.code, "invalid_request");
        assert!(error.hint.is_some());
    }
}
