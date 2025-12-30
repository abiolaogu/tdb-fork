//! JWT token generation and validation

use chrono::{Duration, Utc};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use supabase_common::error::{Error, Result};
use supabase_common::types::{AuthMethod, TokenClaims, User};

/// JWT manager for token generation and validation
pub struct JwtManager {
    secret: Vec<u8>,
    issuer: String,
    audience: String,
    access_token_expiry: i64,
    refresh_token_expiry: i64,
    algorithm: Algorithm,
}

impl JwtManager {
    /// Create a new JWT manager with the given configuration
    pub fn new(
        secret: &str,
        issuer: &str,
        access_token_expiry: u64,
        refresh_token_expiry: u64,
    ) -> Self {
        Self {
            secret: secret.as_bytes().to_vec(),
            issuer: issuer.to_string(),
            audience: "authenticated".to_string(),
            access_token_expiry: access_token_expiry as i64,
            refresh_token_expiry: refresh_token_expiry as i64,
            algorithm: Algorithm::HS256,
        }
    }

    /// Generate a new JWT secret if none provided
    pub fn generate_secret() -> String {
        use base64::Engine;
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..32).map(|_| rng.gen()).collect();
        base64::engine::general_purpose::STANDARD.encode(&bytes)
    }

    /// Generate access token for a user
    pub fn generate_access_token(&self, user: &User, session_id: &str) -> Result<String> {
        let now = Utc::now();
        let exp = now + Duration::seconds(self.access_token_expiry);

        let claims = TokenClaims {
            aud: self.audience.clone(),
            exp: exp.timestamp(),
            iat: now.timestamp(),
            iss: self.issuer.clone(),
            sub: user.id.to_string(),
            email: user.email.clone(),
            phone: user.phone.clone(),
            app_metadata: user.app_metadata.clone(),
            user_metadata: user.user_metadata.clone(),
            role: user.role.clone(),
            amr: Some(vec![AuthMethod {
                method: "password".to_string(),
                timestamp: now.timestamp(),
            }]),
            session_id: Some(session_id.to_string()),
        };

        let header = Header::new(self.algorithm);
        let token = encode(&header, &claims, &EncodingKey::from_secret(&self.secret))
            .map_err(|e| Error::InternalError(format!("Failed to encode JWT: {}", e)))?;

        Ok(token)
    }

    /// Generate refresh token
    pub fn generate_refresh_token(&self) -> String {
        Uuid::new_v4().to_string()
    }

    /// Validate and decode an access token
    pub fn validate_access_token(&self, token: &str) -> Result<TokenClaims> {
        let mut validation = Validation::new(self.algorithm);
        validation.set_audience(&[&self.audience]);
        validation.set_issuer(&[&self.issuer]);

        let token_data =
            decode::<TokenClaims>(token, &DecodingKey::from_secret(&self.secret), &validation)
                .map_err(|e| match e.kind() {
                    jsonwebtoken::errors::ErrorKind::ExpiredSignature => Error::TokenExpired,
                    _ => Error::InvalidToken,
                })?;

        Ok(token_data.claims)
    }

    /// Extract claims without validation (for expired token refresh)
    pub fn decode_without_validation(&self, token: &str) -> Result<TokenClaims> {
        let mut validation = Validation::new(self.algorithm);
        validation.validate_exp = false;
        validation.validate_aud = false;

        let token_data =
            decode::<TokenClaims>(token, &DecodingKey::from_secret(&self.secret), &validation)
                .map_err(|_| Error::InvalidToken)?;

        Ok(token_data.claims)
    }

    /// Get expiration times
    pub fn access_token_expiry(&self) -> u64 {
        self.access_token_expiry as u64
    }

    pub fn refresh_token_expiry(&self) -> u64 {
        self.refresh_token_expiry as u64
    }
}

/// Refresh token stored in database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefreshToken {
    pub id: Uuid,
    pub token: String,
    pub user_id: Uuid,
    pub session_id: String,
    pub parent: Option<String>,
    pub revoked: bool,
    pub created_at: chrono::DateTime<Utc>,
    pub updated_at: chrono::DateTime<Utc>,
}

impl RefreshToken {
    pub fn new(user_id: Uuid, session_id: &str) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            token: Uuid::new_v4().to_string(),
            user_id,
            session_id: session_id.to_string(),
            parent: None,
            revoked: false,
            created_at: now,
            updated_at: now,
        }
    }

    /// Create a rotated refresh token (for token rotation)
    pub fn rotate(&self) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            token: Uuid::new_v4().to_string(),
            user_id: self.user_id,
            session_id: self.session_id.clone(),
            parent: Some(self.token.clone()),
            revoked: false,
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_user() -> User {
        User::new(Some("test@example.com".to_string()), None)
    }

    #[test]
    fn test_generate_access_token() {
        let manager = JwtManager::new("test-secret-key", "http://localhost", 3600, 604800);
        let user = create_test_user();
        let session_id = Uuid::new_v4().to_string();

        let token = manager.generate_access_token(&user, &session_id).unwrap();
        assert!(!token.is_empty());
    }

    #[test]
    fn test_validate_access_token() {
        let manager = JwtManager::new("test-secret-key", "http://localhost", 3600, 604800);
        let user = create_test_user();
        let session_id = Uuid::new_v4().to_string();

        let token = manager.generate_access_token(&user, &session_id).unwrap();
        let claims = manager.validate_access_token(&token).unwrap();

        assert_eq!(claims.sub, user.id.to_string());
        assert_eq!(claims.email, user.email);
    }

    #[test]
    fn test_invalid_token() {
        let manager = JwtManager::new("test-secret-key", "http://localhost", 3600, 604800);
        let result = manager.validate_access_token("invalid.token.here");
        assert!(result.is_err());
    }

    #[test]
    fn test_refresh_token_rotation() {
        let user_id = Uuid::new_v4();
        let session_id = Uuid::new_v4().to_string();
        let original = RefreshToken::new(user_id, &session_id);
        let rotated = original.rotate();

        assert_ne!(original.token, rotated.token);
        assert_eq!(rotated.parent, Some(original.token));
        assert_eq!(original.user_id, rotated.user_id);
    }
}
