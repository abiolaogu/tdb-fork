//! JWT authentication

use async_trait::async_trait;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation, Algorithm};
use serde::{Deserialize, Serialize};

use lumadb_common::error::{Result, Error, AuthError};

use super::Authenticator;
use crate::manager::{AuthenticatedUser, Credentials};

/// JWT claims
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub username: String,
    pub roles: Vec<String>,
    pub exp: usize,
    pub iat: usize,
}

/// JWT authenticator
pub struct JwtAuth {
    secret: String,
}

impl JwtAuth {
    pub fn new(secret: &str) -> Self {
        Self {
            secret: secret.to_string(),
        }
    }

    /// Generate a JWT token
    pub fn generate_token(&self, user: &AuthenticatedUser) -> Result<String> {
        let now = chrono::Utc::now().timestamp() as usize;
        let claims = Claims {
            sub: user.id.clone(),
            username: user.username.clone(),
            roles: user.roles.clone(),
            exp: now + 86400, // 24 hours
            iat: now,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_bytes()),
        )
        .map_err(|e| Error::Auth(AuthError::InvalidToken(e.to_string())))
    }

    /// Validate a JWT token
    pub fn validate_token(&self, token: &str) -> Result<Claims> {
        let validation = Validation::new(Algorithm::HS256);
        let token_data = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_bytes()),
            &validation,
        )
        .map_err(|e| Error::Auth(AuthError::InvalidToken(e.to_string())))?;

        Ok(token_data.claims)
    }
}

#[async_trait]
impl Authenticator for JwtAuth {
    async fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser> {
        match credentials {
            Credentials::Token(token) => {
                let claims = self.validate_token(token)?;
                Ok(AuthenticatedUser {
                    id: claims.sub,
                    username: claims.username,
                    roles: claims.roles,
                })
            }
            _ => Err(Error::Auth(AuthError::InvalidCredentials)),
        }
    }
}
