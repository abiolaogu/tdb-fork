//! mTLS authentication (stub)

use async_trait::async_trait;

use lumadb_common::error::{Result, Error, AuthError};

use super::Authenticator;
use crate::manager::{AuthenticatedUser, Credentials};

/// mTLS authenticator
pub struct MtlsAuth;

impl MtlsAuth {
    pub fn new() -> Self {
        Self
    }
}

impl Default for MtlsAuth {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Authenticator for MtlsAuth {
    async fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser> {
        match credentials {
            Credentials::Certificate(cert) => {
                // In production, validate certificate and extract identity
                Ok(AuthenticatedUser {
                    id: "cert-user".to_string(),
                    username: "cert-user".to_string(),
                    roles: vec!["user".to_string()],
                })
            }
            _ => Err(Error::Auth(AuthError::InvalidCredentials)),
        }
    }
}
