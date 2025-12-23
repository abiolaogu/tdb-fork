//! SASL authentication

use async_trait::async_trait;
use dashmap::DashMap;

use lumadb_common::error::{Result, Error, AuthError};

use super::Authenticator;
use crate::manager::{AuthenticatedUser, Credentials};

/// SASL authenticator
pub struct SaslAuth {
    /// Users database (in production, this would be external)
    users: DashMap<String, UserRecord>,
}

struct UserRecord {
    id: String,
    username: String,
    password_hash: String,
    roles: Vec<String>,
}

impl SaslAuth {
    pub fn new() -> Self {
        let auth = Self {
            users: DashMap::new(),
        };

        // Add default admin user
        auth.users.insert(
            "admin".to_string(),
            UserRecord {
                id: "1".to_string(),
                username: "admin".to_string(),
                password_hash: Self::hash_password("admin"),
                roles: vec!["admin".to_string()],
            },
        );

        auth
    }

    fn hash_password(password: &str) -> String {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        hex::encode(hasher.finalize())
    }

    fn verify_password(password: &str, hash: &str) -> bool {
        Self::hash_password(password) == hash
    }

    pub fn create_user(&self, username: &str, password: &str, roles: Vec<String>) {
        let id = uuid::Uuid::new_v4().to_string();
        self.users.insert(
            username.to_string(),
            UserRecord {
                id,
                username: username.to_string(),
                password_hash: Self::hash_password(password),
                roles,
            },
        );
    }
}

impl Default for SaslAuth {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Authenticator for SaslAuth {
    async fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser> {
        match credentials {
            Credentials::UsernamePassword { username, password } => {
                let user = self
                    .users
                    .get(username)
                    .ok_or(Error::Auth(AuthError::UserNotFound(username.clone())))?;

                if !Self::verify_password(password, &user.password_hash) {
                    return Err(Error::Auth(AuthError::InvalidCredentials));
                }

                Ok(AuthenticatedUser {
                    id: user.id.clone(),
                    username: user.username.clone(),
                    roles: user.roles.clone(),
                })
            }
            _ => Err(Error::Auth(AuthError::InvalidCredentials)),
        }
    }
}

// Need hex for password hashing
fn hex_encode(data: impl AsRef<[u8]>) -> String {
    data.as_ref().iter().map(|b| format!("{:02x}", b)).collect()
}
