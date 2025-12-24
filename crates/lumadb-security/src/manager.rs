//! Security manager implementation

use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::info;

use lumadb_common::config::SecurityConfig;
use lumadb_common::error::{Result, Error, AuthError};

use crate::auth::{Authenticator, JwtAuth, SaslAuth};
use crate::authz::{Authorizer, Permission, Role};

/// Main security manager
pub struct SecurityManager {
    config: SecurityConfig,
    authenticator: Box<dyn Authenticator>,
    authorizer: Authorizer,
    sessions: DashMap<String, Session>,
}

/// User session
#[derive(Debug, Clone)]
pub struct Session {
    pub user_id: String,
    pub username: String,
    pub roles: Vec<String>,
    pub created_at: i64,
    pub expires_at: i64,
}

impl SecurityManager {
    /// Create a new security manager
    pub async fn new(config: &SecurityConfig) -> Result<Self> {
        info!("Initializing security manager");

        let authenticator: Box<dyn Authenticator> = if config.auth_enabled {
            match config.auth_method.as_str() {
                "jwt" => {
                    if config.jwt_secret.starts_with("dev-secret-") {
                        tracing::warn!(
                            "Using auto-generated JWT secret. Set LUMADB_JWT_SECRET environment variable for production!"
                        );
                    }
                    Box::new(JwtAuth::new(&config.jwt_secret))
                },
                "sasl" => Box::new(SaslAuth::new()),
                _ => Box::new(NoAuth),
            }
        } else {
            Box::new(NoAuth)
        };

        let authorizer = Authorizer::new();

        Ok(Self {
            config: config.clone(),
            authenticator,
            authorizer,
            sessions: DashMap::new(),
        })
    }

    /// Authenticate a user
    pub async fn authenticate(
        &self,
        credentials: &Credentials,
    ) -> Result<Session> {
        if !self.config.auth_enabled {
            return Ok(Session {
                user_id: "anonymous".to_string(),
                username: "anonymous".to_string(),
                roles: vec!["admin".to_string()],
                created_at: chrono::Utc::now().timestamp_millis(),
                expires_at: chrono::Utc::now().timestamp_millis() + 86400000,
            });
        }

        let user = self.authenticator.authenticate(credentials).await?;

        let session = Session {
            user_id: user.id.clone(),
            username: user.username.clone(),
            roles: user.roles.clone(),
            created_at: chrono::Utc::now().timestamp_millis(),
            expires_at: chrono::Utc::now().timestamp_millis() + 86400000,
        };

        let session_id = uuid::Uuid::new_v4().to_string();
        self.sessions.insert(session_id.clone(), session.clone());

        Ok(session)
    }

    /// Validate a session
    pub fn validate_session(&self, session_id: &str) -> Result<Session> {
        self.sessions
            .get(session_id)
            .map(|s| s.clone())
            .ok_or_else(|| Error::Auth(AuthError::InvalidToken("Session not found".to_string())))
    }

    /// Check authorization
    pub fn authorize(
        &self,
        session: &Session,
        resource: &str,
        action: &str,
    ) -> Result<()> {
        if !self.config.auth_enabled {
            return Ok(());
        }

        let permission = Permission {
            resource: resource.to_string(),
            action: action.to_string(),
        };

        if self.authorizer.check(&session.roles, &permission) {
            Ok(())
        } else {
            Err(Error::Auth(AuthError::PermissionDenied(format!(
                "Permission denied for {} on {}",
                action, resource
            ))))
        }
    }

    /// Invalidate a session
    pub fn invalidate_session(&self, session_id: &str) {
        self.sessions.remove(session_id);
    }
}

/// User credentials
#[derive(Debug, Clone)]
pub enum Credentials {
    UsernamePassword { username: String, password: String },
    Token(String),
    Certificate(Vec<u8>),
}

/// Authenticated user
#[derive(Debug, Clone)]
pub struct AuthenticatedUser {
    pub id: String,
    pub username: String,
    pub roles: Vec<String>,
}

/// No authentication (passthrough)
struct NoAuth;

#[async_trait::async_trait]
impl Authenticator for NoAuth {
    async fn authenticate(&self, _credentials: &Credentials) -> Result<AuthenticatedUser> {
        Ok(AuthenticatedUser {
            id: "anonymous".to_string(),
            username: "anonymous".to_string(),
            roles: vec!["admin".to_string()],
        })
    }
}
