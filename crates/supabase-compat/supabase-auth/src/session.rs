//! Session management

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use supabase_common::error::{Error, Result};
use supabase_common::types::{Session, User};

use crate::jwt::{JwtManager, RefreshToken};

/// Session store for managing user sessions
pub struct SessionStore {
    sessions: Arc<RwLock<HashMap<String, SessionData>>>,
    refresh_tokens: Arc<RwLock<HashMap<String, RefreshToken>>>,
    user_sessions: Arc<RwLock<HashMap<Uuid, Vec<String>>>>,
    jwt_manager: Arc<JwtManager>,
}

/// Internal session data
#[derive(Debug, Clone)]
pub struct SessionData {
    pub session_id: String,
    pub user_id: Uuid,
    pub access_token: String,
    pub refresh_token: String,
    pub created_at: DateTime<Utc>,
    pub last_refreshed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub user_agent: Option<String>,
    pub ip_address: Option<String>,
}

impl SessionStore {
    /// Create a new session store
    pub fn new(jwt_manager: Arc<JwtManager>) -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            refresh_tokens: Arc::new(RwLock::new(HashMap::new())),
            user_sessions: Arc::new(RwLock::new(HashMap::new())),
            jwt_manager,
        }
    }

    /// Create a new session for a user
    pub fn create_session(
        &self,
        user: &User,
        user_agent: Option<String>,
        ip_address: Option<String>,
    ) -> Result<Session> {
        let session_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        // Generate tokens
        let access_token = self.jwt_manager.generate_access_token(user, &session_id)?;
        let refresh_token = RefreshToken::new(user.id, &session_id);

        let expires_at = now + Duration::seconds(self.jwt_manager.access_token_expiry() as i64);

        // Create session data
        let session_data = SessionData {
            session_id: session_id.clone(),
            user_id: user.id,
            access_token: access_token.clone(),
            refresh_token: refresh_token.token.clone(),
            created_at: now,
            last_refreshed_at: now,
            expires_at,
            user_agent,
            ip_address,
        };

        // Store session
        self.sessions
            .write()
            .insert(session_id.clone(), session_data);
        self.refresh_tokens
            .write()
            .insert(refresh_token.token.clone(), refresh_token.clone());

        // Track user sessions
        self.user_sessions
            .write()
            .entry(user.id)
            .or_default()
            .push(session_id);

        // Create response
        Ok(Session {
            access_token,
            token_type: "bearer".to_string(),
            expires_in: self.jwt_manager.access_token_expiry(),
            expires_at: expires_at.timestamp() as u64,
            refresh_token: refresh_token.token,
            user: user.clone(),
        })
    }

    /// Refresh a session using refresh token
    pub fn refresh_session(&self, refresh_token_str: &str, user: &User) -> Result<Session> {
        // Get and validate refresh token
        let refresh_token = {
            let tokens = self.refresh_tokens.read();
            tokens
                .get(refresh_token_str)
                .cloned()
                .ok_or(Error::RefreshTokenInvalid)?
        };

        if refresh_token.revoked {
            return Err(Error::RefreshTokenInvalid);
        }

        if refresh_token.user_id != user.id {
            return Err(Error::RefreshTokenInvalid);
        }

        // Get existing session
        let session_data = {
            let sessions = self.sessions.read();
            sessions
                .get(&refresh_token.session_id)
                .cloned()
                .ok_or(Error::RefreshTokenInvalid)?
        };

        let now = Utc::now();

        // Rotate refresh token
        let new_refresh_token = refresh_token.rotate();

        // Revoke old refresh token
        {
            let mut tokens = self.refresh_tokens.write();
            if let Some(old_token) = tokens.get_mut(refresh_token_str) {
                old_token.revoked = true;
            }
            tokens.insert(new_refresh_token.token.clone(), new_refresh_token.clone());
        }

        // Generate new access token
        let access_token = self
            .jwt_manager
            .generate_access_token(user, &refresh_token.session_id)?;

        let expires_at = now + Duration::seconds(self.jwt_manager.access_token_expiry() as i64);

        // Update session
        {
            let mut sessions = self.sessions.write();
            if let Some(session) = sessions.get_mut(&refresh_token.session_id) {
                session.access_token = access_token.clone();
                session.refresh_token = new_refresh_token.token.clone();
                session.last_refreshed_at = now;
                session.expires_at = expires_at;
            }
        }

        Ok(Session {
            access_token,
            token_type: "bearer".to_string(),
            expires_in: self.jwt_manager.access_token_expiry(),
            expires_at: expires_at.timestamp() as u64,
            refresh_token: new_refresh_token.token,
            user: user.clone(),
        })
    }

    /// Get session by session ID
    pub fn get_session(&self, session_id: &str) -> Option<SessionData> {
        self.sessions.read().get(session_id).cloned()
    }

    /// Revoke a session (logout)
    pub fn revoke_session(&self, session_id: &str) -> Result<()> {
        // Remove session
        let session_data = self
            .sessions
            .write()
            .remove(session_id)
            .ok_or(Error::ResourceNotFound)?;

        // Revoke refresh token
        {
            let mut tokens = self.refresh_tokens.write();
            if let Some(token) = tokens.get_mut(&session_data.refresh_token) {
                token.revoked = true;
            }
        }

        // Remove from user sessions
        {
            let mut user_sessions = self.user_sessions.write();
            if let Some(sessions) = user_sessions.get_mut(&session_data.user_id) {
                sessions.retain(|s| s != session_id);
            }
        }

        Ok(())
    }

    /// Revoke all sessions for a user (logout all devices)
    pub fn revoke_all_sessions(&self, user_id: &Uuid) -> Result<()> {
        // Get all session IDs for user
        let session_ids: Vec<String> = {
            let user_sessions = self.user_sessions.read();
            user_sessions.get(user_id).cloned().unwrap_or_default()
        };

        // Revoke each session
        for session_id in session_ids {
            let _ = self.revoke_session(&session_id);
        }

        Ok(())
    }

    /// List all sessions for a user
    pub fn list_user_sessions(&self, user_id: &Uuid) -> Vec<SessionData> {
        let session_ids: Vec<String> = {
            let user_sessions = self.user_sessions.read();
            user_sessions.get(user_id).cloned().unwrap_or_default()
        };

        let sessions = self.sessions.read();
        session_ids
            .iter()
            .filter_map(|id| sessions.get(id).cloned())
            .collect()
    }

    /// Clean up expired sessions
    pub fn cleanup_expired(&self) {
        let now = Utc::now();
        let refresh_expiry = Duration::seconds(self.jwt_manager.refresh_token_expiry() as i64);

        // Find expired sessions
        let expired: Vec<String> = {
            let sessions = self.sessions.read();
            sessions
                .iter()
                .filter(|(_, s)| now > s.created_at + refresh_expiry)
                .map(|(id, _)| id.clone())
                .collect()
        };

        // Remove expired sessions
        for session_id in expired {
            let _ = self.revoke_session(&session_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> SessionStore {
        let jwt_manager = Arc::new(JwtManager::new(
            "test-secret",
            "http://localhost",
            3600,
            604800,
        ));
        SessionStore::new(jwt_manager)
    }

    fn create_test_user() -> User {
        User::new(Some("test@example.com".to_string()), None)
    }

    #[test]
    fn test_create_session() {
        let store = create_store();
        let user = create_test_user();

        let session = store.create_session(&user, None, None).unwrap();
        assert!(!session.access_token.is_empty());
        assert!(!session.refresh_token.is_empty());
    }

    #[test]
    fn test_refresh_session() {
        let store = create_store();
        let user = create_test_user();

        let session = store.create_session(&user, None, None).unwrap();
        let new_session = store
            .refresh_session(&session.refresh_token, &user)
            .unwrap();

        // Refresh token must always be different (rotated)
        assert_ne!(session.refresh_token, new_session.refresh_token);
        // New access token should be valid
        assert!(!new_session.access_token.is_empty());
    }

    #[test]
    fn test_revoke_session() {
        let store = create_store();
        let user = create_test_user();

        let session = store.create_session(&user, None, None).unwrap();

        // Get session_id from claims
        let claims = store
            .jwt_manager
            .validate_access_token(&session.access_token)
            .unwrap();
        let session_id = claims.session_id.unwrap();

        store.revoke_session(&session_id).unwrap();

        // Session should be gone
        assert!(store.get_session(&session_id).is_none());
    }
}
