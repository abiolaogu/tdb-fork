//! User management operations

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use chrono::Utc;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use supabase_common::config::PasswordRequirements;
use supabase_common::error::{Error, Result};
use supabase_common::types::User;

/// User store for managing user data
/// In production, this would be backed by the database
pub struct UserStore {
    users: Arc<RwLock<HashMap<Uuid, StoredUser>>>,
    email_index: Arc<RwLock<HashMap<String, Uuid>>>,
    phone_index: Arc<RwLock<HashMap<String, Uuid>>>,
    password_requirements: PasswordRequirements,
}

/// Internal user representation with password hash
#[derive(Debug, Clone)]
pub struct StoredUser {
    pub user: User,
    pub encrypted_password: Option<String>,
}

impl UserStore {
    /// Create a new user store
    pub fn new(password_requirements: PasswordRequirements) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            email_index: Arc::new(RwLock::new(HashMap::new())),
            phone_index: Arc::new(RwLock::new(HashMap::new())),
            password_requirements,
        }
    }

    /// Create a new user with email and password
    pub fn create_user_with_password(
        &self,
        email: &str,
        password: &str,
        metadata: Option<serde_json::Value>,
    ) -> Result<User> {
        // Validate password
        self.validate_password(password)?;

        // Check if user already exists
        if self.email_index.read().contains_key(email) {
            return Err(Error::UserAlreadyExists);
        }

        // Hash password
        let password_hash = self.hash_password(password)?;

        // Create user
        let mut user = User::new(Some(email.to_string()), None);
        if let Some(meta) = metadata {
            user.user_metadata = meta;
        }

        let stored_user = StoredUser {
            user: user.clone(),
            encrypted_password: Some(password_hash),
        };

        // Store user
        self.users.write().insert(user.id, stored_user);
        self.email_index.write().insert(email.to_string(), user.id);

        Ok(user)
    }

    /// Create a user with phone number
    pub fn create_user_with_phone(
        &self,
        phone: &str,
        password: Option<&str>,
        metadata: Option<serde_json::Value>,
    ) -> Result<User> {
        // Check if user already exists
        if self.phone_index.read().contains_key(phone) {
            return Err(Error::UserAlreadyExists);
        }

        let password_hash = if let Some(pwd) = password {
            self.validate_password(pwd)?;
            Some(self.hash_password(pwd)?)
        } else {
            None
        };

        let mut user = User::new(None, Some(phone.to_string()));
        if let Some(meta) = metadata {
            user.user_metadata = meta;
        }

        let stored_user = StoredUser {
            user: user.clone(),
            encrypted_password: password_hash,
        };

        self.users.write().insert(user.id, stored_user);
        self.phone_index.write().insert(phone.to_string(), user.id);

        Ok(user)
    }

    /// Get user by ID
    pub fn get_user(&self, id: &Uuid) -> Result<User> {
        self.users
            .read()
            .get(id)
            .map(|su| su.user.clone())
            .ok_or(Error::UserNotFound)
    }

    /// Get user by email
    pub fn get_user_by_email(&self, email: &str) -> Result<User> {
        let id = self
            .email_index
            .read()
            .get(email)
            .copied()
            .ok_or(Error::UserNotFound)?;
        self.get_user(&id)
    }

    /// Get user by phone
    pub fn get_user_by_phone(&self, phone: &str) -> Result<User> {
        let id = self
            .phone_index
            .read()
            .get(phone)
            .copied()
            .ok_or(Error::UserNotFound)?;
        self.get_user(&id)
    }

    /// Verify user credentials
    pub fn verify_credentials(&self, email: &str, password: &str) -> Result<User> {
        let id = self
            .email_index
            .read()
            .get(email)
            .copied()
            .ok_or(Error::InvalidCredentials)?;

        let stored_user = self
            .users
            .read()
            .get(&id)
            .cloned()
            .ok_or(Error::InvalidCredentials)?;

        let password_hash = stored_user
            .encrypted_password
            .as_ref()
            .ok_or(Error::InvalidCredentials)?;

        self.verify_password(password, password_hash)?;

        // Update last sign in
        let mut users = self.users.write();
        if let Some(user) = users.get_mut(&id) {
            user.user.last_sign_in_at = Some(Utc::now());
        }

        Ok(stored_user.user)
    }

    /// Update user metadata
    pub fn update_user(
        &self,
        id: &Uuid,
        email: Option<String>,
        phone: Option<String>,
        user_metadata: Option<serde_json::Value>,
        app_metadata: Option<serde_json::Value>,
    ) -> Result<User> {
        let mut users = self.users.write();
        let stored_user = users.get_mut(id).ok_or(Error::UserNotFound)?;

        // Update email if changed
        if let Some(new_email) = email {
            if stored_user.user.email.as_ref() != Some(&new_email) {
                // Remove old email index
                if let Some(old_email) = &stored_user.user.email {
                    self.email_index.write().remove(old_email);
                }
                // Add new email index
                self.email_index.write().insert(new_email.clone(), *id);
                stored_user.user.email = Some(new_email);
                stored_user.user.email_confirmed_at = None;
            }
        }

        // Update phone if changed
        if let Some(new_phone) = phone {
            if stored_user.user.phone.as_ref() != Some(&new_phone) {
                // Remove old phone index
                if let Some(old_phone) = &stored_user.user.phone {
                    self.phone_index.write().remove(old_phone);
                }
                // Add new phone index
                self.phone_index.write().insert(new_phone.clone(), *id);
                stored_user.user.phone = Some(new_phone);
                stored_user.user.phone_confirmed_at = None;
            }
        }

        if let Some(meta) = user_metadata {
            stored_user.user.user_metadata = meta;
        }

        if let Some(meta) = app_metadata {
            stored_user.user.app_metadata = meta;
        }

        stored_user.user.updated_at = Utc::now();

        Ok(stored_user.user.clone())
    }

    /// Delete user
    pub fn delete_user(&self, id: &Uuid) -> Result<()> {
        let stored_user = self.users.write().remove(id).ok_or(Error::UserNotFound)?;

        // Remove indices
        if let Some(email) = &stored_user.user.email {
            self.email_index.write().remove(email);
        }
        if let Some(phone) = &stored_user.user.phone {
            self.phone_index.write().remove(phone);
        }

        Ok(())
    }

    /// Confirm user email
    pub fn confirm_email(&self, id: &Uuid) -> Result<User> {
        let mut users = self.users.write();
        let stored_user = users.get_mut(id).ok_or(Error::UserNotFound)?;

        stored_user.user.email_confirmed_at = Some(Utc::now());
        stored_user.user.confirmed_at = Some(Utc::now());
        stored_user.user.updated_at = Utc::now();

        Ok(stored_user.user.clone())
    }

    /// Update user password
    pub fn update_password(&self, id: &Uuid, new_password: &str) -> Result<()> {
        self.validate_password(new_password)?;
        let password_hash = self.hash_password(new_password)?;

        let mut users = self.users.write();
        let stored_user = users.get_mut(id).ok_or(Error::UserNotFound)?;

        stored_user.encrypted_password = Some(password_hash);
        stored_user.user.updated_at = Utc::now();

        Ok(())
    }

    // Private helper methods

    fn hash_password(&self, password: &str) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();

        argon2
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|e| Error::InternalError(format!("Password hashing failed: {}", e)))
    }

    fn verify_password(&self, password: &str, hash: &str) -> Result<()> {
        let parsed_hash = PasswordHash::new(hash)
            .map_err(|_| Error::InternalError("Invalid password hash".to_string()))?;

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .map_err(|_| Error::InvalidCredentials)
    }

    fn validate_password(&self, password: &str) -> Result<()> {
        let req = &self.password_requirements;
        let mut errors = Vec::new();

        if password.len() < req.min_length {
            errors.push(format!("minimum {} characters", req.min_length));
        }

        if req.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            errors.push("at least one uppercase letter".to_string());
        }

        if req.require_lowercase && !password.chars().any(|c| c.is_lowercase()) {
            errors.push("at least one lowercase letter".to_string());
        }

        if req.require_numbers && !password.chars().any(|c| c.is_numeric()) {
            errors.push("at least one number".to_string());
        }

        if req.require_special && !password.chars().any(|c| !c.is_alphanumeric()) {
            errors.push("at least one special character".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(Error::WeakPassword(format!(
                "Password must have {}",
                errors.join(", ")
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_store() -> UserStore {
        UserStore::new(PasswordRequirements::default())
    }

    #[test]
    fn test_create_user() {
        let store = create_store();
        let user = store
            .create_user_with_password("test@example.com", "password123", None)
            .unwrap();

        assert_eq!(user.email, Some("test@example.com".to_string()));
    }

    #[test]
    fn test_duplicate_user() {
        let store = create_store();
        store
            .create_user_with_password("test@example.com", "password123", None)
            .unwrap();

        let result = store.create_user_with_password("test@example.com", "password456", None);
        assert!(matches!(result, Err(Error::UserAlreadyExists)));
    }

    #[test]
    fn test_verify_credentials() {
        let store = create_store();
        store
            .create_user_with_password("test@example.com", "password123", None)
            .unwrap();

        let user = store
            .verify_credentials("test@example.com", "password123")
            .unwrap();
        assert_eq!(user.email, Some("test@example.com".to_string()));
    }

    #[test]
    fn test_invalid_credentials() {
        let store = create_store();
        store
            .create_user_with_password("test@example.com", "password123", None)
            .unwrap();

        let result = store.verify_credentials("test@example.com", "wrongpassword");
        assert!(matches!(result, Err(Error::InvalidCredentials)));
    }

    #[test]
    fn test_password_requirements() {
        let store = UserStore::new(PasswordRequirements {
            min_length: 10,
            require_uppercase: true,
            require_numbers: true,
            ..Default::default()
        });

        // Too short
        let result = store.create_user_with_password("test@example.com", "short", None);
        assert!(matches!(result, Err(Error::WeakPassword(_))));

        // No uppercase
        let result = store.create_user_with_password("test@example.com", "longpassword1", None);
        assert!(matches!(result, Err(Error::WeakPassword(_))));

        // Valid
        let result = store.create_user_with_password("test@example.com", "LongPassword1", None);
        assert!(result.is_ok());
    }
}
