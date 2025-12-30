//! Email-based authentication (password and magic link)

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use supabase_common::error::{Error, Result};

/// Magic link token for passwordless authentication
#[derive(Debug, Clone)]
pub struct MagicLinkToken {
    pub token: String,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub used: bool,
}

impl MagicLinkToken {
    pub fn new(email: &str, expiry_minutes: i64) -> Self {
        let now = Utc::now();
        Self {
            token: generate_secure_token(),
            email: email.to_string(),
            created_at: now,
            expires_at: now + Duration::minutes(expiry_minutes),
            used: false,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.used && Utc::now() < self.expires_at
    }
}

/// Password reset token
#[derive(Debug, Clone)]
pub struct PasswordResetToken {
    pub token: String,
    pub user_id: Uuid,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub used: bool,
}

impl PasswordResetToken {
    pub fn new(user_id: Uuid, email: &str, expiry_minutes: i64) -> Self {
        let now = Utc::now();
        Self {
            token: generate_secure_token(),
            user_id,
            email: email.to_string(),
            created_at: now,
            expires_at: now + Duration::minutes(expiry_minutes),
            used: false,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.used && Utc::now() < self.expires_at
    }
}

/// Email confirmation token
#[derive(Debug, Clone)]
pub struct EmailConfirmToken {
    pub token: String,
    pub user_id: Uuid,
    pub email: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub confirmed: bool,
}

impl EmailConfirmToken {
    pub fn new(user_id: Uuid, email: &str, expiry_hours: i64) -> Self {
        let now = Utc::now();
        Self {
            token: generate_secure_token(),
            user_id,
            email: email.to_string(),
            created_at: now,
            expires_at: now + Duration::hours(expiry_hours),
            confirmed: false,
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.confirmed && Utc::now() < self.expires_at
    }
}

/// OTP (One-Time Password) for phone/email verification
#[derive(Debug, Clone)]
pub struct OtpToken {
    pub code: String,
    pub identifier: String, // email or phone
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub attempts: u32,
    pub max_attempts: u32,
    pub verified: bool,
}

impl OtpToken {
    pub fn new(identifier: &str, expiry_minutes: i64) -> Self {
        let now = Utc::now();
        Self {
            code: generate_otp_code(),
            identifier: identifier.to_string(),
            created_at: now,
            expires_at: now + Duration::minutes(expiry_minutes),
            attempts: 0,
            max_attempts: 3,
            verified: false,
        }
    }

    pub fn verify(&mut self, code: &str) -> bool {
        if self.verified {
            return false;
        }

        self.attempts += 1;

        if Utc::now() > self.expires_at {
            return false;
        }

        if self.attempts > self.max_attempts {
            return false;
        }

        if self.code == code {
            self.verified = true;
            true
        } else {
            false
        }
    }
}

/// Email provider store for managing tokens
pub struct EmailTokenStore {
    magic_links: Arc<RwLock<HashMap<String, MagicLinkToken>>>,
    password_resets: Arc<RwLock<HashMap<String, PasswordResetToken>>>,
    email_confirms: Arc<RwLock<HashMap<String, EmailConfirmToken>>>,
    otps: Arc<RwLock<HashMap<String, OtpToken>>>,
    magic_link_expiry_minutes: i64,
    password_reset_expiry_minutes: i64,
    email_confirm_expiry_hours: i64,
    otp_expiry_minutes: i64,
}

impl EmailTokenStore {
    pub fn new() -> Self {
        Self {
            magic_links: Arc::new(RwLock::new(HashMap::new())),
            password_resets: Arc::new(RwLock::new(HashMap::new())),
            email_confirms: Arc::new(RwLock::new(HashMap::new())),
            otps: Arc::new(RwLock::new(HashMap::new())),
            magic_link_expiry_minutes: 60,
            password_reset_expiry_minutes: 60,
            email_confirm_expiry_hours: 24,
            otp_expiry_minutes: 10,
        }
    }

    /// Create a magic link for passwordless login
    pub fn create_magic_link(&self, email: &str) -> MagicLinkToken {
        let token = MagicLinkToken::new(email, self.magic_link_expiry_minutes);
        self.magic_links
            .write()
            .insert(token.token.clone(), token.clone());
        token
    }

    /// Verify a magic link token
    pub fn verify_magic_link(&self, token: &str) -> Result<String> {
        let mut links = self.magic_links.write();
        let magic_link = links.get_mut(token).ok_or(Error::InvalidToken)?;

        if !magic_link.is_valid() {
            return Err(Error::TokenExpired);
        }

        magic_link.used = true;
        Ok(magic_link.email.clone())
    }

    /// Create a password reset token
    pub fn create_password_reset(&self, user_id: Uuid, email: &str) -> PasswordResetToken {
        let token = PasswordResetToken::new(user_id, email, self.password_reset_expiry_minutes);
        self.password_resets
            .write()
            .insert(token.token.clone(), token.clone());
        token
    }

    /// Verify a password reset token
    pub fn verify_password_reset(&self, token: &str) -> Result<(Uuid, String)> {
        let mut resets = self.password_resets.write();
        let reset_token = resets.get_mut(token).ok_or(Error::InvalidToken)?;

        if !reset_token.is_valid() {
            return Err(Error::TokenExpired);
        }

        reset_token.used = true;
        Ok((reset_token.user_id, reset_token.email.clone()))
    }

    /// Create an email confirmation token
    pub fn create_email_confirm(&self, user_id: Uuid, email: &str) -> EmailConfirmToken {
        let token = EmailConfirmToken::new(user_id, email, self.email_confirm_expiry_hours);
        self.email_confirms
            .write()
            .insert(token.token.clone(), token.clone());
        token
    }

    /// Verify an email confirmation token
    pub fn verify_email_confirm(&self, token: &str) -> Result<(Uuid, String)> {
        let mut confirms = self.email_confirms.write();
        let confirm_token = confirms.get_mut(token).ok_or(Error::InvalidToken)?;

        if !confirm_token.is_valid() {
            return Err(Error::TokenExpired);
        }

        confirm_token.confirmed = true;
        Ok((confirm_token.user_id, confirm_token.email.clone()))
    }

    /// Create an OTP for verification
    pub fn create_otp(&self, identifier: &str) -> OtpToken {
        let token = OtpToken::new(identifier, self.otp_expiry_minutes);
        self.otps
            .write()
            .insert(identifier.to_string(), token.clone());
        token
    }

    /// Verify an OTP
    pub fn verify_otp(&self, identifier: &str, code: &str) -> Result<()> {
        let mut otps = self.otps.write();
        let otp = otps.get_mut(identifier).ok_or(Error::InvalidToken)?;

        if otp.verify(code) {
            Ok(())
        } else if otp.attempts > otp.max_attempts {
            Err(Error::RateLimitExceeded)
        } else {
            Err(Error::InvalidToken)
        }
    }

    /// Clean up expired tokens
    pub fn cleanup_expired(&self) {
        let now = Utc::now();

        self.magic_links.write().retain(|_, t| t.expires_at > now);
        self.password_resets
            .write()
            .retain(|_, t| t.expires_at > now);
        self.email_confirms
            .write()
            .retain(|_, t| t.expires_at > now);
        self.otps.write().retain(|_, t| t.expires_at > now);
    }
}

impl Default for EmailTokenStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a secure random token
fn generate_secure_token() -> String {
    use base64::Engine;
    let mut rng = rand::thread_rng();
    let bytes: [u8; 32] = rng.gen();
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
}

/// Generate a 6-digit OTP code
fn generate_otp_code() -> String {
    let mut rng = rand::thread_rng();
    format!("{:06}", rng.gen_range(0..1_000_000))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_magic_link() {
        let store = EmailTokenStore::new();
        let token = store.create_magic_link("test@example.com");

        let email = store.verify_magic_link(&token.token).unwrap();
        assert_eq!(email, "test@example.com");

        // Cannot reuse
        let result = store.verify_magic_link(&token.token);
        assert!(result.is_err());
    }

    #[test]
    fn test_otp() {
        let store = EmailTokenStore::new();
        let otp = store.create_otp("test@example.com");

        let result = store.verify_otp("test@example.com", &otp.code);
        assert!(result.is_ok());

        // Cannot reuse
        let result = store.verify_otp("test@example.com", &otp.code);
        assert!(result.is_err());
    }

    #[test]
    fn test_otp_wrong_code() {
        let store = EmailTokenStore::new();
        store.create_otp("test@example.com");

        let result = store.verify_otp("test@example.com", "000000");
        assert!(result.is_err());
    }
}
