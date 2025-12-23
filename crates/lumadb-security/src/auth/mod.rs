//! Authentication implementations

use async_trait::async_trait;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

use lumadb_common::error::{Result, Error, AuthError};

use crate::manager::{AuthenticatedUser, Credentials};

pub mod jwt;
pub mod sasl;
pub mod mtls;

pub use jwt::JwtAuth;
pub use sasl::SaslAuth;

/// Authenticator trait
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// Authenticate credentials and return user info
    async fn authenticate(&self, credentials: &Credentials) -> Result<AuthenticatedUser>;
}
