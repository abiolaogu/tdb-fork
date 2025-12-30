//! Auth middleware for protecting routes

use actix_web::{dev::ServiceRequest, Error, HttpMessage};
use std::sync::Arc;

use supabase_common::types::TokenClaims;

use crate::jwt::JwtManager;

/// Authenticated user context attached to requests
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub user_id: uuid::Uuid,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub role: String,
    pub app_metadata: serde_json::Value,
    pub user_metadata: serde_json::Value,
    pub session_id: Option<String>,
}

impl From<TokenClaims> for AuthContext {
    fn from(claims: TokenClaims) -> Self {
        Self {
            user_id: uuid::Uuid::parse_str(&claims.sub).unwrap_or_default(),
            email: claims.email,
            phone: claims.phone,
            role: claims.role,
            app_metadata: claims.app_metadata,
            user_metadata: claims.user_metadata,
            session_id: claims.session_id,
        }
    }
}

/// Validate JWT and extract auth context
pub fn validate_request(
    req: &ServiceRequest,
    jwt_manager: &Arc<JwtManager>,
) -> Result<AuthContext, supabase_common::error::Error> {
    let token = extract_token(req)?;
    let claims = jwt_manager.validate_access_token(&token)?;
    Ok(AuthContext::from(claims))
}

/// Extract bearer token from request
fn extract_token(req: &ServiceRequest) -> Result<String, supabase_common::error::Error> {
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or(supabase_common::error::Error::Unauthorized)?;

    auth_header
        .strip_prefix("Bearer ")
        .map(|s| s.to_string())
        .ok_or(supabase_common::error::Error::Unauthorized)
}

/// Check if user has required role
pub fn require_role(
    context: &AuthContext,
    required_role: &str,
) -> Result<(), supabase_common::error::Error> {
    if context.role == required_role || context.role == "service_role" {
        Ok(())
    } else {
        Err(supabase_common::error::Error::Forbidden)
    }
}

/// Get auth context from request extensions
pub fn get_auth_context(req: &actix_web::HttpRequest) -> Option<AuthContext> {
    req.extensions().get::<AuthContext>().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_context_from_claims() {
        let claims = TokenClaims {
            aud: "authenticated".to_string(),
            exp: 0,
            iat: 0,
            iss: "test".to_string(),
            sub: uuid::Uuid::new_v4().to_string(),
            email: Some("test@example.com".to_string()),
            phone: None,
            app_metadata: serde_json::json!({}),
            user_metadata: serde_json::json!({"name": "Test"}),
            role: "authenticated".to_string(),
            amr: None,
            session_id: Some("session-123".to_string()),
        };

        let context = AuthContext::from(claims);
        assert_eq!(context.email, Some("test@example.com".to_string()));
        assert_eq!(context.role, "authenticated");
    }

    #[test]
    fn test_require_role() {
        let context = AuthContext {
            user_id: uuid::Uuid::new_v4(),
            email: None,
            phone: None,
            role: "authenticated".to_string(),
            app_metadata: serde_json::json!({}),
            user_metadata: serde_json::json!({}),
            session_id: None,
        };

        assert!(require_role(&context, "authenticated").is_ok());
        assert!(require_role(&context, "admin").is_err());
    }

    #[test]
    fn test_service_role_bypass() {
        let context = AuthContext {
            user_id: uuid::Uuid::new_v4(),
            email: None,
            phone: None,
            role: "service_role".to_string(),
            app_metadata: serde_json::json!({}),
            user_metadata: serde_json::json!({}),
            session_id: None,
        };

        // service_role should pass any role check
        assert!(require_role(&context, "authenticated").is_ok());
        assert!(require_role(&context, "admin").is_ok());
    }
}
