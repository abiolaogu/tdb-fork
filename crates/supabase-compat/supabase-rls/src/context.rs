//! RLS Context for request-scoped security information

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// RLS evaluation context containing JWT claims and request info
#[derive(Debug, Clone, Default)]
pub struct RlsContext {
    /// User ID from JWT (auth.uid())
    pub user_id: Option<Uuid>,
    /// User role from JWT (auth.role())
    pub role: String,
    /// User email from JWT (auth.email())
    pub email: Option<String>,
    /// App metadata from JWT
    pub app_metadata: serde_json::Value,
    /// User metadata from JWT
    pub user_metadata: serde_json::Value,
    /// Custom claims
    pub claims: HashMap<String, serde_json::Value>,
    /// Whether the user is authenticated
    pub is_authenticated: bool,
    /// Whether this is a service role request (bypasses RLS)
    pub is_service_role: bool,
}

impl RlsContext {
    /// Create an anonymous context
    pub fn anonymous() -> Self {
        Self {
            role: "anon".to_string(),
            is_authenticated: false,
            ..Default::default()
        }
    }

    /// Create an authenticated context
    pub fn authenticated(user_id: Uuid, role: &str) -> Self {
        Self {
            user_id: Some(user_id),
            role: role.to_string(),
            is_authenticated: true,
            ..Default::default()
        }
    }

    /// Create a service role context (bypasses RLS)
    pub fn service_role() -> Self {
        Self {
            role: "service_role".to_string(),
            is_authenticated: true,
            is_service_role: true,
            ..Default::default()
        }
    }

    /// Build context from JWT claims
    pub fn from_jwt_claims(claims: &supabase_common::types::TokenClaims) -> Self {
        let user_id = Uuid::parse_str(&claims.sub).ok();

        Self {
            user_id,
            role: claims.role.clone(),
            email: claims.email.clone(),
            app_metadata: claims.app_metadata.clone(),
            user_metadata: claims.user_metadata.clone(),
            claims: HashMap::new(),
            is_authenticated: true,
            is_service_role: claims.role == "service_role",
        }
    }

    /// Get auth.uid() value for SQL substitution
    pub fn auth_uid(&self) -> Option<String> {
        self.user_id.map(|id| format!("'{}'", id))
    }

    /// Get auth.role() value for SQL substitution
    pub fn auth_role(&self) -> String {
        format!("'{}'", self.role)
    }

    /// Get auth.email() value for SQL substitution
    pub fn auth_email(&self) -> String {
        self.email
            .as_ref()
            .map(|e| format!("'{}'", e))
            .unwrap_or_else(|| "NULL".to_string())
    }

    /// Get a claim value for SQL substitution
    pub fn get_claim(&self, key: &str) -> String {
        self.claims
            .get(key)
            .map(|v| match v {
                serde_json::Value::String(s) => format!("'{}'", s),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                serde_json::Value::Null => "NULL".to_string(),
                _ => format!("'{}'", v),
            })
            .unwrap_or_else(|| "NULL".to_string())
    }

    /// Check if user should bypass RLS
    pub fn bypasses_rls(&self) -> bool {
        self.is_service_role
    }
}

/// SQL function substitutions for RLS expressions
#[derive(Debug, Clone)]
pub struct RlsFunctions {
    context: RlsContext,
}

impl RlsFunctions {
    pub fn new(context: RlsContext) -> Self {
        Self { context }
    }

    /// Substitute auth.* functions in SQL expression
    pub fn substitute(&self, sql: &str) -> String {
        let mut result = sql.to_string();

        // Substitute auth.uid()
        if let Some(uid) = self.context.auth_uid() {
            result = result.replace("auth.uid()", &uid);
        } else {
            result = result.replace("auth.uid()", "NULL");
        }

        // Substitute auth.role()
        result = result.replace("auth.role()", &self.context.auth_role());

        // Substitute auth.email()
        result = result.replace("auth.email()", &self.context.auth_email());

        // Substitute current_user (maps to role)
        result = result.replace("current_user", &self.context.auth_role());

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anonymous_context() {
        let ctx = RlsContext::anonymous();
        assert!(!ctx.is_authenticated);
        assert_eq!(ctx.role, "anon");
    }

    #[test]
    fn test_authenticated_context() {
        let user_id = Uuid::new_v4();
        let ctx = RlsContext::authenticated(user_id, "authenticated");

        assert!(ctx.is_authenticated);
        assert_eq!(ctx.user_id, Some(user_id));
    }

    #[test]
    fn test_service_role_bypass() {
        let ctx = RlsContext::service_role();
        assert!(ctx.bypasses_rls());
    }

    #[test]
    fn test_function_substitution() {
        let user_id = Uuid::new_v4();
        let ctx = RlsContext::authenticated(user_id, "authenticated");
        let funcs = RlsFunctions::new(ctx);

        let sql = "user_id = auth.uid() AND role = auth.role()";
        let result = funcs.substitute(sql);

        assert!(result.contains(&user_id.to_string()));
        assert!(result.contains("'authenticated'"));
    }
}
