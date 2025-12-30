//! RLS-integrated REST handlers
//!
//! These handlers integrate Row Level Security with query execution,
//! filtering results based on user JWT claims and table policies.

use actix_web::{web, HttpRequest, HttpResponse};
use std::sync::Arc;

use supabase_common::types::ApiError;

use crate::backend::{QueryBackend, QueryContext, QueryResult};
use crate::query::ParsedQuery;
use crate::schema::SchemaCache;

/// RLS-integrated REST state
pub struct RlsRestState {
    pub schema_cache: Arc<SchemaCache>,
    pub backend: Arc<dyn QueryBackend>,
    pub policy_store: Arc<supabase_rls::policy::PolicyStore>,
    pub max_rows: usize,
}

impl RlsRestState {
    /// Create new state with all components
    pub fn new(
        schema_cache: Arc<SchemaCache>,
        backend: Arc<dyn QueryBackend>,
        max_rows: usize,
    ) -> Self {
        Self {
            schema_cache,
            backend,
            policy_store: Arc::new(supabase_rls::policy::PolicyStore::new()),
            max_rows,
        }
    }

    /// Build query context from request
    pub fn build_context(&self, req: &HttpRequest, schema: &str) -> QueryContext {
        // Extract JWT from Authorization header
        let auth_header = req
            .headers()
            .get("Authorization")
            .and_then(|v| v.to_str().ok());

        let mut context = QueryContext::default();
        context.schema = schema.to_string();

        if let Some(header) = auth_header {
            if let Some(token) = header.strip_prefix("Bearer ") {
                // Parse JWT claims (simplified - production would validate)
                if let Ok((_, claims)) = decode_jwt_claims(token) {
                    context.user_id = Some(claims.sub);
                    context.bypass_rls = claims.role == "service_role";
                    context.role = claims.role;
                }
            }
        }

        context
    }

    /// Get RLS evaluator
    pub fn rls_evaluator(&self) -> supabase_rls::PolicyEvaluator {
        supabase_rls::PolicyEvaluator::new(self.policy_store.clone())
    }
}

/// Simplified JWT claims decoder (production would use full validation)
fn decode_jwt_claims(token: &str) -> Result<((), JwtClaims), ()> {
    // Split token and decode payload
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(());
    }

    use base64::Engine;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|_| ())?;

    let claims: JwtClaims = serde_json::from_slice(&payload).map_err(|_| ())?;
    Ok(((), claims))
}

#[derive(serde::Deserialize)]
struct JwtClaims {
    sub: String,
    #[serde(default = "default_role")]
    role: String,
}

fn default_role() -> String {
    "authenticated".to_string()
}

// ============================================================================
// RLS-Integrated Handlers
// ============================================================================

/// GET /{table} - Read rows with RLS enforcement
pub async fn select_with_rls(
    state: web::Data<Arc<RlsRestState>>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();
    let schema = "public";

    // Check if table exists
    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    // Build context from request
    let context = state.build_context(&req, schema);

    // Parse query string
    let query_string = req.query_string();
    let query = match ParsedQuery::parse(query_string) {
        Ok(q) => q,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiError::new("invalid_query", e.to_string()));
        }
    };

    // Get RLS condition
    let evaluator = state.rls_evaluator();
    let rls_context = supabase_rls::RlsContext {
        user_id: context
            .user_id
            .as_ref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok()),
        role: context.role.clone(),
        email: None,
        app_metadata: serde_json::Value::Null,
        user_metadata: serde_json::Value::Null,
        claims: std::collections::HashMap::new(),
        is_authenticated: context.user_id.is_some(),
        is_service_role: context.bypass_rls,
    };

    // Build filter string including RLS
    let mut filters = query.to_sql_where().unwrap_or_default();
    if let Some(rls_condition) =
        evaluator.get_using_expression(schema, &table, "SELECT", &rls_context)
    {
        if filters.is_empty() {
            filters = rls_condition;
        } else {
            filters = format!("({}) AND ({})", filters, rls_condition);
        }
    }

    // Execute query
    let columns = query
        .select
        .as_ref()
        .map(|s| {
            s.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_else(|| "*".to_string());

    let order = query.to_sql_order();

    let result = state
        .backend
        .select(
            &table,
            &columns,
            &filters,
            order.as_deref(),
            query.limit.or(Some(state.max_rows)),
            query.offset,
            &context,
        )
        .await;

    match result {
        Ok(result) => {
            let count = result.rows.len();
            HttpResponse::Ok()
                .insert_header((
                    "Content-Range",
                    format!("0-{}/{}", count.saturating_sub(1), count),
                ))
                .json(result.rows)
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(ApiError::new("query_error", e.to_string()))
        }
    }
}

/// POST /{table} - Insert rows with RLS enforcement
pub async fn insert_with_rls(
    state: web::Data<Arc<RlsRestState>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();
    let schema = "public";

    // Check if table exists
    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    let context = state.build_context(&req, schema);
    let evaluator = state.rls_evaluator();

    // Check INSERT RLS policy
    let rls_context = supabase_rls::RlsContext {
        user_id: context
            .user_id
            .as_ref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok()),
        role: context.role.clone(),
        email: None,
        app_metadata: serde_json::Value::Null,
        user_metadata: serde_json::Value::Null,
        claims: std::collections::HashMap::new(),
        is_authenticated: context.user_id.is_some(),
        is_service_role: context.bypass_rls,
    };

    // Check if RLS allows insert
    if evaluator.is_rls_enabled(schema, &table) && !context.bypass_rls {
        if let Some(check_expr) =
            evaluator.get_check_expression(schema, &table, "INSERT", &rls_context)
        {
            if check_expr == "false" {
                return HttpResponse::Forbidden().json(ApiError::new(
                    "rls_violation",
                    "Row Level Security policy violation",
                ));
            }
        }
    }

    let prefer = req
        .headers()
        .get("Prefer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let returning = prefer.contains("return=representation");

    // Handle array or single object
    let rows: Vec<serde_json::Value> = if body.is_array() {
        body.as_array().cloned().unwrap_or_default()
    } else {
        vec![body.into_inner()]
    };

    let result = state
        .backend
        .insert(&table, rows, returning, &context)
        .await;

    match result {
        Ok(result) => {
            if returning {
                if result.rows.len() == 1 {
                    HttpResponse::Created().json(&result.rows[0])
                } else {
                    HttpResponse::Created().json(result.rows)
                }
            } else {
                HttpResponse::Created().finish()
            }
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(ApiError::new("insert_error", e.to_string()))
        }
    }
}

/// PATCH /{table} - Update rows with RLS enforcement
pub async fn update_with_rls(
    state: web::Data<Arc<RlsRestState>>,
    path: web::Path<String>,
    body: web::Json<serde_json::Value>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();
    let schema = "public";

    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    let context = state.build_context(&req, schema);
    let evaluator = state.rls_evaluator();

    let rls_context = supabase_rls::RlsContext {
        user_id: context
            .user_id
            .as_ref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok()),
        role: context.role.clone(),
        email: None,
        app_metadata: serde_json::Value::Null,
        user_metadata: serde_json::Value::Null,
        claims: std::collections::HashMap::new(),
        is_authenticated: context.user_id.is_some(),
        is_service_role: context.bypass_rls,
    };

    // Parse query string for filters
    let query_string = req.query_string();
    let query = match ParsedQuery::parse(query_string) {
        Ok(q) => q,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiError::new("invalid_query", e.to_string()));
        }
    };

    // Build filter string with RLS
    let mut filters = query.to_sql_where().unwrap_or_default();
    if let Some(rls_condition) =
        evaluator.get_using_expression(schema, &table, "UPDATE", &rls_context)
    {
        if filters.is_empty() {
            filters = rls_condition;
        } else {
            filters = format!("({}) AND ({})", filters, rls_condition);
        }
    }

    let prefer = req
        .headers()
        .get("Prefer")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let returning = prefer.contains("return=representation");

    let result = state
        .backend
        .update(&table, body.into_inner(), &filters, returning, &context)
        .await;

    match result {
        Ok(result) => {
            if returning {
                HttpResponse::Ok().json(result.rows)
            } else {
                HttpResponse::NoContent().finish()
            }
        }
        Err(e) => {
            HttpResponse::InternalServerError().json(ApiError::new("update_error", e.to_string()))
        }
    }
}

/// DELETE /{table} - Delete rows with RLS enforcement
pub async fn delete_with_rls(
    state: web::Data<Arc<RlsRestState>>,
    path: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let table = path.into_inner();
    let schema = "public";

    if !state.schema_cache.has_table(&table) {
        return HttpResponse::NotFound().json(ApiError::new(
            "table_not_found",
            format!("Table '{}' not found", table),
        ));
    }

    let context = state.build_context(&req, schema);
    let evaluator = state.rls_evaluator();

    let rls_context = supabase_rls::RlsContext {
        user_id: context
            .user_id
            .as_ref()
            .and_then(|s| uuid::Uuid::parse_str(s).ok()),
        role: context.role.clone(),
        email: None,
        app_metadata: serde_json::Value::Null,
        user_metadata: serde_json::Value::Null,
        claims: std::collections::HashMap::new(),
        is_authenticated: context.user_id.is_some(),
        is_service_role: context.bypass_rls,
    };

    // Parse query string for filters
    let query_string = req.query_string();
    let query = match ParsedQuery::parse(query_string) {
        Ok(q) => q,
        Err(e) => {
            return HttpResponse::BadRequest().json(ApiError::new("invalid_query", e.to_string()));
        }
    };

    // Build filter string with RLS
    let mut filters = query.to_sql_where().unwrap_or_default();
    if let Some(rls_condition) =
        evaluator.get_using_expression(schema, &table, "DELETE", &rls_context)
    {
        if filters.is_empty() {
            filters = rls_condition;
        } else {
            filters = format!("({}) AND ({})", filters, rls_condition);
        }
    }

    let result = state
        .backend
        .delete(&table, &filters, false, &context)
        .await;

    match result {
        Ok(result) => HttpResponse::NoContent()
            .insert_header(("X-Deleted-Count", result.rows_affected.to_string()))
            .finish(),
        Err(e) => {
            HttpResponse::InternalServerError().json(ApiError::new("delete_error", e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jwt_decode() {
        // Valid test JWT payload (not signed, just for parsing)
        let payload = r#"{"sub":"test-user-id","role":"authenticated"}"#;
        use base64::Engine;
        let encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        let token = format!("header.{}.signature", encoded);

        let result = decode_jwt_claims(&token);
        assert!(result.is_ok());

        let (_, claims) = result.unwrap();
        assert_eq!(claims.sub, "test-user-id");
        assert_eq!(claims.role, "authenticated");
    }
}
