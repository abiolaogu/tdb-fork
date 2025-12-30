//! HTTP handlers for authentication endpoints

use actix_web::{web, HttpRequest, HttpResponse};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{info, warn};

use supabase_common::error::Error;
use supabase_common::types::{ApiError, Session, User};

use crate::jwt::JwtManager;
use crate::providers::email::EmailTokenStore;
use crate::session::SessionStore;
use crate::user::UserStore;

/// Shared auth state
pub struct AuthState {
    pub user_store: Arc<UserStore>,
    pub session_store: Arc<SessionStore>,
    pub email_store: Arc<EmailTokenStore>,
    pub jwt_manager: Arc<JwtManager>,
    pub site_url: String,
}

// ============================================================================
// Request/Response Types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct SignUpRequest {
    pub email: Option<String>,
    pub phone: Option<String>,
    pub password: String,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[derive(Debug, Deserialize)]
pub struct SignInPasswordRequest {
    pub email: Option<String>,
    pub phone: Option<String>,
    pub password: String,
}

#[derive(Debug, Deserialize)]
pub struct RefreshTokenRequest {
    pub refresh_token: String,
}

#[derive(Debug, Deserialize)]
pub struct MagicLinkRequest {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct VerifyOtpRequest {
    pub email: Option<String>,
    pub phone: Option<String>,
    pub token: String,
    #[serde(rename = "type")]
    pub token_type: String,
}

#[derive(Debug, Deserialize)]
pub struct RecoverRequest {
    pub email: String,
}

#[derive(Debug, Deserialize)]
pub struct UpdateUserRequest {
    pub email: Option<String>,
    pub phone: Option<String>,
    pub password: Option<String>,
    #[serde(default)]
    pub data: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: u64,
    pub expires_at: u64,
    pub refresh_token: String,
    pub user: User,
}

impl From<Session> for TokenResponse {
    fn from(session: Session) -> Self {
        Self {
            access_token: session.access_token,
            token_type: session.token_type,
            expires_in: session.expires_in,
            expires_at: session.expires_at,
            refresh_token: session.refresh_token,
            user: session.user,
        }
    }
}

// ============================================================================
// Handlers
// ============================================================================

/// POST /auth/v1/signup
pub async fn signup(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<SignUpRequest>,
    http_req: HttpRequest,
) -> HttpResponse {
    let metadata = if request.data.is_null() {
        None
    } else {
        Some(request.data.clone())
    };

    // Create user
    let user = if let Some(email) = &request.email {
        match state
            .user_store
            .create_user_with_password(email, &request.password, metadata)
        {
            Ok(user) => user,
            Err(e) => return error_response(e),
        }
    } else if let Some(phone) = &request.phone {
        match state
            .user_store
            .create_user_with_phone(phone, Some(&request.password), metadata)
        {
            Ok(user) => user,
            Err(e) => return error_response(e),
        }
    } else {
        return HttpResponse::BadRequest()
            .json(ApiError::new("invalid_request", "Email or phone required"));
    };

    info!("User signed up: {}", user.id);

    // Create session
    let user_agent = http_req
        .headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let ip_address = http_req
        .connection_info()
        .realip_remote_addr()
        .map(|s| s.to_string());

    match state
        .session_store
        .create_session(&user, user_agent, ip_address)
    {
        Ok(session) => HttpResponse::Ok().json(TokenResponse::from(session)),
        Err(e) => error_response(e),
    }
}

/// POST /auth/v1/token?grant_type=password
pub async fn token_password(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<SignInPasswordRequest>,
    http_req: HttpRequest,
) -> HttpResponse {
    let email = match &request.email {
        Some(e) => e,
        None => {
            return HttpResponse::BadRequest()
                .json(ApiError::new("invalid_request", "Email required"));
        }
    };

    let user = match state
        .user_store
        .verify_credentials(email, &request.password)
    {
        Ok(user) => user,
        Err(e) => {
            warn!("Login failed for {}: {:?}", email, e);
            return error_response(e);
        }
    };

    info!("User logged in: {}", user.id);

    let user_agent = http_req
        .headers()
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let ip_address = http_req
        .connection_info()
        .realip_remote_addr()
        .map(|s| s.to_string());

    match state
        .session_store
        .create_session(&user, user_agent, ip_address)
    {
        Ok(session) => HttpResponse::Ok().json(TokenResponse::from(session)),
        Err(e) => error_response(e),
    }
}

/// POST /auth/v1/token?grant_type=refresh_token
pub async fn token_refresh(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<RefreshTokenRequest>,
) -> HttpResponse {
    // Decode refresh token to get user
    let claims = match state
        .jwt_manager
        .decode_without_validation(&request.refresh_token)
    {
        Ok(claims) => claims,
        Err(_) => {
            // Refresh token is not a JWT, try to find it in session store
            // For now, we'll need the user from the session
            return error_response(Error::RefreshTokenInvalid);
        }
    };

    let user_id = match uuid::Uuid::parse_str(&claims.sub) {
        Ok(id) => id,
        Err(_) => return error_response(Error::InvalidToken),
    };

    let user = match state.user_store.get_user(&user_id) {
        Ok(user) => user,
        Err(e) => return error_response(e),
    };

    match state
        .session_store
        .refresh_session(&request.refresh_token, &user)
    {
        Ok(session) => HttpResponse::Ok().json(TokenResponse::from(session)),
        Err(e) => error_response(e),
    }
}

/// POST /auth/v1/logout
pub async fn logout(state: web::Data<Arc<AuthState>>, http_req: HttpRequest) -> HttpResponse {
    let token = match extract_bearer_token(&http_req) {
        Some(t) => t,
        None => return error_response(Error::Unauthorized),
    };

    let claims = match state.jwt_manager.validate_access_token(&token) {
        Ok(c) => c,
        Err(e) => return error_response(e),
    };

    if let Some(session_id) = claims.session_id {
        let _ = state.session_store.revoke_session(&session_id);
    }

    HttpResponse::NoContent().finish()
}

/// POST /auth/v1/magiclink
pub async fn magic_link(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<MagicLinkRequest>,
) -> HttpResponse {
    let token = state.email_store.create_magic_link(&request.email);

    // In production, send email with link
    // For now, just log it
    info!(
        "Magic link created for {}: {}/auth/v1/verify?token={}&type=magiclink",
        request.email, state.site_url, token.token
    );

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Magic link sent"
    }))
}

/// POST /auth/v1/recover
pub async fn recover(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<RecoverRequest>,
) -> HttpResponse {
    // Try to find user
    let user = match state.user_store.get_user_by_email(&request.email) {
        Ok(u) => u,
        Err(_) => {
            // Don't reveal if user exists
            return HttpResponse::Ok().json(serde_json::json!({
                "message": "Password recovery email sent"
            }));
        }
    };

    let token = state
        .email_store
        .create_password_reset(user.id, &request.email);

    info!(
        "Password reset created for {}: {}/auth/v1/verify?token={}&type=recovery",
        request.email, state.site_url, token.token
    );

    HttpResponse::Ok().json(serde_json::json!({
        "message": "Password recovery email sent"
    }))
}

/// POST /auth/v1/verify
pub async fn verify_token(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<VerifyOtpRequest>,
    http_req: HttpRequest,
) -> HttpResponse {
    match request.token_type.as_str() {
        "magiclink" => {
            let email = match state.email_store.verify_magic_link(&request.token) {
                Ok(e) => e,
                Err(e) => return error_response(e),
            };

            // Get or create user
            let user = match state.user_store.get_user_by_email(&email) {
                Ok(u) => u,
                Err(_) => {
                    // Create new user without password
                    match state.user_store.create_user_with_password(
                        &email,
                        &uuid::Uuid::new_v4().to_string(),
                        None,
                    ) {
                        Ok(u) => u,
                        Err(e) => return error_response(e),
                    }
                }
            };

            let user_agent = http_req
                .headers()
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            match state.session_store.create_session(&user, user_agent, None) {
                Ok(session) => HttpResponse::Ok().json(TokenResponse::from(session)),
                Err(e) => error_response(e),
            }
        }
        "recovery" => {
            let (user_id, _email) = match state.email_store.verify_password_reset(&request.token) {
                Ok(r) => r,
                Err(e) => return error_response(e),
            };

            let user = match state.user_store.get_user(&user_id) {
                Ok(u) => u,
                Err(e) => return error_response(e),
            };

            let user_agent = http_req
                .headers()
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            match state.session_store.create_session(&user, user_agent, None) {
                Ok(session) => HttpResponse::Ok().json(TokenResponse::from(session)),
                Err(e) => error_response(e),
            }
        }
        "signup" | "email" => {
            let (user_id, _email) = match state.email_store.verify_email_confirm(&request.token) {
                Ok(r) => r,
                Err(e) => return error_response(e),
            };

            match state.user_store.confirm_email(&user_id) {
                Ok(user) => HttpResponse::Ok().json(user),
                Err(e) => error_response(e),
            }
        }
        _ => {
            HttpResponse::BadRequest().json(ApiError::new("invalid_request", "Unknown token type"))
        }
    }
}

/// GET /auth/v1/user
pub async fn get_user(state: web::Data<Arc<AuthState>>, http_req: HttpRequest) -> HttpResponse {
    let token = match extract_bearer_token(&http_req) {
        Some(t) => t,
        None => return error_response(Error::Unauthorized),
    };

    let claims = match state.jwt_manager.validate_access_token(&token) {
        Ok(c) => c,
        Err(e) => return error_response(e),
    };

    let user_id = match uuid::Uuid::parse_str(&claims.sub) {
        Ok(id) => id,
        Err(_) => return error_response(Error::InvalidToken),
    };

    match state.user_store.get_user(&user_id) {
        Ok(user) => HttpResponse::Ok().json(user),
        Err(e) => error_response(e),
    }
}

/// PUT /auth/v1/user
pub async fn update_user(
    state: web::Data<Arc<AuthState>>,
    request: web::Json<UpdateUserRequest>,
    http_req: HttpRequest,
) -> HttpResponse {
    let token = match extract_bearer_token(&http_req) {
        Some(t) => t,
        None => return error_response(Error::Unauthorized),
    };

    let claims = match state.jwt_manager.validate_access_token(&token) {
        Ok(c) => c,
        Err(e) => return error_response(e),
    };

    let user_id = match uuid::Uuid::parse_str(&claims.sub) {
        Ok(id) => id,
        Err(_) => return error_response(Error::InvalidToken),
    };

    // Update password if provided
    if let Some(password) = &request.password {
        if let Err(e) = state.user_store.update_password(&user_id, password) {
            return error_response(e);
        }
    }

    let metadata = if request.data.is_null() {
        None
    } else {
        Some(request.data.clone())
    };

    match state.user_store.update_user(
        &user_id,
        request.email.clone(),
        request.phone.clone(),
        metadata,
        None,
    ) {
        Ok(user) => HttpResponse::Ok().json(user),
        Err(e) => error_response(e),
    }
}

// ============================================================================
// Helpers
// ============================================================================

fn extract_bearer_token(req: &HttpRequest) -> Option<String> {
    req.headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

fn error_response(error: Error) -> HttpResponse {
    let status = error.status_code();
    let api_error = ApiError::new(error.error_code(), error.to_string());

    match status {
        400 => HttpResponse::BadRequest().json(api_error),
        401 => HttpResponse::Unauthorized().json(api_error),
        403 => HttpResponse::Forbidden().json(api_error),
        404 => HttpResponse::NotFound().json(api_error),
        409 => HttpResponse::Conflict().json(api_error),
        422 => HttpResponse::UnprocessableEntity().json(api_error),
        429 => HttpResponse::TooManyRequests().json(api_error),
        503 => HttpResponse::ServiceUnavailable().json(api_error),
        _ => HttpResponse::InternalServerError().json(api_error),
    }
}
