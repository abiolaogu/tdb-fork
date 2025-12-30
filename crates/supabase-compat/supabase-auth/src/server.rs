//! Auth server implementation

use actix_cors::Cors;
use actix_web::{middleware, web, App, HttpServer};
use std::sync::Arc;
use tracing::info;

use supabase_common::config::AuthConfig;
use supabase_common::error::Result;

use crate::handlers::{self, AuthState};
use crate::jwt::JwtManager;
use crate::providers::email::EmailTokenStore;
use crate::session::SessionStore;
use crate::user::UserStore;

/// GoTrue-compatible authentication server
pub struct AuthServer {
    config: AuthConfig,
    state: Arc<AuthState>,
}

impl AuthServer {
    /// Create a new auth server
    pub fn new(config: &AuthConfig) -> Result<Self> {
        // Generate JWT secret if not provided
        let jwt_secret = config
            .jwt_secret
            .clone()
            .unwrap_or_else(|| JwtManager::generate_secret());

        let jwt_manager = Arc::new(JwtManager::new(
            &jwt_secret,
            &config.site_url,
            config.jwt_expiry,
            config.refresh_token_expiry,
        ));

        let user_store = Arc::new(UserStore::new(config.password_requirements.clone()));
        let session_store = Arc::new(SessionStore::new(jwt_manager.clone()));
        let email_store = Arc::new(EmailTokenStore::new());

        let state = Arc::new(AuthState {
            user_store,
            session_store,
            email_store,
            jwt_manager,
            site_url: config.site_url.clone(),
        });

        Ok(Self {
            config: config.clone(),
            state,
        })
    }

    /// Get auth state for sharing with other services
    pub fn state(&self) -> Arc<AuthState> {
        self.state.clone()
    }

    /// Run the auth server
    pub async fn run(&self) -> Result<()> {
        let state = self.state.clone();
        let host = self.config.host.clone();
        let port = self.config.port;

        info!("Starting Auth server on {}:{}", host, port);

        HttpServer::new(move || {
            let cors = Cors::permissive();

            App::new()
                .app_data(web::Data::new(state.clone()))
                .wrap(cors)
                .wrap(middleware::Compress::default())
                .wrap(middleware::Logger::default())
                .service(
                    web::scope("/auth/v1")
                        // Health check
                        .route("/health", web::get().to(health_check))
                        // Sign up
                        .route("/signup", web::post().to(handlers::signup))
                        // Token endpoint (login)
                        .route("/token", web::post().to(token_handler))
                        // Logout
                        .route("/logout", web::post().to(handlers::logout))
                        // Magic link
                        .route("/magiclink", web::post().to(handlers::magic_link))
                        // Password recovery
                        .route("/recover", web::post().to(handlers::recover))
                        // Verify OTP/token
                        .route("/verify", web::post().to(handlers::verify_token))
                        // User management
                        .route("/user", web::get().to(handlers::get_user))
                        .route("/user", web::put().to(handlers::update_user)),
                )
        })
        .workers(4)
        .bind(format!("{}:{}", host, port))?
        .run()
        .await?;

        Ok(())
    }
}

/// Health check endpoint
async fn health_check() -> actix_web::HttpResponse {
    actix_web::HttpResponse::Ok().json(serde_json::json!({
        "status": "healthy",
        "service": "auth",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// Token handler that routes based on grant_type
async fn token_handler(
    state: web::Data<Arc<AuthState>>,
    query: web::Query<TokenQuery>,
    body: web::Json<serde_json::Value>,
    req: actix_web::HttpRequest,
) -> actix_web::HttpResponse {
    match query.grant_type.as_deref() {
        Some("password") => {
            let request: handlers::SignInPasswordRequest =
                match serde_json::from_value(body.into_inner()) {
                    Ok(r) => r,
                    Err(e) => {
                        return actix_web::HttpResponse::BadRequest().json(
                            supabase_common::types::ApiError::new("invalid_request", e.to_string()),
                        );
                    }
                };
            handlers::token_password(state, web::Json(request), req).await
        }
        Some("refresh_token") => {
            let request: handlers::RefreshTokenRequest =
                match serde_json::from_value(body.into_inner()) {
                    Ok(r) => r,
                    Err(e) => {
                        return actix_web::HttpResponse::BadRequest().json(
                            supabase_common::types::ApiError::new("invalid_request", e.to_string()),
                        );
                    }
                };
            handlers::token_refresh(state, web::Json(request)).await
        }
        _ => actix_web::HttpResponse::BadRequest().json(supabase_common::types::ApiError::new(
            "unsupported_grant_type",
            "Supported grant types: password, refresh_token",
        )),
    }
}

#[derive(serde::Deserialize)]
struct TokenQuery {
    grant_type: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_server_creation() {
        let config = AuthConfig::default();
        let server = AuthServer::new(&config);
        assert!(server.is_ok());
    }
}
