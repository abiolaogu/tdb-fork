//! OAuth providers support

pub mod email;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use supabase_common::error::Result;
use supabase_common::types::User;

/// OAuth user info returned by providers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthUserInfo {
    pub provider: String,
    pub provider_id: String,
    pub email: Option<String>,
    pub email_verified: bool,
    pub name: Option<String>,
    pub avatar_url: Option<String>,
    pub raw_data: serde_json::Value,
}

/// OAuth provider trait for extensibility
#[async_trait]
pub trait OAuthProvider: Send + Sync {
    /// Get the provider name
    fn name(&self) -> &str;

    /// Get the authorization URL for OAuth flow
    fn get_authorize_url(&self, state: &str, redirect_uri: &str) -> String;

    /// Exchange authorization code for tokens
    async fn exchange_code(&self, code: &str, redirect_uri: &str) -> Result<OAuthTokens>;

    /// Get user info using access token
    async fn get_user_info(&self, access_token: &str) -> Result<OAuthUserInfo>;
}

/// OAuth tokens from provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OAuthTokens {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<u64>,
    pub refresh_token: Option<String>,
    pub scope: Option<String>,
    pub id_token: Option<String>,
}

/// Google OAuth provider
pub struct GoogleProvider {
    client_id: String,
    client_secret: String,
}

impl GoogleProvider {
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
        }
    }
}

#[async_trait]
impl OAuthProvider for GoogleProvider {
    fn name(&self) -> &str {
        "google"
    }

    fn get_authorize_url(&self, state: &str, redirect_uri: &str) -> String {
        format!(
            "https://accounts.google.com/o/oauth2/v2/auth?\
            client_id={}&\
            redirect_uri={}&\
            response_type=code&\
            scope=openid%20email%20profile&\
            state={}&\
            access_type=offline",
            urlencoding::encode(&self.client_id),
            urlencoding::encode(redirect_uri),
            urlencoding::encode(state),
        )
    }

    async fn exchange_code(&self, code: &str, redirect_uri: &str) -> Result<OAuthTokens> {
        let client = reqwest::Client::new();
        let response = client
            .post("https://oauth2.googleapis.com/token")
            .form(&[
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("code", &code.to_string()),
                ("redirect_uri", &redirect_uri.to_string()),
                ("grant_type", &"authorization_code".to_string()),
            ])
            .send()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        let tokens: OAuthTokens = response
            .json()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        Ok(tokens)
    }

    async fn get_user_info(&self, access_token: &str) -> Result<OAuthUserInfo> {
        let client = reqwest::Client::new();
        let response = client
            .get("https://www.googleapis.com/oauth2/v2/userinfo")
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        let data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        Ok(OAuthUserInfo {
            provider: "google".to_string(),
            provider_id: data["id"].as_str().unwrap_or("").to_string(),
            email: data["email"].as_str().map(|s| s.to_string()),
            email_verified: data["verified_email"].as_bool().unwrap_or(false),
            name: data["name"].as_str().map(|s| s.to_string()),
            avatar_url: data["picture"].as_str().map(|s| s.to_string()),
            raw_data: data,
        })
    }
}

/// GitHub OAuth provider
pub struct GitHubProvider {
    client_id: String,
    client_secret: String,
}

impl GitHubProvider {
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
        }
    }
}

#[async_trait]
impl OAuthProvider for GitHubProvider {
    fn name(&self) -> &str {
        "github"
    }

    fn get_authorize_url(&self, state: &str, redirect_uri: &str) -> String {
        format!(
            "https://github.com/login/oauth/authorize?\
            client_id={}&\
            redirect_uri={}&\
            scope=user:email&\
            state={}",
            urlencoding::encode(&self.client_id),
            urlencoding::encode(redirect_uri),
            urlencoding::encode(state),
        )
    }

    async fn exchange_code(&self, code: &str, redirect_uri: &str) -> Result<OAuthTokens> {
        let client = reqwest::Client::new();
        let response = client
            .post("https://github.com/login/oauth/access_token")
            .header("Accept", "application/json")
            .form(&[
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("code", &code.to_string()),
                ("redirect_uri", &redirect_uri.to_string()),
            ])
            .send()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        let data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        Ok(OAuthTokens {
            access_token: data["access_token"].as_str().unwrap_or("").to_string(),
            token_type: data["token_type"].as_str().unwrap_or("bearer").to_string(),
            expires_in: None,
            refresh_token: data["refresh_token"].as_str().map(|s| s.to_string()),
            scope: data["scope"].as_str().map(|s| s.to_string()),
            id_token: None,
        })
    }

    async fn get_user_info(&self, access_token: &str) -> Result<OAuthUserInfo> {
        let client = reqwest::Client::new();

        // Get user profile
        let response = client
            .get("https://api.github.com/user")
            .header("User-Agent", "LumaDB-Supabase")
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        let data: serde_json::Value = response
            .json()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        // Get primary email
        let email_response = client
            .get("https://api.github.com/user/emails")
            .header("User-Agent", "LumaDB-Supabase")
            .bearer_auth(access_token)
            .send()
            .await
            .map_err(|e| supabase_common::error::Error::OAuthError(e.to_string()))?;

        let emails: Vec<serde_json::Value> = email_response.json().await.unwrap_or_default();

        let primary_email = emails
            .iter()
            .find(|e| e["primary"].as_bool().unwrap_or(false))
            .or_else(|| emails.first());

        let (email, email_verified) = primary_email
            .map(|e| {
                (
                    e["email"].as_str().map(|s| s.to_string()),
                    e["verified"].as_bool().unwrap_or(false),
                )
            })
            .unwrap_or((None, false));

        Ok(OAuthUserInfo {
            provider: "github".to_string(),
            provider_id: data["id"]
                .as_i64()
                .map(|i| i.to_string())
                .unwrap_or_default(),
            email,
            email_verified,
            name: data["name"].as_str().map(|s| s.to_string()),
            avatar_url: data["avatar_url"].as_str().map(|s| s.to_string()),
            raw_data: data,
        })
    }
}

// URL encoding helper
mod urlencoding {
    pub fn encode(s: &str) -> String {
        url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
    }
}
