//! AWS Signature V4 Authentication
//!
//! Implements AWS SigV4 authentication for DynamoDB API compatibility.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

use crate::core::AdapterError;

type HmacSha256 = Hmac<Sha256>;

/// AWS credentials for signature validation
#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Configuration for AWS auth
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub enabled: bool,
    pub region: String,
    pub service: String,
    pub credentials: Option<AwsCredentials>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            region: "us-east-1".to_string(),
            service: "dynamodb".to_string(),
            credentials: None,
        }
    }
}

/// Validate AWS Signature V4 from request headers
pub fn validate_signature(
    headers: &http::HeaderMap,
    body: &[u8],
    config: &AuthConfig,
) -> Result<(), AdapterError> {
    if !config.enabled {
        return Ok(());
    }

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| AdapterError::AuthenticationError("Missing Authorization header".into()))?;

    // Parse Authorization header
    // Format: AWS4-HMAC-SHA256 Credential=.../region/service/aws4_request, SignedHeaders=..., Signature=...
    if !auth_header.starts_with("AWS4-HMAC-SHA256") {
        return Err(AdapterError::AuthenticationError(
            "Invalid authorization scheme".into(),
        ));
    }

    let parts = parse_auth_header(auth_header)?;
    
    // Extract credential components
    let credential = parts.get("Credential").ok_or_else(|| {
        AdapterError::AuthenticationError("Missing Credential".into())
    })?;
    
    let cred_parts: Vec<&str> = credential.split('/').collect();
    if cred_parts.len() < 5 {
        return Err(AdapterError::AuthenticationError(
            "Invalid Credential format".into(),
        ));
    }

    let access_key = cred_parts[0];
    let date = cred_parts[1];
    let region = cred_parts[2];
    let service = cred_parts[3];

    // Validate access key if credentials are configured
    if let Some(creds) = &config.credentials {
        if access_key != creds.access_key_id {
            return Err(AdapterError::AuthenticationError(
                "Invalid access key".into(),
            ));
        }

        // Compute expected signature
        let signed_headers = parts.get("SignedHeaders").ok_or_else(|| {
            AdapterError::AuthenticationError("Missing SignedHeaders".into())
        })?;
        
        let provided_signature = parts.get("Signature").ok_or_else(|| {
            AdapterError::AuthenticationError("Missing Signature".into())
        })?;

        let x_amz_date = headers
            .get("x-amz-date")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| AdapterError::AuthenticationError("Missing x-amz-date".into()))?;

        let expected_signature = compute_signature(
            &creds.secret_access_key,
            date,
            region,
            service,
            headers,
            body,
            signed_headers,
            x_amz_date,
        )?;

        if expected_signature != *provided_signature {
            return Err(AdapterError::AuthenticationError(
                "Signature mismatch".into(),
            ));
        }
    }

    Ok(())
}

fn parse_auth_header(header: &str) -> Result<HashMap<String, String>, AdapterError> {
    let mut parts = HashMap::new();
    
    // Skip "AWS4-HMAC-SHA256 " prefix
    let content = header.strip_prefix("AWS4-HMAC-SHA256 ").ok_or_else(|| {
        AdapterError::AuthenticationError("Invalid auth header format".into())
    })?;

    for part in content.split(", ") {
        if let Some((key, value)) = part.split_once('=') {
            parts.insert(key.to_string(), value.to_string());
        }
    }

    Ok(parts)
}

fn compute_signature(
    secret_key: &str,
    date: &str,
    region: &str,
    service: &str,
    headers: &http::HeaderMap,
    body: &[u8],
    signed_headers: &str,
    x_amz_date: &str,
) -> Result<String, AdapterError> {
    // Step 1: Create canonical request
    let canonical_request = create_canonical_request(headers, body, signed_headers)?;
    
    // Step 2: Create string to sign
    let string_to_sign = create_string_to_sign(
        x_amz_date,
        date,
        region,
        service,
        &canonical_request,
    );

    // Step 3: Calculate signing key
    let signing_key = get_signing_key(secret_key, date, region, service);

    // Step 4: Calculate signature
    let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes());
    
    Ok(hex::encode(signature))
}

fn create_canonical_request(
    headers: &http::HeaderMap,
    body: &[u8],
    signed_headers: &str,
) -> Result<String, AdapterError> {
    let method = "POST";
    let uri = "/";
    let query_string = "";

    // Build canonical headers
    let header_names: Vec<&str> = signed_headers.split(';').collect();
    let mut canonical_headers = String::new();
    
    for name in &header_names {
        let value = headers
            .get(*name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        canonical_headers.push_str(&format!("{}:{}\n", name.to_lowercase(), value.trim()));
    }

    // Hash the body
    let body_hash = hex::encode(Sha256::digest(body));

    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method,
        uri,
        query_string,
        canonical_headers,
        signed_headers,
        body_hash
    );

    Ok(hex::encode(Sha256::digest(canonical.as_bytes())))
}

fn create_string_to_sign(
    x_amz_date: &str,
    date: &str,
    region: &str,
    service: &str,
    canonical_request_hash: &str,
) -> String {
    format!(
        "AWS4-HMAC-SHA256\n{}\n{}/{}/{}/aws4_request\n{}",
        x_amz_date,
        date,
        region,
        service,
        canonical_request_hash
    )
}

fn get_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{}", secret_key).as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(message);
    mac.finalize().into_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_auth_header() {
        let header = "AWS4-HMAC-SHA256 Credential=AKID/20230101/us-east-1/dynamodb/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123";
        let parts = parse_auth_header(header).unwrap();
        
        assert!(parts.get("Credential").is_some());
        assert!(parts.get("SignedHeaders").is_some());
        assert!(parts.get("Signature").is_some());
    }

    #[test]
    fn test_disabled_auth_passes() {
        let config = AuthConfig::default();
        let headers = http::HeaderMap::new();
        assert!(validate_signature(&headers, b"", &config).is_ok());
    }
}
