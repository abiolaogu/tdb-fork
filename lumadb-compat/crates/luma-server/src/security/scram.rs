//! SCRAM-SHA-256 Authentication
//! PostgreSQL-compatible SCRAM authentication

use sha2::{Sha256, Digest};
use hmac::{Hmac, Mac};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use rand::Rng;

type HmacSha256 = Hmac<Sha256>;

/// SCRAM authentication state
#[derive(Debug, Clone)]
pub struct ScramState {
    pub username: String,
    pub client_first_bare: String,
    pub server_first: String,
    pub client_nonce: String,
    pub server_nonce: String,
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

/// SCRAM-SHA-256 authenticator
pub struct ScramAuthenticator {
    stored_passwords: std::collections::HashMap<String, StoredPassword>,
}

#[derive(Clone)]
struct StoredPassword {
    salt: Vec<u8>,
    iterations: u32,
    stored_key: Vec<u8>,
    server_key: Vec<u8>,
}

impl ScramAuthenticator {
    pub fn new() -> Self {
        Self {
            stored_passwords: std::collections::HashMap::new(),
        }
    }
    
    /// Add user with password (stores derived keys, not plaintext)
    pub fn add_user(&mut self, username: &str, password: &str) {
        let salt: Vec<u8> = rand::thread_rng().gen::<[u8; 16]>().to_vec();
        let iterations = 4096;
        
        let salted_password = hi(password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        
        self.stored_passwords.insert(username.to_string(), StoredPassword {
            salt,
            iterations,
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        });
    }
    
    /// Process client-first message and generate server-first
    pub fn server_first(&self, client_first: &str) -> Result<ScramState, String> {
        // Parse client-first-message
        // Format: n,,n=<username>,r=<client-nonce>
        let client_first_bare = if client_first.starts_with("n,,") {
            &client_first[3..]
        } else {
            return Err("Invalid client-first-message".to_string());
        };
        
        let mut username = String::new();
        let mut client_nonce = String::new();
        
        for part in client_first_bare.split(',') {
            if part.starts_with("n=") {
                username = part[2..].to_string();
            } else if part.starts_with("r=") {
                client_nonce = part[2..].to_string();
            }
        }
        
        if username.is_empty() || client_nonce.is_empty() {
            return Err("Missing username or nonce".to_string());
        }
        
        let stored = self.stored_passwords.get(&username)
            .ok_or_else(|| "User not found".to_string())?;
        
        // Generate server nonce
        let server_nonce_suffix: [u8; 18] = rand::thread_rng().gen();
        let server_nonce = format!("{}{}", client_nonce, BASE64.encode(server_nonce_suffix));
        
        // Build server-first-message
        let server_first = format!(
            "r={},s={},i={}",
            server_nonce,
            BASE64.encode(&stored.salt),
            stored.iterations
        );
        
        Ok(ScramState {
            username,
            client_first_bare: client_first_bare.to_string(),
            server_first: server_first.clone(),
            client_nonce,
            server_nonce,
            salt: stored.salt.clone(),
            iterations: stored.iterations,
            stored_key: stored.stored_key.clone(),
            server_key: stored.server_key.clone(),
        })
    }
    
    /// Process client-final and generate server-final
    pub fn server_final(&self, state: &ScramState, client_final: &str) -> Result<String, String> {
        // Parse client-final-message
        // Format: c=<channel-binding>,r=<nonce>,p=<proof>
        let mut channel_binding = String::new();
        let mut nonce = String::new();
        let mut client_proof_b64 = String::new();
        
        for part in client_final.split(',') {
            if part.starts_with("c=") {
                channel_binding = part[2..].to_string();
            } else if part.starts_with("r=") {
                nonce = part[2..].to_string();
            } else if part.starts_with("p=") {
                client_proof_b64 = part[2..].to_string();
            }
        }
        
        // Verify nonce
        if nonce != state.server_nonce {
            return Err("Nonce mismatch".to_string());
        }
        
        let client_proof = BASE64.decode(&client_proof_b64)
            .map_err(|_| "Invalid proof encoding")?;
        
        // Build auth message
        let client_final_without_proof = format!("c={},r={}", channel_binding, nonce);
        let auth_message = format!(
            "{},{},{}",
            state.client_first_bare,
            state.server_first,
            client_final_without_proof
        );
        
        // Verify client proof
        let client_signature = hmac_sha256(&state.stored_key, auth_message.as_bytes());
        let client_key: Vec<u8> = client_proof.iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        
        let computed_stored_key = sha256(&client_key);
        
        if computed_stored_key.as_slice() != state.stored_key.as_slice() {
            return Err("Authentication failed".to_string());
        }
        
        // Generate server signature
        let server_signature = hmac_sha256(&state.server_key, auth_message.as_bytes());
        let server_final = format!("v={}", BASE64.encode(&server_signature));
        
        Ok(server_final)
    }
}

/// PBKDF2-SHA256 (Hi function in SCRAM)
fn hi(password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
    let mut result = vec![0u8; 32];
    pbkdf2::pbkdf2::<HmacSha256>(password, salt, iterations, &mut result)
        .expect("PBKDF2 should not fail");
    result
}

/// HMAC-SHA256
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// SHA-256 hash
fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_scram_flow() {
        let mut auth = ScramAuthenticator::new();
        auth.add_user("testuser", "testpass");
        
        // This is a simplified test - real SCRAM requires proper client implementation
        let client_first = "n,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL";
        let state = auth.server_first(client_first).unwrap();
        
        assert_eq!(state.username, "testuser");
        assert!(state.server_nonce.starts_with("fyko+d2lbbFgONRv9qkxdawL"));
    }
}
