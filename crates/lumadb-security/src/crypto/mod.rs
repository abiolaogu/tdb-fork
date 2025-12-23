//! Cryptographic utilities

use lumadb_common::error::Result;

/// TLS configuration builder
pub struct TlsConfig {
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    pub ca_path: Option<String>,
    pub verify_client: bool,
}

impl TlsConfig {
    pub fn new() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            ca_path: None,
            verify_client: false,
        }
    }

    pub fn with_cert(mut self, path: &str) -> Self {
        self.cert_path = Some(path.to_string());
        self
    }

    pub fn with_key(mut self, path: &str) -> Self {
        self.key_path = Some(path.to_string());
        self
    }

    pub fn with_ca(mut self, path: &str) -> Self {
        self.ca_path = Some(path.to_string());
        self
    }

    pub fn with_client_verification(mut self) -> Self {
        self.verify_client = true;
        self
    }
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Encryption utilities
pub struct Encryption;

impl Encryption {
    /// Encrypt data using AES-256-GCM
    pub fn encrypt(key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};
        use ring::rand::{SecureRandom, SystemRandom};

        let rng = SystemRandom::new();
        let mut nonce_bytes = [0u8; 12];
        rng.fill(&mut nonce_bytes).map_err(|_| {
            lumadb_common::error::Error::Internal("Failed to generate nonce".to_string())
        })?;

        let unbound_key = UnboundKey::new(&AES_256_GCM, key).map_err(|_| {
            lumadb_common::error::Error::Internal("Invalid key".to_string())
        })?;

        let key = LessSafeKey::new(unbound_key);
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let mut ciphertext = plaintext.to_vec();
        key.seal_in_place_append_tag(nonce, Aad::empty(), &mut ciphertext)
            .map_err(|_| {
                lumadb_common::error::Error::Internal("Encryption failed".to_string())
            })?;

        // Prepend nonce to ciphertext
        let mut result = nonce_bytes.to_vec();
        result.extend(ciphertext);
        Ok(result)
    }

    /// Decrypt data using AES-256-GCM
    pub fn decrypt(key: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
        use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, AES_256_GCM};

        if ciphertext.len() < 12 {
            return Err(lumadb_common::error::Error::Internal(
                "Ciphertext too short".to_string(),
            ));
        }

        let (nonce_bytes, encrypted) = ciphertext.split_at(12);
        let nonce_bytes: [u8; 12] = nonce_bytes.try_into().map_err(|_| {
            lumadb_common::error::Error::Internal("Invalid nonce".to_string())
        })?;

        let unbound_key = UnboundKey::new(&AES_256_GCM, key).map_err(|_| {
            lumadb_common::error::Error::Internal("Invalid key".to_string())
        })?;

        let key = LessSafeKey::new(unbound_key);
        let nonce = Nonce::assume_unique_for_key(nonce_bytes);

        let mut plaintext = encrypted.to_vec();
        key.open_in_place(nonce, Aad::empty(), &mut plaintext)
            .map_err(|_| {
                lumadb_common::error::Error::Internal("Decryption failed".to_string())
            })?;

        // Remove tag
        plaintext.truncate(plaintext.len() - 16);
        Ok(plaintext)
    }
}
