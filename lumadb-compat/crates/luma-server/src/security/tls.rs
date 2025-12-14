//! TLS/SSL Support for LumaDB
//! Provides rustls-based TLS encryption for all protocols

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;
use tokio_rustls::rustls::{
    self, Certificate, PrivateKey, ServerConfig,
    server::AllowAnyAuthenticatedClient,
    RootCertStore,
};
use tokio_rustls::TlsAcceptor;
use tracing::{info, error};

/// TLS configuration
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to certificate file (PEM format)
    pub cert_path: String,
    /// Path to private key file (PEM format)
    pub key_path: String,
    /// Optional CA certificate for client authentication
    pub ca_cert_path: Option<String>,
    /// Require client certificate authentication
    pub require_client_auth: bool,
}

impl TlsConfig {
    pub fn new(cert_path: &str, key_path: &str) -> Self {
        Self {
            cert_path: cert_path.to_string(),
            key_path: key_path.to_string(),
            ca_cert_path: None,
            require_client_auth: false,
        }
    }
    
    pub fn with_client_auth(mut self, ca_cert_path: &str) -> Self {
        self.ca_cert_path = Some(ca_cert_path.to_string());
        self.require_client_auth = true;
        self
    }
}

/// Load certificates from PEM file
fn load_certs(path: &Path) -> Result<Vec<Certificate>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)?;
    Ok(certs.into_iter().map(Certificate).collect())
}

/// Load private key from PEM file
fn load_key(path: &Path) -> Result<PrivateKey, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    
    // Try PKCS8 first
    let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)?;
    if let Some(key) = keys.into_iter().next() {
        return Ok(PrivateKey(key));
    }
    
    // Try RSA private key
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    let keys = rustls_pemfile::rsa_private_keys(&mut reader)?;
    if let Some(key) = keys.into_iter().next() {
        return Ok(PrivateKey(key));
    }
    
    Err("No private key found".into())
}

/// Create TLS acceptor from configuration
pub fn create_tls_acceptor(config: &TlsConfig) -> Result<TlsAcceptor, Box<dyn std::error::Error>> {
    let certs = load_certs(Path::new(&config.cert_path))?;
    let key = load_key(Path::new(&config.key_path))?;
    
    let server_config = if config.require_client_auth {
        // Load CA certs for client verification
        let ca_path = config.ca_cert_path.as_ref()
            .ok_or("CA cert path required for client auth")?;
        let ca_certs = load_certs(Path::new(ca_path))?;
        
        let mut root_store = RootCertStore::empty();
        for cert in ca_certs {
            root_store.add(&cert)?;
        }
        
        let client_verifier = AllowAnyAuthenticatedClient::new(root_store);
        
        ServerConfig::builder()
            .with_safe_defaults()
            .with_client_cert_verifier(Arc::new(client_verifier))
            .with_single_cert(certs, key)?
    } else {
        ServerConfig::builder()
            .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(certs, key)?
    };
    
    Ok(TlsAcceptor::from(Arc::new(server_config)))
}

/// Generate self-signed certificate for development
pub fn generate_self_signed_cert() -> Result<(String, String), Box<dyn std::error::Error>> {
    use std::io::Write;
    
    // Use rcgen for certificate generation
    let subject_alt_names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ];
    
    let cert = rcgen::generate_simple_self_signed(subject_alt_names)?;
    
    let cert_pem = cert.serialize_pem()?;
    let key_pem = cert.serialize_private_key_pem();
    
    // Write to temp files
    let cert_path = "/tmp/lumadb-cert.pem";
    let key_path = "/tmp/lumadb-key.pem";
    
    let mut cert_file = File::create(cert_path)?;
    cert_file.write_all(cert_pem.as_bytes())?;
    
    let mut key_file = File::create(key_path)?;
    key_file.write_all(key_pem.as_bytes())?;
    
    info!("Generated self-signed certificate: {}", cert_path);
    
    Ok((cert_path.to_string(), key_path.to_string()))
}
