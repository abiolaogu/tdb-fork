//! Security module
//! TLS/SSL and authentication support

pub mod tls;
pub mod scram;

pub use tls::{TlsConfig, create_tls_acceptor};
pub use scram::ScramAuthenticator;
