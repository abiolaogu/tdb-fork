//! MongoDB-compatible wire protocol server

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

use lumadb_common::error::Result;
use lumadb_storage::StorageEngine;

use super::handlers::MongoDBState;
use super::protocol::{parse_header, parse_op_msg, serialize_op_msg};
use super::types::OpCode;

/// MongoDB-compatible wire protocol server
pub struct MongoDBServer {
    storage: Arc<StorageEngine>,
    host: String,
    port: u16,
}

impl MongoDBServer {
    /// Create a new MongoDB-compatible server
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        Self {
            storage,
            host: "0.0.0.0".to_string(),
            port: 27017,
        }
    }

    /// Set the bind address
    pub fn bind(mut self, addr: &str) -> Self {
        if let Some((host, port)) = addr.split_once(':') {
            self.host = host.to_string();
            self.port = port.parse().unwrap_or(27017);
        }
        self
    }

    /// Set the port
    pub fn port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Run the MongoDB-compatible server
    pub async fn run(self) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr).await.map_err(|e| {
            lumadb_common::error::Error::Internal(format!("Failed to bind: {}", e))
        })?;

        info!("Starting MongoDB-compatible server on {}", addr);
        info!("MongoDB clients can connect using: mongodb://{}:{}", self.host, self.port);

        let state = Arc::new(MongoDBState {
            storage: self.storage.clone(),
        });

        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    debug!("New MongoDB connection from {}", peer_addr);
                    let state = state.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, state).await {
                            error!("Connection error from {}: {}", peer_addr, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Accept error: {}", e);
                }
            }
        }
    }
}

/// Handle a single MongoDB client connection
async fn handle_connection(
    mut stream: TcpStream,
    state: Arc<MongoDBState>,
) -> Result<()> {
    let mut buffer = vec![0u8; 64 * 1024]; // 64KB buffer
    let mut request_id_counter = 1i32;

    loop {
        // Read message header (16 bytes)
        let header_bytes = match read_exact(&mut stream, 16).await {
            Ok(bytes) => bytes,
            Err(_) => {
                debug!("Client disconnected");
                return Ok(());
            }
        };

        let header = match parse_header(&header_bytes) {
            Ok(h) => h,
            Err(e) => {
                warn!("Failed to parse header: {}", e);
                continue;
            }
        };

        debug!(
            "Received message: length={}, request_id={}, opcode={:?}",
            header.message_length, header.request_id, header.op_code
        );

        // Read message body
        let body_length = header.message_length as usize - 16;
        if body_length > buffer.len() {
            buffer.resize(body_length, 0);
        }

        let body_bytes = match read_exact(&mut stream, body_length).await {
            Ok(bytes) => bytes,
            Err(e) => {
                error!("Failed to read message body: {}", e);
                return Err(lumadb_common::error::Error::Internal(e.to_string()));
            }
        };

        // Handle based on opcode
        let response = match header.op_code {
            OpCode::OpMsg => {
                match parse_op_msg(&body_bytes) {
                    Ok(cmd) => {
                        let response_doc = state.handle_command(cmd).await;
                        Some(response_doc)
                    }
                    Err(e) => {
                        warn!("Failed to parse OP_MSG: {}", e);
                        Some(bson::doc! {
                            "ok": 0.0,
                            "errmsg": format!("Parse error: {}", e),
                            "code": 2
                        })
                    }
                }
            }
            OpCode::OpQuery => {
                // Legacy OP_QUERY - handle basic commands
                match parse_legacy_query(&body_bytes) {
                    Ok(cmd) => {
                        let response_doc = state.handle_command(cmd).await;
                        Some(response_doc)
                    }
                    Err(e) => {
                        warn!("Failed to parse OP_QUERY: {}", e);
                        Some(bson::doc! {
                            "ok": 0.0,
                            "errmsg": format!("Parse error: {}", e),
                            "code": 2
                        })
                    }
                }
            }
            _ => {
                warn!("Unsupported opcode: {:?}", header.op_code);
                Some(bson::doc! {
                    "ok": 0.0,
                    "errmsg": format!("Unsupported opcode: {:?}", header.op_code),
                    "code": 59
                })
            }
        };

        // Send response
        if let Some(response_doc) = response {
            let response_bytes = serialize_op_msg(
                request_id_counter,
                header.request_id,
                &response_doc,
            );
            request_id_counter = request_id_counter.wrapping_add(1);

            if let Err(e) = stream.write_all(&response_bytes).await {
                error!("Failed to send response: {}", e);
                return Err(lumadb_common::error::Error::Internal(e.to_string()));
            }
        }
    }
}

/// Read exact number of bytes from stream
async fn read_exact(stream: &mut TcpStream, len: usize) -> std::io::Result<Vec<u8>> {
    let mut buffer = vec![0u8; len];
    stream.read_exact(&mut buffer).await?;
    Ok(buffer)
}

/// Parse legacy OP_QUERY message (for older clients)
fn parse_legacy_query(body: &[u8]) -> std::result::Result<bson::Document, String> {
    // OP_QUERY format:
    // flags: 4 bytes
    // fullCollectionName: cstring
    // numberToSkip: 4 bytes
    // numberToReturn: 4 bytes
    // query: document

    if body.len() < 8 {
        return Err("Body too short for OP_QUERY".to_string());
    }

    let _flags = i32::from_le_bytes([body[0], body[1], body[2], body[3]]);

    // Find end of collection name (null terminated)
    let mut coll_end = 4;
    while coll_end < body.len() && body[coll_end] != 0 {
        coll_end += 1;
    }

    if coll_end >= body.len() {
        return Err("Invalid collection name".to_string());
    }

    let _collection = String::from_utf8_lossy(&body[4..coll_end]).to_string();

    // Skip collection name null terminator + numberToSkip + numberToReturn
    let doc_start = coll_end + 1 + 8;

    if doc_start >= body.len() {
        return Err("No query document".to_string());
    }

    // Parse BSON document
    bson::from_slice(&body[doc_start..])
        .map_err(|e| format!("Failed to parse query document: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_legacy_query() {
        // This would require a properly formatted OP_QUERY message
        // For now, just verify the function exists and handles errors
        let empty: &[u8] = &[];
        assert!(parse_legacy_query(empty).is_err());
    }
}
