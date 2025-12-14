//! MySQL Wire Protocol Implementation
//! Provides MySQL-compatible binary protocol with prepared statements

use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::sync::Arc;
use std::collections::HashMap;
use dashmap::DashMap;
use tracing::{info, debug, error};

/// MySQL Protocol Constants
const MYSQL_PROTOCOL_VERSION: u8 = 10;
const MYSQL_SERVER_VERSION: &str = "8.0.32-LumaDB";
const MYSQL_CAPABILITY_CLIENT_PROTOCOL_41: u32 = 0x00000200;
const MYSQL_CAPABILITY_CLIENT_SECURE_CONNECTION: u32 = 0x00008000;
const MYSQL_CAPABILITY_CLIENT_DEPRECATE_EOF: u32 = 0x01000000;

const COM_QUIT: u8 = 0x01;
const COM_INIT_DB: u8 = 0x02;
const COM_QUERY: u8 = 0x03;
const COM_FIELD_LIST: u8 = 0x04;
const COM_PING: u8 = 0x0e;
const COM_STMT_PREPARE: u8 = 0x16;
const COM_STMT_EXECUTE: u8 = 0x17;
const COM_STMT_CLOSE: u8 = 0x19;
const COM_STMT_RESET: u8 = 0x1a;
const COM_SET_OPTION: u8 = 0x1b;

/// Prepared statement store
#[derive(Clone, Debug)]
pub struct PreparedStatement {
    pub id: u32,
    pub sql: String,
    pub param_count: u16,
    pub column_count: u16,
}

/// MySQL connection state
pub struct MySQLConnection {
    statements: Arc<DashMap<u32, PreparedStatement>>,
    next_stmt_id: Arc<std::sync::atomic::AtomicU32>,
    database: String,
}

impl MySQLConnection {
    pub fn new() -> Self {
        Self {
            statements: Arc::new(DashMap::new()),
            next_stmt_id: Arc::new(std::sync::atomic::AtomicU32::new(1)),
            database: "lumadb".to_string(),
        }
    }

    /// Handle MySQL handshake
    async fn send_handshake(&self, stream: &mut TcpStream) -> std::io::Result<()> {
        let mut packet = Vec::new();
        
        // Protocol version
        packet.push(MYSQL_PROTOCOL_VERSION);
        
        // Server version (null-terminated)
        packet.extend_from_slice(MYSQL_SERVER_VERSION.as_bytes());
        packet.push(0);
        
        // Connection ID (4 bytes)
        packet.extend_from_slice(&1u32.to_le_bytes());
        
        // Auth plugin data part 1 (8 bytes)
        packet.extend_from_slice(b"lumadbaa");
        
        // Filler
        packet.push(0);
        
        // Capability flags lower 2 bytes
        let capabilities: u32 = MYSQL_CAPABILITY_CLIENT_PROTOCOL_41 
            | MYSQL_CAPABILITY_CLIENT_SECURE_CONNECTION
            | MYSQL_CAPABILITY_CLIENT_DEPRECATE_EOF;
        packet.extend_from_slice(&(capabilities as u16).to_le_bytes());
        
        // Character set (utf8mb4)
        packet.push(45);
        
        // Status flags
        packet.extend_from_slice(&0u16.to_le_bytes());
        
        // Capability flags upper 2 bytes
        packet.extend_from_slice(&((capabilities >> 16) as u16).to_le_bytes());
        
        // Auth plugin data length
        packet.push(21);
        
        // Reserved (10 bytes)
        packet.extend_from_slice(&[0u8; 10]);
        
        // Auth plugin data part 2 (13 bytes)
        packet.extend_from_slice(b"lumadbserver\0");
        
        // Auth plugin name
        packet.extend_from_slice(b"mysql_native_password\0");
        
        self.send_packet(stream, 0, &packet).await
    }

    /// Send MySQL packet
    async fn send_packet(&self, stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> std::io::Result<()> {
        let len = payload.len();
        let mut header = [0u8; 4];
        header[0] = (len & 0xff) as u8;
        header[1] = ((len >> 8) & 0xff) as u8;
        header[2] = ((len >> 16) & 0xff) as u8;
        header[3] = sequence;
        
        stream.write_all(&header).await?;
        stream.write_all(payload).await?;
        stream.flush().await
    }

    /// Read MySQL packet
    async fn read_packet(&self, stream: &mut TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
        let mut header = [0u8; 4];
        stream.read_exact(&mut header).await?;
        
        let len = header[0] as usize | ((header[1] as usize) << 8) | ((header[2] as usize) << 16);
        let sequence = header[3];
        
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        
        Ok((sequence, payload))
    }

    /// Send OK packet
    async fn send_ok(&self, stream: &mut TcpStream, sequence: u8, affected_rows: u64, last_insert_id: u64) -> std::io::Result<()> {
        let mut packet = Vec::new();
        packet.push(0x00); // OK header
        
        // Affected rows (length-encoded int)
        self.write_lenenc_int(&mut packet, affected_rows);
        
        // Last insert ID
        self.write_lenenc_int(&mut packet, last_insert_id);
        
        // Status flags
        packet.extend_from_slice(&0u16.to_le_bytes());
        
        // Warnings
        packet.extend_from_slice(&0u16.to_le_bytes());
        
        self.send_packet(stream, sequence, &packet).await
    }

    /// Send Error packet
    async fn send_error(&self, stream: &mut TcpStream, sequence: u8, code: u16, message: &str) -> std::io::Result<()> {
        let mut packet = Vec::new();
        packet.push(0xff); // ERR header
        packet.extend_from_slice(&code.to_le_bytes());
        packet.push(b'#');
        packet.extend_from_slice(b"HY000");
        packet.extend_from_slice(message.as_bytes());
        
        self.send_packet(stream, sequence, &packet).await
    }

    fn write_lenenc_int(&self, buf: &mut Vec<u8>, val: u64) {
        if val < 251 {
            buf.push(val as u8);
        } else if val < 65536 {
            buf.push(0xfc);
            buf.extend_from_slice(&(val as u16).to_le_bytes());
        } else if val < 16777216 {
            buf.push(0xfd);
            buf.push((val & 0xff) as u8);
            buf.push(((val >> 8) & 0xff) as u8);
            buf.push(((val >> 16) & 0xff) as u8);
        } else {
            buf.push(0xfe);
            buf.extend_from_slice(&val.to_le_bytes());
        }
    }

    fn write_lenenc_str(&self, buf: &mut Vec<u8>, s: &str) {
        self.write_lenenc_int(buf, s.len() as u64);
        buf.extend_from_slice(s.as_bytes());
    }

    /// Handle COM_STMT_PREPARE
    async fn handle_prepare(&mut self, stream: &mut TcpStream, sequence: u8, sql: &str) -> std::io::Result<()> {
        let stmt_id = self.next_stmt_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        
        // Count parameters (? placeholders)
        let param_count = sql.matches('?').count() as u16;
        
        // For simplicity, assume 0 columns in result (SELECT would have more)
        let column_count = if sql.to_uppercase().starts_with("SELECT") { 1 } else { 0 };
        
        let stmt = PreparedStatement {
            id: stmt_id,
            sql: sql.to_string(),
            param_count,
            column_count,
        };
        self.statements.insert(stmt_id, stmt);
        
        // COM_STMT_PREPARE_OK response
        let mut packet = Vec::new();
        packet.push(0x00); // Status OK
        packet.extend_from_slice(&stmt_id.to_le_bytes());
        packet.extend_from_slice(&column_count.to_le_bytes());
        packet.extend_from_slice(&param_count.to_le_bytes());
        packet.push(0x00); // Reserved
        packet.extend_from_slice(&0u16.to_le_bytes()); // Warnings
        
        self.send_packet(stream, sequence + 1, &packet).await?;
        
        // If there are parameters, send parameter definitions
        if param_count > 0 {
            for i in 0..param_count {
                self.send_column_def(stream, sequence + 2 + i as u8, &format!("param{}", i), "VARCHAR").await?;
            }
            // EOF
            self.send_eof(stream, sequence + 2 + param_count as u8).await?;
        }
        
        // If there are columns, send column definitions
        if column_count > 0 {
            for i in 0..column_count {
                self.send_column_def(stream, sequence + 3 + param_count as u8 + i as u8, &format!("col{}", i), "VARCHAR").await?;
            }
            // EOF
            self.send_eof(stream, sequence + 3 + param_count as u8 + column_count as u8).await?;
        }
        
        debug!("Prepared statement {}: {}", stmt_id, sql);
        Ok(())
    }

    /// Send column definition
    async fn send_column_def(&self, stream: &mut TcpStream, sequence: u8, name: &str, type_name: &str) -> std::io::Result<()> {
        let mut packet = Vec::new();
        
        self.write_lenenc_str(&mut packet, "def"); // Catalog
        self.write_lenenc_str(&mut packet, "lumadb"); // Schema
        self.write_lenenc_str(&mut packet, "table"); // Table
        self.write_lenenc_str(&mut packet, "table"); // Org table
        self.write_lenenc_str(&mut packet, name); // Name
        self.write_lenenc_str(&mut packet, name); // Org name
        
        packet.push(0x0c); // Length of fixed fields
        packet.extend_from_slice(&45u16.to_le_bytes()); // Character set (utf8mb4)
        packet.extend_from_slice(&255u32.to_le_bytes()); // Column length
        packet.push(0xfd); // Type: VARCHAR
        packet.extend_from_slice(&0u16.to_le_bytes()); // Flags
        packet.push(0x00); // Decimals
        packet.extend_from_slice(&0u16.to_le_bytes()); // Filler
        
        self.send_packet(stream, sequence, &packet).await
    }

    /// Send EOF packet
    async fn send_eof(&self, stream: &mut TcpStream, sequence: u8) -> std::io::Result<()> {
        let mut packet = Vec::new();
        packet.push(0xfe); // EOF header
        packet.extend_from_slice(&0u16.to_le_bytes()); // Warnings
        packet.extend_from_slice(&0u16.to_le_bytes()); // Status flags
        
        self.send_packet(stream, sequence, &packet).await
    }

    /// Handle COM_STMT_EXECUTE
    async fn handle_execute(&self, stream: &mut TcpStream, sequence: u8, payload: &[u8]) -> std::io::Result<()> {
        if payload.len() < 5 {
            return self.send_error(stream, sequence + 1, 1064, "Malformed execute packet").await;
        }
        
        let stmt_id = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
        
        let stmt = match self.statements.get(&stmt_id) {
            Some(s) => s.clone(),
            None => return self.send_error(stream, sequence + 1, 1243, "Unknown prepared statement").await,
        };
        
        debug!("Executing prepared statement {}: {}", stmt_id, stmt.sql);
        
        // For now, return empty result set or OK based on query type
        if stmt.sql.to_uppercase().starts_with("SELECT") {
            // Send column count
            let mut packet = Vec::new();
            self.write_lenenc_int(&mut packet, 1);
            self.send_packet(stream, sequence + 1, &packet).await?;
            
            // Send column definition
            self.send_column_def(stream, sequence + 2, "result", "VARCHAR").await?;
            
            // Send EOF
            self.send_eof(stream, sequence + 3).await?;
            
            // Send one row with placeholder data
            let mut row_packet = Vec::new();
            self.write_lenenc_str(&mut row_packet, "prepared_result");
            self.send_packet(stream, sequence + 4, &row_packet).await?;
            
            // Final EOF
            self.send_eof(stream, sequence + 5).await?;
        } else {
            self.send_ok(stream, sequence + 1, 1, 0).await?;
        }
        
        Ok(())
    }

    /// Handle COM_QUERY
    async fn handle_query(&self, stream: &mut TcpStream, sequence: u8, sql: &str) -> std::io::Result<()> {
        debug!("Query: {}", sql);
        
        let sql_upper = sql.to_uppercase();
        
        if sql_upper.starts_with("SELECT") {
            // Return simple result set
            // Column count
            let mut packet = Vec::new();
            self.write_lenenc_int(&mut packet, 1);
            self.send_packet(stream, sequence + 1, &packet).await?;
            
            // Column definition
            self.send_column_def(stream, sequence + 2, "result", "VARCHAR").await?;
            
            // EOF
            self.send_eof(stream, sequence + 3).await?;
            
            // One data row
            let mut row_packet = Vec::new();
            self.write_lenenc_str(&mut row_packet, "query_result");
            self.send_packet(stream, sequence + 4, &row_packet).await?;
            
            // Final EOF
            self.send_eof(stream, sequence + 5).await?;
        } else if sql_upper.starts_with("SET") || sql_upper.starts_with("USE") {
            self.send_ok(stream, sequence + 1, 0, 0).await?;
        } else {
            self.send_ok(stream, sequence + 1, 1, 0).await?;
        }
        
        Ok(())
    }

    /// Handle MySQL connection
    pub async fn handle(&mut self, mut stream: TcpStream) -> std::io::Result<()> {
        // Send handshake
        self.send_handshake(&mut stream).await?;
        
        // Read handshake response
        let (_seq, _payload) = self.read_packet(&mut stream).await?;
        
        // Send OK (authentication success)
        self.send_ok(&mut stream, 2, 0, 0).await?;
        
        // Command loop
        loop {
            let (sequence, payload) = match self.read_packet(&mut stream).await {
                Ok(p) => p,
                Err(_) => break,
            };
            
            if payload.is_empty() {
                continue;
            }
            
            let command = payload[0];
            let data = &payload[1..];
            
            match command {
                COM_QUIT => {
                    debug!("Client quit");
                    break;
                }
                COM_PING => {
                    self.send_ok(&mut stream, sequence + 1, 0, 0).await?;
                }
                COM_INIT_DB => {
                    let db = String::from_utf8_lossy(data);
                    debug!("Init DB: {}", db);
                    self.send_ok(&mut stream, sequence + 1, 0, 0).await?;
                }
                COM_QUERY => {
                    let sql = String::from_utf8_lossy(data);
                    self.handle_query(&mut stream, sequence, &sql).await?;
                }
                COM_STMT_PREPARE => {
                    let sql = String::from_utf8_lossy(data);
                    self.handle_prepare(&mut stream, sequence, &sql).await?;
                }
                COM_STMT_EXECUTE => {
                    self.handle_execute(&mut stream, sequence, data).await?;
                }
                COM_STMT_CLOSE => {
                    if data.len() >= 4 {
                        let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                        self.statements.remove(&stmt_id);
                        debug!("Closed statement {}", stmt_id);
                    }
                    // No response for STMT_CLOSE
                }
                COM_STMT_RESET => {
                    self.send_ok(&mut stream, sequence + 1, 0, 0).await?;
                }
                COM_SET_OPTION => {
                    self.send_ok(&mut stream, sequence + 1, 0, 0).await?;
                }
                _ => {
                    debug!("Unknown command: 0x{:02x}", command);
                    self.send_error(&mut stream, sequence + 1, 1047, "Unknown command").await?;
                }
            }
        }
        
        Ok(())
    }
}

/// Run MySQL protocol server
pub async fn run(port: u16) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    info!("MySQL Protocol Server listening on 0.0.0.0:{}", port);
    
    loop {
        let (stream, addr) = listener.accept().await?;
        debug!("MySQL connection from {}", addr);
        
        tokio::spawn(async move {
            let mut conn = MySQLConnection::new();
            if let Err(e) = conn.handle(stream).await {
                error!("MySQL connection error: {}", e);
            }
        });
    }
}

impl Default for MySQLConnection {
    fn default() -> Self {
        Self::new()
    }
}
