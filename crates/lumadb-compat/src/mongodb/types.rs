//! MongoDB protocol types and $vectorSearch support

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ============================================================================
// Wire Protocol Types
// ============================================================================

/// MongoDB wire protocol opcodes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum OpCode {
    OpReply = 1,          // Deprecated
    OpUpdate = 2001,      // Deprecated
    OpInsert = 2002,      // Deprecated
    OpQuery = 2004,       // Deprecated
    OpGetMore = 2005,     // Deprecated
    OpDelete = 2006,      // Deprecated
    OpKillCursors = 2007, // Deprecated
    OpMsg = 2013,         // Current
    OpCompressed = 2012,
}

impl From<u32> for OpCode {
    fn from(value: u32) -> Self {
        match value {
            1 => OpCode::OpReply,
            2001 => OpCode::OpUpdate,
            2002 => OpCode::OpInsert,
            2004 => OpCode::OpQuery,
            2005 => OpCode::OpGetMore,
            2006 => OpCode::OpDelete,
            2007 => OpCode::OpKillCursors,
            2012 => OpCode::OpCompressed,
            2013 => OpCode::OpMsg,
            _ => OpCode::OpMsg,
        }
    }
}

/// Message header
#[derive(Debug, Clone)]
pub struct MsgHeader {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: OpCode,
}

/// OP_MSG flag bits
#[derive(Debug, Clone, Copy)]
pub struct MsgFlags(pub u32);

impl MsgFlags {
    pub const CHECKSUM_PRESENT: u32 = 1;
    pub const MORE_TO_COME: u32 = 2;
    pub const EXHAUST_ALLOWED: u32 = 1 << 16;
}

/// OP_MSG section types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SectionKind {
    Body = 0,
    DocumentSequence = 1,
}

// ============================================================================
// Command Types
// ============================================================================

/// MongoDB command wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Command {
    IsMaster(IsMasterCommand),
    Hello(HelloCommand),
    Ping(PingCommand),
    ListDatabases(ListDatabasesCommand),
    ListCollections(ListCollectionsCommand),
    Create(CreateCommand),
    Drop(DropCommand),
    Insert(InsertCommand),
    Find(FindCommand),
    Update(UpdateCommand),
    Delete(DeleteCommand),
    Aggregate(AggregateCommand),
    CreateIndexes(CreateIndexesCommand),
    Count(CountCommand),
    GetLastError(GetLastErrorCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsMasterCommand {
    #[serde(rename = "isMaster", alias = "ismaster")]
    pub is_master: i32,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloCommand {
    pub hello: i32,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingCommand {
    pub ping: i32,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListDatabasesCommand {
    #[serde(rename = "listDatabases")]
    pub list_databases: i32,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListCollectionsCommand {
    #[serde(rename = "listCollections")]
    pub list_collections: i32,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateCommand {
    pub create: String,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropCommand {
    pub drop: String,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertCommand {
    pub insert: String,
    pub documents: Vec<bson::Document>,
    #[serde(rename = "ordered", default = "default_true")]
    pub ordered: bool,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindCommand {
    pub find: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<bson::Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub projection: Option<bson::Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<bson::Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip: Option<i64>,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateCommand {
    pub update: String,
    pub updates: Vec<UpdateStatement>,
    #[serde(rename = "ordered", default = "default_true")]
    pub ordered: bool,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub q: bson::Document,
    pub u: bson::Document,
    #[serde(default)]
    pub upsert: bool,
    #[serde(default)]
    pub multi: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteCommand {
    pub delete: String,
    pub deletes: Vec<DeleteStatement>,
    #[serde(rename = "ordered", default = "default_true")]
    pub ordered: bool,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub q: bson::Document,
    #[serde(default = "default_limit")]
    pub limit: i32,
}

fn default_limit() -> i32 {
    0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateCommand {
    pub aggregate: String,
    pub pipeline: Vec<bson::Document>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<CursorOptions>,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CursorOptions {
    #[serde(rename = "batchSize", default)]
    pub batch_size: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateIndexesCommand {
    #[serde(rename = "createIndexes")]
    pub create_indexes: String,
    pub indexes: Vec<IndexModel>,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexModel {
    pub key: bson::Document,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sparse: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CountCommand {
    pub count: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query: Option<bson::Document>,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLastErrorCommand {
    #[serde(rename = "getLastError")]
    pub get_last_error: i32,
    #[serde(rename = "$db")]
    pub db: Option<String>,
}

// ============================================================================
// $vectorSearch Types (Atlas Vector Search)
// ============================================================================

/// $vectorSearch aggregation stage
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorSearchStage {
    /// Name of the vector index
    pub index: String,
    /// Path to the vector field
    pub path: String,
    /// Query vector
    pub query_vector: Vec<f32>,
    /// Number of results to return
    pub num_candidates: i32,
    /// Maximum number of results
    pub limit: i32,
    /// Optional filter
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<bson::Document>,
}

/// Vector search index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorSearchIndex {
    pub name: String,
    pub definition: VectorSearchDefinition,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorSearchDefinition {
    pub fields: Vec<VectorFieldDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VectorFieldDefinition {
    #[serde(rename = "type")]
    pub field_type: String,
    pub path: String,
    pub num_dimensions: i32,
    pub similarity: String,
}

// ============================================================================
// Response Types
// ============================================================================

/// Standard command response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResponse {
    pub ok: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub errmsg: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<i32>,
    #[serde(flatten)]
    pub extra: HashMap<String, bson::Bson>,
}

impl CommandResponse {
    pub fn ok() -> Self {
        Self {
            ok: 1.0,
            errmsg: None,
            code: None,
            extra: HashMap::new(),
        }
    }

    pub fn ok_with<K: Into<String>, V: Into<bson::Bson>>(key: K, value: V) -> Self {
        let mut resp = Self::ok();
        resp.extra.insert(key.into(), value.into());
        resp
    }

    pub fn error(code: i32, message: &str) -> Self {
        Self {
            ok: 0.0,
            errmsg: Some(message.to_string()),
            code: Some(code),
            extra: HashMap::new(),
        }
    }
}

/// Insert result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertResult {
    pub ok: f64,
    pub n: i64,
}

/// Update result
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateResult {
    pub ok: f64,
    pub n: i64,
    pub n_modified: i64,
}

/// Delete result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    pub ok: f64,
    pub n: i64,
}

/// Find result with cursor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindResult {
    pub ok: f64,
    pub cursor: CursorResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CursorResult {
    pub id: i64,
    pub ns: String,
    pub first_batch: Vec<bson::Document>,
}
