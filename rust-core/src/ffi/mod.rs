//! Foreign Function Interface for LumaDB
//!
//! Provides C-compatible FFI bindings for Go and Python integration.
//! Uses a handle-based approach for safe cross-language resource management.

mod handles;
mod types;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::Arc;

use crate::Database;
use crate::config::Config;
use crate::error::LumaError;
use crate::types::Document;

use handles::{HandleMap, ENGINES};

/// Result code for FFI operations
#[repr(C)]
pub enum LumaResult {
    Ok = 0,
    ErrInvalidHandle = -1,
    ErrInvalidArgument = -2,
    ErrNotFound = -3,
    ErrAlreadyExists = -4,
    ErrIO = -5,
    ErrCorruption = -6,
    ErrFull = -7,
    ErrInternal = -8,
}

impl From<LumaError> for LumaResult {
    fn from(err: LumaError) -> Self {
        match err {
            LumaError::DocumentNotFound(_) | LumaError::CollectionNotFound(_) => LumaResult::ErrNotFound,
            LumaError::DocumentExists(_) => LumaResult::ErrAlreadyExists,
            LumaError::Io(_) => LumaResult::ErrIO,
            LumaError::Corruption(_) => LumaResult::ErrCorruption,
            LumaError::MemoryLimitExceeded { .. } => LumaResult::ErrFull,
            _ => LumaResult::ErrInternal,
        }
    }
}

/// Opaque handle to a database engine
pub type LumaHandle = u64;

/// Buffer for returning data to callers
#[repr(C)]
pub struct LumaBuffer {
    data: *mut u8,
    len: usize,
    capacity: usize,
}

impl LumaBuffer {
    fn new(data: Vec<u8>) -> Self {
        let mut data = data.into_boxed_slice();
        let ptr = data.as_mut_ptr();
        let len = data.len();
        std::mem::forget(data);
        LumaBuffer {
            data: ptr,
            len,
            capacity: len,
        }
    }

    fn empty() -> Self {
        LumaBuffer {
            data: ptr::null_mut(),
            len: 0,
            capacity: 0,
        }
    }
}

// ============================================================================
// Database Lifecycle
// ============================================================================

/// Open a database with the given path and options
///
/// # Safety
/// The path must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn luma_open(
    path: *const c_char,
    config_json: *const c_char,
    handle_out: *mut LumaHandle,
) -> LumaResult {
    if path.is_null() || handle_out.is_null() {
        return LumaResult::ErrInvalidArgument;
    }

    let _name_str = match unsafe { CStr::from_ptr(path).to_str() } {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    // Parse config if provided
    let config = if !config_json.is_null() {
        match CStr::from_ptr(config_json).to_str() {
            Ok(json) => match serde_json::from_str::<Config>(json) {
                Ok(cfg) => cfg,
                Err(_) => Config::default(),
            },
            Err(_) => Config::default(),
        }
    } else {
        let mut cfg = Config::default();
        cfg.data_dir = _name_str.into();
        cfg
    };

    // Create the storage engine
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    let engine = match rt.block_on(Database::open(config)) {
        Ok(e) => e,
        Err(e) => return e.into(),
    };

    // Store handle
    let handle = ENGINES.get_or_init(HandleMap::new).insert(Arc::new(engine));
    *handle_out = handle;

    LumaResult::Ok
}

/// Close a database and release resources
///
/// # Safety
/// The handle must be a valid database handle.
#[no_mangle]
pub unsafe extern "C" fn luma_close(handle: LumaHandle) -> LumaResult {
    if let Some(map) = ENGINES.get() {
        if map.remove(handle).is_some() {
            return LumaResult::Ok;
        }
    }
    LumaResult::ErrInvalidHandle
}

// ============================================================================
// Collection Operations
// ============================================================================

/// Create a collection
#[no_mangle]
pub unsafe extern "C" fn luma_create_collection(
    handle: LumaHandle,
    name: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(async {
        let _ = engine.collection(name_str);
        Ok::<(), LumaError>(())
    }) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

/// List all collections
#[no_mangle]
pub unsafe extern "C" fn luma_list_collections(
    handle: LumaHandle,
    names_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let names = engine.list_collections();
    let json = serde_json::to_vec(&names).unwrap_or_else(|_| b"[]".to_vec());

    if !names_out.is_null() {
        *names_out = LumaBuffer::new(json);
    }
    LumaResult::Ok
}

/// Drop a collection
#[no_mangle]
pub unsafe extern "C" fn luma_drop_collection(
    handle: LumaHandle,
    name: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(async {
        engine.drop_collection(name_str).await
    }) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

/// Create a secondary index
#[no_mangle]
pub unsafe extern "C" fn luma_create_index(
    handle: LumaHandle,
    collection: *const c_char,
    name: *const c_char,
    field: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let field_str = match CStr::from_ptr(field).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    match engine.create_index(collection_str, name_str, field_str) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

// ============================================================================
// Document Operations
// ============================================================================

/// Insert a document into a collection
///
/// # Safety
/// All pointers must be valid. The document must be valid JSON.
#[no_mangle]
pub unsafe extern "C" fn luma_insert(
    handle: LumaHandle,
    collection: *const c_char,
    document_json: *const c_char,
    id_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let doc_str = match CStr::from_ptr(document_json).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    // Parse data map
    let mut data: std::collections::HashMap<String, crate::types::Value> = match serde_json::from_str(doc_str) {
        Ok(d) => d,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    // Extract ID if present
    let id = if let Some(crate::types::Value::String(s)) = data.remove("_id") {
        s
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    
    // Create new document
    let doc = crate::types::Document::with_id(id, data);

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(engine.insert(collection_str, doc)) {
        Ok(id) => {
            if !id_out.is_null() {
                *id_out = LumaBuffer::new(id.into_bytes());
            }
            LumaResult::Ok
        }
        Err(e) => e.into(),
    }
}

/// Insert a document using MessagePack (binary)
///
/// # Safety
/// input_data must be a valid pointer of length input_len.
#[no_mangle]
pub unsafe extern "C" fn luma_insert_mp(
    handle: LumaHandle,
    collection: *const c_char,
    input_data: *const u8,
    input_len: usize,
    id_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let slice = std::slice::from_raw_parts(input_data, input_len);
    let doc: crate::types::Document = match rmp_serde::from_slice(slice) {
        Ok(d) => d,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(engine.insert(collection_str, doc)) {
        Ok(id) => {
            if !id_out.is_null() {
                *id_out = LumaBuffer::new(id.into_bytes());
            }
            LumaResult::Ok
        }
        Err(e) => e.into(),
    }
}

/// Get a document by ID
#[no_mangle]
pub unsafe extern "C" fn luma_get(
    handle: LumaHandle,
    collection: *const c_char,
    id: *const c_char,
    document_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let id_str = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    let id_string = id_str.to_string();
    match rt.block_on(engine.get(collection_str, &id_string)) {
        Ok(Some(doc)) => {
            if !document_out.is_null() {
                let json = serde_json::to_vec(&doc).unwrap_or_default();
                *document_out = LumaBuffer::new(json);
            }
            LumaResult::Ok
        }
        Ok(None) => LumaResult::ErrNotFound,
        Err(e) => e.into(),
    }
}

/// Update a document by ID
#[no_mangle]
pub unsafe extern "C" fn luma_update(
    handle: LumaHandle,
    collection: *const c_char,
    id: *const c_char,
    updates_json: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let id_str = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let updates_str = match CStr::from_ptr(updates_json).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let updates: serde_json::Value = match serde_json::from_str(updates_str) {
        Ok(u) => u,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    let id_string = id_str.to_string();
    let doc_struct: Document = match serde_json::from_value(updates) {
        Ok(d) => d,
        Err(e) => return LumaError::from(e).into(),
    };
    match rt.block_on(engine.update(collection_str, &id_string, doc_struct)) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

/// Delete a document by ID
#[no_mangle]
pub unsafe extern "C" fn luma_delete(
    handle: LumaHandle,
    collection: *const c_char,
    id: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let id_str = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    let id_string = id_str.to_string();
    match rt.block_on(engine.delete(collection_str, &id_string)) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

// ============================================================================
// Query Operations
// ============================================================================

/// Execute a query and return results
#[no_mangle]
pub unsafe extern "C" fn luma_query(
    handle: LumaHandle,
    collection: *const c_char,
    query_json: *const c_char,
    results_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let query_str = match CStr::from_ptr(query_json).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    // Parse query
    // If empty or invalid JSON, we treat as empty query (scan all)
    let query: crate::types::Query = serde_json::from_str(query_str).unwrap_or(crate::types::Query {
        filter: None,
        limit: None,
    });

    match rt.block_on(engine.query(collection_str, query)) {
        Ok(docs) => {
             let json = serde_json::to_vec(&docs).unwrap_or_else(|_| b"[]".to_vec());
             *results_out = LumaBuffer::new(json);
             LumaResult::Ok
        },
        Err(e) => e.into(),
    }
}

/// Execute a query using MessagePack (binary)
///
/// # Safety
/// query_data must be a valid pointer of length query_len.
#[no_mangle]
pub unsafe extern "C" fn luma_query_mp(
    handle: LumaHandle,
    collection: *const c_char,
    query_data: *const u8,
    query_len: usize,
    results_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let slice = std::slice::from_raw_parts(query_data, query_len);
    let query: crate::types::Query = match rmp_serde::from_slice(slice) {
        Ok(q) => q,
        Err(_) => crate::types::Query { // Fallback or strict error? Let's treat as empty
            filter: None,
            limit: None,
        }
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(engine.query(collection_str, query)) {
        Ok(docs) => {
             // Encode results as MessagePack
             let mp_data = match rmp_serde::to_vec(&docs) {
                 Ok(data) => data,
                 Err(_) => return LumaResult::ErrInternal,
             };
             *results_out = LumaBuffer::new(mp_data);
             LumaResult::Ok
        },
        Err(e) => e.into(),
    }
}

/// Search for similar vectors
#[no_mangle]
pub unsafe extern "C" fn luma_search_vector(
    handle: LumaHandle,
    vector_json: *const c_char,
    k: usize,
    results_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let vector_str = match CStr::from_ptr(vector_json).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let query_vector: Vec<f32> = match serde_json::from_str(vector_str) {
        Ok(v) => v,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    // Call search_vector on Database
    // Note: search_vector is synchronous in Database impl shown previously, no need for async block?
    // But Database::search_vector delegates to shards which is sync.
    // engine.search_vector returns Vec<(Vec<u8>, f32)>
    
    let results = engine.search_vector(&query_vector, k);
    
    // Convert results (Vec<(Vec<u8>, f32)>) to JSON friendly format
    // ID is stored as bytes, convert to String roughly
    let json_results: Vec<serde_json::Value> = results.into_iter().map(|(id_bytes, score)| {
        serde_json::json!({
            "id": String::from_utf8_lossy(&id_bytes),
            "score": score
        })
    }).collect();

    let json = serde_json::to_vec(&json_results).unwrap_or_else(|_| b"[]".to_vec());
    
    if !results_out.is_null() {
        *results_out = LumaBuffer::new(json);
    }

    LumaResult::Ok
}

// ============================================================================
// Batch Operations
// ============================================================================

/// Insert multiple documents in a single batch
#[no_mangle]
pub unsafe extern "C" fn luma_batch_insert(
    handle: LumaHandle,
    collection: *const c_char,
    documents_json: *const c_char,
    count_out: *mut usize,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let docs_str = match CStr::from_ptr(documents_json).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let docs: Vec<serde_json::Value> = match serde_json::from_str(docs_str) {
        Ok(d) => d,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    let docs_struct: Vec<Document> = docs.into_iter().filter_map(|v| serde_json::from_value(v).ok()).collect();
    match rt.block_on(engine.batch_insert(collection_str, docs_struct)) {
        Ok(ids) => {
            let count = ids.len();
            *count_out = count;
            LumaResult::Ok
        }
        Err(e) => e.into(),
    }
}

// ============================================================================
// Memory Management
// ============================================================================

/// Free a buffer returned by Luma functions
#[no_mangle]
pub unsafe extern "C" fn luma_buffer_free(buffer: *mut LumaBuffer) {
    if !buffer.is_null() && !(*buffer).data.is_null() {
        let _ = Vec::from_raw_parts(
            (*buffer).data,
            (*buffer).len,
            (*buffer).capacity,
        );
        (*buffer).data = ptr::null_mut();
        (*buffer).len = 0;
        (*buffer).capacity = 0;
    }
}

/// Get the data pointer from a buffer
#[no_mangle]
pub unsafe extern "C" fn luma_buffer_data(buffer: *const LumaBuffer) -> *const u8 {
    if buffer.is_null() {
        ptr::null()
    } else {
        (*buffer).data
    }
}

/// Get the length of a buffer
#[no_mangle]
pub unsafe extern "C" fn luma_buffer_len(buffer: *const LumaBuffer) -> usize {
    if buffer.is_null() {
        0
    } else {
        (*buffer).len
    }
}

// ============================================================================
// Snapshot Operations
// ============================================================================

/// Save database snapshot to file
///
/// # Safety
/// path must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn luma_snapshot_save(
    handle: LumaHandle,
    path: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(engine.backup(path_str)) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

/// Load database snapshot from file
///
/// # Safety
/// path must be a valid null-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn luma_snapshot_load(
    handle: LumaHandle,
    path: *const c_char,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return LumaResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    match rt.block_on(engine.restore(path_str)) {
        Ok(_) => LumaResult::Ok,
        Err(e) => e.into(),
    }
}

// ============================================================================
// Statistics and Info
// ============================================================================

/// Get database statistics as JSON
#[no_mangle]
pub unsafe extern "C" fn luma_stats(
    handle: LumaHandle,
    stats_out: *mut LumaBuffer,
) -> LumaResult {
    let engine = match ENGINES.get().and_then(|m| m.get(handle)) {
        Some(e) => e,
        None => return LumaResult::ErrInvalidHandle,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return LumaResult::ErrInternal,
    };

    let stats = engine.stats();
    let json = serde_json::to_vec(&stats).unwrap_or_default();

    if !stats_out.is_null() {
        *stats_out = LumaBuffer::new(json);
    }

    LumaResult::Ok
}
