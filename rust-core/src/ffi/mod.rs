//! Foreign Function Interface for TDB+
//!
//! Provides C-compatible FFI bindings for Go and Python integration.
//! Uses a handle-based approach for safe cross-language resource management.

mod handles;
mod types;

use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::ptr;
use std::sync::Arc;

use crate::storage::engine::StorageEngine;
use crate::config::Config;
use crate::error::TdbError;

use handles::{HandleMap, ENGINES};

/// Result code for FFI operations
#[repr(C)]
pub enum TdbResult {
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

impl From<TdbError> for TdbResult {
    fn from(err: TdbError) -> Self {
        match err {
            TdbError::NotFound(_) => TdbResult::ErrNotFound,
            TdbError::AlreadyExists(_) => TdbResult::ErrAlreadyExists,
            TdbError::Io(_) => TdbResult::ErrIO,
            TdbError::Corruption(_) => TdbResult::ErrCorruption,
            TdbError::MemoryLimitExceeded => TdbResult::ErrFull,
            _ => TdbResult::ErrInternal,
        }
    }
}

/// Opaque handle to a database engine
pub type TdbHandle = u64;

/// Buffer for returning data to callers
#[repr(C)]
pub struct TdbBuffer {
    data: *mut u8,
    len: usize,
    capacity: usize,
}

impl TdbBuffer {
    fn new(data: Vec<u8>) -> Self {
        let mut data = data.into_boxed_slice();
        let ptr = data.as_mut_ptr();
        let len = data.len();
        std::mem::forget(data);
        TdbBuffer {
            data: ptr,
            len,
            capacity: len,
        }
    }

    fn empty() -> Self {
        TdbBuffer {
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
pub unsafe extern "C" fn tdb_open(
    path: *const c_char,
    config_json: *const c_char,
    handle_out: *mut TdbHandle,
) -> TdbResult {
    if path.is_null() || handle_out.is_null() {
        return TdbResult::ErrInvalidArgument;
    }

    let path_str = match CStr::from_ptr(path).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
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
        cfg.data_dir = path_str.into();
        cfg
    };

    // Create the storage engine
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    let engine = match rt.block_on(StorageEngine::open(config)) {
        Ok(e) => e,
        Err(e) => return e.into(),
    };

    // Store handle
    let handle = ENGINES.insert(Arc::new(engine));
    *handle_out = handle;

    TdbResult::Ok
}

/// Close a database and release resources
///
/// # Safety
/// The handle must be a valid database handle.
#[no_mangle]
pub unsafe extern "C" fn tdb_close(handle: TdbHandle) -> TdbResult {
    if ENGINES.remove(handle).is_some() {
        TdbResult::Ok
    } else {
        TdbResult::ErrInvalidHandle
    }
}

// ============================================================================
// Collection Operations
// ============================================================================

/// Create a collection
#[no_mangle]
pub unsafe extern "C" fn tdb_create_collection(
    handle: TdbHandle,
    name: *const c_char,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.create_collection(name_str)) {
        Ok(_) => TdbResult::Ok,
        Err(e) => e.into(),
    }
}

/// Drop a collection
#[no_mangle]
pub unsafe extern "C" fn tdb_drop_collection(
    handle: TdbHandle,
    name: *const c_char,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let name_str = match CStr::from_ptr(name).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.drop_collection(name_str)) {
        Ok(_) => TdbResult::Ok,
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
pub unsafe extern "C" fn tdb_insert(
    handle: TdbHandle,
    collection: *const c_char,
    document_json: *const c_char,
    id_out: *mut TdbBuffer,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let doc_str = match CStr::from_ptr(document_json).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    // Parse document
    let doc: serde_json::Value = match serde_json::from_str(doc_str) {
        Ok(d) => d,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.insert(collection_str, doc)) {
        Ok(id) => {
            if !id_out.is_null() {
                *id_out = TdbBuffer::new(id.into_bytes());
            }
            TdbResult::Ok
        }
        Err(e) => e.into(),
    }
}

/// Get a document by ID
#[no_mangle]
pub unsafe extern "C" fn tdb_get(
    handle: TdbHandle,
    collection: *const c_char,
    id: *const c_char,
    document_out: *mut TdbBuffer,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let id_str = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.get(collection_str, id_str)) {
        Ok(Some(doc)) => {
            if !document_out.is_null() {
                let json = serde_json::to_vec(&doc).unwrap_or_default();
                *document_out = TdbBuffer::new(json);
            }
            TdbResult::Ok
        }
        Ok(None) => TdbResult::ErrNotFound,
        Err(e) => e.into(),
    }
}

/// Update a document by ID
#[no_mangle]
pub unsafe extern "C" fn tdb_update(
    handle: TdbHandle,
    collection: *const c_char,
    id: *const c_char,
    updates_json: *const c_char,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let id_str = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let updates_str = match CStr::from_ptr(updates_json).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let updates: serde_json::Value = match serde_json::from_str(updates_str) {
        Ok(u) => u,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.update(collection_str, id_str, updates)) {
        Ok(_) => TdbResult::Ok,
        Err(e) => e.into(),
    }
}

/// Delete a document by ID
#[no_mangle]
pub unsafe extern "C" fn tdb_delete(
    handle: TdbHandle,
    collection: *const c_char,
    id: *const c_char,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let id_str = match CStr::from_ptr(id).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.delete(collection_str, id_str)) {
        Ok(_) => TdbResult::Ok,
        Err(e) => e.into(),
    }
}

// ============================================================================
// Query Operations
// ============================================================================

/// Execute a query and return results
#[no_mangle]
pub unsafe extern "C" fn tdb_query(
    handle: TdbHandle,
    collection: *const c_char,
    query_json: *const c_char,
    results_out: *mut TdbBuffer,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let query_str = match CStr::from_ptr(query_json).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.query(collection_str, query_str)) {
        Ok(results) => {
            if !results_out.is_null() {
                let json = serde_json::to_vec(&results).unwrap_or_default();
                *results_out = TdbBuffer::new(json);
            }
            TdbResult::Ok
        }
        Err(e) => e.into(),
    }
}

// ============================================================================
// Batch Operations
// ============================================================================

/// Insert multiple documents in a single batch
#[no_mangle]
pub unsafe extern "C" fn tdb_batch_insert(
    handle: TdbHandle,
    collection: *const c_char,
    documents_json: *const c_char,
    count_out: *mut usize,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let collection_str = match CStr::from_ptr(collection).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let docs_str = match CStr::from_ptr(documents_json).to_str() {
        Ok(s) => s,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let docs: Vec<serde_json::Value> = match serde_json::from_str(docs_str) {
        Ok(d) => d,
        Err(_) => return TdbResult::ErrInvalidArgument,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    match rt.block_on(engine.batch_insert(collection_str, docs)) {
        Ok(count) => {
            if !count_out.is_null() {
                *count_out = count;
            }
            TdbResult::Ok
        }
        Err(e) => e.into(),
    }
}

// ============================================================================
// Memory Management
// ============================================================================

/// Free a buffer returned by TDB functions
#[no_mangle]
pub unsafe extern "C" fn tdb_buffer_free(buffer: *mut TdbBuffer) {
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
pub unsafe extern "C" fn tdb_buffer_data(buffer: *const TdbBuffer) -> *const u8 {
    if buffer.is_null() {
        ptr::null()
    } else {
        (*buffer).data
    }
}

/// Get the length of a buffer
#[no_mangle]
pub unsafe extern "C" fn tdb_buffer_len(buffer: *const TdbBuffer) -> usize {
    if buffer.is_null() {
        0
    } else {
        (*buffer).len
    }
}

// ============================================================================
// Statistics and Info
// ============================================================================

/// Get database statistics as JSON
#[no_mangle]
pub unsafe extern "C" fn tdb_stats(
    handle: TdbHandle,
    stats_out: *mut TdbBuffer,
) -> TdbResult {
    let engine = match ENGINES.get(handle) {
        Some(e) => e,
        None => return TdbResult::ErrInvalidHandle,
    };

    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(_) => return TdbResult::ErrInternal,
    };

    let stats = rt.block_on(engine.stats());
    let json = serde_json::to_vec(&stats).unwrap_or_default();

    if !stats_out.is_null() {
        *stats_out = TdbBuffer::new(json);
    }

    TdbResult::Ok
}

/// Get the version string
#[no_mangle]
pub extern "C" fn tdb_version() -> *const c_char {
    static VERSION: &str = "1.0.0\0";
    VERSION.as_ptr() as *const c_char
}
