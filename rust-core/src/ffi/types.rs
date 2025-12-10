//! FFI type definitions
//!
//! C-compatible type definitions for cross-language interop.

use std::ffi::c_char;

/// Query options for find operations
#[repr(C)]
#[allow(dead_code)]
pub struct LumaQueryOptions {
    pub limit: i32,
    pub offset: i32,
    pub order_by: *const c_char,
    pub order_desc: bool,
}

/// Index options
#[repr(C)]
#[allow(dead_code)]
pub struct LumaIndexOptions {
    pub unique: bool,
    pub sparse: bool,
}

/// Transaction options
#[repr(C)]
#[allow(dead_code)]
pub struct LumaTransactionOptions {
    pub timeout_ms: u32,
    pub read_only: bool,
}
