//! FFI type definitions
//!
//! C-compatible type definitions for cross-language interop.

use std::ffi::c_char;

/// Query options for find operations
#[repr(C)]
pub struct TdbQueryOptions {
    pub limit: i32,
    pub offset: i32,
    pub order_by: *const c_char,
    pub order_desc: bool,
}

/// Index options
#[repr(C)]
pub struct TdbIndexOptions {
    pub unique: bool,
    pub sparse: bool,
}

/// Transaction options
#[repr(C)]
pub struct TdbTransactionOptions {
    pub timeout_ms: u32,
    pub read_only: bool,
}
