//! I/O Subsystem
//!
//! High-performance I/O operations:
//! - io_uring for Linux
//! - Direct I/O bypass
//! - Batched operations
//! - Zero-copy where possible

pub mod uring;

pub use uring::{IoUring, UringConfig, BatchedIo, DirectFile};
