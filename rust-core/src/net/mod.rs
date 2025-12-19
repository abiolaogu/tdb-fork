//! Network module for high-performance networking
//!
//! Provides feature-gated stubs for:
//! - DPDK (kernel-bypass networking)
//! - RDMA (low-latency InfiniBand)

pub mod dpdk;
pub mod rdma;

pub use dpdk::dpdk_integration;
pub use rdma::rdma_transport;
