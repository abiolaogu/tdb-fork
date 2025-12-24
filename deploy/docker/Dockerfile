# LumaDB Production Dockerfile
# Multi-stage build with security hardening
# Pure Rust Architecture - 100x faster streaming

# ============================================================================
# Stage 1: Build Rust Core
# ============================================================================
FROM rust:1.77-bookworm AS rust-builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake \
    clang \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    nasm \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace Cargo files for dependency caching
COPY crates/Cargo.toml crates/Cargo.lock ./

# Create crate structure for dependency caching
RUN mkdir -p lumadb-common/src lumadb-storage/src lumadb-streaming/src \
    lumadb-query/src lumadb-protocol/src lumadb-security/src lumadb-raft/src \
    lumadb-txn/src lumadb-cluster/src lumadb-api/src lumadb-admin/src lumadb/src

# Copy all Cargo.toml files
COPY crates/lumadb-common/Cargo.toml lumadb-common/
COPY crates/lumadb-storage/Cargo.toml lumadb-storage/
COPY crates/lumadb-streaming/Cargo.toml lumadb-streaming/
COPY crates/lumadb-query/Cargo.toml lumadb-query/
COPY crates/lumadb-protocol/Cargo.toml lumadb-protocol/
COPY crates/lumadb-security/Cargo.toml lumadb-security/
COPY crates/lumadb-raft/Cargo.toml lumadb-raft/
COPY crates/lumadb-txn/Cargo.toml lumadb-txn/
COPY crates/lumadb-cluster/Cargo.toml lumadb-cluster/
COPY crates/lumadb-api/Cargo.toml lumadb-api/
COPY crates/lumadb-admin/Cargo.toml lumadb-admin/
COPY crates/lumadb/Cargo.toml lumadb/

# Create dummy source files for dependency caching
RUN echo "pub fn dummy() {}" > lumadb-common/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-storage/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-streaming/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-query/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-protocol/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-security/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-raft/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-txn/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-cluster/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-api/src/lib.rs && \
    echo "pub fn dummy() {}" > lumadb-admin/src/lib.rs && \
    echo "fn main() {}" > lumadb/src/main.rs

# Build dependencies only (cached layer)
RUN cargo build --release 2>/dev/null || true

# Copy actual source code
COPY crates/ .

# Build with production optimizations
ENV RUSTFLAGS="-C opt-level=3 -C lto=thin -C codegen-units=1"
RUN cargo build --release

# ============================================================================
# Stage 2: Production Image (Debian Slim)
# ============================================================================
FROM debian:bookworm-slim AS production

# Labels
LABEL org.opencontainers.image.source="https://github.com/abiolaogu/LumaDB"
LABEL org.opencontainers.image.description="LumaDB - Ultra-fast unified database (100x faster streaming)"
LABEL org.opencontainers.image.licenses="Apache-2.0"
LABEL org.opencontainers.image.version="0.1.0-beta.1"

# Security: Create non-root user
RUN groupadd -r lumadb && useradd -r -g lumadb lumadb

# Install minimal runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create directories
RUN mkdir -p /var/lib/lumadb /var/log/lumadb /etc/lumadb \
    && chown -R lumadb:lumadb /var/lib/lumadb /var/log/lumadb /etc/lumadb

# Copy binary from builder
COPY --from=rust-builder /build/target/release/lumadb /usr/local/bin/

# Copy configuration
COPY configs/lumadb.production.yaml /etc/lumadb/lumadb.yaml

# Set permissions
RUN chmod 755 /usr/local/bin/lumadb && \
    chmod 700 /var/lib/lumadb /var/log/lumadb

# Switch to non-root
USER lumadb

# Environment
ENV LUMADB_DATA_DIR=/var/lib/lumadb
ENV LUMADB_LOG_LEVEL=info
ENV RUST_BACKTRACE=0

# Expose ports
EXPOSE 8080 9092 4000 50051 9100

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -sf http://localhost:8080/health || exit 1

# Volumes
VOLUME ["/var/lib/lumadb", "/var/log/lumadb"]

# Entry point
ENTRYPOINT ["/usr/local/bin/lumadb"]
CMD ["server", "--config", "/etc/lumadb/lumadb.yaml"]

# ============================================================================
# Stage 3: Development Image
# ============================================================================
FROM production AS development

USER root

# Install development tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    vim \
    htop \
    strace \
    && rm -rf /var/lib/apt/lists/*

USER lumadb

# Override for development
ENV LUMADB_LOG_LEVEL=debug
ENV RUST_BACKTRACE=1
