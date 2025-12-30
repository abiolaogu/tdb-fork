# Multi-stage build for LumaDB Multi-Protocol Server

# Stage 1: Builder
FROM rust:1.75-slim-bookworm as builder

WORKDIR /usr/src/app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifest and source
COPY . .

# Build the binary
RUN cargo build --release -p lumadb-multicompat --bin lumadb-compat-server

# Stage 2: Runtime
FROM debian:bookworm-slim as runtime

# Create non-root user
RUN groupadd -r lumadb && useradd -r -g lumadb lumadb

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /usr/src/app/target/release/lumadb-compat-server /app/lumadb-compat-server

# Set ownership
RUN chown -R lumadb:lumadb /app

# Switch to non-root user
USER lumadb

# Expose port
EXPOSE 8000

# Environment variables with sensible defaults
ENV RUST_LOG=info \
    PORT=8000 \
    HOST=0.0.0.0

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD curl -f http://localhost:8000/health || exit 1

# Entrypoint
ENTRYPOINT ["./lumadb-compat-server"]
