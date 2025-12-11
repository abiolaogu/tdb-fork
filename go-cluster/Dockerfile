# Stage 1: Build Rust Core
FROM rust:1.75-bullseye AS rust-builder
WORKDIR /app

# Copy Rust source
COPY rust-core ./rust-core

# Build shared library
WORKDIR /app/rust-core
RUN cargo build --release

# Stage 2: Build Go Cluster
FROM golang:1.21-bullseye AS go-builder
WORKDIR /app

# Copy Go definitions
COPY go-cluster/go.mod go-cluster/go.sum ./go-cluster/
WORKDIR /app/go-cluster
RUN go mod download

# Copy source code
COPY go-cluster .

# Copy Rust artifacts to expected location for CGO
# The CGO directive expects: ${SRCDIR}/../../../rust-core/target/release
# SRCDIR is /app/go-cluster/pkg/core
# So ../../../ is /app
# We need /app/rust-core/target/release populated
COPY --from=rust-builder /app/rust-core/target/release/libluma_core.so /app/rust-core/target/release/libluma_core.so

# Build the binary
# CGO_ENABLED=1 is default, but good to be explicit
# LD_LIBRARY_PATH isn't strictly needed at build time if -L is correct, but safe to add
ENV CGO_ENABLED=1
RUN go build -o luma-node ./cmd/server

# Stage 3: Runtime
FROM debian:bullseye-slim

WORKDIR /app

# Install necessary runtime libs (e.g. for SSL, etc)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy binary
COPY --from=go-builder /app/go-cluster/luma-node .

# Copy shared library to dynamic linker path
COPY --from=rust-builder /app/rust-core/target/release/libluma_core.so /usr/lib/

# Expose ports
EXPOSE 8080 9090 10000

# Set entrypoint
CMD ["./luma-node"]
