# deploy/docker/Dockerfile

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

# Cache dependencies
COPY rust-core/Cargo.toml rust-core/Cargo.lock ./
# Create dummy source to cache dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -rf src target/release/deps/lumadb*

# Build with all optimizations
COPY rust-core/ .
ENV RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C lto=fat -C codegen-units=1"
# Note: features list might need adjustment based on Cargo.toml actual content
RUN cargo build --release 

# ============================================================================
# Stage 2: Build Go Components
# ============================================================================
FROM golang:1.22-bookworm AS go-builder

WORKDIR /build

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build with optimizations
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w \
        -X main.version=$(git describe --tags --always 2>/dev/null || echo 'dev') \
        -X main.commit=$(git rev-parse HEAD 2>/dev/null || echo 'unknown') \
        -X main.buildTime=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    -o lumadb \
    ./cmd/lumadb

# ============================================================================
# Stage 3: Production Image (Distroless for Security)
# ============================================================================
FROM gcr.io/distroless/cc-debian12:nonroot AS production

# Copy binaries
COPY --from=rust-builder /build/target/release/liblumadb_core.so /usr/local/lib/
COPY --from=go-builder /build/lumadb /usr/local/bin/

# Copy default configuration
COPY configs/lumadb.production.yaml /etc/lumadb/lumadb.yaml

# Set library path
ENV LD_LIBRARY_PATH=/usr/local/lib

# Non-root user (distroless provides nonroot user)
USER nonroot:nonroot

# Expose ports
EXPOSE 8080 9092 4000 9100

# Entry point
ENTRYPOINT ["/usr/local/bin/lumadb"]
CMD ["server", "--config", "/etc/lumadb/lumadb.yaml"]
