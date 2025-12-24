# Quick Start Guide

Get LumaDB running in under 5 minutes.

## Prerequisites

- Docker (recommended) or Rust 1.77+
- 4GB RAM minimum
- Linux, macOS, or Windows

## Option 1: Docker (Recommended)

```bash
# Pull and run
docker run -d \
  --name lumadb \
  -p 8080:8080 \
  -p 9092:9092 \
  -p 4000:4000 \
  -v lumadb-data:/data \
  ghcr.io/abiolaogu/lumadb:latest

# Verify it's running
curl http://localhost:8080/health
```

## Option 2: Docker Compose

```bash
# Clone repository
git clone https://github.com/abiolaogu/LumaDB.git
cd LumaDB

# Start with Docker Compose
docker-compose -f deploy/docker/docker-compose.yml up -d

# Check logs
docker-compose logs -f
```

## Option 3: Build from Source

```bash
# Clone and build
git clone https://github.com/abiolaogu/LumaDB.git
cd LumaDB
make build

# Run
./crates/target/release/lumadb server --config configs/lumadb.production.yaml
```

## Verify Installation

```bash
# Health check
curl http://localhost:8080/health

# Expected response:
# {"status":"healthy","version":"0.1.0-beta.1"}
```

## Next Steps

- [Configuration Guide](../operations/configuration.md)
- [API Reference](../api-reference/index.md)
- [Kafka Compatibility](../api-reference/kafka.md)
