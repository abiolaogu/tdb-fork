# LumaDB Multi-Protocol Compatibility Layer

Drop-in replacement for AWS DynamoDB, Cloudflare D1, and Turso (libSQL).

## Quick Start

### Run All Servers

```bash
cargo run --bin lumadb-compat-server
```

Output:
```
╔═══════════════════════════════════════════════════════════╗
║         LumaDB Multi-Protocol Compatibility Server         ║
╠═══════════════════════════════════════════════════════════╣
║  Main Server:    http://0.0.0.0:9000                       ║
║  DynamoDB:       http://0.0.0.0:8000                       ║
║  D1:             http://0.0.0.0:8787                       ║
║  Turso:          http://0.0.0.0:8080                       ║
╚═══════════════════════════════════════════════════════════╝
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 9000 | Main server (health/metrics) |
| `DYNAMODB_PORT` | 8000 | DynamoDB API |
| `D1_PORT` | 8787 | Cloudflare D1 API |
| `TURSO_PORT` | 8080 | Turso/libSQL API |
| `ENABLE_CORS` | true | CORS support |
| `ENABLE_TRACING` | true | Request tracing |

---

## Endpoints

### Main Server (port 9000)

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /ready` | Readiness (storage check) |
| `GET /metrics` | Server metrics |
| `GET /info` | Server info |

### DynamoDB (port 8000)

All operations via `POST /` with `x-amz-target` header.

### D1 (port 8787)

| Endpoint | Description |
|----------|-------------|
| `POST /query` | Single query |
| `POST /batch` | Batch queries |
| Cloudflare API paths also supported |

### Turso (port 8080)

| Endpoint | Description |
|----------|-------------|
| `POST /v1/execute` | Single statement |
| `POST /v1/batch` | Batch statements |
| `POST /v2/pipeline` | Transactions |
A high-performance, drop-in compatibility layer for LumaDB that provides native support for **AWS DynamoDB**, **Cloudflare D1**, and **Turso/LibSQL** protocols on a single server port.

## 🚀 Features

- **Multi-Protocol Support**: Serve DynamoDB, D1, and Turso clients from port 8000.
- **High Performance**: Sub-millisecond translation overhead, connection pooling, and LRU caching.
- **100% Compatibility**: Works with official SDKs (AWS SDK, @cloudflare/d1, @libsql/client).
- **Production Ready**: Includes metrics (Prometheus), structured logging, and graceful shutdown.
- **Developer Friendly**: Comprehensive examples, migration guides, and local development setup.

## 📚 Documentation

- **[Deployment Guide](docs/DEPLOYMENT.md)**: Docker, Kubernetes, and Monitoring setup.
- **[Migration Guide](docs/MIGRATION.md)**: How to migrate from DynamoDB, D1, or Turso.
- **[API Reference](docs/API_REFERENCE.md)**: Detailed protocol specifications.
- **[Testing & Benchmarks](docs/TESTING.md)**: Test suite and performance targets.

## 🛠️ Client Examples

Ready-to-run examples in multiple languages:

| Protocol | Languages | Path |
|----------|-----------|------|
| **DynamoDB** | Node.js | `examples/dynamodb/nodejs/` |
| **DynamoDB** | Go | `examples/dynamodb/go/` |
| **Cloudflare D1** | TypeScript | `examples/d1/typescript/` |
| **Turso** | Python | `examples/turso/python/` |

## 📦 Quick Start

### Local Development (Docker Compose)
```bash
docker-compose up -d
# Server: http://localhost:8000
# Metrics: http://localhost:9090
# Grafana: http://localhost:3000
```

### Manual Run
```bash
# Run server
cargo run -p lumadb-compat-server

# Run tests
cargo test -p lumadb-multicompat
```

## 🏗️ Architecture

The server uses a single-port, path-based routing architecture:

- `POST /dynamodb` -> DynamoDB Adapter -> Data Store
- `POST /d1/*` -> D1 Adapter -> Data Store
- `POST /turso/*` -> Turso Adapter -> Data Store

## 📊 Performance Targets

- **DynamoDB**: < 10ms p50
- **SQL (D1/Turso)**: < 5ms p50
- **Throughput**: 10K+ TPS per instance

## License

MIT / Apache 2.0
