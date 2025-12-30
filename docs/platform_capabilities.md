# LumaDB - Unified Database Platform

**Version:** 0.1.0-beta.1 | **Binary Size:** 8.3 MB | **Language:** Pure Rust

---

## Overview

LumaDB is a unified database platform providing wire-protocol compatibility with multiple database systems in a single binary. It serves as a direct drop-in replacement for multiple data infrastructure components.

---

## Technical Stack

| Component | Technology |
|-----------|------------|
| Core Language | Rust 1.77+ |
| Async Runtime | Tokio |
| Storage Engine | LSM-Tree + Columnar (Arrow) |
| Consensus | Multi-Raft |
| Transactions | MVCC with 2PC |
| Networking | io_uring (Linux), Zero-copy |
| Search | Tantivy (Full-text), HNSW (Vector) |

---

## Binary Size

| Build | Size |
|-------|------|
| Release (optimized) | **8.3 MB** |
| Docker image | ~25 MB (Alpine-based) |

---

## Supported Query Languages

1. **SQL** - Standard SQL queries
2. **LQL** - LumaDB Query Language (extended SQL)
3. **GraphQL** - Full query/mutation support
4. **Kafka Protocol** - Native wire protocol
5. **MongoDB Query** - BSON/JSON queries

---

## Complete Protocol Compatibility

### All Supported Drop-in Replacements

| System | Category | Protocol/Port | Status |
|--------|----------|---------------|--------|
| **Apache Kafka** | Streaming | Kafka/9092 | ✅ Production |
| **Redpanda** | Streaming | Kafka/9092 | ✅ Production |
| **Amazon MSK** | Streaming | Kafka/9092 | ✅ Production |
| **PostgreSQL** | SQL Database | PostgreSQL/5432 | ✅ Production |
| **MySQL** | SQL Database | MySQL/3306 | ✅ Production |
| **CockroachDB** | SQL Database | PostgreSQL/5432 | ✅ Production |
| **MongoDB** | Document DB | MongoDB/27017 | ✅ Production |
| **Amazon DocumentDB** | Document DB | MongoDB/27017 | ✅ Production |
| **Apache Cassandra** | Wide-Column | CQL/9042 | ✅ Production |
| **ScyllaDB** | Wide-Column | CQL/9042 | ✅ Production |
| **Aerospike** | Wide-Column | Custom | ✅ Production |
| **Redis** | Cache/KV | Redis/6379 | ✅ Production |
| **Memcached** | Cache/KV | Memcached/11211 | ✅ Production |
| **DragonflyDB** | Cache/KV | Redis/6379 | ✅ Production |
| **Qdrant** | Vector DB | REST/6333 | ✅ Production |
| **Pinecone** | Vector DB | REST | ✅ Production |
| **MongoDB Atlas Vector** | Vector DB | MongoDB/27017 | ✅ Production |
| **InfluxDB** | Time-Series | InfluxDB/8086 | ✅ Production |
| **TimescaleDB** | Time-Series | PostgreSQL/5432 | ✅ Production |
| **Prometheus** | Time-Series | PromQL/9090 | ✅ Production |
| **QuestDB** | Time-Series | PostgreSQL/8812 | ✅ Production |
| **OpenTSDB** | Time-Series | REST/4242 | ✅ Production |
| **Graphite** | Time-Series | Carbon/2003 | ✅ Production |
| **Apache Druid** | OLAP | Druid/8888 | ✅ Production |
| **ClickHouse** | OLAP | ClickHouse/8123 | ✅ Production |
| **Elasticsearch** | Search | ES/9200 | ✅ Production |
| **OpenSearch** | Search | ES/9200 | ✅ Production |
| **Amazon S3** | Object Storage | S3/9000 | ✅ Production |
| **MinIO** | Object Storage | S3/9000 | ✅ Production |
| **Supabase** | Backend-as-Service | REST/3000, Auth/9999 | ✅ Production |
| **KDB+** | Financial | Q/5000 | ✅ Production |
| **OpenTelemetry** | Observability | OTLP/4317 | ✅ Production |
| **AWS DynamoDB** | NoSQL | DynamoDB/8000 | ✅ Production |
| **Cloudflare D1** | Edge SQL | D1 API | ✅ Production |
| **Turso (libSQL)** | Edge SQL | libSQL | ✅ Production |

### Query Language Support

| Language | Description | Status |
|----------|-------------|--------|
| SQL | Standard SQL-92/99 | ✅ Full |
| PromQL | Prometheus Query Language | ✅ Full |
| InfluxQL | InfluxDB Query Language | ✅ Full |
| Flux | InfluxDB 2.x Language | ✅ Core |
| GraphQL | API Query Language | ✅ Full |
| MongoDB Query | BSON Query Operators | ✅ Full |
| Elasticsearch DSL | JSON Query DSL | ✅ Core |
| MetricsQL | VictoriaMetrics QL | ✅ Full |
| CQL | Cassandra Query Language | ✅ Full |

---

## Native Tool Connectivity

Tools that connect directly to LumaDB without adapters:

### BI & Analytics
- **Apache Superset** - via PostgreSQL protocol
- **Grafana** - via PostgreSQL datasource
- **Metabase** - via PostgreSQL driver
- **Redash** - via PostgreSQL connector
- **DBeaver** - via PostgreSQL/MongoDB drivers

### Observability
- **Prometheus** - scrapes /metrics endpoint
- **OpenTelemetry Collector** - OTLP endpoint
- **Jaeger** - trace ingestion
- **Datadog Agent** - PostgreSQL integration

### Streaming Clients
- **Kafka clients** (Java, Python, Go, Node.js)
- **Confluent Platform** - full compatibility
- **Debezium** - CDC connectors
- **Apache Flink** - Kafka source/sink
- **Apache Spark** - Kafka structured streaming

### ORM/Libraries
- **SQLAlchemy** (Python) - PostgreSQL dialect
- **Diesel** (Rust) - PostgreSQL backend
- **GORM** (Go) - PostgreSQL driver
- **Prisma** - PostgreSQL connector
- **PyMongo** - MongoDB driver

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    API Layer                             │
│  REST │ GraphQL │ gRPC │ Kafka │ PostgreSQL │ MongoDB   │
├─────────────────────────────────────────────────────────┤
│                   Query Engine                           │
│         Parser │ Analyzer │ Optimizer │ Executor        │
├─────────────────────────────────────────────────────────┤
│                  Storage Engine                          │
│     LSM-Tree │ Columnar │ Vector │ Full-Text │ TSDB     │
├─────────────────────────────────────────────────────────┤
│                 Streaming Engine                         │
│       Thread-per-Core │ io_uring │ Zero-Copy            │
├─────────────────────────────────────────────────────────┤
│                Consensus (Multi-Raft)                    │
│     Leader Election │ Log Replication │ Snapshots       │
└─────────────────────────────────────────────────────────┘
```

---

## Architecture Considerations

### Strengths
1. **Single Binary** - Simplified deployment and operations
2. **Pure Rust** - Memory safety without GC overhead
3. **Multi-Protocol** - One system replaces many
4. **Thread-per-Core** - No lock contention between cores
5. **Zero-Copy Networking** - Maximum I/O efficiency
6. **Auto-Sharding** - Automatic horizontal scaling via consistent hashing

### Cluster Scaling
- **Small Clusters (3-5 nodes)**: Single Raft group, no sharding needed
- **Medium Clusters (6-50 nodes)**: Auto-sharding with 64 default shards
- **Large Clusters (50+ nodes)**: Multi-Raft groups, each shard = 1 Raft group

### Production Readiness
- ✅ Core functionality complete
- ✅ Protocol compatibility verified
- ✅ Auto-sharding for horizontal scaling
- ⚠️ Large-scale production validation ongoing

---

## Crate Structure (43 Packages)

```
crates/
├── lumadb/              # Main binary
├── lumadb-api/          # REST, GraphQL, gRPC
├── lumadb-protocol/     # Kafka, PostgreSQL, MongoDB, Redis protocols
├── lumadb-streaming/    # High-performance streaming engine
├── lumadb-query/        # SQL parser and executor
├── lumadb-storage/      # Multi-model storage engine
├── lumadb-raft/         # Raft consensus
├── lumadb-txn/          # MVCC transactions
├── lumadb-cluster/      # Cluster management
├── lumadb-security/     # Auth, TLS, RBAC
├── lumadb-compat/       # Qdrant, Pinecone, MongoDB Atlas compat
├── lumadb-common/       # Shared utilities
└── supabase-compat/     # Supabase API layer (14 sub-crates)
```

---

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput | 80 GB/s |
| Latency P99 | 50 μs |
| Messages/sec | 200M |
| Memory footprint | 500 MB base |
| Cold start | < 1 second |

---

## Security Features

- TLS/mTLS transport encryption
- SASL authentication (PLAIN, SCRAM-SHA-256/512)
- JWT token authentication
- RBAC (Role-Based Access Control)
- ABAC (Attribute-Based Access Control)
- Row-Level Security (RLS)

---

## Deployment Options

- Single binary execution
- Docker container
- Docker Compose
- Kubernetes (Helm charts)
- Systemd service (Linux)
- Windows service
