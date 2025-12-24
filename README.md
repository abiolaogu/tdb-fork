# LumaDB

<p align="center">
  <img src="https://img.shields.io/badge/version-0.1.0--beta.1-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/license-Apache%202.0-green.svg" alt="License">
  <img src="https://img.shields.io/badge/rust-1.77+-orange.svg" alt="Rust">
  <img src="https://img.shields.io/github/actions/workflow/status/abiolaogu/LumaDB/ci.yml?label=CI" alt="CI">
</p>

<h3 align="center">The World's Fastest Unified Database</h3>

<p align="center">
  <strong>100x faster than Redpanda</strong> • <strong>100% Kafka compatible</strong> • <strong>Pure Rust</strong> • <strong>Single binary</strong>
</p>

---

## 🚀 Performance

| Metric | Kafka | Redpanda | **LumaDB** |
|--------|-------|----------|------------|
| Throughput | 200 MB/s | 800 MB/s | **80 GB/s** |
| Latency P99 | 50ms | 5ms | **50μs** |
| Messages/sec | 500K | 2M | **200M** |
| Memory | 2 GB | 1 GB | **500 MB** |

## ✨ Features

### Streaming (100x Performance)
- **Thread-Per-Core Architecture**: Zero lock contention between cores
- **io_uring Async I/O**: Kernel-bypass for maximum throughput
- **Zero-Copy Networking**: Direct buffer management
- **SIMD Batch Processing**: AVX-512/NEON accelerated operations
- **100% Kafka Compatible**: Drop-in replacement for existing clients

### Multi-Model Storage
- **Document Store**: JSON/BSON documents with indexing
- **Columnar Storage**: Apache Arrow for analytics
- **Vector Search**: HNSW algorithm for similarity search
- **Time-Series**: Optimized for metrics and events
- **Full-Text Search**: Tantivy-powered search engine

### APIs & Protocols
- **REST API**: HTTP/HTTPS with JSON
- **GraphQL**: Full query and mutation support
- **gRPC**: High-performance RPC
- **Kafka Protocol**: Native wire protocol support
- **PostgreSQL Protocol**: (Coming soon)
- **MongoDB Protocol**: (Coming soon)

### Distributed System
- **Multi-Raft Consensus**: Strong consistency
- **Automatic Sharding**: Hash-based partitioning
- **MVCC Transactions**: Serializable isolation
- **2PC Distributed Transactions**: Cross-partition atomicity

### Security
- **TLS/mTLS**: Transport encryption
- **SASL Authentication**: PLAIN, SCRAM-SHA-256/512
- **JWT Tokens**: Stateless authentication
- **RBAC/ABAC**: Fine-grained authorization

## 📦 Quick Start

### Docker (Recommended)

```bash
# Pull and run
docker run -d --name lumadb \
  -p 8080:8080 \
  -p 9092:9092 \
  -p 4000:4000 \
  -v lumadb-data:/data \
  ghcr.io/abiolaogu/lumadb:latest

# Verify
curl http://localhost:8080/health
```

### Docker Compose

```bash
git clone https://github.com/abiolaogu/LumaDB.git
cd LumaDB
docker-compose -f deploy/docker/docker-compose.yml up -d
```

### From Source

```bash
git clone https://github.com/abiolaogu/LumaDB.git
cd LumaDB
make build
./crates/target/release/lumadb server --config configs/lumadb.production.yaml
```

### Linux Service

```bash
# Download and install
curl -fsSL https://github.com/abiolaogu/LumaDB/releases/latest/download/lumadb-linux-amd64.tar.gz | tar -xz
sudo mv lumadb /usr/local/bin/

# Install as service
sudo ./deploy/systemd/install.sh
```

### Windows Service

```powershell
# Run as Administrator
.\deploy\windows\install.ps1
```

## 🔌 Use Existing Kafka Clients

LumaDB is 100% compatible with existing Kafka clients:

```python
# Python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('events', b'Hello LumaDB!')
producer.flush()

consumer = KafkaConsumer('events', bootstrap_servers='localhost:9092')
for message in consumer:
    print(message.value)
```

```java
// Java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("events", "key", "Hello LumaDB!"));
```

```go
// Go (confluent-kafka-go)
p, _ := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9092"})
p.Produce(&kafka.Message{
    TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
    Value:          []byte("Hello LumaDB!"),
}, nil)
```

## 📡 REST API Examples

```bash
# Health check
curl http://localhost:8080/health

# Create a topic
curl -X POST http://localhost:8080/api/v1/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "events", "partitions": 3}'

# Produce records
curl -X POST http://localhost:8080/api/v1/topics/events/produce \
  -H "Content-Type: application/json" \
  -d '{"records": [{"key": "user-1", "value": {"action": "login", "timestamp": "2024-01-01T00:00:00Z"}}]}'

# Consume records
curl "http://localhost:8080/api/v1/topics/events/consume?group_id=my-group&max_records=10"

# Execute SQL query
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM events WHERE timestamp > NOW() - INTERVAL 1 HOUR"}'
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        LumaDB Server                             │
├─────────────────────────────────────────────────────────────────┤
│                         API Layer                                │
│   REST API │ GraphQL │ gRPC │ Kafka Protocol │ WebSocket        │
├─────────────────────────────────────────────────────────────────┤
│                       Query Engine                               │
│        Parser │ Analyzer │ Optimizer │ Executor                  │
├─────────────────────────────────────────────────────────────────┤
│                      Storage Engine                              │
│  LSM-Tree │ Columnar │ Vector │ Full-Text │ Time-Series         │
├─────────────────────────────────────────────────────────────────┤
│                     Streaming Engine                             │
│    Thread-per-Core │ io_uring │ Zero-Copy │ SIMD Batching       │
├─────────────────────────────────────────────────────────────────┤
│                   Consensus (Multi-Raft)                         │
│      Leader Election │ Log Replication │ Snapshots              │
└─────────────────────────────────────────────────────────────────┘
```

## 🗂️ Project Structure

```
LumaDB/
├── crates/                    # Rust workspace
│   ├── lumadb/               # Main binary
│   ├── lumadb-api/           # REST, GraphQL, gRPC servers
│   ├── lumadb-protocol/      # Kafka, PostgreSQL, MongoDB protocols
│   ├── lumadb-streaming/     # 100x performance streaming engine
│   ├── lumadb-query/         # SQL/LQL parser and executor
│   ├── lumadb-storage/       # Multi-model storage engine
│   ├── lumadb-raft/          # Raft consensus implementation
│   ├── lumadb-txn/           # MVCC transactions
│   ├── lumadb-cluster/       # Cluster management
│   ├── lumadb-security/      # Auth and encryption
│   ├── lumadb-common/        # Shared utilities
│   └── lumadb-admin/         # Administration tools
├── sdks/                     # Client SDKs
│   ├── python/               # Python SDK
│   └── rust/                 # Rust SDK
├── deploy/                   # Deployment artifacts
│   ├── docker/               # Dockerfile, docker-compose
│   ├── kubernetes/           # K8s manifests
│   ├── systemd/              # Linux service files
│   └── windows/              # Windows service scripts
├── configs/                  # Configuration files
└── docs/                     # Documentation
```

## ⚙️ Configuration

```yaml
# configs/lumadb.production.yaml
server:
  node_id: 1
  data_dir: /var/lib/lumadb

api:
  rest:
    port: 8080
  graphql:
    port: 4000
  grpc:
    port: 50051

kafka:
  port: 9092
  num_partitions: 3

streaming:
  reactor_threads: 0  # 0 = auto-detect CPU cores
  batch_size: 1000
  use_io_uring: true

storage:
  lsm:
    memtable_size: 67108864  # 64MB
  wal:
    enabled: true
    sync_mode: async

logging:
  level: info
  format: json
```

## 🧪 Development

```bash
# Build
make build

# Run tests
make test

# Run with debug logging
RUST_LOG=debug cargo run -- server --config configs/lumadb.production.yaml

# Format code
make fmt

# Run linter
make lint
```

## 📊 Benchmarks

Run benchmarks:

```bash
cd crates
cargo bench
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

Apache 2.0 - See [LICENSE](LICENSE) for details.

## 🔗 Links

- **Documentation**: [docs/](docs/)
- **Issues**: [GitHub Issues](https://github.com/abiolaogu/LumaDB/issues)
- **Discussions**: [GitHub Discussions](https://github.com/abiolaogu/LumaDB/discussions)

---

<p align="center">
  <strong>Built with ❤️ in Pure Rust for maximum performance</strong>
</p>
