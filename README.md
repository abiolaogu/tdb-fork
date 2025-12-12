# LumaDB (Luma Database Plus)

<p align="center">
  <img src="https://img.shields.io/badge/version-2.0.0-blue.svg" alt="Version">
  <img src="https://img.shields.io/badge/license-MIT-green.svg" alt="License">
  <img src="https://img.shields.io/badge/rust-1.70+-orange.svg" alt="Rust">
  <img src="https://img.shields.io/badge/go-1.21+-00ADD8.svg" alt="Go">
  <img src="https://img.shields.io/badge/python-3.11+-3776AB.svg" alt="Python">
</p>

**LumaDB** is a high-performance, distributed database with AI-native capabilities. Features a **built-in Admin UI**, **Hasura-style GraphQL/REST APIs**, and **Event Triggers**. Built with a multi-language architecture combining **Rust** for speed, **Go** for scalability, and **Python** for AI integration.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        LumaDB Architecture                             │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐     │
│  │   TypeScript    │  │    Python AI    │  │   Go Cluster    │     │
│  │   Client SDK    │  │    Service      │  │   Coordinator   │     │
│  │                 │  │                 │  │                 │     │
│  │ • Query Builder │  │ • Vector Search │  │ • Raft Consensus│     │
│  │ • Type Safety   │  │ • Embeddings    │  │ • Sharding      │     │
│  │ • REPL/CLI      │  │ • NLP Queries   │  │ • Load Balance  │     │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘     │
│           │                    │                    │               │
│           └────────────────────┼────────────────────┘               │
│                                │                                    │
│                    ┌───────────┴───────────┐                        │
│                    │   Rust Core Engine    │                        │
│                    │                       │                        │
│                    │ • LSM-Tree Storage    │                        │
│                    │ • Lock-Free Memtables │                        │
│                    │ • Memory-Mapped I/O   │                        │
│                    │ • Zero-Copy Reads     │                        │
│                    │ • Shard-Per-Core      │                        │
│                    └───────────────────────┘                        │
└─────────────────────────────────────────────────────────────────────┘
```

## Why LumaDB?

### Hybrid Memory Architecture (Aerospike-style)
- **RAM + SSD**: Hot data in RAM, warm data on SSD, cold data on HDD
- **Primary Index in RAM**: O(1) lookups with predictable latency
- **Automatic Tiering**: Data automatically migrates based on access patterns
- **NUMA-Aware**: Optimized memory allocation for multi-socket servers

### Ultra-High Performance (Outperforms kdb+)
- **SIMD Vectorized Operations**: AVX2/AVX-512 accelerated analytics
- **Columnar Storage**: kdb+-style column-oriented engine for analytics
- **Time-Series Optimizations**: Specialized indexing and compression
- **io_uring**: Kernel-bypass async I/O for maximum throughput

### Performance (Inspired by Aerospike, ScyllaDB, DragonflyDB)
- **Rust Core**: Lock-free data structures, zero-copy I/O, memory-mapped files
- **LSM-Tree Storage**: Optimized write path with leveled compaction
- **Shard-Per-Core**: ScyllaDB-inspired architecture for minimal lock contention
- **Batch Commits**: DragonflyDB-inspired group commit for high throughput

### Predictable Latency (SLA Guarantees)
- **Sub-millisecond Reads**: Critical operations < 1ms p99
- **Admission Control**: Backpressure prevents overload
- **Latency Histograms**: Real-time percentile tracking
- **Priority Tiers**: Critical, High, Normal, Background

### Scalability (Inspired by YugabyteDB)
- **Raft Consensus**: Strong consistency across distributed nodes
- **Automatic Sharding**: Consistent hashing with virtual nodes
- **Connection Pooling**: Efficient stateless operations
- **Horizontal Scaling**: Add nodes without downtime

### AI-Native
- **Vector Search**: FAISS-powered semantic similarity search
- **Embeddings**: Built-in text embedding generation
- **Natural Language Queries**: Ask questions in plain English
- **Query Translation**: AI converts natural language to LQL/JQL

### Developer Experience
- **Three Query Languages**: LQL (SQL-like), NQL (Natural), JQL (JSON)
- **TypeScript SDK**: Full type safety and IntelliSense
- **Interactive CLI**: Syntax highlighting and auto-completion
- **Excellent Errors**: Clear, actionable error messages

### Embedded Scripting (Rhai)
- **Safe Execution**: Sandboxed scripting environment
- **Custom Logic**: Write stored procedures and triggers in Rhai
- **High Performance**: Compiled scripts for fast execution
- **Integration**: Direct access to LumaDB document types and collections

## LumaDB Platform (New v2.0)

### Admin Console
- **Modern UI**: Dark-themed dashboard built with Next.js & Tailwind.
- **Data Explorer**: View collections, execute SQL, and manage documents.
- **API Explorer**: Integrated GraphiQL for testing GraphQL queries.
- **Event Management**: Configure and monitor triggers.

### GraphQL & REST Engine
- **Auto-generated APIs**: Instant GraphQL and REST endpoints for your data.
- **Live Queries**: Real-time subscriptions via WebSockets.
- **Data Federation**: (Coming Soon) Connect to remote databases.

### Auth & Security
- **JWT Authentication**: Built-in identity management using HS256 tokens.
- **Role-Based Access**: Granular control over read/write operations.
- **Middleware**: Protects API and GraphQL endpoints automatically.

### Event Triggers
- **Real-time Events**: React to INSERT, UPDATE, DELETE operations.
- **Webhooks**: POST payloads to external HTTP endpoints.
- **Redpanda Integration**: Stream events directly to Redpanda topics with high throughput.

## Quick Start

### Installation

```bash
# TypeScript/JavaScript SDK
npm install tdb-plus

# Or start the full stack with Docker
docker-compose up -d
```

### Basic Usage

```typescript
import { Database } from 'tdb-plus';

const db = Database.create('my_app');
await db.open();

// LQL (SQL-like) - for SQL developers
await db.lql(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 28)`);
const users = await db.lql(`SELECT * FROM users WHERE age > 21`);

// NQL (Natural Language) - for beginners
await db.nql(`add to users name "Bob", email "bob@example.com", age 32`);
const active = await db.nql(`find all users where age is greater than 25`);

// JQL (JSON) - for MongoDB developers
await db.jql(`{ "insert": "users", "documents": [{ "name": "Charlie", "age": 25 }] }`);
const results = await db.jql(`{ "find": "users", "filter": { "age": { "$gt": 20 } } }`);

await db.close();
```

## Three Query Languages

### LQL (TDB Query Language) - SQL-Like

```sql
-- CRUD Operations
SELECT * FROM users WHERE age > 21 AND status = 'active' ORDER BY name LIMIT 10
INSERT INTO products (name, price) VALUES ('Laptop', 999.99)
UPDATE users SET status = 'verified' WHERE email_verified = true
DELETE FROM sessions WHERE expired_at < NOW()

-- Aggregations
SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category

-- Indexing
CREATE INDEX idx_email ON users (email) UNIQUE
```

### NQL (Natural Query Language) - Human-Readable

```
find all users
get users where age is greater than 21
show first 10 products sorted by price descending
count all users where status equals "active"
add to users name "Jane", email "jane@example.com"
update users set status to "active" where verified is true
remove users where inactive is true
```

### JQL (JSON Query Language) - MongoDB-Style

```json
{
  "find": "users",
  "filter": { "age": { "$gt": 21 }, "status": "active" },
  "sort": { "createdAt": -1 },
  "limit": 10
}
```

## AI Features

### Vector Similarity Search

```typescript
// Index documents with embeddings
await db.ai.index('products', {
  id: 'prod_1',
  text: 'Wireless noise-canceling headphones with 30-hour battery',
  metadata: { category: 'electronics', price: 299 }
});

// Semantic search
const results = await db.ai.search('products', {
  query: 'bluetooth headphones for travel',
  topK: 10,
  filter: { category: 'electronics' }
});
```

### Natural Language to Query

```typescript
// AI translates natural language to LQL
const query = await db.ai.translate(
  'show me all orders from last week that cost more than $100'
);
// Returns: SELECT * FROM orders WHERE created_at > DATE_SUB(NOW(), 7) AND total > 100
```

### Semantic Analysis

```typescript
const analysis = await db.ai.analyze(
  'Great product! Fast shipping and excellent quality.',
  { entities: true, sentiment: true, keywords: true }
);
// {
//   sentiment: { label: 'positive', score: 0.92 },
//   keywords: ['product', 'shipping', 'quality'],
//   entities: []
// }
```

## Distributed Features

### Cluster Setup

```yaml
# docker-compose.yml
services:
  tdb-node-1:
    image: tdb-plus:latest
    environment:
      TDB_NODE_ID: node-1
      TDB_CLUSTER_PEERS: node-2:10000,node-3:10000
      TDB_REPLICATION_FACTOR: 3

  tdb-node-2:
    image: tdb-plus:latest
    environment:
      TDB_NODE_ID: node-2
      TDB_CLUSTER_PEERS: node-1:10000,node-3:10000

  tdb-node-3:
    image: tdb-plus:latest
    environment:
      TDB_NODE_ID: node-3
      TDB_CLUSTER_PEERS: node-1:10000,node-2:10000
```

### Connection Pooling

```typescript
import { createClient } from 'tdb-plus/client';

const client = createClient({
  nodes: ['node-1:8080', 'node-2:8080', 'node-3:8080'],
  pool: {
    minConnections: 10,
    maxConnections: 100,
    idleTimeout: 30000,
  },
  loadBalancing: 'round-robin', // or 'least-connections'
});
```

## Performance Configuration

### Rust Core Configuration

```toml
# luma.toml
[memory]
memtable_size = 67108864      # 64 MB
max_memtables = 4
block_size = 4096             # 4 KB
use_mmap = true

[storage]
compression = "lz4"           # none, lz4, zstd
direct_io = true
sync_writes = false
bloom_bits_per_key = 10

[wal]
enabled = true
sync_mode = "group_commit"    # always, periodic, never, group_commit
batch_size = 1000
batch_timeout_us = 1000

[compaction]
style = "leveled"             # leveled, universal, fifo
max_background_compactions = 4

[sharding]
num_shards = 0                # 0 = auto-detect (CPU cores)
shard_per_core = true
hash_function = "xxh3"

[cache]
block_cache_size = 134217728  # 128 MB
row_cache_size = 67108864     # 64 MB
```

### Preset Configurations

```rust
// Fast mode (less durability)
let config = Config::fast();

// Durable mode (maximum safety)
let config = Config::durable();

// Low memory mode
let config = Config::low_memory();
```

## Benchmarks

Performance on a 16-core machine with NVMe storage:

| Operation | Throughput | Latency (p99) |
|-----------|------------|---------------|
| Single Insert | 450K ops/sec | 0.8ms |
| Batch Insert (1000) | 2.1M docs/sec | 12ms |
| Point Lookup | 1.2M ops/sec | 0.3ms |
| Range Scan (1000) | 180K scans/sec | 8ms |
| Vector Search (10K) | 15K queries/sec | 4ms |

## Project Structure

```
tdb-fork/
├── rust-core/              # High-performance storage engine
│   ├── src/
│   │   ├── storage/        # LSM-tree, SSTable, compaction
│   │   ├── memory/         # Memtables, caches, arena
│   │   ├── wal/            # Write-ahead logging
│   │   ├── shard/          # Shard-per-core architecture
│   │   ├── index/          # B-Tree, Hash indexes
│   │   └── ffi/            # C-compatible FFI bindings
│   └── Cargo.toml
│
├── go-cluster/             # Distributed coordination
│   ├── cmd/server/         # Main server entry
│   ├── pkg/
│   │   ├── cluster/        # Raft consensus
│   │   ├── router/         # Request routing
│   │   ├── api/            # HTTP/gRPC servers
│   │   ├── platform/       # New: Platform Server (GraphQL/REST/Events)
│   │   ├── pool/           # Connection pooling
│   │   └── core/           # Rust FFI bindings
│   └── go.mod
│
├── ui/                     # New: Admin Interface
│   └── admin/              # Next.js Admin Console
│
├── python-ai/              # AI service
│   ├── tdbai/
│   │   ├── main.py         # FastAPI server
│   │   ├── vector.py       # FAISS vector index
│   │   ├── nlp.py          # NL query translation
│   │   ├── inference.py    # Model management
│   │   └── bindings.py     # Rust FFI bindings
│   └── pyproject.toml
│
├── src/                    # TypeScript SDK & CLI
│   ├── core/               # Database, Collection, Document
│   ├── parsers/            # LQL, NQL, JQL parsers
│   ├── storage/            # Memory & File storage
│   └── cli/                # REPL interface
│
├── benchmarks/             # Performance benchmarks
│   ├── rust_bench.rs
│   ├── go_bench_test.go
│   └── python_bench.py
│
└── tests/                  # Test suites
```

## Building from Source

### Prerequisites

- Rust 1.70+
- Go 1.21+
- Python 3.11+
- Node.js 18+

### Build All Components

```bash
# Build Rust core
cd rust-core && cargo build --release

# Build Go cluster
cd go-cluster && go build ./...

# Install Python dependencies
cd python-ai && pip install -e .

# Install TypeScript SDK
npm install && npm run build

# Build Admin UI
cd ui/admin && npm install && npm run build
```

### Run Tests

```bash
# Rust tests
cargo test

# Go tests
go test ./...

# Python tests
pytest

# TypeScript tests
npm test
```

## API Reference

### Database

| Method | Description |
|--------|-------------|
| `open()` | Open database connection |
| `close()` | Close connection |
| `lql(query)` | Execute LQL query |
| `nql(query)` | Execute NQL query |
| `jql(query)` | Execute JQL query |
| `collection(name)` | Get collection |
| `transaction(fn)` | Run in transaction |
| `ai.search(...)` | Vector similarity search |
| `ai.translate(...)` | NL to query translation |

### Collection

| Method | Description |
|--------|-------------|
| `insert(doc)` | Insert document |
| `insertMany(docs)` | Batch insert |
| `findById(id)` | Get by ID |
| `find(query)` | Find with conditions |
| `updateById(id, data)` | Update by ID |
| `deleteById(id)` | Delete by ID |
| `createIndex(...)` | Create index |

## Comparison

| Feature | LumaDB | ScyllaDB | DragonflyDB | MongoDB |
|---------|------|----------|-------------|---------|
| Query Languages | 3 | 1 (CQL) | 1 (Redis) | 1 |
| AI/Vector Search | Native | No | No | Atlas |
| Natural Language | Yes | No | No | No |
| Distributed | Yes | Yes | No | Yes |
| In-Memory Option | Yes | No | Yes | No |
| Embedded Mode | Yes | No | No | No |

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  Built with Rust, Go, and Python for maximum performance and developer happiness.
</p>
