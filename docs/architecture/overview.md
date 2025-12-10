# TDB+ Architecture Overview

## Introduction

TDB+ is built on a modern multi-language architecture that combines the best aspects of several leading database systems:

- **Aerospike**: Hybrid memory architecture (RAM + SSD)
- **ScyllaDB**: Shard-per-core design, lock-free data structures
- **DragonflyDB**: Memory efficiency, multi-threaded design
- **YugabyteDB**: Distributed consistency, horizontal scaling
- **kdb+**: Columnar storage, vectorized analytics

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           TDB+ Database Platform                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                         Client Layer                                    │ │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────────┐ │ │
│  │  │   REST   │ │   gRPC   │ │  Native  │ │ PromptQL │ │    SQL       │ │ │
│  │  │   API    │ │   API    │ │  Driver  │ │  Engine  │ │  Interface   │ │ │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────────┘ │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       Go Service Layer                                  │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │ │
│  │  │   Request    │ │   Query      │ │  Connection  │ │   Cluster    │  │ │
│  │  │   Router     │ │   Executor   │ │   Pooling    │ │   Manager    │  │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘  │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │ │
│  │  │  Replication │ │   Sharding   │ │   Failover   │ │   Metrics    │  │ │
│  │  │   Manager    │ │   Router     │ │   Handler    │ │   Collector  │  │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                      Python AI Layer                                    │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │ │
│  │  │  PromptQL    │ │   Vector     │ │    NLP       │ │   Model      │  │ │
│  │  │   Engine     │ │   Index      │ │  Processor   │ │   Manager    │  │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘  │ │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐  │ │
│  │  │   LLM        │ │   Schema     │ │   Query      │ │  Semantic    │  │ │
│  │  │ Integration  │ │  Inference   │ │  Optimizer   │ │   Parser     │  │ │
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                    │                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                       Rust Core Engine                                  │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │ │
│  │  │                    Storage Engine                                 │  │ │
│  │  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────────┐ │  │ │
│  │  │  │  MemTable  │ │   Block    │ │    WAL     │ │   Compaction   │ │  │ │
│  │  │  │ (SkipList) │ │   Cache    │ │  Manager   │ │    Engine      │ │  │ │
│  │  │  └────────────┘ └────────────┘ └────────────┘ └────────────────┘ │  │ │
│  │  └──────────────────────────────────────────────────────────────────┘  │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │ │
│  │  │                  Hybrid Memory Layer                              │  │ │
│  │  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────────┐ │  │ │
│  │  │  │    RAM     │ │    SSD     │ │    HDD     │ │   Migration    │ │  │ │
│  │  │  │   Store    │ │   Store    │ │   Store    │ │    Engine      │ │  │ │
│  │  │  └────────────┘ └────────────┘ └────────────┘ └────────────────┘ │  │ │
│  │  └──────────────────────────────────────────────────────────────────┘  │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │ │
│  │  │                   Columnar Engine                                 │  │ │
│  │  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────────┐ │  │ │
│  │  │  │    SIMD    │ │ Compression│ │ Time-Series│ │   Vectorized   │ │  │ │
│  │  │  │ Operations │ │   Codecs   │ │   Index    │ │    Filters     │ │  │ │
│  │  │  └────────────┘ └────────────┘ └────────────┘ └────────────────┘ │  │ │
│  │  └──────────────────────────────────────────────────────────────────┘  │ │
│  │  ┌──────────────────────────────────────────────────────────────────┐  │ │
│  │  │                   I/O Subsystem                                   │  │ │
│  │  │  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────────┐ │  │ │
│  │  │  │  io_uring  │ │ Direct I/O │ │  Batched   │ │   Prefetch     │ │  │ │
│  │  │  │   Engine   │ │   Bypass   │ │    I/O     │ │    Engine      │ │  │ │
│  │  │  └────────────┘ └────────────┘ └────────────┘ └────────────────┘ │  │ │
│  │  └──────────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Design Principles

### 1. Language-Specific Optimization

Each language in TDB+ is chosen for its strengths:

| Language | Purpose | Key Advantages |
|----------|---------|----------------|
| **Rust** | Core Storage | Zero-cost abstractions, memory safety, SIMD |
| **Go** | Service Layer | Concurrency, networking, deployment |
| **Python** | AI/ML Layer | Rich ecosystem, LLM integration, rapid development |

### 2. Hybrid Memory Architecture

Inspired by Aerospike, TDB+ maintains data across multiple storage tiers:

```
┌─────────────────────────────────────────────────┐
│              Hot Data (RAM)                      │
│  • Primary index always in memory                │
│  • Recently accessed records                     │
│  • Write buffer (MemTable)                       │
│  • Access latency: < 1 microsecond              │
├─────────────────────────────────────────────────┤
│             Warm Data (SSD)                      │
│  • Frequently accessed records                   │
│  • Secondary indexes                             │
│  • Direct I/O with io_uring                     │
│  • Access latency: < 100 microseconds           │
├─────────────────────────────────────────────────┤
│             Cold Data (HDD)                      │
│  • Archived records                              │
│  • Historical data                               │
│  • Compressed storage                            │
│  • Access latency: < 10 milliseconds            │
└─────────────────────────────────────────────────┘
```

### 3. Shard-Per-Core Design

Following ScyllaDB's approach, TDB+ assigns dedicated shards to CPU cores:

- **Lock-free operations** within each shard
- **NUMA-aware allocation** for multi-socket systems
- **Core pinning** for predictable latency
- **Message passing** between shards

### 4. Columnar Analytics

Inspired by kdb+, TDB+ provides high-performance analytics:

- **SIMD vectorization** (AVX2/AVX-512)
- **Specialized compression** (delta, RLE, dictionary)
- **Time-series optimizations**
- **Streaming aggregations**

---

## Component Details

### Rust Core Engine

The Rust core provides:

```rust
// Core Database Instance
pub struct Database {
    config: Config,
    shards: Arc<ShardManager>,
    collections: DashMap<String, Arc<Collection>>,
    wal: Arc<WriteAheadLog>,
    stats: Arc<RwLock<DatabaseStats>>,
}

// Key Operations
impl Database {
    pub async fn insert(&self, collection: &str, doc: Document) -> Result<DocumentId>;
    pub async fn get(&self, collection: &str, id: &DocumentId) -> Result<Option<Document>>;
    pub async fn scan<F>(&self, collection: &str, predicate: F) -> Result<Vec<Document>>;
    pub async fn batch_insert(&self, collection: &str, docs: Vec<Document>) -> Result<Vec<DocumentId>>;
}
```

### Go Service Layer

The Go layer handles:

```go
// Server Configuration
type Server struct {
    config     *Config
    httpServer *http.Server
    grpcServer *grpc.Server
    cluster    *ClusterManager
    pool       *ConnectionPool
}

// Key Services
- HTTP/REST API endpoints
- gRPC service definitions
- Cluster membership management
- Request routing and load balancing
- Replication coordination
```

### Python AI Layer

The Python layer provides:

```python
# PromptQL Engine
class PromptQLEngine:
    async def query(self, prompt: str) -> QueryResult
    async def explain(self, prompt: str) -> str
    async def suggest(self, context: str) -> List[str]

# Key Components
- Multi-step reasoning
- LLM integration (OpenAI, Anthropic, local)
- Semantic understanding
- Query optimization
- Schema inference
```

---

## Data Flow

### Write Path

```
Client Request
      │
      ▼
┌─────────────────┐
│   Go Service    │ ─── Authentication & Routing
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Rust Core     │ ─── Write to WAL (Group Commit)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    MemTable     │ ─── In-memory write buffer
└────────┬────────┘
         │
         ▼ (Background)
┌─────────────────┐
│   SST Flush     │ ─── Persist to SSD
└────────┬────────┘
         │
         ▼ (Background)
┌─────────────────┐
│   Compaction    │ ─── Merge and optimize
└─────────────────┘
```

### Read Path

```
Client Request
      │
      ▼
┌─────────────────┐
│   Go Service    │ ─── Route to correct shard
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Primary Index  │ ─── Always in RAM
└────────┬────────┘
         │
    ┌────┴────┐
    │  Found? │
    └────┬────┘
      Yes │ No
    ┌─────┴─────┐
    ▼           ▼
┌───────┐  ┌────────┐
│  RAM  │  │ Block  │ ─── Check SSD cache
│ Store │  │ Cache  │
└───────┘  └────┬───┘
                │ Miss
                ▼
          ┌──────────┐
          │   SSD    │ ─── io_uring read
          │   Store  │
          └──────────┘
```

---

## Scalability

### Horizontal Scaling

```
                    Load Balancer
                         │
         ┌───────────────┼───────────────┐
         │               │               │
         ▼               ▼               ▼
    ┌─────────┐     ┌─────────┐     ┌─────────┐
    │  Node 1 │     │  Node 2 │     │  Node 3 │
    │         │     │         │     │         │
    │ Shards  │     │ Shards  │     │ Shards  │
    │  1-10   │     │  11-20  │     │  21-30  │
    └─────────┘     └─────────┘     └─────────┘
         │               │               │
         └───────────────┼───────────────┘
                         │
                   Shared Storage
                   (Optional)
```

### Replication

- **Synchronous replication**: Strong consistency
- **Asynchronous replication**: Higher throughput
- **Configurable quorum**: Tunable consistency/availability

---

## Next Steps

- [Multi-Language Architecture](./multi-language.md)
- [Storage Engine Deep Dive](./storage-engine.md)
- [Hybrid Memory System](./hybrid-memory.md)
