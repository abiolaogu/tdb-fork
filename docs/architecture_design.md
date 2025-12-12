# LumaDB Architecture Design Document

## 1. System Overview

LumaDB is a distributed, multi-model database designed for high-performance analytics and AI-native workloads. It combines a Rust-based low-latency storage engine with a Go-based distributed control plane and a Python-based AI processing layer.

### 1.1 High-Level Architecture

```mermaid
graph TD
    Client[Client App] --> LB[Load Balancer]
    LB --> Node1[LumaDB Node 1]
    LB --> Node2[LumaDB Node 2]
    LB --> Node3[LumaDB Node 3]

    subgraph "LumaDB Node"
        API[API Gateway (Go)]
        Router[Request Router (Go)]
        Consensus[Raft Consensus (Go)]
        
        API --> Router
        Router --> Consensus
        Router --> Storage[Storage Engine (Rust)]
        Router --> AIService[AI Service (Python)]
        
        Storage --> SSD[SSD Storage]
        Storage --> HDD[HDD Storage]
        
        AIService --> VectorIndex[FAISS Index]
    end
```

## 2. Component Design

### 2.1 Storage Engine (Rust Core)

The core storage engine (`luma-core`) is written in Rust and implements a hybrid memory architecture inspired by Aerospike and ScyllaDB.

#### Key Features:
- **Shard-per-Core**: Each CPU core is assigned a `Shard` which manages a partition of the data. This eliminates lock contention.
- **Hybrid Storage**:
  - **L1 (Hot)**: In-memory `DashMap` and `MemTable` for O(1) access.
  - **L2 (Warm)**: SSD-backed storage using `io_uring` for high throughput.
  - **L3 (Cold)**: HDD/S3-backed storage for archival capability (Erasure Coded).
- **Log-Structured Merge Tree (LSM)**: Writes are buffered in memory and flushed to sorted runs on disk.
- **Erasure Coding**: Configurable redundancy data protection (e.g., Reed-Solomon 6+3) for warm/cold tiers.

#### Internal Structure:
```rust
struct Database {
    shards: Vec<Shard>,
    wal: WriteAheadLog,
}

struct Shard {
    id: u32,
    storage: HybridStorage,
}

struct HybridStorage {
    ram_store: DashMap<Key, Value>,
    ssd_store: BlockStore, // via io_uring
    tier_policy: PolicyEngine,
}
```

### 2.2 Distributed Cluster (Go)

The cluster management layer (`github.com/lumadb/cluster`) manages topology, consensus, and request routing.

#### Key Features:
- **Raft Consensus**: Hashicorp's Raft implementation ensures strong consistency for metadata and configuration.
- **Gossip Protocol**: Serf is used for node discovery and failure detection.
- **Consistent Hashing**: Data is partitioned across nodes using a consistent hashing ring.
- **FFI Integration**: The Go runtime calls into the Rust core via CGO using the `luma_` FFI bindings.

### 2.3 AI Service (Python)

The AI layer (`luma-ai`) provides vector search and natural language processing capabilities.

#### Key Features:
- **Vector Embeddings**: Generates embeddings for text/image data using models like BERT or CLIP.
- **Semantic Search**: FAISS integration for fast approximate nearest neighbor (ANN) search.
- **Query Translation**: LLM-based translation of Natural Language Queries (NQL) into LQL.

## 3. Data Flow

### 3.1 Write Path
1. Client sends write request (e.g., `INSERT`).
2. Go Router determines owning node via consistent hash.
3. Request forwarded to owning node.
4. Node writes to **Write Ahead Log (WAL)** for durability.
5. Data inserted into **Rust Core** `MemTable` (RAM).
6. Acknowledgment sent to client.
7. Background flush moves data to SSD (SSTables) when `MemTable` fills.
8. Tiering policy moves aged data to HDD/Cold storage.

### 3.2 Read Path
1. Client sends read request.
2. Go Router forwards to owning node.
3. **Rust Core** checks:
   - **L1 (RAM)**: If found, return immediately.
   - **L2 (SSD)**: If not in RAM, read from SSD via `io_uring`.
   - **L3 (HDD)**: If deeply cold, read from HDD.
4. Result returned to client.

## 4. LumaDB Platform Layer (v2.0)
The Platform Layer sits on top of the Go Cluster to provide developer-friendly APIs and tools.

### 4.1 API Gateway
- **GraphQL Engine**: Dynamically generates schemas from collections. Supports queries, mutations, and WebSocket subscriptions.
- **REST API**: Auto-generated CRUD endpoints (`/api/v1/:collection`).
- **GraphiQL**: Interactive GraphQL IDE embedded in the Admin Console.

### 4.2 Event System
- **Trigger Manager**: Listens to database operations (Insert/Update/Delete).
- **Sinks**:
    - **Webhooks**: HTTP callbacks.
    - **Redpanda**: High-throughput event streaming to Kafka-compatible topics.

### 4.3 Admin Console
- **Tech Stack**: Next.js 14, Tailwind CSS, React.
- **Features**: Dashboard, Data Explorer, SQL Runner, Event Configuration.

### 4.4 Security
- **Authentication**: JWT (HS256) based token issuance and validation.
- **Authorization**: Middleware intercepts requests to `/api/*` and `/graphql` to enforce permissions.
- **Identity**: Pluggable `AuthEngine` currently supports local admin users.

## 5. Configuration

LumaDB is configured via `luma.toml`. Users can define storage tiers, redundancy strategies (Replication vs Erasure Coding), and hardware limits.

```toml
[tiering]
warm_policy = { enabled = true, strategy = { type = "ErasureCoding", data_shards = 6, parity_shards = 3 } }
```
