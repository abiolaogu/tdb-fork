# Vector Database Compatibility

LumaDB provides drop-in compatibility layers for popular vector databases, allowing you to use existing SDKs and tools without modification.

## Supported Databases

| Database | Protocol | Port | Status |
|----------|----------|------|--------|
| Qdrant | REST API | 6333 | Full support |
| Pinecone | REST API | 8081 | Full support |
| MongoDB Atlas | Wire Protocol | 27017 | $vectorSearch support |

## Quick Start

### Using Docker

```bash
docker run -d \
  --name lumadb \
  -p 8080:8080 \    # LumaDB API
  -p 6333:6333 \    # Qdrant-compatible
  -p 8081:8081 \    # Pinecone-compatible
  -p 27017:27017 \  # MongoDB-compatible
  -v lumadb-data:/data \
  ghcr.io/abiolaogu/lumadb:latest
```

### Configuration

```yaml
# lumadb.yaml
compatibility:
  qdrant:
    enabled: true
    port: 6333
  pinecone:
    enabled: true
    port: 8081
  mongodb:
    enabled: true
    port: 27017
```

## Qdrant Compatibility

LumaDB implements the full Qdrant REST API. Existing Qdrant clients can connect without modification.

### Supported Operations

- **Collections**: create, delete, get, list
- **Points**: upsert, search, scroll, get, delete, count
- **Filtering**: all Qdrant filter conditions
- **Indexes**: HNSW configuration

### Example: Python Qdrant Client

```python
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

# Connect to LumaDB's Qdrant-compatible endpoint
client = QdrantClient(host="localhost", port=6333)

# Create collection
client.create_collection(
    collection_name="my_collection",
    vectors_config=VectorParams(size=1536, distance=Distance.COSINE)
)

# Insert vectors
client.upsert(
    collection_name="my_collection",
    points=[
        PointStruct(
            id=1,
            vector=[0.1, 0.2, 0.3, ...],
            payload={"name": "example"}
        )
    ]
)

# Search
results = client.search(
    collection_name="my_collection",
    query_vector=[0.1, 0.2, 0.3, ...],
    limit=10
)
```

### Example: Rust Qdrant SDK

```rust
use lumadb_sdk::compat::qdrant::{QdrantClient, Point, Distance, VectorParams};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = QdrantClient::connect("http://localhost:6333").await?;

    // Upsert points
    let points = vec![
        Point::new(1)
            .with_vector(vec![0.1, 0.2, 0.3])
            .with_payload(serde_json::json!({"name": "test"})),
    ];
    client.upsert("my_collection", &points, true).await?;

    // Search
    let results = client.search("my_collection", vec![0.1, 0.2, 0.3], 10).await?;

    Ok(())
}
```

## Pinecone Compatibility

LumaDB implements the Pinecone REST API. Existing Pinecone clients can connect by pointing to LumaDB.

### Supported Operations

- **Vectors**: upsert, query, fetch, update, delete
- **Namespaces**: full namespace support
- **Metadata**: filtering with all operators
- **Stats**: describe_index_stats

### Example: Python Pinecone Client

```python
from pinecone import Pinecone

# Connect to LumaDB's Pinecone-compatible endpoint
pc = Pinecone(
    api_key="any-key",  # Required but not validated
    host="http://localhost:8081"
)

index = pc.Index("my-index")

# Upsert vectors
index.upsert(
    vectors=[
        {
            "id": "vec1",
            "values": [0.1, 0.2, 0.3, ...],
            "metadata": {"category": "A"}
        }
    ],
    namespace="my-namespace"
)

# Query
results = index.query(
    vector=[0.1, 0.2, 0.3, ...],
    top_k=10,
    namespace="my-namespace",
    include_metadata=True
)
```

### Example: Rust Pinecone SDK

```rust
use lumadb_sdk::compat::pinecone::{PineconeClient, Vector};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = PineconeClient::connect(
        "http://localhost:8081",
        "api-key",
        "my-index"
    ).await?;

    // Upsert
    let vectors = vec![
        Vector::new("vec1", vec![0.1, 0.2, 0.3])
            .with_metadata(serde_json::json!({"category": "A"})),
    ];
    client.upsert(&vectors, Some("my-namespace")).await?;

    // Query
    let results = client.query(vec![0.1, 0.2, 0.3], 10, Some("my-namespace")).await?;

    Ok(())
}
```

## MongoDB Atlas Vector Search Compatibility

LumaDB implements the MongoDB wire protocol with full `$vectorSearch` aggregation stage support.

### Supported Operations

- **CRUD**: insert, find, update, delete
- **Aggregation**: `$vectorSearch` stage
- **Indexes**: createIndexes (vector indexes)
- **Collections**: create, drop, list

### Example: Python PyMongo

```python
from pymongo import MongoClient

# Connect to LumaDB's MongoDB-compatible endpoint
client = MongoClient("mongodb://localhost:27017")
db = client["mydb"]
collection = db["embeddings"]

# Insert document with embedding
collection.insert_one({
    "_id": "doc1",
    "text": "Hello world",
    "embedding": [0.1, 0.2, 0.3, ...]
})

# Vector search using $vectorSearch
results = collection.aggregate([
    {
        "$vectorSearch": {
            "index": "vector_index",
            "path": "embedding",
            "queryVector": [0.1, 0.2, 0.3, ...],
            "numCandidates": 100,
            "limit": 10
        }
    }
])

for doc in results:
    print(doc)
```

### Example: Node.js MongoDB Driver

```javascript
const { MongoClient } = require('mongodb');

async function main() {
    // Connect to LumaDB
    const client = new MongoClient('mongodb://localhost:27017');
    await client.connect();

    const db = client.db('mydb');
    const collection = db.collection('embeddings');

    // Vector search
    const results = await collection.aggregate([
        {
            $vectorSearch: {
                index: "vector_index",
                path: "embedding",
                queryVector: [0.1, 0.2, 0.3, ...],
                numCandidates: 100,
                limit: 10
            }
        }
    ]).toArray();

    console.log(results);
}
```

## Migration Tools

LumaDB provides tools to migrate data from existing vector databases.

### CLI Migration

```bash
# Migrate from Qdrant
lumadb migrate \
  --from qdrant \
  --source-url http://qdrant-server:6333 \
  --source-collection my_collection \
  --target my_collection

# Migrate from Pinecone
lumadb migrate \
  --from pinecone \
  --api-key $PINECONE_API_KEY \
  --environment us-east-1 \
  --source-index my-index \
  --target my_collection

# Migrate from MongoDB
lumadb migrate \
  --from mongodb \
  --source-url mongodb://mongo-server:27017 \
  --source-db mydb \
  --source-collection embeddings \
  --vector-field embedding \
  --target my_collection
```

### Programmatic Migration

```rust
use lumadb_compat::migration::{MigrationTool, MigrationConfig, MigrationSource};

let tool = MigrationTool::new(storage.clone())
    .with_config(MigrationConfig {
        batch_size: 1000,
        workers: 4,
        ..Default::default()
    });

// Import from Qdrant
tool.import_from_qdrant(
    "http://qdrant:6333",
    "source_collection",
    Some("target_collection")
).await?;

// Import from JSON file
tool.import_json("./vectors.jsonl", "my_collection").await?;
```

### Export Data

```rust
use lumadb_compat::migration::{Exporter, ExportFormat, ExportOptions};

let exporter = Exporter::new(storage.clone());

// Export to Qdrant format
exporter.export_to_file(
    "my_collection",
    "./export.qdrant.json",
    ExportFormat::Qdrant,
    ExportOptions::default()
).await?;

// Export to Pinecone format
exporter.export_to_file(
    "my_collection",
    "./export.pinecone.json",
    ExportFormat::Pinecone,
    ExportOptions::default()
).await?;
```

## Limitations

While LumaDB strives for full compatibility, some advanced features may have differences:

### Qdrant
- Shard management is handled internally
- Some advanced HNSW parameters may be ignored

### Pinecone
- Serverless index configuration is not applicable
- Sparse vectors are stored but may not be used for search

### MongoDB
- Not all aggregation stages are supported
- Only `$vectorSearch` is fully implemented for vector operations

## Performance Considerations

LumaDB's compatibility layers add minimal overhead:

- **Qdrant REST API**: ~1ms additional latency
- **Pinecone REST API**: ~1ms additional latency
- **MongoDB Wire Protocol**: ~2ms additional latency

For maximum performance, consider using LumaDB's native API.
