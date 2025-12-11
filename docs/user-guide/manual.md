# LumaDB User Manual

## Complete Guide to Using LumaDB

---

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Data Model](#data-model)
4. [Query Languages](#query-languages)
5. [CRUD Operations](#crud-operations)
6. [Indexing](#indexing)
7. [Aggregations](#aggregations)
8. [Transactions](#transactions)
9. [PromptQL Guide](#promptql-guide)
10. [Best Practices](#best-practices)

---

## Introduction

LumaDB is a modern, high-performance database that combines:

- **Speed**: Sub-millisecond latencies
- **Flexibility**: Multiple data models (document, columnar, key-value)
- **Intelligence**: AI-powered query capabilities
- **Scale**: Horizontal scaling to thousands of nodes

### Key Capabilities

| Capability | Description |
|------------|-------------|
| **PromptQL** | Natural language queries with AI reasoning |
| **SQL** | Standard SQL interface |
| **Real-time Analytics** | SIMD-accelerated aggregations |
| **Vector Search** | Semantic similarity search |
| **Time-Series** | Specialized time-series operations |

---

## Getting Started

### Installation

#### Using Docker

```bash
# Pull the LumaDB image
docker pull tdbplus/tdbplus:latest

# Run LumaDB server
docker run -d \
  --name tdbplus \
  -p 8080:8080 \
  -p 9090:9090 \
  -v tdbplus-data:/data \
  tdbplus/tdbplus:latest
```

#### Using Kubernetes

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: tdbplus
spec:
  serviceName: tdbplus
  replicas: 3
  selector:
    matchLabels:
      app: tdbplus
  template:
    metadata:
      labels:
        app: tdbplus
    spec:
      containers:
      - name: tdbplus
        image: tdbplus/tdbplus:latest
        ports:
        - containerPort: 8080
        - containerPort: 9090
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
```

#### Native Installation

```bash
# Download binary
curl -LO https://releases.tdbplus.io/latest/tdbplus-linux-amd64.tar.gz

# Extract
tar xzf tdbplus-linux-amd64.tar.gz

# Install
sudo mv tdbplus /usr/local/bin/

# Start server
tdbplus server --config /etc/tdbplus/config.yaml
```

### Connecting to LumaDB

#### Python

```python
from tdbai import PromptQLEngine, LLMConfig
import tdb

# Connect to LumaDB
client = tdb.connect(
    host="localhost",
    port=8080,
    username="admin",
    password="secret"
)

# Initialize PromptQL (optional, for AI queries)
engine = PromptQLEngine(
    db_client=client,
    llm_config=LLMConfig(
        provider="openai",
        api_key="your-api-key"
    )
)
```

#### Go

```go
import "github.com/tdbplus/tdb-go"

// Connect to LumaDB
client, err := tdb.Connect(&tdb.Config{
    Host:     "localhost",
    Port:     8080,
    Username: "admin",
    Password: "secret",
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

#### REST API

```bash
# Health check
curl http://localhost:8080/health

# Query
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <token>" \
  -d '{"sql": "SELECT * FROM users LIMIT 10"}'
```

---

## Data Model

### Collections

Collections are containers for documents (similar to tables in SQL).

```python
# Create a collection
client.create_collection("users", {
    "indexes": ["email", "created_at"],
    "storage_tier": "hybrid"  # ram, ssd, hybrid
})

# List collections
collections = client.list_collections()

# Drop collection
client.drop_collection("old_data")
```

### Documents

Documents are JSON-like objects with flexible schemas.

```python
# Document structure
user = {
    "_id": "user_123",  # Optional, auto-generated if not provided
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
    "tags": ["premium", "active"],
    "address": {
        "city": "New York",
        "zip": "10001"
    },
    "created_at": datetime.now()
}

# Insert document
doc_id = await client.collection("users").insert(user)
```

### Schema Inference

LumaDB can automatically infer schemas:

```python
from tdbai import SchemaInference

# Infer schema from existing data
inferrer = SchemaInference()
schema = inferrer.infer("users", sample_data)

print(schema.fields)
# {
#   "name": FieldInfo(type=STRING, semantic=NAME),
#   "email": FieldInfo(type=EMAIL, semantic=EMAIL),
#   "age": FieldInfo(type=INTEGER, semantic=COUNT),
#   ...
# }
```

---

## Query Languages

LumaDB supports multiple query interfaces:

### 1. PromptQL (Natural Language + AI)

```python
# Simple query
result = await engine.query("Show all premium users")

# Complex query with reasoning
result = await engine.query(
    "Find users who spent more than the average last month "
    "and compare their activity with the previous quarter"
)

# Conversational follow-up
result = await engine.query("Now show only the top 10")

# Get explanation
explanation = await engine.explain(
    "Why are these users considered high-value?"
)
```

### 2. SQL

```python
# Standard SQL query
result = client.sql("""
    SELECT name, email, SUM(order_total) as total_spent
    FROM users u
    JOIN orders o ON u._id = o.user_id
    WHERE u.status = 'active'
    GROUP BY u._id, name, email
    HAVING SUM(order_total) > 1000
    ORDER BY total_spent DESC
    LIMIT 100
""")
```

### 3. Native API

```python
# Fluent API
result = await client.collection("users") \
    .find({"status": "active"}) \
    .filter(lambda u: u.age > 25) \
    .sort("created_at", descending=True) \
    .limit(100) \
    .execute()
```

---

## CRUD Operations

### Create (Insert)

```python
# Single insert
doc_id = await client.collection("users").insert({
    "name": "Alice",
    "email": "alice@example.com"
})

# Batch insert (high throughput)
doc_ids = await client.collection("users").batch_insert([
    {"name": "Bob", "email": "bob@example.com"},
    {"name": "Carol", "email": "carol@example.com"},
    {"name": "Dave", "email": "dave@example.com"},
])
print(f"Inserted {len(doc_ids)} documents")
```

### Read (Query)

```python
# Get by ID
user = await client.collection("users").get("user_123")

# Find with filter
users = await client.collection("users").find({
    "status": "active",
    "age": {"$gte": 18}
})

# Complex query
users = await client.collection("users").find({
    "$and": [
        {"status": "active"},
        {"$or": [
            {"subscription": "premium"},
            {"total_orders": {"$gte": 10}}
        ]}
    ]
})
```

### Update

```python
# Update by ID
success = await client.collection("users").update(
    "user_123",
    {"$set": {"status": "inactive"}}
)

# Update many
count = await client.collection("users").update_many(
    {"last_login": {"$lt": "2024-01-01"}},
    {"$set": {"status": "dormant"}}
)
print(f"Updated {count} documents")

# Upsert
await client.collection("users").upsert(
    {"email": "new@example.com"},
    {"name": "New User", "email": "new@example.com"}
)
```

### Delete

```python
# Delete by ID
success = await client.collection("users").delete("user_123")

# Delete many
count = await client.collection("users").delete_many({
    "status": "deleted",
    "deleted_at": {"$lt": "2023-01-01"}
})
```

---

## Indexing

### Index Types

| Type | Use Case | Example |
|------|----------|---------|
| **B-Tree** | Range queries, sorting | `created_at`, `price` |
| **Hash** | Exact match lookups | `email`, `user_id` |
| **Vector** | Similarity search | `embedding` |
| **Full-Text** | Text search | `description`, `content` |
| **Composite** | Multi-field queries | `(status, created_at)` |

### Creating Indexes

```python
# Single field index
await client.collection("users").create_index("email", type="hash")

# Composite index
await client.collection("orders").create_index(
    ["user_id", "created_at"],
    type="btree"
)

# Vector index for embeddings
await client.collection("products").create_index(
    "embedding",
    type="vector",
    dimensions=384,
    metric="cosine"
)

# Full-text index
await client.collection("articles").create_index(
    "content",
    type="fulltext",
    language="english"
)
```

### Index Management

```python
# List indexes
indexes = await client.collection("users").list_indexes()

# Drop index
await client.collection("users").drop_index("email_hash")

# Analyze index usage
stats = await client.collection("users").index_stats()
```

---

## Aggregations

### Basic Aggregations

```python
# Count
count = await client.collection("users").count({"status": "active"})

# Sum, Avg, Min, Max
stats = await client.collection("orders").aggregate({
    "total_revenue": {"$sum": "amount"},
    "avg_order": {"$avg": "amount"},
    "min_order": {"$min": "amount"},
    "max_order": {"$max": "amount"}
})
```

### Group By

```python
# Group by single field
result = await client.collection("orders").aggregate([
    {"$match": {"status": "completed"}},
    {"$group": {
        "_id": "$customer_id",
        "total_orders": {"$count": 1},
        "total_spent": {"$sum": "$amount"}
    }},
    {"$sort": {"total_spent": -1}},
    {"$limit": 10}
])

# Group by time period
result = await client.collection("events").aggregate([
    {"$group": {
        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
        "count": {"$count": 1}
    }},
    {"$sort": {"_id": 1}}
])
```

### Time-Series Aggregations

```python
# Downsampling
result = await client.collection("metrics").time_series({
    "field": "timestamp",
    "interval": "1h",
    "aggregations": {
        "avg_cpu": {"$avg": "cpu_usage"},
        "max_memory": {"$max": "memory_usage"}
    },
    "range": {
        "start": "2024-01-01",
        "end": "2024-01-31"
    }
})

# Moving average
result = await client.collection("stocks").moving_average(
    field="price",
    window=7,
    time_field="date"
)
```

---

## Transactions

### Single Document Transactions

```python
# Atomic update
await client.collection("accounts").find_one_and_update(
    {"_id": "acc_123", "balance": {"$gte": 100}},
    {"$inc": {"balance": -100}}
)
```

### Multi-Document Transactions

```python
async with client.transaction() as txn:
    # Debit source account
    await txn.collection("accounts").update(
        "acc_source",
        {"$inc": {"balance": -100}}
    )

    # Credit destination account
    await txn.collection("accounts").update(
        "acc_dest",
        {"$inc": {"balance": 100}}
    )

    # Create transfer record
    await txn.collection("transfers").insert({
        "from": "acc_source",
        "to": "acc_dest",
        "amount": 100,
        "timestamp": datetime.now()
    })

    # Transaction commits automatically on exit
    # Rolls back on exception
```

---

## PromptQL Guide

### Basic Usage

```python
from tdbai import PromptQLEngine, LLMConfig

# Initialize with LLM
engine = PromptQLEngine(
    db_client=client,
    llm_config=LLMConfig(
        provider="openai",  # or "anthropic", "local"
        model="gpt-4",
        api_key="your-key"
    )
)

# Simple query
result = await engine.query("Show me all users from New York")
```

### Query Types

```python
# Retrieval
result = await engine.query("Get all premium customers")

# Counting
result = await engine.query("How many orders were placed last week?")

# Aggregation
result = await engine.query("What's the average order value by category?")

# Comparison
result = await engine.query(
    "Compare sales performance between Q1 and Q2"
)

# Trend Analysis
result = await engine.query(
    "Show me the revenue trend over the past 12 months"
)

# Complex Reasoning
result = await engine.query(
    "Find customers who might churn based on their recent activity"
)
```

### Conversation Context

```python
# First query
result = await engine.query("Show users who signed up this month")

# Follow-up (understands context)
result = await engine.query("Filter those to only premium members")

# Another follow-up
result = await engine.query("Now sort by their total spending")

# Reference previous results
result = await engine.query("Email these users with a promotion")
```

### Explanation Mode

```python
# Get query explanation
explanation = await engine.explain(
    "Why did you filter out inactive users?"
)

# Get suggestions
suggestions = await engine.suggest("I want to analyze customer behavior")
# Returns: ["Show customer segments by spending",
#          "Analyze purchase frequency patterns", ...]
```

---

## Best Practices

### Performance Optimization

1. **Use appropriate indexes**
   ```python
   # Index fields used in WHERE clauses
   await collection.create_index("status")
   await collection.create_index(["user_id", "created_at"])
   ```

2. **Batch operations**
   ```python
   # Instead of individual inserts
   for doc in documents:
       await collection.insert(doc)  # Slow

   # Use batch insert
   await collection.batch_insert(documents)  # Fast
   ```

3. **Project only needed fields**
   ```python
   # Instead of fetching all fields
   users = await collection.find({})  # Slow

   # Select specific fields
   users = await collection.find({}, projection=["name", "email"])  # Fast
   ```

4. **Use appropriate data types**
   ```python
   # Use native datetime instead of strings
   doc = {
       "created_at": datetime.now(),  # Good
       # "created_at": "2024-01-01T00:00:00Z"  # Less efficient
   }
   ```

### Data Modeling

1. **Denormalize for read performance**
   ```python
   # Instead of joining
   order = {
       "user_id": "user_123",
       "user_name": "John Doe",  # Denormalized
       "user_email": "john@example.com",  # Denormalized
       "items": [...]
   }
   ```

2. **Use embedded documents wisely**
   ```python
   # Good: Small, bounded arrays
   user = {
       "tags": ["premium", "active"],
       "recent_orders": [...]  # Limited to last 10
   }

   # Bad: Unbounded arrays
   user = {
       "all_orders": [...]  # Could grow forever
   }
   ```

### Security

1. **Use parameterized queries**
   ```python
   # Safe
   result = await collection.find({"user_id": user_input})

   # Unsafe (SQL injection risk)
   result = await client.sql(f"SELECT * FROM users WHERE id = '{user_input}'")
   ```

2. **Implement field-level access control**
   ```python
   # Define field permissions
   await client.set_permissions("users", {
       "public": ["name", "avatar"],
       "private": ["email", "phone"],
       "admin": ["*"]
   })
   ```

---

## Next Steps

- [PromptQL Tutorial](../tutorials/promptql-tutorial.md)
- [API Reference](../api-reference/python-api.md)
- [Performance Optimization](../performance/optimization.md)
