# LumaDB Migration Guide

This guide details how to migrate your existing applications from AWS DynamoDB, Cloudflare D1, or Turso/LibSQL to LumaDB's multi-protocol compatibility layer.

## 🚀 Migration Strategy Overview

LumaDB is designed as a **drop-in replacement**. In most cases, you only need to change your connection string/endpoint URL and authentication credentials.

| Source System | LumaDB Protocol | Compatibility Level |
|---------------|-----------------|---------------------|
| **DynamoDB** | DynamoDB JSON | High (Core API + SDKs) |
| **D1** | Cloudflare D1 HTTP | High (Workers API) |
| **Turso** | LibSQL / SQLite | High (HTTP V1/V2) |

---

## 📦 AWS DynamoDB to LumaDB

### 1. Update Connection Configuration

Changed your AWS SDK configuration to point to LumaDB.

**Node.js (aws-sdk v3):**
```javascript
const client = new DynamoDBClient({
  endpoint: "http://localhost:8000/dynamodb", // LumaDB endpoint
  region: "us-east-1",                        // Required but ignored
  credentials: {
    accessKeyId: "lumadb",                    // Any string
    secretAccessKey: "lumadb-secret"          // Any string
  }
});
```

**Python (boto3):**
```python
dynamodb = boto3.resource(
    'dynamodb',
    endpoint_url='http://localhost:8000/dynamodb',
    region_name='us-east-1',
    aws_access_key_id='lumadb',
    aws_secret_access_key='lumadb-secret'
)
```

**Go (aws-sdk-go-v2):**
```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithEndpointResolver(aws.EndpointResolverFunc(
        func(service, region string) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:8000/dynamodb"}, nil
        })),
)
```

### 2. Supported Features & Limitations

✅ **Supported:**
- `PutItem`, `GetItem`, `DeleteItem`, `UpdateItem`
- `Query`, `Scan` (with FilterExpression)
- `BatchWriteItem`, `BatchGetItem`
- `TransactWriteItems`
- `CreateTable`, `DeleteTable`, `ListTables`, `DescribeTable`
- Primary Keys (Pk, Sk) and attribute types (S, N, B, BOOL, NULL, L, M, SS, NS, BS)

⚠️ **Limited / In Progress:**
- Global Secondary Indexes (GSI) - Metadata stored but query optimization pending
- UpdateExpression - `SET`, `REMOVE` supported; `ADD`, `DELETE` partial
- Condition Expressions - Basic functionality available
- Streams (DynamoDB Streams) - Not yet implemented

### 3. Data Migration

To migrate data, use the standard AWS `Scan` operation on your existing DynamoDB tables and `BatchWriteItem` to LumaDB.

**Migration Script Example (Pseudo-code):**
```python
src_client = boto3.resource('dynamodb') # AWS
dst_client = boto3.resource('dynamodb', endpoint_url='...') # LumaDB

table = src_client.Table('Users')
scan = table.scan()
with dst_client.Table('Users').batch_writer() as batch:
    for item in scan['Items']:
        batch.put_item(Item=item)
```

---

## 🌩️ Cloudflare D1 to LumaDB

### 1. Worker Configuration (`wrangler.toml`)

You cannot directly re-bind the `DB` binding in `wrangler.toml` to an external HTTP URL yet (Cloudflare limitation). Instead, use a **Service Binding** or simply use `fetch` in your Worker code if your LumaDB is publicly accessible.

**Option A: Using `fetch` (simplest migration)**
Replace `env.DB.prepare(...)` with a helper class that calls LumaDB's HTTP endpoint.

**Option B: LumaDB as D1 (Self-hosted Workers)**
If running self-hosted Workers (e.g., using `workerd`), you can configure the D1 shim to point to LumaDB.

### 2. SQL Compatibility

LumaDB uses a SQLite-compatible parser and execution engine.

✅ **Supported:**
- Standard SQL (`SELECT`, `INSERT`, `UPDATE`, `DELETE`)
- Parameterized queries (`?` bindings)
- Batch execution
- Transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
- JSON functions

### 3. API Response Format

LumaDB mimics the D1 JSON response format exactly:
```json
{
  "success": true,
  "result": [
    {
      "results": [ ...rows... ],
      "meta": { "duration": 1.2 }
    }
  ]
}
```

---

## 💎 Turso / LibSQL to LumaDB

### 1. Client Configuration

LumaDB implements the LibSQL HTTP protocol (v1/v2).

**Python (`libsql-experimental`):**
```python
import libsql_experimental as libsql
conn = libsql.connect("http://localhost:8000/turso", auth_token="any")
```

**TypeScript (`@libsql/client`):**
```typescript
import { createClient } from "@libsql/client";
const client = createClient({
  url: "http://localhost:8000/turso",
  authToken: "any"
});
```

### 2. Protocol Features

✅ **Supported:**
- Batch execution
- Interactive Transactions (via Pipeline API)
- Named arguments (`:name`) and positional arguments (`?`)
- Blob type handling (base64 encoded)

---

## ⚙️ Performance Tuning

### Connection Pooling
LumaDB handles connection pooling internally. However, for clients:
- **Node.js/JS:** Use `keepAlive: true` in your HTTP agents.
- **Python:** Use session objects with `requests` or reuse client instances.

### Batch Operations
Always prefer batch operations for bulk data ingestion:
- **DynamoDB:** `BatchWriteItem` (25 items/req)
- **SQL (D1/Turso):** Batch insert (`INSERT INTO t VALUES (...), (...), (...)`)

### Caching
LumaDB includes an integrated LRU Query Cache.
- Read-heavy workloads benefit automatically.
- Ensure efficient key schemas to maximize cache hits.

---

## 🔍 Troubleshooting

**Error: `ResourceNotFoundException` (DynamoDB)**
- Ensure the table is created first using `CreateTable`. LumaDB does not auto-create tables on write.

**Error: `SQL Syntax Error` (D1/Turso)**
- LumaDB's SQL dialect is SQLite-compatible. Check for non-standard SQL extensions used in your previous environment.

**Latency Issues**
- Check your network latency to the LumaDB instance.
- Enable `RUST_LOG=debug` on the server to trace query execution times.
