# LumaDB User Manual

## 1. Introduction

Welcome to LumaDB! This manual will guide you through installing, configuring, and building applications with LumaDB.

## 2. Installation

### 2.1 Docker (Recommended)
The easiest way to run LumaDB is via Docker.

```bash
docker run -d -p 8080:8080 -p 10000:10000 --name lumadb lumadb/server:latest
```

### 2.2 From Source
Requirements: Rust 1.70+, Go 1.21+.

```bash
git clone https://github.com/lumadb/luma.git
cd luma
./build.sh
./bin/luma-server --config luma.toml
```

## 3. Configuration (`luma.toml`)

LumaDB is highly configurable. The main configuration file `luma.toml` is found in the root directory.

### 3.1 Tiering Configuration
Configure how data moves between RAM, SSD, and HDD.

```toml
[tiering]
# Enable multi-tier storage
enabled = true

[tiering.warm_policy]
# Move to SSD after 1 hour of inactivity
age_threshold_seconds = 3600
# Usage Erasure Coding for efficiency
strategy = { type = "ErasureCoding", data_shards = 6, parity_shards = 3 }
```

## 4. Using the Database

### 4.1 Connecting
LumaDB exposes an HTTP API and a gRPC interface.

**Using curl:**
```bash
curl -X POST http://localhost:8080/query \
  -H "Content-Type: application/json" \
  -d '{"type": "LQL", "query": "INSERT INTO users (name) VALUES (\"Alice\")"}'
```

**Using TypeScript SDK:**
```typescript
import { Database } from 'lumadb';
const db = new Database('http://localhost:8080');
await db.lql("SELECT * FROM users");
```

### 4.2 Query Languages
LumaDB supports three query languages:

1. **LQL (Luma Query Language)**: SQL-compatible.
   `SELECT * FROM users WHERE age > 21`

2. **NQL (Natural Query Language)**: AI-powered.
   `find users older than 21`

3. **JQL (JSON Query Language)**: MongoDB-style.
   `{ "find": "users", "filter": { "age": { "$gt": 21 } } }`

## 5. Troubleshooting

**Common Issues:**
- **"Connection Refused"**: Ensure the server is running and port 8080 is open.
- **"Storage Full"**: Check `luma.toml` limits or add more storage nodes.

For more help, visit [docs.lumadb.com](https://docs.lumadb.com).
