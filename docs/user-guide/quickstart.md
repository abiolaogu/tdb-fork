# LumaDB Quick Start Guide

## Get Running in 5 Minutes

### 1. Install

```bash
# Docker (fastest)
docker run -d -p 8080:8080 -p 9090:9090 tdbplus/tdbplus:latest

# Or native
curl -LO https://releases.tdbplus.io/latest/install.sh && bash install.sh
```

### 2. Connect

```python
# Python
from tdb import LumaDBClient
client = LumaDBClient(host="localhost", port=8080)
```

```go
// Go
client, _ := tdb.Connect(&tdb.Config{Host: "localhost", Port: 8080})
```

```bash
# REST API
curl http://localhost:8080/health
```

### 3. Basic Operations

```python
# Create collection
users = client.collection("users")

# Insert
await users.insert({"name": "Alice", "email": "alice@example.com"})

# Find
user = await users.find({"email": "alice@example.com"})

# Update
await users.update(user["_id"], {"$set": {"status": "active"}})

# Delete
await users.delete(user["_id"])
```

### 4. PromptQL (AI Queries)

```python
from tdbai import PromptQLEngine, LLMConfig

engine = PromptQLEngine(
    db_client=client,
    llm_config=LLMConfig(provider="openai", api_key="your-key")
)

# Natural language queries
result = await engine.query("Show all active users from last month")
result = await engine.query("What's the average order value?")
result = await engine.query("Compare this month vs last month sales")
```

### 5. SQL

```python
result = await client.sql("""
    SELECT name, COUNT(*) as orders
    FROM users u JOIN orders o ON u._id = o.user_id
    GROUP BY name ORDER BY orders DESC LIMIT 10
""")
```

## Next Steps

- [Full User Manual](./manual.md)
- [PromptQL Tutorial](../tutorials/promptql-tutorial.md)
- [Performance Guide](../performance/optimization.md)
