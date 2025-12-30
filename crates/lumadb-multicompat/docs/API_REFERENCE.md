# LumaDB Multi-Protocol API Reference

LumaDB exposes three distinct HTTP protocols on a single server port (default: 8000), routed by path prefixes.

Base URL: `http://<host>:8000`

---

## 1. DynamoDB Protocol
**Prefix:** `/dynamodb`  
**Content-Type:** `application/x-amz-json-1.0`  
**Auth:** AWS Signature V4 (recommended) or ignored if auth disabled.

### Headers
- `X-Amz-Target`: Specifies the operation (e.g., `DynamoDB_20120810.PutItem`)
- `Authorization`: AWS4-HMAC-SHA256 signature

### Supported Operations

| Operation | Description | Request Body Example |
|-----------|-------------|----------------------|
| `PutItem` | Create/Replace item | `{"TableName": "T", "Item": {"pk": {"S": "1"}}}` |
| `GetItem` | Retrieve item | `{"TableName": "T", "Key": {"pk": {"S": "1"}}}` |
| `DeleteItem` | Delete item | `{"TableName": "T", "Key": {"pk": {"S": "1"}}}` |
| `UpdateItem` | Modify attributes | `{"TableName": "T", "Key": {...}, "UpdateExpression": "SET a=:v"}` |
| `Query` | Query by PK/SK | `{"TableName": "T", "KeyConditionExpression": "pk=:v"}` |
| `Scan` | Scan table | `{"TableName": "T", "Limit": 10}` |
| `BatchWriteItem` | Multi-put/delete | `{"RequestItems": {"T": [...]}}` |
| `CreateTable` | Define schema | `{"TableName": "T", "KeySchema": [...], "AttributeDefinitions": [...]}` |

### Data Types (JSON Format)
- `S`: String
- `N`: Number (transported as string)
- `B`: Binary (base64 encoded)
- `BOOL`: Boolean
- `NULL`: Null
- `L`: List
- `M`: Map
- `SS`: String Set
- `NS`: Number Set
- `BS`: Binary Set

---

## 2. Cloudflare D1 Protocol
**Prefix:** `/d1`  
**Content-Type:** `application/json`

### Endpoints

#### POST `/d1/query`
Execute one or more SQL statements.

**Request:**
```json
{
  "sql": "SELECT * FROM users WHERE id = ?",
  "params": [123]
}
```

**Response:**
```json
{
  "result": [
    {
      "results": [ {"id": 123, "name": "Alice"} ],
      "success": true,
      "meta": {
        "duration": 0.5,
        "rows_read": 1,
        "rows_written": 0
      }
    }
  ],
  "success": true,
  "errors": [],
  "messages": []
}
```

#### POST `/d1/execute`
Alias for `/query`, compatible with some D1 client versions.

---

## 3. Turso / LibSQL Protocol
**Prefix:** `/turso`  
**Content-Type:** `application/json`

### Endpoints

#### POST `/turso/v2/pipeline`
Execute a pipeline of requests (used for transactions or batches).

**Request:**
```json
{
  "requests": [
    { "type": "execute", "stmt": { "sql": "BEGIN" } },
    { "type": "execute", "stmt": { "sql": "INSERT INTO foo VALUES (1)" } },
    { "type": "execute", "stmt": { "sql": "COMMIT" } },
    { "type": "close" }
  ]
}
```

**Response:**
```json
{
  "batched": true,
  "results": [
    { "type": "ok", "response": { "type": "execute", "result": { "affected_row_count": 0 } } },
    ...
  ]
}
```

#### POST `/turso/v1/execute`
Single statement execution (Legacy).

**Request:**
```json
{
  "stmt": {
    "sql": "SELECT * FROM users",
    "args": []
  }
}
```

### Custom Types
Turso protocol uses a specific JSON encoding for values:

```json
{ "type": "text", "value": "string" }
{ "type": "integer", "value": "123" }
{ "type": "float", "value": 1.23 }
{ "type": "blob", "base64": "..." }
{ "type": "null" }
```

---

## Error Codes

### Common Errors
- `400 Bad Request`: Invalid JSON, missing parameters, or syntax error.
- `401 Unauthorized`: Missing or invalid signatures/tokens.
- `404 Not Found`: Endpoint or Table not found.
- `500 Internal Server Error`: Storage engine failure.

### Protocol-Specific
- **DynamoDB:** Returns `__type` field (e.g., `com.amazon.coral.service#ResourceNotFoundException`).
- **D1:** Returns `errors` array with code and message.
- **Turso:** Returns `error` object inside result.
