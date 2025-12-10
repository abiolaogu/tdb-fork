# TDB+ Python API Reference

## tdb Module

### TDBClient

```python
class TDBClient:
    def __init__(host: str, port: int, username: str = None, password: str = None)
    def collection(name: str) -> Collection
    async def sql(query: str) -> List[Dict]
    async def close()
```

### Collection

```python
class Collection:
    async def insert(doc: Dict) -> str
    async def batch_insert(docs: List[Dict]) -> List[str]
    async def get(id: str) -> Optional[Dict]
    async def find(query: Dict, projection: List[str] = None) -> List[Dict]
    async def update(id: str, update: Dict) -> bool
    async def update_many(query: Dict, update: Dict) -> int
    async def delete(id: str) -> bool
    async def delete_many(query: Dict) -> int
    async def count(query: Dict = None) -> int
    async def aggregate(pipeline: List[Dict]) -> List[Dict]
    async def create_index(fields: Union[str, List[str]], type: str = "btree")
```

## tdbai Module

### PromptQLEngine

```python
class PromptQLEngine:
    def __init__(db_client: TDBClient, llm_config: LLMConfig)
    async def query(prompt: str) -> QueryResult
    async def explain(prompt: str) -> str
    async def suggest(context: str) -> List[str]
```

### LLMConfig

```python
@dataclass
class LLMConfig:
    provider: str        # "openai", "anthropic", "local"
    model: str           # "gpt-4", "claude-3-opus", etc.
    api_key: str = None
    api_base: str = None
    temperature: float = 0.1
    max_tokens: int = 2000
```

### QueryResult

```python
@dataclass
class QueryResult:
    success: bool
    data: Any
    row_count: int
    execution_time_ms: float
    reasoning_steps: List[str]
```

## Query Operators

| Operator | Example |
|----------|---------|
| `$eq` | `{"status": {"$eq": "active"}}` |
| `$ne` | `{"status": {"$ne": "deleted"}}` |
| `$gt`, `$gte` | `{"age": {"$gt": 18}}` |
| `$lt`, `$lte` | `{"price": {"$lt": 100}}` |
| `$in` | `{"status": {"$in": ["active", "pending"]}}` |
| `$and`, `$or` | `{"$and": [{...}, {...}]}` |
| `$exists` | `{"email": {"$exists": true}}` |

## Update Operators

| Operator | Example |
|----------|---------|
| `$set` | `{"$set": {"status": "active"}}` |
| `$unset` | `{"$unset": {"temp_field": 1}}` |
| `$inc` | `{"$inc": {"count": 1}}` |
| `$push` | `{"$push": {"tags": "new"}}` |
| `$pull` | `{"$pull": {"tags": "old"}}` |
