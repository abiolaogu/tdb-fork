# LumaDB Developer Training Manual

## Comprehensive Training Guide for LumaDB Development

---

## Course Overview

### Target Audience
- Software developers building applications with LumaDB
- Data engineers designing data pipelines
- Full-stack developers needing database expertise

### Prerequisites
- Basic programming knowledge (Python, Go, or JavaScript)
- Understanding of database concepts (CRUD, indexes, queries)
- Familiarity with REST APIs

### Learning Objectives
After completing this training, you will be able to:
- Design efficient data models for LumaDB
- Write high-performance queries using multiple interfaces
- Implement PromptQL for AI-powered applications
- Optimize queries for production workloads
- Integrate LumaDB into your applications

---

## Module 1: LumaDB Fundamentals

### 1.1 Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Your Application                         │
└───────────────────────────┬─────────────────────────────────┘
                            │
                    ┌───────┴───────┐
                    │   LumaDB Client │
                    └───────┬───────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
┌───────┴───────┐   ┌───────┴───────┐   ┌───────┴───────┐
│  REST API     │   │  gRPC API     │   │  Native API   │
│  (Port 8080)  │   │  (Port 9090)  │   │  (Direct)     │
└───────────────┘   └───────────────┘   └───────────────┘
                            │
                    ┌───────┴───────┐
                    │  LumaDB Server  │
                    └───────────────┘
```

### 1.2 Key Concepts

**Collections**: Containers for related documents
```python
# Think of collections as tables, but schema-flexible
users = client.collection("users")
orders = client.collection("orders")
```

**Documents**: JSON-like data records
```python
user_document = {
    "_id": "auto-generated-or-specified",
    "field1": "value1",
    "nested": {
        "field2": "value2"
    },
    "array_field": [1, 2, 3]
}
```

**Indexes**: Performance optimization structures
```python
# B-Tree: Range queries, sorting
# Hash: Exact lookups
# Vector: Similarity search
# Full-text: Text search
```

### 1.3 Hands-On Exercise: Hello LumaDB

```python
# Exercise 1: Connect and perform basic operations

import asyncio
from tdb import LumaDBClient

async def hello_tdbplus():
    # Connect to LumaDB
    client = LumaDBClient(host="localhost", port=8080)

    # Create a collection
    users = client.collection("training_users")

    # Insert a document
    doc_id = await users.insert({
        "name": "Training User",
        "email": "training@example.com",
        "course": "LumaDB Developer Training"
    })
    print(f"Inserted document: {doc_id}")

    # Read the document
    user = await users.get(doc_id)
    print(f"Retrieved: {user}")

    # Update the document
    await users.update(doc_id, {"$set": {"completed": True}})

    # Delete the document
    await users.delete(doc_id)
    print("Document deleted")

    # Cleanup
    await client.drop_collection("training_users")

# Run the exercise
asyncio.run(hello_tdbplus())
```

**Expected Output:**
```
Inserted document: training_users_abc123
Retrieved: {'_id': 'training_users_abc123', 'name': 'Training User', ...}
Document deleted
```

---

## Module 2: Data Modeling

### 2.1 Schema Design Principles

#### Principle 1: Model for Access Patterns

```python
# BAD: Normalized (requires joins)
user = {"_id": "u1", "name": "John"}
order = {"_id": "o1", "user_id": "u1", "total": 100}
# Requires separate queries to get user with orders

# GOOD: Denormalized for common access pattern
user_with_orders = {
    "_id": "u1",
    "name": "John",
    "recent_orders": [
        {"order_id": "o1", "total": 100, "date": "2024-01-01"},
        {"order_id": "o2", "total": 150, "date": "2024-01-02"}
    ],
    "total_orders": 2,
    "total_spent": 250
}
```

#### Principle 2: Bound Array Sizes

```python
# BAD: Unbounded array
user = {
    "all_orders": [...]  # Could grow to millions
}

# GOOD: Bounded arrays with overflow handling
user = {
    "recent_orders": [...],  # Last 10 only
    "order_count": 1000000   # Total count stored separately
}
# Full orders in separate collection
```

#### Principle 3: Use Appropriate Data Types

```python
from datetime import datetime
from decimal import Decimal

document = {
    # Use native datetime
    "created_at": datetime.now(),  # NOT: "2024-01-01T00:00:00Z"

    # Use Decimal for money
    "price": Decimal("99.99"),  # NOT: 99.99 (float precision issues)

    # Use integers for IDs when possible
    "user_id": 12345,  # Faster than "user_12345"

    # Use arrays for tags
    "tags": ["sale", "featured"],  # NOT: "sale,featured"
}
```

### 2.2 Hands-On Exercise: E-Commerce Schema

Design a schema for an e-commerce application:

```python
# Exercise 2: Design e-commerce data model

# Users Collection
user_schema = {
    "_id": "string",  # UUID or incremental
    "email": "string",  # Unique, indexed
    "name": "string",
    "password_hash": "string",
    "address": {
        "street": "string",
        "city": "string",
        "country": "string",
        "zip": "string"
    },
    "payment_methods": [{
        "type": "string",  # credit_card, paypal
        "last_four": "string",
        "is_default": "boolean"
    }],
    "preferences": {
        "currency": "string",
        "language": "string"
    },
    "stats": {
        "total_orders": "integer",
        "total_spent": "decimal",
        "last_order_date": "datetime"
    },
    "created_at": "datetime",
    "updated_at": "datetime"
}

# Products Collection
product_schema = {
    "_id": "string",
    "sku": "string",  # Unique, indexed
    "name": "string",
    "description": "string",  # Full-text indexed
    "category_id": "string",  # Indexed
    "price": "decimal",
    "inventory": {
        "quantity": "integer",
        "reserved": "integer",
        "warehouse": "string"
    },
    "attributes": {  # Flexible attributes
        "color": "string",
        "size": "string"
    },
    "images": ["string"],
    "embedding": "vector",  # For similarity search
    "created_at": "datetime"
}

# Orders Collection
order_schema = {
    "_id": "string",
    "order_number": "string",  # Human-readable
    "user_id": "string",  # Indexed
    "user_email": "string",  # Denormalized for emails
    "status": "string",  # pending, paid, shipped, delivered
    "items": [{
        "product_id": "string",
        "sku": "string",
        "name": "string",  # Denormalized
        "quantity": "integer",
        "unit_price": "decimal",
        "subtotal": "decimal"
    }],
    "totals": {
        "subtotal": "decimal",
        "tax": "decimal",
        "shipping": "decimal",
        "total": "decimal"
    },
    "shipping_address": {
        "name": "string",
        "street": "string",
        "city": "string",
        "country": "string",
        "zip": "string"
    },
    "payment": {
        "method": "string",
        "transaction_id": "string",
        "paid_at": "datetime"
    },
    "created_at": "datetime",  # Indexed for time-series
    "updated_at": "datetime"
}

# Create collections with indexes
async def setup_ecommerce_schema(client):
    # Users
    users = client.collection("users")
    await users.create_index("email", type="hash", unique=True)
    await users.create_index("created_at", type="btree")

    # Products
    products = client.collection("products")
    await products.create_index("sku", type="hash", unique=True)
    await products.create_index("category_id", type="btree")
    await products.create_index("description", type="fulltext")
    await products.create_index("embedding", type="vector", dimensions=384)

    # Orders
    orders = client.collection("orders")
    await orders.create_index("user_id", type="btree")
    await orders.create_index("status", type="hash")
    await orders.create_index("created_at", type="btree")
    await orders.create_index(["user_id", "created_at"], type="btree")
```

---

## Module 3: Query Mastery

### 3.1 Query Interface Comparison

| Interface | Best For | Complexity |
|-----------|----------|------------|
| **PromptQL** | Complex analytics, exploration | Natural language |
| **SQL** | Familiar syntax, joins | Standard SQL |
| **Native API** | Performance-critical, programmatic | Method chaining |

### 3.2 PromptQL Deep Dive

```python
from tdbai import PromptQLEngine, LLMConfig

# Initialize PromptQL
engine = PromptQLEngine(
    db_client=client,
    llm_config=LLMConfig(
        provider="openai",
        model="gpt-4",
        api_key="your-key"
    )
)

# Basic Queries
async def promptql_basics():
    # Simple retrieval
    result = await engine.query("Show all active users")

    # Filtering
    result = await engine.query("Find users from California with more than 5 orders")

    # Aggregation
    result = await engine.query("What's the average order value by product category?")

    # Time-based
    result = await engine.query("Show daily revenue for the last 30 days")

    return result

# Advanced Queries with Reasoning
async def promptql_advanced():
    # Multi-step reasoning
    result = await engine.query(
        "Find customers who spent more than average last quarter, "
        "show how their spending compares to the previous quarter, "
        "and identify which product categories drove the increase"
    )

    # Conversational context
    await engine.query("Show top 100 customers by revenue")
    await engine.query("Filter to only those who joined this year")
    await engine.query("Group them by acquisition channel")

    # Explanation
    explanation = await engine.explain("Why did some customers decrease spending?")

    return result
```

### 3.3 SQL Interface

```python
# SQL queries for complex operations
async def sql_examples():
    # Join example
    result = await client.sql("""
        SELECT
            u.name,
            u.email,
            COUNT(o.id) as order_count,
            SUM(o.total) as total_spent
        FROM users u
        LEFT JOIN orders o ON u._id = o.user_id
        WHERE u.created_at > '2024-01-01'
        GROUP BY u._id, u.name, u.email
        HAVING COUNT(o.id) > 0
        ORDER BY total_spent DESC
        LIMIT 100
    """)

    # Window functions
    result = await client.sql("""
        SELECT
            user_id,
            order_date,
            total,
            SUM(total) OVER (
                PARTITION BY user_id
                ORDER BY order_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as running_total
        FROM orders
    """)

    # CTE (Common Table Expression)
    result = await client.sql("""
        WITH monthly_revenue AS (
            SELECT
                DATE_TRUNC('month', created_at) as month,
                SUM(total) as revenue
            FROM orders
            WHERE status = 'completed'
            GROUP BY DATE_TRUNC('month', created_at)
        )
        SELECT
            month,
            revenue,
            LAG(revenue) OVER (ORDER BY month) as prev_month,
            (revenue - LAG(revenue) OVER (ORDER BY month)) / LAG(revenue) OVER (ORDER BY month) * 100 as growth_pct
        FROM monthly_revenue
        ORDER BY month
    """)

    return result
```

### 3.4 Native API

```python
# High-performance native API
async def native_api_examples():
    users = client.collection("users")

    # Fluent query building
    result = await users \
        .find({"status": "active"}) \
        .filter(lambda u: u["orders_count"] > 5) \
        .project(["name", "email", "orders_count"]) \
        .sort("orders_count", descending=True) \
        .limit(100) \
        .execute()

    # Aggregation pipeline
    result = await users.aggregate([
        {"$match": {"status": "active"}},
        {"$lookup": {
            "from": "orders",
            "localField": "_id",
            "foreignField": "user_id",
            "as": "orders"
        }},
        {"$addFields": {
            "order_count": {"$size": "$orders"},
            "total_spent": {"$sum": "$orders.total"}
        }},
        {"$match": {"order_count": {"$gte": 5}}},
        {"$sort": {"total_spent": -1}},
        {"$limit": 100}
    ])

    return result
```

### 3.5 Hands-On Exercise: Query Challenge

```python
# Exercise 3: Complete these queries using your preferred interface

async def query_challenges():
    """
    Challenge 1: Find the top 10 products by revenue in the last 30 days

    Challenge 2: Calculate customer lifetime value (total spend) by signup month

    Challenge 3: Find products that are frequently bought together

    Challenge 4: Identify customers at risk of churning
                 (no orders in last 60 days but active in previous 60 days)

    Challenge 5: Create a sales report showing daily, weekly, and monthly trends
    """

    # Challenge 1: Top products by revenue
    # PromptQL solution:
    result1 = await engine.query(
        "Show top 10 products by revenue in the last 30 days"
    )

    # SQL solution:
    result1_sql = await client.sql("""
        SELECT p.name, SUM(oi.subtotal) as revenue
        FROM order_items oi
        JOIN products p ON oi.product_id = p._id
        JOIN orders o ON oi.order_id = o._id
        WHERE o.created_at >= NOW() - INTERVAL '30 days'
        GROUP BY p._id, p.name
        ORDER BY revenue DESC
        LIMIT 10
    """)

    # Challenge 2: Customer LTV by signup month
    result2 = await client.sql("""
        SELECT
            DATE_TRUNC('month', u.created_at) as signup_month,
            COUNT(DISTINCT u._id) as customers,
            SUM(o.total) as total_revenue,
            SUM(o.total) / COUNT(DISTINCT u._id) as avg_ltv
        FROM users u
        LEFT JOIN orders o ON u._id = o.user_id
        GROUP BY DATE_TRUNC('month', u.created_at)
        ORDER BY signup_month
    """)

    # Challenge 3: Frequently bought together
    result3 = await client.sql("""
        SELECT
            oi1.product_id as product_a,
            oi2.product_id as product_b,
            COUNT(*) as co_purchase_count
        FROM order_items oi1
        JOIN order_items oi2 ON oi1.order_id = oi2.order_id
            AND oi1.product_id < oi2.product_id
        GROUP BY oi1.product_id, oi2.product_id
        HAVING COUNT(*) >= 10
        ORDER BY co_purchase_count DESC
        LIMIT 20
    """)

    # Challenge 4: Churn risk customers
    result4 = await engine.query(
        "Find customers who were active 60-120 days ago "
        "but have not placed any orders in the last 60 days"
    )

    # Challenge 5: Multi-period sales report
    result5 = await engine.query(
        "Create a sales report showing total revenue and order count "
        "broken down by day for the last week, by week for the last month, "
        "and by month for the last year"
    )

    return {
        "top_products": result1,
        "customer_ltv": result2,
        "bought_together": result3,
        "churn_risk": result4,
        "sales_report": result5
    }
```

---

## Module 4: Performance Optimization

### 4.1 Query Optimization Techniques

```python
# Optimization techniques with examples

# 1. Use covered queries (index contains all needed fields)
# BAD: Requires document fetch
await users.find({"email": "test@example.com"})  # Returns full document

# GOOD: Covered by index
await users.find(
    {"email": "test@example.com"},
    projection=["email", "name"]  # Only indexed fields
)

# 2. Use compound indexes for common query patterns
await collection.create_index(["status", "created_at"])

# Query uses full index
await collection.find({
    "status": "active",
    "created_at": {"$gte": "2024-01-01"}
})

# 3. Avoid $ne and $nin (can't use indexes efficiently)
# BAD
await users.find({"status": {"$ne": "deleted"}})

# GOOD: Query for specific values
await users.find({"status": {"$in": ["active", "pending"]}})

# 4. Use batch operations
# BAD: Individual operations
for item in items:
    await collection.insert(item)

# GOOD: Batch insert
await collection.batch_insert(items)

# 5. Limit returned fields
# BAD
await collection.find({})  # All fields

# GOOD
await collection.find({}, projection=["name", "email"])
```

### 4.2 Explain Plans

```python
# Analyze query execution
async def analyze_query():
    # Get execution plan
    plan = await client.explain("""
        SELECT * FROM orders
        WHERE user_id = 'u123' AND created_at > '2024-01-01'
        ORDER BY created_at DESC
        LIMIT 10
    """)

    print(f"Query Plan: {plan}")
    # Output:
    # {
    #   "operation": "INDEX_SCAN",
    #   "index": "user_id_created_at",
    #   "estimated_rows": 15,
    #   "estimated_cost": 0.05,
    #   "actual_time_ms": 0.3
    # }

    # Analyze index usage
    stats = await collection.index_stats()
    for idx in stats:
        print(f"Index: {idx['name']}, Hits: {idx['hits']}, Misses: {idx['misses']}")
```

### 4.3 Hands-On Exercise: Optimization Challenge

```python
# Exercise 4: Optimize these slow queries

async def optimization_challenge():
    """
    Given: A users collection with 10M documents
    Indexes: _id (primary), email (unique hash)

    Optimize the following queries:
    """

    # Query 1: Find users by status (currently slow)
    # BEFORE (full scan):
    result = await users.find({"status": "premium"})

    # YOUR SOLUTION:
    # Step 1: Create index
    await users.create_index("status", type="hash")
    # Step 2: Query uses index
    result = await users.find({"status": "premium"})

    # Query 2: Find recent orders by user (slow)
    # BEFORE:
    result = await orders.find({"user_id": "u123"}).sort("created_at", -1).limit(10)

    # YOUR SOLUTION:
    # Step 1: Create compound index
    await orders.create_index(["user_id", "created_at"])
    # Step 2: Index covers both filter and sort
    result = await orders.find({"user_id": "u123"}).sort("created_at", -1).limit(10)

    # Query 3: Aggregate orders by category (slow)
    # BEFORE:
    result = await client.sql("""
        SELECT category, SUM(total) FROM orders
        WHERE created_at > '2024-01-01'
        GROUP BY category
    """)

    # YOUR SOLUTION:
    # Use materialized aggregation or pre-computed stats
    # Option 1: Create covering index
    await orders.create_index(["created_at", "category", "total"])

    # Option 2: Use incremental aggregation
    await client.create_materialized_view(
        "category_daily_stats",
        """
        SELECT category, DATE(created_at) as date, SUM(total) as revenue
        FROM orders
        GROUP BY category, DATE(created_at)
        """
    )
```

---

## Module 5: Application Integration

### 5.1 Connection Management

```python
import asyncio
from tdb import LumaDBClient, ConnectionPool

# Production connection setup
async def setup_connection():
    # Create connection pool
    pool = ConnectionPool(
        host="localhost",
        port=8080,
        min_connections=10,
        max_connections=100,
        timeout_seconds=30,
        retry_attempts=3
    )

    # Get connection from pool
    async with pool.connection() as client:
        result = await client.collection("users").find({})
        return result

    # Pool automatically manages connections
```

### 5.2 Error Handling

```python
from tdb.exceptions import (
    LumaDBError,
    ConnectionError,
    QueryError,
    DuplicateKeyError,
    TimeoutError
)

async def robust_operations():
    try:
        result = await collection.insert(document)

    except DuplicateKeyError as e:
        # Handle duplicate key (unique constraint violation)
        print(f"Document already exists: {e.key}")
        # Maybe update instead
        await collection.update({"_id": e.key}, document)

    except TimeoutError:
        # Handle timeout
        print("Query timed out, retrying...")
        result = await collection.insert(document, timeout=60)

    except ConnectionError as e:
        # Handle connection issues
        print(f"Connection failed: {e}")
        # Reconnect logic
        await client.reconnect()

    except QueryError as e:
        # Handle query errors
        print(f"Query error: {e.message}")
        print(f"Query: {e.query}")

    except LumaDBError as e:
        # Generic error handler
        print(f"LumaDB error: {e}")
```

### 5.3 Building a REST API

```python
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from tdb import LumaDBClient

app = FastAPI()
client = LumaDBClient(host="localhost", port=8080)

class User(BaseModel):
    name: str
    email: str
    status: str = "active"

class UserResponse(BaseModel):
    id: str
    name: str
    email: str
    status: str

@app.post("/users", response_model=UserResponse)
async def create_user(user: User):
    try:
        doc_id = await client.collection("users").insert(user.dict())
        return UserResponse(id=doc_id, **user.dict())
    except DuplicateKeyError:
        raise HTTPException(status_code=409, detail="User already exists")

@app.get("/users/{user_id}", response_model=UserResponse)
async def get_user(user_id: str):
    user = await client.collection("users").get(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(id=user["_id"], **user)

@app.get("/users")
async def list_users(status: str = None, limit: int = 100):
    query = {}
    if status:
        query["status"] = status

    users = await client.collection("users") \
        .find(query) \
        .limit(limit) \
        .execute()

    return {"users": users, "count": len(users)}

@app.post("/query")
async def natural_language_query(query: str):
    """Use PromptQL for natural language queries"""
    result = await engine.query(query)
    return result
```

---

## Module 6: Certification Exam

### Sample Questions

**Question 1:** Which index type is most appropriate for email lookups?
- A) B-Tree
- B) Hash
- C) Vector
- D) Full-text

**Answer:** B) Hash - Best for exact match lookups

**Question 2:** What is the primary advantage of PromptQL over SQL?
- A) Faster execution
- B) Multi-step reasoning and context awareness
- C) Lower memory usage
- D) Better compression

**Answer:** B) Multi-step reasoning and context awareness

**Question 3:** How should you model a user's order history?
- A) All orders in a single array
- B) Recent orders embedded, full history in separate collection
- C) All orders in separate collection only
- D) One document per order with user data duplicated

**Answer:** B) Recent orders embedded, full history in separate collection

### Final Project

Build a complete application using LumaDB that includes:

1. **Data model design** for your chosen domain
2. **Indexes** for common query patterns
3. **REST API** with CRUD operations
4. **PromptQL integration** for natural language queries
5. **Performance testing** demonstrating optimization

---

## Resources

- [LumaDB User Manual](./manual.md)
- [API Reference](../api-reference/python-api.md)
- [Performance Benchmarks](../performance/benchmarks.md)
- [Video Tutorials](../videos/README.md)
