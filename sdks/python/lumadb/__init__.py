"""
LumaDB Python SDK

Ultra-fast unified database client for:
- Kafka-compatible streaming (100x faster)
- SQL and LQL queries
- Vector similarity search
- Document operations
- Time-series analytics

Example:
    >>> from lumadb import LumaDB
    >>> db = LumaDB("localhost:8080")
    >>>
    >>> # Kafka-style streaming
    >>> producer = db.producer("events")
    >>> producer.send({"user_id": 123, "action": "click"})
    >>>
    >>> # SQL queries
    >>> results = db.sql("SELECT * FROM users WHERE age > 21")
    >>>
    >>> # Vector search
    >>> similar = db.vector_search("embeddings", [0.1, 0.2, ...], k=10)
"""

from .client import LumaDB, LumaProducer, LumaConsumer

__version__ = "0.1.0"
__all__ = ["LumaDB", "LumaProducer", "LumaConsumer"]
