"""
LumaDB Python Client

Pure Python implementation using httpx for HTTP requests.
For maximum performance, consider the Rust-based PyO3 bindings (coming soon).
"""

from typing import Any, Dict, Iterator, List, Optional, Tuple
import httpx


class LumaDB:
    """Main LumaDB client."""

    def __init__(self, address: str, **kwargs):
        """
        Create a new LumaDB connection.

        Args:
            address: Server address (e.g., "localhost:8080")

        Example:
            >>> db = LumaDB("localhost:8080")
        """
        if not address.startswith("http"):
            address = f"http://{address}"
        self.base_url = address
        self._client = httpx.Client(timeout=30.0)

    def query(self, query: str, params: Optional[List[Any]] = None) -> List[Dict]:
        """
        Execute a SQL or LQL query.

        Args:
            query: Query string
            params: Optional query parameters

        Returns:
            Query results as list of dicts

        Example:
            >>> results = db.query("SELECT * FROM users WHERE age > 21")
            >>> for row in results:
            ...     print(row["name"])
        """
        response = self._client.post(
            f"{self.base_url}/api/v1/query",
            json={"query": query, "params": params or []},
        )
        response.raise_for_status()
        return response.json()

    def sql(self, query: str, params: Optional[List[Any]] = None) -> List[Dict]:
        """Execute a SQL query (alias for query)."""
        return self.query(query, params)

    def to_pandas(self, query: str, params: Optional[List[Any]] = None):
        """
        Execute and return results as pandas DataFrame.

        Example:
            >>> df = db.to_pandas("SELECT * FROM sales")
            >>> df.groupby("category").sum()
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required. Install with: pip install lumadb[pandas]")

        result = self.query(query, params)
        return pd.DataFrame(result)

    def producer(self, topic: str) -> "LumaProducer":
        """
        Create a Kafka-compatible producer.

        Args:
            topic: Topic name

        Returns:
            LumaProducer instance

        Example:
            >>> producer = db.producer("events")
            >>> producer.send({"user_id": 123, "action": "click"})
        """
        return LumaProducer(self, topic)

    def consumer(self, topic: str, group_id: Optional[str] = None) -> "LumaConsumer":
        """
        Create a Kafka-compatible consumer.

        Args:
            topic: Topic name
            group_id: Consumer group ID

        Returns:
            LumaConsumer instance

        Example:
            >>> consumer = db.consumer("events", "my-group")
            >>> for message in consumer:
            ...     print(message)
        """
        return LumaConsumer(self, topic, group_id)

    def collection(self, name: str) -> "LumaCollection":
        """
        Get a collection for document operations.

        Args:
            name: Collection name

        Returns:
            LumaCollection instance
        """
        return LumaCollection(self, name)

    def vector_search(
        self,
        collection: str,
        vector: List[float],
        k: int = 10,
        filter: Optional[str] = None,
    ) -> List[Tuple[Dict, float]]:
        """
        Perform vector similarity search.

        Args:
            collection: Collection name
            vector: Query vector
            k: Number of results
            filter: Optional filter expression

        Returns:
            List of (document, score) tuples

        Example:
            >>> results = db.vector_search("embeddings", [0.1, 0.2, ...], k=10)
            >>> for doc, score in results:
            ...     print(f"{doc['title']}: {score}")
        """
        request = {
            "collection": collection,
            "vector": vector,
            "k": k,
        }
        if filter:
            request["filter"] = filter

        response = self._client.post(
            f"{self.base_url}/api/v1/vectors/search",
            json=request,
        )
        response.raise_for_status()

        results = response.json()
        return [(r.get("document", {}), r["score"]) for r in results]

    def close(self):
        """Close the connection."""
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class LumaProducer:
    """Kafka-compatible producer."""

    def __init__(self, client: LumaDB, topic: str):
        self._client = client
        self._topic = topic

    def send(
        self,
        value: Any,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict:
        """
        Send a message to the topic.

        Args:
            value: Message value (dict or string)
            key: Optional message key
            partition: Optional partition number
            headers: Optional headers dict

        Returns:
            Metadata about the sent message

        Example:
            >>> producer.send({"event": "click"}, key="user-123")
        """
        record = {
            "key": key,
            "value": value,
            "headers": headers,
            "partition": partition,
        }

        response = self._client._client.post(
            f"{self._client.base_url}/api/v1/topics/{self._topic}/produce",
            json={"records": [record], "acks": 1},
        )
        response.raise_for_status()

        results = response.json()
        return results[0] if results else {}

    def send_batch(self, messages: List[Dict]) -> List[Dict]:
        """
        Send multiple messages in a batch.

        Args:
            messages: List of message dicts with 'value' and optional 'key'

        Returns:
            List of metadata for each message
        """
        records = [
            {
                "key": m.get("key"),
                "value": m["value"],
                "headers": m.get("headers"),
                "partition": m.get("partition"),
            }
            for m in messages
        ]

        response = self._client._client.post(
            f"{self._client.base_url}/api/v1/topics/{self._topic}/produce",
            json={"records": records, "acks": 1},
        )
        response.raise_for_status()
        return response.json()

    def flush(self):
        """Flush pending messages."""
        pass


class LumaConsumer:
    """Kafka-compatible consumer with Python iterator protocol."""

    def __init__(self, client: LumaDB, topic: str, group_id: Optional[str] = None):
        self._client = client
        self._topic = topic
        self._group_id = group_id
        self._offset = "latest"

    def poll(
        self, timeout_ms: int = 1000, max_records: int = 100
    ) -> List[Dict]:
        """
        Poll for messages.

        Args:
            timeout_ms: Timeout in milliseconds
            max_records: Maximum records to return

        Returns:
            List of messages
        """
        params = {
            "max_records": max_records,
            "offset": self._offset,
        }
        if self._group_id:
            params["group_id"] = self._group_id

        response = self._client._client.get(
            f"{self._client.base_url}/api/v1/topics/{self._topic}/consume",
            params=params,
        )
        response.raise_for_status()

        records = response.json()
        if records:
            self._offset = str(records[-1]["offset"] + 1)

        return records

    def commit(self):
        """Commit offsets."""
        pass

    def __iter__(self) -> Iterator[Dict]:
        return self

    def __next__(self) -> Dict:
        records = self.poll(timeout_ms=1000, max_records=1)
        if not records:
            raise StopIteration
        return records[0]


class LumaCollection:
    """Collection for document operations."""

    def __init__(self, client: LumaDB, name: str):
        self._client = client
        self._name = name

    def insert(self, documents) -> Dict:
        """
        Insert documents.

        Args:
            documents: Document or list of documents

        Returns:
            Insert result with IDs
        """
        if not isinstance(documents, list):
            documents = [documents]

        response = self._client._client.post(
            f"{self._client.base_url}/api/v1/collections/{self._name}/documents",
            json={"documents": documents},
        )
        response.raise_for_status()
        return response.json()

    def find(
        self, filter: Optional[Dict] = None, limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Find documents.

        Args:
            filter: Filter expression or dict
            limit: Maximum documents to return

        Returns:
            List of matching documents
        """
        params = {}
        if limit:
            params["limit"] = limit

        response = self._client._client.get(
            f"{self._client.base_url}/api/v1/collections/{self._name}/documents",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    def find_one(self, filter: Optional[Dict] = None) -> Optional[Dict]:
        """Find one document."""
        results = self.find(filter=filter, limit=1)
        return results[0] if results else None

    def update(self, filter: Dict, update: Dict) -> Dict:
        """Update documents."""
        response = self._client._client.post(
            f"{self._client.base_url}/api/v1/collections/{self._name}/update",
            json={"filter": filter, "update": update},
        )
        response.raise_for_status()
        return response.json()

    def delete(self, filter: Dict) -> Dict:
        """Delete documents."""
        response = self._client._client.post(
            f"{self._client.base_url}/api/v1/collections/{self._name}/delete",
            json={"filter": filter},
        )
        response.raise_for_status()
        return response.json()
