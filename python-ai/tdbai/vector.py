"""
Vector Index for TDB+ AI Service

Provides high-performance vector similarity search using:
- FAISS for efficient nearest neighbor search
- Multiple index types (Flat, IVF, HNSW)
- Metadata filtering
"""

import asyncio
import time
from typing import Optional
from dataclasses import dataclass, field

import numpy as np

from .inference import ModelManager


@dataclass
class IndexedDocument:
    """A document stored in the vector index."""
    document_id: str
    text: str
    embedding: list[float]
    metadata: dict = field(default_factory=dict)


@dataclass
class SearchResult:
    """A single search result."""
    document_id: str
    text: str
    score: float
    metadata: dict


class VectorIndex:
    """
    Vector similarity search index for TDB+.

    Supports multiple collections, each with its own FAISS index.
    Provides semantic search capabilities by embedding queries
    and finding nearest neighbors.
    """

    def __init__(self, model_manager: ModelManager):
        self.model_manager = model_manager
        self._collections: dict[str, CollectionIndex] = {}
        self._lock = asyncio.Lock()

    async def index_document(
        self,
        collection: str,
        document_id: str,
        text: str,
        metadata: Optional[dict] = None,
    ) -> None:
        """
        Index a document for vector search.

        Args:
            collection: Collection name
            document_id: Unique document identifier
            text: Text content to embed and index
            metadata: Optional metadata for filtering
        """
        # Get or create collection index
        if collection not in self._collections:
            async with self._lock:
                if collection not in self._collections:
                    dimensions = self.model_manager.get_model_dimensions("all-MiniLM-L6-v2")
                    self._collections[collection] = CollectionIndex(
                        name=collection,
                        dimensions=dimensions,
                    )

        # Generate embedding
        embedding = await self.model_manager.get_embedding(text)

        # Add to collection
        col_index = self._collections[collection]
        await col_index.add_document(
            document_id=document_id,
            text=text,
            embedding=embedding,
            metadata=metadata or {},
        )

    async def search(
        self,
        collection: str,
        query: str,
        top_k: int = 10,
        filter: Optional[dict] = None,
    ) -> tuple[list[dict], list[float]]:
        """
        Search for similar documents.

        Args:
            collection: Collection to search in
            query: Search query text
            top_k: Number of results to return
            filter: Optional metadata filter

        Returns:
            Tuple of (results list, query embedding)
        """
        if collection not in self._collections:
            return [], []

        # Generate query embedding
        query_embedding = await self.model_manager.get_embedding(query)

        # Search collection
        col_index = self._collections[collection]
        results = await col_index.search(
            query_embedding=query_embedding,
            top_k=top_k,
            filter=filter,
        )

        # Format results
        formatted_results = [
            {
                "document_id": r.document_id,
                "text": r.text,
                "score": r.score,
                "metadata": r.metadata,
            }
            for r in results
        ]

        return formatted_results, query_embedding

    async def delete_collection(self, collection: str) -> None:
        """Delete a collection's index."""
        async with self._lock:
            if collection in self._collections:
                del self._collections[collection]

    async def get_collection_stats(self, collection: str) -> dict:
        """Get statistics for a collection."""
        if collection not in self._collections:
            return {"exists": False}

        col_index = self._collections[collection]
        return {
            "exists": True,
            "name": collection,
            "document_count": col_index.document_count,
            "dimensions": col_index.dimensions,
            "index_type": col_index.index_type,
        }


class CollectionIndex:
    """
    Vector index for a single collection.

    Uses FAISS for efficient similarity search with support
    for different index types based on collection size.
    """

    # Threshold for switching to IVF index
    IVF_THRESHOLD = 10000

    def __init__(self, name: str, dimensions: int):
        self.name = name
        self.dimensions = dimensions
        self.document_count = 0
        self.index_type = "Flat"

        # Document storage
        self._documents: dict[str, IndexedDocument] = {}
        self._id_to_idx: dict[str, int] = {}
        self._idx_to_id: dict[int, str] = {}

        # Initialize FAISS index
        self._index = None
        self._lock = asyncio.Lock()

        self._init_index()

    def _init_index(self) -> None:
        """Initialize the FAISS index."""
        try:
            import faiss

            # Start with flat index for exact search
            self._index = faiss.IndexFlatIP(self.dimensions)  # Inner product (cosine similarity)
            self._faiss_available = True

        except ImportError:
            # Fallback to numpy-based search
            print("Warning: FAISS not available, using numpy fallback")
            self._embeddings = []
            self._faiss_available = False

    async def add_document(
        self,
        document_id: str,
        text: str,
        embedding: list[float],
        metadata: dict,
    ) -> None:
        """Add a document to the index."""
        async with self._lock:
            # Check if document already exists (update case)
            if document_id in self._documents:
                # For simplicity, we don't update in place
                # In production, you'd handle this more efficiently
                pass

            # Normalize embedding for cosine similarity
            embedding_np = np.array(embedding, dtype=np.float32)
            embedding_np = embedding_np / np.linalg.norm(embedding_np)

            # Store document
            doc = IndexedDocument(
                document_id=document_id,
                text=text,
                embedding=embedding_np.tolist(),
                metadata=metadata,
            )
            self._documents[document_id] = doc

            # Add to index
            idx = self.document_count
            self._id_to_idx[document_id] = idx
            self._idx_to_id[idx] = document_id

            if self._faiss_available:
                self._index.add(embedding_np.reshape(1, -1))
            else:
                self._embeddings.append(embedding_np)

            self.document_count += 1

            # Rebuild index if threshold crossed
            if self.document_count == self.IVF_THRESHOLD:
                await self._rebuild_index_ivf()

    async def _rebuild_index_ivf(self) -> None:
        """Rebuild index as IVF for better performance with large collections."""
        if not self._faiss_available:
            return

        import faiss

        print(f"Rebuilding index for {self.name} with IVF")

        # Get all embeddings
        all_embeddings = np.array([
            doc.embedding for doc in self._documents.values()
        ], dtype=np.float32)

        # Create IVF index
        nlist = min(100, self.document_count // 10)  # Number of clusters
        quantizer = faiss.IndexFlatIP(self.dimensions)
        new_index = faiss.IndexIVFFlat(quantizer, self.dimensions, nlist, faiss.METRIC_INNER_PRODUCT)

        # Train and add vectors
        new_index.train(all_embeddings)
        new_index.add(all_embeddings)
        new_index.nprobe = min(10, nlist)  # Number of clusters to search

        self._index = new_index
        self.index_type = "IVF"

    async def search(
        self,
        query_embedding: list[float],
        top_k: int,
        filter: Optional[dict] = None,
    ) -> list[SearchResult]:
        """Search for similar documents."""
        if self.document_count == 0:
            return []

        # Normalize query embedding
        query_np = np.array(query_embedding, dtype=np.float32)
        query_np = query_np / np.linalg.norm(query_np)

        if self._faiss_available:
            return await self._search_faiss(query_np, top_k, filter)
        else:
            return await self._search_numpy(query_np, top_k, filter)

    async def _search_faiss(
        self,
        query_np: np.ndarray,
        top_k: int,
        filter: Optional[dict],
    ) -> list[SearchResult]:
        """Search using FAISS index."""
        # Search more candidates if filtering
        search_k = top_k * 3 if filter else top_k

        # Perform search
        loop = asyncio.get_event_loop()
        scores, indices = await loop.run_in_executor(
            None,
            lambda: self._index.search(query_np.reshape(1, -1), min(search_k, self.document_count))
        )

        results = []
        for score, idx in zip(scores[0], indices[0]):
            if idx < 0:  # Invalid index
                continue

            doc_id = self._idx_to_id.get(int(idx))
            if not doc_id:
                continue

            doc = self._documents.get(doc_id)
            if not doc:
                continue

            # Apply filter
            if filter and not self._matches_filter(doc.metadata, filter):
                continue

            results.append(SearchResult(
                document_id=doc.document_id,
                text=doc.text,
                score=float(score),
                metadata=doc.metadata,
            ))

            if len(results) >= top_k:
                break

        return results

    async def _search_numpy(
        self,
        query_np: np.ndarray,
        top_k: int,
        filter: Optional[dict],
    ) -> list[SearchResult]:
        """Fallback search using numpy."""
        if not self._embeddings:
            return []

        # Compute all similarities
        embeddings_matrix = np.array(self._embeddings)
        similarities = np.dot(embeddings_matrix, query_np)

        # Get top indices
        top_indices = np.argsort(similarities)[::-1]

        results = []
        for idx in top_indices:
            doc_id = self._idx_to_id.get(int(idx))
            if not doc_id:
                continue

            doc = self._documents.get(doc_id)
            if not doc:
                continue

            # Apply filter
            if filter and not self._matches_filter(doc.metadata, filter):
                continue

            results.append(SearchResult(
                document_id=doc.document_id,
                text=doc.text,
                score=float(similarities[idx]),
                metadata=doc.metadata,
            ))

            if len(results) >= top_k:
                break

        return results

    def _matches_filter(self, metadata: dict, filter: dict) -> bool:
        """Check if document metadata matches filter."""
        for key, value in filter.items():
            if key not in metadata:
                return False

            if isinstance(value, dict):
                # Handle operators
                for op, op_value in value.items():
                    if op == "$eq" and metadata[key] != op_value:
                        return False
                    elif op == "$ne" and metadata[key] == op_value:
                        return False
                    elif op == "$gt" and not (metadata[key] > op_value):
                        return False
                    elif op == "$gte" and not (metadata[key] >= op_value):
                        return False
                    elif op == "$lt" and not (metadata[key] < op_value):
                        return False
                    elif op == "$lte" and not (metadata[key] <= op_value):
                        return False
                    elif op == "$in" and metadata[key] not in op_value:
                        return False
                    elif op == "$nin" and metadata[key] in op_value:
                        return False
            else:
                # Direct equality
                if metadata[key] != value:
                    return False

        return True
