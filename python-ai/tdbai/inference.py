"""
Model Manager for TDB+ AI Service

Handles loading and managing ML models for:
- Text embeddings (sentence-transformers)
- NLP tasks (spaCy, transformers)
- Custom inference
"""

import asyncio
from typing import Optional
from dataclasses import dataclass, field

import numpy as np


@dataclass
class ModelInfo:
    """Information about a loaded model."""
    name: str
    type: str
    dimensions: int
    loaded: bool = False
    device: str = "cpu"


class ModelManager:
    """
    Manages AI/ML models for TDB+ database.

    Provides efficient model loading, caching, and inference
    with support for multiple embedding models.
    """

    # Available embedding models
    EMBEDDING_MODELS = {
        "all-MiniLM-L6-v2": {"dimensions": 384, "type": "sentence-transformer"},
        "all-mpnet-base-v2": {"dimensions": 768, "type": "sentence-transformer"},
        "paraphrase-multilingual-MiniLM-L12-v2": {"dimensions": 384, "type": "sentence-transformer"},
    }

    def __init__(self):
        self._models: dict[str, any] = {}
        self._model_info: dict[str, ModelInfo] = {}
        self._ready = False
        self._lock = asyncio.Lock()
        self._embedding_cache: dict[str, list[float]] = {}
        self._cache_max_size = 10000

    async def load_models(self) -> None:
        """Load default models for the service."""
        async with self._lock:
            # Load the default embedding model
            await self._load_embedding_model("all-MiniLM-L6-v2")
            self._ready = True
            print("Models loaded successfully")

    async def _load_embedding_model(self, model_name: str) -> None:
        """Load a sentence-transformer embedding model."""
        if model_name in self._models:
            return

        model_config = self.EMBEDDING_MODELS.get(model_name)
        if not model_config:
            raise ValueError(f"Unknown model: {model_name}")

        try:
            # Try to load sentence-transformers
            from sentence_transformers import SentenceTransformer

            print(f"Loading embedding model: {model_name}")
            model = SentenceTransformer(model_name)

            self._models[model_name] = model
            self._model_info[model_name] = ModelInfo(
                name=model_name,
                type=model_config["type"],
                dimensions=model_config["dimensions"],
                loaded=True,
                device=str(model.device),
            )
            print(f"Model {model_name} loaded on {model.device}")

        except ImportError:
            # Fallback to mock embeddings if sentence-transformers not available
            print(f"Warning: sentence-transformers not available, using mock embeddings")
            self._models[model_name] = MockEmbeddingModel(model_config["dimensions"])
            self._model_info[model_name] = ModelInfo(
                name=model_name,
                type="mock",
                dimensions=model_config["dimensions"],
                loaded=True,
                device="cpu",
            )

    def is_ready(self) -> bool:
        """Check if models are ready for inference."""
        return self._ready

    async def get_embeddings(
        self,
        texts: list[str],
        model_name: str = "all-MiniLM-L6-v2"
    ) -> list[list[float]]:
        """
        Generate embeddings for a list of texts.

        Args:
            texts: List of text strings to embed
            model_name: Name of the embedding model to use

        Returns:
            List of embedding vectors
        """
        # Ensure model is loaded
        if model_name not in self._models:
            await self._load_embedding_model(model_name)

        model = self._models[model_name]

        # Check cache for previously computed embeddings
        results = []
        texts_to_compute = []
        text_indices = []

        for i, text in enumerate(texts):
            cache_key = f"{model_name}:{hash(text)}"
            if cache_key in self._embedding_cache:
                results.append((i, self._embedding_cache[cache_key]))
            else:
                texts_to_compute.append(text)
                text_indices.append(i)

        # Compute embeddings for uncached texts
        if texts_to_compute:
            # Run embedding generation in thread pool to not block
            loop = asyncio.get_event_loop()
            new_embeddings = await loop.run_in_executor(
                None,
                lambda: model.encode(texts_to_compute, convert_to_numpy=True)
            )

            # Convert to list and cache
            for idx, (text, embedding) in enumerate(zip(texts_to_compute, new_embeddings)):
                embedding_list = embedding.tolist() if hasattr(embedding, 'tolist') else list(embedding)
                cache_key = f"{model_name}:{hash(text)}"

                # Manage cache size
                if len(self._embedding_cache) >= self._cache_max_size:
                    # Remove oldest entries (simple FIFO)
                    keys_to_remove = list(self._embedding_cache.keys())[:1000]
                    for key in keys_to_remove:
                        del self._embedding_cache[key]

                self._embedding_cache[cache_key] = embedding_list
                results.append((text_indices[idx], embedding_list))

        # Sort by original index and return just embeddings
        results.sort(key=lambda x: x[0])
        return [emb for _, emb in results]

    async def get_embedding(
        self,
        text: str,
        model_name: str = "all-MiniLM-L6-v2"
    ) -> list[float]:
        """Get embedding for a single text."""
        embeddings = await self.get_embeddings([text], model_name)
        return embeddings[0]

    def list_models(self) -> list[dict]:
        """List all available and loaded models."""
        models = []
        for name, config in self.EMBEDDING_MODELS.items():
            info = self._model_info.get(name)
            models.append({
                "name": name,
                "type": config["type"],
                "dimensions": config["dimensions"],
                "loaded": info.loaded if info else False,
                "device": info.device if info else None,
            })
        return models

    def get_model_dimensions(self, model_name: str) -> int:
        """Get the embedding dimensions for a model."""
        if model_name in self._model_info:
            return self._model_info[model_name].dimensions
        if model_name in self.EMBEDDING_MODELS:
            return self.EMBEDDING_MODELS[model_name]["dimensions"]
        raise ValueError(f"Unknown model: {model_name}")


class MockEmbeddingModel:
    """
    Mock embedding model for testing when sentence-transformers is not available.
    Generates deterministic pseudo-random embeddings based on text hash.
    """

    def __init__(self, dimensions: int):
        self.dimensions = dimensions
        self.device = "cpu"

    def encode(
        self,
        texts: list[str],
        convert_to_numpy: bool = True
    ) -> np.ndarray:
        """Generate mock embeddings."""
        embeddings = []
        for text in texts:
            # Use text hash as seed for reproducibility
            np.random.seed(hash(text) % (2**32))
            embedding = np.random.randn(self.dimensions).astype(np.float32)
            # Normalize to unit vector
            embedding = embedding / np.linalg.norm(embedding)
            embeddings.append(embedding)
        return np.array(embeddings)
