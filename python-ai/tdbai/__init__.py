"""
TDB+ AI Service

AI-powered features for the TDB+ database:
- Vector similarity search (semantic search)
- Natural language to query translation
- Embeddings generation
- AI-powered query optimization
"""

from .inference import ModelManager
from .vector import VectorIndex
from .nlp import NLPProcessor

__version__ = "1.0.0"
__all__ = ["ModelManager", "VectorIndex", "NLPProcessor"]
