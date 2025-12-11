"""
TDB+ AI Service

AI-powered features for the TDB+ database:
- Vector similarity search (semantic search)
- Natural language to query translation (NLQ)
- PromptQL - Advanced AI query language with multi-step reasoning
- Embeddings generation
- AI-powered query optimization
"""

from .inference import ModelManager
from .vector import VectorIndex
from .nlp import NLPProcessor
from . import promptql

# PromptQL exports
from .promptql import (
    PromptQLEngine,
    QueryPlanner,
    MultiStepReasoner,
    ConversationContext,
    AIQueryOptimizer,
    SchemaInference,
    QueryExecutor,
    LLMConfig,
    LLMProvider,
    SemanticParser,
)

__version__ = "2.0.0"
__all__ = [
    # Core AI components
    "ModelManager",
    "VectorIndex",
    "NLPProcessor",
    # PromptQL module
    "promptql",
    # PromptQL main exports
    "PromptQLEngine",
    "QueryPlanner",
    "MultiStepReasoner",
    "ConversationContext",
    "AIQueryOptimizer",
    "SchemaInference",
    "QueryExecutor",
    "LLMConfig",
    "LLMProvider",
    "SemanticParser",
]
