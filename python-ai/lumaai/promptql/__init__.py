"""
PromptQL - AI-Powered Query Language for TDB+

PromptQL goes beyond traditional NQL by using LLM reasoning to:
- Understand complex, ambiguous queries
- Perform multi-step reasoning
- Maintain conversation context
- Infer schema relationships
- Optimize query execution
- Handle aggregations described in natural language

Example:
    promptql = PromptQL(db)

    # Simple query
    result = await promptql.query("Show me all users who signed up last month")

    # Complex multi-step query
    result = await promptql.query(
        "Find customers who bought more than the average and compare their
        spending patterns with those who bought less"
    )

    # Conversational query
    result = await promptql.query("Now filter those to only premium members")
"""

from .engine import PromptQLEngine
from .planner import QueryPlanner, QueryPlan, QueryStep
from .reasoner import MultiStepReasoner, ReasoningChain
from .context import ConversationContext, QueryMemory, ContextManager
from .optimizer import AIQueryOptimizer, IndexRecommendation, OptimizationResult
from .schema import SchemaInference, RelationshipDetector, InferredSchema, Relationship
from .executor import QueryExecutor, ExecutionResult, ResultFormatter
from .llm import (
    LLMConfig,
    LLMProvider,
    QueryLLMIntegration,
    EmbeddingClient,
    QueryParseResult,
)
from .semantic import (
    SemanticParser,
    SemanticParseResult,
    SemanticEntity,
    QueryIntent,
    TypoCorrector,
    SynonymExpander,
    SemanticSimilarity,
)

__all__ = [
    # Engine
    "PromptQLEngine",
    # Planner
    "QueryPlanner",
    "QueryPlan",
    "QueryStep",
    # Reasoner
    "MultiStepReasoner",
    "ReasoningChain",
    # Context
    "ConversationContext",
    "QueryMemory",
    "ContextManager",
    # Optimizer
    "AIQueryOptimizer",
    "IndexRecommendation",
    "OptimizationResult",
    # Schema
    "SchemaInference",
    "RelationshipDetector",
    "InferredSchema",
    "Relationship",
    # Executor
    "QueryExecutor",
    "ExecutionResult",
    "ResultFormatter",
    # LLM
    "LLMConfig",
    "LLMProvider",
    "QueryLLMIntegration",
    "EmbeddingClient",
    "QueryParseResult",
    # Semantic
    "SemanticParser",
    "SemanticParseResult",
    "SemanticEntity",
    "QueryIntent",
    "TypoCorrector",
    "SynonymExpander",
    "SemanticSimilarity",
]
