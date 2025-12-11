"""
PromptQL Engine - The core orchestrator for AI-powered queries.

This engine coordinates:
- Query understanding via LLM
- Multi-step reasoning
- Query planning and optimization
- Execution and result formatting
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union
from enum import Enum

from .planner import QueryPlanner, QueryPlan
from .reasoner import MultiStepReasoner
from .context import ConversationContext
from .optimizer import AIQueryOptimizer
from .schema import SchemaInference
from .executor import QueryExecutor


class QueryMode(Enum):
    """Query execution modes."""
    SIMPLE = "simple"           # Direct query translation
    REASONING = "reasoning"     # Multi-step reasoning
    CONVERSATIONAL = "conversational"  # With context
    EXPLORATORY = "exploratory"  # Schema discovery


@dataclass
class PromptQLConfig:
    """Configuration for PromptQL engine."""
    # LLM settings
    llm_provider: str = "openai"  # openai, anthropic, local
    llm_model: str = "gpt-4"
    llm_temperature: float = 0.1
    llm_max_tokens: int = 4096

    # Query settings
    default_mode: QueryMode = QueryMode.REASONING
    max_reasoning_steps: int = 10
    enable_caching: bool = True
    cache_ttl_seconds: int = 3600

    # Context settings
    max_context_turns: int = 20
    enable_schema_inference: bool = True

    # Optimization settings
    enable_query_optimization: bool = True
    parallel_execution: bool = True

    # Safety settings
    max_rows_return: int = 10000
    timeout_seconds: float = 30.0
    sanitize_queries: bool = True


@dataclass
class QueryResult:
    """Result of a PromptQL query."""
    success: bool
    data: Any
    query_plan: Optional[QueryPlan] = None
    reasoning_chain: Optional[List[str]] = None
    execution_time_ms: float = 0.0
    tokens_used: int = 0
    cached: bool = False
    error: Optional[str] = None
    suggestions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "success": self.success,
            "data": self.data,
            "execution_time_ms": self.execution_time_ms,
            "cached": self.cached,
            "error": self.error,
            "suggestions": self.suggestions,
        }


class PromptQLEngine:
    """
    Main PromptQL engine that processes natural language queries
    using AI-powered understanding and multi-step reasoning.
    """

    def __init__(
        self,
        database_connection: Any,
        config: Optional[PromptQLConfig] = None,
    ):
        self.db = database_connection
        self.config = config or PromptQLConfig()

        # Initialize components
        self.planner = QueryPlanner(self.config)
        self.reasoner = MultiStepReasoner(self.config)
        self.context = ConversationContext(self.config.max_context_turns)
        self.optimizer = AIQueryOptimizer(self.config)
        self.schema_inference = SchemaInference()
        self.executor = QueryExecutor(database_connection)

        # Query cache
        self._cache: Dict[str, QueryResult] = {}
        self._cache_timestamps: Dict[str, float] = {}

        # Statistics
        self._stats = {
            "queries_processed": 0,
            "cache_hits": 0,
            "total_reasoning_steps": 0,
            "avg_execution_time_ms": 0.0,
        }

    async def query(
        self,
        prompt: str,
        mode: Optional[QueryMode] = None,
        context: Optional[Dict] = None,
        stream: bool = False,
    ) -> QueryResult:
        """
        Execute a PromptQL query.

        Args:
            prompt: Natural language query
            mode: Query execution mode
            context: Additional context for the query
            stream: Whether to stream results

        Returns:
            QueryResult with data and metadata
        """
        start_time = time.time()
        mode = mode or self.config.default_mode

        try:
            # Check cache
            cache_key = self._cache_key(prompt, context)
            if self.config.enable_caching:
                cached = self._get_cached(cache_key)
                if cached:
                    self._stats["cache_hits"] += 1
                    cached.cached = True
                    return cached

            # Add to conversation context
            self.context.add_query(prompt, context)

            # Infer schema if enabled
            schema = None
            if self.config.enable_schema_inference:
                schema = await self.schema_inference.infer(self.db)

            # Create query plan
            plan = await self.planner.create_plan(
                prompt=prompt,
                schema=schema,
                context=self.context.get_context(),
                mode=mode,
            )

            # Multi-step reasoning if needed
            reasoning_chain = None
            if mode == QueryMode.REASONING and plan.requires_reasoning:
                reasoning_chain = await self.reasoner.reason(
                    prompt=prompt,
                    plan=plan,
                    schema=schema,
                    max_steps=self.config.max_reasoning_steps,
                )
                self._stats["total_reasoning_steps"] += len(reasoning_chain)

            # Optimize query plan
            if self.config.enable_query_optimization:
                plan = await self.optimizer.optimize(plan, schema)

            # Execute query
            if self.config.parallel_execution and plan.can_parallelize:
                data = await self.executor.execute_parallel(plan)
            else:
                data = await self.executor.execute(plan)

            # Format result
            execution_time = (time.time() - start_time) * 1000
            result = QueryResult(
                success=True,
                data=data,
                query_plan=plan,
                reasoning_chain=reasoning_chain,
                execution_time_ms=execution_time,
            )

            # Generate suggestions
            result.suggestions = await self._generate_suggestions(prompt, data, schema)

            # Update context with result
            self.context.add_result(result)

            # Cache result
            if self.config.enable_caching:
                self._set_cached(cache_key, result)

            # Update stats
            self._update_stats(execution_time)

            return result

        except asyncio.TimeoutError:
            return QueryResult(
                success=False,
                data=None,
                error="Query timed out",
                execution_time_ms=(time.time() - start_time) * 1000,
            )
        except Exception as e:
            return QueryResult(
                success=False,
                data=None,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000,
            )

    async def explain(self, prompt: str) -> Dict:
        """
        Explain how a query would be executed without running it.

        Args:
            prompt: Natural language query

        Returns:
            Explanation of query plan and reasoning
        """
        schema = await self.schema_inference.infer(self.db)
        plan = await self.planner.create_plan(
            prompt=prompt,
            schema=schema,
            context=self.context.get_context(),
            mode=QueryMode.REASONING,
        )

        reasoning = await self.reasoner.reason(
            prompt=prompt,
            plan=plan,
            schema=schema,
            max_steps=self.config.max_reasoning_steps,
        )

        return {
            "query": prompt,
            "understanding": plan.understanding,
            "steps": [step.to_dict() for step in plan.steps],
            "reasoning_chain": reasoning,
            "estimated_cost": plan.estimated_cost,
            "optimizations": plan.optimizations,
        }

    async def suggest(self, partial_prompt: str) -> List[str]:
        """
        Suggest query completions based on partial input.

        Args:
            partial_prompt: Partial query text

        Returns:
            List of suggested completions
        """
        schema = await self.schema_inference.infer(self.db)
        return await self.planner.suggest_completions(
            partial_prompt,
            schema,
            self.context.get_context(),
        )

    def clear_context(self):
        """Clear conversation context."""
        self.context.clear()

    def clear_cache(self):
        """Clear query cache."""
        self._cache.clear()
        self._cache_timestamps.clear()

    def get_stats(self) -> Dict:
        """Get engine statistics."""
        return {
            **self._stats,
            "cache_size": len(self._cache),
            "context_turns": self.context.turn_count,
        }

    # Private methods

    def _cache_key(self, prompt: str, context: Optional[Dict]) -> str:
        """Generate cache key."""
        key_parts = [prompt]
        if context:
            key_parts.append(json.dumps(context, sort_keys=True))
        return hash(tuple(key_parts))

    def _get_cached(self, key: str) -> Optional[QueryResult]:
        """Get cached result if valid."""
        if key not in self._cache:
            return None

        timestamp = self._cache_timestamps.get(key, 0)
        if time.time() - timestamp > self.config.cache_ttl_seconds:
            del self._cache[key]
            del self._cache_timestamps[key]
            return None

        return self._cache[key]

    def _set_cached(self, key: str, result: QueryResult):
        """Cache a result."""
        self._cache[key] = result
        self._cache_timestamps[key] = time.time()

    def _update_stats(self, execution_time: float):
        """Update statistics."""
        n = self._stats["queries_processed"]
        avg = self._stats["avg_execution_time_ms"]
        self._stats["queries_processed"] = n + 1
        self._stats["avg_execution_time_ms"] = (avg * n + execution_time) / (n + 1)

    async def _generate_suggestions(
        self,
        prompt: str,
        data: Any,
        schema: Optional[Dict],
    ) -> List[str]:
        """Generate follow-up query suggestions."""
        suggestions = []

        # Suggest drill-down queries
        if isinstance(data, list) and len(data) > 0:
            suggestions.append(f"Show me more details about the first result")
            suggestions.append(f"Group these results by category")
            suggestions.append(f"What's the trend over time?")

        # Suggest related queries based on schema
        if schema:
            related_tables = schema.get("related_tables", [])
            for table in related_tables[:2]:
                suggestions.append(f"Show me related {table} data")

        return suggestions[:5]  # Limit suggestions
