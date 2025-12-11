"""
LLM Integration - Connects PromptQL to Large Language Models.

This module provides:
- Multi-provider LLM support (OpenAI, Anthropic, local models)
- Prompt engineering for database queries
- Structured output parsing
- Token optimization
- Caching and rate limiting
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, TypeVar, Generic
from enum import Enum
import asyncio
import hashlib
import json
import time
import re


class LLMProvider(Enum):
    """Supported LLM providers."""
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    COHERE = "cohere"
    LOCAL = "local"  # Ollama, llama.cpp, etc.
    CUSTOM = "custom"


@dataclass
class LLMConfig:
    """Configuration for LLM integration."""
    provider: LLMProvider = LLMProvider.OPENAI
    model: str = "gpt-4"
    api_key: Optional[str] = None
    api_base: Optional[str] = None
    temperature: float = 0.1  # Low for deterministic query generation
    max_tokens: int = 2000
    timeout_seconds: int = 30
    cache_enabled: bool = True
    cache_ttl_seconds: int = 3600
    rate_limit_rpm: int = 60  # Requests per minute
    retry_attempts: int = 3


@dataclass
class LLMResponse:
    """Response from LLM."""
    content: str
    model: str
    tokens_used: int
    latency_ms: float
    cached: bool = False
    raw_response: Optional[Dict] = None


@dataclass
class QueryParseResult:
    """Parsed query structure from LLM."""
    intent: str
    entities: Dict[str, Any]
    filters: List[Dict]
    aggregations: List[Dict]
    sort: Optional[Dict]
    limit: Optional[int]
    confidence: float
    explanation: str


class LLMClient(ABC):
    """Abstract base class for LLM clients."""

    @abstractmethod
    async def complete(self, prompt: str, **kwargs) -> LLMResponse:
        """Generate completion from prompt."""
        pass

    @abstractmethod
    async def chat(self, messages: List[Dict], **kwargs) -> LLMResponse:
        """Generate chat completion."""
        pass


class OpenAIClient(LLMClient):
    """OpenAI API client."""

    def __init__(self, config: LLMConfig):
        self.config = config
        self._client = None

    async def _get_client(self):
        if self._client is None:
            try:
                import openai
                self._client = openai.AsyncOpenAI(
                    api_key=self.config.api_key,
                    base_url=self.config.api_base,
                )
            except ImportError:
                raise RuntimeError("openai package not installed. Run: pip install openai")
        return self._client

    async def complete(self, prompt: str, **kwargs) -> LLMResponse:
        messages = [{"role": "user", "content": prompt}]
        return await self.chat(messages, **kwargs)

    async def chat(self, messages: List[Dict], **kwargs) -> LLMResponse:
        client = await self._get_client()
        start = time.time()

        response = await client.chat.completions.create(
            model=kwargs.get("model", self.config.model),
            messages=messages,
            temperature=kwargs.get("temperature", self.config.temperature),
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
        )

        latency_ms = (time.time() - start) * 1000

        return LLMResponse(
            content=response.choices[0].message.content,
            model=response.model,
            tokens_used=response.usage.total_tokens,
            latency_ms=latency_ms,
            raw_response=response.model_dump(),
        )


class AnthropicClient(LLMClient):
    """Anthropic API client."""

    def __init__(self, config: LLMConfig):
        self.config = config
        self._client = None

    async def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.AsyncAnthropic(
                    api_key=self.config.api_key,
                )
            except ImportError:
                raise RuntimeError("anthropic package not installed. Run: pip install anthropic")
        return self._client

    async def complete(self, prompt: str, **kwargs) -> LLMResponse:
        messages = [{"role": "user", "content": prompt}]
        return await self.chat(messages, **kwargs)

    async def chat(self, messages: List[Dict], **kwargs) -> LLMResponse:
        client = await self._get_client()
        start = time.time()

        response = await client.messages.create(
            model=kwargs.get("model", self.config.model),
            messages=messages,
            temperature=kwargs.get("temperature", self.config.temperature),
            max_tokens=kwargs.get("max_tokens", self.config.max_tokens),
        )

        latency_ms = (time.time() - start) * 1000

        return LLMResponse(
            content=response.content[0].text,
            model=response.model,
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            latency_ms=latency_ms,
            raw_response={"id": response.id},
        )


class LocalLLMClient(LLMClient):
    """Client for local LLM servers (Ollama, llama.cpp, etc.)."""

    def __init__(self, config: LLMConfig):
        self.config = config
        self.base_url = config.api_base or "http://localhost:11434"

    async def complete(self, prompt: str, **kwargs) -> LLMResponse:
        import aiohttp

        start = time.time()

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": kwargs.get("model", self.config.model),
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": kwargs.get("temperature", self.config.temperature),
                    },
                },
                timeout=aiohttp.ClientTimeout(total=self.config.timeout_seconds),
            ) as response:
                result = await response.json()

        latency_ms = (time.time() - start) * 1000

        return LLMResponse(
            content=result.get("response", ""),
            model=result.get("model", self.config.model),
            tokens_used=result.get("eval_count", 0),
            latency_ms=latency_ms,
            raw_response=result,
        )

    async def chat(self, messages: List[Dict], **kwargs) -> LLMResponse:
        import aiohttp

        start = time.time()

        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{self.base_url}/api/chat",
                json={
                    "model": kwargs.get("model", self.config.model),
                    "messages": messages,
                    "stream": False,
                    "options": {
                        "temperature": kwargs.get("temperature", self.config.temperature),
                    },
                },
                timeout=aiohttp.ClientTimeout(total=self.config.timeout_seconds),
            ) as response:
                result = await response.json()

        latency_ms = (time.time() - start) * 1000

        return LLMResponse(
            content=result.get("message", {}).get("content", ""),
            model=result.get("model", self.config.model),
            tokens_used=result.get("eval_count", 0),
            latency_ms=latency_ms,
            raw_response=result,
        )


class LLMCache:
    """LRU cache for LLM responses."""

    def __init__(self, max_size: int = 1000, ttl_seconds: int = 3600):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache: Dict[str, tuple] = {}  # key -> (response, timestamp)

    def _make_key(self, prompt: str, model: str, temperature: float) -> str:
        combined = f"{prompt}:{model}:{temperature}"
        return hashlib.sha256(combined.encode()).hexdigest()

    def get(self, prompt: str, model: str, temperature: float) -> Optional[LLMResponse]:
        key = self._make_key(prompt, model, temperature)

        if key not in self.cache:
            return None

        response, timestamp = self.cache[key]
        age = time.time() - timestamp

        if age > self.ttl_seconds:
            del self.cache[key]
            return None

        # Mark as cached
        response.cached = True
        return response

    def put(self, prompt: str, model: str, temperature: float, response: LLMResponse):
        key = self._make_key(prompt, model, temperature)
        self.cache[key] = (response, time.time())

        # Evict if over capacity
        if len(self.cache) > self.max_size:
            # Remove oldest entries
            sorted_entries = sorted(self.cache.items(), key=lambda x: x[1][1])
            for k, _ in sorted_entries[: len(self.cache) - self.max_size]:
                del self.cache[k]

    def clear(self):
        self.cache.clear()


class RateLimiter:
    """Token bucket rate limiter."""

    def __init__(self, requests_per_minute: int):
        self.rpm = requests_per_minute
        self.tokens = requests_per_minute
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill

            # Refill tokens
            self.tokens = min(self.rpm, self.tokens + elapsed * (self.rpm / 60))
            self.last_refill = now

            if self.tokens < 1:
                # Wait for token
                wait_time = (1 - self.tokens) * (60 / self.rpm)
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class QueryLLMIntegration:
    """
    Main LLM integration for PromptQL.

    Handles:
    - Query understanding
    - SQL/NoSQL generation
    - Result explanation
    - Error recovery
    """

    # Prompt templates
    QUERY_PARSE_PROMPT = """You are a database query parser. Parse the following natural language query into a structured format.

Database Schema:
{schema}

User Query: "{query}"

Previous Context:
{context}

Output a JSON object with the following structure:
{{
    "intent": "<retrieve|count|aggregate|compare|trend|group|filter|join|sort>",
    "entities": {{
        "collection": "<main collection name>",
        "fields": ["<field1>", "<field2>"],
        "values": {{"<field>": "<value>"}}
    }},
    "filters": [
        {{"field": "<name>", "operator": "<eq|ne|gt|lt|gte|lte|in|contains>", "value": "<value>"}}
    ],
    "aggregations": [
        {{"function": "<count|sum|avg|min|max>", "field": "<name>", "alias": "<name>"}}
    ],
    "sort": {{"field": "<name>", "order": "<asc|desc>"}},
    "limit": <number or null>,
    "confidence": <0.0-1.0>,
    "explanation": "<brief explanation of query interpretation>"
}}

Respond with only the JSON object, no additional text."""

    SQL_GENERATION_PROMPT = """You are a SQL expert. Generate an optimized SQL query for the following request.

Database Schema:
{schema}

Parsed Query:
{parsed}

Requirements:
- Use standard SQL syntax
- Include appropriate indexes hints if beneficial
- Handle NULL values correctly
- Use parameterized queries where possible

Output the SQL query only, no explanation."""

    RESULT_EXPLANATION_PROMPT = """You are a data analyst. Explain the following query results in natural language.

Original Question: "{query}"

Query Results:
{results}

Row Count: {count}

Provide a clear, concise explanation of what the data shows. If there are notable patterns or insights, mention them."""

    def __init__(self, config: LLMConfig):
        self.config = config
        self.client = self._create_client()
        self.cache = LLMCache(ttl_seconds=config.cache_ttl_seconds)
        self.rate_limiter = RateLimiter(config.rate_limit_rpm)

    def _create_client(self) -> LLMClient:
        """Create appropriate LLM client."""
        if self.config.provider == LLMProvider.OPENAI:
            return OpenAIClient(self.config)
        elif self.config.provider == LLMProvider.ANTHROPIC:
            return AnthropicClient(self.config)
        elif self.config.provider == LLMProvider.LOCAL:
            return LocalLLMClient(self.config)
        else:
            raise ValueError(f"Unsupported provider: {self.config.provider}")

    async def parse_query(
        self,
        query: str,
        schema: Dict,
        context: Optional[str] = None,
    ) -> QueryParseResult:
        """
        Use LLM to parse natural language query.

        Args:
            query: Natural language query
            schema: Database schema
            context: Previous conversation context

        Returns:
            Parsed query structure
        """
        prompt = self.QUERY_PARSE_PROMPT.format(
            schema=self._format_schema(schema),
            query=query,
            context=context or "None",
        )

        response = await self._call_llm(prompt)

        # Parse JSON response
        parsed = self._parse_json_response(response.content)

        return QueryParseResult(
            intent=parsed.get("intent", "retrieve"),
            entities=parsed.get("entities", {}),
            filters=parsed.get("filters", []),
            aggregations=parsed.get("aggregations", []),
            sort=parsed.get("sort"),
            limit=parsed.get("limit"),
            confidence=parsed.get("confidence", 0.8),
            explanation=parsed.get("explanation", ""),
        )

    async def generate_sql(
        self,
        parsed: QueryParseResult,
        schema: Dict,
    ) -> str:
        """Generate SQL from parsed query."""
        prompt = self.SQL_GENERATION_PROMPT.format(
            schema=self._format_schema(schema),
            parsed=json.dumps({
                "intent": parsed.intent,
                "entities": parsed.entities,
                "filters": parsed.filters,
                "aggregations": parsed.aggregations,
                "sort": parsed.sort,
                "limit": parsed.limit,
            }, indent=2),
        )

        response = await self._call_llm(prompt)
        return response.content.strip()

    async def explain_results(
        self,
        query: str,
        results: List[Dict],
        count: int,
    ) -> str:
        """Generate natural language explanation of results."""
        # Truncate results for prompt
        sample_results = results[:10] if len(results) > 10 else results

        prompt = self.RESULT_EXPLANATION_PROMPT.format(
            query=query,
            results=json.dumps(sample_results, indent=2, default=str),
            count=count,
        )

        response = await self._call_llm(prompt, temperature=0.3)
        return response.content

    async def suggest_queries(
        self,
        schema: Dict,
        context: Optional[str] = None,
    ) -> List[str]:
        """Suggest relevant queries based on schema and context."""
        prompt = f"""Based on the following database schema, suggest 5 useful queries a user might want to run.

Schema:
{self._format_schema(schema)}

Previous Context: {context or 'None'}

Output a JSON array of query strings, e.g., ["query 1", "query 2", ...]"""

        response = await self._call_llm(prompt)
        suggestions = self._parse_json_response(response.content)

        if isinstance(suggestions, list):
            return suggestions
        return []

    async def fix_query_error(
        self,
        query: str,
        error: str,
        schema: Dict,
    ) -> Optional[str]:
        """Attempt to fix a failed query."""
        prompt = f"""The following query failed with an error. Suggest a corrected version.

Original Query: "{query}"
Error: {error}

Schema:
{self._format_schema(schema)}

Output only the corrected query, or "UNABLE_TO_FIX" if the query cannot be fixed."""

        response = await self._call_llm(prompt)
        fixed = response.content.strip()

        if fixed == "UNABLE_TO_FIX":
            return None
        return fixed

    async def _call_llm(
        self,
        prompt: str,
        temperature: Optional[float] = None,
    ) -> LLMResponse:
        """Call LLM with caching and rate limiting."""
        temp = temperature if temperature is not None else self.config.temperature

        # Check cache
        if self.config.cache_enabled:
            cached = self.cache.get(prompt, self.config.model, temp)
            if cached:
                return cached

        # Rate limit
        await self.rate_limiter.acquire()

        # Call LLM with retries
        last_error = None
        for attempt in range(self.config.retry_attempts):
            try:
                response = await self.client.complete(prompt, temperature=temp)

                # Cache response
                if self.config.cache_enabled:
                    self.cache.put(prompt, self.config.model, temp, response)

                return response

            except Exception as e:
                last_error = e
                if attempt < self.config.retry_attempts - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff

        raise RuntimeError(f"LLM call failed after {self.config.retry_attempts} attempts: {last_error}")

    def _format_schema(self, schema: Dict) -> str:
        """Format schema for prompts."""
        lines = []

        for collection, fields in schema.get("fields", {}).items():
            lines.append(f"Collection: {collection}")
            for field in fields:
                lines.append(f"  - {field}")

        if schema.get("indexes"):
            lines.append("\nIndexes:")
            for idx_name, idx_info in schema["indexes"].items():
                lines.append(f"  - {idx_name}: {idx_info}")

        return "\n".join(lines) if lines else "No schema available"

    def _parse_json_response(self, content: str) -> Union[Dict, List]:
        """Parse JSON from LLM response."""
        # Try to extract JSON from response
        content = content.strip()

        # Remove markdown code blocks if present
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])

        try:
            return json.loads(content)
        except json.JSONDecodeError:
            # Try to find JSON in response
            json_match = re.search(r'(\{.*\}|\[.*\])', content, re.DOTALL)
            if json_match:
                try:
                    return json.loads(json_match.group(1))
                except json.JSONDecodeError:
                    pass

        return {}


class EmbeddingClient:
    """Client for generating embeddings for semantic similarity."""

    def __init__(self, config: LLMConfig):
        self.config = config
        self._client = None

    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings for texts."""
        if self.config.provider == LLMProvider.OPENAI:
            return await self._embed_openai(texts)
        elif self.config.provider == LLMProvider.LOCAL:
            return await self._embed_local(texts)
        else:
            # Fallback to simple bag-of-words
            return [self._simple_embed(t) for t in texts]

    async def _embed_openai(self, texts: List[str]) -> List[List[float]]:
        try:
            import openai
            client = openai.AsyncOpenAI(api_key=self.config.api_key)
            response = await client.embeddings.create(
                model="text-embedding-3-small",
                input=texts,
            )
            return [e.embedding for e in response.data]
        except ImportError:
            return [self._simple_embed(t) for t in texts]

    async def _embed_local(self, texts: List[str]) -> List[List[float]]:
        import aiohttp
        base_url = self.config.api_base or "http://localhost:11434"

        embeddings = []
        async with aiohttp.ClientSession() as session:
            for text in texts:
                async with session.post(
                    f"{base_url}/api/embeddings",
                    json={"model": self.config.model, "prompt": text},
                ) as response:
                    result = await response.json()
                    embeddings.append(result.get("embedding", []))

        return embeddings

    def _simple_embed(self, text: str, dim: int = 384) -> List[float]:
        """Simple embedding fallback using character n-grams."""
        import hashlib

        # Generate consistent pseudo-embedding from text
        embedding = [0.0] * dim
        words = text.lower().split()

        for i, word in enumerate(words):
            # Hash word to get indices
            h = hashlib.md5(word.encode()).hexdigest()
            for j in range(0, len(h), 4):
                idx = int(h[j:j+4], 16) % dim
                embedding[idx] += 1.0 / (i + 1)

        # Normalize
        magnitude = sum(x*x for x in embedding) ** 0.5
        if magnitude > 0:
            embedding = [x / magnitude for x in embedding]

        return embedding

    def similarity(self, a: List[float], b: List[float]) -> float:
        """Cosine similarity between two embeddings."""
        if len(a) != len(b):
            return 0.0

        dot = sum(x * y for x, y in zip(a, b))
        mag_a = sum(x * x for x in a) ** 0.5
        mag_b = sum(x * x for x in b) ** 0.5

        if mag_a == 0 or mag_b == 0:
            return 0.0

        return dot / (mag_a * mag_b)
