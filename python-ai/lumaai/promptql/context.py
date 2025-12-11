"""
Conversation Context - Maintains query history and context for multi-turn conversations.

This module implements:
- Conversation state management
- Query memory with relevance scoring
- Context compression for long conversations
- Entity tracking across queries
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime
from collections import deque
import hashlib
import json


@dataclass
class QueryTurn:
    """A single query-response turn in the conversation."""
    turn_id: int
    timestamp: datetime
    query: str
    parsed_intent: str
    entities: Dict[str, Any]
    result_summary: str
    result_count: int
    execution_time_ms: float
    confidence: float

    def to_dict(self) -> Dict:
        return {
            "turn_id": self.turn_id,
            "timestamp": self.timestamp.isoformat(),
            "query": self.query,
            "intent": self.parsed_intent,
            "entities": self.entities,
            "result_summary": self.result_summary,
            "result_count": self.result_count,
            "execution_time_ms": self.execution_time_ms,
            "confidence": self.confidence,
        }


@dataclass
class EntityReference:
    """Tracks an entity mentioned in conversation."""
    name: str
    entity_type: str  # collection, field, value, function
    first_mention: int  # turn_id
    last_mention: int
    mention_count: int
    aliases: Set[str] = field(default_factory=set)

    def relevance_score(self, current_turn: int) -> float:
        """Calculate relevance based on recency and frequency."""
        recency = 1.0 / (1.0 + (current_turn - self.last_mention))
        frequency = min(1.0, self.mention_count / 5.0)
        return 0.6 * recency + 0.4 * frequency


class ConversationContext:
    """
    Maintains conversation state for multi-turn PromptQL queries.

    Tracks:
    - Query history with results
    - Referenced entities
    - User preferences
    - Query patterns
    """

    def __init__(self, max_turns: int = 50, session_id: Optional[str] = None):
        self.session_id = session_id or self._generate_session_id()
        self.max_turns = max_turns
        self.turns: deque[QueryTurn] = deque(maxlen=max_turns)
        self.entities: Dict[str, EntityReference] = {}
        self.current_turn_id = 0
        self.user_preferences: Dict[str, Any] = {}
        self.active_filters: Dict[str, Any] = {}
        self.created_at = datetime.now()

    def _generate_session_id(self) -> str:
        """Generate unique session ID."""
        timestamp = datetime.now().isoformat()
        return hashlib.md5(timestamp.encode()).hexdigest()[:12]

    def add_turn(
        self,
        query: str,
        parsed_intent: str,
        entities: Dict[str, Any],
        result_summary: str,
        result_count: int,
        execution_time_ms: float,
        confidence: float = 1.0,
    ) -> QueryTurn:
        """Add a new query turn to the conversation."""
        self.current_turn_id += 1

        turn = QueryTurn(
            turn_id=self.current_turn_id,
            timestamp=datetime.now(),
            query=query,
            parsed_intent=parsed_intent,
            entities=entities,
            result_summary=result_summary,
            result_count=result_count,
            execution_time_ms=execution_time_ms,
            confidence=confidence,
        )

        self.turns.append(turn)

        # Update entity tracking
        self._track_entities(entities)

        return turn

    def _track_entities(self, entities: Dict[str, Any]):
        """Track entities mentioned in the query."""
        for entity_type, values in entities.items():
            if not isinstance(values, list):
                values = [values]

            for value in values:
                if value is None:
                    continue

                key = f"{entity_type}:{value}"

                if key in self.entities:
                    ref = self.entities[key]
                    ref.last_mention = self.current_turn_id
                    ref.mention_count += 1
                else:
                    self.entities[key] = EntityReference(
                        name=str(value),
                        entity_type=entity_type,
                        first_mention=self.current_turn_id,
                        last_mention=self.current_turn_id,
                        mention_count=1,
                    )

    def get_recent_context(self, n_turns: int = 5) -> List[Dict]:
        """Get recent conversation context."""
        recent = list(self.turns)[-n_turns:]
        return [turn.to_dict() for turn in recent]

    def get_relevant_entities(self, threshold: float = 0.3) -> List[EntityReference]:
        """Get entities relevant to current context."""
        relevant = []
        for ref in self.entities.values():
            score = ref.relevance_score(self.current_turn_id)
            if score >= threshold:
                relevant.append(ref)

        # Sort by relevance
        relevant.sort(key=lambda x: x.relevance_score(self.current_turn_id), reverse=True)
        return relevant

    def resolve_reference(self, reference: str) -> Optional[str]:
        """
        Resolve pronouns and references to actual entities.

        Examples:
        - "it" -> last mentioned collection
        - "them" -> last mentioned entities
        - "that field" -> last mentioned field
        """
        reference_lower = reference.lower()

        # Handle pronouns
        if reference_lower in ["it", "that", "this"]:
            # Return most recently mentioned entity
            recent_entities = self.get_relevant_entities(threshold=0.0)
            if recent_entities:
                return recent_entities[0].name

        elif reference_lower in ["they", "them", "those", "these"]:
            # Return last mentioned collection or plural entity
            for ref in self.get_relevant_entities():
                if ref.entity_type == "collection":
                    return ref.name

        elif "field" in reference_lower:
            # Return last mentioned field
            for ref in self.get_relevant_entities():
                if ref.entity_type == "field":
                    return ref.name

        elif "table" in reference_lower or "collection" in reference_lower:
            # Return last mentioned collection
            for ref in self.get_relevant_entities():
                if ref.entity_type == "collection":
                    return ref.name

        return None

    def get_active_collection(self) -> Optional[str]:
        """Get the currently active collection from context."""
        for ref in self.get_relevant_entities():
            if ref.entity_type == "collection":
                return ref.name
        return None

    def set_filter(self, field: str, value: Any):
        """Set an active filter that persists across queries."""
        self.active_filters[field] = value

    def clear_filter(self, field: str):
        """Clear a specific filter."""
        self.active_filters.pop(field, None)

    def clear_all_filters(self):
        """Clear all active filters."""
        self.active_filters.clear()

    def get_filters(self) -> Dict[str, Any]:
        """Get all active filters."""
        return self.active_filters.copy()

    def set_preference(self, key: str, value: Any):
        """Set a user preference."""
        self.user_preferences[key] = value

    def get_preference(self, key: str, default: Any = None) -> Any:
        """Get a user preference."""
        return self.user_preferences.get(key, default)

    def compress_context(self) -> str:
        """
        Compress conversation context into a summary string.

        Useful for feeding into LLM prompts.
        """
        summary_parts = []

        # Recent queries
        recent = list(self.turns)[-3:]
        if recent:
            summary_parts.append("Recent queries:")
            for turn in recent:
                summary_parts.append(f"  - {turn.query} ({turn.result_count} results)")

        # Active entities
        relevant = self.get_relevant_entities(threshold=0.5)[:5]
        if relevant:
            summary_parts.append("\nActive entities:")
            for ref in relevant:
                summary_parts.append(f"  - {ref.name} ({ref.entity_type})")

        # Active filters
        if self.active_filters:
            summary_parts.append("\nActive filters:")
            for field, value in self.active_filters.items():
                summary_parts.append(f"  - {field} = {value}")

        return "\n".join(summary_parts)

    def to_dict(self) -> Dict:
        """Serialize context to dictionary."""
        return {
            "session_id": self.session_id,
            "current_turn": self.current_turn_id,
            "turns": [t.to_dict() for t in self.turns],
            "entities": {k: {
                "name": v.name,
                "type": v.entity_type,
                "mentions": v.mention_count,
            } for k, v in self.entities.items()},
            "active_filters": self.active_filters,
            "preferences": self.user_preferences,
        }


class QueryMemory:
    """
    Long-term memory for query patterns and results.

    Enables:
    - Query caching with semantic similarity
    - Pattern learning
    - Result reuse
    """

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.memories: Dict[str, MemoryEntry] = {}
        self.query_patterns: Dict[str, int] = {}  # pattern -> count

    def store(
        self,
        query: str,
        normalized_query: str,
        intent: str,
        result: Any,
        execution_time_ms: float,
        schema_hash: str,
    ):
        """Store a query result in memory."""
        key = self._make_key(normalized_query, schema_hash)

        self.memories[key] = MemoryEntry(
            query=query,
            normalized_query=normalized_query,
            intent=intent,
            result=result,
            execution_time_ms=execution_time_ms,
            schema_hash=schema_hash,
            created_at=datetime.now(),
            access_count=1,
            last_accessed=datetime.now(),
        )

        # Track pattern
        pattern = self._extract_pattern(normalized_query)
        self.query_patterns[pattern] = self.query_patterns.get(pattern, 0) + 1

        # Evict if over capacity
        self._maybe_evict()

    def recall(
        self,
        normalized_query: str,
        schema_hash: str,
        max_age_seconds: int = 3600,
    ) -> Optional["MemoryEntry"]:
        """Recall a stored query result if available."""
        key = self._make_key(normalized_query, schema_hash)

        if key not in self.memories:
            return None

        entry = self.memories[key]

        # Check age
        age = (datetime.now() - entry.created_at).total_seconds()
        if age > max_age_seconds:
            del self.memories[key]
            return None

        # Update access stats
        entry.access_count += 1
        entry.last_accessed = datetime.now()

        return entry

    def find_similar(
        self,
        query: str,
        intent: str,
        threshold: float = 0.8,
    ) -> List[Tuple["MemoryEntry", float]]:
        """Find similar queries in memory."""
        similar = []
        query_words = set(query.lower().split())

        for entry in self.memories.values():
            if entry.intent != intent:
                continue

            # Simple word overlap similarity
            entry_words = set(entry.query.lower().split())
            overlap = len(query_words & entry_words)
            union = len(query_words | entry_words)

            if union > 0:
                similarity = overlap / union
                if similarity >= threshold:
                    similar.append((entry, similarity))

        similar.sort(key=lambda x: x[1], reverse=True)
        return similar[:5]

    def get_common_patterns(self, n: int = 10) -> List[Tuple[str, int]]:
        """Get most common query patterns."""
        patterns = sorted(
            self.query_patterns.items(),
            key=lambda x: x[1],
            reverse=True
        )
        return patterns[:n]

    def _make_key(self, normalized_query: str, schema_hash: str) -> str:
        """Create cache key."""
        combined = f"{normalized_query}:{schema_hash}"
        return hashlib.md5(combined.encode()).hexdigest()

    def _extract_pattern(self, query: str) -> str:
        """Extract query pattern for tracking."""
        # Replace specific values with placeholders
        import re
        pattern = query.lower()
        pattern = re.sub(r'"[^"]+"', '"VALUE"', pattern)
        pattern = re.sub(r'\b\d+\b', 'NUM', pattern)
        pattern = re.sub(r'\b[a-f0-9]{8,}\b', 'ID', pattern)
        return pattern

    def _maybe_evict(self):
        """Evict old entries if over capacity."""
        if len(self.memories) <= self.max_size:
            return

        # Sort by last accessed and evict oldest
        entries = sorted(
            self.memories.items(),
            key=lambda x: x[1].last_accessed
        )

        to_remove = len(self.memories) - self.max_size
        for key, _ in entries[:to_remove]:
            del self.memories[key]

    def clear(self):
        """Clear all memories."""
        self.memories.clear()
        self.query_patterns.clear()

    def stats(self) -> Dict:
        """Get memory statistics."""
        return {
            "total_entries": len(self.memories),
            "total_patterns": len(self.query_patterns),
            "top_patterns": self.get_common_patterns(5),
        }


@dataclass
class MemoryEntry:
    """A single memory entry."""
    query: str
    normalized_query: str
    intent: str
    result: Any
    execution_time_ms: float
    schema_hash: str
    created_at: datetime
    access_count: int
    last_accessed: datetime


class ContextManager:
    """
    Manages multiple conversation contexts and shared memory.

    Handles:
    - Multi-session support
    - Context switching
    - Global memory sharing
    """

    def __init__(self):
        self.contexts: Dict[str, ConversationContext] = {}
        self.global_memory = QueryMemory(max_size=10000)
        self.active_session: Optional[str] = None

    def create_context(self, session_id: Optional[str] = None) -> ConversationContext:
        """Create a new conversation context."""
        ctx = ConversationContext(session_id=session_id)
        self.contexts[ctx.session_id] = ctx
        self.active_session = ctx.session_id
        return ctx

    def get_context(self, session_id: str) -> Optional[ConversationContext]:
        """Get context by session ID."""
        return self.contexts.get(session_id)

    def get_or_create_context(self, session_id: str) -> ConversationContext:
        """Get existing context or create new one."""
        if session_id not in self.contexts:
            return self.create_context(session_id)
        return self.contexts[session_id]

    def get_active_context(self) -> Optional[ConversationContext]:
        """Get the currently active context."""
        if self.active_session:
            return self.contexts.get(self.active_session)
        return None

    def switch_context(self, session_id: str) -> bool:
        """Switch to a different context."""
        if session_id in self.contexts:
            self.active_session = session_id
            return True
        return False

    def remove_context(self, session_id: str):
        """Remove a context."""
        self.contexts.pop(session_id, None)
        if self.active_session == session_id:
            self.active_session = None

    def list_sessions(self) -> List[Dict]:
        """List all sessions."""
        return [
            {
                "session_id": ctx.session_id,
                "turns": ctx.current_turn_id,
                "created_at": ctx.created_at.isoformat(),
                "active": ctx.session_id == self.active_session,
            }
            for ctx in self.contexts.values()
        ]
