"""
Semantic Understanding - Advanced NLP for query interpretation.

This module provides:
- Semantic similarity search
- Query intent classification
- Entity recognition
- Typo correction
- Synonym expansion
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set
from enum import Enum
import re
from collections import defaultdict


class QueryIntent(Enum):
    """Semantic query intents."""
    SELECT = "select"           # Retrieve records
    COUNT = "count"             # Count records
    SUM = "sum"                 # Sum values
    AVERAGE = "average"         # Calculate average
    MIN = "min"                 # Find minimum
    MAX = "max"                 # Find maximum
    GROUP = "group"             # Group and aggregate
    COMPARE = "compare"         # Compare entities
    TREND = "trend"             # Analyze over time
    TOP_N = "top_n"             # Get top N
    BOTTOM_N = "bottom_n"       # Get bottom N
    DISTINCT = "distinct"       # Unique values
    EXISTS = "exists"           # Check existence
    SEARCH = "search"           # Full-text search
    RELATIONSHIP = "relationship"  # Find relationships
    UNKNOWN = "unknown"


@dataclass
class SemanticEntity:
    """An entity extracted from a query."""
    text: str
    entity_type: str
    start: int
    end: int
    confidence: float
    resolved_value: Optional[Any] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SemanticParseResult:
    """Result of semantic query parsing."""
    original_query: str
    normalized_query: str
    intent: QueryIntent
    intent_confidence: float
    entities: List[SemanticEntity]
    time_expression: Optional[Dict]
    negation: bool
    comparison_type: Optional[str]
    quantity_expression: Optional[Dict]


class SemanticParser:
    """
    Parses queries using semantic understanding.

    Uses patterns, dictionaries, and embeddings to understand queries.
    """

    # Intent patterns
    INTENT_PATTERNS = {
        QueryIntent.COUNT: [
            r"\bhow many\b", r"\bcount\b", r"\bnumber of\b", r"\btotal\b.*\brecords?\b",
        ],
        QueryIntent.SUM: [
            r"\bsum\b", r"\btotal\b.*\bamount\b", r"\badd up\b",
        ],
        QueryIntent.AVERAGE: [
            r"\baverage\b", r"\bavg\b", r"\bmean\b",
        ],
        QueryIntent.MIN: [
            r"\bminimum\b", r"\bmin\b", r"\blowest\b", r"\bsmallest\b",
        ],
        QueryIntent.MAX: [
            r"\bmaximum\b", r"\bmax\b", r"\bhighest\b", r"\blargest\b", r"\bbiggest\b",
        ],
        QueryIntent.GROUP: [
            r"\bgroup by\b", r"\bper\b", r"\bby\b.*\bcategory\b", r"\bfor each\b",
        ],
        QueryIntent.COMPARE: [
            r"\bcompare\b", r"\bversus\b", r"\bvs\b", r"\bdifference between\b",
        ],
        QueryIntent.TREND: [
            r"\btrend\b", r"\bover time\b", r"\bgrowth\b", r"\bchange\b.*\btime\b",
        ],
        QueryIntent.TOP_N: [
            r"\btop\s+\d+\b", r"\bbest\b", r"\bhighest\s+\d+\b",
        ],
        QueryIntent.BOTTOM_N: [
            r"\bbottom\s+\d+\b", r"\bworst\b", r"\blowest\s+\d+\b",
        ],
        QueryIntent.DISTINCT: [
            r"\bunique\b", r"\bdistinct\b", r"\bdifferent\b",
        ],
        QueryIntent.EXISTS: [
            r"\bis there\b", r"\bare there\b", r"\bdoes.*exist\b", r"\bany\b",
        ],
        QueryIntent.SEARCH: [
            r"\bsearch\b", r"\bfind\b.*\bcontaining\b", r"\blook for\b",
        ],
    }

    # Time expression patterns
    TIME_PATTERNS = [
        (r"\blast\s+(\d+)\s+(day|week|month|year)s?\b", "relative_past"),
        (r"\bpast\s+(\d+)\s+(day|week|month|year)s?\b", "relative_past"),
        (r"\bthis\s+(week|month|year)\b", "relative_current"),
        (r"\byesterday\b", "yesterday"),
        (r"\btoday\b", "today"),
        (r"\btomorrow\b", "tomorrow"),
        (r"\bsince\s+(\w+)\b", "since"),
        (r"\bbetween\s+(.+?)\s+and\s+(.+?)(?:\s|$)", "range"),
        (r"\bbefore\s+(.+?)(?:\s|$)", "before"),
        (r"\bafter\s+(.+?)(?:\s|$)", "after"),
    ]

    # Numeric patterns
    NUMERIC_PATTERNS = [
        (r"\bmore than\s+(\d+(?:\.\d+)?)\b", "gt"),
        (r"\bless than\s+(\d+(?:\.\d+)?)\b", "lt"),
        (r"\bgreater than\s+(\d+(?:\.\d+)?)\b", "gt"),
        (r"\bat least\s+(\d+(?:\.\d+)?)\b", "gte"),
        (r"\bat most\s+(\d+(?:\.\d+)?)\b", "lte"),
        (r"\bexactly\s+(\d+(?:\.\d+)?)\b", "eq"),
        (r"\b(\d+(?:\.\d+)?)\s+or more\b", "gte"),
        (r"\b(\d+(?:\.\d+)?)\s+or less\b", "lte"),
        (r"\bbetween\s+(\d+(?:\.\d+)?)\s+and\s+(\d+(?:\.\d+)?)\b", "between"),
    ]

    # Common synonyms
    SYNONYMS = {
        "show": ["display", "list", "get", "find", "retrieve", "fetch"],
        "all": ["every", "each", "entire"],
        "users": ["customers", "clients", "members", "accounts", "people"],
        "orders": ["purchases", "transactions", "sales"],
        "products": ["items", "goods", "merchandise"],
        "amount": ["value", "sum", "total", "price", "cost"],
        "date": ["time", "when", "timestamp"],
        "name": ["title", "label"],
        "delete": ["remove", "drop", "erase"],
        "update": ["modify", "change", "edit", "set"],
        "create": ["add", "insert", "new", "make"],
    }

    def __init__(self, schema: Optional[Dict] = None):
        self.schema = schema or {}
        self._build_vocabulary()

    def _build_vocabulary(self):
        """Build vocabulary from schema and synonyms."""
        self.vocabulary: Set[str] = set()
        self.field_names: Set[str] = set()
        self.collection_names: Set[str] = set()

        # Add schema fields
        for collection, fields in self.schema.get("fields", {}).items():
            self.collection_names.add(collection.lower())
            self.vocabulary.add(collection.lower())
            for field in fields:
                self.field_names.add(field.lower())
                self.vocabulary.add(field.lower())

        # Add synonyms
        for word, syns in self.SYNONYMS.items():
            self.vocabulary.add(word)
            self.vocabulary.update(syns)

    def parse(self, query: str) -> SemanticParseResult:
        """
        Parse query semantically.

        Args:
            query: Natural language query

        Returns:
            Semantic parse result
        """
        # Normalize query
        normalized = self._normalize(query)

        # Detect intent
        intent, confidence = self._detect_intent(normalized)

        # Extract entities
        entities = self._extract_entities(query, normalized)

        # Extract time expressions
        time_expr = self._extract_time_expression(normalized)

        # Check for negation
        negation = self._detect_negation(normalized)

        # Extract comparison type
        comparison = self._detect_comparison(normalized)

        # Extract quantity expressions
        quantity = self._extract_quantity(normalized)

        return SemanticParseResult(
            original_query=query,
            normalized_query=normalized,
            intent=intent,
            intent_confidence=confidence,
            entities=entities,
            time_expression=time_expr,
            negation=negation,
            comparison_type=comparison,
            quantity_expression=quantity,
        )

    def _normalize(self, query: str) -> str:
        """Normalize query text."""
        # Lowercase
        normalized = query.lower()

        # Remove extra whitespace
        normalized = " ".join(normalized.split())

        # Expand contractions
        contractions = {
            "don't": "do not",
            "doesn't": "does not",
            "didn't": "did not",
            "won't": "will not",
            "can't": "cannot",
            "isn't": "is not",
            "aren't": "are not",
            "wasn't": "was not",
            "weren't": "were not",
            "haven't": "have not",
            "hasn't": "has not",
            "hadn't": "had not",
        }
        for contraction, expansion in contractions.items():
            normalized = normalized.replace(contraction, expansion)

        return normalized

    def _detect_intent(self, query: str) -> Tuple[QueryIntent, float]:
        """Detect query intent."""
        scores: Dict[QueryIntent, float] = defaultdict(float)

        for intent, patterns in self.INTENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, query, re.IGNORECASE):
                    scores[intent] += 1.0

        if not scores:
            return QueryIntent.SELECT, 0.5  # Default to SELECT

        # Normalize scores
        max_score = max(scores.values())
        best_intent = max(scores.items(), key=lambda x: x[1])[0]

        confidence = min(max_score / 2.0, 1.0)  # Cap at 1.0

        return best_intent, confidence

    def _extract_entities(
        self,
        original: str,
        normalized: str,
    ) -> List[SemanticEntity]:
        """Extract entities from query."""
        entities = []

        # Extract quoted strings
        for match in re.finditer(r'"([^"]+)"', original):
            entities.append(SemanticEntity(
                text=match.group(1),
                entity_type="value",
                start=match.start(),
                end=match.end(),
                confidence=1.0,
            ))

        # Extract collection references
        for collection in self.collection_names:
            if collection in normalized:
                match = re.search(r"\b" + re.escape(collection) + r"\b", normalized)
                if match:
                    entities.append(SemanticEntity(
                        text=collection,
                        entity_type="collection",
                        start=match.start(),
                        end=match.end(),
                        confidence=0.9,
                    ))

        # Extract field references
        for field in self.field_names:
            if field in normalized:
                match = re.search(r"\b" + re.escape(field) + r"\b", normalized)
                if match:
                    entities.append(SemanticEntity(
                        text=field,
                        entity_type="field",
                        start=match.start(),
                        end=match.end(),
                        confidence=0.9,
                    ))

        # Extract numbers
        for match in re.finditer(r"\b(\d+(?:\.\d+)?)\b", normalized):
            entities.append(SemanticEntity(
                text=match.group(1),
                entity_type="number",
                start=match.start(),
                end=match.end(),
                confidence=1.0,
                resolved_value=float(match.group(1)),
            ))

        # Extract capitalized words (potential proper nouns)
        for match in re.finditer(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b", original):
            text = match.group(1)
            if text.lower() not in {"the", "and", "or", "for", "with"}:
                entities.append(SemanticEntity(
                    text=text,
                    entity_type="proper_noun",
                    start=match.start(),
                    end=match.end(),
                    confidence=0.7,
                ))

        return entities

    def _extract_time_expression(self, query: str) -> Optional[Dict]:
        """Extract time expression from query."""
        for pattern, expr_type in self.TIME_PATTERNS:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return {
                    "type": expr_type,
                    "raw": match.group(0),
                    "groups": match.groups(),
                }
        return None

    def _detect_negation(self, query: str) -> bool:
        """Detect if query contains negation."""
        negation_patterns = [
            r"\bnot\b", r"\bno\b", r"\bnever\b", r"\bwithout\b",
            r"\bexclude\b", r"\bexcept\b", r"\bexcluding\b",
            r"\bdoes not\b", r"\bdo not\b", r"\bis not\b",
        ]
        for pattern in negation_patterns:
            if re.search(pattern, query, re.IGNORECASE):
                return True
        return False

    def _detect_comparison(self, query: str) -> Optional[str]:
        """Detect comparison type in query."""
        if re.search(r"\bmore than\b|\bgreater than\b|\b>\b", query):
            return "gt"
        if re.search(r"\bless than\b|\bsmaller than\b|\b<\b", query):
            return "lt"
        if re.search(r"\bequal to\b|\bsame as\b|\b=\b", query):
            return "eq"
        if re.search(r"\bbetween\b", query):
            return "between"
        return None

    def _extract_quantity(self, query: str) -> Optional[Dict]:
        """Extract quantity expression from query."""
        for pattern, op in self.NUMERIC_PATTERNS:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                groups = match.groups()
                if op == "between":
                    return {
                        "operator": op,
                        "values": [float(groups[0]), float(groups[1])],
                    }
                return {
                    "operator": op,
                    "value": float(groups[0]),
                }
        return None


class TypoCorrector:
    """
    Corrects typos in queries using edit distance.
    """

    def __init__(self, vocabulary: Set[str], max_distance: int = 2):
        self.vocabulary = vocabulary
        self.max_distance = max_distance

    def correct(self, word: str) -> Tuple[str, float]:
        """
        Correct a potentially misspelled word.

        Returns:
            (corrected_word, confidence)
        """
        if word in self.vocabulary:
            return word, 1.0

        # Find closest matches
        candidates = []
        for vocab_word in self.vocabulary:
            distance = self._edit_distance(word, vocab_word)
            if distance <= self.max_distance:
                candidates.append((vocab_word, distance))

        if not candidates:
            return word, 0.0

        # Return best match
        candidates.sort(key=lambda x: x[1])
        best_word, distance = candidates[0]

        # Calculate confidence based on distance
        confidence = 1.0 - (distance / max(len(word), len(best_word)))

        return best_word, confidence

    def correct_query(self, query: str) -> Tuple[str, List[Tuple[str, str]]]:
        """
        Correct typos in entire query.

        Returns:
            (corrected_query, list of (original, corrected) pairs)
        """
        words = query.split()
        corrections = []
        corrected_words = []

        for word in words:
            # Skip punctuation and short words
            clean_word = re.sub(r'[^\w]', '', word.lower())
            if len(clean_word) < 3:
                corrected_words.append(word)
                continue

            corrected, confidence = self.correct(clean_word)

            if corrected != clean_word and confidence > 0.5:
                # Preserve original case/punctuation
                corrected_word = word.replace(clean_word, corrected)
                corrected_words.append(corrected_word)
                corrections.append((word, corrected_word))
            else:
                corrected_words.append(word)

        return " ".join(corrected_words), corrections

    def _edit_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein edit distance."""
        if len(s1) < len(s2):
            s1, s2 = s2, s1

        if len(s2) == 0:
            return len(s1)

        prev_row = range(len(s2) + 1)

        for i, c1 in enumerate(s1):
            curr_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row

        return prev_row[-1]


class SynonymExpander:
    """
    Expands queries with synonyms for better matching.
    """

    def __init__(self, synonyms: Optional[Dict[str, List[str]]] = None):
        self.synonyms = synonyms or SemanticParser.SYNONYMS
        self._build_reverse_map()

    def _build_reverse_map(self):
        """Build reverse synonym map."""
        self.reverse_map: Dict[str, str] = {}
        for canonical, syns in self.synonyms.items():
            for syn in syns:
                self.reverse_map[syn] = canonical

    def canonicalize(self, word: str) -> str:
        """Convert word to canonical form."""
        word_lower = word.lower()
        return self.reverse_map.get(word_lower, word_lower)

    def expand(self, word: str) -> List[str]:
        """Get all synonyms for a word."""
        canonical = self.canonicalize(word)

        if canonical in self.synonyms:
            return [canonical] + self.synonyms[canonical]

        return [word]

    def expand_query(self, query: str) -> List[str]:
        """
        Generate query variations with synonym expansions.

        Returns list of query variants.
        """
        words = query.lower().split()
        variants = [query]

        for i, word in enumerate(words):
            synonyms = self.expand(word)
            if len(synonyms) > 1:
                for syn in synonyms[1:]:  # Skip original
                    new_words = words.copy()
                    new_words[i] = syn
                    variants.append(" ".join(new_words))

        return variants


class SemanticSimilarity:
    """
    Computes semantic similarity between queries and schema elements.
    """

    def __init__(self, embedding_client: Optional[Any] = None):
        self.embedding_client = embedding_client
        self._cache: Dict[str, List[float]] = {}

    async def similarity(self, text1: str, text2: str) -> float:
        """Compute similarity between two texts."""
        if self.embedding_client:
            # Use embeddings
            emb1 = await self._get_embedding(text1)
            emb2 = await self._get_embedding(text2)
            return self._cosine_similarity(emb1, emb2)
        else:
            # Fallback to token overlap
            return self._token_similarity(text1, text2)

    async def find_similar(
        self,
        query: str,
        candidates: List[str],
        threshold: float = 0.5,
    ) -> List[Tuple[str, float]]:
        """Find similar candidates above threshold."""
        results = []
        for candidate in candidates:
            score = await self.similarity(query, candidate)
            if score >= threshold:
                results.append((candidate, score))

        results.sort(key=lambda x: x[1], reverse=True)
        return results

    async def _get_embedding(self, text: str) -> List[float]:
        """Get embedding for text (with caching)."""
        if text in self._cache:
            return self._cache[text]

        if self.embedding_client:
            embeddings = await self.embedding_client.embed([text])
            self._cache[text] = embeddings[0]
            return embeddings[0]

        return []

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        """Compute cosine similarity."""
        if not a or not b or len(a) != len(b):
            return 0.0

        dot = sum(x * y for x, y in zip(a, b))
        mag_a = sum(x * x for x in a) ** 0.5
        mag_b = sum(x * x for x in b) ** 0.5

        if mag_a == 0 or mag_b == 0:
            return 0.0

        return dot / (mag_a * mag_b)

    def _token_similarity(self, text1: str, text2: str) -> float:
        """Simple token overlap similarity."""
        tokens1 = set(text1.lower().split())
        tokens2 = set(text2.lower().split())

        intersection = len(tokens1 & tokens2)
        union = len(tokens1 | tokens2)

        if union == 0:
            return 0.0

        return intersection / union
