"""
NLP Processor for TDB+ AI Service

Provides natural language processing capabilities:
- Natural language to query translation
- Entity extraction
- Sentiment analysis
- Keyword extraction
- Query completion suggestions
"""

import re
import asyncio
from typing import Optional
from dataclasses import dataclass

from .inference import ModelManager


@dataclass
class TranslationResult:
    """Result of natural language to query translation."""
    query: str
    confidence: float
    explanation: Optional[str] = None


@dataclass
class AnalysisResult:
    """Result of semantic analysis."""
    entities: Optional[list[dict]] = None
    sentiment: Optional[dict] = None
    keywords: Optional[list[str]] = None


class NLPProcessor:
    """
    Natural Language Processor for TDB+.

    Translates natural language queries into TQL, NQL, or JQL,
    and provides semantic analysis capabilities.
    """

    # Pattern mappings for natural language to TQL
    NL_PATTERNS = [
        # Find/Get patterns
        (r"(?:find|get|show|list|retrieve)\s+(?:all\s+)?(\w+)(?:\s+where\s+(.+))?",
         "SELECT * FROM {collection}{where}"),

        # Count patterns
        (r"(?:count|how many)\s+(?:all\s+)?(\w+)(?:\s+where\s+(.+))?",
         "SELECT COUNT(*) FROM {collection}{where}"),

        # Insert patterns
        (r"(?:add|insert|create)\s+(?:to|into)\s+(\w+)\s+(.+)",
         "INSERT INTO {collection} ({fields}) VALUES ({values})"),

        # Update patterns
        (r"(?:update|modify|change)\s+(\w+)\s+set\s+(.+?)\s+where\s+(.+)",
         "UPDATE {collection} SET {updates} WHERE {condition}"),

        # Delete patterns
        (r"(?:delete|remove)\s+(?:from\s+)?(\w+)\s+where\s+(.+)",
         "DELETE FROM {collection} WHERE {condition}"),
    ]

    # Condition translations
    CONDITION_PATTERNS = [
        (r"(\w+)\s+(?:is\s+)?(?:equal(?:s)?(?:\s+to)?|=)\s+[\"']?(\w+)[\"']?", "{field} = '{value}'"),
        (r"(\w+)\s+(?:is\s+)?(?:greater\s+than|>)\s+(\d+)", "{field} > {value}"),
        (r"(\w+)\s+(?:is\s+)?(?:less\s+than|<)\s+(\d+)", "{field} < {value}"),
        (r"(\w+)\s+(?:is\s+)?(?:greater\s+than\s+or\s+equal(?:\s+to)?|>=)\s+(\d+)", "{field} >= {value}"),
        (r"(\w+)\s+(?:is\s+)?(?:less\s+than\s+or\s+equal(?:\s+to)?|<=)\s+(\d+)", "{field} <= {value}"),
        (r"(\w+)\s+(?:is\s+)?(?:not\s+equal(?:\s+to)?|!=|<>)\s+[\"']?(\w+)[\"']?", "{field} != '{value}'"),
        (r"(\w+)\s+contains\s+[\"']?(\w+)[\"']?", "{field} LIKE '%{value}%'"),
        (r"(\w+)\s+(?:starts\s+with|begins\s+with)\s+[\"']?(\w+)[\"']?", "{field} LIKE '{value}%'"),
        (r"(\w+)\s+(?:ends\s+with)\s+[\"']?(\w+)[\"']?", "{field} LIKE '%{value}'"),
        (r"(\w+)\s+is\s+(true|false)", "{field} = {value}"),
        (r"(\w+)\s+is\s+null", "{field} IS NULL"),
        (r"(\w+)\s+is\s+not\s+null", "{field} IS NOT NULL"),
    ]

    # Sort patterns
    SORT_PATTERNS = [
        (r"(?:sort(?:ed)?|order(?:ed)?)\s+by\s+(\w+)\s+(desc(?:ending)?|asc(?:ending)?)?",
         "ORDER BY {field} {direction}"),
    ]

    # Limit patterns
    LIMIT_PATTERNS = [
        (r"(?:first|top)\s+(\d+)", "LIMIT {limit}"),
        (r"limit\s+(\d+)", "LIMIT {limit}"),
    ]

    def __init__(self, model_manager: ModelManager):
        self.model_manager = model_manager
        self._stopwords = {
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "must", "shall", "can", "need", "dare",
            "ought", "used", "to", "of", "in", "for", "on", "with", "at", "by",
            "from", "as", "into", "through", "during", "before", "after",
            "above", "below", "between", "under", "again", "further", "then",
            "once", "here", "there", "when", "where", "why", "how", "all",
            "each", "few", "more", "most", "other", "some", "such", "no",
            "nor", "not", "only", "own", "same", "so", "than", "too", "very",
        }

    async def translate_query(
        self,
        query: str,
        context: Optional[str] = None,
        target_language: str = "tql",
    ) -> dict:
        """
        Translate natural language to database query.

        Args:
            query: Natural language query
            context: Optional context about the database schema
            target_language: Target query language (tql, nql, jql)

        Returns:
            Dictionary with translated query, confidence, and explanation
        """
        query_lower = query.lower().strip()

        if target_language == "tql":
            return await self._translate_to_tql(query_lower, context)
        elif target_language == "nql":
            return await self._translate_to_nql(query_lower, context)
        elif target_language == "jql":
            return await self._translate_to_jql(query_lower, context)
        else:
            raise ValueError(f"Unknown target language: {target_language}")

    async def _translate_to_tql(self, query: str, context: Optional[str]) -> dict:
        """Translate natural language to TQL."""
        result_query = None
        confidence = 0.0
        explanation = []

        # Try to match patterns
        for pattern, template in self.NL_PATTERNS:
            match = re.match(pattern, query, re.IGNORECASE)
            if match:
                groups = match.groups()
                collection = groups[0] if groups else None

                if "SELECT" in template:
                    where_clause = ""
                    if len(groups) > 1 and groups[1]:
                        condition = await self._translate_condition(groups[1])
                        where_clause = f" WHERE {condition}"

                    result_query = template.format(
                        collection=collection,
                        where=where_clause
                    )
                    explanation.append(f"Identified as SELECT query on '{collection}'")
                    confidence = 0.85

                elif "INSERT" in template:
                    # Parse field=value pairs
                    field_values = await self._parse_insert_values(groups[1] if len(groups) > 1 else "")
                    if field_values:
                        fields = ", ".join(field_values.keys())
                        values = ", ".join(f"'{v}'" if isinstance(v, str) else str(v)
                                          for v in field_values.values())
                        result_query = f"INSERT INTO {collection} ({fields}) VALUES ({values})"
                        explanation.append(f"Identified as INSERT query into '{collection}'")
                        confidence = 0.80

                elif "UPDATE" in template:
                    updates = groups[1] if len(groups) > 1 else ""
                    condition = await self._translate_condition(groups[2] if len(groups) > 2 else "")
                    result_query = f"UPDATE {collection} SET {updates} WHERE {condition}"
                    explanation.append(f"Identified as UPDATE query on '{collection}'")
                    confidence = 0.80

                elif "DELETE" in template:
                    condition = await self._translate_condition(groups[1] if len(groups) > 1 else "")
                    result_query = f"DELETE FROM {collection} WHERE {condition}"
                    explanation.append(f"Identified as DELETE query on '{collection}'")
                    confidence = 0.80

                break

        # Add ORDER BY if present
        for pattern, _ in self.SORT_PATTERNS:
            match = re.search(pattern, query, re.IGNORECASE)
            if match and result_query:
                field = match.group(1)
                direction = "DESC" if match.group(2) and "desc" in match.group(2).lower() else "ASC"
                result_query += f" ORDER BY {field} {direction}"
                explanation.append(f"Added ORDER BY {field} {direction}")

        # Add LIMIT if present
        for pattern, _ in self.LIMIT_PATTERNS:
            match = re.search(pattern, query, re.IGNORECASE)
            if match and result_query:
                limit = match.group(1)
                result_query += f" LIMIT {limit}"
                explanation.append(f"Added LIMIT {limit}")

        if not result_query:
            # Fallback: try to construct a basic SELECT
            words = query.split()
            # Find potential collection name (usually a noun)
            for word in words:
                if word not in self._stopwords and len(word) > 2:
                    result_query = f"SELECT * FROM {word}"
                    confidence = 0.3
                    explanation.append(f"Low confidence: guessed collection '{word}'")
                    break

        return {
            "query": result_query or "SELECT * FROM unknown",
            "confidence": confidence,
            "explanation": "; ".join(explanation) if explanation else None,
        }

    async def _translate_to_nql(self, query: str, context: Optional[str]) -> dict:
        """Translate to NQL format (already natural language, minor normalization)."""
        # NQL is already natural language, just normalize
        normalized = query.strip()

        # Ensure it starts with a valid NQL keyword
        nql_keywords = ["find", "get", "show", "count", "add", "insert", "update", "modify", "delete", "remove"]
        starts_with_keyword = any(normalized.lower().startswith(kw) for kw in nql_keywords)

        if not starts_with_keyword:
            # Try to infer intent
            if any(word in normalized.lower() for word in ["how many", "number of"]):
                normalized = f"count {normalized}"
            else:
                normalized = f"find {normalized}"

        return {
            "query": normalized,
            "confidence": 0.9,
            "explanation": "Normalized to NQL format",
        }

    async def _translate_to_jql(self, query: str, context: Optional[str]) -> dict:
        """Translate natural language to JQL (JSON query)."""
        import json

        # First translate to TQL, then convert to JQL
        tql_result = await self._translate_to_tql(query, context)
        tql_query = tql_result["query"]

        jql = {}

        # Parse TQL and convert to JQL
        if tql_query.startswith("SELECT"):
            # Extract collection and conditions
            match = re.match(r"SELECT .+ FROM (\w+)(?:\s+WHERE\s+(.+))?(?:\s+ORDER BY\s+(\w+)\s+(ASC|DESC))?(?:\s+LIMIT\s+(\d+))?", tql_query, re.IGNORECASE)
            if match:
                jql["find"] = match.group(1)
                if match.group(2):
                    jql["filter"] = await self._condition_to_jql_filter(match.group(2))
                if match.group(3):
                    direction = -1 if match.group(4) == "DESC" else 1
                    jql["sort"] = {match.group(3): direction}
                if match.group(5):
                    jql["limit"] = int(match.group(5))

        elif tql_query.startswith("INSERT"):
            match = re.match(r"INSERT INTO (\w+) \((.+)\) VALUES \((.+)\)", tql_query, re.IGNORECASE)
            if match:
                jql["insert"] = match.group(1)
                fields = [f.strip() for f in match.group(2).split(",")]
                values = [v.strip().strip("'\"") for v in match.group(3).split(",")]
                jql["documents"] = [dict(zip(fields, values))]

        elif tql_query.startswith("UPDATE"):
            match = re.match(r"UPDATE (\w+) SET (.+) WHERE (.+)", tql_query, re.IGNORECASE)
            if match:
                jql["update"] = match.group(1)
                jql["filter"] = await self._condition_to_jql_filter(match.group(3))
                jql["set"] = await self._parse_update_set(match.group(2))

        elif tql_query.startswith("DELETE"):
            match = re.match(r"DELETE FROM (\w+) WHERE (.+)", tql_query, re.IGNORECASE)
            if match:
                jql["delete"] = match.group(1)
                jql["filter"] = await self._condition_to_jql_filter(match.group(2))

        return {
            "query": json.dumps(jql, indent=2),
            "confidence": tql_result["confidence"] * 0.95,  # Slightly lower due to extra translation
            "explanation": f"Translated via TQL: {tql_result.get('explanation', '')}",
        }

    async def _translate_condition(self, condition: str) -> str:
        """Translate a natural language condition to SQL-like condition."""
        for pattern, template in self.CONDITION_PATTERNS:
            match = re.match(pattern, condition, re.IGNORECASE)
            if match:
                field = match.group(1)
                value = match.group(2) if len(match.groups()) > 1 else ""
                return template.format(field=field, value=value)

        # Return as-is if no pattern matches
        return condition

    async def _parse_insert_values(self, values_str: str) -> dict:
        """Parse insert values from natural language."""
        result = {}

        # Pattern: field "value" or field value
        pattern = r'(\w+)\s+["\']?([^,"\']+)["\']?'
        matches = re.findall(pattern, values_str)

        for field, value in matches:
            # Try to convert to appropriate type
            try:
                if value.lower() in ("true", "false"):
                    result[field] = value.lower() == "true"
                elif value.isdigit():
                    result[field] = int(value)
                elif re.match(r'^\d+\.\d+$', value):
                    result[field] = float(value)
                else:
                    result[field] = value
            except ValueError:
                result[field] = value

        return result

    async def _condition_to_jql_filter(self, condition: str) -> dict:
        """Convert SQL-like condition to JQL filter."""
        filter_dict = {}

        # Simple equality
        eq_match = re.match(r"(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?", condition)
        if eq_match:
            field, value = eq_match.groups()
            filter_dict[field] = value
            return filter_dict

        # Greater than
        gt_match = re.match(r"(\w+)\s*>\s*(\d+)", condition)
        if gt_match:
            field, value = gt_match.groups()
            filter_dict[field] = {"$gt": int(value)}
            return filter_dict

        # Less than
        lt_match = re.match(r"(\w+)\s*<\s*(\d+)", condition)
        if lt_match:
            field, value = lt_match.groups()
            filter_dict[field] = {"$lt": int(value)}
            return filter_dict

        return filter_dict

    async def _parse_update_set(self, set_clause: str) -> dict:
        """Parse UPDATE SET clause to dictionary."""
        result = {}
        pairs = set_clause.split(",")
        for pair in pairs:
            if "=" in pair:
                field, value = pair.split("=", 1)
                result[field.strip()] = value.strip().strip("'\"")
        return result

    async def analyze(
        self,
        text: str,
        analyze_entities: bool = True,
        analyze_sentiment: bool = True,
        extract_keywords: bool = True,
    ) -> dict:
        """
        Perform semantic analysis on text.

        Args:
            text: Text to analyze
            analyze_entities: Extract named entities
            analyze_sentiment: Analyze sentiment
            extract_keywords: Extract keywords

        Returns:
            Analysis results
        """
        result = {}

        if analyze_entities:
            result["entities"] = await self._extract_entities(text)

        if analyze_sentiment:
            result["sentiment"] = await self._analyze_sentiment(text)

        if extract_keywords:
            result["keywords"] = await self._extract_keywords(text)

        return result

    async def _extract_entities(self, text: str) -> list[dict]:
        """Extract named entities from text."""
        entities = []

        # Simple pattern-based entity extraction
        # Email
        emails = re.findall(r'\b[\w.-]+@[\w.-]+\.\w+\b', text)
        for email in emails:
            entities.append({"type": "EMAIL", "value": email})

        # Phone numbers
        phones = re.findall(r'\b\d{3}[-.]?\d{3}[-.]?\d{4}\b', text)
        for phone in phones:
            entities.append({"type": "PHONE", "value": phone})

        # URLs
        urls = re.findall(r'https?://\S+', text)
        for url in urls:
            entities.append({"type": "URL", "value": url})

        # Dates
        dates = re.findall(r'\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b', text)
        for date in dates:
            entities.append({"type": "DATE", "value": date})

        # Numbers with units
        numbers = re.findall(r'\b\d+(?:\.\d+)?\s*(?:USD|EUR|GBP|%|kg|lb|mi|km)\b', text)
        for num in numbers:
            entities.append({"type": "QUANTITY", "value": num})

        return entities

    async def _analyze_sentiment(self, text: str) -> dict:
        """Analyze sentiment of text."""
        # Simple lexicon-based sentiment analysis
        positive_words = {
            "good", "great", "excellent", "amazing", "wonderful", "fantastic",
            "happy", "love", "best", "perfect", "awesome", "beautiful",
            "positive", "success", "successful", "win", "winning", "better",
        }

        negative_words = {
            "bad", "terrible", "awful", "horrible", "poor", "worst",
            "hate", "sad", "angry", "fail", "failed", "failure", "wrong",
            "negative", "problem", "issue", "error", "broken", "worse",
        }

        words = set(re.findall(r'\b\w+\b', text.lower()))

        pos_count = len(words & positive_words)
        neg_count = len(words & negative_words)

        total = pos_count + neg_count
        if total == 0:
            return {"label": "neutral", "score": 0.5, "positive": 0, "negative": 0}

        score = (pos_count - neg_count + total) / (2 * total)  # Normalize to 0-1

        if score > 0.6:
            label = "positive"
        elif score < 0.4:
            label = "negative"
        else:
            label = "neutral"

        return {
            "label": label,
            "score": score,
            "positive": pos_count,
            "negative": neg_count,
        }

    async def _extract_keywords(self, text: str) -> list[str]:
        """Extract keywords from text using TF-IDF-like scoring."""
        # Tokenize and clean
        words = re.findall(r'\b\w{3,}\b', text.lower())

        # Remove stopwords
        words = [w for w in words if w not in self._stopwords]

        # Count frequency
        freq = {}
        for word in words:
            freq[word] = freq.get(word, 0) + 1

        # Sort by frequency and return top keywords
        sorted_words = sorted(freq.items(), key=lambda x: x[1], reverse=True)
        return [word for word, count in sorted_words[:10]]

    async def suggest_completions(
        self,
        partial_query: str,
        collection: Optional[str] = None,
    ) -> list[str]:
        """
        Suggest query completions for partial input.

        Args:
            partial_query: Partial query string
            collection: Optional collection context

        Returns:
            List of suggested completions
        """
        suggestions = []
        partial_lower = partial_query.lower().strip()

        # Suggest based on partial input
        if partial_lower.startswith("find"):
            if collection:
                suggestions.extend([
                    f"find all {collection}",
                    f"find {collection} where",
                    f"find first 10 {collection}",
                ])
            else:
                suggestions.extend([
                    "find all [collection]",
                    "find [collection] where [field] equals [value]",
                    "find first 10 [collection] sorted by [field]",
                ])

        elif partial_lower.startswith("get"):
            suggestions.extend([
                "get [collection] where [field] equals [value]",
                "get first [n] [collection]",
                "get [collection] sorted by [field] descending",
            ])

        elif partial_lower.startswith("count"):
            suggestions.extend([
                "count all [collection]",
                "count [collection] where [condition]",
            ])

        elif partial_lower.startswith("add") or partial_lower.startswith("insert"):
            suggestions.extend([
                "add to [collection] [field] [value], [field] [value]",
                "insert into [collection] [field] [value]",
            ])

        elif partial_lower.startswith("update"):
            suggestions.extend([
                "update [collection] set [field] to [value] where [condition]",
            ])

        elif partial_lower.startswith("delete") or partial_lower.startswith("remove"):
            suggestions.extend([
                "delete from [collection] where [condition]",
                "remove [collection] where [condition]",
            ])

        else:
            # General suggestions
            suggestions.extend([
                "find all [collection]",
                "count [collection]",
                "add to [collection] [field] [value]",
                "get [collection] where [field] equals [value]",
            ])

        return suggestions[:5]
