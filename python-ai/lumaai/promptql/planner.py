"""
Query Planner - Breaks down natural language queries into executable plans.

The planner uses AI to:
- Understand query intent
- Identify required data sources
- Plan execution steps
- Detect dependencies between steps
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum


class StepType(Enum):
    """Types of query execution steps."""
    FETCH = "fetch"           # Retrieve data
    FILTER = "filter"         # Apply conditions
    JOIN = "join"             # Join data sources
    AGGREGATE = "aggregate"   # Aggregation (sum, avg, etc.)
    SORT = "sort"             # Sort results
    LIMIT = "limit"           # Limit results
    TRANSFORM = "transform"   # Transform data
    COMPUTE = "compute"       # Compute derived values
    COMPARE = "compare"       # Compare datasets
    SUBQUERY = "subquery"     # Nested query


class AggregationType(Enum):
    """Aggregation functions."""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STDDEV = "stddev"
    PERCENTILE = "percentile"
    DISTINCT = "distinct"


@dataclass
class QueryStep:
    """A single step in the query execution plan."""
    id: str
    type: StepType
    description: str
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    estimated_rows: Optional[int] = None
    estimated_cost: float = 1.0

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "type": self.type.value,
            "description": self.description,
            "params": self.params,
            "dependencies": self.dependencies,
            "estimated_rows": self.estimated_rows,
            "estimated_cost": self.estimated_cost,
        }


@dataclass
class QueryPlan:
    """Complete query execution plan."""
    query: str
    understanding: str
    steps: List[QueryStep]
    requires_reasoning: bool = False
    can_parallelize: bool = False
    estimated_cost: float = 0.0
    optimizations: List[str] = field(default_factory=list)
    target_collections: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return {
            "query": self.query,
            "understanding": self.understanding,
            "steps": [s.to_dict() for s in self.steps],
            "requires_reasoning": self.requires_reasoning,
            "can_parallelize": self.can_parallelize,
            "estimated_cost": self.estimated_cost,
            "optimizations": self.optimizations,
        }


class QueryPlanner:
    """
    AI-powered query planner that converts natural language
    to executable query plans.
    """

    # Intent patterns
    INTENT_PATTERNS = {
        "retrieve": [
            r"\b(show|get|find|list|display|fetch|retrieve|select)\b",
            r"\b(what|which|who)\b.*\?",
        ],
        "count": [
            r"\b(count|how many|number of|total)\b",
        ],
        "aggregate": [
            r"\b(sum|total|average|avg|mean|min|max|median)\b",
            r"\b(aggregate|summarize|statistics)\b",
        ],
        "compare": [
            r"\b(compare|versus|vs|difference|between)\b",
            r"\b(more than|less than|greater|smaller)\b.*\b(average|mean)\b",
        ],
        "trend": [
            r"\b(trend|over time|growth|change|progression)\b",
            r"\b(daily|weekly|monthly|yearly)\b.*\b(pattern|trend)\b",
        ],
        "group": [
            r"\b(group by|grouped|per|by each|breakdown)\b",
            r"\b(categorize|segment|partition)\b",
        ],
        "filter": [
            r"\b(where|filter|only|just|exclude|except)\b",
            r"\b(last|past|recent|since|before|after)\b",
        ],
        "join": [
            r"\b(join|combine|merge|with|related|associated)\b",
            r"\b(and their|along with|including)\b",
        ],
        "sort": [
            r"\b(sort|order|rank|top|bottom|highest|lowest)\b",
        ],
    }

    # Time expressions
    TIME_PATTERNS = {
        "today": ("now", "1d"),
        "yesterday": ("1d", "2d"),
        "this week": ("now", "1w"),
        "last week": ("1w", "2w"),
        "this month": ("now", "1M"),
        "last month": ("1M", "2M"),
        "this year": ("now", "1y"),
        "last year": ("1y", "2y"),
        "last 7 days": ("now", "7d"),
        "last 30 days": ("now", "30d"),
        "last 90 days": ("now", "90d"),
    }

    def __init__(self, config):
        self.config = config
        self._step_counter = 0

    async def create_plan(
        self,
        prompt: str,
        schema: Optional[Dict],
        context: Optional[Dict],
        mode: Any,
    ) -> QueryPlan:
        """
        Create an execution plan for a natural language query.

        Args:
            prompt: Natural language query
            schema: Database schema information
            context: Conversation context
            mode: Query execution mode

        Returns:
            QueryPlan with execution steps
        """
        self._step_counter = 0

        # Understand the query
        understanding = self._understand_query(prompt)

        # Detect intents
        intents = self._detect_intents(prompt)

        # Extract entities
        entities = self._extract_entities(prompt, schema)

        # Extract time constraints
        time_constraints = self._extract_time_constraints(prompt)

        # Build execution steps
        steps = self._build_steps(
            prompt, intents, entities, time_constraints, schema, context
        )

        # Determine if reasoning is required
        requires_reasoning = self._requires_reasoning(intents, steps)

        # Check if steps can be parallelized
        can_parallelize = self._can_parallelize(steps)

        # Calculate estimated cost
        estimated_cost = sum(s.estimated_cost for s in steps)

        # Get target collections
        target_collections = list(set(
            s.params.get("collection", "")
            for s in steps
            if s.params.get("collection")
        ))

        return QueryPlan(
            query=prompt,
            understanding=understanding,
            steps=steps,
            requires_reasoning=requires_reasoning,
            can_parallelize=can_parallelize,
            estimated_cost=estimated_cost,
            target_collections=target_collections,
        )

    async def suggest_completions(
        self,
        partial: str,
        schema: Optional[Dict],
        context: Optional[Dict],
    ) -> List[str]:
        """Suggest query completions."""
        suggestions = []
        partial_lower = partial.lower().strip()

        # Collection-based suggestions
        if schema and "collections" in schema:
            for collection in schema["collections"]:
                if partial_lower.endswith("from ") or partial_lower.endswith("in "):
                    suggestions.append(f"{partial}{collection}")
                elif not partial_lower:
                    suggestions.append(f"Show me all {collection}")
                    suggestions.append(f"Count {collection}")

        # Intent-based suggestions
        intent_suggestions = {
            "show": ["Show me all [collection]", "Show top 10 [collection] by [field]"],
            "find": ["Find [collection] where [condition]", "Find all [collection]"],
            "count": ["Count [collection]", "How many [collection] are there?"],
            "compare": ["Compare [field] between [group1] and [group2]"],
            "trend": ["Show [field] trend over time", "What's the [field] pattern?"],
        }

        for intent, templates in intent_suggestions.items():
            if partial_lower.startswith(intent):
                suggestions.extend(templates)

        # Context-based suggestions
        if context and "last_query" in context:
            suggestions.append(f"Same as before but filtered by [condition]")
            suggestions.append(f"Group the previous results by [field]")

        return suggestions[:10]

    def _understand_query(self, prompt: str) -> str:
        """Generate human-readable understanding of the query."""
        intents = self._detect_intents(prompt)

        if "compare" in intents:
            return f"Comparing data based on: {prompt}"
        elif "aggregate" in intents:
            return f"Calculating aggregated metrics: {prompt}"
        elif "trend" in intents:
            return f"Analyzing trends over time: {prompt}"
        elif "count" in intents:
            return f"Counting records: {prompt}"
        elif "retrieve" in intents:
            return f"Retrieving data: {prompt}"
        else:
            return f"Processing query: {prompt}"

    def _detect_intents(self, prompt: str) -> List[str]:
        """Detect query intents from natural language."""
        prompt_lower = prompt.lower()
        detected = []

        for intent, patterns in self.INTENT_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, prompt_lower):
                    detected.append(intent)
                    break

        # Default to retrieve if no intent detected
        if not detected:
            detected.append("retrieve")

        return detected

    def _extract_entities(
        self,
        prompt: str,
        schema: Optional[Dict],
    ) -> Dict[str, List[str]]:
        """Extract entities (collections, fields, values) from prompt."""
        entities = {
            "collections": [],
            "fields": [],
            "values": [],
            "operators": [],
        }

        if not schema:
            return entities

        prompt_lower = prompt.lower()

        # Match collection names
        for collection in schema.get("collections", []):
            if collection.lower() in prompt_lower:
                entities["collections"].append(collection)
            # Also check singular/plural forms
            if collection.lower().rstrip("s") in prompt_lower:
                entities["collections"].append(collection)

        # Match field names
        for collection in schema.get("collections", []):
            fields = schema.get("fields", {}).get(collection, [])
            for field in fields:
                if field.lower() in prompt_lower:
                    entities["fields"].append(field)

        # Extract operators
        operator_patterns = [
            (r"greater than|more than|>|above", ">"),
            (r"less than|fewer than|<|below", "<"),
            (r"equals?|is|=|same as", "="),
            (r"not equals?|!=|different from", "!="),
            (r"contains|includes|like", "LIKE"),
            (r"between", "BETWEEN"),
            (r"in|one of", "IN"),
        ]

        for pattern, op in operator_patterns:
            if re.search(pattern, prompt_lower):
                entities["operators"].append(op)

        # Extract quoted values
        quoted = re.findall(r'"([^"]+)"|\'([^\']+)\'', prompt)
        for q in quoted:
            entities["values"].append(q[0] or q[1])

        # Extract numbers
        numbers = re.findall(r'\b(\d+(?:\.\d+)?)\b', prompt)
        entities["values"].extend(numbers)

        return entities

    def _extract_time_constraints(self, prompt: str) -> Optional[Dict]:
        """Extract time-based constraints from prompt."""
        prompt_lower = prompt.lower()

        for phrase, (start, end) in self.TIME_PATTERNS.items():
            if phrase in prompt_lower:
                return {"type": "relative", "start": start, "end": end, "phrase": phrase}

        # Check for absolute dates
        date_pattern = r'\b(\d{4}-\d{2}-\d{2})\b'
        dates = re.findall(date_pattern, prompt)
        if dates:
            return {"type": "absolute", "dates": dates}

        return None

    def _build_steps(
        self,
        prompt: str,
        intents: List[str],
        entities: Dict,
        time_constraints: Optional[Dict],
        schema: Optional[Dict],
        context: Optional[Dict],
    ) -> List[QueryStep]:
        """Build execution steps based on analysis."""
        steps = []

        # Determine primary collection
        collections = entities.get("collections", [])
        primary_collection = collections[0] if collections else self._infer_collection(prompt, schema)

        # Step 1: Fetch data
        fetch_step = self._create_step(
            StepType.FETCH,
            f"Fetch data from {primary_collection}",
            {"collection": primary_collection},
        )
        steps.append(fetch_step)

        # Step 2: Apply time constraints
        if time_constraints:
            time_step = self._create_step(
                StepType.FILTER,
                f"Apply time filter: {time_constraints.get('phrase', 'custom')}",
                {"time": time_constraints, "collection": primary_collection},
                dependencies=[fetch_step.id],
            )
            steps.append(time_step)

        # Step 3: Apply filters
        if "filter" in intents:
            filter_conditions = self._build_filter_conditions(prompt, entities)
            if filter_conditions:
                filter_step = self._create_step(
                    StepType.FILTER,
                    f"Apply filters: {filter_conditions}",
                    {"conditions": filter_conditions},
                    dependencies=[steps[-1].id],
                )
                steps.append(filter_step)

        # Step 4: Join related data
        if "join" in intents and len(collections) > 1:
            for secondary in collections[1:]:
                join_step = self._create_step(
                    StepType.JOIN,
                    f"Join with {secondary}",
                    {"collection": secondary, "type": "inner"},
                    dependencies=[steps[-1].id],
                )
                steps.append(join_step)

        # Step 5: Group data
        if "group" in intents:
            group_fields = self._extract_group_fields(prompt, entities)
            group_step = self._create_step(
                StepType.AGGREGATE,
                f"Group by {group_fields}",
                {"group_by": group_fields},
                dependencies=[steps[-1].id],
            )
            steps.append(group_step)

        # Step 6: Aggregate
        if "aggregate" in intents or "count" in intents:
            agg_func = self._detect_aggregation(prompt)
            agg_step = self._create_step(
                StepType.AGGREGATE,
                f"Calculate {agg_func.value}",
                {"function": agg_func.value},
                dependencies=[steps[-1].id],
            )
            steps.append(agg_step)

        # Step 7: Compare
        if "compare" in intents:
            compare_step = self._create_step(
                StepType.COMPARE,
                "Compare datasets",
                {"type": "diff"},
                dependencies=[steps[-1].id],
            )
            steps.append(compare_step)

        # Step 8: Sort
        if "sort" in intents:
            sort_info = self._extract_sort_info(prompt)
            sort_step = self._create_step(
                StepType.SORT,
                f"Sort by {sort_info['field']} {sort_info['order']}",
                sort_info,
                dependencies=[steps[-1].id],
            )
            steps.append(sort_step)

        # Step 9: Limit results
        limit = self._extract_limit(prompt)
        if limit:
            limit_step = self._create_step(
                StepType.LIMIT,
                f"Limit to {limit} results",
                {"limit": limit},
                dependencies=[steps[-1].id],
            )
            steps.append(limit_step)

        return steps

    def _create_step(
        self,
        step_type: StepType,
        description: str,
        params: Dict,
        dependencies: List[str] = None,
    ) -> QueryStep:
        """Create a query step with unique ID."""
        self._step_counter += 1
        return QueryStep(
            id=f"step_{self._step_counter}",
            type=step_type,
            description=description,
            params=params,
            dependencies=dependencies or [],
            estimated_cost=self._estimate_step_cost(step_type),
        )

    def _estimate_step_cost(self, step_type: StepType) -> float:
        """Estimate computational cost of a step."""
        costs = {
            StepType.FETCH: 1.0,
            StepType.FILTER: 0.5,
            StepType.JOIN: 3.0,
            StepType.AGGREGATE: 1.5,
            StepType.SORT: 1.0,
            StepType.LIMIT: 0.1,
            StepType.TRANSFORM: 0.5,
            StepType.COMPUTE: 1.0,
            StepType.COMPARE: 2.0,
            StepType.SUBQUERY: 2.0,
        }
        return costs.get(step_type, 1.0)

    def _infer_collection(self, prompt: str, schema: Optional[Dict]) -> str:
        """Infer collection from prompt when not explicit."""
        if not schema:
            return "default"

        prompt_lower = prompt.lower()
        collections = schema.get("collections", [])

        # Try fuzzy matching
        for collection in collections:
            # Check for partial matches
            if any(word in prompt_lower for word in collection.lower().split("_")):
                return collection

        return collections[0] if collections else "default"

    def _build_filter_conditions(
        self,
        prompt: str,
        entities: Dict,
    ) -> List[Dict]:
        """Build filter conditions from prompt."""
        conditions = []
        fields = entities.get("fields", [])
        operators = entities.get("operators", [])
        values = entities.get("values", [])

        # Simple heuristic: pair fields with operators and values
        for i, field in enumerate(fields):
            op = operators[i] if i < len(operators) else "="
            val = values[i] if i < len(values) else None
            if val is not None:
                conditions.append({
                    "field": field,
                    "operator": op,
                    "value": val,
                })

        return conditions

    def _detect_aggregation(self, prompt: str) -> AggregationType:
        """Detect aggregation type from prompt."""
        prompt_lower = prompt.lower()

        agg_patterns = [
            (r"\bcount\b|\bhow many\b", AggregationType.COUNT),
            (r"\bsum\b|\btotal\b", AggregationType.SUM),
            (r"\baverage\b|\bavg\b|\bmean\b", AggregationType.AVG),
            (r"\bmin\b|\bminimum\b|\blowest\b", AggregationType.MIN),
            (r"\bmax\b|\bmaximum\b|\bhighest\b", AggregationType.MAX),
            (r"\bmedian\b", AggregationType.MEDIAN),
            (r"\bstd\b|\bstandard deviation\b", AggregationType.STDDEV),
            (r"\bdistinct\b|\bunique\b", AggregationType.DISTINCT),
        ]

        for pattern, agg_type in agg_patterns:
            if re.search(pattern, prompt_lower):
                return agg_type

        return AggregationType.COUNT

    def _extract_group_fields(self, prompt: str, entities: Dict) -> List[str]:
        """Extract fields to group by."""
        # Look for "by" followed by field name
        match = re.search(r'\bby\s+(\w+)', prompt.lower())
        if match:
            return [match.group(1)]

        # Use first detected field
        fields = entities.get("fields", [])
        return fields[:1] if fields else ["id"]

    def _extract_sort_info(self, prompt: str) -> Dict:
        """Extract sorting information."""
        prompt_lower = prompt.lower()

        # Detect order
        if any(word in prompt_lower for word in ["top", "highest", "most", "desc"]):
            order = "DESC"
        elif any(word in prompt_lower for word in ["bottom", "lowest", "least", "asc"]):
            order = "ASC"
        else:
            order = "DESC"

        # Detect field
        match = re.search(r'\bby\s+(\w+)', prompt_lower)
        field = match.group(1) if match else "created_at"

        return {"field": field, "order": order}

    def _extract_limit(self, prompt: str) -> Optional[int]:
        """Extract result limit from prompt."""
        # Look for "top N" or "first N" patterns
        match = re.search(r'\b(top|first|limit)\s+(\d+)\b', prompt.lower())
        if match:
            return int(match.group(2))

        # Check for specific numbers
        if "top" in prompt.lower() or "first" in prompt.lower():
            numbers = re.findall(r'\b(\d+)\b', prompt)
            if numbers:
                return int(numbers[0])

        return None

    def _requires_reasoning(self, intents: List[str], steps: List[QueryStep]) -> bool:
        """Determine if query requires multi-step reasoning."""
        # Complex intents require reasoning
        complex_intents = {"compare", "trend", "join"}
        if any(intent in complex_intents for intent in intents):
            return True

        # Many steps require reasoning
        if len(steps) > 5:
            return True

        # Subqueries require reasoning
        if any(s.type == StepType.SUBQUERY for s in steps):
            return True

        return False

    def _can_parallelize(self, steps: List[QueryStep]) -> bool:
        """Check if steps can be parallelized."""
        # Find steps with same dependencies
        dep_groups: Dict[str, List[str]] = {}
        for step in steps:
            dep_key = ",".join(sorted(step.dependencies))
            if dep_key not in dep_groups:
                dep_groups[dep_key] = []
            dep_groups[dep_key].append(step.id)

        # Can parallelize if any group has multiple steps
        return any(len(group) > 1 for group in dep_groups.values())
