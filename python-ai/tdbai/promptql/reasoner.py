"""
Multi-Step Reasoner - Handles complex queries requiring chain-of-thought reasoning.

This module implements:
- Chain-of-thought reasoning
- Query decomposition
- Intermediate result handling
- Reasoning validation
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from enum import Enum
import re


class ReasoningStepType(Enum):
    """Types of reasoning steps."""
    DECOMPOSE = "decompose"         # Break down complex query
    CLARIFY = "clarify"             # Clarify ambiguous terms
    LOOKUP = "lookup"               # Look up reference data
    COMPUTE = "compute"             # Perform calculation
    COMPARE = "compare"             # Compare values
    INFER = "infer"                 # Make inference
    VALIDATE = "validate"           # Validate result
    SYNTHESIZE = "synthesize"       # Combine results


@dataclass
class ReasoningStep:
    """A single step in the reasoning chain."""
    step_number: int
    step_type: ReasoningStepType
    thought: str
    action: str
    result: Any = None
    confidence: float = 1.0

    def to_dict(self) -> Dict:
        return {
            "step": self.step_number,
            "type": self.step_type.value,
            "thought": self.thought,
            "action": self.action,
            "result": str(self.result) if self.result else None,
            "confidence": self.confidence,
        }


@dataclass
class ReasoningChain:
    """Complete chain of reasoning steps."""
    query: str
    steps: List[ReasoningStep] = field(default_factory=list)
    final_answer: Any = None
    confidence: float = 1.0

    def add_step(self, step: ReasoningStep):
        self.steps.append(step)
        # Update overall confidence
        self.confidence = min(self.confidence, step.confidence)

    def to_list(self) -> List[str]:
        """Convert to list of reasoning strings."""
        return [
            f"Step {s.step_number}: {s.thought} -> {s.action}"
            for s in self.steps
        ]


class MultiStepReasoner:
    """
    Performs multi-step reasoning for complex queries.

    Uses chain-of-thought prompting to break down complex
    queries into manageable reasoning steps.
    """

    # Reasoning templates
    REASONING_TEMPLATES = {
        "compare": [
            "First, I need to understand what we're comparing: {entities}",
            "Then, I'll fetch data for each comparison group",
            "Next, I'll calculate the metrics for comparison",
            "Finally, I'll present the comparison results",
        ],
        "trend": [
            "First, I need to identify the time range: {time_range}",
            "Then, I'll group data by time intervals",
            "Next, I'll calculate the metric for each interval",
            "Finally, I'll identify the trend pattern",
        ],
        "complex_filter": [
            "First, I need to understand all filter conditions",
            "Then, I'll identify which conditions can be applied first (most selective)",
            "Next, I'll apply filters in optimal order",
            "Finally, I'll validate the filtered results",
        ],
        "aggregation": [
            "First, I need to identify what to aggregate: {field}",
            "Then, I'll determine the aggregation function: {function}",
            "Next, I'll identify any grouping requirements",
            "Finally, I'll compute and return the aggregated result",
        ],
        "subquery": [
            "First, I need to identify the inner query requirement",
            "Then, I'll execute the inner query",
            "Next, I'll use the inner query result in the outer query",
            "Finally, I'll combine results appropriately",
        ],
    }

    def __init__(self, config):
        self.config = config
        self.max_steps = config.max_reasoning_steps

    async def reason(
        self,
        prompt: str,
        plan: Any,
        schema: Optional[Dict],
        max_steps: int = 10,
    ) -> List[str]:
        """
        Perform multi-step reasoning for a query.

        Args:
            prompt: Original query
            plan: Query plan
            schema: Database schema
            max_steps: Maximum reasoning steps

        Returns:
            List of reasoning steps as strings
        """
        chain = ReasoningChain(query=prompt)

        # Step 1: Decompose the query
        decomposition = self._decompose_query(prompt, schema)
        chain.add_step(ReasoningStep(
            step_number=1,
            step_type=ReasoningStepType.DECOMPOSE,
            thought="Breaking down the query into components",
            action=f"Identified {len(decomposition)} sub-queries",
            result=decomposition,
            confidence=0.9,
        ))

        # Step 2: Clarify any ambiguities
        ambiguities = self._identify_ambiguities(prompt, schema)
        if ambiguities:
            resolutions = self._resolve_ambiguities(ambiguities, schema)
            chain.add_step(ReasoningStep(
                step_number=2,
                step_type=ReasoningStepType.CLARIFY,
                thought=f"Found {len(ambiguities)} ambiguous terms",
                action=f"Resolved: {resolutions}",
                result=resolutions,
                confidence=0.8,
            ))

        # Step 3: Determine reasoning strategy
        strategy = self._select_reasoning_strategy(prompt, plan)
        chain.add_step(ReasoningStep(
            step_number=len(chain.steps) + 1,
            step_type=ReasoningStepType.INFER,
            thought=f"Selected reasoning strategy: {strategy}",
            action="Apply template-based reasoning",
            confidence=0.95,
        ))

        # Step 4: Apply reasoning template
        template_steps = self._apply_reasoning_template(strategy, prompt, schema)
        for i, step_text in enumerate(template_steps):
            chain.add_step(ReasoningStep(
                step_number=len(chain.steps) + 1,
                step_type=ReasoningStepType.COMPUTE,
                thought=step_text,
                action="Execute reasoning step",
                confidence=0.9,
            ))

            if len(chain.steps) >= max_steps:
                break

        # Step 5: Synthesize final answer
        chain.add_step(ReasoningStep(
            step_number=len(chain.steps) + 1,
            step_type=ReasoningStepType.SYNTHESIZE,
            thought="Combining all reasoning steps",
            action="Generate final query execution plan",
            confidence=chain.confidence,
        ))

        # Step 6: Validate reasoning
        validation = self._validate_reasoning(chain, schema)
        chain.add_step(ReasoningStep(
            step_number=len(chain.steps) + 1,
            step_type=ReasoningStepType.VALIDATE,
            thought="Validating reasoning chain",
            action=f"Validation: {validation['status']}",
            result=validation,
            confidence=validation.get("confidence", 0.9),
        ))

        return chain.to_list()

    def _decompose_query(
        self,
        prompt: str,
        schema: Optional[Dict],
    ) -> List[Dict]:
        """Decompose complex query into sub-queries."""
        sub_queries = []

        # Check for multiple questions
        questions = re.split(r'\band\b|\bthen\b|\balso\b', prompt.lower())
        for q in questions:
            q = q.strip()
            if q:
                sub_queries.append({
                    "text": q,
                    "type": self._classify_sub_query(q),
                })

        # Check for nested conditions
        if "who" in prompt.lower() and ("more than" in prompt.lower() or "less than" in prompt.lower()):
            # This is a subquery pattern
            sub_queries.append({
                "text": "Calculate reference value",
                "type": "subquery",
            })

        return sub_queries if sub_queries else [{"text": prompt, "type": "simple"}]

    def _classify_sub_query(self, query: str) -> str:
        """Classify sub-query type."""
        query_lower = query.lower()

        if any(word in query_lower for word in ["count", "how many", "number"]):
            return "count"
        elif any(word in query_lower for word in ["average", "avg", "mean", "sum", "total"]):
            return "aggregation"
        elif any(word in query_lower for word in ["compare", "versus", "vs", "difference"]):
            return "comparison"
        elif any(word in query_lower for word in ["trend", "over time", "growth"]):
            return "trend"
        elif any(word in query_lower for word in ["top", "bottom", "highest", "lowest"]):
            return "ranking"
        else:
            return "filter"

    def _identify_ambiguities(
        self,
        prompt: str,
        schema: Optional[Dict],
    ) -> List[Dict]:
        """Identify ambiguous terms in the query."""
        ambiguities = []
        prompt_lower = prompt.lower()

        # Check for ambiguous pronouns
        pronouns = ["it", "they", "them", "these", "those", "this", "that"]
        for pronoun in pronouns:
            if f" {pronoun} " in prompt_lower:
                ambiguities.append({
                    "term": pronoun,
                    "type": "pronoun",
                    "context": self._get_context(prompt, pronoun),
                })

        # Check for ambiguous field references
        if schema:
            similar_fields = self._find_similar_fields(prompt, schema)
            for field_group in similar_fields:
                if len(field_group) > 1:
                    ambiguities.append({
                        "term": field_group[0],
                        "type": "field",
                        "alternatives": field_group,
                    })

        # Check for ambiguous time references
        time_ambiguities = ["recently", "a while ago", "some time", "previously"]
        for term in time_ambiguities:
            if term in prompt_lower:
                ambiguities.append({
                    "term": term,
                    "type": "time",
                })

        return ambiguities

    def _resolve_ambiguities(
        self,
        ambiguities: List[Dict],
        schema: Optional[Dict],
    ) -> Dict[str, str]:
        """Resolve identified ambiguities."""
        resolutions = {}

        for amb in ambiguities:
            if amb["type"] == "pronoun":
                # Resolve pronoun based on context
                resolutions[amb["term"]] = amb.get("context", "the data")

            elif amb["type"] == "field":
                # Choose most likely field based on context
                alternatives = amb.get("alternatives", [])
                resolutions[amb["term"]] = alternatives[0] if alternatives else amb["term"]

            elif amb["type"] == "time":
                # Resolve ambiguous time to default
                time_defaults = {
                    "recently": "last 7 days",
                    "a while ago": "last 30 days",
                    "some time": "last 90 days",
                    "previously": "before now",
                }
                resolutions[amb["term"]] = time_defaults.get(amb["term"], "last 30 days")

        return resolutions

    def _get_context(self, prompt: str, term: str) -> str:
        """Get context around a term."""
        # Find surrounding words
        words = prompt.lower().split()
        try:
            idx = words.index(term)
            start = max(0, idx - 3)
            end = min(len(words), idx + 3)
            return " ".join(words[start:end])
        except ValueError:
            return prompt[:50]

    def _find_similar_fields(
        self,
        prompt: str,
        schema: Dict,
    ) -> List[List[str]]:
        """Find potentially ambiguous similar field names."""
        similar_groups = []
        all_fields = []

        for collection, fields in schema.get("fields", {}).items():
            all_fields.extend(fields)

        # Group fields by similarity
        for field in all_fields:
            field_lower = field.lower()
            similar = [f for f in all_fields if f != field and (
                field_lower in f.lower() or
                f.lower() in field_lower or
                self._levenshtein_similar(field_lower, f.lower())
            )]
            if similar:
                group = [field] + similar
                if sorted(group) not in [sorted(g) for g in similar_groups]:
                    similar_groups.append(group)

        return similar_groups

    def _levenshtein_similar(self, s1: str, s2: str, threshold: float = 0.8) -> bool:
        """Check if two strings are similar using Levenshtein distance."""
        if len(s1) < 3 or len(s2) < 3:
            return False

        # Simple similarity check
        common = sum(c1 == c2 for c1, c2 in zip(s1, s2))
        max_len = max(len(s1), len(s2))
        return common / max_len >= threshold

    def _select_reasoning_strategy(self, prompt: str, plan: Any) -> str:
        """Select the best reasoning strategy."""
        prompt_lower = prompt.lower()

        if "compare" in prompt_lower or "versus" in prompt_lower:
            return "compare"
        elif "trend" in prompt_lower or "over time" in prompt_lower:
            return "trend"
        elif any(word in prompt_lower for word in ["average", "sum", "count", "total"]):
            return "aggregation"
        elif "than" in prompt_lower and "average" in prompt_lower:
            return "subquery"
        elif len(plan.steps) > 4:
            return "complex_filter"
        else:
            return "aggregation"

    def _apply_reasoning_template(
        self,
        strategy: str,
        prompt: str,
        schema: Optional[Dict],
    ) -> List[str]:
        """Apply reasoning template for the selected strategy."""
        template = self.REASONING_TEMPLATES.get(strategy, [])

        # Fill in template variables
        filled_steps = []
        for step in template:
            filled = step.format(
                entities=self._extract_entities_str(prompt),
                time_range=self._extract_time_str(prompt),
                field=self._extract_field_str(prompt, schema),
                function=self._extract_function_str(prompt),
            )
            filled_steps.append(filled)

        return filled_steps

    def _extract_entities_str(self, prompt: str) -> str:
        """Extract entities as string for template."""
        # Simple extraction of quoted strings and capitalized words
        quoted = re.findall(r'"([^"]+)"', prompt)
        if quoted:
            return ", ".join(quoted)

        # Find capitalized words (potential entities)
        caps = re.findall(r'\b([A-Z][a-z]+)\b', prompt)
        if caps:
            return ", ".join(set(caps))

        return "the data"

    def _extract_time_str(self, prompt: str) -> str:
        """Extract time reference as string."""
        time_patterns = [
            r"(last \d+ (?:days?|weeks?|months?|years?))",
            r"(this (?:week|month|year))",
            r"(since \w+)",
            r"(between .+ and .+)",
        ]

        for pattern in time_patterns:
            match = re.search(pattern, prompt.lower())
            if match:
                return match.group(1)

        return "the specified time period"

    def _extract_field_str(self, prompt: str, schema: Optional[Dict]) -> str:
        """Extract field reference as string."""
        if not schema:
            return "the requested field"

        prompt_lower = prompt.lower()
        for fields in schema.get("fields", {}).values():
            for field in fields:
                if field.lower() in prompt_lower:
                    return field

        return "the requested field"

    def _extract_function_str(self, prompt: str) -> str:
        """Extract aggregation function as string."""
        functions = {
            "average": "AVG",
            "avg": "AVG",
            "mean": "AVG",
            "sum": "SUM",
            "total": "SUM",
            "count": "COUNT",
            "minimum": "MIN",
            "maximum": "MAX",
            "min": "MIN",
            "max": "MAX",
        }

        prompt_lower = prompt.lower()
        for word, func in functions.items():
            if word in prompt_lower:
                return func

        return "COUNT"

    def _validate_reasoning(
        self,
        chain: ReasoningChain,
        schema: Optional[Dict],
    ) -> Dict:
        """Validate the reasoning chain."""
        issues = []

        # Check chain length
        if len(chain.steps) > self.max_steps:
            issues.append("Reasoning chain too long")

        # Check confidence
        if chain.confidence < 0.5:
            issues.append("Low confidence in reasoning")

        # Check for circular reasoning
        step_types = [s.step_type for s in chain.steps]
        if step_types.count(ReasoningStepType.DECOMPOSE) > 2:
            issues.append("Possible circular decomposition")

        # Check completeness
        required_types = {ReasoningStepType.DECOMPOSE, ReasoningStepType.SYNTHESIZE}
        present_types = set(step_types)
        if not required_types.issubset(present_types):
            issues.append("Missing required reasoning steps")

        return {
            "status": "valid" if not issues else "warning",
            "issues": issues,
            "confidence": 0.9 if not issues else 0.7,
            "step_count": len(chain.steps),
        }
