"""
Query Executor - Executes optimized query plans against the database.

This module implements:
- Plan execution engine
- Result formatting
- Streaming results
- Execution monitoring
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, AsyncIterator, Callable, Union
from enum import Enum
from datetime import datetime
import asyncio
import time


class ExecutionStatus(Enum):
    """Status of query execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ExecutionResult:
    """Result of query execution."""
    success: bool
    data: Any
    row_count: int
    execution_time_ms: float
    status: ExecutionStatus
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {
            "success": self.success,
            "data": self.data if not isinstance(self.data, (list, dict)) else self.data,
            "row_count": self.row_count,
            "execution_time_ms": self.execution_time_ms,
            "status": self.status.value,
            "error": self.error,
            "warnings": self.warnings,
            "metadata": self.metadata,
        }


@dataclass
class ExecutionContext:
    """Context for query execution."""
    plan: Any
    schema: Optional[Dict]
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout_ms: int = 30000
    max_rows: int = 10000
    include_metadata: bool = True


class QueryExecutor:
    """
    Executes query plans against the database.

    Features:
    - Step-by-step execution
    - Result streaming
    - Timeout handling
    - Progress reporting
    """

    def __init__(self, db_client: Any, config: Any):
        self.db = db_client
        self.config = config
        self.active_executions: Dict[str, "ExecutionState"] = {}

    async def execute(
        self,
        ctx: ExecutionContext,
        progress_callback: Optional[Callable[[float, str], None]] = None,
    ) -> ExecutionResult:
        """
        Execute a query plan.

        Args:
            ctx: Execution context with plan and parameters
            progress_callback: Optional callback for progress updates

        Returns:
            Execution result
        """
        start_time = time.time()
        execution_id = f"exec_{int(start_time * 1000)}"

        state = ExecutionState(
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(),
        )
        self.active_executions[execution_id] = state

        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                self._execute_plan(ctx, state, progress_callback),
                timeout=ctx.timeout_ms / 1000.0,
            )

            elapsed_ms = (time.time() - start_time) * 1000
            state.status = ExecutionStatus.COMPLETED

            return ExecutionResult(
                success=True,
                data=result,
                row_count=len(result) if isinstance(result, list) else 1,
                execution_time_ms=elapsed_ms,
                status=ExecutionStatus.COMPLETED,
                metadata=state.metadata,
            )

        except asyncio.TimeoutError:
            state.status = ExecutionStatus.FAILED
            elapsed_ms = (time.time() - start_time) * 1000

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=elapsed_ms,
                status=ExecutionStatus.FAILED,
                error=f"Query timeout after {ctx.timeout_ms}ms",
            )

        except Exception as e:
            state.status = ExecutionStatus.FAILED
            elapsed_ms = (time.time() - start_time) * 1000

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=elapsed_ms,
                status=ExecutionStatus.FAILED,
                error=str(e),
            )

        finally:
            self.active_executions.pop(execution_id, None)

    async def _execute_plan(
        self,
        ctx: ExecutionContext,
        state: "ExecutionState",
        progress_callback: Optional[Callable],
    ) -> Any:
        """Execute plan steps."""
        plan = ctx.plan
        steps = getattr(plan, 'steps', [])
        total_steps = len(steps)

        # Initial data source
        data = None
        collection = None

        for i, step in enumerate(steps):
            if progress_callback:
                progress = (i / total_steps) * 100
                progress_callback(progress, f"Executing step {i+1}/{total_steps}")

            operation = getattr(step, 'operation', str(step))
            details = getattr(step, 'details', {})

            # Execute step based on operation type
            if operation == "scan":
                collection = details.get("collection")
                data = await self._execute_scan(collection)

            elif operation == "index_scan":
                collection = details.get("collection")
                index = details.get("index")
                conditions = details.get("conditions", [])
                data = await self._execute_index_scan(collection, index, conditions)

            elif operation == "filter":
                conditions = details.get("conditions", [])
                data = self._execute_filter(data, conditions)

            elif operation == "project":
                fields = details.get("fields", [])
                data = self._execute_project(data, fields)

            elif operation == "sort":
                fields = details.get("fields", [])
                order = details.get("order", "asc")
                data = self._execute_sort(data, fields, order)

            elif operation == "group":
                fields = details.get("fields", [])
                aggregates = details.get("aggregates", [])
                data = self._execute_group(data, fields, aggregates)

            elif operation == "aggregate":
                function = details.get("function")
                field_name = details.get("field")
                data = self._execute_aggregate(data, function, field_name)

            elif operation == "limit":
                limit = details.get("limit", ctx.max_rows)
                offset = details.get("offset", 0)
                data = self._execute_limit(data, limit, offset)

            elif operation == "join":
                join_type = details.get("type", "inner")
                right_collection = details.get("collection")
                on = details.get("on")
                data = await self._execute_join(data, right_collection, on, join_type)

            elif operation == "distinct":
                fields = details.get("fields", [])
                data = self._execute_distinct(data, fields)

            # Update state
            state.rows_processed = len(data) if isinstance(data, list) else 1
            state.current_step = i + 1

        # Apply row limit
        if isinstance(data, list) and len(data) > ctx.max_rows:
            data = data[:ctx.max_rows]
            state.metadata["truncated"] = True
            state.metadata["total_rows"] = len(data)

        if progress_callback:
            progress_callback(100, "Execution complete")

        return data

    async def _execute_scan(self, collection: str) -> List[Dict]:
        """Execute full table scan."""
        if self.db is None:
            return []

        # In real implementation, would use db client
        # return await self.db.scan(collection)
        return []

    async def _execute_index_scan(
        self,
        collection: str,
        index: str,
        conditions: List[Dict],
    ) -> List[Dict]:
        """Execute index scan."""
        if self.db is None:
            return []

        # In real implementation, would use index
        # return await self.db.index_scan(collection, index, conditions)
        return []

    def _execute_filter(
        self,
        data: Optional[List[Dict]],
        conditions: List[Dict],
    ) -> List[Dict]:
        """Execute filter operation."""
        if not data:
            return []

        result = []
        for record in data:
            if self._matches_conditions(record, conditions):
                result.append(record)

        return result

    def _matches_conditions(
        self,
        record: Dict,
        conditions: List[Dict],
    ) -> bool:
        """Check if record matches all conditions."""
        for cond in conditions:
            field = cond.get("field")
            op = cond.get("operator", "=")
            value = cond.get("value")

            record_value = record.get(field)

            if not self._compare(record_value, op, value):
                return False

        return True

    def _compare(self, left: Any, op: str, right: Any) -> bool:
        """Compare values with operator."""
        if left is None:
            return op == "is_null"

        try:
            if op in ("=", "==", "eq"):
                return left == right
            elif op in ("!=", "<>", "ne"):
                return left != right
            elif op in ("<", "lt"):
                return left < right
            elif op in ("<=", "lte"):
                return left <= right
            elif op in (">", "gt"):
                return left > right
            elif op in (">=", "gte"):
                return left >= right
            elif op == "in":
                return left in right
            elif op == "not_in":
                return left not in right
            elif op == "contains":
                return right in str(left)
            elif op == "starts_with":
                return str(left).startswith(str(right))
            elif op == "ends_with":
                return str(left).endswith(str(right))
            elif op == "is_null":
                return left is None
            elif op == "is_not_null":
                return left is not None
            elif op == "between":
                return right[0] <= left <= right[1]
        except (TypeError, ValueError):
            return False

        return False

    def _execute_project(
        self,
        data: Optional[List[Dict]],
        fields: List[str],
    ) -> List[Dict]:
        """Execute projection (select specific fields)."""
        if not data or not fields:
            return data or []

        if "*" in fields:
            return data

        result = []
        for record in data:
            projected = {}
            for field in fields:
                if field in record:
                    projected[field] = record[field]
            result.append(projected)

        return result

    def _execute_sort(
        self,
        data: Optional[List[Dict]],
        fields: List[str],
        order: str = "asc",
    ) -> List[Dict]:
        """Execute sort operation."""
        if not data or not fields:
            return data or []

        reverse = order.lower() == "desc"

        def sort_key(record: Dict):
            values = []
            for field in fields:
                val = record.get(field)
                # Handle None values
                if val is None:
                    values.append((1, None))  # Nulls last
                else:
                    values.append((0, val))
            return values

        return sorted(data, key=sort_key, reverse=reverse)

    def _execute_group(
        self,
        data: Optional[List[Dict]],
        fields: List[str],
        aggregates: List[Dict],
    ) -> List[Dict]:
        """Execute group by operation."""
        if not data:
            return []

        # Group records
        groups: Dict[tuple, List[Dict]] = {}
        for record in data:
            key = tuple(record.get(f) for f in fields)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        # Apply aggregates to each group
        result = []
        for key, group_records in groups.items():
            row = {}

            # Add group key fields
            for i, field in enumerate(fields):
                row[field] = key[i]

            # Apply aggregates
            for agg in aggregates:
                agg_func = agg.get("function", "count")
                agg_field = agg.get("field")
                alias = agg.get("alias", f"{agg_func}_{agg_field or 'all'}")

                row[alias] = self._apply_aggregate(group_records, agg_func, agg_field)

            result.append(row)

        return result

    def _execute_aggregate(
        self,
        data: Optional[List[Dict]],
        function: str,
        field_name: Optional[str],
    ) -> Any:
        """Execute single aggregate operation."""
        if not data:
            return None

        return self._apply_aggregate(data, function, field_name)

    def _apply_aggregate(
        self,
        records: List[Dict],
        function: str,
        field_name: Optional[str],
    ) -> Any:
        """Apply aggregate function to records."""
        if function == "count":
            if field_name:
                return sum(1 for r in records if r.get(field_name) is not None)
            return len(records)

        if not field_name:
            return None

        values = [r.get(field_name) for r in records if r.get(field_name) is not None]

        if not values:
            return None

        if function == "sum":
            return sum(values)
        elif function == "avg":
            return sum(values) / len(values)
        elif function == "min":
            return min(values)
        elif function == "max":
            return max(values)
        elif function == "first":
            return values[0] if values else None
        elif function == "last":
            return values[-1] if values else None

        return None

    def _execute_limit(
        self,
        data: Optional[List[Dict]],
        limit: int,
        offset: int = 0,
    ) -> List[Dict]:
        """Execute limit/offset operation."""
        if not data:
            return []

        return data[offset:offset + limit]

    async def _execute_join(
        self,
        left_data: Optional[List[Dict]],
        right_collection: str,
        on: Dict,
        join_type: str = "inner",
    ) -> List[Dict]:
        """Execute join operation."""
        if not left_data:
            return []

        # Get right side data
        right_data = await self._execute_scan(right_collection)
        if not right_data:
            if join_type == "inner":
                return []
            return left_data

        left_field = on.get("left")
        right_field = on.get("right")

        # Build hash table for right side
        right_index: Dict[Any, List[Dict]] = {}
        for record in right_data:
            key = record.get(right_field)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(record)

        result = []
        for left_record in left_data:
            left_key = left_record.get(left_field)
            right_matches = right_index.get(left_key, [])

            if right_matches:
                for right_record in right_matches:
                    merged = {**left_record, **right_record}
                    result.append(merged)
            elif join_type == "left":
                result.append(left_record)

        return result

    def _execute_distinct(
        self,
        data: Optional[List[Dict]],
        fields: List[str],
    ) -> List[Dict]:
        """Execute distinct operation."""
        if not data:
            return []

        seen = set()
        result = []

        for record in data:
            if fields:
                key = tuple(record.get(f) for f in fields)
            else:
                key = tuple(sorted(record.items()))

            if key not in seen:
                seen.add(key)
                result.append(record)

        return result

    async def stream_execute(
        self,
        ctx: ExecutionContext,
        batch_size: int = 100,
    ) -> AsyncIterator[List[Dict]]:
        """
        Execute query with streaming results.

        Yields batches of results for large queries.
        """
        result = await self.execute(ctx)

        if not result.success or not isinstance(result.data, list):
            yield result.data if result.data else []
            return

        data = result.data
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    def cancel(self, execution_id: str) -> bool:
        """Cancel a running execution."""
        if execution_id in self.active_executions:
            state = self.active_executions[execution_id]
            state.status = ExecutionStatus.CANCELLED
            return True
        return False

    def get_active_executions(self) -> List[Dict]:
        """Get list of active executions."""
        return [
            {
                "id": state.execution_id,
                "status": state.status.value,
                "started_at": state.started_at.isoformat(),
                "rows_processed": state.rows_processed,
                "current_step": state.current_step,
            }
            for state in self.active_executions.values()
        ]


@dataclass
class ExecutionState:
    """State of a running execution."""
    execution_id: str
    status: ExecutionStatus
    started_at: datetime
    rows_processed: int = 0
    current_step: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class ResultFormatter:
    """
    Formats query results for different output formats.
    """

    @staticmethod
    def to_table(data: List[Dict], max_width: int = 40) -> str:
        """Format as ASCII table."""
        if not data:
            return "(no results)"

        # Get columns
        columns = list(data[0].keys()) if data else []

        # Calculate column widths
        widths = {}
        for col in columns:
            max_val_width = max(len(str(row.get(col, ""))) for row in data)
            widths[col] = min(max(len(col), max_val_width), max_width)

        # Build header
        header = " | ".join(col.ljust(widths[col]) for col in columns)
        separator = "-+-".join("-" * widths[col] for col in columns)

        # Build rows
        rows = []
        for row in data:
            formatted = []
            for col in columns:
                val = str(row.get(col, ""))
                if len(val) > max_width:
                    val = val[:max_width-3] + "..."
                formatted.append(val.ljust(widths[col]))
            rows.append(" | ".join(formatted))

        return f"{header}\n{separator}\n" + "\n".join(rows)

    @staticmethod
    def to_json(data: Any, pretty: bool = True) -> str:
        """Format as JSON."""
        import json
        return json.dumps(data, indent=2 if pretty else None, default=str)

    @staticmethod
    def to_csv(data: List[Dict]) -> str:
        """Format as CSV."""
        if not data:
            return ""

        import csv
        import io

        output = io.StringIO()
        columns = list(data[0].keys())
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()

    @staticmethod
    def to_markdown(data: List[Dict]) -> str:
        """Format as Markdown table."""
        if not data:
            return "*No results*"

        columns = list(data[0].keys())

        # Header
        header = "| " + " | ".join(columns) + " |"
        separator = "|" + "|".join("---" for _ in columns) + "|"

        # Rows
        rows = []
        for row in data:
            values = [str(row.get(col, "")).replace("|", "\\|") for col in columns]
            rows.append("| " + " | ".join(values) + " |")

        return f"{header}\n{separator}\n" + "\n".join(rows)

    @staticmethod
    def summarize(result: ExecutionResult) -> str:
        """Generate human-readable summary of result."""
        if not result.success:
            return f"Query failed: {result.error}"

        summary = []
        summary.append(f"Found {result.row_count} results in {result.execution_time_ms:.2f}ms")

        if result.warnings:
            summary.append(f"Warnings: {', '.join(result.warnings)}")

        if result.metadata.get("truncated"):
            summary.append(f"Results truncated to {result.row_count} rows")

        return "\n".join(summary)
