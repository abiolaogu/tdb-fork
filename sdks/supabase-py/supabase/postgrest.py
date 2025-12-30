"""
PostgrestClient - PostgREST-compatible query builder for Python.
"""

from typing import Any, Dict, List, Optional, TypeVar, Union
import httpx
from .types import PostgrestResponse, PostgrestSingleResponse, PostgrestError

T = TypeVar("T")


class PostgrestClient:
    """PostgrestClient for building database queries."""

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        schema: str = "public",
    ):
        self.url = url
        self.headers = headers or {}
        self.schema = schema
        self._client = httpx.AsyncClient()

    def from_(self, table: str) -> "PostgrestQueryBuilder":
        """Start a query on a table."""
        url = f"{self.url}/{table}"
        return PostgrestQueryBuilder(url, self.headers.copy(), self._client)

    async def rpc(
        self,
        fn: str,
        args: Optional[Dict[str, Any]] = None,
    ) -> PostgrestResponse:
        """Call a stored function (RPC)."""
        url = f"{self.url}/rpc/{fn}"
        response = await self._client.post(
            url,
            headers=self.headers,
            json=args or {},
        )
        return _parse_response(response)

    async def close(self):
        """Close the HTTP client."""
        await self._client.aclose()


class PostgrestQueryBuilder:
    """Query builder with chainable filter methods."""

    def __init__(
        self,
        url: str,
        headers: Dict[str, str],
        client: httpx.AsyncClient,
    ):
        self.url = url
        self.headers = headers
        self._client = client
        self._params: Dict[str, str] = {}
        self._method = "GET"
        self._body: Optional[Any] = None

    # =========================================================================
    # Query Methods
    # =========================================================================

    def select(
        self,
        columns: str = "*",
        *,
        count: Optional[str] = None,
    ) -> "PostgrestFilterBuilder":
        """SELECT query."""
        self._params["select"] = columns
        self._method = "GET"
        if count:
            self.headers["Prefer"] = f"count={count}"
        return PostgrestFilterBuilder(
            self.url, self.headers, self._params, self._method, None, self._client
        )

    def insert(
        self,
        values: Union[Dict[str, Any], List[Dict[str, Any]]],
        *,
        returning: str = "representation",
    ) -> "PostgrestFilterBuilder":
        """INSERT query."""
        self._method = "POST"
        self.headers["Prefer"] = f"return={returning}"
        self.headers["Content-Type"] = "application/json"
        return PostgrestFilterBuilder(
            self.url, self.headers, self._params, self._method, values, self._client
        )

    def update(
        self,
        values: Dict[str, Any],
        *,
        returning: str = "representation",
    ) -> "PostgrestFilterBuilder":
        """UPDATE query."""
        self._method = "PATCH"
        self.headers["Prefer"] = f"return={returning}"
        self.headers["Content-Type"] = "application/json"
        return PostgrestFilterBuilder(
            self.url, self.headers, self._params, self._method, values, self._client
        )

    def delete(
        self,
        *,
        returning: str = "minimal",
    ) -> "PostgrestFilterBuilder":
        """DELETE query."""
        self._method = "DELETE"
        if returning != "minimal":
            self.headers["Prefer"] = f"return={returning}"
        return PostgrestFilterBuilder(
            self.url, self.headers, self._params, self._method, None, self._client
        )

    def upsert(
        self,
        values: Union[Dict[str, Any], List[Dict[str, Any]]],
        *,
        on_conflict: Optional[str] = None,
        ignore_duplicates: bool = False,
    ) -> "PostgrestFilterBuilder":
        """UPSERT query."""
        self._method = "POST"
        prefer = ["return=representation"]
        if on_conflict:
            prefer.append(f"on_conflict={on_conflict}")
        if ignore_duplicates:
            prefer.append("resolution=ignore-duplicates")
        self.headers["Prefer"] = ",".join(prefer)
        self.headers["Content-Type"] = "application/json"
        return PostgrestFilterBuilder(
            self.url, self.headers, self._params, self._method, values, self._client
        )


class PostgrestFilterBuilder:
    """Filter builder with chainable filter and execution methods."""

    def __init__(
        self,
        url: str,
        headers: Dict[str, str],
        params: Dict[str, str],
        method: str,
        body: Optional[Any],
        client: httpx.AsyncClient,
    ):
        self.url = url
        self.headers = headers
        self._params = params
        self._method = method
        self._body = body
        self._client = client

    # =========================================================================
    # Filter Methods
    # =========================================================================

    def eq(self, column: str, value: Any) -> "PostgrestFilterBuilder":
        """Equal filter."""
        self._params[column] = f"eq.{value}"
        return self

    def neq(self, column: str, value: Any) -> "PostgrestFilterBuilder":
        """Not equal filter."""
        self._params[column] = f"neq.{value}"
        return self

    def gt(self, column: str, value: Any) -> "PostgrestFilterBuilder":
        """Greater than filter."""
        self._params[column] = f"gt.{value}"
        return self

    def gte(self, column: str, value: Any) -> "PostgrestFilterBuilder":
        """Greater than or equal filter."""
        self._params[column] = f"gte.{value}"
        return self

    def lt(self, column: str, value: Any) -> "PostgrestFilterBuilder":
        """Less than filter."""
        self._params[column] = f"lt.{value}"
        return self

    def lte(self, column: str, value: Any) -> "PostgrestFilterBuilder":
        """Less than or equal filter."""
        self._params[column] = f"lte.{value}"
        return self

    def like(self, column: str, pattern: str) -> "PostgrestFilterBuilder":
        """LIKE pattern filter."""
        self._params[column] = f"like.{pattern}"
        return self

    def ilike(self, column: str, pattern: str) -> "PostgrestFilterBuilder":
        """Case-insensitive LIKE filter."""
        self._params[column] = f"ilike.{pattern}"
        return self

    def is_(self, column: str, value: Optional[bool]) -> "PostgrestFilterBuilder":
        """IS filter (null, true, false)."""
        if value is None:
            self._params[column] = "is.null"
        else:
            self._params[column] = f"is.{str(value).lower()}"
        return self

    def in_(self, column: str, values: List[Any]) -> "PostgrestFilterBuilder":
        """IN filter."""
        self._params[column] = f"in.({','.join(str(v) for v in values)})"
        return self

    def contains(self, column: str, value: List[Any]) -> "PostgrestFilterBuilder":
        """Contains filter (@>)."""
        self._params[column] = f"cs.{{{','.join(str(v) for v in value)}}}"
        return self

    def contained_by(self, column: str, value: List[Any]) -> "PostgrestFilterBuilder":
        """Contained by filter (<@)."""
        self._params[column] = f"cd.{{{','.join(str(v) for v in value)}}}"
        return self

    def overlaps(self, column: str, value: List[Any]) -> "PostgrestFilterBuilder":
        """Overlaps filter (&&)."""
        self._params[column] = f"ov.{{{','.join(str(v) for v in value)}}}"
        return self

    def text_search(
        self,
        column: str,
        query: str,
        *,
        type_: str = "plain",
        config: Optional[str] = None,
    ) -> "PostgrestFilterBuilder":
        """Full-text search filter."""
        op_map = {"plain": "plfts", "phrase": "phfts", "websearch": "wfts"}
        op = op_map.get(type_, "fts")
        config_part = f"({config})" if config else ""
        self._params[column] = f"{op}{config_part}.{query}"
        return self

    def match(self, query: Dict[str, Any]) -> "PostgrestFilterBuilder":
        """Match multiple filters."""
        for column, value in query.items():
            self.eq(column, value)
        return self

    def not_(
        self, column: str, operator: str, value: Any
    ) -> "PostgrestFilterBuilder":
        """Negated filter."""
        self._params[column] = f"not.{operator}.{value}"
        return self

    def or_(self, filters: str) -> "PostgrestFilterBuilder":
        """OR filter combination."""
        self._params["or"] = f"({filters})"
        return self

    def filter(
        self, column: str, operator: str, value: Any
    ) -> "PostgrestFilterBuilder":
        """Generic filter."""
        self._params[column] = f"{operator}.{value}"
        return self

    # =========================================================================
    # Ordering & Pagination
    # =========================================================================

    def order(
        self,
        column: str,
        *,
        desc: bool = False,
        nulls_first: Optional[bool] = None,
    ) -> "PostgrestFilterBuilder":
        """Order results."""
        direction = "desc" if desc else "asc"
        order_val = f"{column}.{direction}"
        if nulls_first is not None:
            order_val += ".nullsfirst" if nulls_first else ".nullslast"
        self._params["order"] = order_val
        return self

    def limit(self, count: int) -> "PostgrestFilterBuilder":
        """Limit results."""
        self._params["limit"] = str(count)
        return self

    def offset(self, offset: int) -> "PostgrestFilterBuilder":
        """Offset results."""
        self._params["offset"] = str(offset)
        return self

    def range(self, start: int, end: int) -> "PostgrestFilterBuilder":
        """Range of results."""
        self._params["offset"] = str(start)
        self._params["limit"] = str(end - start + 1)
        return self

    # =========================================================================
    # Execution
    # =========================================================================

    async def execute(self) -> PostgrestResponse:
        """Execute the query and return response."""
        url = self.url
        if self._params:
            from urllib.parse import urlencode
            url = f"{url}?{urlencode(self._params)}"

        response = await self._client.request(
            self._method,
            url,
            headers=self.headers,
            json=self._body,
        )
        return _parse_response(response)

    def execute_sync(self) -> PostgrestResponse:
        """Execute the query synchronously."""
        import asyncio
        return asyncio.get_event_loop().run_until_complete(self.execute())

    async def single(self) -> PostgrestSingleResponse:
        """Execute and return single result."""
        self.headers["Accept"] = "application/vnd.pgrst.object+json"
        response = await self.execute()
        return PostgrestSingleResponse(
            data=response.data[0] if response.data else None,
            error=response.error,
            count=response.count,
            status=response.status,
            status_text=response.status_text,
        )

    async def maybe_single(self) -> PostgrestSingleResponse:
        """Execute and return single result or None."""
        return await self.single()


def _parse_response(response: httpx.Response) -> PostgrestResponse:
    """Parse HTTP response into PostgrestResponse."""
    try:
        data = response.json()
        if not isinstance(data, list):
            data = [data] if data else []
    except Exception:
        data = None

    if not response.is_success:
        error = PostgrestError(
            message=data.get("message", response.reason_phrase) if isinstance(data, dict) else response.reason_phrase,
            details=data.get("details") if isinstance(data, dict) else None,
            hint=data.get("hint") if isinstance(data, dict) else None,
            code=str(response.status_code),
        )
        return PostgrestResponse(
            data=None,
            error=error,
            count=None,
            status=response.status_code,
            status_text=response.reason_phrase,
        )

    # Parse count from Content-Range header
    count = None
    content_range = response.headers.get("content-range")
    if content_range:
        parts = content_range.split("/")
        if len(parts) == 2 and parts[1] != "*":
            try:
                count = int(parts[1])
            except ValueError:
                pass

    return PostgrestResponse(
        data=data,
        error=None,
        count=count,
        status=response.status_code,
        status_text=response.reason_phrase,
    )
