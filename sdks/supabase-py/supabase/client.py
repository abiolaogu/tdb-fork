"""
SupabaseClient - Main client for Supabase-compatible API.
"""

from typing import Any, Dict, Optional
from .auth import AuthClient
from .postgrest import PostgrestClient, PostgrestQueryBuilder


def create_client(
    supabase_url: str,
    supabase_key: str,
    options: Optional[Dict[str, Any]] = None,
) -> "Client":
    """
    Create a new Supabase client.

    Args:
        supabase_url: The Supabase project URL
        supabase_key: The Supabase anon or service role key
        options: Optional configuration options

    Returns:
        A configured Supabase client

    Example:
        >>> supabase = create_client("http://localhost:3000", "your-anon-key")
        >>> response = await supabase.from_("posts").select("*").execute()
    """
    return Client(supabase_url, supabase_key, options)


class Client:
    """Supabase Client - Main entry point for all Supabase operations."""

    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        options: Optional[Dict[str, Any]] = None,
    ):
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_key = supabase_key
        self.options = options or {}

        self._headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
        }

        # Add custom headers
        if "global" in self.options and "headers" in self.options["global"]:
            self._headers.update(self.options["global"]["headers"])

        # Initialize auth client
        auth_url = f"{self.supabase_url}/auth/v1"
        self._auth = AuthClient(auth_url, self._headers.copy())

        # Initialize PostgrestClient
        rest_url = f"{self.supabase_url}/rest/v1"
        schema = self.options.get("db", {}).get("schema", "public")
        self._postgrest = PostgrestClient(rest_url, self._headers.copy(), schema)

    @property
    def auth(self) -> AuthClient:
        """Access to auth operations."""
        return self._auth

    def from_(self, table: str) -> PostgrestQueryBuilder:
        """
        Start a query on a table.

        Args:
            table: The table name to query

        Returns:
            A query builder for chaining operations

        Example:
            >>> response = await supabase.from_("posts") \\
            ...     .select("id, title, author:users(name)") \\
            ...     .eq("published", True) \\
            ...     .order("created_at", desc=True) \\
            ...     .limit(10) \\
            ...     .execute()
        """
        return self._postgrest.from_(table)

    async def rpc(
        self,
        fn: str,
        args: Optional[Dict[str, Any]] = None,
    ):
        """
        Call a stored function (RPC).

        Args:
            fn: The function name
            args: Function arguments

        Returns:
            The function result

        Example:
            >>> result = await supabase.rpc("get_user_stats", {"user_id": "123"})
        """
        return await self._postgrest.rpc(fn, args)

    @property
    def storage(self):
        """Access to storage operations (stub for Phase 2)."""
        return _StorageStub(self.supabase_url, self._headers)

    def channel(self, name: str):
        """Access to realtime operations (stub for Phase 2)."""
        return _RealtimeStub()

    @property
    def functions(self):
        """Access to edge functions (stub for Phase 2)."""
        return _FunctionsStub(self.supabase_url, self._headers)

    async def close(self):
        """Close all HTTP clients."""
        await self._auth.close()
        await self._postgrest.close()


class _StorageStub:
    """Stub for storage operations (Phase 2)."""

    def __init__(self, url: str, headers: Dict[str, str]):
        self.url = url
        self.headers = headers

    def from_(self, bucket: str):
        return _BucketStub(self.url, bucket)


class _BucketStub:
    """Stub for bucket operations."""

    def __init__(self, url: str, bucket: str):
        self.url = url
        self.bucket = bucket

    async def upload(self, path: str, file: bytes):
        raise NotImplementedError("Storage not implemented yet")

    async def download(self, path: str):
        raise NotImplementedError("Storage not implemented yet")

    async def remove(self, paths: list):
        raise NotImplementedError("Storage not implemented yet")

    async def list(self, path: str = ""):
        raise NotImplementedError("Storage not implemented yet")

    def get_public_url(self, path: str) -> str:
        return f"{self.url}/storage/v1/object/public/{self.bucket}/{path}"


class _RealtimeStub:
    """Stub for realtime operations (Phase 2)."""

    def on(self, event: str, callback):
        return self

    def subscribe(self):
        print("Warning: Realtime not implemented yet")
        return self

    def unsubscribe(self):
        pass


class _FunctionsStub:
    """Stub for edge functions (Phase 2)."""

    def __init__(self, url: str, headers: Dict[str, str]):
        self.url = url
        self.headers = headers

    async def invoke(self, function_name: str, options: Optional[Dict[str, Any]] = None):
        import httpx

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.url}/functions/v1/{function_name}",
                headers=self.headers,
                json=options.get("body") if options else None,
            )

            if not response.is_success:
                return {"data": None, "error": Exception(response.text)}

            return {"data": response.json(), "error": None}
