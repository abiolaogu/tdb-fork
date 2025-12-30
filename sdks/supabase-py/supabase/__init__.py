"""
LumaDB Supabase-Compatible Python SDK

Drop-in replacement for supabase-py that connects to LumaDB.
"""

from .client import Client, create_client
from .auth import AuthClient
from .postgrest import PostgrestClient, PostgrestQueryBuilder

__version__ = "0.1.0"
__all__ = [
    "Client",
    "create_client",
    "AuthClient",
    "PostgrestClient",
    "PostgrestQueryBuilder",
]
