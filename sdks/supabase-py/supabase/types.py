"""
Type definitions for Supabase compatibility.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, TypeVar
from datetime import datetime

T = TypeVar("T")


@dataclass
class User:
    """User model matching Supabase schema."""
    id: str
    aud: str = "authenticated"
    role: str = "authenticated"
    email: Optional[str] = None
    phone: Optional[str] = None
    email_confirmed_at: Optional[datetime] = None
    phone_confirmed_at: Optional[datetime] = None
    last_sign_in_at: Optional[datetime] = None
    app_metadata: Dict[str, Any] = field(default_factory=dict)
    user_metadata: Dict[str, Any] = field(default_factory=dict)
    identities: List[Dict[str, Any]] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class Session:
    """Session model matching Supabase schema."""
    access_token: str
    token_type: str = "bearer"
    expires_in: int = 3600
    expires_at: Optional[int] = None
    refresh_token: Optional[str] = None
    user: Optional[User] = None


@dataclass
class AuthResponse:
    """Response from auth operations."""
    user: Optional[User] = None
    session: Optional[Session] = None
    error: Optional["AuthError"] = None


@dataclass
class AuthError:
    """Authentication error."""
    message: str
    status: int = 400
    code: Optional[str] = None


@dataclass
class PostgrestResponse:
    """Response from database operations."""
    data: Optional[List[Dict[str, Any]]] = None
    error: Optional["PostgrestError"] = None
    count: Optional[int] = None
    status: int = 200
    status_text: str = "OK"


@dataclass
class PostgrestSingleResponse:
    """Response from single-row operations."""
    data: Optional[Dict[str, Any]] = None
    error: Optional["PostgrestError"] = None
    count: Optional[int] = None
    status: int = 200
    status_text: str = "OK"


@dataclass
class PostgrestError:
    """Database operation error."""
    message: str
    details: Optional[str] = None
    hint: Optional[str] = None
    code: str = "ERROR"


# Type aliases
Json = Dict[str, Any]
