"""
AuthClient - Supabase-compatible authentication client for Python.
"""

from typing import Any, Dict, Optional
import httpx
from .types import User, Session, AuthResponse, AuthError


class AuthClient:
    """Authentication client for Supabase-compatible auth."""

    def __init__(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ):
        self.url = url
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self._client = httpx.AsyncClient()
        self._session: Optional[Session] = None

    # =========================================================================
    # Session Management
    # =========================================================================

    async def get_session(self) -> Dict[str, Any]:
        """Get current session."""
        return {"data": {"session": self._session}, "error": None}

    async def get_user(self) -> Dict[str, Any]:
        """Get current user."""
        if not self._session:
            return {"data": {"user": None}, "error": None}

        try:
            response = await self._client.get(
                f"{self.url}/user",
                headers={
                    **self.headers,
                    "Authorization": f"Bearer {self._session.access_token}",
                },
            )
            if response.is_success:
                user_data = response.json()
                return {"data": {"user": User(**user_data)}, "error": None}
            else:
                return {
                    "data": {"user": None},
                    "error": AuthError(message=response.text, status=response.status_code),
                }
        except Exception as e:
            return {
                "data": {"user": None},
                "error": AuthError(message=str(e), status=500),
            }

    # =========================================================================
    # Sign Up / Sign In
    # =========================================================================

    async def sign_up(
        self,
        email: str,
        password: str,
        *,
        data: Optional[Dict[str, Any]] = None,
    ) -> AuthResponse:
        """Sign up with email and password."""
        try:
            response = await self._client.post(
                f"{self.url}/signup",
                headers=self.headers,
                json={"email": email, "password": password, "data": data or {}},
            )

            if not response.is_success:
                error_data = response.json() if response.text else {}
                return AuthResponse(
                    error=AuthError(
                        message=error_data.get("message", response.reason_phrase),
                        status=response.status_code,
                        code=error_data.get("code"),
                    )
                )

            data = response.json()
            session = Session(
                access_token=data.get("access_token", ""),
                token_type=data.get("token_type", "bearer"),
                expires_in=data.get("expires_in", 3600),
                expires_at=data.get("expires_at"),
                refresh_token=data.get("refresh_token"),
                user=User(**data.get("user", {})) if data.get("user") else None,
            )
            self._session = session
            return AuthResponse(user=session.user, session=session)

        except Exception as e:
            return AuthResponse(error=AuthError(message=str(e), status=500))

    async def sign_in_with_password(
        self,
        email: str,
        password: str,
    ) -> AuthResponse:
        """Sign in with email and password."""
        try:
            response = await self._client.post(
                f"{self.url}/token?grant_type=password",
                headers=self.headers,
                json={"email": email, "password": password},
            )

            if not response.is_success:
                error_data = response.json() if response.text else {}
                return AuthResponse(
                    error=AuthError(
                        message=error_data.get("message", response.reason_phrase),
                        status=response.status_code,
                        code=error_data.get("code"),
                    )
                )

            data = response.json()
            session = Session(
                access_token=data.get("access_token", ""),
                token_type=data.get("token_type", "bearer"),
                expires_in=data.get("expires_in", 3600),
                expires_at=data.get("expires_at"),
                refresh_token=data.get("refresh_token"),
                user=User(**data.get("user", {})) if data.get("user") else None,
            )
            self._session = session
            return AuthResponse(user=session.user, session=session)

        except Exception as e:
            return AuthResponse(error=AuthError(message=str(e), status=500))

    async def sign_in_with_oauth(
        self,
        provider: str,
        *,
        redirect_to: Optional[str] = None,
        scopes: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get OAuth sign-in URL."""
        params = []
        if redirect_to:
            params.append(f"redirect_to={redirect_to}")
        if scopes:
            params.append(f"scopes={scopes}")
        
        query = "&".join(params)
        url = f"{self.url}/authorize?provider={provider}"
        if query:
            url = f"{url}&{query}"

        return {"data": {"provider": provider, "url": url}, "error": None}

    async def sign_in_with_otp(
        self,
        email: str,
    ) -> Dict[str, Any]:
        """Sign in with magic link (OTP)."""
        try:
            response = await self._client.post(
                f"{self.url}/magiclink",
                headers=self.headers,
                json={"email": email},
            )

            if not response.is_success:
                error_data = response.json() if response.text else {}
                return {
                    "data": {"user": None, "session": None},
                    "error": AuthError(
                        message=error_data.get("message", response.reason_phrase),
                        status=response.status_code,
                    ),
                }

            return {"data": {"user": None, "session": None}, "error": None}

        except Exception as e:
            return {
                "data": {"user": None, "session": None},
                "error": AuthError(message=str(e), status=500),
            }

    # =========================================================================
    # Sign Out
    # =========================================================================

    async def sign_out(self) -> Dict[str, Any]:
        """Sign out current user."""
        if self._session:
            try:
                await self._client.post(
                    f"{self.url}/logout",
                    headers={
                        **self.headers,
                        "Authorization": f"Bearer {self._session.access_token}",
                    },
                )
            except Exception:
                pass  # Ignore errors on sign out

        self._session = None
        return {"error": None}

    # =========================================================================
    # Password Recovery
    # =========================================================================

    async def reset_password_for_email(
        self,
        email: str,
    ) -> Dict[str, Any]:
        """Send password reset email."""
        try:
            response = await self._client.post(
                f"{self.url}/recover",
                headers=self.headers,
                json={"email": email},
            )

            if not response.is_success:
                error_data = response.json() if response.text else {}
                return {
                    "data": {},
                    "error": AuthError(
                        message=error_data.get("message", response.reason_phrase),
                        status=response.status_code,
                    ),
                }

            return {"data": {}, "error": None}

        except Exception as e:
            return {"data": {}, "error": AuthError(message=str(e), status=500)}

    # =========================================================================
    # User Management
    # =========================================================================

    async def update_user(
        self,
        attributes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Update current user."""
        if not self._session:
            return {
                "data": {"user": None},
                "error": AuthError(message="Not authenticated", status=401),
            }

        try:
            response = await self._client.put(
                f"{self.url}/user",
                headers={
                    **self.headers,
                    "Authorization": f"Bearer {self._session.access_token}",
                },
                json=attributes,
            )

            if not response.is_success:
                error_data = response.json() if response.text else {}
                return {
                    "data": {"user": None},
                    "error": AuthError(
                        message=error_data.get("message", response.reason_phrase),
                        status=response.status_code,
                    ),
                }

            user_data = response.json()
            return {"data": {"user": User(**user_data)}, "error": None}

        except Exception as e:
            return {
                "data": {"user": None},
                "error": AuthError(message=str(e), status=500),
            }

    async def refresh_session(self) -> AuthResponse:
        """Refresh the access token."""
        if not self._session or not self._session.refresh_token:
            return AuthResponse(
                error=AuthError(message="No session to refresh", status=400)
            )

        try:
            response = await self._client.post(
                f"{self.url}/token?grant_type=refresh_token",
                headers=self.headers,
                json={"refresh_token": self._session.refresh_token},
            )

            if not response.is_success:
                self._session = None
                error_data = response.json() if response.text else {}
                return AuthResponse(
                    error=AuthError(
                        message=error_data.get("message", response.reason_phrase),
                        status=response.status_code,
                    )
                )

            data = response.json()
            session = Session(
                access_token=data.get("access_token", ""),
                token_type=data.get("token_type", "bearer"),
                expires_in=data.get("expires_in", 3600),
                expires_at=data.get("expires_at"),
                refresh_token=data.get("refresh_token"),
                user=User(**data.get("user", {})) if data.get("user") else None,
            )
            self._session = session
            return AuthResponse(user=session.user, session=session)

        except Exception as e:
            return AuthResponse(error=AuthError(message=str(e), status=500))

    async def close(self):
        """Close the HTTP client."""
        await self._client.aclose()
