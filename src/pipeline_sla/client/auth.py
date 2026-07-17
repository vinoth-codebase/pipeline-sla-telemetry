"""Authentication strategies for the Airflow REST API.

Airflow 2.x exposes several auth modes; the client abstracts them behind a
single :class:`AuthProvider` protocol that yields request headers on demand.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


@runtime_checkable
class AuthProvider(Protocol):
    """Supplies HTTP headers for every outgoing request."""

    def headers(self) -> dict[str, str]:
        ...  # pragma: no cover - protocol definition

    def describe(self) -> str:
        ...  # pragma: no cover - protocol definition


@dataclass
class NoAuthProvider:
    """For unauthenticated dev instances (Airflow 'auth_backend = none')."""

    def headers(self) -> dict[str, str]:
        return {"Accept": "application/json"}

    def describe(self) -> str:
        return "no-auth"


@dataclass
class TokenAuthProvider:
    """Bearer-token auth (Airflow API token / JWT)."""

    token: str
    header_name: str = "Authorization"
    scheme: str = "Bearer"

    def __post_init__(self) -> None:
        if not self.token:
            raise ValueError("TokenAuthProvider requires a non-empty token")

    def headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            self.header_name: f"{self.scheme} {self.token}",
        }

    def describe(self) -> str:
        return f"token({self.header_name})"


@dataclass
class BasicAuthProvider:
    """HTTP Basic auth (Airflow 'auth_backend = basic_auth')."""

    username: str
    password: str
    encoding: str = "utf-8"
    _header: dict[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.username:
            raise ValueError("BasicAuthProvider requires a username")
        raw = f"{self.username}:{self.password}".encode(self.encoding)
        token = base64.b64encode(raw).decode("ascii")
        self._header = {
            "Accept": "application/json",
            "Authorization": f"Basic {token}",
        }

    def headers(self) -> dict[str, str]:
        return dict(self._header)

    def describe(self) -> str:
        return f"basic({self.username})"


def build_auth_provider(
    method: str,
    *,
    token: str = "",
    username: str = "",
    password: str = "",
) -> AuthProvider:
    """Factory returning the right provider for a config ``auth.method``."""
    method = (method or "none").strip().lower()
    if method in {"token", "bearer", "jwt"}:
        return TokenAuthProvider(token=token)
    if method in {"basic", "password"}:
        return BasicAuthProvider(username=username, password=password)
    if method in {"none", "noauth", "anonymous"}:
        return NoAuthProvider()
    raise ValueError(f"Unsupported Airflow auth method: {method!r}")
