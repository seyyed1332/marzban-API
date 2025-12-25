from __future__ import annotations

from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx


class MarzbanApiError(RuntimeError):
    pass


def normalize_marzban_base_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = f"https://{raw}"
    parsed = urlparse(raw)
    if parsed.scheme not in {"http", "https"}:
        return ""
    if not parsed.netloc:
        return ""

    path = (parsed.path or "").rstrip("/")

    lowered = path.lower()
    for suffix in ("/docs", "/redoc", "/openapi.json"):
        if lowered.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/")
            lowered = path.lower()
            break

    if lowered.endswith("/api"):
        path = path[: -len("/api")].rstrip("/")

    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", "")).rstrip("/")


class MarzbanClient:
    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool = True,
        timeout_seconds: float = 30.0,
    ) -> None:
        normalized = normalize_marzban_base_url(base_url)
        if not normalized:
            raise ValueError("Invalid base_url (expected http(s) URL)")
        self._username = username
        self._password = password
        self._token: str | None = None
        self._client = httpx.AsyncClient(
            base_url=normalized,
            timeout=httpx.Timeout(timeout_seconds),
            verify=verify_ssl,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def login(self) -> str:
        resp = await self._client.post(
            "api/admin/token",
            data={"username": self._username, "password": self._password},
        )
        if resp.status_code == 401:
            raise MarzbanApiError("Unauthorized: check admin username/password")
        resp.raise_for_status()
        data = resp.json()
        token = data.get("access_token")
        if not token:
            raise MarzbanApiError("Login succeeded but access_token was missing")
        self._token = token
        self._client.headers["Authorization"] = f"Bearer {token}"
        return token

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        path = (path or "").lstrip("/")
        if not self._token:
            await self.login()
        resp = await self._client.request(method, path, **kwargs)
        if resp.status_code == 401:
            await self.login()
            resp = await self._client.request(method, path, **kwargs)
        return resp

    async def get_inbounds(self) -> dict[str, list[str]]:
        resp = await self._request("GET", "api/inbounds")
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise MarzbanApiError("Unexpected /api/inbounds response")
        return {str(k): list(v) for k, v in data.items()}

    async def get_users(
        self,
        *,
        offset: int = 0,
        limit: int = 50,
        search: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"offset": offset, "limit": limit}
        if search:
            params["search"] = search
        resp = await self._request("GET", "api/users", params=params)
        resp.raise_for_status()
        return resp.json()

    async def get_user(self, username: str) -> dict[str, Any]:
        resp = await self._request("GET", f"api/user/{username}")
        if resp.status_code == 404:
            raise MarzbanApiError(f"User not found: {username}")
        resp.raise_for_status()
        return resp.json()

    async def revoke_user_subscription(self, username: str) -> dict[str, Any]:
        resp = await self._request("POST", f"api/user/{username}/revoke_sub")
        if resp.status_code == 404:
            raise MarzbanApiError(f"User not found: {username}")
        resp.raise_for_status()
        return resp.json()

    async def reset_user_data_usage(self, username: str) -> dict[str, Any]:
        resp = await self._request("POST", f"api/user/{username}/reset")
        if resp.status_code == 404:
            raise MarzbanApiError(f"User not found: {username}")
        resp.raise_for_status()
        return resp.json()

    async def get_user_usage(
        self,
        username: str,
        *,
        start: str = "",
        end: str = "",
    ) -> dict[str, Any]:
        params = {"start": start, "end": end}
        resp = await self._request("GET", f"api/user/{username}/usage", params=params)
        if resp.status_code == 404:
            raise MarzbanApiError(f"User not found: {username}")
        resp.raise_for_status()
        return resp.json()
