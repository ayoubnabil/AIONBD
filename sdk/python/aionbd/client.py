"""Minimal HTTP client for AIONBD.

The implementation intentionally uses only Python standard library modules,
which keeps the initial SDK easy to audit and portable.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any


class AionBDError(RuntimeError):
    """Raised when the AIONBD server returns an error or is unreachable."""


@dataclass(frozen=True)
class DistanceResult:
    """Represents a distance operation response."""

    metric: str
    value: float


class AionBDClient:
    """Small HTTP client targeting the AIONBD server skeleton."""

    def __init__(
        self, base_url: str = "http://127.0.0.1:8080", timeout: float = 5.0
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def health(self) -> dict[str, Any]:
        """Returns server health and uptime metadata."""
        payload = self._request("GET", "/health")
        return payload if isinstance(payload, dict) else {"raw": payload}

    def distance(
        self, left: list[float], right: list[float], metric: str = "dot"
    ) -> DistanceResult:
        """Computes a distance/similarity value through the API."""
        payload = self._request(
            "POST",
            "/distance",
            {
                "left": left,
                "right": right,
                "metric": metric,
            },
        )
        try:
            return DistanceResult(
                metric=str(payload["metric"]), value=float(payload["value"])
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid distance response: {payload}") from exc

    def _request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> Any:
        data = None
        headers = {"Accept": "application/json"}

        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(
            url=f"{self._base_url}{path}",
            method=method,
            data=data,
            headers=headers,
        )

        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                raw = response.read().decode("utf-8")
                if not raw:
                    return {}
                return json.loads(raw)
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise AionBDError(f"HTTP {exc.code} on {method} {path}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise AionBDError(
                f"request failed for {method} {path}: {exc.reason}"
            ) from exc
