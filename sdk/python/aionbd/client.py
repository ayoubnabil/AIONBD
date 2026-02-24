"""Minimal HTTP client for AIONBD.

The implementation intentionally uses only Python standard library modules,
which keeps the initial SDK easy to audit and portable.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
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


@dataclass(frozen=True)
class SearchResult:
    """Represents a top-1 collection search response."""

    id: int
    metric: str
    value: float


@dataclass(frozen=True)
class CollectionInfo:
    """Represents collection metadata returned by the server."""

    name: str
    dimension: int
    strict_finite: bool
    point_count: int


@dataclass(frozen=True)
class UpsertPointResult:
    """Represents point upsert result."""

    id: int
    created: bool


@dataclass(frozen=True)
class PointResult:
    """Represents a stored point payload."""

    id: int
    values: list[float]


@dataclass(frozen=True)
class DeletePointResult:
    """Represents point delete result."""

    id: int
    deleted: bool


@dataclass(frozen=True)
class DeleteCollectionResult:
    """Represents collection delete result."""

    name: str
    deleted: bool


class AionBDClient:
    """Small HTTP client targeting the AIONBD server skeleton."""

    def __init__(
        self, base_url: str = "http://127.0.0.1:8080", timeout: float = 5.0
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def live(self) -> dict[str, Any]:
        """Returns liveness and uptime metadata."""
        payload = self._request("GET", "/live")
        return payload if isinstance(payload, dict) else {"raw": payload}

    def ready(self) -> dict[str, Any]:
        """Returns readiness checks and uptime metadata."""
        payload = self._request("GET", "/ready")
        return payload if isinstance(payload, dict) else {"raw": payload}

    def health(self) -> dict[str, Any]:
        """Backward-compatible alias using readiness endpoint."""
        return self.ready()

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

    def create_collection(
        self, name: str, dimension: int, strict_finite: bool = True
    ) -> CollectionInfo:
        """Creates an in-memory collection."""
        payload = self._request(
            "POST",
            "/collections",
            {
                "name": name,
                "dimension": dimension,
                "strict_finite": strict_finite,
            },
        )
        return self._parse_collection(payload)

    def list_collections(self) -> list[CollectionInfo]:
        """Lists all collections."""
        payload = self._request("GET", "/collections")
        try:
            items = payload["collections"]
            return [self._parse_collection(item) for item in items]
        except (KeyError, TypeError) as exc:
            raise AionBDError(f"invalid list collections response: {payload}") from exc

    def get_collection(self, name: str) -> CollectionInfo:
        """Reads collection metadata."""
        payload = self._request("GET", f"/collections/{self._escaped(name)}")
        return self._parse_collection(payload)

    def search_collection(
        self, collection: str, query: list[float], metric: str = "dot"
    ) -> SearchResult:
        """Runs top-1 search in a collection using the selected metric."""
        payload = self._request(
            "POST",
            f"/collections/{self._escaped(collection)}/search",
            {"query": query, "metric": metric},
        )
        try:
            return SearchResult(
                id=int(payload["id"]),
                metric=str(payload["metric"]),
                value=float(payload["value"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid search response: {payload}") from exc

    def search_collection_top_k(
        self, collection: str, query: list[float], limit: int = 10, metric: str = "dot"
    ) -> list[SearchResult]:
        """Runs top-k search in a collection using the selected metric."""
        payload = self._request(
            "POST",
            f"/collections/{self._escaped(collection)}/search/topk",
            {"query": query, "metric": metric, "limit": limit},
        )
        try:
            response_metric = str(payload["metric"])
            hits = payload["hits"]
            if not isinstance(hits, list):
                raise TypeError("hits must be a list")
            return [
                SearchResult(
                    id=int(hit["id"]),
                    metric=response_metric,
                    value=float(hit["value"]),
                )
                for hit in hits
            ]
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid top-k search response: {payload}") from exc

    def upsert_point(
        self, collection: str, point_id: int, values: list[float]
    ) -> UpsertPointResult:
        """Creates or updates a point in a collection."""
        payload = self._request(
            "PUT",
            f"/collections/{self._escaped(collection)}/points/{point_id}",
            {"values": values},
        )
        try:
            return UpsertPointResult(
                id=int(payload["id"]), created=bool(payload["created"])
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid upsert response: {payload}") from exc

    def get_point(self, collection: str, point_id: int) -> PointResult:
        """Reads a point payload from a collection."""
        payload = self._request(
            "GET", f"/collections/{self._escaped(collection)}/points/{point_id}"
        )
        try:
            values = payload["values"]
            if not isinstance(values, list):
                raise TypeError("values must be a list")
            return PointResult(
                id=int(payload["id"]),
                values=[float(value) for value in values],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid get point response: {payload}") from exc

    def delete_point(self, collection: str, point_id: int) -> DeletePointResult:
        """Deletes a point from a collection."""
        payload = self._request(
            "DELETE", f"/collections/{self._escaped(collection)}/points/{point_id}"
        )
        try:
            return DeletePointResult(
                id=int(payload["id"]), deleted=bool(payload["deleted"])
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid delete point response: {payload}") from exc

    def delete_collection(self, name: str) -> DeleteCollectionResult:
        """Deletes a collection."""
        payload = self._request("DELETE", f"/collections/{self._escaped(name)}")
        try:
            return DeleteCollectionResult(
                name=str(payload["name"]),
                deleted=bool(payload["deleted"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid delete collection response: {payload}") from exc

    def _parse_collection(self, payload: Any) -> CollectionInfo:
        try:
            return CollectionInfo(
                name=str(payload["name"]),
                dimension=int(payload["dimension"]),
                strict_finite=bool(payload["strict_finite"]),
                point_count=int(payload["point_count"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise AionBDError(f"invalid collection response: {payload}") from exc

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

    @staticmethod
    def _escaped(value: str) -> str:
        return urllib.parse.quote(value.strip(), safe="")
