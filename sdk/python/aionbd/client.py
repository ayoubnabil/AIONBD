"""Minimal HTTP client for AIONBD using Python standard library only."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from ._parsers import (
    parse_collection,
    parse_delete_collection,
    parse_delete_point,
    parse_distance,
    parse_list_points,
    parse_metrics,
    parse_point,
    parse_search,
    parse_search_hits,
    parse_upsert_point,
)
from .errors import AionBDError
from .models import (
    CollectionInfo,
    DeleteCollectionResult,
    DeletePointResult,
    DistanceResult,
    MetricsResult,
    PointResult,
    SearchResult,
    UpsertPointResult,
)


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

    def metrics(self) -> MetricsResult:
        """Returns runtime counters and server state indicators."""
        return parse_metrics(self._request("GET", "/metrics"))

    def metrics_prometheus(self) -> str:
        """Returns runtime counters in Prometheus text exposition format."""
        return self._request("GET", "/metrics/prometheus", raw=True)

    def health(self) -> dict[str, Any]:
        """Backward-compatible alias using readiness endpoint."""
        return self.ready()

    def distance(
        self, left: list[float], right: list[float], metric: str = "dot"
    ) -> DistanceResult:
        """Computes a distance/similarity value through the API."""
        return parse_distance(
            self._request(
                "POST", "/distance", {"left": left, "right": right, "metric": metric}
            )
        )

    def create_collection(
        self, name: str, dimension: int, strict_finite: bool = True
    ) -> CollectionInfo:
        """Creates an in-memory collection."""
        return parse_collection(
            self._request(
                "POST",
                "/collections",
                {"name": name, "dimension": dimension, "strict_finite": strict_finite},
            )
        )

    def list_collections(self) -> list[CollectionInfo]:
        """Lists all collections."""
        payload = self._request("GET", "/collections")
        try:
            return [parse_collection(item) for item in payload["collections"]]
        except (KeyError, TypeError) as exc:
            raise AionBDError(f"invalid list collections response: {payload}") from exc

    def get_collection(self, name: str) -> CollectionInfo:
        """Reads collection metadata."""
        return parse_collection(
            self._request("GET", f"/collections/{self._escaped(name)}")
        )

    def search_collection(
        self,
        collection: str,
        query: list[float],
        metric: str = "dot",
        mode: str = "auto",
        target_recall: float | None = None,
        filter: dict[str, Any] | None = None,
    ) -> SearchResult:
        """Runs top-1 search in a collection using the selected metric."""
        body: dict[str, Any] = {"query": query, "metric": metric, "mode": mode}
        if target_recall is not None:
            body["target_recall"] = float(target_recall)
        if filter is not None:
            body["filter"] = filter

        return parse_search(
            self._request(
                "POST",
                f"/collections/{self._escaped(collection)}/search",
                body,
            )
        )

    def search_collection_top_k(
        self,
        collection: str,
        query: list[float],
        limit: int | None = 10,
        metric: str = "dot",
        mode: str = "auto",
        target_recall: float | None = None,
        filter: dict[str, Any] | None = None,
    ) -> list[SearchResult]:
        """Runs top-k search in a collection using the selected metric.

        Set `limit=None` to omit the field and let server defaults apply.
        """
        body: dict[str, Any] = {"query": query, "metric": metric, "mode": mode}
        if limit is not None:
            limit = int(limit)
            if limit <= 0:
                raise ValueError("limit must be > 0")
            body["limit"] = limit
        if target_recall is not None:
            body["target_recall"] = float(target_recall)
        if filter is not None:
            body["filter"] = filter

        payload = self._request(
            "POST", f"/collections/{self._escaped(collection)}/search/topk", body
        )
        return parse_search_hits(payload, mode_fallback=mode)

    def upsert_point(
        self,
        collection: str,
        point_id: int,
        values: list[float],
        payload: dict[str, Any] | None = None,
    ) -> UpsertPointResult:
        """Creates or updates a point in a collection."""
        body: dict[str, Any] = {"values": values}
        if payload is not None:
            body["payload"] = payload
        return parse_upsert_point(
            self._request(
                "PUT",
                f"/collections/{self._escaped(collection)}/points/{point_id}",
                body,
            )
        )

    def get_point(self, collection: str, point_id: int) -> PointResult:
        """Reads a point payload from a collection."""
        return parse_point(
            self._request(
                "GET", f"/collections/{self._escaped(collection)}/points/{point_id}"
            )
        )

    def list_points(
        self,
        collection: str,
        offset: int = 0,
        limit: int | None = 100,
        after_id: int | None = None,
    ) -> dict[str, Any]:
        """Lists point ids with pagination metadata (offset or cursor mode).

        Set `limit=None` to omit the query parameter and let server defaults apply.
        """
        if after_id is not None and offset != 0:
            raise ValueError("offset must be 0 when after_id is provided")

        params: list[str] = []
        if limit is not None:
            limit = int(limit)
            if limit <= 0:
                raise ValueError("limit must be > 0")
            params.append(f"limit={limit}")

        params.append(
            f"offset={int(offset)}" if after_id is None else f"after_id={int(after_id)}"
        )
        return parse_list_points(
            self._request(
                "GET",
                f"/collections/{self._escaped(collection)}/points?{'&'.join(params)}",
            )
        )

    def delete_point(self, collection: str, point_id: int) -> DeletePointResult:
        """Deletes a point from a collection."""
        return parse_delete_point(
            self._request(
                "DELETE", f"/collections/{self._escaped(collection)}/points/{point_id}"
            )
        )

    def delete_collection(self, name: str) -> DeleteCollectionResult:
        """Deletes a collection."""
        return parse_delete_collection(
            self._request("DELETE", f"/collections/{self._escaped(name)}")
        )

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        raw: bool = False,
    ) -> Any:
        data = None
        headers = {"Accept": "text/plain" if raw else "application/json"}
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
                payload = response.read().decode("utf-8")
                if raw:
                    return payload
                return {} if not payload else json.loads(payload)
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
