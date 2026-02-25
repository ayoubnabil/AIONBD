"""Response parsers for AIONBD SDK client."""

from __future__ import annotations

from typing import Any

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


def parse_metrics(payload: Any) -> MetricsResult:
    try:
        return MetricsResult(
            uptime_ms=int(payload["uptime_ms"]),
            ready=bool(payload["ready"]),
            engine_loaded=bool(payload["engine_loaded"]),
            storage_available=bool(payload["storage_available"]),
            http_requests_total=int(payload["http_requests_total"]),
            http_requests_in_flight=int(payload["http_requests_in_flight"]),
            http_responses_2xx_total=int(payload["http_responses_2xx_total"]),
            http_responses_4xx_total=int(payload["http_responses_4xx_total"]),
            http_requests_5xx_total=int(payload["http_requests_5xx_total"]),
            http_request_duration_us_total=int(
                payload["http_request_duration_us_total"]
            ),
            http_request_duration_us_max=int(payload["http_request_duration_us_max"]),
            http_request_duration_us_avg=float(payload["http_request_duration_us_avg"]),
            collections=int(payload["collections"]),
            points=int(payload["points"]),
            l2_indexes=int(payload["l2_indexes"]),
            persistence_enabled=bool(payload["persistence_enabled"]),
            persistence_writes=int(payload["persistence_writes"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid metrics response: {payload}") from exc


def parse_distance(payload: Any) -> DistanceResult:
    try:
        return DistanceResult(
            metric=str(payload["metric"]), value=float(payload["value"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid distance response: {payload}") from exc


def parse_collection(payload: Any) -> CollectionInfo:
    try:
        return CollectionInfo(
            name=str(payload["name"]),
            dimension=int(payload["dimension"]),
            strict_finite=bool(payload["strict_finite"]),
            point_count=int(payload["point_count"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid collection response: {payload}") from exc


def parse_search(payload: Any) -> SearchResult:
    try:
        recall_raw = payload.get("recall_at_k")
        return SearchResult(
            id=int(payload["id"]),
            metric=str(payload["metric"]),
            value=float(payload["value"]),
            mode=str(payload.get("mode", "exact")),
            recall_at_k=None if recall_raw is None else float(recall_raw),
            payload=payload.get("payload"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid search response: {payload}") from exc


def parse_search_hits(payload: Any, mode_fallback: str) -> list[SearchResult]:
    try:
        response_metric = str(payload["metric"])
        response_mode = str(payload.get("mode", mode_fallback))
        response_recall_raw = payload.get("recall_at_k")
        response_recall = (
            None if response_recall_raw is None else float(response_recall_raw)
        )
        hits = payload["hits"]
        if not isinstance(hits, list):
            raise TypeError("hits must be a list")
        return [
            SearchResult(
                id=int(hit["id"]),
                metric=response_metric,
                value=float(hit["value"]),
                mode=response_mode,
                recall_at_k=response_recall,
                payload=hit.get("payload"),
            )
            for hit in hits
        ]
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid top-k search response: {payload}") from exc


def parse_upsert_point(payload: Any) -> UpsertPointResult:
    try:
        return UpsertPointResult(
            id=int(payload["id"]), created=bool(payload["created"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid upsert response: {payload}") from exc


def parse_point(payload: Any) -> PointResult:
    try:
        values = payload["values"]
        if not isinstance(values, list):
            raise TypeError("values must be a list")
        metadata = payload.get("payload")
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            raise TypeError("payload must be an object")
        return PointResult(
            id=int(payload["id"]),
            values=[float(value) for value in values],
            payload=metadata,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid get point response: {payload}") from exc


def parse_list_points(payload: Any) -> dict[str, Any]:
    try:
        points = [int(item["id"]) for item in payload["points"]]
        next_offset = payload["next_offset"]
        next_after_id = payload["next_after_id"]
        return {
            "points": points,
            "total": int(payload["total"]),
            "next_offset": None if next_offset is None else int(next_offset),
            "next_after_id": None if next_after_id is None else int(next_after_id),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid list points response: {payload}") from exc


def parse_delete_point(payload: Any) -> DeletePointResult:
    try:
        return DeletePointResult(
            id=int(payload["id"]), deleted=bool(payload["deleted"])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid delete point response: {payload}") from exc


def parse_delete_collection(payload: Any) -> DeleteCollectionResult:
    try:
        return DeleteCollectionResult(
            name=str(payload["name"]),
            deleted=bool(payload["deleted"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise AionBDError(f"invalid delete collection response: {payload}") from exc
